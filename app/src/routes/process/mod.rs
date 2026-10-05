pub mod advance;
pub mod detail;
pub mod editor;
pub mod export;
pub mod list;
pub mod start;
pub mod upload;

pub use advance::advance_instance;
pub use detail::instance_detail;
pub use editor::process_editor;
pub use export::export_process;
pub use list::{delete_instance, delete_process, list_instances, list_processes};
pub use start::start_process;
pub use upload::{process_upload, process_upload_form};

use pavex::Response;

// ---------- Shared path params ----------

#[pavex::request::path::PathParams]
pub struct ProcessIdParams {
    pub id: uuid::Uuid,
}

#[pavex::request::path::PathParams]
pub struct InstanceIdParams {
    pub id: String, // process_instances.id is VARCHAR, engine-assigned, not UUID
}

// ---------- Shared errors ----------

#[derive(Debug, thiserror::Error)]
pub enum ProcessUploadError {
    #[error("The uploaded file is not valid BPMN: {0}")]
    InvalidBpmn(String),
    #[error("Something went wrong. Please retry later.")]
    UnexpectedError(#[source] anyhow::Error),
}

#[pavex::methods]
impl ProcessUploadError {
    #[error_handler]
    pub fn into_response(&self) -> Response {
        match self {
            ProcessUploadError::InvalidBpmn(_) => Response::bad_request(),
            ProcessUploadError::UnexpectedError(_) => Response::internal_server_error(),
        }
        .set_typed_body(format!("{self}"))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessStartError {
    #[error("Process definition not found, or failed to start.")]
    NotFound,
    #[error("Something went wrong. Please retry later.")]
    UnexpectedError(#[source] anyhow::Error),
}

#[pavex::methods]
impl ProcessStartError {
    #[error_handler]
    pub fn into_response(&self) -> Response {
        match self {
            ProcessStartError::NotFound => Response::not_found(),
            ProcessStartError::UnexpectedError(_) => Response::internal_server_error(),
        }
        .set_typed_body(format!("{self}"))
    }
}

// ---------- Gateway condition parsing / choice discovery ----------
// Shared between detail.rs (rendering the form) and advance.rs would use it
// too if it ever needs to validate submitted choices server-side; kept here
// so both can reach it without a file depending on the other's internals.

use bpm_engine_core::node::EdgeCondition;

/// Extract (key, value) from a gateway edge condition, whether the compiler
/// gave us a structured VariableEq or left it as a raw EL Expression string.
/// Every condition this engine's evaluator (`el::eval_condition`) and the
/// engine's own `EdgeCondition::VariableEq` variant support reduces to simple
/// `key == "value"` string equality -- no other operators exist in this
/// engine (see bpm_engine_runtime::gateway / transition source). So this is
/// not a "best effort" parse; it's the only shape gateway conditions in this
/// engine can take. A condition that fails to match this pattern is
/// malformed BPMN, not an unsupported feature.
pub(crate) fn parse_edge_condition(condition: &EdgeCondition) -> Option<(String, String)> {
    match condition {
        EdgeCondition::VariableEq { key, value } => Some((key.clone(), value.clone())),
        EdgeCondition::Expression(expr) => {
            // Match: <ident> == "value"  or  ${ <ident> == "value" }
            let inner = expr
                .trim()
                .strip_prefix("${")
                .and_then(|s| s.strip_suffix('}'))
                .unwrap_or(expr)
                .trim();
            let (key_part, rest) = inner.split_once("==")?;
            let key = key_part.trim();
            let value = rest.trim().trim_matches('"').trim_matches('\'');
            if key.is_empty() || value.is_empty() {
                return None;
            }
            Some((key.to_string(), value.to_string()))
        }
        EdgeCondition::Default => None,
    }
}

pub(crate) struct GatewayChoice {
    pub key: String,
    pub values: Vec<String>,
}

/// If the node immediately downstream of `from_node_id` is an ExclusiveGateway,
/// return the set of (key, \[values\]) choices a user must supply to route
/// through it. Returns Ok(None) if there's no gateway there (nothing to ask),
/// or if the gateway exists but every edge is unconditional (nothing to
/// choose). Returns Err if the gateway exists and has a condition that
/// couldn't be parsed -- that's broken BPMN, not an unsupported feature, and
/// callers should surface it as an error rather than silently hide the
/// advance button.
pub(crate) fn gateway_choices_after(
    def: &bpm_engine_core::node::ProcessDefinition,
    from_node_id: &str,
) -> Result<Option<Vec<GatewayChoice>>, String> {
    use bpm_engine_core::node::NodeType;
    use std::collections::HashSet;

    let from_node = def
        .nodes
        .get(from_node_id)
        .ok_or_else(|| format!("node {from_node_id} not found in definition"))?;

    let mut grouped: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    let mut visited: HashSet<&str> = HashSet::new();
    let mut stack: Vec<&str> = from_node.outgoing_edges.iter().map(|e| e.target).collect();

    // Follow consecutive ExclusiveGateways through every branch.
    // Stop at any other node type: a user task later asks for its own input.
    while let Some(id) = stack.pop() {
        if !visited.insert(id) {
            continue; // loop-back guard
        }
        let Some(node) = def.nodes.get(id) else {
            continue;
        };
        if !matches!(node.node_type, NodeType::ExclusiveGateway) {
            continue;
        }
        for edge in &node.outgoing_edges {
            match &edge.condition {
                None | Some(EdgeCondition::Default) => {}
                Some(cond) => {
                    let (key, value) = parse_edge_condition(cond).ok_or_else(|| {
                        format!(
                            "gateway {} has an unparseable condition on edge to {}: {:?}",
                            node.id, edge.target, cond
                        )
                    })?;
                    grouped.entry(key).or_default().push(value);
                }
            }
            stack.push(edge.target);
        }
    }

    if grouped.is_empty() {
        return Ok(None);
    }

    Ok(Some(
        grouped
            .into_iter()
            .map(|(key, mut values)| {
                values.sort();
                values.dedup();
                GatewayChoice { key, values }
            })
            .collect(),
    ))
}

// ---------- Diagram active-marker discovery ----------
// Used only by detail.rs, kept here alongside gateway_choices_after since
// both walk the same ProcessDefinition/ProcessInstance shapes and are easier
// to keep in sync side by side than split further.

#[derive(serde::Serialize)]
pub(crate) struct ActiveMarker {
    pub node_id: String,
    pub label: String,
}

/// Pick the single token to mark on the instance diagram.
///
/// No schema-level ordering exists to say "which token is most recent" (no
/// created_at/seq column, and the engine deletes+reinserts the whole token
/// set on every save, so DB row order carries no meaning). Instead this
/// infers liveness from node type: Start/ExclusiveGateway tokens are known,
/// observed-in-testing to leak as permanently `Waiting` even after the
/// instance has moved past them (engine never marks them Completed/
/// Terminated -- confirmed via direct DB inspection). UserTask and
/// ParallelJoin tokens are the ones actually awaiting real progress, so only
/// those are considered "active" for marker purposes while the instance is
/// Running.
///
/// When the instance has finished (Completed), there's no per-token
/// "completed here" signal either -- so instead this marks the process's End
/// node directly, labeled "Completed", which is a statement about the whole
/// instance rather than about a specific leftover token.
///
/// Terminated instances have no per-node signal available at all (nothing
/// in the engine records where termination happened), so no marker is shown
/// for that case.
///
/// This is a heuristic, not a guarantee: the Running-state logic has only
/// been verified against straight-line and single-gateway flows. Loop-backs
/// or parallel forks that leave a *stale* Waiting UserTask/ParallelJoin
/// token (as opposed to a stale Start/Gateway token) would defeat it -- that
/// case has not been observed yet. If several tokens qualify (e.g.
/// concurrent parallel branches), one is chosen arbitrarily (smallest
/// node_id) since the caller requires a single marker; the label notes how
/// many were hidden.
pub(crate) fn pick_active_marker(
    def: &bpm_engine_core::node::ProcessDefinition,
    instance: &bpm_engine_core::ProcessInstance,
) -> Option<ActiveMarker> {
    use bpm_engine_core::node::NodeType;
    use bpm_engine_core::{InstanceState, TokenStatus};

    match instance.state {
        InstanceState::Completed => {
            // Walk all nodes rather than hardcode an id, in case a process
            // has multiple end events.
            let end_node = def
                .nodes
                .values()
                .find(|n| matches!(n.node_type, NodeType::End))?;
            return Some(ActiveMarker {
                node_id: end_node.id.to_string(),
                label: "Completed".to_string(),
            });
        }
        InstanceState::Terminated => {
            return None;
        }
        InstanceState::Running => { /* fall through */ }
    }

    let mut candidates: Vec<&str> = instance
        .tokens
        .iter()
        .filter(|t| matches!(t.status, TokenStatus::Waiting))
        .filter(|t| {
            def.nodes
                .get(t.node_id.as_str())
                .map(|n| {
                    matches!(
                        n.node_type,
                        NodeType::UserTask | NodeType::ParallelJoin { .. }
                    )
                })
                .unwrap_or(false)
        })
        .map(|t| t.node_id.as_str())
        .collect();
    candidates.sort_unstable();
    candidates.dedup();

    let first = candidates.first()?;
    let extra = candidates.len() - 1;
    let (node_id, base) = match first.strip_suffix(crate::engine_def_store::DECISION_SUFFIX) {
        Some(g) => (g.to_string(), "Decision needed"),
        None => (first.to_string(), "Waiting"),
    };
    let label = if extra == 0 {
        base.to_string()
    } else {
        format!("{base} (+{extra} parallel)")
    };
    Some(ActiveMarker { node_id, label })
}

/// Escape `</` so embedding this JSON inside a `<script>` block can't be
/// broken out of by a `</script>` sequence hiding inside, e.g., uploaded
/// BPMN XML content.
pub(crate) fn json_for_script<T: serde::Serialize>(value: &T) -> Result<String, serde_json::Error> {
    Ok(serde_json::to_string(value)?.replace("</", "<\\/"))
}
