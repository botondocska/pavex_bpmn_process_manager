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

use engine_core::{BpmnElement, EngineError, ProcessDefinition};
use pavex::Response;
use serde_json::Value;

// ---------- Shared path params ----------

#[pavex::request::path::PathParams]
pub struct ProcessIdParams {
    pub id: uuid::Uuid,
}

#[pavex::request::path::PathParams]
pub struct InstanceIdParams {
    pub id: uuid::Uuid,
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
    #[error("Process or instance not found.")]
    NotFound,
    #[error("Invalid input: {0}")]
    InvalidChoice(String),
    #[error("Engine error: {0}")]
    Engine(#[from] EngineError),
    #[error("Something went wrong. Please retry later.")]
    UnexpectedError(#[source] anyhow::Error),
}

#[pavex::methods]
impl ProcessStartError {
    #[error_handler]
    pub fn into_response(&self) -> Response {
        match self {
            ProcessStartError::NotFound
            | ProcessStartError::Engine(EngineError::NoSuchInstance(_))
            | ProcessStartError::Engine(EngineError::NoSuchDefinition(_)) => Response::not_found(),
            ProcessStartError::InvalidChoice(_) | ProcessStartError::Engine(_) => {
                Response::bad_request()
            }
            ProcessStartError::UnexpectedError(_) => Response::internal_server_error(),
        }
        .set_typed_body(format!("{self}"))
    }
}

// ---------- Gateway choice discovery ----------
// Exclusive-gateway conditions are plain `key == value` (engine-core's evaluator).
// After a user task, ask user for every key/value that downstream gateways branch on.

pub(crate) struct ChoiceValue {
    pub label: String,
    pub value: Value,
}

pub(crate) struct GatewayChoice {
    pub key: String,
    pub values: Vec<ChoiceValue>,
}

fn unquote(s: &str) -> Option<&str> {
    let b = s.as_bytes();
    if b.len() >= 2
        && ((b[0] == b'"' && b[b.len() - 1] == b'"') || (b[0] == b'\'' && b[b.len() - 1] == b'\''))
    {
        Some(&s[1..s.len() - 1])
    } else {
        None
    }
}

/// Mirror engine-core `parse_rhs`: quoted -> string, else bool/number/string.
fn literal(s: &str) -> Value {
    match s {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        _ => {
            if let Ok(n) = s.parse::<i64>() {
                Value::from(n)
            } else if let Some(n) = s.parse::<f64>().ok().and_then(serde_json::Number::from_f64) {
                Value::Number(n)
            } else {
                Value::String(s.to_string())
            }
        }
    }
}

fn parse_condition(cond: &str) -> Option<(String, ChoiceValue)> {
    let (k, rhs) = cond.trim().split_once("==")?;
    let key = k.trim();
    let rhs = rhs.trim();
    if key.is_empty() || rhs.is_empty() || !key.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }
    let (label, value) = match unquote(rhs) {
        Some(s) => (s.to_string(), Value::String(s.to_string())),
        None => (rhs.to_string(), literal(rhs)),
    };
    Some((key.to_string(), ChoiceValue { label, value }))
}

/// Walk consecutive exclusive gateways after `from`; collect `==` choices.
/// Non-`==` conditions skipped (engine evaluates them against existing variables).
pub(crate) fn gateway_choices_after(
    def: &ProcessDefinition,
    from: &str,
) -> Result<Option<Vec<GatewayChoice>>, String> {
    use std::collections::{BTreeMap, HashSet};

    if def.get_node(from).is_none() {
        return Err(format!("node {from} not found in definition"));
    }
    let mut grouped: BTreeMap<String, Vec<ChoiceValue>> = BTreeMap::new();
    let mut visited: HashSet<&str> = HashSet::new();
    let mut stack: Vec<&str> = def
        .next_nodes(from)
        .iter()
        .map(|f| f.target.as_str())
        .collect();

    while let Some(id) = stack.pop() {
        if !visited.insert(id) {
            continue;
        }
        if !matches!(def.get_node(id), Some(BpmnElement::ExclusiveGateway { .. })) {
            continue;
        }
        for flow in def.next_nodes(id) {
            if let Some(c) = flow.condition.as_deref()
                && let Some((key, v)) = parse_condition(c)
            {
                grouped.entry(key).or_default().push(v);
            }
            stack.push(flow.target.as_str());
        }
    }

    if grouped.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        grouped
            .into_iter()
            .map(|(key, mut values)| {
                values.sort_by(|a, b| a.label.cmp(&b.label));
                values.dedup_by(|a, b| a.label == b.label);
                GatewayChoice { key, values }
            })
            .collect(),
    ))
}

// ---------- Diagram markers ----------

#[derive(serde::Serialize)]
pub(crate) struct ActiveMarker {
    pub node_id: String,
    pub label: String,
}

/// Escape `</` so embedded JSON can't break out of a `<script>` block.
pub(crate) fn json_for_script<T: serde::Serialize>(value: &T) -> Result<String, serde_json::Error> {
    Ok(serde_json::to_string(value)?.replace("</", "<\\/"))
}
