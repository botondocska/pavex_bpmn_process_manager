use async_trait::async_trait;
use bpm_engine_core::node::{
    EdgeCondition, Node, NodeId, NodeType, OutgoingEdge, ProcessDefinition,
};
use bpm_engine_storage::process_store::ProcessDefinitionStore;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// Suffix of the implicit UserTask inserted before each deciding gateway.
pub const DECISION_SUFFIX: &str = "__decide";

/// NodeId is `&'static str`. Intern so repeated loads don't leak new strings.
fn intern(s: String) -> &'static str {
    static POOL: OnceLock<Mutex<HashMap<String, &'static str>>> = OnceLock::new();
    let mut pool = POOL.get_or_init(Default::default).lock().unwrap();
    if let Some(&v) = pool.get(&s) {
        return v;
    }
    let leaked: &'static str = Box::leak(s.clone().into_boxed_str());
    pool.insert(s, leaked);
    leaked
}

/// Insert a UserTask before every ExclusiveGateway that branches on a variable.
/// All incoming edges are redirected to the task; task -> gateway unconditional.
fn insert_decision_tasks(mut def: ProcessDefinition) -> ProcessDefinition {
    let gates: Vec<NodeId> = def
        .nodes
        .values()
        .filter(|n| {
            matches!(n.node_type, NodeType::ExclusiveGateway)
                && n.outgoing_edges.iter().any(
                    |e| matches!(&e.condition, Some(c) if !matches!(c, EdgeCondition::Default)),
                )
        })
        .map(|n| n.id)
        .collect();
    if gates.is_empty() {
        return def;
    }

    let mut redirect: HashMap<NodeId, NodeId> = HashMap::new();
    for g in gates {
        let did = intern(format!("{g}{DECISION_SUFFIX}"));
        redirect.insert(g, did);
        def.nodes.insert(
            did,
            Node {
                id: did,
                node_type: NodeType::UserTask,
                outgoing_edges: vec![OutgoingEdge {
                    target: g,
                    condition: None,
                }],
            },
        );
    }

    for node in def.nodes.values_mut() {
        if node.id.ends_with(DECISION_SUFFIX) {
            continue; // keep decision -> gateway edge intact
        }
        for e in node.outgoing_edges.iter_mut() {
            if let Some(&d) = redirect.get(e.target) {
                e.target = d;
            }
        }
    }
    def
}

pub struct PgProcessDefinitionStore {
    pool: PgPool,
}

impl PgProcessDefinitionStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ProcessDefinitionStore for PgProcessDefinitionStore {
    async fn load(&self, id: &str) -> anyhow::Result<Option<ProcessDefinition>> {
        let Ok(process_id) = uuid::Uuid::parse_str(id) else {
            return Ok(None);
        };

        let row = sqlx::query!(
            r#"SELECT bpmn_xml FROM processes WHERE id = $1"#,
            process_id,
        )
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let def = bpm_engine_bpmn::parse_and_compile(&row.bpmn_xml)
            .map_err(|e| anyhow::anyhow!("Failed to compile stored BPMN for {id}: {e}"))?;

        Ok(Some(insert_decision_tasks(def)))
    }
}
