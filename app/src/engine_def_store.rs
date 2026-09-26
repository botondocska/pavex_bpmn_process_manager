// `ProcessDefinitionStore` impl (bpm-engine-storage) backed by the
// `processes` table. No cache: every `load` re-fetches `bpmn_xml` and
// recompiles via `bpm_engine_bpmn::parse_and_compile`. ProcessDefinition
// isn't Serialize, so this is the intended design, not a shortcut.
use async_trait::async_trait;
use bpm_engine_core::node::ProcessDefinition;
use bpm_engine_storage::process_store::ProcessDefinitionStore;
use sqlx::PgPool;

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
        // TODO: trait signature showed `Result<Option<ProcessDefinition>>`
        // unqualified on docs.rs -- assumed anyhow::Result. Compiler will
        // reject this impl if it's actually a crate-local Result<T, E> alias.
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

        Ok(Some(def))
    }
}
