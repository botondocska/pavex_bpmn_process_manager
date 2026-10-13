use crate::engine_def_store::PgProcessDefinitionStore;
use crate::pg_external_task_store::PgExternalTaskStore;
use crate::pg_process_store::{PgProcessInstanceStore, PgTokenStore};
use bpm_engine_runtime::EngineContext;
use std::sync::Arc;

pub struct EngineDeps {
    pub process_store: Arc<PgProcessInstanceStore>,
    pub token_store: Arc<PgTokenStore>,
    pub def_store: Arc<PgProcessDefinitionStore>,
    pub ext_store: Arc<PgExternalTaskStore>,
}

#[pavex::methods]
impl EngineDeps {
    #[pavex::singleton]
    pub fn new(
        process_store: &Arc<PgProcessInstanceStore>,
        token_store: &Arc<PgTokenStore>,
        def_store: &Arc<PgProcessDefinitionStore>,
        ext_store: &Arc<PgExternalTaskStore>,
    ) -> Self {
        Self {
            process_store: process_store.clone(),
            token_store: token_store.clone(),
            def_store: def_store.clone(),
            ext_store: ext_store.clone(),
        }
    }
}

impl EngineDeps {
    pub fn context(&self) -> EngineContext {
        EngineContext {
            process_store: Some(self.process_store.clone()),
            token_store: Some(self.token_store.clone()),
            process_def_store: Some(self.def_store.clone()),
            external_task_store: Some(self.ext_store.clone()),
            ..Default::default()
        }
    }
}
