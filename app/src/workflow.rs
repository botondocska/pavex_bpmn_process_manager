//! Embedded `engine-core` wiring: singleton, boot restore, background workers.

use crate::pg_persistence::PgWorkflowPersistence;
use engine_core::{EngineResult, PendingServiceTask, WorkflowEngine, WorkflowPersistence};
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const WORKER_ID: &str = "embedded-worker";

#[derive(Debug, thiserror::Error)]
pub enum EngineInitError {
    #[error("Failed to restore workflow engine state")]
    Restore(#[source] anyhow::Error),
}

/// Build engine, restore state from Postgres, start background tasks.
#[pavex::singleton]
pub async fn workflow_engine(pool: &PgPool) -> Result<Arc<WorkflowEngine>, EngineInitError> {
    let persistence = Arc::new(PgWorkflowPersistence::new(pool.clone()));
    let engine =
        WorkflowEngine::new().with_persistence(persistence.clone() as Arc<dyn WorkflowPersistence>);
    restore(&engine, &persistence, pool)
        .await
        .map_err(EngineInitError::Restore)?;
    let engine = Arc::new(engine);
    spawn_timer_scheduler(engine.clone());
    spawn_service_worker(engine.clone());
    Ok(engine)
}

async fn restore(
    engine: &WorkflowEngine,
    p: &PgWorkflowPersistence,
    pool: &PgPool,
) -> anyhow::Result<()> {
    // Definitions: re-parse from `processes`; definition key == process id.
    let rows: Vec<(Uuid, String)> = sqlx::query_as("SELECT id, bpmn_xml FROM processes")
        .fetch_all(pool)
        .await?;
    for (id, xml) in rows {
        if let Err(e) = deploy_xml(engine, id, &xml).await {
            tracing::warn!("skip process {id}: {e}");
        }
    }
    for i in p.list_instances().await? {
        engine.restore_instance(i).await;
    }
    for t in p.list_user_tasks().await? {
        engine.restore_user_task(t);
    }
    for t in p.list_service_tasks().await? {
        engine.restore_service_task(t);
    }
    for t in p.list_timers().await? {
        engine.restore_timer(t);
    }
    for c in p.list_message_catches().await? {
        engine.restore_message_catch(c);
    }
    Ok(())
}

pub async fn deploy_xml(engine: &WorkflowEngine, id: Uuid, xml: &str) -> EngineResult<()> {
    let mut def = bpmn_parser::parse_bpmn_xml(xml)?;
    def.key = id;
    engine.deploy_definition(def).await;
    Ok(())
}

/// Ok(false) = process row missing.
pub async fn ensure_deployed(
    engine: &WorkflowEngine,
    pool: &PgPool,
    id: Uuid,
) -> anyhow::Result<bool> {
    if engine.get_definition(&id).await.is_some() {
        return Ok(true);
    }
    let xml: Option<String> = sqlx::query_scalar("SELECT bpmn_xml FROM processes WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await?;
    let Some(xml) = xml else {
        return Ok(false);
    };
    deploy_xml(engine, id, &xml).await?;
    Ok(true)
}

fn spawn_timer_scheduler(engine: Arc<WorkflowEngine>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            if let Err(e) = engine.process_timers().await {
                tracing::error!("timer scheduler: {e}");
            }
        }
    });
}

/// In-process replacement for external fetch-and-lock workers.
fn spawn_service_worker(engine: Arc<WorkflowEngine>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            let mut topics: Vec<String> = engine
                .get_pending_service_tasks()
                .into_iter()
                .filter(|t| t.retries > 0)
                .map(|t| t.topic)
                .collect();
            topics.sort();
            topics.dedup();
            if topics.is_empty() {
                continue;
            }
            for task in engine
                .fetch_and_lock_service_tasks(WORKER_ID, 10, &topics, 30)
                .await
            {
                if task.retries <= 0 {
                    continue; // incident, leave alone
                }
                match run_topic(&task) {
                    Ok(vars) => {
                        if let Err(e) = engine.complete_service_task(task.id, WORKER_ID, vars).await
                        {
                            tracing::error!("complete service task {}: {e}", task.id);
                        }
                    }
                    Err(msg) => {
                        if let Err(e) = engine
                            .fail_service_task(task.id, WORKER_ID, None, Some(msg), None)
                            .await
                        {
                            tracing::error!("fail service task {}: {e}", task.id);
                        }
                    }
                }
            }
        }
    });
}

/// Extension point: match on `task.topic`, do work (e.g. send email), return output vars.
fn run_topic(task: &PendingServiceTask) -> Result<HashMap<String, Value>, String> {
    match task.topic.as_str() {
        // parser emits these for pass-through events / unhandled boundary events
        "noop" | "event_passthrough" => {}
        other => tracing::warn!(topic = other, "no handler registered, auto-completing"),
    }
    Ok(HashMap::new())
}
