//! Complete an external task and move its token on. Shared by the timer loop
//! and, later, email/message workers.

use crate::engine_def_store::PgProcessDefinitionStore;
use crate::pg_external_task_store::PgExternalTaskStore;
use crate::pg_process_store::{PgProcessInstanceStore, PgTokenStore};
use bpm_engine_core::event::{EngineEvent, payloads};
use bpm_engine_core::{ExternalTask, InstanceState, TokenStatus};
use bpm_engine_runtime::{BpmEngine, EngineContext};
use bpm_engine_storage::{ExternalTaskStore, process_store::ProcessInstanceStore};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;

/// Engine context for callers outside Pavex (background loops).
pub fn engine_ctx(pool: &PgPool) -> EngineContext {
    EngineContext {
        process_store: Some(Arc::new(PgProcessInstanceStore::new(pool.clone()))),
        token_store: Some(Arc::new(PgTokenStore::new(pool.clone()))),
        process_def_store: Some(Arc::new(PgProcessDefinitionStore::new(pool.clone()))),
        external_task_store: Some(Arc::new(PgExternalTaskStore::new(pool.clone()))),
        ..Default::default()
    }
}

/// Worker finished. Store the result, then advance the token.
/// Err only if `complete` failed. A failed advance is logged and the
/// reconciler retries it.
pub async fn complete_and_advance(
    pool: &PgPool,
    engine: &BpmEngine,
    task_id: &str,
    worker_id: &str,
    variables: HashMap<String, String>,
) -> anyhow::Result<()> {
    let ext = PgExternalTaskStore::new(pool.clone());
    ext.complete(task_id, worker_id, variables).await?;
    let task = ext
        .get(task_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("task {task_id} vanished after complete"))?;
    if let Err(e) = advance_after_task(pool, engine, &task).await {
        tracing::error!(error = ?e, task_id, "advance after task failed; reconciler will retry");
    }
    Ok(())
}

/// Idempotent. Advisory lock stops two callers firing the same transition
/// (the engine would duplicate downstream tokens).
pub async fn advance_after_task(
    pool: &PgPool,
    engine: &BpmEngine,
    task: &ExternalTask,
) -> anyhow::Result<()> {
    let ext = PgExternalTaskStore::new(pool.clone());
    let mut tx = pool.begin().await?;
    let got: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtext($1))")
        .bind(&task.task_id)
        .fetch_one(&mut *tx)
        .await?;
    if !got || ext.is_advanced(&task.task_id).await? {
        return Ok(());
    }

    let process_store = Arc::new(PgProcessInstanceStore::new(pool.clone()));
    if let Some(mut instance) = process_store.load(&task.process_instance_id).await? {
        let waiting_node = instance
            .tokens
            .iter()
            .find(|t| t.id == task.token_id && matches!(t.status, TokenStatus::Waiting))
            .map(|t| t.node_id.clone());

        // Instance gone/cancelled/finished, or token already moved: nothing to do.
        if let (true, Some(node_id)) = (
            matches!(instance.state, InstanceState::Running),
            waiting_node,
        ) {
            // UserTaskCompletedHandler drops event variables. Merge ourselves.
            let result = ext.result_of(&task.task_id).await?;
            if !result.is_empty() {
                instance.variables.extend(result);
                process_store.save(&instance).await?;
            }

            let mut ctx = engine_ctx(pool);
            engine
                .run_async(
                    EngineEvent::UserTaskCompleted(payloads::UserTaskCompleted {
                        task_id: task.token_id.clone(),
                        instance_id: task.process_instance_id.clone(),
                        node_id,
                        variables: Default::default(),
                    }),
                    &mut ctx,
                )
                .await;

            // run_async swallows errors. Verify the token left the node.
            let still_there = process_store
                .load(&task.process_instance_id)
                .await?
                .map(|i| i.tokens.iter().any(|t| t.id == task.token_id))
                .unwrap_or(false);
            if still_there {
                anyhow::bail!(
                    "token {} still at node after completion event",
                    task.token_id
                );
            }
        }
    }

    ext.mark_advanced(&task.task_id).await?;
    tx.commit().await?;
    Ok(())
}
