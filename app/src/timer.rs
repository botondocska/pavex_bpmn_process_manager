use crate::configuration::TimerConfig;
use crate::external_tasks::{advance_after_task, complete_and_advance};
use crate::pg_external_task_store::PgExternalTaskStore;
use bpm_engine_core::node::{NodeType, ProcessDefinition};
use bpm_engine_runtime::BpmEngine;
use bpm_engine_storage::ExternalTaskStore;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub const TIMER_PREFIX: &str = "timer:";
const WORKER: &str = "timer-service";

/// None = not a timer task type. Some(Err) = malformed timer.
pub fn parse_timer(task_type: &str) -> Option<Result<Duration, String>> {
    let rest = task_type.strip_prefix(TIMER_PREFIX)?;
    Some(parse_duration(rest).ok_or_else(|| {
        format!("bad timer '{task_type}' (use timer:30s, timer:5m, timer:2h, timer:1d)")
    }))
}

fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim().to_ascii_lowercase();
    let mult: u64 = match s.chars().last()? {
        's' => 1,
        'm' => 60,
        'h' => 3600,
        'd' => 86400,
        _ => return None,
    };
    let n: u64 = s[..s.len() - 1].trim().parse().ok()?;
    if n == 0 {
        return None;
    }
    Some(Duration::from_secs(n.checked_mul(mult)?))
}

/// Upload-time check on the compiled definition (no XML scanning).
pub fn validate_definition(def: &ProcessDefinition) -> Result<(), String> {
    for node in def.nodes.values() {
        if let NodeType::ExternalTask { task_type, .. } = &node.node_type {
            if task_type == "default" {
                return Err(format!(
                    "service task '{}' has no task type (set camunda:topic, e.g. timer:30s)",
                    node.id
                ));
            }
            if let Some(Err(e)) = parse_timer(task_type) {
                return Err(format!("service task '{}': {e}", node.id));
            }
        }
    }
    Ok(())
}

async fn poll_once(pool: &PgPool, engine: &BpmEngine) -> anyhow::Result<()> {
    let ext = PgExternalTaskStore::new(pool.clone());
    ext.reclaim_expired_locks().await?;

    for task in ext.lock_due_timers(WORKER, 20).await? {
        if let Err(e) =
            complete_and_advance(pool, engine, &task.task_id, WORKER, HashMap::new()).await
        {
            tracing::error!(error = ?e, task_id = %task.task_id, "timer fire failed");
            let _ = ext
                .fail(
                    &task.task_id,
                    WORKER,
                    e.to_string(),
                    Some(Duration::from_secs(5)),
                )
                .await;
        }
    }

    // Completed but token never moved (crash mid-advance).
    for task in ext.unadvanced(20).await? {
        if let Err(e) = advance_after_task(pool, engine, &task).await {
            tracing::error!(error = ?e, task_id = %task.task_id, "reconcile failed");
        }
    }
    Ok(())
}

pub fn spawn_timer_loop(cfg: TimerConfig, pool: PgPool, engine: Arc<BpmEngine>) {
    if !cfg.enabled {
        tracing::info!("timer loop disabled");
        return;
    }
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(cfg.poll_interval);
        loop {
            tick.tick().await;
            if let Err(e) = poll_once(&pool, &engine).await {
                tracing::error!(error = ?e, "timer poll failed");
            }
        }
    });
}
