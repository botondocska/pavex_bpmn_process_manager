//! Postgres `ExternalTaskStore` backed by `external_tasks`.
//! Runtime (non-macro) queries: no sqlx offline data to regenerate.

use async_trait::async_trait;
use bpm_engine_core::{ExternalTask, ExternalTaskState};
use bpm_engine_storage::ExternalTaskStore;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub struct PgExternalTaskStore {
    pool: PgPool,
}

impl PgExternalTaskStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

const COLS: &str = "task_id, token_id, instance_id, task_type, state, lock_owner, \
                    lock_expire_at, retries, error_message, variables, created_at, updated_at";

#[derive(sqlx::FromRow)]
struct TaskRow {
    task_id: uuid::Uuid,
    token_id: String,
    instance_id: String,
    task_type: String,
    state: String,
    lock_owner: Option<String>,
    lock_expire_at: Option<OffsetDateTime>,
    retries: i32,
    error_message: Option<String>,
    variables: serde_json::Value,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

fn fmt(t: OffsetDateTime) -> String {
    t.format(&Rfc3339).unwrap_or_else(|_| t.to_string())
}

fn state_from_str(s: &str) -> anyhow::Result<ExternalTaskState> {
    match s {
        "READY" => Ok(ExternalTaskState::Ready),
        "LOCKED" => Ok(ExternalTaskState::Locked),
        "COMPLETED" => Ok(ExternalTaskState::Completed),
        "FAILED" => Ok(ExternalTaskState::Failed),
        other => anyhow::bail!("unknown external task state in DB: {other}"),
    }
}

impl TaskRow {
    fn into_task(self) -> anyhow::Result<ExternalTask> {
        Ok(ExternalTask {
            task_id: self.task_id.to_string(),
            token_id: self.token_id,
            process_instance_id: self.instance_id,
            task_type: self.task_type,
            state: state_from_str(&self.state)?,
            lock_owner: self.lock_owner,
            lock_expire_at: self.lock_expire_at.map(fmt),
            retries: self.retries,
            error_message: self.error_message,
            variables: serde_json::from_value(self.variables)?,
            created_at: Some(fmt(self.created_at)),
            updated_at: Some(fmt(self.updated_at)),
        })
    }
}

impl PgExternalTaskStore {
    /// Lock READY + due tasks. Filter by exact types and/or SQL LIKE pattern.
    async fn lock_ready(
        &self,
        worker_id: &str,
        lock_secs: f64,
        types: Option<Vec<String>>,
        like: Option<&str>,
        limit: i64,
    ) -> anyhow::Result<Vec<ExternalTask>> {
        let sql = format!(
            r#"
            UPDATE external_tasks
            SET state = 'LOCKED', lock_owner = $1,
                lock_expire_at = now() + make_interval(secs => $2),
                updated_at = now()
            WHERE task_id IN (
                SELECT task_id FROM external_tasks
                WHERE state = 'READY' AND due_at <= now()
                  AND ($3::text[] IS NULL OR task_type = ANY($3))
                  AND ($4::text IS NULL OR task_type LIKE $4)
                ORDER BY due_at
                LIMIT $5
                FOR UPDATE SKIP LOCKED
            )
            RETURNING {COLS}
            "#
        );
        let rows: Vec<TaskRow> = sqlx::query_as(&sql)
            .bind(worker_id)
            .bind(lock_secs)
            .bind(types)
            .bind(like)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(TaskRow::into_task).collect()
    }

    /// Timer service: lock due `timer:*` tasks.
    pub async fn lock_due_timers(
        &self,
        worker_id: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<ExternalTask>> {
        self.lock_ready(worker_id, 60.0, None, Some("timer:%"), limit)
            .await
    }

    /// Instance cancelled: kill open tasks so no worker completes them.
    pub async fn cancel_for_instance(&self, instance_id: &str) -> anyhow::Result<u64> {
        let r = sqlx::query(
            r#"UPDATE external_tasks
               SET state = 'FAILED', error_message = 'instance cancelled',
                   lock_owner = NULL, lock_expire_at = NULL, updated_at = now()
               WHERE instance_id = $1 AND state IN ('READY', 'LOCKED')"#,
        )
        .bind(instance_id)
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected())
    }

    /// COMPLETED tasks whose token transition never finished (crash between).
    pub async fn unadvanced(&self, limit: i64) -> anyhow::Result<Vec<ExternalTask>> {
        let sql = format!(
            r#"SELECT {COLS} FROM external_tasks
               WHERE state = 'COMPLETED' AND advanced_at IS NULL
                 AND updated_at < now() - interval '15 seconds'
               ORDER BY updated_at LIMIT $1"#
        );
        let rows: Vec<TaskRow> = sqlx::query_as(&sql)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(TaskRow::into_task).collect()
    }

    pub async fn is_advanced(&self, task_id: &str) -> anyhow::Result<bool> {
        let id = uuid::Uuid::parse_str(task_id)?;
        let v: Option<bool> = sqlx::query_scalar(
            r#"SELECT advanced_at IS NOT NULL FROM external_tasks WHERE task_id = $1"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(v.unwrap_or(true))
    }

    pub async fn mark_advanced(&self, task_id: &str) -> anyhow::Result<()> {
        let id = uuid::Uuid::parse_str(task_id)?;
        sqlx::query(r#"UPDATE external_tasks SET advanced_at = now() WHERE task_id = $1"#)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Worker-provided output only (not the stale instance snapshot in `variables`).
    pub async fn result_of(&self, task_id: &str) -> anyhow::Result<HashMap<String, String>> {
        let id = uuid::Uuid::parse_str(task_id)?;
        let v: Option<serde_json::Value> =
            sqlx::query_scalar(r#"SELECT result FROM external_tasks WHERE task_id = $1"#)
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(match v {
            Some(v) => serde_json::from_value(v)?,
            None => HashMap::new(),
        })
    }

    /// (token_id, task_type, due_at) of open tasks, for the instance page.
    pub async fn pending_for_instance(
        &self,
        instance_id: &str,
    ) -> anyhow::Result<Vec<(String, String, OffsetDateTime)>> {
        Ok(sqlx::query_as(
            r#"SELECT token_id, task_type, due_at FROM external_tasks
               WHERE instance_id = $1 AND state IN ('READY', 'LOCKED')"#,
        )
        .bind(instance_id)
        .fetch_all(&self.pool)
        .await?)
    }
}

#[async_trait]
impl ExternalTaskStore for PgExternalTaskStore {
    async fn create(
        &self,
        token_id: &str,
        process_instance_id: &str,
        task_type: &str,
        retries: i32,
        timeout_secs: u64,
        variables: HashMap<String, String>,
    ) -> anyhow::Result<String> {
        // Timers start now: due_at = now + duration. Everything else is due immediately.
        let delay_secs = match crate::timer::parse_timer(task_type) {
            Some(Ok(d)) => d.as_secs() as f64,
            Some(Err(e)) => anyhow::bail!(e),
            None => 0.0,
        };
        let task_id = uuid::Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO external_tasks
               (task_id, token_id, instance_id, task_type, state, retries, timeout_secs, variables, due_at)
               VALUES ($1, $2, $3, $4, 'READY', $5, $6, $7, now() + make_interval(secs => $8))"#,
        )
        .bind(task_id)
        .bind(token_id)
        .bind(process_instance_id)
        .bind(task_type)
        .bind(retries)
        .bind(i64::try_from(timeout_secs)?)
        .bind(serde_json::to_value(&variables)?)
        .bind(delay_secs)
        .execute(&self.pool)
        .await?;
        Ok(task_id.to_string())
    }

    async fn fetch_and_lock(
        &self,
        worker_id: &str,
        task_types: &[String],
        max_tasks: usize,
        lock_duration: Duration,
    ) -> anyhow::Result<Vec<ExternalTask>> {
        self.lock_ready(
            worker_id,
            lock_duration.as_secs_f64(),
            Some(task_types.to_vec()),
            None,
            max_tasks as i64,
        )
        .await
    }

    async fn complete(
        &self,
        task_id: &str,
        worker_id: &str,
        variables: HashMap<String, String>,
    ) -> anyhow::Result<()> {
        let id = uuid::Uuid::parse_str(task_id)?;
        let vars = serde_json::to_value(&variables)?;
        let r = sqlx::query(
            r#"UPDATE external_tasks
               SET state = 'COMPLETED', variables = variables || $3::jsonb,
                   result = $3::jsonb, updated_at = now()
               WHERE task_id = $1 AND state = 'LOCKED'
                 AND lock_owner = $2 AND lock_expire_at > now()"#,
        )
        .bind(id)
        .bind(worker_id)
        .bind(vars)
        .execute(&self.pool)
        .await?;
        if r.rows_affected() != 1 {
            anyhow::bail!(
                "task {task_id} not completable (unknown, not locked by worker, or lock expired)"
            );
        }
        Ok(())
    }

    async fn fail(
        &self,
        task_id: &str,
        worker_id: &str,
        error: String,
        retry_after: Option<Duration>,
    ) -> anyhow::Result<()> {
        let id = uuid::Uuid::parse_str(task_id)?;
        let delay = retry_after.map(|d| d.as_secs_f64()).unwrap_or(0.0);
        sqlx::query(
            r#"UPDATE external_tasks
               SET retries = retries - 1,
                   state = CASE WHEN retries - 1 > 0 THEN 'READY' ELSE 'FAILED' END,
                   error_message = $3,
                   lock_owner = NULL, lock_expire_at = NULL,
                   due_at = CASE WHEN retries - 1 > 0
                                 THEN now() + make_interval(secs => $4) ELSE due_at END,
                   updated_at = now()
               WHERE task_id = $1 AND state = 'LOCKED' AND lock_owner = $2"#,
        )
        .bind(id)
        .bind(worker_id)
        .bind(error)
        .bind(delay)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn reclaim_expired_locks(&self) -> anyhow::Result<usize> {
        let r = sqlx::query(
            r#"UPDATE external_tasks
               SET state = 'READY', lock_owner = NULL, lock_expire_at = NULL, updated_at = now()
               WHERE state = 'LOCKED' AND lock_expire_at < now()"#,
        )
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected() as usize)
    }

    async fn get(&self, task_id: &str) -> anyhow::Result<Option<ExternalTask>> {
        let Ok(id) = uuid::Uuid::parse_str(task_id) else {
            return Ok(None);
        };
        let sql = format!("SELECT {COLS} FROM external_tasks WHERE task_id = $1");
        let row: Option<TaskRow> = sqlx::query_as(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(TaskRow::into_task).transpose()
    }
}
