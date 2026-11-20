//! Postgres `WorkflowPersistence` for embedded `engine-core`.
//!
//! CRUD paths use compile-checked `query!` macros: metadata lives in `.sqlx/`
//! (run `cargo sqlx prepare --workspace` after any SQL change).
//! Monitoring + bucket browser stay runtime: table/column names are dynamic,
//! so macros cannot check them.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use engine_core::{
    BucketEntry, BucketEntryDetail, BucketInfo, CompletedInstanceQuery, EngineError, EngineResult,
    HistoryEntry, HistoryQuery, InstanceState, PendingMessageCatch, PendingServiceTask,
    PendingTimer, PendingUserTask, ProcessDefinition, ProcessInstance, StorageInfo, Token,
    WorkflowPersistence,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

fn pe(e: impl std::fmt::Display) -> EngineError {
    EngineError::PersistenceError(e.to_string())
}
fn uid(s: &str) -> EngineResult<Uuid> {
    Uuid::parse_str(s).map_err(pe)
}
fn js<T: Serialize>(v: &T) -> EngineResult<Value> {
    serde_json::to_value(v).map_err(pe)
}
fn de<T: DeserializeOwned>(v: Value) -> EngineResult<T> {
    serde_json::from_value(v).map_err(pe)
}
/// Unit-variant enum -> its serde string name.
fn tag<T: Serialize>(v: &T) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|x| x.as_str().map(str::to_owned))
        .unwrap_or_default()
}

pub fn state_label(s: &InstanceState) -> &'static str {
    match s {
        InstanceState::Running => "Running",
        InstanceState::WaitingOnUserTask { .. } => "WaitingOnUserTask",
        InstanceState::WaitingOnServiceTask { .. } => "WaitingOnServiceTask",
        InstanceState::WaitingOnTimer { .. } => "WaitingOnTimer",
        InstanceState::WaitingOnMessage { .. } => "WaitingOnMessage",
        InstanceState::WaitingOnEventBasedGateway => "WaitingOnEventBasedGateway",
        InstanceState::ParallelExecution { .. } => "ParallelExecution",
        InstanceState::WaitingOnCallActivity { .. } => "WaitingOnCallActivity",
        InstanceState::Completed => "Completed",
        InstanceState::CompletedWithError { .. } => "CompletedWithError",
        InstanceState::Suspended { .. } => "Suspended",
    }
}

fn terminal_label(s: &InstanceState) -> &'static str {
    match s {
        InstanceState::Completed => "completed",
        InstanceState::CompletedWithError { .. } => "error",
        _ => "other",
    }
}

// ---- bucket browser metadata (dynamic SQL, runtime queries only) ----

struct Bucket {
    name: &'static str,
    table: &'static str,
    key: &'static str,
    ts: &'static str,
    data: &'static str,
    size: &'static str,
    kind: &'static str,
    encoding: &'static str,
}

macro_rules! json_bucket {
    ($name:expr, $table:expr, $key:expr) => {
        Bucket {
            name: $name,
            table: $table,
            key: $key,
            ts: "updated_at",
            data: "data::text",
            size: "octet_length(data::text)",
            kind: "kv",
            encoding: "utf8",
        }
    };
}

const BUCKETS: &[Bucket] = &[
    json_bucket!("instances", "wf_instances", "id::text"),
    json_bucket!("definitions", "wf_definitions", "id::text"),
    json_bucket!("user_tasks", "wf_user_tasks", "task_id::text"),
    json_bucket!("service_tasks", "wf_service_tasks", "id::text"),
    json_bucket!("timers", "wf_timers", "id::text"),
    json_bucket!("messages", "wf_message_catches", "id::text"),
    json_bucket!(
        "tokens",
        "wf_tokens",
        "(instance_id::text || '-' || token_id::text)"
    ),
    json_bucket!("history_instances", "wf_completed_instances", "id::text"),
    Bucket {
        name: "history",
        table: "wf_history",
        key: "id::text",
        ts: "ts",
        data: "data::text",
        size: "octet_length(data::text)",
        kind: "stream",
        encoding: "utf8",
    },
    Bucket {
        name: "bpmn_xml",
        table: "processes",
        key: "id::text",
        ts: "updated_at",
        data: "bpmn_xml",
        size: "octet_length(bpmn_xml)",
        kind: "object_store",
        encoding: "utf8",
    },
    Bucket {
        name: "instance_files",
        table: "wf_files",
        key: "object_key",
        ts: "updated_at",
        data: "encode(data, 'base64')",
        size: "octet_length(data)",
        kind: "object_store",
        encoding: "base64",
    },
];

fn bucket(name: &str) -> EngineResult<&'static Bucket> {
    BUCKETS
        .iter()
        .find(|b| b.name == name)
        .ok_or_else(|| pe(format!("Unknown bucket name: {name}")))
}

#[derive(Clone)]
pub struct PgWorkflowPersistence {
    pool: PgPool,
}

impl PgWorkflowPersistence {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl WorkflowPersistence for PgWorkflowPersistence {
    // ---- tokens ----
    async fn save_token(&self, instance_id: Uuid, token: &Token) -> EngineResult<()> {
        sqlx::query!(
            "INSERT INTO wf_tokens (instance_id, token_id, data) VALUES ($1, $2, $3) \
             ON CONFLICT (instance_id, token_id) DO UPDATE \
             SET data = EXCLUDED.data, updated_at = now()",
            instance_id,
            token.id,
            js(token)?,
        )
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }

    async fn load_tokens(&self, instance_id: Uuid) -> EngineResult<Vec<Token>> {
        sqlx::query!(
            "SELECT data FROM wf_tokens WHERE instance_id = $1",
            instance_id
        )
        .fetch_all(&self.pool)
        .await
        .map_err(pe)?
        .into_iter()
        .map(|r| de(r.data))
        .collect()
    }

    async fn delete_token(&self, instance_id: Uuid, token_id: Uuid) -> EngineResult<()> {
        sqlx::query!(
            "DELETE FROM wf_tokens WHERE instance_id = $1 AND token_id = $2",
            instance_id,
            token_id,
        )
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }

    // ---- instances ----
    async fn save_instance(&self, i: &ProcessInstance) -> EngineResult<()> {
        sqlx::query!(
            "INSERT INTO wf_instances \
               (id, definition_key, business_key, state_label, data, started_at, completed_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (id) DO UPDATE SET \
               definition_key = EXCLUDED.definition_key, business_key = EXCLUDED.business_key, \
               state_label = EXCLUDED.state_label, data = EXCLUDED.data, \
               started_at = EXCLUDED.started_at, completed_at = EXCLUDED.completed_at, \
               updated_at = now()",
            i.id,
            i.definition_key,
            &i.business_key,
            state_label(&i.state),
            js(i)?,
            i.started_at,
            i.completed_at,
        )
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }

    async fn list_instances(&self) -> EngineResult<Vec<ProcessInstance>> {
        sqlx::query!("SELECT data FROM wf_instances ORDER BY updated_at")
            .fetch_all(&self.pool)
            .await
            .map_err(pe)?
            .into_iter()
            .map(|r| de(r.data))
            .collect()
    }

    async fn delete_instance(&self, id: &str) -> EngineResult<()> {
        let Ok(id) = Uuid::parse_str(id) else {
            return Ok(());
        };
        let mut tx = self.pool.begin().await.map_err(pe)?;
        sqlx::query!("DELETE FROM wf_tokens WHERE instance_id = $1", id)
            .execute(&mut *tx)
            .await
            .map_err(pe)?;
        sqlx::query!("DELETE FROM wf_history WHERE instance_id = $1", id)
            .execute(&mut *tx)
            .await
            .map_err(pe)?;
        sqlx::query!("DELETE FROM wf_completed_instances WHERE id = $1", id)
            .execute(&mut *tx)
            .await
            .map_err(pe)?;
        sqlx::query!("DELETE FROM wf_instances WHERE id = $1", id)
            .execute(&mut *tx)
            .await
            .map_err(pe)?;
        tx.commit().await.map_err(pe)?;
        Ok(())
    }

    // ---- definitions ----
    async fn save_definition(&self, d: &ProcessDefinition) -> EngineResult<()> {
        sqlx::query!(
            "INSERT INTO wf_definitions (id, data) VALUES ($1, $2) \
             ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()",
            d.key,
            js(d)?,
        )
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }
    async fn list_definitions(&self) -> EngineResult<Vec<ProcessDefinition>> {
        sqlx::query!("SELECT data FROM wf_definitions ORDER BY updated_at")
            .fetch_all(&self.pool)
            .await
            .map_err(pe)?
            .into_iter()
            .map(|r| de(r.data))
            .collect()
    }
    async fn delete_definition(&self, key: &str) -> EngineResult<()> {
        sqlx::query!("DELETE FROM wf_definitions WHERE id = $1", uid(key)?)
            .execute(&self.pool)
            .await
            .map_err(pe)?;
        Ok(())
    }

    // ---- user tasks ----
    async fn save_user_task(&self, t: &PendingUserTask) -> EngineResult<()> {
        sqlx::query!(
            "INSERT INTO wf_user_tasks (task_id, data) VALUES ($1, $2) \
             ON CONFLICT (task_id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()",
            t.task_id,
            js(t)?,
        )
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }
    async fn delete_user_task(&self, task_id: Uuid) -> EngineResult<()> {
        sqlx::query!("DELETE FROM wf_user_tasks WHERE task_id = $1", task_id)
            .execute(&self.pool)
            .await
            .map_err(pe)?;
        Ok(())
    }
    async fn list_user_tasks(&self) -> EngineResult<Vec<PendingUserTask>> {
        sqlx::query!("SELECT data FROM wf_user_tasks ORDER BY updated_at")
            .fetch_all(&self.pool)
            .await
            .map_err(pe)?
            .into_iter()
            .map(|r| de(r.data))
            .collect()
    }

    // ---- service tasks ----
    async fn save_service_task(&self, t: &PendingServiceTask) -> EngineResult<()> {
        sqlx::query!(
            "INSERT INTO wf_service_tasks (id, data) VALUES ($1, $2) \
             ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()",
            t.id,
            js(t)?,
        )
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }
    async fn delete_service_task(&self, task_id: Uuid) -> EngineResult<()> {
        sqlx::query!("DELETE FROM wf_service_tasks WHERE id = $1", task_id)
            .execute(&self.pool)
            .await
            .map_err(pe)?;
        Ok(())
    }
    async fn list_service_tasks(&self) -> EngineResult<Vec<PendingServiceTask>> {
        sqlx::query!("SELECT data FROM wf_service_tasks ORDER BY updated_at")
            .fetch_all(&self.pool)
            .await
            .map_err(pe)?
            .into_iter()
            .map(|r| de(r.data))
            .collect()
    }

    // ---- timers ----
    async fn save_timer(&self, t: &PendingTimer) -> EngineResult<()> {
        sqlx::query!(
            "INSERT INTO wf_timers (id, data) VALUES ($1, $2) \
             ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()",
            t.id,
            js(t)?,
        )
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }
    async fn delete_timer(&self, timer_id: Uuid) -> EngineResult<()> {
        sqlx::query!("DELETE FROM wf_timers WHERE id = $1", timer_id)
            .execute(&self.pool)
            .await
            .map_err(pe)?;
        Ok(())
    }
    async fn list_timers(&self) -> EngineResult<Vec<PendingTimer>> {
        sqlx::query!("SELECT data FROM wf_timers ORDER BY updated_at")
            .fetch_all(&self.pool)
            .await
            .map_err(pe)?
            .into_iter()
            .map(|r| de(r.data))
            .collect()
    }

    // ---- message catches ----
    async fn save_message_catch(&self, c: &PendingMessageCatch) -> EngineResult<()> {
        sqlx::query!(
            "INSERT INTO wf_message_catches (id, data) VALUES ($1, $2) \
             ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()",
            c.id,
            js(c)?,
        )
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }
    async fn delete_message_catch(&self, catch_id: Uuid) -> EngineResult<()> {
        sqlx::query!("DELETE FROM wf_message_catches WHERE id = $1", catch_id)
            .execute(&self.pool)
            .await
            .map_err(pe)?;
        Ok(())
    }
    async fn list_message_catches(&self) -> EngineResult<Vec<PendingMessageCatch>> {
        sqlx::query!("SELECT data FROM wf_message_catches ORDER BY updated_at")
            .fetch_all(&self.pool)
            .await
            .map_err(pe)?
            .into_iter()
            .map(|r| de(r.data))
            .collect()
    }

    // ---- files ----
    async fn save_file(&self, object_key: &str, data: &[u8]) -> EngineResult<()> {
        sqlx::query!(
            "INSERT INTO wf_files (object_key, data) VALUES ($1, $2) \
             ON CONFLICT (object_key) DO UPDATE SET data = EXCLUDED.data, updated_at = now()",
            object_key,
            data,
        )
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }

    async fn load_file(&self, object_key: &str) -> EngineResult<Vec<u8>> {
        sqlx::query_scalar!(
            "SELECT data FROM wf_files WHERE object_key = $1",
            object_key
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(pe)?
        .ok_or_else(|| pe("File not found"))
    }

    async fn delete_file(&self, object_key: &str) -> EngineResult<()> {
        sqlx::query!("DELETE FROM wf_files WHERE object_key = $1", object_key)
            .execute(&self.pool)
            .await
            .map_err(pe)?;
        Ok(())
    }

    // ---- BPMN XML: source of truth is the app's `processes` table ----
    async fn save_bpmn_xml(&self, definition_key: &str, xml: &str) -> EngineResult<()> {
        let id = uid(definition_key)?;
        let res = sqlx::query!(
            "UPDATE processes SET bpmn_xml = $2, updated_at = now() WHERE id = $1",
            id,
            xml,
        )
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        if res.rows_affected() == 0 {
            return Err(EngineError::NoSuchDefinition(id));
        }
        Ok(())
    }

    async fn load_bpmn_xml(&self, definition_key: &str) -> EngineResult<String> {
        let id = uid(definition_key)?;
        sqlx::query_scalar!("SELECT bpmn_xml FROM processes WHERE id = $1", id)
            .fetch_optional(&self.pool)
            .await
            .map_err(pe)?
            .ok_or(EngineError::NoSuchDefinition(id))
    }

    async fn list_bpmn_xml_ids(&self) -> EngineResult<Vec<String>> {
        sqlx::query_scalar!(r#"SELECT id::text AS "id!" FROM processes"#)
            .fetch_all(&self.pool)
            .await
            .map_err(pe)
    }

    // ---- monitoring (runtime: dynamic table names + server introspection) ----
    async fn get_storage_info(&self) -> EngineResult<Option<StorageInfo>> {
        let version: String = sqlx::query_scalar("SHOW server_version")
            .fetch_one(&self.pool)
            .await
            .map_err(pe)?;
        let storage: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())")
            .fetch_one(&self.pool)
            .await
            .map_err(pe)?;

        let mut buckets = Vec::with_capacity(BUCKETS.len());
        for b in BUCKETS {
            let (count, size): (i64, i64) = sqlx::query_as(&format!(
                "SELECT count(*)::bigint, pg_total_relation_size('{t}')::bigint FROM {t}",
                t = b.table
            ))
            .fetch_one(&self.pool)
            .await
            .map_err(pe)?;
            buckets.push(BucketInfo {
                name: b.name.to_string(),
                bucket_type: b.kind.to_string(),
                entries: count as u64,
                size_bytes: size as u64,
            });
        }

        let opts = self.pool.connect_options();
        Ok(Some(StorageInfo {
            backend_name: "postgres".to_string(),
            version,
            host: opts.get_host().to_string(),
            port: opts.get_port(),
            memory_bytes: 0,
            storage_bytes: storage as u64,
            streams: 0,
            consumers: 0,
            buckets,
        }))
    }

    // ---- history ----
    async fn append_history_entry(&self, e: &HistoryEntry) -> EngineResult<()> {
        sqlx::query!(
            "INSERT INTO wf_history (id, instance_id, ts, event_type, node_id, actor_type, data) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (id) DO NOTHING",
            e.id,
            e.instance_id,
            e.timestamp,
            tag(&e.event_type),
            e.node_id.clone(),
            tag(&e.actor_type),
            js(e)?,
        )
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }

    async fn query_history(&self, q: HistoryQuery) -> EngineResult<Vec<HistoryEntry>> {
        // Optional filters: `$n IS NULL OR ...`. LIMIT/OFFSET NULL = unbounded/zero.
        let types: Option<Vec<String>> =
            q.event_types.as_ref().map(|t| t.iter().map(tag).collect());
        let actor: Option<String> = q.actor_type.as_ref().map(tag);
        let limit = q.limit.map(|l| l as i64);
        let offset = q.offset.map(|o| o as i64);

        sqlx::query!(
            r#"SELECT data FROM wf_history
               WHERE instance_id = $1
                 AND ($2::text[] IS NULL OR event_type = ANY($2))
                 AND ($3::text IS NULL OR node_id = $3)
                 AND ($4::text IS NULL OR actor_type = $4)
                 AND ($5::timestamptz IS NULL OR ts >= $5)
                 AND ($6::timestamptz IS NULL OR ts <= $6)
               ORDER BY ts ASC, id ASC
               LIMIT $7 OFFSET $8"#,
            q.instance_id,
            types as Option<Vec<String>>,
            q.node_id.as_deref(),
            actor.as_deref(),
            q.from,
            q.to,
            limit,
            offset,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(pe)?
        .into_iter()
        .map(|r| de(r.data))
        .collect()
    }

    // ---- archive ----
    async fn save_completed_instance(&self, i: &ProcessInstance) -> EngineResult<()> {
        sqlx::query!(
            "INSERT INTO wf_completed_instances \
               (id, definition_key, business_key, state_filter, completed_at, data) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (id) DO UPDATE SET \
               definition_key = EXCLUDED.definition_key, business_key = EXCLUDED.business_key, \
               state_filter = EXCLUDED.state_filter, completed_at = EXCLUDED.completed_at, \
               data = EXCLUDED.data, updated_at = now()",
            i.id,
            i.definition_key,
            &i.business_key,
            terminal_label(&i.state),
            i.completed_at,
            js(i)?,
        )
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }

    async fn query_completed_instances(
        &self,
        q: CompletedInstanceQuery,
    ) -> EngineResult<Vec<ProcessInstance>> {
        // Only known terminal labels filter; anything else = no filter (as before).
        let state = q
            .state_filter
            .as_deref()
            .filter(|s| matches!(*s, "completed" | "error"));
        let limit = q.limit.map(|l| l as i64);
        let offset = q.offset.map(|o| o as i64);

        sqlx::query!(
            r#"SELECT data FROM wf_completed_instances
               WHERE ($1::uuid IS NULL OR definition_key = $1)
                 AND ($2::text IS NULL OR strpos(business_key, $2) > 0)
                 AND ($3::timestamptz IS NULL OR completed_at IS NULL OR completed_at >= $3)
                 AND ($4::timestamptz IS NULL OR completed_at IS NULL OR completed_at <= $4)
                 AND ($5::text IS NULL OR state_filter = $5)
               ORDER BY completed_at DESC NULLS LAST, id
               LIMIT $6 OFFSET $7"#,
            q.definition_key,
            q.business_key.as_deref(),
            q.from,
            q.to,
            state,
            limit,
            offset,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(pe)?
        .into_iter()
        .map(|r| de(r.data))
        .collect()
    }

    async fn get_completed_instance(&self, id: &str) -> EngineResult<Option<ProcessInstance>> {
        let id = uid(id)?;
        sqlx::query!("SELECT data FROM wf_completed_instances WHERE id = $1", id)
            .fetch_optional(&self.pool)
            .await
            .map_err(pe)?
            .map(|r| de(r.data))
            .transpose()
    }

    // ---- bucket browser (runtime: dynamic table/column names) ----
    async fn get_bucket_entries(
        &self,
        bucket_name: &str,
        offset: usize,
        limit: usize,
    ) -> EngineResult<Vec<BucketEntry>> {
        let b = bucket(bucket_name)?;
        let sql = format!(
            "SELECT {key} AS k, ({size})::bigint AS sz, {ts} AS ts \
             FROM {table} ORDER BY k OFFSET $1 LIMIT $2",
            key = b.key,
            size = b.size,
            ts = b.ts,
            table = b.table
        );
        let rows: Vec<(String, i64, DateTime<Utc>)> = sqlx::query_as(&sql)
            .bind(offset as i64)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(pe)?;
        Ok(rows
            .into_iter()
            .map(|(key, sz, ts)| BucketEntry {
                key,
                size_bytes: Some(sz as u64),
                created_at: Some(ts),
            })
            .collect())
    }

    async fn get_bucket_entry_detail(
        &self,
        bucket_name: &str,
        key: &str,
    ) -> EngineResult<BucketEntryDetail> {
        let b = bucket(bucket_name)?;
        let sql = format!(
            "SELECT {data} FROM {table} WHERE {key} = $1",
            data = b.data,
            table = b.table,
            key = b.key
        );
        let row: Option<(String,)> = sqlx::query_as(&sql)
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(pe)?;
        let (data,) = row.ok_or_else(|| pe(format!("Key {key} not found in {bucket_name}")))?;
        Ok(BucketEntryDetail {
            key: key.to_string(),
            data,
            encoding: b.encoding.to_string(),
        })
    }
}
