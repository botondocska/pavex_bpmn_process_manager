//! Postgres `WorkflowPersistence` for embedded `engine-core`.
//! Runtime sqlx queries only: no offline metadata needed for `wf_*` tables.

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
use sqlx::{PgPool, Postgres, QueryBuilder};
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

    // `table` / `key_col` are internal constants, never user input.
    async fn upsert<T: Serialize>(
        &self,
        table: &str,
        key_col: &str,
        key: Uuid,
        v: &T,
    ) -> EngineResult<()> {
        let sql = format!(
            "INSERT INTO {table} ({key_col}, data) VALUES ($1, $2) \
             ON CONFLICT ({key_col}) DO UPDATE SET data = EXCLUDED.data, updated_at = now()"
        );
        sqlx::query(&sql)
            .bind(key)
            .bind(js(v)?)
            .execute(&self.pool)
            .await
            .map_err(pe)?;
        Ok(())
    }

    async fn remove(&self, table: &str, key_col: &str, key: Uuid) -> EngineResult<()> {
        sqlx::query(&format!("DELETE FROM {table} WHERE {key_col} = $1"))
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(pe)?;
        Ok(())
    }

    async fn all<T: DeserializeOwned>(&self, table: &str) -> EngineResult<Vec<T>> {
        let rows: Vec<(Value,)> =
            sqlx::query_as(&format!("SELECT data FROM {table} ORDER BY updated_at"))
                .fetch_all(&self.pool)
                .await
                .map_err(pe)?;
        rows.into_iter().map(|(v,)| de(v)).collect()
    }
}

#[async_trait]
impl WorkflowPersistence for PgWorkflowPersistence {
    // ---- tokens ----
    async fn save_token(&self, instance_id: Uuid, token: &Token) -> EngineResult<()> {
        sqlx::query(
            "INSERT INTO wf_tokens (instance_id, token_id, data) VALUES ($1, $2, $3) \
             ON CONFLICT (instance_id, token_id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()",
        )
        .bind(instance_id)
        .bind(token.id)
        .bind(js(token)?)
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }

    async fn load_tokens(&self, instance_id: Uuid) -> EngineResult<Vec<Token>> {
        let rows: Vec<(Value,)> =
            sqlx::query_as("SELECT data FROM wf_tokens WHERE instance_id = $1")
                .bind(instance_id)
                .fetch_all(&self.pool)
                .await
                .map_err(pe)?;
        rows.into_iter().map(|(v,)| de(v)).collect()
    }

    async fn delete_token(&self, instance_id: Uuid, token_id: Uuid) -> EngineResult<()> {
        sqlx::query("DELETE FROM wf_tokens WHERE instance_id = $1 AND token_id = $2")
            .bind(instance_id)
            .bind(token_id)
            .execute(&self.pool)
            .await
            .map_err(pe)?;
        Ok(())
    }

    // ---- instances ----
    async fn save_instance(&self, i: &ProcessInstance) -> EngineResult<()> {
        sqlx::query(
            "INSERT INTO wf_instances \
               (id, definition_key, business_key, state_label, data, started_at, completed_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (id) DO UPDATE SET \
               definition_key = EXCLUDED.definition_key, business_key = EXCLUDED.business_key, \
               state_label = EXCLUDED.state_label, data = EXCLUDED.data, \
               started_at = EXCLUDED.started_at, completed_at = EXCLUDED.completed_at, \
               updated_at = now()",
        )
        .bind(i.id)
        .bind(i.definition_key)
        .bind(&i.business_key)
        .bind(state_label(&i.state))
        .bind(js(i)?)
        .bind(i.started_at)
        .bind(i.completed_at)
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }

    async fn list_instances(&self) -> EngineResult<Vec<ProcessInstance>> {
        self.all("wf_instances").await
    }

    async fn delete_instance(&self, id: &str) -> EngineResult<()> {
        let Ok(id) = Uuid::parse_str(id) else {
            return Ok(());
        };
        let mut tx = self.pool.begin().await.map_err(pe)?;
        for sql in [
            "DELETE FROM wf_tokens WHERE instance_id = $1",
            "DELETE FROM wf_history WHERE instance_id = $1",
            "DELETE FROM wf_completed_instances WHERE id = $1",
            "DELETE FROM wf_instances WHERE id = $1",
        ] {
            sqlx::query(sql)
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(pe)?;
        }
        tx.commit().await.map_err(pe)?;
        Ok(())
    }

    // ---- definitions ----
    async fn save_definition(&self, d: &ProcessDefinition) -> EngineResult<()> {
        self.upsert("wf_definitions", "id", d.key, d).await
    }
    async fn list_definitions(&self) -> EngineResult<Vec<ProcessDefinition>> {
        self.all("wf_definitions").await
    }
    async fn delete_definition(&self, key: &str) -> EngineResult<()> {
        self.remove("wf_definitions", "id", uid(key)?).await
    }

    // ---- user tasks ----
    async fn save_user_task(&self, t: &PendingUserTask) -> EngineResult<()> {
        self.upsert("wf_user_tasks", "task_id", t.task_id, t).await
    }
    async fn delete_user_task(&self, task_id: Uuid) -> EngineResult<()> {
        self.remove("wf_user_tasks", "task_id", task_id).await
    }
    async fn list_user_tasks(&self) -> EngineResult<Vec<PendingUserTask>> {
        self.all("wf_user_tasks").await
    }

    // ---- service tasks ----
    async fn save_service_task(&self, t: &PendingServiceTask) -> EngineResult<()> {
        self.upsert("wf_service_tasks", "id", t.id, t).await
    }
    async fn delete_service_task(&self, task_id: Uuid) -> EngineResult<()> {
        self.remove("wf_service_tasks", "id", task_id).await
    }
    async fn list_service_tasks(&self) -> EngineResult<Vec<PendingServiceTask>> {
        self.all("wf_service_tasks").await
    }

    // ---- timers ----
    async fn save_timer(&self, t: &PendingTimer) -> EngineResult<()> {
        self.upsert("wf_timers", "id", t.id, t).await
    }
    async fn delete_timer(&self, timer_id: Uuid) -> EngineResult<()> {
        self.remove("wf_timers", "id", timer_id).await
    }
    async fn list_timers(&self) -> EngineResult<Vec<PendingTimer>> {
        self.all("wf_timers").await
    }

    // ---- message catches ----
    async fn save_message_catch(&self, c: &PendingMessageCatch) -> EngineResult<()> {
        self.upsert("wf_message_catches", "id", c.id, c).await
    }
    async fn delete_message_catch(&self, catch_id: Uuid) -> EngineResult<()> {
        self.remove("wf_message_catches", "id", catch_id).await
    }
    async fn list_message_catches(&self) -> EngineResult<Vec<PendingMessageCatch>> {
        self.all("wf_message_catches").await
    }

    // ---- files ----
    async fn save_file(&self, object_key: &str, data: &[u8]) -> EngineResult<()> {
        sqlx::query(
            "INSERT INTO wf_files (object_key, data) VALUES ($1, $2) \
             ON CONFLICT (object_key) DO UPDATE SET data = EXCLUDED.data, updated_at = now()",
        )
        .bind(object_key)
        .bind(data)
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }

    async fn load_file(&self, object_key: &str) -> EngineResult<Vec<u8>> {
        sqlx::query_scalar::<_, Vec<u8>>("SELECT data FROM wf_files WHERE object_key = $1")
            .bind(object_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(pe)?
            .ok_or_else(|| pe("File not found"))
    }

    async fn delete_file(&self, object_key: &str) -> EngineResult<()> {
        sqlx::query("DELETE FROM wf_files WHERE object_key = $1")
            .bind(object_key)
            .execute(&self.pool)
            .await
            .map_err(pe)?;
        Ok(())
    }

    // ---- BPMN XML: source of truth is the app's `processes` table ----
    async fn save_bpmn_xml(&self, definition_key: &str, xml: &str) -> EngineResult<()> {
        let id = uid(definition_key)?;
        let res =
            sqlx::query("UPDATE processes SET bpmn_xml = $2, updated_at = now() WHERE id = $1")
                .bind(id)
                .bind(xml)
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
        sqlx::query_scalar::<_, String>("SELECT bpmn_xml FROM processes WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(pe)?
            .ok_or(EngineError::NoSuchDefinition(id))
    }

    async fn list_bpmn_xml_ids(&self) -> EngineResult<Vec<String>> {
        sqlx::query_scalar::<_, String>("SELECT id::text FROM processes")
            .fetch_all(&self.pool)
            .await
            .map_err(pe)
    }

    // ---- monitoring ----
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
        sqlx::query(
            "INSERT INTO wf_history (id, instance_id, ts, event_type, node_id, actor_type, data) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (id) DO NOTHING",
        )
        .bind(e.id)
        .bind(e.instance_id)
        .bind(e.timestamp)
        .bind(tag(&e.event_type))
        .bind(e.node_id.clone())
        .bind(tag(&e.actor_type))
        .bind(js(e)?)
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }

    async fn query_history(&self, q: HistoryQuery) -> EngineResult<Vec<HistoryEntry>> {
        let mut qb: QueryBuilder<Postgres> =
            QueryBuilder::new("SELECT data FROM wf_history WHERE instance_id = ");
        qb.push_bind(q.instance_id);
        if let Some(types) = &q.event_types {
            let t: Vec<String> = types.iter().map(tag).collect();
            qb.push(" AND event_type = ANY(").push_bind(t).push(")");
        }
        if let Some(n) = &q.node_id {
            qb.push(" AND node_id = ").push_bind(n.clone());
        }
        if let Some(a) = &q.actor_type {
            qb.push(" AND actor_type = ").push_bind(tag(a));
        }
        if let Some(f) = q.from {
            qb.push(" AND ts >= ").push_bind(f);
        }
        if let Some(t) = q.to {
            qb.push(" AND ts <= ").push_bind(t);
        }
        qb.push(" ORDER BY ts ASC, id ASC");
        if let Some(l) = q.limit {
            qb.push(" LIMIT ").push_bind(l as i64);
        }
        if let Some(o) = q.offset {
            qb.push(" OFFSET ").push_bind(o as i64);
        }
        let rows: Vec<(Value,)> = qb
            .build_query_as()
            .fetch_all(&self.pool)
            .await
            .map_err(pe)?;
        rows.into_iter().map(|(v,)| de(v)).collect()
    }

    // ---- archive ----
    async fn save_completed_instance(&self, i: &ProcessInstance) -> EngineResult<()> {
        sqlx::query(
            "INSERT INTO wf_completed_instances \
               (id, definition_key, business_key, state_filter, completed_at, data) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (id) DO UPDATE SET \
               definition_key = EXCLUDED.definition_key, business_key = EXCLUDED.business_key, \
               state_filter = EXCLUDED.state_filter, completed_at = EXCLUDED.completed_at, \
               data = EXCLUDED.data, updated_at = now()",
        )
        .bind(i.id)
        .bind(i.definition_key)
        .bind(&i.business_key)
        .bind(terminal_label(&i.state))
        .bind(i.completed_at)
        .bind(js(i)?)
        .execute(&self.pool)
        .await
        .map_err(pe)?;
        Ok(())
    }

    async fn query_completed_instances(
        &self,
        q: CompletedInstanceQuery,
    ) -> EngineResult<Vec<ProcessInstance>> {
        let mut qb: QueryBuilder<Postgres> =
            QueryBuilder::new("SELECT data FROM wf_completed_instances WHERE TRUE");
        if let Some(dk) = q.definition_key {
            qb.push(" AND definition_key = ").push_bind(dk);
        }
        if let Some(bk) = &q.business_key {
            qb.push(" AND strpos(business_key, ")
                .push_bind(bk.clone())
                .push(") > 0");
        }
        if let Some(f) = q.from {
            qb.push(" AND (completed_at IS NULL OR completed_at >= ")
                .push_bind(f)
                .push(")");
        }
        if let Some(t) = q.to {
            qb.push(" AND (completed_at IS NULL OR completed_at <= ")
                .push_bind(t)
                .push(")");
        }
        if let Some(sf) = q.state_filter.as_deref()
            && matches!(sf, "completed" | "error")
        {
            qb.push(" AND state_filter = ").push_bind(sf.to_string());
        }
        qb.push(" ORDER BY completed_at DESC NULLS LAST, id");
        if let Some(l) = q.limit {
            qb.push(" LIMIT ").push_bind(l as i64);
        }
        if let Some(o) = q.offset {
            qb.push(" OFFSET ").push_bind(o as i64);
        }
        let rows: Vec<(Value,)> = qb
            .build_query_as()
            .fetch_all(&self.pool)
            .await
            .map_err(pe)?;
        rows.into_iter().map(|(v,)| de(v)).collect()
    }

    async fn get_completed_instance(&self, id: &str) -> EngineResult<Option<ProcessInstance>> {
        let id = uid(id)?;
        let row: Option<(Value,)> =
            sqlx::query_as("SELECT data FROM wf_completed_instances WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(pe)?;
        row.map(|(v,)| de(v)).transpose()
    }

    // ---- bucket browser ----
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
