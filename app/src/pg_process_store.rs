//! Postgres implementations of `bpm-engine-storage`'s `ProcessInstanceStore`
//! and `TokenStore` traits, backed by the `process_instances` and `tokens`
//! tables (see migration `20260923000000_process_instances_and_tokens.sql`).

use async_trait::async_trait;
use bpm_engine_core::{InstanceState, ProcessInstance, Token, TokenMode, TokenStatus};
use bpm_engine_storage::{ProcessInstanceStore, TokenStore};
use sqlx::PgPool;
use std::collections::HashMap;

pub struct PgProcessInstanceStore {
    pool: PgPool,
}

impl PgProcessInstanceStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

pub struct PgTokenStore {
    pool: PgPool,
}

impl PgTokenStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

// -- InstanceState / TokenStatus / TokenMode <-> text -------------------
// These enums don't implement Display/FromStr upstream, so we map them
// ourselves against the VARCHAR columns. Keep these in sync if the engine
// adds variants.

fn instance_state_to_str(s: InstanceState) -> &'static str {
    match s {
        InstanceState::Running => "Running",
        InstanceState::Completed => "Completed",
        InstanceState::Terminated => "Terminated",
    }
}

fn instance_state_from_str(s: &str) -> anyhow::Result<InstanceState> {
    match s {
        "Running" => Ok(InstanceState::Running),
        "Completed" => Ok(InstanceState::Completed),
        "Terminated" => Ok(InstanceState::Terminated),
        other => anyhow::bail!("unknown InstanceState in DB: {other}"),
    }
}

fn token_status_to_str(s: TokenStatus) -> &'static str {
    match s {
        TokenStatus::Created => "Created",
        TokenStatus::Ready => "Ready",
        TokenStatus::Executing => "Executing",
        TokenStatus::Waiting => "Waiting",
        TokenStatus::Suspended => "Suspended",
        TokenStatus::Completed => "Completed",
        TokenStatus::Terminated => "Terminated",
    }
}

fn token_status_from_str(s: &str) -> anyhow::Result<TokenStatus> {
    match s {
        "Created" => Ok(TokenStatus::Created),
        "Ready" => Ok(TokenStatus::Ready),
        "Executing" => Ok(TokenStatus::Executing),
        "Waiting" => Ok(TokenStatus::Waiting),
        "Suspended" => Ok(TokenStatus::Suspended),
        "Completed" => Ok(TokenStatus::Completed),
        "Terminated" => Ok(TokenStatus::Terminated),
        other => anyhow::bail!("unknown TokenStatus in DB: {other}"),
    }
}

fn token_mode_to_str(m: TokenMode) -> &'static str {
    match m {
        TokenMode::Forward => "Forward",
        TokenMode::Compensation => "Compensation",
    }
}

fn token_mode_from_str(s: &str) -> anyhow::Result<TokenMode> {
    match s {
        "Forward" => Ok(TokenMode::Forward),
        "Compensation" => Ok(TokenMode::Compensation),
        other => anyhow::bail!("unknown TokenMode in DB: {other}"),
    }
}

// -- ProcessInstanceStore -------------------------------------------------

#[async_trait]
impl ProcessInstanceStore for PgProcessInstanceStore {
    async fn load(&self, id: &str) -> anyhow::Result<Option<ProcessInstance>> {
        let row = sqlx::query!(
            r#"
            SELECT id, process_def_id, tenant_id, variables, state, version
            FROM process_instances
            WHERE id = $1
            "#,
            id,
        )
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let variables: HashMap<String, String> = serde_json::from_value(row.variables)?;
        let state = instance_state_from_str(&row.state)?;

        let tokens = load_tokens_for_instance(&self.pool, id).await?;

        Ok(Some(ProcessInstance {
            id: row.id,
            process_def_id: row.process_def_id,
            tenant_id: row.tenant_id,
            tokens,
            variables,
            state,
            // DB column is INT (i32); engine's version field is u32.
            version: u32::try_from(row.version)?,
        }))
    }

    async fn save(&self, instance: &ProcessInstance) -> anyhow::Result<()> {
        let variables_json = serde_json::to_value(&instance.variables)?;
        let state_str = instance_state_to_str(instance.state);
        let version = i32::try_from(instance.version)?;

        let mut tx = self.pool.begin().await?;

        sqlx::query!(
            r#"
            INSERT INTO process_instances (id, process_def_id, tenant_id, variables, state, version, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, now())
            ON CONFLICT (id) DO UPDATE SET
                process_def_id = EXCLUDED.process_def_id,
                tenant_id = EXCLUDED.tenant_id,
                variables = EXCLUDED.variables,
                state = EXCLUDED.state,
                version = EXCLUDED.version,
                updated_at = now()
            "#,
            instance.id,
            instance.process_def_id,
            instance.tenant_id,
            variables_json,
            state_str,
            version,
        )
        .execute(&mut *tx)
        .await?;

        save_tokens_tx(&mut tx, &instance.id, &instance.tokens).await?;

        tx.commit().await?;
        Ok(())
    }

    async fn list_running(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<String>> {
        let ids = match tenant_id {
            Some(t) => sqlx::query_scalar!(
                r#"SELECT id FROM process_instances WHERE state = 'Running' AND tenant_id = $1"#,
                t,
            )
            .fetch_all(&self.pool)
            .await?,
            None => {
                sqlx::query_scalar!(r#"SELECT id FROM process_instances WHERE state = 'Running'"#,)
                    .fetch_all(&self.pool)
                    .await?
            }
        };
        Ok(ids)
    }
}

async fn load_tokens_for_instance(pool: &PgPool, instance_id: &str) -> anyhow::Result<Vec<Token>> {
    let rows = sqlx::query!(
        r#"
        SELECT id, node_id, status, mode, version, attempt, parallel_group_id, updated_at
        FROM tokens
        WHERE instance_id = $1
        "#,
        instance_id,
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(Token {
                id: row.id,
                node_id: row.node_id,
                status: token_status_from_str(&row.status)?,
                mode: token_mode_from_str(&row.mode)?,
                version: u32::try_from(row.version)?,
                attempt: u32::try_from(row.attempt)?,
                parallel_group_id: row.parallel_group_id,
                // Stable, parseable format (RFC3339), not Rust's ad-hoc
                // Debug-derived `.to_string()`. Purely an audit timestamp --
                // the engine never writes this back, so this is read-only.
                updated_at: row
                    .updated_at
                    .map(|t: time::OffsetDateTime| {
                        t.format(&time::format_description::well_known::Rfc3339)
                    })
                    .transpose()?,
            })
        })
        .collect()
}

async fn save_tokens_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    instance_id: &str,
    tokens: &[Token],
) -> anyhow::Result<()> {
    // Simplest correct approach: replace the full token set for this
    // instance. Fine at current scale; revisit with per-token upserts if
    // token counts per instance get large.
    sqlx::query!(r#"DELETE FROM tokens WHERE instance_id = $1"#, instance_id)
        .execute(&mut **tx)
        .await?;

    for token in tokens {
        let status_str = token_status_to_str(token.status);
        let mode_str = token_mode_to_str(token.mode);
        let version = i32::try_from(token.version)?;
        let attempt = i32::try_from(token.attempt)?;

        sqlx::query!(
            r#"
            INSERT INTO tokens (id, instance_id, node_id, status, mode, version, attempt, parallel_group_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
            token.id,
            instance_id,
            token.node_id,
            status_str,
            mode_str,
            version,
            attempt,
            token.parallel_group_id,
        )
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

// -- TokenStore -------------------------------------------------------------

#[async_trait]
impl TokenStore for PgTokenStore {
    async fn load_by_instance(&self, instance_id: &str) -> anyhow::Result<Vec<Token>> {
        load_tokens_for_instance(&self.pool, instance_id).await
    }

    async fn save_tokens(&self, instance_id: &str, tokens: &[Token]) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        save_tokens_tx(&mut tx, instance_id, tokens).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn update_token_cas(&self, instance_id: &str, token: &Token) -> anyhow::Result<bool> {
        // Compare-and-swap: only update if the row's current version matches
        // the version the caller last read. New version is written as +1.
        let status_str = token_status_to_str(token.status);
        let mode_str = token_mode_to_str(token.mode);
        let current_version = i32::try_from(token.version)?;
        let attempt = i32::try_from(token.attempt)?;

        let result = sqlx::query!(
            r#"
            UPDATE tokens
            SET status = $1, mode = $2, version = version + 1, attempt = $3,
                parallel_group_id = $4, updated_at = now()
            WHERE instance_id = $5 AND id = $6 AND version = $7
            "#,
            status_str,
            mode_str,
            attempt,
            token.parallel_group_id,
            instance_id,
            token.id,
            current_version,
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    async fn claim_token(
        &self,
        instance_id: &str,
        token_id: &str,
        version: u32,
    ) -> anyhow::Result<bool> {
        let version = i32::try_from(version)?;
        let executing = token_status_to_str(TokenStatus::Executing);

        // Claim = move Ready -> Executing iff version matches and the token
        // isn't already Completed/Terminated (exactly-once completion).
        let result = sqlx::query!(
            r#"
            UPDATE tokens
            SET status = $1, version = version + 1, updated_at = now()
            WHERE instance_id = $2 AND id = $3 AND version = $4
              AND status NOT IN ('Completed', 'Terminated')
            "#,
            executing,
            instance_id,
            token_id,
            version,
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }
}
