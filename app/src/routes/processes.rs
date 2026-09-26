use crate::routes::nav::NAV_ITEMS;
use crate::session::auth::CheckedInUser;
use crate::session::theme::Theme;
use askama::Template;
use bpm_engine_core::event::{EngineEvent, payloads};
use bpm_engine_runtime::{BpmEngine, EngineContext};
use bpm_engine_storage::process_store::ProcessInstanceStore;
use pavex::request::body::UrlEncodedBody;
use pavex::request::path::PathParams;
use pavex::response::body::Html;
use pavex::{Response, get, post};
use sqlx::PgPool;
use std::sync::Arc;

#[derive(Template)]
#[template(path = "process_upload.html")]
struct ProcessUploadPage {
    active_page: &'static str,
    nav_items: &'static [crate::routes::nav::NavItem],
    theme: Theme,
}

#[get(path = "/processes/upload")]
pub fn process_upload_form(
    _user: &CheckedInUser,
    theme: Theme,
) -> Result<Response, ProcessUploadError> {
    let body = ProcessUploadPage {
        active_page: "processes",
        nav_items: NAV_ITEMS,
        theme,
    }
    .render()
    .map_err(|e| ProcessUploadError::UnexpectedError(e.into()))?;
    let html: Html = body.into();
    Ok(Response::ok().set_typed_body(html))
}

#[derive(serde::Deserialize)]
pub struct ProcessUploadForm {
    pub name: String,
    pub bpmn_xml: String,
}

#[post(path = "/processes/upload")]
pub async fn process_upload(
    user: &CheckedInUser,
    body: &UrlEncodedBody<ProcessUploadForm>,
    db_pool: &PgPool,
) -> Result<Response, ProcessUploadError> {
    let ProcessUploadForm { name, bpmn_xml } = &body.0;

    bpm_engine_bpmn::parse_and_compile(bpmn_xml)
        .map_err(|e| ProcessUploadError::InvalidBpmn(e.to_string()))?;

    let process_id = insert_process(user.0, name, bpmn_xml, db_pool).await?;

    Ok(Response::ok()
        .insert_header(
            pavex::http::header::HeaderName::from_static("hx-redirect"),
            pavex::http::HeaderValue::from_static("/processes/upload"),
        )
        .set_typed_body(process_id.to_string()))
}

async fn insert_process(
    created_by: uuid::Uuid,
    name: &str,
    bpmn_xml: &str,
    pool: &PgPool,
) -> Result<uuid::Uuid, ProcessUploadError> {
    let process_id = uuid::Uuid::new_v4();
    sqlx::query!(
        r#"INSERT INTO processes (id, name, bpmn_xml, created_by) VALUES ($1, $2, $3, $4)"#,
        process_id,
        name,
        bpmn_xml,
        created_by,
    )
    .execute(pool)
    .await
    .map_err(|e| {
        ProcessUploadError::UnexpectedError(
            anyhow::Error::new(e).context("Failed to insert process record."),
        )
    })?;

    Ok(process_id)
}

// ---------- Start ----------

#[PathParams]
pub struct ProcessIdParams {
    id: uuid::Uuid,
}
// --- start_process redirect fix ---
// Was: hardcoded HeaderValue::from_static("/processes/upload")
// Now: dynamic redirect straight to the new instance's detail page.
#[post(path = "/processes/{id}/start")]
pub async fn start_process(
    _user: &CheckedInUser,
    params: &PathParams<ProcessIdParams>,
    process_store: &Arc<crate::pg_process_store::PgProcessInstanceStore>,
    token_store: &Arc<crate::pg_process_store::PgTokenStore>,
    def_store: &Arc<crate::engine_def_store::PgProcessDefinitionStore>,
    engine: &Arc<BpmEngine>,
) -> Result<Response, ProcessStartError> {
    let process_id = params.0.id;
    let instance_id = uuid::Uuid::new_v4().to_string();

    let mut ctx = EngineContext {
        process_store: Some(process_store.clone()),
        token_store: Some(token_store.clone()),
        process_def_store: Some(def_store.clone()),
        ..Default::default()
    };

    engine
        .run_async(
            EngineEvent::ProcessStarted(payloads::ProcessStarted {
                process_id: process_id.to_string(),
                instance_id: instance_id.clone(),
                initial_variables: None,
            }),
            &mut ctx,
        )
        .await;

    let saved = process_store
        .load(&instance_id)
        .await
        .map_err(ProcessStartError::UnexpectedError)?;

    match saved {
        Some(_) => {
            let redirect_path = format!("/processes/instances/{instance_id}");
            let redirect_header = pavex::http::HeaderValue::from_str(&redirect_path)
                .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;
            Ok(Response::ok().insert_header(
                pavex::http::header::HeaderName::from_static("hx-redirect"),
                redirect_header,
            ))
        }
        None => Err(ProcessStartError::NotFound),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessUploadError {
    #[error("The uploaded file is not valid BPMN: {0}")]
    InvalidBpmn(String),
    #[error("Something went wrong. Please retry later.")]
    UnexpectedError(#[source] anyhow::Error),
}

#[pavex::methods]
impl ProcessUploadError {
    #[error_handler]
    pub fn into_response(&self) -> Response {
        match self {
            ProcessUploadError::InvalidBpmn(_) => Response::bad_request(),
            ProcessUploadError::UnexpectedError(_) => Response::internal_server_error(),
        }
        .set_typed_body(format!("{self}"))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessStartError {
    #[error("Process definition not found, or failed to start.")]
    NotFound,
    #[error("Something went wrong. Please retry later.")]
    UnexpectedError(#[source] anyhow::Error),
}

#[pavex::methods]
impl ProcessStartError {
    #[error_handler]
    pub fn into_response(&self) -> Response {
        match self {
            ProcessStartError::NotFound => Response::not_found(),
            ProcessStartError::UnexpectedError(_) => Response::internal_server_error(),
        }
        .set_typed_body(format!("{self}"))
    }
}

#[derive(Template)]
#[template(path = "process_instances.html")]
struct ProcessInstancesPage {
    active_page: &'static str,
    nav_items: &'static [crate::routes::nav::NavItem],
    theme: Theme,
    instances: Vec<InstanceRow>,
}

struct InstanceRow {
    id: String,
    process_def_id: String,
    state: String,
}

#[get(path = "/processes/instances")]
pub async fn list_instances(
    _user: &CheckedInUser,
    theme: Theme,
    db_pool: &PgPool,
) -> Result<Response, ProcessStartError> {
    // Direct query, not through ProcessInstanceStore -- store trait has no
    // "list all" method (only list_running(tenant_id) per commented-out
    // pg_process_store.rs). Reading straight from the table is fine for a
    // listing page; no engine involved.
    let rows = sqlx::query!(
        r#"SELECT id, process_def_id, state FROM process_instances ORDER BY updated_at DESC"#
    )
    .fetch_all(db_pool)
    .await
    .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;

    let instances = rows
        .into_iter()
        .map(|r| InstanceRow {
            id: r.id,
            process_def_id: r.process_def_id,
            state: r.state,
        })
        .collect();

    let body = ProcessInstancesPage {
        active_page: "processes",
        nav_items: NAV_ITEMS,
        theme,
        instances,
    }
    .render()
    .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;
    let html: Html = body.into();
    Ok(Response::ok().set_typed_body(html))
}

// ---------- Instance detail ----------

#[PathParams]
pub struct InstanceIdParams {
    id: String, // process_instances.id is VARCHAR, engine-assigned, not UUID
}

#[derive(Template)]
#[template(path = "process_instance_detail.html")]
struct ProcessInstanceDetailPage {
    active_page: &'static str,
    nav_items: &'static [crate::routes::nav::NavItem],
    theme: Theme,
    instance_id: String,
    state: String,
    tokens: Vec<TokenRow>,
}

struct TokenRow {
    id: String,
    node_id: String,
    status: String,
}

#[get(path = "/processes/instances/{id}")]
pub async fn instance_detail(
    _user: &CheckedInUser,
    params: &PathParams<InstanceIdParams>,
    theme: Theme,
    process_store: &Arc<crate::pg_process_store::PgProcessInstanceStore>,
) -> Result<Response, ProcessStartError> {
    let instance = process_store
        .load(&params.0.id)
        .await
        .map_err(ProcessStartError::UnexpectedError)?
        .ok_or(ProcessStartError::NotFound)?;

    let tokens = instance
        .tokens
        .iter()
        .map(|t| TokenRow {
            id: t.id.clone(),
            node_id: t.node_id.clone(),
            status: format!("{:?}", t.status),
        })
        .collect();

    let body = ProcessInstanceDetailPage {
        active_page: "processes",
        nav_items: NAV_ITEMS,
        theme,
        instance_id: instance.id,
        state: format!("{:?}", instance.state),
        tokens,
    }
    .render()
    .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;
    let html: Html = body.into();
    Ok(Response::ok().set_typed_body(html))
}

// ---------- Advance (complete a UserTask token) ----------

#[derive(serde::Deserialize)]
pub struct AdvanceForm {
    pub token_id: String,
    #[serde(default)]
    pub variables: std::collections::HashMap<String, String>,
}

#[post(path = "/processes/instances/{id}/advance")]
pub async fn advance_instance(
    _user: &CheckedInUser,
    params: &PathParams<InstanceIdParams>,
    body: &UrlEncodedBody<AdvanceForm>,
    process_store: &Arc<crate::pg_process_store::PgProcessInstanceStore>,
    token_store: &Arc<crate::pg_process_store::PgTokenStore>,
    def_store: &Arc<crate::engine_def_store::PgProcessDefinitionStore>,
    engine: &Arc<BpmEngine>,
) -> Result<Response, ProcessStartError> {
    let instance_id = params.0.id.clone();
    let token_id = body.0.token_id.clone();

    let mut ctx = EngineContext {
        process_store: Some(process_store.clone()),
        token_store: Some(token_store.clone()),
        process_def_store: Some(def_store.clone()),
        ..Default::default()
    };

    let instance = process_store
        .load(&instance_id)
        .await
        .map_err(ProcessStartError::UnexpectedError)?
        .ok_or(ProcessStartError::NotFound)?;
    let node_id = instance
        .tokens
        .iter()
        .find(|t| t.id == token_id)
        .map(|t| t.node_id.clone())
        .ok_or(ProcessStartError::NotFound)?;

    engine
        .run_async(
            EngineEvent::UserTaskCompleted(payloads::UserTaskCompleted {
                task_id: token_id.clone(),
                instance_id: instance_id.clone(),
                node_id,
                variables: body.0.variables.clone(),
            }),
            &mut ctx,
        )
        .await;

    // confirm it actually landed, same reasoning as start_process
    let _updated = process_store
        .load(&instance_id)
        .await
        .map_err(ProcessStartError::UnexpectedError)?
        .ok_or(ProcessStartError::NotFound)?;

    let redirect_path = format!("/processes/instances/{instance_id}");
    let redirect_header = pavex::http::HeaderValue::from_str(&redirect_path)
        .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;

    Ok(Response::ok().insert_header(
        pavex::http::header::HeaderName::from_static("hx-redirect"),
        redirect_header,
    ))
}

#[derive(Template)]
#[template(path = "process_list.html")]
struct ProcessListPage {
    active_page: &'static str,
    nav_items: &'static [crate::routes::nav::NavItem],
    theme: Theme,
    processes: Vec<ProcessRow>,
}

struct ProcessRow {
    id: uuid::Uuid,
    name: String,
    created_at: String,
}

#[get(path = "/processes")]
pub async fn list_processes(
    _user: &CheckedInUser,
    theme: Theme,
    db_pool: &PgPool,
) -> Result<Response, ProcessUploadError> {
    let rows =
        sqlx::query!(r#"SELECT id, name, created_at FROM processes ORDER BY created_at DESC"#)
            .fetch_all(db_pool)
            .await
            .map_err(|e| ProcessUploadError::UnexpectedError(e.into()))?;

    let processes = rows
        .into_iter()
        .map(|r| ProcessRow {
            id: r.id,
            name: r.name,
            // created_at is TIMESTAMPTZ -> time::OffsetDateTime per sqlx's
            // `time` feature (confirmed pattern from pg_process_store.rs).
            // Fall back to a plain debug string if Rfc3339 formatting fails,
            // rather than erroring out the whole list on one bad row.
            created_at: r
                .created_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_else(|_| format!("{:?}", r.created_at)),
        })
        .collect();

    let body = ProcessListPage {
        active_page: "processes",
        nav_items: NAV_ITEMS,
        theme,
        processes,
    }
    .render()
    .map_err(|e| ProcessUploadError::UnexpectedError(e.into()))?;
    let html: Html = body.into();
    Ok(Response::ok().set_typed_body(html))
}
