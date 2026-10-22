use super::{InstanceIdParams, ProcessIdParams, ProcessStartError, ProcessUploadError};
use crate::routes::nav::NAV_ITEMS;
use crate::session::auth::CheckedInUser;
use crate::session::theme::Theme;
use askama::Template;
use engine_core::{EngineError, WorkflowEngine};
use pavex::request::path::PathParams;
use pavex::response::body::Html;
use pavex::{Response, get, post};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;

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
    process_name: String,
    state: String,
}

#[get(path = "/processes/instances")]
pub async fn list_instances(
    _user: &CheckedInUser,
    theme: Theme,
    engine: &Arc<WorkflowEngine>,
    db_pool: &PgPool,
) -> Result<Response, ProcessStartError> {
    let names: HashMap<uuid::Uuid, String> =
        sqlx::query_as::<_, (uuid::Uuid, String)>("SELECT id, name FROM processes")
            .fetch_all(db_pool)
            .await
            .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?
            .into_iter()
            .collect();

    let mut all = engine.list_instances().await;
    all.sort_by_key(|i| std::cmp::Reverse(i.started_at));

    let instances = all
        .into_iter()
        .map(|i| InstanceRow {
            id: i.id.to_string(),
            process_name: names
                .get(&i.definition_key)
                .cloned()
                .unwrap_or_else(|| i.definition_key.to_string()),
            state: crate::pg_persistence::state_label(&i.state).to_string(),
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
            created_at: r.created_at.to_rfc3339(),
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

#[post(path = "/processes/{id}/delete")]
pub async fn delete_process(
    _user: &CheckedInUser,
    params: &PathParams<ProcessIdParams>,
    engine: &Arc<WorkflowEngine>,
    db_pool: &PgPool,
) -> Result<Response, ProcessStartError> {
    let process_id = params.0.id;

    // Cascade: engine drops instances + persisted rows. Not-deployed is fine.
    match engine.delete_definition(process_id, true).await {
        Ok(()) | Err(EngineError::NoSuchDefinition(_)) => {}
        Err(e) => return Err(e.into()),
    }

    let result = sqlx::query("DELETE FROM processes WHERE id = $1")
        .bind(process_id)
        .execute(db_pool)
        .await
        .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;

    if result.rows_affected() == 0 {
        return Err(ProcessStartError::NotFound);
    }

    Ok(Response::ok().insert_header(
        pavex::http::header::HeaderName::from_static("hx-redirect"),
        pavex::http::HeaderValue::from_static("/processes"),
    ))
}

#[post(path = "/processes/instances/{id}/delete")]
pub async fn delete_instance(
    _user: &CheckedInUser,
    params: &PathParams<InstanceIdParams>,
    engine: &Arc<WorkflowEngine>,
) -> Result<Response, ProcessStartError> {
    engine.delete_instance(params.0.id).await?;

    Ok(Response::ok().insert_header(
        pavex::http::header::HeaderName::from_static("hx-redirect"),
        pavex::http::HeaderValue::from_static("/processes/instances"),
    ))
}
