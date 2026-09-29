use super::{InstanceIdParams, ProcessIdParams, ProcessStartError, ProcessUploadError};
use crate::routes::nav::NAV_ITEMS;
use crate::session::auth::CheckedInUser;
use crate::session::theme::Theme;
use askama::Template;
use pavex::request::path::PathParams;
use pavex::response::body::Html;
use pavex::{Response, get, post};
use sqlx::PgPool;

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

#[post(path = "/processes/{id}/delete")]
pub async fn delete_process(
    _user: &CheckedInUser,
    params: &PathParams<ProcessIdParams>,
    db_pool: &PgPool,
) -> Result<Response, ProcessStartError> {
    let process_id = params.0.id;

    let result = sqlx::query!(r#"DELETE FROM processes WHERE id = $1"#, process_id)
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
    db_pool: &PgPool,
) -> Result<Response, ProcessStartError> {
    let instance_id = &params.0.id;

    let result = sqlx::query!(
        r#"DELETE FROM process_instances WHERE id = $1"#,
        instance_id
    )
    .execute(db_pool)
    .await
    .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;

    if result.rows_affected() == 0 {
        return Err(ProcessStartError::NotFound);
    }

    Ok(Response::ok().insert_header(
        pavex::http::header::HeaderName::from_static("hx-redirect"),
        pavex::http::HeaderValue::from_static("/processes/instances"),
    ))
}
