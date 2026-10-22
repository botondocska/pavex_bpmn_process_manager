use super::ProcessUploadError;
use crate::routes::nav::NAV_ITEMS;
use crate::session::auth::CheckedInUser;
use crate::session::theme::Theme;
use crate::workflow::ensure_deployed;
use askama::Template;
use engine_core::WorkflowEngine;
use pavex::request::body::UrlEncodedBody;
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
    engine: &Arc<WorkflowEngine>,
) -> Result<Response, ProcessUploadError> {
    let ProcessUploadForm { name, bpmn_xml } = &body.0;

    bpmn_parser::parse_bpmn_xml(bpmn_xml)
        .map_err(|e| ProcessUploadError::InvalidBpmn(e.to_string()))?;

    let process_id = insert_process(user.0, name, bpmn_xml, db_pool).await?;

    // Deploy now so call activities in other processes can resolve it.
    ensure_deployed(engine, db_pool, process_id)
        .await
        .map_err(ProcessUploadError::UnexpectedError)?;

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
