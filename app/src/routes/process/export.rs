use super::{ProcessIdParams, ProcessStartError};
use crate::session::auth::CheckedInUser;
use pavex::http::header::{CONTENT_DISPOSITION, CONTENT_TYPE, HeaderValue};
use pavex::request::path::PathParams;
use pavex::{Response, get};
use sqlx::PgPool;

#[get(path = "/processes/{id}/export")]
pub async fn export_process(
    _user: &CheckedInUser,
    params: &PathParams<ProcessIdParams>,
    db_pool: &PgPool,
) -> Result<Response, ProcessStartError> {
    let row = sqlx::query!(
        r#"SELECT name, bpmn_xml FROM processes WHERE id = $1"#,
        params.0.id,
    )
    .fetch_optional(db_pool)
    .await
    .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?
    .ok_or(ProcessStartError::NotFound)?;

    // Header-safe filename: ascii alnum, '-' and '_' only.
    let safe: String = row
        .name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let safe = if safe.is_empty() {
        "process".to_string()
    } else {
        safe
    };

    let disposition = HeaderValue::from_str(&format!("attachment; filename=\"{safe}.bpmn\""))
        .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;

    Ok(Response::ok()
        .set_typed_body(row.bpmn_xml)
        .insert_header(
            CONTENT_TYPE,
            HeaderValue::from_static("application/xml; charset=utf-8"),
        )
        .insert_header(CONTENT_DISPOSITION, disposition))
}
