use super::{ProcessIdParams, ProcessStartError};
use crate::session::auth::CheckedInUser;
use crate::workflow::ensure_deployed;
use engine_core::WorkflowEngine;
use pavex::request::path::PathParams;
use pavex::{Response, post};
use sqlx::PgPool;
use std::sync::Arc;

#[post(path = "/processes/{id}/start")]
pub async fn start_process(
    _user: &CheckedInUser,
    params: &PathParams<ProcessIdParams>,
    engine: &Arc<WorkflowEngine>,
    db_pool: &PgPool,
) -> Result<Response, ProcessStartError> {
    let process_id = params.0.id;

    if !ensure_deployed(engine, db_pool, process_id)
        .await
        .map_err(ProcessStartError::UnexpectedError)?
    {
        return Err(ProcessStartError::NotFound);
    }

    let instance_id = engine.start_instance(process_id).await?;

    let redirect =
        pavex::http::HeaderValue::from_str(&format!("/processes/instances/{instance_id}"))
            .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;
    Ok(Response::ok().insert_header(
        pavex::http::header::HeaderName::from_static("hx-redirect"),
        redirect,
    ))
}
