use super::{InstanceIdParams, ProcessStartError};
use crate::session::auth::CheckedInUser;
use bpm_engine_core::{InstanceState, TokenStatus};
use bpm_engine_storage::process_store::ProcessInstanceStore;
use pavex::request::path::PathParams;
use pavex::{Response, post};
use std::sync::Arc;

#[post(path = "/processes/instances/{id}/cancel")]
pub async fn cancel_instance(
    _user: &CheckedInUser,
    params: &PathParams<InstanceIdParams>,
    ext_store: &Arc<crate::pg_external_task_store::PgExternalTaskStore>,
    process_store: &Arc<crate::pg_process_store::PgProcessInstanceStore>,
) -> Result<Response, ProcessStartError> {
    let instance_id = &params.0.id;

    let mut instance = process_store
        .load(instance_id)
        .await
        .map_err(ProcessStartError::UnexpectedError)?
        .ok_or(ProcessStartError::NotFound)?;

    if !matches!(instance.state, InstanceState::Running) {
        return Err(ProcessStartError::NotRunning);
    }

    instance.state = InstanceState::Terminated;
    for t in instance.tokens.iter_mut() {
        if !matches!(t.status, TokenStatus::Completed | TokenStatus::Terminated) {
            t.status = TokenStatus::Terminated;
        }
    }

    process_store
        .save(&instance)
        .await
        .map_err(ProcessStartError::UnexpectedError)?;

    ext_store
        .cancel_for_instance(instance_id)
        .await
        .map_err(ProcessStartError::UnexpectedError)?;

    let redirect =
        pavex::http::HeaderValue::from_str(&format!("/processes/instances/{instance_id}"))
            .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;

    Ok(Response::ok().insert_header(
        pavex::http::header::HeaderName::from_static("hx-redirect"),
        redirect,
    ))
}
