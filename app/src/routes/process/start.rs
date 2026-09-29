use super::{ProcessIdParams, ProcessStartError};
use crate::session::auth::CheckedInUser;
use bpm_engine_core::event::{EngineEvent, payloads};
use bpm_engine_runtime::{BpmEngine, EngineContext};
use bpm_engine_storage::process_store::ProcessInstanceStore;
use pavex::request::path::PathParams;
use pavex::{Response, post};
use std::sync::Arc;

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
