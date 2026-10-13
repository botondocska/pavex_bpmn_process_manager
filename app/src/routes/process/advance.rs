use super::{InstanceIdParams, ProcessStartError};
use crate::session::auth::CheckedInUser;
use bpm_engine_core::event::{EngineEvent, payloads};
use bpm_engine_runtime::BpmEngine;
use bpm_engine_storage::process_store::ProcessInstanceStore;
use pavex::request::body::UrlEncodedBody;
use pavex::request::path::PathParams;
use pavex::{Response, post};
use std::sync::Arc;

#[derive(serde::Deserialize)]
pub struct AdvanceForm {
    pub token_id: String,
    #[serde(flatten)]
    pub gateway_vars: std::collections::HashMap<String, String>,
}

#[post(path = "/processes/instances/{id}/advance")]
pub async fn advance_instance(
    _user: &CheckedInUser,
    params: &PathParams<InstanceIdParams>,
    body: &UrlEncodedBody<AdvanceForm>,
    deps: &crate::engine_deps::EngineDeps,
    engine: &Arc<BpmEngine>,
) -> Result<Response, ProcessStartError> {
    let process_store = &deps.process_store;
    let instance_id = params.0.id.clone();
    let token_id = body.0.token_id.clone();

    let mut ctx = deps.context();

    let mut instance = process_store
        .load(&instance_id)
        .await
        .map_err(ProcessStartError::UnexpectedError)?
        .ok_or(ProcessStartError::NotFound)?;

    if !matches!(instance.state, bpm_engine_core::InstanceState::Running) {
        return Err(ProcessStartError::NotRunning);
    }

    let node_id = instance
        .tokens
        .iter()
        .find(|t| t.id == token_id)
        .map(|t| t.node_id.clone())
        .ok_or(ProcessStartError::NotFound)?;

    // WORKAROUND: (keep your existing comment)
    let mut variables = instance.variables.clone();
    variables.extend(body.0.gateway_vars.clone());
    if variables != instance.variables {
        instance.variables = variables;
        process_store
            .save(&instance)
            .await
            .map_err(ProcessStartError::UnexpectedError)?;
    }

    engine
        .run_async(
            EngineEvent::UserTaskCompleted(payloads::UserTaskCompleted {
                task_id: token_id.clone(),
                instance_id: instance_id.clone(),
                node_id,
                variables: body.0.gateway_vars.clone(),
            }),
            &mut ctx,
        )
        .await;

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
