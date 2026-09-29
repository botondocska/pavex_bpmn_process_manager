use super::{InstanceIdParams, ProcessStartError};
use crate::session::auth::CheckedInUser;
use bpm_engine_core::event::{EngineEvent, payloads};
use bpm_engine_runtime::{BpmEngine, EngineContext};
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

    let mut instance = process_store
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

    // WORKAROUND: bpm-engine-runtime 0.2.0's UserTaskCompletedHandler drops
    // the `variables` carried on the UserTaskCompleted event -- it moves
    // tokens and saves the instance, but never merges `e.variables` into
    // `instance.variables`. Confirmed from crate source (user_task_completed_handler.rs):
    // https://docs.rs/bpm-engine-runtime/0.2.0/src/bpm_engine_runtime/user_task_completed_handler.rs.html
    // Without this, any downstream exclusive gateway reading instance
    // variables sees an empty map and dead-ends (its evaluate_exclusive_gateway
    // returns None with no default flow, silently, with no error).
    // We persist the variable ourselves, into the same store/column the
    // gateway reads from, before triggering the engine.
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
