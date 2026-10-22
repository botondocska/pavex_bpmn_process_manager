use super::{InstanceIdParams, ProcessStartError, gateway_choices_after};
use crate::session::auth::CheckedInUser;
use engine_core::WorkflowEngine;
use pavex::request::body::UrlEncodedBody;
use pavex::request::path::PathParams;
use pavex::{Response, post};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

#[derive(serde::Deserialize)]
pub struct AdvanceForm {
    pub task_id: String,
    #[serde(flatten)]
    pub choices: HashMap<String, String>,
}

#[post(path = "/processes/instances/{id}/advance")]
pub async fn advance_instance(
    _user: &CheckedInUser,
    params: &PathParams<InstanceIdParams>,
    body: &UrlEncodedBody<AdvanceForm>,
    engine: &Arc<WorkflowEngine>,
) -> Result<Response, ProcessStartError> {
    let instance_id = params.0.id;
    let task_id = Uuid::parse_str(&body.0.task_id).map_err(|_| ProcessStartError::NotFound)?;

    let task = engine
        .get_pending_user_tasks()
        .into_iter()
        .find(|t| t.task_id == task_id && t.instance_id == instance_id)
        .ok_or(ProcessStartError::NotFound)?;

    let instance = engine.get_instance_details(instance_id).await?;
    let def = engine
        .get_definition(&instance.definition_key)
        .await
        .ok_or(ProcessStartError::NotFound)?;

    // Only whitelisted gateway keys/values reach instance variables.
    let choices = gateway_choices_after(&def, &task.node_id)
        .map_err(|e| ProcessStartError::UnexpectedError(anyhow::anyhow!(e)))?
        .unwrap_or_default();

    let mut vars = HashMap::new();
    for c in &choices {
        let label = body
            .0
            .choices
            .get(&c.key)
            .ok_or_else(|| ProcessStartError::InvalidChoice(format!("missing `{}`", c.key)))?;
        let v = c.values.iter().find(|v| &v.label == label).ok_or_else(|| {
            ProcessStartError::InvalidChoice(format!("bad value for `{}`", c.key))
        })?;
        vars.insert(c.key.clone(), v.value.clone());
    }

    engine.complete_user_task(task_id, vars).await?;

    let redirect =
        pavex::http::HeaderValue::from_str(&format!("/processes/instances/{instance_id}"))
            .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;
    Ok(Response::ok().insert_header(
        pavex::http::header::HeaderName::from_static("hx-redirect"),
        redirect,
    ))
}
