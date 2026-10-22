use super::{
    ActiveMarker, GatewayChoice, InstanceIdParams, ProcessStartError, gateway_choices_after,
    json_for_script,
};
use crate::routes::nav::NAV_ITEMS;
use crate::session::auth::CheckedInUser;
use crate::session::theme::Theme;
use askama::Template;
use engine_core::{InstanceState, WorkflowEngine};
use pavex::request::path::PathParams;
use pavex::response::body::Html;
use pavex::{Response, get};
use sqlx::PgPool;
use std::sync::Arc;

#[derive(Template)]
#[template(path = "process_instance_detail.html")]
struct ProcessInstanceDetailPage {
    active_page: &'static str,
    nav_items: &'static [crate::routes::nav::NavItem],
    theme: Theme,
    instance_id: String,
    process_name: String,
    state: String,
    rows: Vec<PendingRow>,
    bpmn_xml_json: String,
    markers_json: String,
}

struct PendingRow {
    kind: &'static str,
    node: String,
    detail: String,
    task_id: String,
    is_user_task: bool,
    gateway_choices: Vec<GatewayChoice>,
}

fn unexpected(e: impl Into<anyhow::Error>) -> ProcessStartError {
    ProcessStartError::UnexpectedError(e.into())
}

#[get(path = "/processes/instances/{id}")]
pub async fn instance_detail(
    _user: &CheckedInUser,
    params: &PathParams<InstanceIdParams>,
    theme: Theme,
    engine: &Arc<WorkflowEngine>,
    db_pool: &PgPool,
) -> Result<Response, ProcessStartError> {
    let instance = engine.get_instance_details(params.0.id).await?;
    let def = engine
        .get_definition(&instance.definition_key)
        .await
        .ok_or(ProcessStartError::NotFound)?;

    let (process_name, bpmn_xml): (String, String) =
        sqlx::query_as("SELECT name, bpmn_xml FROM processes WHERE id = $1")
            .bind(instance.definition_key)
            .fetch_optional(db_pool)
            .await
            .map_err(unexpected)?
            .ok_or(ProcessStartError::NotFound)?;

    let mut rows = Vec::new();
    let mut markers = Vec::new();

    for t in engine
        .get_pending_user_tasks()
        .into_iter()
        .filter(|t| t.instance_id == instance.id)
    {
        let gateway_choices = gateway_choices_after(&def, &t.node_id)
            .map_err(|e| unexpected(anyhow::anyhow!(e)))?
            .unwrap_or_default();
        markers.push(ActiveMarker {
            node_id: t.node_id.clone(),
            label: format!("Waiting: {}", t.assignee),
        });
        rows.push(PendingRow {
            kind: "User task",
            node: t.node_id,
            detail: format!("assignee: {}", t.assignee),
            task_id: t.task_id.to_string(),
            is_user_task: true,
            gateway_choices,
        });
    }

    for t in engine
        .get_pending_service_tasks()
        .into_iter()
        .filter(|t| t.instance_id == instance.id)
    {
        markers.push(ActiveMarker {
            node_id: t.node_id.clone(),
            label: format!("Service: {}", t.topic),
        });
        rows.push(PendingRow {
            kind: "Service task",
            node: t.node_id,
            detail: format!("topic: {}, retries: {}", t.topic, t.retries),
            task_id: String::new(),
            is_user_task: false,
            gateway_choices: Vec::new(),
        });
    }

    // nil token_id = scope event listener; node_id is not a diagram node.
    for t in engine
        .get_pending_timers()
        .into_iter()
        .filter(|t| t.instance_id == instance.id && !t.token_id.is_nil())
    {
        markers.push(ActiveMarker {
            node_id: t.node_id.clone(),
            label: "Timer".to_string(),
        });
        rows.push(PendingRow {
            kind: "Timer",
            node: t.node_id,
            detail: format!("fires at {}", t.expires_at.to_rfc3339()),
            task_id: String::new(),
            is_user_task: false,
            gateway_choices: Vec::new(),
        });
    }

    for m in engine
        .get_pending_message_catches()
        .into_iter()
        .filter(|m| m.instance_id == instance.id && !m.token_id.is_nil())
    {
        markers.push(ActiveMarker {
            node_id: m.node_id.clone(),
            label: "Message".to_string(),
        });
        rows.push(PendingRow {
            kind: "Message",
            node: m.node_id,
            detail: format!("waiting for `{}`", m.message_name),
            task_id: String::new(),
            is_user_task: false,
            gateway_choices: Vec::new(),
        });
    }

    match &instance.state {
        InstanceState::Completed => markers.push(ActiveMarker {
            node_id: instance.current_node.clone(),
            label: "Completed".to_string(),
        }),
        InstanceState::CompletedWithError { error_code } => markers.push(ActiveMarker {
            node_id: instance.current_node.clone(),
            label: format!("Error: {error_code}"),
        }),
        _ => {}
    }

    let body = ProcessInstanceDetailPage {
        active_page: "processes",
        nav_items: NAV_ITEMS,
        theme,
        instance_id: instance.id.to_string(),
        process_name,
        state: crate::pg_persistence::state_label(&instance.state).to_string(),
        rows,
        bpmn_xml_json: json_for_script(&bpmn_xml).map_err(unexpected)?,
        markers_json: json_for_script(&markers).map_err(unexpected)?,
    }
    .render()
    .map_err(unexpected)?;
    let html: Html = body.into();
    Ok(Response::ok().set_typed_body(html))
}
