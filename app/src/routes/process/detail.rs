use super::{
    GatewayChoice, InstanceIdParams, ProcessStartError, gateway_choices_after, json_for_script,
    pick_active_marker,
};
use crate::routes::nav::NAV_ITEMS;
use crate::session::auth::CheckedInUser;
use crate::session::theme::Theme;
use askama::Template;
use bpm_engine_storage::{ProcessDefinitionStore, process_store::ProcessInstanceStore};
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
    state: String,
    tokens: Vec<TokenRow>,
    bpmn_xml_json: String,
    marker_json: String,
}

struct TokenRow {
    id: String,
    node_id: String,
    status: String,
    is_user_task: bool,
    gateway_choices: Vec<GatewayChoice>,
}

#[get(path = "/processes/instances/{id}")]
pub async fn instance_detail(
    _user: &CheckedInUser,
    params: &PathParams<InstanceIdParams>,
    theme: Theme,
    process_store: &Arc<crate::pg_process_store::PgProcessInstanceStore>,
    def_store: &Arc<crate::engine_def_store::PgProcessDefinitionStore>,
    db_pool: &PgPool,
) -> Result<Response, ProcessStartError> {
    let instance = process_store
        .load(&params.0.id)
        .await
        .map_err(ProcessStartError::UnexpectedError)?
        .ok_or(ProcessStartError::NotFound)?;

    let def = def_store
        .load(&instance.process_def_id)
        .await
        .map_err(ProcessStartError::UnexpectedError)?
        .ok_or(ProcessStartError::NotFound)?;

    // process_def_id is a stringified UUID (see engine_def_store.rs); the
    // `processes` table is where the raw BPMN XML actually lives -- `def`
    // here is only the compiled/executable form and doesn't carry the
    // source XML text needed to render it.
    let process_uuid = uuid::Uuid::parse_str(&instance.process_def_id)
        .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;
    let bpmn_xml = sqlx::query_scalar!(
        r#"SELECT bpmn_xml FROM processes WHERE id = $1"#,
        process_uuid,
    )
    .fetch_one(db_pool)
    .await
    .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;

    let marker = pick_active_marker(&def, &instance);

    let mut tokens = Vec::with_capacity(instance.tokens.len());
    for t in &instance.tokens {
        let is_user_task = def
            .nodes
            .get(t.node_id.as_str())
            .map(|n| matches!(n.node_type, bpm_engine_core::node::NodeType::UserTask))
            .unwrap_or(false);

        // Only advanceable (Waiting UserTask) tokens need gateway-choice
        // discovery; other tokens (fork/join/service/etc.) never render an
        // advance form, so skip the walk for them.
        let gateway_choices =
            if is_user_task && matches!(t.status, bpm_engine_core::TokenStatus::Waiting) {
                gateway_choices_after(&def, &t.node_id)
                    .map_err(|e| ProcessStartError::UnexpectedError(anyhow::anyhow!(e)))?
                    .unwrap_or_default()
            } else {
                Vec::new()
            };

        tokens.push(TokenRow {
            id: t.id.clone(),
            node_id: match t
                .node_id
                .strip_suffix(crate::engine_def_store::DECISION_SUFFIX)
            {
                Some(g) => format!("{g} (decision)"),
                None => t.node_id.clone(),
            },
            status: format!("{:?}", t.status),
            is_user_task,
            gateway_choices,
        });
    }

    let bpmn_xml_json =
        json_for_script(&bpmn_xml).map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;
    let marker_json =
        json_for_script(&marker).map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;

    let body = ProcessInstanceDetailPage {
        active_page: "processes",
        nav_items: NAV_ITEMS,
        theme,
        instance_id: instance.id,
        state: format!("{:?}", instance.state),
        tokens,
        bpmn_xml_json,
        marker_json,
    }
    .render()
    .map_err(|e| ProcessStartError::UnexpectedError(e.into()))?;
    let html: Html = body.into();
    Ok(Response::ok().set_typed_body(html))
}
