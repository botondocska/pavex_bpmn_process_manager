use super::ProcessUploadError;
use crate::routes::nav::NAV_ITEMS;
use crate::session::auth::CheckedInUser;
use crate::session::theme::Theme;
use askama::Template;
use pavex::response::body::Html;
use pavex::{Response, get};

#[derive(Template)]
#[template(path = "process_editor.html")]
struct ProcessEditorPage {
    active_page: &'static str,
    nav_items: &'static [crate::routes::nav::NavItem],
    theme: Theme,
}

#[get(path = "/processes/editor")]
pub fn process_editor(_user: &CheckedInUser, theme: Theme) -> Result<Response, ProcessUploadError> {
    let body = ProcessEditorPage {
        active_page: "processes",
        nav_items: NAV_ITEMS,
        theme,
    }
    .render()
    .map_err(|e| ProcessUploadError::UnexpectedError(e.into()))?;
    let html: Html = body.into();
    Ok(Response::ok().set_typed_body(html))
}
