use crate::routes::nav::NAV_ITEMS;
use crate::session::auth::CheckedInUser;
use crate::session::theme::Theme;
use askama::Template;
use pavex::Response;
use pavex::get;
use pavex::response::body::Html;

#[derive(Template)]
#[template(path = "home.html")]
struct HomePage {
    active_page: &'static str,
    nav_items: &'static [crate::routes::nav::NavItem],
    theme: Theme,
}

#[get(path = "/")]
pub fn home_page(_user: &CheckedInUser, theme: Theme) -> Result<Response, HomeError> {
    let body = HomePage {
        active_page: "home",
        nav_items: NAV_ITEMS,
        theme,
    }
    .render()
    .map_err(|e| HomeError::UnexpectedError(e.into()))?;
    let html: Html = body.into();
    Ok(Response::ok().set_typed_body(html))
}

#[derive(Debug, thiserror::Error)]
pub enum HomeError {
    #[error("Something went wrong. Please retry later.")]
    UnexpectedError(#[source] anyhow::Error),
}

#[pavex::methods]
impl HomeError {
    #[error_handler]
    pub fn into_response(&self) -> Response {
        Response::internal_server_error().set_typed_body(format!("{self}"))
    }
}
