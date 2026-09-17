use crate::routes::password::{AuthError, validate_credentials};
use crate::session::theme::Theme;
use askama::Template;
use pavex::request::body::UrlEncodedBody;
use pavex::response::body::Html;
use pavex::{Response, get, post};
use pavex_session::Session;
use secrecy::Secret;
use sqlx::PgPool;

#[derive(Template)]
#[template(path = "login.html")]
struct LoginPage {
    theme: Theme,
}

#[get(path = "/login")]
pub fn signin_form(theme: Theme) -> Result<Response, SigninError> {
    let body = LoginPage { theme }
        .render()
        .map_err(|e| SigninError::UnexpectedError(e.into()))?;
    let html: Html = body.into();
    Ok(Response::ok().set_typed_body(html))
}

#[post(path = "/login")]
pub async fn signin(
    body: &UrlEncodedBody<SigninForm>,
    db_pool: &PgPool,
    session: &mut Session<'_>,
) -> Result<Response, SigninError> {
    let SigninForm { email, password } = &body.0;
    let user_id = validate_credentials(email, password.clone(), db_pool)
        .await
        .map_err(|e| match e {
            AuthError::InvalidCredentials(e) => SigninError::InvalidCredentials(e),
            AuthError::UnexpectedError(e) => SigninError::UnexpectedError(e),
        })?;

    session
        .insert("user_id", user_id.to_string())
        .await
        .map_err(|e| SigninError::UnexpectedError(e.into()))?;

    Ok(Response::ok().insert_header(
        pavex::http::header::HeaderName::from_static("hx-redirect"),
        pavex::http::HeaderValue::from_static("/"),
    ))
}

#[derive(serde::Deserialize)]
pub struct SigninForm {
    pub email: String,
    pub password: Secret<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SigninError {
    #[error("Invalid email or password.")]
    InvalidCredentials(#[source] anyhow::Error),
    #[error("Something went wrong. Please retry later.")]
    UnexpectedError(#[source] anyhow::Error),
}

#[pavex::methods]
impl SigninError {
    #[error_handler]
    pub fn into_response(&self) -> Response {
        match self {
            SigninError::InvalidCredentials(_) => Response::unauthorized(),
            SigninError::UnexpectedError(_) => Response::internal_server_error(),
        }
        .set_typed_body(format!("{self}"))
    }
}
