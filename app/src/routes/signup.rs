use crate::routes::password::compute_password_hash;
use crate::session::theme::Theme;
use askama::Template;
use pavex::request::body::UrlEncodedBody;
use pavex::response::body::Html;
use pavex::{Response, get, post};
use pavex_session::Session;
use secrecy::{ExposeSecret, Secret};
use sqlx::PgPool;

#[derive(Template)]
#[template(path = "signup.html")]
struct SignupPage {
    theme: Theme,
}

#[get(path = "/signup")]
pub fn signup_form(theme: Theme) -> Result<Response, SignupError> {
    let body = SignupPage { theme }
        .render()
        .map_err(|e| SignupError::UnexpectedError(e.into()))?;
    let html: Html = body.into();
    Ok(Response::ok().set_typed_body(html))
}

#[post(path = "/signup")]
pub async fn signup(
    body: &UrlEncodedBody<SignupForm>,
    db_pool: &PgPool,
    session: &mut Session<'_>,
) -> Result<Response, SignupError> {
    let SignupForm { email, password } = &body.0;
    let password_hash =
        compute_password_hash(password.clone()).map_err(SignupError::UnexpectedError)?;
    let user_id = insert_user_record(email, &password_hash, db_pool).await?;

    session
        .insert("user_id", user_id.to_string())
        .await
        .map_err(|e| SignupError::UnexpectedError(e.into()))?;

    Ok(Response::ok().insert_header(
        pavex::http::header::HeaderName::from_static("hx-redirect"),
        pavex::http::HeaderValue::from_static("/"),
    ))
}

#[derive(serde::Deserialize)]
pub struct SignupForm {
    pub email: String,
    pub password: Secret<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SignupError {
    #[error("That email is already taken.")]
    Conflict(#[source] anyhow::Error),
    #[error("Something went wrong. Please retry later.")]
    UnexpectedError(#[source] anyhow::Error),
}

#[pavex::methods]
impl SignupError {
    #[error_handler]
    pub fn into_response(&self) -> Response {
        match self {
            SignupError::Conflict(_) => Response::conflict(),
            SignupError::UnexpectedError(_) => Response::internal_server_error(),
        }
        .set_typed_body(format!("{self}"))
    }
}

async fn insert_user_record(
    email: &str,
    password_hash: &Secret<String>,
    pool: &PgPool,
) -> Result<uuid::Uuid, SignupError> {
    let user_id = uuid::Uuid::new_v4();
    let password_hash = password_hash.expose_secret();
    sqlx::query!(
        r#"INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)"#,
        user_id,
        email,
        password_hash,
    )
    .execute(pool)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
            SignupError::Conflict(e.into())
        }
        _ => SignupError::UnexpectedError(
            anyhow::Error::new(e).context("Failed to insert user record."),
        ),
    })?;

    Ok(user_id)
}
