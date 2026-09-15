use pavex::Response;
use pavex_session::Session;
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::visit_tracking::catch_up_user;

#[derive(Debug, Clone, Copy)]
pub struct CheckedInUser(pub Uuid);

#[derive(Debug, thiserror::Error)]
pub enum CheckInError {
    #[error("Not logged in")]
    NotLoggedIn,
    #[error("Failed to catch up user history")]
    CatchUpFailed(#[source] anyhow::Error),
}

#[pavex::methods]
impl CheckedInUser {
    #[request_scoped]
    pub async fn extract(session: &Session<'_>, pool: &SqlitePool) -> Result<Self, CheckInError> {
        let raw = session
            .get::<String>("user_id")
            .await
            .map_err(|_| CheckInError::NotLoggedIn)?
            .ok_or(CheckInError::NotLoggedIn)?;
        let id = Uuid::parse_str(&raw).map_err(|_| CheckInError::NotLoggedIn)?;

        let user_id_str = id.to_string();
        catch_up_user(&user_id_str, pool)
            .await
            .map_err(CheckInError::CatchUpFailed)?;

        Ok(CheckedInUser(id))
    }
}

#[pavex::methods]
impl CheckInError {
    #[error_handler]
    pub fn into_response(&self) -> Response {
        match self {
            CheckInError::NotLoggedIn => Response::see_other().insert_header(
                pavex::http::header::LOCATION,
                pavex::http::HeaderValue::from_static("/login"),
            ),
            CheckInError::CatchUpFailed(_) => {
                Response::internal_server_error().set_typed_body(format!("{self}"))
            }
        }
    }
}
