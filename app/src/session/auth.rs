use pavex::Response;
use pavex_session::Session;
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub struct CheckedInUser(pub Uuid);

#[derive(Debug, thiserror::Error)]
pub enum CheckInError {
    #[error("Not logged in")]
    NotLoggedIn,
}

#[pavex::methods]
impl CheckedInUser {
    #[request_scoped]
    pub async fn extract(session: &Session<'_>) -> Result<Self, CheckInError> {
        let raw = session
            .get::<String>("user_id")
            .await
            .map_err(|_| CheckInError::NotLoggedIn)?
            .ok_or(CheckInError::NotLoggedIn)?;
        let id = Uuid::parse_str(&raw).map_err(|_| CheckInError::NotLoggedIn)?;
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
        }
    }
}
