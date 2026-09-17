use pavex::{Response, http::HeaderValue, post};
use pavex_session::Session;

#[post(path = "/logout")]
pub async fn logout(session: &mut Session<'_>) -> Response {
    session.invalidate();
    // redirect to login
    Response::see_other().insert_header(
        pavex::http::header::LOCATION,
        HeaderValue::from_static("/login"),
    )
}
