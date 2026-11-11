use crate::oidc::OidcClient;
use crate::session::theme::Theme;
use askama::Template;
use openidconnect::core::CoreAuthenticationFlow;
use openidconnect::{CsrfToken, Nonce, PkceCodeChallenge, Scope};
use pavex::response::body::Html;
use pavex::{Response, get};
use pavex_session::Session;

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

#[derive(serde::Serialize, serde::Deserialize)]
pub struct OidcFlowState {
    pub csrf: String,
    pub nonce: String,
    pub verifier: String,
}

#[get(path = "/login/adfs")]
pub async fn login_adfs(
    oidc: &OidcClient,
    session: &mut Session<'_>,
) -> Result<Response, SigninError> {
    let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
    let mut req = oidc.client.authorize_url(
        CoreAuthenticationFlow::AuthorizationCode,
        CsrfToken::new_random,
        Nonce::new_random,
    );
    for s in &oidc.scopes {
        req = req.add_scope(Scope::new(s.clone()));
    }
    let (url, csrf, nonce) = req.set_pkce_challenge(challenge).url();

    session
        .insert(
            "oidc",
            OidcFlowState {
                csrf: csrf.secret().clone(),
                nonce: nonce.secret().clone(),
                verifier: verifier.secret().clone(),
            },
        )
        .await
        .map_err(|e| SigninError::UnexpectedError(e.into()))?;

    let loc = pavex::http::HeaderValue::from_str(url.as_str())
        .map_err(|e| SigninError::UnexpectedError(e.into()))?;
    Ok(Response::see_other().insert_header(pavex::http::header::LOCATION, loc))
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
