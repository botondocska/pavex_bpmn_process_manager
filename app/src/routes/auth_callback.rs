use crate::oidc::OidcClient;
use crate::routes::login::{OidcFlowState, SigninError};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use openidconnect::{AuthorizationCode, Nonce, PkceCodeVerifier, TokenResponse};
use pavex::request::query::QueryParams;
use pavex::{Response, get};
use pavex_session::Session;
use sqlx::PgPool;

#[derive(serde::Deserialize)]
pub struct CallbackParams {
    pub code: String,
    pub state: String,
}

#[get(path = "/auth/callback")]
pub async fn auth_callback(
    params: &QueryParams<CallbackParams>,
    oidc: &OidcClient,
    db_pool: &PgPool,
    session: &mut Session<'_>,
) -> Result<Response, SigninError> {
    let unexpected = |e: anyhow::Error| SigninError::UnexpectedError(e);
    let bad = |m: &str| SigninError::InvalidCredentials(anyhow::anyhow!(m.to_string()));

    let flow: OidcFlowState = session
        .remove("oidc")
        .await
        .map_err(|e| unexpected(e.into()))?
        .ok_or_else(|| bad("no flow in session"))?;
    if flow.csrf != params.0.state {
        return Err(bad("state mismatch"));
    }

    let tokens = oidc
        .client
        .exchange_code(AuthorizationCode::new(params.0.code.clone()))
        .map_err(|e| unexpected(e.into()))?
        .set_pkce_verifier(PkceCodeVerifier::new(flow.verifier))
        .request_async(&oidc.http)
        .await
        .map_err(|e| unexpected(e.into()))?;

    let id_token = tokens.id_token().ok_or_else(|| bad("no id_token"))?;
    // Verifies signature, issuer, audience, expiry, nonce.
    id_token
        .claims(&oidc.client.id_token_verifier(), &Nonce::new(flow.nonce))
        .map_err(|e| bad(&e.to_string()))?;

    // Signature already verified, so read raw payload for ADFS custom claims.
    let jwt = id_token.to_string();
    let payload = jwt.split('.').nth(1).ok_or_else(|| bad("bad jwt"))?;
    let json: serde_json::Value = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|e| unexpected(e.into()))?,
    )
    .map_err(|e| unexpected(e.into()))?;

    let sub = json["sub"].as_str().ok_or_else(|| bad("no sub"))?;
    let email = json[&oidc.email_claim]
        .as_str()
        .or_else(|| json["upn"].as_str())
        .ok_or_else(|| bad("no email/upn claim"))?;
    // ADFS: single group = string, many = array.
    let groups: Vec<String> = match &json[&oidc.groups_claim] {
        serde_json::Value::String(s) => vec![s.clone()],
        serde_json::Value::Array(a) => a
            .iter()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect(),
        _ => vec![],
    };

    let user_id = sqlx::query_scalar!(
        r#"INSERT INTO users (id, email, external_id) VALUES ($1, $2, $3)
           ON CONFLICT (external_id) DO UPDATE SET email = EXCLUDED.email, updated_at = now()
           RETURNING id"#,
        uuid::Uuid::new_v4(),
        email,
        sub,
    )
    .fetch_one(db_pool)
    .await
    .map_err(|e| unexpected(e.into()))?;

    session.cycle_id(); // anti session fixation
    session
        .insert("user_id", user_id.to_string())
        .await
        .map_err(|e| unexpected(e.into()))?;
    session
        .insert("groups", groups)
        .await
        .map_err(|e| unexpected(e.into()))?;

    Ok(Response::see_other().insert_header(
        pavex::http::header::LOCATION,
        pavex::http::HeaderValue::from_static("/"),
    ))
}
