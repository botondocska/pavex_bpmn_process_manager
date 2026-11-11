use openidconnect::core::{CoreClient, CoreProviderMetadata};
use openidconnect::{
    ClientId, ClientSecret, EndpointMaybeSet, EndpointNotSet, EndpointSet, IssuerUrl, RedirectUrl,
};
use pavex::config;
use secrecy::{ExposeSecret, Secret};

type Client = CoreClient<
    EndpointSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointMaybeSet,
    EndpointMaybeSet,
>;

fn default_groups_claim() -> String {
    "group".into()
}
fn default_email_claim() -> String {
    "email".into()
}

#[derive(serde::Deserialize, Debug, Clone)]
#[config(key = "auth")]
pub struct AuthConfig {
    pub issuer_url: String,
    pub client_id: String,
    /// `PX_AUTH__CLIENT_SECRET`
    pub client_secret: Secret<String>,
    pub redirect_uri: String,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default = "default_groups_claim")]
    pub groups_claim: String,
    #[serde(default = "default_email_claim")]
    pub email_claim: String,
}

pub struct OidcClient {
    pub client: Client,
    pub http: reqwest::Client,
    pub scopes: Vec<String>,
    pub groups_claim: String,
    pub email_claim: String,
}

#[derive(Debug, thiserror::Error)]
pub enum OidcSetupError {
    #[error("OIDC setup failed")]
    Setup(#[source] anyhow::Error),
}

#[pavex::methods]
impl AuthConfig {
    /// Runs ADFS discovery once at startup.
    #[pavex::singleton]
    pub async fn oidc_client(&self) -> Result<OidcClient, OidcSetupError> {
        build(self).await.map_err(OidcSetupError::Setup)
    }
}

async fn build(c: &AuthConfig) -> anyhow::Result<OidcClient> {
    // No redirects: SSRF guard, required by openidconnect.
    let http = reqwest::ClientBuilder::new()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let meta =
        CoreProviderMetadata::discover_async(IssuerUrl::new(c.issuer_url.clone())?, &http).await?;
    let client = CoreClient::from_provider_metadata(
        meta,
        ClientId::new(c.client_id.clone()),
        Some(ClientSecret::new(c.client_secret.expose_secret().clone())),
    )
    .set_redirect_uri(RedirectUrl::new(c.redirect_uri.clone())?);
    Ok(OidcClient {
        client,
        http,
        scopes: c.scopes.clone(),
        groups_claim: c.groups_claim.clone(),
        email_claim: c.email_claim.clone(),
    })
}
