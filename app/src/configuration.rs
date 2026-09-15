//! Refer to Pavex's [configuration guide](https://pavex.dev/docs/guide/configuration) for more details
//! on how to manage configuration values.
use pavex::config;
use pavex::server::IncomingStream;
use pavex_session::{SessionStore};
use pavex_session_sqlx::postgres::PostgresSessionStore;
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use secrecy::{ExposeSecret, Secret};
use serde_aux::field_attributes::deserialize_number_from_string;


#[derive(serde::Deserialize, Debug, Clone)]
/// Configuration for the HTTP server used to expose our API
/// to users.
#[config(key = "server", include_if_unused)]
pub struct ServerConfig {
    /// The port that the server must listen on.
    ///
    /// Set the `PX_SERVER__PORT` environment variable to override its value.
    #[serde(deserialize_with = "serde_aux::field_attributes::deserialize_number_from_string")]
    pub port: u16,
    /// The network interface that the server must be bound to.
    ///
    /// E.g. `0.0.0.0` for listening to incoming requests from
    /// all sources.
    ///
    /// Set the `PX_SERVER__IP` environment variable to override its value.
    pub ip: std::net::IpAddr,
    /// The timeout for graceful shutdown of the server.
    ///
    /// E.g. `1 minute` for a 1 minute timeout.
    ///
    /// Set the `PX_SERVER__GRACEFUL_SHUTDOWN_TIMEOUT` environment variable to override its value.
    #[serde(deserialize_with = "deserialize_shutdown")]
    pub graceful_shutdown_timeout: std::time::Duration,
}

fn deserialize_shutdown<'de, D>(deserializer: D) -> Result<std::time::Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize as _;

    let duration = pavex::time::SignedDuration::deserialize(deserializer)?;
    if duration.is_negative() {
        Err(serde::de::Error::custom(
            "graceful shutdown timeout must be positive",
        ))
    } else {
        duration.try_into().map_err(serde::de::Error::custom)
    }
}

impl ServerConfig {
    /// Bind a TCP listener according to the specified parameters.
    pub async fn listener(&self) -> Result<IncomingStream, std::io::Error> {
        let addr = std::net::SocketAddr::new(self.ip, self.port);
        IncomingStream::bind(addr).await
    }
}

#[derive(serde::Deserialize, Debug, Clone)]
#[config(key = "database")]
pub struct DatabaseConfig {
    /// Set via `PX_DATABASE__USERNAME`.
    pub username: String,
    /// Set via `PX_DATABASE__PASSWORD`.
    pub password: Secret<String>,
    /// Set via `PX_DATABASE__PORT`.
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub port: u16,
    /// Set via `PX_DATABASE__HOST`.
    pub host: String,
    /// Set via `PX_DATABASE__DATABASE_NAME`.
    pub database_name: String,
    /// Set via `PX_DATABASE__REQUIRE_SSL`.
    pub require_ssl: bool,
}

#[pavex::methods]
impl DatabaseConfig {
    /// Return the database connection options.
    pub fn connection_options(&self) -> PgConnectOptions {
        let ssl_mode = if self.require_ssl {
            PgSslMode::Require
        } else {
            PgSslMode::Prefer
        };
        PgConnectOptions::new()
            .host(&self.host)
            .username(&self.username)
            .password(self.password.expose_secret())
            .port(self.port)
            .ssl_mode(ssl_mode)
            .database(&self.database_name)
    }

    /// Return a database connection pool, running pending migrations first.
    #[pavex::singleton(clone_if_necessary)]
    pub async fn get_pool(&self) -> Result<sqlx::PgPool, sqlx::Error> {
        let pool = sqlx::PgPool::connect_with(self.connection_options()).await?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| sqlx::Error::Migrate(Box::new(e)))?;
        Ok(pool)
    }

    /// Return a session store backed by Postgres, running its migration first.
    #[pavex::singleton]
    pub async fn session_store(pool: &sqlx::PgPool) -> Result<SessionStore, sqlx::Error> {
        let backend = PostgresSessionStore::new(pool.clone());
        backend.migrate().await?;
        Ok(SessionStore::new(backend))
    }
}
