//! This module contains the tools required to validate user-provided passwords
//! and store them securely in the database as PHC-encoded password hashes.
use crate::telemetry::spawn_blocking_with_tracing;
use anyhow::Context;
use argon2::password_hash::phc::PasswordHash;
use argon2::{Algorithm, Argon2, Params, PasswordHasher, PasswordVerifier, Version};
use secrecy::{ExposeSecret, Secret};
use sqlx::postgres::PgPool;

#[derive(thiserror::Error, Debug)]
pub enum AuthError {
    #[error("Invalid credentials")]
    InvalidCredentials(#[source] anyhow::Error),
    #[error("Something went wrong when validating credentials")]
    UnexpectedError(#[source] anyhow::Error),
}

/// Compute the password hash using the Argon2id algorithm.
///
/// The returned hash is in the PHC string format, which includes the salt and
/// the algorithm parameters. 0.6's `hash_password` self-salts via getrandom.
pub fn compute_password_hash(password: Secret<String>) -> Result<Secret<String>, anyhow::Error> {
    let password_hash = Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        Params::new(15000, 2, 1, None).unwrap(),
    )
    .hash_password(password.expose_secret().as_bytes())
    .map_err(|e| anyhow::anyhow!("Failed to hash password: {e}"))?
    .to_string();
    Ok(Secret::new(password_hash))
}

#[tracing::instrument(name = "Validate credentials", skip_all)]
pub async fn validate_credentials(
    email: &str,
    password: Secret<String>,
    pool: &PgPool,
) -> Result<uuid::Uuid, AuthError> {
    let mut user_id = None;
    let mut expected_password_hash = Secret::new(
        "$argon2id$v=19$m=15000,t=2,p=1$\
        gZiV/M1gPc22ElAH/Jh1Hw$\
        CWOrkoo7oJBQ/iyh7uJ0LO2aLEfrHwTWllSAxT0zRno"
            .to_string(),
    );

    if let Some((stored_user_id, stored_password_hash)) = get_stored_credentials(email, pool)
        .await
        .map_err(AuthError::UnexpectedError)?
    {
        user_id = Some(stored_user_id);
        expected_password_hash = stored_password_hash;
    }

    spawn_blocking_with_tracing(move || verify_password_hash(expected_password_hash, password))
        .await
        .context("Failed to spawn blocking task.")
        .map_err(AuthError::UnexpectedError)??;

    user_id
        .ok_or_else(|| anyhow::anyhow!("Unknown email."))
        .map_err(AuthError::InvalidCredentials)
}

#[tracing::instrument(name = "Validate credentials", skip_all)]
fn verify_password_hash(
    expected_password_hash: Secret<String>,
    password_candidate: Secret<String>,
) -> Result<(), AuthError> {
    let expected_password_hash = PasswordHash::new(expected_password_hash.expose_secret())
        .map_err(|e| anyhow::anyhow!("Failed to parse hash in PHC string format: {e}"))
        .map_err(AuthError::UnexpectedError)?;

    Argon2::default()
        .verify_password(
            password_candidate.expose_secret().as_bytes(),
            &expected_password_hash,
        )
        .map_err(|e| anyhow::anyhow!("Invalid password: {e}"))
        .map_err(AuthError::InvalidCredentials)
}

#[tracing::instrument(name = "Get stored credentials", skip_all)]
async fn get_stored_credentials(
    email: &str,
    pool: &PgPool,
) -> Result<Option<(uuid::Uuid, Secret<String>)>, anyhow::Error> {
    let row = sqlx::query!(
        r#"
        SELECT id, password_hash
        FROM users
        WHERE email = $1
        "#,
        email,
    )
    .fetch_optional(pool)
    .await
    .context("Failed to perform a query to retrieve stored credentials.")?;

    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some((row.id, Secret::new(row.password_hash))))
}
