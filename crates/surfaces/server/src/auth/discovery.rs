//! Startup-only OIDC discovery. The fetched document is bounded, its `issuer`
//! must equal the configured issuer byte-for-byte, and every endpoint passes the
//! same URL policy as configured endpoints. Explicit config fields override it.
use super::{AuthConfig, AuthError, oauth::bounded};
use serde::Deserialize;

/// Resolved provider endpoints; one authority for the rest of the RP.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Endpoints {
    pub authorization: String,
    pub token: String,
    pub jwks: String,
    pub end_session: Option<String>,
    pub require_callback_iss: bool,
}

#[derive(Default, Deserialize)]
pub(super) struct Document {
    pub issuer: String,
    #[serde(default)]
    pub authorization_endpoint: Option<String>,
    #[serde(default)]
    pub token_endpoint: Option<String>,
    #[serde(default)]
    pub jwks_uri: Option<String>,
    #[serde(default)]
    pub end_session_endpoint: Option<String>,
    #[serde(default)]
    pub authorization_response_iss_parameter_supported: bool,
    #[serde(default)]
    pub code_challenge_methods_supported: Option<Vec<String>>,
    #[serde(default)]
    pub id_token_signing_alg_values_supported: Option<Vec<String>>,
}

pub(super) fn discovery_url(issuer: &str) -> String {
    format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    )
}

pub(super) async fn fetch(
    config: &AuthConfig,
    http: &reqwest::Client,
) -> Result<Document, AuthError> {
    let response = http
        .get(discovery_url(&config.issuer))
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|_| AuthError::Unavailable)?;
    if !response.status().is_success() {
        return Err(AuthError::Unavailable);
    }
    let bytes = bounded(response, 64 * 1024).await?;
    serde_json::from_slice(&bytes).map_err(|_| AuthError::Configuration)
}

/// Merge configuration with an optional discovery document.
pub(super) fn resolve(
    config: &AuthConfig,
    document: Option<&Document>,
) -> Result<Endpoints, AuthError> {
    if let Some(document) = document {
        // Mix-up defence: a document for another issuer is never trusted.
        if document.issuer != config.issuer {
            return Err(AuthError::Configuration);
        }
        if document
            .code_challenge_methods_supported
            .as_ref()
            .is_some_and(|methods| !methods.iter().any(|method| method == "S256"))
            || document
                .id_token_signing_alg_values_supported
                .as_ref()
                .is_some_and(|algs| !algs.iter().any(|alg| alg == "RS256"))
        {
            return Err(AuthError::Configuration);
        }
    }
    let pick = |explicit: &Option<String>, discovered: Option<&Option<String>>| {
        explicit
            .clone()
            .or_else(|| discovered.and_then(Clone::clone))
    };
    let authorization = pick(
        &config.authorization_endpoint,
        document.map(|d| &d.authorization_endpoint),
    )
    .ok_or(AuthError::Configuration)?;
    let token = pick(&config.token_endpoint, document.map(|d| &d.token_endpoint))
        .ok_or(AuthError::Configuration)?;
    let jwks =
        pick(&config.jwks_uri, document.map(|d| &d.jwks_uri)).ok_or(AuthError::Configuration)?;
    let end_session = if config.rp_logout {
        pick(
            &config.end_session_endpoint,
            document.map(|d| &d.end_session_endpoint),
        )
    } else {
        None
    };
    for url in [&authorization, &token, &jwks]
        .into_iter()
        .chain(end_session.as_ref())
    {
        config.endpoint_url(url)?;
    }
    let require_callback_iss = config.require_callback_iss.unwrap_or_else(|| {
        document.is_some_and(|d| d.authorization_response_iss_parameter_supported)
    });
    Ok(Endpoints {
        authorization,
        token,
        jwks,
        end_session,
        require_callback_iss,
    })
}
