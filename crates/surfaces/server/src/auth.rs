//! OIDC browser login and short-lived, revocable application cookies.
//! Auth owns identity only; SessionManager still authorizes every session access.
mod config;
#[cfg(feature = "dev")]
pub(crate) mod dev;
mod discovery;
mod jwt;
mod oauth;
mod routes;
mod session;
#[cfg(test)]
mod tests;

pub use config::{AuthConfig, TokenAuthMethod};
pub use routes::router;
pub use session::Principal;

use axum::{
    Json,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::Mutex as AsyncMutex;

const SESSION_COOKIE: &str = "microcrab_session";
const LOGIN_COOKIE: &str = "microcrab_login";
const MAX_PENDING: usize = 256;
const MAX_REVOKED: usize = 4096;
const MAX_TOKEN_BYTES: usize = 16 * 1024;

#[derive(Clone)]
pub struct Auth {
    inner: Arc<Inner>,
}
struct Inner {
    generation: String,
    config: AuthConfig,
    endpoints: discovery::Endpoints,
    http: reqwest::Client,
    /// jti -> (ID Token, expiry). Retained server-side until logout, then sent
    /// through the browser as the standard RP-Initiated Logout `id_token_hint`.
    id_tokens: Mutex<HashMap<String, (String, u64)>>,
    pending: Mutex<HashMap<String, PendingLogin>>,
    revoked: Mutex<HashMap<String, u64>>,
    jwks: AsyncMutex<JwksCache>,
    #[cfg(feature = "dev")]
    provider: Option<dev::Provider>,
}
struct PendingLogin {
    verifier: String,
    nonce: String,
    binding: String,
    expires: u64,
}
#[derive(Default)]
struct JwksCache {
    keys: Vec<jwt::Jwk>,
    fetched: Option<Instant>,
    forced_refresh: Option<Instant>,
}

/// Safe error vocabulary: never attach tokens, authorization URLs or IdP bodies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthError {
    Configuration,
    Unauthorized,
    UnauthorizedReason(&'static str),
    ConfigurationReason(&'static str),
    UnavailableReason(&'static str),
    Forbidden,
    InvalidRequest,
    Unavailable,
}
impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Configuration => "invalid authentication configuration",
            Self::Unauthorized => "authentication failed",
            Self::UnauthorizedReason(reason)
            | Self::ConfigurationReason(reason)
            | Self::UnavailableReason(reason) => reason,
            Self::Forbidden => "origin is not allowed",
            Self::InvalidRequest => "invalid authentication request",
            Self::Unavailable => "authentication service unavailable",
        })
    }
}
impl std::error::Error for AuthError {}
impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, code) = match self {
            Self::Configuration
            | Self::Unavailable
            | Self::ConfigurationReason(_)
            | Self::UnavailableReason(_) => (StatusCode::SERVICE_UNAVAILABLE, "auth_unavailable"),
            Self::Unauthorized | Self::UnauthorizedReason(_) => {
                (StatusCode::UNAUTHORIZED, "unauthenticated")
            }
            Self::Forbidden => (StatusCode::FORBIDDEN, "origin_not_allowed"),
            Self::InvalidRequest => (StatusCode::BAD_REQUEST, "invalid_request"),
        };
        tracing::warn!(code, reason=%self, "Authentication operation failed");
        (
            status,
            [("cache-control", "no-store")],
            Json(json!({"error":{"code":code,"message":self.to_string(),"retryable":false}})),
        )
            .into_response()
    }
}
impl Auth {
    /// Application assembly supplies the actual listener address, including an
    /// ephemeral port. External authentication retains ordinary network discovery.
    pub(crate) async fn for_listener(
        config: AuthConfig,
        address: Option<std::net::SocketAddr>,
    ) -> Result<Self, AuthError> {
        if config.dev_oidc {
            #[cfg(feature = "dev")]
            return dev::Provider::connect(config, address).await;
            #[cfg(not(feature = "dev"))]
            {
                tracing::error!(
                    code = "feature_dev_not_enabled",
                    "Development authentication requires the dev feature"
                );
                return Err(AuthError::Configuration);
            }
        }
        let _ = address;
        Self::connect(config).await
    }

    /// Synchronous construction for fully explicit endpoint configuration.
    /// `discovery: true` requires [`Auth::connect`].
    pub fn new(config: AuthConfig) -> Result<Self, AuthError> {
        if config.dev_oidc {
            return Err(AuthError::Configuration);
        }
        config.validate()?;
        if config.discovery {
            return Err(AuthError::Configuration);
        }
        let endpoints = discovery::resolve(&config, None)?;
        Self::assemble(config, endpoints, http_client()?)
    }
    /// Startup construction: performs bounded OIDC discovery when enabled.
    /// Failure is a startup error; there is no silent fallback to guessed paths.
    pub async fn connect(config: AuthConfig) -> Result<Self, AuthError> {
        if config.dev_oidc {
            return Err(AuthError::Configuration);
        }
        config.validate()?;
        let http = http_client()?;
        let document = if config.discovery {
            Some(discovery::fetch(&config, &http).await?)
        } else {
            None
        };
        let endpoints = discovery::resolve(&config, document.as_ref())?;
        Self::assemble(config, endpoints, http)
    }
    fn assemble(
        config: AuthConfig,
        endpoints: discovery::Endpoints,
        http: reqwest::Client,
    ) -> Result<Self, AuthError> {
        Ok(Self {
            inner: Arc::new(Inner {
                generation: uuid::Uuid::new_v4().to_string(),
                config,
                endpoints,
                http,
                id_tokens: Mutex::new(HashMap::new()),
                pending: Mutex::new(HashMap::new()),
                revoked: Mutex::new(HashMap::new()),
                jwks: AsyncMutex::new(JwksCache::default()),
                #[cfg(feature = "dev")]
                provider: None,
            }),
        })
    }

    /// Browser WebSocket and mutating auth routes require an exact configured Origin.
    /// Missing Origin is deliberately rejected; no implicit native-client bypass.
    pub fn check_origin(&self, headers: &HeaderMap) -> Result<(), AuthError> {
        let mut values = headers.get_all(axum::http::header::ORIGIN).iter();
        let origin = values
            .next()
            .ok_or(AuthError::Forbidden)?
            .to_str()
            .map_err(|_| AuthError::Forbidden)?;
        if values.next().is_some()
            || !self
                .inner
                .config
                .allowed_origins
                .iter()
                .any(|allowed| allowed == origin)
        {
            return Err(AuthError::Forbidden);
        }
        Ok(())
    }
}
fn http_client() -> Result<reqwest::Client, AuthError> {
    microcrab_transport::http::client_builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|_| AuthError::Configuration)
}
fn now() -> Result<u64, AuthError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| AuthError::Unavailable)
}
fn same_secret(left: &str, right: &str) -> bool {
    use sha2::{Digest, Sha256};
    let left = Sha256::digest(left.as_bytes());
    let right = Sha256::digest(right.as_bytes());
    left.iter()
        .zip(right.iter())
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}
pub(crate) fn has_session_cookie(headers: &HeaderMap) -> Result<bool, AuthError> {
    cookie(headers, SESSION_COOKIE).map(|value| value.is_some())
}

fn cookie(headers: &HeaderMap, name: &str) -> Result<Option<String>, AuthError> {
    let mut found = None;
    let mut total = 0usize;
    for value in headers.get_all(axum::http::header::COOKIE) {
        let value = value.to_str().map_err(|_| AuthError::Unauthorized)?;
        total = total
            .checked_add(value.len())
            .ok_or(AuthError::Unauthorized)?;
        if total > 32 * 1024 {
            return Err(AuthError::Unauthorized);
        }
        for part in value.split(';') {
            if let Some((key, value)) = part.trim().split_once('=')
                && key == name
            {
                if found.is_some() || value.len() > MAX_TOKEN_BYTES {
                    return Err(AuthError::Unauthorized);
                }
                found = Some(value.to_owned());
            }
        }
    }
    Ok(found)
}
