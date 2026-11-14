//! Development-only issuer, sharing the RP's listener and metadata validation.
//! Local subject labels are operator-selected identities, not credentials. State
//! is process-local: a bounded grant map and at most two fresh RSA signing keys.
mod http;
mod keys;
#[cfg(test)]
mod tests;

use super::{Auth, AuthConfig, AuthError, discovery, now, same_secret};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const PREFIX: &str = "/api/dev/oidc";
const MAX_GRANTS: usize = 256;
const TOKEN_TTL: u64 = 120;

#[derive(Clone)]
pub(crate) struct Provider(Arc<Inner>);
struct Inner {
    config: AuthConfig,
    grants: Mutex<HashMap<String, Grant>>,
    keys: Mutex<keys::KeyRing>,
    rotation: tokio::sync::Mutex<Option<Instant>>,
}
struct Grant {
    subject: String,
    challenge: String,
    nonce: String,
    expires: Instant,
}

impl Provider {
    pub(super) async fn connect(
        mut config: AuthConfig,
        address: Option<SocketAddr>,
    ) -> Result<Auth, AuthError> {
        // Only an actual numeric loopback listener can expose this mock identity
        // source. No Host/Forwarded header is an authority for provider URLs.
        let configured =
            url::Url::parse(&config.redirect_uri).map_err(|_| AuthError::Configuration)?;
        let ip = configured
            .host_str()
            .and_then(|host| {
                host.trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .ok()
            })
            .ok_or(AuthError::Configuration)?;
        let address = address.ok_or(AuthError::Configuration)?;
        if !address.ip().is_loopback()
            || address.port() == 0
            || !ip.is_loopback()
            || configured.scheme() != "http"
            || !configured.username().is_empty()
            || configured.password().is_some()
            || configured.query().is_some()
            || configured.fragment().is_some()
            || configured.path() != "/api/auth/callback"
            || config.cookie_secure
            || !config.allow_http_loopback
        {
            eprintln!("DEV_AUTH_FAIL at {}:{}", file!(), line!());
            return Err(AuthError::Configuration);
        }
        let origin = format!("http://{address}");
        config.issuer = format!("{origin}{PREFIX}");
        config.redirect_uri = format!("{origin}/api/auth/callback");
        config.allowed_origins = vec![origin.clone()];
        config.post_logout_redirect_uri = Some(format!("{origin}/login"));
        config.discovery = true;
        config.authorization_endpoint = None;
        config.token_endpoint = None;
        config.jwks_uri = None;
        config.end_session_endpoint = None;
        config.require_callback_iss = Some(true);
        if let Err(e) = config.validate() {
            eprintln!("CONFIG_VALIDATE_ERR: {e:?}");
            return Err(e);
        }
        let ring = tokio::task::spawn_blocking(keys::KeyRing::new)
            .await
            .map_err(|_| AuthError::Unavailable)??;
        let provider = Self(Arc::new(Inner {
            config: config.clone(),
            grants: Mutex::new(HashMap::new()),
            keys: Mutex::new(ring),
            rotation: tokio::sync::Mutex::new(None),
        }));
        // Bootstrap passes the exact served document through the external RP's
        // resolver. Never perform self-discovery before axum starts serving.
        let document: discovery::Document =
            serde_json::from_value(provider.metadata()).map_err(|_| AuthError::Configuration)?;
        let endpoints = match discovery::resolve(&config, Some(&document)) {
            Ok(ep) => ep,
            Err(e) => {
                eprintln!("DISCOVERY_RESOLVE_ERR: {e:?}");
                return Err(e);
            }
        };
        let mut auth = Auth::assemble(config, endpoints, super::http_client()?)?;
        Arc::get_mut(&mut auth.inner)
            .ok_or(AuthError::Unavailable)?
            .provider = Some(provider);
        Ok(auth)
    }

    fn metadata(&self) -> Value {
        let issuer = &self.0.config.issuer;
        json!({"issuer":issuer, "authorization_endpoint":format!("{issuer}/authorize"),
            "token_endpoint":format!("{issuer}/token"), "jwks_uri":format!("{issuer}/jwks"),
            "end_session_endpoint":format!("{issuer}/logout"),
            "authorization_response_iss_parameter_supported":true,
            "response_types_supported":["code"], "grant_types_supported":["authorization_code"],
            "subject_types_supported":["public"], "id_token_signing_alg_values_supported":["RS256"],
            "code_challenge_methods_supported":["S256"],
            "token_endpoint_auth_methods_supported":["none","client_secret_post","client_secret_basic"],
            "scopes_supported":["openid","profile"]})
    }

    fn authorize(&self, query: http::Authorize) -> Result<String, AuthError> {
        let config = &self.0.config;
        if query.client_id != config.client_id
            || query.redirect_uri != config.redirect_uri
            || query.response_type != "code"
            || query.code_challenge_method != "S256"
            || !query
                .scope
                .split_ascii_whitespace()
                .any(|scope| scope == "openid")
            || !bounded_label(&query.nonce, 512)
            || !bounded_label(&query.state, 512)
            || query.code_challenge.len() != 43
            || !query
                .code_challenge
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
        {
            return Err(AuthError::InvalidRequest);
        }
        let subject = query.login_hint.unwrap_or_else(|| "alice".into());
        if subject.is_empty()
            || subject.len() > 128
            || !subject
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._@:+-".contains(&byte))
        {
            return Err(AuthError::InvalidRequest);
        }
        let code = random_secret();
        let current = Instant::now();
        let mut grants = self.0.grants.lock().map_err(|_| AuthError::Unavailable)?;
        grants.retain(|_, grant| grant.expires > current);
        if grants.len() >= MAX_GRANTS {
            return Err(AuthError::Unavailable);
        }
        grants.insert(
            code.clone(),
            Grant {
                subject,
                challenge: query.code_challenge,
                nonce: query.nonce,
                expires: current + Duration::from_secs(60),
            },
        );
        let mut target =
            url::Url::parse(&config.redirect_uri).map_err(|_| AuthError::Configuration)?;
        target
            .query_pairs_mut()
            .append_pair("code", &code)
            .append_pair("state", &query.state)
            .append_pair("iss", &config.issuer);
        Ok(target.into())
    }

    fn token(&self, form: http::Token) -> Result<Value, AuthError> {
        // Remove before checking bindings: an attempted exchange cannot reuse a
        // code after a bad verifier/client secret, including concurrent requests.
        let grant = self
            .0
            .grants
            .lock()
            .map_err(|_| AuthError::Unavailable)?
            .remove(&form.code)
            .ok_or(AuthError::Unauthorized)?;
        let config = &self.0.config;
        if grant.expires <= Instant::now()
            || form.grant_type != "authorization_code"
            || form.client_id.as_deref() != Some(config.client_id.as_str())
            || form.redirect_uri != config.redirect_uri
            || match (&config.client_secret, &form.client_secret) {
                (Some(expected), Some(actual)) => !same_secret(expected, actual),
                (None, None) => false,
                _ => true,
            }
            || !(43..=128).contains(&form.code_verifier.len())
            || !form
                .code_verifier
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-._~".contains(&byte))
            || !same_secret(
                &grant.challenge,
                &URL_SAFE_NO_PAD.encode(Sha256::digest(form.code_verifier.as_bytes())),
            )
        {
            return Err(AuthError::Unauthorized);
        }
        let current = now()?;
        let mut claims = json!({"iss":config.issuer,"sub":grant.subject,"iat":current,"nbf":current,
            "exp":current + TOKEN_TTL,"aud":"microcrab-resource","scope":"openid profile","token_use":"access"});
        let ring = self.0.keys.lock().map_err(|_| AuthError::Unavailable)?;
        let access = ring.sign(&claims)?;
        claims["aud"] = json!(config.client_id);
        claims["azp"] = json!(config.client_id);
        claims["nonce"] = json!(grant.nonce);
        claims["name"] = claims["sub"].clone();
        claims["preferred_username"] = claims["sub"].clone();
        claims["token_use"] = json!("id");
        claims["at_hash"] = json!(URL_SAFE_NO_PAD.encode(&Sha256::digest(access.as_bytes())[..16]));
        let id_token = ring.sign(&claims)?;
        Ok(
            json!({"access_token":access,"id_token":id_token,"token_type":"Bearer","expires_in":TOKEN_TTL,"scope":"openid profile"}),
        )
    }

    fn logout(&self, query: http::Logout) -> Result<Option<String>, AuthError> {
        let config = &self.0.config;
        if query
            .client_id
            .as_ref()
            .is_some_and(|id| id != &config.client_id)
        {
            return Err(AuthError::InvalidRequest);
        }
        if let Some(hint) = query.id_token_hint {
            if hint.len() > super::MAX_TOKEN_BYTES {
                return Err(AuthError::InvalidRequest);
            }
            self.0
                .keys
                .lock()
                .map_err(|_| AuthError::Unavailable)?
                .verify_logout(&hint, config)?;
        }
        let Some(target) = query.post_logout_redirect_uri else {
            return Ok(None);
        };
        if Some(&target) != config.post_logout_redirect_uri.as_ref() {
            return Err(AuthError::InvalidRequest);
        }
        let mut target = url::Url::parse(&target).map_err(|_| AuthError::Configuration)?;
        if let Some(state) = query.state {
            if !bounded_label(&state, 512) {
                return Err(AuthError::InvalidRequest);
            }
            target.query_pairs_mut().append_pair("state", &state);
        }
        Ok(Some(target.into()))
    }
}

fn bounded_label(value: &str, limit: usize) -> bool {
    !value.is_empty() && value.len() <= limit && !value.chars().any(char::is_control)
}
fn random_secret() -> String {
    // Two OS-random UUID v4 values provide 244 random bits without adding a
    // second random-number authority to the workspace.
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

pub(crate) fn router(auth: Auth) -> axum::Router {
    match auth.inner.provider.clone() {
        Some(provider) => http::router(provider),
        None => axum::Router::new(),
    }
}
