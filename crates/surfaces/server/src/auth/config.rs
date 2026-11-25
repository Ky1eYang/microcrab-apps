//! Startup-only identity configuration. Debug intentionally does not expose secrets.
use super::AuthError;
use serde::Deserialize;
use url::Url;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    /// Opt into same-listener development identity (requires the `dev` feature).
    /// Ordinary external authentication remains the default in every build.
    #[serde(default)]
    pub dev_oidc: bool,
    pub issuer: String,
    /// Fetch `{issuer}/.well-known/openid-configuration` at startup. Explicit
    /// endpoint fields below still override discovered values.
    #[serde(default)]
    pub discovery: bool,
    #[serde(default)]
    pub authorization_endpoint: Option<String>,
    #[serde(default)]
    pub token_endpoint: Option<String>,
    #[serde(default)]
    pub jwks_uri: Option<String>,
    /// RP-Initiated Logout endpoint (AuthPortal: `{origin}/api/logout`).
    #[serde(default)]
    pub end_session_endpoint: Option<String>,
    /// Absolute URL registered with the IdP that it returns to after logout.
    /// Its origin must exactly match one of `allowed_origins`.
    #[serde(default)]
    pub post_logout_redirect_uri: Option<String>,
    /// Set false to keep logout local even when the IdP advertises end_session.
    #[serde(default = "secure")]
    pub rp_logout: bool,
    /// RFC 9207: require the callback `iss` parameter. Defaults to the discovered
    /// `authorization_response_iss_parameter_supported`, else false. A present
    /// `iss` parameter is always compared with `issuer`.
    #[serde(default)]
    pub require_callback_iss: Option<bool>,
    pub client_id: String,
    #[serde(default)]
    pub client_secret: Option<String>,
    #[serde(default)]
    pub token_endpoint_auth_method: TokenAuthMethod,
    #[serde(default = "scopes")]
    pub scopes: Vec<String>,
    /// Accepted clock difference for ID Token exp/nbf/iat (seconds, ≤ 300).
    #[serde(default = "skew")]
    pub clock_skew_seconds: u64,
    /// Upper bound on ID Token `exp - iat`. Defaults to 7 days so AuthPortal's
    /// default 168h tokens verify; the ID Token is consumed once at callback.
    #[serde(default = "id_lifetime")]
    pub max_id_token_lifetime_seconds: u64,
    /// Maximum keys accepted from jwks_uri (prefer an application JWKS).
    #[serde(default = "jwks_keys")]
    pub jwks_max_keys: usize,
    pub redirect_uri: String,
    #[serde(default = "redirect")]
    pub post_login_redirect: String,
    pub allowed_origins: Vec<String>,
    pub jwt_secret: String,
    #[serde(default = "jwt_issuer")]
    pub jwt_issuer: String,
    #[serde(default = "jwt_audience")]
    pub jwt_audience: String,
    #[serde(default = "secure")]
    pub cookie_secure: bool,
    /// Fixed application-session lifetime, independent of callback ID-token
    /// expiry. Defaults to 12 hours; explicit deployments may choose up to 7 days.
    #[serde(default = "lifetime")]
    pub session_ttl_seconds: u64,
    #[serde(default)]
    pub allow_http_loopback: bool,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenAuthMethod {
    #[default]
    ClientSecretPost,
    ClientSecretBasic,
}
pub(super) const MAX_CLOCK_SKEW: u64 = 300;
pub(super) const MAX_ID_LIFETIME: u64 = 30 * 24 * 3600;
fn scopes() -> Vec<String> {
    vec!["openid".into(), "profile".into()]
}
fn skew() -> u64 {
    60
}
fn id_lifetime() -> u64 {
    7 * 24 * 3600
}
fn jwks_keys() -> usize {
    16
}
fn redirect() -> String {
    "/".into()
}
fn jwt_issuer() -> String {
    "microcrab".into()
}
fn jwt_audience() -> String {
    "microcrab-web".into()
}
fn secure() -> bool {
    true
}
fn lifetime() -> u64 {
    12 * 3600
}
fn loopback(url: &Url) -> bool {
    match url.host_str() {
        Some("localhost" | "127.0.0.1" | "[::1]" | "::1") => true,
        Some(host) => host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback()),
        None => false,
    }
}
impl AuthConfig {
    pub(super) fn validate(&self) -> Result<(), AuthError> {
        if self.client_id.is_empty() {
            return Err(AuthError::ConfigurationReason(
                "authentication client_id must not be empty",
            ));
        }
        if self.jwt_secret.len() < 32 {
            return Err(AuthError::ConfigurationReason(
                "authentication jwt_secret must contain at least 32 bytes",
            ));
        }
        if self.allowed_origins.is_empty() {
            return Err(AuthError::ConfigurationReason(
                "authentication allowed_origins must not be empty",
            ));
        }
        if self.client_id.is_empty()
            || self.client_id.len() > 256
            || self
                .client_secret
                .as_ref()
                .is_some_and(|secret| secret.len() > 4096)
            || self.jwt_secret.len() < 32
            || self.jwt_secret.len() > 4096
            || self.jwt_issuer.is_empty()
            || self.jwt_issuer.len() > 256
            || self.jwt_audience.is_empty()
            || self.jwt_audience.len() > 256
            || self.session_ttl_seconds == 0
            || self.session_ttl_seconds > 7 * 24 * 3600
            || self.allowed_origins.is_empty()
            || self.allowed_origins.len() > 16
            || !self.post_login_redirect.starts_with('/')
            || self.post_login_redirect.starts_with("//")
            || self.post_login_redirect.contains('\\')
            || self.post_login_redirect.chars().any(char::is_control)
            || self.post_login_redirect.len() > 2048
            || self.clock_skew_seconds > MAX_CLOCK_SKEW
            || self.max_id_token_lifetime_seconds < 60
            || self.max_id_token_lifetime_seconds > MAX_ID_LIFETIME
            || self.jwks_max_keys == 0
            || self.jwks_max_keys > 64
            || self.scopes.len() > 16
            || !self.scopes.iter().any(|scope| scope == "openid")
            || self.scopes.iter().any(|scope| {
                scope.is_empty()
                    || scope.len() > 64
                    || !scope
                        .bytes()
                        .all(|byte| byte.is_ascii_graphic() && byte != b'"' && byte != b'\\')
            })
            || (!self.discovery
                && (self.authorization_endpoint.is_none()
                    || self.token_endpoint.is_none()
                    || self.jwks_uri.is_none()))
        {
            return Err(AuthError::Configuration);
        }
        let urls = [
            Some(&self.issuer),
            self.authorization_endpoint.as_ref(),
            self.token_endpoint.as_ref(),
            self.jwks_uri.as_ref(),
            self.end_session_endpoint.as_ref(),
            self.post_logout_redirect_uri.as_ref(),
            Some(&self.redirect_uri),
        ];
        for value in urls.into_iter().flatten() {
            self.endpoint_url(value)?;
        }
        if self.jwt_issuer == self.issuer || self.jwt_audience == self.client_id {
            return Err(AuthError::Configuration);
        }
        for origin in &self.allowed_origins {
            let url = self.valid_url(origin)?;
            if url.path() != "/"
                || url.origin().ascii_serialization() != *origin
                || (!self.cookie_secure && !loopback(&url))
            {
                return Err(AuthError::Configuration);
            }
        }
        if let Some(target) = &self.post_logout_redirect_uri {
            let origin = self.endpoint_url(target)?.origin().ascii_serialization();
            if !self.allowed_origins.contains(&origin) {
                return Err(AuthError::Configuration);
            }
        }
        Ok(())
    }
    /// Endpoint rule shared by configured and discovered URLs.
    pub(super) fn endpoint_url(&self, value: &str) -> Result<Url, AuthError> {
        let url = self.valid_url(value)?;
        if !self.cookie_secure && (!self.allow_http_loopback || !loopback(&url)) {
            return Err(AuthError::Configuration);
        }
        Ok(url)
    }
    fn valid_url(&self, value: &str) -> Result<Url, AuthError> {
        if value.len() > 2048 {
            return Err(AuthError::Configuration);
        }
        let url = Url::parse(value).map_err(|_| AuthError::Configuration)?;
        if !url.username().is_empty()
            || url.password().is_some()
            || url.host_str().is_none()
            || url.query().is_some()
            || url.fragment().is_some()
            || (url.scheme() != "https"
                && !(self.allow_http_loopback && url.scheme() == "http" && loopback(&url)))
        {
            return Err(AuthError::Configuration);
        }
        Ok(url)
    }
}
