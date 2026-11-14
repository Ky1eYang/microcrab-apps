//! OAuth2 code+PKCE exchange with pinned, bounded RS256 OIDC verification.
use super::jwt::{self, JwkSet};
use super::{Auth, AuthError, MAX_TOKEN_BYTES, TokenAuthMethod, now, same_secret};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use jwt_simple::prelude::{RS256PublicKey, RSAPublicKeyLike};
use oauth2::{
    AuthType, AuthUrl, ClientId, ClientSecret, EndpointNotSet, EndpointSet, RedirectUrl,
    StandardRevocableToken, StandardTokenResponse, TokenResponse, TokenUrl,
    basic::{
        BasicErrorResponse, BasicRevocationErrorResponse, BasicTokenIntrospectionResponse,
        BasicTokenType,
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};

#[derive(Serialize, Deserialize)]
pub(super) struct OidcTokenFields {
    pub id_token: String,
}
impl std::fmt::Debug for OidcTokenFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OidcTokenFields { id_token: [REDACTED] }")
    }
}
impl oauth2::ExtraTokenFields for OidcTokenFields {}

/// Verified IdP identity. `id_token` is retained server-side only for logout.
pub(super) struct Identity {
    pub subject: String,
    pub display_name: String,
    pub id_token: String,
}
#[derive(Deserialize)]
struct IdClaims {
    iss: String,
    sub: String,
    aud: serde_json::Value,
    exp: u64,
    iat: u64,
    nonce: String,
    #[serde(default)]
    azp: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    preferred_username: Option<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    at_hash: Option<String>,
}
impl Auth {
    #[expect(
        clippy::type_complexity,
        reason = "The OAuth SDK encodes configured endpoints and OIDC token fields in its client typestate; keep that foreign contract explicit"
    )]
    pub(super) fn oauth_client(
        &self,
    ) -> Result<
        oauth2::Client<
            BasicErrorResponse,
            StandardTokenResponse<OidcTokenFields, BasicTokenType>,
            BasicTokenIntrospectionResponse,
            StandardRevocableToken,
            BasicRevocationErrorResponse,
            EndpointSet,
            EndpointNotSet,
            EndpointNotSet,
            EndpointNotSet,
            EndpointSet,
        >,
        AuthError,
    > {
        let config = &self.inner.config;
        let endpoints = &self.inner.endpoints;
        let mut client = oauth2::Client::new(ClientId::new(config.client_id.clone()))
            .set_auth_uri(
                AuthUrl::new(endpoints.authorization.clone())
                    .map_err(|_| AuthError::Configuration)?,
            )
            .set_token_uri(
                TokenUrl::new(endpoints.token.clone()).map_err(|_| AuthError::Configuration)?,
            )
            .set_redirect_uri(
                RedirectUrl::new(config.redirect_uri.clone())
                    .map_err(|_| AuthError::Configuration)?,
            )
            .set_auth_type(match config.token_endpoint_auth_method {
                TokenAuthMethod::ClientSecretPost => AuthType::RequestBody,
                TokenAuthMethod::ClientSecretBasic => AuthType::BasicAuth,
            });
        if let Some(secret) = &config.client_secret {
            client = client.set_client_secret(ClientSecret::new(secret.clone()));
        }
        Ok(client)
    }
    pub(super) async fn exchange(
        &self,
        code: String,
        verifier: String,
        nonce: &str,
    ) -> Result<Identity, AuthError> {
        let client = self.oauth_client()?;
        let http = self.inner.http.clone();
        // OAuth2's SDK handles request/response semantics. The custom transport
        // bounds the body before parsing and never follows provider redirects.
        let transport = move |request: oauth2::HttpRequest| {
            let http = http.clone();
            async move {
                let (parts, body) = request.into_parts();
                let response = http
                    .request(parts.method, parts.uri.to_string())
                    .headers(parts.headers)
                    .body(body)
                    .send()
                    .await
                    .map_err(|_| {
                        AuthError::UnavailableReason(
                            "identity provider token endpoint connection failed",
                        )
                    })?;
                let status = response.status();
                let headers = response.headers().clone();
                let body = bounded(response, 64 * 1024).await?;
                let mut response = axum::http::Response::builder()
                    .status(status)
                    .body(body)
                    .map_err(|_| AuthError::Unavailable)?;
                *response.headers_mut() = headers;
                Ok::<oauth2::HttpResponse, AuthError>(response)
            }
        };
        let token = client
            .exchange_code(oauth2::AuthorizationCode::new(code))
            .set_pkce_verifier(oauth2::PkceCodeVerifier::new(verifier))
            .request_async(&transport)
            .await
            .map_err(|_| AuthError::UnauthorizedReason("identity provider token exchange rejected the authorization code or returned invalid token data"))?;
        self.verify_identity(
            &token.extra_fields().id_token,
            nonce,
            token.access_token().secret(),
        )
        .await
    }
    pub(super) async fn verify_identity(
        &self,
        token: &str,
        nonce: &str,
        access_token: &str,
    ) -> Result<Identity, AuthError> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(AuthError::Unauthorized);
        }
        let header = jwt::header(token)?;
        if header.alg != "RS256" {
            return Err(AuthError::Unauthorized);
        }
        let kid = header.kid.ok_or(AuthError::Unauthorized)?;
        if kid.is_empty() || kid.len() > 128 {
            return Err(AuthError::Unauthorized);
        }
        let key = self.identity_key(&kid).await?;
        let config = &self.inner.config;
        let skew = config.clock_skew_seconds;
        key.verify_token::<serde_json::Value>(
            token,
            Some(jwt::options(
                token,
                &config.issuer,
                &config.client_id,
                skew,
            )?),
        )
        .map_err(|_| AuthError::Unauthorized)?;
        jwt::not_before(token, skew)?;
        let claims: IdClaims = jwt::payload(token)?;
        let current = now()?;
        if claims.iss != config.issuer
            || claims.sub.is_empty()
            || claims.sub.len() > 1024
            || claims.iat > current.saturating_add(skew)
            || claims.exp <= claims.iat
            || claims.exp - claims.iat > config.max_id_token_lifetime_seconds
            || !same_secret(&claims.nonce, nonce)
            || claims
                .azp
                .as_ref()
                .is_some_and(|azp| azp != &config.client_id)
            || claims
                .aud
                .as_array()
                .is_some_and(|audiences| audiences.len() > 1 && claims.azp.is_none())
        {
            return Err(AuthError::Unauthorized);
        }
        if let Some(expected) = claims.at_hash {
            let digest = Sha256::digest(access_token.as_bytes());
            if !same_secret(
                &expected,
                &URL_SAFE_NO_PAD.encode(&digest[..digest.len() / 2]),
            ) {
                return Err(AuthError::Unauthorized);
            }
        }
        // Display only, never authorization: first presentable candidate wins.
        let display_name = [&claims.name, &claims.preferred_username, &claims.email]
            .into_iter()
            .flatten()
            .find(|value| presentable(value))
            .cloned()
            .unwrap_or_else(|| {
                let mut end = claims.sub.len().min(256);
                while !claims.sub.is_char_boundary(end) {
                    end -= 1;
                }
                claims.sub[..end].to_owned()
            });
        if !presentable(&display_name) {
            return Err(AuthError::Unauthorized);
        }
        Ok(Identity {
            subject: claims.sub,
            display_name,
            id_token: token.to_owned(),
        })
    }
    async fn identity_key(&self, kid: &str) -> Result<RS256PublicKey, AuthError> {
        let mut cache = self.inner.jwks.lock().await;
        let current = Instant::now();
        let fresh = cache
            .fetched
            .is_some_and(|fetched| current.duration_since(fetched) < Duration::from_secs(300));
        if fresh {
            if let Some(key) = cache
                .keys
                .iter()
                .find(|key| key.kid.as_deref() == Some(kid))
            {
                return key.decoding_key();
            }
            // One forced unknown-kid refresh is allowed immediately after the
            // initial fetch. Repeated random kids cannot amplify outbound requests.
            if cache
                .forced_refresh
                .is_some_and(|last| current.duration_since(last) < Duration::from_secs(2))
            {
                return Err(AuthError::Unauthorized);
            }
            cache.forced_refresh = Some(current);
        }
        let response = self
            .inner
            .http
            .get(&self.inner.endpoints.jwks)
            .send()
            .await
            .map_err(|_| AuthError::Unavailable)?;
        if !response.status().is_success() {
            return Err(AuthError::Unavailable);
        }
        let max_keys = self.inner.config.jwks_max_keys;
        let bytes = bounded(response, (max_keys * 4096).max(64 * 1024)).await?;
        let keys: JwkSet = serde_json::from_slice(&bytes).map_err(|_| AuthError::Unauthorized)?;
        if keys.keys.is_empty() || keys.keys.len() > max_keys {
            return Err(AuthError::Unauthorized);
        }
        let mut ids = std::collections::HashSet::new();
        for key in &keys.keys {
            let id = key.kid.as_deref().ok_or(AuthError::Unauthorized)?;
            if id.is_empty() || id.len() > 128 || !ids.insert(id) {
                return Err(AuthError::Unauthorized);
            }
        }
        cache.keys = keys.keys;
        cache.fetched = Some(Instant::now());
        let key = cache
            .keys
            .iter()
            .find(|key| key.kid.as_deref() == Some(kid))
            .ok_or(AuthError::Unauthorized)?;
        key.decoding_key()
    }
}
fn presentable(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}
pub(super) async fn bounded(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, AuthError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(AuthError::Unavailable);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| AuthError::Unavailable)? {
        if chunk.len() > limit.saturating_sub(bytes.len()) {
            return Err(AuthError::Unavailable);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
