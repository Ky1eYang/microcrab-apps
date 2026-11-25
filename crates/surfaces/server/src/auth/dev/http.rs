//! Bounded protocol parsing and no-store responses for the development issuer.
use super::{AuthError, Provider, keys};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Query, State},
    http::{HeaderMap, StatusCode, header},
    middleware,
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;

#[derive(Deserialize)]
pub(super) struct Authorize {
    pub client_id: String,
    pub redirect_uri: String,
    pub response_type: String,
    pub scope: String,
    pub state: String,
    pub nonce: String,
    pub code_challenge: String,
    pub code_challenge_method: String,
    pub login_hint: Option<String>,
}
#[derive(Deserialize)]
pub(super) struct Token {
    pub grant_type: String,
    pub code: String,
    pub redirect_uri: String,
    pub code_verifier: String,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
}
#[derive(Deserialize)]
pub(super) struct Logout {
    pub client_id: Option<String>,
    pub id_token_hint: Option<String>,
    pub post_logout_redirect_uri: Option<String>,
    pub state: Option<String>,
}

pub(super) fn router(provider: Provider) -> Router {
    Router::new()
        .route(
            "/api/dev/oidc/.well-known/openid-configuration",
            get(metadata),
        )
        .route("/api/dev/oidc/authorize", get(authorize))
        .route("/api/dev/oidc/token", post(token))
        .route("/api/dev/oidc/jwks", get(jwks))
        .route("/api/dev/oidc/logout", get(logout))
        .route("/api/dev/oidc/rotate", post(rotate))
        .layer(DefaultBodyLimit::max(16 * 1024))
        .layer(middleware::from_fn(no_store))
        .with_state(provider)
}
async fn no_store(request: axum::extract::Request, next: middleware::Next) -> Response {
    let mut response = if request.uri().to_string().len() > 16 * 1024 {
        AuthError::InvalidRequest.into_response()
    } else {
        next.run(request).await
    };
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "no-store".parse().expect("static header"),
    );
    response.headers_mut().insert(
        header::REFERRER_POLICY,
        "no-referrer".parse().expect("static header"),
    );
    response
}
async fn metadata(State(provider): State<Provider>) -> Json<serde_json::Value> {
    Json(provider.metadata())
}
async fn jwks(State(provider): State<Provider>) -> Result<Json<serde_json::Value>, AuthError> {
    Ok(Json(
        provider
            .0
            .keys
            .lock()
            .map_err(|_| AuthError::Unavailable)?
            .jwks(),
    ))
}
async fn authorize(
    State(provider): State<Provider>,
    query: Query<Authorize>,
) -> Result<Response, AuthError> {
    Ok(Redirect::temporary(&provider.authorize(query.0)?).into_response())
}
async fn logout(
    State(provider): State<Provider>,
    query: Query<Logout>,
) -> Result<Response, AuthError> {
    Ok(match provider.logout(query.0)? {
        Some(target) => Redirect::temporary(&target).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}
async fn token(
    State(provider): State<Provider>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let result = parse_token(&headers, &body).and_then(|form| provider.token(form));
    match result {
        Ok(value) => Json(value).into_response(),
        // OAuth transport requires the OAuth error vocabulary, not the API DTO.
        Err(_) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"invalid_grant"})),
        )
            .into_response(),
    }
}
fn parse_token(headers: &HeaderMap, body: &[u8]) -> Result<Token, AuthError> {
    let mut types = headers.get_all(header::CONTENT_TYPE).iter();
    if types
        .next()
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        != Some("application/x-www-form-urlencoded")
        || types.next().is_some()
        || body.len() > 16 * 1024
    {
        return Err(AuthError::InvalidRequest);
    }
    let mut fields = HashMap::new();
    for (key, value) in url::form_urlencoded::parse(body) {
        if fields.len() >= 12
            || fields
                .insert(key.into_owned(), value.into_owned())
                .is_some()
        {
            return Err(AuthError::InvalidRequest);
        }
    }
    let mut authorizations = headers.get_all(header::AUTHORIZATION).iter();
    if let Some(value) = authorizations.next() {
        if authorizations.next().is_some()
            || fields.contains_key("client_id")
            || fields.contains_key("client_secret")
        {
            return Err(AuthError::InvalidRequest);
        }
        let credentials = value
            .to_str()
            .ok()
            .and_then(|value| value.strip_prefix("Basic "))
            .and_then(|value| STANDARD.decode(value).ok())
            .and_then(|value| String::from_utf8(value).ok())
            .ok_or(AuthError::InvalidRequest)?;
        let (client, secret) = credentials
            .split_once(':')
            .ok_or(AuthError::InvalidRequest)?;
        // RFC 6749 Basic credentials are individually form-encoded.
        let decode = |value: &str| {
            let escaped = value.replace('&', "%26");
            url::form_urlencoded::parse(format!("v={escaped}").as_bytes())
                .next()
                .map(|(_, value)| value.into_owned())
                .ok_or(AuthError::InvalidRequest)
        };
        fields.insert("client_id".into(), decode(client)?);
        fields.insert("client_secret".into(), decode(secret)?);
    }
    serde_json::from_value(json!(fields)).map_err(|_| AuthError::InvalidRequest)
}
async fn rotate(
    State(provider): State<Provider>,
    headers: HeaderMap,
) -> Result<StatusCode, AuthError> {
    // Cross-site forms cannot churn signing keys. Serialize rotation requests
    // with try_lock; expensive generation runs outside async workers and locks.
    let origin = provider
        .0
        .config
        .allowed_origins
        .first()
        .ok_or(AuthError::Configuration)?;
    let mut origins = headers.get_all(header::ORIGIN).iter();
    if origins.next().and_then(|value| value.to_str().ok()) != Some(origin.as_str())
        || origins.next().is_some()
    {
        return Err(AuthError::Forbidden);
    }
    let mut permit = provider
        .0
        .rotation
        .try_lock()
        .map_err(|_| AuthError::Unavailable)?;
    let current = std::time::Instant::now();
    if permit.is_some_and(|last| current.duration_since(last) < std::time::Duration::from_secs(2)) {
        return Err(AuthError::Unavailable);
    }
    *permit = Some(current);
    let replacement = tokio::task::spawn_blocking(keys::KeyRing::replacement)
        .await
        .map_err(|_| AuthError::Unavailable)??;
    provider
        .0
        .keys
        .lock()
        .map_err(|_| AuthError::Unavailable)?
        .install(replacement);
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_forms_reject_duplicates_and_mixed_auth_and_decode_basic_credentials() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/x-www-form-urlencoded".parse().unwrap(),
        );
        let form = b"grant_type=authorization_code&code=once&redirect_uri=http%3A%2F%2F127.0.0.1%2Fcallback&code_verifier=verifier";
        let encoded = STANDARD.encode("client%3Aid:secret%26plus%2Bcolon%3A");
        headers.insert(
            header::AUTHORIZATION,
            format!("Basic {encoded}").parse().unwrap(),
        );
        let parsed = parse_token(&headers, form).unwrap();
        assert_eq!(parsed.client_id.as_deref(), Some("client:id"));
        assert_eq!(parsed.client_secret.as_deref(), Some("secret&plus+colon:"));
        let mut mixed = form.to_vec();
        mixed.extend_from_slice(b"&client_id=client");
        assert!(parse_token(&headers, &mixed).is_err());
        headers.remove(header::AUTHORIZATION);
        let mut duplicate = form.to_vec();
        duplicate.extend_from_slice(b"&code=twice");
        assert!(parse_token(&headers, &duplicate).is_err());
        headers.append(
            header::CONTENT_TYPE,
            "application/x-www-form-urlencoded".parse().unwrap(),
        );
        assert!(parse_token(&headers, form).is_err());
    }
}
