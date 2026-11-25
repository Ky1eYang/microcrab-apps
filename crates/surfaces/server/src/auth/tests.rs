//! Auth unit tests; `sso` covers the AuthPortal-shaped provider contract.
mod sso;

use super::*;
use axum::http::{HeaderValue, header};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use jwt_simple::prelude::{HS384Key, MACLike};
use serde_json::{Value, json};

fn config() -> AuthConfig {
    serde_json::from_value(json!({
        "issuer":"http://127.0.0.1:19091",
        "authorization_endpoint":"http://127.0.0.1:19091/authorize",
        "token_endpoint":"http://127.0.0.1:19091/token",
        "jwks_uri":"http://127.0.0.1:19091/jwks",
        "client_id":"test-client", "redirect_uri":"http://127.0.0.1:19090/api/auth/callback",
        "allowed_origins":["http://127.0.0.1:19090"],
        "jwt_secret":"test-only-application-secret-32-characters-minimum",
        "cookie_secure":false, "allow_http_loopback":true,
    }))
    .unwrap()
}
#[cfg(not(feature = "dev"))]
#[tokio::test]
async fn development_selection_is_rejected_and_auth_router_has_no_mock_endpoints() {
    let mut selected = config();
    selected.dev_oidc = true;
    assert!(matches!(
        Auth::for_listener(selected, Some("127.0.0.1:19090".parse().unwrap())).await,
        Err(AuthError::Configuration)
    ));
    use tower::ServiceExt;
    let response = router(Auth::new(config()).unwrap())
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/dev/oidc/jwks")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
}

#[test]
fn application_session_lifetime_is_bounded_and_explicit_values_remain_authoritative() {
    let default_config = config();
    assert_eq!(default_config.session_ttl_seconds, 12 * 3600);
    for lifetime in [1, 3600, 12 * 3600, 7 * 24 * 3600] {
        let mut selected = default_config.clone();
        selected.session_ttl_seconds = lifetime;
        let auth = Auth::new(selected).unwrap();
        let token = auth.issue("alice".into(), "Alice".into()).unwrap();
        let claims: Value = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(token.split('.').nth(1).unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            claims["exp"].as_u64().unwrap() - claims["iat"].as_u64().unwrap(),
            lifetime
        );
        if lifetime > 1 {
            assert!(auth.authenticate(&headers(&token)).is_ok());
        }
    }
    for lifetime in [0, 7 * 24 * 3600 + 1] {
        let mut invalid = default_config.clone();
        invalid.session_ttl_seconds = lifetime;
        assert!(matches!(Auth::new(invalid), Err(AuthError::Configuration)));
    }
}

fn headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::COOKIE,
        HeaderValue::from_str(&format!("{SESSION_COOKIE}={token}")).unwrap(),
    );
    headers
}
fn result_is_unauthorized<T>(result: Result<T, AuthError>) {
    assert!(matches!(result, Err(AuthError::Unauthorized)));
}
#[test]
fn identity_is_issuer_subject_not_display_name_and_cookies_reject_tampering() {
    let auth = Auth::new(config()).unwrap();
    let alice = auth.issue("alice".into(), "Common Name".into()).unwrap();
    let bob = auth.issue("bob".into(), "Common Name".into()).unwrap();
    let principal = auth.authenticate(&headers(&alice)).unwrap();
    assert_ne!(
        principal.owner,
        auth.authenticate(&headers(&bob)).unwrap().owner
    );
    let renamed = auth.issue("alice".into(), "Changed Name".into()).unwrap();
    assert_eq!(
        principal.owner,
        auth.authenticate(&headers(&renamed)).unwrap().owner
    );
    let mut mutated = alice.into_bytes();
    let at = mutated.len() - 8;
    mutated[at] = if mutated[at] == b'A' { b'B' } else { b'A' };
    result_is_unauthorized(auth.authenticate(&headers(&String::from_utf8(mutated).unwrap())));
    let output = serde_json::to_value(&principal).unwrap();
    assert!(output.get("jti").is_none());
    assert!(output.get("generation").is_none());
}
#[test]
fn signed_but_invalid_claims_and_algorithms_are_not_accepted() {
    let config = config();
    let auth = Auth::new(config.clone()).unwrap();
    let valid = auth.issue("alice".into(), "Alice".into()).unwrap();
    let body: Value = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(valid.split('.').nth(1).unwrap())
            .unwrap(),
    )
    .unwrap();
    let invalid = [
        ("iss", json!("other-application")),
        ("aud", json!("other-resource")),
        ("provider_issuer", json!("http://untrusted.invalid")),
        ("owner", json!("another-user")),
        ("generation", json!("previous-process")),
        ("exp", json!(now().unwrap().saturating_sub(1))),
        ("nbf", json!(now().unwrap() + 300)),
        ("iat", json!(now().unwrap() + 300)),
    ];
    for (field, value) in invalid {
        let mut claims = body.clone();
        claims[field] = value;
        let token = jwt::sign_hs256(&claims, config.jwt_secret.as_bytes()).unwrap();
        result_is_unauthorized(auth.authenticate(&headers(&token)));
    }
    for field in ["exp", "nbf", "iat", "iss", "aud", "sub"] {
        let mut missing = body.clone();
        missing.as_object_mut().unwrap().remove(field);
        let token = jwt::sign_hs256(&missing, config.jwt_secret.as_bytes()).unwrap();
        result_is_unauthorized(auth.authenticate(&headers(&token)));
    }
    let wrong_algorithm = HS384Key::from_bytes(config.jwt_secret.as_bytes())
        .authenticate(jwt::claims(&body).unwrap())
        .unwrap();
    result_is_unauthorized(auth.authenticate(&headers(&wrong_algorithm)));
    let wrong_key = jwt::sign_hs256(&body, b"a-distinct-test-only-signing-secret-32bytes").unwrap();
    result_is_unauthorized(auth.authenticate(&headers(&wrong_key)));
}
#[test]
fn logout_revocation_affects_existing_principals_and_restart_requires_login() {
    let config = config();
    let auth = Auth::new(config.clone()).unwrap();
    let token = auth.issue("alice".into(), "Alice".into()).unwrap();
    let principal = auth.authenticate(&headers(&token)).unwrap();
    let other = auth.issue("bob".into(), "Bob".into()).unwrap();
    auth.revoke(&principal).unwrap();
    result_is_unauthorized(auth.authenticate(&headers(&token)));
    result_is_unauthorized(auth.check_active(&principal));
    assert!(auth.authenticate(&headers(&other)).is_ok());
    let restarted = Auth::new(config).unwrap();
    result_is_unauthorized(restarted.authenticate(&headers(&token)));
    result_is_unauthorized(restarted.authenticate(&headers(&other)));
    let mut expired = principal;
    expired.expires_at = 0;
    result_is_unauthorized(auth.check_active(&expired));
}
#[test]
fn exact_origin_and_unambiguous_cookie_are_required() {
    let auth = Auth::new(config()).unwrap();
    let mut request = HeaderMap::new();
    assert_eq!(auth.check_origin(&request), Err(AuthError::Forbidden));
    for origin in [
        "null",
        "http://127.0.0.1:19090.evil.invalid",
        "http://127.0.0.1:19091",
        "https://127.0.0.1:19090",
    ] {
        request.insert(header::ORIGIN, HeaderValue::from_str(origin).unwrap());
        assert_eq!(auth.check_origin(&request), Err(AuthError::Forbidden));
    }
    request.insert(
        header::ORIGIN,
        HeaderValue::from_static("http://127.0.0.1:19090"),
    );
    assert!(auth.check_origin(&request).is_ok());
    request.append(
        header::ORIGIN,
        HeaderValue::from_static("http://127.0.0.1:19090"),
    );
    assert_eq!(auth.check_origin(&request), Err(AuthError::Forbidden));
    let token = auth.issue("alice".into(), "Alice".into()).unwrap();
    let mut duplicate = headers(&token);
    duplicate.append(
        header::COOKIE,
        HeaderValue::from_str(&format!("{SESSION_COOKIE}={token}")).unwrap(),
    );
    result_is_unauthorized(auth.authenticate(&duplicate));
}
#[test]
fn insecure_nonlocal_endpoints_and_redirects_fail_at_startup() {
    let mut invalid = config();
    invalid.issuer = "http://issuer.example".into();
    assert!(matches!(Auth::new(invalid), Err(AuthError::Configuration)));
    for redirect in [
        "//evil.invalid",
        "https://evil.invalid",
        "/\\evil.invalid",
        "/\r\nLocation: x",
    ] {
        let mut invalid = config();
        invalid.post_login_redirect = redirect.into();
        assert!(matches!(Auth::new(invalid), Err(AuthError::Configuration)));
    }
    let mut invalid = config();
    invalid.allowed_origins = vec!["http://127.0.0.1:19090/".into()];
    assert!(matches!(Auth::new(invalid), Err(AuthError::Configuration)));
    let mut invalid = config();
    invalid.jwt_secret = "short".into();
    assert!(matches!(
        Auth::new(invalid),
        Err(AuthError::ConfigurationReason(
            "authentication jwt_secret must contain at least 32 bytes"
        ))
    ));
}
#[tokio::test]
async fn login_issues_pkce_nonce_and_browser_binding_but_wrong_binding_never_exchanges() {
    let auth = Auth::new(config()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = router(auth.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = microcrab_transport::http::client_builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let first = client
        .get(format!("http://{address}/api/auth/login?login_hint=bob"))
        .send()
        .await
        .unwrap();
    assert!(first.status().is_redirection());
    assert_eq!(first.headers()[header::CACHE_CONTROL], "no-store");
    let target = url::Url::parse(first.headers()[header::LOCATION].to_str().unwrap()).unwrap();
    let query: std::collections::HashMap<_, _> = target.query_pairs().into_owned().collect();
    assert_eq!(query["code_challenge_method"], "S256");
    assert_eq!(query["login_hint"], "bob");
    assert_eq!(query["scope"], "openid profile");
    assert!(!query["nonce"].is_empty());
    let state = &query["state"];
    let cookie = first.headers()[header::SET_COOKIE].to_str().unwrap();
    assert!(cookie.contains("HttpOnly; SameSite=Lax"));
    {
        let pending = auth.inner.pending.lock().unwrap();
        assert_eq!(pending.len(), 1);
        assert!(!pending[state].verifier.is_empty());
    }
    let callback = format!("http://{address}/api/auth/callback?code=test-only&state={state}");
    let rejected = client.get(&callback).send().await.unwrap();
    assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
    let wrong = client
        .get(&callback)
        .header(header::COOKIE, format!("{LOGIN_COOKIE}=unrelated-browser"))
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(auth.inner.pending.lock().unwrap().len(), 1);
    let second = client
        .get(format!("http://{address}/api/auth/login"))
        .send()
        .await
        .unwrap();
    assert_ne!(
        first.headers()[header::LOCATION],
        second.headers()[header::LOCATION]
    );
    assert_eq!(auth.inner.pending.lock().unwrap().len(), 2);
    server.abort();
    let _ = server.await;
}
#[tokio::test]
async fn logout_requires_origin_and_invalidates_the_session_probe() {
    let auth = Auth::new(config()).unwrap();
    let token = auth.issue("alice".into(), "Alice".into()).unwrap();
    let principal = auth.authenticate(&headers(&token)).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = router(auth.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = microcrab_transport::http::client_builder().build().unwrap();
    let cookie = format!("{SESSION_COOKIE}={token}");
    let url = format!("http://{address}/api/auth/logout");
    let rejected = client
        .post(&url)
        .header(header::COOKIE, &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::FORBIDDEN);
    assert!(auth.check_active(&principal).is_ok());
    let revoked = client
        .post(&url)
        .header(header::COOKIE, &cookie)
        .header(header::ORIGIN, "http://127.0.0.1:19090")
        .send()
        .await
        .unwrap();
    assert_eq!(revoked.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        revoked.headers().get_all(header::SET_COOKIE).iter().count(),
        2
    );
    result_is_unauthorized(auth.check_active(&principal));
    let probe = client
        .get(format!("http://{address}/api/auth/session"))
        .header(header::COOKIE, &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(probe.status(), StatusCode::UNAUTHORIZED);
    server.abort();
    let _ = server.await;
}
