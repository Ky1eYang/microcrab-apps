//! RP contract against an in-process OIDC provider shaped like AuthPortal 1.0.2:
//! discovery, RFC 9207 callback iss, long-lived ID Tokens, clock skew and
//! RP-Initiated Logout. The RSA key is a throwaway test fixture.
use super::super::*;
use axum::{
    Form, Json as AxumJson, Router as AxumRouter,
    extract::State as AxumState,
    http::{HeaderMap, StatusCode, header},
    routing::{get, post},
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use jwt_simple::prelude::{RS256KeyPair, RSAKeyPairLike};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashMap as Map, sync::Arc as StdArc};

include!("idp_test_key.rs");
const TEST_N: &str = "5h7_LD4zst1fY7QnLWiFjGZMXWV3n7LBz2N1BMvef-StOUrltaVOr0sEWfar6HRhCYusawPi3HPphLrVkosUen30aXEsk-eia74cyz1beJ666HK-oPAJu2icH87GuqSbjIoLdEBBha7RrOb_a5YFvtvRFZxVfYx1CuN6-cg3l3T8oNb9ZNtBoIgjQRVCSVz5Kcp-0PT6sk-wCA5IEnj9XTcYyMsxSXH6noGeXQFUuwKnkiMTrXtp_KwjQBb2pdy75_qwc_9vLFkhHgYBpckTeXPot9pSgt7KnzbW4-d0zq_yUsQMQgB_GPDQ6Qz6ksssBIMYLko8XKkgMq9SeJYXVQ";
const KID: &str = "cert-built-in";

#[derive(Clone)]
struct Provider {
    issuer: StdArc<Mutex<String>>,
    /// Overrides merged into the next ID Token's claims.
    claims: StdArc<Mutex<Value>>,
    iss_supported: bool,
    grants: StdArc<Mutex<Map<String, Grant>>>,
}
struct Grant {
    redirect_uri: String,
    challenge: String,
    nonce: String,
}
async fn discovery(AxumState(provider): AxumState<Provider>) -> AxumJson<Value> {
    let issuer = provider.issuer.lock().unwrap().clone();
    let base = issuer.trim_end_matches('/');
    AxumJson(json!({
        "issuer": issuer,
        "authorization_endpoint": format!("{base}/login/oauth/authorize"),
        "token_endpoint": format!("{base}/api/login/oauth/access_token"),
        "jwks_uri": format!("{base}/.well-known/app-microcrab/jwks"),
        "end_session_endpoint": format!("{base}/api/logout"),
        "code_challenge_methods_supported": ["S256"],
        "id_token_signing_alg_values_supported": ["RS256", "ES256"],
        "authorization_response_iss_parameter_supported": provider.iss_supported,
    }))
}
async fn jwks() -> AxumJson<Value> {
    AxumJson(
        json!({"keys":[{"kty":"RSA","kid":KID,"use":"sig","alg":"RS256","n":TEST_N,"e":"AQAB"}]}),
    )
}
async fn token(
    AxumState(provider): AxumState<Provider>,
    headers: HeaderMap,
    Form(form): Form<Map<String, String>>,
) -> Result<AxumJson<Value>, StatusCode> {
    // Bind each one-use code to the authorization request, not just the
    // presence of PKCE/redirect fields. Both supported client auth modes work.
    let basic = format!(
        "Basic {}",
        STANDARD.encode("test-client:test-only-client-secret")
    );
    let authenticated = if let Some(value) = headers.get(header::AUTHORIZATION) {
        value.to_str().ok() == Some(basic.as_str()) && !form.contains_key("client_secret")
    } else {
        form.get("client_id").map(String::as_str) == Some("test-client")
            && form.get("client_secret").map(String::as_str) == Some("test-only-client-secret")
    };
    if !authenticated {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let code = form.get("code").ok_or(StatusCode::BAD_REQUEST)?;
    let grant = provider
        .grants
        .lock()
        .unwrap()
        .remove(code)
        .ok_or(StatusCode::BAD_REQUEST)?;
    let verifier = form.get("code_verifier").ok_or(StatusCode::BAD_REQUEST)?;
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    if form.get("grant_type").map(String::as_str) != Some("authorization_code")
        || form.get("redirect_uri") != Some(&grant.redirect_uri)
        || challenge != grant.challenge
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let current = now().unwrap();
    let mut claims = json!({
        "iss": provider.issuer.lock().unwrap().clone(), "sub": "user-id-1",
        "aud": ["test-client"], "azp": "test-client",
        "iat": current, "nbf": current, "exp": current + 168 * 3600,
        "preferred_username": "alice", "tokenType": "id-token", "nonce": grant.nonce,
    });
    for (key, value) in provider.claims.lock().unwrap().as_object().unwrap() {
        claims[key] = value.clone();
    }
    let id_token = RS256KeyPair::from_pem(TEST_KEY)
        .unwrap()
        .with_key_id(KID)
        .sign(jwt::claims(&claims).unwrap())
        .unwrap();
    Ok(AxumJson(
        json!({"access_token":"opaque-access","token_type":"Bearer","id_token":id_token,"expires_in":604800}),
    ))
}
async fn start(iss_supported: bool) -> (Provider, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("http://{}", listener.local_addr().unwrap());
    let provider = Provider {
        issuer: StdArc::new(Mutex::new(issuer.clone())),
        claims: StdArc::new(Mutex::new(json!({}))),
        iss_supported,
        grants: StdArc::new(Mutex::new(Map::new())),
    };
    let app = AxumRouter::new()
        .route("/.well-known/openid-configuration", get(discovery))
        .route("/.well-known/app-microcrab/jwks", get(jwks))
        .route("/api/login/oauth/access_token", post(token))
        .with_state(provider.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (provider, issuer)
}
fn sso_config(issuer: &str) -> AuthConfig {
    serde_json::from_value(json!({
        "issuer": issuer, "discovery": true,
        "client_id":"test-client", "client_secret":"test-only-client-secret",
        "redirect_uri":"http://127.0.0.1:19090/api/auth/callback",
        "post_logout_redirect_uri":"http://127.0.0.1:19090/login",
        "allowed_origins":["http://127.0.0.1:19090"],
        "jwt_secret":"test-only-application-secret-32-characters-minimum",
        "cookie_secure":false, "allow_http_loopback":true,
    }))
    .unwrap()
}
struct Rp {
    auth: Auth,
    base: String,
    client: reqwest::Client,
}
async fn rp(config: AuthConfig) -> Rp {
    let auth = Auth::connect(config).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = router(auth.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = microcrab_transport::http::client_builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    Rp { auth, base, client }
}
fn set_cookie(response: &reqwest::Response, name: &str) -> Option<String> {
    response
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .find_map(|value| {
            value
                .strip_prefix(&format!("{name}="))
                .map(|rest| rest.split(';').next().unwrap().to_owned())
        })
        .filter(|value| !value.is_empty())
}
impl Rp {
    /// Run login and return (callback response). `iss` None omits the param.
    async fn login(&self, provider: &Provider, iss: Option<&str>) -> reqwest::Response {
        let login = self
            .client
            .get(format!("{}/api/auth/login", self.base))
            .send()
            .await
            .unwrap();
        let target = url::Url::parse(login.headers()[header::LOCATION].to_str().unwrap()).unwrap();
        assert!(target.path().ends_with("/login/oauth/authorize"));
        let query: Map<_, _> = target.query_pairs().into_owned().collect();
        assert_eq!(query["response_type"], "code");
        assert_eq!(query["client_id"], "test-client");
        assert_eq!(
            query["redirect_uri"],
            "http://127.0.0.1:19090/api/auth/callback"
        );
        assert_eq!(query["code_challenge_method"], "S256");
        let code = uuid::Uuid::new_v4().to_string();
        provider.grants.lock().unwrap().insert(
            code.clone(),
            Grant {
                redirect_uri: query["redirect_uri"].clone(),
                challenge: query["code_challenge"].clone(),
                nonce: query["nonce"].clone(),
            },
        );
        let binding = set_cookie(&login, LOGIN_COOKIE).unwrap();
        let mut callback = url::Url::parse(&format!("{}/api/auth/callback", self.base)).unwrap();
        callback
            .query_pairs_mut()
            .append_pair("code", &code)
            .append_pair("state", &query["state"]);
        if let Some(iss) = iss {
            callback.query_pairs_mut().append_pair("iss", iss);
        }
        self.client
            .get(callback)
            .header(header::COOKIE, format!("{LOGIN_COOKIE}={binding}"))
            .send()
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn discovery_resolves_authportal_endpoints_and_rejects_foreign_issuer() {
    let (provider, issuer) = start(true).await;
    let auth = Auth::connect(sso_config(&issuer)).await.unwrap();
    let endpoints = &auth.inner.endpoints;
    assert_eq!(
        endpoints.authorization,
        format!("{issuer}/login/oauth/authorize")
    );
    assert_eq!(
        endpoints.token,
        format!("{issuer}/api/login/oauth/access_token")
    );
    assert_eq!(
        endpoints.end_session.as_deref(),
        Some(&*format!("{issuer}/api/logout"))
    );
    assert!(endpoints.require_callback_iss);
    // Explicit fields override discovery; rp_logout=false disables end_session.
    let mut config = sso_config(&issuer);
    config.jwks_uri = Some(format!("{issuer}/.well-known/jwks"));
    config.rp_logout = false;
    let auth = Auth::connect(config).await.unwrap();
    assert_eq!(
        auth.inner.endpoints.jwks,
        format!("{issuer}/.well-known/jwks")
    );
    assert!(auth.inner.endpoints.end_session.is_none());
    // discovery=true cannot be built synchronously (no silent guessed paths).
    assert!(matches!(
        Auth::new(sso_config(&issuer)),
        Err(AuthError::Configuration)
    ));
    *provider.issuer.lock().unwrap() = "http://127.0.0.1:1".into();
    assert!(matches!(
        Auth::connect(sso_config(&issuer)).await,
        Err(AuthError::Configuration)
    ));
}

#[tokio::test]
async fn authportal_default_168h_id_token_logs_in_and_lifetime_cap_is_configurable() {
    let (provider, issuer) = start(true).await;
    let rp_default = rp(sso_config(&issuer)).await;
    let response = rp_default.login(&provider, Some(&issuer)).await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let session = set_cookie(&response, SESSION_COOKIE).unwrap();
    let probe: Value = rp_default
        .client
        .get(format!("{}/api/auth/session", rp_default.base))
        .header(header::COOKIE, format!("{SESSION_COOKIE}={session}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(probe["principal"]["subject"], "user-id-1");
    // No `name` claim: display falls back to preferred_username, never authz.
    assert_eq!(probe["principal"]["display_name"], "alice");
    assert!(!probe.to_string().contains("id_token"));
    let mut strict = sso_config(&issuer);
    strict.max_id_token_lifetime_seconds = 24 * 3600;
    let rp_strict = rp(strict).await;
    let response = rp_strict.login(&provider, Some(&issuer)).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn callback_issuer_parameter_is_enforced() {
    let (provider, issuer) = start(true).await;
    let rp = rp(sso_config(&issuer)).await;
    assert_eq!(
        rp.login(&provider, Some("https://evil.invalid"))
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    // Discovery advertises RFC 9207 support, so a missing iss is rejected.
    assert_eq!(
        rp.login(&provider, None).await.status(),
        StatusCode::UNAUTHORIZED
    );
    // A provider without iss support may omit it, but a wrong one still fails.
    let (provider, issuer) = start(false).await;
    let rp = self::rp(sso_config(&issuer)).await;
    assert_eq!(
        rp.login(&provider, None).await.status(),
        StatusCode::SEE_OTHER
    );
    assert_eq!(
        rp.login(&provider, Some("http://127.0.0.1:1"))
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn clock_skew_leeway_is_bounded_and_configurable() {
    let (provider, issuer) = start(true).await;
    let rp = rp(sso_config(&issuer)).await;
    let current = now().unwrap();
    *provider.claims.lock().unwrap() = json!({"iat": current + 30, "nbf": current + 30});
    assert_eq!(
        rp.login(&provider, Some(&issuer)).await.status(),
        StatusCode::SEE_OTHER
    );
    *provider.claims.lock().unwrap() = json!({"iat": current + 120, "nbf": current + 120});
    assert_eq!(
        rp.login(&provider, Some(&issuer)).await.status(),
        StatusCode::UNAUTHORIZED
    );
    *provider.claims.lock().unwrap() =
        json!({"iat": current - 600, "nbf": current - 600, "exp": current - 30});
    assert_eq!(
        rp.login(&provider, Some(&issuer)).await.status(),
        StatusCode::SEE_OTHER
    );
    let mut zero = sso_config(&issuer);
    zero.clock_skew_seconds = 0;
    let rp = self::rp(zero).await;
    *provider.claims.lock().unwrap() = json!({"iat": current + 30, "nbf": current + 30});
    assert_eq!(
        rp.login(&provider, Some(&issuer)).await.status(),
        StatusCode::UNAUTHORIZED
    );
    let mut invalid = sso_config(&issuer);
    invalid.clock_skew_seconds = 301;
    assert!(matches!(
        Auth::connect(invalid).await,
        Err(AuthError::Configuration)
    ));
}

#[tokio::test]
async fn logout_redirects_to_end_session_with_server_held_id_token_hint() {
    let (provider, issuer) = start(true).await;
    let rp = rp(sso_config(&issuer)).await;
    let response = rp.login(&provider, Some(&issuer)).await;
    let session = set_cookie(&response, SESSION_COOKIE).unwrap();
    let logout = |cookie: Option<String>| {
        let mut request = rp
            .client
            .post(format!("{}/api/auth/logout", rp.base))
            .header(header::ORIGIN, "http://127.0.0.1:19090");
        if let Some(cookie) = cookie {
            request = request.header(header::COOKIE, format!("{SESSION_COOKIE}={cookie}"));
        }
        request.send()
    };
    let first = logout(Some(session.clone())).await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let body: Value = first.json().await.unwrap();
    let target = url::Url::parse(body["redirect"].as_str().unwrap()).unwrap();
    assert_eq!(target.path(), "/api/logout");
    let query: Map<_, _> = target.query_pairs().into_owned().collect();
    assert_eq!(
        query["post_logout_redirect_uri"],
        "http://127.0.0.1:19090/login"
    );
    assert_eq!(query["client_id"], "test-client");
    let hint = &query["id_token_hint"];
    assert_eq!(jwt::header(hint).unwrap().kid.as_deref(), Some(KID));
    // The hint is single-use server state and the session is revoked.
    assert!(rp.auth.inner.id_tokens.lock().unwrap().is_empty());
    let again = logout(Some(session)).await.unwrap();
    let body: Value = again.json().await.unwrap();
    assert!(!body["redirect"].as_str().unwrap().contains("id_token_hint"));
}

async fn begin_login(rp: &Rp) -> (String, String) {
    let response = rp
        .client
        .get(format!("{}/api/auth/login", rp.base))
        .send()
        .await
        .unwrap();
    let target = url::Url::parse(response.headers()[header::LOCATION].to_str().unwrap()).unwrap();
    let state = target
        .query_pairs()
        .find(|(key, _)| key == "state")
        .unwrap()
        .1
        .into_owned();
    (state, set_cookie(&response, LOGIN_COOKIE).unwrap())
}

#[tokio::test]
async fn idp_error_callback_requires_browser_binding_and_is_consumed_once() {
    let (_provider, issuer) = start(true).await;
    let rp = rp(sso_config(&issuer)).await;
    for (error, expected) in [
        ("access_denied", "/login?auth_error=access_denied"),
        ("<script>", "/login?auth_error=authentication_failed"),
    ] {
        let (state, binding) = begin_login(&rp).await;
        let mut url = url::Url::parse(&format!("{}/api/auth/callback", rp.base)).unwrap();
        url.query_pairs_mut()
            .append_pair("error", error)
            .append_pair("error_description", "<b>provider text</b>")
            .append_pair("state", &state)
            .append_pair("iss", &issuer);
        // Missing and wrong cookies cannot consume another browser's state.
        for cookie in [None, Some("unrelated-browser")] {
            let mut request = rp.client.get(url.clone());
            if let Some(cookie) = cookie {
                request = request.header(header::COOKIE, format!("{LOGIN_COOKIE}={cookie}"));
            }
            let rejected = request.send().await.unwrap();
            assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
            assert!(!rejected.headers().contains_key(header::SET_COOKIE));
            assert!(rp.auth.inner.pending.lock().unwrap().contains_key(&state));
        }
        let send = || {
            rp.client
                .get(url.clone())
                .header(header::COOKIE, format!("{LOGIN_COOKIE}={binding}"))
                .send()
        };
        let (first, second) = tokio::join!(send(), send());
        let first = first.unwrap();
        let second = second.unwrap();
        let (accepted, rejected) = if first.status() == StatusCode::SEE_OTHER {
            (first, second)
        } else {
            (second, first)
        };
        assert_eq!(accepted.status(), StatusCode::SEE_OTHER);
        assert_eq!(accepted.headers()[header::LOCATION], expected);
        assert_eq!(accepted.headers()[header::REFERRER_POLICY], "no-referrer");
        assert!(set_cookie(&accepted, SESSION_COOKIE).is_none());
        assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
        assert!(!rejected.headers().contains_key(header::SET_COOKIE));
        assert!(!rp.auth.inner.pending.lock().unwrap().contains_key(&state));
    }
}

#[tokio::test]
async fn malformed_and_expired_error_callbacks_do_not_mutate_login_state() {
    let (_provider, issuer) = start(true).await;
    let rp = rp(sso_config(&issuer)).await;
    let (state, binding) = begin_login(&rp).await;
    for suffix in [
        "error=access_denied".to_owned(),
        "error=access_denied&state=unknown".to_owned(),
        format!("error=access_denied&code=ambiguous&state={state}"),
        format!("state={state}"),
        format!("error=access_denied&state={state}&state={state}"),
    ] {
        let response = rp
            .client
            .get(format!(
                "{}/api/auth/callback?iss={issuer}&{suffix}",
                rp.base
            ))
            .header(header::COOKIE, format!("{LOGIN_COOKIE}={binding}"))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_client_error());
        assert!(!response.headers().contains_key(header::SET_COOKIE));
        assert!(rp.auth.inner.pending.lock().unwrap().contains_key(&state));
    }
    rp.auth
        .inner
        .pending
        .lock()
        .unwrap()
        .get_mut(&state)
        .unwrap()
        .expires = 0;
    let response = rp
        .client
        .get(format!(
            "{}/api/auth/callback?iss={issuer}&error=access_denied&state={state}",
            rp.base
        ))
        .header(header::COOKIE, format!("{LOGIN_COOKIE}={binding}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(!response.headers().contains_key(header::SET_COOKIE));
    assert!(rp.auth.inner.pending.lock().unwrap().contains_key(&state));
}

#[tokio::test]
async fn signed_id_token_claims_remain_bound_to_issuer_audience_and_nonce() {
    let (provider, issuer) = start(true).await;
    let rp = rp(sso_config(&issuer)).await;
    for claims in [
        json!({"iss": "https://foreign.invalid"}),
        json!({"aud": "another-client"}),
        json!({"aud": ["another-client"]}),
        json!({"aud": ["test-client", "another-client"], "azp": null}),
        json!({"azp": "another-client"}),
        json!({"nonce": "another-login"}),
        json!({"at_hash": "incorrect"}),
    ] {
        *provider.claims.lock().unwrap() = claims;
        assert_eq!(
            rp.login(&provider, Some(&issuer)).await.status(),
            StatusCode::UNAUTHORIZED
        );
    }
}

#[tokio::test]
async fn basic_client_auth_and_unicode_subject_fallback_work() {
    let (provider, issuer) = start(true).await;
    *provider.claims.lock().unwrap() = json!({"sub": "界".repeat(200), "preferred_username": null});
    let mut config = sso_config(&issuer);
    config.token_endpoint_auth_method = TokenAuthMethod::ClientSecretBasic;
    let rp = rp(config).await;
    let response = rp.login(&provider, Some(&issuer)).await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let token = set_cookie(&response, SESSION_COOKIE).unwrap();
    let principal = rp.auth.authenticate(&super::headers(&token)).unwrap();
    assert_eq!(principal.subject, "界".repeat(200));
    assert_eq!(principal.display_name, "界".repeat(85));
}

#[test]
fn post_logout_target_requires_exact_allowed_origin() {
    let mut config = sso_config("https://identity.example");
    config.cookie_secure = true;
    config.redirect_uri = "https://crab.example/api/auth/callback".into();
    config.allowed_origins = vec![
        "https://crab.example".into(),
        "https://second.example:8443".into(),
    ];
    for target in [
        "https://crab.example/login",
        "https://second.example:8443/login",
    ] {
        config.post_logout_redirect_uri = Some(target.into());
        assert!(config.validate().is_ok());
    }
    for target in [
        "https://evil.example/login",
        "https://crab.example.evil.example/login",
        "https://crab.example:8443/login",
        "https://second.example/login",
        "https://user@crab.example/login",
        "https://crab.example/login#fragment",
    ] {
        config.post_logout_redirect_uri = Some(target.into());
        assert!(matches!(config.validate(), Err(AuthError::Configuration)));
    }
}

#[test]
fn sso_configuration_rules_fail_at_startup() {
    let issuer = "http://127.0.0.1:19091";
    let mut missing = sso_config(issuer);
    missing.discovery = false;
    assert!(matches!(Auth::new(missing), Err(AuthError::Configuration)));
    for scopes in [
        vec!["profile".to_string()],
        vec!["openid".into(), "bad scope".into()],
    ] {
        let mut invalid = sso_config(issuer);
        invalid.discovery = false;
        invalid.authorization_endpoint = Some(format!("{issuer}/a"));
        invalid.token_endpoint = Some(format!("{issuer}/t"));
        invalid.jwks_uri = Some(format!("{issuer}/j"));
        invalid.scopes = scopes;
        assert!(matches!(Auth::new(invalid), Err(AuthError::Configuration)));
    }
    let mut invalid = sso_config(issuer);
    invalid.max_id_token_lifetime_seconds = 31 * 24 * 3600;
    assert!(matches!(invalid.validate(), Err(AuthError::Configuration)));
    let mut invalid = sso_config(issuer);
    invalid.post_logout_redirect_uri = Some("http://evil.example/login".into());
    assert!(matches!(invalid.validate(), Err(AuthError::Configuration)));
}

/// Optional RP smoke test against an independently configured compatible issuer.
/// `MICROCRAB_DEV_IDP` supplies its URL; register client `microcrab-dev` with
/// http://127.0.0.1:19090/api/auth/callback. Automated embedded-provider coverage
/// lives in `auth::dev::tests` and needs no separately running identity service.
#[tokio::test]
#[ignore = "requires an independently configured external test issuer"]
async fn dev_identity_server_full_flow_through_discovery() {
    let issuer = std::env::var("MICROCRAB_DEV_IDP").unwrap();
    let mut config = sso_config(&issuer);
    config.client_id = "microcrab-dev".into();
    config.client_secret = None;
    let rp = rp(config).await;
    let login = rp
        .client
        .get(format!("{}/api/auth/login?login_hint=bob", rp.base))
        .send()
        .await
        .unwrap();
    let binding = set_cookie(&login, LOGIN_COOKIE).unwrap();
    let authorize = rp
        .client
        .get(login.headers()[header::LOCATION].to_str().unwrap())
        .send()
        .await
        .unwrap();
    let callback =
        url::Url::parse(authorize.headers()[header::LOCATION].to_str().unwrap()).unwrap();
    assert!(
        callback
            .query_pairs()
            .any(|(key, value)| key == "iss" && value == issuer)
    );
    let local = format!(
        "{}{}?{}",
        rp.base,
        callback.path(),
        callback.query().unwrap()
    );
    let done = rp
        .client
        .get(local)
        .header(header::COOKIE, format!("{LOGIN_COOKIE}={binding}"))
        .send()
        .await
        .unwrap();
    assert_eq!(done.status(), StatusCode::SEE_OTHER);
    let session = set_cookie(&done, SESSION_COOKIE).unwrap();
    let logout: Value = rp
        .client
        .post(format!("{}/api/auth/logout", rp.base))
        .header(header::ORIGIN, "http://127.0.0.1:19090")
        .header(header::COOKIE, format!("{SESSION_COOKIE}={session}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let end = rp
        .client
        .get(logout["redirect"].as_str().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(end.status(), StatusCode::FOUND);
    assert_eq!(
        end.headers()[header::LOCATION],
        "http://127.0.0.1:19090/login"
    );
}
