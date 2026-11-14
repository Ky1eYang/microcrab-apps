//! Same-listener integration and issuer security boundaries. No private paths.
use super::*;
use axum::http::{HeaderMap, StatusCode, header};
use serde_json::{Value, json};

fn config() -> AuthConfig {
    serde_json::from_value(json!({
        "dev_oidc":true,"issuer":"http://127.0.0.1:1/obsolete",
        "client_id":"embedded-client","redirect_uri":"http://127.0.0.1:0/api/auth/callback",
        "allowed_origins":["http://127.0.0.1:1"],
        "jwt_secret":"embedded-only-test-application-secret-32-bytes",
        "cookie_secure":false,"allow_http_loopback":true
    }))
    .unwrap()
}
struct Fixture {
    auth: Auth,
    provider: Provider,
    base: String,
    client: reqwest::Client,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn start() -> Fixture {
    start_with(config()).await
}
async fn start_with(config: AuthConfig) -> Fixture {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    // Listener is bound but deliberately not served: metadata must bootstrap
    // without self-HTTP. A bounded wait makes a discovery regression observable.
    let auth = tokio::time::timeout(
        Duration::from_secs(30),
        Auth::for_listener(config, Some(address)),
    )
    .await
    .unwrap()
    .unwrap();
    let provider = auth.inner.provider.clone().unwrap();
    let app = super::super::router(auth.clone()).merge(super::router(auth.clone()));
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Fixture {
        auth,
        provider,
        base: format!("http://{address}"),
        client: microcrab_transport::http::client_builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
        task,
    }
}
fn set_cookie(response: &reqwest::Response, name: &str) -> String {
    response
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .filter_map(|header| header.to_str().ok())
        .find_map(|value| {
            value
                .strip_prefix(&format!("{name}="))
                .map(|rest| rest.split(';').next().unwrap().to_owned())
        })
        .unwrap()
}
impl Fixture {
    async fn begin(&self, subject: &str) -> (String, String) {
        let response = self
            .client
            .get(format!("{}/api/auth/login", self.base))
            .query(&[("login_hint", subject)])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        let binding = set_cookie(&response, super::super::LOGIN_COOKIE);
        let authorization = response.headers()[header::LOCATION].to_str().unwrap();
        assert!(authorization.starts_with(&format!("{}/api/dev/oidc/authorize?", self.base)));
        let response = self.client.get(authorization).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        (
            response.headers()[header::LOCATION]
                .to_str()
                .unwrap()
                .to_owned(),
            binding,
        )
    }
    async fn callback(&self, target: &str, binding: &str) -> reqwest::Response {
        self.client
            .get(target)
            .header(
                header::COOKIE,
                format!("{}={binding}", super::super::LOGIN_COOKIE),
            )
            .send()
            .await
            .unwrap()
    }
    async fn login(&self, subject: &str) -> String {
        let (target, binding) = self.begin(subject).await;
        let response = self.callback(&target, &binding).await;
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        set_cookie(&response, super::super::SESSION_COOKIE)
    }
    fn principal(&self, token: &str) -> super::super::Principal {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            format!("{}={token}", super::super::SESSION_COOKIE)
                .parse()
                .unwrap(),
        );
        self.auth.authenticate(&headers).unwrap()
    }
}

#[tokio::test]
async fn same_listener_bootstrap_login_state_logout_and_stable_isolated_owners() {
    let fixture = start().await;
    let document: Value = fixture
        .client
        .get(format!(
            "{}/api/dev/oidc/.well-known/openid-configuration",
            fixture.base
        ))
        .header(header::HOST, "attacker.invalid")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(document, fixture.provider.metadata());
    assert_eq!(document["issuer"], format!("{}/api/dev/oidc", fixture.base));
    let (target, binding) = fixture.begin("alice").await;
    assert_eq!(
        fixture.callback(&target, "wrong-browser").await.status(),
        StatusCode::UNAUTHORIZED
    );
    let mut wrong_issuer = url::Url::parse(&target).unwrap();
    let pairs: Vec<_> = wrong_issuer
        .query_pairs()
        .into_owned()
        .filter(|(key, _)| key != "iss")
        .collect();
    wrong_issuer.set_query(None);
    wrong_issuer
        .query_pairs_mut()
        .extend_pairs(pairs)
        .append_pair("iss", "http://127.0.0.1:1");
    assert_eq!(
        fixture
            .callback(wrong_issuer.as_str(), &binding)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let alice_response = fixture.callback(&target, &binding).await;
    assert_eq!(alice_response.status(), StatusCode::SEE_OTHER);
    let alice_token = set_cookie(&alice_response, super::super::SESSION_COOKIE);
    assert_eq!(
        fixture.callback(&target, &binding).await.status(),
        StatusCode::UNAUTHORIZED
    );
    let alice = fixture.principal(&alice_token);
    assert_eq!(alice.subject, "alice");
    assert_eq!(
        alice.owner,
        fixture.principal(&fixture.login("alice").await).owner
    );
    assert_ne!(
        alice.owner,
        fixture.principal(&fixture.login("bob").await).owner
    );
    let response = fixture
        .client
        .post(format!("{}/api/auth/logout", fixture.base))
        .header(header::ORIGIN, &fixture.base)
        .header(
            header::COOKIE,
            format!("{}={alice_token}", super::super::SESSION_COOKIE),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    let response = fixture
        .client
        .get(body["redirect"].as_str().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        response.headers()[header::LOCATION],
        format!("{}/login", fixture.base)
    );
    assert!(fixture.auth.check_active(&alice).is_err());
}

fn authorize(provider: &Provider, subject: &str) -> (String, String) {
    let verifier = "a".repeat(64);
    let target = provider
        .authorize(http::Authorize {
            client_id: provider.0.config.client_id.clone(),
            redirect_uri: provider.0.config.redirect_uri.clone(),
            response_type: "code".into(),
            scope: "openid profile".into(),
            state: "test-state".into(),
            nonce: "test-nonce".into(),
            code_challenge: URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes())),
            code_challenge_method: "S256".into(),
            login_hint: Some(subject.into()),
        })
        .unwrap();
    let url = url::Url::parse(&target).unwrap();
    (
        url.query_pairs()
            .find(|(key, _)| key == "code")
            .unwrap()
            .1
            .into_owned(),
        verifier,
    )
}
fn form(provider: &Provider, code: String, verifier: String) -> http::Token {
    http::Token {
        grant_type: "authorization_code".into(),
        code,
        redirect_uri: provider.0.config.redirect_uri.clone(),
        code_verifier: verifier,
        client_id: Some(provider.0.config.client_id.clone()),
        client_secret: provider.0.config.client_secret.clone(),
    }
}

#[tokio::test]
async fn grants_are_bounded_expiring_single_use_and_bind_pkce_client_redirect() {
    let fixture = start().await;
    let provider = &fixture.provider;
    for field in ["pkce", "client", "redirect", "expired"] {
        let (code, verifier) = authorize(provider, "arbitrary:user@example.test");
        let mut bad = form(provider, code.clone(), verifier.clone());
        match field {
            "pkce" => bad.code_verifier = "b".repeat(64),
            "client" => bad.client_id = Some("another-client".into()),
            "redirect" => bad.redirect_uri.push_str("/elsewhere"),
            _ => {
                provider
                    .0
                    .grants
                    .lock()
                    .unwrap()
                    .get_mut(&code)
                    .unwrap()
                    .expires = Instant::now() - Duration::from_secs(1)
            }
        }
        assert!(matches!(provider.token(bad), Err(AuthError::Unauthorized)));
        assert!(matches!(
            provider.token(form(provider, code, verifier)),
            Err(AuthError::Unauthorized)
        ));
    }
    for _ in 0..MAX_GRANTS {
        authorize(provider, "alice");
    }
    let query = http::Authorize {
        client_id: provider.0.config.client_id.clone(),
        redirect_uri: provider.0.config.redirect_uri.clone(),
        response_type: "code".into(),
        scope: "openid".into(),
        state: "s".into(),
        nonce: "n".into(),
        code_challenge: "a".repeat(43),
        code_challenge_method: "S256".into(),
        login_hint: None,
    };
    assert!(matches!(
        provider.authorize(query),
        Err(AuthError::Unavailable)
    ));
    assert_eq!(provider.0.grants.lock().unwrap().len(), MAX_GRANTS);
}

#[tokio::test]
async fn signed_tokens_reuse_full_rp_validation_and_jwks_refresh_after_rotation() {
    let fixture = start().await;
    let (code, verifier) = authorize(&fixture.provider, "alice");
    let result = fixture
        .provider
        .token(form(&fixture.provider, code, verifier))
        .unwrap();
    let id = result["id_token"].as_str().unwrap();
    let access = result["access_token"].as_str().unwrap();
    assert!(
        fixture
            .auth
            .verify_identity(id, "test-nonce", access)
            .await
            .is_ok()
    );
    for (id, nonce, access) in [
        (id, "wrong-nonce", access),
        (id, "test-nonce", "wrong-access"),
        (access, "test-nonce", access),
    ] {
        assert!(matches!(
            fixture.auth.verify_identity(id, nonce, access).await,
            Err(AuthError::Unauthorized)
        ));
    }
    fixture.provider.0.keys.lock().unwrap().rotate().unwrap();
    assert_eq!(
        fixture.provider.0.keys.lock().unwrap().jwks()["keys"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let (code, verifier) = authorize(&fixture.provider, "bob");
    let rotated = fixture
        .provider
        .token(form(&fixture.provider, code, verifier))
        .unwrap();
    let new_id = rotated["id_token"].as_str().unwrap();
    assert_ne!(
        crate::auth::jwt::header(id).unwrap().kid,
        crate::auth::jwt::header(new_id).unwrap().kid
    );
    assert!(
        fixture
            .auth
            .verify_identity(
                new_id,
                "test-nonce",
                rotated["access_token"].as_str().unwrap()
            )
            .await
            .is_ok()
    );
    assert!(
        fixture
            .auth
            .verify_identity(id, "test-nonce", access)
            .await
            .is_ok()
    );
    assert_eq!(
        fixture
            .client
            .post(format!("{}/api/dev/oidc/rotate", fixture.base))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn public_provider_is_absent_for_external_selection_and_listener_is_required() {
    let mut config = config();
    assert!(matches!(
        Auth::for_listener(config.clone(), None).await,
        Err(AuthError::Configuration)
    ));
    assert!(matches!(
        Auth::for_listener(config.clone(), Some("0.0.0.0:4096".parse().unwrap())).await,
        Err(AuthError::Configuration)
    ));
    config.dev_oidc = false;
    config.discovery = false;
    config.authorization_endpoint = Some("http://127.0.0.1:1/authorize".into());
    config.token_endpoint = Some("http://127.0.0.1:1/token".into());
    config.jwks_uri = Some("http://127.0.0.1:1/jwks".into());
    let auth = Auth::for_listener(config, None).await.unwrap();
    assert!(auth.inner.provider.is_none());
    use tower::ServiceExt;
    let response = super::router(auth)
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/dev/oidc/jwks")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn confidential_basic_client_uses_same_flow_and_bad_secret_consumes_grant() {
    let mut config = config();
    config.client_secret = Some("test-only&secret:+encoded".into());
    config.token_endpoint_auth_method = super::super::TokenAuthMethod::ClientSecretBasic;
    let fixture = start_with(config).await;
    assert_eq!(
        fixture.principal(&fixture.login("alice").await).subject,
        "alice"
    );
    let (code, verifier) = authorize(&fixture.provider, "bob");
    let mut bad = form(&fixture.provider, code.clone(), verifier.clone());
    bad.client_secret = Some("incorrect-secret".into());
    assert!(matches!(
        fixture.provider.token(bad),
        Err(AuthError::Unauthorized)
    ));
    assert!(matches!(
        fixture
            .provider
            .token(form(&fixture.provider, code, verifier)),
        Err(AuthError::Unauthorized)
    ));
}

#[test]
fn logout_hint_preserves_expired_and_future_issue_time_policy() {
    let ring = keys::KeyRing::new().unwrap();
    let config = config();
    for (iat, nbf, exp) in [(1, 1, 2), (now().unwrap() + 3600, now().unwrap() + 3600, 1)] {
        let claims = json!({"iss":config.issuer,"aud":config.client_id,
            "azp":config.client_id,"token_use":"id","iat":iat,"nbf":nbf,"exp":exp});
        let token = ring.sign(&claims).unwrap();
        assert!(ring.verify_logout(&token, &config).is_ok());
        for (field, value) in [
            ("iss", json!("other-issuer")),
            ("aud", json!("other-client")),
            ("azp", json!("other-client")),
            ("token_use", json!("access")),
        ] {
            let mut invalid = claims.clone();
            invalid[field] = value;
            assert!(
                ring.verify_logout(&ring.sign(&invalid).unwrap(), &config)
                    .is_err()
            );
        }
        let mut missing = claims.clone();
        missing.as_object_mut().unwrap().remove("exp");
        assert!(
            ring.verify_logout(&ring.sign(&missing).unwrap(), &config)
                .is_err()
        );
    }
}
