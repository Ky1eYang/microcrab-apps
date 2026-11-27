//! Owner Provider ids authorize no privileges beyond the authenticated owner.
use super::*;

fn provider(name: &str) -> Value {
    json!({
        "name": name, "protocol": "openai-compatible",
        "endpoint": "https://provider.example/v1/chat/completions", "auth": "bearer",
        "timeout_ms": 60000, "max_response_bytes": 1048576,
        "models": [{"id": "test-model"}],
    })
}

fn assert_redacted(value: &Value) {
    let encoded = value.to_string();
    assert!(!encoded.contains("\"api_key\":"));
    for secret in [
        "YOUR_LITERAL_API_KEY",
        "admin-test-credential",
        "owner-test-credential",
    ] {
        assert!(!encoded.contains(secret));
    }
}

#[tokio::test]
async fn provider_http_private_edits_are_owner_scoped_and_startup_is_read_only() {
    let mut fixture = Fixture::new(false).await;
    let ordinary = fixture.token.clone();
    let admin = fixture.state.auth.login_for_test("admin");
    assert_ne!(fixture.owner_of(&ordinary), fixture.owner_of(&admin));
    fixture.token = admin.clone();

    let listing = body(fixture.request("GET", "/api/v1/providers", None).await).await;
    assert_redacted(&listing);
    let startup = listing["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["scope"] == "admin")
        .expect("startup provider");
    assert_eq!(startup["read_only"], true);
    let shared = format!("/api/v1/providers/{}", startup["id"].as_str().unwrap());
    for (method, path, payload) in [
        (
            "PUT",
            shared.clone(),
            json!({"expected_revision":0,"provider":provider("Hijack")}),
        ),
        ("DELETE", shared.clone(), json!({"expected_revision":0})),
    ] {
        let response = fixture.request(method, &path, Some(payload)).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body(response).await["error"]["code"], "read_only");
    }
    // Admin owner can maintain per-owner model overrides on the shared provider
    // without affecting other owners' view of that shared provider.
    let admin_override = fixture
        .request(
            "PUT",
            &shared,
            Some(json!({
                "expected_revision": 0,
                "models": [{"id": "admin-personal-model", "name": "Personal Model"}]
            })),
        )
        .await;
    assert_eq!(admin_override.status(), StatusCode::OK);
    assert_eq!(body(admin_override).await["revision"], 1);
    let response = fixture
        .request(
            "PUT",
            "/api/v1/providers/not-namespaced",
            Some(json!({
                "expected_revision":0, "provider":provider("Bare id"),
            })),
        )
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(body(response).await["error"]["code"], "invalid_params");

    let path = "/api/v1/providers/user%3Aprivate";
    for (token, name, secret) in [
        (&admin, "Admin private", "admin-test-credential"),
        (&ordinary, "Owner private", "owner-test-credential"),
    ] {
        fixture.token.clone_from(token);
        let missing = fixture.request("GET", path, None).await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        let response = fixture
            .request(
                "PUT",
                path,
                Some(json!({
                    "expected_revision":0, "provider":provider(name), "api_key":secret,
                })),
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let saved = body(response).await;
        assert_eq!(saved["id"], "user:private");
        assert_eq!(saved["revision"], 1);
        assert_eq!(saved["read_only"], false);
        assert_eq!(saved["api_key_configured"], true);
        assert_eq!(saved["name"], name);
        assert_redacted(&saved);
    }

    fixture.token = admin;
    let current = body(fixture.request("GET", path, None).await).await;
    assert_eq!(current["name"], "Admin private");
    assert_redacted(&current);
    let stale = fixture
        .request(
            "PUT",
            path,
            Some(json!({
                "expected_revision":0, "provider":provider("Stale edit"),
            })),
        )
        .await;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    assert_eq!(body(stale).await["error"]["code"], "revision_conflict");
    let edited = fixture
        .request(
            "PUT",
            path,
            Some(json!({
                "expected_revision":1, "provider":provider("Admin edited"),
            })),
        )
        .await;
    assert_eq!(edited.status(), StatusCode::OK);
    let edited = body(edited).await;
    assert_eq!(edited["name"], "Admin edited");
    assert_eq!(edited["revision"], 2);
    assert_eq!(edited["api_key_configured"], true);
    assert_redacted(&edited);

    fixture.token = ordinary;
    let listing = body(fixture.request("GET", "/api/v1/providers", None).await).await;
    assert_redacted(&listing);
    assert_eq!(
        listing["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["scope"] == "user")
            .count(),
        1
    );
    let current = body(fixture.request("GET", path, None).await).await;
    assert_eq!(current["name"], "Owner private");
    assert_eq!(current["revision"], 1);
    assert_redacted(&current);
    assert_eq!(
        body(fixture.request("GET", &shared, None).await).await,
        *startup
    );
    fixture.state.manager.close().await.unwrap();
}
