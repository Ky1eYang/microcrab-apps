//! Exercise the assembled router, not a second implementation of provenance.
use super::*;

#[tokio::test]
async fn provenance_records_safe_native_results_and_rejects_private_labels() {
    let capture = crate::route::provenance::testing::capture();
    let fixture = Fixture::new(false).await;
    let tab = uuid::Uuid::new_v4().to_string();
    for (path, params, expected) in [
        (
            "/api/v1/projects/workspace/host/disconnect",
            json!({"revoke":false}),
            StatusCode::OK,
        ),
        (
            "/api/v1/projects/workspace/host/connect",
            json!({"url":"private-url-value", "token":"private-token-value", "allow_legacy_binding":false}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let request_id = uuid::Uuid::new_v4().to_string();
        let response = fixture
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("cookie", format!("microcrab_session={}", fixture.token))
                    .header("origin", "http://127.0.0.1:4096")
                    .header("content-type", "application/json")
                    .header("x-request-id", &request_id)
                    .header("x-client-tab-id", &tab)
                    .header("x-client-trigger", "action:connect_host")
                    .body(Body::from(params.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        assert_eq!(response.headers()["x-request-id"], request_id);
        let payload = body(response).await;
        let text = capture.text();
        let line = text
            .lines()
            .find(|line| line.contains(&request_id) && line.contains("HTTP API request completed"))
            .unwrap();
        assert!(line.contains(&tab), "{line}");
        assert!(line.contains("action:connect_host"), "{line}");
        assert!(line.contains("project_id=\"workspace\""), "{line}");
        assert!(line.contains(&crate::route::provenance::owner_prefix(
            &fixture.owner_of(&fixture.token)
        )));
        assert!(line.contains("elapsed_ms="));
        if expected.is_success() {
            assert!(line.contains("revoke=false"));
        } else {
            assert!(
                line.contains(payload["error"]["code"].as_str().unwrap()),
                "{line}"
            );
            assert!(line.contains("has_token=true"));
            assert!(line.contains("carrier=\"invalid\""));
        }
    }
    let id = uuid::Uuid::new_v4().to_string();
    let response = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/sessions?private-query-key=private-query-value")
                .header("cookie", format!("microcrab_session={}", fixture.token))
                .header("x-request-id", &id)
                .header("x-client-tab-id", "private-tab-value")
                .header("x-client-trigger", "private-trigger-value")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let text = capture.text();
    let line = text
        .lines()
        .find(|line| line.contains(&id) && line.contains("HTTP API request completed"))
        .unwrap();
    assert!(
        line.contains("tab_id=invalid") || line.contains("tab_id=\"invalid\""),
        "{line}"
    );
    assert!(line.contains("trigger=\"invalid\""), "{line}");
    for private in [
        "private-token-value",
        "private-url-value",
        "private-query-key",
        "private-query-value",
        "private-tab-value",
        "private-trigger-value",
        &fixture.token,
        &fixture.owner_of(&fixture.token),
    ] {
        assert!(
            !text.contains(private),
            "private value leaked into diagnostics"
        );
    }
    fixture.state.manager.close().await.unwrap();
}
