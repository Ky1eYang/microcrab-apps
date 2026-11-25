//! Central HTTP tree exposes offline inspection without granting execution or metadata deletion.
use super::*;

#[tokio::test]
async fn host_inspection_and_provisional_cleanup_are_owner_scoped_http_resources() {
    let fixture = Fixture::new(false).await;
    let created = fixture
        .request(
            "POST",
            "/api/v1/projects",
            Some(json!({"id":"draft","name":"Draft","root":"/remote/uninterpreted/../alias"})),
        )
        .await;
    assert_eq!(created.status(), StatusCode::OK);
    let created = body(created).await;
    assert_eq!(created["provisional"], true);
    assert_eq!(created["root"], "/remote/uninterpreted/../alias");
    let state = fixture
        .request("GET", "/api/v1/projects/draft/host-state", None)
        .await;
    assert_eq!(state.status(), StatusCode::OK);
    let state = body(state).await;
    assert_eq!(state["connected"], false);
    assert_eq!(state["ready"], false);
    assert_eq!(state["reconciling"], false);
    assert_eq!(state["carrier_mode"], "detached");
    assert_eq!(state["descriptor"], Value::Null);
    assert_eq!(state["connection_generation"], Value::Null);
    assert_eq!(state["scope"], Value::Null);

    let operations = fixture
        .request("GET", "/api/v1/projects/draft/operations?limit=100", None)
        .await;
    assert_eq!(operations.status(), StatusCode::OK);
    let operations = body(operations).await;
    assert_eq!(operations["operations"], json!([]));
    assert_eq!(operations["limit"], 100);
    let invalid = fixture
        .request("GET", "/api/v1/projects/draft/operations?limit=101", None)
        .await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    let disconnected = fixture
        .request(
            "POST",
            "/api/v1/projects/draft/host-reconcile",
            Some(json!({})),
        )
        .await;
    assert_eq!(disconnected.status(), StatusCode::CONFLICT);
    assert_eq!(body(disconnected).await["error"]["code"], "host_detached");
    let foreign_token = fixture.state.auth.login_for_test("foreign-owner");
    for (method, path) in [
        ("GET", "host-state"),
        ("GET", "operations"),
        ("POST", "host-reconcile"),
        ("DELETE", "provisional"),
    ] {
        let mut request = Request::builder()
            .method(method)
            .uri(format!("/api/v1/projects/draft/{path}"))
            .header("cookie", format!("microcrab_session={foreign_token}"));
        let request_body = if method == "GET" {
            Body::empty()
        } else {
            request = request
                .header("origin", "http://127.0.0.1:4096")
                .header("content-type", "application/json");
            Body::from("{}")
        };
        let response = fixture
            .router
            .clone()
            .oneshot(request.body(request_body).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let value = body(response).await;
        assert!(!value.to_string().contains("/remote/uninterpreted"));
    }
    let deleted = fixture
        .request(
            "DELETE",
            "/api/v1/projects/draft/provisional",
            Some(json!({})),
        )
        .await;
    assert_eq!(deleted.status(), StatusCode::OK);
    assert_eq!(body(deleted).await["deleted"], true);
    assert!(!fixture.state.manager.projects.find("draft").await.is_some());
    fixture.state.manager.close().await.unwrap();
}
