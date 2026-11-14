//! Pending permission requests are delivered by `session.attach`; the
//! retired HTTP query is gone and inspection stays owner-scoped and read-only.
use super::*;
#[tokio::test]
async fn attach_pending_permissions_are_owner_scoped_and_never_reopen_closed_runtime() {
    let fixture = Fixture::new(true).await;
    let created = fixture
        .request(
            "POST",
            "/api/v1/sessions",
            Some(json!({"project_id":"workspace","operation_id":uuid::Uuid::new_v4().to_string()})),
        )
        .await;
    assert_eq!(created.status(), StatusCode::OK);
    let created = body(created).await;
    let id = created["id"].as_str().unwrap();
    let retired = fixture
        .request(
            "GET",
            &format!("/api/v1/sessions/{id}/permission/requests"),
            None,
        )
        .await;
    assert!(!retired.status().is_success());
    let owner = microcrab_auth_test_principal(&fixture, "owner");
    let pending = fixture.state.manager.pending_requests(&owner, id).await;
    let permissions = &pending["permissions"];
    assert_eq!(permissions["requests"], json!([]));
    assert_eq!(permissions["interactive_ask"], true);
    assert_eq!(permissions["remembered_grants"], true);
    assert_eq!(permissions["executor_connected"], false);
    let foreign = microcrab_auth_test_principal(&fixture, "foreign-owner");
    let foreign = fixture.state.manager.pending_requests(&foreign, id).await;
    assert!(foreign["permissions"].is_null());
    assert!(foreign["round_limits"].is_null());
    let closed = fixture
        .request(
            "DELETE",
            &format!("/api/v1/sessions/{id}/runtime"),
            Some(json!({})),
        )
        .await;
    assert_eq!(closed.status(), StatusCode::OK);
    let pending = fixture.state.manager.pending_requests(&owner, id).await;
    assert_eq!(pending["permissions"]["interactive_ask"], false);
    assert_eq!(pending["permissions"]["requests"], json!([]));
    fixture.state.manager.close().await.unwrap();
}

fn microcrab_auth_test_principal(fixture: &Fixture, owner: &str) -> crate::auth::Principal {
    let token = fixture.state.auth.login_for_test(owner);
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "cookie",
        format!("microcrab_session={token}").parse().unwrap(),
    );
    fixture.state.auth.authenticate(&headers).unwrap()
}
