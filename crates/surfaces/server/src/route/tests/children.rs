//! Child inspection remains a fixed parent-authorized HTTP query.
use super::*;
#[tokio::test]
async fn child_resources_are_parent_scoped_readonly_and_validate_query_limits() {
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
    let parent = fixture
        .state
        .manager
        .config
        .data_dir
        .join("sessions")
        .join(id);
    let (child, _) = microcrab_creator::paths::reserve_child(&parent).unwrap();
    let path = format!("/api/v1/sessions/{id}/children");
    let response = fixture.request("GET", &path, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let list = body(response).await;
    assert_eq!(list["parent_session_id"], id);
    assert_eq!(list["children"][0]["id"], child);
    assert_eq!(list["children"][0]["availability"], "pending");
    let detail_path = format!("{path}/{child}?limit=1");
    let detail = fixture.request("GET", &detail_path, None).await;
    assert_eq!(detail.status(), StatusCode::OK);
    let detail = body(detail).await;
    assert_eq!(detail["messages"], json!([]));
    assert_eq!(detail["availability"], "pending");
    assert_eq!(
        fixture
            .request("GET", &format!("{path}?limit=101"), None)
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        fixture
            .request("GET", &format!("{path}?unexpected=true"), None)
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    let other = fixture.state.auth.login_for_test("intruder");
    let response = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri(&detail_path)
                .header("cookie", format!("microcrab_session={other}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let enter_path = format!("{path}/{child}/enter");
    let foreign = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&enter_path)
                .header("cookie", format!("microcrab_session={other}"))
                .header("origin", "http://127.0.0.1:4096")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(foreign.status(), StatusCode::NOT_FOUND);
    let wrong_origin = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&enter_path)
                .header("cookie", format!("microcrab_session={}", fixture.token))
                .header("origin", "https://untrusted.example")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong_origin.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        fixture
            .request("POST", &enter_path, Some(json!({"unexpected":true})))
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    let pending = fixture.request("POST", &enter_path, Some(json!({}))).await;
    assert!(
        !pending.status().is_success(),
        "unpublished child has no parent retention certificate and cannot be entered"
    );
    let after = body(fixture.request("GET", &detail_path, None).await).await;
    assert_eq!(after["availability"], "pending");
    assert_eq!(after["accepts_user_input"], false);
    fixture.state.manager.close().await.unwrap();
}
