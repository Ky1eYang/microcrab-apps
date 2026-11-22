use super::*;

#[tokio::test]
async fn execution_history_query_is_additive_and_stale_is_http_conflict() {
    let fixture = Fixture::new(true).await;
    let created = fixture
        .request(
            "POST",
            "/api/v1/sessions",
            Some(json!({
                "project_id":"workspace", "operation_id":uuid::Uuid::new_v4().to_string()
            })),
        )
        .await;
    assert_eq!(created.status(), StatusCode::OK);
    let session = body(created).await;
    let id = session["id"].as_str().unwrap();
    let uri = format!("/api/v1/sessions/{id}/executions?cursor=0&limit=1");
    let first = fixture.request("GET", &uri, None).await;
    assert_eq!(first.status(), StatusCode::OK);
    let first = body(first).await;
    let witness = &first["witness"];
    let revision = witness["rows_revision"].as_str().unwrap();
    assert_eq!(witness["session_id"], id);
    assert_eq!(first["items"], json!([]));
    assert!(first["next_cursor"].is_null());
    let repeated = fixture
        .request("GET", &format!("{uri}&expected_revision={revision}"), None)
        .await;
    assert_eq!(repeated.status(), StatusCode::OK);
    assert_eq!(body(repeated).await, first);
    let stale = fixture
        .request("GET", &format!("{uri}&expected_revision=foreign"), None)
        .await;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    assert_eq!(
        body(stale).await["error"]["code"],
        "execution_history_changed"
    );
    let bad_client_query =
        format!("/api/v1/sessions/{id}/client-tools/calls?expected_revision={revision}");
    assert_eq!(
        fixture
            .request("GET", &bad_client_query, None)
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    fixture.state.manager.close().await.unwrap();
}
