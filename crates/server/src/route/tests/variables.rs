//! Variable editor transport: authority comes from the authenticated route and
//! State witness, while operation receipts distinguish identity from input digest.
use super::*;

async fn create(fixture: &Fixture) -> String {
    let response = fixture
        .request(
            "POST",
            "/api/v1/sessions",
            Some(json!({
                "project_id":"workspace", "operation_id":uuid::Uuid::new_v4().to_string()
            })),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    body(response).await["id"].as_str().unwrap().to_owned()
}

async fn inspection(fixture: &Fixture, id: &str) -> Value {
    let response = fixture
        .request(
            "GET",
            &format!("/api/v1/sessions/{id}/state/inspection"),
            None,
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    body(response).await
}

#[tokio::test]
async fn checked_variable_edit_duplicate_conflict_and_unknown_are_exact_and_body_free() {
    let fixture = Fixture::new(true).await;
    let id = create(&fixture).await;
    let initial = inspection(&fixture, &id).await;
    let path = format!("/api/v1/sessions/{id}/variables");
    let operation = uuid::Uuid::new_v4().to_string();
    let receipt_path = format!("{path}/operations/{operation}");
    let request = json!({"operation_id":operation,"expected":initial["witness"],"change":{
        "scoped_set":[{"name":"editor_secret","value":"ephemeral-route-fixture","visibility":"invisible","persistence":"ephemeral"}]
    }});
    let response = fixture.request("PATCH", &path, Some(request.clone())).await;
    assert_eq!(response.status(), StatusCode::OK);
    let applied = body(response).await;
    assert_eq!(applied["status"], "applied");
    assert_eq!(applied["commit"]["changed"], true);
    assert_eq!(applied["commit"]["before"], initial["witness"]);
    assert_ne!(applied["commit"]["after"], initial["witness"]);
    assert!(!applied.to_string().contains("ephemeral-route-fixture"));
    let current = inspection(&fixture, &id).await;
    assert_eq!(
        current["values"]["editor_secret"],
        "ephemeral-route-fixture"
    );
    assert_eq!(current["metadata"]["editor_secret"]["owner"], "client");
    assert_eq!(current["descriptors"]["editor_secret"]["editable"], true);
    let duplicate = body(fixture.request("PATCH", &path, Some(request.clone())).await).await;
    assert_eq!(duplicate, applied);
    assert_eq!(
        inspection(&fixture, &id).await["witness"],
        current["witness"]
    );
    let observed = body(fixture.request("GET", &receipt_path, None).await).await;
    assert_eq!(observed, applied);
    for field in ["payload_hash", "set", "change", "owner", "server_instance"] {
        assert!(observed.get(field).is_none(), "{field}");
    }
    let mut changed_input = request;
    changed_input["change"]["set"] = json!({"another":1});
    let conflict = fixture.request("PATCH", &path, Some(changed_input)).await;
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    assert_eq!(body(conflict).await["error"]["code"], "operation_conflict");
    let missing = body(
        fixture
            .request(
                "GET",
                &format!("{path}/operations/{}", uuid::Uuid::new_v4()),
                None,
            )
            .await,
    )
    .await;
    assert_eq!(missing["status"], "unknown");
    let stale_id = uuid::Uuid::new_v4().to_string();
    let stale = json!({"operation_id":stale_id,"expected":initial["witness"],"change":{"set":{"must_not_commit":true}}});
    let rejected = fixture.request("PATCH", &path, Some(stale.clone())).await;
    assert_eq!(rejected.status(), StatusCode::CONFLICT);
    assert_eq!(body(rejected).await["error"]["code"], "variable_conflict");
    let rejected = body(
        fixture
            .request("GET", &format!("{path}/operations/{stale_id}"), None)
            .await,
    )
    .await;
    assert_eq!(rejected["status"], "rejected");
    assert_eq!(rejected["reason"]["code"], "variable_conflict");
    assert!(
        inspection(&fixture, &id).await["values"]
            .get("must_not_commit")
            .is_none()
    );
    let duplicate_rejected = fixture.request("PATCH", &path, Some(stale)).await;
    assert_eq!(duplicate_rejected.status(), StatusCode::CONFLICT);
    fixture.state.manager.close().await.unwrap();
}

#[tokio::test]
async fn variable_routes_authenticate_owner_and_reject_payload_authority() {
    let fixture = Fixture::new(true).await;
    let id = create(&fixture).await;
    let initial = inspection(&fixture, &id).await;
    let path = format!("/api/v1/sessions/{id}/variables");
    let valid = json!({"operation_id":uuid::Uuid::new_v4().to_string(),"expected":initial["witness"],"change":{"set":{"x":1}}});
    for field in ["owner", "role", "session_id", "set", "scoped_set"] {
        let mut spoof = valid.clone();
        spoof[field] = json!("runtime");
        assert_eq!(
            fixture.request("PATCH", &path, Some(spoof)).await.status(),
            StatusCode::BAD_REQUEST
        );
    }
    for field in ["owner", "role", "session_id"] {
        let mut spoof = valid.clone();
        spoof["change"][field] = json!("runtime");
        assert_eq!(
            fixture.request("PATCH", &path, Some(spoof)).await.status(),
            StatusCode::BAD_REQUEST
        );
    }
    let mut scoped = valid;
    scoped["change"] = json!({"scoped_set":[{"name":"x","value":1,"visibility":"visible","persistence":"persisted","owner":"runtime"}]});
    assert_eq!(
        fixture.request("PATCH", &path, Some(scoped)).await.status(),
        StatusCode::BAD_REQUEST
    );
    let other = fixture.state.auth.login_for_test("another-owner");
    for route in [
        format!("/api/v1/sessions/{id}/state/inspection"),
        format!("{path}/operations/{}", uuid::Uuid::new_v4()),
    ] {
        let response = fixture
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&route)
                    .header("cookie", format!("microcrab_session={other}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    let foreign_write = fixture.router.clone().oneshot(Request::builder()
        .method("PATCH").uri(&path)
        .header("cookie", format!("microcrab_session={other}"))
        .header("origin", "http://127.0.0.1:4096")
        .header("content-type", "application/json")
        .body(Body::from(json!({"operation_id":uuid::Uuid::new_v4().to_string(),"expected":initial["witness"],"change":{"set":{"foreign_write":true}}}).to_string())).unwrap())
        .await.unwrap();
    assert_eq!(foreign_write.status(), StatusCode::NOT_FOUND);
    let unauth = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/sessions/{id}/state/inspection"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);
    fixture.state.manager.close().await.unwrap();
}

#[tokio::test]
async fn legacy_scoped_changes_are_governed_and_reopen_does_not_certify_old_commits() {
    let fixture = Fixture::new(true).await;
    let id = create(&fixture).await;
    let path = format!("/api/v1/sessions/{id}/variables");
    let scoped = json!({"scoped_set":[{"name":"retained_scope","value":1,"visibility":"invisible","persistence":"persisted","editors":["client","agent"]}]});
    assert_eq!(
        fixture.request("PATCH", &path, Some(scoped)).await.status(),
        StatusCode::OK
    );
    let original = inspection(&fixture, &id).await;
    let operation = uuid::Uuid::new_v4().to_string();
    let patch = json!({"operation_id":operation,"expected":original["witness"],"change":{"set":{"retained_scope":2}}});
    assert_eq!(
        fixture
            .request("PATCH", &path, Some(patch.clone()))
            .await
            .status(),
        StatusCode::OK
    );
    let changed = inspection(&fixture, &id).await;
    assert_eq!(
        changed["metadata"]["retained_scope"],
        original["metadata"]["retained_scope"]
    );
    assert_eq!(changed["values"]["retained_scope"], 2);
    assert_eq!(
        fixture
            .request(
                "DELETE",
                &format!("/api/v1/sessions/{id}/runtime"),
                Some(json!({}))
            )
            .await
            .status(),
        StatusCode::OK
    );
    let closed = fixture
        .request("PATCH", &path, Some(json!({"set":{"closed_write":true}})))
        .await;
    assert_eq!(closed.status(), StatusCode::CONFLICT);
    let reopened = inspection(&fixture, &id).await;
    assert_ne!(
        reopened["witness"]["runtime_instance_id"],
        changed["witness"]["runtime_instance_id"]
    );
    let receipt = body(
        fixture
            .request("GET", &format!("{path}/operations/{operation}"), None)
            .await,
    )
    .await;
    assert_eq!(receipt["status"], "unknown");
    let duplicate = body(fixture.request("PATCH", &path, Some(patch)).await).await;
    assert_eq!(duplicate["status"], "unknown");
    assert_eq!(
        inspection(&fixture, &id).await["witness"],
        reopened["witness"]
    );
    fixture.state.manager.close().await.unwrap();
}
