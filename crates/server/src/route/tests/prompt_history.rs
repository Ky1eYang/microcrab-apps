//! Exercise the opt-in route against canonical owner history and real storage.
use super::*;

fn url(id: &str, view: &Value) -> String {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair(
            "history_lineage",
            view["history_view"]["lineage"].as_str().unwrap(),
        )
        .append_pair("epoch", &view["history_view"]["epoch"].to_string())
        .finish();
    format!("/api/v1/sessions/{id}/history/prompt?{query}")
}

async fn create(fixture: &Fixture) -> Value {
    let response = fixture.request("POST", "/api/v1/sessions", Some(json!({
        "project_id":"workspace",
        "operation_id":uuid::Uuid::new_v4().to_string(),
        "system_template":"F5 {{ client_prompt }}",
        "client_prompt":"  {{ literal }}\n",
        "client_tools":[{
            "name":"f5_example",
            "description":"Original declared tool",
            "parameters":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]},
            "permissions":[{"tool":{"kind":"exact","value":"f5_example"},"args":[],"action":"allow"}]
        }]
    }))).await;
    let status = response.status();
    let session = body(response).await;
    assert_eq!(status, StatusCode::OK, "{session}");
    session
}

async fn read(fixture: &Fixture, uri: &str) -> Value {
    let response = fixture.request("GET", uri, None).await;
    let status = response.status();
    let value = body(response).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    value
}

#[tokio::test]
async fn frozen_prompt_is_exact_owner_only_and_survives_dormancy() {
    let fixture = Fixture::new(true).await;
    let session = create(&fixture).await;
    let id = session["id"].as_str().unwrap();
    let view = fixture.inspect(id).await.unwrap();
    assert!(!view.to_string().contains("{{ literal }}"));
    let uri = url(id, &view);
    let active = read(&fixture, &uri).await;
    assert_eq!(active["session_id"], id);
    assert_eq!(active["history_lineage"], view["history_view"]["lineage"]);
    assert_eq!(active["epoch"], view["history_view"]["epoch"]);
    assert_eq!(active["status"], "available");
    assert_eq!(active["system"], "F5   {{ literal }}\n");
    assert_eq!(active.as_object().unwrap().len(), 6);
    let tool = active["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "f5_example")
        .unwrap();
    assert_eq!(
        tool,
        &json!({
            "name":"f5_example",
            "description":"Original declared tool",
            "parameters":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}
        })
    );

    let other = fixture.state.auth.login_for_test("intruder");
    for (token, expected) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some(other), StatusCode::NOT_FOUND),
    ] {
        let mut request = Request::builder().uri(&uri);
        if let Some(token) = token {
            request = request.header("cookie", format!("microcrab_session={token}"));
        }
        let denied = fixture
            .router
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(denied.status(), expected);
        assert!(body(denied).await.get("system").is_none());
    }
    let mut wrong_epoch = view.clone();
    wrong_epoch["history_view"]["epoch"] = json!(999);
    let wrong = url(id, &wrong_epoch);
    let stale = fixture.request("GET", &wrong, None).await;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    assert_eq!(body(stale).await["error"]["code"], "stale_history");
    let malformed = fixture
        .request("GET", &format!("{uri}&epoch=0"), None)
        .await;
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        fixture
            .request("GET", &format!("{uri}&extra=x"), None)
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );

    let other_session = create(&fixture).await;
    let other_id = other_session["id"].as_str().unwrap();
    let cross_session = fixture
        .request("GET", &uri.replacen(id, other_id, 1), None)
        .await;
    assert_eq!(cross_session.status(), StatusCode::CONFLICT);

    let closed = fixture
        .request(
            "DELETE",
            &format!("/api/v1/sessions/{id}/runtime"),
            Some(json!({})),
        )
        .await;
    assert_eq!(closed.status(), StatusCode::OK);
    assert_eq!(
        fixture.request("GET", &uri, None).await.status(),
        StatusCode::CONFLICT
    );
    let view = fixture.inspect(id).await.unwrap();
    let dormant = read(&fixture, &url(id, &view)).await;
    assert_eq!(dormant["system"], active["system"]);
    assert_eq!(dormant["tools"], active["tools"]);
    assert_ne!(dormant["history_lineage"], active["history_lineage"]);
    // An unarchived dormant Session is reported as idle. The canonical detached
    // lineage distinguishes a dormant projection from the live Journal lineage.
    assert!(
        dormant["history_lineage"]
            .as_str()
            .unwrap()
            .starts_with("detached:")
    );
    let after = fixture.inspect(id).await.unwrap();
    assert_eq!(after["history_view"]["lineage"], dormant["history_lineage"]);
    assert_eq!(after["history"], view["history"]);
    assert_eq!(after["host_connected"], false);
    fixture.state.manager.close().await.unwrap();
}
