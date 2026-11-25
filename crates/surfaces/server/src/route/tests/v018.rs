//! Public HTTP contracts for the v0.1.8 client catalog and audit queries.
use super::*;

#[tokio::test]
async fn client_catalog_epoch_and_closed_audit_use_versioned_http() {
    struct Capture(tokio::sync::mpsc::UnboundedSender<microcrab_transport::Content>);
    #[async_trait::async_trait]
    impl microcrab_session::Client for Capture {
        async fn complete(
            &self,
            content: &microcrab_transport::Content,
            _: &dyn microcrab_auditor::Auditor,
            _: &dyn microcrab_auditor::VariableView,
            _: tokio_util::sync::CancellationToken,
        ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
            self.0.send(content.clone()).unwrap();
            Ok(microcrab_protocol::Response {
                content: "catalog boundary answer".into(),
                tool_calls: vec![],
                usage: Default::default(),
                provider_metadata: None,
            })
        }
    }
    let (sent, mut requests) = tokio::sync::mpsc::unbounded_channel();
    let fixture = Fixture::with_client(true, Some(Arc::new(Capture(sent)))).await;
    let definition = json!({"name":"client_echo","description":"Browser owned echo","parameters":{"type":"object","properties":{},"additionalProperties":false},"permissions":[{"tool":{"kind":"exact","value":"client_echo"},"args":[],"action":"allow"}]});
    let created = fixture.request("POST", "/api/v1/sessions", Some(json!({"project_id":"workspace","operation_id":uuid::Uuid::new_v4().to_string(),"client_tools":[definition]}))).await;
    assert_eq!(created.status(), StatusCode::OK);
    let created = body(created).await;
    let id = created["id"].as_str().unwrap();
    let catalog_path = format!("/api/v1/sessions/{id}/client-tools");
    let catalog = fixture.inspect(id).await.unwrap();
    assert_eq!(
        catalog["client_tools"]["definitions"][0]["name"],
        "client_echo"
    );
    let state = fixture
        .request("GET", &format!("/api/v1/sessions/{id}/state"), None)
        .await;
    assert_eq!(state.status(), StatusCode::OK);
    assert!(body(state).await.get("permission").is_none());
    let current = fixture.request("GET", &format!("/api/v1/sessions/{id}/permission?view=current"), None).await;
    assert_eq!(current.status(), StatusCode::OK);
    assert!(body(current).await["current"]["tools"].as_array().unwrap().iter().any(|row| row["name"] == "client_echo" && row["policy"]["action"] == "allow"));
    async fn submit(
        fixture: &Fixture,
        id: &str,
        requests: &mut tokio::sync::mpsc::UnboundedReceiver<microcrab_transport::Content>,
    ) -> microcrab_transport::Content {
        let command = serde_json::from_value(json!({"v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),"method":"turn.submit","session_id":id,"params":{"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text("catalog boundary")}})).unwrap();
        fixture
            .state
            .manager
            .command(
                &crate::auth::Principal::for_test(&fixture.owner_of(&fixture.token)),
                &command,
            )
            .await
            .unwrap();
        let content = tokio::time::timeout(std::time::Duration::from_secs(5), requests.recv())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if fixture.inspect(id).await.unwrap()["session"]["status"] == "idle" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        content
    }
    let before = submit(&fixture, id, &mut requests).await;
    assert!(before.tools.iter().any(|tool| tool.name == "client_echo"));
    let admitted = fixture.inspect(id).await.unwrap();
    let expected_revision = &admitted["session"]["revision"];
    let changed = fixture
        .request(
            "POST",
            &catalog_path,
            Some(json!({"expected_revision":expected_revision,"definitions":[]})),
        )
        .await;
    assert_eq!(changed.status(), StatusCode::OK);
    let changed = body(changed).await;
    assert_eq!(changed["pending_catalog_change"], true);
    assert_eq!(
        changed["definitions"],
        catalog["client_tools"]["definitions"]
    );
    assert_eq!(
        changed["catalog_digest"],
        catalog["client_tools"]["catalog_digest"]
    );
    assert!(
        requests.try_recv().is_err(),
        "configuration alone issues no request"
    );
    assert!(
        before.tools.iter().any(|tool| tool.name == "client_echo"),
        "the frozen issuing Context retains its catalog"
    );
    let stale = fixture
        .request(
            "POST",
            &catalog_path,
            Some(json!({"expected_revision":expected_revision,"definitions":[]})),
        )
        .await;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    let after = submit(&fixture, id, &mut requests).await;
    assert!(!after.tools.iter().any(|tool| tool.name == "client_echo"));
    assert_eq!(after.epoch, before.epoch + 1);
    assert_eq!(
        fixture.inspect(id).await.unwrap()["client_tools"]["definitions"],
        json!([])
    );
    let close = fixture
        .request(
            "DELETE",
            &format!("/api/v1/sessions/{id}/runtime"),
            Some(json!({})),
        )
        .await;
    assert_eq!(close.status(), StatusCode::OK);
    let audit_path = format!("/api/v1/sessions/{id}/audit/requests");
    let audit = fixture
        .request(
            "POST",
            &audit_path,
            Some(json!({"epoch":0,"turn":1,"round":1})),
        )
        .await;
    assert_eq!(audit.status(), StatusCode::OK);
    assert_eq!(body(audit).await["status"], "missing");
    let calls = fixture
        .request(
            "GET",
            &format!("{catalog_path}/calls?cursor=0&limit=64"),
            None,
        )
        .await;
    assert_eq!(calls.status(), StatusCode::OK);
    assert_eq!(body(calls).await["calls"], json!([]));
    let intruder = fixture.state.auth.login_for_test("intruder");
    let response = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(audit_path)
                .header("cookie", format!("microcrab_session={intruder}"))
                .header("origin", "http://127.0.0.1:4096")
                .header("content-type", "application/json")
                .body(Body::from(json!({"epoch":0}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    fixture.state.manager.close().await.unwrap();
}
