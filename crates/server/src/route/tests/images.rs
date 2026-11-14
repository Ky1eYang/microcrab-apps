//! Cookie-authorized image delivery stays binary across runtime closure.
use super::*;

struct ImageClient;
#[async_trait::async_trait]
impl microcrab_session::Client for ImageClient {
    async fn complete(
        &self,
        _: &microcrab_transport::Content,
        _: &dyn microcrab_auditor::Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        Ok(microcrab_protocol::Response {
            provider_metadata: None,
            content: "image fixture completed".into(),
            tool_calls: vec![],
            usage: microcrab_protocol::Usage::default(),
        })
    }
}

#[tokio::test]
async fn image_endpoint_authenticates_owner_and_reads_existing_history_after_runtime_close() {
    let fixture = Fixture::with_client(true, Some(Arc::new(ImageClient))).await;
    let session = body(fixture.request("POST", "/api/v1/sessions", Some(json!({"project_id":"workspace","operation_id":uuid::Uuid::new_v4().to_string()}))).await).await;
    let id = session["id"].as_str().unwrap();
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "cookie",
        format!("microcrab_session={}", fixture.token)
            .parse()
            .unwrap(),
    );
    let principal = fixture.state.auth.authenticate(&headers).unwrap();
    let bytes = {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD
            .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j7S8AAAAASUVORK5CYII=")
            .unwrap()
    };
    let command = serde_json::from_value(json!({
        "v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),
        "method":"turn.submit","session_id":id,
        "params":{"operation_id":uuid::Uuid::new_v4().to_string(),"input":{"parts":[
            {"type":"text","text":"attached"},
            {"type":"image","source":{"type":"data","media_type":"image/png","data":bytes}}
        ]}}
    }))
    .unwrap();
    let mut subscription = fixture
        .state
        .manager
        .subscribe(&principal, id)
        .await
        .unwrap();
    let mut view = subscription.snapshot.clone();
    fixture
        .state
        .manager
        .command(&principal, &command)
        .await
        .unwrap();
    // Wait for the deterministic Client's actual canonical publication. An
    // unreachable HTTP provider retries before settlement and is not a witness
    // for this test's binary ownership/recovery contract.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let event = subscription.receiver.recv().await.unwrap();
            if crate::manager::publication::apply(
                &mut view,
                event.event.name(),
                event.event.payload(),
            ) && view["session"]["status"] == "idle"
                && view["messages"].as_array().is_some_and(|messages| {
                    messages.iter().any(|message| {
                        message["role"] == "assistant"
                            && message["text"] == "image fixture completed"
                    })
                })
            {
                break;
            }
        }
    })
    .await
    .unwrap();
    let blocks = serde_json::from_value(json!({"v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),"method":"session.blocks","session_id":id,"params":{}})).unwrap();
    let page = fixture
        .state
        .manager
        .command(&principal, &blocks)
        .await
        .unwrap();
    let message = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "user")
        .expect("settled turn publishes its canonical image-bearing user message");
    let url = message["parts"][1]["source"]["url"].as_str().unwrap();
    assert!(url.starts_with(&format!("/api/v1/sessions/{id}/images/message-")));
    assert!(url.ends_with("/1"));
    for closed in [false, true] {
        if closed {
            let response = fixture
                .request(
                    "DELETE",
                    &format!("/api/v1/sessions/{id}/runtime"),
                    Some(json!({})),
                )
                .await;
            assert_eq!(response.status(), StatusCode::OK);
        }
        let response = fixture.request("GET", url, None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "image/png");
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        assert!(
            !response
                .headers()
                .contains_key("access-control-allow-origin")
        );
        assert_eq!(
            to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
            bytes.as_slice()
        );
    }
    let no_cookie = fixture
        .router
        .clone()
        .oneshot(Request::builder().uri(url).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(no_cookie.status(), StatusCode::UNAUTHORIZED);
    let token = fixture.state.auth.login_for_test("intruder");
    let denied = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri(url)
                .header("cookie", format!("microcrab_session={token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::NOT_FOUND);
    assert_eq!(body(denied).await["error"]["code"], "not_found");
    let prefix = url.rsplit_once('/').unwrap().0;
    for index in [0, 2, usize::MAX] {
        let response = fixture
            .request("GET", &format!("{prefix}/{index}"), None)
            .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    let response = fixture
        .request("GET", &format!("{url}?path=private"), None)
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    fixture.state.manager.close().await.unwrap();
}
