//! Question previews authenticate independently and never borrow message citations.
use super::*;
use base64::Engine;
struct QuestionClient;
#[async_trait::async_trait]
impl microcrab_session::Client for QuestionClient {
    async fn complete(
        &self,
        content: &microcrab_transport::Content,
        _: &dyn microcrab_auditor::Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        let _ = content;
        Ok(microcrab_protocol::Response {
            content: String::new(),
            provider_metadata: None,
            tool_calls: vec![microcrab_protocol::ToolCall {
                provider_metadata: None,
                id: "image-question".into(),
                name: "ask_user_question".into(),
                arguments: json!({"questions":[{"question":"![image](picture.png)"}]}),
            }],
            usage: microcrab_protocol::Usage::default(),
        })
    }
}
#[tokio::test]
async fn question_images_route_checks_lease_and_delivers_only_safe_raster_headers() {
    let fixture = Fixture::with_client(true, Some(Arc::new(QuestionClient))).await;
    let bytes = base64::engine::general_purpose::STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j7S8AAAAASUVORK5CYII=").unwrap();
    std::fs::write(fixture._root.path().join("picture.png"), &bytes).unwrap();
    let created = body(fixture.request("POST", "/api/v1/sessions", Some(json!({"project_id":"workspace","operation_id":uuid::Uuid::new_v4().to_string(),
        "client_tools":[{"name":"ask_user_question","description":"Question route fixture","parameters":{"type":"object"},"reclaimable":true,
            "permissions":[{"tool":{"kind":"exact","value":"ask_user_question"},"args":[],"action":"allow"}]}]}))).await).await;
    let id = created["id"].as_str().unwrap();
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "cookie",
        format!("microcrab_session={}", fixture.token)
            .parse()
            .unwrap(),
    );
    let principal = fixture.state.auth.authenticate(&headers).unwrap();
    let mut subscription = fixture
        .state
        .manager
        .subscribe(&principal, id)
        .await
        .unwrap();
    let mut view = subscription.snapshot.clone();
    let command = |method: &str, params: Value| {
        serde_json::from_value(json!({"v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),"method":method,"session_id":id,"params":params})).unwrap()
    };
    let bound = fixture.state.manager.client_tool_command(&principal, "question-http-socket", &command("client_tools.bind", json!({"client_id":"question-http-browser","catalog_digest":view["client_tools"]["catalog_digest"]}))).await.unwrap();
    let lease = bound["lease_id"].as_str().unwrap();
    fixture.state.manager.command(&principal, &command("turn.submit", json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":{"parts":[{"type":"text","text":"ask"}]}}))).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while view["client_tools"]["pending"]
            .as_array()
            .is_none_or(|calls| calls.is_empty())
        {
            let event = subscription.receiver.recv().await.unwrap();
            crate::manager::publication::apply(
                &mut view,
                event.event.name(),
                event.event.payload(),
            );
        }
    })
    .await
    .unwrap();
    let invocation = view["client_tools"]["pending"][0]["invocation_id"]
        .as_str()
        .unwrap();
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair(
            "history_lineage",
            view["history_view"]["lineage"].as_str().unwrap(),
        )
        .append_pair("target_session_id", id)
        .append_pair("href", "picture.png")
        .finish();
    let path = format!("/api/v1/sessions/{id}/questions/{invocation}/image?{query}");
    assert_eq!(
        fixture.request("GET", &path, None).await.status(),
        StatusCode::FORBIDDEN
    );
    let request = |cookie: Option<&str>, lease: &str| {
        let mut builder = Request::builder()
            .uri(&path)
            .header("x-microcrab-executor-lease", lease);
        if let Some(cookie) = cookie {
            builder = builder.header("cookie", format!("microcrab_session={cookie}"));
        }
        builder.body(Body::empty()).unwrap()
    };
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request(None, lease))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request(Some(&fixture.token), "wrong-lease"))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    let response = fixture
        .router
        .clone()
        .oneshot(request(Some(&fixture.token), lease))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "image/png");
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    assert_eq!(
        response.headers()["content-security-policy"],
        "default-src 'none'; sandbox"
    );
    assert!(
        response.headers()["content-disposition"]
            .to_str()
            .unwrap()
            .starts_with("inline;")
    );
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
        bytes
    );
    fixture.state.manager.close().await.unwrap();
}
