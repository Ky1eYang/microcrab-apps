//! Citation HTTP completion retains the existing read-only auth vocabulary.
use super::*;
struct CitationClient;
#[async_trait::async_trait]
impl microcrab_session::Client for CitationClient {
    async fn complete(
        &self,
        _: &microcrab_transport::Content,
        _: &dyn microcrab_auditor::Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        Ok(microcrab_protocol::Response {
            content: "[report](report.txt) [svg](picture.svg)".into(),
            provider_metadata: None,
            tool_calls: vec![],
            usage: microcrab_protocol::Usage::default(),
        })
    }
}
#[tokio::test]
async fn citations_route_authenticates_delivers_safe_headers_and_rejects_revoked_reads() {
    let fixture = Fixture::with_client(true, Some(Arc::new(CitationClient))).await;
    std::fs::write(fixture._root.path().join("report.txt"), b"current report").unwrap();
    std::fs::write(fixture._root.path().join("picture.svg"), b"<svg/>").unwrap();
    let created = body(fixture.request("POST", "/api/v1/sessions", Some(json!({"project_id":"workspace","operation_id":uuid::Uuid::new_v4().to_string()}))).await).await;
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
    let submit = serde_json::from_value(json!({"v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),"method":"turn.submit","session_id":id,"params":{"operation_id":uuid::Uuid::new_v4().to_string(),"input":{"parts":[{"type":"text","text":"produce report"}]}}})).unwrap();
    fixture
        .state
        .manager
        .command(&principal, &submit)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let event = subscription.receiver.recv().await.unwrap();
            if crate::manager::publication::apply(
                &mut view,
                event.event.name(),
                event.event.payload(),
            ) && view["session"]["status"] == "idle"
                && view["messages"].as_array().is_some_and(|messages| {
                    messages
                        .iter()
                        .any(|message| message["role"] == "assistant")
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
        .find(|item| item["role"] == "assistant")
        .unwrap()["id"]
        .as_str()
        .unwrap();
    let lineage = page["history_view"]["lineage"].as_str().unwrap();
    let url = |href: &str, history: &str| {
        let query: String = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("href", href)
            .append_pair("history_lineage", history)
            .finish();
        format!("/api/v1/sessions/{id}/files/{message}?{query}")
    };
    for href in ["report.txt", "picture.svg"] {
        let response = fixture.request("GET", &url(href, lineage), None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            "application/octet-stream"
        );
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
                .starts_with("attachment;")
        );
    }
    assert_eq!(
        fixture
            .request("GET", &url("uncited.txt", lineage), None)
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        fixture
            .request("GET", &url("report.txt", "stale"), None)
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    let missing_cookie = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri(url("report.txt", lineage))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing_cookie.status(), StatusCode::UNAUTHORIZED);
    let token = fixture.state.auth.login_for_test("intruder");
    let stranger = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri(url("report.txt", lineage))
                .header("cookie", format!("microcrab_session={token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stranger.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        fixture
            .request("POST", "/api/auth/logout", Some(json!({})))
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    let rejected = http::check_completion_auth(&fixture.state.auth, &principal, false).unwrap_err();
    assert_eq!(body(rejected).await["error"]["code"], "unauthenticated");
    assert_eq!(
        fixture
            .request("GET", &url("report.txt", lineage), None)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    fixture.state.manager.close().await.unwrap();
}
