//! Raw upload route admission and the complete upload/reference/turn/history path.
use super::*;
struct AttachmentClient;
#[async_trait::async_trait]
impl microcrab_session::Client for AttachmentClient {
    async fn complete(
        &self,
        content: &microcrab_transport::Content,
        _: &dyn microcrab_auditor::Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        assert!(content.items.iter().any(|item| matches!(item, microcrab_transport::ContentItem::User {parts,..} if parts.iter().any(|part| matches!(part, microcrab_protocol::InputPart::Image { source: microcrab_protocol::ImageSource::Data {data,..} } if data.len() == 128 * 1024)))));
        Ok(microcrab_protocol::Response {
            provider_metadata: None,
            content: "attachment resolved".into(),
            tool_calls: vec![],
            usage: Default::default(),
        })
    }
}
fn png() -> Vec<u8> {
    use base64::Engine as _;
    let mut data = base64::engine::general_purpose::STANDARD
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j7S8AAAAASUVORK5CYII=")
        .unwrap();
    data.resize(128 * 1024, 0);
    data
}
async fn raw(
    fixture: &Fixture,
    id: &str,
    token: Option<&str>,
    origin: bool,
    mime: &str,
    data: Vec<u8>,
    declared: Option<usize>,
) -> Response {
    let mut request = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/sessions/{id}/attachments"))
        .header("content-type", mime);
    if let Some(token) = token {
        request = request.header("cookie", format!("microcrab_session={token}"));
    }
    if origin {
        request = request.header("origin", "http://127.0.0.1:4096");
    }
    if let Some(length) = declared {
        request = request.header("content-length", length);
    }
    fixture
        .router
        .clone()
        .oneshot(request.body(Body::from(data)).unwrap())
        .await
        .unwrap()
}
#[tokio::test]
async fn raw_upload_auth_byte_limits_scope_and_turn_resolution() {
    let fixture = Fixture::with_client(true, Some(Arc::new(AttachmentClient))).await;
    let create =
        || json!({"project_id":"workspace","operation_id":uuid::Uuid::new_v4().to_string()});
    let session = body(
        fixture
            .request("POST", "/api/v1/sessions", Some(create()))
            .await,
    )
    .await;
    let id = session["id"].as_str().unwrap();
    assert_eq!(
        raw(&fixture, id, None, true, "image/png", png(), None)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        raw(
            &fixture,
            id,
            Some(&fixture.token),
            false,
            "image/png",
            png(),
            None
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        raw(
            &fixture,
            id,
            Some(&fixture.token),
            true,
            "application/json",
            png(),
            None
        )
        .await
        .status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    let intruder = fixture.state.auth.login_for_test("intruder");
    assert_eq!(
        raw(
            &fixture,
            id,
            Some(&intruder),
            true,
            "image/png",
            png(),
            None
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    let cap = microcrab_session::admission::image::MAX_IMAGE_BYTES;
    assert_eq!(
        raw(
            &fixture,
            id,
            Some(&fixture.token),
            true,
            "image/png",
            vec![],
            Some(cap + 1)
        )
        .await
        .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(
        raw(
            &fixture,
            id,
            Some(&fixture.token),
            true,
            "image/png",
            vec![0; cap + 1],
            None
        )
        .await
        .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let response = raw(
        &fixture,
        id,
        Some(&fixture.token),
        true,
        "image/png",
        png(),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let uploaded = body(response).await;
    assert_eq!(uploaded["bytes"], 128 * 1024);
    assert_eq!(uploaded["width"], 1);
    let attachment_id = uploaded["attachment_id"].as_str().unwrap();
    let url = format!("/api/v1/sessions/{id}/attachments/{attachment_id}");
    let preview = fixture.request("GET", &url, None).await;
    assert_eq!(preview.status(), StatusCode::OK);
    assert_eq!(preview.headers()["content-type"], "image/png");
    assert_eq!(preview.headers()["x-content-type-options"], "nosniff");
    assert_eq!(
        to_bytes(preview.into_body(), 256 * 1024)
            .await
            .unwrap()
            .as_ref(),
        png().as_slice()
    );
    let other = body(
        fixture
            .request("POST", "/api/v1/sessions", Some(create()))
            .await,
    )
    .await;
    let other_url = format!(
        "/api/v1/sessions/{}/attachments/{attachment_id}",
        other["id"].as_str().unwrap()
    );
    assert_eq!(
        fixture.request("GET", &other_url, None).await.status(),
        StatusCode::NOT_FOUND
    );
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
    let command = serde_json::from_value(json!({"v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),"method":"turn.submit","session_id":id,"params":{"operation_id":uuid::Uuid::new_v4().to_string(),"input":{"parts":[{"type":"image","source":{"type":"attachment","attachment_id":attachment_id}}]}}})).unwrap();
    fixture
        .state
        .manager
        .command(&principal, &command)
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
                        .any(|message| message["text"] == "attachment resolved")
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
    let user = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "user")
        .unwrap();
    let history_url = user["parts"][0]["source"]["url"].as_str().unwrap();
    let historical = fixture.request("GET", history_url, None).await;
    assert_eq!(historical.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(historical.into_body(), 256 * 1024)
            .await
            .unwrap()
            .as_ref(),
        png().as_slice()
    );
    fixture.state.manager.close().await.unwrap();
}
