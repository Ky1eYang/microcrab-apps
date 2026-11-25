//! Private byte variables have a write-only raw route and live metadata-only inspection.
use super::{
    attachments::png,
    variables::{create, inspection},
    *,
};
use axum::{body::Bytes, http::request::Builder};
use std::{
    convert::Infallible,
    sync::atomic::{AtomicBool, Ordering},
};

fn request(method: &str, path: &str, witness: &Value, token: Option<&str>) -> Builder {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header(
            "x-microcrab-runtime-instance-id",
            witness["runtime_instance_id"].as_str().unwrap(),
        )
        .header(
            "x-microcrab-variables-revision",
            witness["variables_revision"].as_str().unwrap(),
        );
    if let Some(token) = token {
        request = request.header("cookie", format!("microcrab_session={token}"));
    }
    request
}

async fn send(fixture: &Fixture, request: Builder, body: Body) -> Response {
    fixture
        .router
        .clone()
        .oneshot(request.body(body).unwrap())
        .await
        .unwrap()
}

async fn put(
    fixture: &Fixture,
    path: &str,
    witness: &Value,
    mime: &str,
    image: Vec<u8>,
) -> Response {
    send(
        fixture,
        request("PUT", path, witness, Some(&fixture.token))
            .header("origin", "http://127.0.0.1:4096")
            .header("content-type", mime)
            .header("content-length", image.len()),
        Body::from(image),
    )
    .await
}

async fn remove(fixture: &Fixture, path: &str, witness: &Value) -> Response {
    send(
        fixture,
        request("DELETE", path, witness, Some(&fixture.token))
            .header("origin", "http://127.0.0.1:4096"),
        Body::empty(),
    )
    .await
}

#[tokio::test]
async fn image_variable_inspection_receipts_and_legacy_views_never_include_bytes() {
    let fixture = Fixture::new(true).await;
    let id = create(&fixture).await;
    let state = format!("/api/v1/sessions/{id}/state");
    let path = format!("/api/v1/sessions/{id}/variables/bytes/private_image");
    let view = fixture.inspect(&id).await.unwrap();
    let prompt = format!(
        "/api/v1/sessions/{id}/history/prompt?history_lineage={}&epoch={}",
        view["history_view"]["lineage"].as_str().unwrap(),
        view["history_view"]["epoch"]
    );
    let catalog = body(fixture.request("GET", &prompt, None).await).await;
    assert_eq!(catalog["status"], "available");
    assert!(
        catalog["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "view_image")
    );
    let initial = inspection(&fixture, &id).await;
    assert!(initial.get("bytes").is_none());
    let image = png();
    let response = put(
        &fixture,
        &path,
        &initial["witness"],
        "image/png",
        image.clone(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let receipt = body(response).await;
    let metadata = &receipt["metadata"];
    assert_eq!(receipt.as_object().unwrap().len(), 2);
    assert_eq!(receipt["commit"]["before"], initial["witness"]);
    assert_eq!(receipt["commit"]["changed"], true);
    assert_eq!(metadata.as_object().unwrap().len(), 8);
    assert_eq!(metadata["name"], "private_image");
    assert_eq!(metadata["kind"], "bytes");
    assert_eq!(metadata["lifecycle"], "ephemeral");
    assert_eq!(metadata["availability"], "available");
    assert_eq!(metadata["media_type"], "image/png");
    assert_eq!(metadata["byte_length"], image.len() as u64);
    assert_eq!(
        metadata["revision"],
        receipt["commit"]["after"]["variables_revision"]
    );
    assert!(!metadata["selector"].as_str().unwrap().is_empty());

    let current = inspection(&fixture, &id).await;
    assert_eq!(current["bytes"], json!({"private_image": metadata}));
    assert_eq!(current["witness"], receipt["commit"]["after"]);
    for key in ["values", "metadata", "descriptors"] {
        assert!(current[key].get("private_image").is_none());
    }
    assert_eq!(current["metadata"], initial["metadata"]);
    assert_eq!(current["descriptors"], initial["descriptors"]);
    for legacy in [&state, &format!("{state}/metadata")] {
        let response = fixture.request("GET", legacy, None).await;
        assert_eq!(response.status(), StatusCode::OK);
        let legacy = body(response).await.to_string();
        assert!(!legacy.contains("private_image"));
        assert!(!legacy.contains(metadata["selector"].as_str().unwrap()));
    }
    assert_eq!(
        fixture.request("GET", &path, None).await.status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    let fork = fixture
        .request(
            "POST",
            &format!("/api/v1/sessions/{id}/fork"),
            Some(json!({"operation_id":uuid::Uuid::new_v4().to_string()})),
        )
        .await;
    let status = fork.status();
    let fork = body(fork).await;
    assert_eq!(status, StatusCode::OK, "{fork}");
    let fork_state = inspection(&fixture, fork["id"].as_str().unwrap()).await;
    assert!(fork_state.get("bytes").is_none());
    let foreign = fixture.state.auth.login_for_test("foreign");
    let denied = send(
        &fixture,
        request("DELETE", &path, &current["witness"], Some(&foreign))
            .header("origin", "http://127.0.0.1:4096"),
        Body::empty(),
    )
    .await;
    assert_eq!(denied.status(), StatusCode::NOT_FOUND);
    // Replaying a lost acknowledgement with the same witness cannot apply again.
    let stale = put(&fixture, &path, &initial["witness"], "image/png", image).await;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    assert_eq!(body(stale).await["error"]["code"], "variable_conflict");
    let stale_delete = remove(&fixture, &path, &initial["witness"]).await;
    assert_eq!(stale_delete.status(), StatusCode::CONFLICT);
    assert_eq!(inspection(&fixture, &id).await, current);
    let deleted = remove(&fixture, &path, &current["witness"]).await;
    assert_eq!(deleted.status(), StatusCode::OK);
    assert_eq!(body(deleted).await["changed"], true);
    assert!(inspection(&fixture, &id).await.get("bytes").is_none());
    fixture.state.manager.close().await.unwrap();
}

#[tokio::test]
async fn preflight_rejects_unauthorized_and_unbounded_body_without_polling() {
    let fixture = Fixture::new(true).await;
    let id = create(&fixture).await;
    let initial = inspection(&fixture, &id).await;
    let path = format!("/api/v1/sessions/{id}/variables/bytes/image");
    let foreign = fixture.state.auth.login_for_test("foreign");
    let oversized = microcrab_session::admission::image::MAX_IMAGE_BYTES + 1;
    for (token, origin, mime, length, status) in [
        (None, true, "image/png", Some(64), StatusCode::UNAUTHORIZED),
        (
            Some(fixture.token.as_str()),
            false,
            "image/png",
            Some(64),
            StatusCode::FORBIDDEN,
        ),
        (
            Some(foreign.as_str()),
            true,
            "image/png",
            Some(64),
            StatusCode::NOT_FOUND,
        ),
        (
            Some(fixture.token.as_str()),
            true,
            "application/json",
            Some(64),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
        (
            Some(fixture.token.as_str()),
            true,
            "image/png",
            None,
            StatusCode::BAD_REQUEST,
        ),
        (
            Some(fixture.token.as_str()),
            true,
            "image/png",
            Some(oversized),
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
    ] {
        let mut req =
            request("PUT", &path, &initial["witness"], token).header("content-type", mime);
        if origin {
            req = req.header("origin", "http://127.0.0.1:4096");
        }
        if let Some(length) = length {
            req = req.header("content-length", length);
        }
        let polled = Arc::new(AtomicBool::new(false));
        let tracker = polled.clone();
        let stream = futures_util::stream::once(async move {
            tracker.store(true, Ordering::Relaxed);
            Ok::<_, Infallible>(Bytes::from_static(b"must not read"))
        });
        assert_eq!(
            send(&fixture, req, Body::from_stream(stream))
                .await
                .status(),
            status
        );
        assert!(!polled.load(Ordering::Relaxed));
    }
    assert_eq!(inspection(&fixture, &id).await, initial);
    fixture.state.manager.close().await.unwrap();
}

#[tokio::test]
async fn upload_rechecks_witness_after_the_body_without_locking_out_json_edits() {
    let fixture = Fixture::new(true).await;
    let id = create(&fixture).await;
    let initial = inspection(&fixture, &id).await;
    let variables = format!("/api/v1/sessions/{id}/variables");
    let path = format!("{variables}/bytes/image");
    let image = png();
    let req = request("PUT", &path, &initial["witness"], Some(&fixture.token))
        .header("origin", "http://127.0.0.1:4096")
        .header("content-type", "image/png")
        .header("content-length", image.len());
    let (polled, reading) = tokio::sync::oneshot::channel();
    let (resume, wait) = tokio::sync::oneshot::channel();
    let split = image.len() / 2;
    let first = Bytes::copy_from_slice(&image[..split]);
    let second = Bytes::copy_from_slice(&image[split..]);
    use futures_util::StreamExt as _;
    let stream = futures_util::stream::iter([Ok::<_, Infallible>(first)]).chain(
        futures_util::stream::once(async move {
            polled.send(()).unwrap();
            wait.await.unwrap();
            Ok(second)
        }),
    );
    let upload = fixture
        .router
        .clone()
        .oneshot(req.body(Body::from_stream(stream)).unwrap());
    let upload = tokio::spawn(upload);
    tokio::time::timeout(std::time::Duration::from_secs(5), reading)
        .await
        .unwrap()
        .unwrap();
    let edited = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fixture.request("PATCH", &variables, Some(json!({"set":{"json_edit":1}}))),
    )
    .await
    .unwrap();
    assert_eq!(edited.status(), StatusCode::OK);
    resume.send(()).unwrap();
    let rejected = upload.await.unwrap().unwrap();
    assert_eq!(rejected.status(), StatusCode::CONFLICT);
    assert_eq!(body(rejected).await["error"]["code"], "variable_conflict");
    let current = inspection(&fixture, &id).await;
    assert!(current.get("bytes").is_none());
    assert_eq!(current["values"]["json_edit"], 1);
    let retry = put(&fixture, &path, &current["witness"], "image/png", image).await;
    assert_eq!(retry.status(), StatusCode::OK);
    fixture.state.manager.close().await.unwrap();
}

#[tokio::test]
async fn image_variable_disclosure_requires_explicit_tool_call_and_retains_history_after_delete() {
    use microcrab_protocol::{ImageSource, InputPart, ToolCall};
    use microcrab_transport::{Content, ContentItem};
    use tokio_util::sync::CancellationToken;

    struct ViewClient(std::sync::OnceLock<String>);
    #[async_trait::async_trait]
    impl microcrab_session::Client for ViewClient {
        async fn complete(
            &self,
            content: &Content,
            _: &dyn microcrab_auditor::Auditor,
            variables: &dyn microcrab_auditor::VariableView,
            _: CancellationToken,
        ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
            assert!(variables.get("private_image").is_none());
            let image = content.items.iter().find_map(|item| match item {
                ContentItem::Tool { parts, .. } => parts.iter().find_map(|part| match part {
                    InputPart::Image {
                        source: ImageSource::Data { media_type, data },
                    } => {
                        assert_eq!(media_type, "image/png");
                        Some(data)
                    }
                    _ => None,
                }),
                _ => None,
            });
            let already_called = content
                .items
                .iter()
                .any(|item| matches!(item, ContentItem::Tool { .. }));
            let (text, tool_calls) = if already_called {
                assert_eq!(image.unwrap(), &png());
                ("image explicitly viewed", vec![])
            } else {
                assert!(image.is_none());
                (
                    "",
                    vec![ToolCall {
                        provider_metadata: None,
                        id: "view-once".into(),
                        name: "view_image".into(),
                        arguments: json!({"image_url":format!("{{{{var:base64:@{}}}}}", self.0.get().unwrap())}),
                    }],
                )
            };
            Ok(microcrab_protocol::Response {
                provider_metadata: None,
                content: text.into(),
                tool_calls,
                usage: Default::default(),
            })
        }
    }

    let client = Arc::new(ViewClient(Default::default()));
    let fixture = Fixture::with_client(true, Some(client.clone())).await;
    let id = create(&fixture).await;
    let initial = inspection(&fixture, &id).await;
    let path = format!("/api/v1/sessions/{id}/variables/bytes/private_image");
    let uploaded = put(&fixture, &path, &initial["witness"], "image/png", png()).await;
    assert_eq!(uploaded.status(), StatusCode::OK);
    let receipt = body(uploaded).await;
    let selector = receipt["metadata"]["selector"].as_str().unwrap();
    client.0.set(selector.to_owned()).unwrap();
    let permission = format!("/api/v1/sessions/{id}/permission");
    let current_policy = body(fixture.request("GET", &permission, None).await).await;
    let grant = fixture.request("PUT", &permission, Some(json!({
        "expected_revision":current_policy["revision"], "review":false,
        "overrides":[{"tool":{"kind":"exact","value":"view_image"},"args":[{"pointer":"/image_url","matcher":{"kind":"glob","value":"bytes:*"}}],"action":"allow"}]
    }))).await;
    assert_eq!(grant.status(), StatusCode::OK);
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "cookie",
        format!("microcrab_session={}", fixture.token)
            .parse()
            .unwrap(),
    );
    let owner = fixture.state.auth.authenticate(&headers).unwrap();
    let mut subscription = fixture.state.manager.subscribe(&owner, &id).await.unwrap();
    let operation = uuid::Uuid::new_v4().to_string();
    let message = serde_json::from_value(json!({
        "v":1, "type":"command", "request_id":uuid::Uuid::new_v4().to_string(),
        "method":"turn.submit", "session_id":id,
        "params":{"operation_id":operation,"input":{"parts":[
            {"type":"text","text":"view this privately uploaded image"}
        ]}}
    }))
    .unwrap();
    fixture
        .state
        .manager
        .command(&owner, &message)
        .await
        .unwrap();
    let mut view = subscription.snapshot.clone();
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
                        .any(|item| item["text"] == "image explicitly viewed")
                })
            {
                break;
            }
        }
    })
    .await
    .unwrap();
    let current = inspection(&fixture, &id).await;
    let deleted = remove(&fixture, &path, &current["witness"]).await;
    assert_eq!(deleted.status(), StatusCode::OK);
    let message = view["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| {
            item["role"] == "tool"
                && item["parts"]
                    .as_array()
                    .is_some_and(|parts| parts.iter().any(|part| part["type"] == "image"))
        })
        .unwrap();
    let url = message["parts"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|part| part["source"]["url"].as_str())
        .unwrap();
    assert!(
        url.starts_with(&format!("/api/v1/sessions/{id}/images/")),
        "{url}"
    );
    let retained = fixture.request("GET", url, None).await;
    assert_eq!(retained.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(retained.into_body(), 1024).await.unwrap().as_ref(),
        png().as_slice()
    );
    let current = inspection(&fixture, &id).await;
    assert!(current.get("bytes").is_none());
    let uploaded_again = put(&fixture, &path, &current["witness"], "image/png", png()).await;
    assert_eq!(uploaded_again.status(), StatusCode::OK);
    let travelled = fixture
        .request(
            "POST",
            &format!("/api/v1/sessions/{id}/history/travel"),
            Some(json!({"direction":"undo"})),
        )
        .await;
    assert_eq!(travelled.status(), StatusCode::OK);
    assert!(inspection(&fixture, &id).await.get("bytes").is_none());
    fixture.state.manager.close().await.unwrap();
}

#[tokio::test]
async fn invalid_streams_and_mime_do_not_commit_and_closed_runtime_requires_fresh_witness() {
    let fixture = Fixture::new(true).await;
    let id = create(&fixture).await;
    let initial = inspection(&fixture, &id).await;
    let path = format!("/api/v1/sessions/{id}/variables/bytes/image");
    let bytes = png();
    let witness = &initial["witness"];
    for length in [bytes.len() - 1, bytes.len() + 1] {
        let response = send(
            &fixture,
            request("PUT", &path, witness, Some(&fixture.token))
                .header("origin", "http://127.0.0.1:4096")
                .header("content-type", "image/png")
                .header("content-length", length),
            Body::from(bytes.clone()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert_eq!(
        put(&fixture, &path, witness, "image/jpeg", bytes.clone())
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(inspection(&fixture, &id).await, initial);
    let response = put(&fixture, &path, witness, "image/png", bytes.clone()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let receipt = body(response).await;
    let runtime = format!("/api/v1/sessions/{id}/runtime");
    assert_eq!(
        fixture
            .request("DELETE", &runtime, Some(json!({})))
            .await
            .status(),
        StatusCode::OK
    );
    let previous = &receipt["commit"]["after"];
    let closed = put(&fixture, &path, previous, "image/png", bytes.clone()).await;
    assert_eq!(closed.status(), StatusCode::CONFLICT);
    assert_eq!(body(closed).await["error"]["code"], "closed");
    let current = inspection(&fixture, &id).await;
    assert!(current.get("bytes").is_none());
    assert_ne!(
        current["witness"]["runtime_instance_id"],
        previous["runtime_instance_id"]
    );
    let stale = put(&fixture, &path, previous, "image/png", bytes).await;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    assert_eq!(body(stale).await["error"]["code"], "variable_conflict");
    assert_eq!(inspection(&fixture, &id).await, current);
    fixture.state.manager.close().await.unwrap();
}
