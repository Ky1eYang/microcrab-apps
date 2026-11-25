use super::*;
use crate::config::Config;
use axum::{Json, http::StatusCode, routing::post};
use microcrab_auditor::{ChannelAuditor, EmptyVariableView, NoopAuditor};
use microcrab_protocol::{ImageSource, InputPart};
use microcrab_transport::{
    ContentItem, HostedImageHint, ImageHintRevocation, ImageUpload, ImageUploadError,
};
use serde_json::{Value, json};
use std::{sync::Mutex, time::SystemTime};

const HOSTED: &str = "https://loopback-media.invalid/fixture?signature=test-only";
const INLINE: &str = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAAAAAAAA";
const FRESH: ToolImagePosition = ToolImagePosition { item: 1, part: 0 };

fn png() -> Vec<u8> {
    let mut bytes = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&[8, 2, 0, 0, 0, 0, 0, 0, 0]);
    bytes
}

fn canonical() -> Content {
    let tool = |id: &str| ContentItem::Tool {
        call_id: id.into(),
        content: "image".into(),
        parts: vec![InputPart::Image {
            source: ImageSource::Data {
                media_type: "image/png".into(),
                data: png(),
            },
        }],
    };
    Content {
        epoch: 1,
        subagent: None,
        system: "frozen".into(),
        items: vec![tool("retained"), tool("fixture-fresh")],
        tools: vec![],
        max_output_tokens: 32,
        max_bytes: 65536,
        turn_id: Some(1),
        round: Some(2),
    }
}

#[derive(Default)]
struct FakeUploader(Mutex<Vec<ImageHostingScope>>);
#[async_trait::async_trait]
impl ImageUrlResolver for FakeUploader {
    async fn upload(
        &self,
        scope: &ImageHostingScope,
        image: ImageUpload,
        _: CancellationToken,
    ) -> Result<Option<HostedImageHint>, ImageUploadError> {
        assert_eq!(image.media_type, "image/png");
        assert_eq!(image.data, png());
        self.0.lock().unwrap().push(scope.clone());
        Ok(Some(HostedImageHint {
            url: HOSTED.into(),
            expires_at: SystemTime::now() + Duration::from_secs(30),
            revocation: ImageHintRevocation::ExpiresOnly,
        }))
    }
}

fn selection() -> ModelSelection {
    ModelSelection {
        provider_id: "configured".into(),
        model: "fixture-model".into(),
        variant: None,
    }
}

fn policy(endpoint: &url::Url, uploader: Arc<FakeUploader>) -> ProviderImageHosting {
    ProviderImageHosting {
        owner: "owner".into(),
        selection: selection(),
        protocol: Protocol::OpenaiCompatible,
        request_endpoint: endpoint.clone(),
        authority: "fixture-authority".into(),
        allowed_hosts: vec!["loopback-media.invalid".into()],
        max_hint_ttl: Duration::from_secs(60),
        uploader,
    }
}

fn images(request: &Value) -> Vec<&str> {
    request["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|message| {
            message
                .pointer("/content/1/image_url/url")
                .and_then(Value::as_str)
        })
        .collect()
}

#[tokio::test]
async fn actual_server_hosting_route_requires_exact_policy_and_explicit_fresh_grant() {
    let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
    let recorded = seen.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = url::Url::parse(&format!(
        "http://{}/chat/completions",
        listener.local_addr().unwrap()
    ))
    .unwrap();
    let app = axum::Router::new().route("/chat/completions", post(move |Json(body): Json<Value>| {
        let recorded = recorded.clone();
        async move {
            let hosted = images(&body).contains(&HOSTED);
            recorded.lock().unwrap().push(body);
            if hosted {
                (StatusCode::BAD_REQUEST, Json(json!({"error":{"code":"failed_to_download_image","message":HOSTED}})))
            } else {
                (StatusCode::OK, Json(json!({"choices":[{"message":{"role":"assistant","content":"accepted"},"finish_reason":"stop"}]})))
            }
        }
    }));
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let root = tempfile::tempdir().unwrap();
    let mut config: Value =
        serde_json::from_str(include_str!("../../../../config.example.json")).unwrap();
    config.as_object_mut().unwrap().remove("agent");
    config["data_dir"] = json!(root.path().join("data"));
    config["storage"] = json!("file");
    config["provider"] = json!({"protocol":"openai-compatible","endpoint":endpoint.as_str(),"model":"fixture-model","timeout_ms":1000});
    config["model_metadata"] = json!({"enabled":false});
    let config: Config = serde_json::from_value(config).unwrap();
    let manager = Manager::with_host(config, Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let uploader = Arc::new(FakeUploader::default());
    let mut wrong_endpoint = policy(&endpoint, uploader.clone());
    wrong_endpoint
        .request_endpoint
        .set_path("/other/chat/completions");
    assert!(
        manager
            .prepare_image_hosting_route(wrong_endpoint)
            .await
            .is_err()
    );
    let mut wrong_protocol = policy(&endpoint, uploader.clone());
    wrong_protocol.protocol = Protocol::OpenaiResponses;
    assert!(
        manager
            .prepare_image_hosting_route(wrong_protocol)
            .await
            .is_err()
    );
    let mut wrong_model = policy(&endpoint, uploader.clone());
    wrong_model.selection.model = "other-model".into();
    assert!(
        manager
            .prepare_image_hosting_route(wrong_model)
            .await
            .is_err()
    );
    let mut wrong_variant = policy(&endpoint, uploader.clone());
    wrong_variant.selection.variant = Some("high".into());
    assert!(
        manager
            .prepare_image_hosting_route(wrong_variant)
            .await
            .is_err()
    );
    let mut owner_catalog = policy(&endpoint, uploader.clone());
    owner_catalog.selection.provider_id = "user:fixture".into();
    assert!(
        manager
            .prepare_image_hosting_route(owner_catalog)
            .await
            .is_err()
    );
    let hosted = manager
        .prepare_image_hosting_route(policy(&endpoint, uploader.clone()))
        .await
        .unwrap();
    let request = canonical();
    let frozen = serde_json::to_vec(&request).unwrap();
    assert!(hosted.authorize("wrong-owner", &request, &[FRESH]).is_err());
    let mut second_owner = policy(&endpoint, uploader.clone());
    second_owner.owner = "other-owner".into();
    let unrelated = manager
        .prepare_image_hosting_route(second_owner)
        .await
        .unwrap();
    assert!(
        unrelated
            .complete(
                hosted.authorize("owner", &request, &[FRESH]).unwrap(),
                &NoopAuditor,
                &EmptyVariableView,
                CancellationToken::new()
            )
            .await
            .is_err()
    );
    assert!(uploader.0.lock().unwrap().is_empty());

    // Normal Server target assembly remains inline even while a scoped concrete
    // hosting route exists for the same owner, model and endpoint.
    let ordinary = super::super::target(
        &manager,
        "owner",
        &selection(),
        Arc::new(super::super::Hub::new()),
    )
    .await
    .unwrap();
    let routed = super::super::Routing::new(ordinary).client(true);
    assert_eq!(
        routed
            .complete(
                &request,
                &NoopAuditor,
                &EmptyVariableView,
                CancellationToken::new()
            )
            .await
            .unwrap()
            .content,
        "accepted"
    );
    assert!(uploader.0.lock().unwrap().is_empty());

    let (sender, mut audit) = mpsc::unbounded_channel();
    let auditor = ChannelAuditor::from_sender(sender);
    let (events, mut event_rx) = mpsc::channel(8);
    let accepted = hosted
        .stream(
            hosted.authorize("owner", &request, &[FRESH]).unwrap(),
            &auditor,
            &EmptyVariableView,
            CancellationToken::new(),
            Some(events),
        )
        .await
        .unwrap();
    assert_eq!(accepted.content, "accepted");
    assert!(
        matches!(event_rx.try_recv(), Ok(ModelEvent::TextDelta { text }) if text == "accepted")
    );
    assert!(matches!(
        event_rx.try_recv(),
        Ok(ModelEvent::Finished { .. })
    ));
    assert!(event_rx.try_recv().is_err());
    let results: Vec<_> = std::iter::from_fn(|| audit.try_recv().ok()).collect();
    assert_eq!(results.len(), 4);
    assert!(results.iter().all(|entry| entry.sent_request.is_none()
        && entry.response.is_none()
        && entry.response_bytes.is_none()));
    assert_ne!(results[0].call_id, results[2].call_id);
    assert_eq!(results[1].response_status, Some(400));
    assert_eq!(results[3].response_status, Some(200));
    assert_eq!(serde_json::to_vec(&request).unwrap(), frozen);
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert!(seen.iter().all(|r| r["model"] == "fixture-model"));
        assert_eq!(images(&seen[0]), [INLINE, INLINE]);
        assert_eq!(images(&seen[1]), [INLINE, HOSTED]);
        assert_eq!(images(&seen[2]), [INLINE, INLINE]);
        let scopes = uploader.0.lock().unwrap();
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].authority, "fixture-authority");
    }
    server.abort();
    let _ = server.await;
}
