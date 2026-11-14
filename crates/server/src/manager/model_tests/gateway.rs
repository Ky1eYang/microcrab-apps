//! Real mixed-gateway routes exercise current selection and durable recovery admission.
use super::*;
use axum::response::IntoResponse;
use microcrab_session::Journal;
use std::{sync::atomic::AtomicBool, time::Duration};

struct Gateway {
    base: String,
    requests: Arc<Mutex<Vec<(String, String, Value)>>>,
    fail_next: Arc<AtomicBool>,
    worker: tokio::task::JoinHandle<()>,
}
impl Drop for Gateway {
    fn drop(&mut self) {
        self.worker.abort();
    }
}
async fn gateway() -> Gateway {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let fail_next = Arc::new(AtomicBool::new(false));
    let failing = fail_next.clone();
    let router = Router::new().fallback(move |method: axum::http::Method, uri: axum::http::Uri,
        headers: HeaderMap, bytes: axum::body::Bytes| {
        let recorded = recorded.clone();
        let failing = failing.clone();
        async move {
            if method == axum::http::Method::GET {
                assert_eq!(uri.path(), "/v1/models");
                return Json(json!({"data":[]})).into_response();
            }
            assert_eq!(method, axum::http::Method::POST);
            let authorization = headers.get("authorization").or_else(|| headers.get("x-api-key"))
                .unwrap().to_str().unwrap().to_owned();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            recorded.lock().unwrap().push((uri.path().to_owned(), authorization, body));
            if failing.swap(false, Ordering::AcqRel) {
                return (axum::http::StatusCode::BAD_REQUEST, "synthetic permanent upstream rejection").into_response();
            }
            let response = if uri.path().ends_with("/messages") {
                assert_eq!(headers["anthropic-version"], "2023-06-01");
                json!({"type":"message","role":"assistant","content":[{"type":"text","text":"Native answer"}],"stop_reason":"end_turn"})
            } else if uri.path().ends_with("/responses") {
                json!({"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Responses answer"}]}]})
            } else {
                assert_eq!(uri.path(), "/v1/chat/completions");
                json!({"choices":[{"message":{"role":"assistant","content":"Chat answer"},"finish_reason":"stop"}]})
            };
            Json(response).into_response()
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let worker = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(30), async move {
            axum::serve(listener, router).await.unwrap();
        })
        .await;
    });
    Gateway {
        base,
        requests,
        fail_next,
        worker,
    }
}
fn gateway_config(
    root: &std::path::Path,
    gateway: &Gateway,
    protocol: &str,
    auth: &str,
    key: &str,
    explicit: Option<&str>,
) -> Config {
    let suffix = match protocol {
        "anthropic" => "messages",
        "openai" => "responses",
        _ => "chat/completions",
    };
    let endpoint = explicit
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{}/v1/{suffix}", gateway.base));
    let catalog = root.join("providers.toml");
    std::fs::write(
        &catalog,
        format!(
            r#"
[provider.mixed]
protocol='openai-compatible'
[provider.mixed.options]
baseURL='{base}/v1'
model='wire'
apiKey='{key}'
timeoutMs=1000
[provider.mixed.models.native]
id='wire'
protocol='{protocol}'
endpoint='{endpoint}'
auth='{auth}'
[provider.mixed.models.chat]
id='chat'
variants=['high']
"#,
            base = gateway.base
        ),
    )
    .unwrap();
    let mut value: Value =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    value["storage"] = "file".into();
    value["data_dir"] = json!(root.join(".data"));
    value["frontend_dir"] = json!(root.join("frontend"));
    value["auto_rename"] = false.into();
    value["model_metadata"]["enabled"] = false.into();
    value.as_object_mut().unwrap().remove("agent");
    value["provider"] = json!({"config_file":catalog,"name":"mixed"});
    let path = root.join("server.json");
    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    let config = Config::read(path).unwrap();
    crate::project::seed_test_projects(
        &config.data_dir,
        json!([{"id":"project","name":"Project","root":root,"owners":["owner"]}]),
    );
    config
}
async fn manager(config: Config) -> Arc<Manager> {
    Manager::with_host(config, Arc::new(crate::noop::NoopHost))
        .await
        .unwrap()
}
fn assert_public_route_privacy(value: &Value, gateway: &Gateway) {
    let public = value.to_string();
    for private in [
        "synthetic-route-secret",
        gateway.base.as_str(),
        "route_witness",
    ] {
        assert!(
            !public.contains(private),
            "public API leaked private route data"
        );
    }
}

fn assert_retained_state_uses_patches(
    before: &[microcrab_transport::ContentItem],
    after: &[microcrab_transport::ContentItem],
    epoch: u64,
) {
    use microcrab_transport::{ContentItem, StateFrame};
    assert!(
        after.starts_with(before),
        "epoch changes retain the exact prior content prefix"
    );
    assert_eq!(
        after
            .iter()
            .filter(|item| matches!(item, ContentItem::State(StateFrame::StateSnapshot { .. })))
            .count(),
        1,
        "retained State history has one initial snapshot across epoch changes"
    );
    let appended = after[before.len()..]
        .iter()
        .filter_map(|item| match item {
            ContentItem::State(frame) => Some(frame),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(!appended.is_empty(), "a new Turn publishes current State");
    assert!(
        appended
            .iter()
            .all(|frame| matches!(frame, StateFrame::StatePatch { .. })),
        "an epoch change with retained State appends patches"
    );
    let mut projected = json!({});
    for item in after {
        if let ContentItem::State(frame) = item {
            microcrab_transport::apply_state_patch(&mut projected, frame).unwrap();
        }
    }
    assert_eq!(
        projected["session"]["epoch"], epoch,
        "patches project the live epoch without replacing the original snapshot"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_override_selection_helpers_and_restart_use_effective_model_routes() {
    let root = tempfile::tempdir().unwrap();
    let gateway = gateway().await;
    let config = gateway_config(
        root.path(),
        &gateway,
        "anthropic",
        "bearer",
        "synthetic-route-secret",
        None,
    );
    let manager = manager(config.clone()).await;
    let managed = create(&manager).await;
    let id = managed.record.lock().await.id.clone();
    assert_eq!(
        managed.routing.selection(),
        selection("mixed", "wire", None)
    );
    let initial_epoch = managed.session.state().epoch_id;
    let inherited = managed.routing.epoch_client(initial_epoch);
    submit(&manager, &managed, "first native input").await;
    let native_context = managed.session.context_snapshot().unwrap();
    let (_, _, witness) = managed.journal.restore_model_route().await.unwrap();
    assert_eq!(witness.as_deref(), Some(managed.routing.witness().as_str()));
    let revision = managed.record.lock().await.revision;
    let snapshot = select(
        &manager,
        &managed,
        revision,
        selection("mixed", "chat", Some("high")),
    )
    .await
    .unwrap();
    assert_public_route_privacy(&snapshot, &gateway);
    assert_eq!(managed.routing.selection().model, "wire");
    submit(&manager, &managed, "apply chat on next Turn").await;
    let chat_context = managed.session.context_snapshot().unwrap();
    assert!(chat_context.epoch > native_context.epoch);
    assert_retained_state_uses_patches(
        &native_context.items,
        &chat_context.items,
        chat_context.epoch,
    );
    managed
        .session
        .rename_with_model("name using current chat route")
        .await
        .unwrap();
    let content = microcrab_transport::Content {
        epoch: initial_epoch,
        subagent: Some("child".into()),
        system: "Synthetic helper".into(),
        items: vec![microcrab_transport::ContentItem::User {
            source: None,
            parts: vec![microcrab_protocol::InputPart::Text {
                text: "begin".into(),
            }],
        }],
        tools: vec![],
        max_output_tokens: 32,
        max_bytes: 8192,
        turn_id: None,
        round: None,
    };
    tokio::time::timeout(
        Duration::from_secs(2),
        inherited.complete(
            &content,
            &microcrab_auditor::NoopAuditor,
            &managed.session.state_handle(),
            tokio_util::sync::CancellationToken::new(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    {
        let requests = gateway.requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .map(|request| request.0.as_str())
                .collect::<Vec<_>>(),
            [
                "/v1/messages",
                "/v1/chat/completions",
                "/v1/chat/completions",
                "/v1/messages"
            ]
        );
        for (_, credential, _) in requests.iter() {
            assert_eq!(credential, "Bearer synthetic-route-secret");
        }
        assert_eq!(requests[1].2["model"], "chat");
        assert_eq!(requests[1].2["reasoning_effort"], "high");
    }
    assert_public_route_privacy(
        &serde_json::to_value(config.model_choices()).unwrap(),
        &gateway,
    );
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let reopened = self::manager(config).await;
    let restored = reopened
        .active(reopened.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    assert_eq!(
        restored.routing.selection(),
        selection("mixed", "chat", Some("high"))
    );
    submit(&reopened, &restored, "chat after restart").await;
    assert_eq!(
        gateway.requests.lock().unwrap().last().unwrap().0,
        "/v1/chat/completions"
    );
    reopened.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn witnessed_protocol_endpoint_and_auth_drift_reject_before_model_effects() {
    let root = tempfile::tempdir().unwrap();
    let gateway = gateway().await;
    let config = gateway_config(
        root.path(),
        &gateway,
        "anthropic",
        "bearer",
        "synthetic-route-secret",
        None,
    );
    let manager = manager(config).await;
    let managed = create(&manager).await;
    let id = managed.record.lock().await.id.clone();
    submit(&manager, &managed, "witness this native route").await;
    let cursor = managed.journal.cursor().await.unwrap();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let changed_endpoint = format!("{}/changed/messages", gateway.base);
    for (protocol, auth, endpoint) in [
        ("openai", "bearer", None),
        ("anthropic", "native", None),
        ("anthropic", "bearer", Some(changed_endpoint.as_str())),
    ] {
        let changed = gateway_config(
            root.path(),
            &gateway,
            protocol,
            auth,
            "synthetic-route-secret",
            endpoint,
        );
        let reopened = self::manager(changed).await;
        let record = reopened.record("owner", &id).await.unwrap();
        let error = match reopened.active(record.clone(), true).await {
            Ok(_) => panic!("witnessed route drift must fail before activation"),
            Err(error) => error,
        };
        let reason = error.message;
        assert!(
            reason.contains("historical model wire route differs"),
            "{reason}"
        );
        assert!(!reason.contains("synthetic-route-secret"));
        assert!(!reason.contains(&gateway.base));
        let (journal, _) = reopened.inspect_journal(&record).await.unwrap();
        assert_eq!(journal.cursor().await.as_ref(), Some(&cursor));
        journal.close().await.unwrap();
        assert_eq!(gateway.requests.lock().unwrap().len(), 1);
        reopened.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn credential_rotation_and_equivalent_endpoint_preserve_nonsecret_route_witness() {
    let root = tempfile::tempdir().unwrap();
    let gateway = gateway().await;
    let config = gateway_config(
        root.path(),
        &gateway,
        "anthropic",
        "bearer",
        "synthetic-route-secret",
        None,
    );
    let manager = manager(config).await;
    let managed = create(&manager).await;
    let id = managed.record.lock().await.id.clone();
    submit(&manager, &managed, "before credential rotation").await;
    let witness = managed.routing.witness();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let equivalent_endpoint = format!("{}/v1/", gateway.base);
    let rotated = gateway_config(
        root.path(),
        &gateway,
        "anthropic",
        "bearer",
        "synthetic-rotated-secret",
        Some(&equivalent_endpoint),
    );
    let reopened = self::manager(rotated).await;
    let restored = reopened
        .active(reopened.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    assert_eq!(restored.routing.witness(), witness);
    submit(&reopened, &restored, "after credential rotation").await;
    assert_eq!(
        gateway.requests.lock().unwrap().last().unwrap().1,
        "Bearer synthetic-rotated-secret"
    );
    reopened.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unwitnessed_history_rebinds_current_config_with_new_live_epoch_only() {
    let root = tempfile::tempdir().unwrap();
    let gateway = gateway().await;
    let config = gateway_config(
        root.path(),
        &gateway,
        "anthropic",
        "bearer",
        "synthetic-route-secret",
        None,
    );
    let manager = manager(config.clone()).await;
    let managed = create(&manager).await;
    let id = managed.record.lock().await.id.clone();
    submit(&manager, &managed, "historical native input").await;
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    // Publish a synthetic old envelope after releasing the Session's writer.
    let path = config.data_dir.join("sessions").join(&id);
    let (journal, recover) = CreatorJournal::open(path.to_str().unwrap(), id.clone(), true, true)
        .await
        .unwrap();
    let original = recover.unwrap();
    let old_epoch = original.context.epoch;
    journal
        .prepare_model_route(selection("mixed", "wire", None), None)
        .await;
    journal.commit(original.clone()).await.unwrap();
    let historical_cursor = journal.cursor().await.unwrap();
    assert!(journal.restore_model_route().await.unwrap().2.is_none());
    journal.close().await.unwrap();
    let rebound = gateway_config(
        root.path(),
        &gateway,
        "openai-compatible",
        "bearer",
        "synthetic-route-secret",
        None,
    );
    let reopened = self::manager(rebound).await;
    let restored = reopened
        .active(reopened.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    assert!(restored.session.state().epoch_id > old_epoch);
    assert_eq!(
        restored.journal.cursor().await.as_ref(),
        Some(&historical_cursor)
    );
    assert_eq!(
        serde_json::to_value(restored.journal.head_recover().await.unwrap().unwrap()).unwrap(),
        serde_json::to_value(&original).unwrap()
    );
    assert!(
        restored
            .journal
            .restore_model_route()
            .await
            .unwrap()
            .2
            .is_none()
    );
    submit(
        &reopened,
        &restored,
        "new chat input after explicit live refresh",
    )
    .await;
    let resumed_context = restored.session.context_snapshot().unwrap();
    assert_retained_state_uses_patches(
        &original.context.items,
        &resumed_context.items,
        resumed_context.epoch,
    );
    assert_eq!(
        gateway.requests.lock().unwrap().last().unwrap().0,
        "/v1/chat/completions"
    );
    assert!(
        restored
            .journal
            .restore_model_route()
            .await
            .unwrap()
            .2
            .is_some()
    );
    reopened.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unfinished_retry_keeps_witnessed_route_then_applies_configured_cross_protocol_model() {
    let root = tempfile::tempdir().unwrap();
    let gateway = gateway().await;
    let config = gateway_config(
        root.path(),
        &gateway,
        "anthropic",
        "bearer",
        "synthetic-route-secret",
        None,
    );
    let manager = manager(config.clone()).await;
    let managed = create(&manager).await;
    let id = managed.record.lock().await.id.clone();
    gateway.fail_next.store(true, Ordering::Release);
    submit(&manager, &managed, "unfinished original input").await;
    assert!(managed.session.retry_ready());
    let failed_operation = managed.hub.snapshot()["session"]["retry_operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let revision = managed.record.lock().await.revision;
    select(
        &manager,
        &managed,
        revision,
        selection("mixed", "chat", Some("high")),
    )
    .await
    .unwrap();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    // Startup cannot reinterpret this unfinished native request as Responses.
    let drift = gateway_config(
        root.path(),
        &gateway,
        "openai",
        "bearer",
        "synthetic-route-secret",
        None,
    );
    let rejected = self::manager(drift).await;
    let error = match rejected
        .active(rejected.record("owner", &id).await.unwrap(), true)
        .await
    {
        Ok(_) => panic!("unfinished witnessed route must reject protocol drift"),
        Err(error) => error,
    };
    assert!(
        error
            .message
            .contains("historical model wire route differs")
    );
    rejected.close().await.unwrap();
    let reopened = self::manager(config).await;
    let restored = reopened
        .active(reopened.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    assert!(restored.session.retry_ready());
    assert_eq!(
        restored.routing.selection(),
        selection("mixed", "wire", None)
    );
    assert_eq!(
        restored.record.lock().await.model_selection.as_ref(),
        Some(&selection("mixed", "chat", Some("high")))
    );
    {
        let _gate = restored.commands.lock().await;
        commands::submit(&reopened, restored.clone(), &command("turn.retry", Some(&id),
            json!({"operation_id":uuid::Uuid::new_v4().to_string(),"failed_operation_id":failed_operation})))
            .await.unwrap();
    }
    let worker = restored.worker.lock().await.take().unwrap();
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap();
    assert!(!restored.session.retry_ready());
    {
        let requests = gateway.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
        assert_eq!(requests[1].0, "/v1/messages");
        assert_eq!(
            requests[1]
                .2
                .to_string()
                .matches("unfinished original input")
                .count(),
            1
        );
    }
    let retried_context = restored.session.context_snapshot().unwrap();
    submit(
        &reopened,
        &restored,
        "apply configured Chat only on a new Turn",
    )
    .await;
    let configured_context = restored.session.context_snapshot().unwrap();
    assert!(configured_context.epoch > retried_context.epoch);
    assert_retained_state_uses_patches(
        &retried_context.items,
        &configured_context.items,
        configured_context.epoch,
    );
    let last = gateway.requests.lock().unwrap().last().unwrap().clone();
    assert_eq!(last.0, "/v1/chat/completions");
    assert_eq!(last.2["model"], "chat");
    assert_eq!(last.2["reasoning_effort"], "high");
    reopened.close().await.unwrap();
}
