//! Real loopback provider requests verify routing independently of UI labels.
#![cfg(feature = "storage-file")]
mod context_budget;
mod display;
mod gateway;
mod retry;
mod review;
use super::*;
use axum::{Json, Router, extract::State as HttpState, http::HeaderMap, routing::post};
use microcrab_protocol::application::Command;
use microcrab_storage::ModelSelection;
use serde_json::json;
use tokio::sync::Semaphore;

struct Wire {
    requests: Mutex<Vec<(String, Value)>>,
    entered: Semaphore,
    release: Semaphore,
}
struct ProviderMock {
    endpoint: String,
    wire: Arc<Wire>,
    worker: tokio::task::JoinHandle<()>,
}
impl Drop for ProviderMock {
    fn drop(&mut self) {
        self.worker.abort();
    }
}
async fn provider(permits: usize) -> ProviderMock {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let wire = Arc::new(Wire {
        requests: Mutex::new(vec![]),
        entered: Semaphore::new(0),
        release: Semaphore::new(permits),
    });
    let router = Router::new().route("/chat/completions", post(|HttpState(wire): HttpState<Arc<Wire>>, headers: HeaderMap, Json(body): Json<Value>| async move {
        let authorization = headers.get("authorization").unwrap().to_str().unwrap().to_owned();
        wire.requests.lock().unwrap().push((authorization, body));
        wire.entered.add_permits(1);
        wire.release.acquire().await.unwrap().forget();
        Json(json!({"choices":[{"message":{"role":"assistant","content":"Wire answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}))
    })).with_state(wire.clone());
    let worker = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    ProviderMock {
        endpoint,
        wire,
        worker,
    }
}
fn config(root: &std::path::Path, a: &ProviderMock, b: &ProviderMock) -> Config {
    let catalog = root.join("providers.toml");
    std::fs::write(
        &catalog,
        format!(
            r#"
[provider.a]
protocol = 'openai-compatible'
[provider.a.options]
endpoint = '{}'
model = 'model-a'
apiKey = 'test-a'
[provider.a.models.model-a]
variants = ['low', 'high']
[provider.b]
protocol = 'openai-compatible'
[provider.b.options]
endpoint = '{}'
model = 'model-b'
apiKey = 'test-b'
[provider.b.models.model-b]
variants = ['low', 'high']
"#,
            a.endpoint, b.endpoint
        ),
    )
    .unwrap();
    let path = root.join("server.json");
    std::fs::write(&path, serde_json::to_vec(&json!({
        "data_dir":root.join(".data"),
        "provider":{"config_file":catalog,"name":"a"},
        "auto_rename":false,
        "auth":{
            "issuer":"http://127.0.0.1:9900","authorization_endpoint":"http://127.0.0.1:9900/authorize",
            "token_endpoint":"http://127.0.0.1:9900/token","jwks_uri":"http://127.0.0.1:9900/jwks",
            "client_id":"test","redirect_uri":"http://127.0.0.1:4096/api/auth/callback",
            "allowed_origins":["http://127.0.0.1:4096"],"jwt_secret":"test-key-at-least-thirty-two-characters"
        }
    })).unwrap()).unwrap();
    let config = Config::read(path).unwrap();
    crate::project::seed_test_projects(
        &config.data_dir,
        json!([
            {"id":"project","name":"Project","root":root,"owners":["owner"]}
        ]),
    );
    config
}
fn command(method: &str, id: Option<&str>, params: Value) -> Command {
    serde_json::from_value(json!({"v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),"method":method,"session_id":id,"params":params})).unwrap()
}
async fn create(manager: &Arc<Manager>) -> Arc<Managed> {
    let created = commands::create(
        manager,
        "owner",
        &command(
            "session.create",
            None,
            json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}),
        ),
    )
    .await
    .unwrap();
    let record = manager
        .record("owner", created["id"].as_str().unwrap())
        .await
        .unwrap();
    manager.active(record, true).await.unwrap()
}
async fn submit(manager: &Arc<Manager>, managed: &Arc<Managed>, text: &str) {
    let id = managed.record.lock().await.id.clone();
    {
        let _gate = managed.commands.lock().await;
        commands::submit(manager, managed.clone(), &command("turn.submit", Some(&id),
            json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text(text)}))).await.unwrap();
    }
    if let Some(worker) = managed.worker.lock().await.take() {
        tokio::time::timeout(std::time::Duration::from_secs(10), worker)
            .await
            .unwrap()
            .unwrap();
    }
    assert!(!managed.faulted.load(Ordering::Acquire));
}
fn selection(provider_id: &str, model: &str, variant: Option<&str>) -> ModelSelection {
    ModelSelection {
        provider_id: provider_id.into(),
        model: model.into(),
        variant: variant.map(str::to_owned),
    }
}
async fn select(
    manager: &Manager,
    managed: &Managed,
    revision: u64,
    selection: ModelSelection,
) -> Result<Value, WireError> {
    let _gate = managed.commands.lock().await;
    model::select(manager, managed, revision, selection).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_provider_switch_preserves_context_and_restores_selected_wire_model() {
    let root = tempfile::tempdir().unwrap();
    let a = provider(10).await;
    let b = provider(10).await;
    let config = config(root.path(), &a, &b);
    let manager = Manager::with_host(config.clone(), Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let managed = create(&manager).await;
    let inherited_child = managed.routing.client(false);
    let id = managed.record.lock().await.id.clone();
    submit(&manager, &managed, "first input").await;
    let before = managed.session.checkpoint().await.unwrap();
    let cursor = managed.journal.cursor().await;
    let revision = managed.record.lock().await.revision;
    let chosen = selection("b", "model-b", Some("high"));
    let snapshot = select(&manager, &managed, revision, chosen.clone())
        .await
        .unwrap();
    assert_eq!(snapshot["session"]["id"], id);
    assert_eq!(snapshot["session"]["model"], "model-b");
    assert_eq!(
        snapshot["session"]["model_selection"],
        serde_json::to_value(&chosen).unwrap()
    );
    assert_eq!(managed.journal.cursor().await, cursor);
    let after = managed.session.checkpoint().await.unwrap();
    assert_eq!(
        serde_json::to_value(&before.context).unwrap(),
        serde_json::to_value(&after.context).unwrap()
    );
    assert_eq!(before.state.epoch_id, after.state.epoch_id);
    submit(&manager, &managed, "second input").await;
    {
        let a_requests = a.wire.requests.lock().unwrap();
        let b_requests = b.wire.requests.lock().unwrap();
        assert_eq!(a_requests.len(), 1);
        assert_eq!(b_requests.len(), 1);
        assert_eq!(a_requests[0].0, "Bearer test-a");
        assert_eq!(b_requests[0].0, "Bearer test-b");
        assert_eq!(a_requests[0].1["model"], "model-a");
        assert!(a_requests[0].1.get("reasoning_effort").is_none());
        assert_eq!(b_requests[0].1["model"], "model-b");
        assert_eq!(b_requests[0].1["reasoning_effort"], "high");
        assert_eq!(a_requests[0].1["tools"], b_requests[0].1["tools"]);
        assert_eq!(
            a_requests[0].1["messages"][0],
            b_requests[0].1["messages"][0]
        );
        assert!(
            b_requests[0].1["messages"]
                .to_string()
                .contains("first input")
        );
        assert!(
            b_requests[0].1["messages"]
                .to_string()
                .contains("second input")
        );
    }
    // Functional naming uses the same selected target without another Session.
    managed
        .session
        .rename_with_model("name this conversation")
        .await
        .unwrap();
    assert_eq!(b.wire.requests.lock().unwrap().len(), 2);
    let preview = managed.hub.snapshot();
    let child_content = microcrab_transport::Content {
        epoch: 0,
        subagent: Some("child".into()),
        system: "Child system".into(),
        items: vec![microcrab_transport::ContentItem::User {
            source: None,
            parts: vec![microcrab_protocol::InputPart::Text {
                text: "child input".into(),
            }],
        }],
        tools: vec![],
        max_output_tokens: 64,
        max_bytes: 8192,
        turn_id: Some(1),
        round: Some(1),
    };
    inherited_child
        .complete(
            &child_content,
            &microcrab_auditor::NoopAuditor,
            &managed.session.state_handle(),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(b.wire.requests.lock().unwrap().len(), 2);
    assert_eq!(a.wire.requests.lock().unwrap().len(), 2);
    assert_eq!(
        a.wire.requests.lock().unwrap().last().unwrap().1["model"],
        "model-a"
    );
    assert_eq!(managed.hub.snapshot(), preview);
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let reopened = Manager::with_host(config, Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let restored = reopened
        .active(reopened.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    assert_eq!(
        restored.record.lock().await.model_selection.as_ref(),
        Some(&chosen)
    );
    assert_eq!(restored.hub.snapshot()["session"]["id"], id);
    submit(&reopened, &restored, "third input after restart").await;
    let last = b.wire.requests.lock().unwrap().last().unwrap().clone();
    assert_eq!(last.1["model"], "model-b");
    assert_eq!(last.1["reasoning_effort"], "high");
    assert_eq!(a.wire.requests.lock().unwrap().len(), 2);
    reopened.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn google_selection_streams_native_wire_and_restores_selected_protocol() {
    use axum::extract::OriginalUri;
    use std::io::Write;

    let root = tempfile::tempdir().unwrap();
    let chat = provider(10).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1beta", listener.local_addr().unwrap());
    let wire = Arc::new(Wire {
        requests: Mutex::new(vec![]),
        entered: Semaphore::new(0),
        release: Semaphore::new(10),
    });
    let router = Router::new()
        .route(
            "/v1beta/models/gemini-selected:streamGenerateContent",
            post(
                |HttpState(wire): HttpState<Arc<Wire>>,
                 OriginalUri(uri): OriginalUri,
                 headers: HeaderMap,
                 Json(body): Json<Value>| async move {
                    assert_eq!(
                        uri.path_and_query().unwrap().as_str(),
                        "/v1beta/models/gemini-selected:streamGenerateContent?alt=sse"
                    );
                    assert!(!headers.contains_key("authorization"));
                    assert!(body.get("system_instruction").is_none());
                    assert!(body.get("generation_config").is_none());
                    assert!(body["generationConfig"]["maxOutputTokens"].as_u64().is_some());
                    if let Some(tools) = body["tools"].as_array() {
                        for tool in tools {
                            assert!(tool.get("function_declarations").is_none());
                            for declaration in tool["functionDeclarations"].as_array().unwrap() {
                                assert!(declaration["parametersJsonSchema"].is_object());
                                assert!(declaration.get("parameters").is_none());
                            }
                        }
                    }
                    let key = headers
                        .get("x-goog-api-key")
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_owned();
                    wire.requests.lock().unwrap().push((key, body));
                    (
                        [("content-type", "text/event-stream")],
                        format!(
                            "data: {}\n\n",
                            json!({
                                "candidates":[{
                                    "content":{"role":"model","parts":[{"text":"Google wire answer"}]},
                                    "finishReason":"STOP"
                                }],
                                "usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":3,"totalTokenCount":5}
                            })
                        ),
                    )
                },
            ),
        )
        .with_state(wire.clone());
    let worker = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let google = ProviderMock {
        endpoint,
        wire,
        worker,
    };

    // Keep the startup default on chat and configure Google IDs explicitly so
    // this integration case never performs discovery or external requests.
    let _ = config(root.path(), &chat, &chat);
    let mut catalog = std::fs::OpenOptions::new()
        .append(true)
        .open(root.path().join("providers.toml"))
        .unwrap();
    write!(
        catalog,
        "\n[provider.google]\nprotocol='google'\n[provider.google.options]\nbaseURL='{}'\nmodel='gemini-default'\napiKey='synthetic-google-key'\n[provider.google.models.gemini-selected]\nname='Selected Gemini'\n",
        google.endpoint
    )
    .unwrap();
    drop(catalog);
    let config = Config::read(root.path().join("server.json")).unwrap();
    assert_eq!(
        config.provider.protocol,
        microcrab_transport::Protocol::OpenaiCompatible
    );
    assert!(
        config
            .model_choices()
            .iter()
            .any(|model| model.provider_id == "google" && model.id == "gemini-selected")
    );
    let manager = Manager::with_host(config.clone(), Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let managed = create(&manager).await;
    let id = managed.record.lock().await.id.clone();
    submit(&manager, &managed, "chat input before Google selection").await;
    let before = managed.session.checkpoint().await.unwrap();
    let cursor = managed.journal.cursor().await;
    let revision = managed.record.lock().await.revision;
    let chosen = selection("google", "gemini-selected", None);
    let snapshot = select(&manager, &managed, revision, chosen.clone())
        .await
        .unwrap();
    assert_eq!(
        snapshot["session"]["model_selection"],
        serde_json::to_value(&chosen).unwrap()
    );
    assert_eq!(managed.journal.cursor().await, cursor);
    let after = managed.session.checkpoint().await.unwrap();
    assert_eq!(before.state.epoch_id, after.state.epoch_id);
    assert_eq!(
        serde_json::to_value(&before.context).unwrap(),
        serde_json::to_value(&after.context).unwrap()
    );
    submit(&manager, &managed, "first Google input").await;
    {
        let requests = google.wire.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, "synthetic-google-key");
        let body = &requests[0].1;
        assert!(body["contents"].is_array());
        assert!(body.get("messages").is_none());
        assert!(body.get("reasoning_effort").is_none());
        assert!(
            body["contents"]
                .to_string()
                .contains("chat input before Google selection")
        );
        assert!(body["contents"].to_string().contains("first Google input"));
    }
    assert!(
        serde_json::to_value(managed.session.checkpoint().await.unwrap().context)
            .unwrap()
            .to_string()
            .contains("Google wire answer")
    );
    assert_eq!(chat.wire.requests.lock().unwrap().len(), 1);
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);

    let reopened = Manager::with_host(config, Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let restored = reopened
        .active(reopened.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    assert_eq!(
        restored.record.lock().await.model_selection.as_ref(),
        Some(&chosen)
    );
    submit(&reopened, &restored, "Google input after reopen").await;
    {
        let requests = google.wire.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].0, "synthetic-google-key");
        assert_eq!(
            requests[0].1.get("systemInstruction"),
            requests[1].1.get("systemInstruction")
        );
        assert_eq!(requests[0].1.get("tools"), requests[1].1.get("tools"));
        assert!(requests[1].1.get("messages").is_none());
        assert!(
            requests[1].1["contents"]
                .to_string()
                .contains("Google wire answer")
        );
        assert!(
            requests[1].1["contents"]
                .to_string()
                .contains("Google input after reopen")
        );
    }
    assert_eq!(chat.wire.requests.lock().unwrap().len(), 1);
    reopened.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_revision_owner_and_invalid_selection_do_not_change_actual_route() {
    let root = tempfile::tempdir().unwrap();
    let a = provider(0).await;
    let b = provider(10).await;
    let manager = Manager::with_host(config(root.path(), &a, &b), Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let managed = create(&manager).await;
    let initial = managed.record.lock().await.clone();
    assert_eq!(
        manager
            .record("intruder", &initial.id)
            .await
            .unwrap_err()
            .code,
        "not_found"
    );
    assert_eq!(
        select(
            &manager,
            &managed,
            initial.revision + 1,
            selection("b", "model-b", None)
        )
        .await
        .unwrap_err()
        .code,
        "revision_conflict"
    );
    assert_eq!(
        select(
            &manager,
            &managed,
            initial.revision,
            selection("b", "model-a", None)
        )
        .await
        .unwrap_err()
        .code,
        "model_unavailable"
    );
    {
        let _gate = managed.commands.lock().await;
        commands::submit(&manager, managed.clone(), &command("turn.submit", Some(&initial.id),
            json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text("blocked input")}))).await.unwrap();
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), a.wire.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let pending = select(
        &manager,
        &managed,
        initial.revision,
        selection("b", "model-b", Some("high")),
    )
    .await
    .unwrap();
    assert_eq!(pending["session"]["model"], "model-b");
    assert_eq!(
        pending["session"]["applied_model_selection"]["model"],
        "model-a"
    );
    assert_eq!(
        pending["session"]["turn_model_selection"]["model"],
        "model-a"
    );
    assert_eq!(pending["session"]["pending_model_change"], true);
    let revision = managed.record.lock().await.revision;
    let restored_choice = select(
        &manager,
        &managed,
        revision,
        initial.model_selection.clone().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(restored_choice["session"]["pending_model_change"], false);
    assert!(b.wire.requests.lock().unwrap().is_empty());
    assert!(!managed.faulted.load(Ordering::Acquire));
    a.wire.release.add_permits(1);
    if let Some(worker) = managed.worker.lock().await.take() {
        worker.await.unwrap();
    }
    assert_eq!(a.wire.requests.lock().unwrap()[0].1["model"], "model-a");
    let epoch = managed.session.state().epoch_id;
    a.wire.release.add_permits(1);
    submit(&manager, &managed, "next input after round-trip").await;
    assert_eq!(
        managed.session.state().epoch_id,
        epoch,
        "A -> B -> A has no epoch effect"
    );
    assert_eq!(a.wire.requests.lock().unwrap().len(), 2);
    assert!(b.wire.requests.lock().unwrap().is_empty());
    let revision = managed.record.lock().await.revision;
    select(
        &manager,
        &managed,
        revision,
        selection("b", "model-b", None),
    )
    .await
    .unwrap();
    submit(&manager, &managed, "new Turn on B").await;
    assert_eq!(managed.session.state().epoch_id, epoch + 1);
    assert_eq!(b.wire.requests.lock().unwrap().len(), 1);
    manager.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_selection_migrates_once_and_missing_configured_route_does_not_replace_applied() {
    let root = tempfile::tempdir().unwrap();
    let a = provider(10).await;
    let b = provider(10).await;
    let config = config(root.path(), &a, &b);
    let manager = Manager::with_host(config.clone(), Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let managed = create(&manager).await;
    let mut record = managed.record.lock().await.clone();
    let id = record.id.clone();
    manager.close_managed(&managed).await.unwrap();
    manager.registry.lock().unwrap().remove(&id);
    let revision = record.revision;
    record.model_selection = None;
    record.revision += 1;
    record.updated_at_ms = now().max(record.updated_at_ms);
    manager
        .directory
        .replace("owner", revision, record)
        .await
        .unwrap();
    let migrated = manager.record("owner", &id).await.unwrap();
    assert_eq!(
        migrated.model_selection,
        Some(selection("a", "model-a", None))
    );
    let reread = manager.record("owner", &id).await.unwrap();
    assert_eq!(reread.revision, migrated.revision);
    let mut unavailable = reread.clone();
    unavailable.model_selection = Some(selection("removed", "model-a", None));
    unavailable.revision += 1;
    manager
        .directory
        .replace("owner", reread.revision, unavailable)
        .await
        .unwrap();
    let record = manager.record("owner", &id).await.unwrap();
    let restored = manager.active(record, true).await.unwrap();
    assert_eq!(
        restored.routing.selection(),
        selection("a", "model-a", None)
    );
    assert!(
        restored
            .session
            .submit("new input cannot apply removed configured provider")
            .await
            .is_err()
    );
    assert!(!restored.session.is_faulted());
    assert_eq!(
        restored
            .record
            .lock()
            .await
            .model_selection
            .as_ref()
            .unwrap()
            .provider_id,
        "removed"
    );
    manager.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn functional_issue_route_is_pinned_before_its_future_is_polled() {
    let root = tempfile::tempdir().unwrap();
    let a = provider(10).await;
    let b = provider(10).await;
    let manager = Manager::with_host(config(root.path(), &a, &b), Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let managed = create(&manager).await;
    let epoch = managed.session.state().epoch_id;
    let helper = managed.routing.epoch_client(epoch);
    let issued = microcrab_transport::Content {
        epoch,
        subagent: Some("rename".into()),
        system: "issued helper".into(),
        items: vec![microcrab_transport::ContentItem::User {
            source: None,
            parts: vec![microcrab_protocol::InputPart::Text {
                text: "begin".into(),
            }],
        }],
        tools: vec![],
        max_output_tokens: 16,
        max_bytes: 4096,
        turn_id: None,
        round: None,
    };
    let variables = microcrab_session::State::new(microcrab_session::Limits::default());
    let delayed = helper.complete(
        &issued,
        &microcrab_auditor::NoopAuditor,
        &variables,
        tokio_util::sync::CancellationToken::new(),
    );
    let revision = managed.record.lock().await.revision;
    select(
        &manager,
        &managed,
        revision,
        selection("b", "model-b", None),
    )
    .await
    .unwrap();
    submit(&manager, &managed, "apply B before polling old helper").await;
    delayed.await.unwrap();
    assert_eq!(
        a.wire.requests.lock().unwrap().last().unwrap().1["model"],
        "model-a"
    );
    let new_helper = managed
        .routing
        .epoch_client(managed.session.state().epoch_id);
    let mut new_issued = issued.clone();
    new_issued.epoch = managed.session.state().epoch_id;
    new_helper
        .complete(
            &new_issued,
            &microcrab_auditor::NoopAuditor,
            &variables,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(b.wire.requests.lock().unwrap().len(), 2);
    manager.close().await.unwrap();
}
