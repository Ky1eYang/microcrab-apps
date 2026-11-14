//! Whole-input navigation exercises real admissions, epochs and immutable storage.
use super::*;
use microcrab_session::Journal;

#[allow(dead_code)]
#[path = "../../../../tests/support/typed_agent_fixture.rs"]
mod typed_agent_fixture;

async fn create(manager: &Arc<Manager>) -> Arc<Managed> {
    let created = commands::create(
        manager,
        "owner",
        &command(
            "session.create",
            None,
            json!({
                "project_id":"project", "operation_id":uuid::Uuid::new_v4().to_string()
            }),
        ),
    )
    .await
    .unwrap();
    manager
        .active(
            manager
                .record("owner", created["id"].as_str().unwrap())
                .await
                .unwrap(),
            true,
        )
        .await
        .unwrap()
}

async fn submit(manager: &Arc<Manager>, managed: &Arc<Managed>, text: &str) {
    let id = managed.record.lock().await.id.clone();
    commands::execute(
        manager,
        &crate::auth::Principal::for_test("owner"),
        &command(
            "turn.submit",
            Some(&id),
            json!({
                "operation_id":uuid::Uuid::new_v4().to_string(),
                "input":microcrab_session::SessionInput::text(text)
            }),
        ),
    )
    .await
    .unwrap();
    if let Some(worker) = managed.worker.lock().await.take() {
        tokio::time::timeout(std::time::Duration::from_secs(10), worker)
            .await
            .expect("deterministic history turn must settle")
            .unwrap();
    }
    assert!(!managed.faulted.load(Ordering::Acquire));
    assert!(!managed.session.has_continuation());
}

async fn travel(manager: &Arc<Manager>, managed: &Managed, direction: &str) -> Value {
    let id = managed.record.lock().await.id.clone();
    commands::execute(
        manager,
        &crate::auth::Principal::for_test("owner"),
        &command("history.travel", Some(&id), json!({"direction":direction})),
    )
    .await
    .unwrap()
}

fn context(managed: &Managed) -> Value {
    serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap()
}

fn user_texts(managed: &Managed) -> Vec<String> {
    managed
        .session
        .context_snapshot()
        .unwrap()
        .items
        .iter()
        .filter_map(|item| match item {
            microcrab_transport::ContentItem::User {
                source: None,
                parts,
            } => Some(
                parts
                    .iter()
                    .filter_map(|part| match part {
                        InputPart::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            _ => None,
        })
        .collect()
}

async fn initial_epoch_contract(storage: &str) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(1),
    });
    let (manager, _root) = manager(model.clone(), storage).await;
    let managed = create(&manager).await;
    submit(&manager, &managed, "first actual input").await;
    let departure = managed.journal.cursor().await.unwrap();
    let selected = context(&managed);
    let route = managed.journal.restore_model_route().await.unwrap();
    let epoch = managed.session.state().epoch_id;
    let undone = travel(&manager, &managed, "undo").await;
    assert!(user_texts(&managed).is_empty());
    assert!(
        !undone["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["role"] == "assistant")
    );
    assert_eq!(managed.session.state().epoch_id, epoch);
    assert_eq!(undone["history_available"]["redo"], true);
    let initial = managed.journal.cursor().await.unwrap();
    assert_ne!(initial, departure);
    let baseline = context(&managed);
    travel(&manager, &managed, "redo").await;
    assert_eq!(managed.journal.cursor().await.as_ref(), Some(&departure));
    assert_eq!(context(&managed), selected);
    assert_eq!(managed.journal.restore_model_route().await.unwrap(), route);
    travel(&manager, &managed, "undo").await;
    assert_eq!(managed.journal.cursor().await.as_ref(), Some(&initial));
    assert_eq!(context(&managed), baseline);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    manager.close().await.unwrap();
}

struct FourRoundModel(AtomicUsize);
#[async_trait::async_trait]
impl microcrab_session::Client for FourRoundModel {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let call = self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Response {
            provider_metadata: None,
            content: format!("history round {call}"),
            tool_calls: if (1..=3).contains(&call) {
                vec![microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: format!("history-effect-{call}"),
                    name: "history_effect".into(),
                    arguments: json!({"round":call}),
                }]
            } else {
                vec![]
            },
            usage: Usage::default(),
        })
    }
}

async fn multi_round_contract(storage: &str) {
    use crate::manager::client_tools::{BindRequest, ResultRequest};
    let model = Arc::new(FourRoundModel(AtomicUsize::new(0)));
    let (manager, _root) = manager(model.clone(), storage).await;
    let created = commands::create(&manager, "owner", &command("session.create", None, json!({
        "project_id":"project", "operation_id":uuid::Uuid::new_v4().to_string(),
        "client_tools":[{
            "name":"history_effect", "description":"Count a history test effect",
            "parameters":{"type":"object","properties":{"round":{"type":"integer"}},"required":["round"],"additionalProperties":false},
            "permissions":[{"tool":{"kind":"exact","value":"history_effect"},"args":[],"action":"allow"}],
            "reclaimable":true
        }]
    }))).await.unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let bound = managed
        .client_tools
        .bind(
            "history-socket",
            &BindRequest {
                client_id: "history-browser".into(),
                catalog_digest: managed.client_tools.snapshot().await.catalog_digest,
            },
        )
        .await
        .unwrap();
    submit(&manager, &managed, "keep this complete prior turn").await;
    let prior = managed.journal.cursor().await.unwrap();
    let prior_context = context(&managed);
    let mut subscription = managed.hub.subscribe();
    let effects = async {
        for round in 1..=3 {
            let call = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let event = subscription.receiver.recv().await.unwrap();
                    if matches!(
                        &event.event,
                        super::super::super::SessionRuntimeEvent::ClientToolRequested(_)
                    ) {
                        break event.event.payload()["call"].clone();
                    }
                }
            })
            .await
            .expect("each real tool round must request its effect");
            managed
                .client_tools
                .result(
                    "history-socket",
                    &ResultRequest {
                        lease_id: bound.lease_id.clone().unwrap(),
                        invocation_id: call["invocation_id"].as_str().unwrap().into(),
                        result: Some(json!({"round":round,"effect":"completed once"})),
                        error: None,
                    },
                )
                .await
                .unwrap();
        }
    };
    tokio::join!(
        submit(&manager, &managed, "remove this entire four round turn"),
        effects
    );
    let departure = managed.journal.cursor().await.unwrap();
    let selected = context(&managed);
    let route = managed.journal.restore_model_route().await.unwrap();
    let calls = managed.client_tools.snapshot().await.calls.len();
    assert_eq!(calls, 3);
    assert_eq!(model.0.load(Ordering::SeqCst), 5);
    assert_eq!(
        managed
            .session
            .context_snapshot()
            .unwrap()
            .items
            .iter()
            .filter(|item| matches!(item, microcrab_transport::ContentItem::Tool { .. }))
            .count(),
        3
    );
    // An immediate-parent Undo would retain some of this Turn's actual tool rounds.
    travel(&manager, &managed, "undo").await;
    assert_eq!(managed.journal.cursor().await.as_ref(), Some(&prior));
    assert_eq!(context(&managed), prior_context);
    assert_eq!(user_texts(&managed), ["keep this complete prior turn"]);
    travel(&manager, &managed, "redo").await;
    assert_eq!(managed.journal.cursor().await.as_ref(), Some(&departure));
    assert_eq!(context(&managed), selected);
    assert_eq!(managed.journal.restore_model_route().await.unwrap(), route);
    assert_eq!(model.0.load(Ordering::SeqCst), 5);
    assert_eq!(managed.client_tools.snapshot().await.calls.len(), calls);
    while let Ok(event) = subscription.receiver.try_recv() {
        assert!(
            !matches!(
                event.event,
                super::super::super::SessionRuntimeEvent::ClientToolRequested(_)
            ),
            "navigation must not replay a client tool effect"
        );
    }
    manager.close().await.unwrap();
}

// The injected Client still goes through the real catalog, route witness and
// next-input model admission. All configuration and history are test-owned.
async fn route_manager(storage: &str, model: Arc<dyn microcrab_session::Client>) -> Arc<Manager> {
    let (fixture, root) = manager(model.clone(), storage).await;
    fixture.close().await.unwrap();
    let catalog = root.join("history-providers.toml");
    std::fs::write(
        &catalog,
        r#"
[provider.a]
protocol = 'openai-compatible'
[provider.a.options]
endpoint = 'http://127.0.0.1:1/v1/chat/completions'
model = 'model-a'
[provider.a.models.model-a]
[provider.b]
protocol = 'openai-compatible'
[provider.b.options]
endpoint = 'http://127.0.0.1:1/v1/chat/completions'
model = 'model-b'
[provider.b.models.model-b]
"#,
    )
    .unwrap();
    let path = root.join("history-server.json");
    std::fs::write(&path, serde_json::to_vec(&json!({
        "data_dir":fixture.config.data_dir, "storage":storage,
        "provider":{"config_file":catalog,"name":"a"}, "auto_rename":false,
        "auth":{
            "issuer":"http://127.0.0.1:9900","authorization_endpoint":"http://127.0.0.1:9900/authorize",
            "token_endpoint":"http://127.0.0.1:9900/token","jwks_uri":"http://127.0.0.1:9900/jwks",
            "client_id":"test","redirect_uri":"http://127.0.0.1:4096/api/auth/callback",
            "allowed_origins":["http://127.0.0.1:4096"],"jwt_secret":"test-key-at-least-thirty-two-characters"
        }
    })).unwrap()).unwrap();
    Manager::with_client(Config::read(path).unwrap(), model)
        .await
        .unwrap()
}

async fn route_epoch_contract(storage: &str) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(2),
    });
    let manager = route_manager(storage, model.clone()).await;
    let managed = create(&manager).await;
    let id = managed.record.lock().await.id.clone();
    submit(
        &manager,
        &managed,
        "identical input across reset turn counters",
    )
    .await;
    let old_epoch = managed.session.state().epoch_id;
    let inherited = managed.session.context_snapshot().unwrap().items;
    let revision = managed.record.lock().await.revision;
    commands::execute(
        &manager,
        &crate::auth::Principal::for_test("owner"),
        &command(
            "session.set_model",
            Some(&id),
            json!({
                "expected_revision":revision,"provider_id":"b","model":"model-b"
            }),
        ),
    )
    .await
    .unwrap();
    submit(
        &manager,
        &managed,
        "identical input across reset turn counters",
    )
    .await;
    let state = managed.session.state();
    assert!(state.epoch_id > old_epoch);
    assert_eq!(state.last_turn_id(), 1);
    let departure = managed.journal.cursor().await.unwrap();
    let selected = context(&managed);
    let route = managed.journal.restore_model_route().await.unwrap();
    assert_eq!(route.1.as_ref().unwrap().model, "model-b");
    travel(&manager, &managed, "undo").await;
    assert_eq!(
        managed.session.state().epoch_id,
        state.epoch_id,
        "first-input Undo must select this model epoch's initial baseline"
    );
    assert_eq!(
        user_texts(&managed),
        ["identical input across reset turn counters"]
    );
    let baseline = managed.session.context_snapshot().unwrap();
    assert_eq!(&baseline.items[..inherited.len()], inherited.as_slice());
    assert_eq!(managed.routing.selection().model, "model-b");
    assert_eq!(
        managed
            .record
            .lock()
            .await
            .model_selection
            .as_ref()
            .unwrap()
            .model,
        "model-b"
    );
    let initial = managed.journal.cursor().await.unwrap();
    travel(&manager, &managed, "redo").await;
    assert_eq!(managed.journal.cursor().await.as_ref(), Some(&departure));
    assert_eq!(context(&managed), selected);
    assert_eq!(managed.journal.restore_model_route().await.unwrap(), route);
    travel(&manager, &managed, "undo").await;
    assert_eq!(managed.journal.cursor().await.as_ref(), Some(&initial));
    assert_eq!(context(&managed), serde_json::to_value(baseline).unwrap());
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    manager.close().await.unwrap();
}

async fn compaction_epoch_contract(storage: &str) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(2),
    });
    let (manager, _root) = manager(model.clone(), storage).await;
    let managed = create(&manager).await;
    submit(&manager, &managed, "retired input").await;
    managed
        .session
        .compact("certified summary baseline", json!({}))
        .await
        .unwrap();
    let marker = managed.session.compaction().unwrap();
    submit(&manager, &managed, "first actual input after compaction").await;
    let departure = managed.journal.cursor().await.unwrap();
    let selected = context(&managed);
    let epoch = managed.session.state().epoch_id;
    travel(&manager, &managed, "undo").await;
    assert_eq!(managed.session.compaction().unwrap(), marker);
    assert_eq!(managed.session.state().epoch_id, epoch);
    assert!(
        context(&managed)
            .to_string()
            .contains("certified summary baseline")
    );
    assert!(
        !user_texts(&managed)
            .iter()
            .any(|text| text == "first actual input after compaction" || text == "retired input")
    );
    let baseline = context(&managed);
    let initial = managed.journal.cursor().await.unwrap();
    let id = managed.record.lock().await.id.clone();
    assert!(
        commands::execute(
            &manager,
            &crate::auth::Principal::for_test("owner"),
            &command("history.travel", Some(&id), json!({"direction":"undo"}))
        )
        .await
        .is_err(),
        "synthetic summary cannot become the next real input target"
    );
    assert_eq!(managed.journal.cursor().await.as_ref(), Some(&initial));
    assert_eq!(context(&managed), baseline);
    travel(&manager, &managed, "redo").await;
    assert_eq!(managed.journal.cursor().await.as_ref(), Some(&departure));
    assert_eq!(context(&managed), selected);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    manager.close().await.unwrap();
}

async fn unwitnessed_reopen_contract(storage: &str) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(2),
    });
    let (manager, _root) = manager(model.clone(), storage).await;
    let managed = create(&manager).await;
    let id = managed.record.lock().await.id.clone();
    submit(
        &manager,
        &managed,
        "preserve prior input across live recovery",
    )
    .await;
    submit(
        &manager,
        &managed,
        "remove latest input after readonly reopen",
    )
    .await;
    let record = manager.record("owner", &id).await.unwrap();
    let config = manager.config.clone();
    manager.close().await.unwrap();
    drop(managed);
    // Model a historical envelope without a route witness after releasing the
    // active writer. Reopening must not persist its live recovery projection.
    let (journal, source) = manager.inspect_journal(&record).await.unwrap();
    let source = source.unwrap();
    journal
        .prepare_model_route(record.model_selection.clone().unwrap(), None)
        .await;
    journal.commit(source.clone()).await.unwrap();
    let departure = journal.cursor().await.unwrap();
    journal.close().await.unwrap();
    drop(manager);
    let reopened = Manager::with_client(config, model.clone()).await.unwrap();
    let restored = reopened
        .active(reopened.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    assert!(restored.session.state().epoch_id > source.context.epoch);
    assert_ne!(
        context(&restored),
        serde_json::to_value(&source.context).unwrap()
    );
    assert_eq!(restored.journal.cursor().await.as_ref(), Some(&departure));
    assert_eq!(
        serde_json::to_value(restored.journal.head_recover().await.unwrap().unwrap()).unwrap(),
        serde_json::to_value(&source).unwrap()
    );
    let snapshot = reopened.refresh(&restored).await.unwrap();
    let message_id = snapshot["messages"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|message| message["role"] == "user")
        .unwrap()["id"]
        .as_str()
        .unwrap();
    let target = command(
        "history.travel",
        Some(&id),
        json!({
            "direction":"undo","message_id":message_id,"expected_checkpoint_id":departure.checkpoint_id
        }),
    );
    let live = context(&restored);
    for (principal, request) in [
        (crate::auth::Principal::for_test("intruder"), target.clone()),
        (
            crate::auth::Principal::for_test("owner"),
            command(
                "history.travel",
                Some(&id),
                json!({
                    "direction":"undo","message_id":message_id,"expected_checkpoint_id":"0".repeat(64)
                }),
            ),
        ),
    ] {
        assert!(
            commands::execute(&reopened, &principal, &request)
                .await
                .is_err()
        );
        assert_eq!(restored.journal.cursor().await.as_ref(), Some(&departure));
        assert_eq!(context(&restored), live);
    }
    let undone = commands::execute(
        &reopened,
        &crate::auth::Principal::for_test("owner"),
        &target,
    )
    .await
    .unwrap();
    assert_eq!(
        undone["restored_input"]["parts"],
        json!([{"type":"text","text":"remove latest input after readonly reopen"}])
    );
    assert_eq!(
        user_texts(&restored),
        ["preserve prior input across live recovery"]
    );
    travel(&reopened, &restored, "redo").await;
    assert_eq!(restored.journal.cursor().await.as_ref(), Some(&departure));
    assert_eq!(
        serde_json::to_value(restored.journal.head_recover().await.unwrap().unwrap()).unwrap(),
        serde_json::to_value(source).unwrap()
    );
    assert_eq!(
        user_texts(&restored),
        [
            "preserve prior input across live recovery",
            "remove latest input after readonly reopen"
        ]
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    reopened.close().await.unwrap();
}

async fn initialized_guest_contract(storage: &str) {
    use microcrab_session::runtime::journal::AgentInitStatus;
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(1),
    });
    let (fixture, root) = manager(model.clone(), storage).await;
    let mut config = fixture.config.clone();
    fixture.close().await.unwrap();
    let catalog = r#"[{"name":"history-init","modes":[{"name":"execute","role":"both"}],"variables":{},"tools":[{"name":"echo","description":"fixture","parameters":{"type":"object"}}]}]"#;
    let escaped: String = catalog
        .bytes()
        .map(|byte| format!("\\{byte:02x}"))
        .collect();
    let source = format!(
        r#"(component
      (core module $m
        (memory (export "memory") 2)
        (global $heap (mut i32) (i32.const 4096))
        (data (i32.const 128) "{escaped}")
        (func (export "realloc") (param i32 i32 i32 i32) (result i32)
          (local $old i32) global.get $heap local.tee $old
          local.get 3 i32.add global.set $heap local.get $old)
        (func (export "describe") (param i32 i32) (result i32)
          i32.const 0 i32.const 0 i32.store
          i32.const 4 i32.const 128 i32.store
          i32.const 8 i32.const {length} i32.store i32.const 0)
        (func (export "invoke") (param i32 i32 i32 i32 i32 i32) (result i32)
          i32.const 16 i32.const 0 i32.store
          i32.const 20 i32.const 0 i32.store
          i32.const 24 i32.const 0 i32.store i32.const 16))
      (core instance $i (instantiate $m))
      (func (export "describe") (param "host-info" string) (result (result string (error string)))
        (canon lift (core func $i "describe") (memory (core memory $i "memory")) (realloc (core func $i "realloc"))))
      (func (export "invoke") (param "host-info" string) (param "name" string) (param "arguments" string)
        (result (result string (error string)))
        (canon lift (core func $i "invoke") (memory (core memory $i "memory")) (realloc (core func $i "realloc")))))"#,
        length = catalog.len()
    );
    let path = root.join("history-init.wasm");
    std::fs::write(&path, typed_agent_fixture::component(&source)).unwrap();
    config.agent = Some(crate::config::Agent {
        path,
        name: Some("history-init".into()),
        mode: Some("execute".into()),
        restore_only: false,
    });
    let manager = Manager::with_client(config.clone(), model.clone())
        .await
        .unwrap();
    let created = commands::create(
        &manager,
        "owner",
        &command(
            "session.create",
            None,
            json!({
                "project_id":"project", "agent":"history-init", "mode":"execute",
                "operation_id":uuid::Uuid::new_v4().to_string()
            }),
        ),
    )
    .await
    .unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let slot = manager.registry.lock().unwrap().get(&id).cloned().unwrap();
    let managed = slot.active.lock().await.clone().unwrap();
    // This fixture tests navigation across completed initialization. Its owner
    // explicitly trusts lifecycle setup before opening the generated component.
    let revision = managed
        .session
        .permission_controller()
        .snapshot()
        .unwrap()
        .revision;
    let permission = commands::execute(
        &manager,
        &crate::auth::Principal::for_test("owner"),
        &command(
            "permission.set",
            Some(&id),
            json!({
                "expected_revision":revision, "overrides":[],
                "initialization":"allow", "review":false
            }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(permission["config"]["initialization"], "allow");
    assert_eq!(
        manager.record("owner", &id).await.unwrap().initialization,
        microcrab_permission::Action::Allow
    );
    manager.client_tool_command(&crate::auth::Principal::for_test("owner"), "history-init-executor", &command(
        "client_tools.bind", Some(&id), json!({
            "client_id":"history-init-browser", "catalog_digest":managed.client_tools.snapshot().await.catalog_digest
        })
    )).await.unwrap();
    commands::execute(
        &manager,
        &crate::auth::Principal::for_test("owner"),
        &command("session.open", Some(&id), json!({})),
    )
    .await
    .unwrap();
    assert!(managed.session.agent_initialization_complete());
    let ready = managed.session.state().agent_init;
    assert_eq!(ready.len(), 1);
    assert!(matches!(ready[0].status, AgentInitStatus::Ready));
    assert!(!managed.session.state().agent_initialization_pending);
    submit(
        &manager,
        &managed,
        "first input after durable guest initialization",
    )
    .await;
    let departure = managed.journal.cursor().await.unwrap();
    let selected = context(&managed);
    travel(&manager, &managed, "undo").await;
    assert!(user_texts(&managed).is_empty());
    assert!(
        !managed.session.state().agent_initialization_pending,
        "Undo must not select the pre-initialization entry Recover"
    );
    assert_eq!(
        serde_json::to_value(&managed.session.state().agent_init).unwrap(),
        serde_json::to_value(&ready).unwrap()
    );
    assert!(managed.session.agent_initialization_complete());
    // Reopening the selected initial baseline must consume the retained Ready
    // witness, rather than admitting a second initialization attempt.
    commands::execute(
        &manager,
        &crate::auth::Principal::for_test("owner"),
        &command("session.open", Some(&id), json!({})),
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::to_value(&managed.session.state().agent_init).unwrap(),
        serde_json::to_value(&ready).unwrap()
    );
    travel(&manager, &managed, "redo").await;
    assert_eq!(managed.journal.cursor().await.as_ref(), Some(&departure));
    assert_eq!(context(&managed), selected);
    assert_eq!(
        serde_json::to_value(&managed.session.state().agent_init).unwrap(),
        serde_json::to_value(&ready).unwrap()
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    manager.close().await.unwrap();
}

macro_rules! backend_cases {
    ($feature:literal, $module:ident, $storage:literal) => {
        #[cfg(feature = $feature)]
        mod $module {
            use super::*;
            #[tokio::test]
            async fn first_input_undo_keeps_guest_ready_evidence_without_reinitialization() {
                initialized_guest_contract($storage).await;
            }
            #[tokio::test]
            async fn first_input_restores_empty_same_epoch_and_exact_redo() {
                initial_epoch_contract($storage).await;
            }
            #[tokio::test]
            async fn latest_input_spans_four_rounds_and_tool_effects_without_replay() {
                multi_round_contract($storage).await;
            }
            #[tokio::test]
            async fn set_model_first_turn_preserves_inherited_prefix_and_same_epoch_initial() {
                route_epoch_contract($storage).await;
            }
            #[tokio::test]
            async fn compaction_summary_is_baseline_not_a_real_input_target() {
                compaction_epoch_contract($storage).await;
            }
            #[tokio::test]
            async fn unwitnessed_readonly_reopen_resolves_live_source_drift_without_rewrite() {
                unwitnessed_reopen_contract($storage).await;
            }
        }
    };
}
backend_cases!("storage-file", file, "file");
backend_cases!("storage-sqlite", sqlite, "sqlite");
