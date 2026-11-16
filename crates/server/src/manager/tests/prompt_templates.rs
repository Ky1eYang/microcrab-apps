//! Prompt defaults resolve only at creation; durable snapshots outlive settings.
use super::*;
use serde_json::json;

fn model() -> Arc<Model> {
    Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(64),
    })
}
async fn create(manager: &Manager, extra: Value) -> String {
    let mut params =
        json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()});
    params
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    commands::create(manager, "owner", &command("session.create", None, params))
        .await
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned()
}
async fn save(manager: &Manager, scope: &str, revision: u64, values: Value) -> Value {
    let mut params = json!({"expected_revision":revision,"overrides":values});
    if scope == "project.settings.set" {
        params["project_id"] = json!("project");
    }
    super::super::settings::command(manager, "owner", &command(scope, None, params))
        .await
        .unwrap()
}
struct LegacyDelegateModel {
    calls: AtomicUsize,
    requests: std::sync::Mutex<Vec<Content>>,
    entered: Semaphore,
}
impl Default for LegacyDelegateModel {
    fn default() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            requests: std::sync::Mutex::new(Vec::new()),
            entered: Semaphore::new(0),
        }
    }
}
#[async_trait::async_trait]
impl microcrab_session::Client for LegacyDelegateModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.requests.lock().unwrap().push(content.clone());
        self.entered.add_permits(1);
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        Ok(Response {
            provider_metadata: None,
            content: if first {
                String::new()
            } else {
                "legacy snapshot answer".into()
            },
            tool_calls: if first {
                vec![microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: "legacy-delegate".into(),
                    name: "delegate".into(),
                    arguments: json!({"action":"start","target":"execute","mode":"execute","input":"Verify inherited frozen prompts","context_mode":"clean"}),
                }]
            } else {
                vec![]
            },
            usage: Default::default(),
        })
    }
}
#[cfg(feature = "storage-file")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires existing root-built Universal component in MICROCRAB_TEST_UNIVERSAL_COMPONENT"]
async fn legacy_prompt_history_snapshot_reaches_main_and_real_child_factory() {
    let component = std::env::var("MICROCRAB_TEST_UNIVERSAL_COMPONENT").unwrap();
    let model = Arc::new(LegacyDelegateModel::default());
    let (bootstrap, _root) = manager(model.clone(), "file").await;
    let mut config = bootstrap.config.clone();
    bootstrap.close().await.unwrap();
    config.agent =
        Some(serde_json::from_value(json!({"path":component,"name":"universal"})).unwrap());
    config.session_defaults.review = Some(false);
    config.session_defaults.initialization = Some(microcrab_permission::Action::Allow);
    config.session_defaults.permissions = Some(
        serde_json::from_value(json!([
            {"tool":{"kind":"exact","value":"delegate"},"action":"allow"}
        ]))
        .unwrap(),
    );
    let original = Manager::with_client(config.clone(), model.clone())
        .await
        .unwrap();
    let id = create(
        &original,
        json!({"agent":"universal","mode":"execute",
        "system_template":"LEGACY {{ client_prompt }}","client_prompt":"  {{ literal }} ",
        "rename_template":"LEGACY NAME {{ user_input }}"}),
    )
    .await;
    let managed = original
        .active(original.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let frozen = managed.session.prompt_config().clone();
    let system = managed.session.context_snapshot().unwrap().system;
    original.close().await.unwrap();
    // Decode-only legacy creator metadata has no prompt field. Immutable Journal
    // bytes still witness the original runtime snapshot and are never rewritten.
    let path = config.data_dir.join("directory/directory.json");
    let mut directory: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    directory["sessions"][&id]
        .as_object_mut()
        .unwrap()
        .remove("prompt_config");
    std::fs::write(&path, serde_json::to_vec(&directory).unwrap()).unwrap();
    config.session_defaults.system_template = Some("NEW DEFAULT".into());
    config.session_defaults.client_prompt = Some("new client".into());
    config.session_defaults.rename_template = Some("NEW NAME {{ user_input }}".into());
    let restored = Manager::with_client(config, model.clone()).await.unwrap();
    let record = restored.record("owner", &id).await.unwrap();
    assert!(record.prompt_config.is_none());
    let managed = restored.active(record, true).await.unwrap();
    assert_eq!(managed.session.prompt_config(), &frozen);
    assert_eq!(managed.session.context_snapshot().unwrap().system, system);
    let submit = command(
        "turn.submit",
        Some(&id),
        json!({"operation_id":uuid::Uuid::new_v4().to_string(),
        "input":microcrab_session::SessionInput::text("Delegate a bounded task")}),
    );
    commands::submit(&restored, managed.clone(), &submit)
        .await
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        model.entered.acquire_many(3),
    )
    .await
    .expect("main, delegated child and main continuation reach the model")
    .unwrap()
    .forget();
    let requests = model.requests.lock().unwrap().clone();
    assert!(
        requests
            .iter()
            .any(|request| request.tools.iter().any(|tool| tool.name == "delegate"))
    );
    assert!(
        requests
            .iter()
            .any(|request| !request.tools.iter().any(|tool| tool.name == "delegate")),
        "actual child model request excludes recursive delegation"
    );
    for request in requests {
        assert_eq!(
            request.system, system,
            "main and real child both retain the Journal snapshot"
        );
    }
    restored.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn prompt_defaults_sparse_inheritance_empty_reset_and_literal_bytes() {
    use microcrab_session::context::system::DEFAULT_SYSTEM;
    let (manager, _root) = manager(model(), "file").await;
    let baseline = create(&manager, json!({})).await;
    assert_eq!(
        manager
            .record("owner", &baseline)
            .await
            .unwrap()
            .prompt_config
            .unwrap()
            .client_prompt,
        super::super::prompt_defaults::DEFAULT_CLIENT_PROMPT
    );
    save(
        &manager,
        "settings.set",
        0,
        json!({"system_template":"USER {{ client_prompt }}","client_prompt":"user"}),
    )
    .await;
    save(
        &manager,
        "project.settings.set",
        0,
        json!({"client_prompt":"  project {{ literal }}\n"}),
    )
    .await;
    let inherited = create(&manager, json!({"client_prompt":null})).await;
    let record = manager.record("owner", &inherited).await.unwrap();
    let config = record.prompt_config.clone().unwrap();
    assert_eq!(config.client_prompt, "  project {{ literal }}\n");
    assert_eq!(config.system_template, "USER {{ client_prompt }}");
    let managed = manager.active(record, true).await.unwrap();
    assert_eq!(
        managed.session.context_snapshot().unwrap().system,
        "USER   project {{ literal }}\n"
    );
    assert!(
        manager
            .configuration
            .get("owner", "session_settings", &inherited)
            .await
            .unwrap()
            .is_none()
    );
    let reset = create(&manager, json!({"system_template":"","client_prompt":"","rename_template":"CUSTOM {{ user_input }}"})).await;
    let config = manager
        .record("owner", &reset)
        .await
        .unwrap()
        .prompt_config
        .unwrap();
    assert_eq!(config.system_template, DEFAULT_SYSTEM);
    assert_eq!(config.client_prompt, "");
    let source = manager
        .configuration
        .get("owner", "session_settings", &reset)
        .await
        .unwrap()
        .unwrap()
        .value
        .unwrap();
    assert_eq!(source["system_template"], "");
    assert_eq!(source["client_prompt"], "");
    assert!(source.get("compact_template").is_none());
    manager.close().await.unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn prompt_invalid_templates_reject_before_pins_records_tools_or_receipts() {
    let (manager, _root) = manager(model(), "file").await;
    for invalid in [
        json!({"system_template":"{% if invalid"}),
        json!({"system_template":"{{ agent.required }}"}),
        json!({"system_template":"{{ range(end=100000) }}"}),
        json!({"system_template":"{% for rule in agent.rules %}{% for hint in agent.hints %}{% endfor %}{% endfor %}"}),
        json!({"system_template":"{% set captured %}hidden{% endset %}"}),
        json!({"system_template":"{% if client_prompt | json_encode %}hidden{% endif %}"}),
        json!({"compact_template":"{{ user_input | safe }}"}),
        json!({"auto_compact_template":"{{ user_input }}"}),
        json!({"rename_template":"{{ not_declared }}"}),
        json!({"action_review_template":"{% if tool %}bad{% endif %}"}),
        json!({"client_prompt":"x".repeat(256 * 1024 + 1)}),
    ] {
        let operation = uuid::Uuid::new_v4().to_string();
        let id = microcrab_storage::history::content_hash(&json!(["owner", operation])).unwrap()
            [..32]
            .to_owned();
        let mut params = json!({"project_id":"project","operation_id":operation});
        params
            .as_object_mut()
            .unwrap()
            .extend(invalid.as_object().unwrap().clone());
        assert!(
            commands::create(&manager, "owner", &command("session.create", None, params))
                .await
                .is_err()
        );
        assert!(manager.directory.get("owner", &id).await.unwrap().is_none());
        assert!(
            manager
                .directory
                .get_operation("owner", &operation)
                .await
                .unwrap()
                .is_none()
        );
        assert!(manager.pins.component(&id).await.unwrap().is_none());
        assert!(
            manager
                .configuration
                .get("owner", "session_effective", &id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(!manager.client_tool_owners.lock().unwrap().contains_key(&id));
    }
    assert!(manager.registry.lock().unwrap().is_empty());
    assert!(
        super::super::settings::command(
            &manager,
            "owner",
            &command(
                "settings.set",
                None,
                json!({"expected_revision":0,"overrides":{"system_template":"{{ range(end=100000) }}"}})
            )
        )
        .await
        .is_err()
    );
    assert!(
        manager
            .configuration
            .get("owner", "settings", "user")
            .await
            .unwrap()
            .is_none()
    );
    manager.close().await.unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn prompt_settings_follow_selected_history_with_legacy_creator_metadata() {
    let model = model();
    let (original, _root) = manager(model.clone(), "file").await;
    let id = create(
        &original,
        json!({"system_template":"RETAINED {{ client_prompt }}", "client_prompt":"  original {{ literal }}\n", "rename_template":"RETAINED NAME {{ user_input }}"}),
    )
    .await;
    let managed = original
        .active(original.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let frozen = managed.session.prompt_config().clone();
    let config = original.config.clone();
    original.close().await.unwrap();
    let path = config.data_dir.join("directory/directory.json");
    let mut directory: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    directory["sessions"][&id]
        .as_object_mut()
        .unwrap()
        .remove("prompt_config");
    std::fs::write(&path, serde_json::to_vec(&directory).unwrap()).unwrap();
    let restored = Manager::with_client(config, model).await.unwrap();
    for active in [false, true] {
        if active {
            restored
                .active(restored.record("owner", &id).await.unwrap(), true)
                .await
                .unwrap();
        }
        let observed = super::super::settings::command(
            &restored,
            "owner",
            &command("session.settings.get", Some(&id), json!({})),
        )
        .await
        .unwrap();
        for field in ["effective", "desired_effective"] {
            assert_eq!(observed[field]["system_template"], frozen.system_template);
            assert_eq!(observed[field]["client_prompt"], frozen.client_prompt);
            assert_eq!(observed[field]["rename_template"], frozen.rename_template);
        }
        if !active {
            assert!(
                restored.registry.lock().unwrap().is_empty(),
                "settings reads must not activate dormant Sessions"
            );
        } else {
            let saved = super::super::settings::command(
                &restored,
                "owner",
                &command(
                    "session.settings.set",
                    Some(&id),
                    json!({
                        "expected_revision":observed["revision"],
                        "overrides":{"limits":{"max_rounds":12}}
                    }),
                ),
            )
            .await
            .unwrap();
            assert_eq!(saved["effective"]["client_prompt"], frozen.client_prompt);
            assert_eq!(
                saved["desired_effective"]["system_template"],
                frozen.system_template
            );
            assert_eq!(
                saved["overrides"]["rename_template"],
                "RETAINED NAME {{ user_input }}"
            );
        }
    }
    restored.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn prompt_snapshot_survives_defaults_edits_unrelated_save_restart_and_fork() {
    let model = model();
    let (manager, _root) = manager(model.clone(), "file").await;
    save(&manager,"settings.set",0,json!({"system_template":"ORIGINAL {{ client_prompt }}","client_prompt":"  original\n","compact_template":"COMPACT {{ user_input }}","rename_template":"NAME {{ user_input }}"})).await;
    let id = create(
        &manager,
        json!({"rename_template":"CREATION NAME {{ user_input }}"}),
    )
    .await;
    let record = manager.record("owner", &id).await.unwrap();
    let frozen = record.prompt_config.clone().unwrap();
    let managed = manager.active(record, true).await.unwrap();
    let context = managed.session.context_snapshot().unwrap().system;
    save(
        &manager,
        "settings.set",
        1,
        json!({"system_template":"REPLACEMENT","client_prompt":"new"}),
    )
    .await;
    super::super::settings::command(
        &manager,
        "owner",
        &command(
            "session.settings.set",
            Some(&id),
            json!({"expected_revision":1,"overrides":{"limits":{"max_rounds":12}}}),
        ),
    )
    .await
    .unwrap();
    let sparse = manager
        .configuration
        .get("owner", "session_settings", &id)
        .await
        .unwrap()
        .unwrap()
        .value
        .unwrap();
    assert_eq!(sparse["rename_template"], "CREATION NAME {{ user_input }}");
    assert!(sparse.get("client_prompt").is_none());
    assert_eq!(managed.session.prompt_config(), &frozen);
    assert_eq!(managed.session.context_snapshot().unwrap().system, context);
    let error = super::super::settings::command(
        &manager,
        "owner",
        &command(
            "session.settings.set",
            Some(&id),
            json!({"expected_revision":2,"overrides":{"client_prompt":"live change"}}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "configuration_rebuild_required");
    let fork = super::super::fork::execute(
        &manager,
        "owner",
        &command(
            "session.fork",
            Some(&id),
            json!({"operation_id":uuid::Uuid::new_v4().to_string()}),
        ),
    )
    .await
    .unwrap();
    let fork_id = fork["id"].as_str().unwrap();
    assert_eq!(
        manager
            .record("owner", fork_id)
            .await
            .unwrap()
            .prompt_config
            .as_ref(),
        Some(&frozen)
    );
    let config = manager.config.clone();
    manager.close().await.unwrap();
    let restarted = Manager::with_client(config, model).await.unwrap();
    let restored = restarted
        .active(restarted.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    assert_eq!(restored.session.prompt_config(), &frozen);
    assert_eq!(restored.session.context_snapshot().unwrap().system, context);
    let new_id = create(&restarted, json!({})).await;
    assert_eq!(
        restarted
            .record("owner", &new_id)
            .await
            .unwrap()
            .prompt_config
            .unwrap()
            .system_template,
        "REPLACEMENT"
    );
    restarted.close().await.unwrap();
}
