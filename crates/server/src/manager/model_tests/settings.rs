//! Settings inheritance must configure the next admitted provider route.
use super::*;
#[path = "../../../tests/support/host_shell_fixture.rs"]
mod host_shell_fixture;
#[allow(dead_code)]
#[path = "../../../tests/support/typed_agent_fixture.rs"]
mod typed_agent_fixture;

async fn save(
    manager: &Manager,
    id: &str,
    revision: u64,
    overrides: Value,
) -> Result<Value, WireError> {
    super::super::settings::command(
        manager,
        "owner",
        &command(
            "session.settings.set",
            Some(id),
            json!({"expected_revision":revision,"overrides":overrides}),
        ),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settings_reset_to_admin_fallback_changes_next_wire_route_and_rejects_stale_cas() {
    let root = tempfile::tempdir().unwrap();
    let a = provider(10).await;
    let b = provider(10).await;
    let manager = Manager::with_host(config(root.path(), &a, &b), Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    assert!(manager.config.session_defaults.model_selection.is_none());
    let managed = create(&manager).await;
    let id = managed.record.lock().await.id.clone();
    let selected_b = selection("b", "model-b", Some("high"));
    let fallback = selection("a", "model-a", None);
    let saved = save(&manager, &id, 0, json!({"model_selection":selected_b}))
        .await
        .unwrap();
    assert_eq!(saved["application"], "applied");
    submit(&manager, &managed, "use explicit B").await;
    assert_eq!(managed.routing.selection(), selected_b);
    let revision = managed.record.lock().await.revision;
    let cursor = managed.journal.cursor().await;

    let error = save(&manager, &id, 0, json!({})).await.unwrap_err();
    assert_eq!(error.code, "revision_conflict");
    assert_eq!(managed.record.lock().await.revision, revision);
    assert_eq!(managed.routing.selection(), selected_b);

    let reset = save(&manager, &id, 1, json!({})).await.unwrap();
    assert_eq!(reset["revision"], 2);
    assert_eq!(reset["applied_revision"], 2);
    assert_eq!(reset["application"], "applied");
    assert!(reset["overrides"].get("model_selection").is_none());
    assert_eq!(reset["sources"]["model_selection"], "admin");
    assert_eq!(
        reset["desired_effective"]["model_selection"],
        json!(fallback)
    );
    assert_eq!(reset["effective"]["model_selection"], json!(selected_b));
    assert_eq!(
        managed.record.lock().await.model_selection,
        Some(fallback.clone())
    );
    assert_eq!(
        manager
            .directory
            .get("owner", &id)
            .await
            .unwrap()
            .unwrap()
            .model_selection,
        Some(fallback.clone())
    );
    assert_eq!(managed.journal.cursor().await, cursor);
    assert_eq!(managed.routing.selection(), selected_b);

    submit(&manager, &managed, "inherit admin A").await;
    assert_eq!(managed.routing.selection(), fallback);
    assert_eq!(a.wire.requests.lock().unwrap().len(), 1);
    assert_eq!(b.wire.requests.lock().unwrap().len(), 1);
    assert_eq!(a.wire.requests.lock().unwrap()[0].1["model"], "model-a");
    let observed = super::super::settings::command(
        &manager,
        "owner",
        &command("session.settings.get", Some(&id), json!({})),
    )
    .await
    .unwrap();
    assert_eq!(observed["effective"]["model_selection"], json!(fallback));
    assert_eq!(
        observed["desired_effective"]["model_selection"],
        json!(fallback)
    );
    assert_eq!(observed["applied_revision"], 2);
    manager.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settings_reset_inherits_project_before_user_model_selection() {
    let root = tempfile::tempdir().unwrap();
    let a = provider(10).await;
    let b = provider(10).await;
    let manager = Manager::with_host(config(root.path(), &a, &b), Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let user = selection("a", "model-a", Some("low"));
    let project = selection("a", "model-a", Some("high"));
    for (method, params) in [
        (
            "settings.set",
            json!({"expected_revision":0,"overrides":{"model_selection":user}}),
        ),
        (
            "project.settings.set",
            json!({"project_id":"project","expected_revision":0,"overrides":{"model_selection":project}}),
        ),
    ] {
        super::super::settings::command(&manager, "owner", &command(method, None, params))
            .await
            .unwrap();
    }
    let managed = create(&manager).await;
    let id = managed.record.lock().await.id.clone();
    let explicit = selection("b", "model-b", None);
    for (index, (source, inherited)) in [("project", project), ("user", user)]
        .into_iter()
        .enumerate()
    {
        let expected = (index * 2) as u64;
        save(&manager, &id, expected, json!({"model_selection":explicit}))
            .await
            .unwrap();
        submit(&manager, &managed, "explicit B before reset").await;
        let reset = save(&manager, &id, expected + 1, json!({})).await.unwrap();
        assert_eq!(reset["application"], "applied");
        assert_eq!(reset["applied_revision"], expected + 2);
        assert_eq!(reset["sources"]["model_selection"], source);
        assert_eq!(
            reset["desired_effective"]["model_selection"],
            json!(inherited)
        );
        assert_eq!(
            managed.record.lock().await.model_selection,
            Some(inherited.clone())
        );
        submit(&manager, &managed, "inherit explicit default").await;
        assert_eq!(managed.routing.selection(), inherited);
        let request = a.wire.requests.lock().unwrap().last().unwrap().1.clone();
        assert_eq!(request["model"], "model-a");
        assert_eq!(request["reasoning_effort"], inherited.variant.unwrap());
        if index == 0 {
            super::super::settings::command(
                &manager,
                "owner",
                &command(
                    "project.settings.set",
                    None,
                    json!({"project_id":"project","expected_revision":1,"overrides":{}}),
                ),
            )
            .await
            .unwrap();
        }
    }
    manager.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settings_reset_while_busy_defers_without_changing_admitted_route() {
    let root = tempfile::tempdir().unwrap();
    let a = provider(10).await;
    let b = provider(0).await;
    let manager = Manager::with_host(config(root.path(), &a, &b), Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let managed = create(&manager).await;
    let id = managed.record.lock().await.id.clone();
    let explicit = selection("b", "model-b", None);
    let fallback = selection("a", "model-a", None);
    save(&manager, &id, 0, json!({"model_selection":explicit}))
        .await
        .unwrap();
    {
        let _gate = managed.commands.lock().await;
        commands::submit(
            &manager,
            managed.clone(),
            &command("turn.submit", Some(&id), json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text("hold B request")})),
        )
        .await
        .unwrap();
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), b.wire.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let reset = save(&manager, &id, 1, json!({})).await.unwrap();
    assert_eq!(reset["application"], "deferred");
    assert_eq!(reset["revision"], 2);
    assert_eq!(reset["applied_revision"], 1);
    assert_eq!(
        reset["desired_effective"]["model_selection"],
        json!(fallback)
    );
    assert_eq!(reset["effective"]["model_selection"], json!(explicit));
    assert_eq!(
        managed.record.lock().await.model_selection,
        Some(explicit.clone())
    );
    assert_eq!(managed.routing.selection(), explicit);
    assert!(!managed.faulted.load(Ordering::Acquire));
    b.wire.release.add_permits(1);
    if let Some(worker) = managed.worker.lock().await.take() {
        tokio::time::timeout(std::time::Duration::from_secs(10), worker)
            .await
            .unwrap()
            .unwrap();
    }
    let applied = save(&manager, &id, 2, json!({})).await.unwrap();
    assert_eq!(applied["application"], "applied");
    assert_eq!(applied["applied_revision"], 3);
    submit(&manager, &managed, "inherit A after busy completion").await;
    assert_eq!(managed.routing.selection(), fallback);
    assert_eq!(a.wire.requests.lock().unwrap().len(), 1);
    manager.close().await.unwrap();
}

async fn mode_manager(
    root: &std::path::Path,
    a: &ProviderMock,
    b: &ProviderMock,
    default: Option<&str>,
) -> Arc<Manager> {
    let catalog = r#"[{"name":"fixed","modes":[{"name":"child-only","role":"sub_agent"},{"name":"a","role":"main_agent"},{"name":"b","role":"both"}],"variables":{},"tools":[]},{"name":"other","modes":[{"name":"other-mode","role":"main_agent"}],"variables":{},"tools":[]}]"#;
    let path = root.join("mode-fixture.wasm");
    std::fs::write(
        &path,
        typed_agent_fixture::component(&host_shell_fixture::shell_component_source(
            false, catalog, 0,
        )),
    )
    .unwrap();
    let mut configuration = config(root, a, b);
    configuration.agent = Some(crate::config::Agent {
        path,
        name: Some("other".into()),
        mode: default.map(str::to_owned),
        restore_only: false,
    });
    let host = microcrab_host_local::LocalHost::new(root, Default::default()).unwrap();
    Manager::with_host(configuration, Arc::new(host))
        .await
        .unwrap()
}

async fn create_mode(manager: &Arc<Manager>) -> Arc<Managed> {
    let created = commands::create(
        manager,
        "owner",
        &command("session.create", None, json!({"project_id":"project","agent":"fixed","mode":"b","operation_id":uuid::Uuid::new_v4().to_string()})),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mode_restore_retains_selected_history_over_mutable_creator_settings() {
    use microcrab_session::Journal;
    let root = tempfile::tempdir().unwrap();
    let a = provider(10).await;
    let b = provider(10).await;
    let original = mode_manager(root.path(), &a, &b, None).await;
    let managed = create_mode(&original).await;
    let id = managed.record.lock().await.id.clone();
    let selected = managed.session.checkpoint().await.unwrap();
    assert_eq!(selected.state.selected_mode.as_deref(), Some("b"));
    save(&original, &id, 0, json!({"mode":"a"})).await.unwrap();
    assert_eq!(managed.session.state().selected_mode.as_deref(), Some("a"));
    let config = original.config.clone();
    let host = original.host.clone();
    original.close().await.unwrap();
    // Select an exact earlier history owner while retaining the later creator
    // metadata, as history travel may legitimately do. Do not rewrite snapshots.
    let (journal, _) = match config.storage {
        crate::config::Storage::File => {
            let directory = config.data_dir.join("sessions").join(&id);
            microcrab_storage::CreatorJournal::open(
                directory.to_str().unwrap(),
                id.clone(),
                true,
                true,
            )
            .await
            .unwrap()
        }
        crate::config::Storage::Sqlite => {
            #[cfg(feature = "storage-sqlite")]
            {
                microcrab_storage::CreatorJournal::open_sqlite(
                    config.data_dir.join("storage.sqlite"),
                    id.clone(),
                    true,
                )
                .await
                .unwrap()
            }
            #[cfg(not(feature = "storage-sqlite"))]
            panic!("SQLite fixture requires the compiled backend")
        }
    };
    journal.commit(selected.clone()).await.unwrap();
    journal.close().await.unwrap();
    let restored = Manager::with_host(config, host).await.unwrap();
    let record = restored.record("owner", &id).await.unwrap();
    assert_eq!(record.mode.as_deref(), Some("a"));
    let managed = restored.active(record, true).await.unwrap();
    assert_eq!(managed.session.state().selected_mode.as_deref(), Some("b"));
    assert_eq!(
        serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap(),
        serde_json::to_value(selected.context).unwrap(),
        "restoration must retain frozen model-facing history"
    );
    let observed = super::super::settings::command(
        &restored,
        "owner",
        &command("session.settings.get", Some(&id), json!({})),
    )
    .await
    .unwrap();
    assert_eq!(observed["effective"]["mode"], "b");
    assert_eq!(observed["desired_effective"]["mode"], "a");
    restored.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settings_mode_reset_uses_fixed_agent_component_default_and_ancestor_precedence() {
    for default in [None, Some("b")] {
        let root = tempfile::tempdir().unwrap();
        let a = provider(10).await;
        let b = provider(10).await;
        let manager = mode_manager(root.path(), &a, &b, default).await;
        let managed = create_mode(&manager).await;
        let id = managed.record.lock().await.id.clone();
        let inherited = default.unwrap_or("a");
        assert_eq!(managed.default_mode, Ok(Some(inherited.into())));
        save(&manager, &id, 0, json!({"mode":"b"})).await.unwrap();
        let before = managed.session.checkpoint().await.unwrap();
        let reset = save(&manager, &id, 1, json!({})).await.unwrap();
        assert_eq!(reset["application"], "applied");
        assert_eq!(reset["applied_revision"], 2);
        assert_eq!(reset["effective"]["mode"], inherited);
        assert_eq!(reset["desired_effective"]["mode"], inherited);
        assert!(reset["overrides"].get("mode").is_none());
        assert_eq!(managed.record.lock().await.agent.as_deref(), Some("fixed"));
        assert_eq!(managed.record.lock().await.mode.as_deref(), Some(inherited));
        assert_eq!(
            managed.session.state().selected_mode.as_deref(),
            Some(inherited)
        );
        let after = managed.session.checkpoint().await.unwrap();
        assert_eq!(
            serde_json::to_value(before.context).unwrap(),
            serde_json::to_value(after.context).unwrap()
        );
        super::super::settings::command(
            &manager,
            "owner",
            &command(
                "settings.set",
                None,
                json!({"expected_revision":0,"overrides":{"mode":"b"}}),
            ),
        )
        .await
        .unwrap();
        super::super::settings::command(
            &manager,
            "owner",
            &command(
                "project.settings.set",
                None,
                json!({"project_id":"project","expected_revision":0,"overrides":{"mode":"a"}}),
            ),
        )
        .await
        .unwrap();
        let project = save(&manager, &id, 2, json!({})).await.unwrap();
        assert_eq!(project["desired_effective"]["mode"], "a");
        assert_eq!(project["effective"]["mode"], "a");
        assert_eq!(project["sources"]["mode"], "project");
        super::super::settings::command(
            &manager,
            "owner",
            &command(
                "project.settings.set",
                None,
                json!({"project_id":"project","expected_revision":1,"overrides":{}}),
            ),
        )
        .await
        .unwrap();
        let user = save(&manager, &id, 3, json!({})).await.unwrap();
        assert_eq!(user["effective"]["mode"], "b");
        assert_eq!(user["sources"]["mode"], "user");
        manager.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settings_mode_reset_busy_defers_until_fresh_revisioned_apply() {
    let root = tempfile::tempdir().unwrap();
    let a = provider(0).await;
    let b = provider(10).await;
    let manager = mode_manager(root.path(), &a, &b, None).await;
    let managed = create_mode(&manager).await;
    let id = managed.record.lock().await.id.clone();
    save(&manager, &id, 0, json!({"mode":"b"})).await.unwrap();
    {
        let _gate = managed.commands.lock().await;
        commands::submit(&manager, managed.clone(), &command("turn.submit", Some(&id), json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text("hold mode B")}))).await.unwrap();
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), a.wire.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let deferred = save(&manager, &id, 1, json!({})).await.unwrap();
    assert_eq!(deferred["application"], "deferred");
    assert_eq!(deferred["applied_revision"], 1);
    assert_eq!(deferred["effective"]["mode"], "b");
    assert_eq!(deferred["desired_effective"]["mode"], "a");
    assert_eq!(managed.session.state().selected_mode.as_deref(), Some("b"));
    assert!(!managed.faulted.load(Ordering::Acquire));
    a.wire.release.add_permits(1);
    if let Some(worker) = managed.worker.lock().await.take() {
        tokio::time::timeout(std::time::Duration::from_secs(10), worker)
            .await
            .unwrap()
            .unwrap();
    }
    let reset = save(&manager, &id, 2, json!({})).await.unwrap();
    assert_eq!(reset["application"], "applied");
    assert_eq!(reset["applied_revision"], 3);
    assert_eq!(managed.session.state().selected_mode.as_deref(), Some("a"));
    manager.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settings_invalid_pinned_mode_default_fails_before_cas_and_explicit_mode_bypasses_it() {
    let root = tempfile::tempdir().unwrap();
    let a = provider(10).await;
    let b = provider(10).await;
    let manager = mode_manager(root.path(), &a, &b, Some("other-mode")).await;
    let managed = create_mode(&manager).await;
    let id = managed.record.lock().await.id.clone();
    assert!(
        managed
            .default_mode
            .as_ref()
            .unwrap_err()
            .contains("Pinned component default mode")
    );
    let before = managed.record.lock().await.clone();
    let error = save(&manager, &id, 0, json!({})).await.unwrap_err();
    assert_eq!(error.code, "configuration_default_unavailable");
    assert_eq!(*managed.record.lock().await, before);
    assert!(
        manager
            .configuration
            .get("owner", "session_settings", &id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!managed.faulted.load(Ordering::Acquire));
    let explicit = save(&manager, &id, 0, json!({"mode":"a"})).await.unwrap();
    assert_eq!(explicit["application"], "applied");
    assert_eq!(explicit["effective"]["mode"], "a");
    manager.close().await.unwrap();
}
