//! Owner defaults, secrets, failed refresh and refs save contracts; not run here.
use super::*;
use microcrab_storage::ModelSelection;
use serde_json::json;
fn model() -> Arc<Model> {
    Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(16),
    })
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn defaults_precedence_reset_and_owner_isolation() {
    let (manager, _root) = manager(model(), "file").await;
    let saved=super::super::settings::command(&manager,"owner",&command("settings.set",None,json!({"expected_revision":0,"overrides":{"limits":{"max_rounds":12},"review":false,"review_model":null}}))).await.unwrap();
    assert_eq!(saved["effective"]["limits"]["max_rounds"], 12);
    let other = super::super::settings::command(
        &manager,
        "other",
        &command("settings.get", None, json!({})),
    )
    .await
    .unwrap();
    assert_ne!(other["effective"]["limits"]["max_rounds"], 12);
    let project=super::super::settings::command(&manager,"owner",&command("project.settings.set",None,json!({"project_id":"project","expected_revision":0,"overrides":{"limits":{"max_rounds":7}}}))).await.unwrap();
    assert_eq!(project["effective"]["limits"]["max_rounds"], 7);
    assert_eq!(project["effective"]["review"], false);
    let reset = super::super::settings::command(
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
    assert_eq!(reset["effective"]["limits"]["max_rounds"], 12);
    assert!(
        super::super::settings::command(
            &manager,
            "other",
            &command(
                "project.references.get",
                None,
                json!({"project_id":"project"})
            )
        )
        .await
        .is_err()
    );
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn provider_secrets_refresh_failure_and_route_pin() {
    let (manager, _root) = manager(model(), "file").await;
    let provider = json!({"name":"Owner gateway","protocol":"openai-compatible","auth":"bearer","endpoint":"http://127.0.0.1:1/v1/chat/completions","timeout_ms":100,"max_response_bytes":1073741824,"models":[{"id":"manual"}]});
    let saved=super::super::providers::command(&manager,"owner","provider.set",json!({"provider_id":"user:gateway","expected_revision":0,"provider":provider,"api_key":"never-return-this"})).await.unwrap();
    assert_eq!(saved["api_key_configured"], true);
    assert!(!saved.to_string().contains("never-return-this"));
    assert!(
        super::super::providers::command(
            &manager,
            "intruder",
            "provider.get",
            json!({"provider_id":"user:gateway"})
        )
        .await
        .is_err()
    );
    let selection = ModelSelection {
        provider_id: "user:gateway".into(),
        model: "manual".into(),
        variant: None,
    };
    let target = super::super::model::target(
        &manager,
        "owner",
        &selection,
        Arc::new(super::super::hub::Hub::new()),
    )
    .await
    .unwrap();
    let routing = super::super::model::Routing::new(target);
    let pinned = routing.witness();
    assert!(
        super::super::providers::command(
            &manager,
            "owner",
            "provider.discover",
            json!({"provider_id":"user:gateway","expected_revision":1})
        )
        .await
        .is_err()
    );
    let after = super::super::providers::command(
        &manager,
        "owner",
        "provider.get",
        json!({"provider_id":"user:gateway"}),
    )
    .await
    .unwrap();
    assert_eq!(after["revision"], 1);
    assert_eq!(after["models"][0]["id"], "manual");
    super::super::providers::command(&manager,"owner","provider.set",json!({"provider_id":"user:gateway","expected_revision":1,"provider":provider,"api_key":"new-secret"})).await.unwrap();
    assert_eq!(routing.witness(), pinned);
    let next = super::super::model::target(
        &manager,
        "owner",
        &selection,
        Arc::new(super::super::hub::Hub::new()),
    )
    .await
    .unwrap();
    let next = super::super::model::Routing::new(next);
    assert_ne!(next.witness(), pinned);
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn project_reference_mounts_bind_owner_library_items_without_creating_runtime() {
    let (manager, _root) = manager(model(), "file").await;
    let owner = Principal::for_test("owner");
    let permit = manager.library_permit().unwrap();
    let created = manager
        .library_create(
            &owner,
            microcrab_session::state::reference::CreateLibraryObject::Shortcut {
                title: "Agents guide".into(),
                target: microcrab_session::state::reference::ShortcutTarget::HostPath {
                    path: "AGENTS.md".into(),
                    project_id: Some("project".into()),
                    host_id: None,
                    workspace_id: None,
                },
            },
            None,
            None,
            permit,
        )
        .await
        .unwrap();
    let ref_id = created["id"].as_str().unwrap().to_owned();
    let saved = super::super::settings::command(
        &manager,
        "owner",
        &command(
            "project.references.set",
            None,
            json!({"project_id":"project","ref_ids":[ref_id]}),
        ),
    )
    .await
    .unwrap();
    assert_eq!(saved["ref_ids"], json!([created["id"]]));
    assert!(manager.registry.lock().unwrap().is_empty());
    let read = super::super::settings::command(
        &manager,
        "owner",
        &command(
            "project.references.get",
            None,
            json!({"project_id":"project"}),
        ),
    )
    .await
    .unwrap();
    assert_eq!(read, saved);
    assert!(
        super::super::settings::command(
            &manager,
            "other",
            &command(
                "project.references.set",
                None,
                json!({"project_id":"project","ref_ids":[created["id"]]})
            )
        )
        .await
        .is_err()
    );
    assert!(
        super::super::settings::command(
            &manager,
            "owner",
            &command(
                "project.references.set",
                None,
                json!({"project_id":"project","ref_ids":["not-a-ref-id"]})
            )
        )
        .await
        .is_err()
    );
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn unsupported_capacity_change_defers_without_quarantining_or_discarding() {
    let (manager, _root) = manager(model(), "file").await;
    let created = commands::create(
        &manager,
        "owner",
        &command(
            "session.create",
            None,
            json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}),
        ),
    )
    .await
    .unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let previous = managed.session.state().limits.max_processes;
    let response = super::super::settings::command(
        &manager,
        "owner",
        &command(
            "session.settings.set",
            Some(id),
            json!({"expected_revision":0,"overrides":{"limits":{"max_processes":1}}}),
        ),
    )
    .await
    .unwrap();
    assert_eq!(response["application"], "deferred");
    assert_eq!(response["effective"]["limits"]["max_processes"], previous);
    assert_eq!(response["desired_effective"]["limits"]["max_processes"], 1);
    assert_eq!(response["applied_revision"], 0);
    assert!(!managed.faulted.load(Ordering::Acquire));
    assert!(response["reason"].as_str().unwrap().contains("rebuild"));
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn partial_live_settings_failure_is_quarantined_and_observed_truthfully() {
    let (manager, _root) = manager(model(), "file").await;
    let created = commands::create(
        &manager,
        "owner",
        &command(
            "session.create",
            None,
            json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}),
        ),
    )
    .await
    .unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let initial = managed.session.auto_compaction_policy().unwrap();
    // A competing durable metadata change leaves the active cache stale. Limits
    // and compaction will commit live before the next Directory CAS rejects it.
    let mut record = manager.directory.get("owner", id).await.unwrap().unwrap();
    let revision = record.revision;
    record.title = "Concurrent metadata edit".into();
    record.revision += 1;
    manager
        .directory
        .replace("owner", revision, record)
        .await
        .unwrap();
    let update = json!({"expected_revision":0,"overrides":{"limits":{"max_rounds":12},"auto_compaction":{"enabled":true,"turn_threshold":0.6,"round_threshold":0.9,"headroom_tokens":7}}});
    let error = super::super::settings::command(
        &manager,
        "owner",
        &command("session.settings.set", Some(id), update),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "settings_reconciliation_required");
    assert!(error.message.contains("quarantined"));
    assert!(error.message.contains("instead of replaying"));
    assert!(managed.faulted.load(Ordering::Acquire));
    assert_eq!(managed.session.state().limits.max_rounds, 12);
    let current = managed.session.auto_compaction_policy().unwrap();
    assert_ne!(current, initial);
    assert_eq!(current.turn_threshold, 0.6);
    let details = error.details.unwrap();
    assert!(details.get("effective").is_none());
    assert!(details.get("desired_effective").is_none());
    assert!(serde_json::to_vec(&details).unwrap().len() < 4096);
    assert_eq!(details["desired_revision"], 1);
    assert!(
        manager
            .configuration
            .get("owner", "session_settings_applied", id)
            .await
            .unwrap()
            .is_none()
    );
    let observed = super::super::settings::command(
        &manager,
        "owner",
        &command("session.settings.get", Some(id), json!({})),
    )
    .await
    .unwrap();
    assert_eq!(
        observed["effective"]["auto_compaction"]["turn_threshold"],
        0.6
    );
    assert_eq!(observed["effective"]["limits"]["max_rounds"], 12);
    assert!(observed["reason"].as_str().unwrap().contains("quarantined"));
    assert_eq!(observed["applied_revision"], 0);
    let replay = super::super::settings::command(
        &manager,
        "owner",
        &command(
            "session.settings.set",
            Some(id),
            json!({"expected_revision":1,"overrides":{}}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(replay.code, "settings_reconciliation_required");
    assert_eq!(
        manager
            .configuration
            .get("owner", "session_settings", id)
            .await
            .unwrap()
            .unwrap()
            .revision,
        1
    );
    let variables = super::super::settings::command(
        &manager,
        "owner",
        &command(
            "session.variables.patch",
            Some(id),
            json!({"set":{"should_not_commit":true},"remove":[]}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(variables.code, "settings_reconciliation_required");
    assert!(
        managed
            .session
            .state_handle()
            .extract("should_not_commit")
            .is_none()
    );
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn shared_provider_shadow_mutations_are_read_only_before_schema_or_revision() {
    let (manager, _root) = manager(model(), "file").await;
    for method in ["provider.set", "provider.delete", "provider.discover"] {
        let error = super::super::providers::command(
            &manager,
            "owner",
            method,
            json!({"provider_id":"configured","expected_revision":0}),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "read_only");
        assert!(error.message.contains("cannot be shadowed"));
    }
    assert!(
        manager
            .configuration
            .get("owner", "provider", "configured")
            .await
            .unwrap()
            .is_none()
    );
}
#[test]
fn every_limit_is_bounded_and_inherited_review_reset_is_explicit() {
    use crate::config::settings::*;
    let ceilings = hard_limits();
    let map = serde_json::to_value(&ceilings).unwrap();
    for (key, value) in map.as_object().unwrap() {
        let mut patch = std::collections::BTreeMap::new();
        patch.insert(key.clone(), value.as_u64().unwrap() + 1);
        assert!(limits(&patch, &ceilings).is_err(), "{key}");
    }
    let inherited: SettingsOverrides =
        serde_json::from_value(json!({"review_model":{"provider_id":"configured","model":"test"}}))
            .unwrap();
    let reset: SettingsOverrides = serde_json::from_value(json!({"review_model":null})).unwrap();
    let mut merged = inherited;
    merged.overlay(&reset);
    assert_eq!(merged.review_model, Some(None));
    assert_eq!(
        serde_json::to_value(SettingsOverrides::default()).unwrap(),
        json!({"limits":{}})
    );
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn owner_state_reads_all_original_values_with_metadata_separate_from_model_projection() {
    let (manager, _) = manager(model(), "file").await;
    let owner = Principal::for_test("owner");
    let created = manager
        .command(
            &owner,
            &command(
                "session.create",
                None,
                json!({
                    "project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()
                }),
            ),
        )
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let change = serde_json::from_value(json!({"set":{"public_value":7},"remove":[],"scoped_set":[
        {"name":"hidden_settings","value":{"nested":true},"visibility":"invisible","persistence":"persisted"},
        {"name":"hidden_live","value":"original-fixture-value","visibility":"invisible","persistence":"ephemeral"}
    ]})).unwrap();
    managed.session.state_handle().change(change).await.unwrap();
    let values = manager
        .command(&owner, &command("state.get", Some(id), json!({})))
        .await
        .unwrap();
    assert_eq!(values["public_value"], 7);
    assert_eq!(values["hidden_settings"], json!({"nested":true}));
    assert_eq!(values["hidden_live"], "original-fixture-value");
    assert!(values.get("private_variables").is_none());
    let metadata = manager
        .command(&owner, &command("state.metadata", Some(id), json!({})))
        .await
        .unwrap();
    assert_eq!(metadata["hidden_live"]["visibility"], "invisible");
    assert_eq!(metadata["hidden_live"]["persistence"], "ephemeral");
    assert_eq!(metadata["hidden_live"]["owner"], "client");
    assert_eq!(metadata["hidden_live"]["editors"], json!(["client"]));
    for key in ["agent_init", "agent_initialization_pending"] {
        assert!(values.get(key).is_some());
        assert_eq!(metadata[key]["visibility"], "invisible");
        assert_eq!(metadata[key]["persistence"], "persisted");
        assert!(managed.session.state_view().unwrap().get(key).is_none());
    }
    assert!(!metadata.to_string().contains("original-fixture-value"));
    let automatic = manager.refresh(&managed).await.unwrap();
    assert!(automatic["state"].get("hidden_settings").is_none());
    assert!(automatic["state"].get("hidden_live").is_none());
    for method in ["state.get", "state.metadata"] {
        let error = manager
            .command(
                &Principal::for_test("other"),
                &command(method, Some(id), json!({})),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "not_found");
    }
    manager.close().await.unwrap();
}
