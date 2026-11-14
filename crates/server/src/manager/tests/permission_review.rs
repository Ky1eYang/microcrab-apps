//! Dedicated review selection is durable configuration, not a provider replay.
use super::*;
use serde_json::json;

async fn contract(storage: &str) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _root) = manager(model.clone(), storage).await;
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
    let id = created["id"].as_str().unwrap().to_owned();
    let principal = crate::auth::Principal::for_test("owner");
    let get = || command("permission.get", Some(&id), json!({}));
    let initial = commands::execute(&manager, &principal, &get())
        .await
        .unwrap();
    assert_eq!(initial["config"]["review"], true);
    assert_eq!(initial["config"]["initialization"], "allow");
    assert_eq!(
        manager.record("owner", &id).await.unwrap().initialization,
        microcrab_permission::Action::Allow
    );
    assert!(initial["config"].get("review_model").is_none());
    assert!(manager.record("owner", &id).await.unwrap().review);
    assert!(initial["tool_inventory"]["agent"].is_string());
    assert!(initial["tool_inventory"]["modes"].is_array());
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), false)
        .await
        .unwrap();
    let context = serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap();
    let history = managed.journal.cursor().await;
    let target = model::default_selection(&manager.config);
    let selection = serde_json::to_value(microcrab_permission::ReviewModelSelection {
        provider_id: target.provider_id,
        model: target.model,
        variant: target.variant,
    })
    .unwrap();
    let overrides = json!([{"agent":"future-agent","mode":"plan","tool":{"kind":"glob","value":"read*"},"args":[{"pointer":"/path","matcher":{"kind":"glob","value":"docs/**"}}],"action":"ask"}]);
    let set = |revision: Value, review_model: Value| {
        command(
            "permission.set",
            Some(&id),
            json!({"expected_revision":revision,"overrides":overrides,"initialization":"deny","review":true,"review_model":review_model}),
        )
    };
    let saved = commands::execute(
        &manager,
        &principal,
        &set(initial["revision"].clone(), selection.clone()),
    )
    .await
    .unwrap();
    assert_eq!(saved["config"]["review_model"], selection);
    assert_eq!(saved["config"]["initialization"], "deny");
    assert_eq!(
        manager.record("owner", &id).await.unwrap().initialization,
        microcrab_permission::Action::Deny
    );
    let stale = commands::execute(
        &manager,
        &principal,
        &set(initial["revision"].clone(), Value::Null),
    )
    .await
    .unwrap_err();
    assert_eq!(stale.code, "revision_conflict");
    let record = manager.record("owner", &id).await.unwrap();
    let bad = commands::execute(
        &manager,
        &principal,
        &set(
            saved["revision"].clone(),
            json!({"provider_id":"not-configured","model":"unavailable","variant":null}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(bad.code, "model_unavailable");
    assert_eq!(manager.record("owner", &id).await.unwrap(), record);
    let after_bad = commands::execute(&manager, &principal, &get())
        .await
        .unwrap();
    assert_eq!(after_bad, saved);
    // Old clients omit the additive selection field: preserving it is mandatory.
    let omitted = commands::execute(
        &manager,
        &principal,
        &command(
            "permission.set",
            Some(&id),
            json!({"expected_revision":saved["revision"],"overrides":overrides,"review":true}),
        ),
    )
    .await
    .unwrap();
    assert_eq!(omitted["config"]["review_model"], selection);
    assert_eq!(omitted["config"]["initialization"], "deny");
    assert_eq!(
        serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap(),
        context
    );
    assert_eq!(managed.journal.cursor().await, history);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    let config = manager.config.clone();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let restored = Manager::with_client(config, model.clone()).await.unwrap();
    let current = commands::execute(&restored, &principal, &get())
        .await
        .unwrap();
    assert_eq!(current["config"]["review_model"], selection);
    assert_eq!(current["config"]["initialization"], "deny");
    assert_eq!(
        restored.record("owner", &id).await.unwrap().initialization,
        microcrab_permission::Action::Deny
    );
    assert_eq!(
        current["config"]["overrides"],
        omitted["config"]["overrides"]
    );
    let reset = commands::execute(
        &restored,
        &principal,
        &set(current["revision"].clone(), Value::Null),
    )
    .await
    .unwrap();
    assert!(reset["config"].get("review_model").is_none());
    assert!(
        restored
            .record("owner", &id)
            .await
            .unwrap()
            .review_model
            .is_none()
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    let disabled = commands::execute(
        &restored,
        &principal,
        &command(
            "permission.set",
            Some(&id),
            json!({"expected_revision":reset["revision"],"overrides":overrides,"review":false}),
        ),
    )
    .await
    .unwrap();
    assert_eq!(disabled["config"]["review"], false);
    let config = restored.config.clone();
    restored.close().await.unwrap();
    drop(restored);
    let reopened = Manager::with_client(config, model.clone()).await.unwrap();
    let retained = commands::execute(&reopened, &principal, &get())
        .await
        .unwrap();
    assert_eq!(retained["config"]["review"], false);
    assert!(!reopened.record("owner", &id).await.unwrap().review);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    reopened.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_review_selection_is_validated_revision_guarded_and_restored() {
    contract("file").await;
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_review_selection_is_validated_revision_guarded_and_restored() {
    contract("sqlite").await;
}
