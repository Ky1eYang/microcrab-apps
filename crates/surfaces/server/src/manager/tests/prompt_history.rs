use super::*;
use serde_json::json;

async fn prompt(
    manager: &Manager,
    id: &str,
    lineage: String,
    epoch: u64,
) -> Result<Value, WireError> {
    let selected =
        serde_json::from_value(json!({"history_lineage":lineage, "epoch":epoch})).unwrap();
    let bytes = manager
        .frozen_prompt(&Principal::for_test("owner"), id.into(), selected)
        .await?;
    Ok(serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn frozen_prompt_for_missing_dormant_head_never_creates_history_or_migrates_a_record() {
    use microcrab_storage::{RECORD_VERSION, SessionRecord};

    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _root) = manager(model.clone(), "file").await;
    let id = "no-history";
    let record = manager
        .directory
        .create(SessionRecord {
            version: RECORD_VERSION,
            id: id.into(),
            owner: "owner".into(),
            title: "No history".into(),
            project_id: Some("project".into()),
            project_root: _root.to_str().unwrap().into(),
            agent: None,
            mode: None,
            model_selection: None,
            auto_compaction: None,
            prompt_config: None,
            permissions: vec![],
            initialization: microcrab_permission::Action::Allow,
            review: false,
            review_model: None,
            revision: 0,
            archived: false,
            created_at_ms: 1,
            updated_at_ms: 1,
        })
        .await
        .unwrap();
    let directory = manager.config.data_dir.join("sessions").join(id);
    assert!(!directory.exists());
    let response = prompt(
        &manager,
        id,
        detached::detached_lineage(&manager.instance, None),
        0,
    )
    .await
    .unwrap();
    assert_eq!(response["status"], "unavailable");
    assert_eq!(response["reason"], "missing_evidence");
    assert!(!directory.exists());
    assert!(manager.registry.lock().unwrap().is_empty());
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        manager.directory.get("owner", id).await.unwrap(),
        Some(record)
    );
    manager.close().await.unwrap();
}

#[tokio::test]
async fn frozen_prompt_rejects_old_lineages_after_travel_even_when_epoch_is_unchanged() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(1),
    });
    let (manager, _root) = manager(model.clone(), "file").await;
    let created = commands::create(
        &manager,
        "owner",
        &command(
            "session.create",
            None,
            json!({
                "project_id":"project", "operation_id":uuid::Uuid::new_v4().to_string(),
                "system_template":"Frozen through travel"
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
    let principal = Principal::for_test("owner");
    commands::execute(&manager, &principal, &command("turn.submit", Some(id), json!({
        "operation_id":uuid::Uuid::new_v4().to_string(),
        "input":microcrab_session::SessionInput::text("frozen prompt must not include the transcript")
    }))).await.unwrap();
    if let Some(driver) = managed.worker.lock().await.take() {
        tokio::time::timeout(std::time::Duration::from_secs(10), driver)
            .await
            .unwrap()
            .unwrap();
    }
    let original = managed.journal.display_witness().await;
    let cursor = original.cursor.as_ref().unwrap();
    let initial = prompt(&manager, id, original.lineage.clone(), cursor.epoch)
        .await
        .unwrap();
    assert_eq!(initial["system"], "Frozen through travel");
    assert!(
        !initial
            .to_string()
            .contains("must not include the transcript")
    );
    let mut prior_lineage = original.lineage;
    for direction in ["undo", "redo"] {
        let result = commands::execute(
            &manager,
            &principal,
            &command("history.travel", Some(id), json!({"direction":direction})),
        )
        .await
        .unwrap();
        assert_eq!(result["history_view"]["epoch"], cursor.epoch);
        let new_lineage = result["history_view"]["lineage"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_ne!(prior_lineage, new_lineage);
        assert_eq!(
            prompt(&manager, id, prior_lineage, cursor.epoch)
                .await
                .unwrap_err()
                .code,
            "stale_history"
        );
        let selected = prompt(&manager, id, new_lineage.clone(), cursor.epoch)
            .await
            .unwrap();
        assert_eq!(selected["system"], initial["system"]);
        assert_eq!(selected["tools"], initial["tools"]);
        prior_lineage = new_lineage;
    }
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    manager.close().await.unwrap();
}
