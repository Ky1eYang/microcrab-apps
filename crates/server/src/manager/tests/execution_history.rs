use super::*;
use microcrab_session::{
    Journal,
    runtime::execution::{ExecutionId, ExecutionSnapshot, ExecutionStatus},
};
use serde_json::json;

async fn list(manager: &Manager, owner: &str, id: &str, params: Value) -> Result<Value, WireError> {
    manager
        .command(
            &Principal::for_test(owner),
            &command(commands::EXECUTIONS_LIST, Some(id), params),
        )
        .await
}

async fn create_session(manager: &Manager) -> String {
    commands::create(
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
    .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn execution(id: &str, execution: u64) -> ExecutionSnapshot {
    let mut row = ExecutionSnapshot::new(
        id.into(),
        format!("call-{execution}"),
        "tool".into(),
        "agent".into(),
        None,
    );
    row.execution_id = ExecutionId(execution);
    row.status = ExecutionStatus::Succeeded;
    row.arguments = Some("private arguments".into());
    row.result = Some("private result".into());
    row
}

async fn observed_rows(storage: &str) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(1),
    });
    let (manager, _root) = manager(model.clone(), storage).await;
    let id = create_session(&manager).await;
    let record = manager.record("owner", &id).await.unwrap();
    let managed = manager.active(record.clone(), true).await.unwrap();
    let registry = managed.session.executions();
    let mut rows = vec![execution(&id, 1), execution(&id, 2)];
    registry.restore(&rows).unwrap();
    let before = managed.journal.display_witness().await;
    let first = list(&manager, "owner", &id, json!({"limit":1}))
        .await
        .unwrap();
    let revision = first["witness"]["rows_revision"].clone();
    assert_eq!(first["items"][0]["execution_id"], 1);
    assert_eq!(first["items"][0]["arguments_available"], true);
    assert!(first["items"][0].get("arguments").is_none());
    assert!(first["items"][0].get("result").is_none());
    assert_eq!(first["witness"]["history_lineage"], before.lineage);
    assert_eq!(
        first["witness"]["history_epoch"],
        managed.session.state().epoch_id
    );
    assert_eq!(first["witness"]["dormant"], false);
    let next = list(
        &manager,
        "owner",
        &id,
        json!({"cursor":1,"expected_revision":revision}),
    )
    .await
    .unwrap();
    assert_eq!(next["items"][0]["execution_id"], 2);
    assert_eq!(next["witness"], first["witness"]);
    assert_eq!(
        list(
            &manager,
            "intruder",
            &id,
            json!({"expected_revision":revision})
        )
        .await
        .unwrap_err()
        .code,
        "not_found"
    );

    rows[1].elapsed_ms = Some(44);
    registry.restore(&rows).unwrap();
    let directory_before = manager.directory.get("owner", &id).await.unwrap();
    assert_eq!(
        list(
            &manager,
            "owner",
            &id,
            json!({"cursor":1,"expected_revision":revision})
        )
        .await
        .unwrap_err()
        .code,
        "execution_history_changed"
    );
    let legacy = list(&manager, "owner", &id, json!({"cursor":1}))
        .await
        .unwrap();
    assert_eq!(legacy["items"][0]["elapsed_ms"], 44);
    assert_ne!(legacy["witness"]["rows_revision"], revision);
    // The registry's asynchronous checkpoint owner may flush the synthetic
    // update independently; paging must preserve that selected lineage.
    assert_eq!(
        managed.journal.display_witness().await.lineage,
        before.lineage
    );
    assert_eq!(
        manager.directory.get("owner", &id).await.unwrap(),
        directory_before
    );

    let mut detached = managed.session.checkpoint().await.unwrap();
    detached.state.executions[1].status = ExecutionStatus::Running;
    commands::close_session(&manager, &managed, "owner", &id, None)
        .await
        .unwrap();
    drop(managed);
    {
        let _gate = manager.journal_open_gate(&id).lock_owned().await;
        let (journal, _) = manager.inspect_journal(&record).await.unwrap();
        journal.commit(detached).await.unwrap();
        journal.close().await.unwrap();
    }
    let detached_dir = manager.inspect_detached_head(&id).await.unwrap().unwrap();
    let detached_view = list(&manager, "owner", &id, json!({"cursor":1}))
        .await
        .unwrap();
    assert_eq!(
        detached_view["witness"]["history_lineage"],
        detached::detached_lineage(&manager.instance, Some(&detached_dir.cursor))
    );
    assert_eq!(
        detached_view["witness"]["history_epoch"],
        detached_dir.cursor.epoch
    );
    assert_eq!(detached_view["witness"]["dormant"], true);
    assert_eq!(
        detached_view["items"][0]["status"],
        json!(ExecutionStatus::Interrupted)
    );
    assert_eq!(
        list(
            &manager,
            "owner",
            &id,
            json!({"cursor":1,"expected_revision":legacy["witness"]["rows_revision"]})
        )
        .await
        .unwrap_err()
        .code,
        "execution_history_changed"
    );
    assert_eq!(
        manager
            .inspect_detached_head(&id)
            .await
            .unwrap()
            .unwrap()
            .cursor,
        detached_dir.cursor
    );
    assert!(manager.registry.lock().unwrap().is_empty());
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_live_and_dormant_rows_have_exact_revision() {
    observed_rows("file").await;
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_live_and_dormant_rows_have_exact_revision() {
    observed_rows("sqlite").await;
}

#[tokio::test]
async fn selection_change_rejects_an_unchanged_empty_history() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(1),
    });
    let storage = if cfg!(feature = "storage-sqlite") {
        "sqlite"
    } else {
        "file"
    };
    let (manager, _root) = manager(model, storage).await;
    let id = create_session(&manager).await;
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let principal = Principal::for_test("owner");
    manager
        .command(
            &principal,
            &command(
                "turn.submit",
                Some(&id),
                json!({
                    "operation_id":uuid::Uuid::new_v4().to_string(),
                    "input":microcrab_session::SessionInput::text("same epoch, new selection")
                }),
            ),
        )
        .await
        .unwrap();
    if let Some(worker) = managed.worker.lock().await.take() {
        tokio::time::timeout(std::time::Duration::from_secs(10), worker)
            .await
            .unwrap()
            .unwrap();
    }
    let first = list(&manager, "owner", &id, json!({})).await.unwrap();
    let selection = manager
        .command(
            &principal,
            &command("history.travel", Some(&id), json!({"direction":"undo"})),
        )
        .await
        .unwrap();
    let current = list(&manager, "owner", &id, json!({})).await.unwrap();
    assert_eq!(first["items"], json!([]));
    assert_eq!(first["items"], current["items"]);
    assert_eq!(
        first["witness"]["history_epoch"],
        current["witness"]["history_epoch"]
    );
    assert_ne!(
        first["witness"]["history_lineage"],
        current["witness"]["history_lineage"]
    );
    assert_eq!(
        current["witness"]["history_lineage"],
        selection["history_view"]["lineage"]
    );
    assert_eq!(
        list(
            &manager,
            "owner",
            &id,
            json!({"expected_revision":first["witness"]["rows_revision"]})
        )
        .await
        .unwrap_err()
        .code,
        "execution_history_changed"
    );
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn dormant_no_head_does_not_create_history_or_migrate_legacy_record() {
    use microcrab_storage::RECORD_VERSION;
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, root) = manager(model, "file").await;
    let id = "legacy-executions";
    let record = manager
        .directory
        .create(SessionRecord {
            version: RECORD_VERSION,
            id: id.into(),
            owner: "owner".into(),
            title: "History only".into(),
            project_id: Some("project".into()),
            project_root: root.to_str().unwrap().into(),
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
    let first = list(&manager, "owner", id, json!({})).await.unwrap();
    assert_eq!(first["items"], json!([]));
    assert_eq!(first["witness"]["dormant"], true);
    assert_eq!(first["witness"]["history_epoch"], 0);
    assert_eq!(
        list(&manager, "owner", id, json!({"expected_revision":"stale"}))
            .await
            .unwrap_err()
            .code,
        "execution_history_changed"
    );
    assert!(!directory.exists());
    assert!(manager.registry.lock().unwrap().is_empty());
    assert_eq!(
        manager.directory.get("owner", id).await.unwrap(),
        Some(record)
    );
    manager.close().await.unwrap();
}
