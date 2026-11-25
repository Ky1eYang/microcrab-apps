//! Fork creator isolation and receipt behavior across both durable journal backends.
use super::*;
use serde_json::json;

async fn fork_contract(storage: &str) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(8),
    });
    let (manager, _root) = manager(model.clone(), storage).await;
    let create = command(
        "session.create",
        None,
        json!({
            "project_id":"project", "title":"Selected source", "operation_id":uuid::Uuid::new_v4().to_string(),
            "client_tools":[{"name":"external_note","description":"Frozen external metadata", "parameters":{"type":"object"}, "reclaimable":true}]
        }),
    );
    let created = commands::create(&manager, "owner", &create).await.unwrap();
    let source_id = created["id"].as_str().unwrap();
    let source = manager
        .active(manager.record("owner", source_id).await.unwrap(), true)
        .await
        .unwrap();
    source
        .session
        .submit("Visible selected transcript")
        .await
        .unwrap();
    manager.refresh(&source).await.unwrap();
    let selected_context =
        serde_json::to_value(source.session.context_snapshot().unwrap()).unwrap();
    let source_cursor = source.journal.cursor().await;
    let source_catalog = source.client_tools.snapshot().await;
    let source_record = source.record.lock().await.clone();
    let fork_command = command(
        "session.fork",
        Some(source_id),
        json!({
            "operation_id":uuid::Uuid::new_v4().to_string(),
            "expected_revision":source_record.revision, "title":"Fork title"
        }),
    );
    let forked = super::super::fork::execute(&manager, "owner", &fork_command)
        .await
        .unwrap();
    let id = forked["id"].as_str().unwrap();
    assert_ne!(id, source_id);
    assert_eq!(forked["title"], "Fork title");
    let destination = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(destination.session.context_snapshot().unwrap()).unwrap(),
        selected_context
    );
    assert_eq!(source.journal.cursor().await, source_cursor);
    assert_eq!(
        serde_json::to_value(source.session.context_snapshot().unwrap()).unwrap(),
        selected_context
    );
    assert!(destination.session.executions().snapshot().is_empty());
    assert!(!destination.session.has_continuation());
    let record = destination.record.lock().await.clone();
    assert_eq!(record.model_selection, source_record.model_selection);
    assert_eq!(record.permissions, source_record.permissions);
    assert_eq!(record.review, source_record.review);
    assert_eq!(record.agent, source_record.agent);
    assert_eq!(record.mode, source_record.mode);
    assert_eq!(record.project_id, source_record.project_id);
    assert_eq!(record.project_root, source_record.project_root);
    assert_eq!(record.owner, source_record.owner);
    assert_eq!(record.revision, 0);
    let bridge = destination.client_tools.snapshot().await;
    assert_eq!(bridge.definitions, source_catalog.definitions);
    assert_eq!(bridge.catalog_digest, source_catalog.catalog_digest);
    assert!(bridge.calls.is_empty());
    assert!(bridge.pending.is_empty());
    assert!(bridge.lease_id.is_none());
    let mut source_pin = manager.pins.component(source_id).await.unwrap().unwrap();
    source_pin.session_id = id.into();
    assert_eq!(manager.pins.component(id).await.unwrap(), Some(source_pin));
    assert!(destination.journal.cursor().await.is_some());
    assert_eq!(
        serde_json::to_value(destination.journal.history_available().await.unwrap()).unwrap(),
        json!({"undo":false,"redo":false})
    );
    assert_eq!(
        destination.hub.snapshot()["history_available"],
        json!({"undo":false,"redo":false})
    );
    // Retry must not capture the now different source or reapply revision preconditions.
    source.session.submit("Later source message").await.unwrap();
    assert_eq!(
        super::super::fork::execute(&manager, "owner", &fork_command)
            .await
            .unwrap(),
        forked
    );
    assert_eq!(
        serde_json::to_value(destination.session.context_snapshot().unwrap()).unwrap(),
        selected_context
    );
    let mut changed = fork_command.clone();
    changed.params["title"] = json!("another payload");
    assert_eq!(
        super::super::fork::execute(&manager, "owner", &changed)
            .await
            .unwrap_err()
            .code,
        "operation_conflict"
    );
    assert_eq!(
        super::super::fork::execute(&manager, "intruder", &fork_command)
            .await
            .unwrap_err()
            .code,
        "not_found"
    );
    let config = manager.config.clone();
    manager.close().await.unwrap();
    drop(destination);
    drop(source);
    drop(manager);
    let reopened = Manager::with_client(config, model).await.unwrap();
    // Receipt replay does not need to open source or destination.
    assert_eq!(
        super::super::fork::execute(&reopened, "owner", &fork_command)
            .await
            .unwrap(),
        forked
    );
    let record = reopened.record("owner", id).await.unwrap();
    let snapshot = reopened.inspect(&record).await.unwrap();
    assert_eq!(
        snapshot["history_available"],
        json!({"undo":false,"redo":false})
    );
    assert!(
        snapshot["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["text"] == "Visible selected transcript")
    );
    let restored = reopened.active(record, true).await.unwrap();
    assert_eq!(
        serde_json::to_value(restored.session.context_snapshot().unwrap()).unwrap(),
        selected_context
    );
    assert_eq!(
        serde_json::to_value(restored.journal.history_available().await.unwrap()).unwrap(),
        json!({"undo":false,"redo":false})
    );
    reopened.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_fork_is_independent_root_and_receipt_survives_source_advance_and_restart() {
    fork_contract("file").await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_fork_is_independent_root_and_receipt_survives_source_advance_and_restart() {
    fork_contract("sqlite").await;
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn fork_revision_and_closed_source_rejections_create_no_destination() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model, "file").await;
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
    let request = command(
        "session.fork",
        Some(id),
        json!({"operation_id":uuid::Uuid::new_v4().to_string(),"expected_revision":999}),
    );
    assert_eq!(
        super::super::fork::execute(&manager, "owner", &request)
            .await
            .unwrap_err()
            .code,
        "revision_conflict"
    );
    assert!(
        manager
            .directory
            .get_operation("owner", request.params["operation_id"].as_str().unwrap())
            .await
            .unwrap()
            .is_none()
    );
    let source = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let submit = command(
        "turn.submit",
        Some(id),
        json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text("pending")}),
    );
    commands::submit(&manager, source.clone(), &submit)
        .await
        .unwrap();
    let pending = command(
        "session.fork",
        Some(id),
        json!({"operation_id":uuid::Uuid::new_v4().to_string()}),
    );
    assert_eq!(
        super::super::fork::execute(&manager, "owner", &pending)
            .await
            .unwrap_err()
            .code,
        "busy"
    );
    assert_eq!(manager.directory.list("owner").await.unwrap().len(), 1);
    manager.close().await.unwrap();
}
