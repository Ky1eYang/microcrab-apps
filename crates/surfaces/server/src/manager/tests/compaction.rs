//! Compaction admission, durable ownership and historical summary provenance.

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn stale_mode_revision_has_no_runtime_or_directory_effect() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let before = managed.session.checkpoint().await.unwrap();
    let permissions = managed.session.permission_controller().snapshot().unwrap();
    let record = managed.record.lock().await.clone();
    let error = manager
        .command(
            &Principal::for_test("owner"),
            &command(
                "session.set_mode",
                Some(&id),
                serde_json::json!({"mode":"plan","expected_revision":record.revision + 1}),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "revision_conflict");
    assert_eq!(
        managed.session.state().selected_mode,
        before.state.selected_mode
    );
    assert_eq!(
        managed.session.permission_controller().snapshot().unwrap(),
        permissions
    );
    assert_eq!(
        serde_json::to_value(managed.session.checkpoint().await.unwrap().context).unwrap(),
        serde_json::to_value(before.context).unwrap()
    );
    assert_eq!(
        manager.record("owner", &id).await.unwrap().revision,
        record.revision
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    manager.close().await.unwrap();
}
use super::*;

async fn admit(
    manager: &Manager,
    managed: &Arc<Managed>,
    request: &Command,
) -> Result<Value, microcrab_protocol::application::WireError> {
    let _gate = managed.commands.lock().await;
    super::super::compaction::start(manager, managed.clone(), request, commands::parse(request)?)
        .await
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn compact_is_owned_idempotent_and_restores_its_explicit_marker() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let operation_id = uuid::Uuid::new_v4().to_string();
    let request = command(
        "session.compact",
        Some(&id),
        serde_json::json!({"operation_id":operation_id}),
    );
    let snapshot = admit(&manager, &managed, &request).await.unwrap();
    assert_eq!(snapshot["session"]["status"], "running");
    model.entered.notified().await;
    admit(&manager, &managed, &request).await.unwrap();
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    let conflict = command(
        "session.compact",
        Some(&id),
        serde_json::json!({"operation_id":operation_id,"instruction":"different"}),
    );
    assert_eq!(
        admit(&manager, &managed, &conflict).await.unwrap_err().code,
        "operation_conflict"
    );
    model.release.add_permits(1);
    let worker = managed.worker.lock().await.take().unwrap();
    worker.await.unwrap();
    let receipt = manager
        .directory
        .get_operation("owner", &operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.status, "completed");
    let marker = managed.session.compaction().unwrap();
    assert_eq!(marker.summary, "canonical answer");
    assert!(
        managed
            .session
            .checkpoint()
            .await
            .unwrap()
            .context
            .pending_epoch
    );
    assert_eq!(managed.hub.snapshot()["compactions"][0]["id"], marker.id);
    assert_eq!(
        model.calls.load(Ordering::SeqCst),
        1,
        "compaction never auto-resumes a turn"
    );
    let config = manager.config.clone();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let reopened = Manager::with_client(config, model.clone()).await.unwrap();
    let record = reopened.record("owner", &id).await.unwrap();
    let restored = reopened.inspect(&record).await.unwrap();
    assert_eq!(restored["compactions"][0]["id"], marker.id);
    assert_eq!(restored["compactions"][0]["summary"], marker.summary);
    assert_eq!(
        model.calls.load(Ordering::SeqCst),
        1,
        "detached history does not call a model"
    );
    reopened.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn interrupt_compact_settles_cancelled_without_installing_a_summary() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let operation_id = uuid::Uuid::new_v4().to_string();
    let request = command(
        "session.compact",
        Some(&id),
        serde_json::json!({"operation_id":operation_id,"instruction":"keep exact paths"}),
    );
    admit(&manager, &managed, &request).await.unwrap();
    // The creator token covers cancellation even before the worker first polls;
    // runtime interrupt additionally covers already admitted Session work.
    managed.hub.interrupt_operation();
    managed.session.interrupt();
    let worker = managed.worker.lock().await.take().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap();
    let receipt = manager
        .directory
        .get_operation("owner", &operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.status, "cancelled");
    assert!(managed.session.compaction().is_none());
    assert!(managed.hub.operation().is_none());
    assert!(
        managed.hub.snapshot()["compactions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    manager.close().await.unwrap();
}
