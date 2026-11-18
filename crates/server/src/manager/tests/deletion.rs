//! Cascade, owner isolation, hard deletion and activation/archive lifecycle regressions.
use super::*;

async fn cascade(storage: &str) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), storage).await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let component = manager.pins.component(&id).await.unwrap();
    let mut inactive = managed.record.lock().await.clone();
    inactive.id = "inactive-child".into();
    manager.directory.create(inactive.clone()).await.unwrap();
    let mut unrelated = inactive.clone();
    unrelated.id = "other-project-session".into();
    unrelated.project_id = Some("unrelated-project".into());
    manager.directory.create(unrelated.clone()).await.unwrap();
    let mut foreign = unrelated.clone();
    foreign.id = "foreign-owner-session".into();
    foreign.owner = "other-owner".into();
    manager.directory.create(foreign.clone()).await.unwrap();
    let operation = uuid::Uuid::new_v4().to_string();
    commands::submit(&manager, managed.clone(), &command("turn.submit", Some(&id),
        serde_json::json!({"operation_id":operation,"input":microcrab_session::SessionInput::text("blocked provider")})))
        .await.unwrap();
    model.entered.notified().await;
    assert!(
        super::super::deletion::delete_project(&manager, "other-owner", "project")
            .await
            .is_err()
    );
    assert!(
        !manager
            .directory
            .get("owner", &id)
            .await
            .unwrap()
            .unwrap()
            .archived
    );
    let deleted = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        super::super::deletion::delete_project(&manager, "owner", "project"),
    )
    .await
    .expect("cascade must cancel an in-flight provider without waiting for its response")
    .unwrap();
    let ids = deleted["deleted_session_ids"].as_array().unwrap();
    assert!(ids.contains(&serde_json::json!(id)));
    assert!(ids.contains(&serde_json::json!(inactive.id)));
    assert_eq!(ids.len(), 2);
    // Hard deletion removes the records, their receipts and their byte trees.
    for session in [&id, &inactive.id] {
        assert_eq!(manager.directory.get("owner", session).await.unwrap(), None);
    }
    assert_eq!(
        manager
            .directory
            .get_operation("owner", &operation)
            .await
            .unwrap(),
        None
    );
    // Byte removal is acknowledged first and completed in the background.
    assert!(super::super::deletion::cleanup_settled(&manager, &id).await);
    assert!(super::super::deletion::cleanup_settled(&manager, &inactive.id).await);
    let sessions_root = manager.config.data_dir.join("sessions");
    assert!(!sessions_root.join(&id).exists());
    assert_eq!(
        manager.directory.get("owner", &unrelated.id).await.unwrap(),
        Some(unrelated)
    );
    assert_eq!(
        manager
            .directory
            .get("other-owner", &foreign.id)
            .await
            .unwrap(),
        Some(foreign)
    );
    // Immutable pins are project metadata, not session bytes.
    assert_eq!(manager.pins.component(&id).await.unwrap(), component);
    assert!(!manager.project_allowed("owner", "project"));
    assert!(!manager.registry.lock().unwrap().contains_key(&id));
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    let config = manager.config.clone();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let reopened = Manager::with_client(config, model).await.unwrap();
    assert_eq!(reopened.directory.get("owner", &id).await.unwrap(), None);
    assert_eq!(
        reopened.directory.get("owner", &inactive.id).await.unwrap(),
        None
    );
    assert!(!reopened.project_allowed("owner", "project"));
    reopened.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_project_cascade_deletes_owned_sessions_and_receipts() {
    cascade("file").await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_project_cascade_deletes_owned_sessions_and_receipts() {
    cascade("sqlite").await;
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn delete_session_is_revision_fenced_and_removes_bytes_records_and_receipts() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let record = manager.record("owner", &id).await.unwrap();
    let managed = manager.active(record.clone(), true).await.unwrap();
    let operation = uuid::Uuid::new_v4().to_string();
    commands::submit(&manager, managed.clone(), &command("turn.submit", Some(&id),
        serde_json::json!({"operation_id":operation,"input":microcrab_session::SessionInput::text("blocked provider")})))
        .await.unwrap();
    model.entered.notified().await;
    let directory = manager.config.data_dir.join("sessions").join(&id);
    assert!(directory.exists());
    // A stale revision and a foreign owner destroy nothing.
    assert_eq!(
        super::super::deletion::delete_session(&manager, "owner", &id, Some(record.revision + 7))
            .await
            .unwrap_err()
            .code,
        "revision_conflict"
    );
    assert!(
        super::super::deletion::delete_session(&manager, "other-owner", &id, Some(record.revision))
            .await
            .is_err()
    );
    assert!(directory.exists());
    let revision = manager
        .directory
        .get("owner", &id)
        .await
        .unwrap()
        .unwrap()
        .revision;
    let deleted = super::super::deletion::delete_session(&manager, "owner", &id, Some(revision))
        .await
        .unwrap();
    assert_eq!(deleted["deleted"], true);
    assert_eq!(deleted["session_id"], id);
    assert!(deleted["removed_operation_receipts"].as_u64().unwrap() >= 1);
    assert_eq!(manager.directory.get("owner", &id).await.unwrap(), None);
    assert_eq!(
        manager
            .directory
            .get_operation("owner", &operation)
            .await
            .unwrap(),
        None
    );
    assert!(super::super::deletion::cleanup_settled(&manager, &id).await);
    assert!(!directory.exists(), "hard deletion removes the byte tree");
    assert!(!manager.registry.lock().unwrap().contains_key(&id));
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn archive_rejects_stale_restore_and_publishes_only_lifecycle_metadata() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model, "file").await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap();
    let id = created["id"].as_str().unwrap();
    let stale = manager.record("owner", id).await.unwrap();
    let managed = manager.active(stale.clone(), true).await.unwrap();
    let mut snapshot = managed.hub.snapshot();
    snapshot["retained_projection_witness"] = serde_json::json!("no full rebuild after close");
    managed.hub.replace(snapshot);
    let archived =
        super::super::deletion::archive_session(&manager, "owner", id, Some(stale.revision))
            .await
            .unwrap();
    assert_eq!(archived["archived"], true);
    assert_eq!(managed.hub.snapshot()["session"], archived);
    assert_eq!(
        managed.hub.snapshot()["retained_projection_witness"],
        "no full rebuild after close"
    );
    assert_eq!(
        manager.active(stale, true).await.err().unwrap().code,
        "archived"
    );
    assert!(!manager.registry.lock().unwrap().contains_key(id));
    manager.close().await.unwrap();
}

async fn inspection_waiting_for_archive_uses_settled_metadata(storage: &str) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model, storage).await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap();
    let id = created["id"].as_str().unwrap();
    let before = manager.record("owner", id).await.unwrap();
    let managed = manager.active(before.clone(), true).await.unwrap();
    let fence = manager.journal_open_gate(id).lock_owned().await;
    let inspection = manager.inspect(&before);
    tokio::pin!(inspection);
    // Poll once while the archive owner has the fence. The read has already
    // selected its pre-archive record and must remain pending through cleanup.
    tokio::select! {
        biased;
        result = &mut inspection => panic!("inspection bypassed archive fence: {result:?}"),
        _ = std::future::ready(()) => {}
    }
    let archived =
        commands::archive_managed(&manager, &managed, "owner", id, Some(before.revision))
            .await
            .unwrap();
    drop(fence);
    let snapshot = inspection.await.unwrap();
    assert_eq!(snapshot["session"]["archived"], true);
    assert_eq!(snapshot["session"]["revision"], archived["revision"]);
    assert_eq!(snapshot["session"]["status"], "closed");
    assert!(!manager.registry.lock().unwrap().contains_key(id));
    assert_eq!(
        manager
            .directory
            .get("owner", id)
            .await
            .unwrap()
            .unwrap()
            .revision,
        archived["revision"].as_u64().unwrap()
    );
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_inspection_waiting_for_archive_uses_settled_metadata() {
    inspection_waiting_for_archive_uses_settled_metadata("file").await;
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_inspection_waiting_for_archive_uses_settled_metadata() {
    inspection_waiting_for_archive_uses_settled_metadata("sqlite").await;
}

#[cfg(feature = "storage-file")]
async fn attachment_fixture() -> (Arc<Manager>, String, Arc<Managed>) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model, "file").await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    (manager, id, managed)
}

#[cfg(feature = "storage-file")]
fn attachment_png() -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j7S8AAAAASUVORK5CYII=")
        .unwrap()
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn queued_attachment_revalidates_after_delete_or_archive() {
    use std::future::Future;
    for archive in [false, true] {
        let (manager, id, managed) = attachment_fixture().await;
        let principal = Principal::for_test("owner");
        let permit = manager.authorize_attachment(&principal, &id).await.unwrap();
        let revision = managed.record.lock().await.revision;
        let gate = manager.journal_open_gate(&id);
        let fence = gate.clone().lock_owned().await;
        let retirement = async {
            if archive {
                super::super::deletion::archive_session(&manager, "owner", &id, Some(revision))
                    .await
            } else {
                super::super::deletion::delete_session(&manager, "owner", &id, Some(revision)).await
            }
        };
        tokio::pin!(retirement);
        // Queue retirement first: Tokio's mutex admits waiters in FIFO order.
        tokio::select! {
            biased;
            result = &mut retirement => panic!("retirement bypassed its fence: {result:?}"),
            _ = std::future::ready(()) => {}
        }
        let waiting_owners = Arc::strong_count(&gate);
        let upload =
            manager.upload_attachment(&principal, &id, "image/png", attachment_png(), permit);
        tokio::pin!(upload);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            std::future::poll_fn(|cx| {
                use std::task::Poll;
                if let Poll::Ready(result) = upload.as_mut().poll(cx) {
                    panic!("upload bypassed the lifecycle fence: {result:?}");
                }
                if Arc::strong_count(&gate) > waiting_owners {
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }),
        )
        .await
        .unwrap();
        drop(fence);
        let (retired, uploaded) = tokio::join!(retirement, upload);
        retired.unwrap();
        assert_eq!(
            uploaded.unwrap_err().code,
            if archive { "archived" } else { "not_found" }
        );
        let directory = manager.config.data_dir.join("sessions").join(&id);
        if !archive {
            assert!(super::super::deletion::cleanup_settled(&manager, &id).await);
        }
        if archive {
            assert!(!directory.join("attachments").exists());
        } else {
            assert!(
                !directory.exists(),
                "rejected upload must not recreate deleted bytes"
            );
        }
        manager.close().await.unwrap();
    }
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn disconnected_attachment_waiter_keeps_storage_lifecycle_fence() {
    use std::{future::Future, task::Poll};
    let (manager, id, managed) = attachment_fixture().await;
    let principal = Principal::for_test("owner");
    let permit = manager.authorize_attachment(&principal, &id).await.unwrap();
    // Manager::active leaves its completed tracked activation in the JoinSet.
    // Retire it before observing this upload's admission; a nonempty set alone
    // otherwise mistakes the fixture's old owner for the new storage effect.
    {
        let mut tasks = manager.tasks.lock().unwrap();
        while let Some(result) = tasks.try_join_next() {
            result.unwrap();
        }
        assert!(tasks.is_empty(), "fixture activation must be settled");
    }
    let gate = manager.journal_open_gate(&id);
    let upload = manager.upload_attachment(&principal, &id, "image/png", attachment_png(), permit);
    let mut upload = Box::pin(upload);
    // On this current-thread runtime, observe tracked ownership in the same poll
    // that installs it, before the spawned filesystem effect can run.
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            if let Poll::Ready(result) = upload.as_mut().poll(cx) {
                panic!("upload finished before tracked storage admission: {result:?}");
            }
            if !manager.tasks.lock().unwrap().is_empty() {
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }),
    )
    .await
    .unwrap();
    drop(upload);
    assert!(
        gate.try_lock().is_err(),
        "tracked storage retains the fence after disconnect"
    );
    let revision = managed.record.lock().await.revision;
    super::super::deletion::delete_session(&manager, "owner", &id, Some(revision))
        .await
        .unwrap();
    assert!(super::super::deletion::cleanup_settled(&manager, &id).await);
    assert!(!manager.config.data_dir.join("sessions").join(&id).exists());
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
struct DeleteHost {
    disconnected: AtomicBool,
    unresolved: AtomicBool,
    released: Mutex<Vec<microcrab_host_api::process::ProcessHandle>>,
    kills: AtomicUsize,
    changes: tokio::sync::watch::Sender<u64>,
}
#[cfg(feature = "storage-file")]
#[async_trait::async_trait]
impl microcrab_host_api::ProcessApi for DeleteHost {
    fn watch(
        &self,
        _: microcrab_host_api::process::ProcessHandle,
    ) -> Result<tokio::sync::watch::Receiver<u64>, microcrab_host_api::HostError> {
        Ok(self.changes.subscribe())
    }
    async fn status(
        &self,
        _: &microcrab_host_api::OpContext,
        _: microcrab_host_api::process::ProcessHandle,
    ) -> Result<microcrab_host_api::process::ProcessStatus, microcrab_host_api::HostError> {
        Ok(microcrab_host_api::process::ProcessStatus {
            pid: None,
            state: microcrab_host_api::process::ProcessState::Running,
            elapsed_s: 0,
        })
    }
    async fn kill_tree(
        &self,
        _: &microcrab_host_api::OpContext,
        _: microcrab_host_api::process::ProcessHandle,
        _: bool,
    ) -> Result<(), microcrab_host_api::HostError> {
        self.kills.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn release(
        &self,
        _: &microcrab_host_api::OpContext,
        handle: microcrab_host_api::process::ProcessHandle,
    ) -> Result<(), microcrab_host_api::HostError> {
        if self.disconnected.load(Ordering::Acquire) {
            return Err(microcrab_host_api::HostError::new(
                microcrab_host_api::ErrorCategory::Io,
                "injected disconnected Host",
            ));
        }
        self.released.lock().unwrap().push(handle);
        Ok(())
    }
    fn has_unresolved_cleanup(&self) -> bool {
        self.unresolved.load(Ordering::Acquire)
    }
}
#[cfg(feature = "storage-file")]
#[async_trait::async_trait]
impl microcrab_host_api::ShellApi for DeleteHost {
    async fn launch(
        &self,
        _: &microcrab_host_api::OpContext,
        _: microcrab_host_api::ShellRequest,
    ) -> Result<microcrab_host_api::process::ProcessHandle, microcrab_host_api::HostError> {
        Ok(microcrab_host_api::process::ProcessHandle(
            microcrab_host_api::HandleId(17),
        ))
    }
    async fn capture_status(
        &self,
        _: &microcrab_host_api::OpContext,
        _: microcrab_host_api::process::ProcessHandle,
    ) -> Result<microcrab_host_api::ShellCaptureStatus, microcrab_host_api::HostError> {
        Ok(microcrab_host_api::ShellCaptureStatus {
            closed: false,
            cursor: microcrab_host_api::ShellPosition::default(),
            stdout_bytes: 0,
            stderr_bytes: 0,
            truncated: false,
        })
    }
}
#[cfg(feature = "storage-file")]
impl microcrab_host_api::FsApi for DeleteHost {}
#[cfg(feature = "storage-file")]
impl microcrab_host_api::NetApi for DeleteHost {}
#[cfg(feature = "storage-file")]
impl microcrab_host_api::HostAPI for DeleteHost {}
#[cfg(feature = "storage-file")]
impl microcrab_host_api::Host for DeleteHost {}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn explicit_delete_reconciles_exact_cleanup_ack_without_losing_quarantine() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (seed, _) = manager(model.clone(), "file").await;
    let config = seed.config.clone();
    seed.close().await.unwrap();
    let host = Arc::new(DeleteHost {
        disconnected: AtomicBool::new(false),
        unresolved: AtomicBool::new(false),
        released: Mutex::new(Vec::new()),
        kills: AtomicUsize::new(0),
        changes: tokio::sync::watch::channel(0).0,
    });
    let manager = Manager::initialize(config, host.clone(), Some(model), true)
        .await
        .unwrap();
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap();
    let id = created["id"].as_str().unwrap();
    let record = manager.record("owner", id).await.unwrap();
    let managed = manager.active(record.clone(), true).await.unwrap();
    let handle = managed
        .session
        .process_control()
        .launch_shell(
            "fixture",
            "delete-owned-process",
            microcrab_host_api::ShellRequest {
                command: "fixture".into(),
                cwd: None,
                output_bytes: 32,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let tree = manager.config.data_dir.join("sessions").join(id);
    host.disconnected.store(true, Ordering::Release);
    for _ in 0..2 {
        let error = deleted_with_failure(&manager, id, record.revision).await;
        assert_eq!(error.code, "operation_failed");
        assert!(!error.retryable);
        let details = error.details.unwrap();
        assert_eq!(details["reason"], "cleanup_pending");
        assert_eq!(details["quarantined"], true);
        assert_eq!(details["session_cleanup_unresolved"], true);
        assert_eq!(details["sticky_failure"], false);
        assert_eq!(details["cleanup_failures"], serde_json::json!([]));
        assert_eq!(
            details["session_cleanup_failure"]["code"],
            "operation_failed"
        );
        assert!(tree.exists());
        assert!(manager.registry.lock().unwrap().contains_key(id));
        assert!(managed.session.process_control().owns_process(handle));
    }
    assert!(host.released.lock().unwrap().is_empty());
    assert_eq!(
        host.kills.load(Ordering::SeqCst),
        1,
        "retry must not replay preliminary kill"
    );
    assert!(
        managed.session.close().await.is_err(),
        "ordinary close retains failed outcome"
    );
    assert!(
        super::super::deletion::archive_session(&manager, "owner", id, Some(record.revision))
            .await
            .is_err()
    );
    // Acknowledging the known process still cannot erase other unresolved Host ownership.
    host.disconnected.store(false, Ordering::Release);
    host.unresolved.store(true, Ordering::Release);
    deleted_with_failure(&manager, id, record.revision).await;
    assert_eq!(*host.released.lock().unwrap(), vec![handle]);
    assert!(tree.exists());
    host.unresolved.store(false, Ordering::Release);
    let deleted =
        super::super::deletion::delete_session(&manager, "owner", id, Some(record.revision))
            .await
            .unwrap();
    assert_eq!(deleted["deleted"], true);
    assert!(super::super::deletion::cleanup_settled(&manager, id).await);
    assert_eq!(
        *host.released.lock().unwrap(),
        vec![handle],
        "ACKed identity is never replayed"
    );
    assert!(!tree.exists());
    assert_eq!(manager.directory.get("owner", id).await.unwrap(), None);
    assert!(!manager.registry.lock().unwrap().contains_key(id));
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn cleanup_failure_snapshot_survives_later_successful_reconciliation() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (seed, _) = manager(model.clone(), "file").await;
    let config = seed.config.clone();
    seed.close().await.unwrap();
    let host = Arc::new(DeleteHost {
        disconnected: AtomicBool::new(false),
        unresolved: AtomicBool::new(false),
        released: Mutex::new(Vec::new()),
        kills: AtomicUsize::new(0),
        changes: tokio::sync::watch::channel(0).0,
    });
    let manager = Manager::initialize(config, host.clone(), Some(model), true)
        .await
        .unwrap();
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap();
    let id = created["id"].as_str().unwrap();
    let record = manager.record("owner", id).await.unwrap();
    let managed = manager.active(record.clone(), true).await.unwrap();
    let tree = manager.config.data_dir.join("sessions").join(id);
    host.unresolved.store(true, Ordering::Release);
    let failure = manager
        .close_managed_owned(&managed, false)
        .await
        .expect_err("unresolved Host cleanup must return a diagnostic snapshot");
    let before = serde_json::to_value(failure.wire_error()).unwrap();
    assert_eq!(before["details"]["session_cleanup_unresolved"], true);
    assert_eq!(before["details"]["sticky_failure"], false);
    assert_eq!(
        before["details"]["session_cleanup_failure"]["code"],
        "operation_failed"
    );
    assert!(tree.exists());

    host.unresolved.store(false, Ordering::Release);
    assert!(manager.close_managed_owned(&managed, true).await.is_ok());
    assert!(
        managed
            .cleanup
            .lock()
            .await
            .as_ref()
            .unwrap()
            .session_failure
            .is_none()
    );
    // Delay wire projection until after reconciliation, as the racing caller can.
    assert_eq!(serde_json::to_value(failure.wire_error()).unwrap(), before);
    assert!(
        tree.exists(),
        "reconciliation alone must retain the byte tree"
    );
    assert_eq!(
        manager.directory.get("owner", id).await.unwrap(),
        Some(record.clone())
    );
    let deleted =
        super::super::deletion::delete_session(&manager, "owner", id, Some(record.revision))
            .await
            .unwrap();
    assert_eq!(deleted["deleted"], true);
    assert!(super::super::deletion::cleanup_settled(&manager, id).await);
    assert!(!tree.exists());
    assert_eq!(manager.directory.get("owner", id).await.unwrap(), None);
    manager.close().await.unwrap();
}

/// DELETE acknowledges with a tombstone; the background release outcome is
/// read from that receipt once its cleanup task has finished.
#[cfg(feature = "storage-file")]
async fn deleted_with_failure(
    manager: &Manager,
    id: &str,
    revision: u64,
) -> microcrab_protocol::application::WireError {
    let ack = super::super::deletion::delete_session(manager, "owner", id, Some(revision))
        .await
        .unwrap();
    assert_eq!(ack["deleted"], true);
    let stone = super::super::deletion::cleanup_outcome(manager, id)
        .await
        .expect("acknowledged deletion keeps a receipt");
    assert_eq!(stone.state, "cleanup_pending");
    assert_eq!(manager.directory.get("owner", id).await.unwrap(), None);
    serde_json::from_value(stone.failure.expect("failed cleanup keeps its cause")).unwrap()
}

#[cfg(feature = "storage-file")]
async fn sticky_owner_failure_diagnostics(simultaneous: bool) {
    let (manager, id, managed) = attachment_fixture().await;
    let record = manager.record("owner", &id).await.unwrap();
    *managed.worker.lock().await = Some(tokio::spawn(async {
        panic!("private turn panic payload /private/owner-path")
    }));
    if simultaneous {
        let previous = managed.statistics_events.lock().await.take();
        *managed.statistics_events.lock().await = Some(tokio::spawn(async move {
            if let Some(previous) = previous {
                previous.await.unwrap();
            }
            panic!("private statistics panic payload /private/statistics-path")
        }));
    }
    let expected = if simultaneous {
        serde_json::json!([
            {"stage":"turn_owner","code":"panic","message":"Cleanup owner panicked"},
            {"stage":"statistics_events","code":"panic","message":"Cleanup owner panicked"},
        ])
    } else {
        serde_json::json!([
            {"stage":"turn_owner","code":"panic","message":"Cleanup owner panicked"},
        ])
    };
    let mut first_failures = None;
    for _ in 0..2 {
        let error = deleted_with_failure(&manager, &id, record.revision).await;
        assert_eq!(error.code, "operation_failed");
        assert!(!error.retryable);
        let serialized = serde_json::to_string(&error).unwrap();
        assert!(!serialized.contains("private"));
        assert!(!serialized.contains("owner-path"));
        assert!(!serialized.contains("statistics-path"));
        let details = error.details.unwrap();
        assert_eq!(details["reason"], "cleanup_pending");
        assert_eq!(details["quarantined"], true);
        assert_eq!(details["sticky_failure"], true);
        assert_eq!(details["session_cleanup_unresolved"], false);
        assert_eq!(details["cleanup_failures"], expected);
        if let Some(first) = first_failures.as_ref() {
            assert_eq!(&details["cleanup_failures"], first);
        } else {
            first_failures = Some(details["cleanup_failures"].clone());
        }
        assert!(manager.registry.lock().unwrap().contains_key(&id));
        assert!(manager.config.data_dir.join("sessions").join(&id).exists());
        // Later stages still ran despite the failed earlier join(s).
        assert!(managed.tool_task_events.lock().await.is_none());
        assert!(managed.statistics_events.lock().await.is_none());
        assert!(managed.session.close().await.is_ok());
    }
    assert!(manager.close().await.is_err());
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn explicit_delete_never_erases_sticky_owner_failure() {
    sticky_owner_failure_diagnostics(false).await;
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn explicit_delete_retains_simultaneous_owner_failure_diagnostics() {
    sticky_owner_failure_diagnostics(true).await;
}

#[cfg(feature = "storage-file")]
struct DeleteSnapshotFailure {
    journal: microcrab_storage::CreatorJournal,
    commits: AtomicUsize,
    closes: AtomicUsize,
}
#[cfg(feature = "storage-file")]
#[async_trait::async_trait]
impl microcrab_session::Journal for DeleteSnapshotFailure {
    async fn commit(&self, _: microcrab_session::Recover) -> Result<(), Error> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        Err(Error::Protocol(
            "injected final snapshot validation failure".into(),
        ))
    }
    async fn close(&self) -> Result<(), Error> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        microcrab_session::Journal::close(&self.journal).await
    }
}

#[cfg(feature = "storage-file")]
async fn delete_with_failed_final_snapshot(retry_cleanup: bool) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (seed, _) = manager(model.clone(), "file").await;
    let config = seed.config.clone();
    seed.close().await.unwrap();
    let host = Arc::new(DeleteHost {
        disconnected: AtomicBool::new(false),
        unresolved: AtomicBool::new(false),
        released: Mutex::new(Vec::new()),
        kills: AtomicUsize::new(0),
        changes: tokio::sync::watch::channel(0).0,
    });
    let manager = Manager::initialize(config, host.clone(), Some(model.clone()), true)
        .await
        .unwrap();
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap();
    let id = created["id"].as_str().unwrap();
    let record = manager.record("owner", id).await.unwrap();
    let managed = manager.active(record.clone(), true).await.unwrap();
    // Replace only the test runtime's persistence port. Drain its observation
    // owners first, so no background strong reference or old writer is abandoned.
    managed.close_token.cancel();
    if let Some(task) = managed.tool_task_events.lock().await.take() {
        task.await.unwrap();
    }
    if let Some(task) = managed.statistics_events.lock().await.take() {
        task.await.unwrap();
    }
    managed.session.close().await.unwrap();
    let slot = manager.registry.lock().unwrap().get(id).unwrap().clone();
    drop(slot.active.lock().await.take());
    while let Some(result) = manager.tasks.lock().unwrap().try_join_next() {
        result.unwrap();
    }
    let mut managed = Arc::try_unwrap(managed).unwrap_or_else(|_| panic!("fixture owners drained"));
    let journal = Arc::new(DeleteSnapshotFailure {
        journal: managed.journal.clone(),
        commits: AtomicUsize::new(0),
        closes: AtomicUsize::new(0),
    });
    managed.session = Session::builder_arc(
        model,
        microcrab_session::runtime::Runtime::new(
            Vec::new(),
            Arc::new(microcrab_session::runtime::tool::DenyAll),
        )
        .unwrap(),
    )
    .session_id(id)
    .is_main(true)
    .host(host.clone())
    .journal_arc(journal.clone())
    .build()
    .unwrap();
    let managed = Arc::new(managed);
    *slot.active.lock().await = Some(managed.clone());
    let handle = managed
        .session
        .process_control()
        .launch_shell(
            "fixture",
            "final-snapshot-owned-process",
            microcrab_host_api::ShellRequest {
                command: "fixture".into(),
                cwd: None,
                output_bytes: 32,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let tree = manager.config.data_dir.join("sessions").join(id);
    for (owner, revision) in [
        ("other-owner", record.revision),
        ("owner", record.revision + 1),
    ] {
        assert!(
            super::super::deletion::delete_session(&manager, owner, id, Some(revision))
                .await
                .is_err()
        );
        assert!(tree.exists());
        assert!(managed.session.process_control().owns_process(handle));
    }
    assert_eq!(host.kills.load(Ordering::SeqCst), 0);
    if retry_cleanup {
        host.disconnected.store(true, Ordering::Release);
        deleted_with_failure(&manager, id, record.revision).await;
        // Destructive close never writes the terminal checkpoint.
        assert_eq!(journal.commits.load(Ordering::SeqCst), 0);
        assert_eq!(journal.closes.load(Ordering::SeqCst), 1);
        assert!(tree.exists());
        assert!(managed.session.process_control().owns_process(handle));
        assert!(manager.registry.lock().unwrap().contains_key(id));
        assert!(managed.session.close().await.is_err());
        host.disconnected.store(false, Ordering::Release);
    }
    let deleted =
        super::super::deletion::delete_session(&manager, "owner", id, Some(record.revision))
            .await
            .unwrap();
    assert_eq!(deleted["deleted"], true);
    assert!(super::super::deletion::cleanup_settled(&manager, id).await);
    assert_eq!(*host.released.lock().unwrap(), vec![handle]);
    assert_eq!(
        host.kills.load(Ordering::SeqCst),
        1,
        "reconciliation does not repeat preliminary kill"
    );
    assert_eq!(
        journal.commits.load(Ordering::SeqCst),
        0,
        "destructive delete skips the terminal checkpoint"
    );
    assert_eq!(journal.closes.load(Ordering::SeqCst), 1);
    assert!(!tree.exists());
    assert_eq!(manager.directory.get("owner", id).await.unwrap(), None);
    assert!(!manager.registry.lock().unwrap().contains_key(id));
    // The failing final snapshot is never attempted by a destructive close, so
    // there is no persistence failure for a later ordinary close to report.
    assert!(managed.session.close().await.is_ok());
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn delete_after_final_snapshot_failure_preserves_authority() {
    delete_with_failed_final_snapshot(false).await;
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn delete_after_final_snapshot_failure_retries_host_cleanup() {
    delete_with_failed_final_snapshot(true).await;
}

/// Lifecycle requests are idempotent on their target state: a repeated
/// archive returns the same settled view without a new revision, and a
/// repeated delete answers from its owner-scoped receipt instead of 404.
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn archive_and_delete_are_idempotent_and_owner_scoped() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let mut ids = vec![];
    for _ in 0..2 {
        let created = commands::create(&manager, "owner", &command("session.create", None,
            serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
            .await.unwrap();
        ids.push(created["id"].as_str().unwrap().to_owned());
    }
    // Active archive, then a duplicated request with the stale revision.
    let record = manager.record("owner", &ids[0]).await.unwrap();
    let first =
        super::super::deletion::archive_session(&manager, "owner", &ids[0], Some(record.revision))
            .await
            .unwrap();
    let again =
        super::super::deletion::archive_session(&manager, "owner", &ids[0], Some(record.revision))
            .await
            .unwrap();
    assert_eq!(first, again);
    assert!(
        first["agent_id"]
            .as_str()
            .is_some_and(|id| id.starts_with("agt_"))
    );
    assert_eq!(first["depth"], 0);
    assert_eq!(first["interaction"], "persistent");
    assert!(first["current_role"].is_null());
    assert_eq!(first["accepts_user_input"], false);
    assert!(!manager.registry.lock().unwrap().contains_key(&ids[0]));
    assert!(
        super::super::deletion::archive_session(&manager, "foreign", &ids[0], None)
            .await
            .is_err()
    );
    assert!(!manager.registry.lock().unwrap().contains_key(&ids[0]));
    assert_eq!(first["archived"], true);
    assert_eq!(
        manager
            .directory
            .get("owner", &ids[0])
            .await
            .unwrap()
            .unwrap()
            .revision,
        first["revision"].as_u64().unwrap()
    );
    // Active delete: the response returns before cleanup; retries agree.
    let record = manager.record("owner", &ids[1]).await.unwrap();
    manager.active(record.clone(), true).await.unwrap();
    let started = std::time::Instant::now();
    let deleted =
        super::super::deletion::delete_session(&manager, "owner", &ids[1], Some(record.revision))
            .await
            .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(deleted["deleted"], true);
    assert_eq!(manager.directory.get("owner", &ids[1]).await.unwrap(), None);
    assert!(super::super::deletion::cleanup_settled(&manager, &ids[1]).await);
    for _ in 0..2 {
        let repeated = super::super::deletion::delete_session(
            &manager,
            "owner",
            &ids[1],
            Some(record.revision),
        )
        .await
        .unwrap();
        assert_eq!(repeated["deleted"], true);
        assert_eq!(repeated["session_id"], ids[1]);
        assert_eq!(repeated["state"], "deleted");
    }
    assert!(!manager.registry.lock().unwrap().contains_key(&ids[1]));
    assert!(
        !manager
            .config
            .data_dir
            .join("sessions")
            .join(&ids[1])
            .exists()
    );
    // Owner isolation and unknown ids still answer not found.
    assert_eq!(
        super::super::deletion::delete_session(&manager, "other-owner", &ids[1], None)
            .await
            .unwrap_err()
            .code,
        "not_found"
    );
    assert_eq!(
        super::super::deletion::delete_session(
            &manager,
            "owner",
            "0123456789abcdef0123456789abcdef",
            None
        )
        .await
        .unwrap_err()
        .code,
        "not_found"
    );
    eprintln!(
        "metric active_delete_ack_ms={}",
        elapsed.as_secs_f64() * 1000.0
    );
    manager.close().await.unwrap();
}

/// A title is directory metadata: renaming a dormant session never restores
/// its runtime, and the explicit title suppresses later auto naming.
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn dormant_rename_does_not_activate_the_runtime() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let record = manager.record("owner", &id).await.unwrap();
    let _ = manager
        .command(
            &Principal::for_test("owner"),
            &command("session.close", Some(&id), serde_json::json!({})),
        )
        .await
        .unwrap();
    assert!(manager.registry.lock().unwrap().get(&id).is_none());
    let renamed = manager
        .command(
            &Principal::for_test("owner"),
            &command(
                "session.rename",
                Some(&id),
                serde_json::json!({"title":"Dormant title","expected_revision":record.revision}),
            ),
        )
        .await
        .unwrap();
    assert_eq!(renamed["title"], "Dormant title");
    assert_eq!(renamed["revision"], record.revision + 1);
    assert!(
        manager.registry.lock().unwrap().get(&id).is_none(),
        "dormant rename must not restore a runtime"
    );
    let stored = manager.directory.get("owner", &id).await.unwrap().unwrap();
    assert_eq!(stored.title, "Dormant title");
    manager.close().await.unwrap();
}

/// Simulate a crash after durable deletion intent but before directory removal.
/// Recovery must remove metadata before the reopened Manager can admit readers.
async fn restart_reconciles_deletion_intent(storage: &str) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), storage).await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let config = manager.config.clone();
    assert!(manager.directory.get("owner", &id).await.unwrap().is_some());
    manager.close().await.unwrap();
    super::super::deletion::tombstone::write(
        &manager,
        &id,
        "owner",
        super::super::deletion::tombstone::CLEANUP_PENDING,
        None,
    )
    .await
    .unwrap();
    drop(manager);

    let reopened = Manager::with_client(config, model).await.unwrap();
    assert_eq!(reopened.directory.get("owner", &id).await.unwrap(), None);
    assert_eq!(
        reopened.record("owner", &id).await.unwrap_err().code,
        "not_found"
    );
    assert!(super::super::deletion::cleanup_settled(&reopened, &id).await);
    assert!(!reopened.config.data_dir.join("sessions").join(&id).exists());
    assert_eq!(
        super::super::deletion::delete_session(&reopened, "owner", &id, None)
            .await
            .unwrap()["state"],
        "deleted"
    );
    assert_eq!(
        super::super::deletion::delete_session(&reopened, "other-owner", &id, None)
            .await
            .unwrap_err()
            .code,
        "not_found"
    );
    reopened.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_restart_reconciles_deletion_intent_before_metadata_removal() {
    restart_reconciles_deletion_intent("file").await;
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_restart_reconciles_deletion_intent_before_metadata_removal() {
    restart_reconciles_deletion_intent("sqlite").await;
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn malformed_deletion_receipt_blocks_startup_instead_of_serving_metadata() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let config = manager.config.clone();
    manager.close().await.unwrap();
    drop(manager);
    let root = config.data_dir.join("deleted-sessions");
    tokio::fs::create_dir_all(&root).await.unwrap();
    tokio::fs::write(root.join(format!("{id}.json")), b"{invalid")
        .await
        .unwrap();
    let error = Manager::with_client(config, model)
        .await
        .err()
        .expect("invalid durable intent must stop startup");
    let rendered = error.to_string();
    assert!(
        rendered.contains("Deletion receipt JSON is invalid"),
        "{rendered}"
    );
    assert!(rendered.contains("line 1"), "{rendered}");
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn deletion_receipt_scan_reports_non_directory_instead_of_empty_recovery() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model, "file").await;
    tokio::fs::write(
        manager.config.data_dir.join("deleted-sessions"),
        b"not a directory",
    )
    .await
    .unwrap();
    let error = super::super::deletion::tombstone::pending(&manager)
        .await
        .err()
        .expect("receipt enumeration failure must propagate");
    assert!(error.message.contains("Cannot enumerate deletion receipts"));
    assert!(error.message.contains("OS code"));
    manager.close().await.unwrap();
}
