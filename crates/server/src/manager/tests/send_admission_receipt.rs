//! A durable inbox receipt is independent from blocked observational Directory I/O.
use super::*;
use microcrab_storage::{
    Directory, OperationRecord, SessionRecord, directory::OperationSettlement,
};

async fn create(manager: &Manager) -> String {
    manager.command(&Principal::for_test("owner"), &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap()["id"].as_str().unwrap().to_owned()
}
fn insertion(method: &str, id: &str, text: &str) -> Command {
    command(
        method,
        Some(id),
        serde_json::json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text(text)}),
    )
}
async fn published(managed: &Managed, predicate: impl Fn(&Value) -> bool) {
    let mut events = managed.hub.subscribe().receiver;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if predicate(&managed.hub.snapshot()) {
                return;
            }
            events.recv().await.unwrap();
        }
    })
    .await
    .expect("owned observer did not publish after the Directory fence was released");
}

struct SnapshotDirectory {
    inner: Arc<dyn Directory>,
    blocked: AtomicBool,
    entered: Semaphore,
    release: CancellationToken,
    fail_with_worker: std::sync::Mutex<Option<std::sync::Weak<Managed>>>,
    rejected: AtomicUsize,
    fail_projection: AtomicBool,
}
#[async_trait::async_trait]
impl Directory for SnapshotDirectory {
    async fn get(&self, owner: &str, id: &str) -> Result<Option<SessionRecord>, Error> {
        self.inner.get(owner, id).await
    }
    async fn list(&self, owner: &str) -> Result<Vec<SessionRecord>, Error> {
        self.inner.list(owner).await
    }
    async fn create(&self, record: SessionRecord) -> Result<SessionRecord, Error> {
        self.inner.create(record).await
    }
    async fn replace(
        &self,
        owner: &str,
        revision: u64,
        record: SessionRecord,
    ) -> Result<SessionRecord, Error> {
        self.inner.replace(owner, revision, record).await
    }
    async fn get_operation(&self, owner: &str, id: &str) -> Result<Option<OperationRecord>, Error> {
        self.inner.get_operation(owner, id).await
    }
    async fn list_operations(&self, owner: &str, id: &str) -> Result<Vec<OperationRecord>, Error> {
        let managed = self
            .fail_with_worker
            .lock()
            .unwrap()
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        let worker_owned = match managed {
            Some(managed) => managed.worker.lock().await.is_some(),
            None => false,
        };
        if worker_owned || self.fail_projection.load(Ordering::Acquire) {
            self.rejected.fetch_add(1, Ordering::SeqCst);
            return Err(Error::Busy);
        }
        if self.blocked.load(Ordering::Acquire) {
            self.entered.add_permits(1);
            self.release.cancelled().await;
        }
        self.inner.list_operations(owner, id).await
    }
    async fn create_operation(&self, record: OperationRecord) -> Result<OperationRecord, Error> {
        self.inner.create_operation(record).await
    }
    async fn replace_operation(
        &self,
        owner: &str,
        revision: u64,
        record: OperationRecord,
    ) -> Result<OperationRecord, Error> {
        self.inner.replace_operation(owner, revision, record).await
    }
    async fn settle_operation(
        &self,
        owner: &str,
        settlement: OperationSettlement,
    ) -> Result<(SessionRecord, OperationRecord), Error> {
        self.inner.settle_operation(owner, settlement).await
    }
    async fn remove(&self, owner: &str, id: &str, revision: u64) -> Result<usize, Error> {
        self.inner.remove(owner, id, revision).await
    }
    async fn close(&self) -> Result<(), Error> {
        self.inner.close().await
    }
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn durable_inbox_receipts_return_while_owned_snapshot_observer_is_blocked() {
    // Use the ordinary assembled configuration and actual persistent Directory;
    // only list_operations is fenced, never admission or terminal publication.
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (bootstrap, _) = manager(model.clone(), "file").await;
    let config = bootstrap.config.clone();
    bootstrap.close().await.unwrap();
    drop(bootstrap);
    let directory = Arc::new(SnapshotDirectory {
        inner: Arc::new(
            microcrab_storage::FileDirectory::open_with_history(
                config.data_dir.join("directory"),
                config.data_dir.join("sessions"),
            )
            .await
            .unwrap(),
        ),
        blocked: AtomicBool::new(false),
        entered: Semaphore::new(0),
        release: CancellationToken::new(),
        fail_with_worker: std::sync::Mutex::new(None),
        rejected: AtomicUsize::new(0),
        fail_projection: AtomicBool::new(false),
    });
    let host = crate::host::startup_local_host(&config).unwrap();
    let manager = Manager::initialize_with_directory(
        config,
        host,
        Some(model.clone()),
        true,
        directory.clone(),
    )
    .await
    .unwrap();
    let owner = Principal::for_test("owner");
    let id = create(&manager).await;
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    managed.session.interrupt();
    let mut events = managed.hub.subscribe().receiver;
    directory.blocked.store(true, Ordering::Release);
    let insert = insertion("turn.queue", &id, "retained while projection is blocked");
    let inserted = manager.command(&owner, &insert);
    tokio::pin!(inserted);
    // Both futures are polled. The timeout is only a deadlock guard: the explicit
    // Directory barrier remains closed until after the exact receipt is checked.
    let receipt = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (receipt, entered) = tokio::join!(&mut inserted, directory.entered.acquire(),);
        entered.unwrap().forget();
        receipt.unwrap()
    })
    .await
    .expect("terminal receipt waited for unrelated snapshot work");
    assert!(!directory.release.is_cancelled());
    assert_eq!(receipt["status"], "completed");
    assert_eq!(receipt["revision"], 1);
    assert_eq!(receipt["result"]["admitted"], true);
    let operation_id = receipt["id"].as_str().unwrap();
    assert_eq!(
        serde_json::to_value(
            directory
                .get_operation("owner", operation_id)
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        receipt
    );
    let inbox = managed.session.inbox_snapshot();
    assert_eq!(inbox.items.len(), 1);
    let item = &inbox.items[0];
    let update = command(
        "inbox.update",
        Some(&id),
        serde_json::json!({
            "operation_id":uuid::Uuid::new_v4().to_string(),"expected_inbox_id":inbox.inbox_id,
            "item_id":item.id,"expected_revision":item.revision,
            "input":microcrab_session::SessionInput::text("edited while projection is blocked")
        }),
    );
    let updated = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        manager.command(&owner, &update),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(updated["status"], "completed");
    let current = managed.session.inbox_snapshot();
    let item = &current.items[0];
    let remove = command(
        "inbox.remove",
        Some(&id),
        serde_json::json!({
            "operation_id":uuid::Uuid::new_v4().to_string(),"expected_inbox_id":current.inbox_id,
            "item_id":item.id,"expected_revision":item.revision
        }),
    );
    let removed = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        manager.command(&owner, &remove),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(removed["status"], "completed");
    assert!(managed.session.inbox_snapshot().items.is_empty());
    // Exact duplicate commands read durable terminal state without reinsertion,
    // duplicate events, another model call or a snapshot prerequisite.
    for (command, expected) in [
        (&insert, &receipt),
        (&update, &updated),
        (&remove, &removed),
    ] {
        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                manager.command(&owner, command)
            )
            .await
            .unwrap()
            .unwrap(),
            *expected
        );
    }
    let mut turn_ids = Vec::new();
    while let Ok(event) = events.try_recv() {
        if matches!(&event.event, super::super::SessionRuntimeEvent::Turn(_)) {
            turn_ids.push(event.event.payload()["id"].as_str().unwrap().to_owned());
        }
    }
    let mut expected = vec![
        receipt["id"].as_str().unwrap().to_owned(),
        updated["id"].as_str().unwrap().to_owned(),
        removed["id"].as_str().unwrap().to_owned(),
    ];
    expected.sort();
    turn_ids.sort();
    assert_eq!(turn_ids, expected);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    // Releasing only observational I/O lets the same existing owned watcher
    // publish canonical state, with no command replay or new background owner.
    directory.blocked.store(false, Ordering::Release);
    directory.release.cancel();
    published(&managed, |snapshot| {
        snapshot["operations"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["id"] == removed["id"]))
    })
    .await;
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn accepted_turn_and_terminal_receipt_survive_busy_projection_without_replay_or_fault() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (bootstrap, _) = manager(model.clone(), "file").await;
    let config = bootstrap.config.clone();
    bootstrap.close().await.unwrap();
    drop(bootstrap);
    let directory = Arc::new(SnapshotDirectory {
        inner: Arc::new(
            microcrab_storage::FileDirectory::open_with_history(
                config.data_dir.join("directory"),
                config.data_dir.join("sessions"),
            )
            .await
            .unwrap(),
        ),
        blocked: AtomicBool::new(false),
        entered: Semaphore::new(0),
        release: CancellationToken::new(),
        fail_with_worker: std::sync::Mutex::new(None),
        rejected: AtomicUsize::new(0),
        fail_projection: AtomicBool::new(false),
    });
    let host = crate::host::startup_local_host(&config).unwrap();
    let manager = Manager::initialize_with_directory(
        config,
        host,
        Some(model.clone()),
        true,
        directory.clone(),
    )
    .await
    .unwrap();
    let owner = Principal::for_test("owner");
    let id = create(&manager).await;
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let mut events = managed.hub.subscribe().receiver;
    // The pre-start refresh sees no registered worker. The post-start refresh
    // deterministically sees this exact slot and fails, independent of scheduling.
    *directory.fail_with_worker.lock().unwrap() = Some(Arc::downgrade(&managed));
    let submit = insertion("turn.submit", &id, "accepted despite projection conflict");
    let accepted = manager.command(&owner, &submit).await.unwrap();
    assert_eq!(accepted["status"], "accepted");
    assert!(directory.rejected.load(Ordering::SeqCst) > 0);
    tokio::time::timeout(std::time::Duration::from_secs(5), model.entered.notified())
        .await
        .unwrap();
    assert!(managed.session.inbox_snapshot().active);
    assert_eq!(managed.hub.operation().as_deref(), accepted["id"].as_str());
    assert!(!managed.faulted.load(Ordering::Acquire));
    assert_eq!(
        serde_json::to_value(
            directory
                .get_operation("owner", accepted["id"].as_str().unwrap())
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        accepted
    );
    let mut resync = false;
    while let Ok(event) = events.try_recv() {
        resync |= matches!(
            &event.event,
            super::super::SessionRuntimeEvent::ResyncRequired(
                super::super::ResyncReason::SnapshotFailed
            )
        );
    }
    assert!(
        resync,
        "accepted projection conflict must request resynchronization"
    );
    assert_eq!(manager.command(&owner, &submit).await.unwrap(), accepted);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);

    // Keep projection Busy through the driver's final refresh as well. Its
    // completed durable receipt remains authoritative and the Session stays usable.
    *directory.fail_with_worker.lock().unwrap() = None;
    directory.fail_projection.store(true, Ordering::Release);
    model.release.add_permits(1);
    let worker = managed.worker.lock().await.take().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap();
    let terminal = directory
        .get_operation("owner", accepted["id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.status, "completed");
    assert!(!managed.faulted.load(Ordering::Acquire));
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    directory.fail_projection.store(false, Ordering::Release);
    manager.refresh(&managed).await.unwrap();
    assert_eq!(managed.hub.snapshot()["session"]["status"], "idle");
    assert_eq!(
        manager.command(&owner, &submit).await.unwrap(),
        serde_json::to_value(terminal).unwrap()
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    manager.close().await.unwrap();
}
