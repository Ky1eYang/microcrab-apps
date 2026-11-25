//! Completed retry replays never invert Journal-before-command lifecycle locks.
use super::*;
use microcrab_storage::{
    Directory, OperationRecord, SessionRecord, directory::OperationSettlement,
};
use serde_json::json;

struct ReplayDirectory {
    inner: Arc<dyn Directory>,
    target: Mutex<Option<String>>,
    lookups: AtomicUsize,
    second_lookup: Semaphore,
    release_lookup: Semaphore,
}
#[async_trait::async_trait]
impl Directory for ReplayDirectory {
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
        let target = self.target.lock().unwrap().as_deref() == Some(id);
        if target {
            match self.lookups.fetch_add(1, Ordering::SeqCst) {
                0 => return Ok(None),
                1 => {
                    self.second_lookup.add_permits(1);
                    self.release_lookup.acquire().await.unwrap().forget();
                }
                _ => {}
            }
        }
        self.inner.get_operation(owner, id).await
    }
    async fn list_operations(&self, owner: &str, id: &str) -> Result<Vec<OperationRecord>, Error> {
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
async fn completed_retry_replay_releases_commands_before_journal(method: &str) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (bootstrap, _root) = manager(model.clone(), "file").await;
    let config = bootstrap.config.clone();
    bootstrap.close().await.unwrap();
    let directory = Arc::new(ReplayDirectory {
        inner: Arc::new(
            microcrab_storage::FileDirectory::open_with_history(
                config.data_dir.join("directory"),
                config.data_dir.join("sessions"),
            )
            .await
            .unwrap(),
        ),
        target: Mutex::new(None),
        lookups: AtomicUsize::new(0),
        second_lookup: Semaphore::new(0),
        release_lookup: Semaphore::new(0),
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
    let id = created["id"].as_str().unwrap().to_owned();
    let record = manager.record("owner", &id).await.unwrap();
    let managed = manager.active(record.clone(), true).await.unwrap();
    let operation_id = uuid::Uuid::new_v4().to_string();
    let replay = command(
        method,
        Some(&id),
        json!({"operation_id":operation_id,"expected_revision":record.revision}),
    );
    directory
        .create_operation(OperationRecord {
            version: microcrab_storage::RECORD_VERSION,
            id: operation_id.clone(),
            session_id: id.clone(),
            owner: "owner".into(),
            method: method.into(),
            payload_hash: commands::hash(&replay).unwrap(),
            status: "completed".into(),
            result: Some(json!({"ready":true})),
            revision: 0,
            created_at_ms: super::super::now(),
            updated_at_ms: super::super::now(),
        })
        .await
        .unwrap();
    *directory.target.lock().unwrap() = Some(operation_id);
    let task_manager = manager.clone();
    let retry = tokio::spawn(async move { task_manager.command(&owner, &replay).await });
    // Force the first lookup to miss and the gated duplicate lookup to observe
    // completion. Its command lock is now held, while lifecycle takes Journal.
    directory.second_lookup.acquire().await.unwrap().forget();
    let journal = manager.journal_open_gate(&id).lock_owned().await;
    directory.release_lookup.add_permits(1);
    // This models archive/delete's second lock. The old implementation deadlocks:
    // retry waits for our Journal while we wait for its command lock.
    let command_guard = managed.commands.lock().await;
    drop(command_guard);
    drop(journal);
    let result = retry.await.unwrap().unwrap();
    assert_eq!(result["session"]["id"], id);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn completed_agent_init_replay_obeys_lifecycle_lock_order() {
    completed_retry_replay_releases_commands_before_journal(commands::SESSION_RETRY_AGENT_INIT)
        .await;
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn completed_auto_compaction_replay_obeys_lifecycle_lock_order() {
    completed_retry_replay_releases_commands_before_journal(
        commands::SESSION_RETRY_AUTO_COMPACTION,
    )
    .await;
}
