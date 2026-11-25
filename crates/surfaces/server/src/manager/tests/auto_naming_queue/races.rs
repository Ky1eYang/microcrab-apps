//! Submitted Directory writes settle before deletion acknowledges removal.
use super::*;
use microcrab_storage::{
    Directory, OperationRecord, SessionRecord, directory::OperationSettlement,
};

struct TitleDirectory {
    inner: Arc<dyn Directory>,
    title_entered: Semaphore,
    title_release: Semaphore,
    removed: AtomicBool,
    late_writes: AtomicUsize,
}
#[async_trait::async_trait]
impl Directory for TitleDirectory {
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
        if record.title == "Queued session title" {
            self.title_entered.add_permits(1);
            self.title_release.acquire().await.unwrap().forget();
            if self.removed.load(Ordering::Acquire) {
                self.late_writes.fetch_add(1, Ordering::SeqCst);
            }
        }
        self.inner.replace(owner, revision, record).await
    }
    async fn get_operation(&self, owner: &str, id: &str) -> Result<Option<OperationRecord>, Error> {
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
        let count = self.inner.remove(owner, id, revision).await?;
        self.removed.store(true, Ordering::Release);
        Ok(count)
    }
    async fn close(&self) -> Result<(), Error> {
        self.inner.close().await
    }
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn delete_joins_submitted_title_compare_before_directory_removal() {
    let model = NamingModel::new(false, false);
    let (bootstrap, _) = manager(model.clone(), "file").await;
    let mut config = bootstrap.config.clone();
    config.auto_rename = true;
    bootstrap.close().await.unwrap();
    let directory = Arc::new(TitleDirectory {
        inner: Arc::new(
            microcrab_storage::FileDirectory::open_with_history(
                config.data_dir.join("directory"),
                config.data_dir.join("sessions"),
            )
            .await
            .unwrap(),
        ),
        title_entered: Semaphore::new(0),
        title_release: Semaphore::new(0),
        removed: AtomicBool::new(false),
        late_writes: AtomicUsize::new(0),
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
                json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}),
            ),
        )
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    manager
        .command(
            &owner,
            &insert("turn.queue", &id, SessionInput::text("deletion seed")),
        )
        .await
        .unwrap();
    permit(&model.rename_entered).await;
    permit(&model.entered).await;
    let primary_token = model.primary_token.lock().unwrap().clone().unwrap();
    assert!(!primary_token.is_cancelled());
    model.rename_release.add_permits(1);
    permit(&directory.title_entered).await;
    let delete = super::super::super::deletion::delete_session(&manager, "owner", &id, None);
    tokio::pin!(delete);
    // Directory owns the title CAS and command gate. Foreground cancellation
    // must happen before waiting for that gate, while deletion still waits for
    // the naming write to settle before acknowledging metadata removal.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::select! {
            biased;
            result = &mut delete => panic!("delete acknowledged before title settlement: {result:?}"),
            _ = primary_token.cancelled() => {},
        }
    })
    .await
    .expect("foreground cancellation waited for the naming Directory CAS");
    assert!(managed.closing.load(Ordering::Acquire));
    assert!(managed.close_token.is_cancelled());
    assert!(primary_token.is_cancelled());
    assert!(!directory.removed.load(Ordering::Acquire));
    assert!(directory.get("owner", &id).await.unwrap().is_some());
    directory.title_release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), &mut delete)
        .await
        .unwrap()
        .unwrap();
    assert!(managed.naming.lock().await.is_none());
    assert!(managed.naming_token.is_cancelled());
    assert!(directory.get("owner", &id).await.unwrap().is_none());
    assert_eq!(directory.late_writes.load(Ordering::SeqCst), 0);
    assert!(super::super::super::deletion::cleanup_settled(&manager, &id).await);
    manager.close().await.unwrap();
}

async fn terminal_cancels_model(storage: &str) {
    for archive in [false, true] {
        let model = NamingModel::new(false, false);
        let (manager, id, managed) = fixture(model.clone(), storage).await;
        manager
            .command(
                &Principal::for_test("owner"),
                &insert("turn.steer", &id, SessionInput::text("blocked naming")),
            )
            .await
            .unwrap();
        permit(&model.rename_entered).await;
        if archive {
            super::super::super::deletion::archive_session(&manager, "owner", &id, None)
                .await
                .unwrap();
            assert!(
                manager
                    .directory
                    .get("owner", &id)
                    .await
                    .unwrap()
                    .unwrap()
                    .archived
            );
            assert_eq!(claim(&manager, &id).await.unwrap().status, "interrupted");
        } else {
            super::super::super::deletion::delete_session(&manager, "owner", &id, None)
                .await
                .unwrap();
            assert!(manager.directory.get("owner", &id).await.unwrap().is_none());
            assert!(super::super::super::deletion::cleanup_settled(&manager, &id).await);
        }
        assert!(managed.naming.lock().await.is_none());
        assert_eq!(model.renames.load(Ordering::SeqCst), 1);
        manager.close().await.unwrap();
    }
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn delete_and_archive_join_blocked_naming_file() {
    terminal_cancels_model("file").await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn delete_and_archive_join_blocked_naming_sqlite() {
    terminal_cancels_model("sqlite").await;
}
