//! Real Session streams exercise joined child drivers, caller cancellation and durable receipt failures.
use super::*;
use microcrab_storage::{Directory, OperationRecord};
use serde_json::json;
struct ChildModel {
    entered: Semaphore,
    release: Semaphore,
    cleaned: Semaphore,
}
struct ModelCleanup<'a>(&'a Semaphore);
impl Drop for ModelCleanup<'_> {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}
#[async_trait::async_trait]
impl microcrab_session::Client for ChildModel {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        cancel: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let _cleanup = ModelCleanup(&self.cleaned);
        self.entered.add_permits(1);
        tokio::select! {biased; _=cancel.cancelled()=>Err(microcrab_transport::Error::Cancelled),permit=self.release.acquire()=>{permit.unwrap().forget();Ok(Response{content:"actual child result".into(),tool_calls:vec![],provider_metadata:None,usage:Usage::default()})}}
    }
}
async fn fixture() -> (
    Arc<Manager>,
    Arc<Managed>,
    Arc<ChildModel>,
    microcrab_session::Session,
) {
    let root_model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(root_model, "file").await;
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
    let parent = manager
        .active(
            manager
                .record("owner", created["id"].as_str().unwrap())
                .await
                .unwrap(),
            true,
        )
        .await
        .unwrap();
    let model = Arc::new(ChildModel {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        cleaned: Semaphore::new(0),
    });
    let child = microcrab_session::Session::builder_arc(
        model.clone(),
        microcrab_session::runtime::Runtime::new(
            vec![],
            Arc::new(microcrab_session::runtime::tool::DenyAll),
        )
        .unwrap(),
    )
    .session_id("0123456789abcdef")
    .build()
    .unwrap();
    (manager, parent, model, child)
}
async fn receipt(manager: &Manager, parent: &Managed) -> OperationRecord {
    let id = parent.record.lock().await.id.clone();
    manager
        .directory
        .create_operation(OperationRecord {
            version: microcrab_storage::RECORD_VERSION,
            id: uuid::Uuid::new_v4().to_string(),
            session_id: id,
            owner: "owner".into(),
            method: "child.turn.submit".into(),
            payload_hash: "f".repeat(64),
            status: "accepted".into(),
            result: Some(json!({"child_id":"0123456789abcdef","server_instance":manager.instance})),
            revision: 0,
            created_at_ms: 1,
            updated_at_ms: 1,
        })
        .await
        .unwrap()
}
async fn permit(semaphore: &Semaphore) {
    tokio::time::timeout(std::time::Duration::from_secs(5), semaphore.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn child_input_parent_close_joins_actual_stream_cleanup_and_terminal_receipt() {
    let (manager, parent, model, child) = fixture().await;
    let receipt = receipt(&manager, &parent).await;
    let operation = receipt.id.clone();
    let (stream, settlement) = child
        .stream_submission_with_settlement(
            microcrab_session::SessionInput::text("child work"),
            operation.clone(),
            parent.close_token.child_token(),
        )
        .await
        .unwrap();
    parent
        .child_drivers
        .lock()
        .await
        .spawn(super::super::child_input::drive_child(
            manager.directory.clone(),
            parent.clone(),
            "0123456789abcdef".into(),
            receipt,
            stream,
            settlement,
        ));
    permit(&model.entered).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        manager.close_managed(&parent),
    )
    .await
    .unwrap()
    .unwrap();
    permit(&model.cleaned).await;
    assert!(parent.child_drivers.lock().await.is_empty());
    let observed = manager
        .directory
        .get_operation("owner", &operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.status, "cancelled");
    assert_eq!(observed.result.unwrap()["child_id"], "0123456789abcdef");
    child.close().await.unwrap();
    manager.close().await.unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn child_input_caller_abort_after_directory_ack_does_not_abandon_admission_driver() {
    let (manager, parent, model, child) = fixture().await;
    let ack = Arc::new(Semaphore::new(0));
    let continue_start = Arc::new(Semaphore::new(0));
    let (operation_tx, operation_rx) = tokio::sync::oneshot::channel();
    let owner = manager.clone();
    let owned_parent = parent.clone();
    let owned_child = child.clone();
    let owned_ack = ack.clone();
    let proceed = continue_start.clone();
    let registered = Arc::new(Semaphore::new(0));
    let owned_registered = registered.clone();
    let receiver =
        manager
            .track(async move {
                let receipt = receipt(&owner, &owned_parent).await;
                operation_tx.send(receipt.id.clone()).unwrap();
                owned_ack.add_permits(1);
                proceed.acquire().await.unwrap().forget();
                let (stream, settlement) = owned_child
                    .stream_submission_with_settlement(
                        microcrab_session::SessionInput::text("owned child"),
                        receipt.id.clone(),
                        owned_parent.close_token.child_token(),
                    )
                    .await
                    .unwrap();
                owned_parent.child_drivers.lock().await.spawn(
                    super::super::child_input::drive_child(
                        owner.directory.clone(),
                        owned_parent.clone(),
                        "0123456789abcdef".into(),
                        receipt,
                        stream,
                        settlement,
                    ),
                );
                owned_registered.add_permits(1);
            })
            .unwrap();
    let caller = tokio::spawn(receiver);
    permit(&ack).await;
    caller.abort();
    let _ = caller.await;
    let operation = operation_rx.await.unwrap();
    continue_start.add_permits(1);
    permit(&registered).await;
    permit(&model.entered).await;
    model.release.add_permits(1);
    {
        let mut drivers = parent.child_drivers.lock().await;
        while let Some(result) = drivers.join_next().await {
            result.unwrap().unwrap();
        }
    }
    let observed = manager
        .directory
        .get_operation("owner", &operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.status, "completed");
    assert_eq!(
        observed.result.unwrap()["outcome"]["content"],
        "actual child result"
    );
    child.close().await.unwrap();
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn child_input_restart_observation_never_guesses_completion_or_replays() {
    let (manager, parent, model, child) = fixture().await;
    let mut receipt = receipt(&manager, &parent).await;
    receipt.result =
        Some(json!({"child_id":"0123456789abcdef","server_instance":"previous-server"}));
    receipt.revision = 1;
    let receipt = manager
        .directory
        .replace_operation("owner", 0, receipt)
        .await
        .unwrap();
    assert_eq!(
        super::super::child_input::observe_prior(&manager, receipt.clone(), false)
            .await
            .unwrap_err()
            .code,
        "outcome_unknown"
    );
    assert_eq!(
        manager
            .directory
            .get_operation("owner", &receipt.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "accepted"
    );
    let observed = super::super::child_input::observe_prior(&manager, receipt.clone(), true)
        .await
        .unwrap();
    assert_eq!(observed.status, "failed");
    assert_eq!(
        observed.result.as_ref().unwrap()["error"]["code"],
        "operation_interrupted"
    );
    assert_eq!(
        observed.result.unwrap()["error"]["details"]["admission"],
        "unknown"
    );
    assert_eq!(model.entered.available_permits(), 0);
    child.close().await.unwrap();
    manager.close().await.unwrap();
}

struct RejectTerminal {
    inner: Arc<dyn Directory>,
}
#[async_trait::async_trait]
impl Directory for RejectTerminal {
    async fn get(&self, o: &str, id: &str) -> Result<Option<SessionRecord>, Error> {
        self.inner.get(o, id).await
    }
    async fn list(&self, o: &str) -> Result<Vec<SessionRecord>, Error> {
        self.inner.list(o).await
    }
    async fn create(&self, r: SessionRecord) -> Result<SessionRecord, Error> {
        self.inner.create(r).await
    }
    async fn replace(&self, o: &str, e: u64, r: SessionRecord) -> Result<SessionRecord, Error> {
        self.inner.replace(o, e, r).await
    }
    async fn get_operation(&self, o: &str, id: &str) -> Result<Option<OperationRecord>, Error> {
        self.inner.get_operation(o, id).await
    }
    async fn list_operations(&self, o: &str, id: &str) -> Result<Vec<OperationRecord>, Error> {
        self.inner.list_operations(o, id).await
    }
    async fn create_operation(&self, r: OperationRecord) -> Result<OperationRecord, Error> {
        self.inner.create_operation(r).await
    }
    async fn replace_operation(
        &self,
        _: &str,
        _: u64,
        _: OperationRecord,
    ) -> Result<OperationRecord, Error> {
        Err(std::io::Error::other("injected child terminal receipt publication failure").into())
    }
    async fn settle_operation(
        &self,
        o: &str,
        s: microcrab_storage::directory::OperationSettlement,
    ) -> Result<(SessionRecord, OperationRecord), Error> {
        self.inner.settle_operation(o, s).await
    }
    async fn remove(&self, o: &str, id: &str, e: u64) -> Result<usize, Error> {
        self.inner.remove(o, id, e).await
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn child_input_terminal_receipt_fault_keeps_accepted_unknown_without_replay() {
    let (manager, parent, model, child) = fixture().await;
    let receipt = receipt(&manager, &parent).await;
    let operation = receipt.id.clone();
    let (stream, settlement) = child
        .stream_submission_with_settlement(
            microcrab_session::SessionInput::text("child work"),
            operation.clone(),
            parent.close_token.child_token(),
        )
        .await
        .unwrap();
    let directory: Arc<dyn Directory> = Arc::new(RejectTerminal {
        inner: manager.directory.clone(),
    });
    let driver = tokio::spawn(super::super::child_input::drive_child(
        directory,
        parent.clone(),
        "0123456789abcdef".into(),
        receipt,
        stream,
        settlement,
    ));
    permit(&model.entered).await;
    model.release.add_permits(1);
    let error = driver.await.unwrap().unwrap_err();
    assert_eq!(error.code, "outcome_unknown");
    assert_eq!(error.details.unwrap()["operation_id"], operation);
    assert!(parent.faulted.load(Ordering::Acquire));
    assert_eq!(
        manager
            .directory
            .get_operation("owner", &operation)
            .await
            .unwrap()
            .unwrap()
            .status,
        "accepted"
    );
    assert_eq!(
        model.entered.available_permits(),
        0,
        "receipt observation never starts another model turn"
    );
    child.close().await.unwrap();
    manager.close().await.unwrap();
}
