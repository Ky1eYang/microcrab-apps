//! Tool policy commands retain durable settings and isolate incompatible records.
use super::*;
use microcrab_permission::Action;
use microcrab_storage::{
    Directory, OperationRecord, SessionRecord, directory::OperationSettlement,
};
use serde_json::json;

/// Advance native policy after a durable replace, before the command can publish
/// its user settings. This deterministically exercises the persistence rebase.
struct RebaseDirectory {
    inner: Arc<dyn Directory>,
    advance: std::sync::Mutex<Option<microcrab_session::Session>>,
}
#[async_trait::async_trait]
impl Directory for RebaseDirectory {
    async fn get(&self, owner: &str, id: &str) -> Result<Option<SessionRecord>, Error> {
        self.inner.get(owner, id).await
    }
    async fn list_entries(
        &self,
        owner: &str,
    ) -> Result<Vec<microcrab_storage::directory::SessionEntry>, Error> {
        self.inner.list_entries(owner).await
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
        let saved = self.inner.replace(owner, revision, record).await?;
        let advance = self.advance.lock().unwrap().take();
        if let Some(session) = advance {
            let current = session.permission_controller().snapshot().unwrap();
            let mut config = current.config;
            config.initialization = Action::Allow;
            session
                .replace_permissions(current.revision, config)
                .unwrap();
        }
        Ok(saved)
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
        self.inner.remove(owner, id, revision).await
    }
    async fn close(&self) -> Result<(), Error> {
        self.inner.close().await
    }
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn initialization_setting_is_rebased_after_native_policy_advances_during_persistence() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (bootstrap, _) = manager(model.clone(), "file").await;
    let config = bootstrap.config.clone();
    bootstrap.close().await.unwrap();
    let directory = Arc::new(RebaseDirectory {
        inner: Arc::new(
            microcrab_storage::FileDirectory::open_with_history(
                config.data_dir.join("directory"),
                config.data_dir.join("sessions"),
            )
            .await
            .unwrap(),
        ),
        advance: std::sync::Mutex::new(None),
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
    let created = commands::create(
        &manager,
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
    .unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), false)
        .await
        .unwrap();
    let before = managed.session.permission_controller().snapshot().unwrap();
    let context = serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap();
    let history = managed.journal.cursor().await;
    *directory.advance.lock().unwrap() = Some(managed.session.clone());
    let result = commands::execute(
        &manager,
        &Principal::for_test("owner"),
        &command(
            "permission.set",
            Some(id),
            json!({
                "expected_revision":before.revision, "overrides":[],
                "initialization":"deny", "review":false
            }),
        ),
    )
    .await
    .unwrap();
    let after = managed.session.permission_controller().snapshot().unwrap();
    assert_eq!(after.revision, before.revision + 2);
    assert_eq!(after.config.initialization, Action::Deny);
    assert_eq!(result["config"]["initialization"], "deny");
    assert_eq!(
        manager.record("owner", id).await.unwrap().initialization,
        Action::Deny
    );
    assert_eq!(
        after.config.provider_defaults,
        before.config.provider_defaults
    );
    assert_eq!(after.config.defaults, before.config.defaults);
    assert_eq!(after.config.agent, before.config.agent);
    assert_eq!(after.config.mode, before.config.mode);
    assert!(!after.config.review);
    assert_eq!(
        serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap(),
        context
    );
    assert_eq!(managed.journal.cursor().await, history);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn legacy_permission_inputs_are_rejected_without_durable_or_native_mutation() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(
        &manager,
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
    .unwrap();
    let id = created["id"].as_str().unwrap();
    let owner = Principal::for_test("owner");
    let get = command("permission.get", Some(id), json!({}));
    let before = commands::execute(&manager, &owner, &get).await.unwrap();
    let record = manager.record("owner", id).await.unwrap();
    for legacy in [
        json!({"capability":"file.read"}),
        json!({"resource":{"kind":"glob","value":"docs/**"}}),
        json!({"arguments":[]}),
        json!({"operation_arguments":{}}),
        json!({"conditions":[]}),
        json!({"action":"conditional"}),
    ] {
        let mut rule = json!({"tool":{"kind":"exact","value":"sleep"},"args":[],"action":"allow"});
        rule.as_object_mut()
            .unwrap()
            .extend(legacy.as_object().unwrap().clone());
        let failure = commands::execute(
            &manager,
            &owner,
            &command(
                "permission.set",
                Some(id),
                json!({
                    "expected_revision":before["revision"],"overrides":[rule],"review":false
                }),
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(failure.code, "invalid_request", "legacy input: {legacy}");
        assert_eq!(manager.record("owner", id).await.unwrap(), record);
        assert_eq!(
            commands::execute(&manager, &owner, &get).await.unwrap(),
            before
        );
    }
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn session_list_keeps_healthy_records_and_reports_owner_scoped_unavailable_records() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(
        &manager,
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
    .unwrap();
    let id = created["id"].as_str().unwrap();
    let record = manager.record("owner", id).await.unwrap();
    let config = manager.config.clone();
    manager.close().await.unwrap();
    drop(manager);
    let path = config.data_dir.join("directory").join("directory.json");
    let mut source: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let mut legacy = serde_json::to_value(&record).unwrap();
    legacy["id"] = json!("legacy-policy");
    legacy["permissions"] = json!([{"capability":"file.read","action":"allow"}]);
    source["sessions"]["legacy-policy"] = legacy.clone();
    legacy["id"] = json!("other-owner-legacy");
    legacy["owner"] = json!("other-owner");
    source["sessions"]["other-owner-legacy"] = legacy;
    let bytes = serde_json::to_vec(&source).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    let reopened = Manager::with_client(config, model.clone()).await.unwrap();
    let listed = reopened
        .command(
            &Principal::for_test("owner"),
            &command("session.list", None, json!({})),
        )
        .await
        .unwrap();
    let items = listed["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"], id);
    assert_eq!(listed["complete"], false);
    let unavailable = listed["unavailable"].as_array().unwrap();
    assert_eq!(unavailable.len(), 1);
    assert_eq!(unavailable[0]["id"], "legacy-policy");
    assert!(
        unavailable[0]["reason"]
            .as_str()
            .unwrap()
            .contains("capability")
    );
    assert_eq!(reopened.record("owner", id).await.unwrap(), record);
    assert_eq!(
        reopened
            .command(
                &Principal::for_test("intruder"),
                &command("session.list", None, json!({}))
            )
            .await
            .unwrap(),
        json!({"items":[],"complete":true,"unavailable":[]})
    );
    assert_eq!(
        std::fs::read(&path).unwrap(),
        bytes,
        "inspection must preserve incompatible bytes"
    );
    assert!(reopened.registry.lock().unwrap().is_empty());
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    reopened.close().await.unwrap();
}
