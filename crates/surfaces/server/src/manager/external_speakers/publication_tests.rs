//! Model a file rename that publishes durably but leaves a quarantined stale reader after sync failure.
use super::{model, store};
use microcrab_session::Error;
use microcrab_storage::configuration::{Configuration, ConfigurationRecord};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct PublishedThenError {
    inner: Arc<dyn Configuration>,
    quarantined: AtomicBool,
    writes: AtomicUsize,
}
impl PublishedThenError {
    fn new(inner: Arc<dyn Configuration>) -> Self {
        Self {
            inner,
            quarantined: AtomicBool::new(false),
            writes: AtomicUsize::new(0),
        }
    }
}
#[async_trait::async_trait]
impl Configuration for PublishedThenError {
    async fn close(&self) -> Result<(), Error> {
        self.inner.close().await
    }
    async fn get(
        &self,
        owner: &str,
        kind: &str,
        target: &str,
    ) -> Result<Option<ConfigurationRecord>, Error> {
        if self.quarantined.load(Ordering::Acquire) {
            return Ok(None);
        }
        self.inner.get(owner, kind, target).await
    }
    async fn list(&self, owner: &str, kind: &str) -> Result<Vec<ConfigurationRecord>, Error> {
        if self.quarantined.load(Ordering::Acquire) {
            return Ok(vec![]);
        }
        self.inner.list(owner, kind).await
    }
    async fn compare_exchange(
        &self,
        owner: &str,
        kind: &str,
        target: &str,
        expected_revision: u64,
        value: Option<Value>,
    ) -> Result<ConfigurationRecord, Error> {
        self.writes.fetch_add(1, Ordering::AcqRel);
        self.inner
            .compare_exchange(owner, kind, target, expected_revision, value)
            .await?;
        self.quarantined.store(true, Ordering::Release);
        Err(std::io::Error::other(
            "injected parent directory synchronization failure after publication",
        )
        .into())
    }
}
fn config(operation: &str) -> model::Configuration {
    let mut config = model::Configuration::default();
    let layer = model::Layer::from_changes(
        serde_json::from_value(json!({"monitor":{"action":"set","key":{"kind":"generated"}}}))
            .unwrap(),
        false,
        None,
    )
    .unwrap();
    config.projects.insert("project".into(), layer);
    let result =
        super::snapshot(&config, 1, Some("project"), None, Some(operation.into())).unwrap();
    config.operations.insert(
        operation.into(),
        model::Operation {
            project_id: Some("project".into()),
            session_id: None,
            request_fingerprint: "f".repeat(64),
            result,
        },
    );
    config
}
async fn fail_after_publish(storage: Arc<dyn Configuration>, operation: &str) -> Value {
    let config = config(operation);
    let expected = serde_json::to_value(&config.operations[operation].result).unwrap();
    let wrapper = PublishedThenError::new(storage);
    let error = store::save(&wrapper, "owner", 0, &config, operation)
        .await
        .unwrap_err();
    assert_eq!(error.code, "outcome_unknown");
    assert!(
        error
            .message
            .contains("parent directory synchronization failure after publication")
    );
    let details = error.details.unwrap();
    assert_eq!(details["operation_id"], operation);
    assert_eq!(details["cause"]["code"], "io");
    assert_eq!(details["cause"]["details"]["kind"], "Other");
    assert!(
        !serde_json::to_string(&details).unwrap().contains(
            config.effective(Some("project"), None).unwrap()["monitor"]
                .key
                .expose()
        )
    );
    // The stale in-process view does not prove failure. No write/replay is attempted during observation.
    let (_, stale) = store::load(&wrapper, "owner").await.unwrap();
    assert!(
        super::receipt(&stale, operation, Some("project"), None)
            .unwrap()
            .is_none()
    );
    assert_eq!(wrapper.writes.load(Ordering::Acquire), 1);
    wrapper.close().await.unwrap();
    expected
}
async fn observe_reopened(storage: &dyn Configuration, operation: &str, expected: Value) {
    let (revision, restored) = store::load(storage, "owner").await.unwrap();
    assert_eq!(revision, 1);
    let receipt = super::receipt(&restored, operation, Some("project"), None)
        .unwrap()
        .unwrap();
    assert_eq!(serde_json::to_value(&receipt.result).unwrap(), expected);
    assert_eq!(receipt.result.operation_id.as_deref(), Some(operation));
    storage.close().await.unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_uncertain_migration_withholds_proof_until_durable_reload() {
    let root = tempfile::tempdir().unwrap();
    let storage = microcrab_storage::configuration::FileConfiguration::open(root.path())
        .await
        .unwrap();
    let key = "z".repeat(43);
    storage
        .compare_exchange(
            "owner",
            "external_speakers",
            "resources",
            0,
            Some(json!({"projects":{"project":{"bot":{"action":"set","key":key}}},"enabled_projects":["project"],"sessions":{}})),
        )
        .await
        .unwrap();
    let wrapper = PublishedThenError::new(Arc::new(storage));
    let failed =
        store::authenticate_witnessed(&wrapper, "owner", Some("project"), "one", &key, true).await;
    assert!(matches!(failed, Err(error) if error.code == "storage_unavailable"));
    assert_eq!(wrapper.writes.load(Ordering::Acquire), 1);
    wrapper.close().await.unwrap();
    let reopened = microcrab_storage::configuration::FileConfiguration::open(root.path())
        .await
        .unwrap();
    let observed =
        store::authenticate_witnessed(&reopened, "owner", Some("project"), "one", &key, false)
            .await
            .unwrap()
            .unwrap();
    let admitted =
        store::authenticate_witnessed(&reopened, "owner", Some("project"), "two", &key, true)
            .await
            .unwrap()
            .unwrap();
    assert!(observed.witness() == admitted.witness());
    assert_eq!(store::load(&reopened, "owner").await.unwrap().0, 2);
    reopened.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_published_error_is_unknown_until_read_only_same_uuid_recovery() {
    let root = tempfile::tempdir().unwrap();
    let operation = uuid::Uuid::new_v4().to_string();
    let storage = microcrab_storage::configuration::FileConfiguration::open(root.path())
        .await
        .unwrap();
    let expected = fail_after_publish(Arc::new(storage), &operation).await;
    let reopened = microcrab_storage::configuration::FileConfiguration::open(root.path())
        .await
        .unwrap();
    observe_reopened(&reopened, &operation, expected).await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_published_error_is_unknown_until_read_only_same_uuid_recovery() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config.sqlite");
    let operation = uuid::Uuid::new_v4().to_string();
    let storage = microcrab_storage::configuration::SqliteConfiguration::open(&path)
        .await
        .unwrap();
    let expected = fail_after_publish(Arc::new(storage), &operation).await;
    let reopened = microcrab_storage::configuration::SqliteConfiguration::open(&path)
        .await
        .unwrap();
    observe_reopened(&reopened, &operation, expected).await;
}
