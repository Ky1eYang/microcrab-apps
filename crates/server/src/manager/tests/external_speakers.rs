//! Dedicated resource integration: no runtime activation and exact durable mutation receipts.
use super::*;
use serde_json::json;

async fn fixture(storage: &str) -> (Arc<Manager>, String, String) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _root) = manager(model, storage).await;
    let mut ids = Vec::new();
    for _ in 0..2 {
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
        let id = created["id"].as_str().unwrap().to_owned();
        let managed = manager
            .active(manager.record("owner", &id).await.unwrap(), true)
            .await
            .unwrap();
        commands::close_session(&manager, &managed, "owner", &id, None)
            .await
            .unwrap();
        ids.push(id);
    }
    (manager, ids.remove(0), ids.remove(0))
}
async fn request(
    manager: &Manager,
    method: &str,
    id: Option<&str>,
    params: Value,
) -> Result<Value, microcrab_protocol::application::WireError> {
    super::super::external_speakers::command(manager, "owner", &command(method, id, params)).await
}
async fn contract(storage: &str) {
    let (manager, one, two) = fixture(storage).await;
    let operation = uuid::Uuid::new_v4().to_string();
    let params = json!({"project_id":"project","operation_id":operation,"expected_revision":0,"speakers":{"monitor":{"action":"set","key":{"kind":"generated"}}}});
    let initial = request(
        &manager,
        "session.external_speakers.get",
        Some(&one),
        json!({}),
    )
    .await
    .unwrap();
    assert_eq!(initial["revision"], 0);
    assert!(initial["speakers"].as_array().unwrap().is_empty());
    assert!(
        manager
            .configuration
            .get("owner", "external_speakers", "resources")
            .await
            .unwrap()
            .is_none()
    );
    let saved = request(
        &manager,
        "project.external_speakers.set",
        None,
        params.clone(),
    )
    .await
    .unwrap();
    assert_eq!(saved["revision"], 1);
    assert_eq!(saved["operation_id"], operation);
    assert_eq!(saved["overrides"]["monitor"]["key"]["kind"], "keep");
    assert_eq!(
        request(
            &manager,
            "project.external_speakers.set",
            None,
            params.clone()
        )
        .await
        .unwrap(),
        saved
    );
    let revealed = request(
        &manager,
        "project.external_speakers.key",
        None,
        json!({"project_id":"project","identity":"monitor"}),
    )
    .await
    .unwrap();
    let key = revealed["key"].as_str().unwrap();
    assert!(!saved.to_string().contains(key));
    for id in [&one, &two] {
        assert_eq!(
            manager
                .authenticate_external_speaker("owner", id, key)
                .await
                .unwrap()
                .identity,
            "monitor"
        );
        assert_eq!(
            manager
                .effective_external_speakers("owner", id)
                .await
                .unwrap()
                .len(),
            1
        );
    }
    assert!(
        manager
            .authenticate_external_speaker("intruder", &one, key)
            .await
            .is_err()
    );
    let mut different = params.clone();
    different["expected_revision"] = json!(1);
    assert_eq!(
        request(&manager, "project.external_speakers.set", None, different)
            .await
            .unwrap_err()
            .code,
        "operation_conflict"
    );
    let keep = json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string(),"expected_revision":1,"speakers":{"monitor":{"action":"set","key":{"kind":"keep"}}}});
    request(&manager, "project.external_speakers.set", None, keep)
        .await
        .unwrap();
    let receipt = request(
        &manager,
        "project.external_speakers.operation",
        None,
        json!({"project_id":"project","operation_id":operation}),
    )
    .await
    .unwrap();
    assert_eq!(receipt, saved);
    let unknown = uuid::Uuid::new_v4().to_string();
    assert_eq!(
        request(
            &manager,
            "project.external_speakers.operation",
            None,
            json!({"project_id":"project","operation_id":unknown})
        )
        .await
        .unwrap_err()
        .code,
        "operation_not_found"
    );
    let promote = json!({"operation_id":uuid::Uuid::new_v4().to_string(),"expected_revision":2,"speakers":{"monitor":{"action":"set","key":{"kind":"keep"}}}});
    assert_eq!(
        request(
            &manager,
            "session.external_speakers.set",
            Some(&one),
            promote
        )
        .await
        .unwrap_err()
        .code,
        "invalid_params"
    );
    let disable_operation = uuid::Uuid::new_v4().to_string();
    let disable = json!({"operation_id":disable_operation,"expected_revision":2,"speakers":{"monitor":{"action":"disable"}}});
    let disabled = request(
        &manager,
        "session.external_speakers.set",
        Some(&one),
        disable,
    )
    .await
    .unwrap();
    assert_eq!(disabled["revision"], 3);
    assert_eq!(disabled["overrides"]["monitor"]["action"], "disable");
    assert!(
        manager
            .authenticate_external_speaker("owner", &one, key)
            .await
            .is_err()
    );
    assert!(
        manager
            .authenticate_external_speaker("owner", &two, key)
            .await
            .is_ok()
    );
    assert_eq!(
        request(
            &manager,
            "session.external_speakers.key",
            Some(&one),
            json!({"identity":"monitor"})
        )
        .await
        .unwrap_err()
        .code,
        "not_found"
    );
    let stale_operation = uuid::Uuid::new_v4().to_string();
    let stale = json!({"operation_id":stale_operation,"expected_revision":2,"speakers":{}});
    assert_eq!(
        request(&manager, "session.external_speakers.set", Some(&one), stale)
            .await
            .unwrap_err()
            .code,
        "revision_conflict"
    );
    assert_eq!(
        request(
            &manager,
            "session.external_speakers.operation",
            Some(&one),
            json!({"operation_id":stale_operation})
        )
        .await
        .unwrap_err()
        .code,
        "operation_not_found"
    );
    assert_eq!(
        request(
            &manager,
            "session.external_speakers.operation",
            Some(&two),
            json!({"operation_id":disable_operation})
        )
        .await
        .unwrap_err()
        .code,
        "operation_conflict"
    );
    // Neither management reads/writes nor effective/authentication lookups construct a live runtime.
    assert!(manager.registry.lock().unwrap().is_empty());
    let record = manager.directory.get("owner", &one).await.unwrap().unwrap();
    let prompt = serde_json::to_string(&record.prompt_config).unwrap();
    assert!(!prompt.contains(key));
    manager.close().await.unwrap();
}
async fn inbox_contract(storage: &str) {
    let (manager, one, two) = fixture(storage).await;
    let key = "a".repeat(43);
    request(&manager,"project.external_speakers.set",None,json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string(),"expected_revision":0,"speakers":{"monitor":{"action":"set","key":{"kind":"custom","value":key}}}})).await.unwrap();
    let operation = uuid::Uuid::new_v4().to_string();
    assert_eq!(
        manager
            .external_inbox(
                one.clone(),
                "b".repeat(43),
                operation.clone(),
                Some("hello".into())
            )
            .await
            .unwrap_err()
            .code,
        "unauthenticated"
    );
    assert!(manager.registry.lock().unwrap().is_empty());
    let receipt = manager
        .external_inbox(
            one.clone(),
            key.clone(),
            operation.clone(),
            Some("hello".into()),
        )
        .await
        .unwrap();
    assert_eq!(receipt.operation_id, operation);
    let observed = manager
        .external_inbox(one.clone(), key.clone(), operation.clone(), None)
        .await
        .unwrap();
    assert_eq!(observed.operation_id, operation);
    let managed = manager
        .active(manager.record("owner", &one).await.unwrap(), true)
        .await
        .unwrap();
    commands::close_session(&manager, &managed, "owner", &one, None)
        .await
        .unwrap();
    assert!(manager.registry.lock().unwrap().is_empty());
    let dormant = manager
        .external_inbox(one.clone(), key.clone(), operation.clone(), None)
        .await
        .unwrap();
    assert_eq!(dormant.operation_id, operation);
    assert!(manager.registry.lock().unwrap().is_empty());
    request(&manager,"session.external_speakers.set",Some(&one),json!({"operation_id":uuid::Uuid::new_v4().to_string(),"expected_revision":1,"speakers":{"monitor":{"action":"disable"}}})).await.unwrap();
    assert_eq!(
        manager
            .external_inbox(
                one.clone(),
                key.clone(),
                operation.clone(),
                Some("hello".into())
            )
            .await
            .unwrap_err()
            .code,
        "unauthenticated"
    );
    assert_eq!(
        manager
            .external_inbox(one, key.clone(), operation, None)
            .await
            .unwrap_err()
            .code,
        "unauthenticated"
    );
    let second = manager
        .external_inbox(
            two,
            key,
            uuid::Uuid::new_v4().to_string(),
            Some("shared project".into()),
        )
        .await
        .unwrap();
    assert!(second.ordinal > 0);
    manager.close().await.unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn external_speakers_file_inbox_revocation_and_dormant_receipts() {
    inbox_contract("file").await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn external_speakers_sqlite_inbox_revocation_and_dormant_receipts() {
    inbox_contract("sqlite").await;
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn speaker_file_management_receipts_and_dormant_reads() {
    contract("file").await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn speaker_sqlite_management_receipts_and_dormant_reads() {
    contract("sqlite").await;
}
