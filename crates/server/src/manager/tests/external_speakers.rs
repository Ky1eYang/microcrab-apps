//! Dedicated resource integration: no runtime activation and exact durable mutation receipts.
use super::*;
use microcrab_protocol::application::WireError;
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
async fn credential_incarnation_contract(storage: &str) {
    use super::super::external_speakers::CredentialUse::{Admit, Observe};
    let (manager, one, two) = fixture(storage).await;
    let bearer = "a".repeat(43);
    let wrong = "b".repeat(43);
    let operation = uuid::Uuid::new_v4().to_string();
    let legacy_receipt =
        json!({"operation_id":operation,"revision":1,"speakers":[],"overrides":{}});
    // Fixture from the old schema: a stored key plus a redacted management receipt.
    manager.configuration.compare_exchange("owner", "external_speakers", "resources", 0, Some(json!({
        "projects":{"project":{"monitor":{"action":"set","key":bearer}}},
        "sessions":{},
        "operations":{(operation.clone()):{"project_id":"project","session_id":null,"request_fingerprint":"legacy","result":legacy_receipt}}
    }))).await.unwrap();

    let read_witness = async |id: &str, key: &str, usage| -> Result<_, WireError> {
        let (record, _gate) = manager.lock_external_speaker_session(id).await?;
        let credential = manager
            .external_speaker_credential_locked(&record, key, usage)
            .await?
            .ok_or_else(super::super::external_inbox::denied)?;
        assert_eq!(record.owner, "owner");
        assert_eq!(credential.identity, "monitor");
        assert!(manager.speaker_gate(&record.owner).try_lock().is_err());
        Ok(*credential.witness())
    };
    let stored = async || {
        manager
            .configuration
            .get("owner", "external_speakers", "resources")
            .await
            .unwrap()
            .unwrap()
    };
    for (id, key, usage) in [
        (&one, &bearer, Observe),
        (&one, &wrong, Admit),
        (&"unknown-session".to_owned(), &bearer, Admit),
    ] {
        assert_eq!(
            read_witness(id, key, usage).await.unwrap_err().code,
            "unauthenticated"
        );
    }
    assert!(
        manager
            .authenticate_external_speaker("owner", &one, &bearer)
            .await
            .is_ok()
    );
    assert_eq!(stored().await.revision, 1);

    let initial = read_witness(&one, &bearer, Admit).await.unwrap();
    let migrated = stored().await;
    assert_eq!(migrated.revision, 2);
    let private = migrated.value.unwrap();
    let encoded = private["projects"]["project"]["monitor"]["witness"]
        .as_str()
        .unwrap();
    assert_eq!(encoded.len(), 43);
    assert!(private["operations"][&operation]["result"] == legacy_receipt);
    assert!(read_witness(&two, &bearer, Observe).await.unwrap() == initial);
    assert_eq!(stored().await.revision, 2);

    let edit = async |id: Option<&str>, expected_revision, speakers: Value| {
        let operation_id = uuid::Uuid::new_v4().to_string();
        let mut params = json!({"expected_revision":expected_revision,"operation_id":operation_id,"speakers":speakers});
        let method = if id.is_some() {
            "session.external_speakers.set"
        } else {
            params["project_id"] = json!("project");
            "project.external_speakers.set"
        };
        let result = request(&manager, method, id, params.clone()).await.unwrap();
        assert_eq!(request(&manager, method, id, params).await.unwrap(), result);
        let public = result.to_string();
        assert!(!public.contains(encoded) && !public.contains(&bearer));
        assert!(!public.contains("witness"));
        result["revision"].as_u64().unwrap()
    };
    let keep = json!({"monitor":{"action":"set","key":{"kind":"keep"}}});
    let custom = json!({"monitor":{"action":"set","key":{"kind":"custom","value":bearer}}});
    let revision = edit(None, 2, keep.clone()).await;
    assert!(read_witness(&one, &bearer, Observe).await.unwrap() == initial);
    let revision = edit(
        Some(&one),
        revision,
        json!({"monitor":{"action":"disable"}}),
    )
    .await;
    assert_eq!(
        read_witness(&one, &bearer, Admit).await.unwrap_err().code,
        "unauthenticated"
    );
    let revision = edit(Some(&one), revision, json!({})).await;
    let after_disable = read_witness(&one, &bearer, Observe).await.unwrap();
    assert!(after_disable != initial);
    let revision = edit(None, revision, keep).await;
    assert!(read_witness(&one, &bearer, Observe).await.unwrap() == after_disable);
    let revision = edit(
        Some(&one),
        revision,
        json!({"monitor":{"action":"set","key":{"kind":"custom","value":wrong}}}),
    )
    .await;
    assert_eq!(
        read_witness(&one, &bearer, Observe).await.unwrap_err().code,
        "unauthenticated"
    );
    let local = read_witness(&one, &wrong, Observe).await.unwrap();
    let revision = edit(Some(&one), revision, json!({})).await;
    let after_override = read_witness(&one, &bearer, Observe).await.unwrap();
    assert!(
        after_override != local && after_override != initial && after_override != after_disable
    );
    assert!(read_witness(&two, &bearer, Observe).await.unwrap() == initial);

    let revision = edit(None, revision, custom.clone()).await;
    let replacement = read_witness(&one, &bearer, Observe).await.unwrap();
    assert!(replacement != initial && replacement != after_override);
    assert!(read_witness(&two, &bearer, Observe).await.unwrap() == replacement);
    assert!(
        stored().await.value.unwrap()["sessions"][&one]
            .get("inherited_rebinds")
            .is_none()
    );
    let revision = edit(None, revision, json!({})).await;
    assert_eq!(
        read_witness(&one, &bearer, Observe).await.unwrap_err().code,
        "unauthenticated"
    );
    edit(None, revision, custom).await;
    assert!(read_witness(&one, &bearer, Observe).await.unwrap() != replacement);
    assert!(manager.registry.lock().unwrap().is_empty());
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn external_speakers_file_credential_incarnations() {
    credential_incarnation_contract("file").await;
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn external_speakers_sqlite_credential_incarnations() {
    credential_incarnation_contract("sqlite").await;
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
    use super::super::external_inbox::ExternalInboxReceipt::{Dialogue, Legacy};
    let (manager, one, two) = fixture(storage).await;
    let key = "a".repeat(43);
    // Seed a pre-upgrade key and legacy operation via the old, unbound Core port.
    manager.configuration.compare_exchange("owner", "external_speakers", "resources", 0,
        Some(json!({"projects":{"project":{"monitor":{"action":"set","key":key}}},"sessions":{}})))
        .await.unwrap();
    let legacy_id = uuid::Uuid::new_v4().to_string();
    let managed = manager
        .active(manager.record("owner", &one).await.unwrap(), true)
        .await
        .unwrap();
    let legacy = managed
        .session
        .enqueue_external(legacy_id.clone(), "monitor".into(), "legacy text".into())
        .await
        .unwrap();
    commands::close_session(&manager, &managed, "owner", &one, None)
        .await
        .unwrap();
    drop(managed);
    let read = manager
        .external_inbox(one.clone(), key.clone(), legacy_id.clone(), None)
        .await
        .unwrap();
    assert!(matches!(read, Legacy(row) if row.ordinal == legacy.ordinal));
    assert!(manager.registry.lock().unwrap().is_empty());
    let replay = manager
        .external_inbox(
            one.clone(),
            key.clone(),
            legacy_id.clone(),
            Some("legacy text".into()),
        )
        .await
        .unwrap();
    assert!(matches!(replay, Legacy(row) if row.ordinal == legacy.ordinal));
    assert_eq!(
        manager
            .external_inbox(
                one.clone(),
                key.clone(),
                legacy_id,
                Some("different".into())
            )
            .await
            .unwrap_err()
            .code,
        "operation_conflict"
    );
    assert_eq!(
        manager
            .configuration
            .get("owner", "external_speakers", "resources")
            .await
            .unwrap()
            .unwrap()
            .revision,
        1
    );
    let managed = manager
        .active(manager.record("owner", &one).await.unwrap(), true)
        .await
        .unwrap();
    commands::close_session(&manager, &managed, "owner", &one, None)
        .await
        .unwrap();
    drop(managed);
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
    let Dialogue(receipt) = receipt else {
        panic!("new operation has no dialogue")
    };
    assert_eq!(receipt.receipt.operation_id, operation);
    let replay = manager
        .external_inbox(
            one.clone(),
            key.clone(),
            operation.clone(),
            Some("hello".into()),
        )
        .await
        .unwrap();
    assert!(
        matches!(replay, Dialogue(row) if row.dialogue_id == receipt.dialogue_id && row.receipt.ordinal == receipt.receipt.ordinal)
    );
    let observed = manager
        .external_inbox(one.clone(), key.clone(), operation.clone(), None)
        .await
        .unwrap();
    assert!(
        matches!(observed, Dialogue(row) if row.dialogue_id == receipt.dialogue_id && row.receipt.ordinal == receipt.receipt.ordinal)
    );
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
    assert!(
        matches!(dormant, Dialogue(row) if row.dialogue_id == receipt.dialogue_id && row.receipt.ordinal == receipt.receipt.ordinal)
    );
    assert!(manager.registry.lock().unwrap().is_empty());
    let other_session = manager
        .external_inbox(two.clone(), key.clone(), operation.clone(), None)
        .await
        .unwrap_err();
    assert_eq!(other_session.code, "operation_not_found");
    request(&manager,"project.external_speakers.set",None,json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string(),"expected_revision":2,"speakers":{"monitor":{"action":"set","key":{"kind":"custom","value":key}}}})).await.unwrap();
    assert_eq!(
        manager
            .external_inbox(one.clone(), key.clone(), operation.clone(), None)
            .await
            .unwrap_err()
            .code,
        "operation_not_found"
    );
    assert!(manager.registry.lock().unwrap().is_empty());
    assert!(
        manager
            .external_inbox(
                one.clone(),
                key.clone(),
                operation.clone(),
                Some("hello".into())
            )
            .await
            .is_err()
    );
    request(&manager,"session.external_speakers.set",Some(&one),json!({"operation_id":uuid::Uuid::new_v4().to_string(),"expected_revision":3,"speakers":{"monitor":{"action":"disable"}}})).await.unwrap();
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
    assert!(matches!(second, Dialogue(row) if row.receipt.ordinal > 0));
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
