use super::*;
use crate::config::Storage;
use microcrab_storage::ModelSelection;

async fn lifecycle_contract(storage: Storage) {
    let dir = tempfile::tempdir().unwrap();
    let keys = RelayKeys::open_at(storage, dir.path().to_path_buf())
        .await
        .unwrap();
    let created = keys
        .create(
            "owner-a",
            CreateKey {
                name: Some("CLI client".into()),
                enabled: Some(true),
                allowed_providers: Some(vec!["configured".into(), "user:personal".into()]),
                default_model_selection: Some(ModelSelection {
                    provider_id: "user:personal".into(),
                    model: "model-a".into(),
                    variant: None,
                }),
            },
        )
        .await
        .unwrap();
    assert!(created.secret.starts_with(KEY_PREFIX));
    assert_eq!(created.key.revision, 1);
    assert_eq!(created.key.request_count, 0);
    assert_eq!(created.key.last_used_at_ms, None);
    let looked = keys.lookup(&created.secret).await.unwrap().unwrap();
    assert_eq!(looked.owner, "owner-a");
    assert!(keys.lookup("mc_sk_invalid").await.unwrap().is_none());

    keys.record_use("owner-a", &created.key.id, 123_456)
        .await
        .unwrap();
    let listed = keys.list("owner-a").await.unwrap();
    assert_eq!(listed[0].request_count, 1);
    assert_eq!(listed[0].last_used_at_ms, Some(123_456));
    assert_eq!(listed[0].revision, 1);

    let updated = keys
        .update(
            "owner-a",
            &created.key.id,
            UpdateKey {
                expected_revision: 1,
                name: Some("Renamed client".into()),
                enabled: Some(false),
                allowed_providers: Some(None),
                default_model_selection: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.revision, 2);
    assert!(!updated.enabled);
    assert!(keys.lookup(&created.secret).await.unwrap().is_none());

    assert!(matches!(
        keys.revoke("owner-a", &created.key.id, 1).await,
        Err(KeyError::Conflict)
    ));
    keys.revoke("owner-a", &created.key.id, 2).await.unwrap();
    assert!(keys.lookup(&created.secret).await.unwrap().is_none());
    assert!(matches!(
        keys.update(
            "owner-a",
            &created.key.id,
            UpdateKey {
                expected_revision: 3,
                name: Some("After delete".into()),
                enabled: None,
                allowed_providers: None,
                default_model_selection: None,
            }
        )
        .await,
        Err(KeyError::NotFound)
    ));
    keys.close().await.unwrap();

    let reopened = RelayKeys::open_at(storage, dir.path().to_path_buf())
        .await
        .unwrap();
    assert!(reopened.list("owner-a").await.unwrap().is_empty());
    assert!(reopened.list("owner-b").await.unwrap().is_empty());
    reopened.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_relay_keys_preserve_cas_stats_and_owner_isolation() {
    lifecycle_contract(Storage::File).await;
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_relay_keys_preserve_cas_stats_and_owner_isolation() {
    lifecycle_contract(Storage::Sqlite).await;
}
