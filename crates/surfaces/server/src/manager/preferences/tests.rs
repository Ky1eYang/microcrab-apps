use super::*;
use serde_json::{Value, json};

fn update(value: Value) -> PreferencesUpdate {
    PreferencesUpdate::parse(&serde_json::to_vec(&value).unwrap()).unwrap()
}

async fn storage_contract(storage: &dyn Configuration) {
    let operation = Uuid::new_v4();
    let mut input = json!({
        "expected_revision": 0,
        "operation_id": operation,
        "dashboard_default_range": "30d",
        "low_cache_hit_warn_pct": 25,
        "relay_default_selection": {"provider_id":"user:personal", "model":"exact"},
        "default_conversation_flavor": "chat_only",
        "theme": "dark",
        "locale": "zh-CN"
    });
    assert_eq!(
        serde_json::to_value(load(storage, "one").await.unwrap()).unwrap(),
        json!({
            "revision":0, "last_operation_id":null, "dashboard_default_range":"7d",
            "low_cache_hit_warn_pct":null, "relay_default_selection":null,
            "default_conversation_flavor":null, "theme":null, "locale":null
        })
    );
    assert!(matches!(
        save(storage, "one", update(input.clone()), &[]).await,
        Err(PreferencesError::Invalid(_))
    ));
    let models = [ModelSelection {
        provider_id: "user:personal".into(),
        model: "exact".into(),
        variant: None,
    }];
    let (a, b) = tokio::join!(
        save(storage, "one", update(input.clone()), &models),
        save(storage, "one", update(input.clone()), &models)
    );
    assert!(matches!(
        (&a, &b),
        (Ok(_), Err(PreferencesError::Conflict)) | (Err(PreferencesError::Conflict), Ok(_))
    ));
    let current = load(storage, "one").await.unwrap();
    assert_eq!(current.revision, 1);
    assert_eq!(current.last_operation_id, Some(operation));
    assert_eq!(current.values.low_cache_hit_warn_pct, Some(25));
    assert_eq!(
        current.values.default_conversation_flavor,
        Some(ConversationFlavor::ChatOnly)
    );
    assert_eq!(current.values.theme, Some(ThemePreference::Dark));
    assert_eq!(current.values.locale, Some(LocalePreference::ZhCn));
    assert_eq!(load(storage, "two").await.unwrap().revision, 0);
    input["expected_revision"] = json!(1);
    input["low_cache_hit_warn_pct"] = json!(100);
    assert!(matches!(
        save(storage, "one", update(input.clone()), &models).await,
        Err(PreferencesError::Invalid(_))
    ));
    input["low_cache_hit_warn_pct"] = Value::Null;
    let retired = save(storage, "one", update(input.clone()), &[])
        .await
        .unwrap();
    assert_eq!(
        retired.values.relay_default_selection,
        Some(models[0].clone())
    );
    input["expected_revision"] = json!(2);
    input["relay_default_selection"] = Value::Null;
    let cleared = save(storage, "one", update(input), &[]).await.unwrap();
    assert_eq!(cleared.revision, 3);
    assert!(cleared.values.relay_default_selection.is_none());
}

#[test]
fn wire_validates_synced_fields_and_rejects_unknown_keys() {
    let valid = json!({
        "expected_revision":0,
        "dashboard_default_range":"7d",
        "low_cache_hit_warn_pct":null,
        "relay_default_selection":null,
        "default_conversation_flavor":"browser_sandbox",
        "theme":"system",
        "locale":"en"
    });
    assert!(PreferencesUpdate::parse(&serde_json::to_vec(&valid).unwrap()).is_ok());

    let mut bad_theme = valid.clone();
    bad_theme["theme"] = json!("neon");
    assert!(PreferencesUpdate::parse(&serde_json::to_vec(&bad_theme).unwrap()).is_err());

    let mut bad_extra = valid.clone();
    bad_extra["extra_field"] = json!(1);
    assert!(PreferencesUpdate::parse(&serde_json::to_vec(&bad_extra).unwrap()).is_err());
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_preferences_owner_cas_survives_restart() {
    use microcrab_storage::configuration::FileConfiguration;
    let root = tempfile::tempdir().unwrap();
    let storage = FileConfiguration::open(root.path()).await.unwrap();
    storage_contract(&storage).await;
    storage.close().await.unwrap();
    let reopened = FileConfiguration::open(root.path()).await.unwrap();
    assert_eq!(load(&reopened, "one").await.unwrap().revision, 3);
    reopened.close().await.unwrap();
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_preferences_owner_cas_survives_restart() {
    use microcrab_storage::configuration::SqliteConfiguration;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("preferences.sqlite");
    let storage = SqliteConfiguration::open(&path).await.unwrap();
    storage_contract(&storage).await;
    storage.close().await.unwrap();
    let reopened = SqliteConfiguration::open(&path).await.unwrap();
    assert_eq!(load(&reopened, "one").await.unwrap().revision, 3);
    reopened.close().await.unwrap();
}
