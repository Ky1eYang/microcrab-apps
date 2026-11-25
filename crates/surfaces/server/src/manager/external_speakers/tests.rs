use super::{model::*, store};
use serde_json::{Value, json};

fn key(byte: char) -> String {
    std::iter::repeat_n(byte, 43).collect()
}
fn layer(value: Value, session: bool, previous: Option<&Layer>) -> Layer {
    Layer::from_changes(serde_json::from_value(value).unwrap(), session, previous).unwrap()
}
fn configuration() -> Configuration {
    let mut config = Configuration::default();
    config.projects.insert(
        "project".into(),
        layer(
            json!({"monitor":{"action":"set","key":{"kind":"custom","value":key('a')}}}),
            false,
            None,
        ),
    );
    config.enabled_projects.insert("project".into());
    config
}
#[test]
fn project_shared_keys_authenticate_inheriting_sessions_only() {
    let config = configuration();
    config.validate().unwrap();
    for session in ["one", "two"] {
        assert_eq!(
            config
                .authenticate(Some("project"), session, &key('a'))
                .unwrap()
                .identity,
            "monitor"
        );
    }
    for project in [None, Some("other")] {
        assert_eq!(
            config
                .authenticate(project, "unbound", &key('a'))
                .unwrap_err()
                .code,
            "unauthenticated"
        );
    }
}
#[test]
fn session_override_disable_add_and_keep_are_sparse_and_fail_closed() {
    let mut config = configuration();
    config.sessions.insert(
        "override".into(),
        SessionLayer {
            project_id: Some("project".into()),
            inherited_rebinds: Default::default(),
            speakers: layer(
                json!({
                    "monitor":{"action":"set","key":{"kind":"custom","value":key('b')}},
                    "local":{"action":"set","key":{"kind":"custom","value":key('c')}}
                }),
                true,
                None,
            ),
        },
    );
    config.sessions.insert(
        "disabled".into(),
        SessionLayer {
            project_id: Some("project".into()),
            inherited_rebinds: Default::default(),
            speakers: layer(json!({"monitor":{"action":"disable"}}), true, None),
        },
    );
    config.validate().unwrap();
    assert!(
        config
            .authenticate(Some("project"), "override", &key('a'))
            .is_err()
    );
    assert!(
        config
            .authenticate(Some("project"), "disabled", &key('a'))
            .is_err()
    );
    assert_eq!(
        config
            .authenticate(Some("project"), "override", &key('b'))
            .unwrap()
            .identity,
        "monitor"
    );
    assert!(
        config
            .authenticate(Some("project"), "other", &key('b'))
            .is_err()
    );
    assert!(
        config
            .authenticate(Some("project"), "other", &key('c'))
            .is_err()
    );
    let overrides =
        serde_json::to_value(config.overrides(Some("project"), Some("override"))).unwrap();
    assert_eq!(
        overrides["monitor"],
        json!({"action":"set","key":{"kind":"keep"}})
    );
    let kept = layer(
        overrides,
        true,
        config.layer(Some("project"), Some("override")),
    );
    config.sessions.get_mut("override").unwrap().speakers = kept;
    assert!(
        config
            .authenticate(Some("project"), "override", &key('b'))
            .is_ok()
    );
    let keep_inherited =
        serde_json::from_value(json!({"monitor":{"action":"set","key":{"kind":"keep"}}})).unwrap();
    assert!(Layer::from_changes(keep_inherited, true, None).is_err());
    config.sessions.remove("override");
    assert!(
        config
            .authenticate(Some("project"), "override", &key('a'))
            .is_ok()
    );
    let view =
        serde_json::to_value(config.project(Some("project"), Some("disabled")).unwrap()).unwrap();
    assert_eq!(view[0]["disabled"], true);
    assert!(view[0].get("fingerprint").is_none());
}
#[test]
fn duplicate_key_origins_and_ambiguous_effective_sets_reject_atomically() {
    let mut config = configuration();
    config.sessions.insert(
        "one".into(),
        SessionLayer {
            project_id: Some("project".into()),
            inherited_rebinds: Default::default(),
            speakers: layer(
                json!({"different":{"action":"set","key":{"kind":"custom","value":key('a')}}}),
                true,
                None,
            ),
        },
    );
    assert!(config.effective(Some("project"), Some("one")).is_err());
    assert!(config.validate().is_err());
    let mut config = configuration();
    for session in ["one", "two"] {
        config.sessions.insert(
            session.into(),
            SessionLayer {
                project_id: Some("project".into()),
                inherited_rebinds: Default::default(),
                speakers: layer(
                    json!({"local":{"action":"set","key":{"kind":"custom","value":key('b')}}}),
                    true,
                    None,
                ),
            },
        );
    }
    assert!(config.validate().is_err());
}
#[test]
fn generated_keys_and_custom_format_have_explicit_bounds_and_safe_projection() {
    let config: Vec<_> = (0..2)
        .map(|_| {
            layer(
                json!({"bot":{"action":"set","key":{"kind":"generated"}}}),
                false,
                None,
            )
        })
        .collect();
    let mut generated = Vec::new();
    for layer in config {
        let mut config = Configuration::default();
        config.projects.insert("project".into(), layer);
        config.validate().unwrap();
        let effective = config.effective(Some("project"), None).unwrap();
        let secret = effective["bot"].key.expose();
        use base64::Engine;
        assert_eq!(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(secret)
                .unwrap()
                .len(),
            32
        );
        let projection =
            serde_json::to_string(&config.project(Some("project"), None).unwrap()).unwrap();
        assert!(!projection.contains(secret));
        assert_eq!(effective["bot"].key.fingerprint().len(), 64);
        generated.push(secret.to_owned());
    }
    assert_ne!(generated[0], generated[1]);
    for value in [
        "a".repeat(42),
        "a".repeat(129),
        format!("{} ", "a".repeat(43)),
        "密".repeat(43),
    ] {
        let changes = serde_json::from_value(
            json!({"bot":{"action":"set","key":{"kind":"custom","value":value}}}),
        )
        .unwrap();
        let error = Layer::from_changes(changes, false, None).err().unwrap();
        assert!(!error.message.contains(&value));
    }
    for identity in ["", "9bot", "bot.tag", "bot>bad", &"a".repeat(65)] {
        assert!(validate_identity(identity).is_err());
    }
}
async fn storage_contract(storage: &dyn microcrab_storage::configuration::Configuration) {
    let mut config = configuration();
    let result = super::snapshot(
        &config,
        1,
        Some("project"),
        None,
        Some("saved-operation".into()),
    )
    .unwrap();
    config.operations.insert(
        "saved-operation".into(),
        Operation {
            project_id: Some("project".into()),
            session_id: None,
            request_fingerprint: "f".repeat(64),
            result,
        },
    );
    assert_eq!(store::load(storage, "owner").await.unwrap().0, 0);
    assert_eq!(
        store::save(storage, "owner", 0, &config, "saved-operation")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store::save(storage, "owner", 0, &config, "saved-operation")
            .await
            .unwrap_err()
            .code,
        "revision_conflict"
    );

    let (revision, restored) = store::load(storage, "owner").await.unwrap();
    assert_eq!(revision, 1);
    assert_eq!(restored.operations["saved-operation"].result.revision, 1);
    assert_eq!(
        restored.operations["saved-operation"].result.enabled,
        Some(true)
    );
    assert!(
        !serde_json::to_string(&restored.operations)
            .unwrap()
            .contains(&key('a'))
    );
    assert!(
        restored
            .authenticate(Some("project"), "one", &key('a'))
            .is_ok()
    );
    assert!(
        store::load(storage, "other")
            .await
            .unwrap()
            .1
            .authenticate(Some("project"), "one", &key('a'))
            .is_err()
    );
    let snapshot = config.operations["saved-operation"].result.clone();
    for index in 0..4095 {
        config.operations.insert(
            format!("operation-{index}"),
            Operation {
                project_id: Some("project".into()),
                session_id: None,
                request_fingerprint: "f".repeat(64),
                result: snapshot.clone(),
            },
        );
    }
    assert_eq!(
        store::save(storage, "owner", 1, &config, "capacity-operation")
            .await
            .unwrap_err()
            .code,
        "capacity"
    );
    let (revision, retained) = store::load(storage, "owner").await.unwrap();
    assert_eq!(revision, 1);
    assert_eq!(retained.operations.len(), 1);
    assert!(
        retained
            .authenticate(Some("project"), "one", &key('a'))
            .is_ok()
    );
    storage.close().await.unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_persistence_cas_owner_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let storage = microcrab_storage::configuration::FileConfiguration::open(root.path())
        .await
        .unwrap();
    storage_contract(&storage).await;
    drop(storage);
    let storage = microcrab_storage::configuration::FileConfiguration::open(root.path())
        .await
        .unwrap();
    assert_eq!(store::load(&storage, "owner").await.unwrap().0, 1);
    assert_eq!(
        store::load(&storage, "owner").await.unwrap().1.operations["saved-operation"]
            .result
            .revision,
        1
    );
    assert!(
        store::load(&storage, "owner")
            .await
            .unwrap()
            .1
            .authenticate(Some("project"), "after-restart", &key('a'))
            .is_ok()
    );
    microcrab_storage::configuration::Configuration::close(&storage)
        .await
        .unwrap();
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_persistence_cas_owner_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("configuration.sqlite");
    let storage = microcrab_storage::configuration::SqliteConfiguration::open(&path)
        .await
        .unwrap();
    storage_contract(&storage).await;
    drop(storage);
    let storage = microcrab_storage::configuration::SqliteConfiguration::open(&path)
        .await
        .unwrap();
    assert_eq!(store::load(&storage, "owner").await.unwrap().0, 1);
    assert_eq!(
        store::load(&storage, "owner").await.unwrap().1.operations["saved-operation"]
            .result
            .revision,
        1
    );
    assert!(
        store::load(&storage, "owner")
            .await
            .unwrap()
            .1
            .authenticate(Some("project"), "after-restart", &key('a'))
            .is_ok()
    );
    microcrab_storage::configuration::Configuration::close(&storage)
        .await
        .unwrap();
}
