//! Runtime migration upgrades paired legacy defaults without rewriting history.
use super::*;

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn context_budget_migration_requires_both_legacy_defaults_and_preserves_node_bytes() {
    const LEGACY: usize = 2 * 1024 * 1024;
    const CURRENT: usize = 64 * 1024 * 1024;
    for (state_budget, context_budget, explicit) in [
        (LEGACY, LEGACY, false),
        (LEGACY, LEGACY, true),
        (3 * LEGACY, 3 * LEGACY, false),
        (LEGACY / 2, LEGACY / 2, false),
        (3 * LEGACY, LEGACY, false),
    ] {
        let model = Arc::new(Model {
            calls: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Semaphore::new(0),
        });
        let (manager, _) = manager(model.clone(), "file").await;
        let config = manager.config.clone();
        let created = commands::create(&manager, "owner", &command("session.create", None,
            serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
        let id = created["id"].as_str().unwrap().to_owned();
        let managed = manager
            .active(manager.record("owner", &id).await.unwrap(), true)
            .await
            .unwrap();
        assert_eq!(managed.session.state().limits.max_context_bytes, CURRENT);
        assert_eq!(
            managed.session.context_snapshot().unwrap().max_bytes,
            CURRENT
        );
        if explicit {
            let saved = manager
                .configuration
                .get("owner", "session_effective", &id)
                .await
                .unwrap()
                .unwrap();
            let mut value = saved.value.unwrap();
            value["limits"]["max_context_bytes"] = serde_json::json!(LEGACY);
            manager
                .configuration
                .compare_exchange(
                    "owner",
                    "session_effective",
                    &id,
                    saved.revision,
                    Some(value),
                )
                .await
                .unwrap();
        }
        let mut historical = managed.session.checkpoint().await.unwrap();
        historical.state.limits.max_context_bytes = state_budget;
        historical.context.max_bytes = context_budget;
        let historical_context = serde_json::to_value(&historical.context).unwrap();
        manager.close().await.unwrap();
        drop(managed);
        drop(manager);
        let directory = config.data_dir.join("sessions").join(&id);
        let (journal, _) = microcrab_storage::CreatorJournal::open(
            directory.to_str().unwrap(),
            id.clone(),
            true,
            true,
        )
        .await
        .unwrap();
        microcrab_session::Journal::commit(&journal, historical)
            .await
            .unwrap();
        let cursor = journal.cursor().await.unwrap();
        microcrab_session::Journal::close(&journal).await.unwrap();
        let node = directory
            .join("journal/nodes")
            .join(format!("{}.json", cursor.checkpoint_id));
        let historical_bytes = std::fs::read(&node).unwrap();
        let manager = Manager::with_client(config, model.clone()).await.unwrap();
        let managed = manager
            .active(manager.record("owner", &id).await.unwrap(), true)
            .await
            .unwrap();
        let migrate = !explicit && state_budget == LEGACY && context_budget == LEGACY;
        assert_eq!(
            managed.session.state().limits.max_context_bytes,
            if migrate { CURRENT } else { state_budget }
        );
        assert_eq!(
            managed.session.context_snapshot().unwrap().max_bytes,
            if migrate { CURRENT } else { context_budget }
        );
        assert_eq!(std::fs::read(&node).unwrap(), historical_bytes);
        let untouched = managed
            .journal
            .prepare_travel(microcrab_storage::Travel::To {
                checkpoint_id: cursor.checkpoint_id,
            })
            .await
            .unwrap();
        assert_eq!(untouched.state.limits.max_context_bytes, state_budget);
        assert_eq!(
            serde_json::to_value(untouched.context).unwrap(),
            historical_context
        );
        managed.journal.cancel_pending().await;
        assert_eq!(model.calls.load(Ordering::SeqCst), 0);
        manager.close().await.unwrap();
        assert_eq!(std::fs::read(&node).unwrap(), historical_bytes);
    }
}
