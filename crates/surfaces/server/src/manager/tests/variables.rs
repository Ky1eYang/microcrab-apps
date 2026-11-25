//! Manager boundary coverage without a second variable mutation or receipt owner.
use super::*;
use serde_json::json;

fn model() -> Arc<Model> {
    Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    })
}

async fn create(manager: &Manager) -> String {
    let created = commands::create(
        manager,
        "owner",
        &command(
            "session.create",
            None,
            json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}),
        ),
    )
    .await
    .unwrap();
    created["id"].as_str().unwrap().to_owned()
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn active_turn_checked_edits_use_state_queue_and_only_one_stale_witness_commits() {
    let model = model();
    let (manager, _) = manager(model.clone(), "file").await;
    let id = create(&manager).await;
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let turn = command(
        "turn.submit",
        Some(&id),
        json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text("hold active turn")}),
    );
    commands::submit(&manager, managed.clone(), &turn)
        .await
        .unwrap();
    model.entered.notified().await;
    let witness = managed.session.state_handle().inspection().unwrap().witness;
    let first = command(
        "session.variables.patch",
        Some(&id),
        json!({"operation_id":uuid::Uuid::new_v4().to_string(),"expected":witness,"change":{"set":{"first_edit":1}}}),
    );
    let second = command(
        "session.variables.patch",
        Some(&id),
        json!({"operation_id":uuid::Uuid::new_v4().to_string(),"expected":witness,"change":{"set":{"second_edit":2}}}),
    );
    let owner = Principal::for_test("owner");
    let (first_result, second_result) =
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(
                manager.command(&owner, &first),
                manager.command(&owner, &second)
            )
        })
        .await
        .expect("State queue remains available during an active model request");
    assert_ne!(first_result.is_ok(), second_result.is_ok());
    let rejection = first_result
        .as_ref()
        .err()
        .or(second_result.as_ref().err())
        .unwrap();
    assert_eq!(rejection.code, "variable_conflict");
    let values = managed.session.state_handle().inspection().unwrap().values;
    assert_ne!(
        values.get("first_edit").is_some(),
        values.get("second_edit").is_some()
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    model.release.add_permits(1);
    if let Some(driver) = managed.worker.lock().await.take() {
        driver.await.unwrap();
    }
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn checked_receipts_never_store_effective_ephemeral_bodies_and_do_not_replay_after_restart() {
    let (manager, _) = manager(model(), "file").await;
    let id = create(&manager).await;
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    managed.session.state_handle().change(serde_json::from_value(json!({"scoped_set":[
        {"name":"live_secret","value":"first-private","visibility":"invisible","persistence":"ephemeral"}
    ]})).unwrap()).await.unwrap();
    let witness = managed.session.state_handle().inspection().unwrap().witness;
    let patch = command(
        "session.variables.patch",
        Some(&id),
        json!({"operation_id":uuid::Uuid::new_v4().to_string(),"expected":witness,"change":{"set":{"live_secret":"plain-private-body"}}}),
    );
    let owner = Principal::for_test("owner");
    let applied = manager.command(&owner, &patch).await.unwrap();
    assert_eq!(applied["status"], "applied");
    let operation_id = patch.params["operation_id"].as_str().unwrap();
    let operation = manager
        .directory
        .get_operation("owner", operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.payload_hash, commands::hash(&patch).unwrap());
    assert!(
        !serde_json::to_string(&operation)
            .unwrap()
            .contains("plain-private-body")
    );
    assert!(
        !serde_json::to_string(&operation)
            .unwrap()
            .contains("first-private")
    );
    let configuration = manager.config.clone();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let restarted = Manager::with_client(configuration, model()).await.unwrap();
    let replay = restarted.command(&owner, &patch).await.unwrap();
    assert_eq!(replay["status"], "unknown");
    let observation = restarted
        .command(
            &owner,
            &command(
                "session.variables.operation",
                Some(&id),
                json!({"operation_id":operation_id}),
            ),
        )
        .await
        .unwrap();
    assert_eq!(observation["status"], "unknown");
    let open = restarted
        .command(&owner, &command("state.inspection", Some(&id), json!({})))
        .await
        .unwrap();
    assert!(open["values"].get("live_secret").is_none());
    restarted.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn variable_mutations_obey_suspension_and_quarantine_while_inspection_remains_available() {
    let (manager, _) = manager(model(), "file").await;
    let id = create(&manager).await;
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let owner = Principal::for_test("owner");
    managed.suspended.store(true, Ordering::Release);
    let patch = command(
        "session.variables.patch",
        Some(&id),
        json!({"set":{"must_not_write":true}}),
    );
    assert_eq!(
        manager.command(&owner, &patch).await.unwrap_err().code,
        "host_suspended"
    );
    assert!(
        manager
            .command(&owner, &command("state.inspection", Some(&id), json!({})))
            .await
            .is_ok()
    );
    managed.suspended.store(false, Ordering::Release);
    managed.faulted.store(true, Ordering::Release);
    assert_eq!(
        manager.command(&owner, &patch).await.unwrap_err().code,
        "settings_reconciliation_required"
    );
    assert!(
        managed
            .session
            .state_handle()
            .extract("must_not_write")
            .is_none()
    );
    managed.faulted.store(false, Ordering::Release);
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn client_checked_and_legacy_patches_cannot_override_persisted_agent_ownership() {
    let (manager, _) = manager(model(), "file").await;
    let id = create(&manager).await;
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let recover = managed.session.checkpoint().await.unwrap();
    let configuration = manager.config.clone();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    // Publish a new fixture checkpoint, preserving every old immutable node.
    // Trusted recovery supplies governance; no HTTP request may select this role.
    let mut wire = serde_json::to_value(recover).unwrap();
    wire["state"]["variables"]["agent_owned"] = json!(1);
    wire["state"]["variable_metadata"]["agent_owned"] = json!({
        "visibility":"visible", "persistence":"persisted", "owner":"agent", "editors":["agent"]
    });
    let recover: microcrab_session::Recover = serde_json::from_value(wire).unwrap();
    let path = configuration.data_dir.join("sessions").join(&id);
    let (journal, _) = CreatorJournal::open(path.to_str().unwrap(), id.clone(), true, true)
        .await
        .unwrap();
    microcrab_session::Journal::commit(&journal, recover)
        .await
        .unwrap();
    microcrab_session::Journal::close(&journal).await.unwrap();
    let manager = Manager::with_client(configuration, model()).await.unwrap();
    let owner = Principal::for_test("owner");
    let inspection = manager
        .command(&owner, &command("state.inspection", Some(&id), json!({})))
        .await
        .unwrap();
    assert_eq!(inspection["values"]["agent_owned"], 1);
    assert_eq!(inspection["descriptors"]["agent_owned"]["editable"], false);
    for change in [
        json!({"set":{"agent_owned":2,"atomic_sibling":true}}),
        json!({"remove":["agent_owned"]}),
        json!({"scoped_set":[{"name":"agent_owned","value":2,"visibility":"visible","persistence":"persisted","editors":["client"]}]}),
    ] {
        let operation = uuid::Uuid::new_v4().to_string();
        let checked = command(
            "session.variables.patch",
            Some(&id),
            json!({"operation_id":operation,"expected":inspection["witness"],"change":change}),
        );
        let error = manager.command(&owner, &checked).await.unwrap_err();
        assert_eq!(error.code, "variable_forbidden");
        assert!(error.message.contains("agent_owned"));
        let receipt = manager
            .command(
                &owner,
                &command(
                    "session.variables.operation",
                    Some(&id),
                    json!({"operation_id":operation}),
                ),
            )
            .await
            .unwrap();
        assert_eq!(receipt["status"], "rejected");
        assert_eq!(receipt["reason"]["code"], "variable_forbidden");
        let legacy = command("session.variables.patch", Some(&id), change);
        assert_eq!(
            manager.command(&owner, &legacy).await.unwrap_err().code,
            "variable_forbidden"
        );
        let current = manager
            .command(&owner, &command("state.inspection", Some(&id), json!({})))
            .await
            .unwrap();
        assert_eq!(current["values"]["agent_owned"], 1);
        assert!(current["values"].get("atomic_sibling").is_none());
        assert_eq!(current["witness"], inspection["witness"]);
    }
    manager.close().await.unwrap();
}
