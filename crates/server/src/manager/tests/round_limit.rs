//! Actual queued rounds settle once, retain exact anchors and survive reload/travel.
use super::*;
use microcrab_session::Journal;
use serde_json::json;
use std::time::Duration;

struct RoundModel(AtomicUsize);
#[async_trait::async_trait]
impl microcrab_session::Client for RoundModel {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let call = self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Response {
            provider_metadata: None,
            content: format!("round answer {call}"),
            tool_calls: if call == 0 {
                vec![microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: "round-limit-sleep".into(),
                    name: "sleep".into(),
                    arguments: json!({"time_ms":1}),
                }]
            } else {
                vec![]
            },
            usage: Usage::default(),
        })
    }
}

async fn create(manager: &Manager) -> String {
    commands::create(
        manager,
        "owner",
        &command(
            "session.create",
            None,
            json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}),
        ),
    )
    .await
    .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned()
}
async fn cap(managed: &Managed, rounds: usize) {
    let mut recover = managed.session.checkpoint().await.unwrap();
    recover.state.limits.max_rounds = rounds;
    managed.journal.commit(recover).await.unwrap();
    let cursor = managed.journal.cursor().await.unwrap();
    let recover = managed
        .journal
        .prepare_travel(microcrab_storage::Travel::To {
            checkpoint_id: cursor.checkpoint_id,
        })
        .await
        .unwrap();
    managed.session.recover(recover).await.unwrap();
}
async fn event(subscription: &mut super::super::hub::Subscription, name: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = subscription.receiver.recv().await.unwrap();
            if serde_json::to_value(&event.event)
                .ok()
                .and_then(|value| {
                    value
                        .get("event")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .as_deref()
                == Some(name)
            {
                return event.event.payload().clone();
            }
        }
    })
    .await
    .expect("round-limit integration event")
}
async fn quiet(managed: &Managed) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match managed.session.checkpoint().await {
                Ok(_) => return,
                Err(Error::Busy) => tokio::task::yield_now().await,
                Err(error) => panic!("checkpoint: {error}"),
            }
        }
    })
    .await
    .unwrap();
}
fn stops(snapshot: &Value) -> Vec<&Value> {
    snapshot["operations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|operation| {
            operation["method"] == "turn.inbox"
                && operation["result"]["stop_reason"] == "round_limit"
        })
        .collect()
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn queued_stop_receipt_is_once_only_anchored_and_survives_reload_and_travel() {
    let model = Arc::new(RoundModel(AtomicUsize::new(0)));
    let (manager, _) = manager(model.clone(), "file").await;
    let id = create(&manager).await;
    let owner = Principal::for_test("owner");
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    cap(&managed, 1).await;
    let policy = managed.session.permission_controller().snapshot().unwrap();
    let mut permissions = policy.config;
    permissions.overrides.push(
        serde_json::from_value(json!({
            "tool":{"kind":"exact","value":"sleep"},
            "args":[],"action":"allow"
        }))
        .unwrap(),
    );
    managed
        .session
        .replace_permissions(policy.revision, permissions)
        .unwrap();
    let bound = managed
        .client_tools
        .bind(
            "round-socket",
            &super::super::client_tools::BindRequest {
                client_id: "round-browser".into(),
                catalog_digest: managed.client_tools.snapshot().await.catalog_digest,
            },
        )
        .await
        .unwrap();
    let mut subscription = managed.hub.subscribe();
    manager
        .command(
            &owner,
            &command(
                "turn.queue",
                Some(&id),
                json!({
                    "operation_id":uuid::Uuid::new_v4().to_string(),
                    "activate_if_idle":true,
                    "input":microcrab_session::SessionInput::text("queued round work")
                }),
            ),
        )
        .await
        .unwrap();
    let requested = event(&mut subscription, "round_limit.requested").await;
    let request = &requested["request"];
    assert_eq!(request["kind"], "rounds");
    assert_eq!(request["used"], 1);
    assert_eq!(request["block"], 1);
    assert_eq!(request["session_id"], id);
    manager.round_limit_reply(&owner, "round-socket", &command("round_limit.reply", Some(&id), json!({
        "request_id":request["request_id"], "lease_id":bound.lease_id.unwrap(), "decision":"stop"
    }))).await.unwrap();
    let receipt = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let receipt = event(&mut subscription, "turn").await;
            if receipt["method"] == "turn.inbox"
                && receipt["result"]["stop_reason"] == "round_limit"
            {
                break receipt;
            }
        }
    })
    .await
    .unwrap();
    quiet(&managed).await;
    assert_eq!(model.0.load(Ordering::SeqCst), 1);
    assert_eq!(receipt["status"], "completed");
    assert_eq!(
        receipt["result"]["position"],
        json!({"epoch":request["epoch"],"turn":request["turn_id"],"round":1})
    );
    assert_eq!(receipt["result"]["rounds"], 1);
    let snapshot = manager.refresh(&managed).await.unwrap();
    let anchor = snapshot["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["id"] == receipt["result"]["after_message_id"])
        .expect("stop receipt anchors an actual message");
    assert_eq!(anchor["position"], receipt["result"]["position"]);
    assert_eq!(stops(&snapshot).len(), 1);
    let mut projected_receipt = receipt.clone();
    let public_result = projected_receipt["result"].as_object_mut().unwrap();
    public_result.remove("server_instance");
    public_result.remove("round_limit_stop_sequence");
    assert_eq!(stops(&snapshot), vec![&projected_receipt]);
    let stop = managed
        .session
        .subscribe_activity()
        .borrow()
        .latest_round_limit_stop
        .clone()
        .unwrap();
    assert_eq!(
        receipt["result"]["round_limit_stop_sequence"],
        stop.sequence
    );
    super::super::inbox::settle_round_stop(&manager, &managed)
        .await
        .unwrap();
    assert_eq!(stops(&manager.refresh(&managed).await.unwrap()).len(), 1);

    let submit = command(
        "turn.submit",
        Some(&id),
        json!({
            "operation_id":uuid::Uuid::new_v4().to_string(),
            "input":microcrab_session::SessionInput::text("ordinary next turn")
        }),
    );
    commands::submit(&manager, managed.clone(), &submit)
        .await
        .unwrap();
    managed.worker.lock().await.take().unwrap().await.unwrap();
    assert_eq!(model.0.load(Ordering::SeqCst), 2);
    assert_eq!(
        managed
            .session
            .subscribe_activity()
            .borrow()
            .latest_round_limit_stop
            .as_ref(),
        Some(&stop)
    );
    super::super::inbox::settle_round_stop(&manager, &managed)
        .await
        .unwrap();
    let snapshot = manager.refresh(&managed).await.unwrap();
    assert_eq!(stops(&snapshot), vec![&projected_receipt]);
    let first_user = snapshot["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "user")
        .unwrap()["id"]
        .clone();
    let head = managed.journal.cursor().await.unwrap();
    let undone = commands::execute(&manager, &owner, &command("history.travel", Some(&id), json!({
        "direction":"undo","message_id":first_user,"expected_checkpoint_id":head.checkpoint_id
    }))).await.unwrap();
    assert!(stops(&undone).is_empty());
    assert_eq!(
        managed
            .session
            .subscribe_activity()
            .borrow()
            .latest_round_limit_stop
            .as_ref(),
        Some(&stop)
    );
    super::super::inbox::settle_round_stop(&manager, &managed)
        .await
        .unwrap();
    assert!(stops(&manager.refresh(&managed).await.unwrap()).is_empty());
    let redone = commands::execute(
        &manager,
        &owner,
        &command("history.travel", Some(&id), json!({"direction":"redo"})),
    )
    .await
    .unwrap();
    assert_eq!(stops(&redone), vec![&projected_receipt]);
    let config = manager.config.clone();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let manager = Manager::with_client(config, model.clone()).await.unwrap();
    let restored = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let snapshot = manager.refresh(&restored).await.unwrap();
    assert_eq!(stops(&snapshot), vec![&projected_receipt]);
    assert_eq!(model.0.load(Ordering::SeqCst), 2);
    let stored = manager
        .directory
        .list_operations("owner", &id)
        .await
        .unwrap();
    assert_eq!(
        stored
            .iter()
            .filter(|operation| operation.method == "turn.inbox"
                && operation
                    .result
                    .as_ref()
                    .is_some_and(|result| result["stop_reason"] == "round_limit"))
            .count(),
        1
    );
    let durable = stored
        .iter()
        .find(|operation| operation.id == receipt["id"].as_str().unwrap())
        .unwrap();
    assert_eq!(serde_json::to_value(durable).unwrap(), receipt);
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn recovered_round_defaults_upgrade_only_the_runtime_and_keep_custom_caps() {
    for (original, explicit) in [(16, false), (16, true), (2, false)] {
        let model = Arc::new(RoundModel(AtomicUsize::new(0)));
        let (manager, _) = manager(model.clone(), "file").await;
        let config = manager.config.clone();
        let id = create(&manager).await;
        let managed = manager
            .active(manager.record("owner", &id).await.unwrap(), true)
            .await
            .unwrap();
        if explicit {
            let saved = manager
                .configuration
                .get("owner", "session_effective", &id)
                .await
                .unwrap()
                .unwrap();
            let mut value = saved.value.unwrap();
            value["limits"]["max_rounds"] = serde_json::json!(original);
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
        historical.state.limits.max_rounds = original;
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
        journal.commit(historical).await.unwrap();
        let cursor = journal.cursor().await.unwrap();
        journal.close().await.unwrap();
        let node = directory
            .join("journal/nodes")
            .join(format!("{}.json", cursor.checkpoint_id));
        let original_bytes = std::fs::read(&node).unwrap();
        let manager = Manager::with_client(config, model.clone()).await.unwrap();
        let managed = manager
            .active(manager.record("owner", &id).await.unwrap(), true)
            .await
            .unwrap();
        assert_eq!(
            managed.session.state().limits.max_rounds,
            if original == 16 && !explicit {
                64
            } else {
                original
            }
        );
        assert_eq!(std::fs::read(&node).unwrap(), original_bytes);
        let untouched = managed
            .journal
            .prepare_travel(microcrab_storage::Travel::To {
                checkpoint_id: cursor.checkpoint_id,
            })
            .await
            .unwrap();
        assert_eq!(untouched.state.limits.max_rounds, original);
        managed.journal.cancel_pending().await;
        assert_eq!(model.0.load(Ordering::SeqCst), 0);
        manager.close().await.unwrap();
        assert_eq!(std::fs::read(&node).unwrap(), original_bytes);
    }
}
