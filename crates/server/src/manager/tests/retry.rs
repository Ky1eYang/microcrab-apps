//! Explicit retries reuse the owned turn driver and durable operation receipts.
use super::*;
use serde_json::json;

struct RetryModel {
    calls: AtomicUsize,
    seen: std::sync::Mutex<Vec<Value>>,
}
#[async_trait::async_trait]
impl microcrab_session::Client for RetryModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.seen
            .lock()
            .unwrap()
            .push(serde_json::to_value(content).unwrap());
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                return Err(microcrab_transport::Error::Protocol(
                    "response JSON expected colon at line 1 column 8".into(),
                ));
            }
            1 => return Err(microcrab_transport::Error::Cancelled),
            _ => {}
        }
        Ok(Response {
            provider_metadata: None,
            content: "retried answer".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn retry_policy_preserves_provider_timeout_and_historical_limits() {
    for historical_rounds in [8, 5] {
        let model = Arc::new(RetryModel {
            calls: AtomicUsize::new(0),
            seen: std::sync::Mutex::new(vec![]),
        });
        let (seed, _) = manager(model.clone(), "file").await;
        let mut config = seed.config.clone();
        config.provider.timeout_ms = 30_000;
        seed.close().await.unwrap();
        drop(seed);
        let manager = Manager::with_client(config.clone(), model.clone())
            .await
            .unwrap();
        let created = commands::create(&manager, "owner", &command("session.create", None,
            serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
        let id = created["id"].as_str().unwrap().to_owned();
        let managed = manager
            .active(manager.record("owner", &id).await.unwrap(), true)
            .await
            .unwrap();
        assert_eq!(
            managed.session.state().limits.max_rounds,
            microcrab_session::Limits::default().max_rounds
        );
        assert_eq!(manager.config.provider.timeout_ms, 30_000);
        let mut historical = managed.session.checkpoint().await.unwrap();
        historical.state.limits.max_rounds = historical_rounds;
        let context = serde_json::to_value(&historical.context).unwrap();
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

        let manager = Manager::with_client(config, model.clone()).await.unwrap();
        let managed = manager
            .active(manager.record("owner", &id).await.unwrap(), true)
            .await
            .unwrap();
        assert_eq!(managed.session.state().limits.max_rounds, historical_rounds);
        assert_eq!(manager.config.provider.timeout_ms, 30_000);
        assert_eq!(
            serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap(),
            context
        );
        let untouched = managed
            .journal
            .prepare_travel(microcrab_storage::Travel::To {
                checkpoint_id: cursor.checkpoint_id,
            })
            .await
            .unwrap();
        assert_eq!(untouched.state.limits.max_rounds, historical_rounds);
        assert_eq!(serde_json::to_value(untouched.context).unwrap(), context);
        managed.journal.cancel_pending().await;
        assert_eq!(model.calls.load(Ordering::SeqCst), 0);
        manager.close().await.unwrap();
    }
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn retry_receipts_restore_reject_stale_actions_and_never_duplicate_input() {
    let model = Arc::new(RetryModel {
        calls: AtomicUsize::new(0),
        seen: std::sync::Mutex::new(vec![]),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let failure = uuid::Uuid::new_v4().to_string();
    commands::submit(&manager, managed.clone(), &command("turn.submit", Some(&id),
        serde_json::json!({"operation_id":failure,"input":microcrab_session::SessionInput::text("original")}))).await.unwrap();
    managed.worker.lock().await.take().unwrap().await.unwrap();
    let snapshot = managed.hub.snapshot();
    assert_eq!(snapshot["session"]["retry_ready"], true);
    assert_eq!(snapshot["session"]["retry_operation_id"], failure);
    let receipt = manager
        .directory
        .get_operation("owner", &failure)
        .await
        .unwrap()
        .unwrap();
    let result = receipt.result.unwrap();
    assert_eq!(result["position"]["round"], 1);
    assert_eq!(result["after_message_id"], snapshot["messages"][0]["id"]);
    let config = manager.config.clone();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);

    let manager = Manager::with_client(config, model.clone()).await.unwrap();
    let record = manager.record("owner", &id).await.unwrap();
    let offline = manager.inspect(&record).await.unwrap();
    assert_eq!(offline["session"]["retry_operation_id"], failure);
    assert_eq!(
        offline["operations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|operation| operation["id"] == failure)
            .unwrap()["result"]["after_message_id"],
        result["after_message_id"]
    );
    let managed = manager.active(record, true).await.unwrap();
    let retry_id = uuid::Uuid::new_v4().to_string();
    let retry = command(
        "turn.retry",
        Some(&id),
        serde_json::json!({"operation_id":retry_id,"failed_operation_id":failure}),
    );
    let accepted = commands::submit(&manager, managed.clone(), &retry)
        .await
        .unwrap();
    assert_eq!(accepted["status"], "accepted");
    assert_eq!(accepted["result"]["retry_of_operation_id"], failure);
    assert_eq!(
        accepted["result"]["retry_after_message_id"],
        result["after_message_id"]
    );
    managed.worker.lock().await.take().unwrap().await.unwrap();
    let replay = commands::submit(&manager, managed.clone(), &retry)
        .await
        .unwrap();
    assert_eq!(replay["status"], "cancelled");
    assert_eq!(replay["result"]["retry_of_operation_id"], failure);
    assert_eq!(
        replay["result"]["retry_after_message_id"],
        result["after_message_id"]
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        managed.hub.snapshot()["session"]["retry_operation_id"],
        retry_id
    );
    let stale = command(
        "turn.retry",
        Some(&id),
        serde_json::json!({"operation_id":uuid::Uuid::new_v4().to_string(),"failed_operation_id":failure}),
    );
    assert_eq!(
        commands::submit(&manager, managed.clone(), &stale)
            .await
            .unwrap_err()
            .code,
        "retry_unavailable"
    );
    let final_id = uuid::Uuid::new_v4().to_string();
    let final_retry = command(
        "turn.retry",
        Some(&id),
        serde_json::json!({"operation_id":final_id,"failed_operation_id":retry_id}),
    );
    commands::submit(&manager, managed.clone(), &final_retry)
        .await
        .unwrap();
    managed.worker.lock().await.take().unwrap().await.unwrap();
    assert_eq!(managed.hub.snapshot()["session"]["retry_ready"], false);
    assert_eq!(commands::submit(&manager, managed.clone(), &command("turn.retry", Some(&id),
        serde_json::json!({"operation_id":uuid::Uuid::new_v4().to_string(), "failed_operation_id":retry_id})))
        .await.unwrap_err().code, "retry_unavailable");
    assert_eq!(
        managed.hub.snapshot()["messages"].as_array().unwrap().len(),
        2
    );
    {
        let requests = model.seen.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0], requests[1]);
        assert_eq!(requests[1], requests[2]);
    }
    let config = manager.config.clone();
    let snapshot = managed.hub.snapshot();
    assert_retry_ancestry(&snapshot, &retry_id, &failure, &result["after_message_id"]);
    assert_retry_ancestry(&snapshot, &final_id, &retry_id, &result["after_message_id"]);
    // An admitted retry left by another process retains its exact ancestry in
    // both lazy receipt reconciliation and full Session activation.
    let template = manager
        .directory
        .get_operation("owner", &retry_id)
        .await
        .unwrap()
        .unwrap();
    let mut pending_id = String::new();
    for reconcile_now in [true, false] {
        let mut pending = template.clone();
        pending.id = uuid::Uuid::new_v4().to_string();
        pending.status = "accepted".into();
        pending.revision = 0;
        pending.result = Some(json!({
            "server_instance":"prior-process",
            "retry_of_operation_id":failure,
            "retry_after_message_id":result["after_message_id"],
        }));
        let pending = manager.directory.create_operation(pending).await.unwrap();
        if reconcile_now {
            let reconciled = commands::reconcile(&manager, pending).await.unwrap();
            assert_eq!(reconciled.status, "interrupted");
            let reconciled = reconciled.result.unwrap();
            assert_eq!(reconciled["retry_of_operation_id"], failure);
            assert_eq!(
                reconciled["retry_after_message_id"],
                result["after_message_id"]
            );
        } else {
            pending_id = pending.id;
        }
    }
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let manager = Manager::with_client(config, model.clone()).await.unwrap();
    let detached = manager
        .inspect(&manager.record("owner", &id).await.unwrap())
        .await
        .unwrap();
    assert_retry_ancestry(&detached, &retry_id, &failure, &result["after_message_id"]);
    assert_retry_ancestry(&detached, &final_id, &retry_id, &result["after_message_id"]);
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let pending = manager
        .directory
        .get_operation("owner", &pending_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.status, "interrupted");
    assert_retry_ancestry(
        &managed.hub.snapshot(),
        &pending_id,
        &failure,
        &result["after_message_id"],
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 3);
    manager.close().await.unwrap();
}

fn assert_retry_ancestry(snapshot: &Value, id: &str, previous: &str, anchor: &Value) {
    let operation = snapshot["operations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|operation| operation["id"] == id)
        .unwrap();
    assert_eq!(operation["result"]["retry_of_operation_id"], previous);
    assert_eq!(operation["result"]["retry_after_message_id"], *anchor);
}
