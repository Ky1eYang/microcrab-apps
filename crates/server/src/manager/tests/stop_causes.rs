//! Actual owned-turn receipts preserve first cancellation cause and are durable.
use super::*;

async fn receipt_case(host_replacement: bool) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let owner = Principal::for_test("owner");
    let created = manager.command(&owner, &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let operation = uuid::Uuid::new_v4().to_string();
    let admitted = commands::submit(&manager, managed.clone(), &command("turn.submit", Some(id),
        serde_json::json!({"operation_id":operation,"input":microcrab_session::SessionInput::text("original task")})))
        .await.unwrap();
    assert_eq!(admitted["status"], "accepted");
    tokio::time::timeout(std::time::Duration::from_secs(3), model.entered.notified())
        .await
        .unwrap();
    if host_replacement {
        manager
            .suspend_project_with_reason("project", microcrab_session::HostDetachCode::Replaced)
            .await;
        // A later user stop must not replace the exact infrastructure cause.
        managed.session.interrupt();
    } else {
        manager
            .command(
                &owner,
                &command("session.interrupt", Some(id), serde_json::json!({})),
            )
            .await
            .unwrap();
    }
    if let Some(worker) = managed.worker.lock().await.take() {
        tokio::time::timeout(std::time::Duration::from_secs(3), worker)
            .await
            .unwrap()
            .unwrap();
    }
    let stored = manager
        .directory
        .get_operation("owner", &operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, "cancelled");
    let result = stored.result.unwrap();
    assert_eq!(result["error"]["code"], "cancelled");
    assert_eq!(
        result["error"]["details"]["cancel_reason"],
        if host_replacement {
            "host_detached"
        } else {
            "user_requested"
        }
    );
    if host_replacement {
        assert_eq!(result["error"]["details"]["host_detach_code"], "replaced");
        assert!(
            result["error"]["message"]
                .as_str()
                .unwrap()
                .contains("replaced")
        );
    } else {
        assert_eq!(result["error"]["message"], "Stopped by user");
    }
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    manager.close().await.unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn autonomous_inbox_receipt_retains_host_lease_reason() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let owner = Principal::for_test("owner");
    let id = manager.command(&owner, &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap()["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let mut events = managed.hub.subscribe().receiver;
    manager.command(&owner, &command("turn.queue", Some(&id),
        serde_json::json!({"operation_id":uuid::Uuid::new_v4().to_string(),"activate_if_idle":true,
            "input":microcrab_session::SessionInput::text("queued task")})))
        .await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), model.entered.notified())
        .await
        .unwrap();
    manager
        .suspend_project_with_reason("project", microcrab_session::HostDetachCode::LeaseExpired)
        .await;
    let receipt = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(receipt) = manager
                .directory
                .list_operations("owner", &id)
                .await
                .unwrap()
                .into_iter()
                .find(|op| op.method == "turn.inbox" && op.status == "cancelled")
            {
                break receipt;
            }
            events.recv().await.unwrap();
        }
    })
    .await
    .unwrap();
    let error = &receipt.result.unwrap()["error"];
    assert_eq!(error["details"]["cancel_reason"], "host_detached");
    assert_eq!(error["details"]["host_detach_code"], "lease_expired");
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn user_stop_receipt_keeps_actual_stream_reason() {
    receipt_case(false).await;
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn host_replacement_receipt_keeps_first_cause_even_after_user_stop() {
    receipt_case(true).await;
}
