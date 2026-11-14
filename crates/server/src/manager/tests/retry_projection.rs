//! Bounded snapshots must keep the current retry receipt independently of UUID order.
use super::*;

struct FailedModel(AtomicUsize);
#[async_trait::async_trait]
impl microcrab_session::Client for FailedModel {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(microcrab_transport::Error::Transport)
    }
}

async fn bounded_retry_snapshot(storage: &str) {
    let model = Arc::new(FailedModel(AtomicUsize::new(0)));
    let (manager, _) = manager(model.clone(), storage).await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let failure = "00000000-0000-4000-8000-000000000001";
    commands::submit(&manager, managed.clone(), &command("turn.submit", Some(&id),
        serde_json::json!({"operation_id":failure,"input":microcrab_session::SessionInput::text("retry the current request")}))).await.unwrap();
    managed.worker.lock().await.take().unwrap().await.unwrap();
    let receipt = manager
        .directory
        .get_operation("owner", failure)
        .await
        .unwrap()
        .unwrap();
    let anchor = receipt.result.as_ref().unwrap()["after_message_id"].clone();
    for later in [false, true] {
        // Persist through the actual directory: both file and SQLite list by UUID,
        // not by timestamp. Later metadata commands do not alter prepared Context.
        for index in 0..40_u64 {
            let mut unrelated = receipt.clone();
            unrelated.id = format!(
                "ffffffff-ffff-4fff-8fff-{:012x}",
                index + if later { 100 } else { 0 }
            );
            unrelated.method = "session.rename".into();
            unrelated.status = "completed".into();
            unrelated.revision = 0;
            unrelated.created_at_ms = if later {
                receipt.created_at_ms + index + 1
            } else {
                receipt.created_at_ms - 100 + index
            };
            unrelated.updated_at_ms = unrelated.created_at_ms;
            unrelated.result = None;
            manager.directory.create_operation(unrelated).await.unwrap();
        }
        manager.refresh(&managed).await.unwrap();
        let snapshot = managed.hub.snapshot();
        assert_bounded_retry(&snapshot, failure, &anchor);
        let operations = snapshot["operations"].as_array().unwrap();
        let selected_id = if later {
            &operations[31]["id"]
        } else {
            &operations[0]["id"]
        };
        assert_eq!(selected_id.as_str(), Some(failure));
    }
    let config = manager.config.clone();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let reopened = Manager::with_client(config, model.clone()).await.unwrap();
    let detached = reopened
        .inspect(&reopened.record("owner", &id).await.unwrap())
        .await
        .unwrap();
    assert_bounded_retry(&detached, failure, &anchor);
    assert_eq!(model.0.load(Ordering::SeqCst), 1);
    reopened.close().await.unwrap();
}

fn assert_bounded_retry(snapshot: &Value, failure: &str, anchor: &Value) {
    assert_eq!(snapshot["session"]["retry_ready"], true);
    assert_eq!(snapshot["session"]["retry_operation_id"], failure);
    assert_eq!(snapshot["operations_truncated"], true);
    let operations = snapshot["operations"].as_array().unwrap();
    assert_eq!(operations.len(), 32);
    let selected = operations
        .iter()
        .find(|operation| operation["id"] == failure)
        .unwrap();
    assert_eq!(selected["status"], "failed");
    assert_eq!(selected["result"]["after_message_id"], *anchor);
    assert_eq!(selected["result"]["retry_ready"], true);
    assert!(operations.windows(2).all(|pair| {
        pair[0]["created_at_ms"].as_u64().unwrap() >= pair[1]["created_at_ms"].as_u64().unwrap()
    }));
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_bounded_snapshot_keeps_low_uuid_retry_receipt_live_and_detached() {
    bounded_retry_snapshot("file").await;
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_bounded_snapshot_keeps_low_uuid_retry_receipt_live_and_detached() {
    bounded_retry_snapshot("sqlite").await;
}
