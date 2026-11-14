//! Failed receipts retain canonical admission scope for chronological UI replay.
use super::*;
use serde_json::json;

struct FailFirstModel(AtomicUsize);
#[async_trait::async_trait]
impl microcrab_session::Client for FailFirstModel {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(microcrab_transport::Error::ProviderStatus { status: 503 });
        }
        Ok(Response {
            provider_metadata: None,
            content: "Recovered answer".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}

async fn failed_receipt_keeps_admitted_position(storage: &str) {
    let (manager, _root) = manager(Arc::new(FailFirstModel(AtomicUsize::new(0))), storage).await;
    let created = commands::create(
        &manager,
        "owner",
        &command(
            "session.create",
            None,
            json!({
                "project_id":"project", "operation_id":uuid::Uuid::new_v4().to_string()
            }),
        ),
    )
    .await
    .unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let failed_id = uuid::Uuid::new_v4().to_string();
    for (operation_id, prompt) in [
        (failed_id.clone(), "Fail this request"),
        (uuid::Uuid::new_v4().to_string(), "Continue after failure"),
    ] {
        commands::submit(
            &manager,
            managed.clone(),
            &command(
                "turn.submit",
                Some(id),
                json!({
                    "operation_id":operation_id,
                    "input":microcrab_session::SessionInput::text(prompt)
                }),
            ),
        )
        .await
        .unwrap();
        let worker = managed.worker.lock().await.take().unwrap();
        worker.await.unwrap();
    }
    let receipt = manager
        .directory
        .get_operation("owner", &failed_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.status, "failed");
    let position = &receipt.result.as_ref().unwrap()["position"];
    assert_eq!(position, &json!({"epoch":0,"turn":1,"round":1}));
    let snapshot = managed.hub.snapshot();
    let users: Vec<_> = snapshot["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "user")
        .collect();
    assert_eq!(users.len(), 2);
    assert_eq!(&users[0]["position"], position);
    assert_eq!(users[1]["position"]["turn"], 2);
    assert_eq!(managed.session.state().turn_id, None);
    assert_eq!(managed.session.state().round_id, None);
    // The first receipt is still attributed to the failed input after another
    // invocation has replaced live state and refreshed the durable projection.
    let projected = snapshot["operations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|operation| operation["id"] == failed_id)
        .unwrap();
    assert_eq!(&projected["result"]["position"], position);
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn dropping_manager_releases_display_hub_and_writer_without_explicit_close() {
    let (manager, _root) = manager(Arc::new(FailFirstModel(AtomicUsize::new(0))), "file").await;
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
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let hub = Arc::downgrade(&managed.hub);
    let recorder = Arc::downgrade(&managed.hub.recording().unwrap());
    drop(managed);
    drop(manager);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while hub.upgrade().is_some() || recorder.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(
        "Dropping the manager must release display callback ownership and let its writer drain",
    );
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_failed_receipt_retains_its_turn_after_a_new_turn() {
    failed_receipt_keeps_admitted_position("file").await;
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_failed_receipt_retains_its_turn_after_a_new_turn() {
    failed_receipt_keeps_admitted_position("sqlite").await;
}
