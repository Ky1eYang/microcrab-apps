//! Real Manager queue admission for the frontend's frozen plugin inventory input.
//! Inventory is a bounded, non-secret model summary, never execution authority.
use super::*;

fn inventory_input(persistence: &str, plugins: Value) -> microcrab_session::SessionInput {
    serde_json::from_value(json!({
        "parts":[{"type":"text","text":"Inspect the available plugin summaries."}],
        "variables":{"scoped_set":[{
            "name":"plugins", "value":plugins, "visibility":"visible",
            "persistence":persistence, "editors":["client"]
        }]}
    }))
    .unwrap()
}
fn queue(id: &str, input: microcrab_session::SessionInput) -> Command {
    command(
        "turn.queue",
        Some(id),
        json!({
            "operation_id":uuid::Uuid::new_v4().to_string(),
            "activate_if_idle":true, "input":input
        }),
    )
}
async fn setup() -> (Arc<Manager>, Arc<InboxModel>, Arc<Managed>, String) {
    let model = model(false);
    let (manager, _) = manager(model.clone(), "file").await;
    let id = create(&manager).await;
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    (manager, model, managed, id)
}
fn assert_inventory(managed: &Managed, expected: &Value) {
    let inspection = managed.session.state_handle().inspection().unwrap();
    assert_eq!(inspection.values["plugins"], *expected);
    assert_eq!(
        inspection.metadata["plugins"],
        json!({
            "owner":"client", "editors":["client"],
            "visibility":"visible", "persistence":"persisted"
        })
    );
    assert_eq!(managed.session.state_view().unwrap()["plugins"], *expected);
}
async fn finish(model: &InboxModel, managed: &Managed) {
    model.release.add_permits(1);
    published(managed, |snapshot| {
        snapshot["session"]["status"] == "idle"
            && snapshot["inbox"]["active"] == false
            && snapshot["inbox"]["items"]
                .as_array()
                .is_some_and(Vec::is_empty)
    })
    .await;
}

#[tokio::test]
async fn visible_ephemeral_plugin_inventory_rejects_before_queue_or_model_effects() {
    let (manager, model, managed, id) = setup().await;
    let owner = Principal::for_test("owner");
    let before_context = serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap();
    let before_inbox = serde_json::to_value(managed.session.inbox_snapshot()).unwrap();
    let before_state = managed.session.state_handle().inspection().unwrap();
    let before_journal = managed.journal.cursor().await;
    // Empty inventory is also injected by the frontend; it must exercise the
    // same real admission rule rather than bypassing it as a no-op.
    for plugins in [
        json!([]),
        json!([{"id":"summary-only","name":"Summary only"}]),
    ] {
        let request = queue(&id, inventory_input("ephemeral", plugins));
        let receipt = manager.command(&owner, &request).await.unwrap();
        assert_eq!(receipt["status"], "failed");
        assert!(
            receipt["result"]["error"]["message"]
                .as_str()
                .unwrap()
                .contains("model-visible ephemeral variables are not supported")
        );
        assert_eq!(
            manager.command(&owner, &request).await.unwrap(),
            receipt,
            "observing the failed operation cannot replay input admission"
        );
        assert_eq!(
            serde_json::to_value(managed.session.inbox_snapshot()).unwrap(),
            before_inbox
        );
        assert_eq!(
            serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap(),
            before_context
        );
        let after_state = managed.session.state_handle().inspection().unwrap();
        assert_eq!(after_state.witness, before_state.witness);
        assert!(after_state.values.get("plugins").is_none());
        assert_eq!(
            managed
                .session
                .state_handle()
                .inspection()
                .unwrap()
                .metadata,
            before_state.metadata
        );
        assert_eq!(managed.journal.cursor().await, before_journal);
        assert_eq!(model.calls.load(Ordering::SeqCst), 0);
        assert!(model.seen.lock().unwrap().is_empty());
    }
    manager.close().await.unwrap();
}

#[tokio::test]
async fn visible_persisted_empty_plugin_inventory_reaches_model_with_client_metadata() {
    let (manager, model, managed, id) = setup().await;
    let owner = Principal::for_test("owner");
    let request = queue(&id, inventory_input("persisted", json!([])));
    let receipt = manager.command(&owner, &request).await.unwrap();
    assert_eq!(receipt["status"], "completed");
    assert_eq!(receipt["result"]["admitted"], true);
    entered(&model).await;
    assert_inventory(&managed, &json!([]));
    // This is the actual model Client boundary, not an assertion on the submitted DTO.
    assert!(
        model.seen.lock().unwrap()[0]
            .to_string()
            .contains("plugins")
    );
    assert_eq!(manager.command(&owner, &request).await.unwrap(), receipt);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    finish(&model, &managed).await;
    let checkpoint = serde_json::to_value(managed.session.checkpoint().await.unwrap()).unwrap();
    assert_eq!(
        checkpoint["state"]["variable_metadata"]["plugins"],
        json!({
            "owner":"client", "editors":["client"],
            "visibility":"visible", "persistence":"persisted"
        })
    );
    assert!(checkpoint["context"].to_string().contains("plugins"));
    manager.close().await.unwrap();
}

#[tokio::test]
async fn refreshed_plugin_inventory_preserves_admitted_model_input_and_frozen_journal() {
    let (manager, model, managed, id) = setup().await;
    let owner = Principal::for_test("owner");
    let summary = json!([{"id":"summary-fixture", "name":"Summary fixture", "description":"Bounded public summary"}]);
    let mut request = queue(&id, inventory_input("persisted", summary.clone()));
    let admitted_request = request.clone();
    let first_receipt = manager.command(&owner, &request).await.unwrap();
    assert_eq!(first_receipt["status"], "completed");
    // The caller's mutable draft cannot rewrite the admitted SessionInput.
    request.params["input"]["variables"]["scoped_set"][0]["value"] = json!([]);
    entered(&model).await;
    assert_inventory(&managed, &summary);
    let first_model_input = model.seen.lock().unwrap()[0].clone();
    assert!(first_model_input.to_string().contains("summary-fixture"));
    finish(&model, &managed).await;
    let first_context = managed.session.context_snapshot().unwrap();
    let first_context_bytes = serde_json::to_vec(&first_context).unwrap();
    let cursor = managed.journal.cursor().await.unwrap();
    let node = manager
        .config
        .data_dir
        .join("sessions")
        .join(&id)
        .join("journal/nodes")
        .join(format!("{}.json", cursor.checkpoint_id));
    let frozen_journal = std::fs::read(&node).unwrap();
    let clear = queue(&id, inventory_input("persisted", json!([])));
    assert_eq!(
        manager.command(&owner, &clear).await.unwrap()["status"],
        "completed"
    );
    entered(&model).await;
    assert_inventory(&managed, &json!([]));
    assert_eq!(model.seen.lock().unwrap()[0], first_model_input);
    assert_eq!(
        serde_json::to_vec(&first_context).unwrap(),
        first_context_bytes
    );
    assert_eq!(std::fs::read(&node).unwrap(), frozen_journal);
    finish(&model, &managed).await;
    assert_eq!(
        manager.command(&owner, &admitted_request).await.unwrap(),
        first_receipt
    );
    assert_inventory(&managed, &json!([]));
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(std::fs::read(&node).unwrap(), frozen_journal);
    manager.close().await.unwrap();
}
