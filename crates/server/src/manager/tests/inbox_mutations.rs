//! Authenticated pending-input mutation receipts exercise the actual Session owner.

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn paused_input_resume_is_witnessed_and_duplicate_receipts_never_reinsert() {
    let model = controlled();
    let (manager, _) = manager(model.clone(), "file").await;
    let id = allocate(&manager).await;
    let owner = Principal::for_test("owner");
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    managed.session.interrupt();
    pending(&manager, &id, SessionInput::text("retained original input")).await;
    let inbox = managed.session.inbox_snapshot();
    assert!(inbox.paused);
    let mut stale = mutation("inbox.resume", &id, &inbox, &inbox.items[0], json!({}));
    stale.params["expected_revision"] = json!(inbox.items[0].revision + 1);
    let rejected = manager.command(&owner, &stale).await.unwrap();
    assert_eq!(rejected["status"], "failed");
    assert_eq!(
        rejected["result"]["error"]["code"],
        "inbox_revision_conflict"
    );
    assert_eq!(managed.hub.snapshot()["session"]["status"], "idle");
    assert_eq!(manager.command(&owner, &stale).await.unwrap(), rejected);
    assert_eq!(
        serde_json::to_value(managed.session.inbox_snapshot()).unwrap(),
        serde_json::to_value(&inbox).unwrap()
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    let resume = mutation("inbox.resume", &id, &inbox, &inbox.items[0], json!({}));
    assert_eq!(
        manager
            .command(&Principal::for_test("other"), &resume)
            .await
            .unwrap_err()
            .code,
        "not_found"
    );
    let receipt = manager.command(&owner, &resume).await.unwrap();
    assert_eq!(receipt["status"], "accepted");
    tokio::time::timeout(std::time::Duration::from_secs(5), model.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        manager.command(&owner, &resume).await.unwrap()["id"],
        receipt["id"]
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    model.release.add_permits(1);
    if let Some(worker) = managed.worker.lock().await.take() {
        worker.await.unwrap();
    }
    let terminal = manager
        .command(
            &owner,
            &command(
                "turn.get",
                Some(&id),
                json!({"operation_id":resume.params["operation_id"]}),
            ),
        )
        .await
        .unwrap();
    assert_eq!(terminal["status"], "completed");
    assert_eq!(manager.command(&owner, &resume).await.unwrap(), terminal);
    assert!(managed.session.inbox_snapshot().items.is_empty());
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    manager.close().await.unwrap();
}
use super::*;
use microcrab_protocol::{ImageSource, InputPart, SessionInput};
use serde_json::json;

fn controlled() -> Arc<Model> {
    Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    })
}
async fn allocate(manager: &Manager) -> String {
    manager
        .command(
            &Principal::for_test("owner"),
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
fn mutation(
    method: &str,
    id: &str,
    inbox: &microcrab_session::InboxSnapshot,
    item: &microcrab_session::PendingInput,
    extra: Value,
) -> Command {
    let mut params = json!({"operation_id":uuid::Uuid::new_v4().to_string(),
        "expected_inbox_id":inbox.inbox_id,"item_id":item.id,"expected_revision":item.revision});
    params
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    command(method, Some(id), params)
}
async fn pending(manager: &Manager, id: &str, input: SessionInput) {
    let receipt = manager
        .command(
            &Principal::for_test("owner"),
            &command(
                "turn.steer",
                Some(id),
                json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":input}),
            ),
        )
        .await
        .unwrap();
    assert_eq!(receipt["status"], "completed");
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn mutation_receipts_preserve_multipart_and_compare_revisions_without_replay() {
    let model = controlled();
    let (manager, _) = manager(model.clone(), "file").await;
    let id = allocate(&manager).await;
    let owner = Principal::for_test("owner");
    pending(&manager, &id, SessionInput::text("original")).await;
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let before = managed.session.inbox_snapshot();
    let item = &before.items[0];
    let replacement = SessionInput {
        parts: vec![
            InputPart::Text {
                text: "revised".into(),
            },
            InputPart::Image {
                source: ImageSource::Data {
                    media_type: "image/png".into(),
                    data: {
                        use base64::Engine as _;
                        base64::engine::general_purpose::STANDARD
                            .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j7S8AAAAASUVORK5CYII=")
                            .unwrap()
                    },
                },
            },
        ],
        variables: Default::default(),
    };
    let update = mutation(
        "inbox.update",
        &id,
        &before,
        item,
        json!({"input":replacement,"mode":"steer"}),
    );
    let receipt = manager.command(&owner, &update).await.unwrap();
    assert_eq!(receipt["status"], "completed");
    assert_eq!(receipt["result"]["item"]["revision"], 1);
    assert_eq!(receipt["result"]["item"]["mode"], "queue");
    assert_eq!(
        receipt["result"]["item"]["input"],
        serde_json::to_value(&replacement).unwrap()
    );
    assert_eq!(manager.command(&owner, &update).await.unwrap(), receipt);
    assert_eq!(managed.session.inbox_snapshot().items[0].revision, 1);
    let mut conflict = update.clone();
    conflict.params["mode"] = json!("queue");
    assert_eq!(
        manager.command(&owner, &conflict).await.unwrap_err().code,
        "operation_conflict"
    );
    let stale = mutation("inbox.remove", &id, &before, item, json!({}));
    let rejected = manager.command(&owner, &stale).await.unwrap();
    assert_eq!(rejected["status"], "failed");
    assert_eq!(
        rejected["result"]["error"]["code"],
        "inbox_revision_conflict"
    );
    let current = managed.session.inbox_snapshot();
    let remove = mutation("inbox.remove", &id, &current, &current.items[0], json!({}));
    assert_eq!(
        manager
            .command(&Principal::for_test("other"), &remove)
            .await
            .unwrap_err()
            .code,
        "not_found"
    );
    let removed = manager.command(&owner, &remove).await.unwrap();
    assert_eq!(removed["status"], "completed");
    assert_eq!(removed["result"]["removed"], true);
    assert_eq!(
        removed["result"]["item"]["input"],
        serde_json::to_value(&replacement).unwrap()
    );
    assert_eq!(manager.command(&owner, &remove).await.unwrap(), removed);
    assert!(managed.session.inbox_snapshot().items.is_empty());
    assert_eq!(managed.session.inbox_snapshot().used_bytes, 0);
    let missing = mutation("inbox.remove", &id, &current, &current.items[0], json!({}));
    assert_eq!(
        manager.command(&owner, &missing).await.unwrap()["result"]["error"]["code"],
        "inbox_item_not_found"
    );
    let snapshot = manager.refresh(&managed).await.unwrap();
    assert!(
        snapshot["operations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|op| op["method"] == "inbox.update" || op["method"] == "inbox.remove")
            .all(|op| op["result"].get("item").is_none())
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn fresh_stale_incarnation_is_rejected_while_durable_duplicate_keeps_its_result() {
    let model = controlled();
    let (manager, _) = manager(model.clone(), "file").await;
    let id = allocate(&manager).await;
    let owner = Principal::for_test("owner");
    pending(&manager, &id, SessionInput::text("old inbox")).await;
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let before = managed.session.inbox_snapshot();
    let remove = mutation("inbox.remove", &id, &before, &before.items[0], json!({}));
    let original = manager.command(&owner, &remove).await.unwrap();
    manager
        .command(&owner, &command("session.close", Some(&id), json!({})))
        .await
        .unwrap();
    pending(&manager, &id, SessionInput::text("new inbox")).await;
    let restored = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let after = restored.session.inbox_snapshot();
    assert_ne!(before.inbox_id, after.inbox_id);
    assert_eq!(before.items[0].id, after.items[0].id);
    assert_eq!(manager.command(&owner, &remove).await.unwrap(), original);
    let stale = mutation("inbox.remove", &id, &before, &before.items[0], json!({}));
    let rejected = manager.command(&owner, &stale).await.unwrap();
    assert_eq!(rejected["result"]["error"]["code"], "inbox_changed");
    assert_eq!(
        restored.session.inbox_snapshot().items[0].input.parts,
        SessionInput::text("new inbox").parts
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn pending_mutation_does_not_replace_running_turn_ownership_or_feed_old_input() {
    let model = controlled();
    let (manager, _) = manager(model.clone(), "file").await;
    let id = allocate(&manager).await;
    let owner = Principal::for_test("owner");
    manager.command(&owner, &command("turn.submit", Some(&id), json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":SessionInput::text("running")}))).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), model.entered.notified())
        .await
        .unwrap();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let active = managed.hub.operation();
    pending(&manager, &id, SessionInput::text("do not execute")).await;
    let inbox = managed.session.inbox_snapshot();
    assert_eq!(
        inbox.items[0].mode,
        microcrab_session::PendingInputMode::Steer
    );
    let update = mutation(
        "inbox.update",
        &id,
        &inbox,
        &inbox.items[0],
        json!({"mode":"queue"}),
    );
    assert_eq!(
        manager.command(&owner, &update).await.unwrap()["status"],
        "completed"
    );
    assert_eq!(managed.hub.operation(), active);
    let current = managed.session.inbox_snapshot();
    let remove = mutation("inbox.remove", &id, &current, &current.items[0], json!({}));
    assert_eq!(
        manager.command(&owner, &remove).await.unwrap()["status"],
        "completed"
    );
    assert_eq!(managed.hub.operation(), active);
    model.release.add_permits(1);
    if let Some(worker) = managed.worker.lock().await.take() {
        worker.await.unwrap();
    }
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert!(managed.session.inbox_snapshot().items.is_empty());
    assert!(managed.hub.operation().is_none());
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn delayed_publication_rebinds_current_inbox_and_rechecks_transport_budget() {
    let (manager, _) = manager(controlled(), "file").await;
    let id = allocate(&manager).await;
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let record = managed.record.lock().await.clone();
    let witness = super::super::statistics::PublicationWitness::capture(&managed, &record).unwrap();
    let mut delayed = manager.refresh(&managed).await.unwrap();
    let frame = microcrab_protocol::application::MAX_FRAME_BYTES - 16 * 1024;
    delayed["state"] = json!({"padding":"x".repeat(frame - 128 * 1024)});
    let delayed = view::bound_snapshot(delayed).unwrap();
    assert!(!delayed["state"].is_null());
    managed.session.steer("y".repeat(256 * 1024)).unwrap();
    let current = managed.session.inbox_snapshot();
    let published = witness
        .publish(&managed, delayed, true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(published["inbox"], serde_json::to_value(current).unwrap());
    assert_eq!(published["state_omitted"], true);
    assert!(published["state"].is_null());
    assert!(serde_json::to_vec(&published).unwrap().len() <= frame);
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn invisible_ephemeral_changes_remain_in_pending_input_but_not_durable_mutation_receipts() {
    let (manager, _) = manager(controlled(), "file").await;
    let id = allocate(&manager).await;
    let owner = Principal::for_test("owner");
    let input: SessionInput = serde_json::from_value(json!({
        "parts":[{"type":"text","text":"retained input"}],
        "variables":{"set":{"ordinary":1},"remove":[],"scoped_set":[
            {"name":"runtime_token","value":"ephemeral-fixture-value","visibility":"invisible","persistence":"ephemeral"},
            {"name":"settings","value":{"enabled":true},"visibility":"invisible","persistence":"persisted"}
        ]}
    })).unwrap();
    pending(&manager, &id, input.clone()).await;
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let inbox = managed.session.inbox_snapshot();
    let update = mutation(
        "inbox.update",
        &id,
        &inbox,
        &inbox.items[0],
        json!({"input":input}),
    );
    let receipt = manager.command(&owner, &update).await.unwrap();
    assert_eq!(receipt["status"], "completed");
    assert!(!receipt.to_string().contains("ephemeral-fixture-value"));
    let stored = manager
        .directory
        .get_operation("owner", update.params["operation_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !serde_json::to_string(&stored)
            .unwrap()
            .contains("ephemeral-fixture-value")
    );
    let pending = managed.session.inbox_snapshot();
    assert!(
        serde_json::to_string(&pending.items[0].input)
            .unwrap()
            .contains("ephemeral-fixture-value")
    );
    assert_eq!(
        receipt["result"]["item"]["input"]["variables"]["scoped_set"][0]["name"],
        "settings"
    );
    let removal = mutation("inbox.remove", &id, &pending, &pending.items[0], json!({}));
    let removed = manager.command(&owner, &removal).await.unwrap();
    assert_eq!(removed["status"], "completed");
    assert!(!removed.to_string().contains("ephemeral-fixture-value"));
    assert!(managed.session.inbox_snapshot().items.is_empty());
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn plain_set_retains_effective_ephemeral_privacy_in_updated_and_removed_receipts() {
    let (manager, _) = manager(controlled(), "file").await;
    let id = allocate(&manager).await;
    let owner = Principal::for_test("owner");
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    managed.session.state_handle().change(serde_json::from_value(json!({
        "scoped_set":[{"name":"live_secret","value":"initial-private","visibility":"invisible","persistence":"ephemeral"}]
    })).unwrap()).await.unwrap();
    let input: SessionInput = serde_json::from_value(json!({
        "parts":[{"type":"text","text":"visible retained draft"}],
        "variables":{"set":{"live_secret":"plain-private-update","ordinary":9}}
    }))
    .unwrap();
    pending(&manager, &id, input.clone()).await;
    let inbox = managed.session.inbox_snapshot();
    let update = mutation(
        "inbox.update",
        &id,
        &inbox,
        &inbox.items[0],
        json!({"input":input}),
    );
    let expected_digest = super::super::commands::hash(&update).unwrap();
    let receipt = manager.command(&owner, &update).await.unwrap();
    assert_eq!(receipt["status"], "completed");
    assert!(!receipt.to_string().contains("plain-private-update"));
    assert_eq!(
        receipt["result"]["item"]["input"]["variables"]["set"]["ordinary"],
        9
    );
    assert_eq!(
        receipt["result"]["item"]["input"]["parts"][0]["text"],
        "visible retained draft"
    );
    let stored = manager
        .directory
        .get_operation("owner", update.params["operation_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.payload_hash, expected_digest);
    assert!(
        !serde_json::to_string(&stored)
            .unwrap()
            .contains("plain-private-update")
    );
    let inbox = managed.session.inbox_snapshot();
    assert_eq!(
        inbox.items[0].input.variables.set["live_secret"],
        "plain-private-update"
    );
    let removal = mutation("inbox.remove", &id, &inbox, &inbox.items[0], json!({}));
    let removed = manager.command(&owner, &removal).await.unwrap();
    assert!(!removed.to_string().contains("plain-private-update"));
    assert_eq!(
        removed["result"]["item"]["input"]["variables"]["set"]["ordinary"],
        9
    );
    manager.close().await.unwrap();
}
