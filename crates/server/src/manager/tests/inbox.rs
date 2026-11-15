//! Actual Session insertion lanes, receipt replay and autonomous failure projection.
#[cfg(feature = "storage-file")]
mod plugin_inventory;
use super::*;
use serde_json::json;

struct InboxModel {
    calls: AtomicUsize,
    entered: Semaphore,
    release: Semaphore,
    seen: std::sync::Mutex<Vec<Value>>,
    fail_first: bool,
    failure_kind: Option<&'static str>,
}
#[async_trait::async_trait]
impl microcrab_session::Client for InboxModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.seen
            .lock()
            .unwrap()
            .push(serde_json::to_value(content).unwrap());
        self.entered.add_permits(1);
        tokio::select! {
            _ = token.cancelled() => return Err(microcrab_transport::Error::Cancelled),
            permit = self.release.acquire() => permit.unwrap().forget(),
        }
        if call == 0
            && let Some(kind) = self.failure_kind
        {
            return Err(match kind {
                "protocol" => microcrab_transport::Error::Protocol(
                    "provider schema mismatch; token=private invalid response".into(),
                ),
                "known_protocol" => microcrab_transport::Error::Protocol(
                    microcrab_transport::ProtocolError::with_private_reason(
                        "invalid tool arguments JSON",
                        "invalid tool arguments JSON for private_call_id".into(),
                    ),
                ),
                "known_provider_protocol" => microcrab_transport::Error::Provider(
                    microcrab_transport::ProviderError::protocol(
                        microcrab_transport::ProtocolError::with_private_reason(
                            "invalid tool arguments JSON",
                            "invalid tool arguments JSON for private_call_id".into(),
                        ),
                    ),
                ),
                "limit" => microcrab_transport::Error::Limit("private limit details".into()),
                "cancelled" => microcrab_transport::Error::Cancelled,
                _ => unreachable!(),
            });
        }
        if call == 0 && self.fail_first {
            return Err(microcrab_transport::Error::Transport);
        }
        Ok(Response {
            provider_metadata: None,
            content: format!("answer {call}"),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}
fn model(fail_first: bool) -> Arc<InboxModel> {
    Arc::new(InboxModel {
        calls: AtomicUsize::new(0),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        seen: std::sync::Mutex::new(vec![]),
        fail_first,
        failure_kind: None,
    })
}
async fn entered(model: &InboxModel) {
    tokio::time::timeout(std::time::Duration::from_secs(5), model.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn autonomous_pre_admission_errors_keep_exact_retry_evidence_without_provider_misclassification()
 {
    for kind in ["protocol", "limit", "cancelled"] {
        // These errors originate in Client.complete before any response/effects.
        // A generic Limit still lacks the typed pre_effect certification.
        let retry_ready = kind != "limit";
        let mut model = model(false);
        Arc::get_mut(&mut model).unwrap().failure_kind = Some(kind);
        let (manager, _) = manager(model.clone(), "file").await;
        let id = create(&manager).await;
        let owner = Principal::for_test("owner");
        manager
            .command(&owner, &insertion("turn.queue", &id, "queued failure"))
            .await
            .unwrap();
        entered(&model).await;
        let managed = manager
            .active(manager.record("owner", &id).await.unwrap(), true)
            .await
            .unwrap();
        model.release.add_permits(1);
        let snapshot = published(&managed, |snapshot| {
            snapshot["operations"].as_array().is_some_and(|operations| {
                operations
                    .iter()
                    .any(|operation| operation["method"] == "turn.inbox")
            })
        })
        .await;
        let failures = snapshot["operations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|operation| operation["method"] == "turn.inbox")
            .collect::<Vec<_>>();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0]["result"]["error"]["code"], kind);
        assert_eq!(failures[0]["result"]["retry_ready"], retry_ready);
        assert_eq!(
            failures[0]["status"],
            if kind == "cancelled" {
                "cancelled"
            } else {
                "failed"
            }
        );
        assert!(!failures[0].to_string().contains("private"));
        assert_eq!(snapshot["session"]["retry_ready"], retry_ready);
        assert_eq!(snapshot["inbox"]["paused"], true);
        assert!(failures[0]["result"]["after_message_id"].is_string());
        if retry_ready {
            assert!(managed.session.has_continuation());
            let failed = failures[0]["id"].as_str().unwrap();
            manager
                .command(
                    &owner,
                    &command(
                        "turn.retry",
                        Some(&id),
                        json!({
                            "operation_id":uuid::Uuid::new_v4().to_string(),
                            "failed_operation_id":failed
                        }),
                    ),
                )
                .await
                .unwrap();
            entered(&model).await;
            {
                let seen = model.seen.lock().unwrap();
                assert_eq!(seen.len(), 2);
                assert_eq!(seen[0], seen[1]);
            }
            model.release.add_permits(1);
            published(&managed, |snapshot| {
                snapshot["session"]["status"] == "idle"
                    && snapshot["session"]["retry_ready"] == false
            })
            .await;
        }
        manager.close().await.unwrap();
    }
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn autonomous_known_protocol_causes_survive_queue_publication_without_private_ids() {
    for kind in ["known_protocol", "known_provider_protocol"] {
        // Both producers fail within Client.complete before response admission;
        // wrapping the same cause as Provider cannot change that phase witness.
        let retry_ready = true;
        let mut model = model(false);
        Arc::get_mut(&mut model).unwrap().failure_kind = Some(kind);
        let (manager, _) = manager(model.clone(), "file").await;
        let id = create(&manager).await;
        let owner = Principal::for_test("owner");
        manager
            .command(
                &owner,
                &insertion("turn.queue", &id, "queued known failure"),
            )
            .await
            .unwrap();
        entered(&model).await;
        let managed = manager
            .active(manager.record("owner", &id).await.unwrap(), true)
            .await
            .unwrap();
        model.release.add_permits(1);
        let snapshot = published(&managed, |snapshot| {
            snapshot["operations"].as_array().is_some_and(|operations| {
                operations
                    .iter()
                    .any(|operation| operation["method"] == "turn.inbox")
            })
        })
        .await;
        let failures = snapshot["operations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|operation| operation["method"] == "turn.inbox")
            .collect::<Vec<_>>();
        assert_eq!(failures.len(), 1, "{kind}");
        let result = &failures[0]["result"];
        assert_eq!(result["error"]["code"], "protocol", "{kind}");
        assert_eq!(
            result["error"]["message"], "invalid tool arguments JSON",
            "{kind}"
        );
        assert_eq!(
            result["error"]["details"]["reason"], "invalid tool arguments JSON",
            "{kind}"
        );
        assert_eq!(result["retry_ready"], retry_ready, "{kind}");
        assert_eq!(snapshot["session"]["retry_ready"], retry_ready, "{kind}");
        assert_eq!(snapshot["inbox"]["paused"], true, "{kind}");
        assert_eq!(failures[0]["status"], "failed");
        assert!(!snapshot.to_string().contains("private_call_id"));
        // Refresh after settlement does not resume or replay the failed queue item.
        manager.refresh(&managed).await.unwrap();
        assert_eq!(model.calls.load(Ordering::SeqCst), 1, "{kind}");
        manager.close().await.unwrap();
    }
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn rollback_before_inbox_failure_observation_does_not_create_ghost_error() {
    let model = model(true);
    let (manager, _) = manager(model.clone(), "file").await;
    let id = create(&manager).await;
    let owner = Principal::for_test("owner");
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    managed.session.checkpoint().await.unwrap();
    manager
        .command(&owner, &insertion("turn.queue", &id, "discarded failure"))
        .await
        .unwrap();
    entered(&model).await;
    // The observer cannot acquire this gate until the history owner has selected
    // the earlier branch. Session still publishes the actual model failure.
    let gate = managed.commands.lock().await;
    let mut activity = managed.session.subscribe_activity();
    model.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let settled = {
                let value = activity.borrow();
                !value.active && value.latest_inbox_failure.is_some()
            };
            if settled {
                break;
            }
            activity.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let (index, input) = managed
        .session
        .messages()
        .into_iter()
        .enumerate()
        .find(|(_, item)| {
            matches!(
                item,
                microcrab_session::context::ContentItem::User { source: None, .. }
            )
        })
        .unwrap();
    let recover = managed
        .journal
        .prepare_undo_input(managed.journal.cursor().await.unwrap(), index, &input)
        .await
        .unwrap();
    // Quiescence publication precedes worker reservation release. Retry only
    // that short Busy handoff; the Journal's prepared selection is unchanged.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match managed.session.recover(recover.clone()).await {
                Ok(()) => break,
                Err(Error::Busy) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected recovery error: {error}"),
            }
        }
    })
    .await
    .unwrap();
    assert!(
        managed
            .session
            .subscribe_activity()
            .borrow()
            .latest_inbox_failure
            .is_some()
    );
    assert!(!managed.session.messages().iter().any(|item| matches!(
        item,
        microcrab_session::context::ContentItem::User { source: None, .. }
    )));
    drop(gate);
    super::super::inbox::settle_failure(&manager, &managed)
        .await
        .unwrap();
    let snapshot = manager.refresh(&managed).await.unwrap();
    assert!(
        !manager
            .directory
            .list_operations("owner", &id)
            .await
            .unwrap()
            .iter()
            .any(|operation| operation.method == "turn.inbox")
    );
    assert!(
        !snapshot["operations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|operation| operation["method"] == "turn.inbox")
    );
    assert_eq!(snapshot["session"]["retry_ready"], false);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    manager.close().await.unwrap();
}

async fn published(managed: &Managed, predicate: impl Fn(&Value) -> bool) -> Value {
    let mut events = managed.hub.subscribe().receiver;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let snapshot = managed.hub.snapshot();
            if predicate(&snapshot) {
                break snapshot;
            }
            events.recv().await.unwrap();
        }
    })
    .await
    .unwrap_or_else(|_| {
        let snapshot = managed.hub.snapshot();
        let positions: Vec<_> = snapshot["messages"].as_array().into_iter().flatten().map(|message| message["position"].clone()).collect();
        let operations: Vec<_> = snapshot["operations"].as_array().into_iter().flatten().map(|operation| json!({"method":operation["method"],"status":operation["status"],"code":operation["result"]["error"]["code"]})).collect();
        let activity = managed.session.subscribe_activity().borrow().clone();
        panic!("publication timeout: active={}, paused={}, failure={:?}, session={}, message_positions={positions:?}, operations={operations:?}", activity.active, activity.paused, activity.latest_inbox_failure, snapshot["session"]);
    })
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
        .into()
}
fn insertion(method: &str, id: &str, text: &str) -> Command {
    command(
        method,
        Some(id),
        json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text(text)}),
    )
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn explicit_idle_queue_activates_fifo_but_receipt_replay_never_reactivates() {
    let model = model(false);
    let (manager, _) = manager(model.clone(), "file").await;
    let id = create(&manager).await;
    let owner = Principal::for_test("owner");
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    managed.session.interrupt();
    manager
        .command(&owner, &insertion("turn.queue", &id, "older pending"))
        .await
        .unwrap();
    let mut send = insertion("turn.queue", &id, "explicit send");
    send.params["activate_if_idle"] = json!(true);
    let receipt = manager.command(&owner, &send).await.unwrap();
    assert_eq!(receipt["status"], "completed");
    entered(&model).await;
    let seen = model.seen.lock().unwrap()[0].to_string();
    assert!(seen.contains("older pending"));
    assert!(!seen.contains("explicit send"));
    assert!(!managed.session.inbox_snapshot().paused);
    let active_operation = managed.hub.operation();
    let mut busy = insertion("turn.queue", &id, "explicit busy");
    busy.params["activate_if_idle"] = json!(true);
    assert_eq!(
        manager.command(&owner, &busy).await.unwrap()["status"],
        "completed"
    );
    assert_eq!(managed.hub.operation(), active_operation);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(managed.session.inbox_snapshot().items.len(), 2);
    manager
        .command(&owner, &command("session.interrupt", Some(&id), json!({})))
        .await
        .unwrap();
    published(&managed, |snapshot| {
        snapshot["inbox"]["active"] == false && snapshot["inbox"]["paused"] == true
    })
    .await;
    let before = serde_json::to_value(managed.session.inbox_snapshot()).unwrap();
    assert_eq!(manager.command(&owner, &send).await.unwrap(), receipt);
    assert_eq!(
        serde_json::to_value(managed.session.inbox_snapshot()).unwrap(),
        before
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    let mut changed = send.clone();
    changed.params["activate_if_idle"] = json!(false);
    assert_eq!(
        manager.command(&owner, &changed).await.unwrap_err().code,
        "operation_conflict"
    );
    let mut invalid_steer = insertion("turn.steer", &id, "must not admit");
    invalid_steer.params["activate_if_idle"] = json!(true);
    assert_eq!(
        manager
            .command(&owner, &invalid_steer)
            .await
            .unwrap_err()
            .code,
        "invalid_request"
    );
    assert_eq!(
        serde_json::to_value(managed.session.inbox_snapshot()).unwrap(),
        before
    );
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn busy_queue_and_steer_use_actual_lanes_and_replays_never_duplicate() {
    let model = model(false);
    let (manager, _) = manager(model.clone(), "file").await;
    let id = create(&manager).await;
    let owner = Principal::for_test("owner");
    let first = insertion("turn.submit", &id, "first");
    manager.command(&owner, &first).await.unwrap();
    entered(&model).await;
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let active_operation = managed.hub.operation();
    let queue = insertion("turn.queue", &id, "later");
    let steer = insertion("turn.steer", &id, "change direction");
    let queued = manager.command(&owner, &queue).await.unwrap();
    assert_eq!(queued["status"], "completed");
    assert_eq!(manager.command(&owner, &queue).await.unwrap(), queued);
    manager.command(&owner, &steer).await.unwrap();
    assert_eq!(managed.hub.operation(), active_operation);
    // Admission receipts finish before observational snapshot work. Both entries
    // already belong to Session; wait for that exact read model to be published.
    assert_eq!(managed.session.inbox_snapshot().items.len(), 2);
    let snapshot = published(&managed, |snapshot| {
        snapshot["inbox"]["items"]
            .as_array()
            .is_some_and(|items| items.len() == 2)
    })
    .await;
    assert_eq!(snapshot["session"]["status"], "running");
    assert_eq!(snapshot["inbox"]["items"].as_array().unwrap().len(), 2);
    assert_eq!(
        snapshot["inbox"]["items"][0]["submission_operation_id"],
        queue.params["operation_id"]
    );
    assert_eq!(
        snapshot["inbox"]["items"][1]["submission_operation_id"],
        steer.params["operation_id"]
    );
    assert_eq!(snapshot["inbox"]["items"][0]["mode"], "queue");
    assert_eq!(snapshot["inbox"]["items"][1]["mode"], "steer");
    assert_ne!(
        snapshot["inbox"]["items"][0]["id"],
        snapshot["inbox"]["items"][1]["id"]
    );
    assert_eq!(
        manager
            .command(&Principal::for_test("intruder"), &queue)
            .await
            .unwrap_err()
            .code,
        "not_found"
    );
    let mut conflict = queue.clone();
    conflict.params["input"] =
        serde_json::to_value(microcrab_session::SessionInput::text("different")).unwrap();
    assert_eq!(
        manager.command(&owner, &conflict).await.unwrap_err().code,
        "operation_conflict"
    );
    model.release.add_permits(1);
    entered(&model).await;
    {
        let seen = model.seen.lock().unwrap();
        let items: Vec<microcrab_transport::ContentItem> =
            serde_json::from_value(seen[1]["items"].clone()).unwrap();
        let inputs: Vec<_> = items
            .into_iter()
            .filter_map(|item| match item {
                microcrab_transport::ContentItem::User {
                    parts,
                    source: None,
                } => Some(parts),
                _ => None,
            })
            .collect();
        assert_eq!(
            inputs,
            vec![
                microcrab_session::SessionInput::text("first").parts,
                microcrab_session::SessionInput::text("change direction").parts,
            ]
        );
    }
    manager.refresh(&managed).await.unwrap();
    let admitted = published(&managed, |snapshot| {
        snapshot["messages"].as_array().is_some_and(|messages| {
            messages
                .iter()
                .any(|message| message["submission_operation_id"] == steer.params["operation_id"])
        })
    })
    .await;
    let users: Vec<_> = admitted["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "user")
        .collect();
    assert_eq!(users.len(), 2);
    assert_eq!(
        users[0]["submission_operation_id"],
        first.params["operation_id"]
    );
    assert_eq!(
        users[1]["submission_operation_id"],
        steer.params["operation_id"]
    );
    assert!(
        users
            .iter()
            .all(|message| message["submission_operation_id"] != queue.params["operation_id"])
    );
    // Delay server receipt settlement after Session finishes the steered turn.
    // The public stream's retained reservation must keep the autonomous queue
    // from changing the completed turn's Context/witness during that gap.
    let settlement_gate = managed.commands.lock().await;
    let mut activity = managed.session.subscribe_activity();
    model.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let active = activity.borrow().active;
            if !active {
                break;
            }
            activity.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(managed.session.pending_inputs().len(), 1);
    drop(settlement_gate);
    entered(&model).await;
    {
        let seen = model.seen.lock().unwrap();
        let items: Vec<microcrab_transport::ContentItem> =
            serde_json::from_value(seen[2]["items"].clone()).unwrap();
        let inputs: Vec<_> = items
            .into_iter()
            .filter_map(|item| match item {
                microcrab_transport::ContentItem::User {
                    parts,
                    source: None,
                } => Some(parts),
                _ => None,
            })
            .collect();
        assert_eq!(
            inputs,
            vec![
                microcrab_session::SessionInput::text("first").parts,
                microcrab_session::SessionInput::text("change direction").parts,
                microcrab_session::SessionInput::text("later").parts,
            ]
        );
    }
    manager.refresh(&managed).await.unwrap();
    let admitted = published(&managed, |snapshot| {
        snapshot["messages"].as_array().is_some_and(|messages| {
            messages
                .iter()
                .any(|message| message["submission_operation_id"] == queue.params["operation_id"])
        })
    })
    .await;
    assert_eq!(
        admitted["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|message| message["submission_operation_id"] == queue.params["operation_id"])
            .count(),
        1
    );
    assert!(managed.session.inbox_snapshot().active);
    assert_eq!(
        manager
            .command(
                &owner,
                &command("history.travel", Some(&id), json!({"direction":"undo"}))
            )
            .await
            .unwrap_err()
            .code,
        "busy"
    );
    model.release.add_permits(1);
    let final_snapshot = published(&managed, |snapshot| {
        snapshot["session"]["status"] == "idle"
            && snapshot["inbox"]["items"]
                .as_array()
                .is_some_and(Vec::is_empty)
    })
    .await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        final_snapshot["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|message| message["role"] == "user")
            .count(),
        3
    );
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn autonomous_queue_failure_is_retryable_and_paused_input_stays_authoritative() {
    for resume in [false, true] {
        let model = model(true);
        let (manager, _) = manager(model.clone(), "file").await;
        let id = create(&manager).await;
        let owner = Principal::for_test("owner");
        let managed = manager
            .active(manager.record("owner", &id).await.unwrap(), true)
            .await
            .unwrap();
        if resume {
            managed.session.interrupt();
        }
        let queue = insertion("turn.queue", &id, "queued first");
        let queued_receipt = manager.command(&owner, &queue).await.unwrap();
        if resume {
            let inbox = managed.session.inbox_snapshot();
            let item = &inbox.items[0];
            manager.command(&owner, &command("inbox.resume", Some(&id), json!({
            "operation_id":uuid::Uuid::new_v4().to_string(),
            "expected_inbox_id":inbox.inbox_id,"item_id":item.id,"expected_revision":item.revision
        }))).await.unwrap();
        }
        entered(&model).await;
        let managed = manager
            .active(manager.record("owner", &id).await.unwrap(), true)
            .await
            .unwrap();
        model.release.add_permits(1);
        let snapshot = published(&managed, |snapshot| {
            snapshot["session"]["retry_ready"] == true
        })
        .await;
        assert_eq!(snapshot["inbox"]["paused"], true);
        let failed = snapshot["session"]["retry_operation_id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(
            snapshot["operations"]
                .as_array()
                .unwrap()
                .iter()
                .any(|operation| operation["id"] == failed
                    && operation["method"] == if resume { "inbox.resume" } else { "turn.inbox" })
        );
        manager
            .command(&owner, &insertion("turn.queue", &id, "after failure"))
            .await
            .unwrap();
        let inbox = managed.session.inbox_snapshot();
        assert_eq!(inbox.items.len(), 1);
        assert!(inbox.paused);
        // Observation can publish failure before the foreground owner releases.
        // This exact checkpoint fence proves idle without retrying any model work.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match managed.session.checkpoint().await {
                    Ok(_) => break,
                    Err(microcrab_session::Error::Busy) => tokio::task::yield_now().await,
                    Err(error) => panic!("settlement checkpoint failed: {error}"),
                }
            }
        })
        .await
        .unwrap();
        let before_send = serde_json::to_value(managed.session.inbox_snapshot()).unwrap();
        assert_eq!(
            manager.command(&owner, &queue).await.unwrap(),
            queued_receipt
        );
        assert_eq!(
            serde_json::to_value(managed.session.inbox_snapshot()).unwrap(),
            before_send
        );
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        let pending = published(&managed, |snapshot| {
            snapshot["inbox"]["items"]
                .as_array()
                .is_some_and(|items| items.len() == 1)
                && snapshot["inbox"]["paused"] == true
        })
        .await;
        assert_eq!(pending["inbox"]["items"].as_array().unwrap().len(), 1);
        assert_eq!(pending["inbox"]["paused"], true);
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        manager.command(&owner,&command("turn.retry",Some(&id),json!({"operation_id":uuid::Uuid::new_v4().to_string(),"failed_operation_id":failed}))).await.unwrap();
        entered(&model).await;
        {
            let seen = model.seen.lock().unwrap();
            assert_eq!(seen[0], seen[1]);
        }
        model.release.add_permits(1);
        entered(&model).await;
        model.release.add_permits(1);
        published(&managed, |snapshot| {
            snapshot["session"]["status"] == "idle"
                && snapshot["inbox"]["items"]
                    .as_array()
                    .is_some_and(Vec::is_empty)
        })
        .await;
        assert_eq!(model.calls.load(Ordering::SeqCst), 3);
        manager.close().await.unwrap();
    }
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn explicit_activation_after_failure_preserves_fifo_and_supersedes_old_retry_after_admission()
{
    let model = model(true);
    let (manager, _) = manager(model.clone(), "file").await;
    let id = create(&manager).await;
    let owner = Principal::for_test("owner");
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    manager
        .command(
            &owner,
            &insertion("turn.queue", &id, "failed initial input"),
        )
        .await
        .unwrap();
    entered(&model).await;
    model.release.add_permits(1);
    let failed_snapshot = published(&managed, |snapshot| {
        snapshot["session"]["retry_ready"] == true
    })
    .await;
    let failed = failed_snapshot["session"]["retry_operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match managed.session.checkpoint().await {
                Ok(_) => break,
                Err(microcrab_session::Error::Busy) => tokio::task::yield_now().await,
                Err(error) => panic!("settlement failed: {error}"),
            }
        }
    })
    .await
    .unwrap();
    let before = managed.session.context_snapshot().unwrap();
    let inbox_before = serde_json::to_value(managed.session.inbox_snapshot()).unwrap();
    // Invalid browser source is rejected by the real input preflight before any
    // new-Turn selection or clearing of the old prepared continuation.
    let mut invalid = insertion("turn.queue", &id, "invalid activation");
    invalid.params["activate_if_idle"] = json!(true);
    invalid.params["input"] = json!({"parts":[{"type":"image","source":{"type":"url","url":"https://example.invalid/image.png"}}]});
    assert_eq!(
        manager.command(&owner, &invalid).await.unwrap_err().code,
        "unsupported_image_source"
    );
    assert_eq!(
        serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap(),
        serde_json::to_value(&before).unwrap()
    );
    assert_eq!(
        serde_json::to_value(managed.session.inbox_snapshot()).unwrap(),
        inbox_before
    );
    assert!(managed.session.has_continuation());
    assert!(managed.session.retry_ready());
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    let queued = insertion("turn.queue", &id, "older queued input");
    let queued_receipt = manager.command(&owner, &queued).await.unwrap();
    assert!(managed.session.inbox_snapshot().paused);
    let mut activate = insertion("turn.queue", &id, "explicit fresh input");
    activate.params["activate_if_idle"] = json!(true);
    let receipt = manager.command(&owner, &activate).await.unwrap();
    assert_eq!(receipt["status"], "completed");
    entered(&model).await;
    // An admitted new Turn supersedes the old retry; it does not replay its
    // failed provider request. The older FIFO input still receives precedence.
    {
        let seen = model.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_ne!(seen[0], seen[1]);
        let next = seen[1].to_string();
        assert!(next.contains("failed initial input"));
        assert!(next.contains("older queued input"));
        assert!(!next.contains("explicit fresh input"));
    }
    assert_eq!(manager.command(&owner, &activate).await.unwrap(), receipt);
    assert_eq!(
        manager.command(&owner, &queued).await.unwrap(),
        queued_receipt
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    model.release.add_permits(1);
    entered(&model).await;
    assert!(
        model.seen.lock().unwrap()[2]
            .to_string()
            .contains("explicit fresh input")
    );
    model.release.add_permits(1);
    published(&managed, |snapshot| {
        snapshot["session"]["status"] == "idle"
            && snapshot["inbox"]["items"]
                .as_array()
                .is_some_and(Vec::is_empty)
    })
    .await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        manager
            .command(
                &owner,
                &command(
                    "turn.retry",
                    Some(&id),
                    json!({
                        "operation_id":uuid::Uuid::new_v4().to_string(),"failed_operation_id":failed
                    })
                )
            )
            .await
            .unwrap_err()
            .code,
        "retry_unavailable"
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 3);
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn queue_execution_clock_uses_foreground_lifecycle_not_admission_receipt() {
    let model = model(false);
    let (manager, _) = manager(model.clone(), "file").await;
    let id = create(&manager).await;
    let owner = Principal::for_test("owner");
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let admission = manager
        .command(&owner, &insertion("turn.queue", &id, "first"))
        .await
        .unwrap();
    assert_eq!(admission["status"], "completed");
    entered(&model).await;
    let running = published(&managed, |snapshot| {
        snapshot["statistics"]["activity"]["execution"]["active"] == true
    })
    .await;
    let execution = running["statistics"]["activity"]["execution"].clone();
    assert!(
        execution["started_at_ms"]
            .as_u64()
            .is_some_and(|value| value > 0)
    );
    assert!(execution["ended_at_ms"].is_null());
    assert!(execution["elapsed_ms"].is_null());
    assert!(running["statistics"]["activity"]["operation"].is_null());
    // Fresh attach/snapshot reads use the same native start, not browser mount time.
    let fresh = manager.subscribe(&owner, &id).await.unwrap().snapshot;
    assert_eq!(fresh["statistics"]["activity"]["execution"], execution);
    manager
        .command(&owner, &insertion("turn.queue", &id, "later"))
        .await
        .unwrap();
    manager
        .command(&owner, &insertion("turn.steer", &id, "in this turn"))
        .await
        .unwrap();
    let unchanged = manager.subscribe(&owner, &id).await.unwrap().snapshot;
    assert_eq!(unchanged["statistics"]["activity"]["execution"], execution);
    // Cancellation ends this interval and leaves the waiting input paused, not timed.
    manager
        .command(&owner, &command("session.interrupt", Some(&id), json!({})))
        .await
        .unwrap();
    let stopped = published(&managed, |snapshot| {
        snapshot["statistics"]["activity"]["execution"]["active"] == false
            && snapshot["statistics"]["activity"]["execution"]["ended_at_ms"].is_u64()
    })
    .await;
    let stopped = &stopped["statistics"]["activity"]["execution"];
    assert_eq!(stopped["started_at_ms"], execution["started_at_ms"]);
    assert!(stopped["elapsed_ms"].is_u64());
    assert_eq!(
        stopped["elapsed_ms"].as_u64(),
        stopped["ended_at_ms"]
            .as_u64()
            .unwrap()
            .checked_sub(execution["started_at_ms"].as_u64().unwrap())
    );
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn interrupt_of_an_active_runtime_does_not_wait_for_journal_admission() {
    let model = model(false);
    let (manager, _) = manager(model.clone(), "file").await;
    let id = create(&manager).await;
    let owner = Principal::for_test("owner");
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let mut send = insertion("turn.queue", &id, "long turn");
    send.params["activate_if_idle"] = json!(true);
    manager.command(&owner, &send).await.unwrap();
    entered(&model).await;
    // An attach refresh or dormant projection holds this gate for the
    // projection's duration; stop must not queue behind it.
    let gate = manager.journal_open_gate(&id).lock_owned().await;
    let interrupt = command("session.interrupt", Some(&id), json!({}));
    let reply = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        manager.command(&owner, &interrupt),
    )
    .await
    .expect("interrupt waited for journal admission")
    .unwrap();
    assert_eq!(reply, json!({"requested":true}));
    published(&managed, |snapshot| snapshot["inbox"]["active"] == false).await;
    drop(gate);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    manager.close().await.unwrap();
}
