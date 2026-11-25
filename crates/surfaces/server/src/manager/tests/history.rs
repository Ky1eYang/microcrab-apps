//! Exact input rollback is one owner-scoped mutation with lossless restore data.
use super::*;
use microcrab_protocol::{ImageSource, InputPart};
use serde_json::json;

mod epoch_tests;
mod inspection_continuity_tests;

fn history_png(bytes: usize) -> Vec<u8> {
    // Complete 1x1 PNG; trailing padding retains the large-input/frame scenario.
    use base64::Engine as _;
    let mut data = base64::engine::general_purpose::STANDARD
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j7S8AAAAASUVORK5CYII=")
        .unwrap();
    data.resize(bytes.max(data.len()), 7);
    data
}

async fn targeted_undo_contract(storage: &str) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(8),
    });
    let (manager, _root) = manager(model.clone(), storage).await;
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
    let mut original = microcrab_session::SessionInput::text("First input\nwith formatting");
    original.parts.push(InputPart::Image {
        source: ImageSource::Data {
            media_type: "image/png".into(),
            data: history_png(0),
        },
    });
    managed.session.submit(original.clone()).await.unwrap();
    let first_head = managed.journal.cursor().await.unwrap();
    managed.session.submit("Second input").await.unwrap();
    let selected = manager.refresh(&managed).await.unwrap();
    let selected_context =
        serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap();
    let first_id = selected["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "user")
        .unwrap()["id"]
        .as_str()
        .unwrap();
    let last_id = selected["messages"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|message| message["role"] == "user")
        .unwrap()["id"]
        .as_str()
        .unwrap();
    let head = managed.journal.cursor().await.unwrap();
    let principal = crate::auth::Principal::for_test("owner");
    for params in [
        json!({"direction":"undo","message_id":first_id,"expected_checkpoint_id":first_head.checkpoint_id}),
        json!({"direction":"undo","message_id":"foreign-user-id","expected_checkpoint_id":head.checkpoint_id}),
        json!({"direction":"undo","message_id":first_id}),
        json!({"direction":"redo","message_id":first_id,"expected_checkpoint_id":head.checkpoint_id}),
    ] {
        assert!(
            commands::execute(
                &manager,
                &principal,
                &command("history.travel", Some(id), params)
            )
            .await
            .is_err()
        );
        assert_eq!(managed.journal.cursor().await.as_ref(), Some(&head));
        assert_eq!(
            serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap(),
            selected_context
        );
    }
    let target = command(
        "history.travel",
        Some(id),
        json!({"direction":"undo","message_id":first_id,"expected_checkpoint_id":head.checkpoint_id}),
    );
    assert!(
        commands::execute(
            &manager,
            &crate::auth::Principal::for_test("intruder"),
            &target
        )
        .await
        .is_err()
    );
    let undone = commands::execute(&manager, &principal, &target)
        .await
        .unwrap();
    assert_eq!(
        undone["restored_input"],
        json!({"message_id":first_id,"parts":original.parts})
    );
    assert!(
        !undone["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["role"] == "user")
    );
    assert_eq!(undone["history_available"]["redo"], true);
    assert!(
        commands::execute(&manager, &principal, &target)
            .await
            .is_err()
    );
    let redone = commands::execute(
        &manager,
        &principal,
        &command("history.travel", Some(id), json!({"direction":"redo"})),
    )
    .await
    .unwrap();
    assert!(redone.get("restored_input").is_none());
    assert_eq!(managed.journal.cursor().await.as_ref(), Some(&head));
    assert_eq!(
        serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap(),
        selected_context
    );
    // Targeting the last input preserves the first complete turn, not just its text.
    let undone_last = commands::execute(&manager, &principal, &command("history.travel", Some(id), json!({"direction":"undo","message_id":last_id,"expected_checkpoint_id":head.checkpoint_id}))).await.unwrap();
    assert_eq!(managed.journal.cursor().await.as_ref(), Some(&first_head));
    assert_eq!(
        undone_last["restored_input"]["parts"],
        json!([{"type":"text","text":"Second input"}])
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    let mut remote = microcrab_session::SessionInput::text("Historical remote image");
    remote.parts.push(InputPart::Image {
        source: ImageSource::Url {
            url: "https://example.invalid/image.png".into(),
        },
    });
    managed.session.submit(remote).await.unwrap();
    let remote_snapshot = manager.refresh(&managed).await.unwrap();
    let remote_head = managed.journal.cursor().await.unwrap();
    let remote_id = remote_snapshot["messages"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|message| message["role"] == "user")
        .unwrap()["id"]
        .as_str()
        .unwrap();
    let error = commands::execute(&manager, &principal, &command("history.travel", Some(id), json!({"direction":"undo","message_id":remote_id,"expected_checkpoint_id":remote_head.checkpoint_id}))).await.unwrap_err();
    assert_eq!(error.code, "unsupported_history_input");
    assert_eq!(managed.journal.cursor().await.as_ref(), Some(&remote_head));
    // Rejection left no pending transaction; legacy checkpoint undo still works.
    let legacy = commands::execute(
        &manager,
        &principal,
        &command("history.travel", Some(id), json!({"direction":"undo"})),
    )
    .await
    .unwrap();
    assert!(legacy.get("restored_input").is_none());
    assert_eq!(managed.journal.cursor().await.as_ref(), Some(&first_head));
    // Equal input bytes remain two distinct Turns. Skip only this Turn's
    // pre-request admission checkpoint, never the previous identical input.
    managed
        .session
        .submit("identical repeated input")
        .await
        .unwrap();
    let repeated_first = managed.journal.cursor().await.unwrap();
    let repeated_first_context =
        serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap();
    managed
        .session
        .submit("identical repeated input")
        .await
        .unwrap();
    let repeated_second = managed.journal.cursor().await.unwrap();
    let repeated_second_context =
        serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap();
    commands::execute(
        &manager,
        &principal,
        &command("history.travel", Some(id), json!({"direction":"undo"})),
    )
    .await
    .unwrap();
    assert_eq!(
        managed.journal.cursor().await.as_ref(),
        Some(&repeated_first)
    );
    assert_eq!(
        serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap(),
        repeated_first_context
    );
    commands::execute(
        &manager,
        &principal,
        &command("history.travel", Some(id), json!({"direction":"redo"})),
    )
    .await
    .unwrap();
    assert_eq!(
        managed.journal.cursor().await.as_ref(),
        Some(&repeated_second)
    );
    assert_eq!(
        serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap(),
        repeated_second_context
    );
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn targeted_undo_file_preserves_input_identity_and_redo() {
    targeted_undo_contract("file").await;
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn restored_inline_image_omits_near_limit_state_without_losing_full_state_read() {
    use microcrab_session::{Journal, context::Context};

    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _root) = manager(model.clone(), "file").await;
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
    let frame_budget = microcrab_protocol::application::MAX_FRAME_BYTES - 16 * 1024;
    let initial = manager.refresh(&managed).await.unwrap();
    let large = "x".repeat(frame_budget - serde_json::to_vec(&initial).unwrap().len() - 32 * 1024);
    managed
        .session
        .state_handle()
        .inject("near_frame", json!(large))
        .await
        .unwrap();
    let before = managed.session.checkpoint_navigation().await.unwrap();
    let boundary = managed.journal.cursor().await.unwrap();
    let mut original = microcrab_session::SessionInput::text("Restore this image completely");
    original.parts.push(InputPart::Image {
        source: ImageSource::Data {
            media_type: "image/png".into(),
            data: history_png(32 * 1024),
        },
    });
    // Model-facing custom values are injected only at the next boundary. Keep
    // this deliberately small Context while exercising the real large State DTO.
    let mut context = Context::from_snapshot(before.context.clone()).unwrap();
    context
        .admit_user(
            original.clone(),
            &before
                .context
                .committed_state
                .clone()
                .unwrap_or_else(|| json!({})),
        )
        .unwrap();
    let mut source = before;
    source.context = context.snapshot().unwrap();
    managed.journal.commit(source).await.unwrap();
    let head = managed.journal.cursor().await.unwrap();
    let source = managed
        .journal
        .prepare_travel(microcrab_storage::Travel::To {
            checkpoint_id: head.checkpoint_id.clone(),
        })
        .await
        .unwrap();
    managed.session.recover(source).await.unwrap();
    let source_snapshot = manager.refresh(&managed).await.unwrap();
    assert_ne!(source_snapshot["state_omitted"], true);
    let message_id = source_snapshot["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "user")
        .unwrap()["id"]
        .as_str()
        .unwrap();
    let principal = crate::auth::Principal::for_test("owner");
    let response = commands::execute(&manager, &principal, &command("history.travel", Some(id), json!({"direction":"undo","message_id":message_id,"expected_checkpoint_id":head.checkpoint_id}))).await.unwrap();
    assert_eq!(managed.journal.cursor().await.as_ref(), Some(&boundary));
    assert!(response["state"].is_null());
    assert_eq!(response["state_omitted"], true);
    assert_eq!(
        response["restored_input"],
        json!({"message_id":message_id,"parts":original.parts})
    );
    assert!(serde_json::to_vec(&response).unwrap().len() <= frame_budget);
    let state = commands::execute(
        &manager,
        &principal,
        &command("state.get", Some(id), json!({})),
    )
    .await
    .unwrap();
    assert_eq!(state["near_frame"].as_str().unwrap(), large);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn dormant_subscription_omits_large_execution_state_and_preserves_full_read() {
    use microcrab_session::{
        Journal,
        context::Context,
        runtime::execution::{ExecutionId, ExecutionSnapshot, ExecutionStatus},
    };

    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(
        &manager,
        "owner",
        &command(
            "session.create",
            None,
            json!({
                "project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()
            }),
        ),
    )
    .await
    .unwrap();
    let id = created["id"].as_str().unwrap();
    let record = manager.record("owner", id).await.unwrap();
    let managed = manager.active(record.clone(), true).await.unwrap();
    let frame_budget = microcrab_protocol::application::MAX_FRAME_BYTES - 16 * 1024;
    let near_frame = "x".repeat(frame_budget - 64 * 1024);
    managed
        .session
        .state_handle()
        .inject("near_frame", json!(near_frame))
        .await
        .unwrap();
    let mut source = managed.session.checkpoint().await.unwrap();
    let mut context = Context::from_snapshot(source.context.clone()).unwrap();
    context
        .admit_user(
            microcrab_session::SessionInput::text("Keep this dormant conversation visible"),
            &source
                .context
                .committed_state
                .clone()
                .unwrap_or_else(|| json!({})),
        )
        .unwrap();
    source.context = context.snapshot().unwrap();
    let large_tool = "t".repeat(microcrab_protocol::application::MAX_FRAME_BYTES);
    let mut execution = ExecutionSnapshot::new(
        id.into(),
        "large-metadata".into(),
        large_tool.clone(),
        "main".into(),
        None,
    );
    execution.execution_id = ExecutionId(1);
    execution.status = ExecutionStatus::Succeeded;
    execution.result = Some("completed".into());
    source.state.executions.push(execution);
    commands::close_session(&manager, &managed, "owner", id, None)
        .await
        .unwrap();
    drop(managed);
    // Publish the synthetic recovery only after close has released its writer.
    let (journal, _) = manager.inspect_journal(&record).await.unwrap();
    journal.commit(source).await.unwrap();
    journal.close().await.unwrap();
    let principal = crate::auth::Principal::for_test("owner");
    let subscription = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        manager.subscribe(&principal, id),
    )
    .await
    .expect("large dormant subscription must settle")
    .unwrap();
    assert_eq!(subscription.snapshot["state_omitted"], true);
    assert!(subscription.snapshot["state"].is_null());
    assert!(
        subscription.snapshot["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["text"] == "Keep this dormant conversation visible")
    );
    assert!(serde_json::to_vec(&subscription.snapshot).unwrap().len() <= frame_budget);
    assert!(manager.registry.lock().unwrap().is_empty());
    let state = commands::execute(
        &manager,
        &principal,
        &command("state.get", Some(id), json!({})),
    )
    .await
    .unwrap();
    assert_eq!(state["near_frame"].as_str().unwrap(), near_frame);
    assert!(
        state.get("executions").is_none(),
        "Final invocation history is separate from readable State"
    );
    assert!(subscription.snapshot["executions"].is_null());
    assert_eq!(subscription.snapshot["executions_omitted"], true);
    let history_error = commands::execute(
        &manager,
        &principal,
        &command(
            commands::EXECUTIONS_LIST,
            Some(id),
            json!({"cursor":0,"limit":64}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(history_error.code, "execution_metadata_too_large");
    assert!(history_error.message.contains("execution 1"));
    assert!(history_error.message.contains("65536-byte"));
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn dormant_subscription_rejects_oversized_metadata_before_publishing_hub() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(
        &manager,
        "owner",
        &command(
            "session.create",
            None,
            json!({
                "project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()
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
    commands::close_session(&manager, &managed, "owner", id, None)
        .await
        .unwrap();
    drop(managed);
    let mut operation = manager
        .directory
        .list_operations("owner", id)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    operation.id = uuid::Uuid::new_v4().to_string();
    operation.method = "turn.submit".into();
    operation.status = "failed".into();
    operation.revision = 0;
    operation.result = Some(json!({"error":{"code":"test_failure", "message":
        "x".repeat(microcrab_protocol::application::MAX_FRAME_BYTES)
    }}));
    manager.directory.create_operation(operation).await.unwrap();
    let principal = crate::auth::Principal::for_test("owner");
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        manager.subscribe(&principal, id),
    )
    .await
    .expect("oversized metadata must return a structured error");
    let error = match result {
        Ok(_) => panic!("oversized metadata must not publish a subscription"),
        Err(error) => error,
    };
    assert_eq!(error.code, "limit");
    assert!(!manager.read_hubs.lock().unwrap().contains_key(id));
    assert!(manager.registry.lock().unwrap().is_empty());
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn dormant_subscription_capacity_tracks_live_owners_and_delete_retires_index() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (original, _root) = manager(model.clone(), "file").await;
    let mut config = original.config.clone();
    config.max_active_sessions = 1;
    original.close().await.unwrap();
    let manager = Manager::with_client(config, model.clone()).await.unwrap();
    let principal = crate::auth::Principal::for_test("owner");
    let mut records = Vec::new();
    for _ in 0..4 {
        let created = commands::create(
            &manager,
            "owner",
            &command(
                "session.create",
                None,
                json!({
                    "project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()
                }),
            ),
        )
        .await
        .unwrap();
        let id = created["id"].as_str().unwrap();
        super::super::deletion::archive_session(&manager, "owner", id, None)
            .await
            .unwrap();
        let record = manager.record("owner", id).await.unwrap();
        // Resolve the deferred probe first: this test measures subscriptions,
        // independently of the bounded history worker's temporary ownership.
        manager.inspect(&record).await.unwrap();
        records.push(record);
    }
    let first = manager.subscribe(&principal, &records[0].id).await.unwrap();
    let second = manager.subscribe(&principal, &records[0].id).await.unwrap();
    assert_eq!(first.generation, second.generation);
    assert!(Arc::ptr_eq(
        first._observation_owner.as_ref().unwrap(),
        second._observation_owner.as_ref().unwrap(),
    ));
    let weak = Arc::downgrade(first._observation_owner.as_ref().unwrap());
    let refused = manager.subscribe(&principal, &records[1].id).await;
    assert!(matches!(refused, Err(error) if error.code == "capacity"));
    drop(first);
    assert!(
        weak.upgrade().is_some(),
        "another subscriber still owns this Hub"
    );
    let refused = manager.subscribe(&principal, &records[1].id).await;
    assert!(matches!(refused, Err(error) if error.code == "capacity"));
    drop(second);
    assert!(
        weak.upgrade().is_none(),
        "last disconnect releases snapshot bytes"
    );
    for record in &records[1..] {
        let subscription = manager.subscribe(&principal, &record.id).await.unwrap();
        drop(subscription);
    }
    let subscription = manager.subscribe(&principal, &records[0].id).await.unwrap();
    super::super::deletion::delete_session(&manager, "owner", &records[0].id, None)
        .await
        .unwrap();
    assert!(
        !manager
            .read_hubs
            .lock()
            .unwrap()
            .contains_key(&records[0].id)
    );
    assert!(super::super::deletion::cleanup_settled(&manager, &records[0].id).await);
    drop(subscription);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn targeted_undo_sqlite_preserves_input_identity_and_redo() {
    targeted_undo_contract("sqlite").await;
}
