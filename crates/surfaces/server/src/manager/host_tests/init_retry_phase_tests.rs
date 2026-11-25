//! Real imported process permission tests across Server create/bind/open phases.
use super::*;
/// Failed assertions cancel the real permission wait instead of leaving a
/// detached initialization request alive until its production timeout.
struct InitCleanup {
    session: microcrab_session::Session,
    task: tokio::task::AbortHandle,
}
impl Drop for InitCleanup {
    fn drop(&mut self) {
        self.session.interrupt();
        self.task.abort();
    }
}
#[tokio::test]
async fn explicit_initialization_deny_applies_before_first_init_import() {
    let root = tempfile::tempdir().unwrap();
    let configuration = fixture::configuration(root.path(), "restricted", true);
    let host = fixture::host(root.path());
    let manager = Manager::with_host(configuration, host.clone())
        .await
        .unwrap();
    let managed = fixture::admit(&manager, "restricted").await;
    let id = managed.record.lock().await.id.clone();
    assert_eq!(
        managed.session.state().selected_mode.as_deref(),
        Some("restricted")
    );
    assert_eq!(host.launches.load(Ordering::SeqCst), 0);
    let record = managed.record.lock().await.clone();
    let before = manager.inspect(&record).await.unwrap();
    assert_eq!(before["session"]["agent_initialization_deferred"], true);
    let open = command(commands::SESSION_OPEN, &id, json!({}));
    assert_eq!(
        commands::execute(&manager, &Principal::for_test("owner"), &open)
            .await
            .unwrap_err()
            .code,
        "executor_required"
    );
    assert!(super::super::super::agent_init::deferred(&managed));
    assert!(managed.session.state().agent_init.is_empty());
    fixture::bind(&manager, &managed).await;
    let mut subscription = managed.hub.subscribe();
    let opening_manager = manager.clone();
    let opening_id = id.clone();
    let mut opening = tokio::spawn(async move {
        commands::execute(
            &opening_manager,
            &Principal::for_test("owner"),
            &command(commands::SESSION_OPEN, &opening_id, json!({})),
        )
        .await
    });
    let _cleanup = InitCleanup {
        session: managed.session.clone(),
        task: opening.abort_handle(),
    };
    let failure = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            tokio::select! {
                result = &mut opening => break result.unwrap().unwrap_err(),
                event = subscription.receiver.recv() => {
                    let event = event.unwrap();
                    assert!(
                        !matches!(&event.event, crate::manager::SessionRuntimeEvent::PermissionRequested(_)),
                        "restricted mode unexpectedly asked: {}",
                        event.event.payload()
                    );
                }
            }
        }
    }).await.unwrap_or_else(|_| panic!("restricted init did not settle: state={:?}, policy={:?}, executions={:?}", managed.session.state().agent_init, managed.session.permission_controller().snapshot(), managed.session.execution_snapshots()));
    assert_eq!(failure.code, "agent_init_failed");
    assert!(
        host.launches.load(Ordering::SeqCst) == 0,
        "explicit initialization Deny must prevent actual Host process admission"
    );
    assert!(!super::super::super::agent_init::deferred(&managed));
    assert_eq!(managed.session.state().agent_init.len(), 1);
    assert!(matches!(
        managed.session.state().agent_init[0].status,
        AgentInitStatus::Failed { .. }
    ));
    manager.close().await.unwrap();
}
#[tokio::test]
async fn fresh_create_binds_executor_then_answers_first_init_permission_question() {
    let root = tempfile::tempdir().unwrap();
    let configuration = fixture::configuration(root.path(), "ask", true);
    let host = fixture::host(root.path());
    let manager = Manager::with_host(configuration, host.clone())
        .await
        .unwrap();
    let managed = fixture::admit(&manager, "ask").await;
    let id = managed.record.lock().await.id.clone();
    assert!(managed.session.state().agent_init.is_empty());
    assert_eq!(host.launches.load(Ordering::SeqCst), 0);
    let lease = fixture::bind(&manager, &managed).await;
    let mut subscription = managed.hub.subscribe();
    let opening_manager = manager.clone();
    let opening_id = id.clone();
    use tracing::instrument::WithSubscriber;
    let diagnostic = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::ERROR)
        .with_test_writer()
        .with_ansi(false)
        .finish();
    let opening = tokio::spawn(
        async move {
            commands::execute(
                &opening_manager,
                &Principal::for_test("owner"),
                &command(commands::SESSION_OPEN, &opening_id, json!({})),
            )
            .await
        }
        .with_subscriber(diagnostic),
    );
    let _cleanup = InitCleanup {
        session: managed.session.clone(),
        task: opening.abort_handle(),
    };
    let question = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let event = subscription.receiver.recv().await.unwrap();
            if matches!(
                &event.event,
                crate::manager::SessionRuntimeEvent::PermissionRequested(_)
            ) {
                break event.event.payload().clone();
            }
        }
    })
    .await
    .expect("first init import must reach the already-bound executor");
    let owner = Principal::for_test("owner");
    let requests = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        manager.permission_requests(&owner, &id),
    )
    .await
    .expect("published init permission must be inspectable")
    .unwrap();
    assert_eq!(requests["requests"].as_array().unwrap().len(), 1);
    let snapshot = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        manager.subscribe(&owner, &id),
    )
    .await
    .expect("initialization must not hold the Journal inspection gate")
    .unwrap()
    .snapshot;
    assert_eq!(snapshot["session"]["agent_init_pending"], true);
    assert_eq!(host.launches.load(Ordering::SeqCst), 0);
    let reply = command(
        commands::PERMISSION_REPLY,
        &id,
        json!({"request_id":question["request"]["request_id"],"lease_id":lease,"decision":"allow"}),
    );
    assert!(
        manager
            .permission_reply(&owner, "other_socket", &reply)
            .await
            .is_err()
    );
    manager
        .permission_reply(&owner, "executor", &reply)
        .await
        .unwrap();
    let initialized = tokio::time::timeout(std::time::Duration::from_secs(10), opening)
        .await
        .unwrap()
        .unwrap();
    initialized.unwrap_or_else(|error| {
        panic!(
            "approved init failed after {} successful native shell launches: {error:?}",
            host.launches.load(Ordering::SeqCst)
        )
    });
    assert_eq!(host.launches.load(Ordering::SeqCst), 1);
    assert!(managed.session.agent_initialization_complete());

    commands::execute(
        &manager,
        &owner,
        &command(commands::SESSION_OPEN, &id, json!({})),
    )
    .await
    .unwrap();
    assert_eq!(
        host.launches.load(Ordering::SeqCst),
        1,
        "ordinary reopen cannot replay initialization effects"
    );

    // Inspect actual Manager publications after the real WASM initialization,
    // rather than reconstructing its execution row in a transport mock.
    let initialization = managed.session.state().agent_init[0].clone();
    let initialization_row = |snapshot: &Value| {
        let raw = snapshot["executions"]
            .as_array()
            .expect("subscription must retain execution metadata")
            .iter()
            .find(|row| row["subject"]["kind"] == "agent_init")
            .expect("subscription must retain the lifecycle execution");
        let row: microcrab_session::runtime::execution::ExecutionSnapshot =
            serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(row.session_id, id);
        assert!(row.execution_id.0 > 0);
        assert!(!row.call_id.is_empty());
        assert_eq!(row.tool, "");
        assert_eq!(
            row.subject,
            microcrab_session::policy::InvocationSubject::AgentInit {
                component: initialization.component.clone(),
                generation: initialization.generation,
                attempt: initialization.attempt,
            }
        );
        assert!(raw.get("arguments").is_none());
        assert!(raw.get("invocation_result").is_none());
        raw.clone()
    };
    let active = manager.subscribe(&owner, &id).await.unwrap();
    let active_initialization = initialization_row(&active.snapshot);
    drop(active);

    commands::execute(
        &manager,
        &owner,
        &command(commands::SESSION_CLOSE, &id, json!({})),
    )
    .await
    .unwrap();
    let dormant = manager.subscribe(&owner, &id).await.unwrap();
    let dormant_initialization = initialization_row(&dormant.snapshot);
    for field in ["execution_id", "session_id", "call_id", "subject", "tool"] {
        assert_eq!(
            active_initialization[field], dormant_initialization[field],
            "dormant subscription must preserve initialization {field}"
        );
    }
    assert_eq!(
        host.launches.load(Ordering::SeqCst),
        1,
        "active and dormant subscriptions cannot replay initialization effects"
    );
    manager.close().await.unwrap();
}
