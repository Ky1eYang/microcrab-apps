//! Permission interaction regressions; the parent runs the cohesive native graph.
mod concurrency;
mod factory;
mod manager;
mod persistence;
use super::super::client_tools::BindRequest;
use super::*;
use microcrab_permission::{Permission, PermissionConfig};
use microcrab_protocol::{Response, ToolCall, ToolDefinition, Usage};
use microcrab_session::{
    Auditor, Client, Error, Limits, Session, SessionInput, VariableView,
    context::{Content, ContentItem},
    runtime::{
        Runtime, ToolRegistration,
        tool::{Tool, ToolContext},
    },
};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

fn request(session: &str, call: &str) -> PermissionRequest {
    PermissionRequest {
        subject: Default::default(),
        session_id: session.into(),
        call_id: call.into(),
        agent: "fixture".into(),
        mode: "execute".into(),
        tool: "effect".into(),
        arguments: json!({"path":"file"}),
        revision: 1,
    }
}
async fn setup() -> (tempfile::TempDir, Arc<PermissionAsk>, String) {
    let root = tempfile::tempdir().unwrap();
    let tools = ClientTools::open(root.path(), "parent").await.unwrap();
    tools.register(vec![], 0).await.unwrap();
    let bind = tools
        .bind(
            "socket_a",
            &BindRequest {
                client_id: "client_a".into(),
                catalog_digest: tools.snapshot().await.catalog_digest,
            },
        )
        .await
        .unwrap();
    let lease = bind.lease_id.unwrap();
    (
        root,
        PermissionAsk::new("parent".into(), tools, Arc::new(Hub::new()), Arc::default()),
        lease,
    )
}
async fn event(subscription: &mut super::super::hub::Subscription, name: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let item = subscription.receiver.recv().await.unwrap();
            if serde_json::to_value(&item.event).unwrap()["event"] == name {
                return item.event.payload().clone();
            }
        }
    })
    .await
    .expect("expected permission event")
}
fn answer(id: &str, lease: &str, decision: ReplyDecision) -> Reply {
    Reply {
        request_id: id.into(),
        lease_id: lease.into(),
        decision,
    }
}
#[tokio::test]
async fn parent_and_child_owner_participants_share_first_winner_receipts() {
    let (_root, bridge, lease) = setup().await;
    let observer = bridge
        .tools
        .bind(
            "observer",
            &BindRequest {
                client_id: "observer_client".into(),
                catalog_digest: bridge.tools.snapshot().await.catalog_digest,
            },
        )
        .await
        .unwrap();
    assert!(observer.lease_id.is_none());
    let mut subscription = bridge.hub.subscribe();
    for session in ["parent", "child_session"] {
        let pending_bridge = bridge.clone();
        let pending =
            tokio::spawn(async move { pending_bridge.ask(&request(session, "call")).await });
        let row = event(&mut subscription, "permission.requested").await;
        assert_eq!(row["request"]["session_id"], session);
        assert_eq!(row["request"]["parent_session_id"], "parent");
        let id = row["request"]["request_id"].as_str().unwrap();
        assert_eq!(
            bridge
                .reply("unbound", answer(id, "", ReplyDecision::Allow))
                .await
                .unwrap_err()
                .code,
            "permission_participant_required"
        );
        let accepted = bridge
            .reply("observer", answer(id, "", ReplyDecision::Allow))
            .await
            .unwrap();
        assert_eq!(accepted["remembered"], false);
        assert_eq!(pending.await.unwrap().unwrap().action, Action::Allow);
        let duplicate = bridge
            .reply("socket_a", answer(id, &lease, ReplyDecision::Allow))
            .await
            .unwrap();
        assert_eq!(duplicate["duplicate"], true);
        let losing = bridge
            .reply("socket_a", answer(id, &lease, ReplyDecision::Deny))
            .await
            .unwrap();
        assert_eq!(losing["decision"], "allow");
        assert_eq!(losing["duplicate"], true);
        let settled = event(&mut subscription, "permission.settled").await;
        assert_eq!(settled["request_id"], id);
        assert_eq!(settled["decision"], "allow");
        assert_eq!(settled["remembered"], false);
        assert_eq!(bridge.snapshot().await["requests"], json!([]));
    }
    assert!(bridge.features().remembered_grants);
}
#[tokio::test]
async fn executor_disconnect_preserves_live_question_but_restart_never_revives_it() {
    let (_root, bridge, old_lease) = setup().await;
    let mut subscription = bridge.hub.subscribe();
    let pending_bridge = bridge.clone();
    let pending = tokio::spawn(async move { pending_bridge.ask(&request("child", "call")).await });
    let row = event(&mut subscription, "permission.requested").await;
    bridge.tools.detach("socket_a").await.unwrap();
    let new_lease = bridge
        .tools
        .bind(
            "socket_b",
            &BindRequest {
                client_id: "client_b".into(),
                catalog_digest: bridge.tools.snapshot().await.catalog_digest,
            },
        )
        .await
        .unwrap()
        .lease_id
        .unwrap();
    let id = row["request"]["request_id"].as_str().unwrap();
    assert!(
        bridge
            .reply("socket_a", answer(id, &old_lease, ReplyDecision::Allow))
            .await
            .is_err()
    );
    bridge
        .reply("socket_b", answer(id, &new_lease, ReplyDecision::Allow))
        .await
        .unwrap();
    assert_eq!(pending.await.unwrap().unwrap().action, Action::Allow);
    assert_eq!(
        event(&mut subscription, "permission.settled").await["decision"],
        "allow"
    );
    assert_eq!(bridge.snapshot().await["requests"], json!([]));
    bridge.tools.detach("socket_b").await.unwrap();
    let reopened = ClientTools::open(_root.path(), "parent").await.unwrap();
    let fresh = PermissionAsk::new(
        "parent".into(),
        reopened,
        Arc::new(Hub::new()),
        Arc::default(),
    );
    assert_eq!(fresh.snapshot().await["requests"], json!([]));
    assert_eq!(fresh.snapshot().await["executor_connected"], false);
}
#[tokio::test]
async fn dropped_future_expiry_capacity_and_close_fail_closed_without_persistence() {
    let (_root, bridge, lease) = setup().await;
    let mut subscription = bridge.hub.subscribe();
    let mut tasks = vec![];
    let mut first_id = String::new();
    for index in 0..MAX_PENDING {
        let pending_bridge = bridge.clone();
        tasks.push(tokio::spawn(async move {
            pending_bridge
                .ask(&request("parent", &format!("call-{index}")))
                .await
        }));
        let row = event(&mut subscription, "permission.requested").await;
        if index == 0 {
            first_id = row["request"]["request_id"].as_str().unwrap().into();
        }
    }
    assert!(matches!(
        bridge.ask(&request("parent", "overflow")).await,
        Err(PermissionError::Unavailable)
    ));
    let mut oversized = request("parent", "large");
    oversized.arguments = json!("x".repeat(MAX_REQUEST_BYTES));
    assert!(matches!(
        bridge.ask(&oversized).await,
        Err(PermissionError::Unavailable)
    ));
    bridge
        .state
        .lock()
        .unwrap()
        .pending
        .get_mut(&first_id)
        .unwrap()
        .deadline = Instant::now() - Duration::from_secs(1);
    assert!(
        bridge
            .reply("socket_a", answer(&first_id, &lease, ReplyDecision::Allow))
            .await
            .is_err()
    );
    assert_eq!(tasks.remove(0).await.unwrap().unwrap().action, Action::Deny);
    let aborted = tasks.remove(0);
    aborted.abort();
    let _ = aborted.await;
    assert_eq!(
        bridge.snapshot().await["requests"]
            .as_array()
            .unwrap()
            .len(),
        MAX_PENDING - 2
    );
    bridge.close();
    for task in tasks {
        assert_eq!(task.await.unwrap().unwrap().action, Action::Deny);
    }
    assert_eq!(bridge.snapshot().await["requests"], json!([]));
    assert_eq!(bridge.snapshot().await["interactive_ask"], false);
    assert!(matches!(
        bridge.ask(&request("parent", "after-close")).await,
        Err(PermissionError::Unavailable)
    ));
}
struct Model;
#[async_trait]
impl Client for Model {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let mut response = Response {
            provider_metadata: None,
            content: "done".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        };
        if !content
            .items
            .iter()
            .any(|item| matches!(item, ContentItem::Tool { .. }))
        {
            response.tool_calls.push(ToolCall {
                provider_metadata: None,
                id: "effect-call".into(),
                name: "effect".into(),
                arguments: json!({}),
            });
        }
        Ok(response)
    }
}
struct Effect(Arc<AtomicUsize>);
#[async_trait]
impl Tool for Effect {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "effect".into(),
            description: "test effect".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(
        &self,
        _: Value,
        _: ToolContext,
    ) -> Result<microcrab_session::runtime::tool::ToolOutput, Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(microcrab_session::runtime::tool::ToolOutput::text("effect"))
    }
}
fn rule(action: Action) -> Permission {
    Permission {
        agent: None,
        mode: None,
        tool: None,
        args: vec![],
        action,
    }
}
fn session(bridge: Arc<PermissionAsk>, count: Arc<AtomicUsize>, id: &str) -> Session {
    let runtime = Runtime::new(
        vec![ToolRegistration::Wasm(Arc::new(Effect(count)))],
        bridge,
    )
    .unwrap();
    Session::builder_arc(Arc::new(Model), runtime)
        .session_id(id)
        .permissions(PermissionConfig {
            overrides: vec![rule(Action::Ask)],
            review: false,
            ..Default::default()
        })
        .build()
        .unwrap()
}
#[tokio::test]
async fn actual_runtime_approval_revision_and_parent_ceiling_remain_authoritative() {
    let (_root, bridge, lease) = setup().await;
    let count = Arc::new(AtomicUsize::new(0));
    let mut subscription = bridge.hub.subscribe();
    let main = Arc::new(session(bridge.clone(), count.clone(), "parent"));
    let submitting = main.clone();
    let task = tokio::spawn(async move { submitting.submit(SessionInput::text("run")).await });
    let row = event(&mut subscription, "permission.requested").await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let id = row["request"]["request_id"].as_str().unwrap();
    bridge
        .reply("socket_a", answer(id, &lease, ReplyDecision::Allow))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    main.close().await.unwrap();
    let next = Arc::new(session(bridge.clone(), count.clone(), "child"));
    let submitting = next.clone();
    let task = tokio::spawn(async move { submitting.submit(SessionInput::text("run")).await });
    let row = event(&mut subscription, "permission.requested").await;
    assert_eq!(row["request"]["session_id"], "child");
    let controller = next.permission_controller();
    let mut changed = controller.snapshot().unwrap();
    changed.config.overrides = vec![rule(Action::Deny)];
    next.replace_permissions(changed.revision, changed.config)
        .unwrap();
    let id = row["request"]["request_id"].as_str().unwrap();
    bridge
        .reply("socket_a", answer(id, &lease, ReplyDecision::Allow))
        .await
        .unwrap();
    let _ = task.await.unwrap();
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "old approval cannot execute after revision change"
    );
    next.close().await.unwrap();
    let parent = microcrab_session::policy::PermissionController::new(Arc::new(
        microcrab_creator::DeclarativePermission,
    ));
    // A fresh parent's unknown-Deny ceiling cannot be overridden by child Ask/Allow.
    let child_runtime = Runtime::new(
        vec![ToolRegistration::Wasm(Arc::new(Effect(count.clone())))],
        bridge.clone(),
    )
    .unwrap();
    let child = Session::builder_arc(Arc::new(Model), child_runtime)
        .session_id("ceiling-child")
        .permission_parent(Arc::new(parent))
        .permissions(PermissionConfig {
            overrides: vec![rule(Action::Allow)],
            review: false,
            ..Default::default()
        })
        .build()
        .unwrap();
    let _ = child.submit(SessionInput::text("run")).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(bridge.snapshot().await["requests"], json!([]));
    child.close().await.unwrap();
}
#[tokio::test]
async fn runtime_interrupt_cancels_but_questions_outlive_old_tool_deadline() {
    for cancelled in [true, false] {
        let (_root, bridge, lease) = setup().await;
        let mut subscription = bridge.hub.subscribe();
        let count = Arc::new(AtomicUsize::new(0));
        let session = Arc::new(session(bridge.clone(), count.clone(), "parent"));
        let submitting = session.clone();
        let mut task =
            tokio::spawn(async move { submitting.submit(SessionInput::text("run")).await });
        let row = event(&mut subscription, "permission.requested").await;
        if cancelled {
            session.interrupt();
        } else {
            assert!(
                tokio::time::timeout(Duration::from_millis(120), &mut task)
                    .await
                    .is_err(),
                "a pending human decision waits with no elapsed deadline"
            );
            assert_eq!(
                bridge.snapshot().await["requests"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(count.load(Ordering::SeqCst), 0);
            bridge
                .reply(
                    "socket_a",
                    answer(
                        row["request"]["request_id"].as_str().unwrap(),
                        &lease,
                        ReplyDecision::Allow,
                    ),
                )
                .await
                .unwrap();
        }
        let outcome = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        if cancelled {
            assert!(matches!(outcome, Err(Error::Cancelled)));
        } else {
            assert_eq!(outcome.unwrap().content, "done");
        }
        let execution = session
            .execution_snapshots()
            .into_iter()
            .find(|row| row.call_id == "effect-call")
            .unwrap();
        assert_eq!(
            execution.status,
            if cancelled {
                microcrab_session::runtime::execution::ExecutionStatus::Cancelled
            } else {
                microcrab_session::runtime::execution::ExecutionStatus::Succeeded
            }
        );
        let settled = event(&mut subscription, "permission.settled").await;
        assert_eq!(
            settled["decision"],
            if cancelled { "deny" } else { "allow" }
        );
        assert_eq!(count.load(Ordering::SeqCst), if cancelled { 0 } else { 1 });
        assert_eq!(bridge.snapshot().await["requests"], json!([]));
        let repeated = bridge
            .reply(
                "socket_a",
                answer(
                    row["request"]["request_id"].as_str().unwrap(),
                    &lease,
                    ReplyDecision::Allow,
                ),
            )
            .await;
        let repeated = repeated.unwrap();
        assert_eq!(repeated["duplicate"], true);
        assert_eq!(
            repeated["decision"],
            if cancelled { "deny" } else { "allow" }
        );
        if !cancelled {
            let opposing = bridge
                .reply(
                    "socket_a",
                    answer(
                        row["request"]["request_id"].as_str().unwrap(),
                        &lease,
                        ReplyDecision::Deny,
                    ),
                )
                .await
                .unwrap();
            assert_eq!(opposing["decision"], "allow");
            assert_eq!(opposing["duplicate"], true);
        }
        assert_eq!(count.load(Ordering::SeqCst), if cancelled { 0 } else { 1 });
        session.close().await.unwrap();
    }
}
