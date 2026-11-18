//! Bridge state-machine regression tests. No real browser or model is required.
#[path = "participation_tests.rs"]
mod participation_tests;
use super::*;
use microcrab_protocol::ToolDefinition;
use microcrab_session::{
    Limits, SessionState,
    runtime::{
        ToolRegistration,
        tool::{Tool, ToolContext},
    },
};
use serde_json::json;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

struct TextTool(Arc<dyn Tool>);
impl std::ops::Deref for TextTool {
    type Target = dyn Tool;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}
impl TextTool {
    async fn execute(
        &self,
        arguments: serde_json::Value,
        context: ToolContext,
    ) -> Result<String, microcrab_session::Error> {
        match self.execute_output(arguments, context).await? {
            microcrab_session::runtime::tool::ToolOutput::Final(output) => match output.failure {
                Some(failure) => Err(microcrab_session::Error::ToolFailure(failure)),
                None => Ok(output.content),
            },
            _ => Err(microcrab_session::Error::Tool(
                "expected Final in bridge state-machine fixture".into(),
            )),
        }
    }
    async fn execute_output(
        &self,
        arguments: serde_json::Value,
        mut context: ToolContext,
    ) -> Result<microcrab_session::runtime::tool::ToolOutput, microcrab_session::Error> {
        use microcrab_session::runtime::{Runtime, tool::AllowTools};
        let runtime = Runtime::new(
            vec![ToolRegistration::External(self.0.clone())],
            Arc::new(AllowTools::new([self.0.definition().name.clone()])),
        )?;
        context.tool = self.0.definition().name;
        context.ensure_detached_execution()?;
        let issuer = microcrab_session::context::Content {
            epoch: 0,
            subagent: None,
            system: "Direct bridge fixture admission".into(),
            items: vec![],
            tools: vec![],
            max_output_tokens: 1024,
            max_bytes: 65536,
            turn_id: None,
            round: None,
        };
        runtime
            .admit_invocation(
                &microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: context.call_id.clone(),
                    name: context.tool.clone(),
                    arguments: arguments.clone(),
                },
                &mut context,
                &issuer,
            )
            .await
            .map_err(|error| {
                microcrab_session::Error::protocol_with_private_reason(
                    "fixture admission failed",
                    error.to_string(),
                )
            })?;
        self.0.execute(arguments, context).await
    }
}

fn definition(reclaimable: bool) -> ClientToolDefinition {
    ClientToolDefinition {
        definition: ToolDefinition {
            name: "browser_action".into(),
            description: "Client supplied".into(),
            parameters: json!({"type":"object"}),
        },
        permissions: vec![],
        reclaimable,
        interaction: None,
    }
}
fn context(call_id: &str) -> ToolContext {
    let mut context = ToolContext::detached(
        SessionState::new(Limits::default()),
        CancellationToken::new(),
    );
    context.call_id = call_id.into();
    context.tool = "browser_action".into();
    context.session_id = "child_session".into();
    context.turn_id = Some(1);
    context.round_id = Some(2);
    context
}
async fn setup(
    reclaimable: bool,
) -> (
    tempfile::TempDir,
    Arc<ClientTools>,
    Arc<TextTool>,
    BindSnapshot,
) {
    setup_definition(definition(reclaimable)).await
}
async fn setup_definition(
    entry: ClientToolDefinition,
) -> (
    tempfile::TempDir,
    Arc<ClientTools>,
    Arc<TextTool>,
    BindSnapshot,
) {
    let directory = tempfile::tempdir().expect("test temporary directory");
    let bridge = ClientTools::open(directory.path(), "parent_session")
        .await
        .expect("open bridge");
    bridge
        .register(vec![entry], 0)
        .await
        .expect("register catalog");
    let snapshot = bridge.snapshot().await;
    let bind = bridge
        .bind(
            "socket_a",
            &BindRequest {
                client_id: "client_a".into(),
                catalog_digest: snapshot.catalog_digest,
            },
        )
        .await
        .expect("bind executor");
    let tool = match bridge.registrations().await.remove(0) {
        ToolRegistration::External(tool) => tool,
        _ => panic!("expected external registration"),
    };
    (directory, bridge, Arc::new(TextTool(tool)), bind)
}
async fn pending(bridge: &ClientTools) -> CallRecord {
    // Cooperatively yield to the spawned tool without timers or arbitrary sleep.
    for _ in 0..1000 {
        if let Some(call) = bridge.snapshot().await.pending.first() {
            return call.clone();
        }
        tokio::task::yield_now().await;
    }
    panic!("tool was not admitted");
}
#[tokio::test]
async fn fork_catalog_uses_frozen_selection_without_copying_or_mutating_lease() {
    let (directory, bridge, _tool, bind) = setup(true).await;
    let old = definition(true);
    let copied = bridge
        .fork_catalog(std::slice::from_ref(&old.definition))
        .await
        .unwrap();
    assert_eq!(copied, vec![old.clone()]);
    let fresh = ClientTools::open(directory.path(), "fork_session")
        .await
        .unwrap();
    fresh.register(copied, 0).await.unwrap();
    let snapshot = fresh.snapshot().await;
    assert_eq!(snapshot.definitions, vec![old.clone()]);
    assert!(snapshot.lease_id.is_none());
    assert!(snapshot.calls.is_empty());
    let still_owner = bridge
        .bind(
            "socket_a",
            &BindRequest {
                client_id: "client_a".into(),
                catalog_digest: bind.catalog_digest,
            },
        )
        .await
        .unwrap();
    assert_eq!(still_owner.lease_id, bind.lease_id);
    let mut candidate = old.clone();
    candidate.definition.description = "new pending epoch".into();
    bridge.register_pending(vec![candidate], 1).await.unwrap();
    // Read old frozen Context metadata even when the bridge holds a newer candidate.
    assert_eq!(
        bridge
            .fork_catalog(std::slice::from_ref(&old.definition))
            .await
            .unwrap(),
        vec![old]
    );
    assert_eq!(
        bridge.snapshot().await.definitions[0]
            .definition
            .description,
        "new pending epoch"
    );
}

#[tokio::test]
async fn fork_catalog_rejects_unsettled_calls_without_settling_source() {
    let (_directory, bridge, tool, _bind) = setup(true).await;
    let task = tokio::spawn(async move { tool.execute(json!({}), context("fork_pending")).await });
    let call = pending(&bridge).await;
    assert!(
        bridge
            .fork_catalog(&[definition(true).definition])
            .await
            .is_err()
    );
    assert_eq!(
        bridge.snapshot().await.pending[0].invocation_id,
        call.invocation_id
    );
    bridge.settle_all("test cleanup").await.unwrap();
    let _ = task.await.unwrap();
}

#[tokio::test]
async fn child_routes_to_parent_single_executor_and_duplicate_results_are_idempotent() {
    let (_directory, bridge, tool, bind) = setup(true).await;
    let observer = bridge
        .bind(
            "socket_b",
            &BindRequest {
                client_id: "client_b".into(),
                catalog_digest: bind.catalog_digest.clone(),
            },
        )
        .await
        .expect("observer bind");
    assert_eq!(observer.role, ClientRole::Participant);
    assert!(observer.lease_id.is_none());
    bridge
        .set_history_anchor("child_session", Some("history_a".into()))
        .await
        .expect("history anchor");
    let task =
        tokio::spawn(async move { tool.execute(json!({"value":1}), context("call_1")).await });
    let call = pending(&bridge).await;
    assert_eq!(call.parent_session_id, "parent_session");
    assert_eq!(call.session_id, "child_session");
    assert_eq!(call.history_anchor.as_deref(), Some("history_a"));
    let reply = ResultRequest {
        lease_id: bind.lease_id.expect("executor lease"),
        invocation_id: call.invocation_id,
        result: Some(json!({"ok":true})),
        error: None,
    };
    assert!(bridge.result("socket_b", &reply).await.is_err());
    let first = bridge
        .result("socket_a", &reply)
        .await
        .expect("first result");
    let second = bridge
        .result("socket_a", &reply)
        .await
        .expect("idempotent duplicate");
    assert_eq!(first, second);
    assert_eq!(
        task.await.expect("task join").expect("tool result"),
        "{\"ok\":true}"
    );
    let conflicting = ResultRequest {
        result: Some(json!({"ok":false})),
        ..reply
    };
    assert!(bridge.result("socket_a", &conflicting).await.is_err());
}
#[tokio::test]
async fn reconnect_reclaims_interaction_with_original_deadline_and_rejects_old_lease() {
    let (_directory, bridge, tool, bind) = setup(true).await;
    let task = tokio::spawn(async move { tool.execute(json!({}), context("call_1")).await });
    let original = pending(&bridge).await;
    bridge.detach("socket_a").await.expect("detach");
    let rebound = bridge
        .bind(
            "socket_b",
            &BindRequest {
                client_id: "client_b".into(),
                catalog_digest: bind.catalog_digest,
            },
        )
        .await
        .expect("rebind");
    assert_eq!(rebound.pending[0].deadline_ms, original.deadline_ms);
    let mut reply = ResultRequest {
        lease_id: bind.lease_id.expect("old lease"),
        invocation_id: original.invocation_id,
        result: Some(json!("answer")),
        error: None,
    };
    assert!(bridge.result("socket_a", &reply).await.is_err());
    reply.lease_id = rebound.lease_id.expect("new lease");
    bridge
        .result("socket_b", &reply)
        .await
        .expect("reclaimed answer");
    assert_eq!(task.await.expect("join").expect("answer"), "answer");
}
#[tokio::test]
async fn mutation_disconnect_settles_without_replay() {
    let (_directory, bridge, tool, bind) = setup(false).await;
    let task = tokio::spawn(async move { tool.execute(json!({}), context("call_1")).await });
    pending(&bridge).await;
    bridge.detach("socket_a").await.expect("detach");
    assert!(task.await.expect("join").is_err());
    let rebound = bridge
        .bind(
            "socket_b",
            &BindRequest {
                client_id: "client_b".into(),
                catalog_digest: bind.catalog_digest,
            },
        )
        .await
        .expect("bind");
    assert!(rebound.pending.is_empty());
    assert_eq!(rebound.calls.len(), 1);
    assert_eq!(rebound.calls[0].status, CallStatus::Interrupted);
}
#[tokio::test]
async fn restart_interrupts_unsettled_records_and_retains_catalog() {
    let (directory, bridge, tool, bind) = setup(true).await;
    let task = tokio::spawn(async move { tool.execute(json!({}), context("call_1")).await });
    let call = pending(&bridge).await;
    // Opening a fresh owner simulates a crashed process; it deliberately has no
    // recovered oneshot waiter and cannot repeat the browser effect.
    let restarted = ClientTools::open(directory.path(), "parent_session")
        .await
        .expect("restart open");
    let snapshot = restarted.snapshot().await;
    assert_eq!(snapshot.catalog_digest, bind.catalog_digest);
    assert_eq!(snapshot.calls[0].invocation_id, call.invocation_id);
    assert_eq!(snapshot.calls[0].status, CallStatus::Interrupted);
    assert!(snapshot.pending.is_empty());
    // Keep the simulated old process from overwriting the restarted store.
    bridge
        .settle_all("old test process cleanup")
        .await
        .expect("cleanup");
    assert!(task.await.expect("join").is_err());
}
#[tokio::test]
async fn cancellation_and_close_settle_pending_calls() {
    let (_directory, bridge, tool, _bind) = setup(true).await;
    let context = context("call_1");
    let token = context.cancellation.clone();
    let task = tokio::spawn(async move { tool.execute(json!({}), context).await });
    pending(&bridge).await;
    token.cancel();
    assert!(task.await.expect("join").is_err());
    assert_eq!(
        bridge.snapshot().await.calls[0].status,
        CallStatus::Interrupted
    );
    let tool = match bridge.registrations().await.remove(0) {
        ToolRegistration::External(tool) => Arc::new(TextTool(tool)),
        _ => panic!("external"),
    };
    let task = tokio::spawn(async move { tool.execute(json!({}), self::context("call_2")).await });
    pending(&bridge).await;
    bridge.settle_all("session closed").await.expect("close");
    assert!(task.await.expect("join").is_err());
    assert!(bridge.snapshot().await.pending.is_empty());
}
#[tokio::test]
async fn catalog_only_changes_at_explicit_epoch_and_rules_are_origin_scoped() {
    let (_directory, bridge, _tool, bind) = setup(true).await;
    let mut changed = definition(true);
    changed.definition.description = "new description".into();
    assert!(bridge.register(vec![changed.clone()], 0).await.is_err());
    bridge.register(vec![changed], 1).await.expect("new epoch");
    assert!(
        bridge
            .bind(
                "socket_a",
                &BindRequest {
                    client_id: "client_a".into(),
                    catalog_digest: bind.catalog_digest
                }
            )
            .await
            .is_err()
    );
    let mut bad = definition(true);
    bad.permissions = vec![microcrab_permission::Permission {
        agent: None,
        mode: None,
        tool: Some(microcrab_permission::Matcher::Exact("another_tool".into())),
        args: vec![],
        action: microcrab_permission::Action::Allow,
    }];
    assert!(bridge.register(vec![bad], 2).await.is_err());
}
#[tokio::test]
async fn unsafe_identifiers_and_oversize_arguments_are_rejected() {
    let directory = tempfile::tempdir().expect("temporary directory");
    assert!(
        ClientTools::open(directory.path(), "../escape")
            .await
            .is_err()
    );
    let (_directory, bridge, tool, _bind) = setup(false).await;
    assert!(
        tool.execute(json!({"large":"x".repeat(256 * 1024)}), context("call_1"))
            .await
            .is_err()
    );
    assert!(bridge.snapshot().await.calls.is_empty());
}

#[tokio::test]
async fn stale_unbind_cannot_revoke_new_same_socket_generation() {
    let (_directory, bridge, _tool, bind) = setup(true).await;
    let original_lease = bind.lease_id.expect("original lease");
    assert!(
        bridge
            .unbind("socket_a", &original_lease)
            .await
            .expect("unbind")
    );
    let rebound = bridge
        .bind(
            "socket_a",
            &BindRequest {
                client_id: "client_a".into(),
                catalog_digest: bind.catalog_digest.clone(),
            },
        )
        .await
        .expect("rebind");
    assert!(
        !bridge
            .unbind("socket_a", &original_lease)
            .await
            .expect("stale unbind")
    );
    let still_bound = bridge
        .bind(
            "socket_b",
            &BindRequest {
                client_id: "client_b".into(),
                catalog_digest: bind.catalog_digest,
            },
        )
        .await
        .expect("observer");
    assert_eq!(still_bound.role, ClientRole::Participant);
    assert!(
        bridge
            .unbind("socket_a", &rebound.lease_id.expect("new lease"))
            .await
            .expect("new unbind")
    );
}

#[tokio::test]
async fn journal_reconciles_old_or_committed_candidate_without_metadata_ambiguity() {
    let (directory, bridge, _tool, original) = setup(true).await;
    let mut candidate = definition(true);
    candidate.definition.description = "New frozen definition".into();
    bridge
        .register(vec![candidate.clone()], 1)
        .await
        .expect("candidate persisted");
    let candidate_digest = bridge.snapshot().await.catalog_digest;
    let recovered = ClientTools::open(directory.path(), "parent_session")
        .await
        .expect("restart");
    recovered
        .reconcile_catalog(&[definition(true).definition])
        .await
        .expect("journal retains old tools");
    assert_eq!(
        recovered.snapshot().await.catalog_digest,
        original.catalog_digest
    );
    recovered
        .reconcile_catalog(&[candidate.definition])
        .await
        .expect("pending epoch checkpoint has new tools");
    assert_eq!(recovered.snapshot().await.catalog_digest, candidate_digest);
    let mut metadata_change = definition(false);
    metadata_change.definition.description = "Client supplied".into();
    assert!(recovered.register(vec![metadata_change], 2).await.is_err());
    recovered
        .reconcile_catalog(&[])
        .await
        .expect("legacy empty external catalog");
    assert!(recovered.snapshot().await.definitions.is_empty());
}

#[tokio::test]
async fn reused_numeric_position_keeps_distinct_history_branches_and_blocks_duplicate_effects() {
    let (_directory, bridge, tool, bind) = setup(true).await;
    let lease_id = bind.lease_id.expect("lease");
    for branch in ["branch_a", "branch_b"] {
        bridge
            .set_history_anchor("child_session", Some(branch.into()))
            .await
            .expect("branch");
        let tool = tool.clone();
        let task = tokio::spawn(async move { tool.execute(json!({}), context("same_call")).await });
        let call = pending(&bridge).await;
        bridge
            .result(
                "socket_a",
                &ResultRequest {
                    lease_id: lease_id.clone(),
                    invocation_id: call.invocation_id,
                    result: Some(json!(branch)),
                    error: None,
                },
            )
            .await
            .expect("result");
        assert_eq!(task.await.expect("join").expect("result"), branch);
    }
    let calls = bridge.calls_page(0, 64).await.expect("page").calls;
    assert_eq!(calls.len(), 2);
    assert_ne!(calls[0].invocation_id, calls[1].invocation_id);
    assert_ne!(calls[0].history_anchor, calls[1].history_anchor);
    assert!(tool.execute(json!({}), context("same_call")).await.is_err());
}

#[tokio::test]
async fn explicit_null_result_survives_restart_and_is_not_absence() {
    let (directory, bridge, tool, bind) = setup(true).await;
    let task = tokio::spawn(async move { tool.execute(json!({}), context("null_call")).await });
    let call = pending(&bridge).await;
    let reply: ResultRequest = serde_json::from_value(json!({"lease_id":bind.lease_id.expect("lease"),"invocation_id":call.invocation_id,"result":null})).expect("explicit null DTO");
    assert_eq!(reply.result, Some(serde_json::Value::Null));
    bridge
        .result("socket_a", &reply)
        .await
        .expect("null result");
    assert_eq!(task.await.expect("join").expect("result"), "null");
    let recovered = ClientTools::open(directory.path(), "parent_session")
        .await
        .expect("restart");
    assert_eq!(
        recovered.snapshot().await.calls[0].result,
        Some(serde_json::Value::Null)
    );
}

#[tokio::test]
async fn dropped_future_settles_but_ordinary_calls_have_no_bridge_deadline() {
    let (_directory, bridge, tool, bind) = setup(true).await;
    let first_tool = tool.clone();
    let task =
        tokio::spawn(async move { first_tool.execute(json!({}), context("dropped_call")).await });
    pending(&bridge).await;
    task.abort();
    assert!(task.await.expect_err("aborted join").is_cancelled());
    for _ in 0..1000 {
        if bridge.snapshot().await.pending.is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        bridge.snapshot().await.calls[0].status,
        CallStatus::Interrupted
    );
    let state = SessionState::new(Limits::default());
    let mut deadline_context = ToolContext::detached(state, CancellationToken::new());
    deadline_context.call_id = "deadline_call".into();
    let mut task = tokio::spawn(async move { tool.execute(json!({}), deadline_context).await });
    let call = pending(&bridge).await;
    assert_eq!(call.deadline_ms, 0);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut task)
            .await
            .is_err()
    );
    assert_eq!(
        bridge.snapshot().await.pending[0].status,
        CallStatus::Pending
    );
    bridge
        .result(
            "socket_a",
            &ResultRequest {
                lease_id: bind.lease_id.expect("lease"),
                invocation_id: call.invocation_id,
                result: Some(json!("late ordinary result")),
                error: None,
            },
        )
        .await
        .expect("Runtime may retain the task after its foreground timeout");
    assert_eq!(
        task.await.expect("join").expect("late result"),
        "late ordinary result"
    );
}

#[tokio::test]
async fn snapshot_truncation_is_explicit_and_pages_preserve_complete_calls() {
    let (_directory, bridge, tool, bind) = setup(false).await;
    for index in 0..8 {
        let tool = tool.clone();
        let task = tokio::spawn(async move {
            tool.execute(
                json!({"text":"x".repeat(40 * 1024)}),
                context(&format!("call_{index}")),
            )
            .await
        });
        let call = pending(&bridge).await;
        bridge
            .result(
                "socket_a",
                &ResultRequest {
                    lease_id: bind.lease_id.clone().expect("lease"),
                    invocation_id: call.invocation_id,
                    result: Some(json!("done")),
                    error: None,
                },
            )
            .await
            .expect("result");
        task.await.expect("join").expect("tool result");
    }
    let snapshot = bridge.snapshot().await;
    assert!(snapshot.calls_truncated);
    assert_eq!(snapshot.next_cursor, Some(0));
    assert!(
        serde_json::to_vec(&snapshot).expect("snapshot JSON").len()
            <= super::model::MAX_SNAPSHOT_BYTES
    );
    let first = bridge.calls_page(0, 3).await.expect("first page");
    assert_eq!(first.calls.len(), 3);
    assert_eq!(first.next_cursor, Some(3));
    let second = bridge.calls_page(3, 64).await.expect("second page");
    assert_eq!(second.calls.len(), 5);
    assert_eq!(second.next_cursor, None);
    assert_eq!(second.total_calls, 8);
    assert!(bridge.calls_page(9, 1).await.is_err());
}

fn interaction_definition(wait_timeout_ms: u64) -> ClientToolDefinition {
    let mut entry = definition(true);
    entry.interaction = Some(super::model::ClientInteraction {
        mode_pointer: "/presentation/mode".into(),
        wait_timeout_ms,
        secret_writes: None,
    });
    entry
}
async fn paused(bridge: &ClientTools) -> CallRecord {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Some(call) = bridge
                .snapshot()
                .await
                .pending
                .into_iter()
                .find(|call| call.status == CallStatus::Paused)
            {
                return call;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("interaction becomes paused")
}
fn prepare_request(
    bind: &BindSnapshot,
    call: &CallRecord,
    timeout_result: Option<serde_json::Value>,
) -> PrepareRequest {
    PrepareRequest {
        lease_id: bind.lease_id.clone().expect("lease"),
        invocation_id: call.invocation_id.clone(),
        timeout_result,
    }
}

#[test]
fn adapter_mode_uses_only_frozen_pointer_and_interaction_default() {
    use super::model::InteractionMode;
    let entry: ClientToolDefinition = serde_json::from_value(json!({
        "name":"generic_action", "description":"opaque client schema", "parameters":{},
        "interaction":{"mode_pointer":"/presentation/mode"}
    }))
    .expect("default interaction budget");
    assert_eq!(entry.interaction.as_ref().unwrap().wait_timeout_ms, 300_000);
    assert_eq!(Limits::default().interaction_timeout_ms, 300_000);
    // This is the same adapter-private selector consumed by ClientTools::execute.
    assert_eq!(
        entry.interaction_mode(&json!({})).unwrap(),
        Some(InteractionMode::Blocking)
    );
    assert_eq!(
        entry
            .interaction_mode(&json!({"presentation":{"mode":"blocking"}}))
            .unwrap(),
        Some(InteractionMode::Blocking)
    );
    assert_eq!(
        entry
            .interaction_mode(&json!({"presentation":{"mode":"deferred"},"mode":"invalid"}))
            .unwrap(),
        Some(InteractionMode::Deferred)
    );
    for mode in [json!(false), json!(null), json!("unsupported")] {
        assert!(
            entry
                .interaction_mode(&json!({"presentation":{"mode":mode}}))
                .is_err()
        );
    }
    assert_eq!(
        definition(false)
            .interaction_mode(&json!({"mode":"deferred"}))
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn prepared_null_fallback_commits_before_events_and_waiter_completion() {
    let (directory, bridge, tool, bind) = setup_definition(interaction_definition(200)).await;
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let events = observed.clone();
    let root = directory.path().to_path_buf();
    bridge.set_observer(Arc::new(move |event| {
        let (_, durable) =
            super::store::Store::open(&root, "parent_session").expect("committed file");
        assert_eq!(durable.calls.last().unwrap(), &event.call);
        events.lock().unwrap().push(event.phase);
    }));
    let task = tokio::spawn(async move { tool.execute(json!({}), context("fallback")).await });
    let call = pending(&bridge).await;
    let request: PrepareRequest = serde_json::from_value(json!({
        "lease_id":bind.lease_id.clone().unwrap(), "invocation_id":call.invocation_id,
        "timeout_result":null
    }))
    .expect("explicit null fallback");
    assert_eq!(request.timeout_result, Some(serde_json::Value::Null));
    bridge
        .prepare("socket_a", &request)
        .await
        .expect("prepare fallback");
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        "null"
    );
    let settled = bridge.snapshot().await.calls[0].clone();
    assert_eq!(settled.status, CallStatus::Completed);
    assert!(settled.prepared && settled.timeout_applied);
    assert_eq!(settled.timeout_result, Some(serde_json::Value::Null));
    assert_eq!(bridge.prepare("socket_a", &request).await.unwrap(), settled);
    let mut conflict = request.clone();
    conflict.timeout_result = Some(json!("different"));
    assert!(bridge.prepare("socket_a", &conflict).await.is_err());
    assert_eq!(
        *observed.lock().unwrap(),
        vec![
            ClientToolPhase::Requested,
            ClientToolPhase::Prepared,
            ClientToolPhase::Settled
        ]
    );
    let recovered = ClientTools::open(directory.path(), "parent_session")
        .await
        .unwrap();
    assert_eq!(
        recovered.snapshot().await.calls[0].timeout_result,
        Some(serde_json::Value::Null)
    );
}

#[tokio::test]
async fn paused_interaction_reclaims_lease_and_accepts_late_answer() {
    let (_directory, bridge, tool, bind) = setup_definition(interaction_definition(60_000)).await;
    let mut invocation = context("late_answer");
    invocation.interaction_timeout_ms = 10;
    invocation.state = ToolContext::detached(
        SessionState::new(Limits {
            ..Limits::default()
        }),
        CancellationToken::new(),
    )
    .state;
    let task = tokio::spawn(async move { tool.execute(json!({}), invocation).await });
    let call = paused(&bridge).await;
    assert_eq!(call.deadline_ms - call.created_at_ms, 10);
    assert!(!task.is_finished());
    let marker = prepare_request(&bind, &call, None);
    bridge
        .prepare("socket_a", &marker)
        .await
        .expect("prepare without fallback");
    assert!(
        bridge
            .register(vec![interaction_definition(60_000)], 1)
            .await
            .is_err()
    );
    bridge.detach("socket_a").await.unwrap();
    let rebound = bridge
        .bind(
            "socket_b",
            &BindRequest {
                client_id: "client_b".into(),
                catalog_digest: bind.catalog_digest.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(rebound.pending[0].status, CallStatus::Paused);
    assert_eq!(rebound.pending[0].deadline_ms, call.deadline_ms);
    assert!(bridge.prepare("socket_a", &marker).await.is_err());
    bridge
        .result(
            "socket_b",
            &ResultRequest {
                lease_id: rebound.lease_id.unwrap(),
                invocation_id: call.invocation_id,
                result: Some(json!("human late answer")),
                error: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(task.await.unwrap().unwrap(), "human late answer");
}

#[tokio::test]
async fn late_prepare_applies_fallback_but_deferred_never_expires() {
    let (_directory, bridge, tool, bind) = setup_definition(interaction_definition(10)).await;
    let blocking = tool.clone();
    let task =
        tokio::spawn(async move { blocking.execute(json!({}), context("late_prepare")).await });
    let call = paused(&bridge).await;
    let request = prepare_request(&bind, &call, Some(json!({"opaque":"fallback"})));
    assert_eq!(
        bridge.prepare("socket_a", &request).await.unwrap().status,
        CallStatus::Completed
    );
    assert_eq!(task.await.unwrap().unwrap(), "{\"opaque\":\"fallback\"}");
    assert_eq!(
        bridge
            .match_canonical_output(
                "child_session",
                "late_prepare",
                "browser_action",
                Some(0),
                Some(1),
                Some(2),
                "{\"opaque\":\"fallback\"}",
            )
            .await,
        Some(call.invocation_id)
    );
    let mut invocation = context("deferred");
    invocation.interaction_timeout_ms = 1;
    let mut task = tokio::spawn(async move {
        tool.execute_output(json!({"presentation":{"mode":"deferred"}}), invocation)
            .await
    });
    let call = pending(&bridge).await;
    assert_eq!(call.deadline_ms, 0);
    bridge
        .prepare(
            "socket_a",
            &prepare_request(&bind, &call, Some(json!("must not apply"))),
        )
        .await
        .unwrap();
    let deferred = tokio::time::timeout(std::time::Duration::from_secs(2), &mut task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        deferred,
        microcrab_session::runtime::tool::ToolOutput::Buffered(_)
    ));
    let current = bridge.snapshot().await.pending[0].clone();
    assert_eq!(current.status, CallStatus::Pending);
    assert!(!current.timeout_applied);
    bridge
        .result(
            "socket_a",
            &ResultRequest {
                lease_id: bind.lease_id.unwrap(),
                invocation_id: call.invocation_id,
                result: Some(json!("actual answer")),
                error: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        bridge.snapshot().await.calls.last().unwrap().result,
        Some(json!("actual answer"))
    );
}

#[tokio::test]
async fn paused_calls_close_and_restart_as_unsettled_not_replayed() {
    let (directory, bridge, tool, _bind) = setup_definition(interaction_definition(5)).await;
    let first = tool.clone();
    let task = tokio::spawn(async move { first.execute(json!({}), context("close_paused")).await });
    paused(&bridge).await;
    bridge.settle_all("closed").await.unwrap();
    assert!(task.await.unwrap().is_err());
    let task =
        tokio::spawn(async move { tool.execute(json!({}), context("restart_paused")).await });
    let call = paused(&bridge).await;
    let recovered = ClientTools::open(directory.path(), "parent_session")
        .await
        .unwrap();
    assert!(recovered.snapshot().await.pending.is_empty());
    assert_eq!(
        recovered
            .snapshot()
            .await
            .calls
            .last()
            .unwrap()
            .invocation_id,
        call.invocation_id
    );
    assert_eq!(
        recovered.snapshot().await.calls.last().unwrap().status,
        CallStatus::Interrupted
    );
    bridge.settle_all("old process cleanup").await.unwrap();
    assert!(task.await.unwrap().is_err());
}

#[tokio::test]
async fn repeated_pending_epoch_updates_retain_candidates_and_strict_registration() {
    let (_directory, bridge, _tool, initial) = setup(true).await;
    let mut first = definition(true);
    first.definition.description = "first pending candidate".into();
    let mut second = first.clone();
    second.definition.description = "second pending candidate".into();
    bridge
        .register_pending(vec![first.clone()], 1)
        .await
        .unwrap();
    let first_digest = bridge.snapshot().await.catalog_digest;
    assert!(bridge.register(vec![second.clone()], 1).await.is_err());
    bridge
        .register_pending(vec![second.clone()], 1)
        .await
        .unwrap();
    let second_digest = bridge.snapshot().await.catalog_digest;
    assert_ne!(first_digest, second_digest);
    bridge
        .register_pending(vec![second.clone()], 1)
        .await
        .unwrap();
    bridge.reconcile_catalog(&[first.definition]).await.unwrap();
    assert_eq!(bridge.snapshot().await.catalog_digest, first_digest);
    bridge
        .reconcile_catalog(&[second.definition])
        .await
        .unwrap();
    assert_eq!(bridge.snapshot().await.catalog_digest, second_digest);
    bridge
        .reconcile_catalog(&[definition(true).definition])
        .await
        .unwrap();
    assert_eq!(
        bridge.snapshot().await.catalog_digest,
        initial.catalog_digest
    );
}

#[tokio::test]
async fn completed_linkage_uses_full_original_output_and_newest_identical_branch() {
    let (_directory, bridge, tool, bind) = setup(true).await;
    let original = "x".repeat(40 * 1024);
    let mut latest = String::new();
    for branch in ["first", "second"] {
        bridge
            .set_history_anchor("child_session", Some(branch.into()))
            .await
            .unwrap();
        let tool = tool.clone();
        let task =
            tokio::spawn(async move { tool.execute(json!({}), context("large_call")).await });
        let call = pending(&bridge).await;
        latest = call.invocation_id.clone();
        bridge
            .result(
                "socket_a",
                &ResultRequest {
                    lease_id: bind.lease_id.clone().unwrap(),
                    invocation_id: call.invocation_id,
                    result: Some(json!(original.clone())),
                    error: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(task.await.unwrap().unwrap(), original);
    }
    bridge.use_unlinked_receipt_fixture().await;
    assert_eq!(
        bridge
            .match_canonical_output(
                "child_session",
                "large_call",
                "foreign_tool",
                Some(0),
                Some(1),
                Some(2),
                &original
            )
            .await,
        None
    );
    assert_eq!(
        bridge
            .match_canonical_output(
                "child_session",
                "large_call",
                "browser_action",
                Some(0),
                Some(1),
                Some(2),
                &original
            )
            .await,
        Some(latest)
    );
    assert_eq!(
        bridge
            .match_canonical_output(
                "child_session",
                "large_call",
                "browser_action",
                Some(0),
                Some(1),
                Some(2),
                &original[..32 * 1024]
            )
            .await,
        None
    );
    assert_eq!(
        bridge
            .match_canonical_output(
                "child_session",
                "large_call",
                "browser_action",
                Some(1),
                Some(1),
                Some(2),
                &original
            )
            .await,
        None
    );
    assert_eq!(
        bridge
            .match_canonical_output(
                "other_session",
                "large_call",
                "browser_action",
                None,
                Some(1),
                Some(2),
                &original
            )
            .await,
        None
    );
}

#[tokio::test]
async fn arbitrary_json_never_becomes_a_runtime_receipt() {
    let (_directory, bridge, tool, _bind) = setup(true).await;
    let task =
        tokio::spawn(async move { tool.execute(json!({}), context("background_call")).await });
    pending(&bridge).await;
    bridge.use_unlinked_receipt_fixture().await;
    let receipt = json!({"task_id":42,"session_id":"child_session","status":"running","background":true,"reason":"deferred","message":"opaque text"});
    assert_eq!(
        bridge
            .match_canonical_output(
                "child_session",
                "background_call",
                "browser_action",
                Some(0),
                Some(1),
                Some(2),
                &receipt.to_string()
            )
            .await,
        None
    );
    bridge.settle_all("test cleanup").await.unwrap();
    let _ = task.await.unwrap();
}

#[tokio::test]
async fn typed_final_preserves_failure_and_content_without_semantic_json_parsing() {
    let (_directory, bridge, tool, bind) = setup(false).await;
    let native = tool.clone();
    let task = tokio::spawn(async move {
        native
            .execute_output(json!({}), context("typed_failure"))
            .await
    });
    let call = pending(&bridge).await;
    let request: model::OutputRequest = serde_json::from_value(json!({"lease_id":bind.lease_id.unwrap(),"invocation_id":call.invocation_id,"event":{"kind":"final","content":"partial detail","failure":{"kind":"timeout","code":"service_timeout","message":"Service deadline exceeded","retryable":true}}})).unwrap();
    assert!(bridge.output("other-socket", &request).await.is_err());
    bridge.output("socket_a", &request).await.unwrap();
    let outcome = task.await.unwrap().unwrap();
    match outcome {
        microcrab_session::runtime::tool::ToolOutput::Final(output) => {
            assert_eq!(output.content, "partial detail");
            let failure = output.failure.unwrap();
            assert_eq!(
                failure.kind,
                microcrab_session::runtime::execution::ToolFailureKind::Timeout
            );
            assert_eq!(failure.code, "service_timeout");
            assert!(failure.retryable);
        }
        _ => panic!("expected typed final"),
    }
}
