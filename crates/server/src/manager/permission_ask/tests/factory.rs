//! Native dependency assembly executes through the real parent-owned lifecycle.
use super::*;
use microcrab_agent_types::{AgentInfo, AgentMode, AgentRole, Variable};
use microcrab_creator::delegate::{
    NativeChildFactory, NativeDelegateConfig, OwnedDelegateExecutor,
};
use microcrab_session::runtime::BuiltinTool;

/// The parent delegates once; the native child's Model remains responsible for
/// its own effect call. Buffered updates cannot replay the parent assignment.
struct DelegateModel;
#[async_trait]
impl Client for DelegateModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let delegated = content.items.iter().any(|item| {
            matches!(item, ContentItem::Assistant { tool_calls, .. }
                if tool_calls.iter().any(|call| call.name == "delegate"))
        });
        Ok(Response {
            provider_metadata: None,
            content: "parent turn settled".into(),
            tool_calls: if delegated {
                vec![]
            } else {
                vec![ToolCall {
                    provider_metadata: None,
                    id: "native-child-delegation".into(),
                    name: "delegate".into(),
                    arguments: json!({"target":"child-agent","mode":"execute","input":"run"}),
                }]
            },
            usage: Usage::default(),
        })
    }
}

#[tokio::test]
async fn native_child_factory_injects_callback_and_preserves_child_identity() {
    let (root, bridge, lease) = setup().await;
    let count = Arc::new(AtomicUsize::new(0));
    let children = microcrab_creator::Children::default();
    let host: Arc<dyn microcrab_host_api::Host> = Arc::new(crate::noop::NoopHost);
    let effect = Arc::new(Effect(count.clone()));
    let catalog = vec![AgentInfo {
        name: "child-agent".into(),
        modes: vec![AgentMode {
            name: "execute".into(),
            description: "Execute one permission-gated fixture effect.".into(),
            prompt: "Run the assigned effect once and report its outcome.".into(),
            role: AgentRole::SubAgent,
            permissions: vec![],
        }],
        tools: vec![],
        variables: Variable::default(),
    }];
    // The issuing parent advertises the child's effect so its independent live
    // ceiling asks rather than denying an unknown tool. Factory and parent share
    // this exact controller, Host and catalog; a copied allowlist is insufficient.
    let runtime = Runtime::new(
        vec![
            ToolRegistration::Builtin(BuiltinTool::Delegate),
            ToolRegistration::Wasm(effect.clone()),
        ],
        bridge.clone(),
    )
    .unwrap();
    let factory = NativeChildFactory::new(NativeDelegateConfig {
        children: children.clone(),
        aggregate: microcrab_creator::UsageLedger::open(None).unwrap(),
        client: Arc::new(Model),
        host: host.clone(),
        wasm: vec![effect],
        external: vec![],
        catalog: catalog.clone(),
        permissions: runtime.permission_controller(),
        limits: Limits::default(),
        directory: root.path().join("children"),
        disk: false,
        system: None,
        auditors: None,
        journals: None,
    })
    .with_permission_adapter(bridge.clone());
    let parent = Session::builder(
        DelegateModel,
        runtime.with_delegate_executor(Arc::new(OwnedDelegateExecutor::new(
            children.clone(),
            Arc::new(factory),
        ))),
    )
    .session_id("parent")
    .host(host)
    .catalog(catalog)
    .permissions(PermissionConfig {
        overrides: vec![
            serde_json::from_value(json!({
                "tool":{"kind":"exact","value":"delegate"},"action":"allow"
            }))
            .unwrap(),
            serde_json::from_value(json!({
                "tool":{"kind":"exact","value":"effect"},"action":"ask"
            }))
            .unwrap(),
        ],
        review: false,
        ..Default::default()
    })
    .build()
    .unwrap();
    let mut subscription = bridge.hub.subscribe();
    let submitting = parent.clone();
    let task = tokio::spawn(async move {
        submitting
            .submit(SessionInput::text("delegate the effect"))
            .await
            .unwrap();
        let executions = submitting.executions();
        let mut changed = executions.subscribe();
        // A parent Turn result is distinct from the child Turn and its lifetime.
        // Read the committed reply from the actual bounded Buffered execution.
        let (execution_id, child_id) = loop {
            let row = executions
                .snapshot()
                .into_iter()
                .find(|row| row.tool == "delegate");
            if let Some(row) = row {
                if let Some(binding) = row.delegate_child {
                    assert_eq!(binding.target, "child-agent");
                    let page = submitting
                        .read_execution_output(row.execution_id, 0, 16 * 1024)
                        .unwrap();
                    if page.chunks.iter().any(|chunk| {
                        chunk.channel.as_deref() == Some("reply")
                            && chunk.content.lines().any(|line| {
                                serde_json::from_str::<Value>(line).is_ok_and(|reply| {
                                    reply["child_id"] == binding.session_id
                                        && reply["outcome"]["content"] == "done"
                                })
                            })
                    }) {
                        break (row.execution_id, binding.session_id);
                    }
                }
                assert!(
                    row.status.is_pending(),
                    "delegate ended before a committed child reply"
                );
            }
            changed.changed().await.unwrap();
        };
        let child = submitting.inspect_child(&child_id, "child-agent").unwrap();
        assert_eq!(child.kind(), microcrab_session::SessionKind::Delegated);
        assert_eq!(child.state().selected_mode.as_deref(), Some("execute"));
        let relationship = child.parent_relationship().unwrap();
        assert_eq!(relationship.parent_session_id, "parent");
        assert_eq!(relationship.parent_execution_id, execution_id);
        assert_eq!(relationship.target, "child-agent");
        assert_eq!(relationship.mode, "execute");
        assert!(
            executions.get(execution_id).unwrap().status.is_pending(),
            "a completed child Turn remains available until its parent closes it"
        );
        submitting
            .close_child(&child_id, "child-agent")
            .await
            .unwrap();
        while executions.get(execution_id).unwrap().status.is_pending() {
            changed.changed().await.unwrap();
        }
        assert!(
            !child.executions().has_pending(),
            "close acknowledgement includes child effects"
        );
        child_id
    });
    tokio::pin!(task);
    let mut approvals = 0;
    let mut observed_child = None;
    let child_id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                result = &mut task => break result.unwrap(),
                notification = subscription.receiver.recv() => {
                    let notification = notification.unwrap();
                    if !matches!(&notification.event, crate::manager::SessionRuntimeEvent::PermissionRequested(_)) {
                        continue;
                    }
                    let row = &notification.event.payload()["request"];
                    assert_eq!(row["parent_session_id"], "parent");
                    let child = row["session_id"].as_str().unwrap();
                    assert_ne!(child, "parent", "the callback identifies the actual delegated Session");
                    if let Some(expected) = &observed_child {
                        assert_eq!(child, expected);
                    } else {
                        observed_child = Some(child.to_owned());
                    }
                    assert_eq!(row["tool"], "effect");
                    assert_eq!(count.load(Ordering::SeqCst), 0, "the effect waits for both independent permission owners");
                    bridge.reply("socket_a", answer(row["request_id"].as_str().unwrap(), &lease, ReplyDecision::Allow)).await.unwrap();
                    approvals += 1;
                }
            }
        }
    })
    .await
    .expect("native delegation, shared permission replies and acknowledged child close must settle");
    assert!(
        approvals >= 1,
        "child uses the installed shared interaction adapter"
    );
    assert_eq!(observed_child.as_deref(), Some(child_id.as_str()));
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "the admitted child effect executes exactly once"
    );
    assert_eq!(bridge.snapshot().await["requests"], json!([]));
    parent.close().await.unwrap();
    children.close().await.unwrap();
}
