//! Real Server-owned mode dispatch through a loopback provider and the native Ask route.
use super::{
    settings::{create_mode, mode_manager},
    *,
};
use microcrab_protocol::application::Command;
use microcrab_session::{
    Journal,
    context::ContentItem,
    runtime::{
        execution::{ExecutionCapabilities, ExecutionController, ExecutionStatus, OutputAvailability},
        tool::{Tool, ToolContext, ToolOutput},
    },
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const DEADLINE: Duration = Duration::from_secs(10);

fn switch_reply(mode: &str) -> Value {
    json!({"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"change-mode","type":"function","function":{"name":"switch_mode","arguments":json!({"mode":mode}).to_string()}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}})
}

fn advertises(request: &Value) -> bool {
    request["tools"].as_array().is_some_and(|tools| {
        tools
            .iter()
            .any(|tool| tool["function"]["name"] == "switch_mode")
    })
}

async fn start_turn(manager: &Manager, managed: &Arc<Managed>, id: &str) -> Command {
    let turn = command(
        "turn.submit",
        Some(id),
        json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text("Check native mode") }),
    );
    let _gate = managed.commands.lock().await;
    commands::submit(manager, managed.clone(), &turn)
        .await
        .unwrap();
    turn
}

async fn finish_turn(managed: &Managed) {
    let worker = managed.worker.lock().await.take().expect("turn owner");
    tokio::time::timeout(DEADLINE, worker)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_switch_mode_ask_updates_next_request_and_ordered_mode_only_when_applied() {
    for (target, decision, applied) in [
        ("a", "deny", "b"),
        ("child-only", "allow", "b"),
        ("a", "allow", "a"),
    ] {
        let root = tempfile::tempdir().unwrap();
        // Block the second provider response until ordered metadata is observed.
        let provider_a = scripted_provider(1, vec![switch_reply(target)]).await;
        let provider_b = provider(0).await;
        let manager = mode_manager(root.path(), &provider_a, &provider_b, None).await;
        let managed = create_mode(&manager).await;
        let initial = managed.record.lock().await.clone();
        let id = &initial.id;
        let owner = Principal::for_test("owner");
        let bind = manager.client_tool_command(&owner, "socket_a", &command("client_tools.bind", Some(id), json!({"client_id":"client_a","catalog_digest":managed.client_tools.snapshot().await.catalog_digest}))).await.unwrap();
        let lease = bind["lease_id"].as_str().unwrap();
        let mut events = managed.hub.subscribe();
        assert_eq!(events.snapshot["session"]["mode"], "b");
        assert_eq!(events.snapshot["session"]["configured_mode"], "b");
        let initial_context = managed.session.context_snapshot().unwrap();
        let turn = start_turn(&manager, &managed, id).await;
        let ask = tokio::time::timeout(DEADLINE, async {
            loop {
                let event = events.receiver.recv().await.unwrap();
                if event.event.name() == "permission.requested" {
                    break event;
                }
            }
        })
        .await
        .expect("native Ask must precede tool dispatch");
        let request = &ask.event.payload()["request"];
        assert_eq!(request["tool"], "switch_mode");
        assert_eq!(request["mode"], "b");
        assert_eq!(managed.hub.snapshot()["session"]["mode"], "b");
        manager.permission_reply(&owner, "socket_a", &command("permission.reply", Some(id), json!({"request_id":request["request_id"],"lease_id":lease,"decision":decision}))).await.unwrap();
        tokio::time::timeout(DEADLINE, async {
            for _ in 0..2 {
                provider_a.wire.entered.acquire().await.unwrap().forget();
            }
        })
        .await
        .expect("provider must receive the next round");
        let before_completion = managed.hub.snapshot();
        assert_eq!(before_completion["session"]["mode"], applied);
        assert_eq!(before_completion["session"]["configured_mode"], "b");
        assert_eq!(before_completion["session"]["revision"], initial.revision);
        assert_eq!(
            manager.record("owner", id).await.unwrap().revision,
            initial.revision
        );
        if applied == "a" {
            let patch = tokio::time::timeout(DEADLINE, async {
                loop {
                    let event = events.receiver.recv().await.unwrap();
                    if event.event.name() == "session.meta"
                        && event.event.payload()["session"]["mode"] == "a"
                    {
                        break event;
                    }
                }
            })
            .await
            .expect("mode produces an ordered metadata patch");
            assert!(patch.sequence > ask.sequence);
            assert_eq!(patch.event.payload()["session"]["configured_mode"], "b");
        }
        {
            let requests = provider_a.wire.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert!(requests.iter().all(|(_, request)| advertises(request)));
            assert_eq!(requests[0].1["tools"], requests[1].1["tools"]);
            assert_eq!(requests[0].1["messages"][0], requests[1].1["messages"][0]);
        }
        provider_a.wire.release.add_permits(1);
        finish_turn(&managed).await;
        let operation = manager
            .directory
            .get_operation("owner", turn.params["operation_id"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(operation.status, "completed");
        let settled = managed.session.context_snapshot().unwrap();
        assert_eq!(settled.epoch, initial_context.epoch);
        assert_eq!(settled.tools, initial_context.tools);
        let mut projected = json!({});
        for item in &settled.items {
            if let ContentItem::State(frame) = item {
                microcrab_transport::apply_state_patch(&mut projected, frame).unwrap();
            }
        }
        assert_eq!(projected["mode"], applied);
        assert_eq!(
            projected["mode_prompt"],
            if applied == "a" {
                "Mode A instruction"
            } else {
                "Mode B instruction"
            }
        );
        let record = manager.record("owner", id).await.unwrap();
        assert_eq!(record.mode.as_deref(), Some("b"));
        // The Server's normal terminal turn settlement advances the Session
        // revision once. A native switch need not write Directory metadata.
        assert_eq!(record.revision, initial.revision + 1);
        assert_eq!(
            managed
                .journal
                .head_recover()
                .await
                .unwrap()
                .unwrap()
                .state
                .selected_mode
                .as_deref(),
            Some(applied)
        );
        manager.close().await.unwrap();
    }
}

struct PublicationProbe {
    observer: std::thread::ThreadId,
    capability_reads: std::sync::atomic::AtomicUsize,
    cleanup_entered: Semaphore,
    cleanup_ack: Semaphore,
}

#[async_trait::async_trait]
impl ExecutionController for PublicationProbe {
    fn capabilities(&self) -> ExecutionCapabilities {
        if std::thread::current().id() == self.observer {
            self.capability_reads.fetch_add(1, Ordering::Relaxed);
        }
        ExecutionCapabilities::default()
    }

    async fn cancel(&self, _: CancellationToken) -> Result<(), Error> {
        Ok(())
    }

    async fn release(&self, _: CancellationToken) -> Result<(), Error> {
        self.cleanup_entered.add_permits(1);
        self.cleanup_ack
            .acquire()
            .await
            .map_err(|_| Error::Closed)?
            .forget();
        Ok(())
    }
}

struct BufferedProbe(Arc<PublicationProbe>);

#[async_trait::async_trait]
impl Tool for BufferedProbe {
    fn definition(&self) -> microcrab_protocol::ToolDefinition {
        microcrab_protocol::ToolDefinition {
            name: "buffered_probe".into(),
            description: "Produce terminal retained output".into(),
            parameters: json!({"type":"object","additionalProperties":false}),
        }
    }

    fn permission_defaults(&self) -> Vec<microcrab_session::policy::Permission> {
        vec![
            serde_json::from_value(
                json!({"tool":{"kind":"exact","value":"buffered_probe"},"action":"allow"}),
            )
            .unwrap(),
        ]
    }

    async fn execute(&self, _: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        let (writer, output) = context.open_output(Default::default(), self.0.clone())?;
        writer.append(None, "retained output")?;
        writer.finish(Ok(()))?;
        Ok(ToolOutput::Buffered(output))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_publication_does_not_project_executions_and_attach_survives_release() {
    use super::super::statistics::PublicationWitness;

    let root = tempfile::tempdir().unwrap();
    let provider = scripted_provider(
        2,
        vec![json!({"choices":[{"message":{
        "role":"assistant","content":null,"tool_calls":[{"id":"buffered-call","type":"function",
        "function":{"name":"buffered_probe","arguments":"{}"}}]},"finish_reason":"tool_calls"}]})],
    )
    .await;
    let manager = mode_manager(root.path(), &provider, &provider, None).await;
    let managed = create_mode(&manager).await;
    let probe = Arc::new(PublicationProbe {
        observer: std::thread::current().id(),
        capability_reads: 0.into(),
        cleanup_entered: Semaphore::new(0),
        cleanup_ack: Semaphore::new(0),
    });
    managed
        .session
        .replace_external_tools(vec![Arc::new(BufferedProbe(probe.clone()))])
        .await
        .unwrap();
    submit(&manager, &managed, "Retain terminal output").await;
    let executions = managed.session.executions();
    let buffered = executions
        .snapshot()
        .into_iter()
        .find(|row| row.tool == "buffered_probe")
        .unwrap();
    assert_eq!(buffered.status, ExecutionStatus::Succeeded);
    assert!(matches!(
        buffered.output_availability,
        Some(OutputAvailability::Retained)
    ));
    let output = buffered.output.unwrap();
    assert!(output.closed && output.start_cursor < output.end_cursor);

    let record = managed.record.lock().await.clone();
    let candidate = managed.hub.snapshot();
    let fresh = PublicationWitness::capture(&managed, &record).unwrap();
    let stale = PublicationWitness::capture(&managed, &record).unwrap();
    // The Tokio test root is separate from its workers. Prove that this exact
    // publication does not traverse even one live Execution controller. Old
    // code fails here without inducing an uninterruptible mutex deadlock.
    assert!(
        managed
            .session
            .state()
            .executions
            .iter()
            .any(|row| row.execution_id == buffered.execution_id)
    );
    assert!(probe.capability_reads.swap(0, Ordering::Relaxed) > 0);
    assert!(
        fresh
            .publish(&managed, candidate.clone(), true)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        probe.capability_reads.load(Ordering::Relaxed),
        0,
        "snapshot publication must not visit the Execution registry"
    );

    let context = managed.session.context_measurement().unwrap();
    let head = managed.journal.cursor().await;
    {
        let live_record = managed.record.lock().await;
        assert_eq!(live_record.revision, record.revision);
        managed.session.select_mode("a").await.unwrap();
        managed.hub.applied_mode(Some("a"));
    }
    assert_eq!(
        managed.session.context_measurement().unwrap().fingerprint,
        context.fingerprint
    );
    assert_eq!(managed.journal.cursor().await, head);
    assert_eq!(candidate["session"]["mode"], "b");
    assert!(
        stale
            .publish(&managed, candidate, true)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(managed.hub.snapshot()["session"]["mode"], "a");

    let id = buffered.execution_id;
    let release = tokio::spawn({
        let executions = executions.clone();
        async move { executions.release(id, CancellationToken::new()).await }
    });
    tokio::time::timeout(DEADLINE, probe.cleanup_entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(matches!(
        executions.get(id).unwrap().output_availability,
        Some(OutputAvailability::Retained)
    ));
    let owner = Principal::for_test("owner");
    let (subscription, released) = tokio::time::timeout(DEADLINE, async {
        tokio::join!(manager.subscribe(&owner, &record.id), async {
            probe.cleanup_ack.add_permits(1);
            release.await.unwrap()
        })
    })
    .await
    .expect("release and active attach must both settle");
    released.unwrap();
    let snapshot = subscription.unwrap().snapshot;
    assert_eq!(snapshot["session"]["mode"], "a");
    assert_eq!(snapshot["session"]["configured_mode"], "b");
    assert!(matches!(
        executions.get(id).unwrap().output_availability,
        Some(OutputAvailability::Released)
    ));
    manager.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switch_mode_restore_and_fork_preserve_selected_mode_and_old_catalog_until_update() {
    let root = tempfile::tempdir().unwrap();
    let a = provider(10).await;
    let b = provider(0).await;
    let original = mode_manager(root.path(), &a, &b, None).await;
    let source = create_mode(&original).await;
    let id = source.record.lock().await.id.clone();
    // Simulate an immutable pre-feature checkpoint from an older Server.
    let mut old = source.journal.head_recover().await.unwrap().unwrap();
    assert!(
        old.context
            .tools
            .iter()
            .any(|tool| tool.name == "switch_mode")
    );
    old.context.tools.retain(|tool| tool.name != "switch_mode");
    let config = original.config.clone();
    let host = original.host.clone();
    original.close().await.unwrap();
    let record = source.record.lock().await.clone();
    let (journal, _) = original.inspect_journal(&record).await.unwrap();
    journal.commit(old.clone()).await.unwrap();
    journal.close().await.unwrap();
    let manager = Manager::with_host(config, host).await.unwrap();
    let source = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    assert_eq!(
        source.session.context_snapshot().unwrap().tools,
        old.context.tools
    );
    submit(&manager, &source, "Observe frozen catalog").await;
    assert!(!advertises(&a.wire.requests.lock().unwrap()[0].1));
    let epoch = source.session.context_snapshot().unwrap().epoch;
    let before = source.journal.cursor().await;
    let record_revision = source.record.lock().await.revision;
    let owner = Principal::for_test("owner");
    let manual = manager
        .command(
            &owner,
            &command(
                "session.set_mode",
                Some(&id),
                json!({"mode":"a","expected_revision":record_revision}),
            ),
        )
        .await
        .unwrap();
    assert_eq!(manual["session"]["mode"], "a");
    assert_eq!(manual["session"]["configured_mode"], "a");
    assert_eq!(source.journal.cursor().await, before);
    let forked = manager.command(&owner, &command("session.fork", Some(&id), json!({"operation_id":uuid::Uuid::new_v4().to_string(),"expected_revision":manual["session"]["revision"]}))).await.unwrap();
    assert_eq!(
        forked["mode"], "b",
        "fork starts from the selected Journal, not the live/manual mode"
    );
    assert_eq!(forked["configured_mode"], "a");
    let fork_id = forked["id"].as_str().unwrap();
    let destination = manager
        .active(manager.record("owner", fork_id).await.unwrap(), true)
        .await
        .unwrap();
    assert_eq!(
        destination.session.context_snapshot().unwrap().tools,
        old.context.tools
    );
    assert_eq!(source.session.state().selected_mode.as_deref(), Some("a"));
    assert_eq!(source.journal.cursor().await, before);
    commands::close_session(&manager, &destination, "owner", fork_id, None)
        .await
        .unwrap();
    let record = manager.record("owner", fork_id).await.unwrap();
    let dormant = manager.inspect(&record).await.unwrap();
    assert_eq!(dormant["session"]["mode"], "b");
    assert_eq!(dormant["session"]["configured_mode"], "a");
    let reopened = manager.active(record, true).await.unwrap();
    assert_eq!(reopened.session.state().selected_mode.as_deref(), Some("b"));
    assert_eq!(
        reopened.session.context_snapshot().unwrap().tools,
        old.context.tools
    );

    let update = manager.command(&owner, &command(commands::CLIENT_TOOLS_SET, Some(&id), json!({"expected_revision":manual["session"]["revision"],"definitions":[{"name":"explicit_catalog_boundary","description":"Explicit new catalog","parameters":{"type":"object"}}]}))).await.unwrap();
    assert_eq!(update["pending_catalog_change"], true);
    assert_eq!(
        source.session.context_snapshot().unwrap().tools,
        old.context.tools
    );
    submit(&manager, &source, "Apply approved catalog update").await;
    assert!(advertises(&a.wire.requests.lock().unwrap()[1].1));
    assert_eq!(source.session.context_snapshot().unwrap().epoch, epoch + 1);
    manager.close().await.unwrap();
}
