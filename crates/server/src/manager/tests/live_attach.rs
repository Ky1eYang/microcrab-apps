//! Reattachment reads admitted live Context while an owned turn is still running.
use super::*;
use microcrab_protocol::ToolCall;

struct AttachModel {
    calls: AtomicUsize,
    entered: [Semaphore; 2],
    release: [Semaphore; 2],
}
impl AttachModel {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            entered: [Semaphore::new(0), Semaphore::new(0)],
            release: [Semaphore::new(0), Semaphore::new(0)],
        })
    }
    async fn entered(&self, round: usize) {
        self.entered[round].acquire().await.unwrap().forget();
    }
}
#[async_trait::async_trait]
impl microcrab_session::Client for AttachModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        if content.subagent.as_deref() == Some("rename") {
            assert!(content.tools.is_empty());
            assert_eq!(content.items.len(), 1);
            assert!(
                !serde_json::to_string(&content.items)
                    .unwrap()
                    .contains("accepted first reply")
            );
            assert!(content.max_bytes > 0);
            return Ok(Response {
                provider_metadata: None,
                content: "Independent title".into(),
                tool_calls: vec![],
                usage: Usage::default(),
            });
        }
        let round = self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(
            round < 2,
            "reattachment must not submit or replay a model request"
        );
        self.entered[round].add_permits(1);
        tokio::select! {
            _ = token.cancelled() => return Err(microcrab_transport::Error::Cancelled),
            permit = self.release[round].acquire() => permit.unwrap().forget(),
        }
        Ok(Response {
            provider_metadata: None,
            content: if round == 0 {
                "accepted first reply"
            } else {
                "final reply"
            }
            .into(),
            tool_calls: if round == 0 {
                vec![ToolCall {
                    provider_metadata: None,
                    id: "attach-tool-call".into(),
                    name: "sleep".into(),
                    arguments: serde_json::json!({"time_ms":60_000}),
                }]
            } else {
                vec![]
            },
            usage: Usage::default(),
        })
    }
}
fn has_text(snapshot: &Value, text: &str) -> bool {
    snapshot["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["text"] == text)
}
fn message_ids(snapshot: &Value) -> Vec<String> {
    snapshot["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap().to_owned())
        .collect()
}
async fn running_attach(storage: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let model = AttachModel::new();
        let (manager, _) = manager(model.clone(), storage).await;
        let created = commands::create(&manager, "owner", &command("session.create", None,
            serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
        let id = created["id"].as_str().unwrap();
        let managed = manager.active(manager.record("owner", id).await.unwrap(), true).await.unwrap();
        let initial = manager.inspect_subscription(&manager.record("owner", id).await.unwrap()).await.unwrap();
        assert!(initial.snapshot["messages"].as_array().unwrap().is_empty());
        drop(initial);
        let operation = uuid::Uuid::new_v4().to_string();
        commands::submit(&manager, managed.clone(), &command("turn.submit", Some(id),
            serde_json::json!({"operation_id":operation,"input":microcrab_session::SessionInput::text("keep this admitted input")}))).await.unwrap();
        model.entered(0).await;

        // No provider response has completed. A fresh attach and HTTP inspection
        // must nevertheless include the already admitted user input.
        let record = manager.record("owner", id).await.unwrap();
        let first = manager.inspect_subscription(&record).await.unwrap();
        assert_eq!(first.snapshot["session"]["status"], "running");
        assert!(has_text(&first.snapshot, "keep this admitted input"));
        assert!(!has_text(&first.snapshot, "accepted first reply"));
        assert!(has_text(&manager.inspect(&record).await.unwrap(), "keep this admitted input"));
        drop(first);

        let registry = managed.session.executions();
        let mut execution_changes = registry.subscribe();
        model.release[0].add_permits(1);
        let sleeping = loop {
            if let Some(row) = registry.snapshot().into_iter().find(|row|
                row.call_id == "attach-tool-call"
                    && row.status == microcrab_session::runtime::execution::ExecutionStatus::Running)
            {
                break row.execution_id;
            }
            execution_changes.changed().await.unwrap();
        };
        let pending = manager.inspect_subscription(&record).await.unwrap();
        assert!(has_text(&pending.snapshot, "keep this admitted input"));
        assert!(has_text(&pending.snapshot, "accepted first reply"));
        assert!(!pending.snapshot["messages"].as_array().unwrap().iter().any(|row|
            row["role"] == "tool" && row["call_id"] == "attach-tool-call"),
            "synthetic interrupted recovery receipts must not appear as live results");
        assert!(managed.session.context_snapshot().is_err(),
            "a pending tool response is visible material, not a settled recovery snapshot");
        let rename = managed.session.rename_with_model("an independent title").await.unwrap();
        assert_eq!(rename.content, "Independent title");
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        assert!(managed.session.context_snapshot().is_err());
        assert!(managed.session.messages().iter().any(|item| matches!(item,
            microcrab_session::context::ContentItem::Assistant { content, .. }
                if content == "accepted first reply")));
        drop(pending);
        registry.cancel(sleeping).await.unwrap();
        model.entered(1).await;
        // A complete response and its real tool result were admitted, but the
        // invocation is still blocked on another provider call. This was the
        // former stale-Context window: switching back returned the empty base.
        let second = manager.inspect_subscription(&record).await.unwrap();
        assert_eq!(second.snapshot["session"]["status"], "running");
        assert!(has_text(&second.snapshot, "keep this admitted input"));
        assert!(has_text(&second.snapshot, "accepted first reply"));
        assert!(second.snapshot["messages"].as_array().unwrap().iter().any(|row|
            row["role"] == "tool" && row["call_id"] == "attach-tool-call"));
        let ids = message_ids(&second.snapshot);
        let again = manager.inspect_subscription(&record).await.unwrap();
        assert_eq!(message_ids(&again.snapshot), ids, "reattach must not duplicate or replace admitted identities");
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        assert_eq!(manager.directory.get_operation("owner", &operation).await.unwrap().unwrap().status, "accepted");
        let mut view = again.snapshot.clone();
        let mut receiver = again.receiver;
        drop(second);
        model.release[1].add_permits(1);
        managed.worker.lock().await.take().unwrap().await.unwrap();
        // The receiver registered with the snapshot still receives the final
        // semantic snapshot, strictly after the captured subscription sequence.
        loop {
            let event = receiver.recv().await.unwrap();
            if crate::manager::publication::apply(&mut view, event.event.name(), event.event.payload())
                && has_text(&view, "final reply")
            {
                assert!(event.sequence > again.sequence);
                assert!(has_text(&view, "keep this admitted input"));
                assert!(has_text(&view, "accepted first reply"));
                break;
            }
        }
        assert_eq!(managed.hub.snapshot()["session"]["status"], "idle");
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        manager.close().await.unwrap();
    }).await.expect("running Attach must not wait for the model or the whole invocation");
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_running_attach_restores_admitted_input_and_prior_round_before_turn_completion() {
    running_attach("file").await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_running_attach_restores_admitted_input_and_prior_round_before_turn_completion() {
    running_attach("sqlite").await;
}
