//! Real active Session/registry/bridge settlement, without a browser or model network.
use super::*;
use microcrab_auditor::{Auditor, VariableView};
use microcrab_protocol::{Response, ToolCall};
use microcrab_session::{
    Client, CompletedTurn, Limits, Session,
    context::{Content, ContentItem},
    runtime::{
        Runtime,
        execution::{ExecutionId, ExecutionStatus},
        tool::AllowTools,
    },
};
use serde_json::json;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct QuestionModel(Mutex<Vec<Content>>);
#[async_trait]
impl Client for QuestionModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let mut seen = self.0.lock().unwrap();
        let first = seen.is_empty();
        seen.push(content.clone());
        Ok(Response {
            content: if first {
                String::new()
            } else {
                "Waiting for the deferred answer".into()
            },
            tool_calls: if first {
                vec![ToolCall {
                    id: "question-call".into(),
                    name: "ask_user_question".into(),
                    arguments: json!({"mode":"deferred"}),
                    provider_metadata: None,
                }]
            } else {
                vec![]
            },
            provider_metadata: None,
            usage: Default::default(),
        })
    }
}

struct Fixture {
    directory: tempfile::TempDir,
    bridge: Arc<ClientTools>,
    session: Session,
    model: Arc<QuestionModel>,
    answer: ResultRequest,
    turns: broadcast::Receiver<CompletedTurn>,
    events: Arc<Mutex<Vec<ClientToolPhase>>>,
    execution: ExecutionId,
}
impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let bridge = ClientTools::open(directory.path(), "deferred_session")
            .await
            .unwrap();
        bridge
            .register(
                vec![ClientToolDefinition {
                    definition: ToolDefinition {
                        name: "ask_user_question".into(),
                        description: "Frontend-defined deferred question".into(),
                        parameters: json!({"type":"object"}),
                    },
                    permissions: vec![],
                    reclaimable: true,
                    interaction: Some(ClientInteraction {
                        mode_pointer: "/mode".into(),
                        wait_timeout_ms: 300_000,
                    }),
                }],
                0,
            )
            .await
            .unwrap();
        let bound = bridge
            .bind(
                "socket",
                &BindRequest {
                    client_id: "browser".into(),
                    catalog_digest: bridge.snapshot().await.catalog_digest,
                },
            )
            .await
            .unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = events.clone();
        bridge.set_observer(Arc::new(move |event| {
            observed.lock().unwrap().push(event.phase);
        }));
        let runtime = Runtime::new(
            bridge.registrations().await,
            Arc::new(AllowTools::new(["ask_user_question"])),
        )
        .unwrap();
        let model = Arc::new(QuestionModel::default());
        let session = Session::builder_arc(model.clone(), runtime)
            .session_id("deferred_session")
            .limits(Limits::default())
            .build()
            .unwrap();
        let mut turns = session.subscribe_turns();
        assert_eq!(
            session
                .submit("Show a deferred frontend question")
                .await
                .unwrap()
                .rounds,
            2
        );
        assert_eq!(turns.recv().await.unwrap().sequence, 1);
        assert_eq!(model.0.lock().unwrap().len(), 2);
        let call = bridge.snapshot().await.pending.into_iter().next().unwrap();
        assert_eq!(call.blocking_mode, Some(InteractionMode::Deferred));
        let execution = ExecutionId(call.execution_id.unwrap());
        assert!(
            session
                .executions()
                .get(execution)
                .unwrap()
                .status
                .is_pending()
        );
        Self {
            directory,
            bridge,
            session,
            model,
            turns,
            events,
            execution,
            answer: ResultRequest {
                lease_id: bound.lease_id.unwrap(),
                invocation_id: call.invocation_id,
                result: Some(json!({"answer":"selected"})),
                error: None,
            },
        }
    }
    async fn assert_no_framework_turn(&mut self) {
        let model_calls = self.model.0.lock().unwrap().len();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), self.turns.recv())
                .await
                .is_err()
        );
        assert_eq!(self.model.0.lock().unwrap().len(), model_calls);
    }
    async fn assert_one_framework_turn(&mut self, expected_status: &str) {
        let turn = tokio::time::timeout(Duration::from_secs(3), self.turns.recv())
            .await
            .expect("terminal producer wakes the active idle dispatcher")
            .unwrap();
        assert_eq!(turn.sequence, 2);
        {
            let seen = self.model.0.lock().unwrap();
            assert_eq!(
                seen.len(),
                3,
                "one framework turn makes one additional model request"
            );
            let content = &seen[2];
            assert_ne!(seen[1].turn_id, content.turn_id);
            assert!(content.items.iter().any(|item| matches!(
                item,
                ContentItem::User {
                    source: Some(microcrab_transport::UserSource::Framework),
                    ..
                }
            )));
            let mut projected = json!({});
            for item in &content.items {
                if let ContentItem::State(frame) = item {
                    microcrab_transport::apply_state_patch(&mut projected, frame).unwrap();
                }
            }
            let row = projected["executions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|row| row["execution_id"] == self.execution.0)
                .unwrap();
            assert_eq!(row["tool"], "ask_user_question");
            assert_eq!(row["status"], expected_status);
        }
        self.assert_no_framework_turn().await;
    }
    fn output(&self) -> String {
        self.session
            .executions()
            .read(self.execution, 0, 65_536)
            .unwrap()
            .chunks
            .iter()
            .map(|chunk| chunk.content.as_str())
            .collect()
    }
    async fn assert_unsettled(&self) {
        let state = self.bridge.state.lock().await;
        assert!(state.durable.calls[0].status.unsettled());
        assert!(state.writers.contains_key(&self.answer.invocation_id));
        assert!(state.implicit_buffers.contains(&self.answer.invocation_id));
        let (_, disk) = Store::open(self.directory.path(), "deferred_session").unwrap();
        assert!(disk.calls[0].status.unsettled());
        assert!(
            !self
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event == &ClientToolPhase::Settled)
        );
    }
}

#[tokio::test]
async fn deferred_card_has_no_wake_and_answer_finishes_once_even_when_repeated() {
    let mut fixture = Fixture::new().await;
    fixture.assert_no_framework_turn().await;
    let settled = fixture
        .bridge
        .result("socket", &fixture.answer)
        .await
        .unwrap();
    assert_eq!(settled.status, CallStatus::Completed);
    assert_eq!(
        fixture
            .session
            .executions()
            .get(fixture.execution)
            .unwrap()
            .status,
        ExecutionStatus::Succeeded
    );
    fixture.assert_one_framework_turn("succeeded").await;
    let expected = fixture.answer.result.as_ref().unwrap().to_string();
    assert_eq!(fixture.output(), expected);
    assert_eq!(
        fixture
            .bridge
            .result("socket", &fixture.answer)
            .await
            .unwrap(),
        settled
    );
    fixture.assert_no_framework_turn().await;
    assert_eq!(fixture.output(), expected);
    assert_eq!(
        fixture
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| *event == &ClientToolPhase::Settled)
            .count(),
        1
    );
    let state = fixture.bridge.state.lock().await;
    assert!(state.deferred_completions.is_empty());
    assert!(state.writers.is_empty());
    drop(state);
    fixture.session.close().await.unwrap();
}

#[tokio::test]
async fn rejected_failure_bounds_leave_question_pending_and_allow_a_valid_answer() {
    let mut fixture = Fixture::new().await;
    let invalid = ResultRequest {
        result: None,
        error: Some("x".repeat(4097)),
        ..fixture.answer.clone()
    };
    assert!(fixture.bridge.result("socket", &invalid).await.is_err());
    fixture.assert_unsettled().await;
    assert!(
        fixture
            .session
            .executions()
            .get(fixture.execution)
            .unwrap()
            .status
            .is_pending()
    );
    assert_eq!(fixture.output(), "");
    fixture.assert_no_framework_turn().await;
    fixture
        .bridge
        .result("socket", &fixture.answer)
        .await
        .unwrap();
    fixture.assert_one_framework_turn("succeeded").await;
    fixture.session.close().await.unwrap();
}

#[tokio::test]
async fn actual_writer_finish_failure_is_propagated_without_settlement_or_lost_retry() {
    let mut fixture = Fixture::new().await;
    // A real writer whose owning Session/registry has gone away exercises the
    // finish error itself, independently of result validation or filesystem ACLs.
    let foreign = Fixture::new().await;
    let stale = foreign.bridge.state.lock().await.writers[&foreign.answer.invocation_id].clone();
    foreign.session.close().await.unwrap();
    drop(foreign);
    assert!(stale.finish(Ok(())).is_err());
    let original = fixture
        .bridge
        .state
        .lock()
        .await
        .writers
        .insert(fixture.answer.invocation_id.clone(), stale)
        .unwrap();
    let error_answer = ResultRequest {
        result: None,
        error: Some("Client failed".into()),
        ..fixture.answer.clone()
    };
    assert!(
        fixture
            .bridge
            .result("socket", &error_answer)
            .await
            .is_err()
    );
    fixture.assert_unsettled().await;
    assert!(
        fixture
            .session
            .executions()
            .get(fixture.execution)
            .unwrap()
            .status
            .is_pending()
    );
    assert_eq!(fixture.output(), "");
    fixture.assert_no_framework_turn().await;
    fixture
        .bridge
        .state
        .lock()
        .await
        .writers
        .insert(fixture.answer.invocation_id.clone(), original);
    fixture
        .bridge
        .result("socket", &fixture.answer)
        .await
        .unwrap();
    fixture.assert_one_framework_turn("succeeded").await;
    assert_eq!(
        fixture.output(),
        fixture.answer.result.as_ref().unwrap().to_string()
    );
    fixture.session.close().await.unwrap();
}

#[tokio::test]
async fn storage_failure_after_finish_retries_without_reappending_or_a_second_wake() {
    let mut fixture = Fixture::new().await;
    // Use the existing store byte budget to fail before any filesystem write.
    // This deliberately avoids permission changes or environmental fault repair.
    let original_description = {
        let mut state = fixture.bridge.state.lock().await;
        std::mem::replace(
            &mut state.durable.catalogs[0].definitions[0]
                .definition
                .description,
            "x".repeat(MAX_STORE_BYTES),
        )
    };
    assert!(
        fixture
            .bridge
            .result("socket", &fixture.answer)
            .await
            .is_err()
    );
    fixture.assert_unsettled().await;
    assert_eq!(
        fixture
            .session
            .executions()
            .get(fixture.execution)
            .unwrap()
            .status,
        ExecutionStatus::Succeeded
    );
    fixture.assert_one_framework_turn("succeeded").await;
    let output = fixture.output();
    assert_eq!(output, fixture.answer.result.as_ref().unwrap().to_string());
    fixture.bridge.state.lock().await.durable.catalogs[0].definitions[0]
        .definition
        .description = original_description;
    let mut conflict = fixture.answer.clone();
    conflict.result = Some(json!("different answer"));
    assert!(fixture.bridge.result("socket", &conflict).await.is_err());
    let settled = fixture
        .bridge
        .result("socket", &fixture.answer)
        .await
        .unwrap();
    assert_eq!(settled.status, CallStatus::Completed);
    assert_eq!(fixture.output(), output);
    fixture.assert_no_framework_turn().await;
    let (_, disk) = Store::open(fixture.directory.path(), "deferred_session").unwrap();
    assert_eq!(disk.calls[0], settled);
    assert!(
        fixture
            .bridge
            .state
            .lock()
            .await
            .deferred_completions
            .is_empty()
    );
    fixture.session.close().await.unwrap();
}

#[tokio::test]
async fn cancellation_completes_the_same_producer_and_rejects_a_late_answer() {
    let mut fixture = Fixture::new().await;
    fixture
        .session
        .executions()
        .cancel(fixture.execution)
        .await
        .unwrap();
    assert_eq!(
        fixture.bridge.snapshot().await.calls[0].status,
        CallStatus::Cancelled
    );
    assert_eq!(
        fixture
            .session
            .executions()
            .get(fixture.execution)
            .unwrap()
            .status,
        ExecutionStatus::Cancelled
    );
    fixture.assert_one_framework_turn("cancelled").await;
    assert!(
        fixture
            .bridge
            .result("socket", &fixture.answer)
            .await
            .is_err()
    );
    fixture.assert_no_framework_turn().await;
    fixture.session.close().await.unwrap();
}

#[tokio::test]
async fn long_close_reason_uses_interruption_without_failure_finish_bounds() {
    let mut fixture = Fixture::new().await;
    let reason = "x".repeat(16 * 1024);
    fixture.bridge.settle_all(&reason).await.unwrap();
    let settled = fixture.bridge.snapshot().await.calls[0].clone();
    assert_eq!(settled.status, CallStatus::Interrupted);
    assert_eq!(settled.error.as_deref(), Some(reason.as_str()));
    assert_eq!(
        fixture
            .session
            .executions()
            .get(fixture.execution)
            .unwrap()
            .status,
        ExecutionStatus::Interrupted
    );
    fixture.assert_one_framework_turn("interrupted").await;
    assert!(
        fixture
            .bridge
            .result("socket", &fixture.answer)
            .await
            .is_err()
    );
    fixture.assert_no_framework_turn().await;
    fixture.session.close().await.unwrap();
}
