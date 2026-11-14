//! Failed previews are durable display facts, never an admitted model response.
use super::*;
use microcrab_transport::{IdleTimeout, ModelObservation, ModelObserver, ObservationState};
use serde_json::json;

const PARTIAL_TEXT: &str = "This visible answer remains incomplete.";
const PARTIAL_REASONING: &str = "Unfinished reasoning fragment.";

struct StreamFailureModel {
    calls: AtomicUsize,
    entered: Semaphore,
    release: Semaphore,
    requests: std::sync::Mutex<Vec<Content>>,
}
impl StreamFailureModel {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            requests: std::sync::Mutex::new(vec![]),
        })
    }
    async fn entered(&self) {
        self.entered.acquire().await.unwrap().forget();
    }
}
#[async_trait::async_trait]
impl microcrab_session::Client for StreamFailureModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        cancellation: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(content.clone());
        self.entered.add_permits(1);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(microcrab_transport::Error::Cancelled),
            permit = self.release.acquire() => permit.unwrap().forget(),
        }
        if call == 0 {
            Err(microcrab_transport::Error::ProviderIdleTimeout(
                IdleTimeout {
                    timeout_ms: 60_000,
                    elapsed_ms: 95_000,
                    first_token_ms: Some(10_000),
                    streamed_bytes: 512,
                    streamed_chars: PARTIAL_TEXT.chars().count() as u64,
                },
            ))
        } else {
            Ok(Response {
                provider_metadata: None,
                content: "Accepted retry answer".into(),
                tool_calls: vec![],
                usage: Usage::default(),
            })
        }
    }
}

fn failure(snapshot: &Value) -> Option<&Value> {
    snapshot["operations"]
        .as_array()?
        .iter()
        .find(|operation| operation["method"] == "turn.inbox" && operation["status"] == "failed")
}
fn assert_artifact(snapshot: &Value, call: &str, failed_id: &str) {
    let failure = failure(snapshot).expect("failed autonomous execution receipt");
    assert_eq!(failure["id"], failed_id);
    let result = &failure["result"];
    assert_eq!(result["error"]["code"], "provider_timeout");
    assert_eq!(
        result["error"]["details"],
        json!({
            "reason":"idle_timeout", "timeout_ms":60_000, "elapsed_ms":95_000,
            "first_token_ms":10_000, "streamed_bytes":512,
            "streamed_chars":PARTIAL_TEXT.chars().count(),
        })
    );
    assert_eq!(result["partial_output"]["call_id"], call);
    assert_eq!(result["partial_output"]["text"], PARTIAL_TEXT);
    assert_eq!(result["partial_output"]["reasoning"], PARTIAL_REASONING);
    assert_eq!(result["partial_output"]["truncated"], false);
    assert!(result["after_message_id"].is_string());
    assert_eq!(result["retry_ready"], true);
    assert_eq!(
        result["partial_output"]["epoch"],
        result["position"]["epoch"]
    );
    assert_eq!(result["partial_output"]["turn"], result["position"]["turn"]);
    assert_eq!(
        result["partial_output"]["round"],
        result["position"]["round"]
    );
    assert!(!snapshot["messages"].to_string().contains(PARTIAL_TEXT));
    assert!(!snapshot["messages"].to_string().contains(PARTIAL_REASONING));
}

async fn durable_stream_failure(storage: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let model = StreamFailureModel::new();
        let (manager, _root) = manager(model.clone(), storage).await;
        let owner = Principal::for_test("owner");
        let created = manager.command(&owner, &command("session.create", None,
            json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
        let id = created["id"].as_str().unwrap().to_owned();
        let managed = manager.active(manager.record("owner", &id).await.unwrap(), true).await.unwrap();
        let admission = manager.command(&owner, &command("turn.queue", Some(&id),
            json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text("Keep the admitted request boundary")}))).await.unwrap();
        assert_eq!(admission["status"], "completed");
        model.entered().await;
        let request = model.requests.lock().unwrap()[0].clone();
        let state = managed.session.state();
        assert_eq!(request.epoch, state.epoch_id);
        assert_eq!(request.turn_id, state.turn_id);
        assert_eq!(request.round, state.round_id);
        let call = uuid::Uuid::new_v4().to_string();
        for observation in [
            ObservationState::Started,
            ObservationState::ReasoningDelta { text: PARTIAL_REASONING.into() },
            ObservationState::TextDelta { text: PARTIAL_TEXT.into() },
            ObservationState::Invalidated { reason: "timed_out".into() },
        ] {
            // Inject only the real advisory transport boundary; the failure itself
            // must still flow through the actual Session worker and inbox observer.
            ModelObserver::publish(managed.hub.as_ref(), ModelObservation {
                call_id: call.clone(), epoch: request.epoch, turn: request.turn_id,
                round: request.round, subagent: None, state: observation,
            });
        }
        let mut events = managed.hub.subscribe().receiver;
        model.release.add_permits(1);
        let snapshot = loop {
            let snapshot = managed.hub.snapshot();
            if failure(&snapshot).is_some() { break snapshot; }
            events.recv().await.unwrap();
        };
        let failed_id = failure(&snapshot).unwrap()["id"].as_str().unwrap().to_owned();
        assert_artifact(&snapshot, &call, &failed_id);
        let durable = manager.directory.get_operation("owner", &failed_id).await.unwrap().unwrap();
        assert_eq!(durable.result.as_ref().unwrap()["partial_output"], failure(&snapshot).unwrap()["result"]["partial_output"]);
        let fresh = manager.subscribe(&owner, &id).await.unwrap().snapshot;
        assert_artifact(&fresh, &call, &failed_id);
        let canonical = serde_json::to_string(&managed.session.messages()).unwrap();
        assert!(!canonical.contains(PARTIAL_TEXT));
        assert!(!canonical.contains(PARTIAL_REASONING));
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);

        let config = manager.config.clone();
        manager.close().await.unwrap();
        drop(managed);
        drop(manager);
        let reopened = Manager::with_client(config, model.clone()).await.unwrap();
        let detached = reopened.inspect(&reopened.record("owner", &id).await.unwrap()).await.unwrap();
        assert_artifact(&detached, &call, &failed_id);
        assert_eq!(model.calls.load(Ordering::SeqCst), 1, "inspection must not replay the failed request");

        let retry_id = uuid::Uuid::new_v4().to_string();
        let accepted = reopened.command(&owner, &command("turn.retry", Some(&id),
            json!({"operation_id":retry_id,"failed_operation_id":failed_id}))).await.unwrap();
        assert_eq!(accepted["status"], "accepted");
        assert_eq!(accepted["result"]["retry_of_operation_id"], failed_id);
        model.entered().await;
        let retry_request = model.requests.lock().unwrap()[1].clone();
        assert_eq!(serde_json::to_value(&retry_request.items).unwrap(), serde_json::to_value(&request.items).unwrap());
        let managed = reopened.active(reopened.record("owner", &id).await.unwrap(), true).await.unwrap();
        model.release.add_permits(1);
        managed.worker.lock().await.take().unwrap().await.unwrap();
        let final_snapshot = reopened.subscribe(&owner, &id).await.unwrap().snapshot;
        assert!(final_snapshot["superseded_failure_operation_ids"].as_array().unwrap().iter().any(|id| id.as_str() == Some(failed_id.as_str())));
        assert!(final_snapshot["messages"].as_array().unwrap().iter().any(|message| message["text"] == "Accepted retry answer"));
        assert!(!serde_json::to_string(&managed.session.messages()).unwrap().contains(PARTIAL_TEXT));
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        reopened.close().await.unwrap();
    }).await.expect("failed previews must settle and restore without a live provider");
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_failed_stream_preview_survives_snapshot_and_reopen_without_entering_context() {
    durable_stream_failure("file").await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_failed_stream_preview_survives_snapshot_and_reopen_without_entering_context() {
    durable_stream_failure("sqlite").await;
}
