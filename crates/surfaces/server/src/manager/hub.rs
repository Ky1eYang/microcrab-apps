//! Snapshot capture and subscription registration share one short critical section.
//! Preview events have sequence/generation but never mutate canonical conversation.
mod partial;
pub(crate) mod publication;

use microcrab_creator::display::DisplayRecorder;
use microcrab_storage::display::ArchiveRecord;
use microcrab_transport::{ModelObservation, ModelObserver};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// One closed vocabulary for everything a session subscription can deliver.
/// `#[serde(tag = "event", content = "data")]` is the wire form, so the socket
/// forwards a notification by flattening it and never names an event itself.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "event", content = "data")]
pub enum SessionRuntimeEvent {
    #[serde(rename = "blocks.page")]
    BlocksPage(Value),
    #[serde(rename = "blocks.upsert")]
    BlocksUpsert(Value),
    #[serde(rename = "session.meta")]
    SessionMeta(Value),
    #[serde(rename = "history.reset")]
    HistoryReset(Value),
    #[serde(rename = "sessions.changed")]
    SessionsChanged(Value),
    #[serde(rename = "session.cleanup_failed")]
    SessionCleanupFailed(Value),
    #[serde(rename = "session")]
    Session(Value),
    #[serde(rename = "turn")]
    Turn(Value),
    #[serde(rename = "tool")]
    Tool(Value),
    #[serde(rename = "model_observation")]
    ModelObservation(Value),
    #[serde(rename = "display.batch")]
    DisplayBatch(Value),
    #[serde(rename = "display.settled")]
    DisplaySettled(Value),
    #[serde(rename = "display.fragment")]
    DisplayFragment(Value),
    #[serde(rename = "display.unavailable")]
    DisplayUnavailable(Value),
    #[serde(rename = "executions.updated")]
    ExecutionsUpdated(Value),
    #[serde(rename = "statistics.updated")]
    StatisticsUpdated(Value),
    #[serde(rename = "compaction")]
    Compaction(Value),
    #[serde(rename = "permission.requested")]
    PermissionRequested(Value),
    #[serde(rename = "permission.updated")]
    PermissionUpdated(Value),
    #[serde(rename = "permission.proposal.requested")]
    PermissionProposalRequested(Value),
    #[serde(rename = "permission.proposal.settled")]
    PermissionProposalSettled(Value),
    #[serde(rename = "permission.settled")]
    PermissionSettled(Value),
    #[serde(rename = "round_limit.requested")]
    RoundLimitRequested(Value),
    #[serde(rename = "round_limit.settled")]
    RoundLimitSettled(Value),
    #[serde(rename = "client_tool.requested")]
    ClientToolRequested(Value),
    #[serde(rename = "client_tool.prepared")]
    ClientToolPrepared(Value),
    #[serde(rename = "client_tool.paused")]
    ClientToolPaused(Value),
    #[serde(rename = "client_tool.settled")]
    ClientToolSettled(Value),
    #[serde(rename = "client_tool.cancel_requested")]
    ClientToolCancelRequested(Value),
    #[serde(rename = "client_tools.lease_changed")]
    ClientToolsLeaseChanged(Value),
    #[serde(rename = "audit.available")]
    AuditAvailable(Value),
    #[serde(rename = "child.display.available")]
    ChildDisplayAvailable(Value),
    #[serde(rename = "child.display.unavailable")]
    ChildDisplayUnavailable(Value),
    #[serde(rename = "host_detached")]
    HostDetached(Value),
    #[serde(rename = "host_connection")]
    HostConnection(Value),
    #[serde(rename = "resync_required")]
    ResyncRequired(ResyncReason),
}
/// Why a subscriber must drop this socket and reopen with its watermark.
/// A reason with no reachable evidence stays unnamed rather than guessed.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum ResyncReason {
    SlowConsumer,
    ListPublicationFailed { code: String, message: String },
    ReceiptPublicationFailed,
    SnapshotFailed,
    InboxReceiptFailed,
    InboxSnapshotFailed,
    NamingReceiptFailed,
}
static NO_PAYLOAD: Value = Value::Null;
impl SessionRuntimeEvent {
    pub fn name(&self) -> &'static str {
        match self {
            Self::BlocksPage(..) => "blocks.page",
            Self::BlocksUpsert(..) => "blocks.upsert",
            Self::SessionMeta(..) => "session.meta",
            Self::HistoryReset(..) => "history.reset",
            Self::SessionsChanged(..) => "sessions.changed",
            Self::SessionCleanupFailed(..) => "session.cleanup_failed",
            Self::Session(..) => "session",
            Self::Turn(..) => "turn",
            Self::Tool(..) => "tool",
            Self::ModelObservation(..) => "model_observation",
            Self::DisplayBatch(..) => "display.batch",
            Self::DisplaySettled(..) => "display.settled",
            Self::DisplayFragment(..) => "display.fragment",
            Self::DisplayUnavailable(..) => "display.unavailable",
            Self::ExecutionsUpdated(..) => "executions.updated",
            Self::StatisticsUpdated(..) => "statistics.updated",
            Self::Compaction(..) => "compaction",
            Self::PermissionRequested(..) => "permission.requested",
            Self::PermissionSettled(..) => "permission.settled",
            Self::PermissionUpdated(..) => "permission.updated",
            Self::PermissionProposalRequested(..) => "permission.proposal.requested",
            Self::PermissionProposalSettled(..) => "permission.proposal.settled",
            Self::RoundLimitRequested(..) => "round_limit.requested",
            Self::RoundLimitSettled(..) => "round_limit.settled",
            Self::ClientToolRequested(..) => "client_tool.requested",
            Self::ClientToolPrepared(..) => "client_tool.prepared",
            Self::ClientToolPaused(..) => "client_tool.paused",
            Self::ClientToolSettled(..) => "client_tool.settled",
            Self::ClientToolCancelRequested(..) => "client_tool.cancel_requested",
            Self::ClientToolsLeaseChanged(..) => "client_tools.lease_changed",
            Self::AuditAvailable(..) => "audit.available",
            Self::ChildDisplayAvailable(..) => "child.display.available",
            Self::ChildDisplayUnavailable(..) => "child.display.unavailable",
            Self::HostDetached(..) => "host_detached",
            Self::HostConnection(..) => "host_connection",
            Self::ResyncRequired(..) => "resync_required",
        }
    }
    /// Borrowed JSON payload for in-process consumers. The wire form is always the
    /// derived serialization; `ResyncRequired` carries a typed reason, not a Value.
    pub fn payload(&self) -> &Value {
        use SessionRuntimeEvent::*;
        match self {
            BlocksPage(v)
            | BlocksUpsert(v)
            | SessionMeta(v)
            | HistoryReset(v)
            | SessionsChanged(v)
            | SessionCleanupFailed(v)
            | Session(v)
            | Turn(v)
            | Tool(v)
            | ModelObservation(v)
            | DisplayBatch(v)
            | DisplaySettled(v)
            | DisplayFragment(v)
            | DisplayUnavailable(v)
            | ExecutionsUpdated(v)
            | StatisticsUpdated(v)
            | Compaction(v)
            | PermissionRequested(v)
            | PermissionSettled(v)
            | PermissionUpdated(v)
            | PermissionProposalRequested(v)
            | PermissionProposalSettled(v)
            | RoundLimitRequested(v)
            | RoundLimitSettled(v)
            | ClientToolRequested(v)
            | ClientToolPrepared(v)
            | ClientToolPaused(v)
            | ClientToolSettled(v)
            | ClientToolCancelRequested(v)
            | ClientToolsLeaseChanged(v)
            | AuditAvailable(v)
            | ChildDisplayAvailable(v)
            | ChildDisplayUnavailable(v)
            | HostDetached(v)
            | HostConnection(v) => v,
            ResyncRequired(_) => &NO_PAYLOAD,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Notification {
    pub sequence: u64,
    pub event: SessionRuntimeEvent,
}
pub struct Subscription {
    pub id: String,
    pub generation: String,
    pub sequence: u64,
    pub snapshot: Value,
    pub receiver: broadcast::Receiver<Notification>,
    // Dormant subscriptions own their Hub; the Manager index stores only Weak.
    // Live runtime subscriptions already have their Managed owner.
    pub(super) _observation_owner: Option<Arc<Hub>>,
}
struct State {
    snapshot: Value,
    sequence: u64,
    operation: Option<String>,
    operation_cancel: Option<tokio_util::sync::CancellationToken>,
    partial: partial::PartialOutput,
    display_high_water: u64,
    display_failed: bool,
    recorded_calls: std::collections::HashSet<String>,
    request_identities: std::collections::HashMap<String, String>,
}
pub(super) struct Hub {
    state: Mutex<State>,
    sender: broadcast::Sender<Notification>,
    generation: String,
    recording: Mutex<Option<Arc<DisplayRecorder>>>,
    /// Told when the published `session` view changes (owner list increments).
    on_session: std::sync::OnceLock<Box<dyn Fn() + Send + Sync>>,
}
impl Hub {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(256);
        Self {
            state: Mutex::new(State {
                snapshot: json!({}),
                sequence: 0,
                operation: None,
                operation_cancel: None,
                partial: partial::PartialOutput::default(),
                display_high_water: 0,
                display_failed: false,
                recorded_calls: std::collections::HashSet::new(),
                request_identities: std::collections::HashMap::new(),
            }),
            sender,
            generation: uuid::Uuid::new_v4().to_string(),
            recording: Mutex::new(None),
            on_session: std::sync::OnceLock::new(),
        }
    }
    /// Install the session-view change observer once; later calls are ignored.
    pub fn watch_session(&self, observer: impl Fn() + Send + Sync + 'static) {
        let _ = self.on_session.set(Box::new(observer));
    }
    fn session_changed(&self) {
        if let Some(observer) = self.on_session.get() {
            observer();
        }
    }
    pub fn recording(&self) -> Option<Arc<DisplayRecorder>> {
        self.recording
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub fn install_recording(&self, recorder: Arc<DisplayRecorder>, high_water: u64) {
        *self.recording.lock().unwrap_or_else(|e| e.into_inner()) = Some(recorder);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.display_high_water = high_water;
        state.display_failed = false;
    }
    fn display_boundary(state: &State) -> Value {
        json!({"high_water":state.display_high_water,"coverage":if state.display_failed {"unavailable"} else {"available"}})
    }
    pub fn request_identity(&self, attempt: &str) -> Option<String> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .request_identities
            .get(attempt)
            .cloned()
    }
    pub fn display_high_water(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .display_high_water
    }
    pub fn display_failed(&self, _message: String) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.display_failed = true;
        state.snapshot["display"] = Self::display_boundary(&state);
        self.send(&mut state, SessionRuntimeEvent::DisplayUnavailable(json!({"code":"display_save_failed","message":"Display history could not be saved; live content remains available"})));
    }
    /// The writer invokes this only after durable append, before record acknowledgement.
    /// One mutex covers publication and attach's snapshot barrier; no I/O is held here.
    pub fn recorded(&self, record: ArchiveRecord) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if record.sequence <= state.display_high_water {
            return;
        }
        state.display_high_water = record.sequence;
        state.snapshot["display"] = Self::display_boundary(&state);
        let mut event = record.event;
        if event["phase"] == "started"
            && let Some(identity) = event["request_identity"].as_str()
        {
            state
                .request_identities
                .insert(record.attempt_id.clone(), identity.to_owned());
        }
        event["attempt_id"] = json!(record.attempt_id);
        event["archive_sequence"] = json!(record.sequence);
        if let Some(call) = event["call_id"].as_str() {
            state.recorded_calls.insert(call.to_owned());
        }
        if let Ok(observation) = serde_json::from_value::<ModelObservation>(event.clone()) {
            state
                .partial
                .observe_recorded(&observation, &record.attempt_id, record.sequence);
        }
        self.send(&mut state, SessionRuntimeEvent::ModelObservation(event));
    }
    pub fn subscribe(&self) -> Subscription {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let receiver = self.sender.subscribe();
        Subscription {
            id: uuid::Uuid::new_v4().to_string(),
            generation: self.generation.clone(),
            sequence: state.sequence,
            snapshot: state.snapshot.clone(),
            receiver,
            _observation_owner: None,
        }
    }
    fn send(&self, state: &mut State, event: SessionRuntimeEvent) {
        if let Some(sequence) = state.sequence.checked_add(1) {
            state.sequence = sequence;
            let _ = self.sender.send(Notification { sequence, event });
        }
    }
    /// The only publication path. Callers name a variant; nothing here accepts a
    /// free event string, so a producer cannot invent a second vocabulary.
    pub fn publish(&self, event: SessionRuntimeEvent) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        self.send(&mut state, event);
    }
    pub fn executions(&self, items: Value) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.snapshot["executions"] = items.clone();
        state.snapshot["executions_omitted"] = json!(false);
        self.send(
            &mut state,
            SessionRuntimeEvent::ExecutionsUpdated(json!({"items":items})),
        );
    }
    /// Lifecycle updates reuse the last settled conversation and statistics.
    /// Closing must not rescan transcripts or audit bodies after journal release.
    pub fn session(&self, session: Value) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let changed = state.snapshot["session"] != session;
        state.snapshot["session"] = session.clone();
        self.send(
            &mut state,
            SessionRuntimeEvent::SessionMeta(json!({"session":session})),
        );
        drop(state);
        if changed {
            self.session_changed();
        }
    }
    /// Update the applied mode on the ordered session stream without touching
    /// configured settings, directory revision or the settled transcript.
    pub fn applied_mode(&self, mode: Option<&str>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let Some(session) = state
            .snapshot
            .get_mut("session")
            .filter(|value| value.is_object())
        else {
            return;
        };
        if session["mode"].as_str() == mode {
            return;
        }
        session["mode"] = json!(mode);
        let session = session.clone();
        self.send(
            &mut state,
            SessionRuntimeEvent::SessionMeta(json!({"session":session})),
        );
        drop(state);
        self.session_changed();
    }
    pub fn statistics(&self, statistics: Value, expected_revision: u64) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.snapshot["session"]["revision"].as_u64() != Some(expected_revision) {
            return;
        }
        if state.snapshot["statistics"] == statistics {
            return;
        }
        state.snapshot["statistics"] = statistics.clone();
        self.send(
            &mut state,
            SessionRuntimeEvent::StatisticsUpdated(statistics),
        );
    }
    pub fn replace(&self, mut snapshot: Value) -> Value {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if snapshot["session"]["revision"]
            .as_u64()
            .zip(state.snapshot["session"]["revision"].as_u64())
            .is_some_and(|(incoming, current)| incoming < current)
        {
            return state.snapshot.clone();
        }
        if state.display_high_water > 0
            || state.display_failed
            || self
                .recording
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_some()
        {
            snapshot["display"] = Self::display_boundary(&state);
        }
        // Subscribers receive only the delta from the view they already hold;
        // the materialized value stays server-side as the attach barrier.
        let events = publication::diff(&state.snapshot, &snapshot);
        let changed = state.snapshot.get("session") != snapshot.get("session");
        state.snapshot = snapshot.clone();
        for event in events {
            self.send(&mut state, event);
        }
        drop(state);
        if changed {
            self.session_changed();
        }
        snapshot
    }
    /// Merge top-level fields computed after attach (for example lazy
    /// history availability) and publish them as one `session.meta` patch.
    pub fn patch(&self, fields: serde_json::Map<String, Value>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !state.snapshot.is_object() {
            return;
        }
        let mut changed = serde_json::Map::new();
        for (key, value) in fields {
            if state.snapshot.get(&key) != Some(&value) {
                state.snapshot[&key] = value.clone();
                changed.insert(key, value);
            }
        }
        if !changed.is_empty() {
            let session = changed.contains_key("session");
            self.send(
                &mut state,
                SessionRuntimeEvent::SessionMeta(Value::Object(changed)),
            );
            drop(state);
            if session {
                self.session_changed();
            }
        }
    }
    pub fn snapshot(&self) -> Value {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot
            .clone()
    }
    pub fn failed_partial(
        &self,
        epoch: u64,
        turn: Option<u64>,
        round: Option<u64>,
        pre_effect: bool,
    ) -> Option<Value> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .partial
            .failed_or_rejected(epoch, turn, round, pre_effect)
    }
    pub fn operation(&self) -> Option<String> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .operation
            .clone()
    }
    pub fn set_operation(&self, operation: Option<String>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.operation_cancel = operation
            .as_ref()
            .map(|_| tokio_util::sync::CancellationToken::new());
        state.operation = operation;
    }
    pub fn operation_cancellation(&self) -> Option<tokio_util::sync::CancellationToken> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .operation_cancel
            .clone()
    }
    pub fn interrupt_operation(&self) {
        if let Some(token) = self.operation_cancellation() {
            token.cancel();
        }
    }
}
impl ModelObserver for Hub {
    fn publish(&self, observation: ModelObservation) {
        if observation.subagent.is_some() {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        // Autonomous inbox/StateDelivery turns have no browser command owner.
        // Their producer's epoch/turn identity scopes advisory previews only;
        // this never installs a second active operation or cancellation token.
        if state.recorded_calls.contains(&observation.call_id) && !state.display_failed {
            return;
        }
        let operation = state.operation.clone().unwrap_or_else(|| {
            format!(
                "inbox:{}:{}",
                observation.epoch,
                observation.turn.unwrap_or_default()
            )
        });
        state.partial.observe(&observation);
        if let Ok(mut value) = serde_json::to_value(observation) {
            value["operation_id"] = json!(operation);
            self.send(&mut state, SessionRuntimeEvent::ModelObservation(value));
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retained_execution_updates_preserve_current_state_and_omission() {
        let hub = Hub::new();
        hub.replace(
            json!({"state":{"custom":7,"executions":[{"execution_id":2}]},"executions":[]}),
        );
        let mut subscription = hub.subscribe();
        let history =
            json!([{"execution_id":1,"status":"succeeded"},{"execution_id":2,"status":"running"}]);
        hub.executions(history.clone());
        assert_eq!(hub.snapshot()["executions"], history);
        assert_eq!(
            hub.snapshot()["state"],
            json!({"custom":7,"executions":[{"execution_id":2}]})
        );
        let event = subscription.receiver.try_recv().unwrap();
        assert_eq!(event.event.name(), "executions.updated");
        assert_eq!(event.event.payload()["items"], history);
        hub.replace(json!({"state":null,"state_omitted":true,"executions":[]}));
        hub.executions(history.clone());
        assert!(hub.snapshot()["state"].is_null());
        assert_eq!(hub.subscribe().snapshot["executions"], history);
    }
    #[test]
    fn recorded_prefix_is_atomic_with_attach_and_failed_body_is_a_reference() {
        use microcrab_transport::ObservationState;
        let hub = Hub::new();
        hub.replace(json!({"messages":[]}));
        let observation = |state| ModelObservation {
            call_id: "call".into(),
            epoch: 7,
            turn: Some(3),
            round: Some(2),
            subagent: None,
            state,
        };
        let started = observation(ObservationState::Started);
        hub.recorded(ArchiveRecord {
            sequence: 1,
            attempt_id: "attempt".into(),
            event: serde_json::to_value(&started).unwrap(),
        });
        let mut sub = hub.subscribe();
        assert_eq!(sub.snapshot["display"]["high_water"], 1);
        assert!(sub.receiver.try_recv().is_err());
        let reasoning = observation(ObservationState::ReasoningDelta {
            text: "思考".repeat(70_000),
        });
        hub.recorded(ArchiveRecord {
            sequence: 2,
            attempt_id: "attempt".into(),
            event: serde_json::to_value(&reasoning).unwrap(),
        });
        ModelObserver::publish(&hub, reasoning);
        let event = sub.receiver.try_recv().unwrap();
        assert_eq!(event.event.payload()["archive_sequence"], 2);
        assert_eq!(
            event.event.payload()["text"].as_str().unwrap().len(),
            420_000
        );
        assert!(sub.receiver.try_recv().is_err());
        hub.recorded(ArchiveRecord {
            sequence: 3,
            attempt_id: "attempt".into(),
            event: serde_json::to_value(observation(ObservationState::Invalidated {
                reason: "cancelled".into(),
            }))
            .unwrap(),
        });
        let failed = hub.failed_partial(7, Some(3), Some(2), false).unwrap();
        assert_eq!(
            failed["display_ref"],
            json!({"attempt_id":"attempt","high_water":3})
        );
        assert!(failed.get("reasoning").is_none());
        hub.replace(json!({"messages":[{"text":"canonical"}]}));
        assert_eq!(hub.snapshot()["display"]["high_water"], 3);
        assert_eq!(hub.snapshot()["messages"], json!([{"text":"canonical"}]));
    }
    #[test]
    fn stale_revision_returns_canonical_snapshot_and_does_not_emit_old_statistics() {
        let hub = Hub::new();
        let current = json!({"session":{"revision":2,"model":"B"},"statistics":{"version":"new"}});
        hub.replace(current.clone());
        let mut subscription = hub.subscribe();
        assert_eq!(
            hub.replace(json!({"session":{"revision":1,"model":"A"}})),
            current
        );
        hub.statistics(json!({"version":"old"}), 1);
        assert_eq!(hub.snapshot(), current);
        assert!(subscription.receiver.try_recv().is_err());
    }
    #[test]
    fn subscribe_joins_after_canonical_boundary_and_previews_never_enter_snapshot() {
        let hub = Hub::new();
        // The first materialization has no older subscriber view to diff.
        hub.replace(json!({"session":{"id":"s"},"messages":[]}));
        let mut sub = hub.subscribe();
        assert_eq!(sub.sequence, 0);
        assert!(sub.receiver.try_recv().is_err());
        hub.set_operation(Some("op".into()));
        ModelObserver::publish(
            &hub,
            ModelObservation {
                call_id: "call".into(),
                epoch: 0,
                turn: Some(1),
                round: Some(1),
                subagent: None,
                state: microcrab_transport::ObservationState::TextDelta {
                    text: "temporary".into(),
                },
            },
        );
        let event = sub.receiver.try_recv().unwrap();
        assert_eq!(event.sequence, 1);
        assert_eq!(event.event.payload()["call_id"], "call");
        assert_eq!(hub.snapshot(), json!({"session":{"id":"s"},"messages":[]}));
        hub.replace(json!({"session":{"id":"s"},"messages":[{"id":"m1","text":"canonical"}]}));
        let upsert = sub.receiver.try_recv().unwrap();
        assert_eq!(upsert.event.name(), "blocks.upsert");
        assert_eq!(
            upsert.event.payload()["messages"],
            json!([{"id":"m1","text":"canonical"}])
        );
        assert!(sub.receiver.try_recv().is_err());
        let next = hub.subscribe();
        assert_eq!(next.sequence, 2);
        assert_eq!(next.snapshot["messages"][0]["text"], "canonical");
    }
}
