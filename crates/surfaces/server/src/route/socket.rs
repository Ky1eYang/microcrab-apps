//! Typed browser WebSocket protocol. This file owns only the definitions and the
//! two upgrade entries; the loops live in `socket/{writer,global,session,display}.rs`.
//!
//! - The global socket (`/api/v1/ws`) carries browser Conversation Host attachment
//!   and its `conversation_host` lane for the authenticated app lifetime.
//! - A session socket (`/api/v1/sessions/{session_id}/ws`) exists per selected
//!   Session. Opening it subscribes; the path is the only Session authority, and
//!   closing it releases the subscription and every executor lease it bound.
//!
//! Each client frame is decoded once: the shared `Command` envelope, then its
//! `method` into the socket's closed method enum. Dispatch is a `match` on that
//! enum. An unknown method fails with `unsupported_method` naming the method.
mod display;
mod global;
mod session;
#[cfg(all(test, feature = "storage-file"))]
mod transport_tests;
mod writer;

use crate::{Application, manager::SessionRuntimeEvent};
use axum::{
    extract::{Path, Query, State, ws::WebSocketUpgrade},
    http::HeaderMap,
    response::Response,
};
use microcrab_protocol::application::{Command, MAX_FRAME_BYTES, VERSION, WireError};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::sync::Arc;
use tracing::Instrument;

/// Methods accepted on the global socket.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub(crate) enum GlobalMethod {
    #[serde(rename = "sessions.watch")]
    SessionsWatch,
    #[serde(rename = "project.host_watch")]
    ProjectHostWatch,
    #[serde(rename = "project.host_unwatch")]
    ProjectHostUnwatch,
    #[serde(rename = "host.relay_attach")]
    HostRelayAttach,
    #[serde(rename = "conversation.attach")]
    ConversationAttach,
    #[serde(rename = "conversation.detach")]
    ConversationDetach,
    /// Durable browser/workspace pin. It precedes attachment and has no socket
    /// lease, so it stays a command rather than part of the host lane.
    #[serde(rename = "conversation.binding")]
    ConversationBinding,
}

/// Methods accepted on a session socket. Socket-owned methods are handled by the
/// session loop; the rest go to `Manager::command` with the path's Session id.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub(crate) enum SessionMethod {
    #[serde(rename = "session.blocks")]
    SessionBlocks,
    #[serde(rename = "display.subscribe")]
    DisplaySubscribe,
    #[serde(rename = "session.interrupt")]
    SessionInterrupt,
    #[serde(rename = "turn.submit")]
    TurnSubmit,
    #[serde(rename = "turn.queue")]
    TurnQueue,
    #[serde(rename = "turn.steer")]
    TurnSteer,
    #[serde(rename = "turn.retry")]
    TurnRetry,
    #[serde(rename = "inbox.update")]
    InboxUpdate,
    #[serde(rename = "inbox.remove")]
    InboxRemove,
    #[serde(rename = "inbox.resume")]
    InboxResume,
    #[serde(rename = "permission.reply")]
    PermissionReply,
    #[serde(rename = "permission.proposal.reply")]
    PermissionProposalReply,
    #[serde(rename = "round_limit.reply")]
    RoundLimitReply,
    #[serde(rename = "client_tools.bind")]
    ClientToolsBind,
    #[serde(rename = "client_tools.result")]
    ClientToolsResult,
    #[serde(rename = "client_tools.unbind")]
    ClientToolsUnbind,
    #[serde(rename = "client_tools.prepare")]
    ClientToolsPrepare,
    #[serde(rename = "client_tools.output")]
    ClientToolsOutput,
    #[serde(rename = "client_tools.takeover")]
    ClientToolsTakeover,
    #[serde(rename = "client_tools.arguments")]
    ClientToolsArguments,
    #[serde(rename = "client_tools.secret")]
    ClientToolsSecret,
    #[serde(rename = "client_tools.secret_operation")]
    ClientToolsSecretOperation,
    #[serde(rename = "client_tools.decision")]
    ClientToolsDecision,
    #[serde(rename = "executions.read")]
    ExecutionsRead,
    #[serde(rename = "executions.cancel")]
    ExecutionsCancel,
}

/// Resume metadata is an object on the wire, matching the browser's strict
/// subscription decoder. Keeping this wrapper typed prevents a bare mode string
/// from silently replacing the agreed envelope during serialization.
#[derive(Debug, Serialize)]
pub(crate) struct SubscriptionResume {
    pub(crate) mode: &'static str,
}

/// Every server-to-browser frame. `Event` flattens the Hub's typed event, so the
/// wire keeps `{"event": <name>, "data": ...}` from `SessionRuntimeEvent`'s serde.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ServerFrame {
    Result {
        v: u16,
        request_id: String,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        data: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<WireError>,
    },
    /// Metadata first, then exactly `initial_events` ordered typed events from
    /// the private coherent barrier before any live or display delivery.
    Subscribed {
        v: u16,
        subscription_id: String,
        generation: String,
        sequence: u64,
        session_id: String,
        meta: Value,
        pending: Value,
        resume: SubscriptionResume,
        initial_events: usize,
    },
    Event {
        v: u16,
        subscription_id: String,
        generation: String,
        sequence: u64,
        session_id: String,
        #[serde(flatten)]
        event: SessionRuntimeEvent,
    },
    ProjectHostEvent {
        v: u16,
        project_id: String,
        subscription_id: String,
        sequence: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        summary: Option<Value>,
    },
    /// A session socket that cannot subscribe sends this once, then closes.
    Error { v: u16, error: WireError },
}
impl ServerFrame {
    pub(crate) fn result(request_id: String, result: Result<Value, WireError>) -> Self {
        let (ok, data, error) = match result {
            Ok(data) => (true, Some(data), None),
            Err(error) => (false, None, Some(error)),
        };
        Self::Result {
            v: VERSION,
            request_id,
            ok,
            data,
            error,
        }
    }
}

/// Session socket query: the reconnect watermark for display catch-up.
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionQuery {
    #[serde(default)]
    display_watermark: u64,
    #[serde(default)]
    history_lineage: Option<String>,
}

/// Why a client frame was not dispatched.
#[derive(Debug)]
pub(crate) enum Rejected {
    /// Not a command envelope at all: answer once, then close the socket.
    Malformed,
    /// A correlated envelope that fails its contract: answer and keep reading.
    Reply(String, WireError),
}

/// Decode one client frame into the envelope plus the socket's closed method.
/// Both sockets take their scope from the socket itself, so `session_id` in a
/// frame is a contract error rather than a second Session authority.
pub(crate) fn decode<M: DeserializeOwned>(text: &str) -> Result<(Command, M), Rejected> {
    let value = serde_json::from_str(text).map_err(|_| Rejected::Malformed)?;
    decode_value(value)
}

/// Validate the command after the global socket has selected its lane.
pub(crate) fn decode_value<M: DeserializeOwned>(value: Value) -> Result<(Command, M), Rejected> {
    let command: Command = serde_json::from_value(value).map_err(|_| Rejected::Malformed)?;
    let reject = |error| Rejected::Reply(command.request_id.clone(), error);
    command.validate().map_err(reject)?;
    if command.session_id.is_some() {
        return Err(reject(WireError::new(
            "invalid_command",
            "Socket commands take their Session from the socket, not session_id",
        )));
    }
    let method =
        serde_json::from_value::<M>(Value::String(command.method.clone())).map_err(|_| {
            reject(WireError::new(
                "unsupported_method",
                format!(
                    "Method `{}` is not supported on this socket",
                    command.method
                ),
            ))
        })?;
    Ok((command, method))
}

/// Parse socket-owned params with a concrete serde reason.
pub(crate) fn params<T: DeserializeOwned>(command: &Command) -> Result<T, WireError> {
    serde_json::from_value(command.params.clone()).map_err(|error| {
        WireError::new(
            "invalid_params",
            format!("Invalid `{}` params: {error}", command.method),
        )
    })
}

/// The method is logged only after it decoded into a closed enum.
pub(crate) fn command_span(command: &Command, socket_id: &str) -> tracing::Span {
    let request_id = uuid::Uuid::parse_str(&command.request_id).unwrap_or(uuid::Uuid::nil());
    tracing::info_span!("ws_command", request_id = %request_id, method = %command.method, socket_id, project_id = tracing::field::Empty, session_id = tracing::field::Empty, carrier = tracing::field::Empty, has_token = tracing::field::Empty, revoke = tracing::field::Empty, allow_legacy_binding = tracing::field::Empty)
}
pub(crate) fn command_completed<T>(result: &Result<T, WireError>, started: std::time::Instant) {
    tracing::info!(
        success = result.is_ok(),
        error_code = result
            .as_ref()
            .err()
            .map_or("none", |error| error.code.as_str()),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "WS command completed"
    );
}

/// First causal exit wins; teardown cannot overwrite it after cancellation.
#[derive(Clone, Default)]
pub(super) struct DeliveryEvidence(Arc<DeliveryFacts>);
#[derive(Default)]
struct DeliveryFacts {
    first: std::sync::OnceLock<&'static str>,
    enqueued: std::sync::atomic::AtomicU64,
    written: std::sync::atomic::AtomicU64,
}
impl DeliveryEvidence {
    fn record(&self, reason: &'static str) {
        let _ = self.0.first.set(reason);
    }
    fn enqueued(&self, sequence: u64) {
        self.0.enqueued.store(sequence, std::sync::atomic::Ordering::Relaxed);
    }
    fn written(&self, sequence: u64) {
        self.0.written.store(sequence, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Lifetime evidence survives every early return and records only closed causes.
pub(super) struct ConnectionLog {
    started: std::time::Instant,
    pub(super) reason: &'static str,
    evidence: DeliveryEvidence,
}
impl ConnectionLog {
    fn open() -> Self {
        tracing::info!(event = "ws.open", "WS connection opened");
        Self {
            started: std::time::Instant::now(),
            reason: "connection_ended",
            evidence: DeliveryEvidence::default(),
        }
    }
}
impl Drop for ConnectionLog {
    fn drop(&mut self) {
        tracing::info!(
            event = "ws.close",
            reason = self.evidence.0.first.get().copied().unwrap_or(self.reason),
            teardown = self.reason,
            last_enqueued_seq = self.evidence.0.enqueued.load(std::sync::atomic::Ordering::Relaxed),
            last_written_seq = self.evidence.0.written.load(std::sync::atomic::Ordering::Relaxed),
            elapsed_ms = self.started.elapsed().as_millis() as u64,
            "WS connection closed"
        );
    }
}
fn connection_span(
    principal: &crate::auth::Principal,
    session_id: &str,
    headers: &HeaderMap,
    socket_id: &str,
) -> tracing::Span {
    tracing::info_span!("ws_connection", socket_id, owner_prefix = %super::provenance::owner_prefix(&principal.owner), session_id = if session_id.is_empty() { "" } else { super::provenance::identity(session_id) }, tab_id = %super::provenance::tab_id(headers))
}

/// `/api/v1/ws`: authenticated app lifetime, Conversation Host lane only.
pub(crate) async fn global(
    State(state): State<Arc<Application>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, Response> {
    state
        .auth
        .check_origin(&headers)
        .map_err(super::provenance::auth_error)?;
    let principal = state
        .auth
        .authenticate(&headers)
        .map_err(super::provenance::auth_error)?;
    state
        .auth
        .check_active(&principal)
        .map_err(super::provenance::auth_error)?;
    let socket_id = uuid::Uuid::new_v4().to_string();
    let span = connection_span(&principal, "", &headers, &socket_id);
    Ok(ws
        .max_message_size(MAX_FRAME_BYTES)
        .max_frame_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| {
            global::connection(state, principal, socket, socket_id).instrument(span)
        }))
}

/// `/api/v1/sessions/{session_id}/ws`: opening subscribes. Ownership is checked
/// by the subscription itself; a refused subscription answers with one `error`
/// frame and closes, because browsers cannot read an upgrade response body.
pub(crate) async fn session(
    State(state): State<Arc<Application>>,
    Path(session_id): Path<String>,
    query: Result<Query<SessionQuery>, axum::extract::rejection::QueryRejection>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    use axum::response::IntoResponse;
    let principal = match state
        .auth
        .check_origin(&headers)
        .and_then(|_| state.auth.authenticate(&headers))
        .and_then(|principal| state.auth.check_active(&principal).map(|_| principal))
    {
        Ok(principal) => principal,
        Err(error) => return super::provenance::auth_error(error),
    };
    let Query(query) = match query {
        Ok(query) => query,
        Err(error) => return super::error::Error::query(error).into_response(),
    };
    let socket_id = uuid::Uuid::new_v4().to_string();
    let span = connection_span(&principal, &session_id, &headers, &socket_id);
    ws.max_message_size(MAX_FRAME_BYTES)
        .max_frame_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| {
            session::connection(state, principal, session_id, query, socket, socket_id)
                .instrument(span)
        })
}

#[cfg(test)]
mod tests {
    #[test]
    fn first_delivery_cause_is_not_replaced_by_teardown() {
        let first = super::DeliveryEvidence::default();
        let writer = first.clone();
        writer.record("send_timeout");
        first.record("delivery_cancelled");
        assert_eq!(first.0.first.get(), Some(&"send_timeout"));
        let first = super::DeliveryEvidence::default();
        first.record("peer_closed");
        first.clone().record("sink_error");
        assert_eq!(first.0.first.get(), Some(&"peer_closed"));
    }

    #[test]
    fn subscription_serialization_matches_browser_contract_fixture() {
        use super::{ServerFrame, SubscriptionResume};
        use crate::manager::SessionRuntimeEvent;
        use serde_json::{Value, json};

        // Playwright feeds these exact frames to the production SessionSocket.
        // Comparing serde output here closes the gap between native transport
        // tests and an independently handwritten browser fixture.
        let expected: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/session-subscription.json"
        ))
        .unwrap();
        for (fixture, mode, initialization) in [
            ("fresh", "fresh", false),
            ("reset", "reset", false),
            ("agent_init_fresh", "fresh", true),
            ("agent_init_reset", "reset", true),
        ] {
            let executions = if initialization {
                use microcrab_session::{
                    policy::InvocationSubject,
                    runtime::execution::{ExecutionId, ExecutionSnapshot},
                };

                // Session lifecycle initialization has a real execution identity
                // and an explicit subject, but no fabricated tool name. Keep its
                // typed serialization in the shared browser contract.
                let mut row = ExecutionSnapshot::new(
                    "s1".into(),
                    "agent-init:1:1:contract-component".into(),
                    String::new(),
                    "coding".into(),
                    Some("plan".into()),
                );
                row.execution_id = ExecutionId(1);
                row.subject = InvocationSubject::AgentInit {
                    component: "contract-component".into(),
                    generation: 1,
                    attempt: 1,
                };
                vec![row.metadata()]
            } else {
                vec![]
            };
            let subscribed = ServerFrame::Subscribed {
                v: 1,
                subscription_id: "contract-subscription".into(),
                generation: "contract-generation".into(),
                sequence: 0,
                session_id: "s1".into(),
                meta: json!({"session":{"id":"s1"},"operations":[],"executions":executions,"executions_omitted":false}),
                pending: json!({"permissions":null,"round_limits":null}),
                resume: SubscriptionResume { mode },
                initial_events: 1,
            };
            let page = ServerFrame::Event {
                v: 1,
                subscription_id: "contract-subscription".into(),
                generation: "contract-generation".into(),
                sequence: 1,
                session_id: "s1".into(),
                event: SessionRuntimeEvent::BlocksPage(json!({"reason":"attach","messages":[]})),
            };
            let actual = json!([
                serde_json::to_value(subscribed).unwrap(),
                serde_json::to_value(page).unwrap(),
            ]);
            assert_eq!(
                actual, expected[fixture],
                "subscription wire contract for {fixture}"
            );
        }
    }
}
