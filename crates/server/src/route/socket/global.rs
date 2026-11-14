//! Global socket loop: browser Conversation attachment and the
//! `conversation_host` lane. Host replies bypass business admission because a
//! pending attach can be awaiting this very reply.
use super::{
    GlobalMethod, Rejected, ServerFrame, command_completed, command_span, decode_value, params,
    writer::{self, Delivery, LANE, send},
};
use crate::{Application, auth::Principal};
use axum::extract::ws::{Message, WebSocket};
use futures_util::StreamExt;
use microcrab_host_protocol::HostFrame;
use microcrab_protocol::application::WireError;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::mpsc,
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HostEnvelope {
    lane: String,
    #[serde(default)]
    conversation_id: Option<String>,
    connection_generation: String,
    frame: HostFrame,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Detach {
    conversation_id: String,
}

pub(super) async fn connection(
    state: Arc<Application>,
    principal: Principal,
    socket: WebSocket,
    socket_id: String,
) {
    let cancellation = CancellationToken::new();
    let mut log = super::ConnectionLog::open();
    let (sink, mut source) = socket.split();
    let (sender, outgoing) = mpsc::channel::<Delivery>(LANE);
    let (host_sender, host_outgoing) = mpsc::channel::<Value>(LANE);
    let (relay_out, relay_outgoing) = mpsc::channel::<Vec<u8>>(LANE);
    let relay_in: Arc<std::sync::Mutex<Option<crate::manager::direct::RelayBytes>>> =
        Arc::new(std::sync::Mutex::new(None));
    // The cleanup witness outlives the intake guard: cancelling an in-flight
    // handshake clears intake, but must still retire any just-published route.
    let relay_cleanup: Arc<std::sync::Mutex<Option<(String, uuid::Uuid)>>> =
        Arc::new(std::sync::Mutex::new(None));
    // A single browser socket owns one native relay. Serialize replacement so
    // concurrent commands cannot orphan an earlier credential-bearing bridge.
    let relay_gate = Arc::new(tokio::sync::Mutex::new(()));
    let writer = writer::spawn(
        state.clone(),
        principal.clone(),
        sink,
        outgoing,
        Some(host_outgoing),
        Some(relay_outgoing),
        cancellation.clone(),
    );
    let mut requests = JoinSet::<(String, Result<Value, WireError>)>::new();
    let mut host_forwarders = HashMap::<(String, u64), JoinHandle<()>>::new();
    let mut watcher: Option<JoinHandle<()>> = None;
    let mut expiry = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => { log.reason = "delivery_cancelled"; break; },
            _ = expiry.tick() => {
                if state.auth.check_active(&principal).is_err() { log.reason = "authentication_lost"; break; }
                state.manager.poll_socket_routes(&socket_id).await;
            },
            completed = requests.join_next(), if !requests.is_empty() => {
                let Some(Ok((request_id, result))) = completed else { log.reason = "command_task_failed"; break };
                if state.auth.check_active(&principal).is_err() { log.reason = "authentication_lost"; break; }
                if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
            },
            incoming = source.next() => {
                let message = match incoming { Some(Ok(message)) => message, Some(Err(_)) => { log.reason = "reader_error"; break; }, None => { log.reason = "reader_eof"; break; } };
                if state.auth.check_active(&principal).is_err() { log.reason = "authentication_lost"; break; }
                let text = match message {
                    Message::Text(text) => text,
                    Message::Ping(_) | Message::Pong(_) => continue,
                    Message::Binary(bytes) => {
                        if bytes.len() > microcrab_host_protocol::MAX_FRAME_BYTES { log.reason = "host_frame_too_large"; break; }
                        let inbound = relay_in.lock().unwrap_or_else(|error| error.into_inner()).as_ref().map(|relay| relay.sender.clone());
                        if let Some(inbound) = inbound {
                            // Losing a native protocol frame would leave effect receipt
                            // ownership indeterminate. Tear down rather than dropping it.
                            if inbound.try_send(bytes.to_vec()).is_err() { log.reason = "relay_delivery_failed"; break; }
                        }
                        continue;
                    },
                    Message::Close(_) => { log.reason = "peer_closed"; break; },
                };
                let value: Value = match serde_json::from_str(&text) {
                    Ok(value) => value,
                    Err(_) => {
                        log.reason = "malformed_json";
                        tracing::warn!(event = "ws.reject", error_code = "invalid_command", "WS command rejected");
                        writer::finish(&sender, ServerFrame::result("invalid".into(), Err(WireError::new("invalid_command", "Expected native JSON command"))), &cancellation).await;
                        break;
                    }
                };
                if value.get("lane").and_then(Value::as_str) == Some("conversation_host") {
                    if text.len() > microcrab_host_protocol::MAX_FRAME_BYTES + 4096 { log.reason = "host_frame_too_large"; break; }
                    let Ok(envelope) = serde_json::from_value::<HostEnvelope>(value) else { log.reason = "invalid_host_envelope"; break };
                    let Ok(generation) = envelope.connection_generation.parse::<u64>() else { log.reason = "invalid_host_generation"; break };
                    let (Some(id), "conversation_host") = (envelope.conversation_id.as_deref(), envelope.lane.as_str()) else { log.reason = "invalid_host_lane"; break };
                    if let Err(error) = state.manager.receive_conversation_host(&principal, &socket_id, id, generation, envelope.frame).await {
                        // A replaced route or late frame can still be in transit. Reject
                        // it without tearing down the current valid controller.
                        tracing::warn!(error_code = %error.code, socket_id = %socket_id, generation, "Host frame rejected on socket");
                    }
                    continue;
                }
                let (command, method) = match decode_value::<GlobalMethod>(value) {
                    Ok(decoded) => decoded,
                    Err(Rejected::Malformed) => {
                        log.reason = "malformed_command";
                        tracing::warn!(event = "ws.reject", error_code = "invalid_command", "WS command rejected");
                        writer::finish(&sender, ServerFrame::result("invalid".into(), Err(WireError::new("invalid_command", "Expected native JSON command"))), &cancellation).await;
                        break;
                    }
                    Err(Rejected::Reply(request_id, error)) => {
                        tracing::warn!(event = "ws.reject", error_code = %error.code, "WS command rejected");
                        if send(&sender, ServerFrame::result(request_id, Err(error)), &cancellation).await.is_err() { break; }
                        continue;
                    }
                };
                let request_id = command.request_id.clone();
                let span = command_span(&command, &socket_id);
                tracing::info!(parent: &span, "WS command received");
                let started = std::time::Instant::now();
                span.in_scope(|| super::super::provenance::actions(&command.method, &command.params));
                match method {
                    GlobalMethod::SessionsWatch => {
                        if let Err(error) = params::<Empty>(&command) {
                            let result: Result<Value, WireError> = Err(error);
                            span.in_scope(|| command_completed(&result, started));
                            if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
                            continue;
                        }
                        if let Some(previous) = watcher.take() { previous.abort(); }
                        let id = uuid::Uuid::new_v4().to_string();
                        let generation = uuid::Uuid::new_v4().to_string();
                        let receiver = state.manager.watch_sessions();
                        let result = Ok(json!({"subscription_id":id,"generation":generation,"sequence":0}));
                        span.in_scope(|| command_completed(&result, started));
                        if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
                        watcher = Some(tokio::spawn(watch_sessions(receiver, principal.owner.clone(), id, generation, sender.clone(), cancellation.clone())));
                    }
                    GlobalMethod::HostRelayAttach => {
                        if requests.len() >= 32 {
                            let result: Result<Value, WireError> = Err(WireError::new("command_limit", "Too many pending commands")); span.in_scope(|| command_completed(&result, started)); if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
                            continue;
                        }
                        let attach = match params::<crate::manager::direct::Connect>(&command) {
                            Ok(attach) => attach,
                            Err(error) => { let result: Result<Value, WireError> = Err(error); span.in_scope(|| command_completed(&result, started)); if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; } continue; }
                        };
                        let (task_state, task_principal, route_socket) = (state.clone(), principal.clone(), socket_id.clone());
                        let (outbound, slot, gate) = (relay_out.clone(), relay_in.clone(), relay_gate.clone());
                        let cleanup = relay_cleanup.clone();
                        requests.spawn(async move {
                            let started = std::time::Instant::now();
                            let _guard = gate.lock().await;
                            let previous = cleanup.lock().unwrap_or_else(|error| error.into_inner()).clone();
                            if let Some((previous, previous_id)) = previous {
                                task_state.manager.stop_client_relay(&previous, &route_socket, previous_id).await;
                                if previous != attach.project_id {
                                    task_state.manager.detach_socket_with_cause(&format!("relay:{route_socket}"), Some(&previous), microcrab_host_protocol::ErrorCode::Disconnected, "Browser replaced its native Host relay with another Project").await;
                                }
                            }
                            let id = uuid::Uuid::new_v4();
                            *cleanup.lock().unwrap_or_else(|error| error.into_inner()) = Some((attach.project_id.clone(), id));
                            let (inbound_tx, inbound_rx) = mpsc::channel(LANE);
                            *slot.lock().unwrap_or_else(|error| error.into_inner()) = Some(crate::manager::direct::RelayBytes { id, sender: inbound_tx });
                            let result = task_state.manager.attach_client_relay(&task_principal, &route_socket, attach, outbound, inbound_rx, slot, id).await;
                            command_completed(&result, started);
                            (request_id, result)
                        }.instrument(span));
                    }
                    GlobalMethod::ConversationAttach => {
                        if requests.len() >= 32 {
                            let result: Result<Value, WireError> = Err(WireError::new("command_limit", "Too many pending commands")); span.in_scope(|| command_completed(&result, started)); if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
                            continue;
                        }
                        let attach = match params::<crate::manager::conversations::Attach>(&command) {
                            Ok(attach) => attach,
                            Err(error) => { let result: Result<Value, WireError> = Err(error); span.in_scope(|| command_completed(&result, started)); if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; } continue; }
                        };
                        if !state.manager.allows_conversation(&principal, &attach.conversation_id).await {
                            let result: Result<Value, WireError> = Err(WireError::new("unavailable", "Conversation unavailable")); span.in_scope(|| command_completed(&result, started)); if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
                            continue;
                        }
                        host_forwarders.retain(|_, task| !task.is_finished());
                        if host_forwarders.len() >= 8 {
                            let result: Result<Value, WireError> = Err(WireError::new("host_route_limit", "Too many Host routes")); span.in_scope(|| command_completed(&result, started)); if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
                            continue;
                        }
                        let generation = state.manager.next_connection_generation();
                        let (outbound, host_frames) = mpsc::channel::<HostFrame>(LANE);
                        let id = attach.conversation_id.clone();
                        host_forwarders.insert((id.clone(), generation), tokio::spawn(forward_host(host_frames, host_sender.clone(), id, generation, cancellation.clone())));
                        let (task_state, task_principal, route_socket) = (state.clone(), principal.clone(), socket_id.clone());
                        requests.spawn(async move {
                            let started = std::time::Instant::now();
                            let result = task_state.manager.attach_conversation(&task_principal, &route_socket, attach, outbound, generation).await;
                            command_completed(&result, started);
                            (request_id, result)
                        }.instrument(span));
                    }
                    GlobalMethod::ConversationBinding => {
                        if requests.len() >= 32 {
                            let result: Result<Value, WireError> = Err(WireError::new("command_limit", "Too many pending commands")); span.in_scope(|| command_completed(&result, started)); if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
                            continue;
                        }
                        let (task_state, task_principal, task_command) = (state.clone(), principal.clone(), command);
                        requests.spawn(async move {
                            let started = std::time::Instant::now();
                            let result = task_state.manager.command(&task_principal, &task_command).await;
                            command_completed(&result, started);
                            (request_id, result)
                        }.instrument(span));
                    }
                    GlobalMethod::ConversationDetach => {
                        let result = match params::<Detach>(&command) {
                            Ok(detach) if state.manager.allows_conversation(&principal, &detach.conversation_id).await => {
                                state.manager.detach_conversation_socket(&socket_id, Some(&detach.conversation_id)).await;
                                let keys: Vec<_> = host_forwarders.keys().filter(|(id, _)| id == &detach.conversation_id).cloned().collect();
                                for key in keys { if let Some(task) = host_forwarders.remove(&key) { task.abort(); } }
                                Ok(json!({"detached": true}))
                            }
                            Ok(_) => Err(WireError::new("unavailable", "Conversation unavailable")),
                            Err(error) => Err(error),
                        };
                        span.in_scope(|| command_completed(&result, started));
                        if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
                    }
                }
            },
        }
    }
    // Teardown order: cancel, drain waiters, detach this socket's routes, abort
    // forwarders, drop senders, await the writer.
    cancellation.cancel();
    requests.abort_all();
    while requests.join_next().await.is_some() {}
    if let Some(watcher) = watcher {
        watcher.abort();
        let _ = watcher.await;
    }
    relay_in
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .take();
    let relay = relay_cleanup
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .take();
    if let Some((project, id)) = relay {
        state
            .manager
            .stop_client_relay(&project, &socket_id, id)
            .await;
    }
    state
        .manager
        .detach_socket_with_cause(
            &format!("relay:{socket_id}"),
            None,
            microcrab_host_protocol::ErrorCode::Disconnected,
            "Browser socket closed its native Host relay",
        )
        .await;
    state
        .manager
        .detach_conversation_socket(&socket_id, None)
        .await;
    for (_, task) in host_forwarders {
        task.abort();
        let _ = task.await;
    }
    drop(relay_out);
    drop(host_sender);
    drop(sender);
    let _ = writer.await;
}

async fn forward_host(
    mut input: mpsc::Receiver<HostFrame>,
    output: mpsc::Sender<Value>,
    conversation: String,
    generation: u64,
    cancellation: CancellationToken,
) {
    loop {
        let frame = tokio::select! { biased; _ = cancellation.cancelled() => break, frame = input.recv() => match frame { Some(frame) => frame, None => break } };
        let value = json!({"lane":"conversation_host","conversation_id":conversation,"connection_generation":generation.to_string(),"frame":frame});
        let delivered = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            result = tokio::time::timeout(Duration::from_secs(2), output.send(value)) => matches!(result, Ok(Ok(()))),
        };
        if !delivered {
            cancellation.cancel();
            break;
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

async fn watch_sessions(
    mut receiver: tokio::sync::broadcast::Receiver<
        Arc<crate::manager::session_watch::SessionChange>,
    >,
    owner: String,
    subscription_id: String,
    generation: String,
    sender: mpsc::Sender<Delivery>,
    cancellation: CancellationToken,
) {
    use crate::manager::{ResyncReason, SessionRuntimeEvent};
    let mut sequence = 0u64;
    loop {
        let (event, terminal) = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            change = receiver.recv() => match change {
                Ok(change) if change.owner == owner => {
                    let (event, terminal) = match change.event {
                        "sessions.changed" => (SessionRuntimeEvent::SessionsChanged(change.data.clone()), false),
                        "session.cleanup_failed" => (SessionRuntimeEvent::SessionCleanupFailed(change.data.clone()), false),
                        "resync_required" => {
                            let reason = match serde_json::from_value::<ResyncReason>(change.data.clone()) {
                                Ok(reason) => reason,
                                Err(error) => ResyncReason::ListPublicationFailed {
                                    code: "invalid_resync_reason".into(),
                                    message: format!("Owner watch resync reason failed validation: {error}"),
                                },
                            };
                            (SessionRuntimeEvent::ResyncRequired(reason), true)
                        },
                        _ => continue,
                    };
                    (event, terminal)
                },
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => (SessionRuntimeEvent::ResyncRequired(ResyncReason::SlowConsumer), true),
                Err(_) => break,
            },
        };
        let Some(next) = sequence.checked_add(1) else {
            break;
        };
        sequence = next;
        let frame = ServerFrame::Event {
            v: microcrab_protocol::application::VERSION,
            subscription_id: subscription_id.clone(),
            generation: generation.clone(),
            sequence,
            session_id: String::new(),
            event,
        };
        if terminal {
            writer::finish(&sender, frame, &cancellation).await;
            break;
        }
        if send(&sender, frame, &cancellation).await.is_err() {
            cancellation.cancel();
            break;
        }
    }
}
