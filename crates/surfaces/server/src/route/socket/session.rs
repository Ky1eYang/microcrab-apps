//! Session socket loop. The path is the only Session authority: every command
//! is bound to it here, so no client frame can name another Session.
//!
//! Opening subscribes. The first frame is `subscribed` (or one `error` frame,
//! then close, when the Session is not owned or unavailable). Closing releases
//! the subscription and detaches every client-tool lease this socket bound.
use super::{
    Rejected, ServerFrame, SessionMethod, SessionQuery, command_completed, command_span, decode,
    display::{self, DisplayRequest, Forward},
    params,
    writer::{self, Delivery, LANE, send},
};
use crate::{Application, auth::Principal};
use axum::extract::ws::{Message, WebSocket};
use futures_util::StreamExt;
use microcrab_protocol::application::{VERSION, WireError};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

enum Completed {
    Command {
        request_id: String,
        result: Result<Value, WireError>,
    },
}

pub(super) async fn connection(
    state: Arc<Application>,
    principal: Principal,
    session_id: String,
    query: SessionQuery,
    socket: WebSocket,
    socket_id: String,
) {
    let cancellation = CancellationToken::new();
    let mut log = super::ConnectionLog::open();
    let (sink, mut source) = socket.split();
    let (sender, outgoing) = mpsc::channel::<Delivery>(LANE);
    let writer = writer::spawn(
        state.clone(),
        principal.clone(),
        sink,
        outgoing,
        None,
        None,
        cancellation.clone(),
        log.evidence.clone(),
    );
    let subscribe_started = std::time::Instant::now();
    let opening = async {
        tokio::time::timeout(
            Duration::from_secs(12),
            state.manager.subscribe_with_lineage(&principal, &session_id, query.history_lineage.as_deref()),
        ).await.unwrap_or_else(|_| {
            let mut error = WireError::new("busy", "Session subscription preparation timed out");
            error.retryable = true;
            Err(error)
        })
    };
    tokio::pin!(opening);
    let result = loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break None,
            result = &mut opening => break Some(result),
            incoming = source.next() => match incoming {
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) => { log.evidence.record("peer_closed_during_attach"); break None; }
                Some(Err(_)) => { log.evidence.record("reader_error_during_attach"); break None; }
                None => { log.evidence.record("reader_eof_during_attach"); break None; }
                Some(Ok(_)) => break Some(Err(WireError::new("subscription_pending", "Wait for Session subscription before sending commands"))),
            }
        }
    };
    let Some(result) = result else {
        log.reason = "attachment_cancelled";
        cancellation.cancel();
        let _ = writer.await;
        return;
    };
    let subscription = match result {
        Ok(subscription) => subscription,
        Err(error) => {
            log.reason = "subscription_rejected";
            log.evidence.record(log.reason);
            tracing::warn!(event = "ws.reject", error_code = %error.code, elapsed_ms = subscribe_started.elapsed().as_millis() as u64, "WS subscription rejected");
            let _ = sender
                .send(Delivery::Last(ServerFrame::Error { v: VERSION, error }))
                .await;
            drop(sender);
            let _ = writer.await;
            return;
        }
    };
    tracing::info!(event = "ws.subscribed", subscription_id = %subscription.id, generation = %subscription.generation, elapsed_ms = subscribe_started.elapsed().as_millis() as u64, "Session subscription prepared");
    let (resume, initial_events) = crate::manager::publication::attach(
        &subscription.snapshot,
        &crate::manager::publication::Resume {
            history_lineage: query.history_lineage.clone(),
        },
    );
    let pending = state
        .manager
        .pending_requests(&principal, &session_id)
        .await;
    let initial = DisplayRequest {
        history_lineage: query.history_lineage.clone(),
        display_watermark: query.display_watermark,
        ..DisplayRequest::default()
    };
    let subscribed = ServerFrame::Subscribed {
        v: VERSION,
        subscription_id: subscription.id.clone(),
        generation: subscription.generation.clone(),
        sequence: subscription.sequence,
        session_id: session_id.clone(),
        meta: Value::Object(crate::manager::publication::meta(&subscription.snapshot)),
        pending,
        resume: super::SubscriptionResume { mode: resume },
        initial_events: initial_events.len(),
    };
    // The coherent snapshot goes out before delivery starts from its barrier.
    if let Err(reason) = send(&sender, subscribed, &cancellation).await {
        log.reason = "subscription_delivery_failed";
        if reason != "delivery_cancelled" { log.evidence.record(reason); }
        cancellation.cancel();
        drop(sender);
        let _ = writer.await;
        return;
    }
    // Initialization owns the wire until every announced typed event is sent.
    // Hub sequence remains the private barrier; synthetic initial/display pages
    // advance only the socket sequence, never the Hub's filtering watermark.
    let mut wire_sequence = subscription.sequence;
    log.evidence.enqueued(wire_sequence);
    for event in initial_events {
        let Some(next) = wire_sequence.checked_add(1) else {
            log.evidence.record("sequence_exhausted");
            cancellation.cancel();
            break;
        };
        wire_sequence = next;
        let frame = ServerFrame::Event {
            v: VERSION,
            subscription_id: subscription.id.clone(),
            generation: subscription.generation.clone(),
            sequence: wire_sequence,
            session_id: session_id.clone(),
            event,
        };
        if let Err(reason) = send(&sender, frame, &cancellation).await {
            if reason != "delivery_cancelled" { log.evidence.record(reason); }
            cancellation.cancel();
            break;
        }
        log.evidence.enqueued(wire_sequence);
    }
    let (display_sender, display_receiver) = mpsc::channel::<DisplayRequest>(8);
    let forwarder = tokio::spawn(display::forward(
        Forward {
            subscription,
            wire_sequence,
            session_id: session_id.clone(),
            sender: sender.clone(),
            cancellation: cancellation.clone(),
            evidence: log.evidence.clone(),
            state: state.clone(),
            principal: principal.clone(),
            initial,
        },
        display_receiver,
    ).in_current_span());
    let mut requests = JoinSet::<Completed>::new();
    let mut expiry = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => { log.reason = "delivery_cancelled"; break; },
            _ = expiry.tick() => { if state.auth.check_active(&principal).is_err() { log.reason = "authentication_lost"; break; } },
            completed = requests.join_next(), if !requests.is_empty() => {
                let Some(Ok(Completed::Command { request_id, result })) = completed else { log.reason = "command_task_failed"; break };
                if state.auth.check_active(&principal).is_err() { log.reason = "authentication_lost"; break; }
                if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
            },
            incoming = source.next() => {
                let message = match incoming { Some(Ok(message)) => message, Some(Err(_)) => { log.reason = "reader_error"; break; }, None => { log.reason = "reader_eof"; break; } };
                if state.auth.check_active(&principal).is_err() { log.reason = "authentication_lost"; break; }
                let Message::Text(text) = message else {
                    if matches!(message, Message::Ping(_) | Message::Pong(_)) { continue; }
                    log.reason = if matches!(message, Message::Close(_)) { "peer_closed" } else { "unsupported_frame" };
                    break;
                };
                let (mut command, method) = match decode::<SessionMethod>(&text) {
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
                // The path is the Session authority; manager code reads session_id.
                command.session_id = Some(session_id.clone());
                let span = command_span(&command, &socket_id);
                tracing::info!(parent: &span, "WS command received");
                span.record("session_id", super::super::provenance::identity(&session_id));
                let started = std::time::Instant::now();
                if matches!(method, SessionMethod::DisplaySubscribe) {
                    let result = match params::<DisplayRequest>(&command) {
                        Ok(request) => display_sender.try_send(request).map(|_| serde_json::json!({"accepted": true})).map_err(|_| WireError::new("capacity", "Display subscription queue is full")),
                        Err(error) => Err(error),
                    };
                    span.in_scope(|| command_completed(&result, started));
                    if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
                    continue;
                }
                if matches!(method, SessionMethod::RoundLimitReply) {
                    let result = state.manager.round_limit_reply(&principal, &socket_id, &command).instrument(span.clone()).await;
                    span.in_scope(|| command_completed(&result, started));
                    if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
                    continue;
                }
                if matches!(method, SessionMethod::PermissionProposalReply) {
                    let result = state.manager.permission_proposal_reply(&principal, &socket_id, &command).instrument(span.clone()).await;
                    span.in_scope(|| command_completed(&result, started));
                    if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
                    continue;
                }
                if matches!(method, SessionMethod::PermissionReply) {
                    let result = state.manager.permission_reply(&principal, &socket_id, &command).instrument(span.clone()).await;
                    span.in_scope(|| command_completed(&result, started));
                    if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
                    continue;
                }
                if matches!(method, SessionMethod::ClientToolsSecret) {
                    if requests.len() >= 32 {
                        let result: Result<Value, WireError> = Err(WireError::new("command_limit", "Too many pending commands"));
                        span.in_scope(|| command_completed(&result, started));
                        if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
                        continue;
                    }
                    let (task_state, task_principal, task_socket) = (state.clone(), principal.clone(), socket_id.clone());
                    requests.spawn(async move {
                        let result = if task_state.auth.check_active(&task_principal).is_err() {
                            Err(WireError::new("unauthenticated", "Authentication unavailable"))
                        } else {
                            task_state.manager.client_tool_command(&task_principal, &task_socket, &command).await
                        };
                        command_completed(&result, started);
                        Completed::Command { request_id, result }
                    }.instrument(span));
                    continue;
                }
                if matches!(method, SessionMethod::ClientToolsBind | SessionMethod::ClientToolsResult | SessionMethod::ClientToolsUnbind | SessionMethod::ClientToolsPrepare | SessionMethod::ClientToolsOutput | SessionMethod::ClientToolsTakeover | SessionMethod::ClientToolsArguments | SessionMethod::ClientToolsSecretOperation | SessionMethod::ClientToolsDecision) {
                    let result = state.manager.client_tool_command(&principal, &socket_id, &command).instrument(span.clone()).await;
                    span.in_scope(|| command_completed(&result, started));
                    if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
                    continue;
                }
                // Reserve two lanes for interruption even while ordinary callers
                // are waiting on long compaction. Manager caps effect owners itself.
                let limit = if matches!(method, SessionMethod::SessionInterrupt) { 34 } else { 32 };
                if requests.len() >= limit {
                    let result: Result<Value, WireError> = Err(WireError::new("command_limit", "Too many pending commands"));
                    span.in_scope(|| command_completed(&result, started));
                    if send(&sender, ServerFrame::result(request_id, result), &cancellation).await.is_err() { log.reason = "delivery_failed"; break; }
                    continue;
                }
                let (task_state, task_principal, sender_socket) = (state.clone(), principal.clone(), socket_id.clone());
                requests.spawn(async move {
                    let started = std::time::Instant::now();
                    tracing::info!("WS command started");
                    let result = if task_state.auth.check_active(&task_principal).is_err() {
                        Err(WireError::new("unauthenticated", "Authentication unavailable"))
                    } else {
                        task_state.manager.command_with_sender(&task_principal, &command, Some(sender_socket)).await
                    };
                    command_completed(&result, started);
                    Completed::Command { request_id, result }
                }.instrument(span));
            },
        }
    }
    // Teardown order: cancel, drain waiters, detach this socket's client tools,
    // abort the forwarder, drop the sender, await the writer.
    log.evidence.record(log.reason);
    cancellation.cancel();
    requests.abort_all();
    while requests.join_next().await.is_some() {}
    state.manager.detach_client_tools(&socket_id).await;
    forwarder.abort();
    let _ = forwarder.await;
    drop(sender);
    let _ = writer.await;
}
