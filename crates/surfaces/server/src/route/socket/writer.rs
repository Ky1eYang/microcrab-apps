//! One writer task per socket owns the sink. Producers enqueue into bounded
//! lanes; the writer re-checks authentication before each frame, enforces the
//! frame cap and the 2s send timeout, and closes after a terminal frame.
use super::{DeliveryEvidence, ServerFrame};
use tracing::Instrument;
use crate::{Application, auth::Principal};
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, stream::SplitSink};
use microcrab_protocol::application::MAX_FRAME_BYTES;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

pub(crate) enum Delivery {
    Frame(ServerFrame),
    /// Project observations are fenced again at delivery, not only at sampling.
    ProjectHost {
        project: String,
        watch: CancellationToken,
        frame: ServerFrame,
    },
    /// Written, then the socket closes (resync, refused subscription).
    Last(ServerFrame),
    /// The reader waits for this frame to reach the sink before cancelling teardown.
    Terminal(ServerFrame, oneshot::Sender<()>),
}

/// Bounded queue capacity shared by both socket kinds.
pub(crate) const LANE: usize = 64;

pub(crate) async fn send(
    sender: &mpsc::Sender<Delivery>,
    frame: ServerFrame,
    cancellation: &CancellationToken,
) -> Result<(), &'static str> {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err("delivery_cancelled"),
        result = tokio::time::timeout(Duration::from_secs(2), sender.send(Delivery::Frame(frame))) =>
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) => {
                    tracing::debug!(phase = "queue_admission", reason = "writer_closed", "Socket delivery stopped");
                    Err("writer_closed")
                }
                Err(_) => {
                    tracing::warn!(phase = "queue_admission", reason = "queue_timeout", timeout_ms = 2000, "Socket delivery stopped");
                    Err("queue_timeout")
                }
            },
    }
}

/// A protocol error must reach the sink before the reader cancels this socket.
/// Both queue admission and acknowledgement are bounded, including a stuck peer.
/// The writer acknowledges only successful writes and then closes the socket.
pub(crate) async fn finish(
    sender: &mpsc::Sender<Delivery>,
    frame: ServerFrame,
    cancellation: &CancellationToken,
) {
    let (acknowledge, delivered) = oneshot::channel();
    let queued = tokio::select! {
        biased;
        _ = cancellation.cancelled() => false,
        result = tokio::time::timeout(Duration::from_secs(2), sender.send(Delivery::Terminal(frame, acknowledge))) =>
            matches!(result, Ok(Ok(()))),
    };
    if queued {
        let _ = tokio::time::timeout(Duration::from_secs(3), delivered).await;
    }
}

async fn lane_frame<T>(host: &mut Option<mpsc::Receiver<T>>) -> Option<T> {
    match host {
        Some(host) => host.recv().await,
        None => std::future::pending().await,
    }
}

/// `host` is the global socket's Conversation Host lane; it is drained first so
/// a pending command awaiting a Host reply is never starved by results.
pub(crate) fn spawn(
    state: Arc<Application>,
    principal: Principal,
    mut sink: SplitSink<WebSocket, Message>,
    mut outgoing: mpsc::Receiver<Delivery>,
    mut host: Option<mpsc::Receiver<Value>>,
    mut relay: Option<mpsc::Receiver<Vec<u8>>>,
    cancellation: CancellationToken,
    evidence: DeliveryEvidence,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let (text, close, host_lane, acknowledge, sequence) = tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                frame = lane_frame(&mut host) => match frame {
                    Some(frame) => match serde_json::to_string(&frame) { Ok(text) => (text, false, true, None, None), Err(_) => { evidence.record("frame_encode_failed"); break } },
                    None => { evidence.record("host_lane_closed"); break },
                },
                bytes = lane_frame(&mut relay) => {
                    let Some(bytes) = bytes else { evidence.record("relay_lane_closed"); break };
                    if state.auth.check_active(&principal).is_err() { evidence.record("authentication_lost"); break; }
                    if bytes.len() > microcrab_host_protocol::MAX_FRAME_BYTES { evidence.record("frame_too_large"); break; }
                    match tokio::time::timeout(Duration::from_secs(10), sink.send(Message::Binary(bytes.into()))).await {
                        Ok(Ok(())) => {},
                        Ok(Err(_)) => { evidence.record("relay_sink_error"); break; },
                        Err(_) => { evidence.record("relay_send_timeout"); break; },
                    }
                    continue;
                },
                delivery = outgoing.recv() => {
                    let (frame, close, acknowledge) = match delivery {
                        Some(Delivery::Frame(frame)) => (frame, false, None),
                        Some(Delivery::ProjectHost { project, watch, mut frame }) => {
                            if watch.is_cancelled() { continue; }
                            if let Err(error) = state.manager.authorize_project_host_watch(&principal, &project).await {
                                frame = match frame {
                                    ServerFrame::Result { request_id, .. } => ServerFrame::result(request_id, Err(error)),
                                    ServerFrame::ProjectHostEvent { v, project_id, subscription_id, sequence, .. } =>
                                        ServerFrame::ProjectHostEvent { v, project_id, subscription_id, sequence, summary: None },
                                    _ => continue,
                                };
                            }
                            if watch.is_cancelled() { continue; }
                            (frame, false, None)
                        },
                        Some(Delivery::Last(frame)) => (frame, true, None),
                        Some(Delivery::Terminal(frame, acknowledge)) => (frame, true, Some(acknowledge)),
                        None => { evidence.record("delivery_lane_closed"); break },
                    };
                    let sequence = match &frame {
                        ServerFrame::Event { sequence, .. } | ServerFrame::Subscribed { sequence, .. } => Some(*sequence),
                        _ => None,
                    };
                    match serde_json::to_string(&frame) { Ok(text) => (text, close, false, acknowledge, sequence), Err(_) => { evidence.record("frame_encode_failed"); break } }
                },
            };
            if state.auth.check_active(&principal).is_err() {
                evidence.record("authentication_lost");
                break;
            }
            if text.len() > MAX_FRAME_BYTES
                || (host_lane && text.len() > microcrab_host_protocol::MAX_FRAME_BYTES + 4096)
            {
                evidence.record("frame_too_large");
                break;
            }
            let frame_bytes = text.len();
            let sent = tokio::time::timeout(
                Duration::from_secs(2),
                sink.send(Message::Text(text.into())),
            )
            .await;
            match sent {
                Ok(Ok(())) => { if let Some(sequence) = sequence { evidence.written(sequence); } }
                Ok(Err(_)) => {
                    evidence.record("sink_error");
                    tracing::debug!(
                        phase = "sink_send",
                        reason = "sink_error",
                        frame_bytes,
                        "Socket delivery stopped"
                    );
                    break;
                }
                Err(_) => {
                    evidence.record("send_timeout");
                    tracing::warn!(
                        phase = "sink_send",
                        reason = "send_timeout",
                        frame_bytes,
                        timeout_ms = 2000,
                        "Socket delivery stopped"
                    );
                    break;
                }
            }
            if let Some(acknowledge) = acknowledge {
                let _ = acknowledge.send(());
            }
            if close {
                evidence.record("terminal_frame_written");
                break;
            }
        }
        cancellation.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), sink.close()).await;
    }.in_current_span())
}
