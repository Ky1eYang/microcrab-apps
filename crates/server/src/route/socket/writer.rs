//! One writer task per socket owns the sink. Producers enqueue into bounded
//! lanes; the writer re-checks authentication before each frame, enforces the
//! frame cap and the 2s send timeout, and closes after a terminal frame.
use super::ServerFrame;
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
) -> Result<(), ()> {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(()),
        result = tokio::time::timeout(Duration::from_secs(2), sender.send(Delivery::Frame(frame))) =>
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) => {
                    tracing::debug!(phase = "queue_admission", reason = "writer_closed", "Socket delivery stopped");
                    Err(())
                }
                Err(_) => {
                    tracing::warn!(phase = "queue_admission", reason = "queue_timeout", timeout_ms = 2000, "Socket delivery stopped");
                    Err(())
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
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let (text, close, host_lane, acknowledge) = tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                frame = lane_frame(&mut host) => match frame {
                    Some(frame) => match serde_json::to_string(&frame) { Ok(text) => (text, false, true, None), Err(_) => break },
                    None => break,
                },
                bytes = lane_frame(&mut relay) => {
                    let Some(bytes) = bytes else { break };
                    if state.auth.check_active(&principal).is_err()
                        || bytes.len() > microcrab_host_protocol::MAX_FRAME_BYTES {
                        break;
                    }
                    if !matches!(tokio::time::timeout(Duration::from_secs(10), sink.send(Message::Binary(bytes.into()))).await, Ok(Ok(()))) {
                        break;
                    }
                    continue;
                },
                delivery = outgoing.recv() => {
                    let (frame, close, acknowledge) = match delivery {
                        Some(Delivery::Frame(frame)) => (frame, false, None),
                        Some(Delivery::Last(frame)) => (frame, true, None),
                        Some(Delivery::Terminal(frame, acknowledge)) => (frame, true, Some(acknowledge)),
                        None => break,
                    };
                    match serde_json::to_string(&frame) { Ok(text) => (text, close, false, acknowledge), Err(_) => break }
                },
            };
            if state.auth.check_active(&principal).is_err() {
                break;
            }
            if text.len() > MAX_FRAME_BYTES
                || (host_lane && text.len() > microcrab_host_protocol::MAX_FRAME_BYTES + 4096)
            {
                break;
            }
            let frame_bytes = text.len();
            let sent = tokio::time::timeout(
                Duration::from_secs(2),
                sink.send(Message::Text(text.into())),
            )
            .await;
            match sent {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    tracing::debug!(
                        phase = "sink_send",
                        reason = "sink_error",
                        frame_bytes,
                        "Socket delivery stopped"
                    );
                    break;
                }
                Err(_) => {
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
                break;
            }
        }
        let _ = tokio::time::timeout(Duration::from_secs(1), sink.close()).await;
        cancellation.cancel();
    })
}
