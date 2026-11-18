//! Exact Origin/authority admission, bounded queues and first-frame pairing.
use crate::BridgeService;
use crate::session::{ControlDecision, Live};
use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{Sink, SinkExt, StreamExt};
use microcrab_host_protocol::*;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Semaphore, mpsc};

#[derive(Clone)]
struct Endpoint {
    bridge: Arc<BridgeService>,
    origin: String,
    authority: String,
    sockets: Arc<Semaphore>,
}
pub(crate) async fn serve(
    bridge: Arc<BridgeService>,
    listener: tokio::net::TcpListener,
    origin: String,
) -> Result<(), std::io::Error> {
    let address = listener.local_addr()?;
    if !address.ip().is_loopback() {
        return Err(std::io::Error::other("bridge listener must be loopback"));
    }
    let endpoint = Endpoint {
        bridge,
        origin,
        authority: address.to_string(),
        sockets: Arc::new(Semaphore::new(4)),
    };
    axum::serve(
        listener,
        Router::new()
            .route("/host/signal", get(signal_upgrade))
            .route("/host", get(upgrade))
            .with_state(endpoint),
    )
    .await
}
async fn signal_upgrade(
    State(endpoint): State<Endpoint>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if headers.get("origin").and_then(|h| h.to_str().ok()) != Some(endpoint.origin.as_str())
        || headers.get("host").and_then(|h| h.to_str().ok()) != Some(endpoint.authority.as_str())
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Ok(permit) = endpoint.sockets.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    ws.max_message_size(4096)
        .max_frame_size(4096)
        .on_upgrade(move |mut socket| async move {
            let _permit = permit;
            let signal = match tokio::time::timeout(Duration::from_secs(5), socket.recv()).await {
                Ok(Some(Ok(Message::Text(text)))) => {
                    serde_json::from_str::<crate::outbound::Signal>(&text).ok()
                }
                _ => None,
            };
            let Some(signal) = signal else {
                return;
            };
            let (progress, mut results) = mpsc::channel(2);
            // The worker owns the permit for its full native lifetime. The browser
            // observation socket is neither a cancellation token nor a route lease.
            let bridge = endpoint.bridge;
            let origin = endpoint.origin;
            let worker = tokio::spawn(async move {
                let _permit = _permit;
                crate::outbound::run(bridge, signal, origin, progress).await;
            });
            drop(worker);
            let _ = socket
                .send(Message::Text(
                    serde_json::json!({"status":"connecting"})
                        .to_string()
                        .into(),
                ))
                .await;
            while let Some(value) = results.recv().await {
                if !matches!(
                    tokio::time::timeout(
                        Duration::from_secs(2),
                        socket.send(Message::Text(value.to_string().into()))
                    )
                    .await,
                    Ok(Ok(()))
                ) {
                    return;
                }
            }
        })
}
async fn upgrade(
    State(endpoint): State<Endpoint>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if headers.get("origin").and_then(|h| h.to_str().ok()) != Some(endpoint.origin.as_str())
        || headers.get("host").and_then(|h| h.to_str().ok()) != Some(endpoint.authority.as_str())
    {
        tracing::warn!(
            reason = "origin_or_authority",
            "Host WebSocket upgrade rejected"
        );
        return StatusCode::FORBIDDEN.into_response();
    }
    let Ok(permit) = endpoint.sockets.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    ws.max_message_size(MAX_FRAME_BYTES)
        .max_frame_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            connection(endpoint.bridge, socket).await
        })
}
async fn send<S: Sink<Message> + Unpin>(socket: &mut S, frame: WireFrame) -> Result<(), ()> {
    // One binary message is one protobuf HostFrame, the same bytes gRPC carries.
    let bytes = encode_frame(&frame).map_err(|_| ())?;
    tokio::time::timeout(
        Duration::from_secs(10),
        socket.send(Message::Binary(bytes.into())),
    )
    .await
    .map_err(|_| ())?
    .map_err(|_| ())
}
/// The sole writer owns bounded egress and send deadlines. The reader never
/// awaits socket writes, so a slow peer cannot fence cancellation or lease pings.
async fn write_frames<S: Sink<Message> + Unpin>(
    mut socket: S,
    mut control: mpsc::Receiver<WireFrame>,
    mut chunks: mpsc::Receiver<WireFrame>,
    mut diagnostics: mpsc::Receiver<WireFrame>,
) -> Result<(), ()> {
    loop {
        let frame = tokio::select! {
            biased;
            frame = control.recv() => frame,
            frame = chunks.recv() => frame,
            frame = diagnostics.recv() => frame,
        };
        let Some(frame) = frame else {
            return Ok(());
        };
        send(&mut socket, frame).await?;
    }
}
async fn close_handshake(socket: &mut WebSocket, code: u16, reason: &'static str) {
    tracing::warn!(reason, "Host handshake rejected");
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        socket.send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        }))),
    )
    .await;
}
async fn connection(bridge: Arc<BridgeService>, mut socket: WebSocket) {
    tracing::info!("Host WebSocket opened; awaiting pairing or resume");
    let first = tokio::time::timeout(Duration::from_secs(5), socket.recv()).await;
    let frame = match first {
        Ok(Some(Ok(Message::Binary(bytes)))) => decode_frame(&bytes).ok(),
        Err(_) => {
            close_handshake(&mut socket, 4408, "handshake_timeout").await;
            return;
        }
        _ => None,
    };
    let Some(mut frame) = frame else {
        close_handshake(&mut socket, 4400, "invalid_handshake").await;
        return;
    };
    if let WireFrame::Inspect { request } = frame {
        let reply = bridge.inspect(request).await;
        let accepted = matches!(&reply, WireFrame::Inspected { result: Ok(_), .. });
        if send(&mut socket, reply).await.is_err() || !accepted {
            return;
        }
        frame = match tokio::time::timeout(Duration::from_secs(10), socket.recv()).await {
            Ok(Some(Ok(Message::Binary(bytes)))) => match decode_frame(&bytes) {
                Ok(frame) => frame,
                Err(_) => return,
            },
            _ => return,
        };
    }
    let handshake = match &frame {
        WireFrame::Pair { .. } => "pair",
        WireFrame::Resume { .. } => "resume",
        _ => "invalid",
    };
    let (bridge, hello) = match bridge.authenticate(frame).await {
        Ok(authenticated) => authenticated,
        Err(error) => {
            let _ = send(
                &mut socket,
                WireFrame::HandshakeRejected {
                    error: error.clone(),
                },
            )
            .await;
            let (code, reason) = if error.code == ErrorCode::Timeout {
                (4408, "pairing_expired")
            } else if error.code == ErrorCode::WorkspaceDenied {
                (4403, "workspace_denied")
            } else if error.code == ErrorCode::BindingMismatch {
                (4403, "binding_mismatch")
            } else if handshake == "resume" {
                (4401, "resume_rejected")
            } else if handshake == "pair" {
                (4401, "pairing_denied")
            } else {
                (4400, "invalid_handshake")
            };
            close_handshake(&mut socket, code, reason).await;
            return;
        }
    };
    let generation = match &hello {
        WireFrame::Hello {
            lease_generation, ..
        } => lease_generation.clone(),
        _ => return,
    };
    tracing::info!(handshake, lease_generation=%generation, "Host controller authenticated");
    if send(&mut socket, hello).await.is_err() {
        return;
    }
    let (outgoing, incoming) = mpsc::channel::<WireFrame>(MAX_PENDING_REQUESTS);
    let (chunk_outgoing, chunk_incoming) = mpsc::channel::<WireFrame>(8);
    let (diagnostic_outgoing, diagnostic_incoming) = mpsc::channel::<WireFrame>(8);
    let (writer, mut reader) = socket.split();
    let mut writer = tokio::spawn(write_frames(
        writer,
        incoming,
        chunk_incoming,
        diagnostic_incoming,
    ));
    let live = Live::new(
        bridge,
        generation.clone(),
        outgoing,
        chunk_outgoing,
        diagnostic_outgoing,
        true,
    );
    let notifications = live.watch_processes();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
    let close_reason: &'static str;
    loop {
        tokio::select! {
            biased;
            _ = heartbeat.tick() => {
                if !live.lease_ok().await {
                    close_reason = "lease_expired_or_replaced";
                    tracing::info!(lease_generation = %generation, reason = close_reason, "Host controller stopped");
                    break;
                }
            }
            _ = &mut writer => {
                close_reason = "writer_stopped";
                break;
            }
            message = reader.next() => {
                let frame = match message {
                    Some(Ok(Message::Binary(bytes))) => decode_frame(&bytes).ok(),
                    Some(Ok(Message::Close(_))) | None => {
                        close_reason = "peer_closed";
                        break;
                    }
                    Some(Ok(Message::Ping(_))) => continue,
                    _ => None,
                };
                let Some(frame) = frame else {
                    close_reason = "invalid_frame";
                    break;
                };
                if let ControlDecision::Stop(reason) = live.on_control(frame).await {
                    close_reason = reason;
                    break;
                }
            }
        }
    }
    notifications.abort();
    live.shutdown(close_reason).await;
    writer.abort();
}

#[cfg(test)]
mod tests;
