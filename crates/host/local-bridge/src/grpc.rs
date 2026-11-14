//! gRPC direct carrier. local-bridge is the server. One Control stream owns the lease;
//! each admitted Execute is its own HTTP/2 stream so a drain does not block other tasks.
//! Frames are the same protobuf `HostFrame` the WebSocket relay carries.
use crate::{BridgeService, session::Live};
use microcrab_host_protocol::{
    GRPC_EXECUTE_TOKEN_HEADER, HostBridge, HostBridgeServer, MAX_FRAME_BYTES, MAX_PENDING_REQUESTS,
    WireFrame, frame_from_proto, frame_to_proto, pb,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming, metadata::MetadataMap};

const HTTP2_WINDOW: u32 = 1024 * 1024;
const MAX_STREAMS: u32 = 1 + 32 + 8;

/// One process-wide gRPC service. Tonic holds it behind its own `Arc`.
struct Service {
    bridge: Arc<BridgeService>,
    origin: String,
    // A fresh secret identifies one authenticated Control, exact Bridge and lease.
    // Per-workspace numeric generations may collide and cannot select a workspace.
    sessions: Arc<Mutex<HashMap<String, Arc<Live>>>>,
}

pub(crate) async fn serve(
    bridge: Arc<BridgeService>,
    listener: tokio::net::TcpListener,
    origin: String,
) -> Result<(), std::io::Error> {
    let service = Service {
        bridge,
        origin,
        sessions: Arc::new(Mutex::new(HashMap::new())),
    };
    tonic::transport::Server::builder()
        .initial_connection_window_size(HTTP2_WINDOW)
        .initial_stream_window_size(HTTP2_WINDOW)
        .max_concurrent_streams(MAX_STREAMS)
        .add_service(
            HostBridgeServer::new(service)
                .max_decoding_message_size(MAX_FRAME_BYTES)
                .max_encoding_message_size(MAX_FRAME_BYTES),
        )
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))
}

fn same_origin(metadata: &MetadataMap, expected: &str) -> Result<(), Status> {
    let presented = metadata.get("origin").and_then(|value| value.to_str().ok());
    if presented != Some(expected) {
        return Err(Status::permission_denied(
            "origin rejected; the gRPC peer did not present the configured bridge origin",
        ));
    }
    Ok(())
}

fn status_for(error: microcrab_host_protocol::WireError) -> Status {
    use microcrab_host_protocol::ErrorCode;
    let message = format!("{} ({:?})", error.message, error.code);
    match error.code {
        ErrorCode::Timeout => Status::deadline_exceeded(message),
        ErrorCode::PairingDenied | ErrorCode::Denied | ErrorCode::WorkspaceDenied => {
            Status::permission_denied(message)
        }
        ErrorCode::BindingMismatch | ErrorCode::Conflict | ErrorCode::OperationConflict => {
            Status::failed_precondition(message)
        }
        ErrorCode::InvalidRequest => Status::invalid_argument(message),
        ErrorCode::Limit => Status::resource_exhausted(message),
        _ => Status::failed_precondition(message),
    }
}

async fn forward_frames(
    mut control: mpsc::Receiver<WireFrame>,
    mut chunks: mpsc::Receiver<WireFrame>,
    mut diagnostics: mpsc::Receiver<WireFrame>,
    outbound: mpsc::Sender<Result<pb::HostFrame, Status>>,
) {
    loop {
        let frame = tokio::select! {
            biased;
            frame = control.recv() => frame,
            frame = diagnostics.recv() => frame,
            frame = chunks.recv() => frame,
        };
        let Some(frame) = frame else {
            break;
        };
        match frame_to_proto(&frame) {
            Ok(proto) => {
                if outbound.send(Ok(proto)).await.is_err() {
                    break;
                }
            }
            Err(error) => {
                let _ = outbound.send(Err(Status::internal(error.message))).await;
                break;
            }
        }
    }
}

#[tonic::async_trait]
impl HostBridge for Service {
    type ControlStream = ReceiverStream<Result<pb::HostFrame, Status>>;
    type ExecuteStream = ReceiverStream<Result<pb::HostFrame, Status>>;

    async fn control(
        &self,
        request: Request<Streaming<pb::HostFrame>>,
    ) -> Result<Response<Self::ControlStream>, Status> {
        same_origin(request.metadata(), &self.origin)?;
        let mut inbound = request.into_inner();
        let first = tokio::time::timeout(Duration::from_secs(5), inbound.message())
            .await
            .map_err(|_| Status::deadline_exceeded("handshake timeout"))?
            .map_err(|status| Status::invalid_argument(status.message().to_owned()))?;
        let Some(first) = first else {
            return Err(Status::invalid_argument(
                "Host closed before the pairing or resume frame",
            ));
        };
        let frame = frame_from_proto(first).map_err(status_for)?;
        let (bridge, hello) = self.bridge.authenticate(frame).await.map_err(status_for)?;
        let WireFrame::Hello {
            lease_generation: generation,
            ..
        } = &hello
        else {
            return Err(Status::internal("authenticated Host did not return Hello"));
        };
        let generation = generation.clone();
        tracing::info!(lease_generation = %generation, "gRPC Host controller authenticated");
        let (control_tx, control_rx) = mpsc::channel(MAX_PENDING_REQUESTS);
        let (chunk_tx, chunk_rx) = mpsc::channel(8);
        let (diagnostic_tx, diagnostic_rx) = mpsc::channel(8);
        let live = Live::new(
            bridge,
            generation.clone(),
            control_tx,
            chunk_tx,
            diagnostic_tx,
            false,
        );
        let capability = uuid::Uuid::new_v4().to_string();
        let mut capability_metadata: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
            capability
                .parse()
                .map_err(|_| Status::internal("cannot encode the Execute capability"))?;
        capability_metadata.set_sensitive(true);
        self.sessions
            .lock()
            .await
            .insert(capability.clone(), Arc::clone(&live));
        let (out_tx, out_rx) = mpsc::channel(32);
        let hello_proto = match frame_to_proto(&hello) {
            Ok(frame) => frame,
            Err(error) => {
                self.sessions.lock().await.remove(&capability);
                return Err(status_for(error));
            }
        };
        if out_tx.send(Ok(hello_proto)).await.is_err() {
            self.sessions.lock().await.remove(&capability);
            return Err(Status::unavailable("cannot queue authenticated Hello"));
        }
        let sessions = Arc::clone(&self.sessions);
        let retirement_capability = capability.clone();
        tokio::spawn(async move {
            let notifications = live.watch_processes();
            let mut forwarder =
                tokio::spawn(forward_frames(control_rx, chunk_rx, diagnostic_rx, out_tx));
            let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
            let close_reason: &'static str;
            loop {
                tokio::select! {
                    biased;
                    _ = heartbeat.tick() => {
                        if !live.lease_ok().await {
                            close_reason = "lease_expired_or_replaced";
                            break;
                        }
                    }
                    _ = &mut forwarder => {
                        close_reason = "writer_stopped";
                        break;
                    }
                    incoming = inbound.message() => {
                        let proto = match incoming {
                            Ok(Some(proto)) => proto,
                            Ok(None) => {
                                close_reason = "peer_closed";
                                break;
                            }
                            Err(status) => {
                                tracing::warn!(reason = %status.message(), "gRPC control frame rejected");
                                close_reason = "invalid_frame";
                                break;
                            }
                        };
                        let frame = match frame_from_proto(proto) {
                            Ok(frame) => frame,
                            Err(error) => {
                                tracing::warn!(reason = %error.message, "gRPC control frame rejected");
                                close_reason = "invalid_frame";
                                break;
                            }
                        };
                        if let crate::session::ControlDecision::Stop(reason) = live.on_grpc_control(frame).await {
                            close_reason = reason;
                            break;
                        }
                    }
                }
            }
            notifications.abort();
            sessions.lock().await.remove(&retirement_capability);
            live.shutdown(close_reason).await;
            drop(live);
            forwarder.abort();
        });
        let mut response = Response::new(ReceiverStream::new(out_rx));
        response
            .metadata_mut()
            .insert(GRPC_EXECUTE_TOKEN_HEADER, capability_metadata);
        Ok(response)
    }

    async fn execute(
        &self,
        request: Request<pb::HostFrame>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        same_origin(request.metadata(), &self.origin)?;
        let capability = request
            .metadata()
            .get(GRPC_EXECUTE_TOKEN_HEADER)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| {
                Status::unauthenticated("Execute requires an authenticated Control capability")
            })?;
        let live = self
            .sessions
            .lock()
            .await
            .get(capability)
            .cloned()
            .ok_or_else(|| {
                Status::unauthenticated("Execute capability is unknown or its Control has closed")
            })?;
        let frame = frame_from_proto(request.into_inner()).map_err(status_for)?;
        if !matches!(&frame, WireFrame::Request { .. }) {
            return Err(Status::invalid_argument(
                "Execute accepts one Host request frame",
            ));
        }
        let (frames_tx, mut frames_rx) = mpsc::channel(8);
        if let crate::session::ControlDecision::Stop(reason) =
            live.on_execute(frame, frames_tx).await
        {
            return Err(Status::failed_precondition(reason));
        }
        let (out_tx, out_rx) = mpsc::channel(8);
        tokio::spawn(async move {
            while let Some(frame) = frames_rx.recv().await {
                match frame_to_proto(&frame) {
                    Ok(proto) => {
                        if out_tx.send(Ok(proto)).await.is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = out_tx.send(Err(Status::internal(error.message))).await;
                        break;
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(out_rx)))
    }
}

#[cfg(test)]
mod tests;
