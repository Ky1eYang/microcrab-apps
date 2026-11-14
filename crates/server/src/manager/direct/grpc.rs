//! Server-dialed gRPC carrier. One authenticated Control supplies a private
//! Execute capability. Bounded child streams retain exact invocation identity;
//! a failed stream completes that caller without inventing an effect receipt.
use super::{Correlation, StopReason, failure};
use crate::manager::bindings;
use microcrab_host_protocol::{
    GRPC_EXECUTE_TOKEN_HEADER, HostBridgeClient, HostFrame, MAX_FRAME_BYTES, MAX_PENDING_REQUESTS,
    TransferRef, frame_from_proto, frame_to_proto,
};
use microcrab_protocol::application::WireError;
use std::{collections::HashMap, time::Duration};
use tokio::{
    sync::mpsc,
    task::{AbortHandle, Id, JoinSet},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::metadata::{Ascii, MetadataValue};
use tracing::{Instrument, instrument::WithSubscriber};

const HTTP2_WINDOW: u32 = 1024 * 1024;
const MAX_EXECUTES: usize = MAX_PENDING_REQUESTS + 8;

pub(super) struct Link {
    client: HostBridgeClient<tonic::transport::Channel>,
    inbound: tonic::Streaming<microcrab_host_protocol::pb::HostFrame>,
    control: mpsc::Sender<microcrab_host_protocol::pb::HostFrame>,
    origin: String,
    execute_token: MetadataValue<Ascii>,
}

fn status_error(action: &str, status: tonic::Status) -> WireError {
    failure(format!(
        "{action}: {} ({})",
        status.message(),
        status.code()
    ))
}

pub(super) async fn dial(
    url: &str,
    origin: &str,
    handshake: &HostFrame,
) -> Result<(Link, HostFrame), WireError> {
    let endpoint = tonic::transport::Endpoint::from_shared(url.to_owned())
        .map_err(|error| failure(format!("gRPC host address is invalid: {error}")))?
        .connect_timeout(Duration::from_secs(10))
        .initial_stream_window_size(HTTP2_WINDOW)
        .initial_connection_window_size(HTTP2_WINDOW);
    let channel = tokio::time::timeout(Duration::from_secs(10), endpoint.connect())
        .await
        .map_err(|_| failure("gRPC host dial exceeded 10 seconds"))?
        .map_err(|error| failure(format!("gRPC host connection failed: {error}")))?;
    let mut client = HostBridgeClient::new(channel)
        .max_decoding_message_size(MAX_FRAME_BYTES)
        .max_encoding_message_size(MAX_FRAME_BYTES);
    let (control, control_rx) = mpsc::channel(32);
    let mut request = tonic::Request::new(ReceiverStream::new(control_rx));
    request.metadata_mut().insert(
        "origin",
        MetadataValue::try_from(origin)
            .map_err(|_| failure("Server origin is not valid gRPC metadata"))?,
    );
    let hello_frame = frame_to_proto(handshake)
        .map_err(|error| failure(format!("Cannot encode Host handshake: {}", error.message)))?;
    control
        .send(hello_frame)
        .await
        .map_err(|_| failure("gRPC host disconnected while sending handshake"))?;
    let response = tokio::time::timeout(Duration::from_secs(10), client.control(request))
        .await
        .map_err(|_| failure("gRPC host control stream exceeded 10 seconds"))?
        .map_err(|status| status_error("gRPC host control stream rejected", status))?;
    let mut execute_token = response
        .metadata()
        .get(GRPC_EXECUTE_TOKEN_HEADER)
        .filter(|value| value.to_str().is_ok_and(|text| !text.is_empty()))
        .cloned()
        .ok_or_else(|| failure("Authenticated gRPC Control omitted its Execute capability"))?;
    execute_token.set_sensitive(true);
    let mut inbound = response.into_inner();
    let hello = tokio::time::timeout(Duration::from_secs(10), inbound.message())
        .await
        .map_err(|_| failure("gRPC host handshake exceeded 10 seconds"))?
        .map_err(|status| status_error("gRPC host handshake failed", status))?
        .ok_or_else(|| failure("gRPC host closed before authenticated Hello"))?;
    let hello = frame_from_proto(hello).map_err(|error| {
        failure(format!(
            "gRPC host returned an invalid handshake frame: {}",
            error.message
        ))
    })?;
    Ok((
        Link {
            client,
            inbound,
            control,
            origin: origin.to_owned(),
            execute_token,
        },
        hello,
    ))
}

#[derive(Clone)]
struct RouteWitness {
    manager: std::sync::Weak<super::super::Manager>,
    owner: String,
    project: String,
    socket: String,
    generation: u64,
}
impl RouteWitness {
    async fn remote(&self) -> Option<std::sync::Arc<microcrab_host_remote::RemoteHost>> {
        let manager = self.manager.upgrade()?;
        let routes = manager.routes.lock().await;
        routes
            .get(&self.project)
            .filter(|route| {
                route.socket == self.socket
                    && route.generation == self.generation
                    && route.owner == self.owner
            })
            .map(|route| route.remote.clone())
    }
    async fn fail(&self, request: &str, operation: &str, message: String) {
        if let Some(remote) = self.remote().await {
            remote.fail_request(self.generation, request, operation, message);
        }
    }
}

/// Only this operation's Response and declared transfer may enter its stream.
/// RemoteHost owns sequence, digest, receipt and generation validation.
async fn execute(
    mut client: HostBridgeClient<tonic::transport::Channel>,
    call: tonic::Request<microcrab_host_protocol::pb::HostFrame>,
    route: RouteWitness,
    request_id: String,
    operation_id: String,
) -> Result<(), String> {
    let response = tokio::time::timeout(Duration::from_secs(10), client.execute(call))
        .await
        .map_err(|_| "gRPC Host Execute admission exceeded 10 seconds".to_owned())?
        .map_err(|status| {
            format!(
                "gRPC Host Execute rejected: {} ({})",
                status.message(),
                status.code()
            )
        })?;
    let mut stream = response.into_inner();
    let mut transfer: Option<String> = None;
    let mut responded = false;
    loop {
        let proto = stream
            .message()
            .await
            .map_err(|status| {
                format!(
                    "gRPC Host Execute stream failed: {} ({})",
                    status.message(),
                    status.code()
                )
            })?
            .ok_or_else(|| {
                if responded {
                    "gRPC Host Execute ended before the declared transfer EOF".to_owned()
                } else {
                    "gRPC Host Execute ended before its response".to_owned()
                }
            })?;
        let frame = frame_from_proto(proto)
            .map_err(|error| format!("gRPC Host Execute frame rejected: {}", error.message))?;
        let terminal =
            match &frame {
                HostFrame::Response {
                    request_id: request,
                    operation_id: operation,
                    result,
                } if !responded && request == &request_id && operation == &operation_id => {
                    responded = true;
                    transfer = result
                        .as_ref()
                        .ok()
                        .and_then(|value| serde_json::from_value::<TransferRef>(value.clone()).ok())
                        .map(|reference| reference.transfer_id);
                    transfer.is_none()
                }
                HostFrame::TransferChunk {
                    transfer_id, eof, ..
                } if transfer.as_ref() == Some(transfer_id) => *eof,
                _ => return Err(
                    "gRPC Host Execute returned a frame outside its exact invocation or transfer"
                        .into(),
                ),
            };
        let Some(remote) = route.remote().await else {
            return Ok(());
        };
        remote
            .receive(route.generation, frame)
            .map_err(|error| format!("gRPC Host Execute reply validation failed: {error}"))?;
        if terminal {
            return Ok(());
        }
    }
}

pub(super) fn spawn_worker(
    link: Link,
    mut outgoing: mpsc::Receiver<HostFrame>,
    stop: CancellationToken,
    weak: std::sync::Weak<super::super::Manager>,
    owner: String,
    project: String,
    socket_id: String,
    generation: u64,
    diagnostics: Correlation,
) -> tokio::task::JoinHandle<()> {
    let parent = tracing::Span::current();
    tokio::spawn(async move {
        let route = RouteWitness {
            manager: weak.clone(),
            owner,
            project: project.clone(),
            socket: socket_id.clone(),
            generation,
        };
        let Link {
            client,
            origin,
            control,
            mut inbound,
            execute_token,
        } = link;
        let writer_route = route.clone();
        let writer = async move {
            // Dropping this future on disconnect aborts every RPC child. Completion
            // is reaped before new admission; Cancel aborts only its exact request.
            let mut tasks = JoinSet::<Result<(), String>>::new();
            let mut calls = HashMap::<Id, (String, String, AbortHandle)>::new();
            loop {
                tokio::select! {
                    biased;
                    completed = tasks.join_next_with_id(), if !tasks.is_empty() => {
                        if let Some(completed) = completed {
                            let (id, result) = match completed {
                                Ok((id, result)) => (id, result),
                                Err(error) => (error.id(), Err(format!("gRPC Host Execute task stopped: {error}"))),
                            };
                            if let Some((request, operation, _)) = calls.remove(&id) {
                                if let Err(message) = result {
                                    writer_route.fail(&request, &operation, message).await;
                                }
                            }
                        }
                    }
                    frame = outgoing.recv() => {
                        let Some(frame) = frame else { return super::carrier_stop("gRPC host outgoing route closed") };
                        if let HostFrame::Request { request_id, operation_id, .. } = &frame {
                            if tasks.len() >= MAX_EXECUTES {
                                writer_route.fail(request_id, operation_id,
                                    "gRPC Host Execute task capacity exhausted before dispatch".into()).await;
                                continue;
                            }
                            let proto = match frame_to_proto(&frame) {
                                Ok(proto) => proto,
                                Err(error) => {
                                    writer_route.fail(request_id, operation_id, format!("Cannot encode gRPC Host request: {}", error.message)).await;
                                    continue;
                                }
                            };
                            let mut call = tonic::Request::new(proto);
                            let origin_value = match MetadataValue::try_from(origin.as_str()) {
                                Ok(value) => value,
                                Err(_) => return super::carrier_stop("Server origin is not valid gRPC metadata"),
                            };
                            call.metadata_mut().insert("origin", origin_value);
                            call.metadata_mut().insert(GRPC_EXECUTE_TOKEN_HEADER, execute_token.clone());
                            let child = tasks.spawn(execute(client.clone(), call, writer_route.clone(), request_id.clone(), operation_id.clone()));
                            calls.insert(child.id(), (request_id.clone(), operation_id.clone(), child));
                        } else {
                            if let HostFrame::Cancel { request_id, .. } = &frame {
                                for (request, _, child) in calls.values() {
                                    if request == request_id { child.abort(); }
                                }
                            }
                            let proto = frame_to_proto(&frame).map_err(|_| "Cannot encode outgoing gRPC Host control frame")?;
                            control.send(proto).await.map_err(|_| "gRPC host control send failed")?;
                        }
                    }
                }
            }
        };
        let reader = async {
            loop {
                let proto = match inbound.message().await {
                    Ok(Some(proto)) => proto,
                    Ok(None) => return super::carrier_stop("gRPC host control stream ended"),
                    Err(status) => {
                        tracing::warn!(error_code = %status.code(), "gRPC host control stream failed");
                        return super::carrier_stop("gRPC host control stream failed");
                    }
                };
                let frame = frame_from_proto(proto)
                    .map_err(|_| "gRPC host control frame does not match the protocol schema")?;
                if !bindings::is_reply(&frame) {
                    return super::carrier_stop(
                        "gRPC host sent a controller request on the control stream",
                    );
                }
                if let Some(remote) = route.remote().await {
                    remote
                        .receive(generation, frame)
                        .map_err(|_| "gRPC host control reply validation failed")?;
                }
            }
        };
        let health = async {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                let Some(manager) = weak.upgrade() else {
                    return;
                };
                let routes = manager.routes.lock().await;
                match routes.get(&project) {
                    Some(route) if route.socket == socket_id && route.generation == generation => {
                        if !route.remote.is_connected() {
                            return;
                        }
                    }
                    Some(route) if route.generation > generation => return,
                    _ => continue,
                }
            }
        };
        let (stop_reason, reason) = tokio::select! {
            biased;
            _ = stop.cancelled() => (StopReason::StopCancelled, Some("Native Host connection was explicitly disconnected or replaced")),
            result = writer => (StopReason::WriterErr, result.err()),
            result = reader => {
                let reason = result.err();
                let stopped = if reason == Some("gRPC host control stream ended") {
                    StopReason::ReaderEof
                } else {
                    StopReason::ReaderErr
                };
                (stopped, reason)
            },
            _ = health => (StopReason::HealthUnhealthy, None),
        };
        diagnostics.stopped(stop_reason, reason.unwrap_or("Host route is no longer healthy"));
        if let Some(manager) = weak.upgrade() {
            if let Some(reason) = reason {
                manager
                    .detach_socket_with_cause(
                        &socket_id,
                        Some(&project),
                        microcrab_host_protocol::ErrorCode::Disconnected,
                        reason,
                    )
                    .await;
            } else {
                let current = manager
                    .routes
                    .lock()
                    .await
                    .get(&project)
                    .is_some_and(|route| {
                        route.socket == socket_id && route.generation == generation
                    });
                if current {
                    manager.suspend_project(&project).await;
                }
            }
        }
    }.instrument(parent).with_current_subscriber())
}
