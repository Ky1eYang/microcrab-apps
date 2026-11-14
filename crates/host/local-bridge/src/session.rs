//! One authenticated controller shared by the WebSocket relay and gRPC direct carrier.
//!
//! The WebSocket path has a single writer, so control frames stay ahead of chunks and
//! a multi-chunk transfer is paced. gRPC gives each admitted Execute its own stream;
//! HTTP/2 flow control replaces that pace. Admission, cancellation, receipt lookup and
//! lease ownership stay here. Neither carrier replays a request or refunds process capacity.
use crate::diagnostics;
use microcrab_host_protocol::{
    BridgeDiagnosticPhase, ErrorCode, HostRequest, MAX_PENDING_REQUESTS, WireError, WireFrame,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, Semaphore, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

#[derive(Clone)]
struct PendingOperation {
    cancellation: CancellationToken,
    operation_id: String,
    method: &'static str,
}

pub(crate) enum ControlDecision {
    Continue,
    Stop(&'static str),
}

enum ReplySink {
    /// Control replies and chunked transfers share the carrier writer.
    Shared,
    /// One gRPC Execute stream owns this call's response and chunks.
    Direct(mpsc::Sender<WireFrame>),
}

pub(crate) struct Live {
    bridge: Arc<crate::Bridge>,
    generation: String,
    pending: Arc<Mutex<HashMap<String, PendingOperation>>>,
    permits: Arc<Semaphore>,
    control_permits: Arc<Semaphore>,
    control: mpsc::Sender<WireFrame>,
    chunks: mpsc::Sender<WireFrame>,
    diagnostics: mpsc::Sender<WireFrame>,
    /// WebSocket-only. gRPC streams do not share a chunk writer.
    pace_chunks: bool,
}

impl Live {
    pub(crate) fn new(
        bridge: Arc<crate::Bridge>,
        generation: String,
        control: mpsc::Sender<WireFrame>,
        chunks: mpsc::Sender<WireFrame>,
        diagnostics: mpsc::Sender<WireFrame>,
        pace_chunks: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            bridge,
            generation,
            pending: Arc::new(Mutex::new(HashMap::new())),
            permits: Arc::new(Semaphore::new(MAX_PENDING_REQUESTS)),
            control_permits: Arc::new(Semaphore::new(8)),
            control,
            chunks,
            diagnostics,
            pace_chunks,
        })
    }

    pub(crate) fn watch_processes(self: &Arc<Self>) -> JoinHandle<()> {
        let live = Arc::clone(self);
        let mut process_changes = live.bridge.processes.changed.subscribe();
        process_changes.mark_changed();
        tokio::spawn(async move {
            while process_changes.changed().await.is_ok() {
                let Ok(scope) = live.bridge.check_lease(&live.generation).await else {
                    break;
                };
                for (handle, revision) in live.bridge.process_revisions(&scope).await {
                    if live
                        .control
                        .send(WireFrame::ProcessChanged {
                            lease_generation: live.generation.clone(),
                            handle,
                            revision,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        })
    }

    pub(crate) async fn lease_ok(&self) -> bool {
        self.bridge.check_lease(&self.generation).await.is_ok()
    }

    /// WebSocket carries both control and request frames on its one socket.
    pub(crate) async fn on_control(self: &Arc<Self>, frame: WireFrame) -> ControlDecision {
        self.dispatch(frame, true).await
    }

    /// gRPC operations belong to Execute, leaving Control available for ownership,
    /// cancellation and receipt queries even while an operation is blocked.
    #[cfg(feature = "grpc")]
    pub(crate) async fn on_grpc_control(self: &Arc<Self>, frame: WireFrame) -> ControlDecision {
        self.dispatch(frame, false).await
    }

    pub(crate) async fn on_execute(
        self: &Arc<Self>,
        frame: WireFrame,
        replies: mpsc::Sender<WireFrame>,
    ) -> ControlDecision {
        let WireFrame::Request { .. } = &frame else {
            return ControlDecision::Stop("execute_requires_request");
        };
        self.dispatch_request(frame, ReplySink::Direct(replies))
            .await
    }

    async fn dispatch(
        self: &Arc<Self>,
        frame: WireFrame,
        inline_requests: bool,
    ) -> ControlDecision {
        if !self.lease_ok().await {
            return ControlDecision::Stop("lease_expired_or_replaced");
        }
        let generation = self.generation.clone();
        match frame {
            WireFrame::Ping { lease_generation } if lease_generation == generation => {
                // The authenticated controller owns the exact scoped lease.
                let mut auth = self.bridge.authority.lock().await;
                if auth.generation.to_string() != generation {
                    return ControlDecision::Stop("control_route_stopped");
                }
                auth.lease_deadline = std::time::Instant::now() + Duration::from_secs(15);
                drop(auth);
                if self
                    .control
                    .try_send(WireFrame::Pong {
                        lease_generation: generation.clone(),
                    })
                    .is_err()
                {
                    return ControlDecision::Stop("control_route_stopped");
                }
                ControlDecision::Continue
            }
            WireFrame::Request { .. } if !inline_requests => {
                ControlDecision::Stop("request_on_control_stream")
            }
            WireFrame::Request {
                request_id,
                operation_id,
                lease_generation,
                remaining_budget_ms,
                method,
            } if lease_generation == generation => {
                self.admit(
                    request_id,
                    operation_id,
                    lease_generation,
                    remaining_budget_ms,
                    method,
                    ReplySink::Shared,
                )
                .await
            }
            WireFrame::Cancel {
                request_id,
                lease_generation,
            } if lease_generation == generation => {
                let witness = self.pending.lock().await.get(&request_id).cloned();
                if let Some(pending) = witness {
                    pending.cancellation.cancel();
                    let mut event = diagnostics::event(
                        &self.bridge,
                        &generation,
                        BridgeDiagnosticPhase::CancelReceived,
                    );
                    event.request_id = Some(request_id);
                    event.operation_id = Some(pending.operation_id.clone());
                    event.method = Some(pending.method.into());
                    diagnostics::emit(&self.diagnostics, event);
                }
                ControlDecision::Continue
            }
            WireFrame::Status {
                request_id,
                operation_id,
                lease_generation,
            } if lease_generation == generation => {
                let scope = match self.bridge.check_lease(&generation).await {
                    Ok(scope) => scope,
                    Err(_) => return ControlDecision::Stop("lease_expired_or_replaced"),
                };
                let mut records = self.bridge.receipts.lock().await;
                let receipt = if records
                    .fence(&operation_id)
                    .is_some_and(|fence| fence.scope == scope)
                {
                    match records.get(&operation_id) {
                        Ok(receipt) => receipt,
                        Err(_) => return ControlDecision::Stop("control_route_stopped"),
                    }
                } else {
                    None
                };
                if self
                    .control
                    .try_send(WireFrame::Receipt {
                        request_id,
                        receipt,
                    })
                    .is_err()
                {
                    return ControlDecision::Stop("control_route_stopped");
                }
                ControlDecision::Continue
            }
            WireFrame::Detach { lease_generation } if lease_generation == generation => {
                let mut auth = self.bridge.authority.lock().await;
                if auth.generation.to_string() == generation {
                    if auth.require_token {
                        auth.credential = None;
                    }
                    auth.lease_deadline = std::time::Instant::now();
                }
                ControlDecision::Stop("detach")
            }
            _ => ControlDecision::Stop("control_route_stopped"),
        }
    }

    async fn dispatch_request(
        self: &Arc<Self>,
        frame: WireFrame,
        sink: ReplySink,
    ) -> ControlDecision {
        if !self.lease_ok().await {
            return ControlDecision::Stop("lease_expired_or_replaced");
        }
        let WireFrame::Request {
            request_id,
            operation_id,
            lease_generation,
            remaining_budget_ms,
            method,
        } = frame
        else {
            return ControlDecision::Stop("execute_requires_request");
        };
        if lease_generation != self.generation {
            return ControlDecision::Stop("control_route_stopped");
        }
        self.admit(
            request_id,
            operation_id,
            lease_generation,
            remaining_budget_ms,
            method,
            sink,
        )
        .await
    }

    async fn admit(
        self: &Arc<Self>,
        request_id: String,
        operation_id: String,
        lease_generation: String,
        remaining_budget_ms: u32,
        method: HostRequest,
        sink: ReplySink,
    ) -> ControlDecision {
        if request_id.is_empty() || request_id.len() > 256 {
            return ControlDecision::Stop("invalid_request");
        }
        let admission = if method.is_control() {
            self.control_permits.clone()
        } else {
            self.permits.clone()
        };
        let Ok(permit) = admission.try_acquire_owned() else {
            let limited = WireFrame::Response {
                request_id,
                operation_id,
                result: Err(WireError::new(
                    ErrorCode::Limit,
                    "too many pending Host requests",
                )),
            };
            let queued = match &sink {
                ReplySink::Shared => self.control.try_send(limited).is_ok(),
                ReplySink::Direct(replies) => replies.try_send(limited).is_ok(),
            };
            return if queued {
                ControlDecision::Continue
            } else {
                ControlDecision::Stop("control_route_stopped")
            };
        };
        let cancel = CancellationToken::new();
        {
            let mut map = self.pending.lock().await;
            if map.contains_key(&request_id) {
                return ControlDecision::Stop("duplicate_request");
            }
            map.insert(
                request_id.clone(),
                PendingOperation {
                    cancellation: cancel.clone(),
                    operation_id: operation_id.clone(),
                    method: method.method_name(),
                },
            );
        }
        let live = Arc::clone(self);
        let method_name = method.method_name();
        let mutation = method.is_mutation();
        tracing::info!(
            request_id = %diagnostics::safe_id(&request_id),
            operation_id = %diagnostics::safe_id(&operation_id),
            lease_generation = %lease_generation,
            bridge_instance_id = %self.bridge.descriptor.bridge_instance_id,
            method = method_name,
            mutation,
            "Host operation admitted"
        );
        tokio::spawn(async move {
            let _permit = permit;
            let started = std::time::Instant::now();
            let budget = remaining_budget_ms
                .saturating_sub(started.elapsed().as_millis().min(u128::from(u32::MAX)) as u32);
            let span = tracing::info_span!(
                "bridge_operation",
                request_id = %diagnostics::safe_id(&request_id),
                operation_id = %diagnostics::safe_id(&operation_id),
                lease_generation = %lease_generation,
                bridge_instance_id = %live.bridge.descriptor.bridge_instance_id,
                method = method_name
            );
            let (result, chunks) = live
                .bridge
                .execute(
                    operation_id.clone(),
                    lease_generation.clone(),
                    budget,
                    method,
                    cancel.clone(),
                )
                .instrument(span)
                .await;
            let elapsed_ms = started.elapsed().as_millis() as u64;
            match &result {
                Ok(_) => tracing::info!(
                    request_id = %diagnostics::safe_id(&request_id),
                    operation_id = %diagnostics::safe_id(&operation_id),
                    lease_generation = %lease_generation,
                    method = method_name,
                    elapsed_ms,
                    outgoing_chunks = chunks.len(),
                    "Host operation completed"
                ),
                Err(error) => {
                    let mut event = diagnostics::event(
                        &live.bridge,
                        &lease_generation,
                        BridgeDiagnosticPhase::OperationFailed,
                    );
                    event.request_id = Some(request_id.clone());
                    event.operation_id = Some(operation_id.clone());
                    event.method = Some(method_name.into());
                    event.error_code = Some(error.code.clone());
                    event.elapsed_ms = Some(elapsed_ms);
                    diagnostics::emit(&live.diagnostics, event);
                    tracing::warn!(
                        request_id = %diagnostics::safe_id(&request_id),
                        operation_id = %diagnostics::safe_id(&operation_id),
                        lease_generation = %lease_generation,
                        method = method_name,
                        elapsed_ms,
                        error_code = ?error.code,
                        caller_cancelled = cancel.is_cancelled(),
                        "Host operation failed"
                    );
                }
            }
            // A direct Execute stream orders its own response and chunks. The shared
            // WebSocket writer still serializes one transfer through EOF so a later
            // declaration cannot overtake it. Effects have already settled here.
            let paced = live.pace_chunks && matches!(sink, ReplySink::Shared);
            if paced && !chunks.is_empty() {
                let remaining = remaining_budget_ms
                    .saturating_sub(started.elapsed().as_millis().min(u128::from(u32::MAX)) as u32);
                let egress = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => None,
                    result = tokio::time::timeout(
                        Duration::from_millis(u64::from(remaining.min(30000))),
                        live.bridge.egress.lock(),
                    ) => result.ok(),
                };
                let Some(_guard) = egress else {
                    let code = if mutation {
                        ErrorCode::OutcomeUnknown
                    } else if cancel.is_cancelled() {
                        ErrorCode::Cancelled
                    } else {
                        ErrorCode::Timeout
                    };
                    let _sent = live
                        .send_control(
                            &sink,
                            WireFrame::Response {
                                request_id: request_id.clone(),
                                operation_id,
                                result: Err(WireError::new(code, "Host result admission stopped")),
                            },
                        )
                        .await;
                    live.pending.lock().await.remove(&request_id);
                    return;
                };
                let response = WireFrame::Response {
                    request_id: request_id.clone(),
                    operation_id,
                    result,
                };
                if live.send_chunks(&sink, response).await {
                    let multiple = chunks.len() > 1;
                    for chunk in chunks {
                        let sent = tokio::select! {
                            biased;
                            _ = cancel.cancelled() => false,
                            result = live.send_chunks(&sink, chunk) => result,
                        };
                        if !sent {
                            break;
                        }
                        if multiple {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    }
                }
            } else {
                let response = WireFrame::Response {
                    request_id: request_id.clone(),
                    operation_id,
                    result,
                };
                let sent = if chunks.is_empty() {
                    live.send_control(&sink, response).await
                } else {
                    live.send_chunks(&sink, response).await
                };
                if sent {
                    for chunk in chunks {
                        let sent = tokio::select! {
                            biased;
                            _ = cancel.cancelled() => false,
                            result = live.send_chunks(&sink, chunk) => result,
                        };
                        if !sent {
                            break;
                        }
                    }
                }
            }
            live.pending.lock().await.remove(&request_id);
        });
        ControlDecision::Continue
    }

    async fn send_control(&self, sink: &ReplySink, frame: WireFrame) -> bool {
        match sink {
            ReplySink::Shared => self.control.send(frame).await.is_ok(),
            ReplySink::Direct(replies) => replies.send(frame).await.is_ok(),
        }
    }

    async fn send_chunks(&self, sink: &ReplySink, frame: WireFrame) -> bool {
        match sink {
            ReplySink::Shared => self.chunks.send(frame).await.is_ok(),
            ReplySink::Direct(replies) => replies.send(frame).await.is_ok(),
        }
    }

    pub(crate) async fn shutdown(&self, reason: &str) {
        let cancellations: Vec<_> = self
            .pending
            .lock()
            .await
            .values()
            .map(|entry| entry.cancellation.clone())
            .collect();
        let pending_operations = cancellations.len();
        for cancellation in cancellations {
            cancellation.cancel();
        }
        diagnostics::emit(
            &self.diagnostics,
            diagnostics::event(
                &self.bridge,
                &self.generation,
                BridgeDiagnosticPhase::ConnectionClosed,
            ),
        );
        tracing::info!(
            lease_generation = %self.generation,
            pending_operations,
            reason,
            "Host controller stopped; pending operations cancelled without replay"
        );
        let mut auth = self.bridge.authority.lock().await;
        if auth.generation.to_string() == self.generation {
            auth.lease_deadline = std::time::Instant::now();
        }
    }
}
