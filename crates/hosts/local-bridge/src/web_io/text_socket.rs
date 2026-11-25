//! An exact Bridge slot owns one connection task; no CDP commands or reconnects.
use super::{
    Endpoint, Error, Kind, MAX_RECEIVE_BYTES, MAX_SEND_BYTES, Operation, Result, SocketIo, limit,
    ws_error,
};
use futures_util::{SinkExt, StreamExt};
use hyper::body::Bytes;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, sleep_until},
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        self, Message, Utf8Bytes, client::IntoClientRequest, error::CapacityError,
        protocol::WebSocketConfig,
    },
};
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;

#[cfg(debug_assertions)]
mod pressure_snapshot;

const IDLE_PROBE_AFTER: Duration = Duration::from_secs(15);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);
const PRESSURE_WARN_INTERVAL: Duration = Duration::from_secs(5);
// This count includes the one reserved ingress slot even for empty messages.
const MAX_BUFFERED_FRAMES: usize = 32;
const MAX_INGRESS_BYTES: usize = MAX_RECEIVE_BYTES;

#[derive(Clone, Copy, PartialEq, Eq)]
enum PressureReason {
    MessageCount,
    Bytes,
}

#[derive(Clone, Copy)]
struct Pressure {
    reason: PressureReason,
    buffered_messages: usize,
    buffered_bytes: usize,
    staged_bytes: Option<usize>,
    #[cfg(debug_assertions)]
    snapshot_ordinal: Option<u64>,
}

impl Pressure {
    fn emit(self, event: &'static str, operation_id: Option<uuid::Uuid>) {
        macro_rules! warn_pressure {
            ($($extra:tt)*) => {
                tracing::warn!(
                    event,
                    reason = match self.reason {
                        PressureReason::MessageCount => "inbound_queue_count_pressure",
                        PressureReason::Bytes => "inbound_queue_byte_pressure",
                    },
                    bridge_operation_id = operation_id.map(tracing::field::display),
                    buffered_messages = self.buffered_messages,
                    buffered_bytes = self.buffered_bytes,
                    staged_messages = usize::from(self.staged_bytes.is_some()),
                    staged_bytes = self.staged_bytes.unwrap_or(0),
                    message_limit = MAX_BUFFERED_FRAMES,
                    byte_limit = MAX_RECEIVE_BYTES,
                    ingress_byte_limit = MAX_INGRESS_BYTES,
                    $($extra)*
                    "Bridge WebSocket paused inbound reads for capacity"
                );
            };
        }
        #[cfg(debug_assertions)]
        warn_pressure!(snapshot_ordinal = self.snapshot_ordinal,);
        #[cfg(not(debug_assertions))]
        warn_pressure!();
    }
}

struct Saturation {
    pressure: Pressure,
    observed_by_pull: bool,
}

fn allow_warn(last: &mut Option<Instant>, now: Instant) -> bool {
    if last.is_some_and(|at| now.duration_since(at) < PRESSURE_WARN_INTERVAL) {
        return false;
    }
    *last = Some(now);
    true
}

#[derive(Clone, Copy)]
enum Reason {
    InboundMessageTooLarge,
    LeaseCancelled,
    IdleProbeTimeout,
    ControlWriteFailed,
    HandleReleased,
    SendOutcomeUnknown,
    PeerClose,
    ReadFailed,
    NonTextMessage,
}

impl Reason {
    fn code(self) -> &'static str {
        match self {
            Self::InboundMessageTooLarge => "inbound_message_too_large",
            Self::LeaseCancelled => "lease_cancelled",
            Self::IdleProbeTimeout => "idle_probe_timeout",
            Self::ControlWriteFailed => "control_write_failed",
            Self::HandleReleased => "handle_released",
            Self::SendOutcomeUnknown => "send_outcome_unknown",
            Self::PeerClose => "peer_close",
            Self::ReadFailed => "read_failed",
            Self::NonTextMessage => "non_text_message",
        }
    }
}

#[derive(Clone, Copy)]
struct Terminal {
    reason: Reason,
    error: Error,
}

impl Terminal {
    fn observe(self, operation_id: Option<uuid::Uuid>) {
        self.emit("bridge_websocket_terminal_observed", operation_id);
    }

    fn emit(self, event: &'static str, operation_id: Option<uuid::Uuid>) {
        tracing::warn!(
            event,
            reason = self.reason.code(),
            error_category = ?self.error,
            bridge_operation_id = operation_id.map(tracing::field::display),
            "Bridge observed WebSocket termination"
        );
    }
}

struct End {
    read_error: Error,
    evidence: Terminal,
    observed_by_operation: bool,
}

#[derive(Default)]
struct Inbox {
    frames: VecDeque<Utf8Bytes>,
    bytes: usize,
    // Before polling, the actor reserves a count slot and MAX_INGRESS_BYTES.
    // Tungstenite only hands us complete messages. Hold at most one that fits
    // the ingress budget but not the remaining FIFO bytes, then stop polling.
    // Total logical text: <=32 messages and <=256 KiB FIFO + 256 KiB ingress.
    staged: Option<Utf8Bytes>,
    saturation: Option<Saturation>,
    last_actor_warn: Option<Instant>,
    last_pull_warn: Option<Instant>,
    terminal: Option<End>,
}

impl Inbox {
    fn pressure(&self) -> Option<Pressure> {
        let reason = if self.frames.len() == MAX_BUFFERED_FRAMES {
            PressureReason::MessageCount
        } else if self.staged.is_some() || self.bytes == MAX_RECEIVE_BYTES {
            PressureReason::Bytes
        } else {
            return None;
        };
        Some(Pressure {
            reason,
            buffered_messages: self.frames.len(),
            buffered_bytes: self.bytes,
            staged_bytes: self.staged.as_ref().map(|text| text.len()),
            #[cfg(debug_assertions)]
            snapshot_ordinal: self
                .saturation
                .as_ref()
                .filter(|saturation| saturation.pressure.reason == reason)
                .and_then(|saturation| saturation.pressure.snapshot_ordinal),
        })
    }

    fn refresh_pressure(&mut self) -> Option<Pressure> {
        let Some(pressure) = self.pressure() else {
            self.saturation = None;
            return None;
        };
        if let Some(current) = &mut self.saturation
            && current.pressure.reason == pressure.reason
        {
            current.pressure = pressure;
            return None;
        }
        self.saturation = Some(Saturation {
            pressure,
            observed_by_pull: false,
        });
        allow_warn(&mut self.last_actor_warn, Instant::now()).then_some(pressure)
    }

    fn observe_pressure(&mut self) -> Option<Pressure> {
        let saturation = self.saturation.as_mut()?;
        if saturation.observed_by_pull || !allow_warn(&mut self.last_pull_warn, Instant::now()) {
            return None;
        }
        saturation.observed_by_pull = true;
        Some(saturation.pressure)
    }

    fn terminal(&mut self, operation_id: Option<uuid::Uuid>) -> Option<(Error, Option<Terminal>)> {
        let end = self.terminal.as_mut()?;
        let evidence = if operation_id.is_some() && !end.observed_by_operation {
            end.observed_by_operation = true;
            Some(end.evidence)
        } else {
            None
        };
        Some((end.read_error, evidence))
    }
}

#[derive(Default)]
struct Incoming {
    inbox: Mutex<Inbox>,
    ready: Notify,
    space: Notify,
}

impl Incoming {
    fn lock(&self) -> MutexGuard<'_, Inbox> {
        self.inbox.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn push(&self, text: Utf8Bytes) {
        let mut inbox = self.lock();
        // Only this actor pushes. A pull racing the read can only free space.
        debug_assert!(inbox.staged.is_none() && inbox.frames.len() < MAX_BUFFERED_FRAMES);
        debug_assert!(text.len() <= MAX_INGRESS_BYTES);
        if inbox.bytes + text.len() <= MAX_RECEIVE_BYTES {
            inbox.bytes += text.len();
            inbox.frames.push_back(text);
        } else {
            inbox.staged = Some(text);
        }
        let warning = inbox.refresh_pressure();
        #[cfg(debug_assertions)]
        let (warning, snapshot) = match warning {
            Some(mut pressure) => {
                let snapshot = pressure_snapshot::Snapshot::capture(&inbox);
                if let Some(snapshot) = &snapshot {
                    pressure.snapshot_ordinal = Some(snapshot.ordinal());
                    if let Some(saturation) = &mut inbox.saturation {
                        saturation.pressure.snapshot_ordinal = pressure.snapshot_ordinal;
                    }
                }
                (Some(pressure), snapshot)
            }
            None => (None, None),
        };
        drop(inbox);
        self.ready.notify_one();
        if let Some(warning) = warning {
            warning.emit("bridge_websocket_inbound_backpressure", None);
        }
        #[cfg(debug_assertions)]
        if let Some(snapshot) = snapshot {
            snapshot.emit();
        }
    }

    fn finish(&self, terminal: Terminal, read_error: Error, discard: bool, has_attribution: bool) {
        let mut inbox = self.lock();
        if discard {
            inbox.frames.clear();
            inbox.bytes = 0;
            inbox.staged = None;
            inbox.saturation = None;
        }
        inbox.terminal = Some(End {
            read_error,
            evidence: terminal,
            observed_by_operation: has_attribution,
        });
        self.ready.notify_one();
    }

    fn pop(&self, inbox: &mut Inbox) -> Option<(Utf8Bytes, Option<Pressure>)> {
        let text = inbox.frames.pop_front()?;
        let was_paused = inbox.saturation.is_some();
        let pressure = inbox.observe_pressure();
        inbox.bytes -= text.len();
        if inbox
            .staged
            .as_ref()
            .is_some_and(|staged| inbox.bytes + staged.len() <= MAX_RECEIVE_BYTES)
            && let Some(staged) = inbox.staged.take()
        {
            inbox.bytes += staged.len();
            inbox.frames.push_back(staged);
        }
        // Promotion may leave byte pressure in place but cannot add count pressure.
        let _ = inbox.refresh_pressure();
        if was_paused && inbox.saturation.is_none() {
            self.space.notify_one();
        }
        Some((text, pressure))
    }

    async fn next(
        &self,
        operation_id: Option<uuid::Uuid>,
        observed_pressure: &mut Option<Pressure>,
    ) -> std::result::Result<Utf8Bytes, (Error, Option<Terminal>)> {
        loop {
            let ready = self.ready.notified();
            let terminal = {
                let mut inbox = self.lock();
                if let Some((text, pressure)) = self.pop(&mut inbox) {
                    *observed_pressure = pressure;
                    return Ok(text);
                }
                inbox.terminal(operation_id)
            };
            if let Some(terminal) = terminal {
                return Err(terminal);
            }
            ready.await;
        }
    }
}

struct Send {
    text: String,
    deadline: Instant,
    caller: CancellationToken,
    bridge_operation_id: Option<uuid::Uuid>,
    // The cancellation and start paths race for this latch. A caller that wins
    // can report its original error: the actor cannot attempt the write later.
    started_or_abandoned: Arc<AtomicBool>,
    reply: oneshot::Sender<Result<()>>,
}

struct Live {
    incoming: Arc<Incoming>,
    send: mpsc::Sender<Send>,
    lease: CancellationToken,
    task: JoinHandle<()>,
}

impl Drop for Live {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Non-cloneable slot handle. Status inspects local state only; it never reads
/// the network or sends a probe. Queued text precedes an orderly terminal event.
pub(crate) struct TextSocket(Option<Live>);

impl TextSocket {
    pub(crate) async fn open(raw: &str, op: &Operation<'_>) -> Result<Self> {
        Self::open_with_idle(raw, op, IDLE_PROBE_AFTER, CONTROL_TIMEOUT).await
    }

    pub(super) async fn open_with_idle(
        raw: &str,
        op: &Operation<'_>,
        idle_after: Duration,
        control_timeout: Duration,
    ) -> Result<Self> {
        op.check()?;
        let endpoint = Endpoint::parse(raw, Kind::WebSocket)?;
        let request = endpoint
            .uri
            .clone()
            .into_client_request()
            .map_err(|_| Error::InvalidEndpoint)?;
        let socket = op
            .run(async {
                let stream = endpoint.dial().await?;
                op.check()?;
                let config = WebSocketConfig::default()
                    .read_buffer_size(8 * 1024)
                    .write_buffer_size(0)
                    .max_write_buffer_size(MAX_SEND_BYTES + 1024)
                    .max_message_size(Some(MAX_RECEIVE_BYTES))
                    .max_frame_size(Some(MAX_RECEIVE_BYTES));
                let (socket, response) =
                    tokio_tungstenite::client_async_with_config(request, stream, Some(config))
                        .await
                        .map_err(ws_error)?;
                // No extension was offered. Tungstenite does not yet enforce this.
                if response.headers().contains_key("sec-websocket-extensions") {
                    return Err(Error::InvalidResponse);
                }
                Ok(socket)
            })
            .await?;
        op.check()?;
        let incoming = Arc::new(Incoming::default());
        let lease = op.lease.clone();
        let (send, requests) = mpsc::channel(1);
        let task = tokio::spawn(
            drive(
                socket,
                incoming.clone(),
                requests,
                lease.clone(),
                idle_after,
                control_timeout,
            )
            .with_current_subscriber(),
        );
        Ok(Self(Some(Live {
            incoming,
            send,
            lease,
            task,
        })))
    }

    pub(crate) fn status(&self) -> Result<()> {
        self.status_with_id(None)
    }

    pub(crate) fn status_for(&self, op: &Operation<'_>) -> Result<()> {
        self.status_with_id(op.bridge_operation_id)
    }

    fn status_with_id(&self, operation_id: Option<uuid::Uuid>) -> Result<()> {
        let live = self.0.as_ref().ok_or(Error::Closed)?;
        if live.lease.is_cancelled() {
            return Err(Error::Closed);
        }
        let terminal = {
            let mut inbox = live.incoming.lock();
            if !inbox.frames.is_empty() {
                return Ok(());
            }
            inbox.terminal(operation_id)
        };
        if let Some((_, evidence)) = terminal {
            if let Some(evidence) = evidence {
                evidence.observe(operation_id);
            }
            return Err(Error::Closed);
        }
        Ok(())
    }

    pub(crate) fn close(&mut self) {
        // Local retirement never waits on a peer handshake.
        self.0.take();
    }

    pub(crate) async fn send(&mut self, text: String, op: &Operation<'_>) -> Result<()> {
        op.check()?;
        limit(text.len(), MAX_SEND_BYTES)?;
        let live = self.0.as_ref().ok_or(Error::Closed)?;
        let terminal = live.incoming.lock().terminal(op.bridge_operation_id);
        if let Some((_, evidence)) = terminal {
            if let Some(evidence) = evidence {
                evidence.observe(op.bridge_operation_id);
            }
            return Err(Error::Closed);
        }
        let (reply, done) = oneshot::channel();
        let started_or_abandoned = Arc::new(AtomicBool::new(false));
        let request = Send {
            text,
            deadline: op.deadline,
            caller: op.cancel.clone(),
            bridge_operation_id: op.bridge_operation_id,
            started_or_abandoned: started_or_abandoned.clone(),
            reply,
        };
        // The exclusive handle issues only one request at a time. A dropped
        // future retires the handle, so it cannot leave a queued predecessor.
        live.send.try_send(request).map_err(|_| Error::Closed)?;
        let mut pending = Pending {
            socket: self,
            armed: true,
        };
        let result = op
            .run(async { done.await.map_err(|_| Error::Closed)? })
            .await;
        let result = match result {
            Err(_) if started_or_abandoned.swap(true, Ordering::AcqRel) => {
                Err(Error::OutcomeUnknown)
            }
            other => other,
        };
        // Once handed off, interruption retires the exact socket, even if
        // the latch proves that no application bytes were written.
        pending.armed = result.is_err();
        result
    }

    /// Scoped pull owns the slot gate from peek through commit. The actor only
    /// appends, so preparing a bounded reply cannot invalidate a peeked head.
    pub(crate) async fn peek_pull(
        &self,
        max_bytes: usize,
        soft_deadline: Option<Instant>,
        op: &Operation<'_>,
    ) -> Result<Option<Utf8Bytes>> {
        op.check()?;
        limit(max_bytes, MAX_RECEIVE_BYTES)?;
        let incoming = &self.0.as_ref().ok_or(Error::Closed)?.incoming;
        let mut observed = None;
        let result = op
            .run(async {
                loop {
                    let ready = incoming.ready.notified();
                    {
                        let mut inbox = incoming.lock();
                        op.check()?;
                        if let Some(text) = inbox.frames.front() {
                            limit(text.len(), max_bytes)?;
                            return Ok(Some(text.clone()));
                        }
                        if let Some((error, evidence)) = inbox.terminal(op.bridge_operation_id) {
                            observed = evidence;
                            return Err(error);
                        }
                        if soft_deadline.is_none_or(|deadline| Instant::now() >= deadline) {
                            return Ok(None);
                        }
                    }
                    if let Some(deadline) = soft_deadline {
                        tokio::select! {
                            _ = ready => {},
                            _ = sleep_until(deadline) => {},
                        }
                    }
                    // Even on the soft-timer wake, take a final synchronized
                    // snapshot; Empty attests to this snapshot, not a timer.
                }
            })
            .await;
        if let Some(evidence) = observed {
            evidence.observe(op.bridge_operation_id);
        }
        result
    }

    pub(crate) fn commit_pull(&self, op: &Operation<'_>, committed: &mut bool) -> Result<()> {
        let incoming = &self.0.as_ref().ok_or(Error::Closed)?.incoming;
        let pressure = {
            let mut inbox = incoming.lock();
            op.check()?;
            let (_, pressure) = incoming.pop(&mut inbox).ok_or(Error::OutcomeUnknown)?;
            *committed = true;
            pressure
        };
        if let Some(pressure) = pressure {
            pressure.emit(
                "bridge_websocket_inbound_backpressure_observed",
                op.bridge_operation_id,
            );
        }
        Ok(())
    }

    pub(crate) async fn receive(&mut self, max_bytes: usize, op: &Operation<'_>) -> Result<String> {
        op.check()?;
        limit(max_bytes, MAX_RECEIVE_BYTES)?;
        self.0.as_ref().ok_or(Error::Closed)?;
        let mut pending = Pending {
            socket: self,
            armed: true,
        };
        let live = pending.socket.0.as_ref().ok_or(Error::Closed)?;
        let mut observed = None;
        let mut observed_pressure = None;
        let result = op
            .run(async {
                let text = live
                    .incoming
                    .next(op.bridge_operation_id, &mut observed_pressure)
                    .await
                    .map_err(|(error, evidence)| {
                        observed = evidence;
                        error
                    })?;
                op.check().map_err(|_| Error::OutcomeUnknown)?;
                limit(text.len(), max_bytes)?;
                Ok(text.as_str().to_owned())
            })
            .await;
        if let Some(pressure) = observed_pressure {
            pressure.emit(
                "bridge_websocket_inbound_backpressure_observed",
                op.bridge_operation_id,
            );
        }
        if let Some(evidence) = observed {
            evidence.observe(op.bridge_operation_id);
        }
        pending.armed = result.is_err();
        result
    }
}

struct Pending<'a> {
    socket: &'a mut TextSocket,
    armed: bool,
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.socket.close();
        }
    }
}

async fn drive(
    mut socket: WebSocketStream<Box<dyn SocketIo>>,
    incoming: Arc<Incoming>,
    mut requests: mpsc::Receiver<Send>,
    lease: CancellationToken,
    idle_after: Duration,
    control_timeout: Duration,
) {
    let mut next_probe = Instant::now() + idle_after;
    let mut pending_probe: Option<([u8; 16], Instant)> = None;
    let mut was_paused = false;
    let (reason, error, discard) = loop {
        let paused = incoming.lock().saturation.is_some();
        // Don't start a probe whose reply we deliberately won't read. A probe
        // already sent keeps its original hard deadline, even while paused.
        if was_paused && !paused && pending_probe.is_none() {
            next_probe = Instant::now() + idle_after;
        }
        was_paused = paused;
        let control_deadline = pending_probe.as_ref().map(|(_, deadline)| *deadline);
        tokio::select! {
            biased;
            _ = lease.cancelled() => break (Reason::LeaseCancelled, Error::Cancelled, true),
            _ = sleep_until(control_deadline.unwrap_or(next_probe)), if control_deadline.is_some() || !paused => {
                if pending_probe.is_some() {
                    break (Reason::IdleProbeTimeout, Error::Closed, false);
                }
                let nonce = *uuid::Uuid::new_v4().as_bytes();
                let deadline = Instant::now() + control_timeout;
                if let Err(error) = control_write(&lease, deadline, socket.send(Message::Ping(Bytes::copy_from_slice(&nonce)))).await {
                    break (Reason::ControlWriteFailed, error, error == Error::Cancelled);
                }
                pending_probe = Some((nonce, deadline));
            }
            request = requests.recv() => {
                let Some(Send { text, deadline, caller, bridge_operation_id, started_or_abandoned, reply }) = request else {
                    break (Reason::HandleReleased, Error::Closed, true);
                };
                let op = Operation {
                    deadline,
                    lease: &lease,
                    cancel: &caller,
                    bridge_operation_id,
                };
                let mut attempted = false;
                let result = op.run(async {
                    if control_deadline.is_some_and(|probe| Instant::now() >= probe) {
                        return Err(Error::Closed);
                    }
                    if started_or_abandoned.swap(true, Ordering::AcqRel) {
                        return Err(Error::Cancelled);
                    }
                    attempted = true;
                    let write = socket.send(Message::Text(text.into()));
                    if let Some(probe) = control_deadline {
                        control_write(&lease, probe, write).await?;
                    } else {
                        write.await.map_err(ws_error)?;
                    }
                    op.check()
                }).await;
                if let Err(error) = result && attempted {
                    drop(socket);
                    let terminal = Terminal { reason: Reason::SendOutcomeUnknown, error };
                    incoming.finish(terminal, Error::Closed, true, bridge_operation_id.is_some());
                    let _ = reply.send(Err(Error::OutcomeUnknown));
                    terminal.emit("bridge_websocket_terminated", bridge_operation_id);
                    return;
                }
                if result.is_ok() && pending_probe.is_none() {
                    next_probe = Instant::now() + idle_after;
                }
                let _ = reply.send(result);
            }
            // A single actor waits here. notify_one retains a permit if a
            // pull frees space after we snapshot `paused` but before polling.
            _ = incoming.space.notified(), if paused => {},
            // Keep raw tungstenite logging suppressed while polling peer I/O;
            // emit our typed evidence outside the suppressed future.
            message = socket.next().with_subscriber(tracing::Dispatch::none()), if !paused => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        incoming.push(text);
                        if pending_probe.is_none() {
                            next_probe = Instant::now() + idle_after;
                        }
                    }
                    Some(Ok(Message::Ping(_))) => {
                        // Tungstenite already queued the matching Pong.
                        let deadline = control_deadline.unwrap_or_else(|| Instant::now() + control_timeout);
                        if let Err(error) = control_write(&lease, deadline, socket.flush()).await {
                            break (Reason::ControlWriteFailed, error, error == Error::Cancelled);
                        }
                    }
                    Some(Ok(Message::Pong(payload))) => {
                        if pending_probe.as_ref().is_some_and(|(nonce, _)| payload.as_ref() == nonce) {
                            pending_probe = None;
                            next_probe = Instant::now() + idle_after;
                        }
                    }
                    None | Some(Ok(Message::Close(_))) => break (Reason::PeerClose, Error::Closed, false),
                    Some(Err(tungstenite::Error::Capacity(CapacityError::MessageTooLong { .. }))) => {
                        break (Reason::InboundMessageTooLarge, Error::Limit, false);
                    }
                    Some(Err(error)) => break (Reason::ReadFailed, ws_error(error), false),
                    Some(Ok(_)) => break (Reason::NonTextMessage, Error::InvalidResponse, false),
                }
            }
        }
        // A peer flooding buffered frames must leave room for a waiter/drainer
        // on the same executor, even when the stream itself is always ready.
        tokio::task::consume_budget().await;
    };
    drop(socket);
    let terminal = Terminal { reason, error };
    // The peer task has no admitted operation here. A subsequent operation can
    // separately witness the same terminal state; it did not cause it.
    incoming.finish(terminal, error, discard, false);
    terminal.emit("bridge_websocket_terminated", None);
}

async fn control_write(
    lease: &CancellationToken,
    deadline: Instant,
    write: impl Future<Output = std::result::Result<(), tokio_tungstenite::tungstenite::Error>>,
) -> Result<()> {
    tokio::select! {
        biased;
        _ = lease.cancelled() => Err(Error::Cancelled),
        _ = sleep_until(deadline) => Err(Error::Closed),
        result = write.with_subscriber(tracing::Dispatch::none()) => result.map_err(ws_error),
    }
}
