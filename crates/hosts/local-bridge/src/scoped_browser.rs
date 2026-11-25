//! Ephemeral web transport ownership. One map belongs to one authenticated
//! controller/lease, shared by every request on that route; no receipt path.
#[cfg(test)]
#[path = "scoped_browser/tests.rs"]
mod tests;
use crate::{transfers, web_io};
use microcrab_host_api::web_transport::{
    ConnectionOwner, ConnectionScope, HttpResponse, WebTransportError as Error,
    WebTransportResult as Result, WsPull, WsReceive,
};
use microcrab_host_protocol::{
    BindingScope, MAX_FRAME_BYTES, WebTransportRequest, WireError, WireFrame,
};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::{sync::OwnedMutexGuard, time::Instant};
use tokio_util::sync::CancellationToken;

// Never evict a fence and make an old identity executable again: once the
// bounded lease-local history fills, refuse further admissions, except cleanup.
const MAX_OWNERS: usize = 128;
const MAX_IDENTITIES: usize = 4096;
const MAX_OPERATIONS: usize = 65536;
// Reserve headroom for serialization and carrier egress, not a new hard deadline.
const PULL_EGRESS_RESERVE: Duration = Duration::from_millis(250);

pub(super) struct BrowserTiming {
    pub(super) admitted_at: Instant,
    pub(super) deadline: Instant,
}

pub(crate) struct ScopedBrowser {
    binding: BindingScope,
    lease: CancellationToken,
    ws_pull: bool,
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    owners: HashMap<ConnectionOwner, Owner>,
    identities: HashSet<ConnectionScope>,
    operations: HashSet<String>,
    closed: HashSet<ConnectionScope>,
    // If an out-of-order Close/Release cannot leave a fence at capacity,
    // this lease must never grant another reservation.
    sealed: bool,
}

#[derive(Default)]
struct Owner {
    retired: bool,
    discovery: Option<Discovery>,
    slot: Option<Arc<Slot>>,
}

struct Discovery {
    scope: ConnectionScope,
    stop: CancellationToken,
    url: String,
    ready: bool,
}

struct PullRequest {
    scope: ConnectionScope,
    max_bytes: u32,
    soft_wait_ms: u32,
    max_reply: Option<usize>,
    bridge_operation_id: Option<uuid::Uuid>,
}

struct Slot {
    scope: ConnectionScope,
    stop: CancellationToken,
    socket: Arc<tokio::sync::Mutex<Option<web_io::TextSocket>>>,
}

// Only a committed text reply owns this guard through bounded carrier egress.
// If that reply cannot be admitted, fence the exact stream before the next
// operation can acquire the gate. Close/release do not need this mutex.
pub(super) struct BrowserReply {
    pub(super) value: serde_json::Value,
    pub(super) chunks: Vec<WireFrame>,
    pub(super) pull: Option<PullCommit>,
}

impl From<(serde_json::Value, Vec<WireFrame>)> for BrowserReply {
    fn from((value, chunks): (serde_json::Value, Vec<WireFrame>)) -> Self {
        Self {
            value,
            chunks,
            pull: None,
        }
    }
}

pub(super) struct PullCommit {
    socket: OwnedMutexGuard<Option<web_io::TextSocket>>,
    committed: bool,
    slot: Arc<Slot>,
    state: Arc<Mutex<State>>,
}

impl PullCommit {
    pub(super) fn stop(&self) -> &CancellationToken {
        &self.slot.stop
    }

    pub(super) fn delivered(mut self) {
        self.committed = false;
    }
}

impl Drop for PullCommit {
    fn drop(&mut self) {
        if self.committed {
            if let Some(socket) = self.socket.as_mut() {
                socket.close();
            }
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.remember_closed(&self.slot);
        }
    }
}

impl State {
    fn remember_closed(&mut self, slot: &Arc<Slot>) {
        if let Some(owner) = self.owners.get_mut(&slot.scope.owner())
            && owner
                .slot
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, slot))
        {
            owner.slot = None;
            self.closed.insert(slot.scope.clone());
        }
    }
}

impl ScopedBrowser {
    pub(super) fn new(binding: BindingScope, lease: CancellationToken, ws_pull: bool) -> Self {
        Self {
            binding,
            lease,
            ws_pull,
            state: Arc::default(),
        }
    }

    pub(super) fn supports_pull(&self) -> bool {
        self.ws_pull
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(super) fn stop(&self) {
        self.lease.cancel();
        *self.lock() = State::default();
    }

    pub(super) fn token(&self) -> &CancellationToken {
        &self.lease
    }

    fn check(&self, deadline: Instant, caller: &CancellationToken) -> Result<()> {
        if self.lease.is_cancelled() {
            Err(Error::Stale)
        } else if caller.is_cancelled() {
            Err(Error::Cancelled)
        } else if Instant::now() >= deadline {
            Err(Error::Timeout)
        } else {
            Ok(())
        }
    }

    pub(super) async fn execute(
        &self,
        operation_id: String,
        request: WebTransportRequest,
        budget_ms: u32,
        timing: BrowserTiming,
        caller: &CancellationToken,
    ) -> std::result::Result<BrowserReply, WireError> {
        let deadline = timing.deadline;
        if matches!(request, WebTransportRequest::WsPull { .. }) && !self.supports_pull() {
            return Err(Error::Unsupported.into());
        }
        request.validate_for_lease(&self.binding, true, budget_ms)?;
        self.check(deadline, caller)?;
        let max_reply = request.max_reply_json_bytes();
        let cleanup = matches!(
            request,
            WebTransportRequest::WsClose { .. } | WebTransportRequest::ReleaseScope { .. }
        );
        // Both carriers provide the same admitted operation identity. Only a
        // parsed UUID is safe to log; never log arbitrary request strings.
        let bridge_operation_id = uuid::Uuid::parse_str(&operation_id)
            .ok()
            .filter(|id| id.to_string() == operation_id);
        {
            let mut state = self.lock();
            if operation_id.is_empty() || operation_id.len() > 256 {
                return Err(Error::InvalidRequest.into());
            }
            if state.operations.contains(&operation_id) {
                return Err(Error::OutcomeUnknown.into());
            }
            if state.operations.len() >= MAX_OPERATIONS {
                if !cleanup {
                    return Err(Error::Limit.into());
                }
            } else {
                state.operations.insert(operation_id);
            }
        }
        match request {
            WebTransportRequest::HttpGet {
                scope,
                url,
                max_bytes,
            } => {
                let reply = self
                    .discover(scope, url, max_bytes, deadline, caller)
                    .await?;
                outgoing(reply, max_reply).map(Into::into)
            }
            WebTransportRequest::WsOpen { scope, url } => {
                self.open(scope, &url, deadline, caller).await?;
                Ok((serde_json::Value::Null, vec![]).into())
            }
            WebTransportRequest::WsClose { scope } => {
                self.close(&scope);
                Ok((serde_json::Value::Null, vec![]).into())
            }
            WebTransportRequest::ReleaseScope { owner } => {
                self.release(&owner);
                Ok((serde_json::Value::Null, vec![]).into())
            }
            WebTransportRequest::WsPull {
                scope,
                max_bytes,
                soft_wait_ms,
            } => {
                let pull = PullRequest {
                    scope,
                    max_bytes,
                    soft_wait_ms,
                    max_reply,
                    bridge_operation_id,
                };
                self.pull(pull, timing, caller).await
            }
            request => self
                .socket_operation(request, deadline, caller, max_reply, bridge_operation_id)
                .await
                .map(Into::into),
        }
    }

    fn reserve(&self, state: &mut State, scope: &ConnectionScope) -> Result<()> {
        self.check_lease()?;
        let owner = scope.owner();
        if state.sealed
            || state.identities.len() >= MAX_IDENTITIES
            || (!state.owners.contains_key(&owner) && state.owners.len() >= MAX_OWNERS)
        {
            return Err(Error::Limit);
        }
        if state.owners.get(&owner).is_some_and(|owner| owner.retired)
            || !state.identities.insert(scope.clone())
        {
            return Err(Error::Stale);
        }
        Ok(())
    }

    fn check_lease(&self) -> Result<()> {
        if self.lease.is_cancelled() {
            Err(Error::Stale)
        } else {
            Ok(())
        }
    }

    async fn discover(
        &self,
        scope: ConnectionScope,
        url: String,
        max_bytes: u32,
        deadline: Instant,
        caller: &CancellationToken,
    ) -> Result<HttpResponse> {
        let owner = scope.owner();
        let stop = self.lease.child_token();
        {
            let mut state = self.lock();
            if state
                .owners
                .get(&owner)
                .is_some_and(|owner| owner.discovery.is_some())
            {
                return Err(Error::InvalidRequest);
            }
            self.reserve(&mut state, &scope)?;
            state.owners.entry(owner.clone()).or_default().discovery = Some(Discovery {
                scope: scope.clone(),
                stop: stop.clone(),
                url: url.clone(),
                ready: false,
            });
        }
        let operation = web_io::Operation::new(deadline, &stop, caller);
        let reply = web_io::http_get(&url, max_bytes as usize, &operation)
            .await
            .map_err(io_error)
            .and_then(|reply| {
                let value = HttpResponse {
                    status: reply.status,
                    body: reply.body,
                };
                value.validate(max_bytes)?;
                self.check(deadline, caller)?;
                Ok(value)
            });
        let mut state = self.lock();
        let Some(owner) = state.owners.get_mut(&owner) else {
            return Err(Error::Stale);
        };
        let Some(discovery) = owner
            .discovery
            .as_mut()
            .filter(|pending| pending.scope == scope)
        else {
            return Err(Error::Stale);
        };
        if reply.is_err() || stop.is_cancelled() {
            owner.discovery = None;
        } else {
            discovery.ready = true;
        }
        if stop.is_cancelled() {
            return Err(Error::Stale);
        }
        reply
    }

    async fn open(
        &self,
        scope: ConnectionScope,
        url: &str,
        deadline: Instant,
        caller: &CancellationToken,
    ) -> Result<()> {
        let owner = scope.owner();
        let slot = Arc::new(Slot {
            scope: scope.clone(),
            stop: self.lease.child_token(),
            socket: Arc::new(tokio::sync::Mutex::new(None)),
        });
        // Keep the opening slot occupied before awaiting the transport. Exact
        // Close or epoch release can remove it without waiting for the handshake.
        let mut socket = slot.socket.lock().await;
        {
            let mut state = self.lock();
            self.check(deadline, caller)?;
            if state
                .owners
                .get(&owner)
                .is_some_and(|owner| owner.slot.is_some())
            {
                return Err(Error::InvalidRequest);
            }
            if let Some(discovery) = state
                .owners
                .get_mut(&owner)
                .and_then(|owner| owner.discovery.as_mut())
                .filter(|discovery| discovery.scope == scope)
            {
                if !discovery.ready {
                    return Err(Error::InvalidRequest);
                }
                let check = web_io::same_discovery_authority(&discovery.url, url).map_err(io_error);
                state
                    .owners
                    .get_mut(&owner)
                    .expect("discovery owner")
                    .discovery = None;
                check?;
            } else {
                if state
                    .owners
                    .get(&owner)
                    .is_some_and(|owner| owner.discovery.is_some())
                {
                    return Err(Error::InvalidRequest);
                }
                self.reserve(&mut state, &scope)?;
            }
            state.owners.entry(owner.clone()).or_default().slot = Some(slot.clone());
        }
        let operation = web_io::Operation::new(deadline, &slot.stop, caller);
        let opened = web_io::TextSocket::open(url, &operation)
            .await
            .map_err(io_error);
        let mut state = self.lock();
        let Some(entry) = state.owners.get_mut(&owner).filter(|entry| {
            entry
                .slot
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &slot))
        }) else {
            return Err(Error::Stale);
        };
        match opened.and_then(|opened| {
            self.check(deadline, caller)?;
            Ok(opened)
        }) {
            Ok(opened) => {
                *socket = Some(opened);
                Ok(())
            }
            Err(error) => {
                entry.slot = None;
                slot.stop.cancel();
                Err(error)
            }
        }
    }

    async fn pull(
        &self,
        request: PullRequest,
        timing: BrowserTiming,
        caller: &CancellationToken,
    ) -> std::result::Result<BrowserReply, WireError> {
        let PullRequest {
            scope,
            max_bytes,
            soft_wait_ms,
            max_reply,
            bridge_operation_id,
        } = request;
        let BrowserTiming {
            admitted_at,
            deadline,
        } = timing;
        let slot = match self.slot(&scope) {
            Ok(slot) => slot,
            Err(Error::Closed) => return outgoing(WsPull::Closed, max_reply).map(Into::into),
            Err(error) => return Err(error.into()),
        };
        let requested_soft = (soft_wait_ms != 0)
            .then(|| admitted_at + Duration::from_millis(u64::from(soft_wait_ms)));
        let latest = (deadline - PULL_EGRESS_RESERVE).max(admitted_at);
        let soft_deadline = requested_soft.map(|soft| soft.min(latest));
        let soft_curtailed = soft_deadline != requested_soft;
        let socket = match soft_deadline {
            None => slot
                .socket
                .clone()
                .try_lock_owned()
                .map_err(|_| Error::Timeout)?,
            Some(soft_deadline) => tokio::select! {
                biased;
                _ = slot.stop.cancelled() => return Err(Error::Stale.into()),
                _ = caller.cancelled() => return Err(Error::Cancelled.into()),
                _ = tokio::time::sleep_until(deadline) => return Err(Error::Timeout.into()),
                socket = slot.socket.clone().lock_owned() => socket,
                _ = tokio::time::sleep_until(soft_deadline) => return Err(Error::Timeout.into()),
            },
        };
        if slot.stop.is_cancelled() {
            return Err(Error::Stale.into());
        }
        self.check(deadline, caller)?;
        let mut guard = PullCommit {
            socket,
            committed: false,
            slot: slot.clone(),
            state: self.state.clone(),
        };
        let Some(socket) = guard.socket.as_ref() else {
            return outgoing(WsPull::Closed, max_reply).map(Into::into);
        };
        let operation = web_io::Operation::new(deadline, &slot.stop, caller)
            .with_bridge_operation_id(bridge_operation_id);
        let text = socket
            .peek_pull(max_bytes as usize, soft_deadline, &operation)
            .await;
        if slot.stop.is_cancelled() {
            return Err(Error::Stale.into());
        }
        let text = match text {
            Ok(Some(text)) => text,
            Ok(None) if soft_curtailed => return Err(Error::Timeout.into()),
            Ok(None) => return outgoing(WsPull::Empty, max_reply).map(Into::into),
            Err(error) => {
                if socket.status().is_err() {
                    self.lock().remember_closed(&slot);
                }
                return match error {
                    web_io::Error::Closed => outgoing(WsPull::Closed, max_reply).map(Into::into),
                    error => Err(io_error(error).into()),
                };
            }
        };
        let reply = WsPull::Message(text.as_str().to_owned());
        reply.validate(max_bytes)?;
        let (value, chunks) = outgoing(reply, max_reply)?;
        // Validation and response/chunk encoding precede dequeue. The gate
        // prevents another operation from consuming the head in the meantime.
        match socket.commit_pull(&operation, &mut guard.committed) {
            Ok(()) => (),
            Err(_) if slot.stop.is_cancelled() => return Err(Error::Stale.into()),
            Err(error) => return Err(io_error(error).into()),
        }
        Ok(BrowserReply {
            value,
            chunks,
            pull: Some(guard),
        })
    }

    fn slot(&self, scope: &ConnectionScope) -> Result<Arc<Slot>> {
        let state = self.lock();
        state
            .owners
            .get(&scope.owner())
            .and_then(|owner| owner.slot.as_ref())
            .filter(|slot| &slot.scope == scope)
            .cloned()
            .ok_or_else(|| {
                if state.closed.contains(scope) {
                    Error::Closed
                } else {
                    Error::Stale
                }
            })
    }

    async fn socket_operation(
        &self,
        request: WebTransportRequest,
        deadline: Instant,
        caller: &CancellationToken,
        max_reply: Option<usize>,
        bridge_operation_id: Option<uuid::Uuid>,
    ) -> std::result::Result<(serde_json::Value, Vec<WireFrame>), WireError> {
        let scope = match &request {
            WebTransportRequest::WsStatus { scope }
            | WebTransportRequest::WsSend { scope, .. }
            | WebTransportRequest::WsReceive { scope, .. } => scope.clone(),
            _ => return Err(Error::InvalidRequest.into()),
        };
        let slot = self.slot(&scope)?;
        let mut locked = tokio::select! {
            biased;
            _ = slot.stop.cancelled() => return Err(Error::Stale.into()),
            _ = caller.cancelled() => return Err(Error::Cancelled.into()),
            _ = tokio::time::sleep_until(deadline) => return Err(Error::Timeout.into()),
            locked = slot.socket.lock() => locked,
        };
        if slot.stop.is_cancelled() {
            return Err(Error::Stale.into());
        }
        self.check(deadline, caller)?;
        let socket = locked.as_mut().ok_or(Error::Closed)?;
        let operation = web_io::Operation::new(deadline, &slot.stop, caller)
            .with_bridge_operation_id(bridge_operation_id);
        let result = match request {
            WebTransportRequest::WsStatus { .. } => socket
                .status_for(&operation)
                .map(|()| (serde_json::Value::Null, vec![]))
                .map_err(|e| io_error(e).into()),
            WebTransportRequest::WsSend { text, .. } => socket
                .send(text, &operation)
                .await
                .map(|()| (serde_json::Value::Null, vec![]))
                .map_err(|e| io_error(e).into()),
            WebTransportRequest::WsReceive { max_bytes, .. } => {
                match socket.receive(max_bytes as usize, &operation).await {
                    Ok(text) => {
                        let reply = WsReceive::Message(text);
                        reply.validate(max_bytes)?;
                        outgoing(reply, max_reply)
                    }
                    Err(web_io::Error::Timeout) => outgoing(WsReceive::Timeout, max_reply),
                    Err(web_io::Error::Closed) => outgoing(WsReceive::Closed, max_reply),
                    Err(error) => Err(io_error(error).into()),
                }
            }
            _ => unreachable!(),
        };
        if slot.stop.is_cancelled() {
            return Err(Error::Stale.into());
        }
        if socket.status().is_err() {
            self.lock().remember_closed(&slot);
        }
        result
    }

    pub(super) fn close(&self, scope: &ConnectionScope) {
        let mut state = self.lock();
        state.closed.remove(scope);
        if !state.identities.contains(scope) {
            if state.identities.len() < MAX_IDENTITIES {
                state.identities.insert(scope.clone());
            } else {
                state.sealed = true;
            }
        }
        if let Some(owner) = state.owners.get_mut(&scope.owner()) {
            if owner
                .discovery
                .as_ref()
                .is_some_and(|item| &item.scope == scope)
            {
                owner
                    .discovery
                    .take()
                    .expect("matching discovery")
                    .stop
                    .cancel();
            }
            if owner.slot.as_ref().is_some_and(|slot| &slot.scope == scope) {
                owner.slot.take().expect("matching slot").stop.cancel();
            }
        }
    }

    fn release(&self, owner: &ConnectionOwner) {
        let mut state = self.lock();
        if !state.owners.contains_key(owner) && state.owners.len() >= MAX_OWNERS {
            state.sealed = true;
            return;
        }
        let entry = state.owners.entry(owner.clone()).or_default();
        entry.retired = true;
        if let Some(discovery) = entry.discovery.take() {
            discovery.stop.cancel();
        }
        if let Some(slot) = entry.slot.take() {
            slot.stop.cancel();
        }
        state.closed.retain(|scope| scope.owner() != *owner);
    }
}

fn io_error(error: web_io::Error) -> Error {
    match error {
        web_io::Error::InvalidEndpoint => Error::InvalidRequest,
        web_io::Error::AuthorityMismatch => Error::Denied,
        web_io::Error::Limit => Error::Limit,
        web_io::Error::Cancelled => Error::Cancelled,
        web_io::Error::Timeout => Error::Timeout,
        web_io::Error::Closed => Error::Closed,
        web_io::Error::OutcomeUnknown => Error::OutcomeUnknown,
        web_io::Error::Io | web_io::Error::InvalidResponse => Error::Io,
    }
}

fn outgoing(
    value: impl Serialize,
    max_reply: Option<usize>,
) -> std::result::Result<(serde_json::Value, Vec<WireFrame>), WireError> {
    let value = serde_json::to_value(value).map_err(|_| Error::Io)?;
    let bytes = serde_json::to_vec(&value).map_err(|_| Error::Io)?;
    if bytes.len() > max_reply.ok_or(Error::Io)? {
        return Err(Error::Limit.into());
    }
    if bytes.len() > MAX_FRAME_BYTES / 2 {
        transfers::outgoing(bytes, "json")
    } else {
        Ok((value, vec![]))
    }
}
