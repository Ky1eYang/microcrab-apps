//! Renewable round questions are control-plane decisions, never permission grants.
//! Lock order is executor lease -> registry -> Hub. Human waits hold no locks;
//! lease loss retires the old question and waits for a fresh executor indefinitely.
use super::{Manager, client_tools::ClientTools, hub::Hub, now, unavailable};
use crate::auth::Principal;
use async_trait::async_trait;
use microcrab_protocol::application::{Command, WireError};
use microcrab_session::{
    Error,
    budget::{RoundLimitCallback, RoundLimitDecision, RoundLimitRequest},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;

const MAX_PENDING: usize = 32;
const MAX_SETTLED: usize = 128;
#[derive(Clone, Serialize)]
struct Request {
    request_id: String,
    parent_session_id: String,
    #[serde(flatten)]
    invocation: RoundLimitRequest,
    created_at_ms: u64,
}
struct Pending {
    request: Request,
    socket: String,
    lease: String,
    sender: oneshot::Sender<Option<RoundLimitDecision>>,
}
struct Settled {
    id: String,
    socket: String,
    lease: String,
    decision: Option<RoundLimitDecision>,
}
#[derive(Default)]
struct State {
    pending: HashMap<String, Pending>,
    settled: VecDeque<Settled>,
}
pub(super) struct RoundLimit {
    parent: String,
    tools: Arc<ClientTools>,
    hub: Arc<Hub>,
    state: Mutex<State>,
    changed: Notify,
    closed: CancellationToken,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    request_id: String,
    lease_id: String,
    decision: RoundLimitDecision,
}

struct Retire<'a> {
    bridge: &'a RoundLimit,
    id: String,
}
impl Drop for Retire<'_> {
    fn drop(&mut self) {
        self.bridge
            .retire(&self.id, "cancelled_or_executor_changed");
    }
}
impl RoundLimit {
    pub(super) fn new(parent: String, tools: Arc<ClientTools>, hub: Arc<Hub>) -> Arc<Self> {
        Arc::new(Self {
            parent,
            tools,
            hub,
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            closed: CancellationToken::new(),
        })
    }
    fn finish(
        &self,
        state: &mut State,
        id: &str,
        decision: Option<RoundLimitDecision>,
        reason: &str,
    ) {
        let Some(pending) = state.pending.remove(id) else {
            return;
        };
        self.hub.publish(super::SessionRuntimeEvent::RoundLimitSettled(json!({"request_id":id,"parent_session_id":self.parent,
            "session_id":pending.request.invocation.session_id,"decision":decision,"reason":reason})));
        state.settled.push_back(Settled {
            id: id.into(),
            socket: pending.socket,
            lease: pending.lease,
            decision,
        });
        while state.settled.len() > MAX_SETTLED {
            state.settled.pop_front();
        }
        let _ = pending.sender.send(decision);
        self.changed.notify_waiters();
    }
    fn retire(&self, id: &str, reason: &str) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        self.finish(&mut state, id, None, reason);
    }
    pub(super) fn close(&self) {
        self.closed.cancel();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let ids: Vec<_> = state.pending.keys().cloned().collect();
        for id in ids {
            self.finish(&mut state, &id, None, "session_closed");
        }
    }
    async fn snapshot(&self) -> Value {
        self.tools.with_executor(|executor| {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let mut requests: Vec<_> = state.pending.values().filter(|pending| executor.is_some_and(|(socket, lease)|
                socket == pending.socket && lease == pending.lease)).map(|pending| pending.request.clone()).collect();
            requests.sort_by(|a,b| a.created_at_ms.cmp(&b.created_at_ms).then(a.request_id.cmp(&b.request_id)));
            json!({"parent_session_id":self.parent,"requests":requests,"executor_connected":executor.is_some()})
        }).await
    }
    async fn reply(&self, socket: &str, reply: Reply) -> Result<Value, WireError> {
        self.tools.with_executor(|executor| {
            if executor != Some((socket, reply.lease_id.as_str())) {
                return Err(WireError::new("round_limit_executor_required", "Current executor lease required"));
            }
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if self.closed.is_cancelled() {
                return Err(WireError::new("round_limit_request_settled", "Round confirmation is closed"));
            }
            if let Some(pending) = state.pending.get(&reply.request_id) {
                if pending.socket != socket || pending.lease != reply.lease_id {
                    return Err(WireError::new("round_limit_request_settled", "Round question belongs to an expired executor"));
                }
                self.finish(&mut state, &reply.request_id, Some(reply.decision), "user_once");
                return Ok(json!({"request_id":reply.request_id,"decision":reply.decision,"duplicate":false}));
            }
            if state.settled.iter().any(|settled| settled.id == reply.request_id && settled.socket == socket && settled.lease == reply.lease_id && settled.decision == Some(reply.decision)) {
                return Ok(json!({"request_id":reply.request_id,"decision":reply.decision,"duplicate":true}));
            }
            Err(WireError::new("round_limit_request_settled", "Round question is absent or settled"))
        }).await
    }
}
#[async_trait]
impl RoundLimitCallback for RoundLimit {
    async fn request(
        &self,
        invocation: RoundLimitRequest,
        cancellation: CancellationToken,
    ) -> Result<RoundLimitDecision, Error> {
        if invocation.session_id.is_empty()
            || serde_json::to_vec(&invocation)
                .map_err(|error| {
                    Error::protocol_with_private_reason(
                        "round confirmation metadata serialization failed",
                        error.to_string(),
                    )
                })?
                .len()
                > 64 * 1024
        {
            return Err(Error::protocol("Invalid round confirmation metadata"));
        }
        // Subscribe before registration, so lease changes cannot fall into an await gap.
        let mut changes = self.tools.executor_changes();
        loop {
            if cancellation.is_cancelled() || self.closed.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let available = self.changed.notified();
            tokio::pin!(available);
            available.as_mut().enable();
            let registered = self
                .tools
                .with_executor(|executor| {
                    let (socket, lease) = executor?;
                    let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                    if self.closed.is_cancelled() || state.pending.len() >= MAX_PENDING {
                        return None;
                    }
                    let id = uuid::Uuid::new_v4().to_string();
                    let request = Request {
                        request_id: id.clone(),
                        parent_session_id: self.parent.clone(),
                        invocation: invocation.clone(),
                        created_at_ms: now(),
                    };
                    let (sender, receiver) = oneshot::channel();
                    state.pending.insert(
                        id.clone(),
                        Pending {
                            request: request.clone(),
                            socket: socket.into(),
                            lease: lease.into(),
                            sender,
                        },
                    );
                    self.hub
                        .publish(super::SessionRuntimeEvent::RoundLimitRequested(
                            json!({"request":request}),
                        ));
                    Some((id, (socket.to_owned(), lease.to_owned()), receiver))
                })
                .await;
            let Some((id, lease, receiver)) = registered else {
                tokio::select! { biased;
                    _ = cancellation.cancelled() => return Err(Error::Cancelled),
                    _ = self.closed.cancelled() => return Err(Error::Cancelled),
                    changed = changes.changed() => { if changed.is_err() { return Err(Error::Cancelled); } },
                    _ = &mut available => {},
                }
                continue;
            };
            let _retire = Retire { bridge: self, id };
            let revoked = async {
                loop {
                    if changes.borrow_and_update().as_ref() != Some(&lease) {
                        break;
                    }
                    if changes.changed().await.is_err() {
                        break;
                    }
                }
            };
            tokio::select! { biased;
                _ = cancellation.cancelled() => return Err(Error::Cancelled),
                _ = self.closed.cancelled() => return Err(Error::Cancelled),
                answer = receiver => match answer {
                    Ok(Some(decision)) => return Ok(decision),
                    Ok(None) => {},
                    Err(_) => return Err(Error::Cancelled),
                },
                _ = revoked => {},
            }
            // Dropping Retire retires the old identity. A replacement executor must
            // receive a new question and cannot replay the former lease's answer.
        }
    }
}
impl Manager {
    async fn round_limit_bridge(&self, parent: &str) -> Option<Arc<RoundLimit>> {
        let slot = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(parent)
            .cloned()?;
        let managed = slot.active.lock().await.clone()?;
        Some(managed.round_limit.clone())
    }
    pub(crate) async fn round_limit_requests(
        &self,
        principal: &Principal,
        parent: &str,
    ) -> Result<Value, WireError> {
        self.record(&principal.owner, parent).await?;
        match self.round_limit_bridge(parent).await {
            Some(bridge) => Ok(bridge.snapshot().await),
            None => {
                Ok(json!({"parent_session_id":parent,"requests":[],"executor_connected":false}))
            }
        }
    }
    pub(crate) async fn round_limit_reply(
        &self,
        principal: &Principal,
        socket: &str,
        command: &Command,
    ) -> Result<Value, WireError> {
        let parent = command.session_id.as_deref().ok_or_else(unavailable)?;
        self.record(&principal.owner, parent).await?;
        let reply: Reply = super::commands::parse(command)?;
        if reply.request_id.is_empty()
            || reply.request_id.len() > 256
            || reply.lease_id.is_empty()
            || reply.lease_id.len() > 256
        {
            return Err(unavailable());
        }
        self.round_limit_bridge(parent)
            .await
            .ok_or_else(unavailable)?
            .reply(socket, reply)
            .await
    }
}

#[cfg(test)]
mod tests;
