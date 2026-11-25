//! Renewable round questions are control-plane decisions, never permission grants.
//! Lock order is participant membership -> registry -> Hub. Human waits hold no locks;
//! questions retain their identity across socket and executor changes.
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
    responder: Option<String>,
    sender: oneshot::Sender<Option<RoundLimitDecision>>,
}
struct Settled {
    id: String,
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
    #[serde(default)]
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
    ) -> Result<(), Error> {
        let Some(pending) = state.pending.get(id) else {
            return Ok(());
        };
        let outcome = json!({"request_id":id,"parent_session_id":self.parent,
            "session_id":pending.request.invocation.session_id,"responder_socket":pending.responder,"decision":decision,"reason":reason});
        self.tools.record_decision("round_limit", id, &outcome)?;
        let Some(pending) = state.pending.remove(id) else {
            return Ok(());
        };
        self.hub
            .publish(super::SessionRuntimeEvent::RoundLimitSettled(outcome));
        state.settled.push_back(Settled {
            id: id.into(),
            decision,
        });
        while state.settled.len() > MAX_SETTLED {
            state.settled.pop_front();
        }
        let _ = pending.sender.send(decision);
        self.changed.notify_waiters();
        Ok(())
    }
    fn retire(&self, id: &str, reason: &str) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        self.retire_locked(&mut state, id, reason);
    }
    fn retire_locked(&self, state: &mut State, id: &str, reason: &str) {
        if let Err(error) = self.finish(state, id, None, reason) {
            state.pending.remove(id);
            self.hub.publish(super::SessionRuntimeEvent::RoundLimitSettled(json!({"request_id":id,"decision":null,"reason":"outcome_unavailable","durable":false,"cause":error.diagnostic_reason()})));
            self.changed.notify_waiters();
        }
    }
    pub(super) fn close(&self) {
        self.closed.cancel();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let ids: Vec<_> = state.pending.keys().cloned().collect();
        for id in ids {
            self.retire_locked(&mut state, &id, "session_closed");
        }
    }
    async fn snapshot(&self) -> Value {
        self.tools.with_executor(|executor| {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let mut requests: Vec<_> = state.pending.values().map(|pending| pending.request.clone()).collect();
            requests.sort_by(|a,b| a.created_at_ms.cmp(&b.created_at_ms).then(a.request_id.cmp(&b.request_id)));
            json!({"parent_session_id":self.parent,"requests":requests,"executor_connected":executor.is_some()})
        }).await
    }
    async fn reply(&self, socket: &str, reply: Reply) -> Result<Value, WireError> {
        self.tools.with_participant(socket, |participant| {
            if !participant {
                return Err(WireError::new("round_limit_participant_required", "A bound owner participant is required"));
            }
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if self.closed.is_cancelled() {
                return Err(WireError::new("round_limit_request_settled", "Round confirmation is closed"));
            }
            if state.pending.contains_key(&reply.request_id) {
                if let Some(pending) = state.pending.get_mut(&reply.request_id) { pending.responder = Some(socket.into()); }
                self.finish(&mut state, &reply.request_id, Some(reply.decision), "user_once").map_err(super::safe)?;
                if state.pending.contains_key(&reply.request_id) { return Err(WireError::new("interaction_outcome_unknown", "Round outcome was not acknowledged; observe the request")); }
                return Ok(json!({"request_id":reply.request_id,"decision":reply.decision,"duplicate":false}));
            }
            if let Some(settled) = state.settled.iter().find(|settled| settled.id == reply.request_id) {
                return Ok(json!({"request_id":reply.request_id,"decision":settled.decision,"duplicate":true}));
            }
            if let Some(receipt) = self.tools.decision("round_limit", &reply.request_id) { return Ok(receipt); }
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
        // Subscribe before registration, so capacity wakeups cannot fall into an await gap.
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
                .with_interaction_audience(|_connected| {
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
                            responder: None,
                            sender,
                        },
                    );
                    self.hub
                        .publish(super::SessionRuntimeEvent::RoundLimitRequested(
                            json!({"request":request}),
                        ));
                    Some((id, receiver))
                })
                .await;
            let Some((id, receiver)) = registered else {
                tokio::select! { biased;
                    _ = cancellation.cancelled() => return Err(Error::Cancelled),
                    _ = self.closed.cancelled() => return Err(Error::Cancelled),
                    changed = changes.changed() => { if changed.is_err() { return Err(Error::Cancelled); } },
                    _ = &mut available => {},
                }
                continue;
            };
            let _retire = Retire { bridge: self, id };
            tokio::select! { biased;
                _ = cancellation.cancelled() => return Err(Error::Cancelled),
                _ = self.closed.cancelled() => return Err(Error::Cancelled),
                answer = receiver => match answer {
                    Ok(Some(decision)) => return Ok(decision),
                    Ok(None) => {},
                    Err(_) => return Err(Error::Cancelled),
                },
            }
            // Retire releases this question before another registration attempt.
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
        if reply.request_id.is_empty() || reply.request_id.len() > 256 || reply.lease_id.len() > 256
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
