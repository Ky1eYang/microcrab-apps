//! Lease-backed permission transport. Session owns the sole review arbiter.
//! Executor -> registry -> Hub is the synchronous lock order; no registry guard
//! crosses await. Accepted user receipts remain authoritative at timer settlement.
use super::{Manager, client_tools::ClientTools, hub::Hub, now, unavailable};
use crate::auth::Principal;
use async_trait::async_trait;
use microcrab_protocol::application::{Command, WireError};
use microcrab_session::policy::{
    Action, AskAttribution, AskReply, Decision, FutureGrantAdoption, FutureGrantProposal,
    PermissionError, PermissionFeatures, PermissionInteraction, PermissionRequest, PolicySnapshot,
    ReviewResult, SessionPermission,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{sync::oneshot, time::Instant};

const MAX_PENDING: usize = 32;
const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_SETTLED: usize = 128;
const ASK_TIMEOUT: Duration = Duration::from_secs(120);
const GRACE: Duration = Duration::from_secs(5);
const PROPOSAL_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone, Serialize)]
struct ReviewSummary {
    action: Action,
    reason: String,
}
#[derive(Clone, Serialize)]
struct Request {
    request_id: String,
    parent_session_id: String,
    controller_id: u64,
    owning_session_id: String,
    #[serde(flatten)]
    invocation: PermissionRequest,
    phase: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    review: Option<ReviewSummary>,
    created_at_ms: u64,
    expires_at_ms: u64,
}
struct Pending {
    request: Request,
    socket: String,
    lease: String,
    sender: oneshot::Sender<AskReply>,
    deadline: Instant,
}
struct Settled {
    request_id: String,
    socket: String,
    lease: String,
    decision: Action,
    reason: String,
    evidence: Value,
}
#[derive(Clone, Serialize)]
struct Proposal {
    proposal_id: String,
    parent_session_id: String,
    #[serde(flatten)]
    grant: FutureGrantProposal,
    created_at_ms: u64,
    expires_at_ms: u64,
}
struct PendingProposal {
    proposal: Proposal,
    socket: String,
    lease: String,
    deadline: Instant,
    sender: oneshot::Sender<bool>,
    adoption: Arc<dyn FutureGrantAdoption>,
}
struct ProposalReceipt {
    id: String,
    socket: String,
    lease: String,
    accept: bool,
    result: Value,
}
#[derive(Default)]
struct State {
    pending: HashMap<String, Pending>,
    settled: VecDeque<Settled>,
    proposals: HashMap<String, PendingProposal>,
    proposal_receipts: VecDeque<ProposalReceipt>,
    proposal_committing: HashMap<String, ProposalReceipt>,
    closed: bool,
}
pub(super) struct PermissionAsk {
    parent: String,
    tools: Arc<ClientTools>,
    hub: Arc<Hub>,
    state: Mutex<State>,
    timeout: Duration,
    weak: Weak<Self>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    request_id: String,
    lease_id: String,
    decision: ReplyDecision,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReplyDecision {
    Allow,
    Deny,
}
impl ReplyDecision {
    fn action(self) -> Action {
        match self {
            Self::Allow => Action::Allow,
            Self::Deny => Action::Deny,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProposalReply {
    proposal_id: String,
    lease_id: String,
    accept: bool,
}
struct Interaction {
    bridge: Arc<PermissionAsk>,
    id: String,
    socket: String,
    lease: String,
    deadline: Instant,
    receiver: tokio::sync::Mutex<Option<oneshot::Receiver<AskReply>>>,
}
impl Drop for Interaction {
    fn drop(&mut self) {
        self.bridge
            .retire(&self.id, "cancelled_or_runtime_deadline");
    }
}
impl PermissionAsk {
    pub(super) fn new(parent: String, tools: Arc<ClientTools>, hub: Arc<Hub>) -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            parent,
            tools,
            hub,
            state: Mutex::new(State::default()),
            timeout: ASK_TIMEOUT,
            weak: weak.clone(),
        })
    }
    fn finish(
        &self,
        state: &mut State,
        id: &str,
        action: Action,
        reason: &str,
    ) -> Option<AskReply> {
        let pending = state.pending.remove(id)?;
        let reply = AskReply {
            action,
            reason: reason.into(),
        };
        let source = match reason {
            "user_once" => "user",
            "review_auto" => "review",
            "timeout" | "timeout_or_closed" => "timeout",
            "executor_disconnected" => "unavailable",
            _ => "cancelled",
        };
        let evidence = json!({"request_id":id,"parent_session_id":self.parent,
            "session_id":pending.request.invocation.session_id,"owning_session_id":pending.request.owning_session_id,
            "call_id":pending.request.invocation.call_id,"revision":pending.request.invocation.revision,
            "agent":pending.request.invocation.agent,"mode":pending.request.invocation.mode,"tool":pending.request.invocation.tool,
            "controller_id":pending.request.controller_id,"decision":action,"source":source,"reason":reason,
            "review_reason":pending.request.review.as_ref().map(|r| &r.reason),"remembered":false});
        self.hub
            .publish(super::SessionRuntimeEvent::PermissionSettled(
                evidence.clone(),
            ));
        state.settled.push_back(Settled {
            request_id: id.into(),
            socket: pending.socket,
            lease: pending.lease,
            decision: action,
            reason: reason.into(),
            evidence,
        });
        while state.settled.len() > MAX_SETTLED {
            state.settled.pop_front();
        }
        let _ = pending.sender.send(reply.clone());
        Some(reply)
    }
    fn retire(&self, id: &str, reason: &str) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.finish(&mut state, id, Action::Deny, reason);
    }
    pub(super) fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        let ids: Vec<_> = state.pending.keys().cloned().collect();
        for id in ids {
            self.finish(&mut state, &id, Action::Deny, "session_closed");
        }
        for (_, proposal) in state.proposals.drain() {
            let _ = proposal.sender.send(false);
        }
    }
    async fn snapshot(&self) -> Value {
        self.tools.with_executor(|executor| {
            let state=self.state.lock().unwrap_or_else(|e|e.into_inner());
            let mut requests:Vec<_>=state.pending.values().filter(|p|executor==Some((p.socket.as_str(),p.lease.as_str()))).map(|p|p.request.clone()).collect();
            requests.sort_by(|a,b|a.created_at_ms.cmp(&b.created_at_ms).then(a.request_id.cmp(&b.request_id)));
            let mut proposals:Vec<_>=state.proposals.values().filter(|p|executor==Some((p.socket.as_str(),p.lease.as_str())) && Instant::now()<p.deadline).map(|p|p.proposal.clone()).collect();
            proposals.sort_by(|a,b|a.proposal_id.cmp(&b.proposal_id));
            let mut proposal_results:Vec<_>=state.proposal_receipts.iter().chain(state.proposal_committing.values()).map(|receipt|receipt.result.clone()).collect();
            proposal_results.sort_by(|a,b|a["proposal_id"].as_str().cmp(&b["proposal_id"].as_str()));
            let decisions:Vec<_>=state.settled.iter().map(|receipt|receipt.evidence.clone()).collect();
            json!({"parent_session_id":self.parent,"requests":requests,"decisions":decisions,"proposals":proposals,"proposal_results":proposal_results,"interactive_ask":!state.closed,"remembered_grants":true,"executor_connected":executor.is_some()})
        }).await
    }
    async fn reply(&self, socket: &str, reply: Reply) -> Result<Value, WireError> {
        self.tools.with_executor(|executor| {
            if executor!=Some((socket,reply.lease_id.as_str())) { return Err(WireError::new("permission_executor_required","Current executor lease required")); }
            let mut state=self.state.lock().unwrap_or_else(|e|e.into_inner()); let action=reply.decision.action();
            if let Some(pending)=state.pending.get(&reply.request_id) {
                if pending.socket!=socket || pending.lease!=reply.lease_id { return Err(WireError::new("permission_request_settled","Permission request belongs to an expired executor")); }
                if Instant::now()>=pending.deadline || state.closed {
                    self.finish(&mut state,&reply.request_id,Action::Deny,"timeout_or_closed");
                    return Err(WireError::new("permission_request_settled","Permission request expired or closed"));
                }
                self.finish(&mut state,&reply.request_id,action,"user_once");
                return Ok(json!({"request_id":reply.request_id,"decision":action,"remembered":false,"duplicate":false}));
            }
            if state.settled.iter().any(|s|s.request_id==reply.request_id&&s.socket==socket&&s.lease==reply.lease_id&&s.decision==action&&s.reason=="user_once") {
                return Ok(json!({"request_id":reply.request_id,"decision":action,"remembered":false,"duplicate":true}));
            }
            Err(WireError::new("permission_request_settled","Permission request is absent or settled"))
        }).await
    }
    async fn open(
        &self,
        request: &PermissionRequest,
        attribution: &AskAttribution,
    ) -> Result<Arc<dyn PermissionInteraction>, PermissionError> {
        if serde_json::to_vec(request)
            .map_err(|_| PermissionError::Unavailable)?
            .len()
            > MAX_REQUEST_BYTES
            || request.session_id.is_empty()
        {
            return Err(PermissionError::Unavailable);
        }
        let id = uuid::Uuid::new_v4().to_string();
        let (sender, receiver) = oneshot::channel();
        let deadline = Instant::now() + self.timeout;
        let (socket, lease) = self
            .tools
            .with_executor(|executor| {
                let (socket, lease) = executor.ok_or(PermissionError::Unavailable)?;
                let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                if state.closed || state.pending.len() >= MAX_PENDING {
                    return Err(PermissionError::Unavailable);
                }
                let created = now();
                let row = Request {
                    request_id: id.clone(),
                    parent_session_id: self.parent.clone(),
                    controller_id: attribution.controller_id,
                    owning_session_id: attribution.owning_session_id.clone(),
                    invocation: request.clone(),
                    phase: "waiting_user",
                    review: None,
                    created_at_ms: created,
                    expires_at_ms: created.saturating_add(self.timeout.as_millis() as u64),
                };
                state.pending.insert(
                    id.clone(),
                    Pending {
                        request: row.clone(),
                        socket: socket.into(),
                        lease: lease.into(),
                        sender,
                        deadline,
                    },
                );
                self.hub
                    .publish(super::SessionRuntimeEvent::PermissionRequested(
                        json!({"request":row}),
                    ));
                Ok((socket.to_owned(), lease.to_owned()))
            })
            .await?;
        Ok(Arc::new(Interaction {
            bridge: self.weak.upgrade().ok_or(PermissionError::Unavailable)?,
            id,
            socket,
            lease,
            deadline,
            receiver: tokio::sync::Mutex::new(Some(receiver)),
        }))
    }
}
#[async_trait]
impl PermissionInteraction for Interaction {
    fn deadline(&self) -> Instant {
        self.deadline
    }
    fn validate_dispatch(&self) -> Result<(), PermissionError> {
        let changes = self.bridge.tools.executor_changes();
        let current = changes.borrow().clone();
        if current.as_ref() != Some(&(self.socket.clone(), self.lease.clone()))
            || self
                .bridge
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .closed
        {
            Err(PermissionError::Unavailable)
        } else {
            Ok(())
        }
    }
    async fn user_reply(&self) -> Result<AskReply, PermissionError> {
        let receiver = self
            .receiver
            .lock()
            .await
            .take()
            .ok_or(PermissionError::Unavailable)?;
        let mut changes = self.bridge.tools.executor_changes();
        let lease = (self.socket.clone(), self.lease.clone());
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
            _=revoked=>{self.bridge.retire(&self.id,"executor_disconnected");Err(PermissionError::Unavailable)},
            reply=receiver=>reply.map_err(|_|PermissionError::Unavailable),
        }
    }
    async fn review(
        &self,
        result: &ReviewResult,
        received_at: Instant,
    ) -> Result<Instant, PermissionError> {
        self.bridge
            .tools
            .with_executor(|executor| {
                if executor != Some((self.socket.as_str(), self.lease.as_str())) {
                    return Err(PermissionError::Unavailable);
                }
                let mut state = self.bridge.state.lock().unwrap_or_else(|e| e.into_inner());
                // Accepted user reply already won. It remains available through the
                // receipt; do not turn a user win into a transport error.
                if state
                    .settled
                    .iter()
                    .any(|r| r.request_id == self.id && r.reason == "user_once")
                {
                    return Ok(Instant::now() + GRACE);
                }
                if state.closed {
                    return Err(PermissionError::Unavailable);
                }
                let pending = state
                    .pending
                    .get_mut(&self.id)
                    .ok_or(PermissionError::Unavailable)?;
                if received_at >= self.deadline {
                    return Err(PermissionError::Deadline);
                }
                if pending.request.review.is_none() {
                    pending.deadline = Instant::now() + GRACE;
                    pending.request.expires_at_ms = now().saturating_add(GRACE.as_millis() as u64);
                    pending.request.phase = "review_grace";
                    pending.request.review = Some(ReviewSummary {
                        action: result.action,
                        reason: result.reason.clone(),
                    });
                    self.bridge
                        .hub
                        .publish(super::SessionRuntimeEvent::PermissionUpdated(
                            json!({"request":pending.request}),
                        ));
                }
                Ok(pending.deadline)
            })
            .await
    }
    async fn settle(&self, decision: &Decision) -> Result<AskReply, PermissionError> {
        self.bridge
            .tools
            .with_executor(|executor| {
                let mut state = self.bridge.state.lock().unwrap_or_else(|e| e.into_inner());
                if executor != Some((self.socket.as_str(), self.lease.as_str())) || state.closed {
                    self.bridge
                        .finish(&mut state, &self.id, Action::Deny, "executor_disconnected");
                    return Err(PermissionError::Unavailable);
                }
                if let Some(receipt) = state.settled.iter().find(|r| {
                    r.request_id == self.id && r.socket == self.socket && r.lease == self.lease
                }) {
                    return Ok(AskReply {
                        action: receipt.decision,
                        reason: receipt.reason.clone(),
                    });
                }
                if state
                    .pending
                    .get(&self.id)
                    .is_some_and(|pending| Instant::now() < pending.deadline)
                {
                    return Err(PermissionError::Deadline);
                }
                let reason = if decision.source == "review" {
                    "review_auto"
                } else {
                    "timeout"
                };
                self.bridge
                    .finish(&mut state, &self.id, decision.action, reason)
                    .ok_or(PermissionError::Unavailable)
            })
            .await
    }
}
#[async_trait]
impl SessionPermission for PermissionAsk {
    fn declarative_policy(&self) -> bool {
        true
    }
    fn features(&self) -> PermissionFeatures {
        PermissionFeatures {
            interactive_ask: true,
            remembered_grants: true,
            live_edit: true,
        }
    }
    async fn evaluate(
        &self,
        policy: &PolicySnapshot,
        request: &PermissionRequest,
    ) -> Result<Decision, PermissionError> {
        microcrab_permission::evaluate(policy, request)
            .map_err(|e| PermissionError::InvalidConfiguration(e.to_string()))
    }
    async fn open_interaction(
        &self,
        request: &PermissionRequest,
        attribution: &AskAttribution,
    ) -> Result<Option<Arc<dyn PermissionInteraction>>, PermissionError> {
        Ok(Some(self.open(request, attribution).await?))
    }
    async fn ask(&self, request: &PermissionRequest) -> Result<AskReply, PermissionError> {
        let interaction = self
            .open(
                request,
                &AskAttribution {
                    controller_id: 0,
                    owning_session_id: request.session_id.clone(),
                },
            )
            .await?;
        tokio::select! { biased;
            reply=interaction.user_reply()=>reply,
            _=tokio::time::sleep_until(interaction.deadline())=>{interaction.settle(&Decision::new(Action::Deny,"permission deadline exceeded","timeout")).await},
        }
    }
    async fn propose_future_grant(
        &self,
        proposal: FutureGrantProposal,
        adoption: Arc<dyn FutureGrantAdoption>,
    ) -> Result<(), PermissionError> {
        self.propose(proposal, adoption).await
    }
}
impl Manager {
    async fn permission_bridge(&self, parent: &str) -> Option<Arc<PermissionAsk>> {
        let slot = self
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(parent)
            .cloned()?;
        let managed = slot.active.lock().await.clone()?;
        Some(managed.permission_ask.clone())
    }
    pub(crate) async fn permission_requests(
        &self,
        principal: &Principal,
        parent: &str,
    ) -> Result<Value, WireError> {
        self.record(&principal.owner, parent).await?;
        match self.permission_bridge(parent).await {
            Some(bridge) => Ok(bridge.snapshot().await),
            None => Ok(
                json!({"parent_session_id":parent,"requests":[],"proposals":[],"interactive_ask":false,"remembered_grants":false,"executor_connected":false}),
            ),
        }
    }
    pub(crate) async fn permission_reply(
        &self,
        principal: &Principal,
        socket: &str,
        command: &Command,
    ) -> Result<Value, WireError> {
        let parent = command.session_id.as_deref().ok_or_else(unavailable)?;
        self.record(&principal.owner, parent).await?;
        let reply: Reply = super::commands::parse(command)?;
        if reply.request_id.len() > 256 || reply.lease_id.len() > 256 {
            return Err(unavailable());
        }
        self.permission_bridge(parent)
            .await
            .ok_or_else(unavailable)?
            .reply(socket, reply)
            .await
    }
    pub(crate) async fn permission_proposal_reply(
        &self,
        principal: &Principal,
        socket: &str,
        command: &Command,
    ) -> Result<Value, WireError> {
        let parent = command.session_id.as_deref().ok_or_else(unavailable)?;
        self.record(&principal.owner, parent).await?;
        let reply: ProposalReply = super::commands::parse(command)?;
        if reply.proposal_id.len() > 256 || reply.lease_id.len() > 256 {
            return Err(unavailable());
        }
        self.permission_bridge(parent)
            .await
            .ok_or_else(unavailable)?
            .proposal_reply(socket, reply)
            .await
    }
}
mod proposals;
#[cfg(test)]
mod tests;
