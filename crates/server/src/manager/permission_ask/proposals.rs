//! Independent proposal confirmation and cancellation-safe durable adoption.
use super::*;
impl PermissionAsk {
    pub(super) async fn propose(
        &self,
        grant: FutureGrantProposal,
        adoption: Arc<dyn FutureGrantAdoption>,
    ) -> Result<(), PermissionError> {
        let id = uuid::Uuid::new_v4().to_string();
        let (sender, receiver) = oneshot::channel();
        let deadline = Instant::now() + PROPOSAL_TIMEOUT;
        let mut changes = self.tools.executor_changes();
        let lease = self
            .tools
            .with_executor(|executor| {
                let (socket, lease) = executor.ok_or(PermissionError::Unavailable)?;
                let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                if state.closed
                    || state.proposals.len() + state.proposal_committing.len() >= MAX_PENDING
                {
                    return Err(PermissionError::Unavailable);
                }
                let created = now();
                let proposal = Proposal {
                    proposal_id: id.clone(),
                    parent_session_id: self.parent.clone(),
                    grant,
                    created_at_ms: created,
                    expires_at_ms: created.saturating_add(PROPOSAL_TIMEOUT.as_millis() as u64),
                };
                state.proposals.insert(
                    id.clone(),
                    PendingProposal {
                        proposal: proposal.clone(),
                        socket: socket.into(),
                        lease: lease.into(),
                        deadline,
                        sender,
                        adoption,
                    },
                );
                self.hub.publish(
                    crate::manager::SessionRuntimeEvent::PermissionProposalRequested(
                        json!({"proposal":proposal}),
                    ),
                );
                Ok((socket.to_owned(), lease.to_owned()))
            })
            .await?;
        let _retire = ProposalRetire {
            bridge: self,
            id: id.clone(),
        };
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
        tokio::select! {biased;
            _=revoked=>{self.retire_proposal(&id,"declined","executor_disconnected");Err(PermissionError::Unavailable)},
            _=tokio::time::sleep_until(deadline)=>{self.retire_proposal(&id,"expired","confirmation deadline exceeded");Ok(())},
            _=receiver=>Ok(()),
        }
    }
    fn retire_proposal(&self, id: &str, status: &str, reason: &str) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(pending) = state.proposals.remove(id) {
            let result = json!({"proposal_id":id,"session_id":pending.proposal.grant.session_id,"status":status,"reason":reason,"remembered":false,"duplicate":false,"revision":null});
            self.hub.publish(
                crate::manager::SessionRuntimeEvent::PermissionProposalSettled(result.clone()),
            );
            state.proposal_receipts.push_back(ProposalReceipt {
                id: id.into(),
                socket: pending.socket,
                lease: pending.lease,
                accept: false,
                result,
            });
            while state.proposal_receipts.len() > MAX_SETTLED {
                state.proposal_receipts.pop_front();
            }
            let _ = pending.sender.send(false);
        }
    }
    pub(super) async fn proposal_reply(
        &self,
        socket: &str,
        reply: ProposalReply,
    ) -> Result<Value, WireError> {
        enum Accepted {
            Pending(PendingProposal),
            Duplicate(Value),
        }
        let accepted=self.tools.with_executor(|executor| {
            if executor!=Some((socket,reply.lease_id.as_str())) {return Err(WireError::new("permission_executor_required","Current executor lease required"));}
            let mut state=self.state.lock().unwrap_or_else(|e|e.into_inner());
            if let Some(receipt)=state.proposal_receipts.iter().chain(state.proposal_committing.values()).find(|r|r.id==reply.proposal_id&&r.socket==socket&&r.lease==reply.lease_id&&r.accept==reply.accept) {return Ok(Accepted::Duplicate(receipt.result.clone()));}
            let pending=state.proposals.get(&reply.proposal_id).ok_or_else(||WireError::new("permission_proposal_settled","Proposal is absent or settled"))?;
            if state.closed||pending.socket!=socket||pending.lease!=reply.lease_id||Instant::now()>=pending.deadline {return Err(WireError::new("permission_proposal_settled","Proposal expired, closed or belongs to a revoked executor"));}
            let pending=state.proposals.remove(&reply.proposal_id).ok_or_else(||WireError::new("permission_proposal_settled","Proposal already settled"))?;
            state.proposal_committing.insert(reply.proposal_id.clone(),ProposalReceipt {id:reply.proposal_id.clone(),socket:socket.into(),lease:reply.lease_id.clone(),accept:reply.accept,
                result:json!({"proposal_id":reply.proposal_id,"session_id":pending.proposal.grant.session_id,"status":"persisting","reason":"Future grant confirmation accepted; persistence pending","remembered":false,"revision":null,"duplicate":false})});
            Ok(Accepted::Pending(pending))
        }).await?;
        let pending = match accepted {
            Accepted::Duplicate(mut value) => {
                value["duplicate"] = json!(true);
                return Ok(value);
            }
            Accepted::Pending(pending) => pending,
        };
        let bridge = self.weak.upgrade().ok_or_else(unavailable)?;
        let socket = socket.to_owned();
        let (sender, receiver) = oneshot::channel();
        // A dropped socket waiter cannot abandon a possibly committed storage
        // write. The worker owns adoption, publication and reconnect receipt.
        tokio::spawn(async move {
            let (status, reason, revision) = if reply.accept {
                match pending.adoption.adopt().await {
                    Ok(snapshot) => (
                        "accepted",
                        String::from("Session conditional grant persisted"),
                        Some(snapshot.revision),
                    ),
                    Err(PermissionError::StalePolicy) => (
                        "stale",
                        String::from("Policy changed; a fresh confirmation is required"),
                        None,
                    ),
                    Err(error) => ("failed", error.to_string(), None),
                }
            } else {
                ("declined", String::from("User declined future grant"), None)
            };
            let result = json!({"proposal_id":reply.proposal_id,"session_id":pending.proposal.grant.session_id,"status":status,"reason":reason,"remembered":status=="accepted","revision":revision,"duplicate":false});
            {
                let mut state = bridge.state.lock().unwrap_or_else(|e| e.into_inner());
                state.proposal_committing.remove(&reply.proposal_id);
                state.proposal_receipts.push_back(ProposalReceipt {
                    id: reply.proposal_id,
                    socket,
                    lease: reply.lease_id,
                    accept: reply.accept,
                    result: result.clone(),
                });
                while state.proposal_receipts.len() > MAX_SETTLED {
                    state.proposal_receipts.pop_front();
                }
                bridge.hub.publish(
                    crate::manager::SessionRuntimeEvent::PermissionProposalSettled(result.clone()),
                );
            }
            let _ = pending.sender.send(status == "accepted");
            let _ = sender.send(result);
        });
        receiver.await.map_err(|_| {
            WireError::new(
                "permission_persistence_uncertain",
                "Grant adoption worker stopped before acknowledgement",
            )
        })
    }
}
struct ProposalRetire<'a> {
    bridge: &'a PermissionAsk,
    id: String,
}
impl Drop for ProposalRetire<'_> {
    fn drop(&mut self) {
        self.bridge
            .retire_proposal(&self.id, "declined", "confirmation_cancelled");
    }
}
