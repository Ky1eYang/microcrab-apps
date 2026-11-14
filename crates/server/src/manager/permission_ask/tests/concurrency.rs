//! Lease, timer and durable proposal races; authored, not run in this worktree.
use super::*;
fn attribution() -> AskAttribution {
    AskAttribution {
        controller_id: 7,
        owning_session_id: "child".into(),
    }
}
fn suggestion() -> ReviewResult {
    ReviewResult {
        action: Action::Allow,
        reason: "Safe fixture action".into(),
        future_grant: None,
    }
}
#[tokio::test(start_paused = true)]
async fn definite_review_before_original_expiry_gets_complete_five_second_window_once() {
    let (_root, bridge, lease) = setup().await;
    let mut subscription = bridge.hub.subscribe();
    let interaction = bridge
        .open(&request("child", "call"), &attribution())
        .await
        .unwrap();
    let row = event(&mut subscription, "permission.requested").await;
    let id = row["request"]["request_id"].as_str().unwrap();
    tokio::time::advance(Duration::from_secs(119)).await;
    let received = Instant::now();
    let deadline = interaction.review(&suggestion(), received).await.unwrap();
    assert_eq!(deadline - Instant::now(), Duration::from_secs(5));
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(
        interaction.review(&suggestion(), received).await.unwrap(),
        deadline
    );
    bridge
        .reply("socket_a", answer(id, &lease, ReplyDecision::Deny))
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(3)).await;
    let selected = interaction
        .settle(&Decision::new(Action::Allow, "safe", "review"))
        .await
        .unwrap();
    assert_eq!(selected.action, Action::Deny);
    assert_eq!(selected.reason, "user_once");
}
#[tokio::test(start_paused = true)]
async fn late_or_retired_review_cannot_revive_request_and_revoke_denies_auto_allow() {
    let (_root, bridge, _lease) = setup().await;
    let interaction = bridge
        .open(&request("child", "call"), &attribution())
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(120)).await;
    assert!(matches!(
        interaction.review(&suggestion(), Instant::now()).await,
        Err(PermissionError::Deadline)
    ));
    interaction
        .settle(&Decision::new(Action::Deny, "expired", "timeout"))
        .await
        .unwrap();
    assert!(
        interaction
            .review(&suggestion(), Instant::now() - Duration::from_secs(2))
            .await
            .is_err()
    );
    let next = bridge
        .open(&request("child", "next"), &attribution())
        .await
        .unwrap();
    let deadline = next.review(&suggestion(), Instant::now()).await.unwrap();
    bridge.tools.detach("socket_a").await.unwrap();
    tokio::time::sleep_until(deadline).await;
    assert!(matches!(
        next.settle(&Decision::new(Action::Allow, "safe", "review"))
            .await,
        Err(PermissionError::Unavailable)
    ));
    assert!(next.validate_dispatch().is_err());
}
#[tokio::test]
async fn wallclock_ui_expiry_does_not_override_monotonic_user_deadline() {
    let (_root, bridge, lease) = setup().await;
    let mut subscription = bridge.hub.subscribe();
    let interaction = bridge
        .open(&request("child", "call"), &attribution())
        .await
        .unwrap();
    let row = event(&mut subscription, "permission.requested").await;
    let id = row["request"]["request_id"].as_str().unwrap();
    bridge
        .state
        .lock()
        .unwrap()
        .pending
        .get_mut(id)
        .unwrap()
        .request
        .expires_at_ms = 0;
    bridge
        .reply("socket_a", answer(id, &lease, ReplyDecision::Allow))
        .await
        .unwrap();
    assert_eq!(
        interaction.user_reply().await.unwrap().action,
        Action::Allow
    );
    bridge.tools.detach("socket_a").await.unwrap();
    assert!(interaction.validate_dispatch().is_err());
}
struct Adoption {
    calls: AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    block: bool,
    fail: bool,
}
#[async_trait]
impl FutureGrantAdoption for Adoption {
    async fn adopt(&self) -> Result<PolicySnapshot, PermissionError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        if self.block {
            self.release.notified().await;
        }
        if self.fail {
            return Err(PermissionError::InvalidConfiguration(
                "fixture persistence rejected".into(),
            ));
        }
        Ok(PolicySnapshot {
            revision: 2,
            config: Default::default(),
        })
    }
}
fn proposal() -> FutureGrantProposal {
    FutureGrantProposal {
        session_id: "exact-child".into(),
        controller_id: 7,
        revision: 1,
        call_id: "call".into(),
        agent: "coding".into(),
        mode: "build".into(),
        tool: "read".into(),
        args: vec![microcrab_permission::ArgumentPredicate {
            pointer: "/path".into(),
            matcher: microcrab_permission::Matcher::Exact("src/main.rs".into()),
        }],
    }
}
fn adoption(block: bool, fail: bool) -> Arc<Adoption> {
    Arc::new(Adoption {
        calls: AtomicUsize::new(0),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        block,
        fail,
    })
}
#[tokio::test(start_paused = true)]
async fn independent_twenty_second_confirmation_defaults_decline_without_policy_write() {
    let (_root, bridge, _lease) = setup().await;
    let mut subscription = bridge.hub.subscribe();
    let adopter = adoption(false, false);
    let cloned = bridge.clone();
    let stored = adopter.clone();
    let task = tokio::spawn(async move { cloned.propose(proposal(), stored).await });
    let row = event(&mut subscription, "permission.proposal.requested").await;
    assert_eq!(row["proposal"]["session_id"], "exact-child");
    assert_eq!(bridge.snapshot().await["requests"], json!([]));
    assert_eq!(adopter.calls.load(Ordering::SeqCst), 0);
    tokio::time::advance(Duration::from_secs(20)).await;
    task.await.unwrap().unwrap();
    assert_eq!(adopter.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        event(&mut subscription, "permission.proposal.settled").await["status"],
        "expired"
    );
}
#[tokio::test]
async fn proposal_persistence_survives_dropped_socket_waiter_and_reconnect_observes_receipt() {
    let (_root, bridge, lease) = setup().await;
    let mut subscription = bridge.hub.subscribe();
    let adopter = adoption(true, false);
    let cloned = bridge.clone();
    let stored = adopter.clone();
    let proposal_task = tokio::spawn(async move { cloned.propose(proposal(), stored).await });
    let row = event(&mut subscription, "permission.proposal.requested").await;
    let id = row["proposal"]["proposal_id"].as_str().unwrap().to_owned();
    let cloned = bridge.clone();
    let reply_id = id.clone();
    let reply_lease = lease.clone();
    let waiter = tokio::spawn(async move {
        cloned
            .proposal_reply(
                "socket_a",
                ProposalReply {
                    proposal_id: reply_id,
                    lease_id: reply_lease,
                    accept: true,
                },
            )
            .await
    });
    adopter.entered.notified().await;
    waiter.abort();
    let _ = waiter.await;
    assert_eq!(
        bridge.snapshot().await["proposal_results"][0]["status"],
        "persisting"
    );
    let duplicate = bridge
        .proposal_reply(
            "socket_a",
            ProposalReply {
                proposal_id: id.clone(),
                lease_id: lease.clone(),
                accept: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(duplicate["status"], "persisting");
    assert_eq!(duplicate["duplicate"], true);
    adopter.release.notify_one();
    let settled = event(&mut subscription, "permission.proposal.settled").await;
    assert_eq!(settled["status"], "accepted");
    assert_eq!(settled["remembered"], true);
    proposal_task.await.unwrap().unwrap();
    assert_eq!(adopter.calls.load(Ordering::SeqCst), 1);
    let duplicate = bridge
        .proposal_reply(
            "socket_a",
            ProposalReply {
                proposal_id: id,
                lease_id: lease,
                accept: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(duplicate["status"], "accepted");
    assert_eq!(duplicate["duplicate"], true);
}
#[tokio::test]
async fn failed_proposal_persistence_never_reports_remembered() {
    let (_root, bridge, lease) = setup().await;
    let mut subscription = bridge.hub.subscribe();
    let adopter = adoption(false, true);
    let cloned = bridge.clone();
    let task = tokio::spawn(async move { cloned.propose(proposal(), adopter).await });
    let row = event(&mut subscription, "permission.proposal.requested").await;
    let receipt = bridge
        .proposal_reply(
            "socket_a",
            ProposalReply {
                proposal_id: row["proposal"]["proposal_id"].as_str().unwrap().into(),
                lease_id: lease,
                accept: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(receipt["status"], "failed");
    assert_eq!(receipt["remembered"], false);
    assert!(
        receipt["reason"]
            .as_str()
            .unwrap()
            .contains("fixture persistence rejected")
    );
    task.await.unwrap().unwrap();
}
