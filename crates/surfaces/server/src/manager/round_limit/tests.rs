//! Questions outlive sockets, while each answer belongs to one current lease.
use super::super::client_tools::BindRequest;
use super::*;
use std::time::Duration;

fn invocation(session: &str) -> RoundLimitRequest {
    RoundLimitRequest {
        session_id: session.into(),
        epoch: 3,
        turn_id: 7,
        kind: microcrab_session::budget::ConfirmationKind::Rounds,
        used: 64,
        block: 64,
    }
}
async fn setup(bound: bool) -> (tempfile::TempDir, Arc<RoundLimit>, Option<String>) {
    let root = tempfile::tempdir().unwrap();
    let tools = ClientTools::open(root.path(), "parent").await.unwrap();
    tools.register(vec![], 0).await.unwrap();
    let bridge = RoundLimit::new("parent".into(), tools, Arc::new(Hub::new()));
    let lease = if bound {
        Some(bind(&bridge, "socket_a", "client_a").await)
    } else {
        None
    };
    (root, bridge, lease)
}
async fn bind(bridge: &RoundLimit, socket: &str, client: &str) -> String {
    bridge
        .tools
        .bind(
            socket,
            &BindRequest {
                client_id: client.into(),
                catalog_digest: bridge.tools.snapshot().await.catalog_digest,
            },
        )
        .await
        .unwrap()
        .lease_id
        .unwrap()
}
async fn event(subscription: &mut super::super::hub::Subscription, name: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let item = subscription.receiver.recv().await.unwrap();
            if serde_json::to_value(&item.event).unwrap()["event"] == name {
                return item.event.payload().clone();
            }
        }
    })
    .await
    .expect("expected round confirmation event")
}
fn answer(id: &str, lease: &str, decision: RoundLimitDecision) -> Reply {
    Reply {
        request_id: id.into(),
        lease_id: lease.into(),
        decision,
    }
}
#[tokio::test]
async fn parent_and_child_answers_are_once_only_and_executor_fenced() {
    let (_root, bridge, lease) = setup(true).await;
    let lease = lease.unwrap();
    let mut subscription = bridge.hub.subscribe();
    for (session, decision) in [
        ("parent", RoundLimitDecision::Continue),
        ("child_session", RoundLimitDecision::Stop),
    ] {
        let worker = bridge.clone();
        let pending = tokio::spawn(async move {
            worker
                .request(invocation(session), CancellationToken::new())
                .await
        });
        let row = event(&mut subscription, "round_limit.requested").await;
        assert_eq!(row["request"]["parent_session_id"], "parent");
        assert_eq!(row["request"]["session_id"], session);
        assert_eq!(row["request"]["block"], 64);
        assert_eq!(row["request"]["kind"], "rounds");
        let id = row["request"]["request_id"].as_str().unwrap();
        assert!(
            bridge
                .reply("observer", answer(id, &lease, decision))
                .await
                .is_err()
        );
        assert_eq!(
            bridge.snapshot().await["requests"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            bridge
                .reply("socket_a", answer(id, &lease, decision))
                .await
                .unwrap()["duplicate"],
            false
        );
        let result = pending.await.unwrap().unwrap();
        assert_eq!(result, decision);
        assert_eq!(
            bridge
                .reply("socket_a", answer(id, &lease, decision))
                .await
                .unwrap()["duplicate"],
            true
        );
        let conflicting = if decision == RoundLimitDecision::Continue {
            RoundLimitDecision::Stop
        } else {
            RoundLimitDecision::Continue
        };
        let lost = bridge
            .reply("socket_a", answer(id, &lease, conflicting))
            .await
            .unwrap();
        assert_eq!(lost["decision"], serde_json::to_value(decision).unwrap());
        assert_eq!(
            event(&mut subscription, "round_limit.settled").await["request_id"],
            id
        );
        assert_eq!(bridge.snapshot().await["requests"], json!([]));
    }
}
#[tokio::test]
async fn missing_executor_does_not_hide_questions_and_reconnect_keeps_identity() {
    let (_root, bridge, _) = setup(false).await;
    let mut subscription = bridge.hub.subscribe();
    let worker = bridge.clone();
    let pending = tokio::spawn(async move {
        worker
            .request(invocation("parent"), CancellationToken::new())
            .await
    });
    let first = event(&mut subscription, "round_limit.requested").await;
    let id = first["request"]["request_id"].as_str().unwrap();
    let old = bind(&bridge, "socket_a", "client_a").await;
    bridge.tools.detach("socket_a").await.unwrap();
    let new = bind(&bridge, "socket_b", "client_b").await;
    assert!(
        bridge
            .reply("socket_a", answer(id, &old, RoundLimitDecision::Continue))
            .await
            .is_err()
    );
    bridge
        .reply("socket_b", answer(id, &new, RoundLimitDecision::Continue))
        .await
        .unwrap();
    assert_eq!(
        pending.await.unwrap().unwrap(),
        RoundLimitDecision::Continue
    );
}
#[tokio::test]
async fn cancellation_drop_and_close_retire_without_approving_or_leaking() {
    let (_root, bridge, lease) = setup(true).await;
    let lease = lease.unwrap();
    let mut subscription = bridge.hub.subscribe();
    let cancellation = CancellationToken::new();
    let token = cancellation.clone();
    let worker = bridge.clone();
    let pending = tokio::spawn(async move { worker.request(invocation("parent"), token).await });
    let row = event(&mut subscription, "round_limit.requested").await;
    let id = row["request"]["request_id"].as_str().unwrap();
    cancellation.cancel();
    assert!(matches!(pending.await.unwrap(), Err(Error::Cancelled)));
    assert!(event(&mut subscription, "round_limit.settled").await["decision"].is_null());
    assert_eq!(bridge.snapshot().await["requests"], json!([]));
    assert!(
        bridge
            .reply("socket_a", answer(id, &lease, RoundLimitDecision::Continue))
            .await
            .unwrap()["decision"]
            .is_null()
    );
    let worker = bridge.clone();
    let dropped = tokio::spawn(async move {
        worker
            .request(invocation("child"), CancellationToken::new())
            .await
    });
    event(&mut subscription, "round_limit.requested").await;
    dropped.abort();
    assert!(dropped.await.unwrap_err().is_cancelled());
    event(&mut subscription, "round_limit.settled").await;
    assert_eq!(bridge.snapshot().await["requests"], json!([]));
    bridge.tools.detach("socket_a").await.unwrap();
    let worker = bridge.clone();
    let pending = tokio::spawn(async move {
        worker
            .request(invocation("parent"), CancellationToken::new())
            .await
    });
    tokio::task::yield_now().await;
    bridge.close();
    assert!(matches!(pending.await.unwrap(), Err(Error::Cancelled)));
}
