//! Interaction authority is independent of the one browser execution lease.
use super::*;

#[tokio::test]
async fn two_bound_participants_share_first_winner_without_browser_reexecution() {
    let (_directory, bridge, tool, binding) =
        setup_definition(interaction_definition(300_000)).await;
    let participant = bridge
        .bind(
            "socket_b",
            &BindRequest {
                client_id: "client_b".into(),
                catalog_digest: binding.catalog_digest,
            },
        )
        .await
        .unwrap();
    assert_eq!(participant.role, ClientRole::Participant);
    assert!(participant.can_interact);
    assert!(participant.participation_id.is_some());
    assert!(participant.lease_id.is_none());
    let task =
        tokio::spawn(async move { tool.execute(json!({}), context("participant-call")).await });
    let call = pending(&bridge).await;
    let left = ResultRequest {
        invocation_id: call.invocation_id.clone(),
        lease_id: String::new(),
        result: Some(json!({"answer":"left"})),
        error: None,
    };
    let right = ResultRequest {
        result: Some(json!({"answer":"right"})),
        ..left.clone()
    };
    let (a, b) = tokio::join!(
        bridge.result("socket_a", &left),
        bridge.result("socket_b", &right)
    );
    assert_eq!(a.unwrap(), b.unwrap());
    let settled = bridge.snapshot().await.calls[0].clone();
    assert!(settled.interaction);
    let winner = if settled.result.as_ref().unwrap()["answer"] == "left" {
        "socket_a"
    } else {
        "socket_b"
    };
    assert_eq!(settled.responder_socket.as_deref(), Some(winner));
    let answer = task.await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&answer).unwrap(),
        settled.result.unwrap()
    );
}

#[tokio::test]
async fn stale_unbind_does_not_revoke_current_reader_membership() {
    let (_directory, bridge, _tool, binding) = setup(true).await;
    let before = bridge
        .bind(
            "socket_a",
            &BindRequest {
                client_id: "client_a".into(),
                catalog_digest: binding.catalog_digest.clone(),
            },
        )
        .await
        .unwrap();
    assert!(!bridge.unbind("socket_a", "stale_lease").await.unwrap());
    let after = bridge
        .bind(
            "socket_a",
            &BindRequest {
                client_id: "client_a".into(),
                catalog_digest: binding.catalog_digest,
            },
        )
        .await
        .unwrap();
    assert_eq!(before.participation_id, after.participation_id);
    assert_eq!(before.lease_id, after.lease_id);
}

#[tokio::test]
async fn executor_takeover_requires_bound_idle_participant() {
    let (_directory, bridge, tool, binding) = setup(false).await;
    let request = BindRequest {
        client_id: "client_b".into(),
        catalog_digest: binding.catalog_digest,
    };
    assert!(bridge.takeover_idle("socket_b", &request).await.is_err());
    bridge.bind("socket_b", &request).await.unwrap();
    let task =
        tokio::spawn(async move { tool.execute(json!({}), context("effect-in-flight")).await });
    pending(&bridge).await;
    assert!(bridge.takeover_idle("socket_b", &request).await.is_err());
    bridge.settle_all("fixture cancellation").await.unwrap();
    let _ = task.await.unwrap();
    let next = bridge.takeover_idle("socket_b", &request).await.unwrap();
    assert_eq!(next.role, ClientRole::Executor);
    assert_ne!(next.lease_id, binding.lease_id);
}
