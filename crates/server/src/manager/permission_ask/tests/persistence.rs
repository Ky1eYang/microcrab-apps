use super::*;

#[tokio::test]
async fn failed_outcome_ack_never_approves_and_close_releases_waiter() {
    let (root, bridge, lease) = setup().await;
    let mut subscription = bridge.hub.subscribe();
    let worker = bridge.clone();
    let pending = tokio::spawn(async move { worker.ask(&request("child", "storage-fault")).await });
    let row = event(&mut subscription, "permission.requested").await;
    let id = row["request"]["request_id"].as_str().unwrap();
    std::fs::create_dir(
        root.path()
            .join("sessions/parent/interaction_outcomes.json.tmp"),
    )
    .unwrap();
    let error = bridge
        .reply("socket_a", answer(id, &lease, ReplyDecision::Allow))
        .await
        .unwrap_err();
    assert_eq!(error.code, "io");
    assert!(!pending.is_finished());
    assert!(bridge.tools.decision("permission", id).is_none());
    bridge.close();
    assert!(matches!(
        pending.await.unwrap(),
        Err(PermissionError::Unavailable)
    ));
    assert_eq!(bridge.snapshot().await["requests"], json!([]));
    let terminal = event(&mut subscription, "permission.settled").await;
    assert_eq!(terminal["decision"], "deny");
    assert_eq!(terminal["durable"], false);
}
