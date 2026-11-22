//! Deterministic phase boundaries: cancellation and dispatch cannot both win.
use super::*;
fn attempt() -> Attempt {
    let (result, _) = watch::channel(None);
    Attempt {
        id: uuid::Uuid::new_v4(),
        principal: Principal::for_test("owner"),
        params: Connect {
            project_id: "a".into(),
            url: "ws://127.0.0.1:8788/host".into(),
            token: Some("one-use".into()),
            directory: None,
            allow_legacy_binding: false,
            only_if_unattached: false,
        },
        deadline: Instant::now() + TTL,
        capability: Mutex::new(Some("a".repeat(64))),
        stop: CancellationToken::new(),
        phase_gate: Mutex::new(false),
        auth_sent: std::sync::atomic::AtomicBool::new(false),
        result,
    }
}
#[test]
fn cancellation_before_dispatch_certifies_fallback_and_fences_late_pair() {
    let attempt = attempt();
    attempt.fail("transport unavailable");
    assert_eq!(
        attempt
            .result
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .code,
        "host_outbound_unavailable"
    );
    assert!(attempt.begin_auth().is_err());
    assert!(!attempt.auth_sent.load(std::sync::atomic::Ordering::SeqCst));
}
#[test]
fn dispatch_before_cancellation_requires_fresh_pairing_and_keeps_first_result() {
    let attempt = attempt();
    attempt.begin_auth().unwrap();
    attempt.fail("transport closed after Pair");
    assert_eq!(
        attempt
            .result
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .code,
        "host_pairing_outcome_unknown"
    );
    attempt.complete(Ok(json!({"ready":true})));
    assert_eq!(
        attempt
            .result
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .code,
        "host_pairing_outcome_unknown"
    );
    assert!(attempt.begin_auth().is_err());
}
#[tokio::test]
async fn hello_commit_stall_outlives_expiry_without_false_failure_or_waiter_spin() {
    use futures_util::FutureExt;
    let mut attempt = attempt();
    attempt.begin_auth().unwrap();
    attempt.begin_commit().unwrap();
    // A deterministic persistence/publication stall crosses the bootstrap
    // deadline. The real waiter must stay pending until the commit owner replies.
    attempt.deadline = Instant::now();
    let mut waiting = Box::pin(attempt.wait_result());
    assert!((&mut waiting).now_or_never().is_none());
    assert!(!attempt.fail("cross-Project attempt pruning"));
    assert!(!attempt.stop.is_cancelled());
    assert!(attempt.result.borrow().is_none());
    attempt.complete(Err(WireError::new(
        "host_credentials_unavailable",
        "Persistence fixture failed",
    )));
    let error = waiting.await.unwrap_err();
    assert_eq!(error.code, "host_credentials_unavailable");
    assert_eq!(error.message, "Persistence fixture failed");
}
#[test]
fn cancellation_or_expiry_before_commit_prevents_publication_claim() {
    let attempt = attempt();
    attempt.begin_auth().unwrap();
    assert!(attempt.fail("Hello arrived after cancellation"));
    assert_eq!(
        attempt.begin_commit().unwrap_err().code,
        "host_pairing_outcome_unknown"
    );
    assert!(!*attempt.phase_gate.lock().unwrap());
}
#[test]
fn completed_commit_cannot_be_replaced_by_cleanup_failure() {
    let attempt = attempt();
    attempt.begin_auth().unwrap();
    attempt.begin_commit().unwrap();
    attempt.complete(Ok(json!({"ready":true})));
    assert!(!attempt.fail("expired attempt cleanup"));
    assert_eq!(
        attempt.result.borrow().as_ref().unwrap().as_ref().unwrap()["ready"],
        true
    );
}
#[test]
fn completed_precommit_failure_is_not_reported_as_a_new_cancellation() {
    let attempt = attempt();
    attempt.complete(Err(unavailable("handshake setup failed")));
    assert!(!attempt.fail("late cancellation"));
    assert!(!attempt.stop.is_cancelled());
    assert_eq!(
        attempt
            .result
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .message,
        "handshake setup failed"
    );
}

#[test]
fn expired_attempt_cannot_dispatch_authentication() {
    let mut attempt = attempt();
    attempt.deadline = Instant::now();
    assert_eq!(
        attempt.begin_auth().unwrap_err().code,
        "host_outbound_unavailable"
    );
    assert!(!attempt.auth_sent.load(std::sync::atomic::Ordering::SeqCst));
}
