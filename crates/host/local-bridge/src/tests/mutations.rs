//! Bridge mutations use the same durable no-replay receipt as conditional writes.
use super::*;
use microcrab_host_api::{FileMutation, MoveRequest};

#[tokio::test]
async fn preflight_is_effect_free_and_duplicate_move_returns_original_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let (bridge, token) = Bridge::open(dir.path()).unwrap();
    assert!(
        bridge
            .descriptor
            .tool_capabilities
            .iter()
            .any(|cap| cap == "file_mutation")
    );
    std::fs::write(dir.path().join("source"), "before").unwrap();
    let (_, lease) = pair(&bridge, token).await;
    let mutation = FileMutation::Move(MoveRequest {
        path: "source".into(),
        destination: "destination".into(),
        expected_version: microcrab_host_protocol::sha256(b"before"),
        content: "after".into(),
    });
    let preflight = bridge
        .execute(
            "preflight".into(),
            lease.clone(),
            5000,
            HostRequest::PreflightMutations {
                requests: vec![mutation.clone()],
            },
            CancellationToken::new(),
        )
        .await
        .0
        .unwrap();
    assert_eq!(preflight[0]["move"]["path"], "source");
    assert!(!dir.path().join("destination").exists());
    let request = HostRequest::Mutate(mutation);
    let first = bridge
        .execute(
            "move".into(),
            lease.clone(),
            5000,
            request.clone(),
            CancellationToken::new(),
        )
        .await
        .0
        .unwrap();
    assert_eq!(first["source_removed"], true);
    assert_eq!(first["destination_written"], true);
    assert_eq!(first["version"], microcrab_host_protocol::sha256(b"after"));
    // Re-executing source removal would fail: returning this receipt proves
    // duplicate operation identity was settled before filesystem dispatch.
    let again = bridge
        .execute(
            "move".into(),
            lease,
            5000,
            request,
            CancellationToken::new(),
        )
        .await
        .0
        .unwrap();
    assert_eq!(first, again);
    assert_eq!(
        std::fs::read_to_string(dir.path().join("destination")).unwrap(),
        "after"
    );
    assert!(!dir.path().join("source").exists());
}

#[test]
fn read_only_bridge_rejects_mutation_preflight_and_hides_capability() {
    let policy = crate::BridgePolicy {
        read_only: true,
        ..Default::default()
    };
    let mutation = FileMutation::Delete(microcrab_host_api::DeleteRequest {
        path: "source".into(),
        expected_version: "a".repeat(64),
    });
    assert_eq!(
        policy
            .check(&HostRequest::Mutate(mutation.clone()))
            .unwrap_err()
            .code,
        ErrorCode::Denied
    );
    assert_eq!(
        policy
            .check(&HostRequest::PreflightMutations {
                requests: vec![mutation]
            })
            .unwrap_err()
            .code,
        ErrorCode::Denied
    );
    assert!(!policy.capability("file_mutation"));
}
