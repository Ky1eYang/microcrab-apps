use super::*;
use microcrab_host_api::{ErrorCategory, HostError, ShellRequest, ToolError};

#[tokio::test]
async fn read_only_rejects_effects_before_receipts_but_keeps_lease_and_reads() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("visible.txt"), "still readable").unwrap();
    let (bridge, token) = Bridge::open_with_policy(
        dir.path(),
        true,
        None,
        BridgePolicy {
            read_only: true,
            ..BridgePolicy::default()
        },
    )
    .unwrap();
    let (_, lease) = pair(&bridge, token.unwrap()).await;
    for method in [
        HostRequest::Write(WriteRequest {
            path: "blocked.txt".into(),
            content: "no".into(),
            precondition: WritePrecondition::Missing,
        }),
        HostRequest::ShellLaunch(ShellRequest {
            command: "echo blocked".into(),
            cwd: None,
            output_bytes: 4096,
        }),
    ] {
        let error = bridge
            .execute(
                Uuid::new_v4().to_string(),
                lease.clone(),
                5000,
                method,
                CancellationToken::new(),
            )
            .await
            .0
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::Denied);
        assert!(error.message.contains("bridge"));
    }
    assert!(!dir.path().join("blocked.txt").exists());
    assert_eq!(bridge.receipts.lock().await.len(), 0);
    assert!(bridge.check_lease(&lease).await.is_ok());
    assert!(
        bridge
            .execute(
                "read-only-read".into(),
                lease,
                5000,
                HostRequest::ReadFile {
                    path: "visible.txt".into(),
                    max_bytes: 1024
                },
                CancellationToken::new()
            )
            .await
            .0
            .is_ok()
    );
    for capability in ["write", "cache_point", "shell", "process"] {
        assert!(
            !bridge
                .descriptor
                .tool_capabilities
                .iter()
                .any(|item| item == capability)
        );
    }
    assert!(
        !bridge
            .descriptor
            .supported_primitives
            .iter()
            .any(|item| item == "shell.launch" || item == "process.spawn")
    );
}

#[tokio::test]
async fn service_selected_workspace_inherits_startup_restrictions() {
    let dir = tempfile::tempdir().unwrap();
    let child = dir.path().join("child");
    std::fs::create_dir(&child).unwrap();
    let (service, _) = BridgeService::open_with_policy(
        dir.path(),
        &[],
        true,
        Some("fixture-token".into()),
        BridgePolicy {
            read_only: true,
            ..BridgePolicy::default()
        },
    )
    .unwrap();
    let (selected, _) = service
        .authenticate(WireFrame::Pair {
            directory: Some(child.to_string_lossy().into()),
            expected_host_id: None,
            expected_workspace_id: None,
            token: "fixture-token".into(),
            scope: scope(),
        })
        .await
        .unwrap();
    assert!(selected.policy.read_only);
    assert!(
        !selected
            .descriptor
            .tool_capabilities
            .iter()
            .any(|name| name == "write")
    );
    assert!(
        selected
            .policy
            .check(&HostRequest::Write(WriteRequest {
                path: "bypass.txt".into(),
                content: "no".into(),
                precondition: WritePrecondition::Missing,
            }))
            .is_err()
    );
}

#[test]
fn io_and_unknown_outcome_preserve_original_failure_feedback() {
    let error = crate::execution::tool_error(ToolError::Io(std::io::Error::other(
        "custom shell pipe reader failed: broken transport at owned stdout",
    )));
    assert_eq!(error.code, ErrorCode::Io);
    assert!(
        error
            .message
            .contains("custom shell pipe reader failed: broken transport at owned stdout")
    );
    assert!(error.message.contains("kind=Other"));
    let response = crate::execution::unknown_outcome(&error);
    assert_eq!(response.code, ErrorCode::OutcomeUnknown);
    assert!(response.message.contains("after Io:"));
    assert!(response.message.contains(&error.message));
    assert!(response.message.contains("effects may have occurred"));
    assert!(response.message.contains("do not replay automatically"));
    let cancelled = crate::execution::unknown_outcome(&WireError::new(
        ErrorCode::Cancelled,
        "shell write cancelled before owner acknowledgement",
    ));
    assert_eq!(cancelled.code, ErrorCode::OutcomeUnknown);
    assert!(
        cancelled
            .message
            .contains("after Cancelled: shell write cancelled before owner acknowledgement")
    );
}

#[test]
fn launch_controls_are_independent_and_failures_keep_actionable_detail() {
    let no_shell = BridgePolicy {
        no_shell: true,
        ..BridgePolicy::default()
    };
    assert!(!no_shell.capability("shell"));
    assert!(no_shell.capability("process.spawn"));
    assert!(no_shell.capability("write"));
    let no_process = BridgePolicy {
        no_process: true,
        ..BridgePolicy::default()
    };
    assert!(no_process.capability("shell.launch"));
    assert!(!no_process.capability("process"));
    let error = crate::execution::host_error(HostError::new(
        ErrorCategory::Cancelled,
        "shell drain cancelled by caller",
    ));
    assert_eq!(error.code, ErrorCode::Cancelled);
    assert_eq!(error.message, "shell drain cancelled by caller");
    let error = crate::execution::tool_error(ToolError::PathOutsideRoot);
    assert_eq!(error.code, ErrorCode::Denied);
    assert!(error.message.contains("outside the configured root"));
    let error = crate::execution::tool_error(ToolError::OperationLimit(
        "process admission capacity exhausted".into(),
    ));
    assert_eq!(error.code, ErrorCode::Limit);
    assert!(error.message.contains("admission capacity"));
}
