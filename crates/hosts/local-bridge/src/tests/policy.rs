use super::*;

#[tokio::test]
async fn unrestricted_scope_changes_immutable_pin_without_changing_tool_linkage() {
    let fixture = tempfile::tempdir().unwrap();
    // System Temp can be reached through a Windows profile junction. Use the
    // same directory's canonical identity for the admitted ordinary-path case.
    let base = fixture.path().canonicalize().unwrap();
    let root = base.join("workspace");
    std::fs::create_dir(&root).unwrap();
    let outside = base.join("outside.txt");
    std::fs::write(&outside, "outside").unwrap();
    let (confined, _) = Bridge::open_with_options(&root, false, None).unwrap();
    let previous = confined.descriptor.clone();
    assert!(
        confined
            .host
            .read_file(outside.clone(), 1024, CancellationToken::new())
            .await
            .is_err()
    );
    drop(confined);
    let policy = BridgePolicy {
        unrestricted_fs: true,
        read_only: true,
        ..BridgePolicy::default()
    };
    let (bridge, _) = Bridge::open_with_policy(&root, false, None, policy).unwrap();
    assert_eq!(
        bridge
            .host
            .read_file(outside.clone(), 1024, CancellationToken::new())
            .await
            .unwrap(),
        b"outside"
    );
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        let has_reparse_ancestor = fixture
            .path()
            .ancestors()
            .any(|path| std::fs::symlink_metadata(path).unwrap().file_attributes() & 0x400 != 0);
        if has_reparse_ancestor {
            // Canonical access above succeeds, while the actual linked alias
            // remains rejected even with unrestricted filesystem scope.
            let error = bridge
                .host
                .read_file(
                    fixture.path().join("outside.txt"),
                    1024,
                    CancellationToken::new(),
                )
                .await
                .unwrap_err();
            assert_eq!(
                error.public_reason(),
                Some("target path contains a disallowed symlink or reparse point")
            );
        }
    }
    assert!(
        bridge
            .descriptor
            .info
            .description
            .as_deref()
            .unwrap()
            .contains("unrestricted")
    );
    assert!(!descriptor_matches_pin(&bridge.descriptor, &previous.descriptor_digest).unwrap());
    assert!(
        descriptor_matches_pin(&bridge.descriptor, &bridge.descriptor.descriptor_digest).unwrap()
    );
    // Scope does not create a tool/import, and read-only/launch gates still win.
    assert!(
        !bridge
            .descriptor
            .tool_capabilities
            .iter()
            .any(|name| name.contains("unrestricted"))
    );
    assert!(!bridge.policy.capability("write"));
    assert!(!bridge.policy.capability("process.spawn"));
    assert!(!bridge.policy.capability("shell.launch"));
    let private = root.join(".microcrab/host-bridge/identity.json");
    assert!(
        bridge
            .host
            .read_file(private, 4096, CancellationToken::new())
            .await
            .is_err()
    );
    drop(bridge);
    let (bridge, _) = Bridge::open_with_policy(
        &root,
        false,
        None,
        BridgePolicy {
            unrestricted_fs: true,
            ..BridgePolicy::default()
        },
    )
    .unwrap();
    assert_eq!(
        bridge.descriptor.tool_capabilities,
        previous.tool_capabilities
    );
    assert_eq!(
        bridge.descriptor.supported_primitives,
        previous.supported_primitives
    );
    assert!(!descriptor_matches_pin(&bridge.descriptor, &previous.descriptor_digest).unwrap());
    let pin = bridge.descriptor.descriptor_digest.clone();
    drop(bridge);
    let (restarted, _) = Bridge::open_with_policy(
        &root,
        false,
        None,
        BridgePolicy {
            unrestricted_fs: true,
            ..BridgePolicy::default()
        },
    )
    .unwrap();
    assert!(descriptor_matches_pin(&restarted.descriptor, &pin).unwrap());
}

#[tokio::test]
async fn service_selected_workspace_inherits_unrestricted_scope_and_external_private_guards() {
    let fixture = tempfile::tempdir().unwrap();
    let base = fixture.path().canonicalize().unwrap();
    let root = base.join("first");
    let second = base.join("second");
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(&second).unwrap();
    std::fs::write(root.join("public.txt"), "public").unwrap();
    let (service, _) = BridgeService::open_with_policy(
        &root,
        std::slice::from_ref(&second),
        false,
        None,
        BridgePolicy {
            unrestricted_fs: true,
            ..BridgePolicy::default()
        },
    )
    .unwrap();
    let (selected, _) = service
        .authenticate(WireFrame::Pair {
            directory: Some(second.to_string_lossy().into()),
            expected_host_id: None,
            expected_workspace_id: None,
            expected_descriptor_digest: None,
            expected_workspace_root: None,
            token: String::new(),
            scope: scope(),
        })
        .await
        .unwrap();
    assert!(selected.policy.unrestricted_fs);
    assert_eq!(
        selected
            .host
            .read_file(root.join("public.txt"), 1024, CancellationToken::new())
            .await
            .unwrap(),
        b"public"
    );
    assert!(
        selected
            .host
            .read_file(
                root.join(".microcrab/host-bridge/identity.json"),
                4096,
                CancellationToken::new()
            )
            .await
            .is_err()
    );
    assert!(
        service
            .default
            .host
            .read_file(
                second.join(".microcrab/host-bridge/identity.json"),
                4096,
                CancellationToken::new()
            )
            .await
            .is_err()
    );
    let third = base.join("unselected");
    std::fs::create_dir(&third).unwrap();
    assert!(
        service
            .authenticate(WireFrame::Pair {
                directory: Some(third.to_string_lossy().into()),
                expected_host_id: None,
                expected_workspace_id: None,
                expected_descriptor_digest: None,
                expected_workspace_root: None,
                token: String::new(),
                scope: scope()
            })
            .await
            .is_err()
    );
}

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
            expected_descriptor_digest: None,
            expected_workspace_root: None,
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
