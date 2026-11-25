use super::*;
mod mutations;
mod policy;
mod processes;
use microcrab_host_api::{WritePrecondition, WriteRequest};
use serde_json::json;
use tokio_util::sync::CancellationToken;
fn scope() -> BindingScope {
    BindingScope::project("backend".into(), "binding".into(), "project".into())
}
async fn pair(bridge: &Bridge, token: String) -> (String, String) {
    match bridge
        .authenticate(WireFrame::Pair {
            directory: None,
            expected_host_id: None,
            expected_workspace_id: None,
            expected_descriptor_digest: None,
            expected_workspace_root: None,
            token,
            scope: scope(),
        })
        .await
        .expect("valid pair")
    {
        WireFrame::Hello {
            credential,
            lease_generation,
            ..
        } => (credential, lease_generation),
        _ => panic!("expected hello"),
    }
}
#[tokio::test]
async fn one_controller_and_project_scope_are_fenced() {
    let dir = tempfile::tempdir().expect("fixture");
    let (bridge, token) = Bridge::open(dir.path()).expect("open bridge");
    assert!(Bridge::open(dir.path()).is_err());
    assert!(
        bridge
            .descriptor
            .tool_capabilities
            .iter()
            .all(|name| !name.starts_with("job_") && name != "start_job")
    );
    assert!(
        bridge
            .descriptor
            .tool_capabilities
            .contains(&"shell".to_string())
    );
    assert_eq!(
        bridge.descriptor.info.capabilities,
        bridge.descriptor.tool_capabilities
    );
    let (credential, first) = pair(&bridge, token.clone()).await;
    assert!(
        bridge
            .authenticate(WireFrame::Pair {
                directory: None,
                expected_host_id: None,
                expected_workspace_id: None,
                expected_descriptor_digest: None,
                expected_workspace_root: None,
                token,
                scope: scope()
            })
            .await
            .is_err()
    );
    let second = bridge
        .authenticate(WireFrame::Resume {
            directory: None,
            expected_host_id: None,
            expected_workspace_id: None,
            expected_descriptor_digest: None,
            expected_workspace_root: None,
            credential: credential.clone(),
            scope: scope(),
        })
        .await
        .expect("resume");
    assert!(bridge.check_lease(&first).await.is_err());
    assert!(matches!(second, WireFrame::Hello { .. }));
    let other = BindingScope::project("backend".into(), "binding".into(), "other".into());
    assert!(
        bridge
            .authenticate(WireFrame::Resume {
                directory: None,
                expected_host_id: None,
                expected_workspace_id: None,
                expected_descriptor_digest: None,
                expected_workspace_root: None,
                credential,
                scope: other
            })
            .await
            .is_err()
    );
}
#[tokio::test]
async fn duplicate_effects_conflict() {
    let dir = tempfile::tempdir().expect("fixture");
    let (bridge, token) = Bridge::open(dir.path()).expect("open bridge");
    let (_, lease) = pair(&bridge, token).await;
    let write = HostRequest::Write(WriteRequest {
        path: "a.txt".into(),
        content: "one".into(),
        precondition: WritePrecondition::Missing,
    });
    let first = bridge
        .execute(
            "effect".into(),
            lease.clone(),
            5000,
            write.clone(),
            CancellationToken::new(),
        )
        .await
        .0
        .expect("write");
    let again = bridge
        .execute(
            "effect".into(),
            lease.clone(),
            5000,
            write,
            CancellationToken::new(),
        )
        .await
        .0
        .expect("known receipt");
    assert_eq!(first, again);
    let conflict = HostRequest::Write(WriteRequest {
        path: "a.txt".into(),
        content: "two".into(),
        precondition: WritePrecondition::Missing,
    });
    assert_eq!(
        bridge
            .execute(
                "effect".into(),
                lease.clone(),
                5000,
                conflict,
                CancellationToken::new()
            )
            .await
            .0
            .expect_err("id conflict")
            .code,
        ErrorCode::OperationConflict
    );
}
#[tokio::test]
async fn expired_capabilities_and_restarted_binding_remain_scoped() {
    let dir = tempfile::tempdir().expect("fixture");
    let (bridge, token) = Bridge::open(dir.path()).expect("open");
    let identity = bridge.descriptor.clone();
    let (credential, _) = pair(&bridge, token).await;
    bridge.authority.lock().await.credential_deadline = Instant::now();
    assert!(
        bridge
            .authenticate(WireFrame::Resume {
                directory: None,
                expected_host_id: None,
                expected_workspace_id: None,
                expected_descriptor_digest: None,
                expected_workspace_root: None,
                credential,
                scope: scope()
            })
            .await
            .is_err()
    );
    drop(bridge);
    let (restarted, token) = Bridge::open(dir.path()).expect("restart");
    assert_eq!(identity.host_id, restarted.descriptor.host_id);
    assert_eq!(identity.workspace_id, restarted.descriptor.workspace_id);
    assert_eq!(
        identity.descriptor_digest,
        restarted.descriptor.descriptor_digest
    );
    assert_ne!(
        identity.bridge_instance_id,
        restarted.descriptor.bridge_instance_id
    );
    let other = BindingScope::project("other-backend".into(), "binding".into(), "project".into());
    let hello = restarted
        .authenticate(WireFrame::Pair {
            directory: None,
            expected_host_id: Some(identity.host_id),
            expected_workspace_id: Some(identity.workspace_id),
            expected_descriptor_digest: None,
            expected_workspace_root: None,
            token,
            scope: other.clone(),
        })
        .await
        .expect("fresh pairing permits another backend registration");
    let WireFrame::Hello {
        lease_generation, ..
    } = hello
    else {
        panic!("hello")
    };
    assert_eq!(
        restarted.check_lease(&lease_generation).await.unwrap(),
        other
    );
    assert!(!restarted.state_dir.join("binding.json").exists());
    assert!(!restarted.state_dir.join("binding-host.json").exists());
}
#[tokio::test]
async fn pairing_requires_the_exact_token_and_consumes_only_generated_tokens() {
    for custom_token in [None, Some("custom token".to_owned())] {
        let dir = tempfile::tempdir().unwrap();
        let (bridge, token) =
            Bridge::open_with_options(dir.path(), true, custom_token.clone()).unwrap();
        let token = token.unwrap();
        let request = |token| WireFrame::Pair {
            directory: None,
            expected_host_id: None,
            expected_workspace_id: None,
            expected_descriptor_digest: None,
            expected_workspace_root: None,
            token,
            scope: scope(),
        };
        for wrong in [String::new(), format!("{token} ")] {
            assert_eq!(
                bridge.authenticate(request(wrong)).await.unwrap_err().code,
                ErrorCode::PairingDenied
            );
        }
        assert!(bridge.authority.lock().await.credential.is_none());

        let (credential, first) = pair(&bridge, token.clone()).await;
        if custom_token.is_some() {
            let (next_credential, second) = pair(&bridge, token).await;
            assert_ne!(credential, next_credential);
            assert!(bridge.check_lease(&first).await.is_err());
            assert_eq!(bridge.check_lease(&second).await.unwrap(), scope());
        } else {
            assert_eq!(
                bridge.authenticate(request(token)).await.unwrap_err().code,
                ErrorCode::PairingDenied
            );
            assert_eq!(bridge.check_lease(&first).await.unwrap(), scope());
        }
    }
}
#[tokio::test]
async fn durable_unknown_blocks_new_mutations() {
    let dir = tempfile::tempdir().expect("fixture");
    let (bridge, token) = Bridge::open(dir.path()).expect("open");
    let (_, lease) = pair(&bridge, token).await;
    let record = OperationReceipt {
        operation_id: "unknown".into(),
        payload_digest: "a".repeat(64),
        scope: scope(),
        state: ReceiptState::Intent,
    };
    bridge.receipts.lock().await.intent(&record).unwrap();
    let write = HostRequest::Write(WriteRequest {
        path: "file".into(),
        content: "bytes".into(),
        precondition: WritePrecondition::Missing,
    });
    assert_eq!(
        bridge
            .execute(
                "new".into(),
                lease.clone(),
                5000,
                write,
                CancellationToken::new()
            )
            .await
            .0
            .expect_err("fenced")
            .code,
        ErrorCode::OutcomeUnknown
    );
    let shell = bridge
        .execute(
            "new-shell".into(),
            lease.clone(),
            5000,
            HostRequest::ShellLaunch(microcrab_host_api::ShellRequest {
                command: "echo must-not-execute".into(),
                cwd: None,
                output_bytes: 4096,
            }),
            CancellationToken::new(),
        )
        .await
        .0
        .expect_err("unknown effects must fence shell execution");
    assert_eq!(shell.code, ErrorCode::OutcomeUnknown);
    let recovery = bridge
        .execute(
            "recovery".into(),
            lease,
            5000,
            HostRequest::Recovery,
            CancellationToken::new(),
        )
        .await
        .0
        .expect("recovery");
    assert_eq!(recovery.as_array().expect("records").len(), 1);
    assert!(!dir.path().join("file").exists());
}

#[tokio::test]
async fn selected_cache_point_is_chunked_and_never_scans_unrelated_files() {
    let dir = tempfile::tempdir().unwrap();
    let (bridge, token) = Bridge::open(dir.path()).unwrap();
    let (_, lease) = pair(&bridge, token).await;
    std::fs::File::create(dir.path().join("unrelated"))
        .unwrap()
        .set_len(128 * 1024 * 1024)
        .unwrap();
    let bytes = vec![255; 32768];
    std::fs::write(dir.path().join("chosen"), &bytes).unwrap();
    let (value, chunks) = bridge
        .execute(
            "capture".into(),
            lease,
            5000,
            HostRequest::CachePoint(microcrab_host_api::CachePointRequest {
                path: "chosen".into(),
                max_bytes: 32768,
            }),
            CancellationToken::new(),
        )
        .await;
    let reference: TransferRef = serde_json::from_value(value.unwrap()).unwrap();
    assert_eq!(reference.kind, "json");
    let mut payload = Vec::new();
    let count = chunks.len();
    for (index, frame) in chunks.into_iter().enumerate() {
        let WireFrame::TransferChunk {
            transfer_id,
            sequence,
            data,
            eof,
        } = frame
        else {
            panic!("chunk expected")
        };
        assert_eq!(transfer_id, reference.transfer_id);
        assert_eq!(sequence as usize, index);
        assert_eq!(eof, index + 1 == count);
        payload.extend(data);
    }
    assert_eq!(payload.len() as u64, reference.byte_length);
    assert_eq!(sha256(&payload), reference.sha256);
    let point: microcrab_host_api::FileCachePoint = serde_json::from_slice(&payload).unwrap();
    assert_eq!(point.path, "chosen");
    assert_eq!(point.data.as_ref(), Some(&bytes));
    assert_eq!(point.version, Some(sha256(&bytes)));
}

#[tokio::test]
async fn cancelled_request_does_not_wait_behind_another_admission() {
    let dir = tempfile::tempdir().unwrap();
    let (bridge, token) = Bridge::open(dir.path()).unwrap();
    let (_, lease) = pair(&bridge, token).await;
    let _occupied = bridge.admission.lock().await;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        bridge.execute(
            "cancelled".into(),
            lease,
            5000,
            HostRequest::Write(WriteRequest {
                path: "never-published".into(),
                content: "cancelled".into(),
                precondition: WritePrecondition::Missing,
            }),
            cancel,
        ),
    )
    .await
    .unwrap();
    assert_eq!(result.0.unwrap_err().code, ErrorCode::Cancelled);
}

#[tokio::test]
async fn selected_reads_remain_live_while_mutation_admission_and_transfer_are_busy() {
    let dir = tempfile::tempdir().unwrap();
    let (bridge, token) = Bridge::open(dir.path()).unwrap();
    let (_, lease) = pair(&bridge, token).await;
    std::fs::write(dir.path().join("chosen"), b"fresh").unwrap();
    // These are the exact fences held by a long effect and a slow transfer.
    // Read-only operations must not join either admission queue.
    let _effect = bridge.admission.lock().await;
    let _transfer = bridge.egress.lock().await;
    let (result, _) = tokio::time::timeout(
        Duration::from_secs(2),
        bridge.execute(
            "point".into(),
            lease.clone(),
            5000,
            HostRequest::CachePoint(microcrab_host_api::CachePointRequest {
                path: "chosen".into(),
                max_bytes: 16,
            }),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("selected content evidence must stay live");
    let point: microcrab_host_api::FileCachePoint =
        serde_json::from_value(result.unwrap()).unwrap();
    assert_eq!(point.data, Some(b"fresh".to_vec()));
    let (result, _) = tokio::time::timeout(
        Duration::from_secs(2),
        bridge.execute(
            "normalize".into(),
            lease,
            5000,
            HostRequest::Normalize {
                path: "chosen".into(),
            },
            CancellationToken::new(),
        ),
    )
    .await
    .expect("path admission must stay live");
    assert_eq!(result.unwrap(), json!("chosen"));
}

fn selected_pair(directory: Option<String>, project: &str) -> WireFrame {
    let selected = BindingScope::project("backend".into(), "binding".into(), project.into());
    WireFrame::Pair {
        token: String::new(),
        scope: selected,
        directory,
        expected_host_id: None,
        expected_workspace_id: None,
        expected_descriptor_digest: None,
        expected_workspace_root: None,
    }
}

#[tokio::test]
async fn device_service_scopes_two_directories_and_restart_identity() {
    let base = tempfile::tempdir().unwrap();
    let a = base.path().join("a");
    let b = base.path().join("b");
    std::fs::create_dir_all(&a).unwrap();
    std::fs::create_dir_all(&b).unwrap();
    let (service, _) = BridgeService::open(&a, &[base.path().to_owned()], false, None).unwrap();
    let (wa, ha) = service
        .authenticate(selected_pair(None, "a"))
        .await
        .unwrap();
    let (wb, hb) = service
        .authenticate(selected_pair(Some(b.to_string_lossy().into()), "b"))
        .await
        .unwrap();
    let WireFrame::Hello {
        descriptor: da,
        lease_generation: ga,
        ..
    } = ha
    else {
        panic!("hello")
    };
    let WireFrame::Hello {
        descriptor: db,
        lease_generation: gb,
        ..
    } = hb
    else {
        panic!("hello")
    };
    assert_eq!(da.host_id, db.host_id);
    assert_ne!(da.workspace_id, db.workspace_id);
    assert!(wa.check_lease(&ga).await.is_ok());
    assert!(wb.check_lease(&gb).await.is_ok());
    let write = HostRequest::Write(WriteRequest {
        path: "same".into(),
        content: "a".into(),
        precondition: WritePrecondition::Missing,
    });
    wa.execute(
        "same-id".into(),
        ga,
        5000,
        write.clone(),
        CancellationToken::new(),
    )
    .await
    .0
    .unwrap();
    wb.execute("same-id".into(), gb, 5000, write, CancellationToken::new())
        .await
        .0
        .unwrap();
    assert_eq!(
        wa.receipts
            .lock()
            .await
            .fence("same-id")
            .unwrap()
            .scope
            .project_id()
            .unwrap(),
        "a"
    );
    assert_eq!(
        wb.receipts
            .lock()
            .await
            .fence("same-id")
            .unwrap()
            .scope
            .project_id()
            .unwrap(),
        "b"
    );
    drop(wa);
    drop(wb);
    drop(service);
    let (restarted, _) = BridgeService::open(&b, &[base.path().to_owned()], false, None).unwrap();
    assert_eq!(restarted.default.descriptor.host_id, da.host_id);
    assert_eq!(restarted.default.descriptor.workspace_id, db.workspace_id);
}

#[tokio::test]
async fn directory_escape_and_wrong_workspace_pin_create_no_private_state() {
    let allowed = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let (service, _) = BridgeService::open(allowed.path(), &[], false, None).unwrap();
    let denied = service
        .authenticate(selected_pair(
            Some(outside.path().to_string_lossy().into()),
            "outside",
        ))
        .await;
    assert!(matches!(
        denied,
        Err(WireError {
            code: ErrorCode::WorkspaceDenied,
            ..
        })
    ));
    assert!(!outside.path().join(".microcrab").exists());
    let child = allowed.path().join("child");
    std::fs::create_dir(&child).unwrap();
    let WireFrame::Pair {
        token,
        scope,
        directory,
        ..
    } = selected_pair(Some(child.to_string_lossy().into()), "child")
    else {
        unreachable!()
    };
    let denied = service
        .authenticate(WireFrame::Pair {
            token,
            scope,
            directory,
            expected_host_id: None,
            expected_workspace_id: Some("foreign".into()),
            expected_descriptor_digest: None,
            expected_workspace_root: None,
        })
        .await;
    assert!(matches!(
        denied,
        Err(WireError {
            code: ErrorCode::BindingMismatch,
            ..
        })
    ));
    assert!(!child.join(".microcrab").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn canonical_directory_symlink_cannot_escape_allowed_root() {
    let allowed = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), allowed.path().join("escape")).unwrap();
    let (service, _) = BridgeService::open(allowed.path(), &[], false, None).unwrap();
    assert!(matches!(
        service
            .authenticate(selected_pair(Some("escape".into()), "outside"))
            .await,
        Err(WireError {
            code: ErrorCode::WorkspaceDenied,
            ..
        })
    ));
    assert!(!outside.path().join(".microcrab").exists());
}

#[tokio::test]
async fn legacy_uuid_pin_is_readable_without_rewriting_identity() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let state = root.join(".microcrab/host-bridge");
    std::fs::create_dir_all(&state).unwrap();
    let old = Identity {
        host_id: Uuid::new_v4().to_string(),
        workspace_id: Uuid::new_v4().to_string(),
        canonical_root: root.clone(),
    };
    let bytes = serde_json::to_vec(&old).unwrap();
    std::fs::write(state.join("identity.json"), &bytes).unwrap();
    let (bridge, _) = Bridge::open_with_options(&root, false, None).unwrap();
    assert_ne!(bridge.descriptor.host_id, old.host_id);
    let hello = bridge
        .authenticate(WireFrame::Pair {
            token: String::new(),
            scope: scope(),
            directory: None,
            expected_host_id: Some(old.host_id.clone()),
            expected_workspace_id: Some(old.workspace_id.clone()),
            expected_descriptor_digest: None,
            expected_workspace_root: None,
        })
        .await
        .unwrap();
    let WireFrame::Hello { descriptor, .. } = hello else {
        panic!("hello")
    };
    assert_eq!(descriptor.host_id, old.host_id);
    assert_eq!(descriptor.workspace_id, old.workspace_id);
    assert_eq!(std::fs::read(state.join("identity.json")).unwrap(), bytes);
}

#[tokio::test]
async fn authorized_new_project_pair_replaces_lease_but_never_transfers_old_credential() {
    let dir = tempfile::tempdir().unwrap();
    let (bridge, _) = Bridge::open_with_options(dir.path(), true, Some("fresh".into())).unwrap();
    let (credential, first) = pair(&bridge, "fresh".into()).await;
    let other = BindingScope::project(
        "backend".into(),
        "binding".into(),
        "recreated-project".into(),
    );
    let wrong = bridge
        .authenticate(WireFrame::Pair {
            token: "wrong".into(),
            scope: other.clone(),
            directory: None,
            expected_host_id: None,
            expected_workspace_id: None,
            expected_descriptor_digest: None,
            expected_workspace_root: None,
        })
        .await
        .unwrap_err();
    assert_eq!(wrong.code, ErrorCode::PairingDenied);
    assert_eq!(bridge.check_lease(&first).await.unwrap(), scope());
    for (host, workspace) in [
        (Some("foreign-host".into()), None),
        (None, Some("foreign-workspace".into())),
    ] {
        let denied = bridge
            .authenticate(WireFrame::Pair {
                token: "fresh".into(),
                scope: other.clone(),
                directory: None,
                expected_host_id: host,
                expected_workspace_id: workspace,
                expected_descriptor_digest: None,
                expected_workspace_root: None,
            })
            .await
            .unwrap_err();
        assert_eq!(denied.code, ErrorCode::BindingMismatch);
        assert_eq!(bridge.check_lease(&first).await.unwrap(), scope());
    }
    assert!(
        bridge
            .authenticate(WireFrame::Resume {
                credential: credential.clone(),
                scope: other.clone(),
                directory: None,
                expected_host_id: None,
                expected_workspace_id: None,
                expected_descriptor_digest: None,
                expected_workspace_root: None,
            })
            .await
            .is_err()
    );
    let hello = bridge
        .authenticate(WireFrame::Pair {
            token: "fresh".into(),
            scope: other.clone(),
            directory: None,
            expected_host_id: Some(bridge.descriptor.host_id.clone()),
            expected_workspace_id: Some(bridge.descriptor.workspace_id.clone()),
            expected_descriptor_digest: None,
            expected_workspace_root: None,
        })
        .await
        .unwrap();
    let WireFrame::Hello {
        credential: new_credential,
        lease_generation,
        ..
    } = hello
    else {
        panic!("hello")
    };
    assert_ne!(credential, new_credential);
    assert!(bridge.check_lease(&first).await.is_err());
    assert_eq!(bridge.check_lease(&lease_generation).await.unwrap(), other);
    assert!(
        bridge
            .authenticate(WireFrame::Resume {
                credential,
                scope: scope(),
                directory: None,
                expected_host_id: None,
                expected_workspace_id: None,
                expected_descriptor_digest: None,
                expected_workspace_root: None,
            })
            .await
            .is_err()
    );
    assert!(!bridge.state_dir.join("binding.json").exists());
    assert!(!bridge.state_dir.join("binding-host.json").exists());
}

#[tokio::test]
async fn unknown_settlement_is_terminal_durable_and_does_not_replay() {
    let dir = tempfile::tempdir().unwrap();
    let (bridge, token) = Bridge::open(dir.path()).unwrap();
    let (_, lease) = pair(&bridge, token).await;
    let original = HostRequest::Write(WriteRequest {
        path: "original".into(),
        content: "never-replayed".into(),
        precondition: WritePrecondition::Missing,
    });
    let digest = payload_digest(&original).unwrap();
    let receipt = OperationReceipt {
        operation_id: "old".into(),
        payload_digest: digest.clone(),
        scope: scope(),
        state: ReceiptState::Intent,
    };
    bridge.receipts.lock().await.intent(&receipt).unwrap();
    let settle = HostRequest::SettleUnknown {
        operation_id: "old".into(),
        payload_digest: digest.clone(),
        scope: scope(),
    };
    let terminal: OperationReceipt = {
        // Even with an old intent, inspection remains live while execution admission is occupied.
        let admission = bridge.admission.lock().await;
        let recovery = bridge
            .execute(
                "inspect".into(),
                lease.clone(),
                1000,
                HostRequest::Recovery,
                CancellationToken::new(),
            )
            .await
            .0
            .unwrap();
        assert_eq!(recovery.as_array().unwrap().len(), 1);
        let waiting = bridge.execute(
            "settlement".into(),
            lease.clone(),
            5000,
            settle.clone(),
            CancellationToken::new(),
        );
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut waiting)
                .await
                .is_err()
        );
        assert!(matches!(
            bridge
                .receipts
                .lock()
                .await
                .get("old")
                .unwrap()
                .unwrap()
                .state,
            ReceiptState::Intent
        ));
        drop(admission);
        serde_json::from_value(waiting.await.0.unwrap()).unwrap()
    };
    assert!(matches!(
        terminal.state,
        ReceiptState::FailedUnknown {
            effects_may_have_occurred: true,
            ..
        }
    ));
    assert_eq!(
        bridge
            .execute(
                "old".into(),
                lease.clone(),
                5000,
                original,
                CancellationToken::new()
            )
            .await
            .0
            .unwrap_err()
            .code,
        ErrorCode::OutcomeUnknown
    );
    assert!(!dir.path().join("original").exists());
    let new = HostRequest::Write(WriteRequest {
        path: "new".into(),
        content: "allowed".into(),
        precondition: WritePrecondition::Missing,
    });
    bridge
        .execute(
            "new".into(),
            lease.clone(),
            5000,
            new,
            CancellationToken::new(),
        )
        .await
        .0
        .unwrap();
    bridge
        .execute(
            "settle-again".into(),
            lease,
            5000,
            settle,
            CancellationToken::new(),
        )
        .await
        .0
        .unwrap();
    drop(bridge);
    let (restarted, _) = Bridge::open(dir.path()).unwrap();
    assert!(matches!(
        restarted
            .receipts
            .lock()
            .await
            .get("old")
            .unwrap()
            .unwrap()
            .state,
        ReceiptState::FailedUnknown {
            effects_may_have_occurred: true,
            ..
        }
    ));
}

#[tokio::test]
async fn settlement_rejects_stale_lease_foreign_scope_and_digest() {
    let dir = tempfile::tempdir().unwrap();
    let (bridge, _) = Bridge::open_with_options(dir.path(), true, Some("fresh".into())).unwrap();
    let (_, stale) = pair(&bridge, "fresh".into()).await;
    let (_, live) = pair(&bridge, "fresh".into()).await;
    let settle = HostRequest::SettleUnknown {
        operation_id: "missing".into(),
        payload_digest: "a".repeat(64),
        scope: scope(),
    };
    assert_eq!(
        bridge
            .execute(
                "s".into(),
                stale,
                5000,
                settle.clone(),
                CancellationToken::new()
            )
            .await
            .0
            .unwrap_err()
            .code,
        ErrorCode::LeaseExpired
    );
    let foreign = BindingScope::project("foreign".into(), "binding".into(), "project".into());
    let denied = HostRequest::SettleUnknown {
        operation_id: "missing".into(),
        payload_digest: "a".repeat(64),
        scope: foreign,
    };
    assert_eq!(
        bridge
            .execute(
                "s".into(),
                live.clone(),
                5000,
                denied,
                CancellationToken::new()
            )
            .await
            .0
            .unwrap_err()
            .code,
        ErrorCode::OperationConflict
    );
    assert!(bridge.receipts.lock().await.is_empty());
    bridge
        .execute(
            "s".into(),
            live.clone(),
            5000,
            settle,
            CancellationToken::new(),
        )
        .await
        .0
        .unwrap();
    let changed = HostRequest::SettleUnknown {
        operation_id: "missing".into(),
        payload_digest: "b".repeat(64),
        scope: scope(),
    };
    assert_eq!(
        bridge
            .execute("s2".into(), live, 5000, changed, CancellationToken::new())
            .await
            .0
            .unwrap_err()
            .code,
        ErrorCode::OperationConflict
    );
}

#[tokio::test]
async fn restart_unknown_intent_settles_and_allows_new_write_without_replay() {
    let dir = tempfile::tempdir().unwrap();
    let original = HostRequest::Write(WriteRequest {
        path: "old".into(),
        content: "never".into(),
        precondition: WritePrecondition::Missing,
    });
    let digest = payload_digest(&original).unwrap();
    {
        let (bridge, token) = Bridge::open(dir.path()).unwrap();
        pair(&bridge, token).await;
        bridge
            .receipts
            .lock()
            .await
            .intent(&OperationReceipt {
                operation_id: "old".into(),
                payload_digest: digest.clone(),
                scope: scope(),
                state: ReceiptState::Intent,
            })
            .unwrap();
    }
    let (bridge, token) = Bridge::open(dir.path()).unwrap();
    let (_, lease) = pair(&bridge, token).await;
    assert!(matches!(
        bridge
            .receipts
            .lock()
            .await
            .get("old")
            .unwrap()
            .unwrap()
            .state,
        ReceiptState::Indeterminate
    ));
    bridge
        .execute(
            "settle".into(),
            lease.clone(),
            5000,
            HostRequest::SettleUnknown {
                operation_id: "old".into(),
                payload_digest: digest,
                scope: scope(),
            },
            CancellationToken::new(),
        )
        .await
        .0
        .unwrap();
    assert_eq!(
        bridge
            .execute(
                "old".into(),
                lease.clone(),
                5000,
                original,
                CancellationToken::new()
            )
            .await
            .0
            .unwrap_err()
            .code,
        ErrorCode::OutcomeUnknown
    );
    assert!(!dir.path().join("old").exists());
    bridge
        .execute(
            "new".into(),
            lease,
            5000,
            HostRequest::Write(WriteRequest {
                path: "new".into(),
                content: "allowed".into(),
                precondition: WritePrecondition::Missing,
            }),
            CancellationToken::new(),
        )
        .await
        .0
        .unwrap();
}

#[tokio::test]
async fn legacy_project_binding_is_ignored_and_explicit_identity_pins_survive() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let state = root.join(".microcrab/host-bridge");
    std::fs::create_dir_all(&state).unwrap();
    let old = Identity {
        host_id: Uuid::new_v4().to_string(),
        workspace_id: Uuid::new_v4().to_string(),
        canonical_root: root.clone(),
    };
    let identity_bytes = serde_json::to_vec(&old).unwrap();
    let binding_bytes = serde_json::to_vec(&scope()).unwrap();
    let host_bytes = serde_json::to_vec(&old.host_id).unwrap();
    std::fs::write(state.join("identity.json"), &identity_bytes).unwrap();
    std::fs::write(state.join("binding.json"), &binding_bytes).unwrap();
    std::fs::write(state.join("binding-host.json"), &host_bytes).unwrap();
    let (bridge, token) = Bridge::open(&root).unwrap();
    let mut request = selected_pair(None, "recreated-project");
    if let WireFrame::Pair { token: value, .. } = &mut request {
        *value = token;
    }
    let hello = bridge.authenticate(request).await.unwrap();
    let WireFrame::Hello {
        descriptor,
        credential,
        lease_generation,
    } = hello
    else {
        panic!("hello")
    };
    assert_eq!(descriptor.host_id, bridge.descriptor.host_id);
    assert_ne!(descriptor.host_id, old.host_id);
    assert_eq!(descriptor.workspace_id, old.workspace_id);
    assert_eq!(
        bridge
            .check_lease(&lease_generation)
            .await
            .unwrap()
            .project_id()
            .unwrap(),
        "recreated-project"
    );
    assert!(
        bridge
            .authenticate(WireFrame::Resume {
                credential,
                scope: scope(),
                directory: None,
                expected_host_id: None,
                expected_workspace_id: None,
                expected_descriptor_digest: None,
                expected_workspace_root: None,
            })
            .await
            .is_err()
    );
    drop(bridge);
    let (bridge, token) = Bridge::open(&root).unwrap();
    let hello = bridge
        .authenticate(WireFrame::Pair {
            token,
            scope: scope(),
            directory: None,
            expected_host_id: Some(old.host_id.clone()),
            expected_workspace_id: Some(old.workspace_id.clone()),
            expected_descriptor_digest: None,
            expected_workspace_root: None,
        })
        .await
        .unwrap();
    let WireFrame::Hello {
        descriptor,
        credential,
        ..
    } = hello
    else {
        panic!("hello")
    };
    assert_eq!(descriptor.host_id, old.host_id);
    let resumed = bridge
        .authenticate(WireFrame::Resume {
            credential,
            scope: scope(),
            directory: None,
            expected_host_id: None,
            expected_workspace_id: None,
            expected_descriptor_digest: None,
            expected_workspace_root: None,
        })
        .await
        .unwrap();
    assert!(
        matches!(resumed, WireFrame::Hello { descriptor, .. } if descriptor.host_id == old.host_id)
    );
    assert_eq!(
        std::fs::read(state.join("identity.json")).unwrap(),
        identity_bytes
    );
    assert_eq!(
        std::fs::read(state.join("binding.json")).unwrap(),
        binding_bytes
    );
    assert_eq!(
        std::fs::read(state.join("binding-host.json")).unwrap(),
        host_bytes
    );
}

#[tokio::test]
async fn initial_tokenless_resume_uses_only_in_memory_binding_scope() {
    let dir = tempfile::tempdir().unwrap();
    let (bridge, _) = Bridge::open_with_options(dir.path(), false, None).unwrap();
    let hello = bridge
        .authenticate(WireFrame::Resume {
            credential: "browser-capability".into(),
            scope: scope(),
            directory: None,
            expected_host_id: None,
            expected_workspace_id: None,
            expected_descriptor_digest: None,
            expected_workspace_root: None,
        })
        .await
        .unwrap();
    assert!(matches!(hello, WireFrame::Hello { .. }));
    assert_eq!(
        bridge.authority.lock().await.bound_host_id.as_deref(),
        Some(bridge.descriptor.host_id.as_str())
    );
    assert_eq!(bridge.authority.lock().await.scope, Some(scope()));
    assert!(!bridge.state_dir.join("binding.json").exists());
    assert!(!bridge.state_dir.join("binding-host.json").exists());
}

#[tokio::test]
async fn unknown_reason_and_terminal_debug_survive_bridge_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (bridge, token) = Bridge::open(dir.path()).unwrap();
        let (_, lease) = pair(&bridge, token).await;
        let receipt = OperationReceipt {
            operation_id: "uncertain".into(),
            payload_digest: "a".repeat(64),
            scope: scope(),
            state: ReceiptState::Intent,
        };
        bridge.receipts.lock().await.intent(&receipt).unwrap();
        let reason = WireError::new(
            ErrorCode::Timeout,
            "original owner timeout; effects may have occurred",
        );
        bridge
            .receipts
            .lock()
            .await
            .save_unknown(&receipt, reason.clone())
            .unwrap();
        let settled: OperationReceipt = serde_json::from_value(
            bridge
                .execute(
                    "s".into(),
                    lease.clone(),
                    5000,
                    HostRequest::SettleUnknown {
                        operation_id: "uncertain".into(),
                        payload_digest: "a".repeat(64),
                        scope: scope(),
                    },
                    CancellationToken::new(),
                )
                .await
                .0
                .unwrap(),
        )
        .unwrap();
        assert!(
            matches!(settled.state, ReceiptState::FailedUnknown {reason: actual, effects_may_have_occurred: true} if actual == reason)
        );
        let missing: OperationReceipt = serde_json::from_value(
            bridge
                .execute(
                    "m".into(),
                    lease,
                    5000,
                    HostRequest::SettleUnknown {
                        operation_id: "missing".into(),
                        payload_digest: "b".repeat(64),
                        scope: scope(),
                    },
                    CancellationToken::new(),
                )
                .await
                .0
                .unwrap(),
        )
        .unwrap();
        assert!(
            matches!(missing.state, ReceiptState::FailedUnknown {reason, ..} if reason.message.starts_with("receipt_missing:"))
        );
    }
    let (bridge, token) = Bridge::open(dir.path()).unwrap();
    let (_, lease) = pair(&bridge, token).await;
    let recovered: Vec<OperationReceipt> = serde_json::from_value(
        bridge
            .execute(
                "recover".into(),
                lease.clone(),
                5000,
                HostRequest::Recovery,
                CancellationToken::new(),
            )
            .await
            .0
            .unwrap(),
    )
    .unwrap();
    assert_eq!(recovered.len(), 2);
    assert!(recovered.iter().all(|r| matches!(
        r.state,
        ReceiptState::FailedUnknown {
            effects_may_have_occurred: true,
            ..
        }
    )));
    let write = HostRequest::Write(WriteRequest {
        path: "new-after-restart".into(),
        content: "allowed".into(),
        precondition: WritePrecondition::Missing,
    });
    bridge
        .execute("new".into(), lease, 5000, write, CancellationToken::new())
        .await
        .0
        .unwrap();
}

#[tokio::test]
async fn failed_receipt_publication_prevents_dispatch_and_poison_fences_following_effects() {
    let dir = tempfile::tempdir().unwrap();
    let (bridge, token) = Bridge::open(dir.path()).unwrap();
    let (_, lease) = pair(&bridge, token).await;
    let index = bridge.state_dir.join("receipts/fence.index");
    let previous = bridge.state_dir.join("receipts/previous.index");
    std::fs::rename(&index, &previous).unwrap();
    std::fs::create_dir(&index).unwrap();
    let request = HostRequest::Write(WriteRequest {
        path: "must-not-be-written".into(),
        content: "bytes".into(),
        precondition: WritePrecondition::Missing,
    });
    let failure = bridge
        .execute(
            "first".into(),
            lease.clone(),
            5000,
            request.clone(),
            CancellationToken::new(),
        )
        .await
        .0
        .unwrap_err();
    assert_eq!(failure.code, ErrorCode::Io);
    assert!(!dir.path().join("must-not-be-written").exists());
    let refused = bridge
        .execute(
            "second".into(),
            lease,
            5000,
            request,
            CancellationToken::new(),
        )
        .await
        .0
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::OutcomeUnknown);
    assert!(!dir.path().join("must-not-be-written").exists());
    drop(bridge);
    std::fs::remove_dir(&index).unwrap();
    std::fs::rename(previous, index).unwrap();
    let (recovered, _) = Bridge::open(dir.path()).unwrap();
    let mut receipts = recovered.receipts.lock().await;
    assert!(matches!(
        receipts.get("first").unwrap().unwrap().state,
        ReceiptState::Indeterminate
    ));
    assert!(receipts.fence("second").is_none());
}

#[cfg(windows)]
#[tokio::test]
async fn canonical_junction_cannot_select_an_outside_workspace() {
    let allowed = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let link = allowed.path().join("escape");
    let status = std::process::Command::new("cmd.exe")
        .args(["/C", "mklink", "/J"])
        .arg(&link)
        .arg(outside.path())
        .status()
        .unwrap();
    assert!(status.success(), "junction fixture must be available");
    let (service, _) = BridgeService::open(allowed.path(), &[], false, None).unwrap();
    assert!(matches!(
        service
            .authenticate(selected_pair(
                Some(link.to_string_lossy().into()),
                "outside"
            ))
            .await,
        Err(WireError {
            code: ErrorCode::WorkspaceDenied,
            ..
        })
    ));
    assert!(!outside.path().join(".microcrab").exists());
}

#[tokio::test]
async fn unknown_output_scope_blocks_only_that_consumer_and_never_cleanup() {
    use microcrab_host_api::{HandleId, process::ProcessHandle};
    let dir = tempfile::tempdir().unwrap();
    let (bridge, token) = Bridge::open(dir.path()).unwrap();
    let (_, lease) = pair(&bridge, token).await;
    let method = HostRequest::ShellDrain {
        handle: ProcessHandle(HandleId(7)),
        max_bytes: 1024,
    };
    let original = OperationReceipt {
        operation_id: "original-output".into(),
        payload_digest: payload_digest(&method).unwrap(),
        scope: scope(),
        state: ReceiptState::Intent,
    };
    bridge
        .receipts
        .lock()
        .await
        .intent_scoped(&original, method.consumption_key())
        .unwrap();
    let (result, _) = bridge
        .execute(
            "same-output".into(),
            lease.clone(),
            30000,
            method,
            CancellationToken::new(),
        )
        .await;
    assert_eq!(result.unwrap_err().code, ErrorCode::OutcomeUnknown);
    let (result, _) = bridge
        .execute(
            "write".into(),
            lease.clone(),
            30000,
            HostRequest::Write(WriteRequest {
                path: "independent.txt".into(),
                content: "text".into(),
                precondition: WritePrecondition::Missing,
            }),
            CancellationToken::new(),
        )
        .await;
    result.unwrap();
    let (result, _) = bridge
        .execute(
            "cleanup".into(),
            lease,
            30000,
            HostRequest::ProcessRelease {
                handle: ProcessHandle(HandleId(7)),
            },
            CancellationToken::new(),
        )
        .await;
    result.expect("idempotent cleanup acknowledgment must bypass unrelated output uncertainty");
    assert!(
        bridge
            .receipts
            .lock()
            .await
            .fence("original-output")
            .unwrap()
            .phase
            .unresolved()
    );
}
