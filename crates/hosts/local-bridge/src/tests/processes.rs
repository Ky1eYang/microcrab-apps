use super::*;
mod followup;
use microcrab_host_api::process::{ProcessHandle, ProcessState, ProcessStatus};
use microcrab_host_api::{ProcessApi, ShellOutput, ShellRequest};

fn foreign_scope() -> BindingScope {
    let BindingScope::Project(current) = scope() else {
        panic!("native bridge fixture requires a Project scope")
    };
    BindingScope::project(
        current.backend_id,
        format!("{}-foreign", current.binding_id),
        current.project_id,
    )
}
fn request(command: &str) -> HostRequest {
    HostRequest::ShellLaunch(ShellRequest {
        command: command.into(),
        cwd: None,
        output_bytes: 4096,
    })
}
fn interactive() -> &'static str {
    #[cfg(windows)]
    {
        r#"$line = [Console]::ReadLine(); Write-Output "${PID}:$line""#
    }
    #[cfg(not(windows))]
    {
        "IFS= read -r line; printf '%s:%s\\n' \"$$\" \"$line\""
    }
}
async fn rpc(bridge: &Bridge, lease: &str, method: HostRequest) -> HostReply {
    bridge
        .execute(
            Uuid::new_v4().to_string(),
            lease.into(),
            5000,
            method,
            CancellationToken::new(),
        )
        .await
        .0
}
#[tokio::test]
async fn relay_keeps_same_real_process_and_forwards_owned_watch_transitions() {
    let dir = tempfile::tempdir().unwrap();
    let (bridge, token) = Bridge::open(dir.path()).unwrap();
    let (_, lease) = pair(&bridge, token).await;
    let cancel = CancellationToken::new();
    let started: ProcessStarted = serde_json::from_value(
        bridge
            .execute(
                "interactive-start".into(),
                lease.clone(),
                5000,
                request(interactive()),
                cancel.clone(),
            )
            .await
            .0
            .unwrap(),
    )
    .unwrap();
    let native_pid = started.pid.expect("native bridge supplies an OS PID");
    assert_ne!(native_pid, 0);
    assert!((1..=(1_u64 << 53) - 1).contains(&started.handle.0.0));
    assert_eq!((started.handle.0.0 as f64) as u64, started.handle.0.0);
    let through_browser_number: ProcessHandle =
        serde_json::from_str(&format!("{}", started.handle.0.0 as f64)).unwrap();
    assert_eq!(through_browser_number, started.handle);
    let wire_receipt: ProcessStarted =
        serde_json::from_str(&serde_json::to_string(&started).unwrap()).unwrap();
    assert_eq!(wire_receipt.handle, started.handle);
    cancel.cancel(); // An ordinary turn interrupt does not terminate this receipt.
    let status: ProcessStatus = serde_json::from_value(
        rpc(
            &bridge,
            &lease,
            HostRequest::ProcessStatus {
                handle: started.handle,
            },
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(status.pid, started.pid);
    assert!(matches!(status.state, ProcessState::Running));
    let foreign = foreign_scope();
    assert!(
        bridge
            .owned_process(started.handle, &foreign)
            .await
            .is_err()
    );
    assert!(bridge.process_revisions(&foreign).await.is_empty());
    let mut changed = bridge.processes.changed.subscribe();
    changed.borrow_and_update();
    rpc(
        &bridge,
        &lease,
        HostRequest::ShellWrite {
            handle: started.handle,
            data: b"same-process\n".to_vec(),
            eof: true,
        },
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status: ProcessStatus = serde_json::from_value(
                rpc(
                    &bridge,
                    &lease,
                    HostRequest::ProcessStatus {
                        handle: started.handle,
                    },
                )
                .await
                .unwrap(),
            )
            .unwrap();
            let capture: microcrab_host_api::ShellCaptureStatus = serde_json::from_value(
                rpc(
                    &bridge,
                    &lease,
                    HostRequest::ShellCaptureStatus {
                        handle: started.handle,
                    },
                )
                .await
                .unwrap(),
            )
            .unwrap();
            if !matches!(status.state, ProcessState::Running) {
                assert!(!capture.closed, "retained output closes only after drain");
                assert!(capture.stdout_bytes > 0);
                break;
            }
            changed.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(
        bridge
            .process_revisions(&scope())
            .await
            .iter()
            .any(|(id, revision)| *id == started.handle && *revision > 0)
    );
    let read: ShellOutput = serde_json::from_value(
        rpc(
            &bridge,
            &lease,
            HostRequest::ShellRead {
                handle: started.handle,
                from: None,
            },
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert!(
        read.stdout.contains(&format!("{native_pid}:same-process")),
        "{}",
        read.stdout
    );
    let drain = HostRequest::ShellDrain {
        handle: started.handle,
        max_bytes: 4096,
    };
    let cancelled_operation = "cancelled-shell-drain";
    let cancellation = CancellationToken::new();
    // Pause after the initial cancellation check, at durable intent admission.
    // Cancellation must settle the original call, rather than dropping its
    // publication future or allowing a duplicate to consume retained output.
    let records = bridge.receipts.lock().await;
    let cancelled = bridge.execute(
        cancelled_operation.into(),
        lease.clone(),
        5000,
        drain.clone(),
        cancellation.clone(),
    );
    tokio::pin!(cancelled);
    assert!(futures_util::poll!(&mut cancelled).is_pending());
    cancellation.cancel();
    drop(records);
    let (cancelled_result, chunks) = cancelled.await;
    let cancelled_error = cancelled_result.unwrap_err();
    assert_eq!(cancelled_error.code, ErrorCode::Cancelled);
    assert!(chunks.is_empty());
    let receipt = bridge
        .receipts
        .lock()
        .await
        .get(cancelled_operation)
        .unwrap()
        .unwrap();
    assert_eq!(receipt.scope, scope());
    assert_eq!(receipt.payload_digest, payload_digest(&drain).unwrap());
    assert!(matches!(
        receipt.state,
        ReceiptState::Completed(Err(ref reason)) if reason == &cancelled_error
    ));
    let (duplicate, chunks) = bridge
        .execute(
            cancelled_operation.into(),
            lease.clone(),
            5000,
            drain.clone(),
            CancellationToken::new(),
        )
        .await;
    assert_eq!(duplicate.unwrap_err(), cancelled_error);
    assert!(chunks.is_empty());
    let retained: ShellOutput = serde_json::from_value(
        rpc(
            &bridge,
            &lease,
            HostRequest::ShellRead {
                handle: started.handle,
                from: None,
            },
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(retained.stdout, read.stdout);
    assert_eq!(retained.stderr, read.stderr);
    assert!(
        bridge
            .process_inventory(&scope())
            .await
            .iter()
            .any(|entry| entry.handle == started.handle),
        "cancelling observation must retain the process cleanup owner"
    );
    assert_eq!(
        rpc(
            &bridge,
            &lease,
            HostRequest::ShellDrain {
                handle: started.handle,
                max_bytes: 0
            }
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::Conflict,
        "insufficient drain budget must preserve the whole retained batch"
    );
    let completed_operation = "completed-shell-drain";
    let completed_cancellation = CancellationToken::new();
    let (drained_value, chunks) = bridge
        .execute(
            completed_operation.into(),
            lease.clone(),
            5000,
            drain.clone(),
            completed_cancellation.clone(),
        )
        .await;
    let drained_value = drained_value.unwrap();
    assert!(chunks.is_empty());
    let drained: ShellOutput = serde_json::from_value(drained_value.clone()).unwrap();
    assert_eq!(read.stdout, drained.stdout);
    // A caller may lose its reply or cancel after consumption. Exact operation
    // replay must return the original bytes even though capture is now empty.
    completed_cancellation.cancel();
    let (duplicate, chunks) = bridge
        .execute(
            completed_operation.into(),
            lease.clone(),
            5000,
            drain,
            CancellationToken::new(),
        )
        .await;
    assert_eq!(duplicate.unwrap(), drained_value);
    assert!(chunks.is_empty());
    let receipt = bridge
        .receipts
        .lock()
        .await
        .get(completed_operation)
        .unwrap()
        .unwrap();
    assert!(matches!(
        receipt.state,
        ReceiptState::Completed(Ok(value)) if value == drained_value
    ));
    let capture: microcrab_host_api::ShellCaptureStatus = serde_json::from_value(
        rpc(
            &bridge,
            &lease,
            HostRequest::ShellCaptureStatus {
                handle: started.handle,
            },
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert!(capture.closed);
    assert_eq!((capture.stdout_bytes, capture.stderr_bytes), (0, 0));
    let empty: ShellOutput = serde_json::from_value(
        rpc(
            &bridge,
            &lease,
            HostRequest::ShellDrain {
                handle: started.handle,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert!(empty.stdout.is_empty());
    rpc(
        &bridge,
        &lease,
        HostRequest::ProcessRelease {
            handle: started.handle,
        },
    )
    .await
    .unwrap();
    assert!(
        bridge
            .owned_process(started.handle, &scope())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn release_stops_running_job_and_stale_ids_cannot_find_another_process() {
    let dir = tempfile::tempdir().unwrap();
    let (bridge, token) = Bridge::open(dir.path()).unwrap();
    let (_, lease) = pair(&bridge, token).await;
    let started: ProcessStarted =
        serde_json::from_value(rpc(&bridge, &lease, request(interactive())).await.unwrap())
            .unwrap();
    let native = bridge
        .owned_process(started.handle, &scope())
        .await
        .unwrap();
    rpc(
        &bridge,
        &lease,
        HostRequest::ProcessKill {
            handle: started.handle,
            force: true,
        },
    )
    .await
    .unwrap();
    let status: ProcessStatus = serde_json::from_value(
        rpc(
            &bridge,
            &lease,
            HostRequest::ProcessStatus {
                handle: started.handle,
            },
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(status.pid, started.pid);
    assert!(!matches!(status.state, ProcessState::Running));
    rpc(
        &bridge,
        &lease,
        HostRequest::ProcessRelease {
            handle: started.handle,
        },
    )
    .await
    .unwrap();
    assert!(
        bridge
            .host
            .status(&microcrab_host_api::OpContext::default(), native.handle)
            .await
            .is_err()
    );
    let next: ProcessStarted =
        serde_json::from_value(rpc(&bridge, &lease, request(interactive())).await.unwrap())
            .unwrap();
    assert_ne!(started.handle, next.handle);
    assert!(
        rpc(
            &bridge,
            &lease,
            HostRequest::ProcessKill {
                handle: started.handle,
                force: true
            }
        )
        .await
        .is_err()
    );
    rpc(
        &bridge,
        &lease,
        HostRequest::ProcessRelease {
            handle: next.handle,
        },
    )
    .await
    .unwrap();
    assert!(
        bridge
            .owned_process(ProcessHandle(microcrab_host_api::HandleId(0)), &scope())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn structured_spawn_owns_raw_pipes_and_same_process_controls() {
    let root = tempfile::tempdir().unwrap();
    let (bridge, token) = Bridge::open(root.path()).unwrap();
    let (_, lease) = pair(&bridge, token).await;
    let spec = bridge.host.shell_spec(interactive()).unwrap();
    let started: ProcessStarted = serde_json::from_value(
        rpc(&bridge, &lease, HostRequest::ProcessSpawn(spec))
            .await
            .unwrap(),
    )
    .unwrap();
    let handle = started.handle;
    let pid: u32 = serde_json::from_value(
        rpc(&bridge, &lease, HostRequest::ProcessPid { handle })
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(Some(pid), started.pid);
    // Structured and shell launches both inhabit the same scoped inventory.
    let shell: ProcessStarted =
        serde_json::from_value(rpc(&bridge, &lease, request(interactive())).await.unwrap())
            .unwrap();
    let inventory = bridge.process_inventory(&scope()).await;
    assert_eq!(inventory.len(), 2);
    assert!(
        inventory
            .iter()
            .any(|entry| entry.handle == handle && entry.pid == Some(pid))
    );
    assert!(
        inventory
            .iter()
            .any(|entry| entry.handle == shell.handle && entry.pid == shell.pid)
    );
    rpc(
        &bridge,
        &lease,
        HostRequest::ProcessStreamOpen {
            handle,
            stderr: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        rpc(
            &bridge,
            &lease,
            HostRequest::ProcessStreamOpen {
                handle,
                stderr: false
            }
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::Conflict
    );
    // A captured shell owns its raw pipes, but accepts the same ProcessApi controls.
    assert_eq!(
        rpc(
            &bridge,
            &lease,
            HostRequest::ProcessStreamOpen {
                handle: shell.handle,
                stderr: false
            }
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::Conflict
    );
    rpc(
        &bridge,
        &lease,
        HostRequest::ProcessWrite {
            handle,
            data: b"raw-process\n".to_vec(),
        },
    )
    .await
    .unwrap();
    rpc(&bridge, &lease, HostRequest::ProcessCloseStdin { handle })
        .await
        .unwrap();
    let mut bytes = Vec::new();
    loop {
        let chunk: ProcessChunk = serde_json::from_value(
            rpc(
                &bridge,
                &lease,
                HostRequest::ProcessStreamRead {
                    handle,
                    stderr: false,
                    max_bytes: 3,
                },
            )
            .await
            .unwrap(),
        )
        .unwrap();
        assert!(chunk.data.len() <= 3);
        bytes.extend(chunk.data);
        if chunk.eof {
            break;
        }
    }
    assert!(
        String::from_utf8(bytes)
            .unwrap()
            .contains(&format!("{pid}:raw-process"))
    );
    let exit: microcrab_host_api::process::ExitStatus = serde_json::from_value(
        rpc(&bridge, &lease, HostRequest::ProcessWait { handle })
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(exit.code, Some(0));
    rpc(
        &bridge,
        &lease,
        HostRequest::ProcessKill {
            handle: shell.handle,
            force: true,
        },
    )
    .await
    .unwrap();
    for handle in [handle, shell.handle] {
        rpc(&bridge, &lease, HostRequest::ProcessRelease { handle })
            .await
            .unwrap();
    }
    assert!(bridge.process_inventory(&scope()).await.is_empty());
}

#[tokio::test]
async fn raw_stream_read_is_bounded_and_scope_checked_without_consuming_foreign_bytes() {
    let root = tempfile::tempdir().unwrap();
    let (bridge, token) = Bridge::open(root.path()).unwrap();
    let (_, lease) = pair(&bridge, token).await;
    let spec = bridge.host.shell_spec(interactive()).unwrap();
    let started: ProcessStarted = serde_json::from_value(
        rpc(&bridge, &lease, HostRequest::ProcessSpawn(spec))
            .await
            .unwrap(),
    )
    .unwrap();
    let handle = started.handle;
    rpc(
        &bridge,
        &lease,
        HostRequest::ProcessStreamOpen {
            handle,
            stderr: false,
        },
    )
    .await
    .unwrap();
    let foreign = foreign_scope();
    assert_eq!(
        bridge
            .read_process_stream(
                handle,
                &foreign,
                false,
                16,
                &microcrab_host_api::OpContext::default()
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::NotFound
    );
    assert_eq!(
        rpc(
            &bridge,
            &lease,
            HostRequest::ProcessStreamRead {
                handle,
                stderr: false,
                max_bytes: MAX_CHUNK_BYTES as u32 + 1
            }
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::InvalidRequest
    );
    rpc(&bridge, &lease, HostRequest::ProcessRelease { handle })
        .await
        .unwrap();
}

#[tokio::test]
async fn pending_quiet_pipe_read_does_not_fence_file_effects() {
    let root = tempfile::tempdir().unwrap();
    let (bridge, token) = Bridge::open(root.path()).unwrap();
    let (_, lease) = pair(&bridge, token).await;
    let spec = bridge.host.shell_spec(interactive()).unwrap();
    let started: ProcessStarted = serde_json::from_value(
        rpc(&bridge, &lease, HostRequest::ProcessSpawn(spec))
            .await
            .unwrap(),
    )
    .unwrap();
    let handle = started.handle;
    rpc(
        &bridge,
        &lease,
        HostRequest::ProcessStreamOpen {
            handle,
            stderr: false,
        },
    )
    .await
    .unwrap();
    let reading = rpc(
        &bridge,
        &lease,
        HostRequest::ProcessStreamRead {
            handle,
            stderr: false,
            max_bytes: 16,
        },
    );
    tokio::pin!(reading);
    assert!(futures_util::poll!(&mut reading).is_pending());
    rpc(
        &bridge,
        &lease,
        HostRequest::Write(WriteRequest {
            path: "parallel.txt".into(),
            content: "written".into(),
            precondition: WritePrecondition::Missing,
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(root.path().join("parallel.txt")).unwrap(),
        "written"
    );
    rpc(&bridge, &lease, HostRequest::ProcessRelease { handle })
        .await
        .unwrap();
    let chunk: ProcessChunk = serde_json::from_value(reading.await.unwrap()).unwrap();
    assert!(chunk.eof);
}
