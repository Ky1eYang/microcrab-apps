use super::*;

#[tokio::test]
async fn queued_fifth_start_does_not_block_controls_or_scope_inventory() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let root = tempfile::tempdir().unwrap();
        let (bridge, token) = Bridge::open(root.path()).unwrap();
        let (_, lease) = pair(&bridge, token).await;
        let mut held = Vec::new();
        for _ in 0..4 {
            let job: ProcessStarted =
                serde_json::from_value(rpc(&bridge, &lease, request(interactive())).await.unwrap())
                    .unwrap();
            held.push(job);
        }
        let fifth = rpc(&bridge, &lease, request(interactive()));
        tokio::pin!(fifth);
        assert!(futures_util::poll!(&mut fifth).is_pending());
        // Polling the start once reaches the exhausted native permit. No lock
        // needed by these controls may be retained by that pending admission.
        let inventory: Vec<ProcessStarted> = serde_json::from_value(
            rpc(&bridge, &lease, HostRequest::ProcessInventory)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(inventory.len(), 4);
        let status: ProcessStatus = serde_json::from_value(
            rpc(
                &bridge,
                &lease,
                HostRequest::ProcessStatus {
                    handle: held[0].handle,
                },
            )
            .await
            .unwrap(),
        )
        .unwrap();
        assert_eq!(status.pid, held[0].pid);
        assert!(matches!(status.state, ProcessState::Running));
        rpc(
            &bridge,
            &lease,
            HostRequest::ProcessKill {
                handle: held[0].handle,
                force: true,
            },
        )
        .await
        .unwrap();
        let admitted: ProcessStarted = serde_json::from_value(fifth.await.unwrap()).unwrap();
        assert_ne!(admitted.handle, held[0].handle);
        for job in held.into_iter().chain(std::iter::once(admitted)) {
            rpc(
                &bridge,
                &lease,
                HostRequest::ProcessRelease { handle: job.handle },
            )
            .await
            .unwrap();
        }
        assert!(bridge.process_inventory(&scope()).await.is_empty());
    })
    .await
    .expect("controls and fifth admission must complete without the RPC deadline");
}

#[tokio::test]
async fn retained_bridge_inventory_identifies_orphan_processes_across_controller_recreation() {
    let root = tempfile::tempdir().unwrap();
    let (bridge, token) = Bridge::open(root.path()).unwrap();
    let (credential, first_lease) = pair(&bridge, token).await;
    let started: ProcessStarted = serde_json::from_value(
        rpc(&bridge, &first_lease, request(interactive()))
            .await
            .unwrap(),
    )
    .unwrap();
    let native = bridge
        .owned_process(started.handle, &scope())
        .await
        .unwrap();
    // The same bridge survives while a newly authenticated backend controller
    // has no Session ownership. Inventory supplies real identities for cleanup.
    let hello = bridge
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
    let WireFrame::Hello {
        lease_generation, ..
    } = hello
    else {
        panic!("resume")
    };
    let inventory: Vec<ProcessStarted> = serde_json::from_value(
        rpc(&bridge, &lease_generation, HostRequest::ProcessInventory)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].handle, started.handle);
    assert_eq!(inventory[0].pid, started.pid);
    let foreign = foreign_scope();
    assert!(bridge.process_inventory(&foreign).await.is_empty());
    assert!(matches!(
        bridge
            .host
            .status(&microcrab_host_api::OpContext::default(), native.handle)
            .await
            .unwrap()
            .state,
        ProcessState::Running
    ));
    rpc(
        &bridge,
        &lease_generation,
        HostRequest::ProcessRelease {
            handle: inventory[0].handle,
        },
    )
    .await
    .unwrap();
    // Idempotent acknowledgment cannot attach to or spawn another process.
    rpc(
        &bridge,
        &lease_generation,
        HostRequest::ProcessRelease {
            handle: inventory[0].handle,
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
    assert!(!bridge.host.has_running_processes());
    assert!(bridge.process_inventory(&scope()).await.is_empty());
}
