//! Exact Host identity, non-evicting secondary attachments and explicit idle handoff.
use super::*;

async fn request(
    manager: &Arc<Manager>,
    owner: &Principal,
    id: &str,
    socket: &str,
    descriptor: HostDescriptor,
    force_takeover: bool,
) -> (
    Result<Value, microcrab_protocol::application::WireError>,
    u64,
    tokio::task::JoinHandle<()>,
) {
    let generation = manager.next_connection_generation();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
    let responder_manager = manager.clone();
    let responder_owner = owner.clone();
    let responder_socket = socket.to_owned();
    let responder_id = id.to_owned();
    let responder = tokio::spawn(async move {
        while let Some(frame) = receiver.recv().await {
            let reply = match frame {
                HostFrame::Request {
                    request_id,
                    operation_id,
                    method: HostRequest::Recovery,
                    ..
                } => HostFrame::Response {
                    request_id,
                    operation_id,
                    result: Ok(json!([])),
                },
                HostFrame::Ping { lease_generation } => HostFrame::Pong { lease_generation },
                _ => panic!("Host handoff must never replay an effect"),
            };
            if responder_manager
                .receive_conversation_host(
                    &responder_owner,
                    &responder_socket,
                    &responder_id,
                    generation,
                    reply,
                )
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let result = manager
        .attach_conversation(
            owner,
            socket,
            Attach {
                conversation_id: id.into(),
                descriptor,
                lease_generation: format!("lease-{generation}"),
                host_flavor: None,
                force_takeover,
            },
            sender,
            generation,
        )
        .await;
    (result, generation, responder)
}

#[tokio::test]
async fn secondary_attach_keeps_primary_route_and_explicit_idle_takeover_fences_old_socket() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    let id = uuid::Uuid::new_v4().to_string();
    let browser = uuid::Uuid::new_v4().to_string();
    let workspace = uuid::Uuid::new_v4().to_string();
    manager
        .conversation_scope(&owner.owner, binding(&id, &browser, &workspace))
        .await
        .unwrap();
    let descriptor = browser_descriptor(&browser, &workspace);
    let (first, old) = attach(&manager, &owner, &id, "primary", descriptor.clone()).await;
    let remote = manager
        .conversation_routes
        .lock()
        .await
        .get(&id)
        .unwrap()
        .remote
        .clone();
    let (secondary, _, second_task) = request(
        &manager,
        &owner,
        &id,
        "secondary",
        descriptor.clone(),
        false,
    )
    .await;
    let secondary = secondary.unwrap();
    assert_eq!(secondary["attached"], false);
    assert_eq!(secondary["role"], "participant");
    assert_eq!(secondary["ready"], true);
    assert_eq!(secondary["connection_generation"], first.to_string());
    assert_eq!(
        manager
            .conversation_routes
            .lock()
            .await
            .get(&id)
            .unwrap()
            .socket,
        "primary"
    );
    assert!(remote.is_connected());
    manager
        .detach_conversation_socket("secondary", Some(&id))
        .await;
    assert!(manager.conversation_host(&owner.owner, &id).await.is_ok());
    second_task.await.unwrap();

    let (taken, second, new) = request(&manager, &owner, &id, "secondary", descriptor, true).await;
    assert_eq!(taken.unwrap()["attached"], true);
    assert_eq!(
        manager
            .conversation_routes
            .lock()
            .await
            .get(&id)
            .unwrap()
            .generation,
        second
    );
    assert!(Arc::ptr_eq(
        &remote,
        &manager
            .conversation_routes
            .lock()
            .await
            .get(&id)
            .unwrap()
            .remote
    ));
    assert!(
        manager
            .receive_conversation_host(
                &owner,
                "primary",
                &id,
                first,
                HostFrame::Pong {
                    lease_generation: format!("lease-{first}")
                }
            )
            .await
            .is_err()
    );
    manager
        .detach_conversation_socket("primary", Some(&id))
        .await;
    assert!(manager.conversation_host(&owner.owner, &id).await.is_ok());
    manager.close().await.unwrap();
    old.abort();
    new.abort();
    let _ = old.await;
    let _ = new.await;
}

#[tokio::test]
async fn takeover_rechecks_initialization_admission_before_replacing_primary() {
    let root = tempfile::tempdir().unwrap();
    let mut configuration = config(root.path());
    let source = crate::manager::host_shell_fixture::shell_component_source(
        false,
        r#"[{"name":"init-race","modes":[{"name":"execute","role":"both","permissions":[]}],"variables":{},"tools":[]}]"#,
        0,
    );
    let component = root.path().join("init-race.wasm");
    std::fs::write(
        &component,
        crate::manager::typed_agent_fixture::component_with_init(
            &source,
            crate::manager::typed_agent_fixture::READY_INIT_BODY,
        ),
    )
    .unwrap();
    configuration.agent = Some(crate::config::Agent {
        path: component,
        name: Some("init-race".into()),
        mode: Some("execute".into()),
        restore_only: false,
    });
    let manager = Manager::new(configuration).await.unwrap();
    let owner = Principal::for_test("owner");
    let id = uuid::Uuid::new_v4().to_string();
    let browser = uuid::Uuid::new_v4().to_string();
    let workspace = uuid::Uuid::new_v4().to_string();
    manager
        .conversation_scope(&owner.owner, binding(&id, &browser, &workspace))
        .await
        .unwrap();
    let descriptor = browser_descriptor(&browser, &workspace);
    let (first, primary) = attach(&manager, &owner, &id, "primary", descriptor.clone()).await;
    commands::create(
        &manager,
        &owner.owner,
        &command(
            "session.create",
            json!({
                "conversation_id":id, "operation_id":uuid::Uuid::new_v4().to_string()
            }),
        ),
    )
    .await
    .unwrap();
    let managed = manager
        .active(manager.record(&owner.owner, &id).await.unwrap(), false)
        .await
        .unwrap();
    let slot = managed.slot.upgrade().unwrap();
    assert!(!slot.initializing.load(Ordering::Acquire));
    // Block route publication until takeover has passed its early check and owns commands.
    let routes = manager.conversation_routes.lock().await;
    let remote = routes.get(&id).unwrap().remote.clone();
    let worker = manager.clone();
    let worker_owner = owner.clone();
    let worker_id = id.clone();
    let takeover = tokio::spawn(async move {
        request(
            &worker,
            &worker_owner,
            &worker_id,
            "secondary",
            descriptor,
            true,
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if managed.commands.try_lock().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("takeover reaches the held route publication barrier");
    // Model deferred init's held admission after it releases commands and before
    // Session.start publishes execution/inbox ownership. Keep the admission live
    // through the takeover result; all other busy indicators remain idle.
    let admission = super::super::super::agent_init::Admission::acquire(slot).unwrap();
    assert!(!managed.session.inbox_snapshot().active);
    assert!(
        !managed
            .session
            .execution_snapshots()
            .iter()
            .any(|execution| execution.status.is_pending())
    );
    assert!(managed.hub.operation().is_none());
    drop(routes);
    let (result, _, responder) = tokio::time::timeout(std::time::Duration::from_secs(3), takeover)
        .await
        .unwrap()
        .unwrap();
    let error = result.unwrap_err();
    assert_eq!(error.code, "host_takeover_busy");
    assert_eq!(error.message, "Conversation initialization is active");
    responder.await.unwrap();
    let routes = manager.conversation_routes.lock().await;
    let current = routes.get(&id).unwrap();
    assert_eq!(current.socket, "primary");
    assert_eq!(current.generation, first);
    assert!(current.ready);
    assert!(Arc::ptr_eq(&current.remote, &remote));
    assert!(remote.is_connected());
    drop(routes);
    drop(admission);
    manager.close().await.unwrap();
    primary.abort();
    let _ = primary.await;
}

#[tokio::test]
async fn takeover_rejects_unready_primary_and_unrelated_workspace_without_detach() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    let id = uuid::Uuid::new_v4().to_string();
    let browser = uuid::Uuid::new_v4().to_string();
    let workspace = uuid::Uuid::new_v4().to_string();
    manager
        .conversation_scope(&owner.owner, binding(&id, &browser, &workspace))
        .await
        .unwrap();
    let descriptor = browser_descriptor(&browser, &workspace);
    let (_, primary) = attach(&manager, &owner, &id, "primary", descriptor.clone()).await;
    manager
        .conversation_routes
        .lock()
        .await
        .get_mut(&id)
        .unwrap()
        .ready = false;
    let (result, _, task) =
        request(&manager, &owner, &id, "secondary", descriptor.clone(), true).await;
    assert_eq!(result.unwrap_err().code, "host_takeover_reconciling");
    task.await.unwrap();
    assert_eq!(
        manager
            .conversation_routes
            .lock()
            .await
            .get(&id)
            .unwrap()
            .socket,
        "primary"
    );
    assert!(
        manager
            .conversation_routes
            .lock()
            .await
            .get(&id)
            .unwrap()
            .remote
            .is_connected()
    );
    let mut other = descriptor;
    other.workspace_id = uuid::Uuid::new_v4().to_string();
    other.descriptor_digest = microcrab_host_protocol::descriptor_fingerprint(&other).unwrap();
    let (result, _, task) = request(&manager, &owner, &id, "secondary", other, true).await;
    assert_eq!(result.unwrap_err().code, "binding_mismatch");
    task.await.unwrap();
    manager.close().await.unwrap();
    primary.abort();
    let _ = primary.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires root-built Universal component path in MICROCRAB_TEST_UNIVERSAL_COMPONENT"]
async fn maintained_sandbox_host_takeover_rejects_busy_command_without_detaching_primary() {
    let root = tempfile::tempdir().unwrap();
    let mut configuration = config(root.path());
    configuration.agent = Some(serde_json::from_value(json!({
        "path":std::env::var("MICROCRAB_TEST_UNIVERSAL_COMPONENT").expect("root-built Universal component path"),
        "name":"universal","mode":"execute"
    })).unwrap());
    let manager = Manager::new(configuration).await.unwrap();
    let owner = Principal::for_test("owner");
    let id = uuid::Uuid::new_v4().to_string();
    let browser = uuid::Uuid::new_v4().to_string();
    let workspace = uuid::Uuid::new_v4().to_string();
    manager
        .conversation_scope(&owner.owner, binding(&id, &browser, &workspace))
        .await
        .unwrap();
    let descriptor = browser_descriptor(&browser, &workspace);
    let (_, primary) = attach(&manager, &owner, &id, "primary", descriptor.clone()).await;
    commands::create(
        &manager,
        &owner.owner,
        &command(
            "session.create",
            json!({
                "conversation_id":id,"agent":"universal","mode":"execute",
                "operation_id":uuid::Uuid::new_v4().to_string()
            }),
        ),
    )
    .await
    .unwrap();
    let managed = manager
        .active(manager.record(&owner.owner, &id).await.unwrap(), false)
        .await
        .unwrap();
    let guard = managed.commands.lock().await;
    let (result, _, task) = request(&manager, &owner, &id, "secondary", descriptor, true).await;
    assert_eq!(result.unwrap_err().code, "host_takeover_busy");
    task.await.unwrap();
    assert_eq!(
        manager
            .conversation_routes
            .lock()
            .await
            .get(&id)
            .unwrap()
            .socket,
        "primary"
    );
    assert!(
        manager
            .conversation_routes
            .lock()
            .await
            .get(&id)
            .unwrap()
            .remote
            .is_connected()
    );
    drop(guard);
    manager.close().await.unwrap();
    primary.abort();
    let _ = primary.await;
}
