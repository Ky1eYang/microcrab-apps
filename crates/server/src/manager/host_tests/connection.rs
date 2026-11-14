//! Canonical identity, retained transport, and provisional metadata safety contracts.
use super::*;
use microcrab_host_protocol::{
    ErrorCode, HostDescriptor, HostFrame, HostRequest, WireError as HostWireError,
};
use serde_json::{Value, json};

fn command(method: &str, params: Value) -> microcrab_protocol::application::Command {
    serde_json::from_value(json!({"v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),"method":method,"params":params})).unwrap()
}
async fn provisional(manager: &Manager, owner: &Principal, id: &str) {
    let result = commands::execute(
        manager,
        owner,
        &command(
            "project.create",
            json!({"id":id,"name":id,"root":"/requested/remote/alias"}),
        ),
    )
    .await
    .unwrap();
    assert_eq!(result["provisional"], true);
    assert_eq!(result["root"], "/requested/remote/alias");
}
async fn attach(
    manager: &Arc<Manager>,
    owner: &Principal,
    project: &str,
    descriptor: HostDescriptor,
    fail_first: bool,
) -> (Value, u64, tokio::task::JoinHandle<()>) {
    let generation = manager.next_connection_generation();
    let socket = format!("socket-{generation}");
    let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
    let reply_manager = manager.clone();
    let reply_owner = owner.clone();
    let reply_project = project.to_owned();
    let reply_socket = socket.clone();
    let responder = tokio::spawn(async move {
        let mut fail = fail_first;
        while let Some(frame) = receiver.recv().await {
            let reply = match frame {
                HostFrame::Request {
                    request_id,
                    operation_id,
                    method: HostRequest::Recovery,
                    ..
                } => {
                    let result = if fail {
                        fail = false;
                        Err(HostWireError::new(
                            ErrorCode::OutcomeUnknown,
                            "original_worker_active: ownership has not joined",
                        ))
                    } else {
                        Ok(json!([]))
                    };
                    HostFrame::Response {
                        request_id,
                        operation_id,
                        result,
                    }
                }
                HostFrame::Ping { lease_generation } => HostFrame::Pong { lease_generation },
                _ => panic!("attach/reconcile must not execute effects"),
            };
            if reply_manager
                .receive_host(
                    &reply_owner,
                    &reply_socket,
                    &reply_project,
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
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        manager.attach_project(
            owner,
            &socket,
            super::super::bindings::Attach {
                project_id: project.into(),
                descriptor,
                lease_generation: generation.to_string(),
                allow_legacy_binding: true,
            },
            sender,
            generation,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    (result, generation, responder)
}
async fn finish(manager: &Manager, task: tokio::task::JoinHandle<()>) {
    manager.close().await.unwrap();
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn reconciliation_failure_retains_route_owner_inspection_and_generation_guard() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    let (attached, generation, responder) =
        attach(&manager, &owner, "a", descriptor("host"), true).await;
    assert_eq!(attached["connected"], true);
    assert_eq!(attached["ready"], false);
    assert_eq!(attached["reconciling"], false);
    assert_eq!(attached["reconciliation_error"]["code"], "outcome_unknown");
    assert!(
        attached["reconciliation_error"]["message"]
            .as_str()
            .unwrap()
            .contains("original_worker_active: ownership has not joined")
    );
    assert_eq!(
        attached["readiness"]["connection_generation"],
        generation.to_string()
    );
    assert_eq!(attached["readiness"]["connected"], true);
    assert_eq!(attached["readiness"]["ready"], false);
    assert_eq!(attached["readiness"]["reconciling"], false);
    assert_eq!(attached["readiness"]["phase"], "unready");
    assert_eq!(
        attached["readiness"]["cause"],
        attached["reconciliation_error"]
    );
    let mut state = manager.host_state(&owner, "a").await.unwrap();
    assert!(state["diagnostics"].is_array());
    state.as_object_mut().unwrap().remove("diagnostics");
    assert_eq!(state, attached);
    let rejected = match manager.project_host("owner", "a").await {
        Ok(_) => panic!("failed Recovery must fence execution"),
        Err(error) => error,
    };
    assert_eq!(rejected.code, "host_detached");
    assert!(
        rejected
            .message
            .contains("original_worker_active: ownership has not joined")
    );
    assert_eq!(
        rejected.details.as_ref().unwrap()["readiness"],
        attached["readiness"]
    );
    assert_eq!(
        manager.host_state(&owner, "a").await.unwrap()["connected"],
        true
    );
    let intruder = Principal::for_test("intruder");
    assert!(manager.host_state(&intruder, "a").await.is_err());
    assert!(manager.host_operations(&intruder, "a", 100).await.is_err());
    assert!(manager.reconcile_host(&intruder, "a", None).await.is_err());
    assert!(manager.host_operations(&owner, "a", 101).await.is_err());
    let operations = manager.host_operations(&owner, "a", 100).await.unwrap();
    assert_eq!(operations["limit"], 100);
    assert_eq!(operations["operations"], json!([]));
    let stale = manager
        .reconcile_host(&owner, "a", Some(&(generation + 1).to_string()))
        .await
        .unwrap_err();
    assert_eq!(stale.code, "route_replaced");
    assert_eq!(
        manager.host_state(&owner, "a").await.unwrap()["connected"],
        true
    );
    let state = manager
        .reconcile_host(&owner, "a", Some(&generation.to_string()))
        .await
        .unwrap();
    assert_eq!(state["ready"], true);
    assert_eq!(state["reconciliation_error"], Value::Null);
    assert_eq!(state["readiness"]["phase"], "ready");
    assert_eq!(state["readiness"]["cause"], Value::Null);
    assert_eq!(state["readiness"]["reconciling"], false);
    manager
        .detach_socket(&format!("socket-{generation}"), None)
        .await;
    let state = manager.host_state(&owner, "a").await.unwrap();
    assert_eq!(state["connected"], false);
    assert_eq!(state["ready"], false);
    assert!(state["scope"].is_object());
    finish(&manager, responder).await;
}

#[tokio::test]
async fn disconnect_during_recovery_reports_current_cause_without_identity_mismatch() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    let generation = manager.next_connection_generation();
    let socket = format!("socket-{generation}");
    let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
    let attaching = {
        let manager = manager.clone();
        let owner = owner.clone();
        let socket = socket.clone();
        tokio::spawn(async move {
            manager
                .attach_project(
                    &owner,
                    &socket,
                    super::super::bindings::Attach {
                        project_id: "a".into(),
                        descriptor: descriptor("host"),
                        lease_generation: generation.to_string(),
                        allow_legacy_binding: true,
                    },
                    sender,
                    generation,
                )
                .await
        })
    };
    loop {
        match receiver.recv().await.unwrap() {
            HostFrame::Request {
                method: HostRequest::Recovery,
                ..
            } => break,
            HostFrame::Ping { .. } => {}
            _ => panic!("only inspection is allowed during attachment"),
        }
    }
    manager.detach_socket(&socket, Some("a")).await;
    let attached = tokio::time::timeout(std::time::Duration::from_secs(3), attaching)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(attached["connection_generation"], generation.to_string());
    assert_eq!(attached["connected"], false);
    assert_eq!(attached["ready"], false);
    assert_eq!(attached["reconciling"], false);
    assert_eq!(attached["readiness"]["phase"], "disconnected");
    assert!(
        attached["readiness"]["cause"]["message"]
            .as_str()
            .is_some_and(|message| !message.is_empty())
    );
    assert_eq!(
        attached["readiness"]["cause"],
        attached["reconciliation_error"]
    );
    assert_eq!(
        manager.host_state(&owner, "a").await.unwrap()["readiness"],
        attached["readiness"]
    );
    manager.close().await.unwrap();
}

#[tokio::test]
async fn bridge_diagnostics_require_current_owner_socket_generation_and_lease() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    let bridge = descriptor("host");
    let bridge_instance_id = bridge.bridge_instance_id.clone();
    let (_, generation, responder) = attach(&manager, &owner, "a", bridge, false).await;
    let socket = format!("socket-{generation}");
    let event = microcrab_host_protocol::BridgeDiagnostic {
        event_id: uuid::Uuid::new_v4().to_string(),
        timestamp_ms: 123,
        bridge_instance_id,
        request_id: Some(uuid::Uuid::new_v4().to_string()),
        operation_id: Some(uuid::Uuid::new_v4().to_string()),
        lease_generation: generation.to_string(),
        method: Some("read".into()),
        phase: microcrab_host_protocol::BridgeDiagnosticPhase::OperationFailed,
        error_code: Some(ErrorCode::Io),
        elapsed_ms: Some(1),
    };
    let frame = || HostFrame::Diagnostic {
        event: event.clone(),
    };
    manager
        .receive_host(&owner, &socket, "a", generation, frame())
        .await
        .unwrap();
    let state = manager.host_state(&owner, "a").await.unwrap();
    assert_eq!(state["diagnostics"][0]["event"]["event_id"], event.event_id);
    assert_eq!(state["ready"], true);
    assert!(
        manager
            .receive_host(
                &Principal::for_test("intruder"),
                &socket,
                "a",
                generation,
                frame()
            )
            .await
            .is_err()
    );
    assert!(
        manager
            .receive_host(&owner, "old-socket", "a", generation, frame())
            .await
            .is_err()
    );
    assert!(
        manager
            .receive_host(&owner, &socket, "a", generation + 1, frame())
            .await
            .is_err()
    );
    let mut stale = event.clone();
    stale.lease_generation = "expired-lease".into();
    assert!(
        manager
            .receive_host(
                &owner,
                &socket,
                "a",
                generation,
                HostFrame::Diagnostic { event: stale }
            )
            .await
            .is_err()
    );
    assert_eq!(
        manager.host_state(&owner, "a").await.unwrap()["diagnostics"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(
        manager
            .host_state(&Principal::for_test("intruder"), "a")
            .await
            .is_err()
    );
    let mut restarted = descriptor("host");
    restarted.bridge_instance_id = "new-instance".into();
    let (_, next_generation, next_responder) =
        attach(&manager, &owner, "a", restarted, false).await;
    let next_socket = format!("socket-{next_generation}");
    let mut current = event.clone();
    current.lease_generation = next_generation.to_string();
    assert!(
        manager
            .receive_host(
                &owner,
                &next_socket,
                "a",
                next_generation,
                HostFrame::Diagnostic {
                    event: current.clone()
                }
            )
            .await
            .is_err()
    );
    current.bridge_instance_id = "new-instance".into();
    current.event_id = uuid::Uuid::new_v4().to_string();
    manager
        .receive_host(
            &owner,
            &next_socket,
            "a",
            next_generation,
            HostFrame::Diagnostic { event: current },
        )
        .await
        .unwrap();
    assert!(
        manager
            .receive_host(&owner, &socket, "a", generation, frame())
            .await
            .is_err()
    );
    let state = manager.host_state(&owner, "a").await.unwrap();
    assert_eq!(state["diagnostics"].as_array().unwrap().len(), 2);
    assert_eq!(
        state["diagnostics"][0]["event"]["bridge_instance_id"],
        "new-instance"
    );
    assert_eq!(state["ready"], true);
    responder.abort();
    let _ = responder.await;
    finish(&manager, next_responder).await;
}

#[tokio::test]
async fn concurrent_same_owner_canonical_identity_deduplicates_before_route_publication() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    provisional(&manager, &owner, "first").await;
    provisional(&manager, &owner, "second").await;
    let (left, right) = tokio::join!(
        attach(&manager, &owner, "first", descriptor("host"), false),
        attach(&manager, &owner, "second", descriptor("host"), false)
    );
    let (winner, loser) = if left.0["connected"] == true {
        (&left, &right)
    } else {
        (&right, &left)
    };
    assert_eq!(winner.0["ready"], true);
    assert_eq!(loser.0["status"], "existing_project");
    assert_eq!(loser.0["existing_project_id"], winner.0["project_id"]);
    assert_eq!(loser.0["connected"], false);
    let winner_id = winner.0["project_id"].as_str().unwrap();
    let loser_id = loser.0["project_id"].as_str().unwrap();
    assert!(!manager.projects.find(winner_id).await.unwrap().provisional);
    assert!(manager.projects.find(loser_id).await.unwrap().provisional);
    assert!(manager.pins.project(loser_id).await.unwrap().is_none());
    assert!(!manager.routes.lock().await.contains_key(loser_id));
    manager.delete_provisional(&owner, loser_id).await.unwrap();
    manager.close().await.unwrap();
    left.2.abort();
    right.2.abort();
    let _ = left.2.await;
    let _ = right.2.await;
}

#[tokio::test]
async fn deleted_project_pin_does_not_block_a_new_project_for_the_same_workspace() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    provisional(&manager, &owner, "old").await;
    let (old, _, old_responder) = attach(&manager, &owner, "old", descriptor("host"), false).await;
    assert_eq!(old["ready"], true);
    let old_pin = serde_json::to_value(manager.pins.project("old").await.unwrap()).unwrap();
    commands::execute(
        &manager,
        &owner,
        &command("project.delete", json!({"project_id":"old"})),
    )
    .await
    .unwrap();
    provisional(&manager, &owner, "replacement").await;
    let (replacement, _, responder) =
        attach(&manager, &owner, "replacement", descriptor("host"), false).await;
    assert_eq!(replacement["ready"], true);
    assert_eq!(replacement["project_id"], "replacement");
    assert_eq!(
        serde_json::to_value(manager.pins.project("old").await.unwrap()).unwrap(),
        old_pin
    );
    assert!(manager.pins.project("replacement").await.unwrap().is_some());
    manager.close().await.unwrap();
    old_responder.abort();
    responder.abort();
    let _ = old_responder.await;
    let _ = responder.await;
}

#[tokio::test]
async fn same_host_distinct_directories_and_other_owner_are_not_duplicates() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    provisional(&manager, &owner, "first").await;
    provisional(&manager, &owner, "second").await;
    let (first, _, first_responder) =
        attach(&manager, &owner, "first", descriptor("host"), false).await;
    let mut other = descriptor("host");
    other.workspace_id = "other-workspace".into();
    other.workspace_root = "/bridge/other".into();
    other.descriptor_digest = microcrab_host_protocol::descriptor_fingerprint(&other).unwrap();
    let (second, _, second_responder) = attach(&manager, &owner, "second", other, false).await;
    assert_eq!(first["ready"], true);
    assert_eq!(second["ready"], true);
    let second_owner = Principal::for_test("second-owner");
    provisional(&manager, &second_owner, "foreign-user").await;
    let (foreign, _, foreign_responder) = attach(
        &manager,
        &second_owner,
        "foreign-user",
        descriptor("host"),
        false,
    )
    .await;
    assert_eq!(foreign["ready"], true);
    assert!(manager.host_state(&second_owner, "first").await.is_err());
    manager.close().await.unwrap();
    first_responder.abort();
    second_responder.abort();
    foreign_responder.abort();
    let _ = first_responder.await;
    let _ = second_responder.await;
    let _ = foreign_responder.await;
}

#[tokio::test]
async fn unbound_legacy_sessions_cannot_adopt_remote_identity_even_with_explicit_compatibility() {
    let root = tempfile::tempdir().unwrap();
    let configuration = config(root.path());
    let embedding = Manager::with_host(configuration.clone(), Arc::new(TestHost))
        .await
        .unwrap();
    let managed = create(&embedding, "a").await;
    let record = managed.record.lock().await.clone();
    embedding.close().await.unwrap();
    drop(managed);
    drop(embedding);
    let manager = Manager::new(configuration).await.unwrap();
    let owner = Principal::for_test("owner");
    let (sender, _receiver) = tokio::sync::mpsc::channel(32);
    let error = manager
        .attach_project(
            &owner,
            "legacy-setup",
            super::super::bindings::Attach {
                project_id: "a".into(),
                descriptor: descriptor("host"),
                lease_generation: "legacy-lease".into(),
                allow_legacy_binding: true,
            },
            sender,
            manager.next_connection_generation(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "binding_mismatch");
    assert!(error.message.contains("Legacy sessions cannot adopt"));
    assert!(manager.pins.project("a").await.unwrap().is_none());
    assert!(manager.pins.session(&record.id).await.unwrap().is_none());
    assert!(!manager.routes.lock().await.contains_key("a"));
    assert_eq!(manager.record("owner", &record.id).await.unwrap(), record);
    manager.close().await.unwrap();
}

#[tokio::test]
async fn provisional_cleanup_preserves_grants_and_refuses_pins_routes_sessions_or_old_metadata() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    provisional(&manager, &owner, "discard").await;
    let microcrab_host_protocol::BindingScope::Project(grant) =
        manager.binding_scope("owner", "discard").await.unwrap()
    else {
        panic!("native Project scope expected");
    };
    assert!(
        manager
            .delete_provisional(&Principal::for_test("intruder"), "discard")
            .await
            .is_err()
    );
    manager.delete_provisional(&owner, "discard").await.unwrap();
    assert!(manager.projects.find("discard").await.is_none());
    assert_eq!(
        manager
            .pins
            .grant("discard")
            .await
            .unwrap()
            .unwrap()
            .binding_id,
        grant.binding_id
    );
    manager.editable_project("owner", "a").await.unwrap();
    assert_eq!(
        manager
            .delete_provisional(&owner, "a")
            .await
            .unwrap_err()
            .code,
        "project_not_provisional"
    );
    provisional(&manager, &owner, "routed").await;
    let remote = microcrab_host_remote::RemoteHost::new(descriptor("host")).unwrap();
    manager.routes.lock().await.insert(
        "routed".into(),
        super::super::bindings::Route {
            owner: "owner".into(),
            socket: "fixture".into(),
            generation: 1,
            remote,
            ready: false,
            diagnostics: None,
        },
    );
    assert_eq!(
        manager
            .delete_provisional(&owner, "routed")
            .await
            .unwrap_err()
            .code,
        "project_in_use"
    );
    provisional(&manager, &owner, "pinned").await;
    let d = descriptor("host");
    manager
        .pins
        .pin_project(microcrab_storage::execution::ProjectBinding {
            project_id: "pinned".into(),
            owner: "owner".into(),
            host_id: d.host_id,
            workspace_id: d.workspace_id,
            workspace_root: d.workspace_root,
            descriptor_digest: d.descriptor_digest,
        })
        .await
        .unwrap();
    assert_eq!(
        manager
            .delete_provisional(&owner, "pinned")
            .await
            .unwrap_err()
            .code,
        "project_in_use"
    );
    provisional(&manager, &owner, "history").await;
    let record:SessionRecord=serde_json::from_value(json!({"version":1,"id":"historical","owner":"owner","title":"history","project_id":"history","project_root":"/fixed","agent":null,"mode":null,"permissions":[],"review":false,"revision":0,"archived":true,"created_at_ms":1,"updated_at_ms":1})).unwrap();
    manager.directory.create(record).await.unwrap();
    assert_eq!(
        manager
            .delete_provisional(&owner, "history")
            .await
            .unwrap_err()
            .code,
        "project_in_use"
    );
    let spoof = commands::execute(
        &manager,
        &owner,
        &command(
            "project.create",
            json!({"id":"spoof","name":"spoof","root":"/remote","host_id":"forged"}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(spoof.code, "invalid_params");
    assert!(manager.projects.find("spoof").await.is_none());
    manager.close().await.unwrap();
}

#[tokio::test]
async fn retained_session_host_waits_for_the_active_manager_reconciliation() {
    use microcrab_host_api::ReadRequest;
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    let generation = manager.next_connection_generation();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    let responder_manager = manager.clone();
    let responder_owner = owner.clone();
    let responder = tokio::spawn(async move {
        let mut entered = Some(entered);
        let mut wait = Some(wait);
        let mut recoveries = 0;
        let mut reads = 0;
        while let Some(frame) = receiver.recv().await {
            let reply = match frame {
                HostFrame::Request {
                    request_id,
                    operation_id,
                    method,
                    ..
                } => {
                    let value = match method {
                        HostRequest::Recovery => {
                            recoveries += 1;
                            if recoveries == 2 {
                                entered.take().unwrap().send(()).unwrap();
                                wait.take().unwrap().await.unwrap();
                            }
                            json!([])
                        }
                        HostRequest::Read(_) => {
                            reads += 1;
                            assert_eq!(recoveries, 2);
                            assert_eq!(reads, 1, "an unadmitted tool waits; it is never replayed");
                            json!({"content":"retained Host reply","start_line":1,"next_line":null,"truncated":false,"version":"fixture"})
                        }
                        _ => panic!("unexpected effect during reconciliation"),
                    };
                    HostFrame::Response {
                        request_id,
                        operation_id,
                        result: Ok(value),
                    }
                }
                HostFrame::Ping { lease_generation } => HostFrame::Pong { lease_generation },
                _ => continue,
            };
            if responder_manager
                .receive_host(&responder_owner, "active-pass", "a", generation, reply)
                .await
                .is_err()
            {
                break;
            }
        }
    });
    manager
        .attach_project(
            &owner,
            "active-pass",
            super::super::bindings::Attach {
                project_id: "a".into(),
                descriptor: descriptor("host"),
                lease_generation: "lease".into(),
                allow_legacy_binding: true,
            },
            sender,
            generation,
        )
        .await
        .unwrap();
    // A running Session retains its Host; creation admission is not repeated for
    // each later tool. Synchronization therefore belongs to RemoteHost.
    let retained = manager.project_host(&owner.owner, "a").await.unwrap();
    let reconcile = {
        let manager = manager.clone();
        let owner = owner.clone();
        tokio::spawn(async move { manager.reconcile_host(&owner, "a", None).await })
    };
    started.await.unwrap();
    let call = retained.read_text(
        ReadRequest {
            path: "fixture".into(),
            offset_line: 1,
            max_lines: 1,
            max_bytes: 128,
        },
        tokio_util::sync::CancellationToken::new(),
    );
    tokio::pin!(call);
    tokio::select! { biased;
        result = &mut call => panic!("active reconciliation must be awaited: {result:?}"),
        _ = std::future::ready(()) => {}
    }
    release.send(()).unwrap();
    let output = tokio::time::timeout(std::time::Duration::from_secs(3), &mut call)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.content, "retained Host reply");
    assert_eq!(reconcile.await.unwrap().unwrap()["ready"], true);
    manager.close().await.unwrap();
    responder.abort();
    let _ = responder.await;
}

#[tokio::test]
async fn shutdown_keeps_host_transport_until_accepted_owners_finish() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    let (_, _, responder) = attach(&manager, &owner, "a", descriptor("host"), false).await;
    let remote = manager.routes.lock().await.get("a").unwrap().remote.clone();
    let (release, wait) = tokio::sync::oneshot::channel();
    let (entered, started) = tokio::sync::oneshot::channel();
    let retained = remote.clone();
    let accepted = manager
        .track(async move {
            entered.send(()).unwrap();
            wait.await.unwrap();
            assert!(
                retained.is_connected(),
                "accepted cleanup still owns authenticated transport"
            );
        })
        .unwrap();
    started.await.unwrap();
    let shutting_down = manager.close();
    tokio::pin!(shutting_down);
    // Poll shutdown until it has sealed admission and is waiting on this owner.
    tokio::select! {
        biased;
        result = &mut shutting_down => panic!("shutdown skipped accepted owner: {result:?}"),
        _ = std::future::ready(()) => {}
    }
    tokio::task::yield_now().await;
    assert!(manager.closed.load(Ordering::Acquire));
    assert!(remote.is_connected());
    release.send(()).unwrap();
    accepted.await.unwrap();
    shutting_down.await.unwrap();
    assert!(!remote.is_connected());
    responder.abort();
    let _ = responder.await;
}
