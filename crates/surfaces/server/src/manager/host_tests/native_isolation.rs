//! Native admission safety against protocol peers, including pre-inspection peers.
use super::*;
use crate::manager::direct::{Connect, RelayBytes};
use microcrab_host_protocol::{HostDescriptor, HostFrame, decode_frame, encode_frame};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

async fn relay_attempt(
    manager: &Arc<Manager>,
    project: &str,
    candidate: HostDescriptor,
    legacy: bool,
    changed_hello: Option<HostDescriptor>,
) -> (
    Result<serde_json::Value, microcrab_protocol::application::WireError>,
    usize,
) {
    relay_attempt_with_legacy_binding(manager, project, candidate, legacy, changed_hello, true)
        .await
}

async fn relay_attempt_with_legacy_binding(
    manager: &Arc<Manager>,
    project: &str,
    candidate: HostDescriptor,
    legacy: bool,
    changed_hello: Option<HostDescriptor>,
    allow_legacy_binding: bool,
) -> (
    Result<serde_json::Value, microcrab_protocol::application::WireError>,
    usize,
) {
    let (outbound, mut frames) = mpsc::channel::<Vec<u8>>(16);
    let (reply, inbound) = mpsc::channel::<Vec<u8>>(16);
    let pairs = Arc::new(AtomicUsize::new(0));
    let observed = pairs.clone();
    let id = uuid::Uuid::new_v4();
    let relay = Arc::new(std::sync::Mutex::new(Some(RelayBytes {
        id,
        sender: reply.clone(),
    })));
    let intake = relay.clone();
    let worker = tokio::spawn(async move {
        while let Some(bytes) = frames.recv().await {
            let response = match decode_frame(&bytes).unwrap() {
                HostFrame::Inspect { request } if !legacy => HostFrame::Inspected {
                    request_id: request.request_id,
                    result: Ok(candidate.clone()),
                },
                HostFrame::Inspect { .. } => {
                    *intake.lock().unwrap() = None;
                    break;
                } // old peer rejects the unknown first frame
                HostFrame::Pair { .. } | HostFrame::Resume { .. } => {
                    observed.fetch_add(1, Ordering::SeqCst);
                    HostFrame::Hello {
                        descriptor: changed_hello.clone().unwrap_or_else(|| candidate.clone()),
                        credential: "fixture-resume".into(),
                        lease_generation: "1".into(),
                    }
                }
                HostFrame::Request {
                    request_id,
                    operation_id,
                    method: microcrab_host_protocol::HostRequest::Recovery,
                    ..
                } => HostFrame::Response {
                    request_id,
                    operation_id,
                    result: Ok(serde_json::json!([])),
                },
                HostFrame::Ping { lease_generation } => HostFrame::Pong { lease_generation },
                _ => continue,
            };
            if reply.send(encode_frame(&response).unwrap()).await.is_err() {
                break;
            }
        }
    });
    let result = manager
        .attach_client_relay(
            &Principal::for_test("owner"),
            &format!("browser-{project}"),
            Connect {
                project_id: project.into(),
                url: "ws://127.0.0.1:8788/host".into(),
                token: Some("fixture-token".into()),
                directory: None,
                allow_legacy_binding,
                only_if_unattached: false,
            },
            outbound,
            inbound,
            relay,
            id,
        )
        .await;
    // A successful route owns the loop; failed/duplicate attempts must release it.
    if result.as_ref().is_ok_and(|value| value["ready"] == true) {
        worker.abort();
        let _ = worker.await;
    } else {
        tokio::time::timeout(std::time::Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap();
    }
    (result, pairs.load(Ordering::SeqCst))
}
async fn pin(manager: &Manager, project: &str, descriptor: &HostDescriptor) {
    manager
        .pins
        .pin_project(microcrab_storage::execution::ProjectBinding {
            project_id: project.into(),
            owner: "owner".into(),
            host_id: descriptor.host_id.clone(),
            workspace_id: descriptor.workspace_id.clone(),
            workspace_root: descriptor.workspace_root.clone(),
            descriptor_digest: descriptor.descriptor_digest.clone(),
        })
        .await
        .unwrap();
}
#[tokio::test]
async fn unbound_legacy_project_requires_explicit_consent_before_pair() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let candidate = descriptor("native-host");
    let (result, pairs) =
        relay_attempt_with_legacy_binding(&manager, "a", candidate.clone(), false, None, false)
            .await;
    let error = result.unwrap_err();
    assert_eq!(error.code, "legacy_binding_required");
    assert_eq!(error.details.as_ref().unwrap()["phase"], "pre_auth");
    assert_eq!(
        error.details.as_ref().unwrap()["pairing_token_consumed"],
        false
    );
    assert_eq!(pairs, 0);
    assert!(manager.pins.project("a").await.unwrap().is_none());
    assert!(manager.routes.lock().await.get("a").is_none());
    // Explicit consent still permits an empty legacy Project to bind normally.
    let (result, pairs) = relay_attempt(&manager, "a", candidate.clone(), false, None).await;
    assert_eq!(result.unwrap()["ready"], true);
    assert_eq!(pairs, 1);
    // Reconnecting an already-pinned Project requires no renewed legacy opt-in.
    let (result, pairs) =
        relay_attempt_with_legacy_binding(&manager, "a", candidate, false, None, false).await;
    assert_eq!(result.unwrap()["ready"], true);
    assert_eq!(pairs, 1);
    manager.close().await.unwrap();
}

#[tokio::test]
async fn provisional_project_pairs_without_legacy_binding_consent() {
    let root = tempfile::tempdir().unwrap();
    let configuration = config(root.path());
    crate::project::seed_test_projects(
        &configuration.data_dir,
        json!([
            {"id":"a","name":"new","root":"bridge-only://workspace","owners":["owner"],"provisional":true}
        ]),
    );
    let manager = Manager::new(configuration).await.unwrap();
    let (result, pairs) = relay_attempt_with_legacy_binding(
        &manager,
        "a",
        descriptor("native-host"),
        false,
        None,
        false,
    )
    .await;
    assert_eq!(result.unwrap()["ready"], true);
    assert_eq!(pairs, 1);
    assert!(!manager.projects.find("a").await.unwrap().provisional);
    manager.close().await.unwrap();
}

#[tokio::test]
async fn retained_legacy_sessions_reject_even_explicit_binding_before_pair() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let record: microcrab_storage::SessionRecord = serde_json::from_value(json!({
        "version":1,"id":"legacy-session","owner":"owner","title":"history",
        "project_id":"a","project_root":"/fixed","agent":null,"mode":null,
        "permissions":[],"review":false,"revision":0,"archived":true,
        "created_at_ms":1,"updated_at_ms":1
    }))
    .unwrap();
    manager.directory.create(record).await.unwrap();
    let (result, pairs) =
        relay_attempt(&manager, "a", descriptor("native-host"), false, None).await;
    let error = result.unwrap_err();
    assert_eq!(error.code, "binding_mismatch");
    assert_eq!(error.details.as_ref().unwrap()["phase"], "pre_auth");
    assert_eq!(
        error.details.as_ref().unwrap()["pairing_token_consumed"],
        false
    );
    assert_eq!(pairs, 0);
    assert!(manager.pins.project("a").await.unwrap().is_none());
    assert!(manager.routes.lock().await.get("a").is_none());
    assert!(
        manager
            .directory
            .get("owner", "legacy-session")
            .await
            .unwrap()
            .is_some()
    );
    manager.close().await.unwrap();
}

#[tokio::test]
async fn relay_old_peer_closes_inspection_without_receiving_pair_or_resume() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let (result, pairs) = relay_attempt(&manager, "a", descriptor("native-host"), true, None).await;
    assert_eq!(result.unwrap_err().code, "host_transport_unavailable");
    assert_eq!(pairs, 0);
    assert!(manager.routes.lock().await.get("a").is_none());
    manager.close().await.unwrap();
}
#[tokio::test]
async fn relay_digest_mismatch_and_canonical_duplicate_reject_before_pair() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let original = descriptor("native-host");
    pin(&manager, "a", &original).await;
    let mut upgraded = original.clone();
    upgraded
        .supported_primitives
        .push("filesystem.future".into());
    upgraded.descriptor_digest =
        microcrab_host_protocol::descriptor_fingerprint(&upgraded).unwrap();
    let (result, pairs) = relay_attempt(&manager, "a", upgraded.clone(), false, None).await;
    let error = result.unwrap_err();
    assert_eq!(error.code, "binding_mismatch");
    assert_eq!(
        error.details.as_ref().unwrap()["reason"],
        "host_descriptor_mismatch"
    );
    assert_eq!(
        error.details.as_ref().unwrap()["pairing_token_consumed"],
        false
    );
    assert_eq!(pairs, 0);
    let (result, pairs) = relay_attempt(&manager, "b", upgraded, false, None).await;
    let result = result.unwrap();
    assert_eq!(result["status"], "existing_project");
    assert_eq!(result["existing_project_id"], "a");
    assert_eq!(result["pairing_token_consumed"], false);
    assert!(result["descriptor"].is_object());
    assert_eq!(pairs, 0);
    assert!(manager.pins.project("b").await.unwrap().is_none());
    manager.close().await.unwrap();
}
#[tokio::test]
async fn unbound_project_rejects_changed_hello_with_truthful_pairing_effects() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let inspected = descriptor("native-host");
    let mut changed = inspected.clone();
    changed
        .supported_primitives
        .push("filesystem.future".into());
    changed.descriptor_digest = microcrab_host_protocol::descriptor_fingerprint(&changed).unwrap();
    let (result, pairs) = relay_attempt(&manager, "a", inspected, false, Some(changed)).await;
    let error = result.unwrap_err();
    assert_eq!(error.code, "binding_mismatch");
    assert_eq!(
        error.details.as_ref().unwrap()["pairing_token_consumed"],
        true
    );
    assert_eq!(error.details.as_ref().unwrap()["phase"], "post_auth");
    assert_eq!(pairs, 1);
    assert!(manager.pins.project("a").await.unwrap().is_none());
    assert!(manager.routes.lock().await.get("a").is_none());
    manager.close().await.unwrap();
}
#[tokio::test]
async fn concurrent_relay_candidates_pair_only_the_canonical_winner() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let candidate = descriptor("native-host");
    let ((left, left_pairs), (right, right_pairs)) = tokio::join!(
        relay_attempt(&manager, "a", candidate.clone(), false, None),
        relay_attempt(&manager, "b", candidate, false, None),
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left_pairs + right_pairs, 1);
    let (winner, duplicate) = if left["ready"] == true {
        (&left, &right)
    } else {
        (&right, &left)
    };
    assert_eq!(winner["ready"], true);
    assert_eq!(duplicate["status"], "existing_project");
    assert_eq!(duplicate["existing_project_id"], winner["project_id"]);
    assert_eq!(duplicate["pairing_token_consumed"], false);
    manager.close().await.unwrap();
}

#[tokio::test]
async fn signed_empty_native_identities_are_rejected_before_pair() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    for field in 0..4 {
        let mut candidate = descriptor("native-host");
        match field {
            0 => candidate.host_id.clear(),
            1 => candidate.workspace_id.clear(),
            2 => candidate.bridge_instance_id.clear(),
            _ => candidate.workspace_root.clear(),
        }
        candidate.descriptor_digest =
            microcrab_host_protocol::descriptor_fingerprint(&candidate).unwrap();
        let (result, pairs) = relay_attempt(&manager, "a", candidate, false, None).await;
        assert_eq!(result.unwrap_err().code, "invalid_host");
        assert_eq!(pairs, 0);
    }
    assert!(manager.pins.project("a").await.unwrap().is_none());
    manager.close().await.unwrap();
}
#[cfg(feature = "grpc")]
#[tokio::test]
async fn grpc_old_peer_unimplemented_inspection_never_receives_control() {
    use microcrab_host_protocol::{HostBridge, HostBridgeServer, pb};
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::{Request, Response, Status};
    struct Legacy(Arc<AtomicUsize>);
    #[tonic::async_trait]
    impl HostBridge for Legacy {
        type ControlStream = ReceiverStream<Result<pb::HostFrame, Status>>;
        type ExecuteStream = ReceiverStream<Result<pb::HostFrame, Status>>;
        async fn inspect(
            &self,
            _: Request<pb::HostFrame>,
        ) -> Result<Response<pb::HostFrame>, Status> {
            Err(Status::unimplemented("unknown RPC"))
        }
        async fn control(
            &self,
            _: Request<tonic::Streaming<pb::HostFrame>>,
        ) -> Result<Response<Self::ControlStream>, Status> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(Status::unimplemented(
                "legacy controller should never be contacted",
            ))
        }
        async fn execute(
            &self,
            _: Request<pb::HostFrame>,
        ) -> Result<Response<Self::ExecuteStream>, Status> {
            Err(Status::unimplemented("no controller"))
        }
    }
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let controls = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let legacy = Legacy(controls.clone());
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(HostBridgeServer::new(legacy))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let error = manager
        .connect_direct_host(
            &Principal::for_test("owner"),
            Connect {
                project_id: "a".into(),
                url,
                token: Some("one-use".into()),
                directory: None,
                allow_legacy_binding: true,
                only_if_unattached: false,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "host_preflight_unsupported");
    assert_eq!(
        error.details.as_ref().unwrap()["pairing_token_consumed"],
        false
    );
    assert_eq!(controls.load(Ordering::SeqCst), 0);
    assert!(manager.routes.lock().await.get("a").is_none());
    manager.close().await.unwrap();
    server.abort();
    let _ = server.await;
}
