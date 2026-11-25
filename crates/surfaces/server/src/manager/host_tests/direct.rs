//! gRPC is the only server-dialed native carrier. WebSocket addresses stay a client relay.
use super::*;
use microcrab_host_protocol::HostFrame;
use tracing::{Instrument, instrument::WithSubscriber};

#[derive(Clone, Default)]
struct HostLogCapture(Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for HostLogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn capture_host_logs() -> (HostLogCapture, tracing::subscriber::DefaultGuard) {
    let capture = HostLogCapture::default();
    let writer = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(move || writer.clone())
        .without_time()
        .with_ansi(false)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (capture, guard)
}
fn native_descriptor() -> microcrab_host_protocol::HostDescriptor {
    let mut value = descriptor("native-host");
    value.bridge_instance_id = "fd3b1876-791e-4d1b-9194-54ea0372beab".into();
    value
}

#[tokio::test]
async fn websocket_address_is_a_client_relay_and_is_not_dialed() {
    let root = tempfile::tempdir().unwrap();
    let configuration = config(root.path());
    let manager = Manager::new(configuration.clone()).await.unwrap();
    let owner = Principal::for_test("owner");
    let error = manager
        .connect_direct_host(
            &owner,
            super::super::direct::Connect {
                project_id: "a".into(),
                url: "ws://127.0.0.1:9/host".into(),
                token: Some("native-pair".into()),
                directory: None,
                allow_legacy_binding: true,
                only_if_unattached: false,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "host_transport_unavailable");
    assert!(
        error.message.contains("host.relay_attach"),
        "{}",
        error.message
    );
    let path = configuration.data_dir.join("host-connections.json");
    std::fs::write(
        &path,
        r#"{"a":{"owner":"owner","url":"ws://127.0.0.1:8788/host","directory":null,"credential":"secret-resume"}}"#,
    )
    .unwrap();
    let refresh = manager
        .refresh_direct_host(&owner, "a".into())
        .await
        .unwrap_err();
    assert_eq!(refresh.code, "socket_required");
    assert!(!refresh.message.contains("secret-resume"));
    manager.close().await.unwrap();
}

#[tokio::test]
async fn canonical_credential_directory_requires_exact_owned_project_pin() {
    let root = tempfile::tempdir().unwrap();
    let configuration = config(root.path());
    let manager = Manager::new(configuration.clone()).await.unwrap();
    let owner = Principal::for_test("owner");
    let url = "http://127.0.0.1:8789";
    let descriptor = native_descriptor();
    let credential_path = configuration.data_dir.join("host-connections.json");
    let saved = serde_json::to_vec(&serde_json::json!({
        "a": {"owner":"owner","url":url,"directory":".","credential":"fixture-resume-a"},
        "b": {"owner":"owner","url":url,"directory":".","credential":"fixture-resume-b"}
    }))
    .unwrap();
    std::fs::write(&credential_path, &saved).unwrap();
    let parameters = |project: &str, directory: Option<String>| super::super::direct::Connect {
        project_id: project.into(),
        url: url.into(),
        token: None,
        directory,
        allow_legacy_binding: false,
        only_if_unattached: false,
    };
    let canonical = parameters("a", Some(descriptor.workspace_root.clone()));
    assert!(
        manager
            .matching_credential(&owner, &canonical, url)
            .await
            .unwrap()
            .is_none(),
        "an unpinned Project cannot certify canonical correspondence"
    );
    assert!(
        manager
            .matching_credential(&owner, &parameters("a", Some(".".into())), url)
            .await
            .unwrap()
            .is_some(),
        "literal directory matching remains valid before a pin exists"
    );
    let pin = microcrab_storage::execution::ProjectBinding {
        project_id: "a".into(),
        owner: owner.owner.clone(),
        host_id: descriptor.host_id,
        workspace_id: descriptor.workspace_id,
        workspace_root: descriptor.workspace_root,
        descriptor_digest: descriptor.descriptor_digest,
    };
    manager.pins.pin_project(pin.clone()).await.unwrap();
    assert!(
        manager
            .matching_credential(&owner, &canonical, url)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        manager
            .matching_credential(&Principal::for_test("intruder"), &canonical, url)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        manager
            .matching_credential(&owner, &canonical, "http://127.0.0.1:8790")
            .await
            .unwrap()
            .is_none()
    );
    for directory in [
        Some("another-workspace".into()),
        Some(format!("{}/.", pin.workspace_root)),
        None,
    ] {
        assert!(
            manager
                .matching_credential(&owner, &parameters("a", directory), url)
                .await
                .unwrap()
                .is_none(),
            "only the exact canonical root may replace the saved directory"
        );
    }
    let other_project = parameters("b", canonical.directory.clone());
    assert!(
        manager
            .matching_credential(&owner, &other_project, url)
            .await
            .unwrap()
            .is_none(),
        "another Project cannot borrow the existing pin"
    );
    manager
        .pins
        .pin_project(microcrab_storage::execution::ProjectBinding {
            project_id: "b".into(),
            owner: "intruder".into(),
            ..pin
        })
        .await
        .unwrap();
    assert!(
        manager
            .matching_credential(&owner, &other_project, url)
            .await
            .unwrap()
            .is_none(),
        "a foreign owner's pin cannot certify directory correspondence"
    );
    assert!(
        std::fs::read(&credential_path).unwrap() == saved,
        "matching must not rewrite saved credentials"
    );
    manager.close().await.unwrap();
}

pub(super) async fn attach_relay_fixture(
    manager: &Arc<Manager>,
    socket: &str,
) -> (
    super::super::direct::RelayRelease,
    tokio::task::JoinHandle<()>,
) {
    attach_relay_fixture_with_features(manager, socket, false, None, Default::default(), false)
        .await
}

async fn attach_relay_fixture_with_features(
    manager: &Arc<Manager>,
    socket: &str,
    pair_with_token: bool,
    expected_offer: Option<microcrab_host_protocol::WebTransportFeatures>,
    acknowledgment: microcrab_host_protocol::WebTransportFeatures,
    changed_instance: bool,
) -> (
    super::super::direct::RelayRelease,
    tokio::task::JoinHandle<()>,
) {
    use super::super::direct::{Connect, RelayBytes};
    use microcrab_host_protocol::{
        decode_frame, decode_frame_with_web_transport_features, encode_frame,
        encode_frame_with_web_transport_features,
    };
    use tokio::sync::mpsc;
    let id = uuid::Uuid::new_v4();
    let (outbound, mut frames) = mpsc::channel::<Vec<u8>>(64);
    let (reply, inbound) = mpsc::channel::<Vec<u8>>(64);
    let relay = Arc::new(std::sync::Mutex::new(Some(RelayBytes {
        id,
        sender: reply.clone(),
    })));
    let task = tokio::spawn(async move {
        let (inspect, offer) =
            decode_frame_with_web_transport_features(&frames.recv().await.unwrap()).unwrap();
        assert_eq!(
            offer,
            Default::default(),
            "inspection never offers web transport"
        );
        let HostFrame::Inspect { request } = inspect else {
            panic!("Inspect required")
        };
        reply
            .send(
                encode_frame(&HostFrame::Inspected {
                    request_id: request.request_id,
                    result: Ok(native_descriptor()),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let (first, offer) =
            decode_frame_with_web_transport_features(&frames.recv().await.unwrap()).unwrap();
        assert!(matches!(
            first,
            HostFrame::Pair { .. } | HostFrame::Resume { .. }
        ));
        if let Some(expected_offer) = expected_offer {
            assert_eq!(offer, expected_offer);
            assert_eq!(
                matches!(first, HostFrame::Resume { .. }),
                !pair_with_token && expected_offer.version.is_some(),
            );
        }
        let mut descriptor = native_descriptor();
        if changed_instance {
            descriptor.bridge_instance_id = uuid::Uuid::new_v4().to_string();
        }
        reply
            .send(
                encode_frame_with_web_transport_features(
                    &HostFrame::Hello {
                        descriptor,
                        credential: "relay-resume".into(),
                        lease_generation: "1".into(),
                    },
                    acknowledgment,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        while let Some(bytes) = frames.recv().await {
            let response = match decode_frame(&bytes).unwrap() {
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
            socket,
            Connect {
                project_id: "a".into(),
                url: "ws://127.0.0.1:8788/host".into(),
                token: pair_with_token.then(|| "synthetic-pair-token".into()),
                directory: None,
                allow_legacy_binding: true,
                only_if_unattached: false,
            },
            outbound,
            inbound,
            relay,
            id,
        )
        .instrument(tracing::info_span!(
            "host_lifecycle_test",
            request_id = "host-test-relay"
        ))
        .await
        .unwrap();
    assert_eq!(result["ready"], true);
    let witness = serde_json::from_value(result["relay_release"].clone()).unwrap();
    (witness, task)
}
pub(super) async fn assert_web_transport_features(
    manager: &Manager,
    expected_v1: bool,
    expected_pull: bool,
) {
    use microcrab_host_api::{
        OpContext,
        web_transport::{ConnectionScope, WebTransportError},
    };
    use microcrab_host_protocol::BindingScope;

    let host = manager.project_host("owner", "a").await.unwrap();
    assert_eq!(host.supports_web_transport(), expected_v1);
    let BindingScope::Project(binding) = manager.binding_scope("owner", "a").await.unwrap() else {
        panic!("fixture requires a Project binding");
    };
    if expected_v1 {
        assert_eq!(host.web_transport_binding().as_ref(), Some(&binding));
    }
    let scope = ConnectionScope {
        binding,
        session_id: "test-session".into(),
        epoch: 1,
        connection_id: "not-opened".into(),
    };
    // A proven pull reaches the exact-open fence; unsupported routes must
    // reject before checking that fence, without dispatching a pull.
    assert_eq!(
        host.ws_pull(&OpContext::default(), &scope, 1, 0).await,
        Err(if expected_pull {
            WebTransportError::Stale
        } else {
            WebTransportError::Unsupported
        })
    );
}

#[tokio::test]
async fn browser_relay_web_transport_requires_matching_authenticated_ack_for_each_route() {
    use microcrab_host_protocol::{WEB_TRANSPORT_V1, WebTransportFeatures};

    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let absent = WebTransportFeatures::default();
    let v1 = WebTransportFeatures::V1;
    let legacy_v1 = WebTransportFeatures {
        version: v1.version,
        ws_pull: false,
    };
    let wrong_version = WebTransportFeatures {
        version: Some(WEB_TRANSPORT_V1 + 1),
        ws_pull: true,
    };
    let pull_only = WebTransportFeatures {
        version: None,
        ws_pull: true,
    };
    let mut peers = Vec::new();
    for (socket, pair_with_token, offer, ack, changed_instance, expected_v1, expected_pull) in [
        ("unsolicited", false, absent, v1, false, false, false),
        ("v1-pair", true, v1, v1, false, true, true),
        ("legacy-v1-resume", false, v1, legacy_v1, false, true, false),
        ("old-peer-resume", false, v1, absent, false, false, false),
        ("v1-resume", false, v1, v1, false, true, true),
        (
            "wrong-version",
            false,
            v1,
            wrong_version,
            false,
            false,
            false,
        ),
        ("pull-only-ack", false, v1, pull_only, false, false, false),
        ("changed-instance", false, v1, v1, true, false, false),
    ] {
        let (_release, task) = attach_relay_fixture_with_features(
            &manager,
            socket,
            pair_with_token,
            Some(offer),
            ack,
            changed_instance,
        )
        .await;
        assert_web_transport_features(&manager, expected_v1, expected_pull).await;
        // Leave the live route installed so the next attach must replace its
        // capability, rather than observing a clean, detached RemoteHost.
        peers.push(task);
    }
    manager.close().await.unwrap();
    for peer in peers {
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn stale_relay_cleanup_cannot_stop_another_socket_or_attachment() {
    use super::super::direct::Connect;
    let (capture, _subscriber) = capture_host_logs();
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let (old_release, old_task) = attach_relay_fixture(&manager, "browser-a").await;
    let (new_release, new_task) = attach_relay_fixture(&manager, "browser-b").await;
    let old_id = old_release.relay_id;
    let new_id = new_release.relay_id;
    let principal = Principal::for_test("owner");
    let view = manager.host_state(&principal, "a").await.unwrap();
    assert_eq!(view["carrier_mode"], "browser_relay");
    assert_eq!(view["connection_generation"], new_release.relay_generation);
    assert_eq!(
        view["readiness"]["connection_generation"],
        new_release.relay_generation
    );
    assert_eq!(view["ready"], true);
    assert!(!view.to_string().contains(&new_id.to_string()));
    let (outbound, mut bridge) = tokio::sync::mpsc::channel(1);
    let (_reply, inbound) = tokio::sync::mpsc::channel(1);
    let relay = Arc::new(std::sync::Mutex::new(None));
    let error = manager
        .attach_client_relay(
            &principal,
            "browser-c",
            Connect {
                project_id: "a".into(),
                url: "ws://127.0.0.1:8788/host".into(),
                token: None,
                directory: None,
                allow_legacy_binding: false,
                only_if_unattached: true,
            },
            outbound,
            inbound,
            relay,
            uuid::Uuid::new_v4(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "host_route_observation_changed");
    assert!(
        bridge.recv().await.is_none(),
        "automatic resume must not send Inspect or Pair"
    );
    let host = manager.routes.lock().await.get("a").unwrap().remote.clone();
    let generation = manager.routes.lock().await.get("a").unwrap().generation;
    for witness in [
        old_release,
        super::super::direct::RelayRelease {
            relay_id: new_id,
            relay_generation: (generation + 1).to_string(),
        },
    ] {
        let release = manager
            .disconnect_direct_host(
                &Principal::for_test("owner"),
                super::super::direct::Disconnect {
                    project_id: "a".into(),
                    revoke: true,
                    relay_release: Some(witness),
                },
            )
            .await
            .unwrap();
        assert_eq!(release["disconnected"], false);
        assert!(host.is_connected());
        assert_eq!(
            manager.routes.lock().await.get("a").unwrap().generation,
            generation
        );
    }
    manager.stop_client_relay("a", "browser-a", old_id).await;
    assert!(
        host.is_connected(),
        "an old browser cannot stop the replacement"
    );
    manager.stop_client_relay("a", "browser-b", old_id).await;
    assert!(
        host.is_connected(),
        "an old id cannot stop a newer attachment on the same socket"
    );
    let released = manager
        .disconnect_direct_host(
            &Principal::for_test("owner"),
            super::super::direct::Disconnect {
                project_id: "a".into(),
                revoke: false,
                relay_release: Some(new_release),
            },
        )
        .await
        .unwrap();
    assert_eq!(released["disconnected"], true);
    assert_eq!(released["detach_requested"], false);
    assert!(!host.is_connected());
    assert!(
        config(root.path())
            .data_dir
            .join("host-connections.json")
            .exists()
    );
    manager.close().await.unwrap();
    old_task.abort();
    new_task.abort();
    let _ = old_task.await;
    let _ = new_task.await;
    let log = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    assert!(log.contains("host.relay_attach"), "{log}");
    assert!(log.contains("host.route_changed"), "{log}");
    assert!(
        log.contains("fd3b1876-791e-4d1b-9194-54ea0372beab"),
        "{log}"
    );
    assert!(log.contains("lease_generation=\"1\""), "{log}");
    assert!(
        log.lines().any(|line| line.contains("host.worker_stopped")
            && line.contains("stop_cancelled")
            && line.contains("host-test-relay")),
        "{log}"
    );
    for secret in ["relay-resume", "ws://127.0.0.1:8788/host"] {
        assert!(!log.contains(secret), "{log}");
    }
}

#[tokio::test]
async fn cancelled_relay_publication_keeps_exact_cleanup_witness() {
    let (capture, _subscriber) = capture_host_logs();
    use super::super::direct::{Connect, RelayBytes};
    use microcrab_host_protocol::{decode_frame, encode_frame};
    use tokio::sync::{mpsc, oneshot};
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let id = uuid::Uuid::new_v4();
    let (outbound, mut frames) = mpsc::channel::<Vec<u8>>(64);
    let (reply, inbound) = mpsc::channel::<Vec<u8>>(64);
    let relay = Arc::new(std::sync::Mutex::new(Some(RelayBytes {
        id,
        sender: reply.clone(),
    })));
    let (reconciling, observed) = oneshot::channel();
    let bridge = tokio::spawn(async move {
        let first = decode_frame(&frames.recv().await.unwrap()).unwrap();
        let HostFrame::Inspect { request } = first else {
            panic!("Inspect required before Pair");
        };
        assert!(!request.request_id.is_empty());
        let mut authenticated = native_descriptor();
        authenticated.bridge_instance_id = "private-instance-body".into();
        reply
            .send(
                encode_frame(&HostFrame::Inspected {
                    request_id: request.request_id,
                    result: Ok(authenticated.clone()),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let handshake = decode_frame(&frames.recv().await.unwrap()).unwrap();
        assert!(matches!(
            handshake,
            HostFrame::Pair { expected_descriptor_digest: Some(ref digest), expected_workspace_root: Some(ref root), .. }
                if digest == &authenticated.descriptor_digest && root == &authenticated.workspace_root
        ));
        reply
            .send(
                encode_frame(&HostFrame::Hello {
                    descriptor: authenticated,
                    credential: "relay-resume".into(),
                    lease_generation: "private-lease-body".into(),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        while let Some(bytes) = frames.recv().await {
            if matches!(
                decode_frame(&bytes).unwrap(),
                HostFrame::Request {
                    method: microcrab_host_protocol::HostRequest::Recovery,
                    ..
                }
            ) {
                let _ = reconciling.send(());
                // Keep the intake live and leave attach_project awaiting recovery.
                while frames.recv().await.is_some() {}
                break;
            }
        }
    });
    let attaching = {
        let manager = manager.clone();
        let relay = relay.clone();
        tokio::spawn(
            async move {
                manager
                    .attach_client_relay(
                        &Principal::for_test("owner"),
                        "browser-a",
                        Connect {
                            project_id: "a".into(),
                            url: "ws://127.0.0.1:8788/host".into(),
                            token: None,
                            directory: None,
                            allow_legacy_binding: true,
                            only_if_unattached: false,
                        },
                        outbound,
                        inbound,
                        relay,
                        id,
                    )
                    .await
            }
            .with_current_subscriber(),
        )
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), observed)
        .await
        .unwrap()
        .unwrap();
    let remote = manager.routes.lock().await.get("a").unwrap().remote.clone();
    assert!(remote.is_connected());
    attaching.abort();
    assert!(attaching.await.unwrap_err().is_cancelled());
    assert!(
        relay.lock().unwrap().is_none(),
        "canceled attach clears its byte slot"
    );
    manager.stop_client_relay("a", "browser-a", id).await;
    assert!(
        !remote.is_connected(),
        "the retained attempt identity must stop the published worker immediately"
    );
    manager.close().await.unwrap();
    bridge.abort();
    let _ = bridge.await;
    let log = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    assert!(log.contains("host.relay_attach"), "{log}");
    assert!(log.contains("lease_generation=\"invalid\""), "{log}");
    for secret in [
        "private-instance-body",
        "private-lease-body",
        "relay-resume",
    ] {
        assert!(!log.contains(secret), "{log}");
    }
}

#[cfg(not(feature = "grpc"))]
#[tokio::test]
async fn grpc_address_requires_the_grpc_carrier() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let error = manager
        .connect_direct_host(
            &Principal::for_test("owner"),
            super::super::direct::Connect {
                project_id: "a".into(),
                url: "http://127.0.0.1:9".into(),
                token: Some("native-pair".into()),
                directory: None,
                allow_legacy_binding: true,
                only_if_unattached: false,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "host_transport_unavailable");
    assert!(
        error
            .message
            .contains("does not include the gRPC host carrier"),
        "{}",
        error.message
    );
    manager.close().await.unwrap();
}

#[cfg(feature = "grpc")]
#[tokio::test]
async fn grpc_direct_host_survives_ui_disconnect_transfers_and_resumes_after_restart() {
    let (capture, _subscriber) = capture_host_logs();
    use microcrab_host_api::{OpContext, PrivateFileBuffer};
    use microcrab_host_protocol::{
        GRPC_EXECUTE_TOKEN_HEADER, GRPC_PRIVATE_FILE_HEADER, GRPC_PRIVATE_FILE_V1, HostBridge,
        HostBridgeServer, HostRequest, TransferRef, WebTransportFeatures, frame_from_proto,
        frame_from_proto_with_web_transport_features, frame_to_proto,
        frame_to_proto_with_web_transport_features,
        pb::{
            self,
            private_file_frame::Body,
            private_files_server::{PrivateFiles, PrivateFilesServer},
        },
    };
    use sha2::{Digest, Sha256};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::{Request, Response, Status};

    #[derive(Clone)]
    struct Mock {
        attempts: Arc<AtomicUsize>,
        detaches: Arc<AtomicUsize>,
    }
    #[tonic::async_trait]
    impl HostBridge for Mock {
        type ControlStream = ReceiverStream<Result<microcrab_host_protocol::pb::HostFrame, Status>>;
        type ExecuteStream = ReceiverStream<Result<microcrab_host_protocol::pb::HostFrame, Status>>;
        async fn inspect(
            &self,
            request: Request<microcrab_host_protocol::pb::HostFrame>,
        ) -> Result<Response<microcrab_host_protocol::pb::HostFrame>, Status> {
            assert_eq!(
                request
                    .metadata()
                    .get("origin")
                    .and_then(|value| value.to_str().ok()),
                Some("http://127.0.0.1:4096")
            );
            let HostFrame::Inspect { request } = frame_from_proto(request.into_inner()).unwrap()
            else {
                panic!("Inspect required")
            };
            assert!(matches!(
                request.authentication,
                microcrab_host_protocol::InspectAuthentication::Pair { .. }
                    | microcrab_host_protocol::InspectAuthentication::Resume { .. }
            ));
            Ok(Response::new(
                frame_to_proto(&HostFrame::Inspected {
                    request_id: request.request_id,
                    result: Ok(native_descriptor()),
                })
                .unwrap(),
            ))
        }
        async fn control(
            &self,
            request: Request<tonic::Streaming<microcrab_host_protocol::pb::HostFrame>>,
        ) -> Result<Response<Self::ControlStream>, Status> {
            assert_eq!(
                request.metadata().get(GRPC_PRIVATE_FILE_HEADER),
                Some(&GRPC_PRIVATE_FILE_V1.parse().unwrap())
            );
            assert_eq!(
                request
                    .metadata()
                    .get("origin")
                    .and_then(|value| value.to_str().ok()),
                Some("http://127.0.0.1:4096")
            );
            let mut inbound = request.into_inner();
            let first = inbound.message().await.unwrap().expect("handshake frame");
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            let (handshake, offer) = frame_from_proto_with_web_transport_features(first).unwrap();
            assert_eq!(
                offer,
                if attempt == 3 {
                    WebTransportFeatures::default()
                } else {
                    WebTransportFeatures::V1
                }
            );
            if attempt == 0 {
                assert!(matches!(
                    handshake,
                    HostFrame::Pair { token, directory, .. }
                        if token == "native-pair" && directory.as_deref() == Some(".")
                ));
            } else if attempt == 3 {
                assert!(matches!(handshake, HostFrame::Pair { token, .. } if token.is_empty()));
            } else {
                assert!(matches!(
                    handshake,
                    HostFrame::Resume { credential, .. } if credential == "private-native-resume"
                ));
            }
            let (outbound, inbound_rx) = mpsc::channel(8);
            let hello = HostFrame::Hello {
                descriptor: native_descriptor(),
                credential: "private-native-resume".into(),
                lease_generation: "1".into(),
            };
            let acknowledgment = match attempt {
                0 => WebTransportFeatures {
                    version: offer.version,
                    ws_pull: false,
                },
                1 => offer,
                _ => WebTransportFeatures::default(),
            };
            outbound
                .send(Ok(frame_to_proto_with_web_transport_features(
                    &hello,
                    acknowledgment,
                )
                .unwrap()))
                .await
                .unwrap();
            let detaches = self.detaches.clone();
            tokio::spawn(async move {
                while let Ok(Some(proto)) = inbound.message().await {
                    let frame = frame_from_proto(proto).unwrap();
                    match frame {
                        HostFrame::Ping { lease_generation } => {
                            let pong = HostFrame::Pong { lease_generation };
                            if outbound
                                .send(Ok(frame_to_proto(&pong).unwrap()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        HostFrame::Cancel { .. } => {}
                        HostFrame::Detach { .. } => {
                            detaches.fetch_add(1, Ordering::SeqCst);
                            let _ = outbound
                                .send(Err(Status::unavailable("private-control-status-body")))
                                .await;
                            break;
                        }
                        other => panic!("unexpected native control frame: {other:?}"),
                    }
                }
            });
            let mut response = Response::new(ReceiverStream::new(inbound_rx));
            response.metadata_mut().insert(
                microcrab_host_protocol::GRPC_EXECUTE_TOKEN_HEADER,
                "mock-private-execute-capability".parse().unwrap(),
            );
            // First connection is a compatible old Bridge. The upgraded
            // resumed connection negotiates without changing its durable pin.
            if attempt != 0 {
                response.metadata_mut().insert(
                    GRPC_PRIVATE_FILE_HEADER,
                    GRPC_PRIVATE_FILE_V1.parse().unwrap(),
                );
            }
            Ok(response)
        }
        async fn execute(
            &self,
            request: Request<microcrab_host_protocol::pb::HostFrame>,
        ) -> Result<Response<Self::ExecuteStream>, Status> {
            assert_eq!(
                request
                    .metadata()
                    .get(microcrab_host_protocol::GRPC_EXECUTE_TOKEN_HEADER)
                    .and_then(|value| value.to_str().ok()),
                Some("mock-private-execute-capability")
            );
            let frame = frame_from_proto(request.into_inner()).unwrap();
            let HostFrame::Request {
                request_id,
                operation_id,
                method,
                ..
            } = frame
            else {
                panic!("Execute accepts one request");
            };
            let frames = match method {
                HostRequest::Recovery => vec![HostFrame::Response {
                    request_id,
                    operation_id,
                    result: Ok(serde_json::json!([])),
                }],
                HostRequest::ReadFile { path, .. } => {
                    if path == "reject" {
                        return Err(Status::permission_denied(
                            "fixture read was rejected before dispatch",
                        ));
                    }
                    if path == "empty" {
                        let (tx, rx) = mpsc::channel(1);
                        drop(tx);
                        return Ok(Response::new(ReceiverStream::new(rx)));
                    }
                    if path == "invalid" {
                        let (tx, rx) = mpsc::channel(1);
                        tx.send(Ok(microcrab_host_protocol::pb::HostFrame { body: None }))
                            .await
                            .unwrap();
                        return Ok(Response::new(ReceiverStream::new(rx)));
                    }
                    let bytes = b"native-output";
                    let reference = TransferRef {
                        transfer_id: "native-binary".into(),
                        byte_length: bytes.len() as u64,
                        sha256: microcrab_host_protocol::sha256(bytes),
                        kind: "bytes".into(),
                    };
                    if path == "partial" {
                        vec![HostFrame::Response {
                            request_id,
                            operation_id,
                            result: Ok(serde_json::to_value(reference).unwrap()),
                        }]
                    } else {
                        vec![
                            HostFrame::Response {
                                request_id,
                                operation_id,
                                result: Ok(serde_json::to_value(reference).unwrap()),
                            },
                            HostFrame::TransferChunk {
                                transfer_id: "native-binary".into(),
                                sequence: 0,
                                data: bytes.to_vec(),
                                eof: true,
                            },
                        ]
                    }
                }
                other => panic!("unexpected native request: {other:?}"),
            };
            let (outbound, inbound_rx) = mpsc::channel(4);
            tokio::spawn(async move {
                for frame in frames {
                    if outbound
                        .send(Ok(frame_to_proto(&frame).unwrap()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
            Ok(Response::new(ReceiverStream::new(inbound_rx)))
        }
    }
    #[tonic::async_trait]
    impl PrivateFiles for Mock {
        type ReadStream = ReceiverStream<Result<pb::PrivateFileFrame, Status>>;
        async fn read(
            &self,
            request: Request<pb::PrivateFileRequest>,
        ) -> Result<Response<Self::ReadStream>, Status> {
            assert_eq!(
                request.metadata().get(GRPC_EXECUTE_TOKEN_HEADER).unwrap(),
                "mock-private-execute-capability"
            );
            assert_eq!(
                request.metadata().get("origin").unwrap(),
                "http://127.0.0.1:4096"
            );
            let request = request.into_inner();
            assert_eq!(request.lease_generation, "1");
            let scope = request.scope.unwrap();
            assert_eq!(scope.project_id, "a");
            assert!(!scope.binding_id.is_empty() && !scope.backend_id.is_empty());
            assert!(request.remaining_budget_ms > 0);
            assert_eq!(request.max_bytes, 1024);
            let bytes = b"\xff\x00native-private-binary\x80";
            let (tx, rx) = mpsc::channel(2);
            let mut digest = Sha256::digest(bytes).to_vec();
            if request.path.ends_with("corrupt") {
                digest[0] ^= 1;
            }
            tx.try_send(Ok(pb::PrivateFileFrame {
                body: Some(Body::Transfer(pb::PrivateFileTransfer {
                    operation_id: request.operation_id.clone(),
                    byte_length: bytes.len() as u32,
                    sha256: digest,
                })),
            }))
            .unwrap();
            tx.try_send(Ok(pb::PrivateFileFrame {
                body: Some(Body::Chunk(pb::TransferChunk {
                    transfer_id: request.operation_id,
                    sequence: 0,
                    data: bytes.to_vec(),
                    eof: true,
                })),
            }))
            .unwrap();
            Ok(Response::new(ReceiverStream::new(rx)))
        }
    }
    let root = tempfile::tempdir().unwrap();
    let configuration = config(root.path());
    let attempts = Arc::new(AtomicUsize::new(0));
    let detaches = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let mock = Mock {
        attempts: attempts.clone(),
        detaches: detaches.clone(),
    };
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(HostBridgeServer::new(mock.clone()))
            .add_service(PrivateFilesServer::new(mock))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let owner = Principal::for_test("owner");
    let manager = Manager::new(configuration.clone()).await.unwrap();
    let (old_relay_release, old_relay_task) = attach_relay_fixture(&manager, "old-browser").await;
    let parameters = |token| super::super::direct::Connect {
        project_id: "a".into(),
        url: url.clone(),
        token,
        directory: Some(native_descriptor().workspace_root),
        allow_legacy_binding: true,
        only_if_unattached: false,
    };
    let mut initial = parameters(Some("native-pair".into()));
    initial.directory = Some(".".into());
    let connected = manager
        .connect_direct_host(&owner, initial)
        .instrument(tracing::info_span!(
            "host_lifecycle_test",
            request_id = "host-test-grpc"
        ))
        .await
        .unwrap();
    assert_eq!(connected["ready"], true);
    assert_eq!(connected["transport"], "grpc");
    assert_web_transport_features(&manager, true, false).await;
    assert!(connected.get("observed_existing").is_none());
    let view = manager.host_state(&owner, "a").await.unwrap();
    assert_eq!(view["carrier_mode"], "server_direct_grpc");
    assert!(view.get("observed_existing").is_none());
    assert_eq!(
        view["connection_generation"],
        connected["connection_generation"]
    );
    assert_eq!(
        view["descriptor"]["workspace_id"],
        connected["descriptor"]["workspace_id"]
    );
    assert!(view["descriptor"].get("bridge_instance_id").is_none());
    assert!(view.get("relay_release").is_none());
    assert!(!connected.to_string().contains("private-native-resume"));
    assert!(!view.to_string().contains("private-native-resume"));
    let mut automatic = parameters(None);
    automatic.allow_legacy_binding = false;
    automatic.only_if_unattached = true;
    let rejected = manager
        .connect_direct_host(&owner, automatic)
        .await
        .unwrap_err();
    assert_eq!(rejected.code, "host_route_observation_changed");
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    let stale = manager
        .disconnect_direct_host(
            &owner,
            super::super::direct::Disconnect {
                project_id: "a".into(),
                revoke: true,
                relay_release: Some(old_relay_release),
            },
        )
        .await
        .unwrap();
    assert_eq!(stale["disconnected"], false);
    assert_eq!(stale["detach_requested"], false);
    let routes = manager.routes.lock().await;
    let current = routes.get("a").unwrap();
    assert!(current.socket.starts_with("direct:"));
    assert!(current.remote.is_connected());
    assert!(
        !current.remote.supports_private_file_read(),
        "legacy Control did not acknowledge the lane"
    );
    assert_eq!(
        current.generation.to_string(),
        connected["connection_generation"].as_str().unwrap()
    );
    drop(routes);
    assert_eq!(detaches.load(Ordering::SeqCst), 0);
    old_relay_task.abort();
    let _ = old_relay_task.await;
    manager.detach_socket("browser-observer", None).await;
    let credential_path = configuration.data_dir.join("host-connections.json");
    let saved_before_observation = std::fs::read(&credential_path).unwrap();
    let same = manager
        .connect_direct_host(&owner, parameters(None))
        .await
        .unwrap();
    assert_eq!(same["observed_existing"], true);
    assert!(same.get("relay_release").is_none());
    assert!(
        std::fs::read(&credential_path).unwrap() == saved_before_observation,
        "canonical observation must not rewrite the saved relative directory"
    );
    assert_eq!(detaches.load(Ordering::SeqCst), 0);
    assert_eq!(
        connected["connection_generation"],
        same["connection_generation"]
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "UI reconnect observes without rotating the Host lease"
    );
    let refreshed = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        manager.refresh_direct_host(&owner, "a".into()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(refreshed["ready"], true);
    assert_eq!(refreshed["transport"], "grpc");
    assert_ne!(
        refreshed["connection_generation"],
        connected["connection_generation"]
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_web_transport_features(&manager, true, true).await;
    let host = manager.project_host("owner", "a").await.unwrap();
    let remote = manager.routes.lock().await.get("a").unwrap().remote.clone();
    assert!(host.supports_private_file_read());
    assert_eq!(
        connected["descriptor"]["descriptor_digest"],
        refreshed["descriptor"]["descriptor_digest"]
    );
    let context = OpContext::default();
    let binary = host
        .read_private_file(
            "/workspace/fixture".into(),
            PrivateFileBuffer::new(1024, ()),
            &context,
        )
        .await
        .unwrap();
    assert_eq!(binary.as_bytes(), b"\xff\x00native-private-binary\x80");
    let corrupt = host
        .read_private_file(
            "/workspace/corrupt".into(),
            PrivateFileBuffer::new(1024, ()),
            &context,
        )
        .await;
    assert!(matches!(corrupt, Err(error) if error.to_string().contains("integrity")));
    assert_eq!(
        host.read_file("out.txt".into(), 1024, CancellationToken::new())
            .await
            .unwrap(),
        b"native-output"
    );
    for (path, expected) in [
        ("reject", "fixture read was rejected before dispatch"),
        ("empty", "ended before its response"),
        ("partial", "ended before the declared transfer EOF"),
        ("invalid", "Host frame has no body"),
    ] {
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            host.read_file(path.into(), 1024, CancellationToken::new()),
        )
        .await
        .expect("stream failure must not wait for the operation deadline")
        .unwrap_err();
        assert!(error.to_string().contains(expected), "{path}: {error}");
        assert!(remote.is_connected());
        assert!(
            remote.is_ready(),
            "a read failure must not fence unrelated workspace work"
        );
    }
    assert!(
        manager
            .connect_direct_host(&Principal::for_test("intruder"), parameters(None))
            .await
            .is_err()
    );
    manager.close().await.unwrap();
    drop(host);
    drop(remote);
    drop(manager);
    let manager = Manager::new(configuration).await.unwrap();
    let resumed = manager
        .connect_direct_host(&owner, parameters(None))
        .await
        .unwrap();
    assert_eq!(resumed["ready"], true);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_web_transport_features(&manager, false, false).await;
    manager
        .disconnect_direct_host(
            &owner,
            super::super::direct::Disconnect {
                project_id: "a".into(),
                revoke: true,
                relay_release: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(detaches.load(Ordering::SeqCst), 1);
    assert_eq!(
        manager
            .refresh_direct_host(&owner, "a".into())
            .await
            .unwrap_err()
            .code,
        "pairing_required"
    );
    let tokenless = manager
        .connect_direct_host(&owner, parameters(None))
        .await
        .unwrap();
    assert_eq!(tokenless["ready"], true);
    assert_eq!(attempts.load(Ordering::SeqCst), 4);
    manager.close().await.unwrap();
    server.abort();
    let _ = server.await;
    let log = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    for event in [
        "host.connect",
        "observe_existing",
        "dial_new",
        "host.disconnect",
        "host.route_changed",
    ] {
        assert!(log.contains(event), "{event}: {log}");
    }
    assert!(
        log.contains("fd3b1876-791e-4d1b-9194-54ea0372beab"),
        "{log}"
    );
    assert!(log.contains("old_lease_generation=\"1\""), "{log}");
    assert!(
        log.lines().any(|line| line.contains("host.worker_stopped")
            && line.contains("stop_cancelled")
            && line.contains("host-test-grpc")),
        "{log}"
    );
    assert!(log.contains("reader_err"), "{log}");
    assert!(log.contains("gRPC host control stream failed"), "{log}");
    for secret in [
        "native-pair",
        "private-native-resume",
        "mock-private-execute-capability",
        "private-control-status-body",
        &url,
        "native-output",
    ] {
        assert!(!log.contains(secret), "{secret}: {log}");
    }
}
