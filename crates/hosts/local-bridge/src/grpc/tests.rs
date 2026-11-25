//! Real sockets verify that public lease counters never authorize Execute or
//! select another workspace, and that Control cannot admit operation effects.
use super::*;
use microcrab_host_api::{ReadRequest, WritePrecondition, WriteRequest};
use microcrab_host_protocol::{BindingScope, HostBridgeClient, HostRequest};
use std::path::Path;
mod glob_ignore;
mod private_files;
mod web_transport;

const ORIGIN: &str = "http://127.0.0.1:4096";
type Client = HostBridgeClient<tonic::transport::Channel>;
struct Controller {
    sender: mpsc::Sender<pb::HostFrame>,
    stream: tonic::Streaming<pb::HostFrame>,
    capability: String,
    generation: String,
}
async fn start(root: &Path) -> (Client, tokio::task::JoinHandle<()>) {
    let (channel, server) = start_channel(root, true).await;
    (HostBridgeClient::new(channel), server)
}
async fn start_channel(
    root: &Path,
    require_token: bool,
) -> (tonic::transport::Channel, tokio::task::JoinHandle<()>) {
    let (bridge, _) =
        BridgeService::open(root, &[], require_token, Some("pair-secret".into())).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        serve(bridge, listener, ORIGIN.into()).await.unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (channel, server)
}
async fn open(client: &mut Client, directory: &Path, project: &str) -> Controller {
    open_control(client, directory, project, false, false).await
}
async fn open_control(
    client: &mut Client,
    directory: &Path,
    project: &str,
    offer: bool,
    acknowledged: bool,
) -> Controller {
    let (sender, rx) = mpsc::channel(8);
    let pair = WireFrame::Pair {
        token: "pair-secret".into(),
        scope: BindingScope::project(
            "backend".into(),
            format!("binding-{project}"),
            project.into(),
        ),
        directory: Some(directory.to_string_lossy().into_owned()),
        expected_host_id: None,
        expected_workspace_id: None,
        expected_descriptor_digest: None,
        expected_workspace_root: None,
    };
    sender.send(frame_to_proto(&pair).unwrap()).await.unwrap();
    let mut request = Request::new(ReceiverStream::new(rx));
    request
        .metadata_mut()
        .insert("origin", ORIGIN.parse().unwrap());
    if offer {
        private_file::acknowledge(request.metadata_mut());
    }
    let response = client.control(request).await.unwrap();
    assert_eq!(private_file::offered(response.metadata()), acknowledged);
    let capability = response
        .metadata()
        .get(GRPC_EXECUTE_TOKEN_HEADER)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let mut stream = response.into_inner();
    let WireFrame::Hello {
        lease_generation: generation,
        ..
    } = frame_from_proto(stream.message().await.unwrap().unwrap()).unwrap()
    else {
        panic!("Hello required")
    };
    Controller {
        sender,
        stream,
        capability,
        generation,
    }
}
fn operation(
    generation: &str,
    method: HostRequest,
    capability: Option<&str>,
) -> Request<pb::HostFrame> {
    let frame = WireFrame::Request {
        request_id: uuid::Uuid::new_v4().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        lease_generation: generation.into(),
        remaining_budget_ms: 5000,
        method,
    };
    let mut request = Request::new(frame_to_proto(&frame).unwrap());
    request
        .metadata_mut()
        .insert("origin", ORIGIN.parse().unwrap());
    if let Some(capability) = capability {
        request
            .metadata_mut()
            .insert(GRPC_EXECUTE_TOKEN_HEADER, capability.parse().unwrap());
    }
    request
}
fn read() -> HostRequest {
    HostRequest::Read(ReadRequest {
        path: "sentinel.txt".into(),
        offset_line: 1,
        max_lines: 10,
        max_bytes: 1024,
    })
}
async fn result(
    client: &mut Client,
    controller: &Controller,
    method: HostRequest,
) -> serde_json::Value {
    let response = client
        .execute(operation(
            &controller.generation,
            method,
            Some(&controller.capability),
        ))
        .await
        .unwrap();
    let frame = response.into_inner().message().await.unwrap().unwrap();
    let WireFrame::Response { result, .. } = frame_from_proto(frame).unwrap() else {
        panic!("Response required")
    };
    result.unwrap()
}
async fn ended(stream: &mut tonic::Streaming<pb::HostFrame>) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while stream.message().await.unwrap().is_some() {}
    })
    .await
    .expect("Control retirement must close its response stream");
}

#[tokio::test]
async fn execute_requires_control_capability_and_keeps_equal_workspace_generations_isolated() {
    let root = tempfile::tempdir().unwrap();
    let a = root.path().join("a");
    let b = root.path().join("b");
    std::fs::create_dir(&a).unwrap();
    std::fs::create_dir(&b).unwrap();
    std::fs::write(a.join("sentinel.txt"), "workspace-a").unwrap();
    std::fs::write(b.join("sentinel.txt"), "workspace-b").unwrap();
    let (mut client, server) = start(root.path()).await;
    let missing = client
        .execute(operation("1", read(), None))
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::Unauthenticated);
    let mut first = open(&mut client, &a, "a").await;
    let second = open(&mut client, &b, "b").await;
    assert_eq!(
        first.generation, second.generation,
        "regression requires colliding workspace counters"
    );
    assert_ne!(first.capability, second.capability);
    let forged = client
        .execute(operation(&first.generation, read(), Some("1")))
        .await
        .unwrap_err();
    assert_eq!(forged.code(), tonic::Code::Unauthenticated);
    assert_eq!(
        result(&mut client, &first, read()).await["content"],
        "workspace-a"
    );
    assert_eq!(
        result(&mut client, &second, read()).await["content"],
        "workspace-b"
    );
    result(
        &mut client,
        &first,
        HostRequest::Write(WriteRequest {
            path: "only-a.txt".into(),
            content: "isolated".into(),
            precondition: WritePrecondition::Missing,
        }),
    )
    .await;
    assert_eq!(
        std::fs::read_to_string(a.join("only-a.txt")).unwrap(),
        "isolated"
    );
    assert!(!b.join("only-a.txt").exists());
    let old_capability = first.capability.clone();
    drop(first.sender);
    ended(&mut first.stream).await;
    let expired = client
        .execute(operation("1", read(), Some(&old_capability)))
        .await
        .unwrap_err();
    assert_eq!(expired.code(), tonic::Code::Unauthenticated);
    assert_eq!(
        result(&mut client, &second, read()).await["content"],
        "workspace-b",
        "retiring A must not erase B"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn request_on_grpc_control_is_rejected_before_effect_admission() {
    let root = tempfile::tempdir().unwrap();
    let (mut client, server) = start(root.path()).await;
    let mut controller = open(&mut client, root.path(), "a").await;
    let request = operation(
        &controller.generation,
        HostRequest::Write(WriteRequest {
            path: "must-not-exist.txt".into(),
            content: "forbidden control effect".into(),
            precondition: WritePrecondition::Missing,
        }),
        None,
    );
    controller.sender.send(request.into_inner()).await.unwrap();
    ended(&mut controller.stream).await;
    assert!(!root.path().join("must-not-exist.txt").exists());
    let rejected = client
        .execute(operation(
            &controller.generation,
            HostRequest::Recovery,
            Some(&controller.capability),
        ))
        .await
        .unwrap_err();
    assert_eq!(rejected.code(), tonic::Code::Unauthenticated);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn inspection_and_guard_rejection_keep_existing_grpc_execute_capability_live() {
    use microcrab_host_protocol::{ErrorCode, InspectRequest};
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("sentinel.txt"), "still-live").unwrap();
    let (mut client, server) = start(root.path()).await;
    let controller = open(&mut client, root.path(), "a").await;
    let mut candidate = WireFrame::Pair {
        token: "wrong".into(),
        scope: BindingScope::project("backend".into(), "binding-b".into(), "b".into()),
        directory: None,
        expected_host_id: None,
        expected_workspace_id: None,
        expected_descriptor_digest: None,
        expected_workspace_root: None,
    };
    for valid in [false, true] {
        if valid && let WireFrame::Pair { token, .. } = &mut candidate {
            *token = "pair-secret".into();
        }
        let probe = WireFrame::Inspect {
            request: InspectRequest::new(format!("probe-{valid}"), &candidate).unwrap(),
        };
        let mut request = Request::new(frame_to_proto(&probe).unwrap());
        request
            .metadata_mut()
            .insert("origin", ORIGIN.parse().unwrap());
        let reply = client.inspect(request).await.unwrap();
        assert!(reply.metadata().get(GRPC_EXECUTE_TOKEN_HEADER).is_none());
        let WireFrame::Inspected {
            result: inspected, ..
        } = frame_from_proto(reply.into_inner()).unwrap()
        else {
            panic!("inspection required")
        };
        if !valid {
            assert_eq!(inspected.unwrap_err().code, ErrorCode::PairingDenied);
        } else {
            let descriptor = inspected.unwrap();
            if let WireFrame::Pair {
                expected_host_id,
                expected_workspace_id,
                expected_descriptor_digest,
                expected_workspace_root,
                ..
            } = &mut candidate
            {
                *expected_host_id = Some(descriptor.host_id);
                *expected_workspace_id = Some(descriptor.workspace_id);
                *expected_workspace_root = Some(descriptor.workspace_root);
                *expected_descriptor_digest = Some("0".repeat(64));
            }
            let (tx, rx) = mpsc::channel(1);
            tx.send(frame_to_proto(&candidate).unwrap()).await.unwrap();
            let mut request = Request::new(ReceiverStream::new(rx));
            request
                .metadata_mut()
                .insert("origin", ORIGIN.parse().unwrap());
            let response = client.control(request).await.unwrap();
            assert!(response.metadata().get(GRPC_EXECUTE_TOKEN_HEADER).is_none());
            let first = response.into_inner().message().await.unwrap().unwrap();
            assert!(
                matches!(frame_from_proto(first).unwrap(), WireFrame::HandshakeRejected { error } if error.code == ErrorCode::BindingMismatch)
            );
        }
        assert_eq!(
            result(&mut client, &controller, read()).await["content"],
            "still-live"
        );
    }
    server.abort();
    let _ = server.await;
}
