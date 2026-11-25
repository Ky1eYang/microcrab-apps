use super::*;
use microcrab_host_api::{GlobIgnoreSnapshot, GlobOutput, GlobRequest, GrepOutput, GrepRequest};
use microcrab_host_protocol::{ErrorCode, HostDescriptor, HostReply};

async fn control(
    client: &mut Client,
    offer: Option<(&'static str, &'static str)>,
    token: &str,
) -> (
    mpsc::Sender<pb::HostFrame>,
    Response<Streaming<pb::HostFrame>>,
) {
    let (sender, rx) = mpsc::channel(8);
    let pair = WireFrame::Pair {
        token: token.into(),
        scope: BindingScope::project("backend".into(), "binding".into(), "project".into()),
        directory: None,
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
    if let Some((header, offer)) = offer {
        request
            .metadata_mut()
            .insert(header, tonic::metadata::MetadataValue::from_static(offer));
    }
    let response = client.control(request).await.unwrap();
    (sender, response)
}

async fn authenticated_control(
    client: &mut Client,
    offer: Option<&'static str>,
) -> (Controller, HostDescriptor) {
    authenticated_offer(client, offer.map(|value| (GRPC_GLOB_IGNORE_HEADER, value))).await
}

async fn authenticated_offer(
    client: &mut Client,
    offer: Option<(&'static str, &'static str)>,
) -> (Controller, HostDescriptor) {
    let (sender, response) = control(client, offer, "pair-secret").await;
    let metadata = response.metadata();
    for (header, supported) in [
        (GRPC_GLOB_IGNORE_HEADER, GRPC_GLOB_IGNORE_V1),
        (GRPC_GLOB_PATHS_HEADER, GRPC_GLOB_PATHS_V1),
    ] {
        assert_eq!(
            metadata.get(header).map(|value| value.to_str().unwrap()),
            (offer == Some((header, supported))).then_some(supported)
        );
    }
    let capability = metadata
        .get(GRPC_EXECUTE_TOKEN_HEADER)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let mut stream = response.into_inner();
    let WireFrame::Hello {
        descriptor,
        lease_generation: generation,
        ..
    } = frame_from_proto(stream.message().await.unwrap().unwrap()).unwrap()
    else {
        panic!("authenticated Hello required");
    };
    (
        Controller {
            sender,
            stream,
            capability,
            generation,
        },
        descriptor,
    )
}

#[tokio::test]
async fn concrete_glob_paths_require_their_own_authenticated_ack() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("target")).unwrap();
    std::fs::write(root.path().join("target/hello.txt"), "hello").unwrap();
    std::fs::write(root.path().join(".gitignore"), "target/\n").unwrap();
    let (mut client, server) = start(root.path()).await;
    let offer = Some((GRPC_GLOB_PATHS_HEADER, GRPC_GLOB_PATHS_V1));
    let (_sender, rejected) = control(&mut client, offer, "wrong").await;
    assert!(!rejected.metadata().contains_key(GRPC_GLOB_PATHS_HEADER));
    drop(rejected);
    let (legacy, legacy_descriptor) =
        authenticated_control(&mut client, Some(GRPC_GLOB_IGNORE_V1)).await;
    assert_eq!(
        call(&mut client, &legacy, HostRequest::GlobIgnoredPaths)
            .await
            .unwrap_err()
            .code,
        ErrorCode::Unsupported
    );
    let (controller, descriptor) = authenticated_offer(&mut client, offer).await;
    assert_eq!(
        descriptor.descriptor_digest,
        legacy_descriptor.descriptor_digest
    );
    let paths: Vec<String> = serde_json::from_value(
        call(&mut client, &controller, HostRequest::GlobIgnoredPaths)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(paths, ["target/"]);
    for (paths, count) in [(paths, 0), (vec![], 1)] {
        let result: microcrab_host_api::ResolvedGlobOutput = serde_json::from_value(
            call(
                &mut client,
                &controller,
                HostRequest::GlobWithIgnoredPaths {
                    request: glob(".", false),
                    paths,
                },
            )
            .await
            .unwrap(),
        )
        .unwrap();
        assert!(!result.truncated);
        assert!(result.scan_incomplete.is_none());
        assert_eq!(result.entries.len(), count);
    }
    server.abort();
    let _ = server.await;
}

fn glob(root: &str, include_ignored: bool) -> GlobRequest {
    GlobRequest {
        root: root.into(),
        pattern: "**/*.txt".into(),
        include_hidden: true,
        include_ignored,
        include_directories: false,
        max_results: 50,
    }
}

async fn call(client: &mut Client, controller: &Controller, method: HostRequest) -> HostReply {
    let mut stream = client
        .execute(operation(
            &controller.generation,
            method,
            Some(&controller.capability),
        ))
        .await
        .unwrap()
        .into_inner();
    let WireFrame::Response { result, .. } =
        frame_from_proto(stream.message().await.unwrap().unwrap()).unwrap()
    else {
        panic!("typed reply required");
    };
    assert!(stream.message().await.unwrap().is_none());
    result
}

#[tokio::test]
async fn glob_ignore_extension_is_authenticated_and_keeps_descriptor_pins() {
    let root = tempfile::tempdir().unwrap();
    let (mut client, server) = start(root.path()).await;
    let (_sender, rejected) = control(
        &mut client,
        Some((GRPC_GLOB_IGNORE_HEADER, GRPC_GLOB_IGNORE_V1)),
        "wrong",
    )
    .await;
    assert!(!rejected.metadata().contains_key(GRPC_GLOB_IGNORE_HEADER));
    assert!(!rejected.metadata().contains_key(GRPC_EXECUTE_TOKEN_HEADER));
    assert!(matches!(
        frame_from_proto(rejected.into_inner().message().await.unwrap().unwrap()).unwrap(),
        WireFrame::HandshakeRejected { .. }
    ));
    let (legacy, legacy_descriptor) = authenticated_control(&mut client, Some("2")).await;
    for method in [
        HostRequest::GlobIgnoreSnapshot,
        HostRequest::GlobWithIgnoreRules {
            request: glob(".", false),
            rules: vec![],
        },
    ] {
        assert_eq!(
            call(&mut client, &legacy, method).await.unwrap_err().code,
            ErrorCode::Unsupported,
        );
    }
    assert!(
        call(&mut client, &legacy, HostRequest::Glob(glob(".", false)))
            .await
            .is_ok()
    );
    let (negotiated, descriptor) =
        authenticated_control(&mut client, Some(GRPC_GLOB_IGNORE_V1)).await;
    assert_eq!(
        descriptor.descriptor_digest,
        legacy_descriptor.descriptor_digest
    );
    assert_eq!(descriptor.workspace_id, legacy_descriptor.workspace_id);
    assert!(
        call(&mut client, &negotiated, HostRequest::GlobIgnoreSnapshot)
            .await
            .is_ok()
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn glob_ignore_dispatch_uses_supplied_rules_without_changing_ordinary_search() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path();
    std::fs::create_dir(path.join(".microcrab")).unwrap();
    for file in ["disk.txt", "state.txt", ".microcrab/private.txt"] {
        std::fs::write(path.join(file), "needle\n").unwrap();
    }
    std::fs::write(path.join(".gitignore"), "disk.txt\n").unwrap();
    let (mut client, server) = start(path).await;
    let (controller, _) = authenticated_control(&mut client, Some(GRPC_GLOB_IGNORE_V1)).await;
    let snapshot: GlobIgnoreSnapshot = serde_json::from_value(
        call(&mut client, &controller, HostRequest::GlobIgnoreSnapshot)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(!snapshot.truncated);
    assert_eq!(snapshot.rules.len(), 1);
    assert_eq!(snapshot.rules[0].path, Path::new(".gitignore"));
    assert_eq!(snapshot.rules[0].content, "disk.txt\n");
    std::fs::write(path.join(".gitignore"), "state.txt\n").unwrap();
    let supplied: GlobOutput = serde_json::from_value(
        call(
            &mut client,
            &controller,
            HostRequest::GlobWithIgnoreRules {
                request: glob(".", false),
                rules: snapshot.rules,
            },
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(supplied.entries.len(), 1);
    assert_eq!(supplied.entries[0].path, Path::new("state.txt"));
    let ordinary: GlobOutput = serde_json::from_value(
        call(
            &mut client,
            &controller,
            HostRequest::Glob(glob(".", false)),
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(ordinary.entries.len(), 1);
    assert_eq!(ordinary.entries[0].path, Path::new("disk.txt"));
    let grep: GrepOutput = serde_json::from_value(
        call(
            &mut client,
            &controller,
            HostRequest::Grep(GrepRequest {
                root: ".".into(),
                pattern: "needle".into(),
                include_glob: Some("**/*.txt".into()),
                case_sensitive: true,
                literal: true,
                max_matches: 50,
                max_bytes: 1024,
            }),
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(grep.matches.len(), 1);
    assert_eq!(grep.matches[0].path, Path::new("disk.txt"));
    assert_eq!(
        call(
            &mut client,
            &controller,
            HostRequest::GlobWithIgnoreRules {
                request: glob(".microcrab", true),
                rules: vec![],
            },
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::Denied,
    );
    server.abort();
    let _ = server.await;
}
