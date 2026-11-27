use super::*;
use microcrab_host_protocol::{
    MAX_CHUNK_BYTES, MAX_PRIVATE_FILE_BYTES,
    pb::{private_file_frame::Body, private_files_client::PrivateFilesClient},
};
use sha2::{Digest, Sha256};

fn request(controller: &Controller, project: &str, path: &Path) -> Request<pb::PrivateFileRequest> {
    let mut request = Request::new(pb::PrivateFileRequest {
        operation_id: uuid::Uuid::new_v4().to_string(),
        lease_generation: controller.generation.clone(),
        scope: Some(pb::PrivateFileScope {
            backend_id: "backend".into(),
            project_id: project.into(),
            binding_id: format!("binding-{project}"),
        }),
        remaining_budget_ms: 5000,
        path: path.to_str().unwrap().into(),
        max_bytes: MAX_PRIVATE_FILE_BYTES as u32,
    });
    request
        .metadata_mut()
        .insert("origin", ORIGIN.parse().unwrap());
    request.metadata_mut().insert(
        GRPC_EXECUTE_TOKEN_HEADER,
        controller.capability.parse().unwrap(),
    );
    request
}

#[tokio::test]
async fn grpc_private_files_are_not_negotiated_on_tokenless_control() {
    let root = tempfile::tempdir().unwrap();
    let (channel, server) = start_channel(root.path(), false).await;
    let controller = open_control(
        &mut HostBridgeClient::new(channel.clone()),
        root.path(),
        "a",
        true,
        false,
    )
    .await;
    let mut files = PrivateFilesClient::new(channel);
    let denied = files
        .read(request(&controller, "a", &root.path().join("binary")))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::Unimplemented);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn grpc_private_files_require_negotiation_exact_binding_and_live_lease() {
    let root = tempfile::tempdir().unwrap();
    let root_path = std::fs::canonicalize(root.path()).unwrap();
    let a = root_path.join("a");
    let b = root_path.join("b");
    std::fs::create_dir(&a).unwrap();
    std::fs::create_dir(&b).unwrap();
    let file = a.join("binary");
    let data: Vec<_> = (0..MAX_CHUNK_BYTES * 2 + 3)
        .map(|value| value as u8)
        .collect();
    std::fs::write(&file, &data).unwrap();
    let (channel, server) = start_channel(root.path(), true).await;
    let mut control = HostBridgeClient::new(channel.clone());
    let mut first = open_control(&mut control, &a, "a", true, true).await;
    let legacy = open(&mut control, &b, "b").await;
    let mut files = PrivateFilesClient::new(channel);
    let mut unauthenticated = request(&first, "a", &file);
    unauthenticated
        .metadata_mut()
        .remove(GRPC_EXECUTE_TOKEN_HEADER);
    assert_eq!(
        files.read(unauthenticated).await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        files
            .read(request(&legacy, "b", &file))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unimplemented
    );
    assert_eq!(
        files
            .read(request(&first, "b", &file))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let req = request(&first, "a", &file);
    let id = req.get_ref().operation_id.clone();
    let mut stream = files.read(req).await.unwrap().into_inner();
    let Some(Body::Transfer(declaration)) = stream.message().await.unwrap().unwrap().body else {
        panic!("typed declaration required")
    };
    assert_eq!(declaration.operation_id, id);
    assert_eq!(declaration.byte_length as usize, data.len());
    assert_eq!(declaration.sha256[..], Sha256::digest(&data)[..]);
    for (sequence, expected) in data.chunks(MAX_CHUNK_BYTES).enumerate() {
        let Some(Body::Chunk(chunk)) = stream.message().await.unwrap().unwrap().body else {
            panic!("binary chunk required")
        };
        assert_eq!(chunk.transfer_id, id);
        assert_eq!(chunk.sequence as usize, sequence);
        assert_eq!(chunk.data, expected);
        assert_eq!(chunk.eof, sequence == 2);
    }
    assert!(stream.message().await.unwrap().is_none());
    let rel_req = request(&first, "a", Path::new("binary"));
    let rel_id = rel_req.get_ref().operation_id.clone();
    let mut rel_stream = files.read(rel_req).await.unwrap().into_inner();
    let Some(Body::Transfer(rel_decl)) = rel_stream.message().await.unwrap().unwrap().body else {
        panic!("relative path typed declaration required")
    };
    assert_eq!(rel_decl.operation_id, rel_id);
    assert_eq!(rel_decl.sha256[..], Sha256::digest(&data)[..]);
    while rel_stream.message().await.unwrap().is_some() {}
    let mut too_small = request(&first, "a", &file);
    too_small.get_mut().max_bytes = 1;
    let mut bounded = files.read(too_small).await.unwrap().into_inner();
    assert_eq!(
        bounded.message().await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(
        files
            .read(request(&first, "a", Path::new("../b/binary")))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    let protected = a.join(".microcrab/host-bridge/identity.json");
    std::fs::write(b.join("binary"), b"outside").unwrap();
    let mut denied = vec![
        protected,
        std::path::PathBuf::from(".microcrab/host-bridge/identity.json"),
        b.join("binary"),
    ];
    #[cfg(unix)]
    {
        let link = a.join("link");
        std::os::unix::fs::symlink(b.join("binary"), &link).unwrap();
        denied.push(link);
    }
    for path in denied {
        let mut stream = files
            .read(request(&first, "a", &path))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            stream.message().await.unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
    }
    let original = request(&first, "a", &file);
    drop(first.sender);
    ended(&mut first.stream).await;
    assert_eq!(
        files.read(original).await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    server.abort();
    let _ = server.await;
}
