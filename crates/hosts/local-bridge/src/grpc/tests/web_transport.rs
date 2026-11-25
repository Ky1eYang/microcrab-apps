use super::*;
use microcrab_host_api::web_transport::ConnectionScope;
use microcrab_host_protocol::{
    ErrorCode, WEB_TRANSPORT_V1, WebTransportFeatures, WebTransportRequest,
};

#[tokio::test]
async fn ack_follows_authenticated_pair_only_and_execute_drop_cancels_connect() {
    let legacy_v1 = WebTransportFeatures {
        version: Some(WEB_TRANSPORT_V1),
        ws_pull: false,
    };
    let absent = WebTransportFeatures::default();
    let unknown = WebTransportFeatures {
        version: Some(2),
        ws_pull: false,
    };
    for (token_required, token, offer, expected_ack) in [
        (true, "wrong", WebTransportFeatures::V1, absent),
        (false, "unused", WebTransportFeatures::V1, absent),
        (true, "pair-secret", unknown, absent),
        (true, "pair-secret", absent, absent),
        (true, "pair-secret", legacy_v1, legacy_v1),
        (
            true,
            "pair-secret",
            WebTransportFeatures::V1,
            WebTransportFeatures::V1,
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let (channel, task) = start_channel(root.path(), token_required).await;
        let mut client = HostBridgeClient::new(channel);
        let (control, rx) = mpsc::channel(8);
        let scope = BindingScope::project("backend".into(), "binding".into(), "project".into());
        let pair = WireFrame::Pair {
            token: token.into(),
            scope: scope.clone(),
            directory: None,
            expected_host_id: None,
            expected_workspace_id: None,
            expected_descriptor_digest: None,
            expected_workspace_root: None,
        };
        control
            .send(frame_to_proto_with_web_transport_features(&pair, offer).unwrap())
            .await
            .unwrap();
        let mut request = Request::new(ReceiverStream::new(rx));
        request
            .metadata_mut()
            .insert("origin", ORIGIN.parse().unwrap());
        let reply = client.control(request).await.unwrap();
        let capability = reply
            .metadata()
            .get(GRPC_EXECUTE_TOKEN_HEADER)
            .map(|m| m.to_str().unwrap().to_owned());
        let mut stream = reply.into_inner();
        let (frame, ack) =
            frame_from_proto_with_web_transport_features(stream.message().await.unwrap().unwrap())
                .unwrap();
        assert_eq!(ack, expected_ack);
        if token == "wrong" {
            assert!(capability.is_none());
            assert!(matches!(frame, WireFrame::HandshakeRejected { .. }));
            task.abort();
            continue;
        }
        let WireFrame::Hello {
            lease_generation,
            descriptor,
            ..
        } = frame
        else {
            panic!("Hello required")
        };
        assert!(
            !descriptor
                .tool_capabilities
                .iter()
                .any(|name| name.contains("web_transport"))
        );
        let BindingScope::Project(binding) = scope else {
            unreachable!()
        };
        let scope = ConnectionScope {
            binding,
            session_id: "session".into(),
            epoch: 1,
            connection_id: "connection".into(),
        };
        let mut pull_result = client
            .execute(operation(
                &lease_generation,
                HostRequest::WebTransport(WebTransportRequest::WsPull {
                    scope: scope.clone(),
                    max_bytes: 1,
                    soft_wait_ms: 0,
                }),
                capability.as_deref(),
            ))
            .await
            .unwrap()
            .into_inner();
        let WireFrame::Response {
            result: Err(error), ..
        } = frame_from_proto(pull_result.message().await.unwrap().unwrap()).unwrap()
        else {
            panic!("pull before open must be rejected");
        };
        assert_eq!(
            error.code,
            if ack.ws_pull {
                ErrorCode::NotFound
            } else {
                ErrorCode::Unsupported
            }
        );
        drop(pull_result);
        let peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let req = operation(
            &lease_generation,
            HostRequest::WebTransport(WebTransportRequest::WsOpen {
                scope: scope.clone(),
                url: format!("ws://{}/fixture", peer.local_addr().unwrap()),
            }),
            capability.as_deref(),
        );
        let mut result = client.execute(req).await.unwrap().into_inner();
        if ack.version.is_some() {
            let (mut upstream, _) = tokio::time::timeout(Duration::from_secs(2), peer.accept())
                .await
                .unwrap()
                .unwrap();
            // Leave upstream handshake blocked and abandon Execute, not Control.
            drop(result);
            let mut bytes = Vec::new();
            use tokio::io::AsyncReadExt;
            tokio::time::timeout(Duration::from_secs(2), upstream.read_to_end(&mut bytes))
                .await
                .unwrap()
                .unwrap();
            // Original scope cannot become live after its abandoned open.
            let mut status = client
                .execute(operation(
                    &lease_generation,
                    HostRequest::WebTransport(WebTransportRequest::WsStatus { scope }),
                    capability.as_deref(),
                ))
                .await
                .unwrap()
                .into_inner();
            let WireFrame::Response {
                result: Err(error), ..
            } = frame_from_proto(status.message().await.unwrap().unwrap()).unwrap()
            else {
                panic!("status must fail")
            };
            assert_eq!(error.code, ErrorCode::NotFound);
        } else {
            let WireFrame::Response {
                result: Err(error), ..
            } = frame_from_proto(result.message().await.unwrap().unwrap()).unwrap()
            else {
                panic!("unnegotiated operation must fail")
            };
            assert_eq!(error.code, ErrorCode::Unsupported);
            assert!(
                tokio::time::timeout(Duration::from_millis(30), peer.accept())
                    .await
                    .is_err()
            );
        }
        drop(control);
        task.abort();
    }
}
