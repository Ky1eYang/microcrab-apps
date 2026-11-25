use super::*;
use microcrab_host_api::web_transport::ConnectionScope;
use tokio_tungstenite::{
    accept_async, client_async, tungstenite::Message as ClientMessage,
    tungstenite::client::IntoClientRequest,
};

#[tokio::test]
async fn relayed_inspect_stays_legacy_and_authenticated_resume_drops_old_socket() {
    let root = tempfile::tempdir().unwrap();
    let (bridge, token) = BridgeService::open(root.path(), &[], true, None).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let origin = format!("http://{addr}");
    let server = tokio::spawn(serve(bridge.clone(), listener, origin.clone()));
    let connect = || async {
        let mut request = format!("ws://{addr}/host").into_client_request().unwrap();
        request
            .headers_mut()
            .insert("origin", origin.parse().unwrap());
        client_async(request, tokio::net::TcpStream::connect(addr).await.unwrap())
            .await
            .unwrap()
            .0
    };
    let mut relay = connect().await;
    let binding = BindingScope::project("backend".into(), "binding".into(), "project".into());
    let pair = WireFrame::Pair {
        token: token.unwrap(),
        scope: binding.clone(),
        directory: None,
        expected_host_id: None,
        expected_workspace_id: None,
        expected_descriptor_digest: None,
        expected_workspace_root: None,
    };
    let inspect = WireFrame::Inspect {
        request: InspectRequest::new("inspect".into(), &pair).unwrap(),
    };
    relay
        .send(ClientMessage::Binary(
            encode_frame(&inspect).unwrap().into(),
        ))
        .await
        .unwrap();
    let ClientMessage::Binary(bytes) = relay.next().await.unwrap().unwrap() else {
        panic!("inspection required")
    };
    let (reply, ack) = decode_frame_with_web_transport_version(&bytes).unwrap();
    assert!(ack.is_none());
    let WireFrame::Inspected {
        result: Ok(inspected),
        ..
    } = reply
    else {
        panic!("inspection required")
    };
    relay
        .send(ClientMessage::Binary(
            encode_frame_with_web_transport_version(&pair, Some(WEB_TRANSPORT_V1))
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();
    let ClientMessage::Binary(bytes) = relay.next().await.unwrap().unwrap() else {
        panic!("Hello required")
    };
    let (reply, ack) = decode_frame_with_web_transport_features(&bytes).unwrap();
    assert_eq!(
        ack,
        WebTransportFeatures {
            version: Some(WEB_TRANSPORT_V1),
            ws_pull: false
        }
    );
    let WireFrame::Hello {
        credential,
        lease_generation,
        descriptor,
    } = reply
    else {
        panic!("Hello required")
    };
    assert_eq!(descriptor.descriptor_digest, inspected.descriptor_digest);
    let BindingScope::Project(project) = binding.clone() else {
        unreachable!()
    };
    let scope = ConnectionScope {
        binding: project,
        session_id: "fixture".into(),
        epoch: 1,
        connection_id: "live".into(),
    };
    let peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/echo", peer.local_addr().unwrap());
    let upstream =
        tokio::spawn(async move { accept_async(peer.accept().await.unwrap().0).await.unwrap() });
    let call = WireFrame::Request {
        request_id: "web".into(),
        operation_id: "web-op".into(),
        lease_generation,
        remaining_budget_ms: 2000,
        method: HostRequest::WebTransport(WebTransportRequest::WsOpen { scope, url }),
    };
    relay
        .send(ClientMessage::Binary(encode_frame(&call).unwrap().into()))
        .await
        .unwrap();
    let ClientMessage::Binary(bytes) = tokio::time::timeout(Duration::from_secs(2), relay.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    else {
        panic!("result required")
    };
    assert!(
        matches!(decode_frame(&bytes).unwrap(), WireFrame::Response { result: Ok(value), .. } if value.is_null())
    );
    let mut upstream = upstream.await.unwrap();
    let mut replacement = connect().await;
    let resume = WireFrame::Resume {
        credential,
        scope: binding,
        directory: None,
        expected_host_id: None,
        expected_workspace_id: None,
        expected_descriptor_digest: Some(descriptor.descriptor_digest),
        expected_workspace_root: None,
    };
    replacement
        .send(ClientMessage::Binary(
            encode_frame_with_web_transport_features(&resume, WebTransportFeatures::V1)
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();
    let ClientMessage::Binary(bytes) = replacement.next().await.unwrap().unwrap() else {
        panic!("resume Hello required")
    };
    let (reply, ack) = decode_frame_with_web_transport_features(&bytes).unwrap();
    assert!(matches!(reply, WireFrame::Hello { .. }));
    assert_eq!(ack, WebTransportFeatures::V1);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), upstream.next())
            .await
            .unwrap(),
        None | Some(Err(_))
    ));
    server.abort();
}
