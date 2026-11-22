//! Reverse carrier lifecycle against a real loopback WebSocket, no live service.
use super::*;
use crate::manager::direct::{Connect, outbound::AttemptParams};
use axum::{
    Router,
    extract::{State, WebSocketUpgrade},
    http::HeaderMap,
    response::IntoResponse,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use microcrab_host_protocol::{HostFrame, decode_frame, encode_frame};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

fn connect() -> Connect {
    Connect {
        project_id: "a".into(),
        url: "ws://127.0.0.1:8788/host".into(),
        token: Some("one-use-token".into()),
        directory: None,
        allow_legacy_binding: true,
        only_if_unattached: false,
    }
}
fn params(start: &serde_json::Value) -> AttemptParams {
    serde_json::from_value(serde_json::json!({"project_id":"a","attempt_id":start["attempt_id"]}))
        .unwrap()
}
fn headers(start: &serde_json::Value) -> HeaderMap {
    let mut value = HeaderMap::new();
    value.insert("origin", "http://127.0.0.1:4096".parse().unwrap());
    value.insert(
        "x-host-attempt-id",
        start["attempt_id"].as_str().unwrap().parse().unwrap(),
    );
    value.insert(
        "authorization",
        format!("Bearer {}", start["capability"].as_str().unwrap())
            .parse()
            .unwrap(),
    );
    value
}
async fn upgrade(
    State(manager): State<Arc<Manager>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    let attempt = match manager.claim_outbound_host(&headers) {
        Ok(a) => a,
        Err(_) => return axum::http::StatusCode::FORBIDDEN.into_response(),
    };
    ws.on_upgrade(move |socket| async move {
        manager.accept_outbound_host(attempt, socket).await;
    })
}
async fn socket(
    manager: Arc<Manager>,
    start: &serde_json::Value,
) -> (
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = Router::new()
        .route("/api/v1/host/outbound", get(upgrade))
        .with_state(manager);
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut request = format!("ws://{address}/api/v1/host/outbound")
        .into_client_request()
        .unwrap();
    request.headers_mut().extend(headers(start));
    let (socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    (socket, server)
}
async fn inspect_before_pair(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    let Message::Binary(bytes) = socket.next().await.unwrap().unwrap() else {
        panic!("Inspect required")
    };
    let HostFrame::Inspect { request } = decode_frame(&bytes).unwrap() else {
        panic!("Inspect required")
    };
    socket
        .send(Message::Binary(
            encode_frame(&HostFrame::Inspected {
                request_id: request.request_id,
                result: Ok(descriptor("native-host")),
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
}
#[tokio::test]
async fn attempts_are_owner_project_scoped_single_use_and_cancelled_before_pair() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    let start = manager
        .start_outbound_host(&owner, connect())
        .await
        .unwrap();
    let mut wrong = headers(&start);
    wrong.insert("origin", "https://other.example".parse().unwrap());
    assert!(manager.claim_outbound_host(&wrong).is_err());
    assert!(
        manager
            .finish_outbound_host(&Principal::for_test("other"), params(&start))
            .await
            .is_err()
    );
    let claimed = manager.claim_outbound_host(&headers(&start)).unwrap();
    drop(claimed);
    assert!(manager.claim_outbound_host(&headers(&start)).is_err());
    let cancelled = manager
        .cancel_outbound_host(&owner, params(&start))
        .await
        .unwrap();
    assert_eq!(cancelled["phase"], "pre_auth");
    assert_eq!(
        manager
            .finish_outbound_host(&owner, params(&start))
            .await
            .unwrap_err()
            .code,
        "host_outbound_unavailable"
    );
    manager.close().await.unwrap();
}
#[tokio::test]
async fn explicit_disconnect_fences_late_outbound_attempt_before_pair() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    let start = manager
        .start_outbound_host(&owner, connect())
        .await
        .unwrap();
    manager
        .disconnect_direct_host(
            &owner,
            crate::manager::direct::Disconnect {
                project_id: "a".into(),
                revoke: false,
                relay_release: None,
            },
        )
        .await
        .unwrap();
    let (mut socket, server) = socket(manager.clone(), &start).await;
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
        .await
        .unwrap();
    assert!(!matches!(first, Some(Ok(Message::Binary(_)))));
    assert_eq!(
        manager
            .finish_outbound_host(&owner, params(&start))
            .await
            .unwrap_err()
            .code,
        "host_outbound_unavailable"
    );
    assert!(manager.routes.lock().await.get("a").is_none());
    server.abort();
    manager.close().await.unwrap();
}
#[tokio::test]
async fn automatic_outbound_cannot_replace_a_relay_published_after_the_offer() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    let mut automatic = connect();
    automatic.token = None;
    automatic.allow_legacy_binding = false;
    automatic.only_if_unattached = true;
    let offer = manager
        .start_outbound_host(&owner, automatic)
        .await
        .unwrap();
    let (release, relay) = super::direct::attach_relay_fixture(&manager, "winning-relay").await;
    let (mut socket, server) = socket(manager.clone(), &offer).await;
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
        .await
        .unwrap();
    assert!(!matches!(first, Some(Ok(Message::Binary(_)))));
    let error = manager
        .finish_outbound_host(&owner, params(&offer))
        .await
        .unwrap_err();
    assert_eq!(error.code, "host_route_observation_changed");
    let state = manager.host_state(&owner, "a").await.unwrap();
    assert_eq!(state["carrier_mode"], "browser_relay");
    assert_eq!(state["connection_generation"], release.relay_generation);
    assert_eq!(state["ready"], true);
    manager.close().await.unwrap();
    server.abort();
    let _ = server.await;
    relay.abort();
    let _ = relay.await;
}

#[tokio::test]
async fn cancellation_after_pair_does_not_wait_for_the_project_authentication_gate() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    let start = manager
        .start_outbound_host(&owner, connect())
        .await
        .unwrap();
    let (mut socket, server) = socket(manager.clone(), &start).await;
    inspect_before_pair(&mut socket).await;
    let Message::Binary(bytes) = socket.next().await.unwrap().unwrap() else {
        panic!("Pair required")
    };
    assert!(matches!(
        decode_frame(&bytes).unwrap(),
        HostFrame::Pair { .. }
    ));
    // The authentication task owns the Project gate until Hello/commit settles.
    // No Hello is sent while the real cancellation API must return.
    let cancelled = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        manager.cancel_outbound_host(&owner, params(&start)),
    )
    .await
    .expect("cancellation must not wait for the in-flight Project gate")
    .unwrap();
    assert_eq!(cancelled["cancelled"], true);
    assert_eq!(cancelled["phase"], "auth_unknown");
    assert_eq!(
        manager
            .finish_outbound_host(&owner, params(&start))
            .await
            .unwrap_err()
            .code,
        "host_pairing_outcome_unknown"
    );
    let _ = socket
        .send(Message::Binary(
            encode_frame(&HostFrame::Hello {
                descriptor: descriptor("cancelled-native-host"),
                credential: "late-resume".into(),
                lease_generation: "1".into(),
            })
            .unwrap()
            .into(),
        ))
        .await;
    let closed = tokio::time::timeout(std::time::Duration::from_secs(2), socket.next())
        .await
        .expect("cancelled authentication must release its socket");
    assert!(!matches!(closed, Some(Ok(Message::Binary(_)))));
    assert!(manager.routes.lock().await.get("a").is_none());
    server.abort();
    manager.close().await.unwrap();
}

#[tokio::test]
async fn lost_hello_requires_fresh_pairing_and_never_publishes_a_route() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    let start = manager
        .start_outbound_host(&owner, connect())
        .await
        .unwrap();
    let (mut socket, server) = socket(manager.clone(), &start).await;
    inspect_before_pair(&mut socket).await;
    let Message::Binary(bytes) = socket.next().await.unwrap().unwrap() else {
        panic!("Pair required")
    };
    assert!(
        matches!(decode_frame(&bytes).unwrap(),HostFrame::Pair{token,..} if token=="one-use-token")
    );
    socket.close(None).await.unwrap();
    let error = manager
        .finish_outbound_host(&owner, params(&start))
        .await
        .unwrap_err();
    assert_eq!(error.code, "host_pairing_outcome_unknown");
    assert!(manager.routes.lock().await.get("a").is_none());
    assert!(manager.claim_outbound_host(&headers(&start)).is_err());
    server.abort();
    manager.close().await.unwrap();
}
#[tokio::test]
async fn authenticated_native_route_survives_observation_and_stale_attempt_cancellation() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    let stale = manager
        .start_outbound_host(&owner, connect())
        .await
        .unwrap();
    let start = manager
        .start_outbound_host(&owner, connect())
        .await
        .unwrap();
    let (mut socket, server) = socket(manager.clone(), &start).await;
    inspect_before_pair(&mut socket).await;
    let Message::Binary(first) = socket.next().await.unwrap().unwrap() else {
        panic!("handshake required")
    };
    assert!(matches!(
        decode_frame(&first).unwrap(),
        HostFrame::Pair { .. }
    ));
    let mut native = descriptor("native-host");
    native.bridge_instance_id = uuid::Uuid::new_v4().to_string();
    socket
        .send(Message::Binary(
            encode_frame(&HostFrame::Hello {
                descriptor: native,
                credential: "native-resume".into(),
                lease_generation: "1".into(),
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    let commit_manager = manager.clone();
    let commit_start = start.clone();
    let native = tokio::spawn(async move {
        let principal = Principal::for_test("owner");
        while let Some(Ok(Message::Binary(bytes))) = socket.next().await {
            let response = match decode_frame(&bytes).unwrap() {
                HostFrame::Request {
                    request_id,
                    operation_id,
                    method: microcrab_host_protocol::HostRequest::Recovery,
                    ..
                } => {
                    // Recovery is requested during publication, after begin_commit
                    // but before the Project authentication gate is released.
                    let cancelled = tokio::time::timeout(
                        std::time::Duration::from_secs(1),
                        commit_manager.cancel_outbound_host(&principal, params(&commit_start)),
                    )
                    .await
                    .expect("commit-winner cancellation must not await publication")
                    .unwrap();
                    assert_eq!(cancelled["cancelled"], false);
                    HostFrame::Response {
                        request_id,
                        operation_id,
                        result: Ok(serde_json::json!([])),
                    }
                }
                HostFrame::Ping { lease_generation } => HostFrame::Pong { lease_generation },
                _ => continue,
            };
            if socket
                .send(Message::Binary(encode_frame(&response).unwrap().into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let finish = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        manager.finish_outbound_host(&owner, params(&start)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(finish["transport"], "outbound_wss");
    assert_eq!(finish["ready"], true);
    assert!(finish.get("observed_existing").is_none());
    let generation = manager.routes.lock().await.get("a").unwrap().generation;
    let view = manager.host_state(&owner, "a").await.unwrap();
    assert_eq!(view["carrier_mode"], "server_direct_outbound");
    assert_eq!(view["connection_generation"], generation.to_string());
    assert_eq!(
        view["descriptor"]["host_id"],
        finish["descriptor"]["host_id"]
    );
    assert!(view.get("capability").is_none());
    let mut automatic = connect();
    automatic.token = None;
    automatic.allow_legacy_binding = false;
    automatic.only_if_unattached = true;
    let conflict = manager
        .start_outbound_host(&owner, automatic)
        .await
        .unwrap_err();
    assert_eq!(conflict.code, "host_route_observation_changed");
    let mut observation = connect();
    observation.token = None;
    let observed = manager
        .start_outbound_host(&owner, observation)
        .await
        .unwrap();
    assert!(observed.get("attempt_id").is_none());
    assert_eq!(observed["observed_existing"], true);
    assert!(observed.get("relay_release").is_none());
    assert_eq!(
        observed["connection_generation"],
        finish["connection_generation"]
    );
    assert_eq!(observed["transport"], "outbound_wss");
    assert!(observed.get("descriptor").is_some());
    manager
        .cancel_outbound_host(&owner, params(&stale))
        .await
        .unwrap();
    manager
        .cancel_outbound_host(&owner, params(&start))
        .await
        .unwrap();
    manager.detach_socket("browser-observer", None).await;
    let routes = manager.routes.lock().await;
    let route = routes.get("a").unwrap();
    assert_eq!(route.generation, generation);
    assert!(route.remote.is_connected());
    drop(routes);
    assert_eq!(
        manager
            .finish_outbound_host(&owner, params(&start))
            .await
            .unwrap()["transport"],
        "outbound_wss"
    );
    manager.close().await.unwrap();
    native.abort();
    server.abort();
}

#[tokio::test]
async fn legacy_outbound_peer_closing_unknown_inspect_never_receives_pair() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::new(config(root.path())).await.unwrap();
    let owner = Principal::for_test("owner");
    let start = manager
        .start_outbound_host(&owner, connect())
        .await
        .unwrap();
    let (mut socket, server) = socket(manager.clone(), &start).await;
    let Message::Binary(bytes) = socket.next().await.unwrap().unwrap() else {
        panic!("Inspect required")
    };
    assert!(matches!(
        decode_frame(&bytes).unwrap(),
        HostFrame::Inspect { .. }
    ));
    // Historical Bridges reject the unknown frame before any Pair/Resume.
    socket.close(None).await.unwrap();
    let error = manager
        .finish_outbound_host(&owner, params(&start))
        .await
        .unwrap_err();
    assert_eq!(error.code, "host_outbound_unavailable");
    assert_eq!(
        error.details.as_ref().unwrap()["pairing_token_consumed"],
        false
    );
    assert!(manager.routes.lock().await.get("a").is_none());
    assert!(manager.pins.project("a").await.unwrap().is_none());
    manager.close().await.unwrap();
    server.abort();
    let _ = server.await;
}
