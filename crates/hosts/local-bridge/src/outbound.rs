//! One dial per authenticated bootstrap; no reconnect or mutation replay.
use crate::{
    BridgeService,
    session::{ControlDecision, Live},
};
use futures_util::{SinkExt, StreamExt};
use microcrab_host_protocol::*;
use serde::Deserialize;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::instrument::WithSubscriber;
trait SocketIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> SocketIo for T {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Signal {
    pub attempt_id: String,
    pub capability: String,
    pub url: String,
    pub expires_in_ms: u64,
}
fn target(signal: &Signal, origin: &str) -> Result<url::Url, String> {
    if uuid::Uuid::parse_str(&signal.attempt_id).is_err()
        || signal.capability.len() != 64
        || !signal.capability.bytes().all(|v| v.is_ascii_hexdigit())
        || signal.expires_in_ms == 0
        || signal.expires_in_ms > 30000
    {
        return Err("Malformed or expired outbound attempt capability".into());
    }
    let mut url = url::Url::parse(&signal.url).map_err(|e| format!("Invalid outbound URL: {e}"))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/api/v1/host/outbound"
    {
        return Err("Outbound URL must use the exact Server rendezvous path without credentials, query or fragment".into());
    }
    let scheme = match url.scheme() {
        "wss" => "https",
        "ws" => "http",
        _ => return Err("Outbound URL requires WSS".into()),
    };
    url.set_scheme(scheme)
        .map_err(|_| "Invalid outbound scheme")?;
    if url.origin().ascii_serialization() != origin {
        return Err("Outbound URL origin differs from the configured allowed Server origin".into());
    }
    if scheme == "http"
        && !url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        })
    {
        return Err("Public outbound transport requires verified TLS".into());
    }
    url.set_scheme(if scheme == "https" { "wss" } else { "ws" })
        .map_err(|_| "Invalid outbound scheme")?;
    Ok(url)
}
async fn dial(signal: &Signal, origin: &str) -> Result<WebSocketStream<Box<dyn SocketIo>>, String> {
    let url = target(signal, origin)?;
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|_| "Cannot construct outbound request")?;
    request.headers_mut().insert(
        "origin",
        origin
            .parse()
            .map_err(|_| "Invalid configured Server origin")?,
    );
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {}", signal.capability)
            .parse()
            .map_err(|_| "Invalid outbound capability")?,
    );
    request.headers_mut().insert(
        "x-host-attempt-id",
        signal
            .attempt_id
            .parse()
            .map_err(|_| "Invalid attempt identity")?,
    );
    let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME_BYTES))
        .max_frame_size(Some(MAX_FRAME_BYTES));
    // The explicitly assembled ring provider avoids feature-unification-dependent
    // provider selection; certificate validation always uses public trust roots.
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| format!("TLS version configuration failed: {e}"))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    let host = url
        .host_str()
        .ok_or("Outbound URL has no hostname")?
        .trim_matches(['[', ']'])
        .to_owned();
    let tcp = tokio::net::TcpStream::connect((
        host.as_str(),
        url.port_or_known_default()
            .ok_or("Outbound URL has no port")?,
    ))
    .await
    .map_err(|e| format!("Outbound TCP connection failed: {e}"))?;
    let stream: Box<dyn SocketIo> = if url.scheme() == "wss" {
        let name = rustls::pki_types::ServerName::try_from(host)
            .map_err(|_| "Outbound Server hostname is invalid for TLS")?;
        Box::new(
            tokio_rustls::TlsConnector::from(Arc::new(tls))
                .connect(name, tcp)
                .await
                .map_err(|e| format!("Outbound Server TLS verification failed: {e}"))?,
        )
    } else {
        Box::new(tcp)
    };
    tokio_tungstenite::client_async_with_config(request, stream, Some(config))
        .await
        .map(|(socket, _)| socket)
        .map_err(|e| format!("Outbound Server WebSocket connection failed: {e}"))
}
// Tungstenite at trace level can expose binary payloads, including web
// transport text. Suppress its per-I/O logging; log only closed route facts.
async fn write_frames<S: futures_util::Sink<Message> + Unpin>(
    mut sink: S,
    mut controls: mpsc::Receiver<WireFrame>,
    mut transfers: mpsc::Receiver<WireFrame>,
    mut diagnostics: mpsc::Receiver<WireFrame>,
) {
    loop {
        let frame = tokio::select! {biased;frame=controls.recv()=>frame,frame=transfers.recv()=>frame,frame=diagnostics.recv()=>frame};
        let Some(frame) = frame else { return };
        let Ok(bytes) = encode_frame(&frame) else {
            return;
        };
        if bytes.len() > MAX_FRAME_BYTES {
            return;
        }
        if !matches!(
            tokio::time::timeout(
                Duration::from_secs(10),
                sink.send(Message::Binary(bytes.into()))
                    .with_subscriber(tracing::Dispatch::none())
            )
            .await,
            Ok(Ok(()))
        ) {
            return;
        }
    }
}
pub(crate) async fn run(
    bridge: Arc<BridgeService>,
    signal: Signal,
    origin: String,
    progress: mpsc::Sender<serde_json::Value>,
) {
    let mut socket = match tokio::time::timeout(
        Duration::from_secs(10),
        dial(&signal, &origin).with_subscriber(tracing::Dispatch::none()),
    )
    .await
    {
        Ok(Ok(socket)) => socket,
        result => {
            let message = match result {
                Ok(Err(message)) => message,
                _ => "Outbound connection exceeded 10 seconds".into(),
            };
            let _ = progress
                .send(json!({"status":"failed","phase":"pre_auth","message":message}))
                .await;
            return;
        }
    };
    let first = tokio::time::timeout(
        Duration::from_secs(10),
        socket.next().with_subscriber(tracing::Dispatch::none()),
    )
    .await;
    let frame = match first {
        Ok(Some(Ok(Message::Binary(bytes)))) => {
            decode_frame_with_web_transport_features(&bytes).ok()
        }
        _ => None,
    };
    let Some((mut frame, mut offer)) = frame else {
        let _=progress.send(json!({"status":"failed","phase":"pre_auth","message":"Server handshake frame was not received"})).await;
        return;
    };
    if let WireFrame::Inspect { request } = frame {
        let reply = bridge.inspect(request).await;
        let accepted = matches!(&reply, WireFrame::Inspected { result: Ok(_), .. });
        let Ok(bytes) = encode_frame(&reply) else {
            return;
        };
        if !matches!(
            tokio::time::timeout(
                Duration::from_secs(10),
                socket
                    .send(Message::Binary(bytes.into()))
                    .with_subscriber(tracing::Dispatch::none())
            )
            .await,
            Ok(Ok(()))
        ) || !accepted
        {
            return;
        }
        (frame, offer) = match tokio::time::timeout(
            Duration::from_secs(10),
            socket.next().with_subscriber(tracing::Dispatch::none()),
        )
        .await
        {
            Ok(Some(Ok(Message::Binary(bytes)))) => {
                match decode_frame_with_web_transport_features(&bytes) {
                    Ok(frame) => frame,
                    Err(_) => return,
                }
            }
            _ => return,
        };
    }
    let (bridge, hello) = match bridge.authenticate(frame).await {
        Ok(value) => value,
        Err(error) => {
            if let Ok(bytes) = encode_frame(&WireFrame::HandshakeRejected {
                error: error.clone(),
            }) {
                let _ = tokio::time::timeout(
                    Duration::from_secs(10),
                    socket
                        .send(Message::Binary(bytes.into()))
                        .with_subscriber(tracing::Dispatch::none()),
                )
                .await;
            }
            let _ = progress
                .send(json!({"status":"failed","phase":"pre_auth","message":error.message}))
                .await;
            return;
        }
    };
    let WireFrame::Hello {
        lease_generation, ..
    } = &hello
    else {
        return;
    };
    let generation = lease_generation.clone();
    let (ack, browser) = crate::session::negotiate_browser(&bridge, &generation, offer).await;
    let Ok(bytes) = encode_frame_with_web_transport_features(&hello, ack) else {
        return;
    };
    if !matches!(
        tokio::time::timeout(
            Duration::from_secs(10),
            socket
                .send(Message::Binary(bytes.into()))
                .with_subscriber(tracing::Dispatch::none())
        )
        .await,
        Ok(Ok(()))
    ) {
        let _=progress.send(json!({"status":"failed","phase":"auth_unknown","message":"Authenticated Hello could not be confirmed; obtain a fresh local pairing token"})).await;
        return;
    }
    // Progress is observation only. Closing the page never owns this socket.
    let _ = progress.try_send(json!({"status":"connected"}));
    drop(progress);
    let (control, controls) = mpsc::channel(MAX_PENDING_REQUESTS);
    let (chunks, transfers) = mpsc::channel(8);
    let (diagnostics, diagnostic_frames) = mpsc::channel(8);
    let live = Live::new(
        bridge,
        generation,
        control,
        chunks,
        diagnostics,
        false,
        (false, false, browser),
    );
    let notifications = live.watch_processes();
    let (sink, mut stream) = socket.split();
    let writer = write_frames(sink, controls, transfers, diagnostic_frames);
    let reader = async {
        while let Some(message) = stream
            .next()
            .with_subscriber(tracing::Dispatch::none())
            .await
        {
            let frame = match message {
                Ok(Message::Binary(bytes)) => decode_frame(&bytes).ok(),
                Ok(Message::Ping(_) | Message::Pong(_)) => continue,
                _ => None,
            };
            let Some(frame) = frame else { return };
            if let ControlDecision::Stop(_) = live.on_control(frame).await {
                return;
            }
        }
    };
    let health = async {
        let mut ticks = tokio::time::interval(Duration::from_secs(1));
        loop {
            ticks.tick().await;
            if !live.lease_ok().await {
                return;
            }
        }
    };
    tokio::select! {_=writer=>(),_=reader=>(),_=health=>(),_=live.browser_lease_replaced()=>()}
    notifications.abort();
    live.shutdown("outbound_connection_closed").await;
}

#[cfg(test)]
mod tests {
    use super::*;
    fn signal(url: &str) -> Signal {
        Signal {
            attempt_id: uuid::Uuid::new_v4().to_string(),
            capability: "a".repeat(64),
            url: url.into(),
            expires_in_ms: 30000,
        }
    }
    #[tokio::test]
    async fn bounded_bulk_writer_prioritizes_new_control_frames() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let (observed, mut messages) = mpsc::unbounded_channel();
        let sink = Box::pin(futures_util::sink::unfold((), {
            let gate = gate.clone();
            move |(), message: Message| {
                let gate = gate.clone();
                let observed = observed.clone();
                async move {
                    observed.send(message).map_err(|_| ())?;
                    gate.acquire().await.map_err(|_| ())?.forget();
                    Ok::<(), ()>(())
                }
            }
        }));
        let (control, controls) = mpsc::channel(1);
        let (chunks, transfers) = mpsc::channel(1);
        let (_diagnostics, diagnostic_frames) = mpsc::channel(1);
        chunks
            .try_send(WireFrame::TransferChunk {
                transfer_id: "bulk".into(),
                sequence: 0,
                data: vec![1],
                eof: false,
            })
            .unwrap();
        let writer = tokio::spawn(write_frames(sink, controls, transfers, diagnostic_frames));
        let _first = tokio::time::timeout(Duration::from_secs(2), messages.recv())
            .await
            .unwrap()
            .unwrap();
        chunks
            .try_send(WireFrame::TransferChunk {
                transfer_id: "bulk".into(),
                sequence: 1,
                data: vec![2],
                eof: true,
            })
            .unwrap();
        assert!(
            chunks
                .try_send(WireFrame::TransferChunk {
                    transfer_id: "overflow".into(),
                    sequence: 0,
                    data: vec![3],
                    eof: true
                })
                .is_err()
        );
        control
            .try_send(WireFrame::Pong {
                lease_generation: "lease".into(),
            })
            .unwrap();
        gate.add_permits(1);
        let Message::Binary(bytes) = tokio::time::timeout(Duration::from_secs(2), messages.recv())
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("binary control")
        };
        assert!(matches!(
            decode_frame(&bytes).unwrap(),
            WireFrame::Pong { .. }
        ));
        writer.abort();
    }
    #[tokio::test]
    async fn native_worker_survives_closed_signal_observation() {
        use axum::{Router, extract::WebSocketUpgrade, routing::get};
        use microcrab_host_api::web_transport::ConnectionScope;
        let root = tempfile::tempdir().unwrap();
        let (bridge, token) = BridgeService::open(root.path(), &[], true, None).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let origin = format!("http://{address}");
        let (observed, mut events) = mpsc::channel(4);
        let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/fixture", upstream.local_addr().unwrap());
        let upstream = tokio::spawn(async move {
            let mut peer = tokio_tungstenite::accept_async(upstream.accept().await.unwrap().0)
                .await
                .unwrap();
            assert!(matches!(peer.next().await, None | Some(Err(_))));
        });
        let router = Router::new().route(
            "/api/v1/host/outbound",
            get(move |ws: WebSocketUpgrade| async move {
                ws.on_upgrade(move |mut socket| async move {
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
                    socket
                        .send(axum::extract::ws::Message::Binary(
                            encode_frame_with_web_transport_features(&pair, WebTransportFeatures::V1).unwrap().into(),
                        ))
                        .await
                        .unwrap();
                    let Some(Ok(axum::extract::ws::Message::Binary(bytes))) = socket.recv().await
                    else {
                        panic!("Hello required")
                    };
                    let (hello, ack) = decode_frame_with_web_transport_features(&bytes).unwrap();
                    assert_eq!(ack, WebTransportFeatures::V1);
                    let WireFrame::Hello { lease_generation, .. } = hello
                    else {
                        panic!("Hello required")
                    };
                    observed.send("hello").await.unwrap();
                    let BindingScope::Project(binding) = binding else { unreachable!() };
                    let request = WireFrame::Request {
                        request_id: "open".into(), operation_id: "open-op".into(),
                        lease_generation: lease_generation.clone(), remaining_budget_ms: 2000,
                        method: HostRequest::WebTransport(WebTransportRequest::WsOpen {
                            scope: ConnectionScope { binding, session_id: "synthetic".into(), epoch: 1, connection_id: "echo".into() }, url,
                        }),
                    };
                    socket.send(axum::extract::ws::Message::Binary(encode_frame(&request).unwrap().into())).await.unwrap();
                    let Some(Ok(axum::extract::ws::Message::Binary(bytes))) = socket.recv().await else { panic!("open reply required") };
                    assert!(matches!(decode_frame(&bytes).unwrap(), WireFrame::Response { result: Ok(value), .. } if value.is_null()));
                    socket
                        .send(axum::extract::ws::Message::Binary(
                            encode_frame(&WireFrame::Ping { lease_generation })
                                .unwrap()
                                .into(),
                        ))
                        .await
                        .unwrap();
                    while let Some(Ok(axum::extract::ws::Message::Binary(bytes))) =
                        socket.recv().await
                    {
                        if matches!(decode_frame(&bytes).unwrap(), WireFrame::Pong { .. }) {
                            observed.send("pong").await.unwrap();
                            break;
                        }
                    }
                })
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let (progress, receiver) = mpsc::channel(2);
        drop(receiver);
        let signal = signal(&format!("ws://{address}/api/v1/host/outbound"));
        let worker = tokio::spawn(run(bridge.clone(), signal, origin, progress));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), events.recv())
                .await
                .unwrap(),
            Some("hello")
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), events.recv())
                .await
                .unwrap(),
            Some("pong")
        );
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), upstream)
            .await
            .unwrap()
            .unwrap();
        server.abort();
    }
    #[test]
    fn exact_server_origin_path_and_tls_are_required() {
        assert!(
            target(
                &signal("wss://server.example/api/v1/host/outbound"),
                "https://server.example"
            )
            .is_ok()
        );
        assert!(
            target(
                &signal("ws://127.0.0.1:123/api/v1/host/outbound"),
                "http://127.0.0.1:123"
            )
            .is_ok()
        );
        for (url, origin) in [
            (
                "ws://server.example/api/v1/host/outbound",
                "http://server.example",
            ),
            (
                "wss://other.example/api/v1/host/outbound",
                "https://server.example",
            ),
            (
                "wss://server.example/api/v1/host/outbound?capability=secret",
                "https://server.example",
            ),
            ("wss://server.example/api/v1/ws", "https://server.example"),
        ] {
            assert!(target(&signal(url), origin).is_err());
        }
    }
    #[test]
    fn malformed_or_unbounded_capabilities_are_rejected() {
        let mut value = signal("wss://server.example/api/v1/host/outbound");
        value.expires_in_ms = 30001;
        assert!(target(&value, "https://server.example").is_err());
        value.expires_in_ms = 30000;
        value.capability = "short".into();
        assert!(target(&value, "https://server.example").is_err());
    }
}
