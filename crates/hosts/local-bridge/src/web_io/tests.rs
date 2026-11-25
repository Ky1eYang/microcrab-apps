use super::*;
use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    time::timeout,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const FIXTURE_TIMEOUT: Duration = Duration::from_secs(3);

async fn fixture<F: Future>(body: F) -> F::Output {
    timeout(FIXTURE_TIMEOUT, body)
        .await
        .expect("loopback fixture exceeded its deadline")
}

async fn listener() -> TcpListener {
    TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap()
}

fn url(listener: &TcpListener, scheme: &str, target: &str) -> String {
    format!("{scheme}://{}{target}", listener.local_addr().unwrap())
}

fn op<'a>(lease: &'a CancellationToken, cancel: &'a CancellationToken) -> Operation<'a> {
    Operation::new(Instant::now() + FIXTURE_TIMEOUT, lease, cancel)
}

async fn headers(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        assert!(bytes.len() < 8192, "request headers exceeded fixture limit");
        bytes.push(stream.read_u8().await.unwrap());
    }
    String::from_utf8(bytes).unwrap()
}

#[test]
fn endpoint_rejects_ambiguity_and_preserves_signed_path_and_authority() {
    let endpoint =
        Endpoint::parse("wss://[::1]:0443/a/../b%2f?q=%40&x=1", Kind::WebSocket).unwrap();
    assert_eq!(endpoint.host, "::1");
    assert_eq!(endpoint.port, 443);
    assert_eq!(endpoint.authority, "[::1]:0443");
    assert_eq!(
        endpoint.uri.path_and_query().unwrap().as_str(),
        "/a/../b%2f?q=%40&x=1"
    );
    let endpoint = Endpoint::parse("https://EXAMPLE.test:443?x=%2F", Kind::Http).unwrap();
    assert_eq!(endpoint.authority, "EXAMPLE.test:443");
    assert_eq!(endpoint.uri.path_and_query().unwrap().as_str(), "/?x=%2F");
    for invalid in [
        "example.test:8080",
        "http://example.test:8080",
        "ws://",
        "ws:///path",
        "ws://@example.test",
        "ws://user:pass@example.test",
        "ws://example.test./#x",
        "ws://example.test\\hidden",
        " ws://example.test",
        "ws://exa\tmple.test",
        "ws://example.test:",
        "ws://example.test:0",
        "ws://example.test:65536",
        "ws://127.1/",
        "ws://0x7f000001/",
        "ws://2130706433/",
        "ws://::1/",
        "ws://[::1]foo/",
        "ws://[::1]:/",
        "ws://caf\u{e9}.test/",
    ] {
        assert!(
            matches!(
                Endpoint::parse(invalid, Kind::WebSocket),
                Err(Error::InvalidEndpoint)
            ),
            "accepted {invalid:?}"
        );
    }
    assert!(matches!(
        Endpoint::parse(
            &format!("ws://127.0.0.1/{}", "x".repeat(MAX_ENDPOINT_BYTES)),
            Kind::WebSocket
        ),
        Err(Error::Limit)
    ));
}

#[test]
fn discovery_does_not_authorize_a_new_host_port_or_downgrade() {
    assert_eq!(
        same_discovery_authority("http://LOCALHOST:9222/a", "ws://localhost:9222/b?q=2"),
        Ok(())
    );
    assert_eq!(
        same_discovery_authority("https://[::1]/a", "wss://[0:0:0:0:0:0:0:1]:443/b"),
        Ok(())
    );
    assert_eq!(
        same_discovery_authority("http://localhost:9222/a", "wss://localhost:9222/b"),
        Ok(())
    );
    for (http, ws) in [
        ("http://localhost:9222/a", "ws://127.0.0.1:9222/b"),
        ("http://localhost:9222/a", "ws://localhost:9223/b"),
        ("https://localhost:9222/a", "ws://localhost:9222/b"),
    ] {
        assert_eq!(
            same_discovery_authority(http, ws),
            Err(Error::AuthorityMismatch)
        );
    }
}

#[tokio::test]
async fn http_exact_get_no_cookies_or_redirect_and_bounded_chunks() {
    fixture(async {
        let first = listener().await;
        let redirect = listener().await;
        let endpoint = url(&first, "http", "/a/../b%2f?q=%40&x=1");
        let lease = CancellationToken::new();
        let cancel = CancellationToken::new();
        let operation = op(&lease, &cancel);
        let server = async {
            let (mut stream, _) = first.accept().await.unwrap();
            let request = headers(&mut stream).await;
            let mut lines = request.lines();
            assert_eq!(lines.next(), Some("GET /a/../b%2f?q=%40&x=1 HTTP/1.1"));
            let mut headers: Vec<_> = lines.filter(|line| !line.is_empty()).map(str::to_ascii_lowercase).collect();
            headers.sort_unstable();
            assert_eq!(headers, ["accept-encoding: identity".to_owned(), "connection: close".to_owned(), format!("host: {}", first.local_addr().unwrap())]);
            let response = format!("HTTP/1.1 302 Found\r\nLocation: {}\r\nSet-Cookie: secret=value\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nab\r\n2\r\ncd\r\n0\r\n\r\n", url(&redirect, "http", "/ignored"));
            stream.write_all(response.as_bytes()).await.unwrap();
        };
        let ((), response) = tokio::join!(server, http_get(&endpoint, 4, &operation));
        assert_eq!(response, Ok(HttpResponse { status: 302, body: "abcd".into() }));
        assert!(redirect.accept().now_or_never().is_none());

        let server = async {
            let (mut stream, _) = first.accept().await.unwrap();
            let request = headers(&mut stream).await.to_ascii_lowercase();
            assert!(!request.contains("cookie"));
            stream.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nab\r\n3\r\ncde\r\n0\r\n\r\n").await.unwrap();
        };
        let ((), response) = tokio::join!(server, http_get(&endpoint, 4, &operation));
        assert_eq!(response, Err(Error::Limit));
    }).await;
}

#[tokio::test]
async fn http_cancellation_drops_body_and_preflight_never_dials() {
    fixture(async {
        let input = listener().await;
        let endpoint = url(&input, "http", "/");
        let lease = CancellationToken::new();
        let cancel = CancellationToken::new();
        let expired = Operation::new(Instant::now(), &lease, &cancel);
        assert_eq!(http_get(&endpoint, 16, &expired).await, Err(Error::Timeout));
        let operation = op(&lease, &cancel);
        assert_eq!(
            http_get(&endpoint, MAX_HTTP_BYTES + 1, &operation).await,
            Err(Error::Limit)
        );
        assert!(input.accept().now_or_never().is_none());

        let server = async {
            let (mut stream, _) = input.accept().await.unwrap();
            headers(&mut stream).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\n\r\na")
                .await
                .unwrap();
            cancel.cancel();
            match stream.read(&mut [0]).await {
                Ok(0) => (),
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => (),
                other => panic!("expected closed connection, got {other:?}"),
            }
        };
        let ((), response) = tokio::join!(server, http_get(&endpoint, 16, &operation));
        assert_eq!(response, Err(Error::Cancelled));
    })
    .await;
}

#[tokio::test]
async fn denied_tls_upgrade_never_falls_back_to_plain_http_or_ws() {
    fixture(async {
        let lease = CancellationToken::new();
        let cancel = CancellationToken::new();
        let operation = op(&lease, &cancel);
        for scheme in ["https", "wss"] {
            let input = listener().await;
            let endpoint = url(&input, scheme, "/");
            let server = async {
                let (mut stream, _) = input.accept().await.unwrap();
                // First TLS record, followed by intentionally invalid TLS.
                assert_eq!(stream.read_u8().await.unwrap(), 0x16);
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await
                    .unwrap();
            };
            let client = async {
                if scheme == "https" {
                    assert_eq!(http_get(&endpoint, 16, &operation).await, Err(Error::Io));
                } else {
                    assert!(matches!(
                        TextSocket::open(&endpoint, &operation).await,
                        Err(Error::Io)
                    ));
                }
            };
            tokio::join!(server, client);
            assert!(input.accept().now_or_never().is_none());
        }
    })
    .await;
}

#[tokio::test]
async fn ws_upgrade_ignores_redirect_and_cancels_pending_handshake() {
    fixture(async {
        let input = listener().await;
        let redirect = listener().await;
        let endpoint = url(&input, "ws", "/");
        let lease = CancellationToken::new();
        let cancel = CancellationToken::new();
        let operation = op(&lease, &cancel);
        let server = async {
            let (mut stream, _) = input.accept().await.unwrap();
            headers(&mut stream).await;
            let response = format!(
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: {}\r\nContent-Length: 0\r\n\r\n",
                url(&redirect, "ws", "/forbidden")
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        };
        let ((), result) = tokio::join!(server, TextSocket::open(&endpoint, &operation));
        assert!(matches!(result, Err(Error::InvalidResponse)));
        assert!(redirect.accept().now_or_never().is_none());

        let server = async {
            let (mut stream, _) = input.accept().await.unwrap();
            headers(&mut stream).await;
            lease.cancel();
            assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
        };
        let ((), result) = tokio::join!(server, TextSocket::open(&endpoint, &operation));
        assert!(matches!(result, Err(Error::Cancelled)));
        assert!(input.accept().now_or_never().is_none());
    })
    .await;
}

#[tokio::test]
async fn ws_exact_persistent_text_with_nonconsuming_status_and_ping() {
    fixture(async {
        let input = listener().await;
        let endpoint = url(&input, "ws", "/a/../b%2f?q=%40&x=1");
        let lease = CancellationToken::new();
        let cancel = CancellationToken::new();
        let operation = op(&lease, &cancel);
        let server = async {
            let (stream, _) = input.accept().await.unwrap();
            let authority = input.local_addr().unwrap().to_string();
            let mut socket = tokio_tungstenite::accept_hdr_async(
                stream,
                move |request: &tungstenite::handshake::server::Request, response| {
                    assert_eq!(
                        request.uri().path_and_query().unwrap().as_str(),
                        "/a/../b%2f?q=%40&x=1"
                    );
                    assert_eq!(request.headers()["host"], authority);
                    for header in ["cookie", "authorization", "proxy-authorization", "origin"] {
                        assert!(!request.headers().contains_key(header));
                    }
                    Ok(response)
                },
            )
            .await
            .unwrap();
            assert_eq!(
                socket.next().await.unwrap().unwrap().into_text().unwrap(),
                "first"
            );
            socket.send(Message::Ping(vec![1, 2].into())).await.unwrap();
            socket.send(Message::Text("answer".into())).await.unwrap();
            assert_eq!(
                socket.next().await.unwrap().unwrap(),
                Message::Pong(vec![1, 2].into())
            );
            assert_eq!(
                socket.next().await.unwrap().unwrap().into_text().unwrap(),
                "second"
            );
            assert!(socket.next().await.unwrap().is_err());
        };
        let client = async {
            let mut socket = TextSocket::open(&endpoint, &operation).await.unwrap();
            assert_eq!(socket.status(), Ok(()));
            assert_eq!(
                socket
                    .send("x".repeat(MAX_SEND_BYTES + 1), &operation)
                    .await,
                Err(Error::Limit)
            );
            socket.send("first".into(), &operation).await.unwrap();
            assert_eq!(socket.status(), Ok(()));
            assert_eq!(socket.receive(6, &operation).await.as_deref(), Ok("answer"));
            socket.send("second".into(), &operation).await.unwrap();
            socket.close();
            assert_eq!(socket.status(), Err(Error::Closed));
            assert_eq!(socket.receive(16, &operation).await, Err(Error::Closed));
        };
        tokio::join!(server, client);
        assert!(input.accept().now_or_never().is_none());
    })
    .await;
}

#[tokio::test]
async fn ws_idle_probes_preserve_order_and_exact_cancellation() {
    fixture(async {
        let input = listener().await;
        let endpoint = url(&input, "ws", "/fixture");
        let lease = CancellationToken::new();
        let cancel = CancellationToken::new();
        let operation = op(&lease, &cancel);
        let (probed, ready) = oneshot::channel();
        let server = async {
            let (stream, _) = input.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            peer.send(Message::Ping(vec![1, 2].into())).await.unwrap();
            for text in ["event", "answer"] {
                peer.send(Message::Text(text.into())).await.unwrap();
            }
            assert_eq!(
                peer.next().await.unwrap().unwrap(),
                Message::Pong(vec![1, 2].into())
            );
            let mut previous = None;
            for _ in 0..2 {
                let Message::Ping(nonce) = peer.next().await.unwrap().unwrap() else {
                    panic!("expected a protocol Ping while the client is idle");
                };
                assert_eq!(nonce.len(), 16);
                assert_ne!(previous.as_ref(), Some(&nonce));
                previous = Some(nonce);
                // Reading a Ping queued the matching Pong on the peer.
                peer.flush().await.unwrap();
            }
            probed.send(()).unwrap();
            assert!(matches!(peer.next().await, None | Some(Err(_))));
        };
        let client = async {
            let mut socket = TextSocket::open_with_idle(
                &endpoint,
                &operation,
                Duration::from_millis(60),
                Duration::from_millis(250),
            )
            .await
            .unwrap();
            ready.await.unwrap();
            assert_eq!(socket.status(), Ok(()));
            for text in ["event", "answer"] {
                assert_eq!(socket.receive(16, &operation).await.as_deref(), Ok(text));
            }
            lease.cancel();
            assert_eq!(socket.status(), Err(Error::Closed));
            assert_eq!(socket.receive(16, &operation).await, Err(Error::Cancelled));
            // Keep the handle alive: the exact lease, not Drop, must stop I/O.
            socket
        };
        let ((), _still_owned) = tokio::join!(server, client);
        assert!(input.accept().now_or_never().is_none());
    })
    .await;
}

#[tokio::test]
async fn ws_idle_probe_wrong_pong_closes_without_reconnect() {
    fixture(async {
        let input = listener().await;
        let endpoint = url(&input, "ws", "/fixture");
        let lease = CancellationToken::new();
        let cancel = CancellationToken::new();
        let operation = op(&lease, &cancel);
        let (closed, closure) = oneshot::channel();
        let server = async {
            let (stream, _) = input.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            assert!(matches!(
                peer.next().await.unwrap().unwrap(),
                Message::Ping(_)
            ));
            // Replace tungstenite's queued matching Pong with unrelated data.
            peer.send(Message::Pong(vec![0].into())).await.unwrap();
            assert!(matches!(peer.next().await, None | Some(Err(_))));
            closed.send(()).unwrap();
        };
        let client = async {
            let mut socket = TextSocket::open_with_idle(
                &endpoint,
                &operation,
                Duration::from_millis(30),
                Duration::from_millis(100),
            )
            .await
            .unwrap();
            closure.await.unwrap();
            assert_eq!(socket.receive(16, &operation).await, Err(Error::Closed));
            assert_eq!(socket.status(), Err(Error::Closed));
            assert_eq!(
                socket.send("no replay".into(), &operation).await,
                Err(Error::Closed)
            );
        };
        tokio::join!(server, client);
        assert!(input.accept().now_or_never().is_none());
    })
    .await;
}

#[path = "tests/termination.rs"]
mod termination;

#[tokio::test]
async fn ws_invalid_and_oversize_messages_retire_socket() {
    fixture(async {
        let lease = CancellationToken::new();
        let cancel = CancellationToken::new();
        let operation = op(&lease, &cancel);
        for (message, expected) in [
            (Message::Binary(vec![0].into()), Error::InvalidResponse),
            (Message::Text("\u{e9}\u{e9}".into()), Error::Limit),
            (
                Message::Text("x".repeat(MAX_RECEIVE_BYTES + 1).into()),
                Error::Limit,
            ),
            (Message::Close(None), Error::Closed),
        ] {
            let input = listener().await;
            let endpoint = url(&input, "ws", "/");
            let server = async {
                let (stream, _) = input.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                socket.send(message).await.unwrap();
            };
            let client = async {
                let mut socket = TextSocket::open(&endpoint, &operation).await.unwrap();
                assert_eq!(socket.receive(3, &operation).await, Err(expected));
                assert_eq!(socket.status(), Err(Error::Closed));
                assert_eq!(
                    socket.send("no replay".into(), &operation).await,
                    Err(Error::Closed)
                );
            };
            tokio::join!(server, client);
            assert!(input.accept().now_or_never().is_none());
        }
    })
    .await;
}

#[tokio::test]
async fn ws_read_cancel_timeout_and_dropped_future_close_without_reconnect() {
    fixture(async {
        for mode in ["cancel", "timeout", "drop"] {
            let input = listener().await;
            let endpoint = url(&input, "ws", "/");
            let lease = CancellationToken::new();
            let cancel = CancellationToken::new();
            let operation = op(&lease, &cancel);
            let (started, ready) = oneshot::channel();
            let server = async {
                let (stream, _) = input.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                ready.await.unwrap();
                if mode == "cancel" {
                    lease.cancel();
                }
                assert!(socket.next().await.unwrap().is_err());
            };
            let client = async {
                let mut socket = TextSocket::open(&endpoint, &operation).await.unwrap();
                let expired = Operation::new(Instant::now(), &lease, &cancel);
                assert_eq!(socket.receive(8, &expired).await, Err(Error::Timeout));
                assert_eq!(socket.status(), Ok(()));
                let short =
                    Operation::new(Instant::now() + Duration::from_millis(30), &lease, &cancel);
                let mut receive = Box::pin(socket.receive(
                    8,
                    if mode == "timeout" {
                        &short
                    } else {
                        &operation
                    },
                ));
                assert!(receive.as_mut().now_or_never().is_none());
                started.send(()).unwrap();
                match mode {
                    "cancel" => assert_eq!((&mut receive).await, Err(Error::Cancelled)),
                    "timeout" => assert_eq!((&mut receive).await, Err(Error::Timeout)),
                    _ => (),
                }
                drop(receive);
                assert_eq!(socket.status(), Err(Error::Closed));
            };
            tokio::join!(server, client);
            assert!(input.accept().now_or_never().is_none());
        }
    })
    .await;
}
