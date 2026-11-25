use super::*;
use std::{io::Write, sync::Mutex};

#[derive(Clone, Default)]
struct Records(Arc<Mutex<Vec<u8>>>);

impl Write for Records {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Records {
    fn subscriber(&self) -> tracing::Dispatch {
        self.subscriber_at(tracing::Level::WARN)
    }

    fn subscriber_at(&self, level: tracing::Level) -> tracing::Dispatch {
        let output = self.clone();
        tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_max_level(level)
                .with_writer(move || output.clone())
                .finish(),
        )
    }

    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }

    async fn wait_for(&self, evidence: &str) {
        while !self.text().contains(evidence) {
            tokio::task::yield_now().await;
        }
    }
}

const MARKER: &str = "never-log-this-frame-or-url";

#[tokio::test]
async fn ws_inbound_backpressure_preserves_fifo_and_warns_at_most_once_per_kind() {
    fixture(async {
        let large_bytes = MAX_RECEIVE_BYTES / 2 + 1;
        for (messages, reason, counts) in [
            (
                (0..6)
                    .map(|index| {
                        format!(
                            "{MARKER}{index}{}",
                            "x".repeat(large_bytes - MARKER.len() - 1)
                        )
                    })
                    .collect::<Vec<_>>(),
                "inbound_queue_byte_pressure",
                (1, large_bytes, 1, large_bytes),
            ),
            (
                (0..70).map(|index| format!("{MARKER}{index:02}")).collect(),
                "inbound_queue_count_pressure",
                (32, 32 * (MARKER.len() + 2), 0, 0),
            ),
        ] {
            let lease = CancellationToken::new();
            let cancel = CancellationToken::new();
            let records = Records::default();
            let input = listener().await;
            let endpoint = url(&input, "ws", &format!("/{MARKER}"));
            let operation = op(&lease, &cancel);
            let observing_id = uuid::Uuid::new_v4();
            let observing = op(&lease, &cancel).with_bridge_operation_id(Some(observing_id));
            let server = async {
                let (stream, _) = input.accept().await.unwrap();
                let mut peer = accept_async(stream).await.unwrap();
                let (burst, tail) = messages.split_at(if counts.2 == 0 { 33 } else { 2 });
                for message in burst {
                    peer.send(Message::Text(message.as_str().into()))
                        .await
                        .unwrap();
                }
                // Application writes still progress with reads paused.
                assert_eq!(
                    peer.next().await.unwrap().unwrap(),
                    Message::Text("while-paused".into())
                );
                // Send more after the pause, then close behind the complete burst.
                for message in tail {
                    peer.send(Message::Text(message.as_str().into()))
                        .await
                        .unwrap();
                }
                peer.send(Message::Close(None)).await.unwrap();
            };
            let client = async {
                let mut socket = TextSocket::open_with_idle(
                    &endpoint,
                    &operation,
                    Duration::from_millis(80),
                    Duration::from_millis(20),
                )
                .await
                .unwrap();
                records.wait_for(reason).await;
                // Remain full longer than both idle and control intervals. A
                // new probe cannot be serviced while we deliberately pause.
                tokio::time::sleep(Duration::from_millis(150)).await;
                assert_eq!(socket.status(), Ok(()));
                socket
                    .send("while-paused".into(), &operation)
                    .await
                    .unwrap();
                for message in &messages {
                    assert_eq!(
                        socket
                            .receive(MAX_RECEIVE_BYTES, &observing)
                            .await
                            .as_deref(),
                        Ok(message.as_str())
                    );
                }
                assert_eq!(
                    socket.receive(MAX_RECEIVE_BYTES, &observing).await,
                    Err(Error::Closed)
                );
                assert_eq!(socket.status(), Err(Error::Closed));
            };
            tokio::join!(server, client.with_subscriber(records.subscriber()));
            assert!(input.accept().now_or_never().is_none());
            let text = records.text();
            assert!(!text.contains(MARKER), "{text}");
            assert!(!text.contains("snapshot_ordinal="), "{text}");
            assert!(
                !text.contains("bridge_websocket_inbound_queue_snapshot"),
                "{text}"
            );
            let pressure: Vec<_> = text.lines().filter(|line| line.contains(reason)).collect();
            assert_eq!(pressure.len(), 2, "{text}");
            let actor = pressure
                .iter()
                .find(|line| !line.contains("_observed"))
                .unwrap();
            let observed = pressure
                .iter()
                .find(|line| line.contains("_observed"))
                .unwrap();
            assert!(!actor.contains("bridge_operation_id"), "{text}");
            assert!(
                observed.contains(&format!("bridge_operation_id={observing_id}")),
                "{text}"
            );
            for line in [actor, observed] {
                assert!(line.contains("WARN"), "{text}");
                for (field, value) in [
                    ("buffered_messages", counts.0),
                    ("buffered_bytes", counts.1),
                    ("staged_messages", counts.2),
                    ("staged_bytes", counts.3),
                    ("message_limit", 32),
                    ("byte_limit", MAX_RECEIVE_BYTES),
                    ("ingress_byte_limit", MAX_RECEIVE_BYTES),
                ] {
                    assert!(line.contains(&format!("{field}={value}")), "{text}");
                }
            }
            assert!(text.contains("reason=\"peer_close\""), "{text}");
        }
    })
    .await;
}

#[tokio::test]
async fn ws_pressure_snapshot_is_redacted_bounded_and_preserves_fifo() {
    fixture(async {
        const SNAPSHOT: &str = "bridge_websocket_inbound_queue_snapshot";
        const CANARIES: &[&str] = &[
            "endpoint-token-canary",
            "command-result-canary",
            "987654321",
            "Sensitive.peerMethodCanary",
            "peer-connection-canary",
            "peer-target-canary",
            "invalid-text-canary",
            "arbitrary-key-canary",
        ];
        let prefix = [
            r#"{"id":987654321,"result":{"secret":"command-result-canary"}}"#,
            r#"{"method":"Sensitive.peerMethodCanary","params":{"connectionId":"peer-connection-canary","targetId":"peer-target-canary"}}"#,
            r#"{"method":"invalid-text-canary""#,
            r#"{"arbitrary-key-canary":[1,2,3]}"#,
        ];
        let mut previous_ordinal = None;
        for byte_pressure in [false, true] {
            let mut messages = prefix.map(str::to_owned).to_vec();
            if byte_pressure {
                // A valid but deliberately uninspected frame consumes the one
                // staged slot behind all the smaller messages.
                messages.push(format!(
                    "{}{}",
                    prefix[1],
                    " ".repeat(MAX_RECEIVE_BYTES - prefix[1].len())
                ));
            } else {
                messages.extend((4..34).map(|index| format!("[]{}", " ".repeat(index))));
            }
            let records = Records::default();
            let input = listener().await;
            let endpoint = url(&input, "ws", "/fixture?token=endpoint-token-canary");
            let lease = CancellationToken::new();
            let cancel = CancellationToken::new();
            let operation = op(&lease, &cancel);
            let pull_id = uuid::Uuid::new_v4();
            let pull = op(&lease, &cancel).with_bridge_operation_id(Some(pull_id));
            let server = async {
                let (stream, _) = input.accept().await.unwrap();
                let mut peer = accept_async(stream).await.unwrap();
                for message in &messages {
                    peer.send(Message::Text(message.as_str().into()))
                        .await
                        .unwrap();
                }
                peer.send(Message::Close(None)).await.unwrap();
            };
            let client = async {
                let mut socket = TextSocket::open(&endpoint, &operation).await.unwrap();
                records.wait_for("bridge_websocket_inbound_backpressure").await;
                if cfg!(debug_assertions) {
                    records.wait_for(SNAPSHOT).await;
                }
                for expected in &messages {
                    assert_eq!(
                        socket
                            .receive(MAX_RECEIVE_BYTES, &pull)
                            .await
                            .as_deref(),
                        Ok(expected.as_str())
                    );
                }
                assert_eq!(socket.receive(1, &operation).await, Err(Error::Closed));
            };
            tokio::join!(
                server,
                client.with_subscriber(records.subscriber_at(tracing::Level::DEBUG))
            );
            let text = records.text();
            for &canary in CANARIES {
                assert!(!text.contains(canary), "{text}");
            }
            let snapshots: Vec<_> = text.lines().filter(|line| line.contains(SNAPSHOT)).collect();
            if cfg!(debug_assertions) {
                assert_eq!(snapshots.len(), 1, "{text}");
                let line = snapshots[0];
                assert!(line.contains("DEBUG"), "{line}");
                assert!(!line.contains("bridge_operation_id"), "{line}");
                let ordinal = line
                    .split_whitespace()
                    .find_map(|field| field.strip_prefix("snapshot_ordinal="))
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                assert!(previous_ordinal.is_none_or(|previous| ordinal > previous));
                previous_ordinal = Some(ordinal);
                let pull_warn = text
                    .lines()
                    .find(|line| line.contains("bridge_websocket_inbound_backpressure_observed"))
                    .unwrap();
                assert!(pull_warn.contains(&format!("bridge_operation_id={pull_id}")));
                assert!(
                    pull_warn
                        .split_whitespace()
                        .any(|field| field == format!("snapshot_ordinal={ordinal}")),
                    "{text}"
                );
                let expected_fifo = ["reply_like", "event_like", "invalid_json", "unclassified"]
                    .into_iter()
                    .enumerate()
                    .map(|(index, shape)| format!("{index}:{}:{shape}", prefix[index].len()))
                    .collect::<Vec<_>>()
                    .join(", ");
                assert!(line.contains(&format!("fifo={expected_fifo}")), "{line}");
                if byte_pressure {
                    assert!(line.contains("sampled_fifo=4 omitted_fifo=0"), "{line}");
                    assert!(
                        line.contains(&format!("staged={MAX_RECEIVE_BYTES}:unclassified")),
                        "{line}"
                    );
                } else {
                    assert!(line.contains("sampled_fifo=8 omitted_fifo=24"), "{line}");
                    assert!(line.contains("7:9:unclassified staged=none"), "{line}");
                }
            } else {
                assert!(snapshots.is_empty(), "{text}");
                assert!(!text.contains("snapshot_ordinal="), "{text}");
            }
        }
    })
    .await;
}

#[tokio::test]
async fn ws_saturated_inbox_honors_lease_cancel_and_an_already_sent_probe_deadline() {
    fixture(async {
        for cancel_lease in [true, false] {
            let lease = CancellationToken::new();
            let cancel = CancellationToken::new();
            let operation = op(&lease, &cancel);
            let records = Records::default();
            let input = listener().await;
            let endpoint = url(&input, "ws", "/");
            let (closed, closure) = oneshot::channel();
            let server = async {
                let (stream, _) = input.accept().await.unwrap();
                let mut peer = accept_async(stream).await.unwrap();
                if !cancel_lease {
                    assert!(matches!(peer.next().await, Some(Ok(Message::Ping(_)))));
                    // Replaces tungstenite's queued matching Pong; the probe
                    // stays in flight even after the inbound queue fills.
                    peer.send(Message::Pong(vec![0].into())).await.unwrap();
                }
                for _ in 0..32 {
                    peer.send(Message::Text("".into())).await.unwrap();
                }
                assert!(matches!(peer.next().await, None | Some(Err(_))));
                closed.send(()).unwrap();
            };
            let client = async {
                let mut socket = TextSocket::open_with_idle(
                    &endpoint,
                    &operation,
                    if cancel_lease {
                        Duration::from_secs(15)
                    } else {
                        Duration::from_millis(40)
                    },
                    Duration::from_millis(500),
                )
                .await
                .unwrap();
                records.wait_for("inbound_queue_count_pressure").await;
                if cancel_lease {
                    lease.cancel();
                }
                closure.await.unwrap();
                if cancel_lease {
                    assert_eq!(socket.receive(1, &operation).await, Err(Error::Cancelled));
                } else {
                    for _ in 0..32 {
                        assert_eq!(socket.receive(1, &operation).await.as_deref(), Ok(""));
                    }
                    assert_eq!(socket.receive(1, &operation).await, Err(Error::Closed));
                }
                assert_eq!(socket.status(), Err(Error::Closed));
            };
            tokio::join!(server, client.with_subscriber(records.subscriber()));
            let reason = if cancel_lease {
                "lease_cancelled"
            } else {
                "idle_probe_timeout"
            };
            let text = records.text();
            assert!(text.contains(reason), "{text}");
            assert!(input.accept().now_or_never().is_none());
        }
    })
    .await;
}

#[tokio::test]
async fn ws_single_oversize_warn_is_distinct_from_inbound_backpressure() {
    fixture(async {
        let lease = CancellationToken::new();
        let cancel = CancellationToken::new();
        let input = listener().await;
        let endpoint = url(&input, "ws", &format!("/{MARKER}"));
        let records = Records::default();
        let operation = op(&lease, &cancel);
        let observing_id = uuid::Uuid::new_v4();
        let observing = op(&lease, &cancel).with_bridge_operation_id(Some(observing_id));
        let server = async {
            let (stream, _) = input.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            let oversized = format!(
                "{MARKER}{}",
                "x".repeat(MAX_RECEIVE_BYTES + 1 - MARKER.len())
            );
            peer.send(Message::Text(oversized.into())).await.unwrap();
        };
        let client = async {
            let mut socket = TextSocket::open(&endpoint, &operation).await.unwrap();
            assert_eq!(
                socket.receive(MAX_RECEIVE_BYTES, &observing).await,
                Err(Error::Limit)
            );
            assert_eq!(socket.status(), Err(Error::Closed));
            records.wait_for("bridge_websocket_terminated").await;
        };
        tokio::join!(server, client.with_subscriber(records.subscriber()));
        let text = records.text();
        assert!(!text.contains(MARKER), "{text}");
        assert!(!text.contains("inbound_backpressure"), "{text}");
        assert_eq!(
            text.lines()
                .filter(|line| line.contains("inbound_message_too_large")
                    && line.contains("error_category=Limit"))
                .count(),
            2,
            "{text}"
        );
        assert!(
            text.lines()
                .any(|line| line.contains("bridge_websocket_terminal_observed")
                    && line.contains(&format!("bridge_operation_id={observing_id}"))),
            "{text}"
        );
    })
    .await;
}
