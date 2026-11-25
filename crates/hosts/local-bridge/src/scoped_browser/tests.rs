use super::*;
use futures_util::{SinkExt, StreamExt};
use microcrab_host_api::web_transport::{ProjectScope, WEB_MAX_WS_RECEIVE_BYTES};
use microcrab_host_protocol::{TransferRef, sha256};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

fn endpoint(session: &str, epoch: u64, connection: &str) -> ConnectionScope {
    ConnectionScope {
        binding: ProjectScope {
            backend_id: "backend".into(),
            binding_id: "binding".into(),
            project_id: "project".into(),
        },
        session_id: session.into(),
        epoch,
        connection_id: connection.into(),
    }
}

fn browser(scope: &ConnectionScope) -> ScopedBrowser {
    ScopedBrowser::new(
        BindingScope::Project(scope.binding.clone()),
        CancellationToken::new(),
        true,
    )
}

async fn execute(
    browser: &ScopedBrowser,
    id: &str,
    request: WebTransportRequest,
) -> std::result::Result<(serde_json::Value, Vec<WireFrame>), Error> {
    let admitted_at = Instant::now();
    browser
        .execute(
            id.into(),
            request,
            2000,
            BrowserTiming {
                admitted_at,
                deadline: admitted_at + Duration::from_secs(2),
            },
            &CancellationToken::new(),
        )
        .await
        .map(
            |BrowserReply {
                 value,
                 chunks,
                 pull,
             }| {
                if let Some(pull) = pull {
                    pull.delivered();
                }
                (value, chunks)
            },
        )
        .map_err(Error::from)
}

fn opening(scope: &ConnectionScope, url: &str) -> WebTransportRequest {
    WebTransportRequest::WsOpen {
        scope: scope.clone(),
        url: url.into(),
    }
}

#[tokio::test]
async fn pending_open_is_owned_and_exact_close_fences_late_publication() {
    let scope = endpoint("one", 0, "pending");
    let browser = Arc::new(browser(&scope));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/fixture", listener.local_addr().unwrap());
    let open = tokio::spawn({
        let browser = browser.clone();
        let request = opening(&scope, &url);
        async move { execute(&browser, "first", request).await }
    });
    let (mut peer, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        execute(
            &browser,
            "other",
            opening(&endpoint("one", 0, "other"), &url)
        )
        .await
        .unwrap_err(),
        Error::InvalidRequest
    );
    let mut foreign = scope.clone();
    foreign.binding.project_id = "another-project".into();
    assert_eq!(
        execute(
            &browser,
            "foreign",
            WebTransportRequest::WsClose { scope: foreign }
        )
        .await
        .unwrap_err(),
        Error::Denied
    );
    execute(
        &browser,
        "close",
        WebTransportRequest::WsClose {
            scope: scope.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(open.await.unwrap().unwrap_err(), Error::Stale);
    assert_eq!(
        execute(&browser, "late", opening(&scope, &url))
            .await
            .unwrap_err(),
        Error::Stale
    );
    assert_eq!(
        execute(&browser, "first", opening(&scope, &url))
            .await
            .unwrap_err(),
        Error::OutcomeUnknown
    );
    let mut bytes = Vec::new();
    use tokio::io::AsyncReadExt;
    tokio::time::timeout(Duration::from_secs(1), peer.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(40), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn independent_sessions_release_and_transfer_bound() {
    let one = endpoint("one", 0, "a");
    let two = endpoint("two", 0, "b");
    let browser = browser(&one);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/fixture", listener.local_addr().unwrap());
    let text = "\u{0001}".repeat(WEB_MAX_WS_RECEIVE_BYTES as usize);
    let fixture = tokio::spawn({
        let text = text.clone();
        async move {
            let mut first = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let mut second = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            second.send(Message::Text(text.into())).await.unwrap();
            assert!(matches!(first.next().await, None | Some(Err(_))));
            assert!(matches!(second.next().await, None | Some(Err(_))));
        }
    });
    execute(&browser, "one-open", opening(&one, &url))
        .await
        .unwrap();
    execute(&browser, "two-open", opening(&two, &url))
        .await
        .unwrap();
    execute(
        &browser,
        "one-release",
        WebTransportRequest::ReleaseScope { owner: one.owner() },
    )
    .await
    .unwrap();
    assert_eq!(
        execute(
            &browser,
            "one-closed",
            WebTransportRequest::WsStatus { scope: one.clone() }
        )
        .await
        .unwrap_err(),
        Error::Stale
    );
    assert_eq!(
        execute(
            &browser,
            "one-reopen",
            opening(&endpoint("one", 0, "never"), &url)
        )
        .await
        .unwrap_err(),
        Error::Stale
    );
    let (value, chunks) = execute(
        &browser,
        "two-read",
        WebTransportRequest::WsReceive {
            scope: two,
            max_bytes: WEB_MAX_WS_RECEIVE_BYTES,
        },
    )
    .await
    .unwrap();
    let transfer: TransferRef = serde_json::from_value(value).unwrap();
    let mut bytes = Vec::new();
    let total = chunks.len();
    for (i, chunk) in chunks.into_iter().enumerate() {
        let WireFrame::TransferChunk {
            transfer_id,
            sequence,
            data,
            eof,
        } = chunk
        else {
            panic!("chunk expected")
        };
        assert_eq!(transfer.transfer_id, transfer_id);
        assert_eq!(sequence as usize, i);
        assert_eq!(eof, i + 1 == total);
        bytes.extend(data);
    }
    assert_eq!(transfer.byte_length as usize, bytes.len());
    assert_eq!(transfer.sha256, sha256(&bytes));
    assert_eq!(
        serde_json::from_slice::<WsReceive>(&bytes).unwrap(),
        WsReceive::Message(text)
    );
    browser.stop();
    tokio::time::timeout(Duration::from_secs(1), fixture)
        .await
        .unwrap()
        .unwrap();
}

fn pulling(scope: &ConnectionScope, max_bytes: u32, soft_wait_ms: u32) -> WebTransportRequest {
    WebTransportRequest::WsPull {
        scope: scope.clone(),
        max_bytes,
        soft_wait_ms,
    }
}

async fn ws_pull_result(
    browser: &ScopedBrowser,
    id: &str,
    request: WebTransportRequest,
) -> std::result::Result<WsPull, Error> {
    let (value, chunks) = execute(browser, id, request).await?;
    assert!(chunks.is_empty());
    Ok(serde_json::from_value(value).unwrap())
}

#[tokio::test]
async fn scoped_pull_preserves_empty_and_rejected_head_then_drains_fifo() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let scope = endpoint("one", 1, "pull");
        let browser = browser(&scope);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/fixture", listener.local_addr().unwrap());
        let (burst, send_burst) = tokio::sync::oneshot::channel();
        let fixture = async {
            let mut peer = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            send_burst.await.unwrap();
            for index in 0..64 {
                peer.send(Message::Text(format!("{index:02}").into()))
                    .await
                    .unwrap();
            }
            peer.send(Message::Close(None)).await.unwrap();
        };
        let client = async {
            execute(&browser, "open", opening(&scope, &url))
                .await
                .unwrap();
            assert_eq!(
                ws_pull_result(&browser, "zero", pulling(&scope, 2, 0)).await,
                Ok(WsPull::Empty)
            );
            assert_eq!(
                ws_pull_result(&browser, "soft", pulling(&scope, 2, 20)).await,
                Ok(WsPull::Empty)
            );
            assert_eq!(
                ws_pull_result(&browser, "invalid", pulling(&scope, 2, 1001)).await,
                Err(Error::Limit)
            );
            let mut foreign = scope.clone();
            foreign.epoch += 1;
            assert_eq!(
                ws_pull_result(&browser, "foreign", pulling(&foreign, 2, 0)).await,
                Err(Error::Stale)
            );
            burst.send(()).unwrap();
            assert_eq!(
                ws_pull_result(&browser, "undersized", pulling(&scope, 1, 500)).await,
                Err(Error::Limit)
            );
            for index in 0..64 {
                assert_eq!(
                    ws_pull_result(&browser, &format!("pull-{index}"), pulling(&scope, 2, 500))
                        .await,
                    Ok(WsPull::Message(format!("{index:02}")))
                );
            }
            assert_eq!(
                ws_pull_result(&browser, "closed", pulling(&scope, 2, 500)).await,
                Ok(WsPull::Closed)
            );
        };
        tokio::join!(fixture, client);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn scoped_pull_preserves_precommit_cancel_but_fences_lost_committed_egress() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let scope = endpoint("one", 1, "egress");
        let browser = browser(&scope);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/fixture", listener.local_addr().unwrap());
        let (burst, send_burst) = tokio::sync::oneshot::channel();
        let fixture = async {
            let mut peer = accept_async(listener.accept().await.unwrap().0).await.unwrap();
            send_burst.await.unwrap();
            for value in ["first", "second"] {
                peer.send(Message::Text(value.into())).await.unwrap();
            }
            assert!(matches!(peer.next().await, None | Some(Err(_))));
        };
        let client = async {
            use futures_util::FutureExt;
            execute(&browser, "open", opening(&scope, &url)).await.unwrap();
            let cancel = CancellationToken::new();
            let admitted_at = Instant::now();
            let timing = || BrowserTiming { admitted_at, deadline: admitted_at + Duration::from_secs(2) };
            let mut waiting = Box::pin(browser.execute("cancel".into(), pulling(&scope, 8, 500), 2000, timing(), &cancel));
            assert!(waiting.as_mut().now_or_never().is_none());
            cancel.cancel();
            assert!(matches!(waiting.await, Err(ref error) if Error::from(error.clone()) == Error::Cancelled));
            burst.send(()).unwrap();
            let reply = browser.execute("commit".into(), pulling(&scope, 8, 500), 2000, timing(), &CancellationToken::new()).await.unwrap();
            assert_eq!(serde_json::from_value::<WsPull>(reply.value).unwrap(), WsPull::Message("first".into()));
            let commit = reply.pull.expect("message must own committed egress");
            // An unadmitted commit keeps the slot exclusive. Even a soft wait
            // cannot claim Empty behind that gate without seeing the FIFO.
            assert_eq!(ws_pull_result(&browser, "busy-zero", pulling(&scope, 8, 0)).await, Err(Error::Timeout));
            assert_eq!(ws_pull_result(&browser, "busy-soft", pulling(&scope, 8, 20)).await, Err(Error::Timeout));
            drop(commit);
            assert_eq!(ws_pull_result(&browser, "fenced", pulling(&scope, 8, 0)).await, Ok(WsPull::Closed));
        };
        tokio::join!(fixture, client);
    }).await.unwrap();
}
