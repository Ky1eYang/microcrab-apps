//! HTTP reply observation through the real Session tool and durable backend.
use super::*;
use futures_util::StreamExt;
use std::time::Duration;
use tokio::sync::Semaphore;

const SPEAKERS: &str = "/api/v1/projects/workspace/external-speakers";
const ORIGIN: &str = "http://127.0.0.1:4096";
const PRIVATE: &str = "ordinary assistant text must not be relayed";
const REPLY: &str = "authorized reply\n\nevent: fake";

struct ReplyClient {
    alias: tokio::sync::watch::Receiver<Option<String>>,
    first: std::sync::atomic::AtomicBool,
    provider_release: Semaphore,
}

#[async_trait::async_trait]
impl microcrab_session::Client for ReplyClient {
    async fn complete(
        &self,
        content: &microcrab_transport::Content,
        _: &dyn microcrab_auditor::Auditor,
        _: &dyn microcrab_auditor::VariableView,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        let _ = content;
        let calls = if self.first.swap(false, std::sync::atomic::Ordering::AcqRel) {
            let mut alias = self.alias.clone();
            let alias = tokio::select! {
                _ = cancel.cancelled() => return Err(microcrab_transport::Error::Cancelled),
                alias = alias.wait_for(Option::is_some) => alias.unwrap().clone().unwrap(),
            };
            vec![microcrab_protocol::ToolCall {
                provider_metadata: None,
                id: "reply-fixture".into(),
                name: "reply_external_speaker".into(),
                arguments: json!({"dialogue_id":alias,"message":REPLY,"is_final":true}),
            }]
        } else {
            // Pause the following provider round, proving `is_final` itself
            // cannot settle the SSE stream. The test releases exactly once.
            tokio::select! {
                _ = cancel.cancelled() => return Err(microcrab_transport::Error::Cancelled),
                permit = self.provider_release.acquire() => permit.unwrap().forget(),
            }
            Vec::new()
        };
        Ok(microcrab_protocol::Response {
            content: PRIVATE.into(),
            provider_metadata: None,
            tool_calls: calls,
            usage: Default::default(),
        })
    }
}

async fn send(fixture: &Fixture, builder: axum::http::request::Builder, data: Body) -> Response {
    fixture
        .router
        .clone()
        .oneshot(builder.body(data).unwrap())
        .await
        .unwrap()
}

fn observe(uri: &str, key: &str) -> axum::http::request::Builder {
    Request::builder()
        .uri(uri)
        .header("authorization", format!("Bearer {key}"))
}

async fn collect(stream: Body) -> String {
    let data = tokio::time::timeout(Duration::from_secs(10), to_bytes(stream, 1048576))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(data.to_vec()).unwrap()
}

#[tokio::test]
async fn external_reply_scoped_cors_does_not_authorize_other_origins_or_owner_apis() {
    let fixture = Fixture::new(false).await;
    let inbox = "/api/v1/sessions/absent/inbox";
    for (path, origin, method, headers, expected) in [
        (
            inbox,
            ORIGIN,
            "GET",
            "Authorization, X-MicroCrab-Dialogue-Id, Last-Event-ID",
            StatusCode::NO_CONTENT,
        ),
        (
            inbox,
            ORIGIN,
            "POST",
            "Authorization, Content-Type, Idempotency-Key",
            StatusCode::NO_CONTENT,
        ),
        (
            inbox,
            ORIGIN,
            "GET",
            "X-Unrecognized",
            StatusCode::FORBIDDEN,
        ),
        (
            inbox,
            "https://other.invalid",
            "GET",
            "Authorization",
            StatusCode::FORBIDDEN,
        ),
        (
            SPEAKERS,
            ORIGIN,
            "PUT",
            "Content-Type",
            StatusCode::METHOD_NOT_ALLOWED,
        ),
    ] {
        let response = send(
            &fixture,
            Request::builder()
                .method("OPTIONS")
                .uri(path)
                .header("origin", origin)
                .header("access-control-request-method", method)
                .header("access-control-request-headers", headers),
            Body::empty(),
        )
        .await;
        assert_eq!(response.status(), expected);
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .and_then(|h| h.to_str().ok()),
            (path == inbox && origin == ORIGIN).then_some(origin)
        );
        assert!(
            !response
                .headers()
                .contains_key("access-control-allow-credentials")
        );
    }
    fixture.state.manager.close().await.unwrap();
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn external_reply_sqlite_stream_replay_revocation_and_cleanup() {
    stream_contract(crate::config::Storage::Sqlite).await;
}

#[tokio::test]
async fn external_reply_file_stream_replay_revocation_and_cleanup() {
    stream_contract(crate::config::Storage::File).await;
}

async fn stream_contract(storage: crate::config::Storage) {
    let (alias_send, alias_receive) = tokio::sync::watch::channel(None);
    let client = Arc::new(ReplyClient {
        alias: alias_receive,
        first: std::sync::atomic::AtomicBool::new(true),
        provider_release: Semaphore::new(0),
    });
    let fixture = Fixture::with_storage(true, Some(client.clone()), storage).await;
    let key = "a".repeat(43);
    let other = "b".repeat(43);
    let set =
        async |revision| {
            let response = fixture.request("PUT", SPEAKERS, Some(json!({
            "operation_id": uuid::Uuid::new_v4().to_string(), "expected_revision": revision,
            "speakers": {
                "monitor": {"action":"set","key":{"kind":"custom","value":key}},
                "other": {"action":"set","key":{"kind":"custom","value":other}}
            }
        }))).await;
            assert_eq!(response.status(), StatusCode::OK);
        };
    set(0).await;
    let session = body(
        fixture
            .request(
                "POST",
                "/api/v1/sessions",
                Some(json!({
                    "project_id":"workspace", "operation_id":uuid::Uuid::new_v4().to_string()
                })),
            )
            .await,
    )
    .await;
    let id = session["id"].as_str().unwrap();
    let inbox = format!("/api/v1/sessions/{id}/inbox");
    let receipt = send(
        &fixture,
        observe(&inbox, &key)
            .method("POST")
            .header("idempotency-key", uuid::Uuid::new_v4().to_string())
            .header("content-type", "application/json"),
        Body::from(r#"{"body":"private external input"}"#),
    )
    .await;
    assert_eq!(receipt.status(), StatusCode::ACCEPTED);
    let receipt = body(receipt).await;
    let alias = receipt["dialogue_id"].as_str().unwrap();
    let path = format!("{inbox}/dialogues/{alias}/reply");
    assert_eq!(
        fixture.request("GET", &path, None).await.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        send(&fixture, observe(&path, &other), Body::empty())
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    let json = body(send(&fixture, observe(&path, &key), Body::empty()).await).await;
    assert!(matches!(
        json["status"].as_str(),
        Some("queued" | "consumed")
    ));
    assert_eq!(json["chunks"], json!([]));
    assert!(json["aggregated_reply"].is_null());

    let response = send(
        &fixture,
        observe(&inbox, &key)
            .header("x-microcrab-dialogue-id", alias)
            .header("origin", ORIGIN)
            .header("accept", "text/event-stream"),
        Body::empty(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["access-control-allow-origin"], ORIGIN);
    let mut receiver = response.into_body().into_data_stream();
    let queued = receiver.next().await.unwrap().unwrap();
    let queued = std::str::from_utf8(&queued).unwrap();
    assert!(queued.contains("id: 0") && queued.contains("event: queued") && queued.contains(alias));
    drop(receiver); // no shared cancellation of the underlying Session

    let owner = fixture
        .state
        .auth
        .authenticate(&{
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                "cookie",
                format!("microcrab_session={}", fixture.token)
                    .parse()
                    .unwrap(),
            );
            headers
        })
        .unwrap();
    let command = |method, params| {
        serde_json::from_value(json!({
            "v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),
            "method":method,"session_id":id,"params":params
        }))
        .unwrap()
    };
    alias_send.send(Some(alias.to_owned())).unwrap();
    let resumed = send(
        &fixture,
        observe(&path, &key)
            .header("accept", "text/event-stream")
            .header("last-event-id", "0"),
        Body::empty(),
    )
    .await;
    assert_eq!(resumed.status(), StatusCode::OK);
    let mut resumed_stream = resumed.into_body().into_data_stream();
    let mut resumed = String::new();
    for marker in ["event: consumed", "event: chunk"] {
        let bytes = tokio::time::timeout(Duration::from_secs(5), resumed_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let event = std::str::from_utf8(&bytes).unwrap();
        assert!(event.contains(marker), "{event}");
        resumed.push_str(event);
    }
    let in_progress = body(send(&fixture, observe(&path, &key), Body::empty()).await).await;
    assert_eq!(in_progress["status"], "consumed");
    assert_eq!(in_progress["chunks"][0]["is_final"], true);
    client.provider_release.add_permits(1);
    let bytes = tokio::time::timeout(Duration::from_secs(5), resumed_stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let done = std::str::from_utf8(&bytes).unwrap();
    assert!(
        done.contains("event: done") && done.contains("id: 3"),
        "{done}"
    );
    resumed.push_str(done);
    assert!(resumed_stream.next().await.is_none());
    assert!(resumed.contains("id: 1",), "{resumed}");
    assert!(resumed.contains("id: 2"), "{resumed}");
    assert!(!resumed.contains("event: queued"));
    assert!(!resumed.contains(PRIVATE));
    assert!(resumed.contains("authorized reply\\n\\nevent: fake"));
    assert_eq!(resumed.matches("\nevent: fake").count(), 0);

    fixture
        .state
        .manager
        .command(&owner, &command("session.close", json!({})))
        .await
        .unwrap();
    let replay = body(send(&fixture, observe(&path, &key), Body::empty()).await).await;
    assert_eq!(replay["status"], "completed");
    assert_eq!(replay["chunks"][0]["message"], REPLY);
    assert_eq!(replay["chunks"][0]["index"], 0);
    assert_eq!(replay["chunks"][0]["is_final"], true);
    assert_eq!(replay["reason"], "turn_settled");
    assert_eq!(replay["aggregated_reply"], REPLY);
    assert!(!replay.to_string().contains(PRIVATE));
    let late = send(
        &fixture,
        observe(&path, &key)
            .header("accept", "text/event-stream")
            .header("last-event-id", "2"),
        Body::empty(),
    )
    .await;
    set(1).await; // same bytes, newly effective credential incarnation
    assert!(collect(late.into_body()).await.is_empty());
    assert_eq!(
        send(&fixture, observe(&path, &key), Body::empty())
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    let current = fixture.inspect(id).await.unwrap();
    let revision = current["session"]["revision"].as_u64().unwrap();
    let session_path = format!("/api/v1/sessions/{id}");
    let remove = async || {
        body(
            fixture
                .request(
                    "DELETE",
                    &session_path,
                    Some(json!({"expected_revision":revision})),
                )
                .await,
        )
        .await
    };
    assert_eq!(remove().await["deleted"], true);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let receipt = remove().await;
            if receipt["state"] == "deleted" {
                break;
            }
            assert!(receipt["failure"].is_null(), "{receipt}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        send(&fixture, observe(&path, &key), Body::empty())
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    fixture.state.manager.close().await.unwrap();
}
