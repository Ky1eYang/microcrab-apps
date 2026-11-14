//! The assembled Server exercises real provider retries before exposing explicit retry.
use super::*;
use axum::response::IntoResponse;
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_provider_six_backoff_attempts_then_manual_retry_preserve_one_input() {
    let root = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let wire = Arc::new(Wire {
        requests: Mutex::new(vec![]),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let starts = Arc::new(Mutex::new(Vec::<Instant>::new()));
    let observed_starts = starts.clone();
    let router = Router::new().route("/chat/completions", post(move |HttpState(wire): HttpState<Arc<Wire>>, headers: HeaderMap, Json(body): Json<Value>| {
        let starts = observed_starts.clone();
        async move {
            starts.lock().unwrap().push(Instant::now());
            let attempt = {
                let authorization = headers.get("authorization").unwrap().to_str().unwrap().to_owned();
                let mut requests = wire.requests.lock().unwrap();
                requests.push((authorization, body));
                requests.len()
            };
            if attempt <= 6 {
                ([(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                    "data: {\"error\":{\"type\":\"server_error\",\"message\":\"fixture-private-upstream-body\"}}\n\n").into_response()
            } else {
                Json(json!({"choices":[{"message":{"role":"assistant","content":"Recovered real provider answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}})).into_response()
            }
        }
    })).with_state(wire.clone());
    let worker = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let a = ProviderMock {
        endpoint,
        wire,
        worker,
    };
    let b = provider(1).await;
    let config = config(root.path(), &a, &b);
    let manager = Manager::with_host(config, Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let managed = create(&manager).await;
    let id = managed.record.lock().await.id.clone();
    let failure_id = uuid::Uuid::new_v4().to_string();
    {
        let _gate = managed.commands.lock().await;
        commands::submit(&manager, managed.clone(), &command("turn.submit", Some(&id),
            json!({"operation_id":failure_id,"input":microcrab_session::SessionInput::text("one original input")}))).await.unwrap();
    }
    let worker = managed.worker.lock().await.take().unwrap();
    tokio::time::timeout(Duration::from_secs(60), worker)
        .await
        .expect("six real provider attempts settle")
        .unwrap();
    assert!(!managed.faulted.load(Ordering::Acquire));
    assert!(managed.session.retry_ready());
    let snapshot = managed.hub.snapshot();
    assert_eq!(snapshot["session"]["retry_ready"], true);
    assert_eq!(snapshot["session"]["retry_operation_id"], failure_id);
    assert_eq!(snapshot["messages"].as_array().unwrap().len(), 1);
    assert!(
        !serde_json::to_string(&snapshot)
            .unwrap()
            .contains("fixture-private-upstream-body")
    );
    let failure = manager
        .directory
        .get_operation("owner", &failure_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failure.status, "failed");
    assert_eq!(failure.result.as_ref().unwrap()["retry_ready"], true);
    assert!(
        !serde_json::to_string(&failure)
            .unwrap()
            .contains("fixture-private-upstream-body")
    );
    {
        let starts = starts.lock().unwrap();
        assert_eq!(starts.len(), 6);
        for (pair, seconds) in starts.windows(2).zip([1, 2, 4, 8, 16]) {
            let elapsed = pair[1].duration_since(pair[0]);
            assert!(
                elapsed >= Duration::from_millis(seconds * 1000 - 100),
                "backoff {seconds}s was {elapsed:?}"
            );
        }
        assert!(starts[5].duration_since(starts[0]) >= Duration::from_secs(30));
    }
    let retry_id = uuid::Uuid::new_v4().to_string();
    {
        let _gate = managed.commands.lock().await;
        commands::submit(
            &manager,
            managed.clone(),
            &command(
                "turn.retry",
                Some(&id),
                json!({"operation_id":retry_id,"failed_operation_id":failure_id}),
            ),
        )
        .await
        .unwrap();
    }
    let worker = managed.worker.lock().await.take().unwrap();
    tokio::time::timeout(Duration::from_secs(10), worker)
        .await
        .expect("manual retry settles")
        .unwrap();
    let completed = manager
        .directory
        .get_operation("owner", &retry_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.status, "completed");
    let snapshot = managed.hub.snapshot();
    assert_eq!(snapshot["session"]["retry_ready"], false);
    assert_eq!(snapshot["messages"].as_array().unwrap().len(), 2);
    assert_eq!(snapshot["messages"][0]["role"], "user");
    assert_eq!(
        snapshot["messages"][1]["text"],
        "Recovered real provider answer"
    );
    assert!(
        !serde_json::to_string(&snapshot)
            .unwrap()
            .contains("fixture-private-upstream-body")
    );
    {
        let requests = a.wire.requests.lock().unwrap();
        assert_eq!(requests.len(), 7);
        for request in &requests[1..] {
            assert_eq!(request, &requests[0]);
        }
    }
    assert_eq!(starts.lock().unwrap().len(), 7);
    manager.close().await.unwrap();
}
