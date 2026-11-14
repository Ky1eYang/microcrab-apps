//! Actual Ask review requests follow the current Session target, including restore.
use super::*;

async fn review_provider() -> ProviderMock {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let wire = Arc::new(Wire {
        requests: Mutex::new(vec![]),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let router = Router::new().route("/chat/completions", post(|HttpState(wire): HttpState<Arc<Wire>>, headers: HeaderMap, Json(body): Json<Value>| async move {
        let review = body.get("tools").is_none();
        // State injection also uses user wire messages. Locate the actual fixture
        // input rather than assuming the last wire message is a tool result.
        let messages = body["messages"].as_array().unwrap();
        let input = messages.iter().rposition(|message| message["role"] == "user" && message["content"].to_string().contains("private conversation seed"));
        let output = messages.iter().rposition(|message| message["role"] == "tool");
        let completed = matches!((input, output), (Some(input), Some(output)) if output > input);
        let message = if review {
            json!({"role":"assistant","content":"{\"action\":\"allow\",\"reason\":\"review wire fixture\"}"})
        } else if completed {
            json!({"role":"assistant","content":"Reviewed operation finished"})
        } else {
            json!({"role":"assistant","tool_calls":[{"id":format!("sleep-call-{}", uuid::Uuid::new_v4()),"type":"function","function":{"name":"sleep","arguments":"{\"time_ms\":1}"}}]})
        };
        let authorization = headers.get("authorization").unwrap().to_str().unwrap().to_owned();
        wire.requests.lock().unwrap().push((authorization, body));
        Json(json!({"choices":[{"message":message,"finish_reason":if review || completed {"stop"} else {"tool_calls"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}))
    })).with_state(wire.clone());
    let worker = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    ProviderMock {
        endpoint,
        wire,
        worker,
    }
}

fn assert_review_wire(
    provider: &ProviderMock,
    model: &str,
    credential: &str,
    variant: Option<&str>,
    expected_reviews: usize,
) {
    let requests = provider.wire.requests.lock().unwrap();
    let reviews = requests
        .iter()
        .filter(|(_, body)| body.get("tools").is_none())
        .collect::<Vec<_>>();
    assert_eq!(reviews.len(), expected_reviews);
    for (authorization, body) in reviews {
        assert_eq!(authorization, credential);
        assert_eq!(body["model"], model);
        assert_eq!(
            body.get("reasoning_effort").and_then(Value::as_str),
            variant
        );
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["role"], "system");
        let prompt = messages[0]["content"].as_str().unwrap();
        assert!(prompt.contains("# system\n"));
        assert!(prompt.contains("# tool\n"));
        assert!(prompt.contains("# user\n"));
        assert!(prompt.contains("<untrusted_tool_name>\nsleep\n</untrusted_tool_name>"));
        let request = prompt
            .split_once("<untrusted_permission_request>\n")
            .unwrap()
            .1
            .split_once("\n</untrusted_permission_request>")
            .unwrap()
            .0;
        let request: Value = serde_json::from_str(request).unwrap();
        assert_eq!(request["tool"], "sleep");
        assert!(!prompt.contains("private conversation seed"));
        assert_eq!(messages[1], json!({"role":"user", "content":"begin"}));
    }
}

async fn bind_review_executor(managed: &Arc<Managed>) {
    managed
        .client_tools
        .bind(
            "review-socket",
            &super::super::client_tools::BindRequest {
                client_id: "review-browser".into(),
                catalog_digest: managed.client_tools.snapshot().await.catalog_digest,
            },
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_review_uses_current_wire_model_after_selection_and_restore() {
    let root = tempfile::tempdir().unwrap();
    let a = review_provider().await;
    let b = review_provider().await;
    let config = config(root.path(), &a, &b);
    let manager = Manager::with_host(config.clone(), Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let managed = create(&manager).await;
    bind_review_executor(&managed).await;
    let id = managed.record.lock().await.id.clone();
    let principal = crate::auth::Principal::for_test("owner");
    let initial = commands::execute(
        &manager,
        &principal,
        &command("permission.get", Some(&id), json!({})),
    )
    .await
    .unwrap();
    assert_eq!(initial["config"]["review"], true);
    assert!(initial["config"].get("review_model").is_none());
    assert!(manager.record("owner", &id).await.unwrap().review);
    // Select Ask without changing the creation default under test.
    commands::execute(
        &manager,
        &principal,
        &command(
            "permission.set",
            Some(&id),
            json!({
                "expected_revision":initial["revision"],
                "overrides":[{"tool":{"kind":"exact","value":"sleep"},"args":[],"action":"ask"}],
                "review":initial["config"]["review"]
            }),
        ),
    )
    .await
    .unwrap();
    submit(&manager, &managed, "private conversation seed first").await;
    assert!(!managed.session.has_continuation());
    assert_review_wire(&a, "model-a", "Bearer test-a", None, 1);
    let revision = managed.record.lock().await.revision;
    select(
        &manager,
        &managed,
        revision,
        selection("b", "model-b", Some("high")),
    )
    .await
    .unwrap();
    submit(&manager, &managed, "private conversation seed second").await;
    assert!(!managed.session.has_continuation());
    assert_review_wire(&b, "model-b", "Bearer test-b", Some("high"), 1);
    assert!(managed.session.messages().iter().all(|message| {
        !serde_json::to_string(message)
            .unwrap()
            .contains("review wire fixture")
    }));
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let reopened = Manager::with_host(config, Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let restored = reopened
        .active(reopened.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    bind_review_executor(&restored).await;
    let policy = restored.session.permission_controller().snapshot().unwrap();
    assert!(policy.config.review);
    assert!(policy.config.review_model.is_none());
    submit(
        &reopened,
        &restored,
        "private conversation seed after restore",
    )
    .await;
    assert!(!restored.session.has_continuation());
    assert_review_wire(&a, "model-a", "Bearer test-a", None, 1);
    assert_review_wire(&b, "model-b", "Bearer test-b", Some("high"), 2);
    reopened.close().await.unwrap();
}
