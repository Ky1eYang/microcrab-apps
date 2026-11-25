//! Real assembled providers must admit the declared parallel-call ceiling.
use super::*;
use microcrab_auditor::Auditor;
use std::time::Duration;

async fn wait_retry(managed: &Managed) -> Value {
    let mut events = managed.hub.subscribe().receiver;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = managed.hub.snapshot();
            if snapshot["session"]["retry_ready"] == true {
                break snapshot;
            }
            events.recv().await.unwrap();
        }
    })
    .await
    .expect("pre-effect failure publishes retry identity")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_custom_context_rejects_pre_effect_and_preserves_receipt_details_and_exact_retry() {
    for method in ["turn.submit", "turn.queue"] {
        let root = tempfile::tempdir().unwrap();
        let a = parallel_provider(8).await;
        let b = provider(0).await;
        let mut config = config(root.path(), &a, &b);
        // This regression publishes a revised checkpoint through CreatorJournal::open
        // below. Select that file backend explicitly even in all-features gates.
        config.storage = crate::config::Storage::File;
        let manager = Manager::with_host(config.clone(), Arc::new(crate::noop::NoopHost))
            .await
            .unwrap();
        let managed = create(&manager).await;
        let id = managed.record.lock().await.id.clone();
        let mut checkpoint = managed.session.checkpoint().await.unwrap();
        checkpoint.state.limits.max_context_bytes = 1024 * 1024;
        checkpoint.context.max_bytes = 1024 * 1024;
        manager.close().await.unwrap();
        drop(managed);
        drop(manager);
        let directory = config.data_dir.join("sessions").join(&id);
        let (journal, _) = microcrab_storage::CreatorJournal::open(
            directory.to_str().unwrap(),
            id.clone(),
            true,
            true,
        )
        .await
        .unwrap();
        microcrab_session::Journal::commit(&journal, checkpoint)
            .await
            .unwrap();
        microcrab_session::Journal::close(&journal).await.unwrap();
        let manager = Manager::with_host(config.clone(), Arc::new(crate::noop::NoopHost))
            .await
            .unwrap();
        let managed = manager
            .active(manager.record("owner", &id).await.unwrap(), true)
            .await
            .unwrap();
        assert_eq!(
            managed.session.context_snapshot().unwrap().max_bytes,
            1024 * 1024
        );
        let submitted_id = uuid::Uuid::new_v4().to_string();
        // A context overflow is now a renewable checkpoint: the assembled Server
        // asks before effects, and a Stop keeps exactly the old pre-effect failure.
        let owner = crate::auth::Principal::for_test("owner");
        let bound = managed
            .client_tools
            .bind(
                "context-socket",
                &super::super::client_tools::BindRequest {
                    client_id: "context-browser".into(),
                    catalog_digest: managed.client_tools.snapshot().await.catalog_digest,
                },
            )
            .await
            .unwrap();
        let mut subscription = managed.hub.subscribe();
        manager.command(&owner, &command(method, Some(&id),
            json!({"operation_id":submitted_id,"input":microcrab_session::SessionInput::text("safe parallel fixture")}))).await.unwrap();
        let requested = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let event = subscription.receiver.recv().await.unwrap();
                if matches!(
                    &event.event,
                    crate::manager::SessionRuntimeEvent::RoundLimitRequested(_)
                ) {
                    return event.event.payload()["request"].clone();
                }
            }
        })
        .await
        .expect("context checkpoint asks the current executor");
        assert_eq!(requested["kind"], "context_bytes");
        assert_eq!(requested["session_id"], id);
        assert!(requested["used"].as_u64().unwrap() > 1024 * 1024);
        manager
            .round_limit_reply(
                &owner,
                "context-socket",
                &command(
                    "round_limit.reply",
                    Some(&id),
                    json!({
                        "request_id":requested["request_id"],
                        "lease_id":bound.lease_id.unwrap(),
                        "decision":"stop"
                    }),
                ),
            )
            .await
            .unwrap();
        if method == "turn.submit" {
            let worker = managed.worker.lock().await.take().unwrap();
            tokio::time::timeout(Duration::from_secs(10), worker)
                .await
                .unwrap()
                .unwrap();
        }
        let snapshot = wait_retry(&managed).await;
        let failure_id = snapshot["session"]["retry_operation_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let failure = manager
            .directory
            .get_operation("owner", &failure_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failure.status, "failed");
        assert_eq!(
            failure.method,
            if method == "turn.queue" {
                "turn.inbox"
            } else {
                "turn.submit"
            }
        );
        let result = failure.result.unwrap();
        let error = result["error"].clone();
        assert_eq!(error["code"], "limit");
        assert_eq!(error["details"]["reason"], "tool_result_reservation");
        assert_eq!(error["details"]["calls"], 8);
        assert_eq!(error["details"]["budget_bytes"], 1024 * 1024);
        assert!(error["details"]["required_bytes"].as_u64().unwrap() >= 8 * 64 * 1024 * 6);
        assert_eq!(error["details"]["pre_effect"], true);
        assert_eq!(result["retry_ready"], true);
        assert!(result["retry_witness"].as_str().is_some());
        assert!(!managed.faulted.load(Ordering::Acquire));
        assert!(managed.session.retry_ready());
        assert!(snapshot["state"].get("executions").is_none());
        assert!(snapshot["executions"].as_array().unwrap().is_empty());
        assert_eq!(
            a.wire.requests.lock().unwrap().len(),
            1,
            "HTTP success with admission failure is not auto-retried"
        );
        let public_failure = snapshot["operations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|operation| operation["id"] == failure_id)
            .unwrap();
        assert_eq!(public_failure["result"]["error"], error);
        manager.close().await.unwrap();
        drop(managed);
        drop(manager);
        let manager = Manager::with_host(config, Arc::new(crate::noop::NoopHost))
            .await
            .unwrap();
        let record = manager.record("owner", &id).await.unwrap();
        let offline = manager.inspect(&record).await.unwrap();
        assert_eq!(offline["session"]["retry_operation_id"], failure_id);
        let offline_failure = offline["operations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|operation| operation["id"] == failure_id)
            .unwrap();
        assert_eq!(offline_failure["result"]["error"], error);
        let managed = manager.active(record, true).await.unwrap();
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
            .unwrap()
            .unwrap();
        assert_eq!(
            manager
                .directory
                .get_operation("owner", &retry_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "completed"
        );
        {
            let requests = a.wire.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert_eq!(
                requests[0], requests[1],
                "manual retry resends the exact prepared request"
            );
        }
        submit(&manager, &managed, "new input after recovered limit").await;
        assert_eq!(a.wire.requests.lock().unwrap().len(), 3);
        assert!(!managed.session.retry_ready());
        assert!(!managed.faulted.load(Ordering::Acquire));
        manager.close().await.unwrap();
    }
}

async fn parallel_provider(calls: usize) -> ProviderMock {
    parallel_provider_tool(calls, false).await
}
async fn parallel_provider_tool(calls: usize, controlled: bool) -> ProviderMock {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let wire = Arc::new(Wire {
        requests: Mutex::new(vec![]),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let router = Router::new().route("/chat/completions", post(move |HttpState(wire): HttpState<Arc<Wire>>, headers: HeaderMap, Json(body): Json<Value>| async move {
        let first = {
            let mut requests = wire.requests.lock().unwrap();
            requests.push((headers.get("authorization").unwrap().to_str().unwrap().to_owned(), body));
            requests.len() == 1
        };
        if first {
            if controlled {
                wire.entered.add_permits(1);
                wire.release.acquire().await.unwrap().forget();
            }
            let tool_calls: Vec<_> = (0..calls).map(|index| json!({"id":format!("parallel-{index}"),"type":"function","function":{"name":if controlled {"controlled_result"} else {"sleep"},"arguments":if controlled {"{}"} else {"{\"time_ms\":1}"}}})).collect();
            Json(json!({"choices":[{"message":{"role":"assistant","content":null,"tool_calls":tool_calls},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}))
        } else {
            Json(json!({"choices":[{"message":{"role":"assistant","content":"Parallel calls completed"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}))
        }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_default_retrying_provider_admits_eight_and_sixteen_parallel_tool_calls() {
    // Repeat independent Session creations so an intermittent owner handoff is
    // exercised without retrying or weakening a rejected admission.
    for (cycle, calls) in [8, 16].into_iter().cycle().take(8).enumerate() {
        let root = tempfile::tempdir().unwrap();
        let a = parallel_provider_tool(calls, true).await;
        let b = provider(0).await;
        let manager =
            Manager::with_host(config(root.path(), &a, &b), Arc::new(crate::noop::NoopHost))
                .await
                .unwrap();
        let created = commands::create(&manager, "owner", &command("session.create", None,
            json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string(),"client_tools":[{
                "name":"controlled_result","description":"Deterministic ordered tool result fixture",
                "parameters":{"type":"object","additionalProperties":false},"reclaimable":true,
                "permissions":[{"tool":{"kind":"exact","value":"controlled_result"},"args":[],"action":"allow"}]
            }]}))).await.unwrap();
        let managed = manager
            .active(
                manager
                    .record("owner", created["id"].as_str().unwrap())
                    .await
                    .unwrap(),
                true,
            )
            .await
            .unwrap();
        let bound = managed
            .client_tools
            .bind(
                "parallel-fixture-socket",
                &super::super::client_tools::BindRequest {
                    client_id: "parallel-fixture-client".into(),
                    catalog_digest: managed.client_tools.snapshot().await.catalog_digest,
                },
            )
            .await
            .unwrap();
        let limits = managed.session.state().limits;
        assert_eq!(limits.max_context_bytes, 64 * 1024 * 1024);
        assert_eq!(limits.max_tool_output_bytes, 64 * 1024);
        assert_eq!(limits.max_tool_calls_per_round, 16);
        let id = managed.record.lock().await.id.clone();
        let operation = uuid::Uuid::new_v4().to_string();
        {
            let _gate = managed.commands.lock().await;
            let result = commands::submit(&manager, managed.clone(), &command("turn.submit", Some(&id),
                json!({"operation_id":operation,"input":microcrab_session::SessionInput::text("parallel tool fixture")}))).await;
            if let Err(error) = result {
                // Capture live owners before awaiting storage or running cleanup.
                // A rejected stream reservation returns a failed receipt; Err(busy)
                // can instead come from idle admission or pre-stream view refresh.
                let inbox = managed.session.inbox_snapshot();
                let state = managed.session.state();
                let snapshot = managed.hub.snapshot();
                let owners = json!({
                    "cycle":cycle,"calls":calls,
                    "hub_operation":managed.hub.operation(),
                    "inbox":{
                        "active":inbox.active,"paused":inbox.paused,
                        "items":inbox.items.len(),"used_bytes":inbox.used_bytes,
                        "execution_started_at_ms":inbox.execution_started_at_ms,
                        "execution_ended_at_ms":inbox.execution_ended_at_ms,
                    },
                    "state":{
                        "epoch":state.epoch_id,"turn":state.turn_id,"round":state.round_id,
                        "executions":state.executions.len(),"agent_init":state.agent_init.len(),
                        "agent_initialization_pending":state.agent_initialization_pending,
                        "retry_ready":managed.session.retry_ready(),
                    },
                    "flags":{
                        "closing":managed.closing.load(Ordering::Acquire),
                        "faulted":managed.faulted.load(Ordering::Acquire),
                        "suspended":managed.suspended.load(Ordering::Acquire),
                    },
                    "hub_status":snapshot["session"]["status"],
                    "hub_retry_ready":snapshot["session"]["retry_ready"],
                });
                let receipt = manager
                    .directory
                    .get_operation("owner", &operation)
                    .await
                    .unwrap();
                let receipt = receipt.map(|receipt| {
                    json!({
                        "status":receipt.status,
                        "error":receipt.result.as_ref().and_then(|result| result.get("error")),
                    })
                });
                panic!(
                    "parallel fixture submit failed: {error:?}; owners={owners}; receipt={receipt:?}"
                );
            }
        }
        tokio::time::timeout(Duration::from_secs(5), a.wire.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        tokio::time::timeout(Duration::from_secs(5), managed.auditor.flush())
            .await
            .unwrap()
            .unwrap();
        a.wire.release.add_permits(1);
        // A provider batch contains N calls; Runtime executes Final results in
        // order. Pending follows ToolCall; the next pending also follows the
        // previous ToolResult, so each flush bounds this fixture's audit burst.
        let mut invocation_ids = std::collections::HashSet::new();
        for index in 0..calls {
            let call = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Some(call) = managed
                        .client_tools
                        .snapshot()
                        .await
                        .pending
                        .into_iter()
                        .find(|call| call.call_id == format!("parallel-{index}"))
                    {
                        break call;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("ordered client invocation is pending");
            assert!(
                invocation_ids.insert(call.invocation_id.clone()),
                "distinct durable invocation per provider call"
            );
            tokio::time::timeout(Duration::from_secs(5), managed.auditor.flush())
                .await
                .unwrap()
                .unwrap();
            managed
                .client_tools
                .result(
                    "parallel-fixture-socket",
                    &super::super::client_tools::ResultRequest {
                        lease_id: bound.lease_id.clone().unwrap(),
                        invocation_id: call.invocation_id,
                        result: Some(json!({"success":true,"cancelled":false})),
                        error: None,
                    },
                )
                .await
                .unwrap();
        }
        assert_eq!(invocation_ids.len(), calls);
        let worker = managed.worker.lock().await.take().unwrap();
        tokio::time::timeout(Duration::from_secs(15), worker)
            .await
            .expect("parallel round settles")
            .unwrap();
        let receipt = manager
            .directory
            .get_operation("owner", &operation)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            receipt.status,
            "completed",
            "receipt={receipt:?}; cycle={cycle}; calls={calls}; faulted={}; closing={}; suspended={}; hub_operation={:?}; inbox={:?}; epoch_turn_round={:?}; executions={}; retry_ready={}; provider_requests={}",
            managed.faulted.load(Ordering::Acquire),
            managed.closing.load(Ordering::Acquire),
            managed.suspended.load(Ordering::Acquire),
            managed.hub.operation(),
            managed.session.inbox_snapshot(),
            {
                let state = managed.session.state();
                (state.epoch_id, state.turn_id, state.round_id)
            },
            managed.session.execution_snapshots().len(),
            managed.session.retry_ready(),
            a.wire.requests.lock().unwrap().len()
        );
        assert!(!managed.faulted.load(Ordering::Acquire));
        assert!(!managed.session.retry_ready());
        assert_eq!(managed.hub.snapshot()["session"]["retry_ready"], false);
        {
            let requests = a.wire.requests.lock().unwrap();
            assert_eq!(
                requests.len(),
                2,
                "no implicit replay or extra provider round"
            );
            let results: Vec<_> = requests[1].1["messages"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|message| message["role"] == "tool")
                .collect();
            assert_eq!(results.len(), calls);
            for (index, result) in results.iter().enumerate() {
                assert_eq!(result["tool_call_id"], format!("parallel-{index}"));
                let body: Value =
                    serde_json::from_str(result["content"].as_str().unwrap()).unwrap();
                assert_eq!(body["success"], true);
                assert_eq!(body["cancelled"], false);
            }
        }
        assert!(b.wire.requests.lock().unwrap().is_empty());
        let snapshot = managed.hub.snapshot();
        assert_eq!(
            snapshot["messages"].as_array().unwrap().last().unwrap()["text"],
            "Parallel calls completed"
        );
        manager.close().await.unwrap();
    }
}
