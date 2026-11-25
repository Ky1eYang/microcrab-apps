//! A late buffered completion advances durable execution observations, not the failed request.
use super::*;
use serde_json::json;

struct DeferredRetryModel {
    calls: AtomicUsize,
    seen: std::sync::Mutex<Vec<Value>>,
}
#[async_trait::async_trait]
impl microcrab_session::Client for DeferredRetryModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.seen
            .lock()
            .unwrap()
            .push(serde_json::to_value(content).unwrap());
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => Ok(Response {
                provider_metadata: None,
                content: "started".into(),
                tool_calls: vec![microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: "deferred-retry-call".into(),
                    name: "client_echo".into(),
                    arguments: json!({"text":"once", "blocking_mode":"deferred"}),
                }],
                usage: Usage::default(),
            }),
            1 => Err(microcrab_transport::Error::Transport),
            _ => Ok(Response {
                provider_metadata: None,
                content: "retried".into(),
                tool_calls: vec![],
                usage: Usage::default(),
            }),
        }
    }
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn late_buffered_completion_keeps_failed_request_retryable_without_tool_replay() {
    let model = Arc::new(DeferredRetryModel {
        calls: AtomicUsize::new(0),
        seen: std::sync::Mutex::new(vec![]),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string(),
            "client_tools":[{"name":"client_echo","description":"Deferred retry fixture", "parameters":{"type":"object","properties":{"text":{"type":"string"},"blocking_mode":{"type":"string","enum":["blocking","deferred"]}},"required":["text"],"additionalProperties":false},"permissions":[{"tool":{"kind":"exact","value":"client_echo"},"args":[],"action":"allow"}],"reclaimable":true,"interaction":{"mode_pointer":"/blocking_mode","wait_timeout_ms":300000}}]}))).await.unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let bound = managed
        .client_tools
        .bind(
            "retry-socket",
            &crate::manager::client_tools::BindRequest {
                client_id: "retry-browser".into(),
                catalog_digest: managed.client_tools.snapshot().await.catalog_digest,
            },
        )
        .await
        .unwrap();
    let mut subscription = managed.hub.subscribe();
    let failure = uuid::Uuid::new_v4().to_string();
    commands::submit(
        &manager,
        managed.clone(),
        &command(
            "turn.submit",
            Some(id),
            json!({"operation_id":failure,"input":microcrab_session::SessionInput::text("once")}),
        ),
    )
    .await
    .unwrap();
    managed.worker.lock().await.take().unwrap().await.unwrap();
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    let invocation_id = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let event = subscription.receiver.recv().await.unwrap();
            if matches!(
                &event.event,
                super::super::SessionRuntimeEvent::ClientToolRequested(_)
            ) {
                break event.event.payload()["call"]["invocation_id"]
                    .as_str()
                    .unwrap()
                    .to_owned();
            }
        }
    })
    .await
    .unwrap();
    let failed_cursor = serde_json::to_value(managed.journal.cursor().await).unwrap();
    let failed_witness = view::retry_witness(
        &managed.session.context_snapshot().unwrap(),
        &managed.session.state(),
    )
    .unwrap();
    managed
        .client_tools
        .result(
            "retry-socket",
            &crate::manager::client_tools::ResultRequest {
                lease_id: bound.lease_id.unwrap(),
                invocation_id,
                result: Some(json!("late completion")),
                error: None,
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if serde_json::to_value(managed.journal.cursor().await).unwrap() != failed_cursor {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        failed_witness,
        view::retry_witness(
            &managed.session.context_snapshot().unwrap(),
            &managed.session.state()
        )
        .unwrap()
    );
    let snapshot = manager.refresh(&managed).await.unwrap();
    assert_eq!(snapshot["session"]["retry_ready"], true);
    assert_eq!(snapshot["session"]["retry_operation_id"], failure);
    commands::submit(
        &manager,
        managed.clone(),
        &command(
            "turn.retry",
            Some(id),
            json!({"operation_id":uuid::Uuid::new_v4().to_string(),"failed_operation_id":failure}),
        ),
    )
    .await
    .unwrap();
    managed.worker.lock().await.take().unwrap().await.unwrap();
    assert_eq!(managed.session.execution_snapshots().len(), 1);
    {
        let seen = model.seen.lock().unwrap();
        assert_eq!(seen[1], seen[2]);
    }
    manager.close().await.unwrap();
}
