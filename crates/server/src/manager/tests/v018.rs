//! Integrated creator/runtime/bridge path, including large canonical results.
//! The model is deterministic; the Runtime, bridge, and Journal are real.
use super::*;
use crate::manager::client_tools::{BindRequest, ResultRequest};
use serde_json::json;

struct ToolModel {
    deferred: bool,
}
#[async_trait::async_trait]
impl microcrab_session::Client for ToolModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        assert!(content.tools.iter().any(|tool| tool.name == "client_echo"));
        let completed = content.items.iter().any(|item| matches!(item, microcrab_session::context::ContentItem::Tool { call_id, content } if call_id == "echo-call" && (self.deferred || content.contains("actual client reply"))));
        Ok(Response {
            provider_metadata: None,
            content: if completed {
                "finished".into()
            } else {
                String::new()
            },
            tool_calls: if completed {
                vec![]
            } else {
                vec![microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: "echo-call".into(),
                    name: "client_echo".into(),
                    arguments: if self.deferred {
                        json!({"text":"hello","blocking_mode":"deferred"})
                    } else {
                        json!({"text":"hello"})
                    },
                }]
            },
            usage: Usage::default(),
        })
    }
}

#[tokio::test]
async fn frontend_defined_tool_runs_through_runtime_and_survives_restore() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (fixture, _root) = manager(model, "file").await;
    let config = fixture.config.clone();
    fixture.close().await.unwrap();
    let manager = Manager::with_client(config.clone(), Arc::new(ToolModel { deferred: false }))
        .await
        .unwrap();
    let created = commands::create(&manager, "owner", &command("session.create", None, json!({
        "project_id":"project", "operation_id":uuid::Uuid::new_v4().to_string(),
        "client_tools":[{"name":"client_echo","description":"Echo through the bound browser","parameters":{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false},"permissions":[{"tool":{"kind":"exact","value":"client_echo"},"args":[],"action":"allow"}],"reclaimable":true}]
    }))).await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let active = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let snapshot = active.client_tools.snapshot().await;
    let bound = active
        .client_tools
        .bind(
            "socket",
            &BindRequest {
                client_id: "browser".into(),
                catalog_digest: snapshot.catalog_digest,
            },
        )
        .await
        .unwrap();
    let mut subscription = active.hub.subscribe();
    let submitted = command(
        "turn.submit",
        Some(&id),
        json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text("echo")}),
    );
    commands::submit(&manager, active.clone(), &submitted)
        .await
        .unwrap();
    let call = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let event = subscription.receiver.recv().await.unwrap();
            if matches!(
                &event.event,
                super::super::SessionRuntimeEvent::ClientToolRequested(_)
            ) {
                break event.event.payload()["call"].clone();
            }
        }
    })
    .await
    .unwrap();
    let request = ResultRequest {
        lease_id: bound.lease_id.unwrap(),
        invocation_id: call["invocation_id"].as_str().unwrap().into(),
        result: Some(json!({"text":format!("actual client reply{}", "x".repeat(40 * 1024))})),
        error: None,
    };
    assert!(
        active
            .client_tools
            .result("other-socket", &request)
            .await
            .is_err()
    );
    active
        .client_tools
        .result("socket", &request)
        .await
        .unwrap();
    // Duplicate delivery returns the committed result without resuming twice.
    active
        .client_tools
        .result("socket", &request)
        .await
        .unwrap();
    if let Some(worker) = active.worker.lock().await.take() {
        worker.await.unwrap();
    }
    assert!(
        active.hub.snapshot()["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["text"] == "finished")
    );
    let canonical = active.hub.snapshot();
    let output = canonical["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "tool")
        .unwrap();
    assert_eq!(output["content_truncated"], true);
    assert_eq!(output["client_invocation_id"], request.invocation_id);
    assert_eq!(
        canonical["client_tools"]["selected_invocation_ids"],
        json!([request.invocation_id])
    );
    assert_eq!(active.client_tools.snapshot().await.calls.len(), 1);
    manager.close().await.unwrap();
    drop(active);
    drop(manager);
    let reopened = Manager::with_client(config, Arc::new(ToolModel { deferred: false }))
        .await
        .unwrap();
    let restored = reopened
        .active(reopened.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    assert_eq!(restored.client_tools.snapshot().await.calls.len(), 1);
    assert_eq!(
        restored.hub.snapshot()["client_tools"]["selected_invocation_ids"],
        json!([request.invocation_id])
    );
    assert_eq!(
        restored.hub.snapshot()["client_tools"]["definitions"][0]["name"],
        "client_echo"
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn deferred_client_result_updates_state_after_model_turn_finishes() {
    let fixture_model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (fixture, _root) = manager(fixture_model, "file").await;
    let config = fixture.config.clone();
    fixture.close().await.unwrap();
    let manager = Manager::with_client(config.clone(), Arc::new(ToolModel { deferred: true }))
        .await
        .unwrap();
    let created = commands::create(&manager, "owner", &command("session.create", None, json!({
        "project_id":"project","operation_id":uuid::Uuid::new_v4().to_string(),
        "client_tools":[{"name":"client_echo","description":"Generic browser interaction","parameters":{"type":"object","properties":{"text":{"type":"string"},"blocking_mode":{"type":"string","enum":["blocking","deferred"]}},"required":["text"],"additionalProperties":false},"permissions":[{"tool":{"kind":"exact","value":"client_echo"},"args":[],"action":"allow"}],"reclaimable":true,"interaction":{"mode_pointer":"/blocking_mode","wait_timeout_ms":300000}}]
    }))).await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let active = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let bound = active
        .client_tools
        .bind(
            "socket",
            &BindRequest {
                client_id: "browser".into(),
                catalog_digest: active.client_tools.snapshot().await.catalog_digest,
            },
        )
        .await
        .unwrap();
    let mut subscription = active.hub.subscribe();
    commands::submit(&manager, active.clone(), &command("turn.submit", Some(&id), json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text("deferred echo")}))).await.unwrap();
    let call = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let event = subscription.receiver.recv().await.unwrap();
            if matches!(
                &event.event,
                super::super::SessionRuntimeEvent::ClientToolRequested(_)
            ) {
                break event.event.payload()["call"].clone();
            }
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        if let Some(worker) = active.worker.lock().await.take() {
            worker.await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(call["deadline_ms"], 0);
    assert_eq!(active.client_tools.snapshot().await.pending.len(), 1);
    assert!(
        active.session.state_view().unwrap()["executions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|task| task["status"] == "running")
    );
    active
        .client_tools
        .result(
            "socket",
            &ResultRequest {
                lease_id: bound.lease_id.unwrap(),
                invocation_id: call["invocation_id"].as_str().unwrap().into(),
                result: Some(json!({"text":"actual client reply"})),
                error: None,
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let event = subscription.receiver.recv().await.unwrap();
            if matches!(
                &event.event,
                super::super::SessionRuntimeEvent::ExecutionsUpdated(_)
            ) && event.event.payload()["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|task| task["status"] == "succeeded")
            {
                break;
            }
        }
    })
    .await
    .unwrap();
    let state = active.session.state_view().unwrap();
    let settled = state["executions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|task| task["status"] == "succeeded")
        .unwrap();
    assert!(settled.get("result").is_none());
    let task_id = settled["execution_id"].as_u64().unwrap();
    assert!(
        settled["output"]["end_cursor"].as_u64().unwrap()
            > settled["output"]["start_cursor"].as_u64().unwrap()
    );
    let output = active
        .session
        .executions()
        .read(
            microcrab_session::runtime::execution::ExecutionId(task_id),
            0,
            65_536,
        )
        .unwrap();
    assert!(
        output
            .chunks
            .iter()
            .any(|chunk| chunk.content.contains("actual client reply"))
    );
    assert!(active.client_tools.snapshot().await.pending.is_empty());
    assert_eq!(active.client_tools.snapshot().await.calls.len(), 1);
    manager.close().await.unwrap();
    drop(active);
    drop(manager);
    let reopened = Manager::with_client(config, Arc::new(ToolModel { deferred: true }))
        .await
        .unwrap();
    let restored = reopened
        .active(reopened.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    assert!(
        restored.session.state_view().unwrap()["executions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|task| task["status"] == "succeeded")
    );
    assert_eq!(
        restored.hub.snapshot()["client_tools"]["selected_invocation_ids"],
        json!([call["invocation_id"]])
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn oversized_state_does_not_break_canonical_subscription_or_full_reads() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _root) = manager(model, "file").await;
    let created = commands::create(
        &manager,
        "owner",
        &command(
            "session.create",
            None,
            json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}),
        ),
    )
    .await
    .unwrap();
    let id = created["id"].as_str().unwrap();
    let active = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let large = "x".repeat(microcrab_protocol::application::MAX_FRAME_BYTES);
    active
        .session
        .state_handle()
        .inject("large_value", json!(large))
        .await
        .unwrap();
    let snapshot = manager.refresh(&active).await.unwrap();
    assert_eq!(snapshot["state_omitted"], true);
    assert!(snapshot["state"].is_null());
    assert_eq!(snapshot["session"]["id"], id);
    assert!(snapshot["messages"].is_array());
    assert!(
        serde_json::to_vec(&snapshot).unwrap().len()
            < microcrab_protocol::application::MAX_FRAME_BYTES - 16 * 1024
    );
    assert_eq!(
        active.session.state_view().unwrap()["large_value"]
            .as_str()
            .unwrap()
            .len(),
        large.len()
    );
    assert_eq!(active.hub.subscribe().snapshot["state_omitted"], true);
    manager.close().await.unwrap();
}
