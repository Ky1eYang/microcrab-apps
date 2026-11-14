//! Explicit retained output reads do not run tools, consume bytes or open sessions.
use super::*;
use serde_json::json;

struct DeferredResult;
#[async_trait::async_trait]
impl microcrab_session::Client for DeferredResult {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let done = content.items.iter().any(|item| matches!(item,
            microcrab_session::context::ContentItem::Tool { call_id, .. } if call_id == "retained-sleep"));
        Ok(Response {
            provider_metadata: None,
            content: if done { "done".into() } else { String::new() },
            tool_calls: if done {
                vec![]
            } else {
                vec![microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: "retained-sleep".into(),
                    name: "client_echo".into(),
                    arguments: json!({"text":"retained output", "blocking_mode":"deferred"}),
                }]
            },
            usage: Usage::default(),
        })
    }
}

#[tokio::test]
async fn explicit_output_read_is_owned_bounded_nonconsuming_and_never_reopens() {
    let (manager, _root) = manager(Arc::new(DeferredResult), "file").await;
    let created = commands::create(
        &manager,
        "owner",
        &command(
            "session.create",
            None,
            json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string(),
                "client_tools":[{"name":"client_echo","description":"Retained output fixture", "parameters":{"type":"object","properties":{"text":{"type":"string"},"blocking_mode":{"type":"string","enum":["blocking","deferred"]}},"required":["text"],"additionalProperties":false},"permissions":[{"tool":{"kind":"exact","value":"client_echo"},"args":[],"action":"allow"}],"reclaimable":true,"interaction":{"mode_pointer":"/blocking_mode","wait_timeout_ms":300000}}]}),
        ),
    )
    .await
    .unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let catalog = managed.client_tools.snapshot().await;
    let bound = managed
        .client_tools
        .bind(
            "test-socket",
            &crate::manager::client_tools::BindRequest {
                client_id: "test-browser".into(),
                catalog_digest: catalog.catalog_digest,
            },
        )
        .await
        .unwrap();
    let mut subscription = managed.hub.subscribe();
    managed.session.submit("one tool").await.unwrap();
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
    let tasks = managed.session.executions();
    let mut changes = tasks.subscribe();
    managed
        .client_tools
        .result(
            "test-socket",
            &crate::manager::client_tools::ResultRequest {
                lease_id: bound.lease_id.unwrap(),
                invocation_id,
                result: Some(json!("retained output fixture")),
                error: None,
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while tasks.has_pending() {
            changes.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let state = managed.session.state_view().unwrap();
    assert_eq!(state["executions"].as_array().unwrap().len(), 1);
    assert!(state["executions"][0].get("result").is_none());
    let principal = crate::auth::Principal::for_test("owner");
    let content = command(
        commands::EXECUTIONS_CONTENT,
        Some(id),
        json!({"execution_id":1,"field":"arguments","cursor":0,"max_bytes":512}),
    );
    let content_page = commands::execute(&manager, &principal, &content)
        .await
        .unwrap();
    assert_eq!(content_page["call_id"], "retained-sleep");
    assert_eq!(content_page["session_id"], id);
    assert_eq!(content_page["available"], true);
    assert!(
        content_page["content"]
            .as_str()
            .unwrap()
            .contains("retained output")
    );
    assert_eq!(
        content_page,
        commands::execute(&manager, &principal, &content)
            .await
            .unwrap()
    );
    assert_eq!(
        commands::execute(
            &manager,
            &crate::auth::Principal::for_test("intruder"),
            &content
        )
        .await
        .unwrap_err()
        .code,
        "not_found"
    );
    assert!(
        commands::execute(
            &manager,
            &principal,
            &command(
                commands::EXECUTIONS_CONTENT,
                Some(id),
                json!({"execution_id":1,"field":"arguments","max_bytes":65537})
            )
        )
        .await
        .is_err()
    );
    let result_page = commands::execute(
        &manager,
        &principal,
        &command(
            commands::EXECUTIONS_CONTENT,
            Some(id),
            json!({"execution_id":1,"field":"result","max_bytes":1024}),
        ),
    )
    .await
    .unwrap();
    assert_eq!(result_page["available"], true);
    // Display inspection preserves the complete retained ring range.
    let retained = managed
        .session
        .executions()
        .get(microcrab_session::runtime::execution::ExecutionId(1))
        .unwrap()
        .output
        .unwrap();
    assert_eq!(retained.start_cursor, 0);
    assert_eq!(retained.end_cursor, "retained output fixture".len() as u64);
    let read = command(
        commands::EXECUTIONS_READ,
        Some(id),
        json!({"execution_id":1,"cursor":0,"max_bytes":512}),
    );
    let first = commands::execute(&manager, &principal, &read)
        .await
        .unwrap();
    assert_eq!(first["cursor"], 0);
    assert_eq!(first["next_cursor"], 23);
    assert_eq!(first["chunks"][0]["content"], "retained output fixture");
    assert_eq!(
        first,
        commands::execute(&manager, &principal, &read)
            .await
            .unwrap()
    );
    let intruder = crate::auth::Principal::for_test("intruder");
    assert_eq!(
        commands::execute(&manager, &intruder, &read)
            .await
            .unwrap_err()
            .code,
        "not_found"
    );
    let oversize = command(
        commands::EXECUTIONS_READ,
        Some(id),
        json!({"execution_id":1,"cursor":0,"max_bytes":65537}),
    );
    assert!(
        commands::execute(&manager, &principal, &oversize)
            .await
            .is_err()
    );
    let unknown = command(
        commands::EXECUTIONS_READ,
        Some(id),
        json!({"execution_id":999,"cursor":0}),
    );
    assert!(
        commands::execute(&manager, &principal, &unknown)
            .await
            .is_err()
    );
    commands::close_session(&manager, &managed, "owner", id, None)
        .await
        .unwrap();
    assert!(
        commands::execute(&manager, &principal, &read)
            .await
            .is_err()
    );
    assert!(
        commands::execute(&manager, &principal, &content)
            .await
            .is_err()
    );
    assert!(!manager.registry.lock().unwrap().contains_key(id));
    manager.close().await.unwrap();
}

#[tokio::test]
async fn typed_external_buffer_is_lease_bound_and_disconnect_is_not_replayed() {
    use crate::manager::client_tools::{BindRequest, OutputRequest};
    use microcrab_session::runtime::execution::{ExecutionId, ExecutionStatus};
    let (manager, _root) = manager(Arc::new(DeferredResult), "file").await;
    let created = commands::create(&manager, "owner", &command("session.create", None, json!({
        "project_id":"project","operation_id":uuid::Uuid::new_v4().to_string(),
        "client_tools":[{"name":"client_echo","description":"Opaque producer","parameters":{"type":"object"},"permissions":[{"tool":{"kind":"exact","value":"client_echo"},"args":[],"action":"allow"}]}]
    }))).await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let binding = managed
        .client_tools
        .bind(
            "producer-socket",
            &BindRequest {
                client_id: "producer".into(),
                catalog_digest: managed.client_tools.snapshot().await.catalog_digest,
            },
        )
        .await
        .unwrap();
    let lease = binding.lease_id.unwrap();
    let mut subscription = managed.hub.subscribe();
    let session = managed.session.clone();
    let submitted = tokio::spawn(async move { session.submit("begin nonprocess producer").await });
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
    let request = |event| {
        serde_json::from_value::<OutputRequest>(
            json!({"lease_id":lease,"invocation_id":call["invocation_id"],"event":event}),
        )
        .unwrap()
    };
    let opened = request(json!({"kind":"buffered","channels":["progress"]}));
    assert!(
        managed
            .client_tools
            .output("foreign-socket", &opened)
            .await
            .is_err()
    );
    managed
        .client_tools
        .output("producer-socket", &opened)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), submitted)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let execution_id = ExecutionId(call["execution_id"].as_u64().unwrap());
    let initial = managed
        .session
        .read_execution_output(execution_id, 0, 4096)
        .unwrap();
    assert!(initial.chunks.is_empty());
    assert!(!initial.closed);
    managed
        .client_tools
        .output(
            "producer-socket",
            &request(json!({"kind":"chunk","channel":"progress","content":"partial work"})),
        )
        .await
        .unwrap();
    managed
        .session
        .cancel_execution(execution_id)
        .await
        .unwrap();
    assert_eq!(
        managed
            .session
            .executions()
            .get(execution_id)
            .unwrap()
            .status,
        ExecutionStatus::Cancelling
    );
    let cancel = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let event = subscription.receiver.recv().await.unwrap();
            if matches!(
                &event.event,
                super::super::SessionRuntimeEvent::ClientToolCancelRequested(_)
            ) {
                break event;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        cancel.event.payload()["call"]["invocation_id"],
        call["invocation_id"]
    );
    managed
        .client_tools
        .detach("producer-socket")
        .await
        .unwrap();
    assert_eq!(
        managed
            .session
            .executions()
            .get(execution_id)
            .unwrap()
            .status,
        ExecutionStatus::Interrupted
    );
    let retained = managed
        .session
        .read_execution_output(execution_id, 0, 4096)
        .unwrap();
    assert!(retained.closed);
    assert_eq!(retained.chunks[0].content, "partial work");
    assert!(
        managed
            .client_tools
            .output("producer-socket", &request(json!({"kind":"finish"})))
            .await
            .is_err()
    );
    let rebound = managed
        .client_tools
        .bind(
            "new-socket",
            &BindRequest {
                client_id: "producer".into(),
                catalog_digest: managed.client_tools.snapshot().await.catalog_digest,
            },
        )
        .await
        .unwrap();
    assert!(rebound.pending.is_empty());
    assert_eq!(rebound.calls.len(), 1);
    manager.close().await.unwrap();
}

struct AnswerWakeModel {
    calls: AtomicUsize,
    entered: Notify,
    release: Semaphore,
}
#[async_trait::async_trait]
impl microcrab_session::Client for AnswerWakeModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 2 {
            assert!(content.items.iter().any(|item| matches!(
                item,
                microcrab_session::context::ContentItem::User {
                    source: Some(microcrab_transport::UserSource::Framework),
                    ..
                }
            )));
            self.entered.notify_one();
            let permit = tokio::select! {
                _ = token.cancelled() => return Err(microcrab_transport::Error::Cancelled),
                permit = self.release.acquire() => permit.map_err(|_| microcrab_transport::Error::Cancelled)?,
            };
            permit.forget();
        }
        assert!(
            call <= 2,
            "one answer must cause only one framework model request"
        );
        Ok(Response {
            provider_metadata: None,
            content: match call {
                0 => String::new(),
                1 => "Waiting for the answer".into(),
                _ => "Autonomous answer publication".into(),
            },
            tool_calls: if call == 0 {
                vec![microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: "deferred-question".into(),
                    name: "ask_user_question".into(),
                    arguments: json!({"mode":"deferred"}),
                }]
            } else {
                vec![]
            },
            usage: Usage::default(),
        })
    }
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn deferred_answer_publishes_autonomous_response_without_another_user_command() {
    use std::time::Duration;

    let model = Arc::new(AnswerWakeModel {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _root) = manager(model.clone(), "file").await;
    let created = commands::create(
        &manager,
        "owner",
        &command("session.create", None, json!({
            "project_id":"project", "operation_id":uuid::Uuid::new_v4().to_string(),
            "client_tools":[{
                "name":"ask_user_question", "description":"Deferred question fixture",
                "parameters":{"type":"object","properties":{"mode":{"type":"string","enum":["blocking","deferred"]}},"required":["mode"],"additionalProperties":false},
                "permissions":[{"tool":{"kind":"exact","value":"ask_user_question"},"args":[],"action":"allow"}],
                "reclaimable":true,
                "interaction":{"mode_pointer":"/mode","wait_timeout_ms":300000}
            }]
        })),
    ).await.unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let catalog = managed.client_tools.snapshot().await;
    let bound = managed
        .client_tools
        .bind(
            "answer-socket",
            &crate::manager::client_tools::BindRequest {
                client_id: "answer-browser".into(),
                catalog_digest: catalog.catalog_digest,
            },
        )
        .await
        .unwrap();
    commands::submit(
        &manager,
        managed.clone(),
        &command(
            "turn.submit",
            Some(id),
            json!({
                "operation_id":uuid::Uuid::new_v4().to_string(),
                "input":microcrab_session::SessionInput::text("Ask a deferred question")
            }),
        ),
    )
    .await
    .unwrap();
    let initial_worker = managed.worker.lock().await.take().unwrap();
    initial_worker.await.unwrap();
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    let invocation_id = managed
        .client_tools
        .snapshot()
        .await
        .pending
        .into_iter()
        .next()
        .unwrap()
        .invocation_id;
    let mut subscription = managed.hub.subscribe();
    let mut view = subscription.snapshot.clone();
    managed
        .client_tools
        .result(
            "answer-socket",
            &crate::manager::client_tools::ResultRequest {
                lease_id: bound.lease_id.unwrap(),
                invocation_id,
                result: Some(json!({"answer":"selected"})),
                error: None,
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), model.entered.notified())
        .await
        .unwrap();

    // Hold the autonomous response until settlement has already refreshed the
    // canonical snapshot. A late execution event must not accidentally publish
    // the reply and conceal a missing completed-turn subscription.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = subscription.receiver.recv().await.unwrap();
            if crate::manager::publication::apply(
                &mut view,
                event.event.name(),
                event.event.payload(),
            ) && view["client_tools"]["calls"]
                .as_array()
                .is_some_and(|calls| {
                    calls.iter().any(|call| {
                        call["call_id"] == "deferred-question" && call["status"] == "completed"
                    })
                })
            {
                break;
            }
        }
        loop {
            match tokio::time::timeout(Duration::from_millis(30), subscription.receiver.recv())
                .await
            {
                Ok(Ok(event)) => {
                    crate::manager::publication::apply(
                        &mut view,
                        event.event.name(),
                        event.event.payload(),
                    );
                }
                Ok(Err(error)) => panic!("subscription closed before answer completion: {error}"),
                Err(_) => break,
            }
        }
    })
    .await
    .unwrap();
    assert!(
        !managed.hub.snapshot()["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| { message["text"] == "Autonomous answer publication" })
    );
    model.release.add_permits(1);
    let snapshot = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = subscription.receiver.recv().await.unwrap();
            if crate::manager::publication::apply(
                &mut view,
                event.event.name(),
                event.event.payload(),
            ) && view["messages"].as_array().is_some_and(|messages| {
                messages.iter().any(|message| {
                    message["role"] == "assistant"
                        && message["text"] == "Autonomous answer publication"
                })
            }) {
                break view.clone();
            }
        }
    })
    .await
    .expect("completed autonomous Session turn must publish without another public command");
    let users: Vec<_> = snapshot["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "user")
        .collect();
    assert_eq!(
        users
            .iter()
            .filter(|message| message["source"] != "framework")
            .count(),
        1
    );
    assert_eq!(
        users
            .iter()
            .filter(|message| message["source"] == "framework")
            .count(),
        1
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 3);
    assert!(
        managed.hub.snapshot()["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| { message["text"] == "Autonomous answer publication" })
    );
    manager.close().await.unwrap();
}
