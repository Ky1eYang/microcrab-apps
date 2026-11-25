//! Advisory tool progress carries identifiers and bounded metadata only. Full
//! retained invocation content is available through authorized paged reads.
use super::hub::Hub;
use microcrab_auditor::{
    AuditBody, Auditor, AuditorError, BoxFuture, HookContext, HookPayload, Injection,
};
use microcrab_protocol::Moment;
use microcrab_session::runtime::execution::ExecutionRegistry;
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

#[derive(Clone)]
pub(super) struct ObservedAuditor {
    pub inner: Arc<dyn Auditor>,
    pub hub: Arc<Hub>,
    pub calls: Arc<Mutex<HashMap<String, String>>>,
    pub executions: Arc<Mutex<Option<Weak<ExecutionRegistry>>>>,
    pub presentations: Arc<super::presentation::Presentations>,
}
impl Auditor for ObservedAuditor {
    fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }
    fn publish(&self, event: Moment, body: AuditBody) {
        // A provider call_id is a different authority. Only the producer's
        // explicit tool_call_id can correlate an invocation with an execution.
        if body.subagent.is_none()
            && let Some(call) = body.tool_call_id.as_ref()
        {
            let execution = body.tool_execution_id.and_then(|id| {
                let registry = self
                    .executions
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .and_then(Weak::upgrade)?;
                registry
                    .get(microcrab_session::runtime::execution::ExecutionId(id))
                    .ok()
                    .filter(|row| row.call_id == *call)
            });
            let presentation = execution
                .as_ref()
                .and_then(|row| self.presentations.observe(&body, row))
                .or_else(|| {
                    // The ordinary ToolCall hook happens before reservation. The
                    // typed producer bit can label this advisory started event,
                    // but cannot associate a row, selected history, or approval.
                    if body.tool_execution_id.is_none()
                        && matches!(body.payload, HookPayload::ToolCall { .. })
                    {
                        super::presentation::Presentation::from_audit(&body)
                    } else {
                        None
                    }
                });
            let operation = self.hub.operation();
            let progress = operation.as_ref().and_then(|_| match &body.payload {
                HookPayload::ToolCall { name, .. } => {
                    let name: String = name.chars().take(256).collect();
                    let mut calls = self.calls.lock().unwrap_or_else(|e| e.into_inner());
                    if calls.len() < 1024 {
                        calls.insert(call.clone(), name.clone());
                    }
                    Some((name, "started"))
                }
                HookPayload::ToolResult { error, .. } => {
                    let name = self
                        .calls
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(call)
                        .or_else(|| {
                            execution
                                .as_ref()
                                .map(|row| row.tool.chars().take(256).collect())
                        })
                        .unwrap_or_else(|| "tool".into());
                    Some((
                        name,
                        if error.is_some() {
                            "failed"
                        } else {
                            "completed"
                        },
                    ))
                }
                _ => None,
            });
            if let (Some(operation), Some((name, phase))) = (operation, progress) {
                let phase = match execution.as_ref().map(|row| row.status) {
                    Some(microcrab_session::runtime::execution::ExecutionStatus::Failed) => {
                        "failed"
                    }
                    Some(microcrab_session::runtime::execution::ExecutionStatus::Cancelled) => {
                        "cancelled"
                    }
                    Some(microcrab_session::runtime::execution::ExecutionStatus::Interrupted) => {
                        "interrupted"
                    }
                    _ => phase,
                };
                let mut payload = json!({
                    "operation_id": operation,
                    "epoch": body.epoch,
                    "turn": body.turn_id,
                    "round": body.round,
                    "call_id": call,
                    "name": name,
                    "phase": phase,
                    "execution_id": body.tool_execution_id.filter(|id| *id > 0),
                    "started_at_unix_ms": execution.as_ref().and_then(|row| row.started_at_unix_ms)
                        .or(body.started_at_unix_ms),
                    "elapsed_ms": execution.as_ref().and_then(|row| row.elapsed_ms).or(body.elapsed_ms),
                    "output_availability": execution.as_ref().and_then(|row| row.output_availability),
                    "arguments_available": execution.as_ref().and_then(|row| row.arguments_available),
                    "result_available": execution.as_ref().and_then(|row| row.result_available),
                    "preview_omitted": true
                });
                if let Some(presentation) = presentation {
                    payload["presentation"] = json!(presentation);
                }
                self.hub.publish(super::SessionRuntimeEvent::Tool(payload));
            }
        }
        self.inner.publish(event, body)
    }
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        context: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        self.inner.intervene(moment, context)
    }
    fn flush(&self) -> BoxFuture<'_, Result<(), AuditorError>> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn progress_decorator_preserves_effective_requests_and_flush_errors() {
        use microcrab_auditor::EmptyVariableView;
        use microcrab_creator::audit::{AuditQuery, AuditStatus, NativeAuditor, RecorderOptions};
        let directory = tempfile::tempdir().unwrap();
        let native = NativeAuditor::open(
            directory.path(),
            Arc::new(Mutex::new(Default::default())),
            RecorderOptions::default(),
        )
        .await
        .unwrap();
        let auditor = ObservedAuditor {
            inner: native.clone(),
            hub: Arc::new(Hub::new()),
            calls: Arc::new(Mutex::new(HashMap::new())),
            executions: Arc::new(Mutex::new(None)),
            presentations: Arc::default(),
        };
        let wire = "{\"messages\":[],\"tools\":[]}";
        auditor
            .intervene(
                Moment::RequestPrepared,
                HookContext {
                    epoch: 0,
                    subagent: None,
                    turn: Some(1),
                    round: Some(1),
                    call_id: Some("actual-server-call".into()),
                    request_body: Some(wire),
                    max_request_bytes: Some(65536),
                    variables: &EmptyVariableView,
                    payload: HookPayload::None,
                },
            )
            .await
            .unwrap();
        assert!(native.request_head("actual-server-call").is_some());
        auditor.publish(
            Moment::RequestPrepared,
            AuditBody {
                epoch: 0,
                turn_id: Some(1),
                round: Some(1),
                call_id: Some("actual-server-call".into()),
                sent_request: Some(wire.into()),
                ..Default::default()
            },
        );
        auditor.flush().await.unwrap();
        let selector: AuditQuery =
            serde_json::from_value(json!({"epoch":0,"turn":1,"round":1})).unwrap();
        let result = microcrab_creator::audit::query(directory.path(), &selector).unwrap();
        assert_eq!(result.status, AuditStatus::Available);
        assert_eq!(result.attempts.len(), 1);
        native.close().await.unwrap();
        auditor.flush().await.unwrap(); // Clean close is intentionally idempotent.
        struct FailedFlush;
        impl Auditor for FailedFlush {
            fn is_enabled(&self) -> bool {
                true
            }
            fn publish(&self, _: Moment, _: AuditBody) {}
            fn flush(&self) -> BoxFuture<'_, Result<(), AuditorError>> {
                Box::pin(async { Err(AuditorError("synthetic durable flush failure".into())) })
            }
        }
        let failed = ObservedAuditor {
            inner: Arc::new(FailedFlush),
            ..auditor
        };
        assert_eq!(
            failed.flush().await.unwrap_err().to_string(),
            "synthetic durable flush failure"
        );
    }

    #[test]
    fn image_presentation_uses_the_builtin_witness_not_name_or_result_text() {
        use microcrab_session::runtime::execution::{ExecutionId, ExecutionSnapshot};
        let hub = Arc::new(Hub::new());
        hub.set_operation(Some("operation".into()));
        let mut subscription = hub.subscribe();
        let registry = ExecutionRegistry::new(Default::default());
        let mut row = ExecutionSnapshot::new(
            "session".into(),
            "call".into(),
            "view_image".into(),
            "agent".into(),
            None,
        );
        row.execution_id = ExecutionId(7);
        registry.restore(&[row]).unwrap();
        let auditor = ObservedAuditor {
            inner: Arc::new(microcrab_auditor::NoopAuditor),
            hub,
            calls: Arc::default(),
            executions: Arc::new(Mutex::new(Some(Arc::downgrade(&registry)))),
            presentations: Arc::default(),
        };
        let start = AuditBody {
            tool_call_id: Some("call".into()),
            tool_execution_id: Some(7),
            payload: HookPayload::ToolCall {
                name: "view_image".into(),
                arguments: json!({"image_url":"unmodified-authored-value"}),
            },
            ..Default::default()
        };
        auditor.publish(Moment::ToolCall, start.clone());
        assert!(
            subscription
                .receiver
                .try_recv()
                .unwrap()
                .event
                .payload()
                .get("presentation")
                .is_none()
        );
        auditor.publish(
            Moment::ToolCall,
            AuditBody {
                authored_image_arguments: true,
                ..start.clone()
            },
        );
        let event = subscription.receiver.try_recv().unwrap();
        assert_eq!(event.event.payload()["presentation"], "view-image");
        assert!(
            !event
                .event
                .payload()
                .to_string()
                .contains("unmodified-authored-value")
        );
        auditor.publish(
            Moment::ToolResult,
            AuditBody {
                payload: HookPayload::ToolResult {
                    result: Some("not-a-preview".into()),
                    error: None,
                },
                ..start
            },
        );
        let event = subscription.receiver.try_recv().unwrap();
        assert_eq!(event.event.payload()["presentation"], "view-image");
        assert!(!event.event.payload().to_string().contains("not-a-preview"));
    }

    #[test]
    fn prereservation_typed_fact_cannot_tag_other_owner_views() {
        use microcrab_auditor::ToolPresentation;
        let hub = Arc::new(Hub::new());
        hub.set_operation(Some("operation".into()));
        let mut subscription = hub.subscribe();
        let auditor = ObservedAuditor {
            inner: Arc::new(microcrab_auditor::NoopAuditor),
            hub,
            calls: Arc::default(),
            executions: Arc::default(),
            presentations: Arc::default(),
        };
        auditor.publish(
            Moment::ToolCall,
            AuditBody {
                tool_call_id: Some("call".into()),
                tool_presentation: Some(ToolPresentation::CodingFile),
                payload: HookPayload::ToolCall {
                    name: "read".into(),
                    arguments: json!({"path":"a"}),
                },
                ..Default::default()
            },
        );
        let event = subscription.receiver.try_recv().unwrap();
        assert_eq!(event.event.payload()["presentation"], "coding-file");
        assert!(event.event.payload()["execution_id"].is_null());
        assert!(
            auditor
                .presentations
                .permission("session", "call", "read")
                .is_none()
        );
    }

    #[test]
    fn tools_use_explicit_tool_identity_and_observed_timing_not_provider_id() {
        let hub = Arc::new(Hub::new());
        hub.set_operation(Some("operation".into()));
        let mut subscription = hub.subscribe();
        let auditor = ObservedAuditor {
            inner: Arc::new(microcrab_auditor::NoopAuditor),
            hub: hub.clone(),
            calls: Arc::new(Mutex::new(HashMap::new())),
            executions: Arc::new(Mutex::new(None)),
            presentations: Arc::default(),
        };
        let start = AuditBody {
            epoch: 3,
            turn_id: Some(7),
            round: Some(2),
            call_id: Some("provider-request".into()),
            tool_call_id: Some("actual-tool".into()),
            started_at_unix_ms: Some(1234),
            payload: HookPayload::ToolCall {
                name: "read".into(),
                arguments: json!({"private":"not-a-preview"}),
            },
            ..Default::default()
        };
        auditor.publish(Moment::ToolCall, start.clone());
        let event = subscription.receiver.try_recv().unwrap();
        assert_eq!(event.event.payload()["call_id"], "actual-tool");
        assert_eq!(event.event.payload()["started_at_unix_ms"], 1234);
        assert!(event.event.payload().get("arguments").is_none());
        assert!(!event.event.payload().to_string().contains("not-a-preview"));
        auditor.publish(
            Moment::ToolResult,
            AuditBody {
                payload: HookPayload::ToolResult {
                    result: Some("private result".into()),
                    error: None,
                },
                elapsed_ms: Some(42),
                ..start.clone()
            },
        );
        let event = subscription.receiver.try_recv().unwrap();
        assert_eq!(event.event.payload()["call_id"], "actual-tool");
        assert_eq!(event.event.payload()["name"], "read");
        assert_eq!(event.event.payload()["elapsed_ms"], 42);
        assert!(!event.event.payload().to_string().contains("private result"));
        auditor.publish(
            Moment::ToolCall,
            AuditBody {
                tool_call_id: None,
                ..start
            },
        );
        assert!(subscription.receiver.try_recv().is_err());
        assert_eq!(hub.snapshot(), json!({}));
    }
}
