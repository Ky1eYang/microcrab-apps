//! Failed admission never retains transient executor payloads.
use super::*;
use microcrab_session::{
    Limits, SessionState,
    context::Content,
    runtime::{Runtime, tool::AllowTools},
};
use tokio_util::sync::CancellationToken;
#[tokio::test]
async fn failed_store_ack_does_not_retain_private_dispatch_maps() {
    let directory = tempfile::tempdir().unwrap();
    let bridge = ClientTools::open(directory.path(), "admission_session")
        .await
        .unwrap();
    bridge
        .register(
            vec![ClientToolDefinition {
                definition: ToolDefinition {
                    name: "browser_action".into(),
                    description: "fixture".into(),
                    parameters: serde_json::json!({"type":"object"}),
                },
                permissions: vec![],
                reclaimable: false,
                interaction: None,
            }],
            0,
        )
        .await
        .unwrap();
    bridge
        .bind(
            "socket",
            &BindRequest {
                client_id: "client".into(),
                catalog_digest: bridge.snapshot().await.catalog_digest,
            },
        )
        .await
        .unwrap();
    std::fs::create_dir(
        directory
            .path()
            .join("sessions/admission_session/client_tools.json.tmp"),
    )
    .unwrap();
    for ordinal in 0..3 {
        let registrations = bridge.registrations().await;
        let tool = match &registrations[0] {
            ToolRegistration::External(tool) => tool.clone(),
            _ => panic!("external fixture"),
        };
        let runtime =
            Runtime::new(registrations, Arc::new(AllowTools::new(["browser_action"]))).unwrap();
        let arguments = serde_json::json!({"value":"PRIVATE_ARGUMENT_SENTINEL"});
        let mut context = ToolContext::detached(
            SessionState::new(Limits::default()),
            CancellationToken::new(),
        );
        context.session_id = "admission_session".into();
        context.call_id = format!("attempt-{ordinal}");
        context.tool = "browser_action".into();
        context.ensure_detached_execution().unwrap();
        let issuer = Content {
            epoch: 0,
            subagent: None,
            system: "fixture".into(),
            items: vec![],
            tools: vec![],
            max_output_tokens: 1024,
            max_bytes: 65536,
            turn_id: None,
            round: None,
        };
        runtime
            .admit_invocation(
                &microcrab_protocol::ToolCall {
                    id: context.call_id.clone(),
                    name: context.tool.clone(),
                    arguments: arguments.clone(),
                    provider_metadata: None,
                },
                &mut context,
                &issuer,
            )
            .await
            .unwrap();
        assert!(tool.execute(arguments, context).await.is_err());
        let state = bridge.state.lock().await;
        assert!(state.private_arguments.is_empty());
        assert!(state.redactions.is_empty());
        assert!(state.redaction_streams.is_empty());
        assert!(state.cancellations.is_empty());
        assert!(state.durable.calls.is_empty());
    }
}
