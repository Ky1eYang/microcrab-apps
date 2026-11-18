//! Parent-owned bridge targets exact child Client State, without actor conversion.
use super::*;
use microcrab_session::{
    Auditor, Client, Session, VariableView,
    context::Content,
    runtime::{Runtime, tool::AllowTools},
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_util::sync::CancellationToken;
struct ChildModel(AtomicUsize);
#[async_trait]
impl Client for ChildModel {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        let first = self.0.fetch_add(1, Ordering::SeqCst) == 0;
        Ok(microcrab_protocol::Response {
            content: if first { String::new() } else { "done".into() },
            tool_calls: if first {
                vec![microcrab_protocol::ToolCall {
                    id: "child-secret".into(),
                    name: "request_secret".into(),
                    arguments: json!({"secrets":[{"key":"CHILD_TOKEN","label":"Child token"}]}),
                    provider_metadata: None,
                }]
            } else {
                vec![]
            },
            usage: Default::default(),
            provider_metadata: None,
        })
    }
}
async fn target_session(bridge: &Arc<ClientTools>, id: &str) -> Session {
    let runtime = Runtime::new(
        bridge.registrations().await,
        Arc::new(AllowTools::new(["request_secret"])),
    )
    .unwrap();
    Session::builder(ChildModel(AtomicUsize::new(0)), runtime)
        .session_id(id)
        .build()
        .unwrap()
}
#[tokio::test]
async fn shared_parent_bridge_requires_exact_child_runtime_and_never_writes_parent_state() {
    let root = tempfile::tempdir().unwrap();
    let bridge = ClientTools::open(root.path(), "parent_target")
        .await
        .unwrap();
    bridge
        .register(
            vec![ClientToolDefinition {
                definition: ToolDefinition {
                    name: "request_secret".into(),
                    description: "Exact target fixture".into(),
                    parameters: json!({"type":"object"}),
                },
                permissions: vec![],
                reclaimable: true,
                interaction: Some(ClientInteraction {
                    mode_pointer: "/blocking_mode".into(),
                    wait_timeout_ms: 300_000,
                    secret_writes: Some(SecretWrites {
                        keys_pointer: "/secrets".into(),
                        max_keys: 8,
                    }),
                }),
            }],
            0,
        )
        .await
        .unwrap();
    let digest = bridge.snapshot().await.catalog_digest;
    bridge
        .bind(
            "executor",
            &BindRequest {
                client_id: "exec".into(),
                catalog_digest: digest.clone(),
            },
        )
        .await
        .unwrap();
    bridge
        .bind(
            "participant",
            &BindRequest {
                client_id: "human".into(),
                catalog_digest: digest,
            },
        )
        .await
        .unwrap();
    let parent = target_session(&bridge, "parent_target").await;
    let child = target_session(&bridge, "child_target").await;
    let reused_id = target_session(&bridge, "child_target").await;
    bridge.install_secret_state(&parent).await.unwrap();
    bridge.install_secret_state(&reused_id).await.unwrap();
    let running = child.clone();
    let turn = tokio::spawn(async move { running.submit("child private input").await });
    let call = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if let Some(call) = bridge.snapshot().await.pending.first().cloned() {
                break call;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(call.parent_session_id, "parent_target");
    assert_eq!(call.session_id, "child_target");
    let request = || SecretRequest {
        invocation_id: call.invocation_id.clone(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        values: std::collections::BTreeMap::from([(
            "CHILD_TOKEN".into(),
            "PRIVATE_CHILD_SENTINEL".into(),
        )]),
    };
    let mismatch = bridge.secret("participant", request()).await.unwrap_err();
    assert!(
        mismatch
            .diagnostic_reason()
            .contains("runtime identity changed")
    );
    assert!(
        bridge
            .state
            .lock()
            .await
            .durable
            .secret_operations
            .is_empty()
    );
    assert!(
        parent
            .state_handle()
            .inspection()
            .unwrap()
            .values
            .get("CHILD_TOKEN")
            .is_none()
    );
    assert!(
        child
            .state_handle()
            .inspection()
            .unwrap()
            .values
            .get("CHILD_TOKEN")
            .is_none()
    );
    bridge.install_secret_state(&child).await.unwrap();
    let receipt = bridge.secret("participant", request()).await.unwrap();
    assert_eq!(receipt.status, CallStatus::Completed);
    assert_eq!(receipt.responder_socket.as_deref(), Some("participant"));
    assert!(
        parent
            .state_handle()
            .inspection()
            .unwrap()
            .values
            .get("CHILD_TOKEN")
            .is_none()
    );
    let child_inspection = child.state_handle().inspection().unwrap();
    assert!(child_inspection.values.get("CHILD_TOKEN").is_some());
    assert!(
        !serde_json::to_string(&child_inspection)
            .unwrap()
            .contains("PRIVATE_CHILD_SENTINEL")
    );
    assert!(
        reused_id
            .state_handle()
            .inspection()
            .unwrap()
            .values
            .get("CHILD_TOKEN")
            .is_none()
    );
    tokio::time::timeout(std::time::Duration::from_secs(3), turn)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    bridge
        .settle_all("exact target fixture cleanup")
        .await
        .unwrap();
    child.close().await.unwrap();
    reused_id.close().await.unwrap();
    parent.close().await.unwrap();
}
