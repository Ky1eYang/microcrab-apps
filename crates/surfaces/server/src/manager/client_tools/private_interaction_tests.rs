//! Actual Session witnesses for private interaction writes.
use super::*;
use microcrab_session::{
    Auditor, Client, Session, VariableView,
    context::Content,
    runtime::{Runtime, tool::AllowTools},
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_util::sync::CancellationToken;

struct SecretModel(AtomicUsize);
#[async_trait]
impl Client for SecretModel {
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
                    id: "private-question".into(),
                    name: "request_secret".into(),
                    arguments: serde_json::json!({"secrets":[{"key":"TOKEN","label":"Token"}]}),
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
#[tokio::test]
async fn concurrent_secret_submissions_claim_once_and_never_persist_values() {
    let directory = tempfile::tempdir().unwrap();
    let bridge = ClientTools::open(directory.path(), "private_session")
        .await
        .unwrap();
    bridge
        .register(
            vec![ClientToolDefinition {
                definition: ToolDefinition {
                    name: "request_secret".into(),
                    description: "Private input fixture".into(),
                    parameters: serde_json::json!({"type":"object"}),
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
    for socket in ["socket_a", "socket_b"] {
        bridge
            .bind(
                socket,
                &BindRequest {
                    client_id: socket.into(),
                    catalog_digest: digest.clone(),
                },
            )
            .await
            .unwrap();
    }
    let runtime = Runtime::new(
        bridge.registrations().await,
        Arc::new(AllowTools::new(["request_secret"])),
    )
    .unwrap();
    let session = Session::builder_arc(Arc::new(SecretModel(AtomicUsize::new(0))), runtime)
        .session_id("private_session")
        .build()
        .unwrap();
    bridge.install_secret_state(&session).await.unwrap();
    let running = session.clone();
    let turn = tokio::spawn(async move { running.submit("collect private input").await });
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
    let request = |value: &str| SecretRequest {
        invocation_id: call.invocation_id.clone(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        values: std::collections::BTreeMap::from([("TOKEN".into(), value.into())]),
    };
    let a = request("PRIVATE_LEFT_SENTINEL");
    let b = request("PRIVATE_RIGHT_SENTINEL");
    let (left, right) = tokio::join!(bridge.secret("socket_a", a), bridge.secret("socket_b", b));
    assert!(left.is_ok() || right.is_ok());
    assert_eq!(bridge.state.lock().await.durable.secret_operations.len(), 1);
    let receipt = bridge.snapshot().await.calls[0].clone();
    assert_eq!(receipt.status, CallStatus::Completed);
    assert_eq!(receipt.result.as_ref().unwrap()["status"], "provided");
    let persisted = std::fs::read_to_string(
        directory
            .path()
            .join("sessions/private_session/client_tools.json"),
    )
    .unwrap();
    for secret in ["PRIVATE_LEFT_SENTINEL", "PRIVATE_RIGHT_SENTINEL"] {
        assert!(!persisted.contains(secret));
        assert!(!serde_json::to_string(&receipt).unwrap().contains(secret));
    }
    tokio::time::timeout(std::time::Duration::from_secs(3), turn)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    bridge.settle_all("test cleanup").await.unwrap();
    session.close().await.unwrap();
}
