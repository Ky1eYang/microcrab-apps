//! Actual secret input -> private executor -> masked result/ring/history pipeline.
use super::*;
use microcrab_session::{
    Auditor, Client, Session, VariableView,
    context::Content,
    runtime::{Runtime, execution::ExecutionId, tool::AllowTools},
};
use microcrab_storage::history::{CreatorJournal, FileJournal};
use serde_json::json;
use tokio_util::sync::CancellationToken;

const PRIVATE_VALUE: &str = "PRIVATE_\"PIPELINE\\SENTINEL";
#[derive(Default)]
struct PipelineModel {
    seen: Mutex<Vec<Content>>,
}
#[async_trait]
impl Client for PipelineModel {
    async fn complete(
        &self,
        content: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        let mut seen = self.seen.lock().unwrap();
        let ordinal = seen.len();
        seen.push(content.clone());
        let tool = match ordinal {
            0 => Some((
                "secret-input",
                "request_secret",
                json!({"secrets":[{"key":"TOKEN","label":"Token"}]}),
            )),
            1 => Some((
                "final-echo",
                "plugin",
                json!({"token":"{{secret:TOKEN}}","style":"final","profile":"{{var:profile}}","literal":"{{se{{var:string:empty}}cret:TOKEN}}"}),
            )),
            2 => Some((
                "buffer-echo",
                "plugin",
                json!({"token":"{{secret:TOKEN}}","style":"buffer","profile":"{{var:profile}}","literal":"{{secret:{{var:string:key}}}}"}),
            )),
            _ => None,
        };
        Ok(microcrab_protocol::Response {
            content: if tool.is_some() {
                String::new()
            } else {
                "done".into()
            },
            tool_calls: tool
                .into_iter()
                .map(|(id, name, arguments)| microcrab_protocol::ToolCall {
                    id: id.into(),
                    name: name.into(),
                    arguments,
                    provider_metadata: None,
                })
                .collect(),
            usage: Default::default(),
            provider_metadata: None,
        })
    }
}
fn definitions() -> Vec<ClientToolDefinition> {
    vec![
        ClientToolDefinition {
            definition: ToolDefinition {
                name: "request_secret".into(),
                description: "Private fixture".into(),
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
        },
        ClientToolDefinition {
            definition: ToolDefinition {
                name: "plugin".into(),
                description: "Private browser echo fixture".into(),
                parameters: json!({"type":"object"}),
            },
            permissions: vec![],
            reclaimable: false,
            interaction: None,
        },
    ]
}
async fn pending_call(bridge: &ClientTools, id: &str) -> CallRecord {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(call) = bridge
                .snapshot()
                .await
                .pending
                .into_iter()
                .find(|call| call.call_id == id)
            {
                return call;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual model reaches the requested carrier")
}
fn assert_no_private(value: &Value) {
    match value {
        Value::String(text) => {
            assert!(
                !text.contains(PRIVATE_VALUE),
                "decoded public text contains the private sentinel"
            );
            // Tool results can be JSON serialized inside a Context text item.
            if let Ok(decoded) = serde_json::from_str::<Value>(text) {
                assert_no_private(&decoded);
            }
        }
        Value::Array(values) => {
            for value in values {
                assert_no_private(value)
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                assert!(!key.contains(PRIVATE_VALUE));
                assert_no_private(value)
            }
        }
        _ => {}
    }
}

#[tokio::test]
async fn secret_to_plugin_echo_masks_public_calls_context_recovery_and_decoded_ring() {
    let root = tempfile::tempdir().unwrap();
    let session_id = "private_pipeline";
    let bridge = ClientTools::open(root.path(), session_id).await.unwrap();
    bridge.register(definitions(), 0).await.unwrap();
    let digest = bridge.snapshot().await.catalog_digest;
    let executor = bridge
        .bind(
            "executor",
            &BindRequest {
                client_id: "executor_client".into(),
                catalog_digest: digest.clone(),
            },
        )
        .await
        .unwrap();
    let participant = bridge
        .bind(
            "participant",
            &BindRequest {
                client_id: "participant_client".into(),
                catalog_digest: digest,
            },
        )
        .await
        .unwrap();
    assert!(participant.can_interact);
    assert!(participant.lease_id.is_none());
    let directory = root.path().join("sessions").join(session_id);
    let (journal, _) =
        CreatorJournal::open(directory.to_str().unwrap(), session_id.into(), true, false)
            .await
            .unwrap();
    let model = Arc::new(PipelineModel::default());
    let runtime = Runtime::new(
        bridge.registrations().await,
        Arc::new(AllowTools::new(["request_secret", "plugin"])),
    )
    .unwrap();
    let session = Session::builder_arc(model.clone(), runtime)
        .session_id(session_id)
        .journal(journal.clone())
        .build()
        .unwrap();
    bridge.install_secret_state(&session).await.unwrap();
    session
        .state_handle()
        .inject("profile", json!({"name":"ada","n":2}))
        .await
        .unwrap();
    session
        .state_handle()
        .inject("empty", json!(""))
        .await
        .unwrap();
    session
        .state_handle()
        .inject("key", json!("TOKEN"))
        .await
        .unwrap();
    let running = session.clone();
    let turn = tokio::spawn(async move {
        running
            .submit("Enter a private value and echo it through the browser carrier")
            .await
    });
    let secret = pending_call(&bridge, "secret-input").await;
    let provided = bridge
        .secret(
            "participant",
            SecretRequest {
                invocation_id: secret.invocation_id,
                operation_id: uuid::Uuid::new_v4().to_string(),
                values: std::collections::BTreeMap::from([("TOKEN".into(), PRIVATE_VALUE.into())]),
            },
        )
        .await
        .unwrap();
    assert_eq!(provided.responder_socket.as_deref(), Some("participant"));
    assert_eq!(provided.result.as_ref().unwrap()["status"], "provided");
    let lease = executor.lease_id.unwrap();
    let final_echo = pending_call(&bridge, "final-echo").await;
    assert_eq!(final_echo.arguments["token"], "{{secret:TOKEN}}");
    assert_eq!(final_echo.arguments["profile"], json!({"name":"ada","n":2}));
    assert_eq!(final_echo.arguments["literal"], "{{secret:TOKEN}}");
    assert!(
        bridge
            .private_arguments("participant", &final_echo.invocation_id, &lease)
            .await
            .is_err()
    );
    let private = bridge
        .private_arguments("executor", &final_echo.invocation_id, &lease)
        .await
        .unwrap();
    assert_eq!(private["token"], PRIVATE_VALUE);
    assert_eq!(private["profile"], json!({"name":"ada","n":2}));
    assert_eq!(private["literal"], "{{secret:TOKEN}}");
    let echoed = bridge
        .result(
            "executor",
            &ResultRequest {
                lease_id: lease.clone(),
                invocation_id: final_echo.invocation_id.clone(),
                result: Some(json!({"echo":private["token"]})),
                error: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(echoed.result.as_ref().unwrap()["echo"], "{{secret:TOKEN}}");
    assert_no_private(&serde_json::to_value(&echoed).unwrap());

    let buffered = pending_call(&bridge, "buffer-echo").await;
    let private = bridge
        .private_arguments("executor", &buffered.invocation_id, &lease)
        .await
        .unwrap();
    assert_eq!(private["token"], PRIVATE_VALUE);
    assert_eq!(private["profile"], json!({"name":"ada","n":2}));
    assert_eq!(private["literal"], "{{secret:TOKEN}}");
    let request = |event| OutputRequest {
        lease_id: lease.clone(),
        invocation_id: buffered.invocation_id.clone(),
        event,
    };
    bridge
        .output(
            "executor",
            &request(ProducerEvent::Buffered {
                channels: vec!["stdout".into(), "stderr".into()],
                initial_output: false,
            }),
        )
        .await
        .unwrap();
    let split = 8;
    bridge
        .output(
            "executor",
            &request(ProducerEvent::Chunk {
                channel: Some("stdout".into()),
                content: PRIVATE_VALUE[..split].into(),
            }),
        )
        .await
        .unwrap();
    bridge
        .output(
            "executor",
            &request(ProducerEvent::Chunk {
                channel: Some("stderr".into()),
                content: "interleaved note\n".into(),
            }),
        )
        .await
        .unwrap();
    bridge
        .output(
            "executor",
            &request(ProducerEvent::Chunk {
                channel: Some("stdout".into()),
                content: PRIVATE_VALUE[split..].into(),
            }),
        )
        .await
        .unwrap();
    bridge
        .output(
            "executor",
            &request(ProducerEvent::Finish { failure: None }),
        )
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), turn)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let ring = session
        .executions()
        .read(ExecutionId(buffered.execution_id.unwrap()), 0, 65_536)
        .unwrap();
    let decoded = ring
        .chunks
        .iter()
        .map(|chunk| chunk.content.as_str())
        .collect::<String>();
    assert!(!decoded.contains(PRIVATE_VALUE));
    assert!(decoded.contains("{{secret:TOKEN}}"));
    assert!(decoded.contains("interleaved note"));
    let out = decoded.find("{{secret:TOKEN}}").unwrap();
    let err = decoded.find("interleaved note").unwrap();
    assert!(
        out < err,
        "the delayed secret run keeps its original position before stderr"
    );
    for content in model.seen.lock().unwrap().iter() {
        assert_no_private(&serde_json::to_value(content).unwrap());
    }
    let authored_calls = session
        .messages()
        .into_iter()
        .filter_map(|item| match item {
            microcrab_session::context::ContentItem::Assistant { tool_calls, .. } => {
                Some(tool_calls)
            }
            _ => None,
        })
        .flatten()
        .filter(|call| call.name == "plugin")
        .collect::<Vec<_>>();
    assert_eq!(authored_calls.len(), 2);
    assert!(
        authored_calls
            .iter()
            .all(|call| call.arguments["profile"] == "{{var:profile}}")
    );
    assert_eq!(
        authored_calls[0].arguments["literal"],
        "{{se{{var:string:empty}}cret:TOKEN}}"
    );
    assert_eq!(
        authored_calls[1].arguments["literal"],
        "{{secret:{{var:string:key}}}}"
    );
    let public = bridge.snapshot().await;
    assert_no_private(&serde_json::to_value(&public).unwrap());
    let persisted: Value =
        serde_json::from_slice(&std::fs::read(directory.join("client_tools.json")).unwrap())
            .unwrap();
    assert_no_private(&persisted);
    let point = FileJournal::inspect_head(&directory)
        .await
        .unwrap()
        .unwrap();
    assert_no_private(&serde_json::to_value(&point.checkpoint.recover).unwrap());
    bridge.settle_all("pipeline fixture cleanup").await.unwrap();
    session.close().await.unwrap();
    drop(session);
    drop(journal);
    let (restored_journal, recover) =
        CreatorJournal::open(directory.to_str().unwrap(), session_id.into(), true, true)
            .await
            .unwrap();
    let reopened = ClientTools::open(root.path(), session_id).await.unwrap();
    let runtime = Runtime::new(
        reopened.registrations().await,
        Arc::new(AllowTools::new(["request_secret", "plugin"])),
    )
    .unwrap();
    let restored = Session::restore(
        Session::builder_arc(model.clone(), runtime)
            .session_id(session_id)
            .journal(restored_journal),
        recover.unwrap(),
    )
    .await
    .unwrap();
    restored
        .submit("Observe the restored completed history")
        .await
        .unwrap();
    for content in model.seen.lock().unwrap().iter() {
        assert_no_private(&serde_json::to_value(content).unwrap());
    }
    assert_no_private(&serde_json::to_value(reopened.snapshot().await).unwrap());
    restored.close().await.unwrap();
}
