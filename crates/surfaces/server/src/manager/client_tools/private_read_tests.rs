//! Only the exact live foreground executor may read transient arguments.
use super::*;
#[tokio::test]
async fn private_argument_read_checks_member_lease_and_live_cancellation() {
    let directory = tempfile::tempdir().unwrap();
    let bridge = ClientTools::open(directory.path(), "private_read")
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
    let digest = bridge.snapshot().await.catalog_digest;
    let binding = bridge
        .bind(
            "executor",
            &BindRequest {
                client_id: "executor_client".into(),
                catalog_digest: digest.clone(),
            },
        )
        .await
        .unwrap();
    bridge
        .bind(
            "participant",
            &BindRequest {
                client_id: "participant_client".into(),
                catalog_digest: digest,
            },
        )
        .await
        .unwrap();
    // A retained pending invocation can race cancellation before its asynchronous
    // durable retirement. Build that exact live window without scheduling cleanup.
    let context = ToolContext::detached(
        microcrab_session::SessionState::new(Default::default()),
        tokio_util::sync::CancellationToken::new(),
    );
    let token = context.cancellation.clone();
    {
        let mut state = bridge.state.lock().await;
        let record:CallRecord = serde_json::from_value(serde_json::json!({"invocation_id":"invocation","parent_session_id":"private_read","session_id":"private_read","call_id":"call","execution_id":null,"tool":"browser_action","arguments":{"value":"{{secret:TOKEN}}"},"epoch":0,"turn_id":null,"round_id":null,"history_anchor":null,"catalog_digest":binding.catalog_digest,"created_at_ms":0,"deadline_ms":0,"settled_at_ms":null,"status":"pending","result":null,"error":null,"reclaimable":false})).unwrap();
        state.durable.calls.push(record);
        state.contexts.insert("invocation".into(), context);
        state
            .cancellations
            .insert("invocation".into(), token.clone());
        state.private_arguments.insert(
            "invocation".into(),
            serde_json::json!({"value":"PRIVATE_READ_SENTINEL"}),
        );
    }
    let lease = binding.lease_id.unwrap();
    assert_eq!(
        bridge
            .private_arguments("executor", "invocation", &lease)
            .await
            .unwrap()["value"],
        "PRIVATE_READ_SENTINEL"
    );
    assert!(
        bridge
            .private_arguments("participant", "invocation", &lease)
            .await
            .is_err()
    );
    assert!(
        bridge
            .private_arguments("executor", "invocation", "stale")
            .await
            .is_err()
    );
    token.cancel();
    assert!(matches!(
        bridge
            .private_arguments("executor", "invocation", &lease)
            .await,
        Err(Error::Cancelled)
    ));
    let final_output = OutputRequest {
        lease_id: lease.clone(),
        invocation_id: "invocation".into(),
        event: ProducerEvent::Final {
            content: "PRIVATE_LATE_RESULT".into(),
            failure: None,
        },
    };
    assert!(matches!(
        bridge.output("executor", &final_output).await,
        Err(Error::Cancelled)
    ));
    let buffered = OutputRequest {
        event: ProducerEvent::Buffered {
            channels: vec![],
            initial_output: false,
        },
        ..final_output
    };
    assert!(matches!(
        bridge.output("executor", &buffered).await,
        Err(Error::Cancelled)
    ));
    assert!(
        bridge.state.lock().await.durable.calls[0]
            .status
            .unsettled()
    );
}
