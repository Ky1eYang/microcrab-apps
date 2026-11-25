use super::*;
use microcrab_protocol::{ImageSource, InputPart, ToolCall, ToolDefinition};
use microcrab_transport::ContentItem;
use serde_json::json;

fn content() -> Content {
    Content {
        epoch: 1,
        subagent: None,
        system: "frozen".into(),
        items: vec![
            ContentItem::Assistant {
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: "call".into(),
                    name: "inspect".into(),
                    arguments: json!({}),
                    provider_metadata: None,
                }],
                provider_metadata: None,
            },
            ContentItem::Tool {
                call_id: "call".into(),
                content: "image".into(),
                parts: vec![InputPart::Image {
                    source: ImageSource::Data {
                        media_type: "image/png".into(),
                        data: vec![1, 2, 3],
                    },
                }],
            },
        ],
        tools: vec![ToolDefinition {
            name: "inspect".into(),
            description: "inspect".into(),
            parameters: json!({"type":"object"}),
        }],
        max_output_tokens: 32,
        max_bytes: 65536,
        turn_id: Some(1),
        round: Some(1),
    }
}
#[tokio::test]
async fn actual_server_target_enables_only_explicit_google_media_and_changes_only_enabled_witness()
{
    let seen = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let recorded = seen.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1beta", listener.local_addr().unwrap());
    let router=axum::Router::new().fallback(axum::routing::post(move |axum::Json(body):axum::Json<serde_json::Value>| {
        let recorded=recorded.clone();
        async move {
            recorded.lock().unwrap().push(body);
            axum::Json(json!({"candidates":[{"content":{"role":"model","parts":[{"text":"accepted"}]},"finishReason":"STOP"}]}))
        }
    }));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut witnesses = Vec::new();
    for flag in [None, Some(false), Some(true)] {
        let root = tempfile::tempdir().unwrap();
        let mut value: serde_json::Value =
            serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
        value.as_object_mut().unwrap().remove("agent");
        value["data_dir"] = json!(root.path().join("data"));
        value["storage"] = json!("file");
        value["provider"] = json!({"protocol":"google","endpoint":endpoint,"model":"gemini-fixture","timeout_ms":1000});
        if let Some(flag) = flag {
            value["provider"]["google_function_response_media"] = json!(flag);
        }
        value["model_metadata"] = json!({"enabled":false});
        let config: Config = serde_json::from_value(value).unwrap();
        let manager = Manager::with_host(config, Arc::new(crate::noop::NoopHost))
            .await
            .unwrap();
        let prepared = target(
            &manager,
            "owner",
            &ModelSelection {
                provider_id: "configured".into(),
                model: "gemini-fixture".into(),
                variant: None,
            },
            Arc::new(Hub::new()),
        )
        .await
        .unwrap();
        witnesses.push(prepared.witness.clone());
        let result = prepared
            .main
            .complete(
                &content(),
                &microcrab_auditor::NoopAuditor,
                &microcrab_auditor::EmptyVariableView,
                CancellationToken::new(),
            )
            .await;
        if flag == Some(true) {
            assert_eq!(result.unwrap().content, "accepted");
        } else {
            assert!(result.unwrap_err().to_string().contains("route/model"));
        }
    }
    assert_eq!(witnesses[0], witnesses[1]);
    assert_ne!(witnesses[0], witnesses[2]);
    {
        let requests = seen.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0]["contents"][1]["parts"][0]["functionResponse"]["parts"][0],
            json!({"inlineData":{"mimeType":"image/png","data":"AQID"}})
        );
    }
    server.abort();
    let _ = server.await;
}
