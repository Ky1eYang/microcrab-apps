//! Real provider -> recorder -> selected replay -> restart/fork acceptance.
use super::*;
use crate::config::Storage;
mod legacy;

async fn thinking_provider(thought: &str) -> ProviderMock {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/chat/completions", listener.local_addr().unwrap());
    let wire = Arc::new(Wire {
        requests: Mutex::new(vec![]),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let response = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"index":0,"delta":{"reasoning_content":thought,"content":"Wire answer"},"finish_reason":null}]}),
        json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]})
    );
    let worker_wire = wire.clone();
    let router = Router::new().route(
        "/chat/completions",
        post(move |headers: HeaderMap, Json(body): Json<Value>| {
            let wire = worker_wire.clone();
            let response = response.clone();
            async move {
                let authorization = headers
                    .get("authorization")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned();
                wire.requests.lock().unwrap().push((authorization, body));
                (
                    [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                    response,
                )
            }
        }),
    );
    let worker = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    ProviderMock {
        endpoint,
        wire,
        worker,
    }
}
async fn replay(manager: &Manager, id: &str) -> (String, Value) {
    let mut cursor = 0;
    let mut high = None;
    let mut reasoning = String::new();
    let mut last = Value::Null;
    loop {
        let page = super::super::display::query(
            manager,
            "owner",
            id,
            serde_json::from_value(json!({"after":cursor,"high_water":high,"limit":2})).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(page["coverage"], "available", "{page}");
        high = Some(page["high_water"].as_u64().unwrap());
        for row in page["records"].as_array().unwrap() {
            if row["event"]["phase"] == "reasoning_delta" {
                reasoning.push_str(row["event"]["text"].as_str().unwrap());
            }
            if matches!(
                row["event"]["phase"].as_str(),
                Some("validated" | "invalidated")
            ) {
                last = row.clone();
            }
        }
        let Some(next) = page["next_cursor"].as_u64() else {
            break;
        };
        assert!(next > cursor);
        cursor = next;
    }
    (reasoning, last)
}

async fn durable_replay(storage: Storage) {
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        let root = tempfile::tempdir().unwrap();
        let thought = "Synthetic durable reasoning 界🦀\n".repeat(12_000);
        assert!(thought.len() > 256 * 1024);
        let a = thinking_provider(&thought).await;
        let b = provider(10).await;
        let mut config = config(root.path(), &a, &b);
        config.storage = storage;
        let manager = Manager::with_host(config.clone(), Arc::new(crate::noop::NoopHost))
            .await
            .unwrap();
        let managed = create(&manager).await;
        let id = managed.record.lock().await.id.clone();
        submit(&manager, &managed, "First input").await;
        let native_source = super::super::display::source(
            &manager.config,
            "owner",
            &id,
            &id,
            manager.config.data_dir.join("sessions").join(&id),
        );
        let native_graph = native_source.graph().unwrap().unwrap();
        let mut cursor = native_graph.selected_head.as_ref();
        let mut chronology = Vec::new();
        while let Some(id) = cursor {
            let block = &native_graph.blocks[id];
            chronology.push(block.clone());
            cursor = block.parent_block_id.as_ref();
        }
        chronology.reverse();
        assert_eq!(chronology[0].block_type, "canonical");
        assert_eq!(
            chronology
                .iter()
                .filter(|block| block.block_type == "reasoning")
                .count(),
            1
        );
        let root_page = native_source
            .query(chronology[0].sequence - 1, Some(native_graph.high_water), 1)
            .unwrap()
            .unwrap();
        assert_eq!(
            root_page.records[0].event["content"]["message"]["role"],
            "user"
        );
        assert_eq!(
            root_page.records[0].event["display_block"]["block_id"],
            chronology[0].block_id
        );
        let before_context = managed.session.context_snapshot().unwrap();
        let assistants = before_context
            .items
            .iter()
            .filter_map(|item| match item {
                microcrab_transport::ContentItem::Assistant {
                    content,
                    tool_calls,
                    provider_metadata,
                } => Some((content, tool_calls, provider_metadata)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            assistants.len(),
            1,
            "one accepted response owns one neutral assistant"
        );
        let (content, calls, metadata) = assistants[0];
        assert_eq!(content, "Wire answer");
        assert!(calls.is_empty());
        let Some(microcrab_protocol::AssistantProviderMetadata::Neutral { parts, .. }) = metadata
        else {
            panic!("accepted reasoning must be retained as neutral assistant semantics");
        };
        assert_eq!(
            parts,
            &vec![
                microcrab_protocol::AssistantPart::Reasoning {
                    text: thought.clone()
                },
                microcrab_protocol::AssistantPart::Text {
                    text: "Wire answer".into()
                },
            ]
        );
        let before = serde_json::to_value(&before_context).unwrap();
        let (restored, terminal) = replay(&manager, &id).await;
        assert_eq!(restored, thought);
        assert_eq!(terminal["event"]["phase"], "validated");
        assert_eq!(terminal["event"]["reasoning_bytes"], thought.len());
        let mut after = 0;
        let mut bindings = Vec::new();
        let mut assistant_anchors = 0;
        loop {
            let page = native_source
                .query(after, Some(native_graph.high_water), 128)
                .unwrap()
                .unwrap();
            for record in page.records {
                if record.event["phase"] == "display_binding" {
                    bindings.push(record.event.clone());
                }
                if record.event["phase"] == "display_canonical"
                    && record.event["content"]["message"]["role"] == "assistant"
                {
                    assistant_anchors += 1;
                }
            }
            match page.next_cursor {
                Some(next) => after = next,
                None => break,
            }
        }
        let binding = bindings
            .iter()
            .find(|binding| binding["producer_call_id"] == terminal["event"]["call_id"])
            .expect("actual successful transport must bind its admitted canonical response");
        assert!(!binding["canonical_source_id"].as_str().unwrap().is_empty());
        assert_eq!(binding["canonical_message_id"], binding["message"]["id"]);
        assert_eq!(
            binding["canonical_source_id"],
            binding["message"]["canonical_source_id"]
        );
        assert!(
            binding["producer_block_ids"]
                .as_array()
                .unwrap()
                .iter()
                .any(|id| native_graph.blocks[id.as_str().unwrap()].block_type == "reasoning")
        );
        assert_eq!(
            assistant_anchors, 0,
            "accepted streamed output must bind existing blocks, never duplicate them"
        );
        assert_eq!(
            before,
            serde_json::to_value(managed.session.context_snapshot().unwrap()).unwrap()
        );
        // Display replay reads its own archive without changing the neutral reasoning above.
        // Canonical public messages keep the answer projection and hide replay metadata.
        let public_messages = managed.hub.snapshot()["messages"].clone();
        assert!(
            public_messages
                .as_array()
                .unwrap()
                .iter()
                .all(|message| message.get("provider_metadata").is_none()
                    && message.get("replay_hints").is_none())
        );
        assert!(
            !public_messages
                .to_string()
                .contains("Synthetic durable reasoning")
        );
        let owner = crate::auth::Principal::for_test("owner");
        let forked = manager
            .command(
                &owner,
                &command(
                    "session.fork",
                    Some(&id),
                    json!({"operation_id":uuid::Uuid::new_v4().to_string()}),
                ),
            )
            .await
            .unwrap();
        let fork_id = forked["id"].as_str().unwrap().to_owned();
        assert_eq!(replay(&manager, &fork_id).await.0, thought);
        let fork_page = super::super::display::query(
            &manager,
            "owner",
            &fork_id,
            serde_json::from_value(json!({"limit":1024})).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            fork_page["graph"]["selected_head"],
            json!(native_graph.selected_head)
        );
        let remap = fork_page["records"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["event"]["phase"] == "display_fork_remap")
            .unwrap();
        let context = managed.session.context_snapshot().unwrap();
        let target_messages = super::super::view::messages(context.items, context.epoch, &fork_id);
        let mappings = remap["event"]["mappings"].as_array().unwrap();
        assert!(!mappings.is_empty());
        for mapping in mappings {
            assert!(
                target_messages
                    .iter()
                    .any(|message| message["id"] == mapping["canonical_message_id"])
            );
            assert_ne!(
                mapping["source_canonical_message_id"],
                mapping["canonical_message_id"]
            );
            assert_eq!(mapping["message"]["id"], mapping["canonical_message_id"]);
        }
        submit(&manager, &managed, "Second input after display replay").await;
        assert_eq!(a.wire.requests.lock().unwrap().len(), 2);
        let next = a.wire.requests.lock().unwrap()[1].1.clone();
        let next_messages = next["messages"].as_array().unwrap();
        let replayed_assistants = next_messages
            .iter()
            .filter(|message| message["role"] == "assistant")
            .collect::<Vec<_>>();
        assert_eq!(
            replayed_assistants.len(),
            1,
            "display replay must not append another assistant input"
        );
        assert_eq!(replayed_assistants[0]["reasoning_content"], thought);
        assert_eq!(replayed_assistants[0]["content"], "Wire answer");
        for message in next_messages {
            assert!(message.get("provider_metadata").is_none());
            assert!(message.get("replay_hints").is_none());
            if message["role"] != "assistant" {
                assert!(
                    !message.to_string().contains("Synthetic durable reasoning"),
                    "display reasoning must not be injected as user, State or system input"
                );
            }
        }
        // The fork references its captured prefix, never later source observations.
        assert_eq!(replay(&manager, &fork_id).await.0, thought);
        let denied = super::super::display::query(
            &manager,
            "foreign",
            &id,
            serde_json::from_value(json!({})).unwrap(),
        )
        .await;
        assert!(denied.is_err());
        manager.close().await.unwrap();
        drop(managed);
        drop(manager);
        let restarted = Manager::with_host(config, Arc::new(crate::noop::NoopHost))
            .await
            .unwrap();
        // Detached replay must work without activating a Session/provider/Host.
        assert_eq!(replay(&restarted, &id).await.0, thought.repeat(2));
        assert_eq!(replay(&restarted, &fork_id).await.0, thought);
        assert!(restarted.registry.lock().unwrap().is_empty());
        assert_eq!(a.wire.requests.lock().unwrap().len(), 2);
        restarted.close().await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_complete_thinking_replays_after_restart_and_fork_with_neutral_context_without_display_duplication()
 {
    durable_replay(Storage::File).await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_complete_thinking_replays_after_restart_and_fork_with_neutral_context_without_display_duplication()
 {
    durable_replay(Storage::Sqlite).await;
}
