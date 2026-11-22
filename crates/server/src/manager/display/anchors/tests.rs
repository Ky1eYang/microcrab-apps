//! Admission/source identity regression coverage; no model bodies enter Context.
use super::*;
use microcrab_protocol::{InputPart, ToolCall};
use microcrab_storage::display::DisplayScope;

fn source(directory: &std::path::Path) -> DisplaySource {
    scoped_source(directory, "session")
}
fn scoped_source(directory: &std::path::Path, session_id: &str) -> DisplaySource {
    let scope = DisplayScope::new("owner", "session", session_id);
    #[cfg(feature = "storage-sqlite")]
    {
        DisplaySource::Sqlite {
            database: directory.join("display.sqlite"),
            scope,
        }
    }
    #[cfg(not(feature = "storage-sqlite"))]
    {
        DisplaySource::File {
            directory: directory.to_owned(),
            scope,
        }
    }
}
async fn open_recorder(source: DisplaySource) -> Arc<DisplayRecorder> {
    DisplayRecorder::open(
        source,
        Arc::new(|_| json!({})),
        Arc::new(|_| {}),
        Arc::new(|_| {}),
    )
    .await
    .unwrap()
}
fn user() -> ContentItem {
    ContentItem::User {
        envelope: None,
        parts: vec![InputPart::Text {
            text: "user".into(),
        }],
        source: None,
    }
}
fn assistant(text: &str, calls: Vec<ToolCall>) -> ContentItem {
    ContentItem::Assistant {
        content: text.into(),
        tool_calls: calls,
        provider_metadata: None,
    }
}
#[tokio::test]
async fn input_certificates_bind_exact_occurrences_and_survive_archive_reopen_without_changing_items()
 {
    let directory = tempfile::tempdir().unwrap();
    let source = source(directory.path());
    let recorder = open_recorder(source.clone()).await;
    let anchors = Anchors::new("session".into(), source.clone(), recorder.clone());
    let items = vec![
        user(),
        user(),
        ContentItem::User {
            envelope: None,
            parts: vec![InputPart::Text {
                text: "plugin transformed".into(),
            }],
            source: None,
        },
        ContentItem::User {
            envelope: None,
            parts: vec![InputPart::Image {
                source: microcrab_protocol::ImageSource::Url {
                    url: "https://example.test/image.png".into(),
                },
            }],
            source: None,
        },
    ];
    let frozen = serde_json::to_vec(&items).unwrap();
    let submissions = vec![
        (0, "first".into()),
        (1, "repeat".into()),
        (2, "transformed".into()),
        (3, "image".into()),
    ];
    anchors.observe_input(0, &items, &submissions).await;
    assert_eq!(serde_json::to_vec(&items).unwrap(), frozen);
    let records = source.query(0, None, 128).unwrap().unwrap();
    let certificates: Vec<_> = records
        .records
        .iter()
        .filter(|record| record.event["phase"] == "display_canonical")
        .map(|record| record.event["content"]["message"]["submission_operation_id"].clone())
        .collect();
    assert_eq!(
        certificates,
        vec![
            json!("first"),
            json!("repeat"),
            json!("transformed"),
            json!("image")
        ]
    );
    recorder.close().await.unwrap();
    let reopened = open_recorder(source.clone()).await;
    let restarted = Anchors::new("session".into(), source.clone(), reopened.clone());
    let high = source.revision().unwrap();
    restarted.observe(0, &items, None).await;
    assert_eq!(
        source.revision().unwrap(),
        high,
        "ordinary observation cannot duplicate admitted inputs"
    );
    let mut messages = super::super::super::view::messages(items, 0, "session");
    restarted.annotate_messages(&mut messages).await.unwrap();
    assert_eq!(
        messages
            .iter()
            .map(|message| message["submission_operation_id"].clone())
            .collect::<Vec<_>>(),
        certificates
    );
    assert_ne!(
        messages[0]["canonical_source_id"],
        messages[1]["canonical_source_id"]
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn identical_sibling_native_ids_receive_distinct_sources_and_restart_replays_selected() {
    let directory = tempfile::tempdir().unwrap();
    let source = source(directory.path());
    let recorder = open_recorder(source.clone()).await;
    let anchors = Anchors::new("session".into(), source.clone(), recorder.clone());
    let mut items = vec![user()];
    anchors.record(0, &items, None).await.unwrap();
    let user_head = source.graph().unwrap().unwrap().selected_head;
    recorder
        .append("first".into(), json!({"phase":"started"}))
        .await
        .unwrap();
    recorder
        .append("first".into(), json!({"phase":"text_delta","text":"same"}))
        .await
        .unwrap();
    items.push(assistant("same", vec![]));
    anchors.record(0, &items, Some("first")).await.unwrap();
    let first = selected(&source).unwrap().canonical;
    recorder.select_parent(user_head).await.unwrap();
    recorder
        .append("second".into(), json!({"phase":"started"}))
        .await
        .unwrap();
    recorder
        .append("second".into(), json!({"phase":"text_delta","text":"same"}))
        .await
        .unwrap();
    anchors.record(0, &items, Some("second")).await.unwrap();
    let second = selected(&source).unwrap().canonical;
    let dto = super::super::super::view::messages(items.clone(), 0, "session");
    let native = dto[1]["id"].as_str().unwrap().to_owned();
    assert_ne!(first[&native], second[&native]);
    let graph = source.graph().unwrap().unwrap();
    assert_eq!(
        graph.blocks.len(),
        3,
        "one user plus two sibling text segments; no duplicate assistant text anchor"
    );
    recorder.close().await.unwrap();
    let reopened = open_recorder(source.clone()).await;
    let restarted = Anchors::new("session".into(), source.clone(), reopened.clone());
    let high = source.revision().unwrap();
    restarted.record(0, &items, None).await.unwrap();
    assert_eq!(source.revision().unwrap(), high);
    let mut projected = dto;
    restarted.annotate_messages(&mut projected).await.unwrap();
    assert_eq!(projected[1]["canonical_source_id"], second[&native]);
    reopened.close().await.unwrap();
}
#[tokio::test]
async fn tool_only_admission_binds_exact_producer_and_result_gets_its_own_row() {
    let directory = tempfile::tempdir().unwrap();
    let source = source(directory.path());
    let recorder = open_recorder(source.clone()).await;
    let anchors = Anchors::new("session".into(), source.clone(), recorder.clone());
    let mut items = vec![user()];
    anchors.record(0, &items, None).await.unwrap();
    recorder
        .append("provider-exact".into(), json!({"phase":"started"}))
        .await
        .unwrap();
    recorder.append("provider-exact".into(), json!({"phase":"tool_call_delta","index":0,"id":"tool-exact","name":"read","arguments":"{}"})).await.unwrap();
    items.push(assistant(
        "",
        vec![ToolCall {
            id: "tool-exact".into(),
            name: "read".into(),
            arguments: json!({}),
            provider_metadata: None,
        }],
    ));
    anchors
        .record(0, &items, Some("provider-exact"))
        .await
        .unwrap();
    items.push(ContentItem::Tool {
        parts: vec![],
        call_id: "tool-exact".into(),
        content: "result".into(),
    });
    anchors.record(0, &items, None).await.unwrap();
    let page = source.query(0, None, 128).unwrap().unwrap();
    let canonical: Vec<_> = page
        .records
        .iter()
        .filter(|r| r.event["phase"] == "display_canonical")
        .collect();
    assert_eq!(
        canonical.len(),
        2,
        "only user and tool result allocate canonical rows"
    );
    assert_eq!(canonical[0].event["content"]["message"]["role"], "user");
    assert_eq!(
        canonical[1].event["content"]["message"]["call_id"],
        "tool-exact"
    );
    let binding = page
        .records
        .iter()
        .find(|r| r.event["phase"] == "display_binding")
        .unwrap();
    assert_eq!(binding.attempt_id, "provider-exact");
    assert_eq!(binding.event["producer_call_id"], "provider-exact");
    assert_eq!(binding.event["tool_call_ids"], json!(["tool-exact"]));
    assert_eq!(
        binding.event["producer_block_ids"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    recorder.close().await.unwrap();
}

fn call(id: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: "read".into(),
        arguments: json!({}),
        provider_metadata: None,
    }
}

fn result(id: &str) -> ContentItem {
    ContentItem::Tool {
        call_id: id.into(),
        content: "result".into(),
        parts: vec![],
    }
}

#[tokio::test]
async fn executions_follow_exact_call_slots_and_survive_dormant_projection() {
    let directory = tempfile::tempdir().unwrap();
    let source = source(directory.path());
    let recorder = open_recorder(source.clone()).await;
    let anchors = Anchors::new("session".into(), source.clone(), recorder.clone());
    let mut items = vec![user(), assistant("", vec![call("reused"), call("other")])];
    anchors.record(0, &items, None).await.unwrap();
    items.push(result("other"));
    anchors
        .observe_tool_result(0, &items, 1, 1, 71, Some("failed"))
        .await;
    items.push(result("reused"));
    anchors
        .observe_tool_result(0, &items, 1, 0, 72, Some("waiting"))
        .await;
    let high = source.revision().unwrap();
    anchors
        .observe_tool_result(0, &items, 1, 0, 72, Some("waiting"))
        .await;
    assert_eq!(
        source.revision().unwrap(),
        high,
        "no extra record per result"
    );
    items.push(assistant("", vec![call("reused")]));
    anchors.record(0, &items, None).await.unwrap();
    items.push(result("reused"));
    let frozen = serde_json::to_vec(&items).unwrap();
    anchors
        .observe_tool_result(0, &items, 4, 0, 73, Some("interrupted"))
        .await;
    assert_eq!(serde_json::to_vec(&items).unwrap(), frozen);
    let raw = source.query(0, None, 128).unwrap().unwrap();
    let last = &raw.records.last().unwrap().event["content"]["message"];
    assert_eq!(last["execution_id"], 73);
    assert_eq!(last["execution_session_id"], "session");
    assert_eq!(last["execution_call_index"], 0);
    assert_eq!(last["execution_status"], "interrupted");
    assert!(uuid::Uuid::parse_str(last["execution_call_source_id"].as_str().unwrap()).is_ok());
    recorder.close().await.unwrap();

    let mut messages = super::super::super::view::messages(items, 0, "session");
    annotate_messages(source, &mut messages).await.unwrap();
    assert_eq!(messages[1]["tool_calls"][0]["execution_id"], 72);
    assert_eq!(messages[1]["tool_calls"][1]["execution_id"], 71);
    assert_eq!(messages[2]["execution_id"], 71);
    assert_eq!(messages[3]["execution_id"], 72);
    assert_eq!(messages[4]["tool_calls"][0]["execution_id"], 73);
    assert_eq!(messages[5]["execution_id"], 73);
    assert!(
        messages[1]["tool_calls"][0]
            .get("execution_status")
            .is_none()
    );
    assert!(messages[3].get("execution_status").is_none());
    assert_eq!(messages[1]["tool_calls"][1]["execution_status"], "failed");
    assert_eq!(messages[2]["execution_status"], "failed");
    assert_eq!(
        messages[4]["tool_calls"][0]["execution_status"],
        "interrupted"
    );
    assert_eq!(messages[5]["execution_status"], "interrupted");
    assert_eq!(
        messages[5]["execution_call_source_id"],
        messages[4]["canonical_source_id"]
    );
}

#[tokio::test]
async fn nonmatching_and_legacy_results_do_not_acquire_an_execution() {
    let directory = tempfile::tempdir().unwrap();
    let source = source(directory.path());
    let recorder = open_recorder(source.clone()).await;
    let anchors = Anchors::new("session".into(), source.clone(), recorder.clone());
    let mut items = vec![assistant("", vec![call("match"), call("different")])];
    anchors.record(0, &items, None).await.unwrap();
    items.push(result("match"));
    anchors.observe_tool_result(0, &items, 0, 1, 7, None).await;
    assert!(source.graph().unwrap().unwrap().projection_incomplete);
    let high = source.revision().unwrap();
    anchors.observe_tool_result(0, &items, 0, 0, 7, None).await;
    assert_eq!(source.revision().unwrap(), high);
    items.push(assistant("", vec![call("match")]));
    anchors.record(0, &items, None).await.unwrap();
    items.push(result("match"));
    anchors.observe_tool_result(0, &items, 0, 0, 8, None).await;
    let mut messages = super::super::super::view::messages(items, 0, "session");
    anchors.annotate_messages(&mut messages).await.unwrap();
    for (assistant, result) in [(0, 1), (2, 3)] {
        assert!(
            messages[assistant]["tool_calls"][0]
                .get("execution_id")
                .is_none()
        );
        assert!(messages[result].get("execution_id").is_none());
        assert!(messages[result]["canonical_source_id"].is_string());
    }
    recorder.close().await.unwrap();
}

#[tokio::test]
async fn sibling_results_are_selected_by_graph_and_fork_remap_drops_actionable_ids() {
    let directory = tempfile::tempdir().unwrap();
    let source = source(directory.path());
    let recorder = open_recorder(source.clone()).await;
    let anchors = Anchors::new("session".into(), source.clone(), recorder.clone());
    let mut items = vec![user()];
    anchors.record(0, &items, None).await.unwrap();
    let root = source.graph().unwrap().unwrap().selected_head;
    items.push(assistant("", vec![call("same")]));
    anchors.record(0, &items, None).await.unwrap();
    items.push(result("same"));
    anchors
        .observe_tool_result(0, &items, 1, 0, 81, Some("cancelled"))
        .await;
    let first = source.graph().unwrap().unwrap().selected_head;
    let mut messages = super::super::super::view::messages(items.clone(), 0, "session");
    anchors.annotate_messages(&mut messages).await.unwrap();
    let first_source = messages[1]["canonical_source_id"].clone();
    assert_eq!(messages[2]["execution_id"], 81);

    recorder.select_parent(root).await.unwrap();
    anchors.annotate_messages(&mut messages).await.unwrap();
    assert!(messages[1]["tool_calls"][0].get("execution_id").is_none());
    assert!(messages[2].get("execution_id").is_none());
    anchors.record(0, &items[..2], None).await.unwrap();
    anchors.observe_tool_result(0, &items, 1, 0, 82, None).await;
    anchors.annotate_messages(&mut messages).await.unwrap();
    assert_ne!(messages[1]["canonical_source_id"], first_source);
    assert_eq!(messages[2]["execution_id"], 82);
    assert!(messages[2].get("execution_status").is_none());

    recorder.select_parent(first).await.unwrap();
    anchors.annotate_messages(&mut messages).await.unwrap();
    assert_eq!(messages[1]["canonical_source_id"], first_source);
    assert_eq!(messages[2]["execution_id"], 81);
    assert_eq!(messages[2]["execution_status"], "cancelled");

    let mut context =
        microcrab_session::context::Context::new("system".into(), vec![], 1024 * 1024)
            .unwrap()
            .snapshot()
            .unwrap();
    context.items = items.clone();
    let remap = fork::remap(&selected(&source).unwrap(), "session", "fork", &context).unwrap();
    assert!(
        remap["mappings"][1]["message"]["tool_calls"][0]
            .get("execution_id")
            .is_none()
    );
    assert!(
        remap["mappings"][2]["message"]
            .get("execution_id")
            .is_none()
    );
    let child_dir = tempfile::tempdir().unwrap();
    let child = scoped_source(child_dir.path(), "fork");
    let child_recorder = open_recorder(child.clone()).await;
    child_recorder
        .append(
            "import".into(),
            source.graph().unwrap().unwrap().import_event().unwrap(),
        )
        .await
        .unwrap();
    child_recorder
        .append("fork-remap".into(), remap)
        .await
        .unwrap();
    let child_anchors = Anchors::new("fork".into(), child.clone(), child_recorder.clone());
    items.push(assistant("new", vec![call("same")]));
    child_anchors.record(0, &items, None).await.unwrap();
    items.push(result("same"));
    child_anchors
        .observe_tool_result(0, &items, 3, 0, 1, None)
        .await;
    let mut copies = super::super::super::view::messages(items, 0, "fork");
    annotate_messages(child, &mut copies).await.unwrap();
    assert_eq!(copies[1]["canonical_source_id"], first_source);
    assert!(copies[1]["tool_calls"][0].get("execution_id").is_none());
    assert!(copies[2].get("execution_id").is_none());
    assert!(copies[1]["tool_calls"][0].get("execution_status").is_none());
    assert!(copies[2].get("execution_status").is_none());
    assert_eq!(copies[3]["tool_calls"][0]["execution_id"], 1);
    assert_eq!(copies[4]["execution_session_id"], "fork");
    child_recorder.close().await.unwrap();
    recorder.close().await.unwrap();
}

#[tokio::test]
async fn copied_context_ordinals_remap_nested_forks_without_duplicate_nodes() {
    let original_dir = tempfile::tempdir().unwrap();
    let original = source(original_dir.path());
    let recording = open_recorder(original.clone()).await;
    let anchors = Anchors::new("original".into(), original.clone(), recording.clone());
    // Equal user text remains two exact native items, with distinct source IDs.
    let items = vec![user(), user()];
    anchors.record(0, &items, None).await.unwrap();
    let mut context =
        microcrab_session::context::Context::new("system".into(), vec![], 1024 * 1024)
            .unwrap()
            .snapshot()
            .unwrap();
    context.items = items.clone();
    let original_selected = selected(&original).unwrap();
    let event = fork::remap(&original_selected, "original", "first-fork", &context).unwrap();
    assert_eq!(event["mappings"][0]["context_item_index"], 0);
    assert_eq!(event["mappings"][1]["context_item_index"], 1);
    assert_ne!(
        event["mappings"][0]["canonical_source_id"],
        event["mappings"][1]["canonical_source_id"]
    );
    let fork_dir = tempfile::tempdir().unwrap();
    let inherited = source(fork_dir.path());
    let fork_recording = open_recorder(inherited.clone()).await;
    fork_recording
        .append(
            "import".into(),
            original.graph().unwrap().unwrap().import_event().unwrap(),
        )
        .await
        .unwrap();
    fork_recording
        .append("fork-remap".into(), event)
        .await
        .unwrap();
    let fork_anchors = Anchors::new(
        "first-fork".into(),
        inherited.clone(),
        fork_recording.clone(),
    );
    let before = inherited.revision().unwrap();
    fork_anchors.record(0, &items, None).await.unwrap();
    assert_eq!(
        inherited.revision().unwrap(),
        before,
        "inherited rows must not be reallocated"
    );
    let first_selected = selected(&inherited).unwrap();
    let nested = fork::remap(&first_selected, "first-fork", "nested-fork", &context).unwrap();
    assert_eq!(nested["mappings"].as_array().unwrap().len(), 2);
    assert_eq!(
        nested["mappings"][0]["canonical_source_id"],
        original_selected.canonical[&super::super::super::view::messages(
            items.clone(),
            0,
            "original"
        )[0]["id"]
            .as_str()
            .unwrap()
            .to_owned()]
    );
    assert_ne!(
        nested["mappings"][0]["source_canonical_message_id"],
        nested["mappings"][0]["canonical_message_id"]
    );
    fork_recording.close().await.unwrap();
    recording.close().await.unwrap();
}

#[tokio::test]
async fn canonical_projection_failure_notifies_owner_and_persists_without_revoking_admission() {
    let directory = tempfile::tempdir().unwrap();
    let source = source(directory.path());
    let recording = open_recorder(source.clone()).await;
    let anchors = Anchors::new("session".into(), source.clone(), recording.clone());
    let failures = Arc::new(Mutex::new(Vec::new()));
    let observed = failures.clone();
    anchors.set_failure_handler(Arc::new(move |cause| observed.lock().unwrap().push(cause)));
    anchors.prepare_retry("missing-exact-producer".into());
    anchors.observe(0, &[user()], None).await;
    assert_eq!(failures.lock().unwrap().len(), 1);
    assert!(!failures.lock().unwrap()[0].is_empty());
    let page = source.query(0, None, 128).unwrap().unwrap();
    assert!(
        page.records
            .iter()
            .any(|row| row.event["phase"] == "display_projection_unavailable"
                && row.event["reason"] == "canonical_admission_certificate_unavailable")
    );
    // Ordinary later admission may still progress, but cannot certify that a
    // missing older row occupied its required chronological position.
    anchors.observe(0, &[user()], None).await;
    let page = source.query(0, None, 128).unwrap().unwrap();
    assert!(
        page.records
            .iter()
            .any(|row| row.event["phase"] == "display_canonical")
    );
    assert_eq!(
        page.records
            .iter()
            .filter(|row| row.event["phase"] == "display_projection_unavailable")
            .count(),
        1
    );
    assert_eq!(failures.lock().unwrap().len(), 1);
    recording.close().await.unwrap();
}
