//! Exact runtime child authority is shared by canonical and display reads.
use super::*;

fn child_admission_recover(
    actual: &Context,
    receipts: &[(&str, &str)],
) -> (
    Recover,
    microcrab_session::runtime::journal::AdmissionCheckpoint,
) {
    let proof =
        microcrab_session::runtime::journal::AdmissionCheckpoint::from_context(actual).unwrap();
    let mut recovery = actual.clone();
    for (call, text) in receipts {
        recovery
            .append_tool_result((*call).into(), (*text).into())
            .unwrap();
    }
    let mut value = recover("unused");
    value.context = recovery.snapshot().unwrap();
    (value, proof)
}

async fn child_display(manager: &Manager, child: &str, lineage: &Value) -> Value {
    let request = serde_json::from_value(json!({
        "child_session_id":child,"history_lineage":lineage,
    }))
    .unwrap();
    crate::manager::display::query(manager, "owner", "parent", request)
        .await
        .unwrap()
}

#[tokio::test]
async fn typed_admission_child_projection_hides_only_synthetic_suffix_across_partial_settlement() {
    use microcrab_protocol::{Response, ToolCall, ToolDefinition};
    use microcrab_transport::{ModelObservation, ModelRecording, ObservationState};
    let root = tempfile::tempdir().unwrap();
    let manager = fixture(root.path()).await;
    let directory = parent(&manager, "parent").await;
    let (id, child) = microcrab_creator::paths::reserve_child(&directory).unwrap();
    let mut actual = Context::new(
        "private-system-marker".into(),
        vec![ToolDefinition {
            name: "inspect".into(),
            description: "child inspection fixture".into(),
            parameters: json!({"type":"object"}),
        }],
        Limits::default().max_context_bytes,
    )
    .unwrap();
    actual
        .admit_user("inspect child siblings", &json!({}))
        .unwrap();
    actual
        .append_response(&Response {
            provider_metadata: None,
            content: "inspect siblings".into(),
            usage: Default::default(),
            tool_calls: ["a", "b"]
                .into_iter()
                .map(|call| ToolCall {
                    id: call.into(),
                    name: "inspect".into(),
                    arguments: json!({"id":call}),
                    provider_metadata: None,
                })
                .collect(),
        })
        .unwrap();
    let (writer, _) = CreatorJournal::open(child.to_str().unwrap(), id.clone(), true, false)
        .await
        .unwrap();
    manager.child_journals.lock().unwrap().insert(
        ("owner".into(), "parent".into(), id.clone()),
        writer.clone(),
    );
    let (first, proof) = child_admission_recover(
        &actual,
        &[("a", "synthetic-first-a"), ("b", "synthetic-first-b")],
    );
    let first_bytes = serde_json::to_vec(&first).unwrap();
    writer.commit_admission(first, proof).await.unwrap();
    let before = manager
        .inspect_child("owner", detail("parent", &id))
        .await
        .unwrap();
    assert_eq!(before["messages"].as_array().unwrap().len(), 2);
    assert!(
        before["messages"]
            .as_array()
            .unwrap()
            .iter()
            .all(|message| message["role"] != "tool")
    );
    assert!(!before["messages"].to_string().contains("synthetic-first"));
    let lineage = before["history_view"]["lineage"].clone();
    let first_cursor = writer.cursor().await.unwrap();
    let legacy = child_display(&manager, &id, &lineage).await;
    assert_eq!(legacy["history_lineage"], lineage);
    assert_eq!(legacy["coverage"], "missing");

    let (repeated, proof) = child_admission_recover(
        &actual,
        &[("a", "synthetic-second-a"), ("b", "synthetic-second-b")],
    );
    writer.commit_admission(repeated, proof).await.unwrap();
    assert_ne!(writer.cursor().await.unwrap(), first_cursor);
    let repeated = manager
        .inspect_child("owner", detail("parent", &id))
        .await
        .unwrap();
    assert_eq!(repeated["history_view"]["lineage"], lineage);
    assert_eq!(repeated["messages"], before["messages"]);
    assert_eq!(
        child_display(&manager, &id, &lineage).await["history_lineage"],
        lineage
    );

    // The same bytes could legitimately be a canonical tool result. Provenance,
    // not JSON/status heuristics, makes this sibling immediately visible.
    let canonical =
        r#"{"status":"waiting","error":"not executed; interrupted before invocation admission"}"#;
    actual
        .append_tool_result("a".into(), canonical.into())
        .unwrap();
    let (partial, proof) = child_admission_recover(&actual, &[("b", "synthetic-pending-b")]);
    writer.commit_admission(partial, proof).await.unwrap();
    let partial = manager
        .inspect_child("owner", detail("parent", &id))
        .await
        .unwrap();
    assert_eq!(partial["history_view"]["lineage"], lineage);
    let tools: Vec<_> = partial["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "tool")
        .collect();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["text"], canonical);
    assert!(
        !partial["messages"]
            .to_string()
            .contains("synthetic-pending")
    );
    assert_eq!(partial["history_view"]["message_count"], 3);

    // Install an actual display graph so the early graph-return path must share
    // the same child fence, not trust a requested lineage as its authority.
    let source =
        crate::manager::display::source(&manager.config, "owner", "parent", &id, child.clone());
    let recorder = microcrab_creator::display::DisplayRecorder::open(
        source.clone(),
        Arc::new(|_| json!({})),
        Arc::new(|_| {}),
        Arc::new(|cause| panic!("child fixture display failed: {cause}")),
    )
    .await
    .unwrap();
    for state in [
        ObservationState::Started,
        ObservationState::TextDelta {
            text: "actual child graph".into(),
        },
    ] {
        recorder
            .record(ModelObservation {
                call_id: "child-display-attempt".into(),
                epoch: 0,
                turn: Some(1),
                round: Some(1),
                subagent: None,
                state,
            })
            .await
            .unwrap();
    }
    recorder.flush().await.unwrap();
    assert!(source.graph_at(None).unwrap().unwrap().selection_known);
    let graph = child_display(&manager, &id, &lineage).await;
    assert_eq!(graph["history_lineage"], lineage);
    assert!(
        graph.get("graph").is_some(),
        "fixture must exercise graph query return"
    );
    let forged =
        serde_json::from_value(json!({"child_session_id":id,"history_lineage":"forged"})).unwrap();
    assert_eq!(
        crate::manager::display::query(&manager, "owner", "parent", forged)
            .await
            .unwrap_err()
            .code,
        "stale_lineage"
    );

    actual
        .append_tool_result("b".into(), "actual final sibling".into())
        .unwrap();
    let mut settled = recover("unused");
    settled.context = actual.snapshot().unwrap();
    writer.commit(settled).await.unwrap();
    let completed = manager
        .inspect_child("owner", detail("parent", &id))
        .await
        .unwrap();
    assert_eq!(completed["history_view"]["lineage"], lineage);
    assert_eq!(
        completed["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|message| message["role"] == "tool")
            .count(),
        2
    );
    assert_eq!(
        child_display(&manager, &id, &lineage).await["history_lineage"],
        lineage
    );
    let frozen = manager
        .child_checkpoint(&child, &id)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(frozen.cursor, first_cursor);
    let first_node = std::fs::read(
        child
            .join("journal")
            .join("nodes")
            .join(format!("{}.json", first_cursor.checkpoint_id)),
    )
    .unwrap();
    let first_node: Value = serde_json::from_slice(&first_node).unwrap();
    assert_eq!(
        first_node["checkpoint"]["recover"],
        serde_json::from_slice::<Value>(&first_bytes).unwrap()
    );
    manager.child_journals.lock().unwrap().clear();
    let fallback = manager
        .inspect_child("owner", detail("parent", &id))
        .await
        .unwrap();
    assert_eq!(
        fallback["history_view"]["lineage"],
        format!(
            "child:{}:{}:{}",
            manager.instance, id, frozen.cursor.checkpoint_id
        )
    );
    assert_ne!(fallback["history_view"]["lineage"], lineage);
    recorder.close().await.unwrap();
    writer.close().await.unwrap();
    manager.close().await.unwrap();
}

#[tokio::test]
async fn runtime_child_append_keeps_shared_lineage_and_travel_revokes_it() {
    let root = tempfile::tempdir().unwrap();
    let manager = fixture(root.path()).await;
    let directory = parent(&manager, "parent").await;
    let (id, child) = microcrab_creator::paths::reserve_child(&directory).unwrap();
    let original = recover("first child input");
    let writer = journal(&child, &id, original.clone()).await;
    manager.child_journals.lock().unwrap().insert(
        ("owner".into(), "parent".into(), id.clone()),
        writer.clone(),
    );
    let before = manager
        .inspect_child("owner", detail("parent", &id))
        .await
        .unwrap();
    let first = writer.cursor().await.unwrap();
    let point = manager
        .child_checkpoint(&child, &id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        manager
            .child_selection("intruder", "parent", &id, &point)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        manager
            .child_selection("owner", "other", &id, &point)
            .await
            .unwrap()
            .is_none()
    );
    let mut context = Context::from_snapshot(original.context).unwrap();
    context
        .append_response(&microcrab_protocol::Response {
            provider_metadata: None,
            content: "finished child response".into(),
            tool_calls: vec![],
            usage: Default::default(),
        })
        .unwrap();
    let mut next = recover("unused");
    next.context = context.snapshot().unwrap();
    writer.commit(next).await.unwrap();
    let after = manager
        .inspect_child("owner", detail("parent", &id))
        .await
        .unwrap();
    assert_ne!(before["history"], after["history"]);
    assert_eq!(
        before["history_view"]["lineage"],
        after["history_view"]["lineage"]
    );
    let request = serde_json::from_value(
        json!({"child_session_id":id,"history_lineage":after["history_view"]["lineage"]}),
    )
    .unwrap();
    let display = crate::manager::display::query(&manager, "owner", "parent", request)
        .await
        .unwrap();
    assert_eq!(display["history_lineage"], after["history_view"]["lineage"]);
    let selected = writer.cursor().await.unwrap();
    let restore = writer
        .prepare_travel(microcrab_storage::Travel::To {
            checkpoint_id: selected.checkpoint_id,
        })
        .await
        .unwrap();
    writer.recovered(restore).await.unwrap();
    let traveled = manager
        .inspect_child("owner", detail("parent", &id))
        .await
        .unwrap();
    assert_ne!(
        traveled["history_view"]["lineage"],
        after["history_view"]["lineage"]
    );
    let stale = serde_json::from_value(
        json!({"child_session_id":id,"history_lineage":after["history_view"]["lineage"]}),
    )
    .unwrap();
    assert_eq!(
        crate::manager::display::query(&manager, "owner", "parent", stale)
            .await
            .unwrap_err()
            .code,
        "stale_lineage"
    );
    assert_ne!(first, writer.cursor().await.unwrap());
    manager.child_journals.lock().unwrap().clear();
    let fallback = manager
        .inspect_child("owner", detail("parent", &id))
        .await
        .unwrap();
    assert_ne!(
        fallback["history_view"]["lineage"],
        traveled["history_view"]["lineage"]
    );
    writer.close().await.unwrap();
    manager.close().await.unwrap();
}
