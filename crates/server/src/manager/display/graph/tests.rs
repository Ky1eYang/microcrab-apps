//! Real recorder frames prove chronological graph placement and retry exclusion.
use super::*;
use microcrab_creator::display::DisplayRecorder;
use microcrab_storage::display::DisplayScope;
use microcrab_transport::{ModelObservation, ModelRecording, ObservationState};
use std::sync::{Arc, Mutex};

async fn produced_graph(source: DisplaySource) {
    let frames = Arc::new(Mutex::new(Vec::new()));
    let committed = frames.clone();
    let recorder = DisplayRecorder::open(
        source.clone(),
        Arc::new(|_| json!({})),
        Arc::new(move |frame| committed.lock().unwrap().push(frame)),
        Arc::new(|cause| panic!("display recording failed: {cause}")),
    )
    .await
    .unwrap();
    let observation = |call: &str, state| ModelObservation {
        call_id: call.into(),
        epoch: 0,
        turn: Some(1),
        round: Some(1),
        subagent: None,
        state,
    };
    for state in [
        ObservationState::Started,
        ObservationState::ReasoningDelta {
            text: "first thought".into(),
        },
        ObservationState::TextDelta {
            text: "intermediate answer".into(),
        },
        ObservationState::ReasoningDelta {
            text: "abandoned thought".into(),
        },
    ] {
        recorder.record(observation("first", state)).await.unwrap();
    }
    let graph = source.graph_at(None).unwrap().unwrap();
    let initial = envelope(&graph).unwrap();
    assert_eq!(
        initial["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|node| node["block_type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["reasoning", "text", "reasoning"]
    );
    let retained = initial["nodes"][1]["block_id"].as_str().unwrap().to_owned();
    let abandoned = initial["nodes"][2]["block_id"].as_str().unwrap().to_owned();
    recorder
        .select_parent(Some(retained.clone()))
        .await
        .unwrap();
    recorder
        .record(observation("retry", ObservationState::Started))
        .await
        .unwrap();
    recorder
        .record(observation(
            "retry",
            ObservationState::ReasoningDelta {
                text: "retry thought".into(),
            },
        ))
        .await
        .unwrap();
    recorder
        .record(observation(
            "retry",
            ObservationState::ToolCallDelta {
                index: 0,
                id: Some("invocation-1".into()),
                name: Some("read".into()),
                arguments: "{\"path\":\"README.md\"}".into(),
            },
        ))
        .await
        .unwrap();
    recorder
        .record(observation(
            "retry",
            ObservationState::Invalidated {
                reason: "cancelled".into(),
            },
        ))
        .await
        .unwrap();
    recorder.flush().await.unwrap();
    assert_cached_pages_match_reference(&source);
    let replayed = page(&source, 0, None, 128).unwrap().unwrap();
    let nodes = replayed["graph"]["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 4);
    assert_eq!(nodes[2]["parent_block_id"], retained);
    assert!(nodes.iter().all(|node| node["block_id"] != abandoned));
    let text = replayed["records"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row["event"]["text"].as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        text,
        ["first thought", "intermediate answer", "retry thought"]
    );
    let frames = frames.lock().unwrap().clone();
    assert!(
        frames
            .iter()
            .any(|frame| frame.event["display_block"]["block_id"] == abandoned)
    );
    assert!(
        frames
            .iter()
            .any(|frame| frame.event["phase"] == "display_head_selected")
    );
    // The exact same parent identity appears in persisted producer frames and replay.
    assert_eq!(
        frames
            .iter()
            .find(|frame| frame.attempt_id == "retry" && frame.event["phase"] == "reasoning_delta")
            .unwrap()
            .event["display_block"]["parent_block_id"],
        retained
    );
    let high = replayed["high_water"].as_u64().unwrap();
    let settled = page(&source, high, Some(high), 1).unwrap().unwrap();
    assert!(settled["records"].as_array().unwrap().is_empty());
    assert_eq!(
        settled["graph"]["selected_head"],
        replayed["graph"]["selected_head"]
    );
    let mut fixture = json!({"frames":frames,"page":replayed,"settled":settled});
    normalize_timestamps(&mut fixture);
    let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/display-block-graph.json");
    if std::env::var_os("MICROCRAB_UPDATE_DISPLAY_GRAPH_FIXTURE").is_some() {
        std::fs::create_dir_all(fixture_path.parent().unwrap()).unwrap();
        std::fs::write(
            &fixture_path,
            serde_json::to_string_pretty(&fixture).unwrap() + "\n",
        )
        .unwrap();
    }
    let frozen: Value = serde_json::from_slice(
        &std::fs::read(&fixture_path)
            .expect("Generate the shared fixture with MICROCRAB_UPDATE_DISPLAY_GRAPH_FIXTURE=1"),
    )
    .unwrap();
    assert_eq!(
        fixture, frozen,
        "Native producer frames and browser fixture diverged"
    );
    recorder.close().await.unwrap();
    assert_eq!(page(&source, 0, None, 128).unwrap().unwrap(), replayed);
    immutable_high_water_survives_suffix(&source);
}

fn immutable_high_water_survives_suffix(source: &DisplaySource) {
    use microcrab_storage::display::ArchiveRecord;
    let mut graph = source.graph_at(None).unwrap().unwrap();
    let prepared = PreparedGraph::new(&graph).unwrap();
    let frozen = page_from_prepared(source, &prepared, 0, 128)
        .unwrap()
        .unwrap();
    let selected_id = envelope(&graph).unwrap()["nodes"][0]["block_id"].clone();
    let mut store = source.open().unwrap();
    let large_text = "x".repeat(256 * 1024);
    for event in [
        json!({"phase":"display_fork_remap","target_session_id":"different-session"}),
        json!({"phase":"display_fork_remap","target_session_id":"root"}),
        json!({"phase":"fork_record","display_source_block_id":selected_id}),
        json!({"phase":"started"}),
        json!({"phase":"text_delta","text":large_text}),
    ] {
        let mut row = ArchiveRecord {
            sequence: graph.high_water + 1,
            attempt_id: "live-suffix".into(),
            event,
        };
        graph.decorate(&mut row, 0).unwrap();
        store.append(graph.high_water, &row).unwrap();
        graph.apply(&row).unwrap();
    }
    store.flush().unwrap();
    drop(store);
    assert_eq!(
        page_from_prepared(source, &prepared, 0, 128)
            .unwrap()
            .unwrap(),
        frozen
    );
    let suffix = PreparedGraph::new(&source.graph_at(None).unwrap().unwrap()).unwrap();
    let remap = page_from_prepared(source, &suffix, prepared.high_water + 1, 1)
        .unwrap()
        .unwrap();
    assert_eq!(remap["records"][0]["event"]["phase"], "display_fork_remap");
    let reference = page_from_prepared(source, &suffix, prepared.high_water + 2, 1)
        .unwrap()
        .unwrap();
    assert_eq!(reference["graph"]["nodes"][0]["block_id"], selected_id);
    let large = page_from_prepared(source, &suffix, suffix.high_water - 1, 1)
        .unwrap()
        .unwrap();
    assert_eq!(
        large["records"][0]["event"]["text"].as_str().unwrap().len(),
        256 * 1024
    );
    assert_eq!(large["next_cursor"], Value::Null);
    assert_cached_pages_match_reference(source);
}

/// Frozen full-chain page projection used before the catch-up cache. Keep this
/// deliberately linear so comparisons exercise the optimized lookup independently.
fn reference_page(source: &DisplaySource, graph: &DisplayGraph, after: u64, limit: usize) -> Value {
    let mut selected = envelope(graph).unwrap();
    let nodes = selected["nodes"].as_array().unwrap();
    let ids: HashSet<_> = nodes
        .iter()
        .filter_map(|node| node["block_id"].as_str())
        .collect();
    let attempts: HashSet<_> = nodes
        .iter()
        .filter_map(|node| node["attempt_id"].as_str())
        .collect();
    let page = source
        .query(after, Some(graph.high_water), limit)
        .unwrap()
        .unwrap();
    let records: Vec<_> = page
        .records
        .into_iter()
        .filter(|record| {
            if record.event["phase"] == "display_fork_remap" {
                let (DisplaySource::File { scope, .. } | DisplaySource::Sqlite { scope, .. }) =
                    source;
                return record.event["target_session_id"].as_str() == Some(scope.session.as_str());
            }
            if let Some(id) = record.event["display_block"]["block_id"]
                .as_str()
                .or_else(|| record.event["display_source_block_id"].as_str())
            {
                ids.contains(id)
            } else {
                attempts.contains(record.attempt_id.as_str())
                    && matches!(
                        record.event["phase"].as_str(),
                        Some(
                            "started"
                                | "validated"
                                | "invalidated"
                                | "display_binding"
                                | "fork_record"
                        )
                    )
            }
        })
        .collect();
    let scanned = page.next_cursor.unwrap_or(graph.high_water);
    let referenced: HashSet<_> = records
        .iter()
        .filter_map(|record| record.event["display_source_block_id"].as_str())
        .collect();
    let emitted = json!(
        nodes
            .iter()
            .filter(|node| {
                node["block_id"]
                    .as_str()
                    .is_some_and(|id| referenced.contains(id))
                    || node["sequence"]
                        .as_u64()
                        .is_some_and(|sequence| sequence > after && sequence <= scanned)
            })
            .collect::<Vec<_>>()
    );
    selected["nodes"] = emitted;
    json!({"records":records,"high_water":graph.high_water,"next_cursor":page.next_cursor,
        "coverage":if selected["complete"] == true {"available"} else {"unavailable"},"graph":selected})
}

fn assert_cached_pages_match_reference(source: &DisplaySource) {
    let graph = source.graph_at(None).unwrap().unwrap();
    let prepared = Arc::new(PreparedGraph::new(&graph).unwrap());
    let frozen_header = prepared.header.clone();
    let frozen_nodes = prepared.nodes.clone();
    let mut saw_filtered_empty_page = false;
    for limit in [1, 2, 7, 128] {
        let mut after = 0;
        loop {
            let expected = reference_page(source, &graph, after, limit);
            let actual = page_from_prepared(source, &prepared, after, limit)
                .unwrap()
                .unwrap();
            assert_eq!(actual, expected, "cursor={after}, limit={limit}");
            saw_filtered_empty_page |=
                actual["records"].as_array().unwrap().is_empty() && actual["next_cursor"].is_u64();
            if let Some(next) = actual["next_cursor"].as_u64() {
                assert!(
                    next > after,
                    "filtered records must not stall scanned coverage"
                );
                after = next;
            } else {
                break;
            }
        }
        assert_eq!(
            page_from_prepared(source, &prepared, graph.high_water, limit)
                .unwrap()
                .unwrap(),
            reference_page(source, &graph, graph.high_water, limit)
        );
    }
    assert!(
        saw_filtered_empty_page,
        "fixture must exercise excluded sibling/selection bytes"
    );
    assert_eq!(prepared.header, frozen_header);
    assert_eq!(prepared.nodes, frozen_nodes);
}

#[test]
fn indexed_nodes_preserve_ancestry_order_and_exact_source_references() {
    use microcrab_storage::display::DisplayBlock;
    let mut graph = DisplayGraph::empty(DisplayScope::new("owner", "root", "root"));
    let mut parent = None;
    // Sequence order deliberately differs from parent order, as with remapped
    // ancestry. A narrow page must still return the original envelope ordering.
    for (id, sequence) in [("first", 30), ("second", 10), ("third", 20)] {
        graph.blocks.insert(
            id.into(),
            DisplayBlock {
                block_id: id.into(),
                parent_block_id: parent,
                block_type: "text".into(),
                content_ref: format!("display:{id}"),
                sequence,
                timestamp_ms: 0,
                attempt_id: "attempt".into(),
                stream_key: "text".into(),
            },
        );
        parent = Some(id.into());
    }
    graph.selected_head = parent;
    graph.high_water = 30;
    let prepared = PreparedGraph::new(&graph).unwrap();
    let nodes = prepared.emitted_nodes(10, 20, ["first", "third", "first", "unknown"].into_iter());
    assert_eq!(
        nodes
            .iter()
            .map(|node| node["block_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["first", "third"]
    );
    assert!(
        prepared
            .emitted_nodes(30, 30, std::iter::empty())
            .is_empty()
    );
    graph.pending_selection = Some(json!({"pending": true}));
    graph.projection_incomplete = true;
    graph.legacy_content_records = 1;
    let mut sibling = graph.blocks["first"].clone();
    sibling.block_id = "unselected".into();
    sibling.attempt_id = "unselected-attempt".into();
    graph.blocks.insert(sibling.block_id.clone(), sibling);
    let incomplete = PreparedGraph::new(&graph).unwrap();
    assert!(incomplete.all_attempts.contains("unselected-attempt"));
    assert!(!incomplete.attempts.contains("unselected-attempt"));
    assert!(incomplete.selection_pending);
    assert!(incomplete.projection_incomplete);
    assert_eq!(incomplete.legacy_content_records, 1);
    assert!(!prepared.selection_pending);
    assert!(!prepared.projection_incomplete);
    assert_eq!(incomplete.header["complete"], false);
    assert_eq!(prepared.header["complete"], true);
}

#[test]
fn repeated_narrow_pages_do_not_scan_selected_history() {
    use microcrab_storage::display::DisplayBlock;
    use std::sync::atomic::Ordering;
    let mut graph = DisplayGraph::empty(DisplayScope::new("owner", "root", "root"));
    let mut parent = None;
    for sequence in 1..=4096 {
        let id = format!("block-{sequence}");
        graph.blocks.insert(
            id.clone(),
            DisplayBlock {
                block_id: id.clone(),
                parent_block_id: parent,
                block_type: "text".into(),
                content_ref: format!("display:{id}"),
                sequence,
                timestamp_ms: 0,
                attempt_id: "attempt".into(),
                stream_key: "text".into(),
            },
        );
        parent = Some(id);
    }
    graph.selected_head = parent;
    graph.high_water = 4096;
    let prepared = Arc::new(PreparedGraph::new(&graph).unwrap());
    for after in [0, 1024, 2048, 4095] {
        let before = prepared.lookup_work.load(Ordering::Relaxed);
        let nodes = prepared.emitted_nodes(after, after + 1, std::iter::empty());
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0]["sequence"], after + 1);
        let work = prepared.lookup_work.load(Ordering::Relaxed) - before;
        assert!(
            work <= 32,
            "a narrow page visited {work} index entries for 4096 selected nodes"
        );
    }
    assert_eq!(prepared.nodes.len(), 4096);
}

fn normalize_timestamps(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                if key == "timestamp_ms" {
                    *value = json!(0);
                } else {
                    normalize_timestamps(value);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                normalize_timestamps(value);
            }
        }
        _ => {}
    }
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_produced_frames_replay_chronologically_and_exclude_retry_sibling() {
    let root = tempfile::tempdir().unwrap();
    produced_graph(DisplaySource::File {
        directory: root.path().to_owned(),
        scope: DisplayScope::new("owner", "root", "root"),
    })
    .await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_produced_frames_replay_chronologically_and_exclude_retry_sibling() {
    let root = tempfile::tempdir().unwrap();
    produced_graph(DisplaySource::Sqlite {
        database: root.path().join("display.sqlite"),
        scope: DisplayScope::new("owner", "root", "root"),
    })
    .await;
}
