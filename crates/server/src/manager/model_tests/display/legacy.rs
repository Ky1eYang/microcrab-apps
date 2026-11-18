//! Mixed legacy/new archives retain selected historical bytes without inventing graph placement.
use super::*;
use microcrab_creator::display::DisplayRecorder;
use microcrab_storage::display::ArchiveRecord;
use microcrab_transport::{ModelObservation, ModelRecording, ObservationState};

async fn mixed_legacy(storage: Storage) {
    let root = tempfile::tempdir().unwrap();
    let a = thinking_provider("unused").await;
    let b = provider(10).await;
    let mut config = config(root.path(), &a, &b);
    config.storage = storage;
    let manager = Manager::with_host(config.clone(), Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let managed = create(&manager).await;
    let id = managed.record.lock().await.id.clone();
    let checkpoint = managed.journal.cursor().await.unwrap().checkpoint_id;
    let source = super::super::super::display::source(
        &manager.config,
        "owner",
        &id,
        &id,
        manager.config.data_dir.join("sessions").join(&id),
    );
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    // These are append-only historical-format fixtures, never rewritten current bytes.
    let mut writer = source.open().unwrap();
    let start = writer.revision().unwrap();
    for (sequence, (attempt, event)) in (start..).zip([
        (
            "legacy-selected",
            json!({"phase":"started","request_identity":"legacy-request"}),
        ),
        (
            "legacy-selected",
            json!({"phase":"reasoning_delta","stream_offset":0,"text":"retained historical thought"}),
        ),
        (
            "legacy-selected",
            json!({"phase":"invalidated","reason":"cancelled","reasoning_bytes":"retained historical thought".len(),"text_bytes":0,"archive_complete":true}),
        ),
        (
            "legacy-selected",
            json!({"phase":"admission_binding","request_identity":"legacy-request","checkpoint_id":checkpoint}),
        ),
        (
            "legacy-abandoned",
            json!({"phase":"started","request_identity":"abandoned-request"}),
        ),
        (
            "legacy-abandoned",
            json!({"phase":"reasoning_delta","stream_offset":0,"text":"excluded historical sibling"}),
        ),
        (
            "legacy-abandoned",
            json!({"phase":"invalidated","reason":"cancelled","reasoning_bytes":"excluded historical sibling".len(),"text_bytes":0,"archive_complete":true}),
        ),
    ]) {
        let record = ArchiveRecord {
            sequence: sequence + 1,
            attempt_id: attempt.into(),
            event,
        };
        writer.append(sequence, &record).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);
    let recorder = DisplayRecorder::open(
        source.clone(),
        Arc::new(|_| json!({})),
        Arc::new(|_| {}),
        Arc::new(|cause| panic!("display save failed: {cause}")),
    )
    .await
    .unwrap();
    for state in [
        ObservationState::Started,
        ObservationState::ReasoningDelta {
            text: "new selected thought".into(),
        },
    ] {
        recorder
            .record(ModelObservation {
                call_id: "new-attempt".into(),
                epoch: 0,
                turn: Some(1),
                round: Some(1),
                subagent: None,
                state,
            })
            .await
            .unwrap();
    }
    recorder.close().await.unwrap();
    let manager = Manager::with_host(config, Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let page = super::super::super::display::query(
        &manager,
        "owner",
        &id,
        serde_json::from_value(json!({"limit":128})).unwrap(),
    )
    .await
    .unwrap();
    let thoughts: Vec<_> = page["records"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["event"]["phase"] == "reasoning_delta")
        .map(|row| row["event"]["text"].as_str().unwrap())
        .collect();
    assert_eq!(
        thoughts,
        ["retained historical thought", "new selected thought"]
    );
    assert_eq!(page["graph"]["complete"], false);
    assert_eq!(page["legacy_ordering"], "incomplete");
    assert_eq!(page["graph"]["nodes"].as_array().unwrap().len(), 1);
    assert!(manager.registry.lock().unwrap().is_empty());
    assert!(a.wire.requests.lock().unwrap().is_empty());
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_mixed_legacy_preserves_selected_thought_and_excludes_historical_sibling() {
    mixed_legacy(Storage::File).await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_mixed_legacy_preserves_selected_thought_and_excludes_historical_sibling() {
    mixed_legacy(Storage::Sqlite).await;
}
