//! Recovery fault fixtures manipulate private files only after the owning store
//! is dropped, except tests deliberately injecting an outside-owner write fault.
use super::*;
use serde_json::json;
use std::io::{Read, Seek, SeekFrom};

const WORKSPACE: &str = "receipt-test-workspace";
fn receipt(id: &str) -> OperationReceipt {
    OperationReceipt {
        operation_id: id.into(),
        payload_digest: "a".repeat(64),
        scope: BindingScope::project("s".into(), "b".into(), "p".into()),
        state: ReceiptState::Intent,
    }
}
fn done(id: &str) -> OperationReceipt {
    let mut receipt = receipt(id);
    receipt.state = ReceiptState::Completed(Ok(json!({ "result": id })));
    receipt
}
fn declaration(receipt: OperationReceipt, phase: Phase) -> Record {
    Record {
        version: 1,
        phase,
        receipt,
        original_reason: None,
        consumption_key: None,
    }
}
fn open(dir: &Path) -> ReceiptStore {
    ReceiptStore::open(dir, WORKSPACE).expect("open receipt store")
}
fn append_without_checkpoint(store: &mut ReceiptStore, record: &Record) -> BodyRef {
    let (bytes, reference) =
        storage::encode_frame(record, store.index.watermark, store.index.sequence + 1).unwrap();
    storage::append_frame(&mut store.log, store.index.watermark, &bytes).unwrap();
    reference
}
fn corrupt_byte(path: &Path, offset: u64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}

#[test]
fn unfinished_intent_is_never_replayed_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    store.intent(&receipt("op")).unwrap();
    assert!(matches!(
        store.get("op").unwrap().unwrap().state,
        ReceiptState::Intent
    ));
    drop(store);
    let mut recovered = open(dir.path());
    assert!(matches!(
        recovered.get("op").unwrap().unwrap().state,
        ReceiptState::Indeterminate
    ));
    assert!(recovered.intent(&receipt("op")).is_err());
}

#[test]
fn complete_suffix_is_folded_after_durable_log_before_index_crash() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    store.intent(&receipt("op")).unwrap();
    let reference = append_without_checkpoint(&mut store, &declaration(done("op"), Phase::Done));
    drop(store);
    let mut recovered = open(dir.path());
    assert_eq!(recovered.index.watermark, reference.end().unwrap());
    assert!(matches!(
        recovered.get("op").unwrap().unwrap().state,
        ReceiptState::Completed(Ok(_))
    ));
    assert_eq!(recovered.len(), 1);
}

#[test]
fn partial_header_after_valid_watermark_is_discarded_and_not_an_intent() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    store.intent(&receipt("existing")).unwrap();
    let watermark = store.index.watermark;
    let (bytes, _) = storage::encode_frame(
        &declaration(receipt("torn"), Phase::Intent),
        watermark,
        store.index.sequence + 1,
    )
    .unwrap();
    storage::append_frame(&mut store.log, watermark, &bytes[..17]).unwrap();
    drop(store);
    let mut recovered = open(dir.path());
    assert_eq!(recovered.log.metadata().unwrap().len(), watermark);
    assert!(recovered.fence("torn").is_none());
    assert!(matches!(
        recovered.get("existing").unwrap().unwrap().state,
        ReceiptState::Indeterminate
    ));
}

#[test]
fn indexless_rebuild_discards_only_a_verified_header_with_incomplete_body() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    store.intent(&receipt("existing")).unwrap();
    let watermark = store.index.watermark;
    let (bytes, _) = storage::encode_frame(
        &declaration(receipt("torn"), Phase::Intent),
        watermark,
        store.index.sequence + 1,
    )
    .unwrap();
    storage::append_frame(
        &mut store.log,
        watermark,
        &bytes[..storage::FRAME_BYTES as usize + 4],
    )
    .unwrap();
    let index_path = store.dir.join("fence.index");
    drop(store);
    std::fs::remove_file(index_path).unwrap();
    let recovered = open(dir.path());
    assert_eq!(recovered.log.metadata().unwrap().len(), watermark);
    assert!(recovered.fence("existing").is_some());
    assert!(recovered.fence("torn").is_none());
}

#[test]
fn indexless_partial_header_has_no_provable_boundary_and_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    let watermark = store.index.watermark;
    storage::append_frame(&mut store.log, watermark, b"MCRREC01").unwrap();
    let index_path = store.dir.join("fence.index");
    drop(store);
    std::fs::remove_file(index_path).unwrap();
    assert!(ReceiptStore::open(dir.path(), WORKSPACE).is_err());
    assert_eq!(
        std::fs::metadata(dir.path().join("receipts/active.log"))
            .unwrap()
            .len(),
        watermark + 8
    );
}

#[test]
fn complete_checksum_bad_tail_is_never_discarded_even_after_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    store.intent(&receipt("op")).unwrap();
    let reference = append_without_checkpoint(&mut store, &declaration(done("op"), Phase::Done));
    let path = store.dir.join("active.log");
    drop(store);
    corrupt_byte(&path, reference.offset + storage::FRAME_BYTES + 5);
    let size = std::fs::metadata(&path).unwrap().len();
    assert!(ReceiptStore::open(dir.path(), WORKSPACE).is_err());
    assert_eq!(std::fs::metadata(path).unwrap().len(), size);
}

#[test]
fn committed_bad_header_length_and_checksum_fail_closed() {
    for offset_in_frame in [16, 56, storage::FRAME_BYTES + 5] {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        store.intent(&receipt("op")).unwrap();
        let reference = store.index.anchor.unwrap();
        let path = store.dir.join("active.log");
        drop(store);
        corrupt_byte(&path, reference.offset + offset_in_frame);
        assert!(ReceiptStore::open(dir.path(), WORKSPACE).is_err());
    }
}

#[test]
fn checksum_valid_but_oversized_length_and_invalid_phase_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    let (mut bytes, _) = storage::encode_frame(
        &declaration(receipt("op"), Phase::Intent),
        store.index.watermark,
        1,
    )
    .unwrap();
    bytes[16..24].copy_from_slice(&(MAX_BODY_BYTES as u64 + 1).to_le_bytes());
    let checksum = storage::hash(&bytes[..56]);
    bytes[56..88].copy_from_slice(&checksum);
    storage::append_frame(&mut store.log, store.index.watermark, &bytes).unwrap();
    drop(store);
    assert!(ReceiptStore::open(dir.path(), WORKSPACE).is_err());

    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    // Correct framing and checksums cannot turn an invalid phase/state into a
    // recoverable terminal record.
    append_without_checkpoint(&mut store, &declaration(done("op"), Phase::Intent));
    drop(store);
    assert!(ReceiptStore::open(dir.path(), WORKSPACE).is_err());
}

#[test]
fn index_mismatch_rebuilds_log_and_never_trusts_a_forged_success_projection() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    store.intent(&receipt("op")).unwrap();
    let mut bad = store.index.clone();
    bad.entries.get_mut("op").unwrap().phase = Phase::Done;
    storage::publish_index(&store.dir, &bad).unwrap();
    drop(store);
    let mut recovered = open(dir.path());
    assert_eq!(recovered.fence("op").unwrap().phase, Phase::Intent);
    assert!(matches!(
        recovered.get("op").unwrap().unwrap().state,
        ReceiptState::Indeterminate
    ));
}

#[test]
fn corrupt_index_rebuilds_but_missing_committed_log_bytes_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    store.intent(&receipt("op")).unwrap();
    store.complete(&done("op")).unwrap();
    let index_path = store.dir.join("fence.index");
    drop(store);
    corrupt_byte(&index_path, 0);
    let recovered = open(dir.path());
    assert_eq!(recovered.fence("op").unwrap().phase, Phase::Done);
    let end = recovered.index.watermark;
    recovered.log.set_len(end - 1).unwrap();
    recovered.log.sync_all().unwrap();
    drop(recovered);
    assert!(ReceiptStore::open(dir.path(), WORKSPACE).is_err());
}

#[test]
fn normal_open_and_metadata_do_not_scan_historical_bodies() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    store.intent(&receipt("old")).unwrap();
    store.complete(&done("old")).unwrap();
    let historical = store.fence("old").unwrap().body.unwrap();
    store.intent(&receipt("new")).unwrap();
    let path = store.dir.join("active.log");
    drop(store);
    corrupt_byte(&path, historical.offset + storage::FRAME_BYTES + 7);
    // The checked tail is intact. Historical corruption is intentionally found
    // on the selected lazy read, not claimed to be detected during fast open.
    let mut recovered = open(dir.path());
    assert_eq!(recovered.body_reads, 0);
    assert_eq!(recovered.len(), 2);
    assert!(matches!(
        recovered.get("new").unwrap().unwrap().state,
        ReceiptState::Indeterminate
    ));
    assert_eq!(recovered.body_reads, 1);
    assert!(recovered.get("old").is_err());
    assert!(recovered.ready().is_err());
}

#[test]
fn original_unknown_reason_is_separate_immutable_and_survives_settlement() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    let intent = receipt("op");
    let reason = WireError::new(ErrorCode::Timeout, "original owner timeout");
    store.intent(&intent).unwrap();
    store.save_unknown(&intent, reason.clone()).unwrap();
    let reason_ref = store.fence("op").unwrap().reason.unwrap();
    let watermark = store.index.watermark;
    store.save_unknown(&intent, reason.clone()).unwrap();
    assert_eq!(store.index.watermark, watermark);
    assert!(
        store
            .save_unknown(&intent, WireError::new(ErrorCode::Io, "replacement"))
            .is_err()
    );
    let mut failed = intent.clone();
    failed.state = ReceiptState::FailedUnknown {
        reason: reason.clone(),
        effects_may_have_occurred: true,
    };
    store.settle(&failed).unwrap();
    assert_ne!(store.fence("op").unwrap().body, Some(reason_ref));
    assert_eq!(store.fence("op").unwrap().reason, Some(reason_ref));
    drop(store);
    let mut recovered = open(dir.path());
    assert_eq!(
        recovered.unknown_reason(&intent).unwrap(),
        Some(reason.clone())
    );
    assert!(matches!(recovered.get("op").unwrap().unwrap().state,
        ReceiptState::FailedUnknown { reason: actual, effects_may_have_occurred: true } if actual == reason));
    let mut foreign = intent;
    foreign.scope = BindingScope::project("s".into(), "b".into(), "foreign".into());
    assert!(recovered.unknown_reason(&foreign).is_err());
}

#[test]
fn suffix_settlement_cannot_change_original_reason_even_with_valid_checksums() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    let mut failed = receipt("op");
    store.intent(&failed).unwrap();
    store
        .save_unknown(&failed, WireError::new(ErrorCode::Timeout, "original"))
        .unwrap();
    failed.state = ReceiptState::FailedUnknown {
        reason: WireError::new(ErrorCode::Io, "replacement"),
        effects_may_have_occurred: true,
    };
    append_without_checkpoint(&mut store, &declaration(failed, Phase::Failed));
    drop(store);
    assert!(ReceiptStore::open(dir.path(), WORKSPACE).is_err());
}

#[test]
fn legacy_migration_keeps_bytes_and_indexless_log_never_reimports_stale_files() {
    let dir = tempfile::tempdir().unwrap();
    let old = receipt("old");
    let original = WireError::new(ErrorCode::Timeout, "legacy owner timeout");
    legacy::save_fixture(dir.path(), &old, "intent");
    legacy::save_reason_fixture(dir.path(), &old, original.clone());
    let before = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect::<Vec<_>>();
    let mut store = open(dir.path());
    assert_eq!(store.unknown_reason(&old).unwrap(), Some(original));
    store.intent(&receipt("new")).unwrap();
    store.complete(&done("new")).unwrap();
    let index_path = store.dir.join("fence.index");
    drop(store);
    for (path, bytes) in before {
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
    std::fs::remove_file(index_path).unwrap();
    let mut recovered = open(dir.path());
    assert_eq!(recovered.len(), 2);
    assert!(matches!(
        recovered.get("new").unwrap().unwrap().state,
        ReceiptState::Completed(Ok(_))
    ));
    assert!(matches!(
        recovered.get("old").unwrap().unwrap().state,
        ReceiptState::Indeterminate
    ));
}

#[test]
fn interrupted_import_temp_is_ignored_and_only_complete_log_is_published() {
    let dir = tempfile::tempdir().unwrap();
    legacy::save_fixture(dir.path(), &receipt("old"), "intent");
    let receipts = dir.path().join("receipts");
    storage::ensure_dir(&receipts).unwrap();
    let unfinished = receipts.join(".import-interrupted.tmp");
    create_durable(&unfinished, b"partial import").unwrap();
    let mut recovered = open(dir.path());
    assert_eq!(recovered.len(), 1);
    assert!(recovered.get("old").unwrap().is_some());
    assert_eq!(std::fs::read(unfinished).unwrap(), b"partial import");
}

#[test]
fn bad_legacy_reason_prevents_publishing_a_partial_migration() {
    let dir = tempfile::tempdir().unwrap();
    let old = receipt("old");
    legacy::save_fixture(dir.path(), &old, "intent");
    let mut foreign = old;
    foreign.payload_digest = "b".repeat(64);
    legacy::save_reason_fixture(
        dir.path(),
        &foreign,
        WireError::new(ErrorCode::Io, "wrong digest"),
    );
    assert!(ReceiptStore::open(dir.path(), WORKSPACE).is_err());
    assert!(!dir.path().join("receipts/active.log").exists());
    assert!(!dir.path().join("receipts/fence.index").exists());
}

#[test]
fn capacity_counts_distinct_retained_ids_and_does_not_block_existing_completion() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    let mut index = store.index.clone();
    store.log.seek(SeekFrom::Start(index.watermark)).unwrap();
    // One batch fixture avoids 4096 unrelated filesystem checkpoint syncs. The
    // actual new-ID and completion paths below use the public store operations.
    for id in 0..MAX_RECEIPTS {
        ReceiptStore::import_record(
            &mut store.log,
            &mut index,
            declaration(receipt(&format!("op-{id}")), Phase::Intent),
        )
        .unwrap();
    }
    store.log.sync_all().unwrap();
    storage::publish_index(&store.dir, &index).unwrap();
    drop(store);
    let mut recovered = open(dir.path());
    assert_eq!(recovered.len(), MAX_RECEIPTS);
    assert_eq!(
        recovered.intent(&receipt("overflow")).unwrap_err().code,
        ErrorCode::Limit
    );
    recovered.complete(&done("op-0")).unwrap();
    assert_eq!(recovered.len(), MAX_RECEIPTS);
    assert!(matches!(
        recovered.get("op-0").unwrap().unwrap().state,
        ReceiptState::Completed(Ok(_))
    ));
}

#[test]
fn checkpoint_publication_failure_poisons_owner_without_publishing_memory_success() {
    for completing in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        if completing {
            store.intent(&receipt("op")).unwrap();
        }
        let path = store.dir.join("fence.index");
        let saved = store.dir.join("previous.index");
        std::fs::rename(&path, &saved).unwrap();
        std::fs::create_dir(&path).unwrap();
        let result = if completing {
            store.complete(&done("op"))
        } else {
            store.intent(&receipt("op"))
        };
        assert!(result.is_err());
        assert!(store.ready().is_err());
        assert!(store.intent(&receipt("next")).is_err());
        if completing {
            assert_eq!(store.fence("op").unwrap().phase, Phase::Intent);
        } else {
            assert!(store.fence("op").is_none());
        }
        drop(store);
        std::fs::remove_dir(&path).unwrap();
        std::fs::rename(saved, path).unwrap();
        let mut recovered = open(dir.path());
        let state = recovered.get("op").unwrap().unwrap().state;
        if completing {
            assert!(matches!(state, ReceiptState::Completed(Ok(_))));
        } else {
            assert!(matches!(state, ReceiptState::Indeterminate));
        }
        assert!(recovered.fence("next").is_none());
    }
}

#[tokio::test]
async fn concurrent_effect_owners_publish_noninterleaved_frames() {
    let dir = tempfile::tempdir().unwrap();
    let store = std::sync::Arc::new(tokio::sync::Mutex::new(open(dir.path())));
    let mut tasks = Vec::new();
    for number in 0..16 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            let id = format!("concurrent-{number}");
            store.lock().await.intent(&receipt(&id)).unwrap();
            // Effect work happens without the receipt mutex and may finish in a
            // different order from intent admission.
            tokio::task::yield_now().await;
            store.lock().await.complete(&done(&id)).unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(store.lock().await.len(), 16);
    drop(store);
    let mut recovered = open(dir.path());
    assert_eq!(recovered.index.sequence, 32);
    for number in 0..16 {
        assert!(matches!(
            recovered
                .get(&format!("concurrent-{number}"))
                .unwrap()
                .unwrap()
                .state,
            ReceiptState::Completed(Ok(_))
        ));
    }
}

#[cfg(unix)]
#[test]
fn linked_receipt_storage_and_legacy_files_fail_closed() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    symlink(outside.path(), dir.path().join("receipts")).unwrap();
    assert!(ReceiptStore::open(dir.path(), WORKSPACE).is_err());
    let dir = tempfile::tempdir().unwrap();
    let old = receipt("old");
    legacy::save_fixture(dir.path(), &old, "intent");
    let path = std::fs::read_dir(dir.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let target = outside.path().join("intent.json");
    std::fs::rename(&path, &target).unwrap();
    symlink(target, path).unwrap();
    assert!(ReceiptStore::open(dir.path(), WORKSPACE).is_err());
}

#[test]
fn output_scope_survives_unknown_receipt_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    let original = receipt("consumption");
    store
        .intent_scoped(&original, Some("shell:7".into()))
        .unwrap();
    store
        .save_unknown(
            &original,
            WireError::new(ErrorCode::Io, "retained output delivery failed"),
        )
        .unwrap();
    drop(store);
    let store = open(dir.path());
    let fence = store.fence("consumption").unwrap();
    assert_eq!(fence.consumption_key.as_deref(), Some("shell:7"));
    assert!(fence.phase.unresolved());
}
