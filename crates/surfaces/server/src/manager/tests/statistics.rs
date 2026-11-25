//! Deterministic delayed-publication interleavings, without sleeps or HTTP work.
use super::*;
use crate::manager::statistics::PublicationWitness;

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn delayed_statistics_and_full_snapshots_cannot_cross_record_head_or_context_changes() {
    let client = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _root) = manager(client, "file").await;
    let create = command(
        "session.create",
        None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}),
    );
    let first = commands::create(&manager, "owner", &create).await.unwrap();
    let id = first["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let old_record = managed.record.lock().await.clone();
    let stale_statistics = PublicationWitness::capture(&managed, &old_record).unwrap();
    let stale_snapshot = PublicationWitness::capture(&managed, &old_record).unwrap();
    let value = managed
        .statistics
        .snapshot(&managed, &old_record, &[], &stale_statistics)
        .await
        .unwrap();
    let full = managed.hub.snapshot();
    // This is the model-metadata publication portion of the already-tested
    // set_model operation. The publication checker must not depend on Context
    // changing as a side effect of a model/metadata revision.
    {
        let mut record = managed.record.lock().await;
        record.revision += 1;
        record.model_selection.as_mut().unwrap().model = "other-model".into();
    }
    manager.refresh(&managed).await.unwrap();
    let canonical = managed.hub.snapshot();
    assert!(
        stale_statistics
            .publish(&managed, value, false)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        stale_snapshot
            .publish(&managed, full, true)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(managed.hub.snapshot(), canonical);
    let current = managed.record.lock().await.clone();
    let head_witness = PublicationWitness::capture(&managed, &current).unwrap();
    let value = managed
        .statistics
        .snapshot(&managed, &current, &[], &head_witness)
        .await
        .unwrap();
    let anchor = managed.journal.history_anchor();
    let old_head = anchor.lock().unwrap().request_head.clone();
    anchor.lock().unwrap().request_head = Some("a".repeat(64));
    assert!(
        head_witness
            .publish(&managed, value, false)
            .await
            .unwrap()
            .is_none()
    );
    anchor.lock().unwrap().request_head = old_head;
    let context_witness = PublicationWitness::capture(&managed, &current).unwrap();
    let value = managed
        .statistics
        .snapshot(&managed, &current, &[], &context_witness)
        .await
        .unwrap();
    managed
        .session
        .compact("new retained baseline", serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(managed.record.lock().await.revision, current.revision);
    assert!(
        context_witness
            .publish(&managed, value, false)
            .await
            .unwrap()
            .is_none()
    );
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn aba_head_and_context_calculation_stays_bound_to_captured_a_source() {
    use microcrab_creator::audit::{EffectiveRequest, Recorder, RecorderOptions};
    let client = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, root) = manager(client, "file").await;
    let create = command(
        "session.create",
        None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}),
    );
    let created = commands::create(&manager, "owner", &create).await.unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let record = managed.record.lock().await.clone();
    managed.session.checkpoint().await.unwrap();
    let measurement_a = managed.session.context_measurement().unwrap();
    // A separate real verified audit fixture supplies distinct causal roots,
    // without replacing the active Session's writer or its bound auditor.
    let mut config = manager.config.clone();
    config.data_dir = root.join("aba-statistics");
    let directory = config.data_dir.join("sessions").join(id);
    std::fs::create_dir_all(&directory).unwrap();
    let mut recorder = Recorder::open(&directory, RecorderOptions::default()).unwrap();
    let make = |call: &str| EffectiveRequest {
        call_id: call.into(),
        epoch: measurement_a.epoch,
        turn: Some(1),
        round: Some(1),
        parent_request: None,
        history_head: None,
        subagent: None,
        request_body: "{}".into(),
    };
    let head_a = recorder.effective(make("A")).unwrap();
    recorder.publish(serde_json::json!({"event":"round_completed","call_id":"A","outcome":"accepted","usage":{"input_tokens":111},"context_fingerprint":measurement_a.fingerprint,"context_target":serde_json::to_string(record.model_selection.as_ref().unwrap()).unwrap()})).unwrap();
    let head_b = recorder.effective(make("B")).unwrap();
    recorder.publish(serde_json::json!({"event":"round_completed","call_id":"B","outcome":"accepted","usage":{"input_tokens":222}})).unwrap();
    let anchor = managed.journal.history_anchor();
    let original_head = anchor.lock().unwrap().request_head.clone();
    anchor.lock().unwrap().request_head = Some(head_a.clone());
    let checkpoint = managed.session.checkpoint().await.unwrap();
    microcrab_session::Journal::commit(&managed.journal, checkpoint)
        .await
        .unwrap();
    let checkpoint_a = managed.journal.cursor().await.unwrap().checkpoint_id;
    let witness_a = PublicationWitness::capture(&managed, &record).unwrap();
    // Change both real selection sources before the asynchronous calculation.
    anchor.lock().unwrap().request_head = Some(head_b);
    managed
        .session
        .compact("branch B baseline", serde_json::json!({}))
        .await
        .unwrap();
    assert_ne!(
        managed.session.context_measurement().unwrap().fingerprint,
        measurement_a.fingerprint
    );
    let statistics = crate::manager::statistics::Statistics::new(&config, id);
    let value = statistics
        .snapshot(&managed, &record, &[], &witness_a)
        .await
        .unwrap();
    assert_eq!(value["context"]["fingerprint"], measurement_a.fingerprint);
    assert_eq!(value["provider"]["latest_prompt"]["request_hash"], head_a);
    assert_eq!(value["provider"]["latest_prompt"]["input_tokens"], 111);
    // A is selected again with a fresh display lineage. The old asynchronous
    // witness must not be reused, even though head and Context match again.
    let recover_a = managed
        .journal
        .prepare_travel(microcrab_storage::history::Travel::To {
            checkpoint_id: checkpoint_a,
        })
        .await
        .unwrap();
    managed.session.recover(recover_a).await.unwrap();
    assert_eq!(anchor.lock().unwrap().request_head.as_ref(), Some(&head_a));
    assert_eq!(
        managed.session.context_measurement().unwrap().fingerprint,
        measurement_a.fingerprint
    );
    assert!(
        witness_a
            .publish(&managed, value, false)
            .await
            .unwrap()
            .is_none()
    );
    let fresh = PublicationWitness::capture(&managed, &record).unwrap();
    let value = statistics
        .snapshot(&managed, &record, &[], &fresh)
        .await
        .unwrap();
    let published = fresh
        .publish(&managed, value, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(published["provider"]["latest_prompt"]["input_tokens"], 111);
    assert_eq!(
        published["context"]["fingerprint"],
        measurement_a.fingerprint
    );
    anchor.lock().unwrap().request_head = original_head;
    manager.close().await.unwrap();
}
