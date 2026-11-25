use super::*;
use crate::manager::model;
use microcrab_creator::audit::{EffectiveRequest, Recorder, RecorderOptions};
use microcrab_session::context::Context;

fn config() -> Config {
    serde_json::from_value(json!({"storage":"file","data_dir":".data","provider":{"endpoint":"http://127.0.0.1:1/chat/completions","model":"test"},"available_models":[{"id":"test","name":"Test","provider_id":"configured","context_window_tokens":1000}],"auth":{"issuer":"http://127.0.0.1","authorization_endpoint":"http://127.0.0.1/authorize","token_endpoint":"http://127.0.0.1/token","jwks_uri":"http://127.0.0.1/jwks","client_id":"test","redirect_uri":"http://127.0.0.1/callback","allowed_origins":["http://127.0.0.1"],"jwt_secret":"test-key-at-least-thirty-two-characters"}})).unwrap()
}
fn record(config: &Config) -> SessionRecord {
    serde_json::from_value(json!({"version":1,"id":"s","owner":"owner","title":"Test","project_id":"project","project_root":".","agent":null,"mode":null,"model_selection":model::default_selection(config),"permissions":[],"initialization":"ask","review":false,"revision":1,"archived":false,"created_at_ms":10,"updated_at_ms":10})).unwrap()
}
fn attempt(measurement: &ContextMeasurement, record: &SessionRecord) -> AuditAttempt {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let hash = recorder
        .effective(EffectiveRequest {
            call_id: "c".into(),
            epoch: measurement.epoch,
            turn: Some(1),
            round: Some(1),
            parent_request: None,
            history_head: None,
            subagent: None,
            request_body: "sensitive-effective".into(),
        })
        .unwrap();
    recorder.publish(json!({"event":"request_prepared","call_id":"c","sent_request":"{\"messages\":[] }","started_at_unix_ms":100})).unwrap();
    recorder.publish(json!({"event":"round_completed","call_id":"c","started_at_unix_ms":100,"elapsed_ms":500,"outcome":"accepted","usage":{"input_tokens":200,"output_tokens":50,"samples":1},"context_fingerprint":measurement.fingerprint,"context_target":serde_json::to_string(record.model_selection.as_ref().unwrap()).unwrap()})).unwrap();
    let result = query_attempt_metadata(
        directory.path(),
        &AuditQuery {
            epoch: measurement.epoch + 1,
            turn: None,
            round: None,
            request_hash: Some(hash),
            history_head: None,
            subagent: None,
            call_id: None,
            tool_call_id: None,
        },
    )
    .unwrap();
    let attempt = result.attempts.into_iter().next().unwrap();
    assert!(attempt.sent_request.is_none());
    assert!(attempt.request_document.is_none());
    assert!(attempt.response.is_none());
    assert_eq!(attempt.epoch, measurement.epoch);
    attempt
}
#[test]
fn current_prompt_requires_exact_context_target_and_epoch_provenance() {
    let config = config();
    let record = record(&config);
    let context = Context::new("system".into(), vec![], 65536).unwrap();
    let mut measurement = context.measure().unwrap();
    let mut attempt = attempt(&measurement, &record);
    let usage = Usage::default();
    let current = project(
        &config,
        &record,
        &measurement,
        &usage,
        &usage,
        &[],
        Some(&attempt),
        Some(&attempt),
        Some(3),
        true,
    );
    assert_eq!(current["provider"]["latest_prompt"]["freshness"], "current");
    assert_eq!(current["model"]["measured_headroom_tokens"], 800);
    assert!(current["provider"]["timing"]["tokens_per_second"].is_null());
    assert_eq!(
        current["provider"]["timing"]["request_tokens_per_second"],
        100.0
    );
    assert!(current["provider"]["timing"]["generation_ms"].is_null());
    attempt.usage.as_mut().unwrap()["input_tokens"] = json!(1001);
    let over = project(
        &config,
        &record,
        &measurement,
        &usage,
        &usage,
        &[],
        Some(&attempt),
        Some(&attempt),
        None,
        true,
    );
    assert_eq!(
        over["model"]["headroom_reason"],
        "measured_input_exceeds_model_window"
    );
    assert!(over["model"]["measured_headroom_tokens"].is_null());
    attempt.usage.as_mut().unwrap()["input_tokens"] = json!(200);
    attempt.ended_at_ms = attempt.started_at_ms;
    assert!(
        project(
            &config,
            &record,
            &measurement,
            &usage,
            &usage,
            &[],
            Some(&attempt),
            Some(&attempt),
            None,
            true
        )["provider"]["timing"]["tokens_per_second"]
            .is_null()
    );
    attempt.ended_at_ms = Some(1);
    assert!(
        project(
            &config,
            &record,
            &measurement,
            &usage,
            &usage,
            &[],
            Some(&attempt),
            Some(&attempt),
            None,
            true
        )["provider"]["timing"]["elapsed_ms"]
            .is_null()
    );
    measurement.pending_epoch = true;
    let pending = project(
        &config,
        &record,
        &measurement,
        &usage,
        &usage,
        &[],
        Some(&attempt),
        Some(&attempt),
        None,
        true,
    );
    assert_eq!(pending["provider"]["latest_prompt"]["freshness"], "stale");
    assert!(pending["model"]["measured_headroom_tokens"].is_null());
    measurement.pending_epoch = false;
    attempt.context_target = Some("another model".into());
    let changed = project(
        &config,
        &record,
        &measurement,
        &usage,
        &usage,
        &[],
        Some(&attempt),
        Some(&attempt),
        None,
        true,
    );
    assert_eq!(
        changed["provider"]["latest_prompt"]["freshness_reason"],
        "model_selection_changed"
    );
    attempt.context_target = None;
    assert_eq!(
        project(
            &config,
            &record,
            &measurement,
            &usage,
            &usage,
            &[],
            Some(&attempt),
            Some(&attempt),
            None,
            true
        )["provider"]["latest_prompt"]["freshness"],
        "unknown"
    );
}
#[test]
fn measured_generation_rate_requires_a_positive_stream_interval_and_real_output_tokens() {
    let config = config();
    let record = record(&config);
    let measurement = Context::new("s".into(), vec![], 1024)
        .unwrap()
        .measure()
        .unwrap();
    let mut attempt = attempt(&measurement, &record);
    attempt.first_token_ms = Some(100);
    attempt.generation_ms = Some(200);
    attempt.timing_source = Some("transport_stream".into());
    let usage = Usage::default();
    let project_timing = |attempt: &AuditAttempt| {
        project(
            &config,
            &record,
            &measurement,
            &usage,
            &usage,
            &[],
            Some(attempt),
            Some(attempt),
            None,
            true,
        )["provider"]["timing"]
            .clone()
    };
    let timing = project_timing(&attempt);
    assert_eq!(timing["first_token_ms"], 100);
    assert_eq!(timing["generation_ms"], 200);
    assert_eq!(timing["tokens_per_second"], 250.0);
    assert_eq!(timing["request_tokens_per_second"], 100.0);
    assert_eq!(timing["rate_basis"], "generation_interval");
    assert_eq!(timing["timing_source"], "transport_stream");
    attempt
        .usage
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .remove("output_tokens");
    assert!(project_timing(&attempt)["tokens_per_second"].is_null());
    attempt.usage.as_mut().unwrap()["output_tokens"] = json!(0);
    assert_eq!(project_timing(&attempt)["tokens_per_second"], 0.0);
    attempt.generation_ms = Some(0);
    assert!(project_timing(&attempt)["tokens_per_second"].is_null());
}

#[tokio::test]
async fn selected_request_timing_is_not_replaced_by_its_measured_prompt_or_another_branch() {
    let config = config();
    let record = record(&config);
    let measurement = Context::new("s".into(), vec![], 1024)
        .unwrap()
        .measure()
        .unwrap();
    for outcome in ["unavailable", "accepted"] {
        let directory = tempfile::tempdir().unwrap();
        let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
        let effective = |call: &str, parent| EffectiveRequest {
            call_id: call.into(),
            epoch: 0,
            turn: Some(1),
            round: Some(1),
            parent_request: parent,
            history_head: None,
            subagent: None,
            request_body: "{}".into(),
        };
        let measured = recorder.effective(effective("measured", None)).unwrap();
        recorder.publish(json!({"event":"round_completed","call_id":"measured","outcome":"accepted","usage":{"input_tokens":123},"started_at_unix_ms":100,"elapsed_ms":500})).unwrap();
        let selected = recorder
            .effective(effective("selected", Some(measured.clone())))
            .unwrap();
        let usage = if outcome == "accepted" {
            json!({"output_tokens":9})
        } else {
            Value::Null
        };
        recorder.publish(json!({"event":"round_completed","call_id":"selected","outcome":outcome,"usage":usage,"started_at_unix_ms":1000,"elapsed_ms":150,"first_token_ms":40,"generation_ms":80,"timing_source":"transport_stream"})).unwrap();
        recorder.effective(effective("unrelated", None)).unwrap();
        recorder.publish(json!({"event":"round_completed","call_id":"unrelated","outcome":"accepted","usage":{"input_tokens":999,"output_tokens":999},"started_at_unix_ms":2000,"elapsed_ms":10})).unwrap();
        let statistics = Statistics::new(&config, "unused");
        for _ in 0..2 {
            let cache = statistics
                .read_attempt(directory.path().to_path_buf(), Some(selected.clone()), 0)
                .await
                .unwrap();
            assert_eq!(cache.attempt.as_ref().unwrap().request_hash, measured);
            assert_eq!(
                cache.timing_attempt.as_ref().unwrap().request_hash,
                selected
            );
            let empty = Usage::default();
            let value = project(
                &config,
                &record,
                &measurement,
                &empty,
                &empty,
                &[],
                cache.attempt.as_ref(),
                cache.timing_attempt.as_ref(),
                cache.sequence,
                cache.available,
            );
            assert_eq!(value["provider"]["latest_prompt"]["input_tokens"], 123);
            assert_eq!(value["provider"]["timing"]["call_id"], "selected");
            assert_eq!(value["provider"]["timing"]["first_token_ms"], 40);
            assert_eq!(value["provider"]["timing"]["generation_ms"], 80);
            if outcome == "accepted" {
                assert_eq!(value["provider"]["timing"]["tokens_per_second"], 112.5);
            } else {
                assert!(value["provider"]["timing"]["tokens_per_second"].is_null());
            }
        }
        let missing = statistics
            .read_attempt(directory.path().to_path_buf(), None, 0)
            .await
            .unwrap();
        assert!(missing.attempt.is_none());
        assert!(missing.timing_attempt.is_none());
    }
}

#[tokio::test]
async fn reported_cache_projection_is_paired_coverage_aware_and_reopens_without_rewriting() {
    let root = tempfile::tempdir().unwrap();
    let ledger = UsageLedger::open(Some(root.path())).unwrap();
    let aggregate = UsageLedger::open_named(Some(root.path()), "aggregate_usage.jsonl").unwrap();
    for sample in [
        Usage {
            input_tokens: Some(100),
            cache_read_tokens: Some(50),
            samples: 1,
            ..Default::default()
        },
        Usage {
            input_tokens: Some(900),
            samples: 1,
            ..Default::default()
        },
    ] {
        ledger.publish(sample.clone());
        aggregate.publish(sample);
    }
    ledger.flush().await.unwrap();
    aggregate.flush().await.unwrap();
    let main = ledger.snapshot();
    let all = aggregate.snapshot();
    assert!(main.total.cache_read_tokens.is_none());
    let mut value = json!({"usage":{"aggregate_cache_hit_rate":null}});
    reported_cache(&mut value, &main, &all).unwrap();
    assert_eq!(
        value["usage"]["aggregate_reported_cache"],
        json!({"scope":"reported_samples", "attempted_samples":2, "paired_samples":1,"input_tokens":100,"cache_read_tokens":50,"cache_hit_rate":0.5})
    );
    assert!(value["usage"]["aggregate_cache_hit_rate"].is_null());
    let before = std::fs::read(root.path().join("usage.jsonl")).unwrap();
    let restored = UsageLedger::read_snapshot_named(root.path(), "usage.jsonl").unwrap();
    assert_eq!(restored, main);
    let mut detached = json!({"usage":{"aggregate_cache_hit_rate":null}});
    let restored_all =
        UsageLedger::read_snapshot_named(root.path(), "aggregate_usage.jsonl").unwrap();
    reported_cache(&mut detached, &restored, &restored_all).unwrap();
    assert_eq!(detached, value);
    assert_eq!(
        std::fs::read(root.path().join("usage.jsonl")).unwrap(),
        before
    );
}

#[test]
fn usage_unknowns_remain_null_and_cache_ratios_require_a_valid_denominator() {
    let config = config();
    let record = record(&config);
    let measurement = Context::new("s".into(), vec![], 1024)
        .unwrap()
        .measure()
        .unwrap();
    let main = Usage {
        input_tokens: Some(100),
        cache_read_tokens: Some(25),
        samples: 1,
        provider_details: vec![json!({"secret":"not projected"})],
        ..Default::default()
    };
    let aggregate = Usage {
        input_tokens: None,
        samples: 2,
        ..Default::default()
    };
    let value = project(
        &config,
        &record,
        &measurement,
        &main,
        &aggregate,
        &[],
        None,
        None,
        None,
        false,
    );
    assert_eq!(value["usage"]["main_cache_hit_rate"], 0.25);
    assert!(value["usage"]["aggregate_cache_hit_rate"].is_null());
    assert!(value["usage"]["main"]["reasoning_tokens"].is_null());
    assert_eq!(value["usage"]["main"]["provider_details"], json!([]));
    assert_eq!(value["usage"]["aggregate"]["samples"], 2);
    assert!(
        cache_rate(&Usage {
            input_tokens: Some(0),
            cache_read_tokens: Some(0),
            ..Default::default()
        })
        .is_none()
    );
    assert!(
        cache_rate(&Usage {
            input_tokens: Some(2),
            cache_read_tokens: Some(3),
            ..Default::default()
        })
        .is_none()
    );
}
#[test]
fn operation_duration_uses_receipt_scope_and_freezes_at_completion() {
    let mut receipt: OperationRecord = serde_json::from_value(json!({"version":1,"id":"op","session_id":"s","owner":"owner","method":"session.compact","payload_hash":"hash","status":"running","result":null,"revision":1,"created_at_ms":100,"updated_at_ms":120})).unwrap();
    assert!(operation(&[receipt.clone()])["elapsed_ms"].is_null());
    receipt.status = "cancelled".into();
    receipt.updated_at_ms = 180;
    assert_eq!(operation(&[receipt])["elapsed_ms"], 80);
}
