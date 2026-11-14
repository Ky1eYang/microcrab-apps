use super::*;
use crate::manager::model;
use microcrab_creator::audit::{EffectiveRequest, Recorder, RecorderOptions};
use microcrab_storage::audit::AuditScope;

#[tokio::test]
async fn sqlite_statistics_requery_committed_sequence_and_replay_detached_consumption() {
    let root = tempfile::tempdir().unwrap();
    let config: Config = serde_json::from_value(json!({"storage":"sqlite","data_dir":root.path(),"provider":{"endpoint":"http://127.0.0.1:1/chat/completions","model":"test"},"auth":{"issuer":"http://127.0.0.1","authorization_endpoint":"http://127.0.0.1/authorize","token_endpoint":"http://127.0.0.1/token","jwks_uri":"http://127.0.0.1/jwks","client_id":"test","redirect_uri":"http://127.0.0.1/callback","allowed_origins":["http://127.0.0.1"],"jwt_secret":"test-key-at-least-thirty-two-characters"}})).unwrap();
    let record: SessionRecord = serde_json::from_value(json!({"version":1,"id":"root","owner":"owner","title":"Test","project_id":"project","project_root":".","agent":null,"mode":null,"model_selection":model::default_selection(&config),"permissions":[],"review":false,"revision":1,"archived":false,"created_at_ms":10,"updated_at_ms":10})).unwrap();
    let statistics = Statistics::new(&config, "root").scoped("owner");
    let source = statistics.audit_source();
    let mut recorder = Recorder::open_source(source, RecorderOptions::default()).unwrap();
    let head = recorder
        .effective(EffectiveRequest {
            call_id: "call".into(),
            epoch: 0,
            turn: Some(1),
            round: Some(1),
            parent_request: None,
            history_head: None,
            subagent: None,
            request_body: "{}".into(),
        })
        .unwrap();
    recorder.publish(json!({"event":"request_prepared","call_id":"call","sent_request":"{}","started_at_unix_ms":100})).unwrap();
    let pending = statistics
        .read_attempt(statistics.directory.clone(), Some(head.clone()), 0)
        .await
        .unwrap();
    assert_eq!(pending.sequence, Some(3));
    assert!(pending.timing_attempt.unwrap().usage.is_none());
    recorder.publish(json!({"event":"round_completed","call_id":"call","outcome":"accepted","usage":{"input_tokens":7,"output_tokens":0,"samples":1},"started_at_unix_ms":100,"elapsed_ms":10})).unwrap();
    let committed = statistics
        .read_attempt(statistics.directory.clone(), Some(head.clone()), 0)
        .await
        .unwrap();
    assert_eq!(committed.sequence, Some(4));
    assert_eq!(
        committed.attempt.as_ref().unwrap().usage.as_ref().unwrap()["output_tokens"],
        0
    );
    let database = root.path().join("storage.sqlite");
    let scope = AuditScope::new("owner", "root", "root");
    let main = UsageLedger::open_sqlite(&database, scope.clone(), "usage.jsonl").unwrap();
    let aggregate = UsageLedger::open_sqlite(&database, scope, "aggregate_usage.jsonl").unwrap();
    main.publish(Usage {
        input_tokens: Some(7),
        output_tokens: Some(0),
        samples: 1,
        ..Default::default()
    });
    aggregate.publish(Usage {
        input_tokens: Some(17),
        output_tokens: Some(4),
        samples: 2,
        ..Default::default()
    });
    main.flush().await.unwrap();
    aggregate.flush().await.unwrap();
    let measurement = microcrab_session::context::Context::new("system".into(), vec![], 1024)
        .unwrap()
        .measure()
        .unwrap();
    let detached = statistics
        .detached(&record, &measurement, Some(head), &[], None)
        .await
        .unwrap();
    assert_eq!(detached["usage"]["main"]["input_tokens"], 7);
    assert_eq!(detached["usage"]["aggregate"]["input_tokens"], 17);
    assert!(!root.path().join("sessions/root/audit.jsonl").exists());
    assert!(!root.path().join("sessions/root/usage.jsonl").exists());
}
