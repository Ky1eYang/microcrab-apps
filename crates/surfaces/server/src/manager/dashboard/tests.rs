use super::*;
use crate::config::{Config, Storage};
use serde_json::json;

async fn projection_restarts_and_isolates_owners(storage: Storage) {
    let directory = tempfile::tempdir().unwrap();
    let mut config: Config =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    config.data_dir = directory.path().into();
    config.storage = storage;
    let dashboard = Dashboard::open(&config).await.unwrap();
    let route = Model {
        provider_id: "openai".into(),
        model: "gpt-example".into(),
        protocol: Some(Protocol::OpenaiCompatible),
    };
    let sink = SessionSink::new(dashboard.clone(), "../first-owner", "session");
    let child = sink.for_session("child");
    let auditor = child.wrap(&microcrab_auditor::NoopAuditor, &route);
    auditor.publish(
        Moment::RequestPrepared,
        AuditBody {
            call_id: Some("try-1".into()),
            sent_request: Some("private-prompt".into()),
            ..AuditBody::default()
        },
    );
    auditor.publish(
        Moment::RoundCompleted,
        AuditBody {
            call_id: Some("try-1".into()),
            outcome: Some(AuditOutcome::Unavailable),
            response_status: Some(503),
            response: Some("private-error-body".into()),
            ..AuditBody::default()
        },
    );
    auditor.publish(
        Moment::RoundCompleted,
        AuditBody {
            call_id: Some("try-2".into()),
            outcome: Some(AuditOutcome::Accepted),
            response_status: Some(200),
            response: Some("private-response".into()),
            usage: Some(Usage {
                samples: 1,
                input_tokens: Some(12),
                cache_read_tokens: Some(0),
                provider_details: vec![json!({"private-provider":"detail"})],
                ..Usage::default()
            }),
            ..AuditBody::default()
        },
    );
    dashboard.record_relay(
        "second-owner",
        "key",
        "=FORMULA()",
        "request",
        route,
        Terminal {
            outcome: Outcome::Cancelled,
            status_code: None,
            latency_ms: Some(32),
            ttft_ms: None,
            usage: None,
        },
    );
    let first = dashboard.snapshot("../first-owner").await.unwrap();
    assert_eq!(first.rows.len(), 2);
    assert!(
        first
            .rows
            .iter()
            .all(|row| row.source == Source::Session && row.source_id == "child")
    );
    let json = serde_json::to_string(&first.rows).unwrap();
    assert!(!json.contains("private-"));
    assert!(!json.contains("provider_details"));
    assert!(!json.contains("first-owner"));
    if storage == Storage::File {
        let persisted = std::fs::read_to_string(
            directory
                .path()
                .join("storage/owners")
                .join(owner_key("../first-owner"))
                .join("dashboard/usage-events.jsonl"),
        )
        .unwrap();
        assert!(!persisted.contains("private-"));
        assert!(persisted.contains("cache_read_tokens\":0"));
    }
    assert_eq!(
        dashboard.snapshot("second-owner").await.unwrap().rows[0].source,
        Source::Relay
    );
    assert!(
        dashboard
            .snapshot("third-owner")
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    dashboard.close().await.unwrap();
    let reopened = Dashboard::open(&config).await.unwrap();
    let rows = reopened.snapshot("../first-owner").await.unwrap();
    assert_eq!(rows.observed_since_ms, first.observed_since_ms);
    assert_eq!(
        rows.rows.iter().map(Event::key).collect::<Vec<_>>(),
        first.rows.iter().map(Event::key).collect::<Vec<_>>()
    );
    reopened.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn dashboard_file_durability_and_owner_isolation() {
    projection_restarts_and_isolates_owners(Storage::File).await;
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn dashboard_sqlite_durability_and_owner_isolation() {
    projection_restarts_and_isolates_owners(Storage::Sqlite).await;
}
