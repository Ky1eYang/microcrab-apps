use super::*;
use microcrab_transport::model_metadata::MetadataError;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct Source {
    value: Arc<MetadataSnapshot>,
    failed: AtomicBool,
    calls: AtomicUsize,
    changed: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl ModelMetadataManager for Source {
    fn snapshot(&self) -> Arc<MetadataSnapshot> {
        Arc::new(MetadataSnapshot {
            models: vec![],
            fetched_at_ms: 0,
            ..(*self.value).clone()
        })
    }
    fn find(&self, provider: &str, model: &str) -> Option<ModelMatch> {
        self.value.find(provider, model)
    }
    async fn refresh(&self) -> Result<Arc<MetadataSnapshot>, MetadataError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_one();
        tokio::task::yield_now().await;
        if self.failed.load(Ordering::SeqCst) {
            Err(MetadataError::RequestFailed)
        } else {
            Ok(self.value.clone())
        }
    }
}
fn configuration(path: &std::path::Path) -> Config {
    serde_json::from_value(json!({"data_dir":path,"provider":{"endpoint":"http://127.0.0.1:1/chat/completions","model":"gemini-test"},"available_models":[{"id":"gemini-test","name":"Test","provider_id":"configured"}],"auth":{"issuer":"http://127.0.0.1","authorization_endpoint":"http://127.0.0.1/authorize","token_endpoint":"http://127.0.0.1/token","jwks_uri":"http://127.0.0.1/jwks","client_id":"test","redirect_uri":"http://127.0.0.1/callback","allowed_origins":["http://127.0.0.1"],"jwt_secret":"test-key-at-least-thirty-two-characters"}})).unwrap()
}
fn source() -> Arc<Source> {
    Arc::new(Source { value: Arc::new(serde_json::from_value(json!({"version":1,"source":SOURCE,"fetched_at_ms":12345,"models":[{"id":"google/gemini-test","context_length":1000000,"top_provider":{"context_length":900000,"max_completion_tokens":64000},"pricing":{"prompt":"0.0000000000123456789","completion":"0.00000002"},"future_field":{"preserved":true}}]})).unwrap()), failed: AtomicBool::new(false), calls: AtomicUsize::new(0), changed: tokio::sync::Notify::new() })
}
#[tokio::test]
async fn refreshed_metadata_is_durable_and_failure_preserves_full_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let config = configuration(directory.path());
    let source = source();
    let metadata = ModelMetadata::with_source(&config, source.clone())
        .await
        .unwrap();
    assert!(metadata.snapshot().find("gateway", "gemini-test").is_none());
    let mut changes = metadata.changes();
    metadata.refresh().await.unwrap();
    assert!(changes.has_changed().unwrap());
    changes.borrow_and_update();
    let matched = metadata.snapshot().find("gateway", "gemini-test").unwrap();
    assert_eq!(capacity(&matched), Some(900000));
    let stored: Value = serde_json::from_slice(
        &tokio::fs::read(directory.path().join("model-metadata.json"))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        stored["models"][0]["pricing"]["prompt"],
        "0.0000000000123456789"
    );
    assert_eq!(stored["models"][0]["future_field"]["preserved"], true);
    source.failed.store(true, Ordering::SeqCst);
    assert!(metadata.refresh().await.is_err());
    assert!(!changes.has_changed().unwrap());
    assert_eq!(metadata.snapshot().fetched_at_ms, 12345);
    assert_eq!(metadata.status()["last_error"], "refresh_failed");
    let reopened = ModelMetadata::with_source(&config, source.clone())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .snapshot()
            .find("google", "gemini-test")
            .unwrap()
            .metadata,
        matched.metadata
    );
    assert_eq!(
        source.calls.load(Ordering::SeqCst),
        2,
        "reopen must only read local cache"
    );
    assert_eq!(
        reopened.choices(&config)[0]["context_window_tokens"],
        900000
    );
}
#[tokio::test]
async fn mixed_case_gateway_model_enriches_capacity_without_changing_route_identity() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = configuration(directory.path());
    config.provider.model = "GeMiNi-TeSt".into();
    config.available_models[0].id = config.provider.model.clone();
    let metadata = ModelMetadata::with_source(&config, source()).await.unwrap();
    metadata.refresh().await.unwrap();
    let choices = metadata.choices(&config);
    assert_eq!(choices[0]["id"], "GeMiNi-TeSt");
    assert_eq!(choices[0]["context_window_tokens"], 900000);
    assert_eq!(choices[0]["metadata_match"], "model_id_fallback");
    let record: microcrab_storage::SessionRecord = serde_json::from_value(json!({
        "version":1,"id":"s","owner":"owner","title":"Test","project_id":"project","project_root":".",
        "model_selection":{"provider_id":"configured","model":"GeMiNi-TeSt","variant":null},
        "permissions":[],"review":false,"revision":1,"archived":false,"created_at_ms":10,"updated_at_ms":10
    })).unwrap();
    let before = serde_json::to_vec(&record).unwrap();
    let mut statistics = json!({"model":{},"provider":{"latest_prompt":{"input_tokens":30648,"freshness":"current"}}});
    metadata.enrich_statistics(&record, &config, &mut statistics);
    assert_eq!(statistics["model"]["context_window_tokens"], 900000);
    assert_eq!(statistics["model"]["measured_headroom_tokens"], 869352);
    assert_eq!(statistics["model"]["context_window_source"], "openrouter");
    assert_eq!(serde_json::to_vec(&record).unwrap(), before);
    assert_eq!(
        config
            .resolve_selection("configured", "GeMiNi-TeSt", None)
            .unwrap()
            .provider
            .model,
        "GeMiNi-TeSt"
    );
}

#[tokio::test]
async fn configured_capacity_wins_and_stale_prompt_never_becomes_current_headroom() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = configuration(directory.path());
    config.available_models[0].context_window_tokens = Some(800000);
    let metadata = ModelMetadata::with_source(&config, source()).await.unwrap();
    metadata.refresh().await.unwrap();
    assert_eq!(
        metadata.choices(&config)[0]["context_window_tokens"],
        800000
    );
    let record = serde_json::from_value(json!({"version":1,"id":"s","owner":"owner","title":"Test","project_id":"project","project_root":".","model_selection":{"provider_id":"configured","model":"gemini-test","variant":null},"permissions":[],"review":false,"revision":1,"archived":false,"created_at_ms":10,"updated_at_ms":10})).unwrap();
    let mut value = json!({"model":{"context_window_tokens":null},"provider":{"latest_prompt":{"input_tokens":200,"freshness":"stale","freshness_reason":"context_changed_since_request"}}});
    metadata.enrich_statistics(&record, &config, &mut value);
    assert_eq!(value["model"]["context_window_tokens"], 800000);
    assert!(value["model"]["measured_headroom_tokens"].is_null());
    value["model"]["context_window_tokens"] = json!(800000);
    value["provider"]["latest_prompt"]["freshness"] = json!("current");
    metadata.enrich_statistics(&record, &config, &mut value);
    assert_eq!(value["model"]["measured_headroom_tokens"], 799800);
    assert_eq!(value["model"]["context_window_source"], "configured");
}
#[tokio::test]
async fn approximate_variant_capacity_enriches_choices_and_statistics_without_rewriting_selection()
{
    let directory = tempfile::tempdir().unwrap();
    let mut config = configuration(directory.path());
    config.provider.model = "gemini-3.8-flash-high".into();
    config.available_models[0].id = config.provider.model.clone();
    let mut value = (*source().value).clone();
    value.models = vec![
        serde_json::from_value(json!({
            "id":"google/gemini-3.8-flash", "context_length":1048576,
            "top_provider":{"context_length":1048576}
        }))
        .unwrap(),
    ];
    let source = Arc::new(Source {
        value: Arc::new(value),
        failed: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
        changed: tokio::sync::Notify::new(),
    });
    let metadata = ModelMetadata::with_source(&config, source).await.unwrap();
    metadata.refresh().await.unwrap();
    let choices = metadata.choices(&config);
    assert_eq!(choices[0]["id"], "gemini-3.8-flash-high");
    assert_eq!(choices[0]["context_window_tokens"], 1048576);
    assert_eq!(choices[0]["metadata_match"], "model_id_approximate");
    assert_eq!(choices[0]["metadata"]["id"], "google/gemini-3.8-flash");
    let record: microcrab_storage::SessionRecord = serde_json::from_value(json!({
        "version":1,"id":"s","owner":"owner","title":"Test","project_id":"project","project_root":".",
        "model_selection":{"provider_id":"configured","model":"gemini-3.8-flash-high","variant":null},
        "permissions":[],"review":false,"revision":1,"archived":false,"created_at_ms":10,"updated_at_ms":10
    })).unwrap();
    let mut statistics = json!({"model":{},"provider":{"latest_prompt":{"input_tokens":4096,"freshness":"current"}}});
    metadata.enrich_statistics(&record, &config, &mut statistics);
    assert_eq!(statistics["model"]["context_window_tokens"], 1048576);
    assert_eq!(statistics["model"]["context_window_source"], "openrouter");
    assert_eq!(
        statistics["model"]["metadata_match"],
        "model_id_approximate"
    );
    assert_eq!(statistics["model"]["measured_headroom_tokens"], 1044480);
    assert_eq!(
        record.model_selection.as_ref().unwrap().model,
        "gemini-3.8-flash-high"
    );
    let resolved = config
        .resolve_selection("configured", "gemini-3.8-flash-high", None)
        .unwrap();
    assert_eq!(resolved.provider.model, "gemini-3.8-flash-high");
}

#[tokio::test]
async fn overlapping_refreshes_share_published_cache_and_corrupt_cache_is_reported() {
    let directory = tempfile::tempdir().unwrap();
    let config = configuration(directory.path());
    tokio::fs::write(directory.path().join("model-metadata.json"), b"{broken")
        .await
        .unwrap();
    let source = source();
    let metadata = ModelMetadata::with_source(&config, source.clone())
        .await
        .unwrap();
    assert_eq!(metadata.status()["last_error"], "invalid_cached_metadata");
    let (a, b) = tokio::join!(metadata.refresh(), metadata.refresh());
    a.unwrap();
    b.unwrap();
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    metadata.close().await.unwrap();
}
#[tokio::test]
async fn concurrent_publications_keep_statistics_provenance_in_one_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let config = configuration(directory.path());
    let metadata = ModelMetadata::with_source(&config, source()).await.unwrap();
    let snapshots: Vec<_> = [100_000, 200_000]
        .into_iter()
        .map(|capacity| {
            Arc::new(
                serde_json::from_value::<MetadataSnapshot>(json!({
                    "version":1,"source":SOURCE,"fetched_at_ms":capacity,
                    "models":[{"id":"google/gemini-test","context_length":capacity}]
                }))
                .unwrap(),
            )
        })
        .collect();
    *metadata.current.write().unwrap() = snapshots[0].clone();
    let record = serde_json::from_value(json!({
        "version":1,"id":"s","owner":"owner","title":"Test","project_id":"project",
        "project_root":".","model_selection":{"provider_id":"configured","model":"gemini-test","variant":null},
        "permissions":[],"review":false,"revision":1,"archived":false,"created_at_ms":10,"updated_at_ms":10
    })).unwrap();
    let start = std::sync::Barrier::new(2);
    // Exercise the actual publication lock concurrently with the actual consumer.
    // Each snapshot encodes its provenance in its capacity so any mixed read fails.
    std::thread::scope(|threads| {
        threads.spawn(|| {
            start.wait();
            for index in 0..4_000 {
                *metadata.current.write().unwrap() = snapshots[index % 2].clone();
                std::thread::yield_now();
            }
        });
        start.wait();
        for _ in 0..4_000 {
            let mut statistics = json!({"model":{},"provider":{}});
            metadata.enrich_statistics(&record, &config, &mut statistics);
            assert_eq!(
                statistics["model"]["metadata"]["context_length"],
                statistics["model"]["metadata_fetched_at_ms"]
            );
        }
    });
}

#[tokio::test]
async fn periodic_owner_refreshes_and_shutdown_awaits_durable_publication() {
    let directory = tempfile::tempdir().unwrap();
    let config = configuration(directory.path());
    let source = source();
    let metadata = ModelMetadata::with_source(&config, source.clone())
        .await
        .unwrap();
    metadata.start_worker(Duration::from_millis(10));
    metadata.start_worker(Duration::from_millis(10)); // Starting again never adds another owner.
    tokio::time::timeout(Duration::from_secs(2), async {
        while source.calls.load(Ordering::SeqCst) < 2 {
            source.changed.notified().await;
        }
    })
    .await
    .unwrap();
    metadata.close().await.unwrap();
    assert_eq!(metadata.snapshot().fetched_at_ms, 12345);
    assert!(directory.path().join("model-metadata.json").is_file());
    assert!(metadata.worker.lock().unwrap().is_none());
}

#[tokio::test]
async fn dropping_metadata_owner_cancels_idle_updater_without_retaining_the_application() {
    let directory = tempfile::tempdir().unwrap();
    let config = configuration(directory.path());
    let source = source();
    let metadata = ModelMetadata::with_source(&config, source.clone())
        .await
        .unwrap();
    let weak = Arc::downgrade(&metadata);
    let cancel = metadata.cancel.clone();
    metadata.start_worker(Duration::from_secs(3600));
    // No yield before drop: the spawned worker must not own a strong reference.
    drop(metadata);
    assert!(weak.upgrade().is_none());
    assert!(cancel.is_cancelled());
    tokio::task::yield_now().await;
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    assert!(!directory.path().join("model-metadata.json").exists());
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn late_metadata_publication_updates_active_statistics_without_a_model_call() {
    use crate::manager::{Manager, commands};
    use microcrab_protocol::application::Command;
    let root = tempfile::tempdir().unwrap();
    let mut config = configuration(&root.path().join("data"));
    config.resolve(root.path()).unwrap();
    crate::project::seed_test_projects(
        &config.data_dir,
        json!([
            {"id":"project","name":"Project","root":root.path(),"owners":["owner"]}
        ]),
    );
    let manager = Manager::with_host(config, Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let create: Command = serde_json::from_value(json!({
        "v":1,"type":"command","request_id":"create","method":"session.create",
        "params":{"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}
    }))
    .unwrap();
    let created = commands::create(&manager, "owner", &create).await.unwrap();
    let managed = manager
        .active(
            manager
                .record("owner", created["id"].as_str().unwrap())
                .await
                .unwrap(),
            true,
        )
        .await
        .unwrap();
    let before = managed.session.context_measurement().unwrap();
    let revision = managed.record.lock().await.revision;
    assert!(managed.hub.snapshot()["statistics"]["model"]["context_window_tokens"].is_null());
    let mut events = managed.hub.subscribe().receiver;
    // Exercise the same durable publication boundary used after a fetch completes.
    let fetched = source().refresh().await.unwrap();
    manager
        .model_metadata
        .publish_snapshot(fetched)
        .await
        .unwrap();
    let update = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = events.recv().await.unwrap();
            if matches!(
                &event.event,
                crate::manager::SessionRuntimeEvent::StatisticsUpdated(_)
            ) {
                break event;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        update.event.payload()["model"]["context_window_tokens"],
        900000
    );
    assert_eq!(
        update.event.payload()["model"]["context_window_source"],
        "openrouter"
    );
    assert_eq!(
        managed.session.context_measurement().unwrap().fingerprint,
        before.fingerprint
    );
    assert_eq!(managed.record.lock().await.revision, revision);
    assert!(managed.ledger.total().input_tokens.is_none());
    assert_eq!(managed.ledger.total().samples, 0);
    manager.close().await.unwrap();
}

#[test]
fn refresh_policy_is_bounded() {
    let mut policy = ModelMetadataSettings::default();
    assert!(policy.validate().is_ok());
    policy.refresh_interval_secs = 0;
    assert!(policy.validate().is_err());
    policy = ModelMetadataSettings::default();
    policy.max_response_bytes = usize::MAX;
    assert!(policy.validate().is_err());
}
