//! Incremental attach: dormant history availability is deferred, and an
//! ignored release probe reports the v0.1.14 latency metrics.
use super::*;

/// Zero-latency model with a fixed-size reply, as in the latency memo.
struct Instant {
    reply: String,
}
#[async_trait::async_trait]
impl microcrab_session::Client for Instant {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            provider_metadata: None,
            content: self.reply.clone(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}

async fn session_with_turns(storage: &str, turns: usize) -> (Arc<Manager>, String) {
    let model = Arc::new(Instant {
        reply: format!("REPLY-{}", "r".repeat(2000)),
    });
    let (manager, _) = manager(model, storage).await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let text = "u".repeat(2000);
    for turn in 0..turns {
        commands::submit(&manager, managed.clone(), &command("turn.submit", Some(&id),
            serde_json::json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text(&text)})))
            .await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let replies = serde_json::to_string(&managed.session.messages())
                    .unwrap()
                    .matches("REPLY-")
                    .count();
                if replies > turn && managed.hub.snapshot()["session"]["status"] == "idle" {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("instant turn settles");
    }
    (manager, id)
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn dormant_attach_answers_before_history_probe_and_patches_it() {
    let (manager, id) = session_with_turns("file", 2).await;
    let owner = Principal::for_test("owner");
    manager
        .command(
            &owner,
            &command("session.close", Some(&id), serde_json::json!({})),
        )
        .await
        .unwrap();
    let mut attached = manager.subscribe(&owner, &id).await.unwrap();
    assert!(attached.snapshot["history_available"].is_null());
    assert_eq!(attached.snapshot["messages"].as_array().unwrap().len(), 4);
    let patch = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let event = attached.receiver.recv().await.unwrap();
            if event.event.name() == "session.meta" {
                break event.event.payload().clone();
            }
        }
    })
    .await
    .expect("deferred history availability is published");
    assert!(patch["history_available"]["undo"].is_boolean());
    assert!(patch["history_available"]["redo"].is_boolean());
    drop(attached);
    // The probe resolved availability into the manifest-keyed projection, so
    // the next attach of the unchanged journal answers it inline.
    let cached = manager.subscribe(&owner, &id).await.unwrap();
    assert_eq!(
        cached.snapshot["history_available"],
        patch["history_available"]
    );
    assert_eq!(cached.snapshot["messages"].as_array().unwrap().len(), 4);
    drop(cached);
    // Without a resident projection (as after a restart) it is deferred again.
    manager.forget_dormant(&id);
    let cold = manager.subscribe(&owner, &id).await.unwrap();
    assert!(cold.snapshot["history_available"].is_null());
    drop(cold);
    // A non-attach inspection still computes it inline.
    let full = manager.inspect_owned("owner", &id).await.unwrap();
    assert_eq!(full["history_available"], patch["history_available"]);
    manager.close().await.unwrap();
}

fn percentile(samples: &mut [f64], p: f64) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    samples[((samples.len() as f64 - 1.0) * p).round() as usize]
}

/// `MC_PROBE_TURNS=200 cargo test --release -p microcrab-server --all-features
/// attach_latency_probe -- --ignored --nocapture`
#[cfg(feature = "storage-file")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn attach_latency_probe() {
    let turns = std::env::var("MC_PROBE_TURNS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(200);
    let (manager, id) = session_with_turns("file", turns).await;
    let owner = Principal::for_test("owner");
    let mut active = vec![];
    for _ in 0..9 {
        let started = std::time::Instant::now();
        let subscription = manager.subscribe(&owner, &id).await.unwrap();
        active.push(started.elapsed().as_secs_f64() * 1000.0);
        drop(subscription);
    }
    manager
        .command(
            &owner,
            &command("session.close", Some(&id), serde_json::json!({})),
        )
        .await
        .unwrap();
    let mut dormant = vec![];
    let mut probed = vec![];
    let mut wire = 0;
    for _ in 0..9 {
        let started = std::time::Instant::now();
        let mut subscription = manager.subscribe(&owner, &id).await.unwrap();
        dormant.push(started.elapsed().as_secs_f64() * 1000.0);
        let (_, initial) = super::super::publication::attach(
            &subscription.snapshot,
            &super::super::publication::Resume::default(),
        );
        wire = initial
            .iter()
            .map(|event| event.payload().to_string().len())
            .sum::<usize>()
            + serde_json::to_string(&super::super::publication::meta(&subscription.snapshot))
                .unwrap()
                .len();
        if subscription.snapshot["history_available"].is_null() {
            loop {
                let event = subscription.receiver.recv().await.unwrap();
                if event.event.name() == "session.meta" {
                    break;
                }
            }
        }
        probed.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    // Cold: the manifest-keyed projection cache is cleared before every attach
    // (as after a restart), so each sample opens, restores and projects.
    let mut cold = vec![];
    let mut cold_probed = vec![];
    for _ in 0..9 {
        manager.forget_dormant(&id);
        let started = std::time::Instant::now();
        let mut subscription = manager.subscribe(&owner, &id).await.unwrap();
        cold.push(started.elapsed().as_secs_f64() * 1000.0);
        assert!(subscription.snapshot["history_available"].is_null());
        loop {
            let event = subscription.receiver.recv().await.unwrap();
            if event.event.name() == "session.meta" {
                break;
            }
        }
        cold_probed.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    let mut list = vec![];
    for _ in 0..9 {
        let started = std::time::Instant::now();
        manager
            .command(
                &owner,
                &command("session.list", None, serde_json::json!({})),
            )
            .await
            .unwrap();
        list.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    // Repeat-attach of an unchanged dormant session with resume: wire bytes.
    let snapshot = manager.subscribe(&owner, &id).await.unwrap().snapshot;
    let resume = super::super::publication::Resume {
        history_lineage: super::super::publication::lineage(&snapshot).map(str::to_owned),
    };
    let (mode, events) = super::super::publication::attach(&snapshot, &resume);
    let resumed: usize = events
        .iter()
        .map(|event| event.payload().to_string().len())
        .sum();
    println!("metric turns {turns}");
    println!(
        "metric active_attach_ms p50={:.2} p95={:.2}",
        percentile(&mut active.clone(), 0.5),
        percentile(&mut active, 0.95)
    );
    println!(
        "metric dormant_attach_ms p50={:.2} p95={:.2}",
        percentile(&mut dormant.clone(), 0.5),
        percentile(&mut dormant, 0.95)
    );
    println!(
        "metric dormant_history_patch_ms p50={:.2} p95={:.2}",
        percentile(&mut probed.clone(), 0.5),
        percentile(&mut probed, 0.95)
    );
    println!(
        "metric dormant_cold_attach_ms p50={:.2} p95={:.2}",
        percentile(&mut cold.clone(), 0.5),
        percentile(&mut cold, 0.95)
    );
    println!(
        "metric dormant_cold_history_patch_ms p50={:.2} p95={:.2}",
        percentile(&mut cold_probed.clone(), 0.5),
        percentile(&mut cold_probed, 0.95)
    );
    println!(
        "metric session_list_ms p50={:.2}",
        percentile(&mut list, 0.5)
    );
    println!("metric attach_wire_bytes cold={wire} resume({mode})={resumed}");
    manager.close().await.unwrap();
}
