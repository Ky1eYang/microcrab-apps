//! Source-level integration contracts for owned drivers, durable receipts and
//! native restore. Parent owns execution with the coherent workspace gate.
mod attach_latency;
mod auto_naming_queue;
mod citations;
mod chronology;
mod compacted_display;
mod compaction;
mod configuration;
mod context_budget;
mod deletion;
mod errors;
mod fork;
mod history;
mod images;
mod inbox;
mod inbox_mutations;
mod live_attach;
mod permission_review;
mod prompt_templates;
mod question_images;
mod retry;
mod retry_buffered;
mod retry_lifecycle;
mod retry_projection;
mod round_limit;
mod send_admission_receipt;
mod session_watch;
mod statistics;
mod stop_causes;
mod stream_failure;
mod task_output;
mod tool_policy;
mod v018;
mod variables;

use super::*;
use microcrab_auditor::{Auditor, VariableView};
use microcrab_protocol::{Response, Usage, application::Command};
use microcrab_transport::Content;
use std::sync::atomic::AtomicUsize;
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;
struct Model {
    calls: AtomicUsize,
    entered: Notify,
    release: Semaphore,
}
#[async_trait::async_trait]
impl microcrab_session::Client for Model {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        let permit = tokio::select! {_ = token.cancelled()=>return Err(microcrab_transport::Error::Cancelled),permit=self.release.acquire()=>permit.map_err(|_|microcrab_transport::Error::Cancelled)?};
        permit.forget();
        Ok(Response {
            provider_metadata: None,
            content: "canonical answer".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}
struct ConcurrentModel {
    calls: AtomicUsize,
    entered: Notify,
    release: [Semaphore; 2],
    fail_rename: bool,
}
#[async_trait::async_trait]
impl microcrab_session::Client for ConcurrentModel {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        let permit = tokio::select! {
            _ = token.cancelled() => return Err(microcrab_transport::Error::Cancelled),
            permit = self.release[call].acquire() => permit.map_err(|_| microcrab_transport::Error::Cancelled)?,
        };
        permit.forget();
        if call == 0 && self.fail_rename {
            return Err(microcrab_transport::Error::ProviderStatus { status: 500 });
        }
        Ok(Response {
            provider_metadata: None,
            content: if call == 0 {
                "Delayed title"
            } else {
                "Primary answer"
            }
            .into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}
fn command(method: &str, id: Option<&str>, params: Value) -> Command {
    serde_json::from_value(serde_json::json!({"v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),"method":method,"session_id":id,"params":params})).unwrap()
}
async fn manager(
    model: Arc<dyn microcrab_session::Client>,
    storage: &str,
) -> (Arc<Manager>, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!("microcrab-manager-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut config:Config=serde_json::from_value(serde_json::json!({"data_dir":root.join(".data"),"storage":storage,"provider":{"endpoint":"http://127.0.0.1:1/v1/chat/completions","model":"test","timeout_ms":100},"auto_rename":false,"auth":{"issuer":"http://127.0.0.1:9900","authorization_endpoint":"http://127.0.0.1:9900/authorize","token_endpoint":"http://127.0.0.1:9900/token","jwks_uri":"http://127.0.0.1:9900/jwks","client_id":"test","redirect_uri":"http://127.0.0.1:4096/api/auth/callback","allowed_origins":["http://127.0.0.1:4096"],"jwt_secret":"test-key-at-least-thirty-two-characters"}})).unwrap();
    config.resolve(&root).unwrap();
    crate::project::seed_test_projects(
        &config.data_dir,
        serde_json::json!([
            {"id":"project","name":"Project","root":root,"owners":["owner"]}
        ]),
    );
    (Manager::with_client(config, model).await.unwrap(), root)
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn owned_turn_outlives_subscriber_and_duplicate_operation_never_replays() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _root) = manager(model.clone(), "file").await;
    let create = command(
        "session.create",
        None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}),
    );
    let first = commands::create(&manager, "owner", &create).await.unwrap();
    let again = commands::create(&manager, "owner", &create).await.unwrap();
    assert_eq!(first, again);
    let id = first["id"].as_str().unwrap();
    assert_eq!(
        manager.record("intruder", id).await.unwrap_err().code,
        "not_found"
    );
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let subscription = managed.hub.subscribe();
    drop(subscription);
    let operation = uuid::Uuid::new_v4().to_string();
    let submit = command(
        "turn.submit",
        Some(id),
        serde_json::json!({"operation_id":operation,"input":microcrab_session::SessionInput::text("hello")}),
    );
    let accepted = commands::submit(&manager, managed.clone(), &submit)
        .await
        .unwrap();
    assert_eq!(accepted["status"], "accepted");
    model.entered.notified().await;
    let replayed = commands::submit(&manager, managed.clone(), &submit)
        .await
        .unwrap();
    assert_eq!(replayed["id"], operation);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    model.release.add_permits(1);
    if let Some(driver) = managed.worker.lock().await.take() {
        driver.await.unwrap();
    }
    let receipt = manager
        .directory
        .get_operation("owner", &operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.status, "completed");
    assert_eq!(receipt.result.unwrap()["content"], "canonical answer");
    assert!(
        managed.hub.snapshot()["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["text"] == "canonical answer")
    );
    let config = manager.config.clone();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let reopened = Manager::with_client(config, model.clone()).await.unwrap();
    let restored = reopened
        .active(reopened.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        restored.hub.snapshot()["messages"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    reopened.close().await.unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn archive_seal_preserves_turn_settlement_and_latest_user_metadata() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let operation = uuid::Uuid::new_v4().to_string();
    commands::submit(&manager, managed.clone(), &command("turn.submit", Some(&id),
        serde_json::json!({"operation_id":operation,"input":microcrab_session::SessionInput::text("wait")}))).await.unwrap();
    model.entered.notified().await;
    let future_time = now().saturating_add(60_000);
    {
        let _gate = managed.commands.lock().await;
        let mut record = managed.record.lock().await;
        let mut next = record.clone();
        next.title = "User title".into();
        next.revision = 1;
        next.updated_at_ms = future_time;
        *record = manager.directory.replace("owner", 0, next).await.unwrap();
    }
    let archived = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        commands::close_session(&manager, &managed, "owner", &id, Some(1)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(archived["archived"], true);
    assert_eq!(archived["title"], "User title");
    assert_eq!(archived["revision"], 3);
    assert_eq!(archived["updated_at_ms"], future_time);
    let receipt = manager
        .directory
        .get_operation("owner", &operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.status, "cancelled");
    assert_eq!(receipt.updated_at_ms, future_time);
    assert!(!receipt.result.unwrap()["journal_cursor"].is_null());
    assert!(!manager.registry.lock().unwrap().contains_key(&id));
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    manager.close().await.unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn naming_token_cancels_admitted_naming_and_keeps_the_claim_nonrepeatable() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    let owner = managed.clone();
    let directory = manager.directory.clone();
    let instance = manager.instance.clone();
    let naming = tokio::spawn(async move {
        super::naming::run(
            owner,
            directory,
            "mock-model".into(),
            microcrab_session::SessionInput::text("title"),
            instance,
        )
        .await;
    });
    model.entered.notified().await;
    managed.close_token.cancel();
    assert!(!managed.naming_token.is_cancelled());
    managed.naming_token.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(10), naming)
        .await
        .unwrap()
        .unwrap();
    let claim = microcrab_storage::history::content_hash(&format!("{id}:auto-rename")).unwrap();
    let receipt = manager
        .directory
        .get_operation("owner", &claim)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.status, "interrupted");
    assert_eq!(managed.record.lock().await.title, "New session");
    // Helper cancellation leaves foreground checkpointing available; the
    // independent naming signal also preempts any fresh helper admission.
    managed.session.checkpoint().await.unwrap();
    super::naming::run(
        managed,
        manager.directory.clone(),
        "mock-model".into(),
        microcrab_session::SessionInput::text("again"),
        manager.instance.clone(),
    )
    .await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        manager
            .directory
            .get_operation("owner", &claim)
            .await
            .unwrap(),
        Some(receipt)
    );
    manager.close().await.unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn foreground_turn_and_naming_complete_independently() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        // Cover both completion orders and an isolated optional-helper failure.
        for (rename_first, fail_rename) in [(false, false), (true, false), (false, true)] {
            let model = Arc::new(ConcurrentModel {
                calls: AtomicUsize::new(0),
                entered: Notify::new(),
                release: [Semaphore::new(0), Semaphore::new(0)],
                fail_rename,
            });
            let (manager, _) = manager(model.clone(), "file").await;
            let created = commands::create(&manager, "owner", &command("session.create", None,
                serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
            let id = created["id"].as_str().unwrap();
            let managed = manager.active(manager.record("owner", id).await.unwrap(), true).await.unwrap();
            super::naming::start(managed.clone(), manager.directory.clone(), "test".into(),
                microcrab_session::SessionInput::text("title"), manager.instance.clone()).await;
            model.entered.notified().await;
            let claim = microcrab_storage::history::content_hash(&format!("{id}:auto-rename")).unwrap();
            let operation = uuid::Uuid::new_v4().to_string();
            let gate = managed.commands.lock().await;
            let accepted = commands::submit(&manager, managed.clone(), &command("turn.submit", Some(id),
                serde_json::json!({"operation_id":operation,"input":microcrab_session::SessionInput::text("continue")}))).await.unwrap();
            assert_eq!(accepted["status"], "accepted");
            drop(gate);
            model.entered.notified().await;
            assert_eq!(model.calls.load(Ordering::SeqCst), 2);
            assert_eq!(manager.directory.get_operation("owner", &claim).await.unwrap().unwrap().status, "running");
            if rename_first {
                model.release[0].add_permits(1);
                super::naming::finish(&managed).await;
                assert_eq!(managed.hub.operation().as_deref(), Some(operation.as_str()));
                assert_eq!(managed.hub.snapshot()["session"]["status"], "running");
                assert_eq!(managed.hub.snapshot()["session"]["title"], "Delayed title");
            }
            model.release[1].add_permits(1);
            let driver = managed.worker.lock().await.take().unwrap();
            driver.await.unwrap();
            assert_eq!(manager.directory.get_operation("owner", &operation).await.unwrap().unwrap().status, "completed");
            assert!(managed.hub.snapshot()["messages"].as_array().unwrap().iter()
                .any(|message| message["text"] == "Primary answer"));
            if !rename_first {
                assert_eq!(managed.record.lock().await.title, "New session");
                assert!(managed.record.lock().await.revision > 0);
                assert_eq!(manager.directory.get_operation("owner", &claim).await.unwrap().unwrap().status, "running");
                model.release[0].add_permits(1);
                super::naming::finish(&managed).await;
            }
            let expected_title = if fail_rename { "New session" } else { "Delayed title" };
            assert_eq!(manager.record("owner", id).await.unwrap().title, expected_title);
            assert_eq!(managed.hub.snapshot()["session"]["title"], expected_title);
            assert_eq!(managed.hub.snapshot()["session"]["status"], "idle");
            let receipt = manager.directory.get_operation("owner", &claim).await.unwrap().unwrap();
            assert_eq!(receipt.status, if fail_rename { "failed" } else { "completed" });
            super::naming::run(managed.clone(), manager.directory.clone(), "test".into(),
                microcrab_session::SessionInput::text("again"), manager.instance.clone()).await;
            assert_eq!(model.calls.load(Ordering::SeqCst), 2);
            assert_eq!(manager.directory.get_operation("owner", &claim).await.unwrap().unwrap(), receipt);
            manager.close().await.unwrap();
        }
    }).await.expect("a foreground turn and optional naming must complete independently");
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn manual_default_title_suppresses_late_naming_before_task_registration() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (initial, _) = manager(model.clone(), "file").await;
    let mut config = initial.config.clone();
    config.auto_rename = true;
    initial.close().await.unwrap();
    let manager = Manager::with_client(config, model.clone()).await.unwrap();
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    manager
        .command(
            &Principal::for_test("owner"),
            &command(
                "session.rename",
                Some(id),
                serde_json::json!({"title":"New session","expected_revision":0}),
            ),
        )
        .await
        .unwrap();
    let claim = microcrab_storage::history::content_hash(&format!("{id}:auto-rename")).unwrap();
    let receipt = manager
        .directory
        .get_operation("owner", &claim)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.result.as_ref().unwrap()["skipped"], "custom_title");
    super::naming::start(
        managed.clone(),
        manager.directory.clone(),
        "test".into(),
        microcrab_session::SessionInput::text("delayed admission"),
        manager.instance.clone(),
    )
    .await;
    assert!(managed.naming.lock().await.is_none());
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    let config = manager.config.clone();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let reopened = Manager::with_client(config, model.clone()).await.unwrap();
    let restored = reopened
        .active(reopened.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    super::naming::run(
        restored,
        reopened.directory.clone(),
        "test".into(),
        microcrab_session::SessionInput::text("after restart"),
        reopened.instance.clone(),
    )
    .await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        reopened
            .directory
            .get_operation("owner", &claim)
            .await
            .unwrap()
            .unwrap(),
        receipt
    );
    reopened.close().await.unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn naming_waiting_for_command_gate_can_be_joined_by_its_holder() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let model = Arc::new(Model {
            calls: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Semaphore::new(0),
        });
        let (manager, _) = manager(model.clone(), "file").await;
        let created = commands::create(&manager, "owner", &command("session.create", None,
            serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
        let id = created["id"].as_str().unwrap();
        let managed = manager.active(manager.record("owner", id).await.unwrap(), true).await.unwrap();
        super::naming::start(managed.clone(), manager.directory.clone(), "test".into(),
            microcrab_session::SessionInput::text("title"), manager.instance.clone()).await;
        model.entered.notified().await;
        let gate = managed.commands.lock().await;
        model.release.add_permits(1);
        // Wait for actual title publication entry while this caller owns the gate.
        super::naming::wait_for_title(&managed).await;
        super::naming::stop(&managed).await.unwrap();
        assert_eq!(managed.record.lock().await.title, "New session");
        drop(gate);
        manager.close().await.unwrap();
    }).await.expect("preemption must cancel queued title publication before joining it");
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn project_metadata_edits_preserve_identity_and_deletion_removes_sessions() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model, "file").await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
    let id = created["id"].as_str().unwrap();
    let original = manager.record("owner", id).await.unwrap();
    let principal = Principal::for_test("owner");
    let renamed = manager
        .command(
            &principal,
            &command(
                "project.rename",
                None,
                serde_json::json!({"project_id":"project","name":"My custom name"}),
            ),
        )
        .await
        .unwrap();
    assert_eq!(renamed["id"], "project");
    assert_eq!(renamed["name"], "My custom name");
    assert_eq!(
        manager.record("owner", id).await.unwrap().project_root,
        original.project_root
    );
    let listed = manager
        .command(
            &principal,
            &command("project.list", None, serde_json::json!({})),
        )
        .await
        .unwrap();
    assert_eq!(listed["items"][0]["name"], "My custom name");
    let deleted = manager
        .command(
            &principal,
            &command(
                "project.delete",
                None,
                serde_json::json!({"project_id":"project"}),
            ),
        )
        .await
        .unwrap();
    assert_eq!(deleted["deleted"], true);
    assert_eq!(
        deleted["deleted_session_ids"],
        serde_json::json!([original.id.clone()])
    );
    assert!(
        manager
            .command(
                &principal,
                &command("project.list", None, serde_json::json!({}))
            )
            .await
            .unwrap()["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    // Project deletion destroys its sessions and their bytes, not the project record.
    assert_eq!(
        manager.record("owner", id).await.unwrap_err().code,
        "not_found"
    );
    let project = manager.projects.find("project").await.unwrap();
    assert!(project.archived);
    assert_eq!(project.name, "My custom name");
    assert_eq!(project.owners, vec!["owner"]);
    let listed = manager
        .command(
            &principal,
            &command("session.list", None, serde_json::json!({})),
        )
        .await
        .unwrap();
    assert!(
        !listed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == id),
        "a deleted session is no longer listed"
    );
    assert_eq!(
        manager.record("intruder", id).await.unwrap_err().code,
        "not_found"
    );
    assert!(!manager.allows_project(&principal, "project"));
    assert!(manager.project_host("owner", "project").await.is_err());
    assert!(manager.active(original, true).await.is_err());
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inactive_journal_reads_subscriptions_and_activation_share_one_live_hub() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let record = manager.record("owner", &id).await.unwrap();
    let managed = manager.active(record.clone(), true).await.unwrap();
    commands::close_session(&manager, &managed, "owner", &id, None)
        .await
        .unwrap();
    drop(managed);
    // Reading dormant history must not allocate active-session capacity.
    for _ in 0..3 {
        manager.inspect(&record).await.unwrap();
        manager.inspect_messages(&record).await.unwrap();
    }
    assert!(manager.registry.lock().unwrap().is_empty());
    let barrier = Arc::new(tokio::sync::Barrier::new(25));
    let mut readers = tokio::task::JoinSet::new();
    for index in 0..24 {
        let owner = manager.clone();
        let record = record.clone();
        let barrier = barrier.clone();
        readers.spawn(async move {
            barrier.wait().await;
            match index % 4 {
                0 => owner.inspect(&record).await.map(|_| None),
                1 => owner.inspect_messages(&record).await.map(|_| None),
                2 => owner.inspect_subscription(&record).await.map(Some),
                _ => owner.active(record, true).await.map(|_| None),
            }
        });
    }
    barrier.wait().await;
    let mut subscriptions = vec![];
    let mut errors = vec![];
    while let Some(result) =
        tokio::time::timeout(std::time::Duration::from_secs(10), readers.join_next())
            .await
            .expect("same-session inspection/activation must not deadlock")
    {
        match result.unwrap() {
            Ok(Some(subscription)) => subscriptions.push(subscription),
            Ok(None) => {}
            Err(error) => errors.push(error.code),
        }
    }
    assert!(
        errors.is_empty(),
        "concurrent same-session requests failed: {errors:?}"
    );
    assert_eq!(subscriptions.len(), 6);
    assert_eq!(manager.registry.lock().unwrap().len(), 1);
    let active = manager.active(record, true).await.unwrap();
    let generation = active.hub.subscribe().generation;
    active.hub.publish(super::SessionRuntimeEvent::Session(
        serde_json::json!({"same_session":true}),
    ));
    for mut subscription in subscriptions {
        assert_eq!(
            subscription.generation, generation,
            "subscription was orphaned during activation"
        );
        let mut received = false;
        while let Ok(event) = subscription.receiver.try_recv() {
            received |= matches!(&event.event, super::SessionRuntimeEvent::Session(_));
        }
        assert!(
            received,
            "subscriber missed the activated session's live event"
        );
    }
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    manager.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn dormant_public_subscription_finishes_before_activation_and_keeps_live_hub() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model.clone(), "file").await;
    let created = commands::create(
        &manager,
        "owner",
        &command(
            "session.create",
            None,
            serde_json::json!({
                "project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()
            }),
        ),
    )
    .await
    .unwrap();
    let id = created["id"].as_str().unwrap();
    let record = manager.record("owner", id).await.unwrap();
    let managed = manager.active(record.clone(), true).await.unwrap();
    commands::close_session(&manager, &managed, "owner", id, None)
        .await
        .unwrap();
    drop(managed);
    let principal = crate::auth::Principal::for_test("owner");
    let mut subscription = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        manager.subscribe(&principal, id),
    )
    .await
    .expect("dormant public subscription must not recursively lock journal admission")
    .unwrap();
    assert_eq!(subscription.snapshot["session"]["id"], id);
    assert!(manager.registry.lock().unwrap().is_empty());
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    let active = manager.active(record, true).await.unwrap();
    assert_eq!(subscription.generation, active.hub.subscribe().generation);
    active
        .hub
        .publish(super::SessionRuntimeEvent::Session(serde_json::json!({})));
    assert!(
        std::iter::from_fn(|| subscription.receiver.try_recv().ok())
            .any(|event| matches!(&event.event, super::SessionRuntimeEvent::Session(_)))
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    manager.close().await.unwrap();
}

#[test]
fn browser_input_paths_urls_and_unknown_parameters_are_rejected() {
    for source in [
        serde_json::json!({"type":"path","path":"secret.png"}),
        serde_json::json!({"type":"url","url":"http://127.0.0.1/private"}),
    ] {
        let value = serde_json::json!({"parts":[{"type":"image","source":source}]});
        let input: microcrab_session::SessionInput = serde_json::from_value(value).unwrap();
        assert!(commands::validate_input(&input).is_err());
    }
    let invalid = command(
        "turn.submit",
        Some("session"),
        serde_json::json!({"operation_id":"x","input":microcrab_session::SessionInput::text("hi"),"unexpected":true}),
    );
    assert!(commands::parse::<commands::Submit>(&invalid).is_err());
}
