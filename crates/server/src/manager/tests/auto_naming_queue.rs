//! Real command paths and durable naming evidence; the parent owns execution gates.
mod races;
use super::*;
use microcrab_auditor::{AuditBody, HookContext, Moment};
use microcrab_protocol::{InputPart, SessionInput};
use serde_json::json;

struct NamingModel {
    primary: AtomicUsize,
    renames: AtomicUsize,
    entered: Semaphore,
    release: Semaphore,
    rename_entered: Semaphore,
    rename_release: Semaphore,
    primary_token: std::sync::Mutex<Option<CancellationToken>>,
    seeds: std::sync::Mutex<Vec<String>>,
    fail_primary: bool,
    fail_naming: bool,
}
impl NamingModel {
    fn new(fail_primary: bool, fail_naming: bool) -> Arc<Self> {
        Arc::new(Self {
            primary: AtomicUsize::new(0),
            renames: AtomicUsize::new(0),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            rename_entered: Semaphore::new(0),
            rename_release: Semaphore::new(0),
            primary_token: std::sync::Mutex::new(None),
            seeds: std::sync::Mutex::new(vec![]),
            fail_primary,
            fail_naming,
        })
    }
}
#[async_trait::async_trait]
impl microcrab_session::Client for NamingModel {
    async fn complete(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let rename = content.subagent.as_deref() == Some("rename");
        let index = if rename {
            self.renames.fetch_add(1, Ordering::SeqCst)
        } else {
            self.primary.fetch_add(1, Ordering::SeqCst)
        };
        let call = format!("{}-{index}", if rename { "rename" } else { "primary" });
        auditor
            .intervene(
                Moment::RequestPrepared,
                HookContext {
                    epoch: content.epoch,
                    turn: content.turn_id,
                    round: content.round,
                    subagent: content.subagent.clone(),
                    call_id: Some(call.clone()),
                    request_body: Some("{}"),
                    max_request_bytes: Some(1024),
                    variables,
                    payload: Default::default(),
                },
            )
            .await
            .map_err(|_| microcrab_transport::Error::Transport)?;
        auditor.publish(
            Moment::RequestPrepared,
            AuditBody {
                epoch: content.epoch,
                turn_id: content.turn_id,
                round: content.round,
                subagent: content.subagent.clone(),
                call_id: Some(call.clone()),
                sent_request: Some("{}".into()),
                ..Default::default()
            },
        );
        if rename {
            let [microcrab_session::context::ContentItem::User { parts, .. }] =
                content.items.as_slice()
            else {
                panic!("naming must have only its trigger user message");
            };
            assert!(matches!(parts.as_slice(), [InputPart::Text { text }] if text == "begin"));
            let seed = content
                .system
                .split_once("<untrusted_session_input>\n")
                .unwrap()
                .1
                .strip_suffix("\n</untrusted_session_input>\n")
                .unwrap();
            self.seeds.lock().unwrap().push(seed.to_owned());
            self.rename_entered.add_permits(1);
        } else {
            *self.primary_token.lock().unwrap() = Some(token.clone());
            self.entered.add_permits(1);
        }
        let release = if rename {
            &self.rename_release
        } else {
            &self.release
        };
        tokio::select! {
            _ = token.cancelled() => return Err(microcrab_transport::Error::Cancelled),
            permit = release.acquire() => permit.unwrap().forget(),
        }
        if !rename && self.fail_primary {
            // Ordinary provider failure preserves the Session for retry. A
            // Protocol error instead quarantines it and invalidates helpers.
            return Err(microcrab_transport::Error::ProviderStatus { status: 500 });
        }
        if rename && self.fail_naming {
            return Err(microcrab_transport::Error::Protocol(
                "fixture rejection".into(),
            ));
        }
        let response = Response {
            provider_metadata: None,
            content: if rename {
                "Queued session title"
            } else {
                "Primary answer"
            }
            .into(),
            tool_calls: vec![],
            usage: Usage::default(),
        };
        auditor.publish(
            Moment::RoundCompleted,
            AuditBody {
                epoch: content.epoch,
                turn_id: content.turn_id,
                round: content.round,
                subagent: content.subagent.clone(),
                call_id: Some(call),
                response_status: Some(200),
                outcome: Some(microcrab_auditor::AuditOutcome::Accepted),
                payload: microcrab_auditor::HookPayload::RoundCompleted {
                    response: Some(response.clone()),
                    status: Some(200),
                    usage: Some(response.usage.clone()),
                },
                ..Default::default()
            },
        );
        Ok(response)
    }
}
async fn permit(semaphore: &Semaphore) {
    tokio::time::timeout(std::time::Duration::from_secs(5), semaphore.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}
async fn fixture(model: Arc<NamingModel>, storage: &str) -> (Arc<Manager>, String, Arc<Managed>) {
    let (initial, _) = manager(model.clone(), storage).await;
    let mut config = initial.config.clone();
    config.auto_rename = true;
    initial.close().await.unwrap();
    let manager = Manager::with_client(config, model).await.unwrap();
    let created = manager
        .command(
            &Principal::for_test("owner"),
            &command(
                "session.create",
                None,
                json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}),
            ),
        )
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    (manager, id, managed)
}
fn insert(method: &str, id: &str, input: SessionInput) -> Command {
    command(
        method,
        Some(id),
        json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":input}),
    )
}
async fn claim(manager: &Manager, id: &str) -> Option<microcrab_storage::OperationRecord> {
    let operation = microcrab_storage::history::content_hash(&format!("{id}:auto-rename")).unwrap();
    manager
        .directory
        .get_operation("owner", &operation)
        .await
        .unwrap()
}
async fn idle(managed: &Managed) {
    let mut activity = managed.session.subscribe_activity();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if !activity.borrow().active && activity.borrow().outcome_sequence > 0 {
                break;
            }
            activity.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}
async fn rename_audit(manager: &Manager, id: &str) -> microcrab_creator::audit::AuditQueryResult {
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    managed.auditor.flush().await.unwrap();
    super::super::audit::query(
        manager,
        "owner",
        id,
        serde_json::from_value(json!({
            "epoch":0,"subagent":"rename","internal_assistants":true
        }))
        .unwrap(),
    )
    .await
    .unwrap()
}

async fn successful_commands(storage: &str) {
    for method in ["turn.queue", "turn.steer", "turn.submit"] {
        let model = NamingModel::new(false, false);
        let (manager, id, managed) = fixture(model.clone(), storage).await;
        let owner = Principal::for_test("owner");
        let first = insert(
            method,
            &id,
            SessionInput {
                parts: vec![
                    InputPart::Text {
                        text: "admitted first".into(),
                    },
                    InputPart::Text {
                        text: "second paragraph".into(),
                    },
                ],
                variables: Default::default(),
            },
        );
        let accepted = manager.command(&owner, &first).await.unwrap();
        assert_eq!(
            accepted["status"],
            if method == "turn.submit" {
                "accepted"
            } else {
                "completed"
            }
        );
        // Both provider barriers remain closed: naming cannot depend on a main
        // model completion, including idle steer that has no foreground turn.
        permit(&model.rename_entered).await;
        assert_eq!(claim(&manager, &id).await.unwrap().status, "running");
        assert_eq!(
            model.seeds.lock().unwrap().as_slice(),
            &["admitted first\nsecond paragraph"]
        );
        if method == "turn.steer" {
            assert_eq!(model.primary.load(Ordering::SeqCst), 0);
            let inbox = managed.session.inbox_snapshot();
            let item = &inbox.items[0];
            manager.command(&owner, &command("inbox.update", Some(&id), json!({
                "operation_id":uuid::Uuid::new_v4().to_string(),
                "expected_inbox_id":inbox.inbox_id,"item_id":item.id,"expected_revision":item.revision,
                "input":SessionInput::text("edited after durable admission")
            }))).await.unwrap();
        } else {
            permit(&model.entered).await;
        }
        manager
            .command(&owner, &command("session.interrupt", Some(&id), json!({})))
            .await
            .unwrap();
        manager
            .command(
                &owner,
                &insert("turn.queue", &id, SessionInput::text("later draft")),
            )
            .await
            .unwrap();
        drop(managed.hub.subscribe());
        // Actual Host suspension cancels foreground work, never the naming lane.
        manager.suspend_project("project").await;
        assert!(managed.suspended.load(Ordering::Acquire));
        assert_eq!(model.renames.load(Ordering::SeqCst), 1);
        assert_eq!(claim(&manager, &id).await.unwrap().status, "running");
        model.rename_release.add_permits(1);
        super::super::naming::finish(&managed).await;
        assert_eq!(
            manager
                .directory
                .get("owner", &id)
                .await
                .unwrap()
                .unwrap()
                .title,
            "Queued session title"
        );
        assert_eq!(
            model.seeds.lock().unwrap().as_slice(),
            &["admitted first\nsecond paragraph"]
        );
        let receipt = claim(&manager, &id).await.unwrap();
        assert_eq!(receipt.status, "completed");
        let config = manager.config.clone();
        manager.close().await.unwrap();
        drop(managed);
        drop(manager);
        let reopened = Manager::with_client(config, model.clone()).await.unwrap();
        let restored = reopened
            .active(reopened.record("owner", &id).await.unwrap(), true)
            .await
            .unwrap();
        super::super::naming::run(
            restored,
            reopened.directory.clone(),
            "test".into(),
            SessionInput::text("after restart"),
            reopened.instance.clone(),
        )
        .await;
        assert_eq!(model.renames.load(Ordering::SeqCst), 1);
        assert_eq!(claim(&reopened, &id).await.unwrap(), receipt);
        let audit = rename_audit(&reopened, &id).await;
        assert_eq!(audit.attempts.len(), 1);
        assert_eq!(audit.attempts[0].subagent.as_deref(), Some("rename"));
        assert_eq!(audit.attempts[0].completion, "succeeded");
        assert_eq!(audit.attempts[0].call_id, "rename-0");
        reopened.close().await.unwrap();
    }
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn first_admission_names_before_primary_completion_and_survives_host_suspension_file() {
    successful_commands("file").await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn first_admission_names_before_primary_completion_and_survives_host_suspension_sqlite() {
    successful_commands("sqlite").await;
}

async fn optional_and_suppressed(storage: &str) {
    for manual in [false, true] {
        let model = NamingModel::new(false, !manual);
        let (manager, id, managed) = fixture(model.clone(), storage).await;
        let owner = Principal::for_test("owner");
        manager
            .command(
                &owner,
                &insert("turn.queue", &id, SessionInput::text("original task")),
            )
            .await
            .unwrap();
        permit(&model.entered).await;
        permit(&model.rename_entered).await;
        if manual {
            assert!(
                manager
                    .command(
                        &owner,
                        &command(
                            "session.rename",
                            Some(&id),
                            json!({"title":"User title","expected_revision":999})
                        )
                    )
                    .await
                    .is_err()
            );
            assert_eq!(claim(&manager, &id).await.unwrap().status, "running");
            manager
                .command(
                    &owner,
                    &command(
                        "session.rename",
                        Some(&id),
                        json!({"title":"New session","expected_revision":0}),
                    ),
                )
                .await
                .unwrap();
            assert!(managed.naming.lock().await.is_none());
            assert_eq!(claim(&manager, &id).await.unwrap().status, "interrupted");
        } else {
            model.rename_release.add_permits(1);
            super::super::naming::finish(&managed).await;
            assert_eq!(claim(&manager, &id).await.unwrap().status, "failed");
        }
        model.release.add_permits(1);
        idle(&managed).await;
        assert_eq!(model.renames.load(Ordering::SeqCst), 1);
        assert_eq!(
            manager.record("owner", &id).await.unwrap().title,
            "New session"
        );
        assert!(managed.session.messages().iter().any(|item| matches!(item,
            microcrab_session::context::ContentItem::Assistant { content, .. } if content == "Primary answer")));
        manager.close().await.unwrap();
    }
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn naming_failure_is_optional_and_manual_default_title_wins_file() {
    optional_and_suppressed("file").await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn naming_failure_is_optional_and_manual_default_title_wins_sqlite() {
    optional_and_suppressed("sqlite").await;
}

async fn rejected_and_failed(storage: &str) {
    let model = NamingModel::new(true, false);
    let (manager, id, managed) = fixture(model.clone(), storage).await;
    let owner = Principal::for_test("owner");
    // Accepted intent is durable, but invalid image preparation rejects before
    // actual runtime admission. It must neither claim naming nor issue a provider.
    let invalid = SessionInput {
        parts: vec![
            InputPart::Text {
                text: "rejected seed".into(),
            },
            InputPart::Image {
                source: microcrab_protocol::ImageSource::Data {
                    media_type: "image/png".into(),
                    data: vec![0, 1, 2],
                },
            },
        ],
        variables: Default::default(),
    };
    let accepted = manager
        .command(&owner, &insert("turn.submit", &id, invalid))
        .await
        .unwrap();
    assert_eq!(accepted["status"], "accepted");
    managed.worker.lock().await.take().unwrap().await.unwrap();
    assert!(claim(&manager, &id).await.is_none());
    assert_eq!(model.primary.load(Ordering::SeqCst), 0);
    assert_eq!(model.renames.load(Ordering::SeqCst), 0);
    manager
        .command(
            &owner,
            &insert(
                "turn.queue",
                &id,
                SessionInput::text("failed primary still names"),
            ),
        )
        .await
        .unwrap();
    // A rejected foreground submit pauses the shared inbox. Queue admission is
    // still successful and must name immediately, but cannot resume execution.
    permit(&model.rename_entered).await;
    assert_eq!(model.primary.load(Ordering::SeqCst), 0);
    let inbox = managed.session.inbox_snapshot();
    assert!(inbox.paused);
    let item = &inbox.items[0];
    let resumed = manager
        .command(
            &owner,
            &command(
                "inbox.resume",
                Some(&id),
                json!({
                    "operation_id": uuid::Uuid::new_v4().to_string(),
                    "expected_inbox_id": inbox.inbox_id,
                    "item_id": item.id,
                    "expected_revision": item.revision,
                }),
            ),
        )
        .await
        .unwrap();
    permit(&model.entered).await;
    model.release.add_permits(1);
    // Explicit resume is a server-owned foreground turn. A failed foreground
    // emits no Completed projection, so outcome_sequence is not its settlement
    // witness. Join its owner and inspect the exact durable operation instead.
    let worker = managed.worker.lock().await.take().unwrap();
    match tokio::time::timeout(std::time::Duration::from_secs(5), worker).await {
        Ok(result) => result.unwrap(),
        Err(error) => {
            let activity = managed.session.subscribe_activity().borrow().clone();
            let inbox = managed.session.inbox_snapshot();
            panic!(
                "resume settlement timed out: {error}; operation={} active={} paused={} outcomes={} pending_ids={:?} primary_calls={} naming_calls={} current_operation={:?} naming_claim={:?}",
                resumed["id"],
                activity.active,
                activity.paused,
                activity.outcome_sequence,
                inbox
                    .items
                    .iter()
                    .map(|item| (item.id, item.revision))
                    .collect::<Vec<_>>(),
                model.primary.load(Ordering::SeqCst),
                model.renames.load(Ordering::SeqCst),
                managed.hub.operation(),
                claim(&manager, &id)
                    .await
                    .map(|operation| (operation.id, operation.status)),
            );
        }
    }
    let resumed_receipt = manager
        .directory
        .get_operation("owner", resumed["id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed_receipt.status, "failed");
    let resumed_error = &resumed_receipt.result.as_ref().unwrap()["error"];
    assert_eq!(resumed_error["code"], "provider_status");
    assert_eq!(resumed_error["message"], "provider HTTP status 500");
    assert!(!managed.faulted.load(Ordering::Acquire));
    // retry_ready also checks the Session's own faulted/closed flags. This
    // proves the fixture exercised recoverable failure rather than quarantine.
    assert!(managed.session.retry_ready());
    assert!(!managed.session.subscribe_activity().borrow().active);
    assert_eq!(claim(&manager, &id).await.unwrap().status, "running");
    model.rename_release.add_permits(1);
    super::super::naming::finish(&managed).await;
    let naming_receipt = claim(&manager, &id).await.unwrap();
    assert_eq!(
        naming_receipt.status,
        "completed",
        "naming settlement after ordinary provider failure: result={:?}; resume_error={resumed_error}; managed_faulted={} retry_ready={} primary_calls={} naming_calls={}",
        naming_receipt.result,
        managed.faulted.load(Ordering::Acquire),
        managed.session.retry_ready(),
        model.primary.load(Ordering::SeqCst),
        model.renames.load(Ordering::SeqCst),
    );
    assert_eq!(
        model.seeds.lock().unwrap().as_slice(),
        &["failed primary still names"]
    );
    assert_eq!(
        manager.record("owner", &id).await.unwrap().title,
        "Queued session title"
    );
    manager.close().await.unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn submit_intent_is_not_admission_and_failed_primary_still_names_file() {
    rejected_and_failed("file").await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn submit_intent_is_not_admission_and_failed_primary_still_names_sqlite() {
    rejected_and_failed("sqlite").await;
}

async fn image_only(storage: &str) {
    let model = NamingModel::new(false, false);
    let (manager, id, managed) = fixture(model.clone(), storage).await;
    let owner = Principal::for_test("owner");
    managed.session.interrupt();
    // Idle steer admits to the shared inbox without image preparation. No text
    // means no claim; a subsequent durable text admission remains eligible.
    use base64::Engine as _;
    let png = base64::engine::general_purpose::STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j7S8AAAAASUVORK5CYII=").unwrap();
    let input = SessionInput {
        parts: vec![InputPart::Image {
            source: microcrab_protocol::ImageSource::Data {
                media_type: "image/png".into(),
                data: png,
            },
        }],
        variables: Default::default(),
    };
    assert_eq!(
        manager
            .command(&owner, &insert("turn.steer", &id, input))
            .await
            .unwrap()["status"],
        "completed"
    );
    assert!(claim(&manager, &id).await.is_none());
    assert_eq!(model.renames.load(Ordering::SeqCst), 0);
    manager
        .command(
            &owner,
            &insert("turn.steer", &id, SessionInput::text("first actual text")),
        )
        .await
        .unwrap();
    permit(&model.rename_entered).await;
    model.rename_release.add_permits(1);
    super::super::naming::finish(&managed).await;
    assert_eq!(
        model.seeds.lock().unwrap().as_slice(),
        &["first actual text"]
    );
    manager.close().await.unwrap();
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn image_only_admission_preserves_claim_file() {
    image_only("file").await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn image_only_admission_preserves_claim_sqlite() {
    image_only("sqlite").await;
}
