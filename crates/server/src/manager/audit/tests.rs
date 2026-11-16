//! Audit inspection must not activate a Session or mutate legacy metadata.
#![cfg(feature = "storage-file")]
use super::*;
use crate::{config::Config, manager::Manager};
use microcrab_creator::audit::{AuditStatus, EffectiveRequest, Recorder, RecorderOptions};
use microcrab_storage::SessionRecord;
use serde_json::json;

async fn fixture(root: &std::path::Path) -> Arc<Manager> {
    let mut config: Config = serde_json::from_value(json!({
        "data_dir":root.join(".data"), "storage":"file", "auto_rename":false,
        "provider":{"endpoint":"http://127.0.0.1:1/v1/chat/completions","model":"test"},
        "auth":{"issuer":"http://127.0.0.1:9900","authorization_endpoint":"http://127.0.0.1:9900/authorize","token_endpoint":"http://127.0.0.1:9900/token","jwks_uri":"http://127.0.0.1:9900/jwks","client_id":"test","redirect_uri":"http://127.0.0.1:4096/api/auth/callback","allowed_origins":["http://127.0.0.1:4096"],"jwt_secret":"test-key-at-least-thirty-two-characters"}
    })).unwrap();
    config.resolve(root).unwrap();
    crate::project::seed_test_projects(
        &config.data_dir,
        json!([
            {"id":"project","name":"Project","root":root,"owners":["owner"]}
        ]),
    );
    Manager::new(config).await.unwrap()
}
fn selector() -> Request {
    Request {
        child_session_id: None,
        internal_assistants: false,
        selector: AuditQuery {
            epoch: 0,
            turn: Some(1),
            round: Some(1),
            request_hash: None,
            history_head: None,
            subagent: None,
            call_id: None,
            tool_call_id: None,
        },
    }
}
#[tokio::test]
async fn closed_audit_is_owner_isolated_without_host_activation_or_metadata_migration() {
    let root = tempfile::tempdir().unwrap();
    let manager = fixture(root.path()).await;
    let id = "closed-audit";
    let record = SessionRecord {
        version: microcrab_storage::RECORD_VERSION,
        id: id.into(),
        owner: "owner".into(),
        project_id: Some("project".into()),
        project_root: root.path().to_string_lossy().into_owned(),
        title: "Closed".into(),
        agent: None,
        mode: None,
        model_selection: None,
        auto_compaction: None,
        prompt_config: None,
        permissions: vec![],
        initialization: microcrab_permission::Action::Ask,
        review: false,
        review_model: None,
        revision: 0,
        archived: false,
        created_at_ms: 1,
        updated_at_ms: 1,
    };
    manager.directory.create(record).await.unwrap();
    let directory = manager.config.data_dir.join("sessions").join(id);
    let mut recorder = Recorder::open(&directory, RecorderOptions::default()).unwrap();
    let identity = recorder
        .effective(EffectiveRequest {
            call_id: "request-1".into(),
            epoch: 0,
            turn: Some(1),
            round: Some(1),
            parent_request: None,
            history_head: None,
            subagent: None,
            request_body: "effective-only".into(),
        })
        .unwrap();
    recorder.publish(json!({"call_id":"request-1","event":"request_prepared","sent_request":"{\"messages\":[]}"})).unwrap();
    drop(recorder);
    let result = query(&manager, "owner", id, selector()).await.unwrap();
    assert_eq!(result.status, AuditStatus::Available);
    assert_eq!(result.attempts[0].request_hash, identity);
    let mut internal = selector();
    internal.internal_assistants = true;
    let internal = query(&manager, "owner", id, internal).await.unwrap();
    assert!(internal.timeline.is_empty());
    assert!(manager.registry.lock().unwrap().is_empty());
    assert_eq!(
        query(&manager, "intruder", id, selector())
            .await
            .unwrap_err()
            .code,
        "not_found"
    );
    let (child_id, child_path) = microcrab_creator::paths::reserve_child(&directory).unwrap();
    let mut child_recorder = Recorder::open(child_path, RecorderOptions::default()).unwrap();
    let child_identity = child_recorder
        .effective(EffectiveRequest {
            call_id: "child-request".into(),
            epoch: 0,
            turn: Some(1),
            round: Some(1),
            parent_request: None,
            history_head: None,
            subagent: None,
            request_body: "effective".into(),
        })
        .unwrap();
    child_recorder.publish(json!({"call_id":"child-request","event":"request_prepared","sent_request":"{\"messages\":[]}"})).unwrap();
    drop(child_recorder);
    let mut child = selector();
    child.child_session_id = Some(child_id.clone());
    let child_result = query(&manager, "owner", id, child).await.unwrap();
    assert_eq!(child_result.attempts[0].request_hash, child_identity);
    let mut forbidden = selector();
    forbidden.child_session_id = Some(child_id);
    assert_eq!(
        query(&manager, "intruder", id, forbidden)
            .await
            .unwrap_err()
            .code,
        "not_found"
    );
    let mut traversal = selector();
    traversal.child_session_id = Some("../other".into());
    assert_eq!(
        query(&manager, "owner", id, traversal)
            .await
            .unwrap_err()
            .code,
        "invalid_params"
    );
    assert!(manager.registry.lock().unwrap().is_empty());
    let retained = manager.directory.get("owner", id).await.unwrap().unwrap();
    assert!(retained.model_selection.is_none());
    assert_eq!(retained.revision, 0);
    manager.close().await.unwrap();
}

#[tokio::test]
async fn internal_helpers_follow_verified_intermediate_checkpoints_and_selected_rollback() {
    use microcrab_session::{Limits, Recover, SessionState, context::Context};
    use microcrab_storage::history::{
        CreatorEnvelope, FileJournal, History, RestoreTarget, Travel,
    };
    fn checkpoint(label: &str, request_head: Option<String>) -> CreatorEnvelope {
        let limits = Limits::default();
        let context = Context::new(label.into(), vec![], limits.max_context_bytes).unwrap();
        CreatorEnvelope {
            session_id: "intermediate-audit".into(),
            request_head,
            model_selection: None,
            route_witness: None,
            navigation_boundary: None,
            recover: Recover {
                state: SessionState::new(limits),
                context: context.snapshot().unwrap(),
                continuation: None,
                workspace: None,
                cache_report: None,
                prompt_config: None,
                agent: None,
            },
        }
    }
    fn effective(
        recorder: &mut Recorder,
        call: &str,
        head: &str,
        parent: Option<String>,
        helper: bool,
    ) -> String {
        recorder
            .effective(EffectiveRequest {
                call_id: call.into(),
                epoch: 0,
                turn: Some(1),
                round: Some(1),
                parent_request: parent,
                history_head: Some(head.into()),
                subagent: helper.then(|| "action_review".into()),
                request_body: "{}".into(),
            })
            .unwrap()
    }
    fn stage(recorder: &mut Recorder, call: &str, head: &str) {
        recorder
            .publish(json!({"event":"tool_call","epoch":0,"turn_id":1,"round":1,
            "tool_call_id":call,"history_head":head,"started_at_unix_ms":10,
            "payload":{"name":"lookup"}}))
            .unwrap();
    }
    let root = tempfile::tempdir().unwrap();
    let manager = fixture(root.path()).await;
    let id = "intermediate-audit";
    manager
        .directory
        .create(SessionRecord {
            version: microcrab_storage::RECORD_VERSION,
            id: id.into(),
            owner: "owner".into(),
            project_id: Some("project".into()),
            project_root: root.path().to_string_lossy().into_owned(),
            title: "Audit".into(),
            agent: None,
            mode: None,
            model_selection: None,
            auto_compaction: None,
            prompt_config: None,
            permissions: vec![],
            initialization: microcrab_permission::Action::Ask,
            review: false,
            review_model: None,
            revision: 0,
            archived: false,
            created_at_ms: 1,
            updated_at_ms: 1,
        })
        .await
        .unwrap();
    let directory = manager.config.data_dir.join("sessions").join(id);
    let journal = FileJournal::open(&directory).await.unwrap();
    let a = journal
        .commit(None, checkpoint("root", None))
        .await
        .unwrap();
    let mut recorder = Recorder::open(&directory, RecorderOptions::default()).unwrap();
    let first = effective(&mut recorder, "main-first", &a.checkpoint_id, None, false);
    let b = journal
        .commit(
            Some(a.clone()),
            checkpoint("intermediate", Some(first.clone())),
        )
        .await
        .unwrap();
    let retained = effective(
        &mut recorder,
        "review-intermediate",
        &b.checkpoint_id,
        None,
        true,
    );
    recorder
        .publish(
            json!({"call_id":"review-intermediate","event":"round_completed",
        "outcome":"accepted","response":{"content":"Allow","tool_calls":[]}}),
        )
        .unwrap();
    stage(&mut recorder, "intermediate-tool", &b.checkpoint_id);
    let c = journal
        .commit(Some(b.clone()), checkpoint("next", Some(first.clone())))
        .await
        .unwrap();
    let second = effective(
        &mut recorder,
        "main-second",
        &c.checkpoint_id,
        Some(first.clone()),
        false,
    );
    let later = effective(&mut recorder, "review-later", &c.checkpoint_id, None, true);
    let d = journal
        .commit(Some(c), checkpoint("selected", Some(second.clone())))
        .await
        .unwrap();
    journal
        .move_head(
            d.clone(),
            a.clone(),
            Travel::To {
                checkpoint_id: a.checkpoint_id.clone(),
            },
        )
        .await
        .unwrap();
    let sibling = journal
        .commit(Some(a.clone()), checkpoint("sibling", Some(first.clone())))
        .await
        .unwrap();
    effective(
        &mut recorder,
        "main-sibling",
        &sibling.checkpoint_id,
        Some(first.clone()),
        false,
    );
    let excluded = effective(
        &mut recorder,
        "review-sibling",
        &sibling.checkpoint_id,
        None,
        true,
    );
    stage(&mut recorder, "sibling-tool", &sibling.checkpoint_id);
    journal
        .move_head(
            sibling,
            d.clone(),
            Travel::To {
                checkpoint_id: d.checkpoint_id.clone(),
            },
        )
        .await
        .unwrap();
    // The writer remains open: inspection must neither contend for it nor publish HEAD.
    let audit_before = std::fs::read(directory.join("audit.jsonl")).unwrap();
    let manifest_before = std::fs::read(directory.join("journal/manifest.json")).unwrap();
    let mut internal = selector();
    internal.internal_assistants = true;
    let selected = query(&manager, "owner", id, internal).await.unwrap();
    assert_eq!(
        selected
            .attempts
            .iter()
            .map(|a| a.request_hash.as_str())
            .collect::<Vec<_>>(),
        vec![retained.as_str(), later.as_str()]
    );
    assert!(!selected.attempts.iter().any(|a| a.request_hash == excluded));
    assert_eq!(
        selected.attempts[0].response.as_ref().unwrap()["content"],
        "Allow"
    );
    assert_eq!(selected.stages.len(), 1);
    assert_eq!(
        selected.stages[0].tool_call_id.as_deref(),
        Some("intermediate-tool")
    );
    assert_eq!(
        std::fs::read(directory.join("journal/manifest.json")).unwrap(),
        manifest_before
    );
    assert_eq!(
        journal.restore(RestoreTarget::Head).await.unwrap().cursor,
        d
    );
    journal
        .move_head(
            d,
            b.clone(),
            Travel::To {
                checkpoint_id: b.checkpoint_id.clone(),
            },
        )
        .await
        .unwrap();
    let mut internal = selector();
    internal.internal_assistants = true;
    let rolled_back = query(&manager, "owner", id, internal).await.unwrap();
    assert_eq!(
        rolled_back
            .attempts
            .iter()
            .map(|a| a.request_hash.as_str())
            .collect::<Vec<_>>(),
        vec![retained.as_str()]
    );
    assert_eq!(rolled_back.stages.len(), 1);
    assert_eq!(
        std::fs::read(directory.join("audit.jsonl")).unwrap(),
        audit_before
    );
    assert!(manager.registry.lock().unwrap().is_empty());
    assert!(
        FileJournal::inspect_ancestry(
            &directory,
            "foreign-session".into(),
            b.checkpoint_id.clone()
        )
        .await
        .is_err()
    );
    // Removing a retained parent must reject the whole query, never yield a partial branch.
    let parent_path = directory
        .join("journal/nodes")
        .join(format!("{}.json", a.checkpoint_id));
    let parent_bytes = std::fs::read(&parent_path).unwrap();
    std::fs::write(&parent_path, b"{}").unwrap();
    let mut internal = selector();
    internal.internal_assistants = true;
    assert_eq!(
        query(&manager, "owner", id, internal)
            .await
            .unwrap_err()
            .code,
        "audit_unavailable"
    );
    std::fs::write(&parent_path, parent_bytes).unwrap();
    journal.close().await.unwrap();
    manager.close().await.unwrap();
}
