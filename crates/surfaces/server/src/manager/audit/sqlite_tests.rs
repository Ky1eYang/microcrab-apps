//! Detached HTTP-facing audit authorization and lifecycle on SQLite.
use super::*;
use microcrab_creator::audit::{AuditStatus, EffectiveRequest, Recorder, RecorderOptions};
use microcrab_session::Journal;
use microcrab_storage::{CreatorJournal, SessionRecord, audit::AuditSource};
use serde_json::json;

fn selector(child: Option<&str>) -> Request {
    Request {
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
        child_session_id: child.map(str::to_owned),
        internal_assistants: false,
    }
}
#[tokio::test]
async fn sqlite_detached_queries_archive_and_scoped_hard_delete_need_no_host() {
    let root = tempfile::tempdir().unwrap();
    let mut config: crate::config::Config = serde_json::from_value(json!({
        "data_dir":root.path().join("data"),"storage":"sqlite","auto_rename":false,
        "provider":{"endpoint":"http://127.0.0.1:1/v1/chat/completions","model":"test"},
        "auth":{"issuer":"http://127.0.0.1:9900","authorization_endpoint":"http://127.0.0.1:9900/authorize","token_endpoint":"http://127.0.0.1:9900/token","jwks_uri":"http://127.0.0.1:9900/jwks","client_id":"test","redirect_uri":"http://127.0.0.1:4096/api/auth/callback","allowed_origins":["http://127.0.0.1:4096"],"jwt_secret":"test-key-at-least-thirty-two-characters"}
    })).unwrap();
    config.resolve(root.path()).unwrap();
    crate::project::seed_test_projects(
        &config.data_dir,
        json!([{"id":"project","name":"Project","root":root.path(),"owners":["owner"]}]),
    );
    let manager = Manager::new(config).await.unwrap();
    let record = SessionRecord {
        version: microcrab_storage::RECORD_VERSION,
        id: "root".into(),
        owner: "owner".into(),
        title: "Detached".into(),
        project_id: Some("project".into()),
        project_root: root.path().to_string_lossy().into_owned(),
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
    let child = "0123456789abcdef";
    let unrelated = "fedcba9876543210";
    for (root_id, id) in [("root", "root"), ("root", child), ("other-root", unrelated)] {
        let (journal, _) = CreatorJournal::open_sqlite(
            manager.config.data_dir.join("storage.sqlite"),
            id.into(),
            false,
        )
        .await
        .unwrap();
        journal.close().await.unwrap();
        let source = source(
            &manager.config,
            "owner",
            root_id,
            id,
            root.path().join("unused"),
        );
        let mut recorder = Recorder::open_source(source, RecorderOptions::default()).unwrap();
        recorder
            .effective(EffectiveRequest {
                call_id: "call".into(),
                epoch: 0,
                turn: Some(1),
                round: Some(1),
                parent_request: None,
                history_head: None,
                subagent: None,
                request_body: "exact request".into(),
            })
            .unwrap();
    }
    let result = query(&manager, "owner", "root", selector(None))
        .await
        .unwrap();
    assert_eq!(result.attempts.len(), 1);
    assert!(
        query(&manager, "other-owner", "root", selector(None))
            .await
            .is_err()
    );
    assert_eq!(
        query(&manager, "owner", "root", selector(Some(child)))
            .await
            .unwrap()
            .attempts
            .len(),
        1
    );
    assert_eq!(
        query(&manager, "owner", "root", selector(Some(unrelated)))
            .await
            .unwrap()
            .status,
        AuditStatus::Missing
    );
    assert!(manager.registry.lock().unwrap().is_empty());
    super::super::deletion::archive_session(&manager, "owner", "root", Some(0))
        .await
        .unwrap();
    assert_eq!(
        query(&manager, "owner", "root", selector(Some(child)))
            .await
            .unwrap()
            .attempts
            .len(),
        1
    );
    super::super::deletion::delete_session(&manager, "owner", "root", Some(1))
        .await
        .unwrap();
    assert!(super::super::deletion::cleanup_settled(&manager, "root").await);
    let deleted = source(
        &manager.config,
        "owner",
        "root",
        child,
        root.path().join("unused"),
    );
    assert!(deleted.reader().unwrap().is_none());
    let retained = source(
        &manager.config,
        "owner",
        "other-root",
        unrelated,
        root.path().join("unused"),
    );
    assert!(retained.reader().unwrap().is_some());
    assert!(
        !manager
            .config
            .data_dir
            .join("sessions/root/audit.jsonl")
            .exists()
    );
    assert!(
        !manager
            .config
            .data_dir
            .join("sessions/root/.audit.lock")
            .exists()
    );
    assert!(matches!(retained, AuditSource::Sqlite { .. }));
}
