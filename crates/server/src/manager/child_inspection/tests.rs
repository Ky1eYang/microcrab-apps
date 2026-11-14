//! Closed/live-writer inspection reads durable checkpoints without runtime activation.
#![cfg(feature = "storage-file")]
#[path = "continuity_tests.rs"]
mod continuity;
#[path = "extra_tests.rs"]
mod extra;
use super::*;
use crate::{config::Config, manager::Manager};
use microcrab_session::{Journal, Limits, SessionState, context::Context};
use microcrab_storage::{CreatorJournal, SessionRecord};
use std::{collections::BTreeMap, sync::Arc};

async fn fixture(root: &Path) -> Arc<Manager> {
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
async fn parent(manager: &Manager, id: &str) -> PathBuf {
    manager
        .directory
        .create(SessionRecord {
            version: microcrab_storage::RECORD_VERSION,
            id: id.into(),
            owner: "owner".into(),
            project_id: Some("project".into()),
            project_root: "opaque".into(),
            title: "Parent".into(),
            agent: None,
            mode: None,
            model_selection: None,
            auto_compaction: None,
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
    std::fs::create_dir_all(&directory).unwrap();
    directory
}
fn recover(text: &str) -> Recover {
    let limits = Limits::default();
    let mut context = Context::new(
        "private-system-marker".into(),
        vec![],
        limits.max_context_bytes,
    )
    .unwrap();
    context.admit_user(text, &json!({})).unwrap();
    Recover {
        state: SessionState::new(limits),
        context: context.snapshot().unwrap(),
        continuation: None,
        workspace: None,
        cache_report: None,
        agent: None,
    }
}
async fn journal(directory: &Path, id: &str, recover: Recover) -> CreatorJournal {
    let (journal, _) = CreatorJournal::open(directory.to_str().unwrap(), id.into(), true, false)
        .await
        .unwrap();
    journal.commit(recover).await.unwrap();
    journal
}
fn detail(parent: &str, child: &str) -> DetailRequest {
    DetailRequest {
        session_id: parent.into(),
        child_id: child.into(),
        ..Default::default()
    }
}
fn list(parent: &str) -> ListRequest {
    ListRequest {
        session_id: parent.into(),
        ..Default::default()
    }
}
fn file_digest(path: &Path) -> (u64, String) {
    let bytes = std::fs::read(path).unwrap();
    (
        bytes.len() as u64,
        microcrab_storage::history::content_hash(&bytes).unwrap(),
    )
}
fn files(root: &Path) -> BTreeMap<PathBuf, (u64, String)> {
    let mut found = BTreeMap::new();
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            found.extend(files(&path));
        } else if path.file_name().is_some_and(|name| name == "lock")
            && path
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|name| name == "journal")
        {
            // Windows denies reads of this deliberately locked coordination file.
            // Its identity/length stay covered; the test separately proves that
            // the original writer's exclusive ownership survives inspection.
            found.insert(
                path.clone(),
                (
                    std::fs::metadata(&path).unwrap().len(),
                    "journal-lock-not-read".into(),
                ),
            );
        } else {
            found.insert(path.clone(), file_digest(&path));
        }
    }
    found
}
async fn writer_remains_exclusive(directory: &Path) {
    assert!(
        matches!(microcrab_storage::history::FileJournal::open(directory).await,
        Err(microcrab_session::Error::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
}

#[tokio::test]
async fn closed_and_active_writer_child_reads_preserve_history_and_parent_authority() {
    let root = tempfile::tempdir().unwrap();
    let manager = fixture(root.path()).await;
    let directory = parent(&manager, "parent").await;
    let other = parent(&manager, "other").await;
    let (id, child) = microcrab_creator::paths::reserve_child(&directory).unwrap();
    let child_journal = journal(&child, &id, recover("actual durable child message")).await;
    // A writer remains open. Read-only inspection must neither conflict nor take ownership.
    writer_remains_exclusive(&child).await;
    let before = files(&directory);
    let result = manager
        .inspect_child("owner", detail("parent", &id))
        .await
        .unwrap();
    assert_eq!(result["availability"], "persisted");
    assert_eq!(result["status"], "unknown");
    assert_eq!(
        result["messages"][0]["text"],
        "actual durable child message"
    );
    assert_eq!(result["parent_session_id"], "parent");
    assert_eq!(result["id"], id);
    assert_eq!(result["receipt"], Value::Null);
    assert_eq!(result["usage"], Value::Null);
    assert!(!result.to_string().contains("private-system-marker"));
    writer_remains_exclusive(&child).await;
    assert_eq!(files(&directory), before);
    assert_eq!(
        manager
            .inspect_child("intruder", detail("parent", &id))
            .await
            .unwrap_err()
            .code,
        "not_found"
    );
    assert!(
        manager
            .inspect_child("owner", detail("other", &id))
            .await
            .is_err()
    );
    assert!(
        manager
            .inspect_child("owner", detail("parent", "../parent"))
            .await
            .is_err()
    );
    assert!(
        manager
            .inspect_children(
                "owner",
                ListRequest {
                    limit: Some(101),
                    ..list("parent")
                }
            )
            .await
            .is_err()
    );
    assert!(manager.registry.lock().unwrap().is_empty());
    assert!(manager.pins.session(&id).await.unwrap().is_none());
    assert_eq!(manager.directory.list("owner").await.unwrap().len(), 2);
    assert!(files(&other).is_empty());
    child_journal.close().await.unwrap();
    let after_close = files(&directory);
    let closed = manager
        .inspect_child("owner", detail("parent", &id))
        .await
        .unwrap();
    assert_eq!(closed["messages"], result["messages"]);
    assert_eq!(files(&directory), after_close);
    manager.close().await.unwrap();
}

#[tokio::test]
async fn pending_corrupt_and_paginated_children_are_explicit_without_creating_history() {
    let root = tempfile::tempdir().unwrap();
    let manager = fixture(root.path()).await;
    let directory = parent(&manager, "parent").await;
    let (first, first_path) = microcrab_creator::paths::reserve_child(&directory).unwrap();
    let (second, second_path) = microcrab_creator::paths::reserve_child(&directory).unwrap();
    let before = files(&directory);
    let result = manager
        .inspect_child("owner", detail("parent", &first))
        .await
        .unwrap();
    assert_eq!(result["availability"], "pending");
    assert_eq!(result["messages"], json!([]));
    assert!(!first_path.join("journal").exists());
    assert_eq!(files(&directory), before);
    let page = manager
        .inspect_children(
            "owner",
            ListRequest {
                limit: Some(1),
                ..list("parent")
            },
        )
        .await
        .unwrap();
    assert_eq!(page["children"].as_array().unwrap().len(), 1);
    assert!(page["next_cursor"].is_string());
    let next = manager
        .inspect_children(
            "owner",
            ListRequest {
                limit: Some(1),
                cursor: Some(page["next_cursor"].as_str().unwrap().into()),
                ..list("parent")
            },
        )
        .await
        .unwrap();
    assert_eq!(next["children"].as_array().unwrap().len(), 1);
    assert_ne!(page["children"][0]["id"], next["children"][0]["id"]);
    assert!(next["next_cursor"].is_null());
    let journal = journal(&second_path, &second, recover("real message")).await;
    journal.close().await.unwrap();
    std::fs::write(second_path.join("journal/manifest.json"), b"corrupt").unwrap();
    let unavailable = manager
        .inspect_child("owner", detail("parent", &second))
        .await
        .unwrap();
    assert_eq!(unavailable["availability"], "unavailable");
    assert_eq!(unavailable["messages"], json!([]));
    manager.close().await.unwrap();
}

#[tokio::test]
async fn completed_receipt_requires_canonical_delegate_tool_link_not_user_json() {
    let root = tempfile::tempdir().unwrap();
    let manager = fixture(root.path()).await;
    let directory = parent(&manager, "parent").await;
    let (id, child) = microcrab_creator::paths::reserve_child(&directory).unwrap();
    let child_journal = journal(&child, &id, recover("child output")).await;
    child_journal.close().await.unwrap();
    let receipt = json!({"child_id":id,"subagent":"researcher","outcome":{"status":"completed"},"usage":{"input_tokens":7},"tool_tasks":[]});
    let mut checkpoint = recover(&receipt.to_string());
    assert!(receipts(&checkpoint).is_empty());
    checkpoint.context.items.push(ContentItem::Assistant {
        provider_metadata: None,
        content: String::new(),
        tool_calls: vec![microcrab_protocol::ToolCall {
            provider_metadata: None,
            id: "delegate-call".into(),
            name: "delegate".into(),
            arguments: json!({"input":"work"}),
        }],
    });
    checkpoint
        .context
        .tools
        .push(microcrab_protocol::ToolDefinition {
            name: "delegate".into(),
            description: "delegate".into(),
            parameters: json!({"type":"object"}),
        });
    checkpoint
        .context
        .used_call_ids
        .insert("delegate-call".into());
    checkpoint.context.items.push(ContentItem::Tool {
        call_id: "delegate-call".into(),
        content: receipt.to_string(),
    });
    let parent_journal = journal(&directory, "parent", checkpoint).await;
    parent_journal.close().await.unwrap();
    let result = manager
        .inspect_child("owner", detail("parent", &id))
        .await
        .unwrap();
    assert_eq!(result["status"], "completed");
    assert_eq!(result["name"], "researcher");
    assert_eq!(result["receipt"], receipt);
    assert_eq!(result["usage"]["input_tokens"], 7);
    manager.close().await.unwrap();
}

#[test]
fn message_pages_preserve_cursor_and_bound_oversized_bodies() {
    let messages=(0..205).map(|index|json!({"id":format!("m{index}"),"role":"tool","text":"x".repeat(40000),"parts":[],"tool_calls":[]})).collect();
    let (page, next, truncated) = message_page(messages, None, 100).unwrap();
    assert_eq!(page.len(), 100);
    assert_eq!(next, Some(105));
    assert!(truncated);
    assert_eq!(page[0]["id"], "m105");
    assert_eq!(page[0]["content_omitted"], true);
    assert!(serde_json::to_vec(&page).unwrap().len() < MAX_PAGE_BYTES);
    assert!(message_page(vec![], Some(1), 100).is_err());
    let (single, next, truncated) = message_page(
        vec![json!({"id":"one","role":"user","text":"x".repeat(40000)})],
        None,
        100,
    )
    .unwrap();
    assert_eq!(single[0]["content_omitted"], true);
    assert!(truncated);
    assert_eq!(next, None);
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_readonly_selected_checkpoint_does_not_create_or_modify_database() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("history.sqlite");
    assert!(
        microcrab_storage::history::SqliteJournal::inspect_head(&database, "child".into())
            .await
            .unwrap()
            .is_none()
    );
    assert!(!database.exists());
    let (journal, _) = CreatorJournal::open_sqlite(&database, "child".into(), false)
        .await
        .unwrap();
    let expected_recover = recover("sqlite committed message");
    let expected_recover_hash =
        microcrab_storage::history::content_hash(&expected_recover).unwrap();
    journal.commit(expected_recover).await.unwrap();
    let expected_cursor = journal.cursor().await.unwrap();
    journal.close().await.unwrap();
    let before_database = file_digest(&database);
    let wal = root.path().join("history.sqlite-wal");
    let before_wal = wal.try_exists().unwrap().then(|| file_digest(&wal));
    let selected =
        microcrab_storage::history::SqliteJournal::inspect_head(&database, "child".into())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(selected.cursor, expected_cursor);
    assert_eq!(
        microcrab_storage::history::content_hash(&selected.checkpoint.recover).unwrap(),
        expected_recover_hash
    );
    let second = microcrab_storage::history::SqliteJournal::inspect_head(&database, "child".into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.cursor, expected_cursor);
    assert_eq!(
        microcrab_storage::history::content_hash(&second.checkpoint.recover).unwrap(),
        expected_recover_hash
    );
    let messages = view::messages(selected.checkpoint.recover.context.items, 0, "child");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["text"], "sqlite committed message");
    assert_eq!(file_digest(&database), before_database);
    if let Some(before) = before_wal {
        assert_eq!(file_digest(&wal), before);
    } else if wal.try_exists().unwrap() {
        // A read-only WAL-mode connection may create empty coordination files,
        // but it must never append a transaction frame to the WAL.
        assert_eq!(std::fs::metadata(&wal).unwrap().len(), 0);
    }
    for entry in std::fs::read_dir(root.path()).unwrap() {
        let name = entry.unwrap().file_name();
        assert!(
            name == "history.sqlite"
                || name == "history.sqlite-wal"
                || name == "history.sqlite-shm",
            "unexpected SQLite inspection artifact: {name:?}"
        );
    }
}

#[tokio::test]
async fn child_history_preserves_recorded_positions_for_shared_chat_and_trajectory() {
    let root = tempfile::tempdir().unwrap();
    let manager = fixture(root.path()).await;
    let directory = parent(&manager, "parent").await;
    let (id, path) = microcrab_creator::paths::reserve_child(&directory).unwrap();
    let limits = Limits::default();
    let mut context = Context::new(
        "private-system-marker".into(),
        vec![microcrab_protocol::ToolDefinition {
            name: "pwsh".into(),
            description: "fixture".into(),
            parameters: json!({"type":"object"}),
        }],
        limits.max_context_bytes,
    )
    .unwrap();
    context.admit_user("child task", &json!({})).unwrap();
    context
        .synchronize_state(&json!({"session":{"epoch":0,"turn":2,"round":1}}))
        .unwrap();
    context
        .append_response(&microcrab_protocol::Response {
            provider_metadata: None,
            content: "working".into(),
            tool_calls: vec![microcrab_protocol::ToolCall {
                provider_metadata: None,
                id: "child-tool".into(),
                name: "pwsh".into(),
                arguments: json!({}),
            }],
            usage: Default::default(),
        })
        .unwrap();
    context
        .append_tool_result("child-tool".into(), "done".into())
        .unwrap();
    let mut snapshot = recover("unused");
    snapshot.context = context.snapshot().unwrap();
    // The checkpoint is settled; historical producing positions remain in its
    // Context State frames, while live SessionState has no active turn/round.
    snapshot.state.turn_id = None;
    snapshot.state.round_id = None;
    let mut persisted_state = serde_json::to_value(&snapshot.state).unwrap();
    persisted_state["last_turn_id"] = json!(2);
    snapshot.state = serde_json::from_value(persisted_state).unwrap();
    let child_writer = journal(&path, &id, snapshot).await;
    let result = manager
        .inspect_child("owner", detail("parent", &id))
        .await
        .unwrap();
    assert_eq!(result["messages"].as_array().unwrap().len(), 3);
    for message in result["messages"].as_array().unwrap() {
        assert_eq!(message["position"], json!({"epoch":0,"turn":2,"round":1}));
    }
    assert_eq!(result["messages"][2]["call_id"], "child-tool");
    assert_eq!(result["messages"][2]["tool_name"], "pwsh");
    writer_remains_exclusive(&path).await;
    drop(child_writer);
}
