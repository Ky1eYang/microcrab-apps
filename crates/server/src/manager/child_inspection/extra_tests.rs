use super::*;

#[tokio::test]
async fn deferred_receipt_link_requires_exact_task_and_selected_tool_result() {
    use microcrab_session::runtime::execution::{ExecutionId, ExecutionSnapshot, ExecutionStatus};
    let mut checkpoint = recover("work");
    checkpoint.context.items.push(ContentItem::Assistant {
        provider_metadata: None,
        content: String::new(),
        tool_calls: vec![microcrab_protocol::ToolCall {
            provider_metadata: None,
            id: "deferred".into(),
            name: "delegate".into(),
            arguments: json!({}),
        }],
    });
    let receipt=json!({"child_id":"0123456789abcdef","subagent":"worker","outcome":{},"usage":{},"tool_tasks":[]}).to_string();
    checkpoint.state.executions.push(ExecutionSnapshot {
        execution_id: ExecutionId(9),
        session_id: "parent".into(),
        call_id: "deferred".into(),
        tool: "delegate".into(),
        agent: "main".into(),
        mode: None,
        status: ExecutionStatus::Succeeded,
        result: Some(receipt),
        failure: None,
        delivery: None,
        capabilities: Default::default(),
        diagnostic: None,
        output: None,
        started_at_unix_ms: None,
        elapsed_ms: None,
        ..ExecutionSnapshot::new(
            "parent".into(),
            "deferred".into(),
            "delegate".into(),
            "main".into(),
            None,
        )
    });
    assert!(receipts(&checkpoint).is_empty());
    checkpoint.context.items.push(ContentItem::Tool {
        parts: vec![],
        call_id: "deferred".into(),
        content: json!({"execution_id":8,"session_id":"parent"}).to_string(),
    });
    assert!(receipts(&checkpoint).is_empty());
    checkpoint.context.items.pop();
    checkpoint.context.items.push(ContentItem::Tool {
        parts: vec![],
        call_id: "deferred".into(),
        content: json!({"execution_id":9,"session_id":"parent"}).to_string(),
    });
    assert!(receipts(&checkpoint).contains_key("0123456789abcdef"));
    checkpoint.state.executions[0].result = None;
    assert!(receipts(&checkpoint).is_empty());
}

#[tokio::test]
async fn linked_child_directory_cannot_escape_the_authorized_parent() {
    let root = tempfile::tempdir().unwrap();
    let manager = fixture(root.path()).await;
    let directory = parent(&manager, "parent").await;
    let other = parent(&manager, "other").await;
    let (id, outside) = microcrab_creator::paths::reserve_child(&other).unwrap();
    let children = directory.join("subsessions");
    std::fs::create_dir(&children).unwrap();
    let link = children.join(&id);
    #[cfg(windows)]
    {
        let status = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&link)
            .arg(&outside)
            .status()
            .unwrap();
        assert!(status.success(), "junction fixture creation failed");
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, &link).unwrap();
    assert!(
        manager
            .inspect_child("owner", detail("parent", &id))
            .await
            .is_err()
    );
    let listed = manager
        .inspect_children("owner", list("parent"))
        .await
        .unwrap();
    assert_eq!(listed["children"][0]["availability"], "unavailable");
    manager.close().await.unwrap();
}

#[tokio::test]
async fn linked_parent_to_foreign_sibling_cannot_disclose_children() {
    let root = tempfile::tempdir().unwrap();
    let manager = fixture(root.path()).await;
    let directory = parent(&manager, "parent").await;
    let mut foreign = manager
        .directory
        .get("owner", "parent")
        .await
        .unwrap()
        .unwrap();
    foreign.id = "foreign".into();
    foreign.owner = "intruder".into();
    manager.directory.create(foreign).await.unwrap();
    let outside = manager.config.data_dir.join("sessions").join("foreign");
    std::fs::create_dir(&outside).unwrap();
    let (id, _) = microcrab_creator::paths::reserve_child(&outside).unwrap();
    let expected = manager
        .config
        .data_dir
        .join("sessions")
        .canonicalize()
        .unwrap()
        .join("parent");
    assert_eq!(directory.canonicalize().unwrap(), expected);
    assert!(std::fs::read_dir(&directory).unwrap().next().is_none());
    std::fs::remove_dir(&directory).unwrap();
    #[cfg(windows)]
    {
        let status = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&directory)
            .arg(&outside)
            .status()
            .unwrap();
        assert!(status.success(), "parent junction fixture creation failed");
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, &directory).unwrap();
    assert!(
        manager
            .inspect_children("owner", list("parent"))
            .await
            .is_err()
    );
    assert!(
        manager
            .inspect_child("owner", detail("parent", &id))
            .await
            .is_err()
    );
    assert!(
        manager
            .inspect_child("owner", detail("foreign", &id))
            .await
            .is_err()
    );
    manager.close().await.unwrap();
}

#[tokio::test]
async fn typed_delegate_binding_preserves_status_and_ledger_usage_without_receipt_body() {
    use microcrab_session::runtime::execution::{
        DelegateChild, ExecutionId, ExecutionSnapshot, ExecutionStatus, OutputMetadata,
    };
    let root = tempfile::tempdir().unwrap();
    let manager = fixture(root.path()).await;
    let directory = parent(&manager, "parent").await;
    let (id, child) = microcrab_creator::paths::reserve_child(&directory).unwrap();
    let child_journal = journal(&child, &id, recover("durable child body")).await;
    child_journal.close().await.unwrap();
    std::fs::write(
        child.join("usage.jsonl"),
        b"{\"input_tokens\":17,\"output_tokens\":3,\"samples\":1}\n",
    )
    .unwrap();
    let mut checkpoint = recover("work");
    checkpoint.context.items.push(ContentItem::Assistant {
        provider_metadata: None,
        content: String::new(),
        tool_calls: vec![microcrab_protocol::ToolCall {
            provider_metadata: None,
            id: "typed-call".into(),
            name: "delegate".into(),
            arguments: json!({}),
        }],
    });
    checkpoint
        .context
        .tools
        .push(microcrab_protocol::ToolDefinition {
            name: "delegate".into(),
            description: "fixture".into(),
            parameters: json!({"type":"object"}),
        });
    checkpoint.context.used_call_ids.insert("typed-call".into());
    let mut row = ExecutionSnapshot::new(
        "parent".into(),
        "typed-call".into(),
        "delegate".into(),
        "main".into(),
        None,
    );
    row.execution_id = ExecutionId(9);
    row.status = ExecutionStatus::Succeeded;
    row.output = Some(OutputMetadata {
        channels: vec![],
        bytes: 0,
        start_cursor: 0,
        end_cursor: 0,
        closed: true,
    });
    row.invocation_result = Some(json!({"execution_id":9,"session_id":"parent"}).to_string());
    row.delegate_child = Some(DelegateChild {
        session_id: id.clone(),
        target: "worker".into(),
    });
    checkpoint.state.executions.push(row);
    assert!(child_executions("parent", &checkpoint, &checkpoint.state.executions).is_empty());
    checkpoint.context.items.push(ContentItem::Tool {
        parts: vec![],
        call_id: "typed-call".into(),
        content: json!({"execution_id":8,"session_id":"parent"}).to_string(),
    });
    assert!(child_executions("parent", &checkpoint, &checkpoint.state.executions).is_empty());
    checkpoint.context.items.pop();
    checkpoint.context.items.push(ContentItem::Tool {
        parts: vec![],
        call_id: "typed-call".into(),
        content: json!({"execution_id":9,"session_id":"parent"}).to_string(),
    });
    assert!(
        child_executions("another-parent", &checkpoint, &checkpoint.state.executions).is_empty()
    );
    assert!(receipts(&checkpoint).is_empty());
    let selected = checkpoint.clone();
    let parent_journal = journal(&directory, "parent", checkpoint).await;
    let before = files(&directory);
    let result = manager
        .inspect_selected_child("owner", detail("parent", &id))
        .await
        .unwrap();
    assert_eq!(result["status"], "succeeded");
    assert_eq!(result["name"], "worker");
    assert_eq!(result["receipt"], Value::Null);
    assert_eq!(result["usage"]["input_tokens"], 17);
    assert_eq!(result["usage"]["output_tokens"], 3);
    assert_eq!(result["messages"][0]["text"], "durable child body");
    let children = manager
        .inspect_children("owner", list("parent"))
        .await
        .unwrap();
    assert_eq!(children["children"][0]["status"], "succeeded");
    assert_eq!(files(&directory), before);
    assert!(manager.registry.lock().unwrap().is_empty());
    assert!(
        manager
            .inspect_child("intruder", detail("parent", &id))
            .await
            .is_err()
    );
    let mut unselected = selected;
    let ContentItem::Tool { content, .. } = unselected.context.items.last_mut().unwrap() else {
        panic!("fixture ends with the selected delegate result");
    };
    *content = "unlinked legacy result".into();
    parent_journal.commit(unselected).await.unwrap();
    assert_eq!(
        manager
            .inspect_selected_child("owner", detail("parent", &id))
            .await
            .unwrap_err()
            .code,
        "permission_denied",
        "retained execution rows cannot authorize an unselected child"
    );
    assert!(manager.registry.lock().unwrap().is_empty());
    parent_journal.close().await.unwrap();
    manager.close().await.unwrap();
}
