//! Main server assembly and authenticated owner/socket callback boundaries.
use super::super::super::{Manager, commands};
use super::*;

fn command(method: &str, id: Option<&str>, params: Value) -> Command {
    serde_json::from_value(json!({"v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),"method":method,"session_id":id,"params":params})).unwrap()
}
#[tokio::test]
async fn main_assembly_owner_snapshot_socket_reply_and_closed_parent() {
    let root = tempfile::tempdir().unwrap();
    let mut config_value: Value =
        serde_json::from_str(include_str!("../../../../config.example.json")).unwrap();
    config_value["agent"] = Value::Null;
    config_value["auto_rename"] = json!(false);
    config_value["provider"]["endpoint"] = json!("http://127.0.0.1:1/v1/chat/completions");
    let mut config: crate::config::Config = serde_json::from_value(config_value).unwrap();
    config.resolve(root.path()).unwrap();
    #[cfg(not(feature = "storage-sqlite"))]
    {
        config.storage = crate::config::Storage::File;
    }
    crate::project::seed_test_projects(
        &config.data_dir,
        json!([
            {"id":"workspace","name":"Workspace","root":".","owners":["owner"]}
        ]),
    );
    let manager = Manager::with_host(config, Arc::new(crate::noop::NoopHost))
        .await
        .unwrap();
    let owner = Principal::for_test("owner");
    let intruder = Principal::for_test("intruder");
    let created = commands::create(
        &manager,
        "owner",
        &command(
            "session.create",
            None,
            json!({"project_id":"workspace","operation_id":uuid::Uuid::new_v4().to_string()}),
        ),
    )
    .await
    .unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let snapshot = manager.permission_requests(&owner, id).await.unwrap();
    assert_eq!(snapshot["interactive_ask"], true);
    assert_eq!(snapshot["executor_connected"], false);
    let bind=manager.client_tool_command(&owner,"socket_a",&command("client_tools.bind",Some(id),json!({"client_id":"client_a","catalog_digest":managed.client_tools.snapshot().await.catalog_digest}))).await.unwrap();
    let lease = bind["lease_id"].as_str().unwrap();
    let mut events = managed.hub.subscribe();
    let bridge = managed.permission_ask.clone();
    let child_id = "native_child_request";
    let waiting = tokio::spawn(async move { bridge.ask(&request(child_id, "child-call")).await });
    let row = event(&mut events, "permission.requested").await;
    let request_id = row["request"]["request_id"].as_str().unwrap();
    let snapshot = manager.pending_requests(&owner, id).await["permissions"].clone();
    assert_eq!(snapshot["requests"][0]["session_id"], child_id);
    assert!(manager.permission_requests(&intruder, id).await.is_err());
    let reply = command(
        "permission.reply",
        Some(id),
        json!({"request_id":request_id,"lease_id":lease,"decision":"allow"}),
    );
    assert!(
        manager
            .permission_reply(&intruder, "socket_a", &reply)
            .await
            .is_err()
    );
    assert!(
        manager
            .permission_reply(&owner, "observer", &reply)
            .await
            .is_err()
    );
    assert_eq!(
        commands::execute(&manager, &owner, &reply)
            .await
            .unwrap_err()
            .code,
        "socket_required"
    );
    manager
        .permission_reply(&owner, "socket_a", &reply)
        .await
        .unwrap();
    assert_eq!(waiting.await.unwrap().unwrap().action, Action::Allow);
    assert_eq!(
        manager
            .permission_reply(&owner, "socket_a", &reply)
            .await
            .unwrap()["duplicate"],
        true
    );
    let invalid = command(
        "permission.reply",
        Some(id),
        json!({"request_id":request_id,"lease_id":lease,"decision":"always"}),
    );
    assert!(
        manager
            .permission_reply(&owner, "socket_a", &invalid)
            .await
            .is_err()
    );
    manager.close_managed(&managed).await.unwrap();
    let snapshot = manager.permission_requests(&owner, id).await.unwrap();
    assert_eq!(snapshot["requests"], json!([]));
    assert_eq!(snapshot["interactive_ask"], false);
    assert!(
        manager
            .permission_requests(&owner, "unavailable-child-id")
            .await
            .is_err()
    );
    manager.close().await.unwrap();
}
