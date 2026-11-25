//! No hidden Project, owner/scope fences, and socket-generation reconnect safety.
#[path = "chat_flavors.rs"]
mod chat_flavors;
#[path = "governance.rs"]
mod governance;
use super::super::conversations::{Attach, Binding};
use super::*;
use microcrab_host_protocol::{HostDescriptor, HostFrame, HostRequest};
use serde_json::{Value, json};

fn command(method: &str, params: Value) -> microcrab_protocol::application::Command {
    serde_json::from_value(json!({"v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),"method":method,"params":params})).unwrap()
}
fn binding(id: &str, browser: &str, workspace: &str) -> Binding {
    Binding {
        conversation_id: id.into(),
        browser_id: browser.into(),
        workspace_id: workspace.into(),
        host_flavor: None,
    }
}
fn browser_descriptor(browser: &str, workspace: &str) -> HostDescriptor {
    let mut value = descriptor(browser);
    value.workspace_id = workspace.into();
    value.platform = "browser".into();
    value.workspace_root = "/workspace".into();
    value.descriptor_digest = microcrab_host_protocol::descriptor_fingerprint(&value).unwrap();
    value
}
async fn attach(
    manager: &Arc<Manager>,
    owner: &Principal,
    id: &str,
    socket: &str,
    descriptor: HostDescriptor,
) -> (u64, tokio::task::JoinHandle<()>) {
    let generation = manager.next_connection_generation();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
    let responder_manager = manager.clone();
    let responder_owner = owner.clone();
    let responder_socket = socket.to_owned();
    let responder_id = id.to_owned();
    let responder = tokio::spawn(async move {
        while let Some(frame) = receiver.recv().await {
            let reply = match frame {
                HostFrame::Request {
                    request_id,
                    operation_id,
                    method: HostRequest::Recovery,
                    ..
                } => HostFrame::Response {
                    request_id,
                    operation_id,
                    result: Ok(json!([])),
                },
                HostFrame::Ping { lease_generation } => HostFrame::Pong { lease_generation },
                _ => panic!("reconnect must inspect outcomes without replaying mutations"),
            };
            if responder_manager
                .receive_conversation_host(
                    &responder_owner,
                    &responder_socket,
                    &responder_id,
                    generation,
                    reply,
                )
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        manager.attach_conversation(
            owner,
            socket,
            Attach {
                force_takeover: false,
                host_flavor: None,
                conversation_id: id.into(),
                descriptor,
                lease_generation: format!("lease-{generation}"),
            },
            sender,
            generation,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result["ready"], true);
    assert_eq!(result["scope"]["scope_version"], 3);
    assert!(result["scope"].get("project_id").is_none());
    (generation, responder)
}

#[tokio::test]
async fn projectless_owner_pins_and_generation_survive_disconnect_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let mut configuration = config(root.path());
    configuration.storage = crate::config::Storage::File;
    crate::project::seed_test_projects(&configuration.data_dir, json!([]));
    let manager = Manager::new(configuration.clone()).await.unwrap();
    let owner = Principal::for_test("owner");
    let stranger = Principal::for_test("stranger");
    let id = uuid::Uuid::new_v4().to_string();
    let browser = uuid::Uuid::new_v4().to_string();
    let workspace = uuid::Uuid::new_v4().to_string();
    let scope = manager
        .conversation_scope(&owner.owner, binding(&id, &browser, &workspace))
        .await
        .unwrap();
    assert!(
        manager
            .conversation_scope(&stranger.owner, binding(&id, &browser, &workspace))
            .await
            .is_err()
    );
    assert!(
        manager
            .conversation_scope(
                &owner.owner,
                binding(&id, &uuid::Uuid::new_v4().to_string(), &workspace)
            )
            .await
            .is_err()
    );
    assert!(manager.conversation_host(&owner.owner, &id).await.is_err());
    let descriptor = browser_descriptor(&browser, &workspace);
    let (old_generation, old_responder) =
        attach(&manager, &owner, &id, "old", descriptor.clone()).await;
    let remote = manager
        .conversation_routes
        .lock()
        .await
        .get(&id)
        .unwrap()
        .remote
        .clone();
    assert!(manager.conversation_host(&owner.owner, &id).await.is_ok());
    assert!(
        manager
            .conversation_host(&stranger.owner, &id)
            .await
            .is_err()
    );
    assert!(manager.projects.list(&owner.owner).await.is_empty());
    assert!(manager.pins.project(&id).await.unwrap().is_none());
    let pin = manager.pins.conversation(&id).await.unwrap().unwrap();
    manager
        .pins
        .pin_conversation_session(microcrab_storage::execution::ConversationSessionBinding {
            session_id: id.clone(),
            conversation: pin,
            component_digest: None,
            discovery_digest: microcrab_storage::history::content_hash(&json!([])).unwrap(),
        })
        .await
        .unwrap();
    let record: microcrab_storage::SessionRecord = serde_json::from_value(json!({
        "version":1,"id":id,"owner":owner.owner,"title":"Conversation","project_id":null,"project_root":"/workspace",
        "agent":null,"mode":null,"permissions":[],"review":false,"revision":0,"archived":false,"created_at_ms":1,"updated_at_ms":1
    })).unwrap();
    manager.directory.create(record.clone()).await.unwrap();
    assert!(
        manager
            .record(&owner.owner, &id)
            .await
            .unwrap()
            .project_id
            .is_none()
    );
    assert!(manager.record(&stranger.owner, &id).await.is_err());
    assert!(manager.session_host(&record).await.is_ok());
    manager.detach_conversation_socket("old", Some(&id)).await;
    let snapshot = remote.readiness();
    let error = manager
        .conversation_host(&owner.owner, &id)
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, "host_detached");
    assert_eq!(
        error.message,
        snapshot["cause"]["message"].as_str().unwrap()
    );
    assert_eq!(error.details.as_deref().unwrap()["readiness"], snapshot);
    assert_eq!(error.details.as_deref().unwrap()["conversation_id"], id);
    let session_error = manager.session_host(&record).await.err().unwrap();
    assert!(session_error.to_string().contains(&error.message));
    assert!(manager.record(&owner.owner, &id).await.is_ok());
    let mut restarted = descriptor.clone();
    restarted.bridge_instance_id = "new-worker".into();
    let (current_generation, current_responder) =
        attach(&manager, &owner, &id, "current", restarted).await;
    assert!(Arc::ptr_eq(
        &remote,
        &manager
            .conversation_routes
            .lock()
            .await
            .get(&id)
            .unwrap()
            .remote
    ));
    for (who, socket, generation) in [
        (&owner, "old", old_generation),
        (&owner, "current", old_generation),
        (&stranger, "current", current_generation),
    ] {
        assert!(
            manager
                .receive_conversation_host(
                    who,
                    socket,
                    &id,
                    generation,
                    HostFrame::Pong {
                        lease_generation: format!("lease-{generation}")
                    }
                )
                .await
                .is_err()
        );
    }
    manager.detach_conversation_socket("old", Some(&id)).await;
    assert!(manager.conversation_host(&owner.owner, &id).await.is_ok());
    let (sender, _receiver) = tokio::sync::mpsc::channel(1);
    assert_eq!(
        manager
            .attach_conversation(
                &owner,
                "old",
                Attach {
                    force_takeover: false,
                    host_flavor: None,
                    conversation_id: id.clone(),
                    descriptor: descriptor.clone(),
                    lease_generation: "stale".into()
                },
                sender,
                old_generation
            )
            .await
            .unwrap_err()
            .code,
        "route_replaced"
    );
    let mut changed = descriptor.clone();
    changed.workspace_root = "/other".into();
    changed.descriptor_digest = microcrab_host_protocol::descriptor_fingerprint(&changed).unwrap();
    let (sender, _receiver) = tokio::sync::mpsc::channel(1);
    assert_eq!(
        manager
            .attach_conversation(
                &owner,
                "wrong",
                Attach {
                    force_takeover: false,
                    host_flavor: None,
                    conversation_id: id.clone(),
                    descriptor: changed,
                    lease_generation: "new".into()
                },
                sender,
                manager.next_connection_generation()
            )
            .await
            .unwrap_err()
            .code,
        "binding_mismatch"
    );
    let listed = commands::execute(&manager, &owner, &command("session.list", json!({})))
        .await
        .unwrap();
    assert_eq!(listed["items"][0]["project_id"], Value::Null);
    assert_eq!(listed["items"][0]["host_connected"], true);
    manager.close().await.unwrap();
    for task in [old_responder, current_responder] {
        task.abort();
        let _ = task.await;
    }
    let reopened = Manager::new(configuration).await.unwrap();
    assert_eq!(
        reopened
            .conversation_scope(&owner.owner, binding(&id, &browser, &workspace))
            .await
            .unwrap(),
        scope
    );
    assert!(
        reopened
            .record(&owner.owner, &id)
            .await
            .unwrap()
            .project_id
            .is_none()
    );
    assert!(reopened.session_host(&record).await.is_err());
    assert!(reopened.projects.list(&owner.owner).await.is_empty());
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn conversation_workspace_cannot_be_leased_by_another_scope_or_missing_binding() {
    let root = tempfile::tempdir().unwrap();
    let mut configuration = config(root.path());
    configuration.storage = crate::config::Storage::File;
    let manager = Manager::new(configuration).await.unwrap();
    let owner = Principal::for_test("owner");
    let first = uuid::Uuid::new_v4().to_string();
    let second = uuid::Uuid::new_v4().to_string();
    let browser = uuid::Uuid::new_v4().to_string();
    let workspace = uuid::Uuid::new_v4().to_string();
    let missing = command(
        "session.create",
        json!({"project_id":null,"conversation_id":first,"operation_id":uuid::Uuid::new_v4().to_string()}),
    );
    assert!(
        commands::create(&manager, &owner.owner, &missing)
            .await
            .is_err()
    );
    for id in [&first, &second] {
        manager
            .conversation_scope(&owner.owner, binding(id, &browser, &workspace))
            .await
            .unwrap();
    }
    let descriptor = browser_descriptor(&browser, &workspace);
    let (_, responder) = attach(&manager, &owner, &first, "first", descriptor.clone()).await;
    let (sender, _receiver) = tokio::sync::mpsc::channel(1);
    assert_eq!(
        manager
            .attach_conversation(
                &owner,
                "second",
                Attach {
                    force_takeover: false,
                    host_flavor: None,
                    conversation_id: second,
                    descriptor,
                    lease_generation: "lease".into()
                },
                sender,
                manager.next_connection_generation()
            )
            .await
            .unwrap_err()
            .code,
        "workspace_in_use"
    );
    assert!(
        manager
            .conversation_host(&owner.owner, &first)
            .await
            .is_ok()
    );
    manager.close().await.unwrap();
    responder.abort();
    let _ = responder.await;
}
