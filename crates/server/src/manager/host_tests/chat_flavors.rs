//! Flavor admission, restoration and cross-language Host identity contract.
use super::super::super::conversation_flavors::HostFlavor;
use super::*;

fn golden() -> Value {
    serde_json::from_str(include_str!("fixtures/chat-only-host-descriptor.json")).unwrap()
}
#[test]
fn frontend_chat_descriptor_matches_rust_typed_canonical_bytes() {
    let fixture = golden();
    let descriptor: HostDescriptor = serde_json::from_value(fixture["descriptor"].clone()).unwrap();
    assert_eq!(
        microcrab_host_protocol::descriptor_fingerprint(&descriptor).unwrap(),
        fixture["digest"]
    );
    let mut canonical = descriptor.clone();
    canonical.bridge_instance_id.clear();
    canonical.descriptor_digest.clear();
    assert_eq!(
        serde_json::to_string(&canonical).unwrap(),
        fixture["canonical"]
    );
    assert!(
        microcrab_host_protocol::descriptor_matches_pin(
            &descriptor,
            descriptor.descriptor_digest.as_str()
        )
        .unwrap()
    );
}

#[tokio::test]
async fn immutable_chat_flavor_precedes_attach_and_survives_restart() {
    let root = tempfile::tempdir().unwrap();
    let mut configuration = config(root.path());
    configuration.storage = crate::config::Storage::File;
    crate::project::seed_test_projects(&configuration.data_dir, json!([]));
    let manager = Manager::new(configuration.clone()).await.unwrap();
    let owner = Principal::for_test("owner");
    let id = uuid::Uuid::new_v4().to_string();
    let browser = uuid::Uuid::new_v4().to_string();
    let workspace = uuid::Uuid::new_v4().to_string();
    let mut params = binding(&id, &browser, &workspace);
    params.host_flavor = Some(HostFlavor::ChatOnly);
    let scope = manager
        .conversation_scope(&owner.owner, params)
        .await
        .unwrap();
    let wire = serde_json::to_value(&scope).unwrap();
    assert_eq!(wire["host_flavor"], "chat_only");
    assert_eq!(wire["scope_kind"], "conversation");
    assert_eq!(
        manager.conversation_flavors.get(&owner.owner, &id),
        Some(HostFlavor::ChatOnly)
    );
    assert!(
        manager
            .pins
            .conversation_grant(&id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(manager.pins.conversation(&id).await.unwrap().is_none());
    let mut switched = binding(&id, &browser, &workspace);
    switched.host_flavor = Some(HostFlavor::BrowserSandbox);
    assert_eq!(
        manager
            .conversation_scope(&owner.owner, switched)
            .await
            .unwrap_err()
            .code,
        "binding_mismatch"
    );
    let (sender, _receiver) = tokio::sync::mpsc::channel(1);
    let error = manager
        .attach_conversation(
            &owner,
            "execution",
            Attach {
                conversation_id: id.clone(),
                descriptor: browser_descriptor(&browser, &workspace),
                lease_generation: "lease".into(),
                host_flavor: Some(HostFlavor::ChatOnly),
            },
            sender,
            manager.next_connection_generation(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "binding_mismatch");
    let mut descriptor: HostDescriptor =
        serde_json::from_value(golden()["descriptor"].clone()).unwrap();
    descriptor.host_id = browser.clone();
    descriptor.workspace_id = workspace.clone();
    descriptor.descriptor_digest =
        microcrab_host_protocol::descriptor_fingerprint(&descriptor).unwrap();
    let (_, responder) = attach(&manager, &owner, &id, "chat", descriptor).await;
    let record: microcrab_storage::SessionRecord = serde_json::from_value(json!({
        "version":1,"id":id,"owner":owner.owner,"title":"Chat","project_id":null,"project_root":"/chat",
        "agent":null,"mode":null,"permissions":[],"review":false,"revision":0,"archived":false,"created_at_ms":1,"updated_at_ms":1
    })).unwrap();
    let projected = super::super::super::view::session(&manager, &record, "model", "idle");
    assert_eq!(projected["host_flavor"], "chat_only");
    let mut unrelated = record.clone();
    unrelated.id = uuid::Uuid::new_v4().to_string();
    assert!(
        super::super::super::view::session(&manager, &unrelated, "model", "idle")
            .get("host_flavor")
            .is_none()
    );
    manager.close().await.unwrap();
    responder.abort();
    let _ = responder.await;
    let reopened = Manager::new(configuration).await.unwrap();
    assert_eq!(
        reopened
            .conversation_scope(&owner.owner, binding(&id, &browser, &workspace))
            .await
            .unwrap(),
        scope
    );
    reopened.close().await.unwrap();
}
