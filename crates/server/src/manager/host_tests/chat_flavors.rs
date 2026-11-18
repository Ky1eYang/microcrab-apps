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
    let pin = manager.pins.conversation(&id).await.unwrap().unwrap();
    assert_eq!(pin.workspace_root, "/chat");
    assert_eq!(wire["server_owned"], true);
    let host = manager.conversation_host(&owner.owner, &id).await.unwrap();
    assert!(host.capabilities().supported_operations.is_empty());
    assert!(host.tool_capabilities().is_empty());
    assert!(host.info().unwrap().capabilities.is_empty());
    let again = manager.conversation_host(&owner.owner, &id).await.unwrap();
    assert!(Arc::ptr_eq(&host, &again));
    assert!(manager.conversation_routes.lock().await.get(&id).is_none());
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
                force_takeover: false,
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

struct AnswerClient;
#[async_trait::async_trait]
impl microcrab_session::Client for AnswerClient {
    async fn complete(
        &self,
        _content: &microcrab_transport::Content,
        _auditor: &dyn microcrab_auditor::Auditor,
        _variables: &dyn microcrab_auditor::VariableView,
        _cancellation: CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        Ok(microcrab_protocol::Response {
            content: "A complete host-independent answer.".into(),
            tool_calls: vec![],
            usage: Default::default(),
            provider_metadata: None,
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires root-built Universal component path in MICROCRAB_TEST_UNIVERSAL_COMPONENT"]
async fn maintained_chat_only_server_host_survives_catalog_init_turn_compaction_and_restart_without_browser()
 {
    let root = tempfile::tempdir().unwrap();
    let mut configuration = config(root.path());
    configuration.storage = crate::config::Storage::File;
    configuration.agent = Some(serde_json::from_value(json!({
        "path":std::env::var("MICROCRAB_TEST_UNIVERSAL_COMPONENT").expect("root-built Universal component path"),
        "name":"universal","mode":"execute"
    })).unwrap());
    crate::project::seed_test_projects(&configuration.data_dir, json!([]));
    let manager = Manager::initialize(
        configuration.clone(),
        Arc::new(crate::noop::NoopHost),
        Some(Arc::new(AnswerClient)),
        false,
    )
    .await
    .unwrap();
    let owner = Principal::for_test("owner");
    let id = uuid::Uuid::new_v4().to_string();
    let browser = uuid::Uuid::new_v4().to_string();
    let workspace = uuid::Uuid::new_v4().to_string();
    let mut scope = binding(&id, &browser, &workspace);
    scope.host_flavor = Some(HostFlavor::ChatOnly);
    manager
        .conversation_scope(&owner.owner, scope)
        .await
        .unwrap();
    let catalog = commands::execute(
        &manager,
        &owner,
        &command("agent.catalog", json!({"conversation_id":id})),
    )
    .await
    .unwrap();
    assert_eq!(catalog["items"][0]["tools"], json!([]));
    commands::create(
        &manager,
        &owner.owner,
        &command(
            "session.create",
            json!({
                "conversation_id":id,"agent":"universal","mode":"execute",
                "operation_id":uuid::Uuid::new_v4().to_string()
            }),
        ),
    )
    .await
    .unwrap();
    let managed = manager
        .active(manager.record(&owner.owner, &id).await.unwrap(), true)
        .await
        .unwrap();
    assert!(managed.session.agent_initialization_complete());
    let record = managed.record.lock().await.clone();
    assert!(Arc::ptr_eq(
        &managed.host,
        &manager.record_host(&record).await.unwrap()
    ));
    let answer = managed.session.submit("hello").await.unwrap();
    assert_eq!(answer.content, "A complete host-independent answer.");
    managed
        .session
        .compact_with_model("Summarize the conversation.", json!({}))
        .await
        .unwrap();
    assert!(managed.session.compaction().is_some());
    assert!(manager.conversation_routes.lock().await.is_empty());
    manager.close().await.unwrap();
    let restarted = Manager::initialize(
        configuration,
        Arc::new(crate::noop::NoopHost),
        Some(Arc::new(AnswerClient)),
        false,
    )
    .await
    .unwrap();
    let restored = restarted
        .active(restarted.record(&owner.owner, &id).await.unwrap(), true)
        .await
        .unwrap();
    let answer = restored.session.submit("continue").await.unwrap();
    assert_eq!(answer.content, "A complete host-independent answer.");
    assert!(restarted.conversation_routes.lock().await.is_empty());
    restarted.close().await.unwrap();
}
