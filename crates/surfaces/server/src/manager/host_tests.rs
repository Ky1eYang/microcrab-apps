//! Startup policy and backend injection contracts. Injection uses no LocalHost
//! or interpreter; real storage exercises restoration across manager lifetimes.
#![cfg(feature = "storage-file")]
use super::*;
mod connection;
mod conversations;
mod direct;
mod init_retry;
mod native_isolation;
mod outbound;
use microcrab_agent_types::{AgentInfo, AgentMode, AgentRole, Variable};
use microcrab_creator::delegate::{NativeChildFactory, NativeDelegateConfig};
use microcrab_host_api::{FsApi, HostAPI, NetApi, ProcessApi, ShellApi, ShellRequest, ToolError};
use microcrab_session::runtime::Runtime;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct TestHost;
impl ProcessApi for TestHost {}
impl FsApi for TestHost {}
impl NetApi for TestHost {}
impl ShellApi for TestHost {}
impl HostAPI for TestHost {}
impl Host for TestHost {}

fn config(root: &std::path::Path) -> Config {
    std::fs::create_dir_all(root.join("project-a")).unwrap();
    std::fs::create_dir_all(root.join("project-b")).unwrap();
    let mut config: Config = serde_json::from_value(serde_json::json!({
        "data_dir":root.join(".data"),
        "provider":{"endpoint":"http://127.0.0.1:1/v1/chat/completions","model":"test"},
        "auto_rename":false,
        "auth":{
            "issuer":"http://127.0.0.1:9900",
            "authorization_endpoint":"http://127.0.0.1:9900/authorize",
            "token_endpoint":"http://127.0.0.1:9900/token",
            "jwks_uri":"http://127.0.0.1:9900/jwks",
            "client_id":"test",
            "redirect_uri":"http://127.0.0.1:4096/api/auth/callback",
            "allowed_origins":["http://127.0.0.1:4096"],
            "jwt_secret":"test-key-at-least-thirty-two-characters"
        }
    }))
    .unwrap();
    config.resolve(root).unwrap();
    crate::project::seed_test_projects(
        &config.data_dir,
        serde_json::json!([
            {"id":"a","name":"Project A","root":root.join("project-a"),"owners":["owner"]},
            {"id":"b","name":"Project B","root":root.join("project-b"),"owners":["owner"]}
        ]),
    );
    config
}

async fn create(manager: &Arc<Manager>, project: &str) -> Arc<Managed> {
    let command = serde_json::from_value(serde_json::json!({
        "v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),
        "method":"session.create","params":{
            "project_id":project,"operation_id":uuid::Uuid::new_v4().to_string()
        }
    }))
    .unwrap();
    let created = commands::create(manager, "owner", &command).await.unwrap();
    let record = manager
        .record("owner", created["id"].as_str().unwrap())
        .await
        .unwrap();
    manager.active(record, false).await.unwrap()
}

#[tokio::test]
async fn durable_project_api_is_authoritative_without_startup_assignments() {
    let root = tempfile::tempdir().unwrap();
    let configuration = config(root.path());
    crate::project::seed_test_projects(&configuration.data_dir, serde_json::json!([]));
    let manager = Manager::new(configuration.clone()).await.unwrap();
    let principal = Principal::for_test("owner");
    let command = |method: &str, params: serde_json::Value| {
        serde_json::from_value(serde_json::json!({
            "v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),
            "method":method,"params":params
        }))
        .unwrap()
    };
    let listed = commands::execute(
        &manager,
        &principal,
        &command("project.list", serde_json::json!({})),
    )
    .await
    .unwrap();
    assert_eq!(listed["items"], serde_json::json!([]));
    assert!(!manager.project_allowed("owner", "a"));
    commands::execute(
        &manager,
        &principal,
        &command(
            "project.create",
            serde_json::json!({
                "id":"a","name":"Dynamic project","root":"bridge-only://workspace"
            }),
        ),
    )
    .await
    .unwrap();
    assert!(manager.project_allowed("owner", "a"));
    assert!(!manager.project_allowed("other", "a"));
    manager.close().await.unwrap();
    drop(manager);

    let manager = Manager::new(configuration).await.unwrap();
    let listed = commands::execute(
        &manager,
        &principal,
        &command("project.list", serde_json::json!({})),
    )
    .await
    .unwrap();
    assert_eq!(listed["items"].as_array().unwrap().len(), 1);
    assert_eq!(listed["items"][0]["root"], "bridge-only://workspace");
    commands::execute(
        &manager,
        &principal,
        &command(
            "project.rename",
            serde_json::json!({
                "project_id":"a","name":"Renamed"
            }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(manager.projects.find("a").await.unwrap().name, "Renamed");
    commands::execute(
        &manager,
        &principal,
        &command(
            "project.delete",
            serde_json::json!({
                "project_id":"a"
            }),
        ),
    )
    .await
    .unwrap();
    assert!(!manager.project_allowed("owner", "a"));
    assert!(manager.projects.find("a").await.unwrap().archived);
    manager.close().await.unwrap();
}

#[tokio::test]
async fn startup_backend_uses_startup_namespace_and_protects_server_paths() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    std::fs::write(root.path().join("startup.txt"), "startup").unwrap();
    std::fs::write(config.data_dir.join("private.txt"), "private").unwrap();
    std::fs::write(config.frontend_dir.join("index.html"), "frontend").unwrap();
    let private_config = root.path().join("server.json");
    std::fs::write(&private_config, "private config").unwrap();
    config.private_paths.push(private_config);
    let selected = root.path().join("selected-component.wasm");
    std::fs::write(&selected, b"private component").unwrap();
    config.agent = Some(crate::config::Agent {
        path: selected.clone(),
        name: None,
        mode: None,
        restore_only: false,
    });
    let host = crate::host::startup_local_host(&config).unwrap();
    assert_eq!(
        host.read_file("startup.txt".into(), 1024, CancellationToken::new())
            .await
            .unwrap(),
        b"startup"
    );
    for path in [
        config.data_dir.join("private.txt"),
        config.frontend_dir.join("index.html"),
        root.path().join("server.json"),
        selected,
    ] {
        let error = host
            .read_file(path.canonicalize().unwrap(), 1024, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(
            &error,
            ToolError::Host(cause) if cause.category == microcrab_host_api::ErrorCategory::Permission
        ));
        assert_eq!(
            error.public_reason(),
            Some("target path is protected by Host private-directory policy")
        );
        for private in ["private config", "private component", "private.txt"] {
            assert!(!error.to_string().contains(private));
        }
    }
    assert!(
        !host
            .tool_capabilities()
            .iter()
            .any(|capability| matches!(capability.as_str(), "shell" | "start_job"))
    );
    assert!(matches!(
        microcrab_host_api::ShellApi::launch(
            host.as_ref(),
            &microcrab_host_api::OpContext::default(),
            ShellRequest {
                command: "unused".into(),
                cwd: None,
                output_bytes: 1024,
            }
        )
        .await,
        Err(microcrab_host_api::HostError {
            category: microcrab_host_api::ErrorCategory::Unsupported,
            ..
        })
    ));
}

#[tokio::test]
async fn injected_backend_is_shared_across_projects_children_and_restoration() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    // An injected backend owns its namespace. Neither startup nor project labels
    // require a usable local execution root when assembling this Manager.
    config.startup_dir = root.path().join("absent-startup-root");
    crate::project::seed_test_projects(
        &config.data_dir,
        serde_json::json!([
            {"id":"a","name":"Project A","root":root.path().join("absent-a"),"owners":["owner"]},
            {"id":"b","name":"Project B","root":root.path().join("absent-b"),"owners":["owner"]}
        ]),
    );
    let backend = Arc::new(TestHost);
    let host: Arc<dyn Host> = backend.clone();
    let manager = Manager::with_host(config.clone(), host.clone())
        .await
        .unwrap();
    let first = create(&manager, "a").await;
    let second = create(&manager, "b").await;
    assert!(Arc::ptr_eq(&host, &manager.host));
    assert!(Arc::ptr_eq(&first.host, &second.host));
    assert!(Arc::ptr_eq(&host, &first.host));
    assert_eq!(first.record.lock().await.project_id.as_deref(), Some("a"));
    assert_eq!(second.record.lock().await.project_id.as_deref(), Some("b"));
    let id = first.record.lock().await.id.clone();
    assert!(manager.record("intruder", &id).await.is_err());
    first.session.checkpoint().await.unwrap();
    second.session.checkpoint().await.unwrap();

    let child_catalog = vec![AgentInfo {
        tool_media_output_bytes: Default::default(),
        name: "reviewer".into(),
        modes: vec![AgentMode {
            description: String::new(),
            prompt: String::new(),
            name: "review".into(),
            role: AgentRole::Both,
            permissions: vec![],
        }],
        tools: vec![],
        variables: Variable::default(),
    }];
    struct DelegateModel(std::sync::atomic::AtomicBool);
    #[async_trait::async_trait]
    impl microcrab_session::Client for DelegateModel {
        async fn complete(
            &self,
            _: &microcrab_session::context::Content,
            _: &dyn microcrab_session::Auditor,
            _: &dyn microcrab_session::VariableView,
            _: CancellationToken,
        ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
            Ok(microcrab_protocol::Response {
                content: "host fixture".into(),
                tool_calls: if self.0.swap(false, Ordering::AcqRel) {
                    vec![microcrab_protocol::ToolCall {
                        id: "delegate-host".into(),
                        name: "delegate".into(),
                        arguments: serde_json::json!({"target":"reviewer","mode":"review","input":"host ownership"}),
                        provider_metadata: None,
                    }]
                } else {
                    vec![]
                },
                provider_metadata: None,
                usage: microcrab_protocol::Usage::default(),
            })
        }
    }
    let parent_runtime = Runtime::new(
        vec![microcrab_session::runtime::ToolRegistration::Builtin(
            microcrab_session::runtime::BuiltinTool::Delegate,
        )],
        Arc::new(microcrab_session::runtime::tool::AllowTools::new([
            "delegate",
        ])),
    )
    .unwrap();
    let children = Children::default();
    let factory = Arc::new(NativeChildFactory::new(NativeDelegateConfig {
        children: children.clone(),
        aggregate: UsageLedger::open(None).unwrap(),
        client: Arc::new(DelegateModel(std::sync::atomic::AtomicBool::new(false))),
        host: first.host.clone(),
        wasm: vec![],
        external: vec![],
        catalog: child_catalog.clone(),
        permissions: parent_runtime.permission_controller(),
        limits: microcrab_session::Limits::default(),
        directory: config.data_dir.join("child-test"),
        disk: true,
        system: None,
        auditors: None,
        journals: None,
    }));
    let parent = microcrab_session::Session::builder(
        DelegateModel(std::sync::atomic::AtomicBool::new(true)),
        parent_runtime.with_delegate_executor(Arc::new(
            microcrab_creator::delegate::OwnedDelegateExecutor::new(children.clone(), factory),
        )),
    )
    .session_id("host-adoption-parent")
    .host(first.host.clone())
    .catalog(child_catalog)
    .build()
    .unwrap();
    parent.submit("create the owned child").await.unwrap();
    let mut changes = parent.executions().subscribe();
    let child = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let row = parent
                .execution_snapshots()
                .into_iter()
                .find(|row| row.tool == "delegate")
                .expect("delegate admitted");
            if let Some(binding) = &row.delegate_child
                && let Ok(child) = parent.inspect_child(&binding.session_id, "reviewer")
            {
                break child;
            }
            assert!(
                row.status.is_pending(),
                "delegate failed before child publication: {:?}",
                row.failure
            );
            changes.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let mut activity = child.subscribe_activity();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while activity.borrow().outcome_sequence < 1 {
            activity.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(
        child
            .communication_identity()
            .unwrap()
            .parent_session_id
            .as_deref(),
        Some("host-adoption-parent")
    );
    child.checkpoint().await.unwrap();
    parent.close().await.unwrap();
    children.close().await.unwrap();
    manager.close().await.unwrap();
    drop(first);
    drop(second);
    drop(manager);

    let reopened = Manager::with_host(config, host.clone()).await.unwrap();
    let restored = reopened
        .active(reopened.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&host, &restored.host));
    restored.session.checkpoint().await.unwrap();
    reopened.close().await.unwrap();
}

fn descriptor(host_id: &str) -> microcrab_host_protocol::HostDescriptor {
    let mut descriptor = microcrab_host_protocol::HostDescriptor {
        protocol_version: microcrab_host_protocol::PROTOCOL_VERSION,
        host_id: host_id.into(),
        workspace_id: "workspace".into(),
        workspace_root: "/bridge/work".into(),
        bridge_instance_id: "instance".into(),
        platform: "linux".into(),
        info: microcrab_host_api::HostInfo {
            os_family: microcrab_host_api::OsFamily::Unix,
            os: "fixture".into(),
            arch: "fixture".into(),
            shell: "none".into(),
            path_separator: "/".into(),
            newline: "\n".into(),
            capabilities: vec![],
            resources: None,
            virtual_shell: None,
            description: None,
        },
        tool_capabilities: vec![],
        supported_primitives: vec![],
        descriptor_digest: String::new(),
    };
    descriptor.descriptor_digest =
        microcrab_host_protocol::descriptor_fingerprint(&descriptor).unwrap();
    descriptor
}
async fn attach_fixture(
    manager: &Arc<Manager>,
    owner: &Principal,
    socket: &str,
    descriptor: microcrab_host_protocol::HostDescriptor,
) -> (u64, tokio::task::JoinHandle<()>) {
    use microcrab_host_protocol::{HostFrame, HostRequest};
    let generation = manager.next_connection_generation();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
    let reply_manager = manager.clone();
    let reply_owner = owner.clone();
    let reply_socket = socket.to_owned();
    let responder = tokio::spawn(async move {
        while let Some(frame) = receiver.recv().await {
            let reply = match frame {
                HostFrame::Request {
                    request_id,
                    operation_id,
                    method: HostRequest::Recovery,
                    ..
                } => Some(HostFrame::Response {
                    request_id,
                    operation_id,
                    result: Ok(serde_json::json!([])),
                }),
                HostFrame::Ping { lease_generation } => Some(HostFrame::Pong { lease_generation }),
                _ => None,
            };
            if let Some(reply) = reply
                && reply_manager
                    .receive_host(&reply_owner, &reply_socket, "a", generation, reply)
                    .await
                    .is_err()
            {
                // A replaced socket's late response is rejected normally.
                break;
            }
        }
    });
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        manager.attach_project(
            owner,
            socket,
            super::bindings::Attach {
                project_id: "a".into(),
                descriptor,
                lease_generation: generation.to_string(),
                allow_legacy_binding: true,
            },
            sender,
            generation,
            super::bindings::ProjectCarrierMode::BrowserRelay,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result["ready"], true);
    // Keep the paired lane alive through the caller's execution assertions.
    // Dropping its receiver would correctly make RemoteHost disconnected.
    (generation, responder)
}

#[tokio::test]
async fn default_server_is_hostless_and_does_not_load_local_component() {
    let root = tempfile::tempdir().unwrap();
    let mut configuration = config(root.path());
    configuration.startup_dir = root.path().join("missing-startup");
    configuration.agent = Some(crate::config::Agent {
        path: root.path().join("missing-component.wasm"),
        name: None,
        mode: None,
        restore_only: false,
    });
    let manager = Manager::new(configuration).await.unwrap();
    assert!(!manager.embedded);
    assert!(manager.host.tool_capabilities().is_empty());
    let command = serde_json::from_value(serde_json::json!({"v":1,"type":"command","request_id":"create","method":"session.create","params":{"project_id":"a","operation_id":uuid::Uuid::new_v4().to_string()}})).unwrap();
    let error = commands::create(&manager, "owner", &command)
        .await
        .unwrap_err();
    assert_eq!(error.code, "host_detached");
    assert!(!error.retryable);
    assert!(error.message.contains("pinned project Host"));
    assert!(error.message.contains("explicitly retry"));
    assert!(error.message.contains("does not replay"));
    assert_eq!(
        error.details.as_deref(),
        Some(&serde_json::json!({"project_id": "a", "readiness": null}))
    );
    let denied = manager
        .project_host("other-owner", "a")
        .await
        .err()
        .unwrap();
    assert_ne!(denied.code, "host_detached");
    assert!(denied.details.is_none());
    assert!(!root.path().join("missing-startup").exists());
    manager.close().await.unwrap();
}

#[tokio::test]
async fn project_binding_refuses_second_owner_host_root_and_stale_socket() {
    use microcrab_host_protocol::HostFrame;
    let root = tempfile::tempdir().unwrap();
    let configuration = config(root.path());
    crate::project::seed_test_projects(
        &configuration.data_dir,
        serde_json::json!([
            {"id":"a","name":"Project A","root":root.path().join("project-a"),"owners":["owner","second"]},
            {"id":"b","name":"Project B","root":root.path().join("project-b"),"owners":["owner"]}
        ]),
    );
    let manager = Manager::new(configuration.clone()).await.unwrap();
    let principal = Principal::for_test("owner");
    let first_scope = manager.binding_scope("owner", "a").await.unwrap();
    // PATCH and DELETE share editable_project. A durable binding grant constrains
    // edits even when the registry record lists another owner.
    assert!(manager.editable_project("second", "a").await.is_err());
    manager.editable_project("owner", "a").await.unwrap();
    assert!(manager.editable_project("second", "a").await.is_err());
    assert!(manager.binding_scope("second", "a").await.is_err());
    assert!(
        manager
            .binding_scope("owner", "not-assigned")
            .await
            .is_err()
    );
    let (first, first_responder) =
        attach_fixture(&manager, &principal, "socket-1", descriptor("host")).await;
    for mut changed in [descriptor("other-host"), descriptor("host")] {
        if changed.host_id == "host" {
            changed.workspace_root = "/different".into();
            changed.descriptor_digest =
                microcrab_host_protocol::descriptor_fingerprint(&changed).unwrap();
        }
        let (sender, _) = tokio::sync::mpsc::channel(64);
        let generation = manager.next_connection_generation();
        let error = manager
            .attach_project(
                &principal,
                "attacker",
                super::bindings::Attach {
                    project_id: "a".into(),
                    descriptor: changed,
                    lease_generation: "lease".into(),
                    allow_legacy_binding: false,
                },
                sender,
                generation,
                super::bindings::ProjectCarrierMode::BrowserRelay,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "binding_mismatch");
    }
    let original_pin = manager.pins.project("a").await.unwrap().unwrap();
    let mut upgraded = descriptor("host");
    upgraded.tool_capabilities.push("cache_point".into());
    upgraded.info.capabilities.push("cache_point".into());
    upgraded.descriptor_digest =
        microcrab_host_protocol::descriptor_fingerprint(&upgraded).unwrap();
    let (sender, _) = tokio::sync::mpsc::channel(64);
    let generation = manager.next_connection_generation();
    let error = manager
        .attach_project(
            &principal,
            "capability-mismatch",
            super::bindings::Attach {
                project_id: "a".into(),
                descriptor: upgraded,
                lease_generation: generation.to_string(),
                allow_legacy_binding: false,
            },
            sender,
            generation,
            super::bindings::ProjectCarrierMode::BrowserRelay,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "binding_mismatch");
    assert!(manager.routes.lock().await["a"].ready);
    let (second, second_responder) =
        attach_fixture(&manager, &principal, "socket-2", descriptor("host")).await;
    assert_eq!(
        manager.pins.project("a").await.unwrap().unwrap(),
        original_pin
    );
    assert!(second > first);
    assert!(
        manager
            .receive_host(
                &principal,
                "socket-1",
                "a",
                first,
                HostFrame::Pong {
                    lease_generation: first.to_string()
                }
            )
            .await
            .is_err()
    );
    assert!(
        manager
            .receive_host(
                &Principal::for_test("second"),
                "socket-2",
                "a",
                second,
                HostFrame::Pong {
                    lease_generation: second.to_string()
                }
            )
            .await
            .is_err()
    );
    assert!(
        manager
            .receive_host(
                &principal,
                "socket-2",
                "b",
                second,
                HostFrame::Pong {
                    lease_generation: second.to_string()
                }
            )
            .await
            .is_err()
    );
    manager.detach_socket("socket-1", None).await;
    assert!(manager.routes.lock().await["a"].ready);
    manager.detach_socket("socket-2", None).await;
    assert!(!manager.routes.lock().await["a"].remote.is_connected());
    first_responder.abort();
    second_responder.abort();
    let _ = first_responder.await;
    let _ = second_responder.await;
    manager.close().await.unwrap();
    drop(manager);
    let reopened = Manager::new(configuration).await.unwrap();
    assert_eq!(
        first_scope,
        reopened.binding_scope("owner", "a").await.unwrap()
    );
    assert!(reopened.binding_scope("second", "a").await.is_err());
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn detached_history_loads_but_legacy_restore_requires_original_identity() {
    let root = tempfile::tempdir().unwrap();
    let configuration = config(root.path());
    let backend = Arc::new(TestHost);
    let embedding = Manager::with_host(configuration.clone(), backend)
        .await
        .unwrap();
    let managed = create(&embedding, "a").await;
    managed.session.checkpoint().await.unwrap();
    let id = managed.record.lock().await.id.clone();
    embedding.close().await.unwrap();
    drop(managed);
    drop(embedding);
    let remote = Manager::new(configuration).await.unwrap();
    let record = remote.record("owner", &id).await.unwrap();
    let snapshot = remote.inspect(&record).await.unwrap();
    assert_eq!(snapshot["session"]["id"], id);
    assert!(snapshot["history"].is_object());
    assert_eq!(snapshot["host_connected"], false);
    let subscription = remote.inspect_subscription(&record).await.unwrap();
    assert_eq!(subscription.snapshot["session"]["id"], id);
    assert!(remote.active(record.clone(), true).await.is_err());
    assert!(remote.session_host(&record).await.is_err());
    remote.close().await.unwrap();
}

#[tokio::test]
async fn restore_refuses_pinned_component_and_project_root_mismatch() {
    let root = tempfile::tempdir().unwrap();
    let configuration = config(root.path());
    let embedding = Manager::with_host(configuration.clone(), Arc::new(TestHost))
        .await
        .unwrap();
    let managed = create(&embedding, "a").await;
    managed.session.checkpoint().await.unwrap();
    let mut record = managed.record.lock().await.clone();
    embedding.close().await.unwrap();
    drop(managed);
    drop(embedding);
    let remote = Manager::new(configuration).await.unwrap();
    let principal = Principal::for_test("owner");
    let mut bridge = descriptor("host");
    // The fixture deliberately pins the original opaque root, so failure below
    // is the missing exact WASM component rather than a backend path lookup.
    bridge.workspace_root = record.project_root.clone();
    bridge.descriptor_digest = microcrab_host_protocol::descriptor_fingerprint(&bridge).unwrap();
    let project = microcrab_storage::execution::ProjectBinding {
        project_id: record.project_id.clone().unwrap(),
        owner: record.owner.clone(),
        host_id: bridge.host_id.clone(),
        workspace_id: bridge.workspace_id.clone(),
        workspace_root: bridge.workspace_root.clone(),
        descriptor_digest: bridge.descriptor_digest.clone(),
    };
    // Restoring a remote Session starts from its existing durable identity. A
    // legacy unbound Session must never acquire a new identity during attach.
    remote.pins.pin_project(project.clone()).await.unwrap();
    remote
        .pins
        .pin_session(microcrab_storage::execution::SessionBinding {
            session_id: record.id.clone(),
            project,
            component_digest: Some("b".repeat(64)),
            discovery_digest: "c".repeat(64),
        })
        .await
        .unwrap();
    let (_, responder) = attach_fixture(&remote, &principal, "socket", bridge).await;
    assert!(remote.session_host(&record).await.is_ok());
    let digest = "b".repeat(64);
    let error = match super::assembly::create(&remote, record.clone(), true).await {
        Err(error @ Error::Config(_)) => error,
        _ => panic!("restore accepted unavailable component identity"),
    };
    assert!(error.to_string().contains(&format!("SHA-256 {digest}")));
    let wire = remote.active(record.clone(), true).await.err().unwrap();
    assert_eq!(wire.code, "invalid_request");
    assert_eq!(wire.message, super::safe(error).message);
    assert!(wire.message.contains("pinned agent component unavailable"));
    assert!(wire.message.contains("SHA-256 bbbbbbbbbbbbbbbb…"));
    assert!(wire.message.contains("Restore the exact Agent component"));
    assert!(wire.message.contains("start a new Session"));
    assert!(!serde_json::to_string(&wire).unwrap().contains(&digest));
    assert_eq!(
        remote
            .pins
            .session(&record.id)
            .await
            .unwrap()
            .unwrap()
            .component_digest,
        Some(digest)
    );
    record.project_root = "/another-root".into();
    assert!(remote.session_host(&record).await.is_err());
    remote.detach_socket("socket", None).await;
    responder.abort();
    let _ = responder.await;
    remote.close().await.unwrap();
}

#[tokio::test]
async fn suspended_session_reopen_keeps_pins_without_global_workspace_verification() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(TestHost);
    let manager = Manager::with_host(config(root.path()), backend.clone())
        .await
        .unwrap();
    let managed = create(&manager, "a").await;
    managed.session.checkpoint().await.unwrap();
    let record = managed.record.lock().await.clone();
    managed.suspended.store(true, Ordering::Release);
    assert!(manager.active(record, true).await.is_ok());
    assert!(!managed.suspended.load(Ordering::Acquire));
    manager.close().await.unwrap();
}

#[tokio::test]
async fn route_loss_returns_before_contended_activation_and_never_skips_suspension() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::with_host(config(root.path()), Arc::new(TestHost))
        .await
        .unwrap();
    let managed = create(&manager, "a").await;
    let id = managed.record.lock().await.id.clone();
    let slot = manager.registry.lock().unwrap().get(&id).cloned().unwrap();
    manager.routes.lock().await.insert(
        "a".into(),
        super::bindings::Route {
            owner: "owner".into(),
            socket: "socket".into(),
            generation: 1,
            remote: microcrab_host_remote::RemoteHost::new(descriptor("host")).unwrap(),
            ready: true,
            carrier: super::bindings::ProjectCarrier {
                mode: super::bindings::ProjectCarrierMode::BrowserRelay,
                descriptor: descriptor("host"),
            },
            diagnostics: None,
        },
    );
    let guard = slot.active.lock().await;
    let detached = manager.clone();
    let task = tokio::spawn(async move {
        detached.detach_socket("socket", None).await;
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), task)
        .await
        .expect("route loss must not wait for the activation lock")
        .unwrap();
    assert!(matches!(
        slot.pending_suspension.lock().unwrap().as_ref(),
        Some(microcrab_session::InterruptReason::HostDetached {
            code: microcrab_session::HostDetachCode::Disconnected,
        })
    ));
    assert!(!managed.suspended.load(Ordering::Acquire));
    drop(guard);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !managed.suspended.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    manager.close().await.unwrap();
}
