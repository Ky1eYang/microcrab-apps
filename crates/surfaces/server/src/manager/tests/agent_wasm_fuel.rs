#[path = "../../../tests/support/host_shell_fixture.rs"]
mod host_shell_fixture;
#[allow(dead_code)]
#[path = "../../../tests/support/typed_agent_fixture.rs"]
mod typed_agent_fixture;

use super::*;
use microcrab_protocol::{Response, Usage};
use microcrab_session::{Auditor, Client, VariableView};
use microcrab_transport::Content;
use serde_json::json;
use tokio_util::sync::CancellationToken;

struct FuelTestModel;

#[async_trait::async_trait]
impl Client for FuelTestModel {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            provider_metadata: None,
            content: "fuel test answer".into(),
            tool_calls: vec![],
            usage: Usage::default(),
        })
    }
}

fn loop_component_bytes() -> Vec<u8> {
    let catalog = r#"[{"name":"fixture","modes":[{"name":"execute","role":"both"}],"variables":{},"tools":[{"name":"echo","description":"fixture","parameters":{"type":"object"}}]}]"#;
    let base_wat = host_shell_fixture::shell_component_source(false, catalog, 0);
    let injected = base_wat.replacen(
        "(func (export \"describe\") (param i32 i32) (result i32)",
        "(func (export \"describe\") (param i32 i32) (result i32)\n          (local $spin_i i32)\n          (loop $spin\n            (local.set $spin_i (i32.add (local.get $spin_i) (i32.const 1)))\n            (br_if $spin (i32.lt_u (local.get $spin_i) (i32.const 60000))))",
        1,
    );
    assert_ne!(base_wat, injected, "describe injection must match fixture");
    typed_agent_fixture::component(&injected)
}

fn config_with_projects(
    root: &std::path::Path,
    wasm_path: &std::path::Path,
    agent_wasm_fuel: u64,
    agent_wasm_fuel_max: Option<u64>,
) -> Config {
    std::fs::create_dir_all(root.join("project-a")).unwrap();
    std::fs::create_dir_all(root.join("project-b")).unwrap();
    let mut value = json!({
        "data_dir": root.join(".data"),
        "storage": if cfg!(feature = "storage-sqlite") { "sqlite" } else { "file" },
        "provider": {"endpoint": "http://127.0.0.1:1/v1/chat/completions", "model": "test", "timeout_ms": 100},
        "agent": {"path": wasm_path, "name": "fixture", "mode": "execute"},
        "agent_wasm_fuel": agent_wasm_fuel,
        "auto_rename": false,
        "auth": {
            "issuer": "http://127.0.0.1:9900",
            "authorization_endpoint": "http://127.0.0.1:9900/authorize",
            "token_endpoint": "http://127.0.0.1:9900/token",
            "jwks_uri": "http://127.0.0.1:9900/jwks",
            "client_id": "test",
            "redirect_uri": "http://127.0.0.1:4096/api/auth/callback",
            "allowed_origins": ["http://127.0.0.1:4096"],
            "jwt_secret": "test-key-at-least-thirty-two-characters"
        }
    });
    if let Some(max) = agent_wasm_fuel_max {
        value["agent_wasm_fuel_max"] = json!(max);
    }
    let mut cfg: Config = serde_json::from_value(value).unwrap();
    cfg.resolve(root).unwrap();
    crate::project::seed_test_projects(
        &cfg.data_dir,
        json!([
            {"id": "a", "name": "Project A", "root": root.join("project-a"), "owners": ["owner"]},
            {"id": "b", "name": "Project B", "root": root.join("project-b"), "owners": ["owner"]}
        ]),
    );
    cfg
}

#[tokio::test]
async fn project_agent_wasm_fuel_validation_scope_and_ceiling_enforcement() {
    let root = tempfile::tempdir().unwrap();
    let wasm_path = root.path().join("agent.wasm");
    std::fs::write(&wasm_path, loop_component_bytes()).unwrap();
    let cfg = config_with_projects(root.path(), &wasm_path, 10_000_000, None);
    let manager = Manager::with_client(cfg, Arc::new(FuelTestModel)).await.unwrap();
    let owner = crate::auth::Principal::for_test("owner");
    let foreign = crate::auth::Principal::for_test("other");

    // Defaults expose Server default and effective ceiling = default when max is omitted.
    let initial = super::super::settings::command(
        &manager,
        "owner",
        &command("project.settings.get", None, json!({"project_id": "a"})),
    )
    .await
    .unwrap();
    assert_eq!(initial["effective"]["agent_wasm_fuel"], 10_000_000);
    assert_eq!(initial["sources"]["agent_wasm_fuel"], "admin");
    assert_eq!(initial["constraints"]["agent_wasm_fuel_default"], 10_000_000);
    assert_eq!(initial["constraints"]["agent_wasm_fuel_max"], 10_000_000);
    assert!(initial["overrides"].get("agent_wasm_fuel").is_none());

    // Without raised ceiling, Project cannot increase Server default.
    let over = super::super::settings::command(
        &manager,
        "owner",
        &command(
            "project.settings.set",
            None,
            json!({"project_id": "a", "expected_revision": 0, "overrides": {"agent_wasm_fuel": 10_000_001}}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(over.code, "invalid_params");
    assert!(over.message.contains("10000000"));

    // Zero or user-scope writes are rejected cleanly.
    let zero = super::super::settings::command(
        &manager,
        "owner",
        &command(
            "project.settings.set",
            None,
            json!({"project_id": "a", "expected_revision": 0, "overrides": {"agent_wasm_fuel": 0}}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(zero.code, "invalid_params");

    let user_write = super::super::settings::command(
        &manager,
        "owner",
        &command(
            "settings.set",
            None,
            json!({"expected_revision": 0, "overrides": {"agent_wasm_fuel": 5_000_000}}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(user_write.code, "invalid_params");

    // Foreign owner cannot read or write Project A settings.
    assert_eq!(
        commands::execute(
            &manager,
            &foreign,
            &command("project.settings.get", None, json!({"project_id": "a"})),
        )
        .await
        .unwrap_err()
        .code,
        "not_found"
    );

    // Lowering within ceiling succeeds and clearing returns to Inherit Server default.
    let saved = super::super::settings::command(
        &manager,
        "owner",
        &command(
            "project.settings.set",
            None,
            json!({"project_id": "a", "expected_revision": 0, "overrides": {"agent_wasm_fuel": 5_000_000}}),
        ),
    )
    .await
    .unwrap();
    assert_eq!(saved["revision"], 1);
    assert_eq!(saved["overrides"]["agent_wasm_fuel"], 5_000_000);
    assert_eq!(saved["effective"]["agent_wasm_fuel"], 5_000_000);
    assert_eq!(saved["sources"]["agent_wasm_fuel"], "project");

    let cleared = super::super::settings::command(
        &manager,
        "owner",
        &command(
            "project.settings.set",
            None,
            json!({"project_id": "a", "expected_revision": 1, "overrides": {}}),
        ),
    )
    .await
    .unwrap();
    assert_eq!(cleared["effective"]["agent_wasm_fuel"], 10_000_000);
    assert_eq!(cleared["sources"]["agent_wasm_fuel"], "admin");
    assert!(cleared["overrides"].get("agent_wasm_fuel").is_none());

    manager.close().await.unwrap();
    let _ = owner;
}

fn host_descriptor(host_id: &str) -> microcrab_host_protocol::HostDescriptor {
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

async fn attach_project(
    manager: &Arc<Manager>,
    owner: &crate::auth::Principal,
    project: &str,
    host_id: &str,
    socket: &str,
) -> tokio::task::JoinHandle<()> {
    use microcrab_host_protocol::{HostFrame, HostRequest};
    let generation = manager.next_connection_generation();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
    let reply_manager = manager.clone();
    let reply_owner = owner.clone();
    let reply_project = project.to_owned();
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
                    result: Ok(json!([])),
                }),
                HostFrame::Ping { lease_generation } => Some(HostFrame::Pong { lease_generation }),
                _ => None,
            };
            if let Some(reply) = reply
                && reply_manager
                    .receive_host(&reply_owner, &reply_socket, &reply_project, generation, reply)
                    .await
                    .is_err()
            {
                break;
            }
        }
    });
    let result = manager
        .attach_project(
            owner,
            socket,
            super::super::bindings::Attach {
                project_id: project.into(),
                descriptor: host_descriptor(host_id),
                lease_generation: generation.to_string(),
                allow_legacy_binding: true,
            },
            sender,
            generation,
            super::super::bindings::ProjectCarrierMode::BrowserRelay,
        )
        .await
        .unwrap();
    assert_eq!(result["ready"], true);
    responder
}

async fn server_manager(cfg: Config) -> (Arc<Manager>, Vec<tokio::task::JoinHandle<()>>) {
    let host = crate::host::startup_local_host(&cfg).unwrap();
    let manager = Manager::initialize(cfg, host, Some(Arc::new(FuelTestModel)), false, None)
        .await
        .unwrap();
    let owner = crate::auth::Principal::for_test("owner");
    let a = attach_project(&manager, &owner, "a", "host-a", "sock-a").await;
    let b = attach_project(&manager, &owner, "b", "host-b", "sock-b").await;
    (manager, vec![a, b])
}

#[tokio::test]
async fn agent_wasm_fuel_governs_discovery_create_restore_live_boundary_and_lowered_ceiling() {
    let root = tempfile::tempdir().unwrap();
    let wasm_path = root.path().join("agent.wasm");
    std::fs::write(&wasm_path, loop_component_bytes()).unwrap();
    // Server default 50_000 is too tight for describe's loop; ceiling 10_000_000 allows raising per-Project.
    let cfg = config_with_projects(root.path(), &wasm_path, 50_000, Some(10_000_000));
    let (manager, _guards) = server_manager(cfg).await;
    let owner = crate::auth::Principal::for_test("owner");

    // 1. Project A inherits 50_000 -> out_of_fuel during describe with limit + phase preserved.
    let catalog_err = commands::execute(
        &manager,
        &owner,
        &command("agent.catalog", None, json!({"project_id": "a"})),
    )
    .await
    .unwrap_err();
    assert_eq!(catalog_err.code, "operation_failed");
    assert!(catalog_err.message.contains("out_of_fuel"));
    assert!(catalog_err.message.contains("50000"));
    assert!(catalog_err.message.contains("guest_execution"));
    let details = catalog_err.details.as_ref().unwrap();
    assert_eq!(details["diagnostic"]["wasm_failure"]["cause"], "out_of_fuel");
    assert_eq!(details["diagnostic"]["wasm_failure"]["fuel_limit"], 50_000);
    assert_eq!(details["diagnostic"]["phase"], "guest_execution");

    let create_a_err = commands::execute(
        &manager,
        &owner,
        &command(
            "session.create",
            None,
            json!({"project_id": "a", "operation_id": uuid::Uuid::new_v4().to_string()}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(create_a_err.code, "operation_failed");
    assert_eq!(
        create_a_err.details.as_ref().unwrap()["diagnostic"]["wasm_failure"]["fuel_limit"],
        50_000
    );

    // 2. Raise Project B override to 5_000_000 -> catalog + create succeed without affecting Project A.
    super::super::settings::command(
        &manager,
        "owner",
        &command(
            "project.settings.set",
            None,
            json!({"project_id": "b", "expected_revision": 0, "overrides": {"agent_wasm_fuel": 5_000_000}}),
        ),
    )
    .await
    .unwrap();
    let cat_b = commands::execute(
        &manager,
        &owner,
        &command("agent.catalog", None, json!({"project_id": "b"})),
    )
    .await
    .unwrap();
    let component_digest = cat_b["items"][0]["component"].as_str().unwrap().to_owned();

    // Project A must still fail at 50_000 (no cache bleed from Project B).
    let still_err = commands::execute(
        &manager,
        &owner,
        &command("agent.catalog", None, json!({"project_id": "a"})),
    )
    .await
    .unwrap_err();
    assert_eq!(
        still_err.details.as_ref().unwrap()["diagnostic"]["wasm_failure"]["fuel_limit"],
        50_000
    );

    let created_b = commands::execute(
        &manager,
        &owner,
        &command(
            "session.create",
            None,
            json!({
                "project_id": "b",
                "component": component_digest,
                "operation_id": uuid::Uuid::new_v4().to_string()
            }),
        ),
    )
    .await
    .unwrap();
    let session_b_id = created_b["id"].as_str().unwrap().to_owned();
    // Open session_b so it is actively loaded at 5_000_000 fuel.
    let _live_b = manager
        .activate(manager.record("owner", &session_b_id).await.unwrap(), false, None, true)
        .await
        .unwrap();

    // 3. Updating Project B fuel while Session B is live does NOT mutate live Session fuel;
    //    reject arbitrary Session override attempt.
    let session_set_err = super::super::settings::command(
        &manager,
        "owner",
        &command(
            "session.settings.set",
            Some(&session_b_id),
            json!({"expected_revision": 0, "overrides": {"agent_wasm_fuel": 2_000_000}}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(session_set_err.code, "invalid_params");

    super::super::settings::command(
        &manager,
        "owner",
        &command(
            "project.settings.set",
            None,
            json!({"project_id": "b", "expected_revision": 1, "overrides": {"agent_wasm_fuel": 2_000_000}}),
        ),
    )
    .await
    .unwrap();
    let live_session_settings = super::super::settings::command(
        &manager,
        "owner",
        &command("session.settings.get", Some(&session_b_id), json!({})),
    )
    .await
    .unwrap();
    assert_eq!(live_session_settings["effective"]["agent_wasm_fuel"], 5_000_000);
    assert_eq!(
        live_session_settings["desired_effective"]["agent_wasm_fuel"],
        2_000_000
    );

    // 4. Projectless resolution & catalog use Server default (50_000 -> OutOfFuel { fuel_limit: Some(50_000) }).
    assert_eq!(manager.resolve_agent_wasm_fuel("owner", None).await.unwrap(), 50_000);
    let projectless_err = match super::super::assembly::components(&manager.config, manager.host.clone()).await {
        Ok(_) => panic!("projectless catalog must enforce Server default 50_000"),
        Err(error) => error,
    };
    assert!(matches!(
        projectless_err,
        microcrab_session::Error::Diagnostic(diagnostic)
            if matches!(
                diagnostic.wasm_failure,
                Some(microcrab_session::observation::diagnostics::WasmFailure::OutOfFuel {
                    fuel_limit: Some(50_000)
                })
            )
    ));
    drop(_live_b);
    manager.close().await.unwrap();
    drop(_guards);

    // 5. Reopen in fresh Manager: restore uses Project B's persisted 2_000_000 cleanly.
    let cfg_reopen = config_with_projects(root.path(), &wasm_path, 50_000, Some(10_000_000));
    let (reopened, _reopen_guards) = server_manager(cfg_reopen).await;
    let record_b = reopened.record("owner", &session_b_id).await.unwrap();
    let restored_b = reopened.activate(record_b, true, None, true).await.unwrap();
    let restored_settings = super::super::settings::command(
        &reopened,
        "owner",
        &command("session.settings.get", Some(&session_b_id), json!({})),
    )
    .await
    .unwrap();
    assert_eq!(restored_settings["effective"]["agent_wasm_fuel"], 2_000_000);
    drop(restored_b);
    reopened.close().await.unwrap();
    drop(_reopen_guards);

    // 6. Restart with LOWERED Server ceiling (1_000_000 < persisted 2_000_000):
    //    - Project B saved override stays intact & readable in project.settings.get
    //    - Affected new loads on Project B (catalog, create, restore) fail explicitly
    //    - Unrelated Project A remains usable with a valid override <= 1_000_000
    let cfg_lowered = config_with_projects(root.path(), &wasm_path, 50_000, Some(1_000_000));
    let (lowered, _lowered_guards) = server_manager(cfg_lowered).await;

    let project_b_view = super::super::settings::command(
        &lowered,
        "owner",
        &command("project.settings.get", None, json!({"project_id": "b"})),
    )
    .await
    .unwrap();
    assert_eq!(project_b_view["overrides"]["agent_wasm_fuel"], 2_000_000);
    assert_eq!(project_b_view["constraints"]["agent_wasm_fuel_max"], 1_000_000);

    let b_catalog_blocked = commands::execute(
        &lowered,
        &owner,
        &command("agent.catalog", None, json!({"project_id": "b"})),
    )
    .await
    .unwrap_err();
    assert_eq!(b_catalog_blocked.code, "invalid_request");
    assert!(b_catalog_blocked.message.contains("1000000"));

    let b_create_blocked = commands::execute(
        &lowered,
        &owner,
        &command(
            "session.create",
            None,
            json!({"project_id": "b", "operation_id": uuid::Uuid::new_v4().to_string()}),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(b_create_blocked.code, "invalid_request");

    let record_b = lowered.record("owner", &session_b_id).await.unwrap();
    assert!(lowered.active(record_b, true).await.is_err());

    // Unrelated Project A remains usable once given a valid override within the lowered ceiling.
    super::super::settings::command(
        &lowered,
        "owner",
        &command(
            "project.settings.set",
            None,
            json!({"project_id": "a", "expected_revision": 0, "overrides": {"agent_wasm_fuel": 1_000_000}}),
        ),
    )
    .await
    .unwrap();
    let cat_a_ok = commands::execute(
        &lowered,
        &owner,
        &command("agent.catalog", None, json!({"project_id": "a"})),
    )
    .await
    .unwrap();
    assert_eq!(cat_a_ok["items"].as_array().unwrap().len(), 1);

    lowered.close().await.unwrap();

    // 7. Embedded startup with insufficient default fuel fails with typed OutOfFuel.
    let cfg_embedded = config_with_projects(root.path(), &wasm_path, 50_000, None);
    let embedded_err = match Manager::with_client(cfg_embedded, Arc::new(FuelTestModel)).await {
        Ok(_) => panic!("embedded startup must enforce agent_wasm_fuel"),
        Err(error) => error,
    };
    assert!(matches!(
        embedded_err,
        microcrab_session::Error::Diagnostic(diagnostic)
            if matches!(
                diagnostic.wasm_failure,
                Some(microcrab_session::observation::diagnostics::WasmFailure::OutOfFuel {
                    fuel_limit: Some(50_000)
                })
            )
    ));
}

#[tokio::test]
async fn disposable_unoptimized_browser_component_compares_default_vs_increased_fuel() {
    let Some(artifact) = std::env::var_os("MICROCRAB_TEST_UNOPTIMIZED_BROWSER_COMPONENT") else {
        return;
    };
    use microcrab_host_api::{
        FsApi, HostAPI, NetApi, ProcessApi, ShellApi, web_transport::ProjectScope,
    };

    struct BrowserScopeHost;
    impl ProcessApi for BrowserScopeHost {}
    impl FsApi for BrowserScopeHost {}
    impl NetApi for BrowserScopeHost {}
    impl ShellApi for BrowserScopeHost {}
    impl HostAPI for BrowserScopeHost {}
    impl microcrab_host_api::Host for BrowserScopeHost {
        fn info(&self) -> Result<microcrab_host_api::HostInfo, microcrab_host_api::ToolError> {
            Ok(microcrab_host_api::HostInfo {
                os_family: microcrab_host_api::OsFamily::Unix,
                os: "linux".into(),
                arch: "x86_64".into(),
                shell: "none".into(),
                path_separator: "/".into(),
                newline: "\n".into(),
                capabilities: vec![],
                resources: None,
                virtual_shell: None,
                description: None,
            })
        }
        fn supports_web_transport(&self) -> bool {
            true
        }
        fn web_transport_binding(&self) -> Option<ProjectScope> {
            Some(ProjectScope {
                binding_id: "binding".into(),
                backend_id: "backend".into(),
                project_id: "a".into(),
            })
        }
    }

    let root = tempfile::tempdir().unwrap();
    let wasm_path = root.path().join("browser-unoptimized.wasm");
    std::fs::copy(&artifact, &wasm_path).unwrap();

    let cfg = config_with_projects(root.path(), &wasm_path, 10_000_000, Some(200_000_000));
    let mut manager_cfg = cfg.clone();
    manager_cfg.agent = Some(crate::config::Agent {
        path: wasm_path.clone(),
        name: None,
        mode: None,
        restore_only: false,
    });
    manager_cfg.agents = vec![];
    let manager = Manager::initialize(
        manager_cfg,
        Arc::new(BrowserScopeHost),
        Some(Arc::new(FuelTestModel)),
        false,
        None,
    )
    .await
    .unwrap();
    let owner = crate::auth::Principal::for_test("owner");
    let generation = manager.next_connection_generation();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
    let reply_manager = manager.clone();
    let reply_owner = owner.clone();
    let _guard = tokio::spawn(async move {
        use microcrab_host_protocol::{HostFrame, HostRequest};
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
                    result: Ok(json!([])),
                }),
                HostFrame::Ping { lease_generation } => Some(HostFrame::Pong { lease_generation }),
                _ => None,
            };
            if let Some(reply) = reply
                && reply_manager
                    .receive_host(&reply_owner, "sock-a", "a", generation, reply)
                    .await
                    .is_err()
            {
                break;
            }
        }
    });
    let binding_scope = manager.binding_scope(&owner.owner, "a").await.unwrap();
    let attach_res = manager
        .attach_native_project(
            &owner,
            "sock-a",
            super::super::bindings::Attach {
                project_id: "a".into(),
                descriptor: host_descriptor("host-a"),
                lease_generation: generation.to_string(),
                allow_legacy_binding: true,
            },
            sender,
            generation,
            super::super::bindings::NativeRouteOptions {
                mode: super::super::bindings::ProjectCarrierMode::BrowserRelay,
                private_files: None,
                glob_ignore: false,
                glob_paths: false,
                web_transport: Some(super::super::bindings::WebTransportProof {
                    scope: binding_scope,
                    ws_pull: true,
                }),
            },
        )
        .await
        .unwrap();
    assert_eq!(attach_res["ready"], true);

    // Default 10_000_000 fuel fails catalog discovery on unoptimized Browser WASM with OutOfFuel.
    let err = commands::execute(
        &manager,
        &owner,
        &command("agent.catalog", None, json!({"project_id": "a"})),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "operation_failed");
    assert_eq!(
        err.details.as_ref().unwrap()["diagnostic"]["wasm_failure"]["cause"],
        "out_of_fuel"
    );
    assert_eq!(
        err.details.as_ref().unwrap()["diagnostic"]["wasm_failure"]["fuel_limit"],
        10_000_000
    );

    // Explicitly increased Project fuel enables catalog discovery, session create, and representative tool execution.
    super::super::settings::command(
        &manager,
        "owner",
        &command(
            "project.settings.set",
            None,
            json!({"project_id": "a", "expected_revision": 0, "overrides": {"agent_wasm_fuel": 100_000_000}}),
        ),
    )
    .await
    .unwrap();
    let catalog = commands::execute(
        &manager,
        &owner,
        &command("agent.catalog", None, json!({"project_id": "a"})),
    )
    .await
    .unwrap();
    assert!(!catalog["items"].as_array().unwrap().is_empty());
    let created = commands::execute(
        &manager,
        &owner,
        &command(
            "session.create",
            None,
            json!({"project_id": "a", "operation_id": uuid::Uuid::new_v4().to_string()}),
        ),
    )
    .await
    .unwrap();
    let session_id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .activate(manager.record("owner", &session_id).await.unwrap(), false, None, true)
        .await
        .unwrap();
    assert_eq!(managed.agent_wasm_fuel, 100_000_000);
    let mut raised_limits = microcrab_session::runtime::wasm::WasmLimits::default();
    raised_limits.fuel = 100_000_000;
    let (loaded, _) = microcrab_app_bootstrap::agent::load_with_limits(
        &wasm_path,
        raised_limits,
        Arc::new(BrowserScopeHost),
    )
    .unwrap();
    let tool = loaded.tools().into_iter().next().expect("browser tool present");
    let ctx = microcrab_session::runtime::tool::ToolContext::detached(
        managed.session.state(),
        CancellationToken::new(),
    );
    // Representative tool call runs inside the guest cleanly without OutOfFuel under increased fuel.
    if let Err(tool_err) = tool.execute(json!({}), ctx).await {
        let diag = tool_err.tool_diagnostic();
        assert!(!matches!(
            diag.wasm_failure,
            Some(microcrab_session::observation::diagnostics::WasmFailure::OutOfFuel { .. })
        ));
    }
    drop(managed);
    drop(_guard);
    manager.close().await.unwrap();
}
