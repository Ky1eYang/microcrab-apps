//! Explicit maintained-component/provider gate. Build bytes through root orchestration first.
use super::*;
use microcrab_host_api::{FsApi, HostAPI, HostInfo, NetApi, ProcessApi, ShellApi, ToolError};

struct ChatHost;
impl ProcessApi for ChatHost {}
impl FsApi for ChatHost {}
impl NetApi for ChatHost {}
impl ShellApi for ChatHost {}
impl HostAPI for ChatHost {}
impl Host for ChatHost {
    fn info(&self) -> Result<HostInfo, ToolError> {
        let fixture: Value = serde_json::from_str(include_str!(
            "../host_tests/fixtures/chat-only-host-descriptor.json"
        ))
        .expect("valid shared Chat Only descriptor fixture");
        Ok(
            serde_json::from_value(fixture["descriptor"]["info"].clone())
                .expect("valid typed Chat Only Host metadata"),
        )
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires root-built maintained Coding and Universal component paths in MICROCRAB_TEST_CODING_COMPONENT and MICROCRAB_TEST_UNIVERSAL_COMPONENT"]
async fn chat_only_maintained_components_preserve_client_tools_in_provider_payload() {
    let coding =
        std::env::var("MICROCRAB_TEST_CODING_COMPONENT").expect("root-built Coding component path");
    let universal = std::env::var("MICROCRAB_TEST_UNIVERSAL_COMPONENT")
        .expect("root-built Universal component path");
    let coding_bytes = std::fs::read(&coding).expect("read maintained Coding component");
    let loaded = tokio::task::spawn_blocking(move || {
        microcrab_session::runtime::wasm::WasmAgent::load(
            &coding_bytes,
            microcrab_session::runtime::wasm::WasmLimits::default(),
            Arc::new(ChatHost),
        )
        .map(|agent| agent.agents().to_vec())
    })
    .await
    .expect("Coding diagnostic worker");
    assert!(
        loaded.is_ok(),
        "maintained Coding failed empty-Host admission: {:?}",
        loaded.err()
    );
    let root = tempfile::tempdir().unwrap();
    let a = provider(10).await;
    let b = provider(10).await;
    let mut configuration = config(root.path(), &a, &b);
    configuration.agent = Some(serde_json::from_value(json!({"path":coding})).unwrap());
    configuration.agents = vec![serde_json::from_value(json!({"path":universal})).unwrap()];
    // Match ChatOnlyHost metadata and empty execution capabilities. NoopHost
    // deliberately has unsupported info(), so it cannot exercise this admission.
    // Route admission/identity is separately exercised with RemoteHost.
    let manager = Manager::with_host(configuration, Arc::new(ChatHost))
        .await
        .unwrap();
    let owner = Principal::for_test("owner");
    let catalog = commands::execute(&manager, &owner, &command("agent.catalog", None, json!({})))
        .await
        .unwrap();
    let names: Vec<_> = catalog["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|agent| agent["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["coding", "universal"]);
    for agent in catalog["items"].as_array().unwrap() {
        assert_eq!(agent["tools"], json!([]), "unexpected Host tools: {agent}");
    }
    let client_tools: Vec<_> = ["ask_user_question", "todo_write", "fixture_lookup"]
        .into_iter()
        .map(|name| {
            json!({
                "name":name,"description":"Client tool provider fixture",
                "parameters":{"type":"object","properties":{},"additionalProperties":false},
                "permissions":[{"tool":{"kind":"exact","value":name},"args":[],"action":"allow"}],
                "reclaimable":true
            })
        })
        .collect();
    for (agent, mode) in [("coding", "plan"), ("universal", "execute")] {
        let before = a.wire.requests.lock().unwrap().len();
        let component = catalog["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["name"] == agent)
            .expect("maintained agent is in the catalog")["component"]
            .as_str()
            .expect("catalog component digest");
        let created = commands::create(
            &manager,
            "owner",
            &command(
                "session.create",
                None,
                json!({
                    "project_id":"project", "component":component, "agent":agent,
                    "mode":mode, "operation_id":uuid::Uuid::new_v4().to_string(),
                    "client_tools":client_tools
                }),
            ),
        )
        .await
        .unwrap();
        let id = created["id"].as_str().unwrap();
        let managed = manager
            .active(manager.record("owner", id).await.unwrap(), true)
            .await
            .unwrap();
        submit(&manager, &managed, "Describe available client tools.").await;
        {
            let requests = a.wire.requests.lock().unwrap();
            assert_eq!(requests.len(), before + 1, "missing fresh {agent} request");
            let body = &requests.last().expect("actual provider request").1;
            let tools: Vec<_> = body["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|tool| tool["function"]["name"].as_str().unwrap())
                .collect();
            for name in ["ask_user_question", "todo_write", "fixture_lookup"] {
                assert!(
                    tools.contains(&name),
                    "missing authorized client tool {name}"
                );
            }
            for name in [
                "read",
                "read_file",
                "write",
                "write_file",
                "edit",
                "patch",
                "apply_patch",
                "glob",
                "grep",
                "shell",
                "bash",
                "pwsh",
                "process",
            ] {
                assert!(!tools.contains(&name), "unexpected Host tool {name}");
            }
        }
    }
    manager.close().await.unwrap();
}
