//! Opt-in real-component acceptance; no staged runtime or live services are used.
//!
//! Pass absolute paths to the composed Zig candidate (`MICROCRAB_TEST_ZIG_CODING_COMPONENT`),
//! the exact retained Rust Coding predecessor (`MICROCRAB_TEST_CODING_COMPONENT`), and
//! maintained Rust Universal (`MICROCRAB_TEST_UNIVERSAL_COMPONENT`). Artifact provenance
//! is supplied by the caller; the fixture derives and verifies the selected byte digests.
//! Run this ignored test by name with `--ignored --test-threads=1`, with a LocalHost
//! interpreter available (Bash on Unix or PowerShell on Windows).

use super::*;
use crate::config::Agent;
use microcrab_host_local::LocalHost;
use sha2::{Digest, Sha256};
use std::{fs, path::PathBuf, time::Duration};

const DEADLINE: Duration = Duration::from_secs(30);
const OWNER: &str = "owner";
const SOCKET: &str = "coding-acceptance-socket";
const FILE: &str = "acceptance.txt";
const ORIGINAL: &str = "original fixture text\n";
const UPDATED: &str = "updated fixture text\n";
const PATCHED: &str = "patched fixture text\n";
const PATCH_FILE: &str = "patch-added.txt";
const PATCH_ADDED: &str = "added by patch\n";
const SHELL_MARKER: &str = "plan-shell.txt";
const SHELL_TEXT: &str = "approved shell fixture";

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn component(env: &str) -> (Agent, String) {
    let path = PathBuf::from(std::env::var_os(env).unwrap_or_else(|| panic!("set {env}")));
    assert!(path.is_absolute(), "{env} must be an absolute path");
    let bytes = fs::read(&path).unwrap_or_else(|err| panic!("read {env}: {err}"));
    (
        Agent {
            path,
            name: None,
            mode: None,
            restore_only: false,
        },
        digest(&bytes),
    )
}

fn tool_reply(id: &str, tool: &str, arguments: &Value) -> Value {
    json!({
        "choices":[{"message":{"role":"assistant","content":null,"tool_calls":[
            {"id":id,"type":"function","function":{"name":tool,"arguments":arguments.to_string()}}
        ]},"finish_reason":"tool_calls"}],
        "usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}
    })
}

fn stop_reply() -> Value {
    json!({"choices":[{"message":{"role":"assistant","content":"Fixture turn complete"},"finish_reason":"stop"}],
        "usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}})
}

#[tokio::test]
#[ignore = "requires built Coding Component and real LocalHost"]
async fn trajectory_execution_ids_survive_dormant_history() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    fs::create_dir(&workspace).unwrap();
    fs::write(workspace.join("a.txt"), "first\n").unwrap();
    fs::write(workspace.join("b.txt"), "second\n").unwrap();
    let calls = [
        ("glob-fixture", "glob", json!({"pattern":"*.txt"})),
        ("read-a", "read", json!({"path":"a.txt"})),
        ("read-b", "read", json!({"path":"b.txt"})),
        ("read-missing", "read", json!({"path":"absent.txt"})),
    ];
    let mut reply = tool_reply(calls[0].0, calls[0].1, &calls[0].2);
    reply["choices"][0]["message"]["tool_calls"] = json!(calls.iter().map(|(id, tool, args)| {
        json!({"id":id,"type":"function","function":{"name":tool,"arguments":args.to_string()}})
    }).collect::<Vec<_>>());
    let provider = scripted_provider(2, vec![reply, stop_reply()]).await;
    let mut config = config(root.path(), &provider, &provider);
    crate::project::seed_test_projects(
        &config.data_dir,
        json!([
            {"id":"project","name":"Project","root":workspace,"owners":[OWNER]}
        ]),
    );
    config.session_defaults.review = Some(false);
    let (coding, digest) = component("MICROCRAB_TEST_ZIG_CODING_COMPONENT");
    config.agent = Some(coding);
    let host = Arc::new(LocalHost::new(&workspace, Default::default()).unwrap());
    let owner = Principal::for_test(OWNER);
    let manager = Manager::with_host(config.clone(), host.clone())
        .await
        .unwrap();
    let catalog = catalog(&manager, &owner).await;
    let (id, managed, _) = create_selected(
        &manager,
        &owner,
        catalog_entry(&catalog, "coding", &digest),
        "plan",
    )
    .await;
    let mut events = managed.hub.subscribe();
    let operation = submit_scripted_turn(&manager, &owner, &id, "inspect fixtures").await;
    settle_turn(&manager, &managed, &operation).await;
    let mut live = std::collections::BTreeMap::new();
    while let Ok(envelope) = events.receiver.try_recv() {
        if let super::super::hub::SessionRuntimeEvent::Tool(event) = &envelope.event {
            if matches!(event["phase"].as_str(), Some("completed" | "failed")) {
                let call = event["call_id"].as_str().unwrap();
                let execution_id = event["execution_id"].as_u64().unwrap();
                let row = managed.session.execution_snapshots().into_iter()
                    .find(|row| row.execution_id.0 == execution_id && row.call_id == call)
                    .expect("same actual runtime invocation");
                let status = serde_json::to_value(row.status).unwrap();
                assert_eq!(event["phase"], if status == "succeeded" { json!("completed") } else { status }, "{call}");
                if call == "read-missing" {
                    assert_eq!(event["phase"], "failed");
                }
                assert!(
                    live.insert(call.to_owned(), event["execution_id"].as_u64().unwrap())
                        .is_none()
                );
            }
        }
    }
    assert_eq!(live.len(), calls.len(), "actual terminal observations");
    assert_eq!(
        live.values()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        calls.len()
    );
    let record = manager.record(OWNER, &id).await.unwrap();
    let params: commands::Messages = serde_json::from_value(json!({"limit":64})).unwrap();
    let assert_page = |page: &Value| {
        let items = page["items"].as_array().unwrap();
        let actual = items
            .iter()
            .flat_map(|item| item["tool_calls"].as_array().into_iter().flatten())
            .map(|call| {
                (
                    call["id"].as_str().unwrap().to_owned(),
                    call["execution_id"].as_u64().unwrap(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(actual, live, "browser call certificates");
        for item in items.iter().filter(|item| item["role"] == "tool") {
            let call = item["call_id"].as_str().unwrap();
            assert_eq!(item["execution_id"], live[call]);
            assert_eq!(item["execution_session_id"], id);
        }
    };
    assert_page(
        &manager
            .inspect_message_page(&record, &params)
            .await
            .unwrap(),
    );
    assert!(
        !serde_json::to_string(&managed.session.messages())
            .unwrap()
            .contains("execution_call_source_id")
    );
    manager.close().await.unwrap();
    drop((managed, manager));
    let reopened = Manager::with_host(config, host).await.unwrap();
    let record = reopened.record(OWNER, &id).await.unwrap();
    assert_page(
        &reopened
            .inspect_message_page(&record, &params)
            .await
            .unwrap(),
    );
    assert!(!reopened.registry.lock().unwrap().contains_key(&id));
    reopened
        .command(&owner, &command("session.open", Some(&id), json!({})))
        .await
        .unwrap();
    assert_page(
        &reopened
            .inspect_message_page(&record, &params)
            .await
            .unwrap(),
    );
    reopened.close().await.unwrap();
}

async fn catalog(manager: &Manager, owner: &Principal) -> Vec<Value> {
    manager
        .command(
            owner,
            &command("agent.catalog", None, json!({"project_id":"project"})),
        )
        .await
        .expect("discover components on the actual LocalHost")["items"]
        .as_array()
        .expect("catalog items")
        .clone()
}

fn catalog_entry<'a>(items: &'a [Value], name: &str, digest: &str) -> &'a Value {
    items
        .iter()
        .find(|item| item["name"] == name && item["component"] == digest)
        .unwrap_or_else(|| panic!("missing catalog entry {name} at SHA-256 {digest}"))
}

fn create_command(agent: &str, mode: &str, digest: &str) -> Command {
    command(
        "session.create",
        None,
        json!({"project_id":"project","agent":agent,"mode":mode,"component":digest,
            "operation_id":uuid::Uuid::new_v4().to_string()}),
    )
}

async fn create_selected(
    manager: &Manager,
    owner: &Principal,
    entry: &Value,
    mode: &str,
) -> (String, Arc<Managed>, String) {
    let agent = entry["name"].as_str().expect("catalog agent name");
    let component = entry["component"].as_str().expect("catalog digest");
    let selected = manager
        .command(owner, &create_command(agent, mode, component))
        .await
        .expect("create Session pinned to exact selected component");
    assert_eq!(selected["component"], component);
    assert_eq!(selected["agent"], agent);
    assert_eq!(selected["configured_mode"], mode);
    assert_eq!(selected["agent_initialization_deferred"], true);
    assert_eq!(selected["agent_init_pending"], true);
    let id = selected["id"].as_str().expect("created Session ID");
    let open = command("session.open", Some(id), json!({}));
    let refused = manager.command(owner, &open).await.unwrap_err();
    assert_eq!(refused.code, "executor_required");
    let lease = bind_ask(manager, owner, id).await;
    let initialized = tokio::time::timeout(DEADLINE, manager.command(owner, &open))
        .await
        .expect("Agent initialization must settle after executor bind")
        .expect("owner opens Session and initializes the component");
    assert_eq!(initialized["session"]["mode"], mode);
    let managed = manager
        .active(manager.record(OWNER, id).await.unwrap(), true)
        .await
        .expect("initialized Session is active");
    assert!(managed.session.agent_initialization_complete());
    assert!(!managed.session.state().agent_initialization_pending);
    (id.to_owned(), managed, lease)
}

async fn submit_scripted_turn(
    manager: &Manager,
    owner: &Principal,
    id: &str,
    call: &str,
) -> String {
    let operation = uuid::Uuid::new_v4().to_string();
    manager
        .command(
            owner,
            &command(
                "turn.submit",
                Some(id),
                json!({"operation_id":operation,"input":microcrab_session::SessionInput::text(call)}),
            ),
        )
        .await
        .unwrap_or_else(|err| panic!("admit {call}: {err:?}"));
    operation
}

async fn settle_turn(manager: &Manager, managed: &Managed, operation: &str) {
    let worker = managed.worker.lock().await.take().expect("turn worker");
    tokio::time::timeout(DEADLINE, worker)
        .await
        .expect("turn must settle")
        .expect("turn worker must not panic");
    assert!(!managed.faulted.load(Ordering::Acquire));
    let receipt = manager
        .directory
        .get_operation(OWNER, operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        receipt.status, "completed",
        "turn receipt: {:?}",
        receipt.result
    );
}

async fn bind_ask(manager: &Manager, owner: &Principal, id: &str) -> String {
    let tools = manager
        .client_tools(id)
        .await
        .expect("registered Session tool catalog");
    let bound = manager
        .client_tool_command(owner, SOCKET, &command("client_tools.bind", Some(id), json!({
            "client_id":"coding-acceptance", "catalog_digest":tools.snapshot().await.catalog_digest
        })))
        .await
        .expect("bind interactive permission executor");
    bound["lease_id"].as_str().expect("lease ID").to_owned()
}

async fn pending_ask(
    events: &mut super::super::hub::Subscription,
    call: &str,
    tool: &str,
    mode: &str,
) -> Value {
    let request = tokio::time::timeout(DEADLINE, async {
        loop {
            let event = events
                .receiver
                .recv()
                .await
                .expect("live permission event stream");
            if event.event.name() == "permission.requested" {
                break event.event.payload()["request"].clone();
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{mode} {tool} must request approval for {call}"));
    assert_eq!(request["call_id"], call);
    assert_eq!(request["tool"], tool);
    assert_eq!(request["agent"], "coding");
    assert_eq!(request["mode"], mode);
    request
}

async fn decide_ask(
    manager: &Manager,
    owner: &Principal,
    id: &str,
    lease: &str,
    request: &Value,
    decision: &str,
) {
    manager
        .permission_reply(
            owner,
            SOCKET,
            &command(
                "permission.reply",
                Some(id),
                json!({
                    "request_id":request["request_id"], "lease_id":lease, "decision":decision
                }),
            ),
        )
        .await
        .expect("the bound participant can settle the exact pending request");
}

/// The next request must replay the exact tool result to the loopback provider,
/// not merely advance to a scripted assistant message after an unrelated failure.
fn wire_result(provider: &ProviderMock, before: usize, call: &str, tool: &str) -> (Value, Value) {
    let requests = provider.wire.requests.lock().unwrap();
    assert_eq!(requests.len(), before + 2, "provider rounds for {call}");
    let tools = &requests[before].1["tools"];
    assert_eq!(
        requests[before + 1].1["tools"],
        *tools,
        "catalog must stay frozen during the turn"
    );
    assert!(
        tools
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["function"]["name"] == tool),
        "provider did not advertise {tool} for {call}"
    );
    let message = requests[before + 1].1["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "tool" && message["tool_call_id"] == call)
        .unwrap_or_else(|| panic!("no model-facing tool result for {call}"));
    let content = message["content"].as_str().expect("text tool result");
    let result = serde_json::from_str(content)
        .unwrap_or_else(|err| panic!("expected JSON result for {call}: {err}: {content}"));
    (tools.clone(), result)
}

#[cfg(target_os = "linux")]
// Config::read synchronously probes the loopback model service; keep that test
// peer runnable on another worker instead of blocking its only executor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires MICROCRAB_TEST_ZIG_CODING_COMPONENT with the negotiated file-version ABI"]
async fn short_versions_round_trip_after_file_and_sqlite_reopen_without_changing_exact_cas() {
    use crate::config::Storage;
    let (coding, component_digest) = component("MICROCRAB_TEST_ZIG_CODING_COMPONENT");
    for storage in [
        Storage::File,
        #[cfg(feature = "storage-sqlite")]
        Storage::Sqlite,
    ] {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let file = workspace.join(FILE);
        fs::write(&file, ORIGINAL).unwrap();
        fs::write(workspace.join("other.txt"), UPDATED).unwrap();
        let receipt_ref = Arc::new(Mutex::new(String::new()));
        let read_ref = Arc::new(Mutex::new(String::new()));
        let provider = indexed_provider(10, {
            let receipt_ref = receipt_ref.clone();
            let read_ref = read_ref.clone();
            move |index| Some(match index {
                0 => tool_reply("short-read", "read", &json!({"path":FILE})),
                2 => tool_reply("short-edit", "edit", &json!({"path":FILE,"expected_version":*read_ref.lock().unwrap(),"old_string":ORIGINAL,"new_string":UPDATED})),
                4 => tool_reply("wrong-path-ref", "write", &json!({"path":"other.txt","expected_version":*receipt_ref.lock().unwrap(),"content":"forbidden change"})),
                6 => tool_reply("stale-content-ref", "write", &json!({"path":FILE,"expected_version":*read_ref.lock().unwrap(),"content":"stale change"})),
                8 => tool_reply("short-write", "write", &json!({"path":FILE,"expected_version":*receipt_ref.lock().unwrap(),"content":PATCHED})),
                _ => stop_reply(),
            })
        }).await;
        let mut config = config(root.path(), &provider, &provider);
        config.storage = storage;
        config.session_defaults.review = Some(false);
        config.agent = Some(coding.clone());
        crate::project::seed_test_projects(
            &config.data_dir,
            json!([{"id":"project","name":"Project","root":workspace,"owners":[OWNER]}]),
        );
        let owner = Principal::for_test(OWNER);
        let host = || Arc::new(LocalHost::new(&workspace, Default::default()).unwrap());
        let manager = Manager::with_host(config.clone(), host()).await.unwrap();
        let items = catalog(&manager, &owner).await;
        let (id, session, _) = create_selected(
            &manager,
            &owner,
            catalog_entry(&items, "coding", &component_digest),
            "build",
        )
        .await;
        let operation =
            submit_scripted_turn(&manager, &owner, &id, "read the initial file version").await;
        settle_turn(&manager, &session, &operation).await;
        let (tools, result) = wire_result(&provider, 0, "short-read", "read");
        let alias = short_file_version(&result);
        assert_read(&result, alias);
        assert!(
            !provider.wire.requests.lock().unwrap()[1]
                .1
                .to_string()
                .contains(&digest(ORIGINAL.as_bytes()))
        );
        *read_ref.lock().unwrap() = alias.to_owned();
        let epoch = session.session.context_snapshot().unwrap().epoch;
        manager.close().await.unwrap();
        drop((manager, session));

        let manager = Manager::with_host(config, host()).await.unwrap();
        manager
            .command(&owner, &command("session.open", Some(&id), json!({})))
            .await
            .unwrap();
        let session = manager
            .active(manager.record(OWNER, &id).await.unwrap(), true)
            .await
            .unwrap();
        assert_eq!(session.session.context_snapshot().unwrap().epoch, epoch);
        let lease = bind_ask(&manager, &owner, &id).await;
        for (before, call, tool, needs_ask) in [
            (2, "short-edit", "edit", true),
            (4, "wrong-path-ref", "write", false),
            (6, "stale-content-ref", "write", true),
            (8, "short-write", "write", true),
        ] {
            let mut events = session.hub.subscribe();
            let operation = submit_scripted_turn(&manager, &owner, &id, call).await;
            if needs_ask {
                let request = pending_ask(&mut events, call, tool, "build").await;
                assert_eq!(
                    fs::read_to_string(&file).unwrap(),
                    if before == 2 { ORIGINAL } else { UPDATED }
                );
                decide_ask(&manager, &owner, &id, &lease, &request, "allow").await;
            }
            settle_turn(&manager, &session, &operation).await;
            let (round_tools, result) = wire_result(&provider, before, call, tool);
            assert_eq!(tools, round_tools);
            match before {
                2 => *receipt_ref.lock().unwrap() = short_file_version(&result).to_owned(),
                4 => assert!(result.to_string().contains("reread"), "{result}"),
                6 => assert!(result.to_string().contains("conflict"), "{result}"),
                8 => assert_ne!(
                    short_file_version(&result),
                    receipt_ref.lock().unwrap().as_str()
                ),
                _ => unreachable!("fixed request program"),
            }
            let requests = provider.wire.requests.lock().unwrap();
            let continuation = &requests[before + 1].1;
            let replayed = continuation["messages"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|row| row["tool_calls"].as_array().into_iter().flatten())
                .find(|row| row["id"] == call)
                .unwrap();
            let args: Value =
                serde_json::from_str(replayed["function"]["arguments"].as_str().unwrap()).unwrap();
            let expected = if matches!(before, 2 | 6) {
                &read_ref
            } else {
                &receipt_ref
            };
            assert_eq!(args["expected_version"], expected.lock().unwrap().as_str());
            assert_eq!(
                fs::read_to_string(&file).unwrap(),
                if before == 8 { PATCHED } else { UPDATED }
            );
            assert_eq!(
                fs::read_to_string(workspace.join("other.txt")).unwrap(),
                UPDATED
            );
        }
        manager.close().await.unwrap();
    }
}

#[cfg(target_os = "linux")]
fn short_file_version(result: &Value) -> &str {
    let version = result["version"]
        .as_str()
        .unwrap_or_else(|| panic!("missing version: {result}"));
    let suffix = version.strip_prefix("fv_").expect("negotiated namespace");
    assert!(
        [16, 20, 24].contains(&suffix.len())
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{version}"
    );
    version
}

fn assert_read(result: &Value, version: &str) {
    assert_eq!(
        result["content"],
        format!("1\t{ORIGINAL}"),
        "read result: {result}"
    );
    assert_eq!(result["start_line"], 1);
    assert_eq!(result["truncated"], false);
    assert_eq!(result["version"], version);
}

fn assert_denied(result: &Value) {
    assert_eq!(
        result["diagnostic"]["code"], "permission_denied",
        "tool result: {result}"
    );
    assert_eq!(
        result["diagnostic"]["phase"], "authorization",
        "tool result: {result}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires MICROCRAB_TEST_ZIG_CODING_COMPONENT, MICROCRAB_TEST_CODING_COMPONENT, and MICROCRAB_TEST_UNIVERSAL_COMPONENT"]
async fn zig_coding_candidate_preserves_rust_pins_and_host_policy() {
    let (zig, zig_digest) = component("MICROCRAB_TEST_ZIG_CODING_COMPONENT");
    let (mut rust, rust_digest) = component("MICROCRAB_TEST_CODING_COMPONENT");
    let (universal, universal_digest) = component("MICROCRAB_TEST_UNIVERSAL_COMPONENT");
    assert_ne!(
        zig_digest, rust_digest,
        "the candidate cannot be the predecessor"
    );
    assert_ne!(zig_digest, universal_digest);
    assert_ne!(rust_digest, universal_digest);

    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    fs::create_dir(&workspace).unwrap();
    let file = workspace.join(FILE);
    let shell_marker = workspace.join(SHELL_MARKER);
    fs::write(&file, ORIGINAL).unwrap();
    let version = digest(ORIGINAL.as_bytes());
    let host = Arc::new(
        LocalHost::new(&workspace, Default::default()).expect("real shell-capable LocalHost"),
    );
    let info = host.info().unwrap();
    let shell = info.shell_tool_name().expect("Host advertises shell");
    // Fixed harmless scripts: no supplied text or external paths are interpolated.
    let (plan_script, control_script) = match shell {
        "bash" => (
            "printf '%s' 'approved shell fixture' > plan-shell.txt && printf 'plan-ok'",
            "printf 'control-ok'",
        ),
        "pwsh" => (
            "Set-Content -LiteralPath plan-shell.txt -Value 'approved shell fixture' -Encoding ascii -NoNewline; Write-Output 'plan-ok'",
            "Write-Output 'control-ok'",
        ),
        _ => panic!("unsupported LocalHost shell in this fixture: {shell}"),
    };
    let read_args = json!({"path":FILE});
    let shell_args = json!({"command":plan_script,"cwd":".","yield_ms":5000});
    let write_args = json!({"path":FILE,"content":UPDATED,"expected_version":version});
    let stale_args =
        json!({"path":FILE,"content":"must not overwrite\n","expected_version":version});
    let added = workspace.join(PATCH_FILE);
    let patch_args = |expected_version: &str| {
        json!({
            "patch":format!("*** Begin Patch\n*** Add File: {PATCH_FILE}\n+{PATCH_ADDED}*** Update File: {FILE}\n@@\n-{UPDATED}+{PATCHED}*** End Patch\n"),
            "preconditions":[
                {"path":PATCH_FILE,"expected":"missing"},
                {"path":FILE,"expected":{"version":expected_version}}
            ]
        })
    };
    let stale_patch = patch_args(&version);
    let patch = patch_args(&digest(UPDATED.as_bytes()));
    let calls = [
        ("rust-read", "read", &read_args),
        ("rust-plan-shell", shell, &shell_args),
        ("zig-read", "read", &read_args),
        ("zig-plan-write", "write", &write_args),
        ("zig-shell-denied", shell, &shell_args),
        ("zig-shell-approved", shell, &shell_args),
        ("zig-write", "write", &write_args),
        ("zig-stale-write", "write", &stale_args),
        ("zig-stale-apply-patch", "apply_patch", &stale_patch),
        ("zig-apply-patch", "apply_patch", &patch),
        (
            "universal-shell",
            shell,
            &json!({"command":control_script,"cwd":".","yield_ms":5000}),
        ),
    ];
    let replies: Vec<_> = calls
        .iter()
        .flat_map(|(id, tool, args)| [tool_reply(id, tool, args), stop_reply()])
        .collect();
    let provider = scripted_provider(replies.len(), replies).await;
    let mut config = config(root.path(), &provider, &provider);
    crate::project::seed_test_projects(
        &config.data_dir,
        json!([
            {"id":"project","name":"Project","root":workspace,"owners":[OWNER]}
        ]),
    );
    // Disable model review only; leave native Agent mode permissions untouched.
    config.session_defaults.review = Some(false);
    config.agent = Some(rust.clone());
    config.agents = vec![universal.clone()];
    let owner = Principal::for_test(OWNER);

    // Create actual predecessor history before introducing the candidate.
    let original = Manager::with_host(config.clone(), host.clone())
        .await
        .unwrap();
    let items = catalog(&original, &owner).await;
    let (rust_id, old, _) = create_selected(
        &original,
        &owner,
        catalog_entry(&items, "coding", &rust_digest),
        "plan",
    )
    .await;
    let operation = submit_scripted_turn(&original, &owner, &rust_id, "rust-read").await;
    settle_turn(&original, &old, &operation).await;
    let (rust_tools, read) = wire_result(&provider, 0, "rust-read", "read");
    assert_read(&read, &version);
    let pin = original
        .pins
        .component(&rust_id)
        .await
        .unwrap()
        .expect("persisted exact Rust pin");
    assert_eq!(pin.component_digest.as_deref(), Some(rust_digest.as_str()));
    let frozen = old.session.context_snapshot().unwrap();
    original.close().await.unwrap();
    drop(old);
    drop(original);

    // The old bytes remain admissible only for restoration, not new selection.
    rust.restore_only = true;
    config.agent = Some(zig);
    config.agents = vec![universal.clone(), rust];
    let manager = Manager::with_host(config.clone(), host.clone())
        .await
        .unwrap();
    let items = catalog(&manager, &owner).await;
    let coding = catalog_entry(&items, "coding", &zig_digest);
    let universal_entry = catalog_entry(&items, "universal", &universal_digest);
    assert!(items.iter().all(|item| item["component"] != rust_digest));
    let unselected = manager
        .command(
            &owner,
            &command(
                "session.create",
                None,
                json!({
                    "project_id":"project", "agent":"coding", "mode":"plan",
                    "operation_id":uuid::Uuid::new_v4().to_string()
                }),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(unselected.code, "component_required");
    let rejected = manager
        .command(&owner, &create_command("coding", "plan", &rust_digest))
        .await
        .unwrap_err();
    assert_eq!(rejected.code, "component_unavailable");
    let reopened = manager
        .command(&owner, &command("session.open", Some(&rust_id), json!({})))
        .await
        .unwrap();
    assert_eq!(reopened["session"]["mode"], "plan");
    let old = manager
        .active(manager.record(OWNER, &rust_id).await.unwrap(), true)
        .await
        .unwrap();
    assert_eq!(
        manager.pins.component(&rust_id).await.unwrap().as_ref(),
        Some(&pin)
    );
    let restored = old.session.context_snapshot().unwrap();
    assert_eq!(restored.epoch, frozen.epoch);
    assert_eq!(restored.tools, frozen.tools);
    // With an executor bound, an accidental Ask (rather than the old Deny)
    // would suspend the turn instead of returning the immediate denial below.
    bind_ask(&manager, &owner, &rust_id).await;
    let operation = submit_scripted_turn(&manager, &owner, &rust_id, "rust-plan-shell").await;
    settle_turn(&manager, &old, &operation).await;
    let (reopened_tools, denied) = wire_result(&provider, 2, "rust-plan-shell", shell);
    assert_eq!(reopened_tools, rust_tools);
    assert_denied(&denied);
    assert!(!shell_marker.try_exists().unwrap());

    let (plan_id, plan, lease) = create_selected(&manager, &owner, coding, "plan").await;
    let operation = submit_scripted_turn(&manager, &owner, &plan_id, "zig-read").await;
    settle_turn(&manager, &plan, &operation).await;
    let (zig_tools, read) = wire_result(&provider, 4, "zig-read", "read");
    assert_read(&read, &version);
    let tool_names = |tools: Value| {
        tools
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["function"]["name"].as_str().unwrap().to_owned())
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert_eq!(tool_names(zig_tools), tool_names(rust_tools));
    let operation = submit_scripted_turn(&manager, &owner, &plan_id, "zig-plan-write").await;
    settle_turn(&manager, &plan, &operation).await;
    assert_denied(&wire_result(&provider, 6, "zig-plan-write", "write").1);
    assert_eq!(fs::read_to_string(&file).unwrap(), ORIGINAL);

    for (before, call, decision) in [
        (8, "zig-shell-denied", "deny"),
        (10, "zig-shell-approved", "allow"),
    ] {
        let mut events = plan.hub.subscribe();
        let operation = submit_scripted_turn(&manager, &owner, &plan_id, call).await;
        let request = pending_ask(&mut events, call, shell, "plan").await;
        assert_eq!(request["arguments"], shell_args);
        assert!(
            !shell_marker.try_exists().unwrap(),
            "shell reached LocalHost before approval"
        );
        assert_eq!(
            provider.wire.requests.lock().unwrap().len(),
            before + 1,
            "provider must wait for the pending decision"
        );
        decide_ask(&manager, &owner, &plan_id, &lease, &request, decision).await;
        settle_turn(&manager, &plan, &operation).await;
        let (_, result) = wire_result(&provider, before, call, shell);
        if decision == "allow" {
            assert_eq!(fs::read_to_string(&shell_marker).unwrap(), SHELL_TEXT);
            assert_shell(&result, "plan-ok");
        } else {
            assert_denied(&result);
            assert!(
                !shell_marker.try_exists().unwrap(),
                "denied shell touched the workspace"
            );
        }
    }

    let (build_id, build, lease) = create_selected(&manager, &owner, coding, "build").await;
    for (before, call, expected) in [
        (12, "zig-write", ORIGINAL),
        (14, "zig-stale-write", UPDATED),
    ] {
        let mut events = build.hub.subscribe();
        let operation = submit_scripted_turn(&manager, &owner, &build_id, call).await;
        let request = pending_ask(&mut events, call, "write", "build").await;
        assert_eq!(fs::read_to_string(&file).unwrap(), expected);
        decide_ask(&manager, &owner, &build_id, &lease, &request, "allow").await;
        settle_turn(&manager, &build, &operation).await;
        let (_, result) = wire_result(&provider, before, call, "write");
        if call == "zig-write" {
            assert_eq!(
                result["version"],
                digest(UPDATED.as_bytes()),
                "write result: {result}"
            );
            assert_eq!(result["bytes"], UPDATED.len());
        } else {
            assert_eq!(
                result["failure"]["code"], "conflict",
                "stale mutation result: {result}"
            );
        }
        assert_eq!(fs::read_to_string(&file).unwrap(), UPDATED);
    }

    // Put Add before the stale Update: whole-call preflight must protect the
    // earlier destination as well as the source. The next turn supplies the
    // current version to the same patch, then checks ordered commit receipts.
    for (before, call, args, succeeds) in [
        (16, "zig-stale-apply-patch", &stale_patch, false),
        (18, "zig-apply-patch", &patch, true),
    ] {
        let mut events = build.hub.subscribe();
        let operation = submit_scripted_turn(&manager, &owner, &build_id, call).await;
        let request = pending_ask(&mut events, call, "apply_patch", "build").await;
        assert_eq!(&request["arguments"], args);
        assert!(!added.try_exists().unwrap());
        assert_eq!(fs::read_to_string(&file).unwrap(), UPDATED);
        decide_ask(&manager, &owner, &build_id, &lease, &request, "allow").await;
        settle_turn(&manager, &build, &operation).await;
        let (_, result) = wire_result(&provider, before, call, "apply_patch");
        if succeeds {
            assert_eq!(result["complete"], true, "apply_patch result: {result}");
            assert_eq!(result["replay_safe"], false);
            let receipts = result["receipts"]
                .as_array()
                .expect("ordered patch receipts");
            assert_eq!(receipts.len(), 2);
            for (index, path, contents) in [(0, PATCH_FILE, PATCH_ADDED), (1, FILE, PATCHED)] {
                let receipt = &receipts[index];
                assert_eq!(receipt["operation"], index + 1);
                assert_eq!(receipt["paths"], json!([path]));
                assert_eq!(receipt["status"], "committed");
                assert_eq!(receipt["outcome"]["version"], digest(contents.as_bytes()));
                assert_eq!(fs::read_to_string(workspace.join(path)).unwrap(), contents);
            }
        } else {
            assert_eq!(
                result["failure"]["code"], "conflict",
                "apply_patch result: {result}"
            );
            assert!(
                !added.try_exists().unwrap(),
                "stale Update must prevent earlier Add"
            );
            assert_eq!(fs::read_to_string(&file).unwrap(), UPDATED);
        }
    }

    // Universal still advertises and runs the same Host shell in execute, with
    // no permission executor left after init, and never inherits Coding file tools.
    assert_eq!(universal_entry["tools"].as_array().unwrap().len(), 1);
    assert_eq!(universal_entry["tools"][0]["name"], shell);
    let (control_id, control, lease) =
        create_selected(&manager, &owner, universal_entry, "execute").await;
    let unbound = manager
        .client_tool_command(
            &owner,
            SOCKET,
            &command(
                "client_tools.unbind",
                Some(&control_id),
                json!({"lease_id":lease}),
            ),
        )
        .await
        .unwrap();
    assert_eq!(unbound["detached"], true);
    let operation = submit_scripted_turn(&manager, &owner, &control_id, "universal-shell").await;
    settle_turn(&manager, &control, &operation).await;
    let (control_tools, result) = wire_result(&provider, 20, "universal-shell", shell);
    assert!(!tool_names(control_tools).contains("read"));
    assert_shell(&result, "control-ok");
    manager.close().await.unwrap();
    drop((manager, old, plan, build, control));

    // Simulate missing retained bytes without modifying the supplied artifact.
    let missing_path = root.path().join("missing-retained-coding.wasm");
    assert!(!missing_path.try_exists().unwrap());
    config.agents[1].path = missing_path;
    let manager = Manager::with_host(config, host).await.unwrap();
    let error = manager
        .command(&owner, &command("session.open", Some(&rust_id), json!({})))
        .await
        .unwrap_err();
    assert_eq!(error.code, "invalid_request");
    assert!(
        error.message.contains("pinned agent component unavailable"),
        "{error:?}"
    );
    assert!(
        error.message.contains(&format!("SHA-256 {rust_digest}")),
        "{error:?}"
    );
    assert!(
        error.message.contains("Restore the exact Agent component"),
        "{error:?}"
    );
    assert_eq!(
        manager.pins.component(&rust_id).await.unwrap().as_ref(),
        Some(&pin)
    );
    assert_eq!(
        provider.wire.requests.lock().unwrap().len(),
        calls.len() * 2,
        "failed recovery must not call the model"
    );
    manager.close().await.unwrap();
}

fn assert_shell(result: &Value, expected: &str) {
    assert_eq!(result["exit_code"], 0, "shell result: {result}");
    assert_eq!(result["stdout"].as_str().unwrap().trim(), expected);
    assert_eq!(result["stderr"], "");
}

#[path = "zig_plan_markdown_acceptance.rs"]
mod zig_plan_markdown_acceptance;
