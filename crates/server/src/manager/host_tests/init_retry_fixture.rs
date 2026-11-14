//! Shared actual WASM construction and authenticated executor setup for init tests.
use super::*;
#[path = "../../../tests/support/host_shell_fixture.rs"]
mod host_shell_fixture;
#[allow(dead_code)]
#[path = "../../../tests/support/typed_agent_fixture.rs"]
mod typed_agent_fixture;

pub(super) fn configuration(root: &std::path::Path, mode: &str, effects: bool) -> Config {
    let mut configuration = config(root);
    configuration.storage = crate::config::Storage::File;
    let catalog = r#"[{"name":"init-shell","modes":[{"name":"execute","role":"both","permissions":[{"tool":{"kind":"exact","value":"echo"},"args":[],"action":"allow"}]},{"name":"restricted","role":"both","permissions":[{"tool":{"kind":"exact","value":"echo"},"args":[],"action":"deny"}]},{"name":"ask","role":"both","permissions":[{"tool":{"kind":"exact","value":"echo"},"args":[],"action":"ask"}]}],"variables":{},"tools":[{"name":"echo","description":"fixture","parameters":{"type":"object"}}]}]"#;
    let source = host_shell_fixture::shell_component_source(false, catalog, 0)
        .replace("typed-fixture", "echo x>proof ");
    let init = if effects {
        typed_agent_fixture::READY_INIT_BODY.replacen("(local $result i32)", "(local $result i32) (local $probe i32)\n i32.const 1 i32.const 1 i32.const 0 i32.const 0 i32.const 0 i32.const 0 call $invoke local.set $probe\n local.get $probe i32.load8_u if unreachable end", 1)
    } else {
        typed_agent_fixture::READY_INIT_BODY.into()
    };
    let path = root.join("init-fixture.wasm");
    std::fs::write(
        &path,
        typed_agent_fixture::component_with_init(&source, &init),
    )
    .unwrap();
    configuration.agent = Some(crate::config::Agent {
        path,
        name: Some("init-shell".into()),
        mode: Some(mode.into()),
        restore_only: false,
    });
    configuration
}
/// Delegate to the real process backend and count successful admissions. Init
/// cleanup may kill a fast shell before it writes a file; its returned native
/// handle is the deterministic witness that the Host actually launched it.
pub(super) struct ObservedHost {
    local: microcrab_host_local::LocalHost,
    pub launches: std::sync::atomic::AtomicUsize,
}
use microcrab_host_api::{
    Capabilities, HostInfo, Platform,
    process::{ExitStatus, ProcessHandle, ProcessStatus},
    types::{OpContext, Result as HostResult},
};
#[async_trait::async_trait]
impl ProcessApi for ObservedHost {
    fn has_running_processes(&self) -> bool {
        self.local.has_running_processes()
    }
    fn has_unresolved_cleanup(&self) -> bool {
        self.local.has_unresolved_cleanup()
    }
    fn watch(&self, handle: ProcessHandle) -> HostResult<tokio::sync::watch::Receiver<u64>> {
        self.local.watch(handle)
    }
    async fn status(&self, ctx: &OpContext, handle: ProcessHandle) -> HostResult<ProcessStatus> {
        ProcessApi::status(&self.local, ctx, handle).await
    }
    async fn wait(&self, ctx: &OpContext, handle: ProcessHandle) -> HostResult<ExitStatus> {
        ProcessApi::wait(&self.local, ctx, handle).await
    }
    async fn kill_tree(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        force: bool,
    ) -> HostResult<()> {
        self.local.kill_tree(ctx, handle, force).await
    }
    async fn release(&self, ctx: &OpContext, handle: ProcessHandle) -> HostResult<()> {
        self.local.release(ctx, handle).await
    }
}
#[async_trait::async_trait]
impl ShellApi for ObservedHost {
    async fn launch(&self, ctx: &OpContext, request: ShellRequest) -> HostResult<ProcessHandle> {
        let handle = self.local.launch(ctx, request).await?;
        self.launches
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(handle)
    }
    async fn capture_status(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
    ) -> HostResult<microcrab_host_api::ShellCaptureStatus> {
        self.local.capture_status(ctx, handle).await
    }
    async fn read(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        from: Option<microcrab_host_api::ShellPosition>,
    ) -> HostResult<microcrab_host_api::ShellOutput> {
        ShellApi::read(&self.local, ctx, handle, from).await
    }
    async fn drain(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        max_bytes: usize,
    ) -> HostResult<microcrab_host_api::ShellOutput> {
        self.local.drain(ctx, handle, max_bytes).await
    }
    async fn write(
        &self,
        ctx: &OpContext,
        handle: ProcessHandle,
        data: Vec<u8>,
        eof: bool,
    ) -> HostResult<()> {
        ShellApi::write(&self.local, ctx, handle, data, eof).await
    }
}
impl FsApi for ObservedHost {}
impl NetApi for ObservedHost {}
impl HostAPI for ObservedHost {
    fn platform(&self) -> Platform {
        self.local.platform()
    }
    fn capabilities(&self) -> &Capabilities {
        self.local.capabilities()
    }
}
#[async_trait::async_trait]
impl Host for ObservedHost {
    fn info(&self) -> Result<HostInfo, ToolError> {
        self.local.info()
    }
    async fn normalize_resource(
        &self,
        path: std::path::PathBuf,
        cancellation: CancellationToken,
    ) -> Result<String, ToolError> {
        self.local.normalize_resource(path, cancellation).await
    }
}
pub(super) fn host(root: &std::path::Path) -> Arc<ObservedHost> {
    Arc::new(ObservedHost {
        local: microcrab_host_local::LocalHost::new(root.join("project-a"), Default::default())
            .unwrap(),
        launches: std::sync::atomic::AtomicUsize::new(0),
    })
}
pub(super) async fn admit(manager: &Arc<Manager>, mode: &str) -> Arc<Managed> {
    let create: Command = serde_json::from_value(json!({"v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),"method":commands::SESSION_CREATE,"params":{"project_id":"a","agent":"init-shell","mode":mode,"operation_id":uuid::Uuid::new_v4().to_string()}})).unwrap();
    let admitted = commands::create(manager, "owner", &create).await.unwrap();
    let id = admitted["id"].as_str().unwrap();
    let slot = manager.registry.lock().unwrap().get(id).cloned().unwrap();
    let managed = slot.active.lock().await.clone().unwrap();
    assert!(super::super::super::agent_init::deferred(&managed));
    assert!(managed.session.state().agent_init.is_empty());
    let revision = managed
        .session
        .permission_controller()
        .snapshot()
        .unwrap()
        .revision;
    // Tool mode defaults cannot authorize component initialization. The owner
    // selects the independent lifecycle gate before any imported Host effect.
    let initialization = match mode {
        "execute" => "allow",
        "restricted" => "deny",
        "ask" => "ask",
        _ => panic!("unexpected initialization fixture mode"),
    };
    let overrides = json!([{"mode":"restricted","tool":{"kind":"exact","value":"echo"},"args":[],"action":"deny"}]);
    commands::execute(
        manager,
        &Principal::for_test("owner"),
        &command(
            commands::PERMISSION_SET,
            id,
            json!({"expected_revision":revision,"overrides":overrides,"initialization":initialization,"review":false}),
        ),
    )
    .await
    .unwrap();
    managed
}
pub(super) async fn bind(manager: &Arc<Manager>, managed: &Arc<Managed>) -> String {
    let id = managed.record.lock().await.id.clone();
    let digest = managed.client_tools.snapshot().await.catalog_digest;
    manager
        .client_tool_command(
            &Principal::for_test("owner"),
            "executor",
            &command(
                commands::CLIENT_TOOLS_BIND,
                &id,
                json!({"client_id":"init-test","catalog_digest":digest}),
            ),
        )
        .await
        .unwrap()["lease_id"]
        .as_str()
        .unwrap()
        .to_owned()
}
