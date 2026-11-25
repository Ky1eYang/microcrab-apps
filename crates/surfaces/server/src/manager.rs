//! Authenticated Session ownership. Accepted commands and activation are tracked;
//! sockets own subscriptions only. Registry/admission locks never cover async IO.
mod agent_init;
mod assembly;
mod attachments;
mod audit;
mod auto_compaction;
pub(crate) mod bindings;
mod cancellation;
mod child_input;
mod child_inspection;
pub(crate) mod citations;
pub(crate) mod client_tools;
pub(crate) mod commands;
mod compaction;
mod conversation_flavors;
pub(crate) mod conversations;
mod deletion;
mod detached;
pub(crate) mod direct;
mod display;
pub(crate) mod execution_history;
pub(crate) mod external_inbox;
pub(crate) mod external_speakers;
mod fork;
mod host_inspection;
#[cfg(test)]
#[path = "../tests/support/host_shell_fixture.rs"]
mod host_shell_fixture;
#[cfg(test)]
mod host_tests;
mod hub;
mod images;
mod inbox;
mod model;
mod model_metadata;
#[cfg(test)]
mod model_tests;
mod naming;
mod observation;
mod permission_ask;
mod presentation;
mod prompt_defaults;
pub(crate) mod prompt_history;
mod providers;
pub(crate) mod question_images;
mod round_limit;
pub(crate) mod session_watch;
pub(crate) mod settings;
mod statistics;
#[cfg(test)]
mod tests;
#[cfg(test)]
#[expect(
    dead_code,
    reason = "Shared typed fixture also provides embedding helpers used by separate integration test targets"
)]
#[path = "../tests/support/typed_agent_fixture.rs"]
mod typed_agent_fixture;
mod view;
use crate::{
    auth::Principal,
    config::{Config, Storage},
};
pub(crate) use hub::publication;
pub use hub::{Notification, ResyncReason, SessionRuntimeEvent, Subscription};
pub use model::image_hosting::{PreparedImageHostingRoute, ProviderImageHosting};
use microcrab_creator::{Children, UsageLedger, audit::NativeAuditor};
use microcrab_host_api::Host;
use microcrab_protocol::application::{Command, WireError};
use microcrab_session::{Error, Session};
use microcrab_storage::{CreatorJournal, Directory, SessionRecord};
use serde_json::Value;
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::{
    sync::{Mutex as AsyncMutex, Semaphore, oneshot},
    task::JoinSet,
};

pub struct Manager {
    pub(crate) config: Config,
    model_metadata: Arc<model_metadata::ModelMetadata>,
    pub(crate) projects: crate::project::ProjectRegistry,
    host: Arc<dyn Host>,
    embedded: bool,
    pins: microcrab_storage::execution::ExecutionPins,
    conversation_flavors: Arc<conversation_flavors::Flavors>,
    routes: AsyncMutex<HashMap<String, bindings::Route<bindings::ProjectCarrier>>>,
    project_host_changes: tokio::sync::broadcast::Sender<String>,
    direct_hosts: direct::Hosts,
    conversation_routes: AsyncMutex<HashMap<String, bindings::Route>>,
    // Serialize canonical identity lookup, immutable pin publication and metadata finalization.
    identity_gate: AsyncMutex<()>,
    speaker_gates: Mutex<HashMap<String, std::sync::Weak<AsyncMutex<()>>>>,
    external_messaging_markers: Mutex<HashMap<(String, String), std::sync::Weak<()>>>,
    external_messaging_quarantine: Mutex<std::collections::HashSet<String>>,
    deployment: Mutex<Option<crate::deployment::DeploymentGuard>>,
    next_route: AtomicU64,
    directory: Arc<dyn Directory>,
    pub(crate) configuration: Arc<dyn microcrab_storage::configuration::Configuration>,
    registry: Mutex<HashMap<String, Arc<Slot>>>,
    journal_open_gates: Mutex<HashMap<String, std::sync::Weak<AsyncMutex<()>>>>,
    read_hubs: Mutex<HashMap<String, std::sync::Weak<hub::Hub>>>,
    /// Runtime-only child selection authority, isolated by owner/root/actual child.
    #[expect(
        clippy::type_complexity,
        reason = "The single child Journal registry retains exact owner, root and child identity"
    )]
    child_journals:
        Arc<Mutex<HashMap<(String, String, String), microcrab_storage::CreatorJournal>>>,
    /// Dormant journal projections keyed by exact manifest bytes (bounded LRU).
    dormant_views: Mutex<std::collections::VecDeque<Arc<detached::Dormant>>>,
    /// Owner-level `sessions.changed` publication.
    session_watch: session_watch::SessionWatch,
    client_tool_owners:
        Mutex<HashMap<String, Arc<tokio::sync::OnceCell<Arc<client_tools::ClientTools>>>>>,
    closed: AtomicBool,
    instance: String,
    self_ref: std::sync::Weak<Manager>,
    client_override: Option<Arc<dyn microcrab_session::Client>>,
    bytes_ingress_admission: Arc<Semaphore>,
    citation_admission: Arc<Semaphore>,
    external_reply_admission: Arc<Semaphore>,
    external_reply_stop: tokio_util::sync::CancellationToken,
    admission: Arc<Semaphore>,
    control_admission: Arc<Semaphore>,
    tasks: Mutex<JoinSet<()>>,
    shutdown: AsyncMutex<()>,
    shutdown_result: Mutex<Option<Result<(), String>>>,
}
struct Slot {
    project_id: Option<String>,
    session_id: String,
    active: AsyncMutex<Option<Arc<Managed>>>,
    pending_suspension: Mutex<Option<microcrab_session::InterruptReason>>,
    initializing: AtomicBool,
    initialization_deferred: AtomicBool,
}
impl Slot {
    /// Caller holds `active`. Consume route loss once, including before a runtime
    /// exists; event publication and admission errors stay with the caller.
    fn take_pending_suspension(&self, active: Option<&Managed>) -> bool {
        let Some(reason) = self
            .pending_suspension
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        else {
            return false;
        };
        if let Some(managed) = active {
            let had_active_turn =
                managed.hub.operation().is_some() || managed.session.inbox_snapshot().active;
            let diagnostic = cancellation::wire(Some(&reason));
            let detail = diagnostic
                .details
                .as_ref()
                .expect("cancellation projection has details");
            tracing::info!(event="session.suspended_interrupt", project_id=self.project_id.as_deref(),
                session_id=%self.session_id, cause_code=detail["cancel_reason"].as_str(),
                host_detach_code=detail["host_detach_code"].as_str(), had_active_turn);
            managed.suspended.store(true, Ordering::Release);
            managed.session.interrupt_with_reason(reason);
        }
        true
    }
}
struct Managed {
    host_flavor: Option<conversation_flavors::HostFlavor>,
    session: Session,
    slot: std::sync::Weak<Slot>,
    routing: model::Routing,
    journal: CreatorJournal,
    host: Arc<dyn Host>,
    children: Children,
    auditor: Arc<NativeAuditor>,
    client_tools: Arc<client_tools::ClientTools>,
    permission_ask: Arc<permission_ask::PermissionAsk>,
    presentations: Arc<presentation::Presentations>,
    round_limit: Arc<round_limit::RoundLimit>,
    ledger: UsageLedger,
    aggregate: UsageLedger,
    statistics: statistics::Statistics,
    statistics_events: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
    record: AsyncMutex<SessionRecord>,
    commands: AsyncMutex<()>,
    worker: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
    child_drivers: AsyncMutex<JoinSet<Result<(), WireError>>>,
    child_factory: Arc<microcrab_creator::delegate::NativeChildFactory>,
    tool_task_events: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
    naming: AsyncMutex<Option<naming::Task>>,
    auto_naming: AtomicBool,
    naming_admission: Arc<naming::Admission>,
    naming_token: tokio_util::sync::CancellationToken,
    hub: Arc<hub::Hub>,
    closing: AtomicBool,
    close_token: tokio_util::sync::CancellationToken,
    faulted: AtomicBool,
    suspended: AtomicBool,
    component: Option<String>,
    agent_wasm_fuel: u64,
    /// Resolved only from this assembly's exact component and fixed Agent.
    default_mode: Result<Option<String>, String>,
    cleanup: AsyncMutex<Option<ManagedCleanup>>,
}
#[derive(Clone)]
struct ManagedCleanup {
    // Each owner runs once, so this list is bounded by the nine cleanup stages.
    // Retained failures are the sole authority for sticky quarantine across retries.
    failures: Vec<CleanupFailure>,
    session_failure: Option<CleanupDetail>,
    reconciliation_failure: Option<CleanupDetail>,
}
#[derive(Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum CleanupStage {
    TurnOwner,
    ChildTurns,
    ToolTaskEvents,
    StatisticsEvents,
    Naming,
    Children,
    ClientTools,
    Ledger,
    Aggregate,
    Auditor,
}
#[derive(Clone, serde::Serialize)]
struct CleanupDetail {
    code: String,
    message: String,
}
impl CleanupDetail {
    fn known(code: &'static str, message: &'static str) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
    fn from_error(error: Error) -> Self {
        let error = safe(error);
        Self {
            code: error.code,
            message: error.message,
        }
    }
    fn from_join(error: tokio::task::JoinError) -> Self {
        // Never format JoinError: its Display text can include a private panic payload.
        let (code, message) = if error.is_panic() {
            ("panic", "Cleanup owner panicked")
        } else if error.is_cancelled() {
            ("cancelled", "Cleanup owner was cancelled")
        } else {
            ("join_failed", "Cleanup owner join failed")
        };
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}
#[derive(Clone, serde::Serialize)]
struct CleanupFailure {
    stage: CleanupStage,
    #[serde(flatten)]
    detail: CleanupDetail,
}
impl ManagedCleanup {
    fn wire_error(&self) -> WireError {
        let mut details = serde_json::json!({
            "reason": "cleanup_pending",
            "quarantined": true,
            "cleanup_failures": self.failures,
            "sticky_failure": self.sticky_failure(),
            "session_cleanup_unresolved": self.session_failure.is_some(),
        });
        if let Some(failure) = self.session_failure.as_ref() {
            details["session_cleanup_failure"] = serde_json::json!(failure);
        }
        if let Some(failure) = self.reconciliation_failure.as_ref() {
            details["reconciliation_failure"] = serde_json::json!(failure);
        }
        let mut error = WireError::new("operation_failed", "Session cleanup has not completed");
        error.details = Some(Box::new(details));
        error
    }
    fn sticky_failure(&self) -> bool {
        !self.failures.is_empty()
    }
    fn retain(&mut self, stage: CleanupStage, result: Result<(), Error>) {
        if let Err(error) = result {
            self.failures.push(CleanupFailure {
                stage,
                detail: CleanupDetail::from_error(error),
            });
        }
    }
    fn retain_join(&mut self, stage: CleanupStage, result: Result<(), tokio::task::JoinError>) {
        if let Err(error) = result {
            self.failures.push(CleanupFailure {
                stage,
                detail: CleanupDetail::from_join(error),
            });
        }
    }
}
impl Manager {
    #[cfg(not(test))]
    pub(crate) fn start_model_metadata(&self) {
        self.model_metadata.start();
    }
    async fn client_tools(
        &self,
        session_id: &str,
    ) -> Result<Arc<client_tools::ClientTools>, Error> {
        let cell = {
            let mut owners = self
                .client_tool_owners
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            owners
                .entry(session_id.to_owned())
                .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                .clone()
        };
        cell.get_or_try_init(|| client_tools::ClientTools::open(&self.config.data_dir, session_id))
            .await
            .cloned()
    }

    pub(crate) async fn client_tool_command(
        &self,
        principal: &Principal,
        socket_id: &str,
        command: &Command,
    ) -> Result<Value, WireError> {
        let id = command.session_id.as_deref().ok_or_else(unavailable)?;
        let record = self.record(&principal.owner, id).await?;
        let tools = self.client_tools(id).await.map_err(safe)?;
        match command.method.as_str() {
            "client_tools.decision" => {
                #[derive(serde::Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Request {
                    kind: String,
                    request_id: String,
                }
                let params = commands::parse::<Request>(command)?;
                if !matches!(
                    params.kind.as_str(),
                    "permission" | "permission_proposal" | "round_limit"
                ) || params.request_id.len() > 128
                {
                    return Err(WireError::new(
                        "invalid_params",
                        "Invalid interaction receipt identity",
                    ));
                }
                tools
                    .with_participant(socket_id, |participant| {
                        if !participant {
                            return Err(WireError::new(
                                "permission_denied",
                                "Bound owner participant required",
                            ));
                        }
                        tools
                            .decision(&params.kind, &params.request_id)
                            .ok_or_else(|| {
                                WireError::new("not_found", "Interaction outcome not retained")
                            })
                    })
                    .await
            }
            "client_tools.takeover" => {
                let params = commands::parse::<client_tools::BindRequest>(command)?;
                // Compatibility for older frontends. This explicit command may
                // not replace the executor selected by an active input owner.
                let slot = self.registry.lock().unwrap_or_else(|error| error.into_inner()).get(&record.id).cloned();
                let active = match slot {
                    Some(slot) => slot.active.lock().await.clone(),
                    None => None,
                };
                let _gate = match active.as_ref() {
                    Some(managed) => {
                        let gate = managed.commands.lock().await;
                        commands::idle(managed)?;
                        Some(gate)
                    }
                    None => None,
                };
                serde_json::to_value(tools.takeover_idle(socket_id, &params).await.map_err(safe)?)
                    .map_err(|_| unavailable())
            }
            "client_tools.arguments" => {
                #[derive(serde::Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Request {
                    invocation_id: String,
                    lease_id: String,
                }
                let params = commands::parse::<Request>(command)?;
                let arguments = tools
                    .private_arguments(socket_id, &params.invocation_id, &params.lease_id)
                    .await
                    .map_err(safe)?;
                Ok(serde_json::json!({"invocation_id":params.invocation_id,"arguments":arguments}))
            }
            "client_tools.secret" => {
                let params = commands::parse::<client_tools::SecretRequest>(command)?;
                serde_json::to_value(tools.secret(socket_id, params).await.map_err(safe)?)
                    .map_err(|_| unavailable())
            }
            "client_tools.secret_operation" => {
                #[derive(serde::Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Request {
                    operation_id: String,
                }
                let params = commands::parse::<Request>(command)?;
                tools
                    .secret_operation(socket_id, &params.operation_id)
                    .await
                    .map_err(safe)
            }
            commands::CLIENT_TOOLS_BIND => {
                let params = commands::parse::<client_tools::BindRequest>(command)?;
                serde_json::to_value(tools.bind(socket_id, &params).await.map_err(safe)?)
                    .map_err(|_| unavailable())
            }
            commands::CLIENT_TOOLS_RESULT => {
                let params = commands::parse::<client_tools::ResultRequest>(command)?;
                serde_json::to_value(tools.result(socket_id, &params).await.map_err(safe)?)
                    .map_err(|_| unavailable())
            }
            commands::CLIENT_TOOLS_OUTPUT => {
                let params = commands::parse::<client_tools::OutputRequest>(command)?;
                serde_json::to_value(tools.output(socket_id, &params).await.map_err(safe)?)
                    .map_err(|_| unavailable())
            }
            commands::CLIENT_TOOLS_PREPARE => {
                let params = commands::parse::<client_tools::PrepareRequest>(command)?;
                serde_json::to_value(tools.prepare(socket_id, &params).await.map_err(safe)?)
                    .map_err(|_| unavailable())
            }
            commands::CLIENT_TOOLS_UNBIND => {
                #[derive(serde::Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Unbind {
                    lease_id: String,
                }
                let params = commands::parse::<Unbind>(command)?;
                let detached = tools
                    .unbind(socket_id, &params.lease_id)
                    .await
                    .map_err(safe)?;
                Ok(serde_json::json!({"detached":detached}))
            }
            _ => Err(unavailable()),
        }
    }

    pub(crate) async fn detach_client_tools(&self, socket_id: &str) {
        let cells: Vec<_> = self
            .client_tool_owners
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        for cell in cells {
            if let Some(tools) = cell.get()
                && let Err(error) = tools.detach(socket_id).await
            {
                tracing::warn!(error_code = %safe(error).code, "Client tool detach failed");
            }
        }
    }

    pub async fn new(config: Config) -> Result<Arc<Self>, Error> {
        let deployment = crate::deployment::DeploymentGuard::acquire_ownership(&config.data_dir)?;
        Self::initialize(
            config,
            Arc::new(crate::noop::NoopHost),
            None,
            false,
            Some(deployment),
        )
        .await
    }
    /// Reuse one caller-owned execution backend for catalog, main and child
    /// sessions, including restoration. The data directory is exclusively owned.
    pub async fn with_host(config: Config, host: Arc<dyn Host>) -> Result<Arc<Self>, Error> {
        let deployment = crate::deployment::DeploymentGuard::acquire_ownership(&config.data_dir)?;
        Self::initialize(config, host, None, true, Some(deployment)).await
    }
    pub(crate) async fn for_application(
        config: Config,
        host: Option<Arc<dyn Host>>,
        deployment: crate::deployment::DeploymentGuard,
    ) -> Result<Arc<Self>, Error> {
        let embedded = host.is_some();
        let host = host.unwrap_or_else(|| Arc::new(crate::noop::NoopHost));
        Self::initialize(config, host, None, embedded, Some(deployment)).await
    }
    #[cfg(test)]
    pub(crate) async fn with_client(
        config: Config,
        client: Arc<dyn microcrab_session::Client>,
    ) -> Result<Arc<Self>, Error> {
        let host = crate::host::startup_local_host(&config)?;
        Self::initialize_test(config, host, Some(client), true).await
    }
    #[cfg(test)]
    async fn initialize_test(
        config: Config,
        host: Arc<dyn Host>,
        client: Option<Arc<dyn microcrab_session::Client>>,
        embedded: bool,
    ) -> Result<Arc<Self>, Error> {
        let deployment = crate::deployment::DeploymentGuard::acquire_ownership(&config.data_dir)?;
        Self::initialize(config, host, client, embedded, Some(deployment)).await
    }
    async fn initialize(
        config: Config,
        host: Arc<dyn Host>,
        client_override: Option<Arc<dyn microcrab_session::Client>>,
        embedded: bool,
        deployment: Option<crate::deployment::DeploymentGuard>,
    ) -> Result<Arc<Self>, Error> {
        config.validate_storage_namespace().map_err(|error| {
            Error::config_with_private("Server storage namespace is invalid", error.to_string())
        })?;
        let directory: Arc<dyn Directory> = match config.storage {
            Storage::File => {
                #[cfg(feature = "storage-file")]
                {
                    Arc::new(
                        microcrab_storage::FileDirectory::open_with_history(
                            config.data_dir.join("directory"),
                            config.data_dir.join("sessions"),
                        )
                        .await?,
                    )
                }
                #[cfg(not(feature = "storage-file"))]
                {
                    return Err(Error::config("File storage support is not compiled"));
                }
            }
            Storage::Sqlite => {
                #[cfg(feature = "storage-sqlite")]
                {
                    Arc::new(
                        microcrab_storage::SqliteDirectory::open(
                            config.data_dir.join("storage.sqlite"),
                        )
                        .await?,
                    )
                }
                #[cfg(not(feature = "storage-sqlite"))]
                {
                    return Err(Error::config("SQLite support is not compiled"));
                }
            }
        };
        Self::initialize_storage(
            config,
            host,
            client_override,
            embedded,
            directory,
            deployment,
        )
        .await
    }
    #[cfg(test)]
    async fn initialize_with_directory(
        config: Config,
        host: Arc<dyn Host>,
        client_override: Option<Arc<dyn microcrab_session::Client>>,
        embedded: bool,
        directory: Arc<dyn Directory>,
    ) -> Result<Arc<Self>, Error> {
        Self::initialize_storage(config, host, client_override, embedded, directory, None).await
    }
    async fn initialize_storage(
        config: Config,
        host: Arc<dyn Host>,
        client_override: Option<Arc<dyn microcrab_session::Client>>,
        embedded: bool,
        directory: Arc<dyn Directory>,
        deployment: Option<crate::deployment::DeploymentGuard>,
    ) -> Result<Arc<Self>, Error> {
        let configuration: Arc<dyn microcrab_storage::configuration::Configuration> =
            match config.storage {
                Storage::File => {
                    #[cfg(feature = "storage-file")]
                    {
                        Arc::new(
                            microcrab_storage::configuration::FileConfiguration::open(
                                config.data_dir.join("configuration"),
                            )
                            .await?,
                        )
                    }
                    #[cfg(not(feature = "storage-file"))]
                    {
                        return Err(Error::config("File storage support is not compiled"));
                    }
                }
                Storage::Sqlite => {
                    #[cfg(feature = "storage-sqlite")]
                    {
                        Arc::new(
                            microcrab_storage::configuration::SqliteConfiguration::open(
                                config.data_dir.join("storage.sqlite"),
                            )
                            .await?,
                        )
                    }
                    #[cfg(not(feature = "storage-sqlite"))]
                    {
                        return Err(Error::config("SQLite support is not compiled"));
                    }
                }
            };
        config
            .validate_agent_wasm_fuel()
            .map_err(|e| Error::Config(e.to_string().into()))?;
        config
            .session_defaults
            .validate(&config.limit_ceilings)
            .map_err(|e| Error::Config(e.into()))?;
        let unowned = directory.unavailable_unowned_count().await?;
        if unowned != 0 {
            tracing::warn!(
                unavailable_records = unowned,
                "Directory contains unavailable records without valid owner attribution; original bytes retained"
            );
        }
        // Explicit embeddings still fail startup for invalid configured bytes;
        // discovery itself is performed against the selected Host at admission.
        if embedded && let Err(error) = assembly::components(&config, host.clone()).await {
            let _ = directory.close().await;
            return Err(error);
        }
        let pins =
            microcrab_storage::execution::ExecutionPins::new(config.data_dir.join("execution"));
        let conversation_flavors = Arc::new(conversation_flavors::Flavors::open(&config.data_dir)?);
        let projects = crate::project::ProjectRegistry::new(&config.data_dir)?;
        let model_metadata = model_metadata::ModelMetadata::open(&config).await?;
        let session_watch = session_watch::SessionWatch::new();
        let directory: Arc<dyn Directory> = Arc::new(session_watch::WatchedDirectory {
            inner: directory,
            dirty: session_watch.dirty(),
        });
        let manager = Arc::new_cyclic(|weak| Self {
            config,
            model_metadata,
            projects,
            host,
            embedded,
            pins,
            conversation_flavors,
            routes: AsyncMutex::new(HashMap::new()),
            project_host_changes: tokio::sync::broadcast::channel(128).0,
            direct_hosts: direct::Hosts::default(),
            conversation_routes: AsyncMutex::new(HashMap::new()),
            identity_gate: AsyncMutex::new(()),
            speaker_gates: Mutex::new(HashMap::new()),
            external_messaging_markers: Mutex::new(HashMap::new()),
            external_messaging_quarantine: Mutex::new(std::collections::HashSet::new()),
            deployment: Mutex::new(deployment),
            next_route: AtomicU64::new(1),
            directory,
            configuration,
            registry: Mutex::new(HashMap::new()),
            journal_open_gates: Mutex::new(HashMap::new()),
            read_hubs: Mutex::new(HashMap::new()),
            child_journals: Arc::new(Mutex::new(HashMap::new())),
            dormant_views: Mutex::new(std::collections::VecDeque::new()),
            session_watch,
            client_tool_owners: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            instance: uuid::Uuid::new_v4().to_string(),
            self_ref: weak.clone(),
            client_override,
            bytes_ingress_admission: Arc::new(Semaphore::new(4)),
            citation_admission: Arc::new(Semaphore::new(4)),
            external_reply_admission: Arc::new(Semaphore::new(32)),
            external_reply_stop: tokio_util::sync::CancellationToken::new(),
            admission: Arc::new(Semaphore::new(128)),
            control_admission: Arc::new(Semaphore::new(16)),
            tasks: Mutex::new(JoinSet::new()),
            shutdown: AsyncMutex::new(()),
            shutdown_result: Mutex::new(None),
        });
        manager.session_watch.start(Arc::downgrade(&manager));
        deletion::resume_pending_cleanup(&manager)
            .await
            .map_err(|error| {
                Error::config_with_private(
                    "Pending Session deletion recovery failed; inspect durable cleanup receipts",
                    format!(
                        "Deletion recovery failed ({}): {}",
                        error.code, error.message
                    ),
                )
            })?;
        Ok(manager)
    }
    /// Open/inspect/activate/archive transitions share this admission gate. Its weak
    /// index is pruned on lookup, so dormant history does not consume active or
    /// observation capacity. Lock order is journal gate, then Slot.active.
    fn journal_open_gate(&self, id: &str) -> Arc<AsyncMutex<()>> {
        let mut gates = self
            .journal_open_gates
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        gates.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = gates.get(id).and_then(std::sync::Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(AsyncMutex::new(()));
        gates.insert(id.to_owned(), Arc::downgrade(&gate));
        gate
    }
    fn track<T: Send + 'static>(
        &self,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Result<oneshot::Receiver<T>, WireError> {
        let mut tasks = self.tasks.lock().unwrap_or_else(|error| error.into_inner());
        if self.closed.load(Ordering::Acquire) {
            return Err(WireError::new("closed", "Server is shutting down"));
        }
        while tasks.try_join_next().is_some() {}
        let (sender, receiver) = oneshot::channel();
        tasks.spawn(tracing::Instrument::in_current_span(async move {
            let result = future.await;
            let _ = sender.send(result);
        }));
        Ok(receiver)
    }
    pub async fn command(
        &self,
        principal: &Principal,
        command: &Command,
    ) -> Result<Value, WireError> {
        self.command_with_sender(principal, command, None).await
    }

    /// Socket identity comes from the authenticated connection, never parameters.
    pub(crate) async fn command_with_sender(
        &self,
        principal: &Principal,
        command: &Command,
        sender_socket: Option<String>,
    ) -> Result<Value, WireError> {
        command.validate()?;
        if principal.expires_at <= now() / 1000 {
            return Err(WireError::new("unauthenticated", "Authentication expired"));
        }
        let lane = if command.method == "session.interrupt" {
            &self.control_admission
        } else {
            &self.admission
        };
        let permit = lane
            .clone()
            .try_acquire_owned()
            .map_err(|_| WireError::new("capacity", "Command capacity reached"))?;
        let manager = self
            .self_ref
            .upgrade()
            .ok_or_else(|| WireError::new("closed", "Manager unavailable"))?;
        let principal = principal.clone();
        let command = command.clone();
        self.track(async move {
            let _permit = permit;
            commands::execute_with_sender(&manager, &principal, &command, sender_socket).await
        })?
        .await
        .map_err(|_| WireError::new("operation_failed", "Command owner failed"))?
    }
    async fn record(&self, owner: &str, id: &str) -> Result<SessionRecord, WireError> {
        if !crate::config::valid_id(id) {
            return Err(unavailable());
        }
        let record = self
            .directory
            .get(owner, id)
            .await
            .map_err(safe)?
            .ok_or_else(unavailable)?;
        if !self.project_visible(&record).await {
            return Err(unavailable());
        }
        if record.model_selection.is_none() {
            // Legacy model selection migration is a write. Refuse a retired
            // live catalog even when the record predates model selection.
            // The selected head reader also works while an inspection caller
            // owns journal-open admission; never reacquire that gate here.
            self.check_retired_checkpoint(id).await?;
            return self.selected_record(record).await;
        }
        Ok(record)
    }
    async fn check_retired_checkpoint(&self, id: &str) -> Result<(), WireError> {
        if let Some(point) = self.inspect_detached_head(id).await.map_err(safe)? {
            if point.cursor.session_id != id || point.checkpoint.session_id != id {
                return Err(WireError::new(
                    "storage_unavailable",
                    "Selected catalog history belongs to another Session",
                ));
            }
            external_speakers::admission::check_retired(&point.checkpoint.recover.context.tools)?;
        }
        Ok(())
    }
    async fn active(
        &self,
        record: SessionRecord,
        restore: bool,
    ) -> Result<Arc<Managed>, WireError> {
        self.activate(record, restore, None, false).await
    }
    async fn activate(
        &self,
        record: SessionRecord,
        restore: bool,
        fork_source: Option<fork::Source>,
        allow_blocked_init: bool,
    ) -> Result<Arc<Managed>, WireError> {
        if !self.record_allowed(&record) {
            return Err(unavailable());
        }
        if record.archived {
            return Err(WireError::new("archived", "Session is archived"));
        }
        let manager = self
            .self_ref
            .upgrade()
            .ok_or_else(|| WireError::new("closed", "Manager unavailable"))?;
        self.track(async move {
            let journal_open = manager.journal_open_gate(&record.id).lock_owned().await;
            // Archive uses this same gate. Refresh after waiting so a restore
            // captured before DELETE cannot reopen an already archived writer.
            let record = manager
                .directory
                .get(&record.owner, &record.id)
                .await
                .map_err(safe)?
                .ok_or_else(unavailable)?;
            if record.archived {
                return Err(WireError::new("archived", "Session is archived"));
            }
            // Reject known retired execution evidence before slot publication,
            // model selector migration, or checking the live Host route.
            let already_live = manager.registry.lock().unwrap_or_else(|error| error.into_inner())
                .get(&record.id).is_some_and(|slot| slot.active.try_lock().is_ok_and(|active| active.is_some()));
            if restore && fork_source.is_none() && !already_live {
                manager.check_retired_checkpoint(&record.id).await?;
            }
            let record = manager.selected_record(record).await?;
            // Activation witnesses the project decision, but releases the owner
            // fence before Core restore, initialization or checkpointing.
            let _activation_marker = manager.reserve_project_catalog_operation(&record).await?;
            loop {
                let slot = {
                    let mut registry = manager
                        .registry
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    if let Some(slot) = registry.get(&record.id) {
                        slot.clone()
                    } else {
                        if registry.len() >= manager.config.max_active_sessions {
                            return Err(WireError::new(
                                "capacity",
                                "Active session capacity reached",
                            ));
                        }
                        let slot = Arc::new(Slot {
                            project_id: record.project_id.clone(),
                            session_id: record.id.clone(),
                            active: AsyncMutex::new(None),
                            pending_suspension: Mutex::new(None),
                            initializing: AtomicBool::new(false),
                            initialization_deferred: AtomicBool::new(false),
                        });
                        registry.insert(record.id.clone(), slot.clone());
                        slot
                    }
                };
                let mut active = slot.active.lock().await;
                // Close/project retirement can remove a Slot while this activation
                // waits for its guard. Never publish into that retired identity.
                let current = manager
                    .registry
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(&record.id)
                    .is_some_and(|current| Arc::ptr_eq(current, &slot));
                if !current {
                    continue;
                }
                if !manager.record_allowed(&record) {
                    return Err(unavailable());
                }
                if slot.take_pending_suspension(active.as_deref()) {
                    if let Some(session) = active.as_ref() {
                        session.hub.publish(SessionRuntimeEvent::HostConnection(
                            serde_json::json!({"state":"detached","reason":"explicit_reopen_required"}),
                        ));
                    }
                    return Err(WireError::new(
                        "host_detached",
                        "Host route changed; reopen the session",
                    ));
                }
                if !manager.embedded {
                    manager.record_host(&record).await?;
                }
                if let Some(session) = active.as_ref() {
                    if slot.initialization_deferred.load(Ordering::Acquire) && !allow_blocked_init {
                        return Err(agent_init::deferred_error());
                    }
                    if slot.initializing.load(Ordering::Acquire) && !allow_blocked_init {
                        return Err(agent_init::pending_error());
                    }
                    if session.closing.load(Ordering::Acquire) {
                        return Err(WireError::new("closed", "Session is closing"));
                    }
                    if session.suspended.load(Ordering::Acquire) {
                        if session.hub.operation().is_some() {
                            return Err(WireError::new(
                                "busy",
                                "Interrupted turn is still settling",
                            ));
                        }
                        // Host identity/root/component pins remain authoritative. Old
                        // full-workspace bytes are historical data, not attach gates.
                        session.suspended.store(false, Ordering::Release);
                    }
                    if !allow_blocked_init
                        && let Some(error) = agent_init::failure(&session.session)
                    {
                        return Err(error);
                    }
                    return Ok(session.clone());
                }
                if manager.closed.load(Ordering::Acquire) {
                    return Err(WireError::new("closed", "Server is shutting down"));
                }
                let speaker_owner = record.owner.clone();
                let speaker_project = record.project_id.clone();
                let speaker_session = record.id.clone();
                let managed = match fork_source.as_ref() {
                    Some(source) => assembly::create_from(&manager, record, restore, Some(source)).await,
                    None => assembly::create(&manager, record, restore).await,
                }
                .map_err(safe)?;
                let speakers = {
                    let _owner = manager.speaker_gate(&speaker_owner).lock_owned().await;
                    manager.synchronize_external_speakers_locked(&speaker_owner,speaker_project.as_deref(),&speaker_session,&managed.session).await
                };
                if let Err(error) = speakers {
                    if let Err(cause)=manager.close_managed(&managed).await {
                        let mut failure=WireError::new("operation_failed","Speaker synchronization failed and Session cleanup requires reconciliation");
                        failure.details=Some(Box::new(serde_json::json!({"cause":error,"cleanup":safe(cause)})));
                        return Err(failure);
                    }
                    return Err(error);
                }
                // A completed initialization permit (including no-WASM Sessions)
                // needs no interactive guest work. Preserve serialized eager start
                // under the existing locks, so concurrent opens reuse its result.
                let eager = managed.session.agent_initialization_complete();
                if eager {
                    managed.session.start().await.map_err(safe)?;
                    if fork_source.is_some() {
                        microcrab_session::Journal::commit(
                            &managed.journal,
                            managed.session.checkpoint().await.map_err(safe)?,
                        )
                        .await
                        .map_err(safe)?;
                    }
                }
                let initialization = if eager {
                    None
                } else {
                    Some(agent_init::Admission::acquire(slot.clone())?)
                };
                *active = Some(managed.clone());
                drop(active);
                drop(journal_open);
                if slot.initialization_deferred.load(Ordering::Acquire) {
                    // Admit identity and persist the pending batch before exposing
                    // create completion. Guest effects wait for bind then open.
                    microcrab_session::Journal::commit(
                        &managed.journal,
                        managed.session.checkpoint().await.map_err(safe)?,
                    )
                    .await
                    .map_err(safe)?;
                    drop(initialization);
                    return Ok(managed);
                }
                if !eager
                    && (!allow_blocked_init || agent_init::failure(&managed.session).is_none())
                {
                    let started = async {
                        managed.session.start().await?;
                        if fork_source.is_some() {
                            microcrab_session::Journal::commit(
                                &managed.journal,
                                managed.session.checkpoint().await?,
                            )
                            .await?;
                        }
                        Ok::<(), Error>(())
                    }
                    .await;
                    if let Err(error) = started {
                        if let Some(failure) = agent_init::failure(&managed.session) {
                            return Err(failure);
                        }
                        managed.faulted.store(true, Ordering::Release);
                        let _ = manager.close_managed(&managed).await;
                        return Err(safe(error));
                    }
                }
                drop(initialization);
                if managed.closing.load(Ordering::Acquire) {
                    return Err(WireError::new("closed", "Session is closing"));
                }
                let current_record = managed.record.lock().await.clone();
                if !manager.record_allowed(&current_record) {
                    managed.suspended.store(true, Ordering::Release);
                    managed.session.interrupt_with_reason(microcrab_session::InterruptReason::HostDetached {
                        code: microcrab_session::HostDetachCode::Unavailable,
                    });
                    return Err(unavailable());
                }
                if slot.take_pending_suspension(Some(&managed))
                    || managed.suspended.load(Ordering::Acquire)
                {
                    managed.hub.publish(SessionRuntimeEvent::HostConnection(
                        serde_json::json!({"state":"detached","reason":"explicit_reopen_required"}),
                    ));
                    return Err(WireError::new(
                        "host_detached",
                        "Host route changed; reopen the session",
                    ));
                }
                if !allow_blocked_init && let Some(error) = agent_init::failure(&managed.session) {
                    return Err(error);
                }
                return Ok(managed);
            }
        })?
        .await
        .map_err(|_| WireError::new("operation_failed", "Activation owner failed"))?
    }
    pub async fn subscribe(
        &self,
        principal: &Principal,
        id: &str,
    ) -> Result<Subscription, WireError> {
        self.subscribe_with_lineage(principal, id, None).await
    }

    pub(crate) async fn subscribe_with_lineage(
        &self,
        principal: &Principal,
        id: &str,
        lineage: Option<&str>,
    ) -> Result<Subscription, WireError> {
        if principal.expires_at <= now() / 1000 {
            return Err(WireError::new("unauthenticated", "Authentication expired"));
        }
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| WireError::new("capacity", "Command capacity reached"))?;
        let _permit = permit;
        if self.closed.load(Ordering::Acquire) {
            return Err(WireError::new("closed", "Server is shutting down"));
        }
        if !crate::config::valid_id(id) {
            return Err(unavailable());
        }
        // Read preparation belongs to the socket, not a detached task. Legacy
        // metadata migration, when required, retains its original owner task.
        let mut record = self.directory.get(&principal.owner, id).await.map_err(safe)?.ok_or_else(unavailable)?;
        if record.model_selection.is_none() {
            let manager = self.self_ref.upgrade().ok_or_else(|| WireError::new("closed", "Manager unavailable"))?;
            let owner = principal.owner.clone();
            let id = id.to_owned();
            record = self.track(async move { manager.record(&owner, &id).await })?
                .await.map_err(|_| WireError::new("operation_failed", "Subscription metadata owner failed"))??;
        }
        self.inspect_subscription_with_lineage(&record, lineage).await
    }
    /// Interactive requests that were outstanding when a client attached; the
    /// attach result carries them so the client needs no follow-up queries.
    pub async fn pending_requests(&self, principal: &Principal, id: &str) -> Value {
        let permissions = self
            .permission_requests(principal, id)
            .await
            .unwrap_or(Value::Null);
        let round_limits = self
            .round_limit_requests(principal, id)
            .await
            .unwrap_or(Value::Null);
        serde_json::json!({"permissions": permissions, "round_limits": round_limits})
    }
    async fn refresh(&self, managed: &Managed) -> Result<Value, Error> {
        view::refresh(
            self.directory.as_ref(),
            managed,
            &self.config.provider.model,
        )
        .await
    }
    async fn close_managed(&self, managed: &Managed) -> Result<(), Error> {
        self.close_managed_owned(managed, false)
            .await
            .map_err(|_| Error::lifecycle("Session cleanup failed"))
    }
    async fn cleanup_route_generation(&self, managed: &Managed) -> Result<u64, CleanupDetail> {
        if self.embedded {
            return Ok(0);
        }
        let record = managed.record.lock().await.clone();
        // Reuse immutable pin and owner checks. Readiness must belong to the same
        // retained transport generation before and after cleanup acknowledgement.
        let host = self
            .session_host(&record)
            .await
            .map_err(CleanupDetail::from_error)?;
        if !Arc::ptr_eq(&host, &managed.host) {
            return Err(CleanupDetail::known(
                "identity_changed",
                "Cleanup Host identity changed",
            ));
        }
        if managed.host_flavor == Some(conversation_flavors::HostFlavor::ChatOnly) {
            return Ok(0);
        }
        fn ready_generation<C>(route: &bindings::Route<C>, owner: &str) -> Option<u64> {
            (route.owner == owner && route.ready && route.remote.is_ready())
                .then_some(route.generation)
        }
        let generation = if let Some(project) = record.project_id.as_ref() {
            self.routes
                .lock()
                .await
                .get(project)
                .and_then(|route| ready_generation(route, &record.owner))
        } else {
            self.conversation_routes
                .lock()
                .await
                .get(&record.id)
                .and_then(|route| ready_generation(route, &record.owner))
        };
        generation
            .ok_or_else(|| CleanupDetail::known("host_not_ready", "Cleanup Host is not ready"))
    }
    async fn close_managed_owned(
        &self,
        managed: &Managed,
        reconcile: bool,
    ) -> Result<(), ManagedCleanup> {
        self.close_managed_with_policy(managed, reconcile, false)
            .await
    }
    async fn close_managed_for_delete(&self, managed: &Managed) -> Result<(), ManagedCleanup> {
        self.close_managed_with_policy(managed, true, true).await
    }
    async fn close_managed_with_policy(
        &self,
        managed: &Managed,
        reconcile: bool,
        discard_snapshot: bool,
    ) -> Result<(), ManagedCleanup> {
        // Return failure evidence under the same guard that determines the outcome;
        // a later explicit reconciliation may replace the cached Session failure.
        let mut cleanup = managed.cleanup.lock().await;
        if let Some(result) = cleanup.as_mut() {
            if reconcile && result.session_failure.is_some() {
                // This describes the current explicit attempt, not a historical route failure.
                result.reconciliation_failure = None;
                let generation = match self.cleanup_route_generation(managed).await {
                    Ok(generation) => generation,
                    Err(error) => {
                        result.reconciliation_failure = Some(error);
                        return Err(result.clone());
                    }
                };
                let settled = if discard_snapshot {
                    managed.session.settle_for_delete(true).await
                } else {
                    managed.session.reconcile_close().await
                };
                let checked = self
                    .cleanup_route_generation(managed)
                    .await
                    .and_then(|current| {
                        if current == generation {
                            Ok(())
                        } else {
                            Err(CleanupDetail::known(
                                "generation_changed",
                                "Cleanup Host generation changed",
                            ))
                        }
                    });
                if let Err(error) = checked {
                    result.reconciliation_failure = Some(error);
                    // A Session acknowledgement cannot supersede a failed Host fence.
                    if let Err(error) = settled {
                        result.session_failure = Some(CleanupDetail::from_error(error));
                    }
                    return Err(result.clone());
                }
                result.session_failure = settled.err().map(CleanupDetail::from_error);
            }
            return if result.sticky_failure() || result.session_failure.is_some() {
                Err(result.clone())
            } else {
                Ok(())
            };
        }
        // Foreground cancellation cannot wait behind a submitted naming CAS.
        managed.closing.store(true, Ordering::Release);
        managed
            .session
            .interrupt_with_reason(microcrab_session::InterruptReason::SessionClosed);
        managed.close_token.cancel();
        managed.hub.interrupt_operation();
        let gate = managed.commands.lock().await;
        let naming_result = naming::stop(managed).await;
        managed.permission_ask.close();
        managed.round_limit.close();
        drop(gate);
        let mut result = ManagedCleanup {
            failures: Vec::with_capacity(9),
            session_failure: None,
            reconciliation_failure: None,
        };
        if let Some(worker) = managed.worker.lock().await.take() {
            result.retain_join(CleanupStage::TurnOwner, worker.await);
        }
        {
            let mut drivers = managed.child_drivers.lock().await;
            while let Some(joined) = drivers.join_next().await {
                match joined {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => result.failures.push(CleanupFailure {
                        stage: CleanupStage::ChildTurns,
                        detail: CleanupDetail {
                            code: error.code,
                            message: error.message,
                        },
                    }),
                    Err(error) => result.failures.push(CleanupFailure {
                        stage: CleanupStage::ChildTurns,
                        detail: CleanupDetail::from_join(error),
                    }),
                }
            }
        }
        if let Some(worker) = managed.tool_task_events.lock().await.take() {
            result.retain_join(CleanupStage::ToolTaskEvents, worker.await);
        }
        if let Some(worker) = managed.statistics_events.lock().await.take() {
            result.retain_join(CleanupStage::StatisticsEvents, worker.await);
        }
        result.retain_join(CleanupStage::Naming, naming_result);
        result.retain(CleanupStage::Children, managed.children.close().await);
        result.session_failure = if discard_snapshot {
            managed.session.settle_for_delete(false).await
        } else {
            managed.session.close().await
        }
        .err()
        .map(CleanupDetail::from_error);
        result.retain(
            CleanupStage::ClientTools,
            managed.client_tools.settle_all("session_closed").await,
        );
        result.retain(CleanupStage::Ledger, managed.ledger.flush().await);
        result.retain(CleanupStage::Aggregate, managed.aggregate.flush().await);
        if let Some(recorder) = managed.hub.recording()
            && let Err(error) = recorder.close().await
        {
            managed.hub.display_failed(safe(error).message);
        }
        result.retain(
            CleanupStage::Auditor,
            managed
                .auditor
                .close()
                .await
                .map_err(|_| Error::audit("audit cleanup failed")),
        );
        let outcome = if result.sticky_failure() || result.session_failure.is_some() {
            Err(result.clone())
        } else {
            Ok(())
        };
        *cleanup = Some(result);
        let record = managed.record.lock().await;
        managed.hub.session(view::session(
            self,
            &record,
            &self.config.provider.model,
            "closed",
        ));
        outcome
    }
    pub async fn close(&self) -> Result<(), Error> {
        let manager = self.self_ref.upgrade().ok_or(Error::Closed)?;
        tokio::spawn(async move { manager.shutdown_inner().await })
            .await
            .map_err(|_| Error::Protocol("shutdown owner failed".into()))?
    }
    async fn shutdown_inner(&self) -> Result<(), Error> {
        self.stop_external_reply_reads();
        let _shutdown = self.shutdown.lock().await;
        if let Some(result) = self
            .shutdown_result
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            return result
                .clone()
                .map_err(|error| Error::Lifecycle(error.into()));
        }
        let mut tasks = {
            let mut accepted = self.tasks.lock().unwrap_or_else(|error| error.into_inner());
            self.closed.store(true, Ordering::Release);
            self.admission.close();
            self.control_admission.close();
            std::mem::replace(&mut *accepted, JoinSet::new())
        };
        let initial: Vec<_> = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect();
        for slot in initial {
            if let Ok(active) = slot.active.try_lock()
                && let Some(managed) = active.as_ref()
            {
                managed
                    .session
                    .interrupt_with_reason(microcrab_session::InterruptReason::SessionClosed);
            }
        }
        let mut failure = None;
        while let Some(result) = tasks.join_next().await {
            if result.is_err() {
                failure.get_or_insert(Error::Protocol("command owner failed".into()));
            }
        }
        let slots: Vec<_> = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect();
        for slot in slots {
            if let Some(managed) = slot.active.lock().await.as_ref()
                && let Err(error) = self.close_managed(managed).await
                && failure.is_none()
            {
                failure = Some(error);
            }
        }
        self.child_journals
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        self.stop_direct_hosts().await;
        // Keep authenticated Host routes alive until owned cleanup has received
        // its acknowledgements; detaching first creates our own containment loss.
        {
            let mut routes = self.routes.lock().await;
            for route in routes.values_mut() {
                route.ready = false;
                route.remote.detach(route.generation);
            }
        }
        {
            let mut routes = self.conversation_routes.lock().await;
            for route in routes.values_mut() {
                route.ready = false;
                route.remote.detach(route.generation);
            }
        }
        if let Err(error) = self.model_metadata.close().await
            && failure.is_none()
        {
            failure = Some(error);
        }
        if let Err(error) = self.configuration.close().await
            && failure.is_none()
        {
            failure = Some(error);
        }
        if let Err(error) = self.directory.close().await
            && failure.is_none()
        {
            failure = Some(error);
        }
        let result = failure.map_or(Ok(()), Err);
        if result.is_ok() {
            self.deployment
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
        }
        *self
            .shutdown_result
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(
            result
                .as_ref()
                .map(|_| ())
                .map_err(|_| "Manager cleanup failed".into()),
        );
        result
    }
}
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}
fn unavailable() -> WireError {
    WireError::new("not_found", "Session unavailable")
}
fn safe(error: Error) -> WireError {
    use microcrab_protocol::diagnostic::{json_reason, sanitize};
    let description = error.diagnostic_reason();
    let mut wire = match &error {
        Error::PendingInput(error) => {
            use microcrab_session::PendingInputError as Kind;
            let code = match error {
                Kind::InboxChanged => "inbox_changed",
                Kind::NotFound => "inbox_item_not_found",
                Kind::RevisionConflict => "inbox_revision_conflict",
                Kind::Claimed => "inbox_item_claimed",
                Kind::NotFirst => "inbox_item_not_first",
                Kind::Continuation => "inbox_continuation_pending",
            };
            WireError::new(code, &description)
        }
        Error::StateObservationConflict => {
            let mut wire = WireError::new("state_changed", &description);
            wire.details = Some(Box::new(
                serde_json::json!({"reason":description,"retryable":true}),
            ));
            wire
        }
        Error::VariableConflict => WireError::new("variable_conflict", &description),
        Error::VariableForbidden(_) => WireError::new("variable_forbidden", &description),
        Error::Busy => WireError::new("busy", &description),
        Error::IncompatibleModelCatalog => {
            WireError::new("legacy_external_dialogue_catalog", &description)
        }
        Error::ModelCatalogContinuationMismatch => {
            WireError::new("external_messaging_continuation_mismatch", &description)
        }
        Error::Closed => WireError::new("closed", &description),
        Error::Cancelled => WireError::new("cancelled", &description),
        Error::ExecutionInterrupted => WireError::new("execution_interrupted", &description),
        Error::Config(_) => WireError::new("invalid_request", &description),
        Error::Denied(_) => WireError::new("permission_denied", &description),
        Error::Transport | Error::TransportFailure(_) => WireError::new("transport", &description),
        Error::ProviderStatus { .. } => WireError::new("provider_status", &description),
        Error::ProviderTimeout => WireError::new("provider_timeout", &description),
        Error::Protocol(_) => WireError::new("protocol", &description),
        Error::Provider(cause) => {
            let mut wire = WireError::new(
                if cause.protocol_error().is_some() {
                    "protocol"
                } else {
                    "provider"
                },
                &description,
            );
            let mut details = serde_json::json!({"reason":description});
            if let Some(empty) = cause.empty_completion_details() {
                details["provider_code"] = serde_json::json!("empty_assistant_response");
                details["reasoning_present"] = serde_json::json!(empty.reasoning_present);
                details["finish_reason"] = serde_json::json!(match empty.finish {
                    microcrab_transport::EmptyCompletionFinish::Stop => "stop",
                    microcrab_transport::EmptyCompletionFinish::Length => "length",
                    microcrab_transport::EmptyCompletionFinish::Unknown => "unknown",
                });
            }
            if let Some(failure) = cause.failure_details() {
                if let Some(kind) = failure.kind() {
                    details["provider_type"] = serde_json::json!(kind);
                }
                if let Some(status) = failure.status() {
                    details["provider_status"] = serde_json::json!(status);
                    wire.message.push_str(&format!(" (HTTP {status})"));
                }
            }
            wire.details = Some(Box::new(details));
            wire
        }
        Error::Json(source) => {
            let reason = json_reason(source);
            let mut wire = WireError::new("invalid_request", &reason);
            wire.details = Some(Box::new(
                serde_json::json!({"reason":reason,"category":format!("{:?}", source.classify()).to_lowercase(),"line":source.line(),"column":source.column()}),
            ));
            wire
        }
        Error::Io(source) => {
            let reason = sanitize(&source.to_string(), 2048);
            let mut wire = WireError::new("io", &reason);
            let mut details =
                serde_json::json!({"kind":format!("{:?}", source.kind()),"reason":reason});
            if let Some(code) = source.raw_os_error() {
                details["raw_os_error"] = serde_json::json!(code);
            }
            wire.details = Some(Box::new(details));
            wire
        }
        Error::ToolFailure(failure) => {
            let message = sanitize(&failure.message, 4096);
            let mut wire = WireError::new("tool_failure", &message);
            wire.details = Some(Box::new(
                serde_json::json!({"kind":failure.kind,"retryable":failure.retryable,"tool_code":sanitize(&failure.code,128),"reason":message}),
            ));
            wire
        }
        Error::Diagnostic(diagnostic) => {
            let mut diagnostic = diagnostic.clone();
            diagnostic.reason = diagnostic.reason.as_ref().map(|reason| {
                microcrab_session::observation::diagnostics::SafeDiagnosticReason::new(
                    reason.as_str(),
                )
            });
            let mut message = diagnostic.summary().to_owned();
            if let Some(microcrab_session::observation::diagnostics::WasmFailure::OutOfFuel {
                fuel_limit,
            }) = diagnostic.wasm_failure
            {
                let phase = serde_json::to_value(diagnostic.phase)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or_else(|| "unknown".into());
                let limit = fuel_limit
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".into());
                message.push_str(&format!(
                    " [cause=out_of_fuel, fuel_limit={limit}, phase={phase}]"
                ));
            }
            let mut wire = WireError::new("operation_failed", message);
            wire.details = Some(Box::new(serde_json::json!({"diagnostic":diagnostic})));
            wire
        }
        Error::ProviderIdleTimeout(timeout) => {
            let mut wire = WireError::new("provider_timeout", "Model stream became inactive");
            let mut details = serde_json::json!({"reason":"idle_timeout"});
            for (name, value) in [
                ("timeout_ms", Some(timeout.timeout_ms)),
                ("elapsed_ms", Some(timeout.elapsed_ms)),
                ("first_token_ms", timeout.first_token_ms),
                ("streamed_bytes", Some(timeout.streamed_bytes)),
                ("streamed_chars", Some(timeout.streamed_chars)),
            ] {
                if let Some(value) = value.filter(|value| *value < (1u64 << 53)) {
                    details[name] = serde_json::json!(value);
                }
            }
            wire.details = Some(Box::new(details));
            wire
        }
        Error::Limit(limit) => {
            let mut details = serde_json::json!({"reason":limit.kind.as_str(),"pre_effect":limit.is_pre_effect()});
            for (name, value) in [
                ("calls", limit.calls),
                ("required_bytes", limit.required_bytes),
                ("budget_bytes", limit.budget_bytes),
                ("required_count", limit.required_count),
                ("budget_count", limit.budget_count),
            ] {
                if let Some(value) = value.filter(|value| (*value as u128) < (1u128 << 53)) {
                    details[name] = serde_json::json!(value);
                }
            }
            let mut wire = WireError::new("limit", limit.safe_message());
            wire.details = Some(Box::new(details));
            wire
        }
        Error::Tool(_)
        | Error::Lifecycle(_)
        | Error::Audit(_)
        | Error::Wasm(_)
        | Error::Reference(_) => WireError::new("operation_failed", &description),
    };
    if wire.details.is_none() {
        wire.details = Some(Box::new(serde_json::json!({"reason":description})));
    }
    // This is the already sanitized public projection; never format the original error.
    tracing::warn!(code=%wire.code, reason=%wire.message, "Session operation failed");
    wire
}
