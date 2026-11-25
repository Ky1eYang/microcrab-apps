//! Complete native creator assembly. Child sessions inherit live permission ceilings
//! and the selected Journal backend; preview observation stays on the main client.
use super::{Managed, Manager, hub::Hub, view};
use crate::config::{Agent, Config, Storage};
use async_trait::async_trait;
use microcrab_agent_types::{AgentInfo, AgentRole};
use microcrab_auditor::Auditor;
use microcrab_creator::{
    Children, UsageAuditor, UsageLedger,
    audit::{NativeAuditor, RecorderOptions},
    delegate::{
        ChildAuditorFactory, ChildJournalFactory, NativeChildFactory, NativeDelegateConfig,
    },
};
use microcrab_host_api::Host;
use microcrab_protocol::application::WireError;
use microcrab_session::{
    Error, Session,
    runtime::{BuiltinTool, Runtime, ToolRegistration, wasm::WasmAgent},
};
use microcrab_storage::{CreatorJournal, SessionRecord};
#[cfg(feature = "storage-sqlite")]
use std::path::PathBuf;
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::Mutex;

#[cfg(test)]
mod component_selection_tests;
mod projection_retry;
use projection_retry::ProjectionRetry;

#[derive(Clone)]
pub(super) struct ComponentCatalog {
    pub digest: String,
    source: Agent,
    pub agents: Vec<AgentInfo>,
}

#[derive(Clone, serde::Serialize)]
pub(super) struct CatalogFailure {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    pub cause: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fuel_limit: Option<u64>,
    pub phase: microcrab_session::observation::diagnostics::DiagnosticPhase,
}

pub(super) struct CatalogOutcome {
    pub components: Vec<ComponentCatalog>,
    pub failures: Vec<CatalogFailure>,
    pub out_of_fuel_error: Option<Error>,
}

impl ComponentCatalog {
    pub(super) fn name(&self) -> Option<&str> {
        self.source.name.as_deref()
    }

    pub(super) fn mode(&self) -> Option<&str> {
        self.source.mode.as_deref()
    }
}

pub(super) fn select_component<'a>(
    components: &'a [ComponentCatalog],
    requested: Option<&str>,
    embedded: bool,
) -> Result<Option<&'a ComponentCatalog>, WireError> {
    if let Some(digest) = requested {
        return components
            .iter()
            .find(|component| component.digest == digest)
            .map(Some)
            .ok_or_else(|| {
                WireError::new("component_unavailable", "Selected component is unavailable")
            });
    }
    match components {
        [only] => Ok(Some(only)),
        [] if embedded => Ok(None),
        [] => Err(WireError::new(
            "component_unavailable",
            "No backend agent component is configured",
        )),
        _ => Err(WireError::new(
            "component_required",
            "Select the exact agent component digest",
        )),
    }
}

pub(super) async fn components(
    config: &Config,
    host: Arc<dyn Host>,
) -> Result<Vec<ComponentCatalog>, Error> {
    components_outcome(config, host)
        .await
        .map(|outcome| outcome.components)
}

pub(super) async fn components_outcome(
    config: &Config,
    host: Arc<dyn Host>,
) -> Result<CatalogOutcome, Error> {
    components_with_fuel(config, host, config.agent_wasm_fuel).await
}

pub(super) async fn components_with_fuel(
    config: &Config,
    host: Arc<dyn Host>,
    fuel: u64,
) -> Result<CatalogOutcome, Error> {
    component_catalog_outcome(config, host, None, false, fuel).await
}

#[allow(dead_code)]
pub(super) async fn project_components(
    config: &Config,
    host: Arc<dyn Host>,
    project: &str,
) -> Result<Vec<ComponentCatalog>, Error> {
    project_components_with_fuel(config, host, project, config.agent_wasm_fuel)
        .await
        .map(|outcome| outcome.components)
}

pub(super) async fn project_components_with_fuel(
    config: &Config,
    host: Arc<dyn Host>,
    project: &str,
    fuel: u64,
) -> Result<CatalogOutcome, Error> {
    component_catalog_outcome(config, host, Some(project), false, fuel).await
}

#[allow(dead_code)]
async fn component_catalog(
    config: &Config,
    host: Arc<dyn Host>,
    project: Option<&str>,
    restore: bool,
) -> Result<Vec<ComponentCatalog>, Error> {
    component_catalog_outcome(config, host, project, restore, config.agent_wasm_fuel)
        .await
        .map(|outcome| outcome.components)
}

async fn component_catalog_outcome(
    config: &Config,
    host: Arc<dyn Host>,
    project: Option<&str>,
    restore: bool,
    fuel: u64,
) -> Result<CatalogOutcome, Error> {
    let candidates: Vec<_> = config
        .configured_agents()
        .filter(|source| restore || !source.restore_only)
        .cloned()
        .collect();
    let project = project.map(str::to_owned);
    tokio::task::spawn_blocking(move || {
        let mut catalogs = Vec::<ComponentCatalog>::new();
        let mut failures = Vec::<CatalogFailure>::new();
        let mut out_of_fuel_error: Option<Error> = None;
        for source in candidates {
            match load(&source, host.clone(), project.as_deref(), fuel) {
                Ok((agent, digest)) => {
                    if !catalogs.iter().any(|entry| entry.digest == digest) {
                        catalogs.push(ComponentCatalog {
                            digest,
                            source,
                            agents: agent.agents().to_vec(),
                        });
                    }
                }
                Err(err) => {
                    if let Error::Diagnostic(diagnostic) = &err
                        && let Some(microcrab_session::observation::diagnostics::WasmFailure::OutOfFuel { fuel_limit }) = diagnostic.wasm_failure
                    {
                        let effective_limit = fuel_limit.unwrap_or(fuel);
                        tracing::warn!(
                            cause = "out_of_fuel",
                            fuel_limit = effective_limit,
                            phase = ?diagnostic.phase,
                            agent = ?source.name,
                            path = ?source.path,
                            "Agent component ran out of WASM fuel during load"
                        );
                        failures.push(CatalogFailure {
                            agent: source.name.clone(),
                            cause: "out_of_fuel",
                            fuel_limit: Some(effective_limit),
                            phase: diagnostic.phase,
                        });
                        if out_of_fuel_error.is_none() {
                            out_of_fuel_error = Some(err);
                        }
                    } else {
                        tracing::warn!(
                            reason = %err.diagnostic_reason(),
                            configured_fuel = fuel,
                            agent = ?source.name,
                            path = ?source.path,
                            "Skipping incompatible agent component"
                        );
                    }
                }
            }
        }
        if catalogs.is_empty()
            && let Some(error) = out_of_fuel_error
        {
            return Err(error);
        }
        Ok(CatalogOutcome {
            components: catalogs,
            failures,
            out_of_fuel_error,
        })
    })
    .await
    .map_err(|_| Error::protocol("catalog worker failed"))?
}

fn load(
    source: &Agent,
    host: Arc<dyn Host>,
    project: Option<&str>,
    fuel: u64,
) -> Result<(WasmAgent, String), Error> {
    let mut limits = microcrab_session::runtime::wasm::WasmLimits::default();
    limits.fuel = fuel;
    let (agent, digest) =
        microcrab_app_bootstrap::agent::load_with_limits(&source.path, limits, host.clone())?;
    // Only the bound Server Project route supplies a Project identity. Use
    // the requirements Core discovered in the actual bytes, not registration metadata.
    if !agent
        .required_capabilities()
        .iter()
        .all(|capability| capability.available_on_project(host.as_ref(), project))
    {
        return Err(Error::config(
            "Component requires capabilities unavailable on this Host",
        ));
    }
    if agent
        .tools()
        .iter()
        .any(|tool| super::external_speakers::admission::reserved(&tool.definition().name))
    {
        return Err(Error::config(
            "Agent tool name is reserved by the Server external messaging catalog",
        ));
    }
    Ok((agent, digest))
}

fn default_mode(
    component: Option<&ComponentCatalog>,
    record: &SessionRecord,
    session: &Session,
) -> Result<Option<String>, String> {
    let Some(component) = component else {
        return if record.agent.is_none() {
            Ok(None)
        } else {
            Err("Pinned Agent has no pinned component for its default mode".into())
        };
    };
    let name = record
        .agent
        .as_ref()
        .ok_or("Pinned component has no fixed Session Agent for its default mode")?;
    let agent = component
        .agents
        .iter()
        .find(|agent| &agent.name == name)
        .ok_or("Fixed Session Agent is unavailable in the pinned component")?;
    if let Some(mode) = component.mode() {
        session.validate_mode(mode).map_err(|error| {
            format!(
                "Pinned component default mode is unavailable for the fixed Session Agent: {}",
                error.diagnostic_reason()
            )
        })?;
        return Ok(Some(mode.to_owned()));
    }
    agent
        .modes
        .iter()
        .find(|mode| session.validate_mode(&mode.name).is_ok())
        .map(|mode| Some(mode.name.clone()))
        .ok_or_else(|| "Fixed Session Agent has no valid default mode for this Session kind".into())
}

fn pinned_component_unavailable(digest: &str) -> Error {
    use microcrab_session::ErrorReason;

    const PUBLIC_REMEDY: &str = "Restore the exact Agent component to the configured Server catalog, or start a new Session with the current Agent. The saved Agent pin has not changed.";
    const PRIVATE_REMEDY: &str = "Restore the exact Agent component to the Server's configured Agent catalog, or start a new Session with the current Agent. This Session's Agent pin has not changed.";

    let short = (digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| format!(" (SHA-256 {}…)", digest[..16].to_ascii_lowercase()))
        .unwrap_or_default();
    Error::Config(ErrorReason::with_private(
        &format!("pinned agent component unavailable{short}. {PUBLIC_REMEDY}"),
        format!("pinned agent component unavailable (SHA-256 {digest}). {PRIVATE_REMEDY}"),
    ))
}

pub(super) async fn create(
    manager: &Manager,
    record: SessionRecord,
    restore: bool,
) -> Result<Arc<Managed>, Error> {
    create_from(manager, record, restore, None).await
}
pub(super) async fn create_from(
    manager: &Manager,
    record: SessionRecord,
    restore: bool,
    fork_source: Option<&super::fork::Source>,
) -> Result<Arc<Managed>, Error> {
    let directory = manager.config.data_dir.join("sessions").join(&record.id);
    let host = manager.session_host(&record).await?;
    let pin = manager.pins.session(&record.id).await?;
    let conversation_pin = manager.pins.conversation_session(&record.id).await?;
    let embedded_component = if manager.embedded {
        manager.pins.component(&record.id).await?
    } else {
        None
    };
    let component_identity = pin
        .as_ref()
        .map(|pin| (&pin.component_digest, &pin.discovery_digest))
        .or_else(|| {
            conversation_pin
                .as_ref()
                .map(|pin| (&pin.component_digest, &pin.discovery_digest))
        })
        .or_else(|| {
            embedded_component
                .as_ref()
                .map(|pin| (&pin.component_digest, &pin.discovery_digest))
        });
    let project = if manager.embedded {
        None
    } else {
        record.project_id.as_deref()
    };
    let agent_wasm_fuel = manager
        .resolve_agent_wasm_fuel(&record.owner, project)
        .await?;
    let mut catalog_outcome = component_catalog_outcome(
        &manager.config,
        host.clone(),
        project,
        restore && component_identity.is_some(),
        agent_wasm_fuel,
    )
    .await?;
    let catalogs = &catalog_outcome.components;
    let component = if let Some((digest, _)) = component_identity {
        match digest {
            Some(digest) => match catalogs.iter().find(|component| &component.digest == digest) {
                Some(component) => Some(component),
                None => {
                    if let Some(error) = catalog_outcome.out_of_fuel_error.take() {
                        return Err(error);
                    }
                    return Err(pinned_component_unavailable(digest));
                }
            },
            None => None,
        }
    } else if manager.embedded {
        catalogs.first()
    } else {
        return Err(Error::config("session execution identity is missing"));
    };
    if let Some((_, discovery_digest)) = component_identity {
        let discovered = component
            .map(|component| component.agents.clone())
            .unwrap_or_default();
        if microcrab_storage::history::content_hash(&discovered)? != *discovery_digest {
            return Err(Error::config(
                "pinned agent discovery differs from session identity",
            ));
        }
    }
    let prompt_config = record.prompt_config.clone().unwrap_or_default();
    prompt_config.validate()?;
    if !restore && fork_source.is_none() {
        let selected_agent = record.agent.as_ref().and_then(|name| {
            component
                .and_then(|component| component.agents.iter().find(|agent| &agent.name == name))
        });
        prompt_config.render_system(selected_agent)?;
    }
    let source = component.map(|component| component.source.clone());
    let project = project.map(str::to_owned);
    let expected_digest = component.map(|component| component.digest.clone());
    let component_digest = expected_digest.clone();
    let expected_discovery = component_identity.map(|(_, digest)| digest.clone());
    let storage = directory.clone();
    let usage_config = manager.config.clone();
    #[cfg(feature = "storage-sqlite")]
    let usage_scope =
        microcrab_storage::audit::AuditScope::new(&record.owner, &record.id, &record.id);
    let (host, loaded, ledger, aggregate) = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&storage)?;
        let loaded = source
            .as_ref()
            .map(|source| load(source, host.clone(), project.as_deref(), agent_wasm_fuel))
            .transpose()?;
        if loaded.as_ref().map(|(_, digest)| digest) != expected_digest.as_ref() {
            return Err(Error::config("agent component changed during assembly"));
        }
        let loaded = loaded.map(|(agent, _)| agent);
        let discovered = loaded.as_ref().map_or(&[][..], WasmAgent::agents);
        if let Some(discovery_digest) = expected_discovery
            && microcrab_storage::history::content_hash(&discovered)? != discovery_digest
        {
            return Err(Error::config(
                "pinned agent discovery differs from session identity",
            ));
        }
        let (ledger, aggregate) = match usage_config.storage {
            Storage::File => (
                UsageLedger::open(Some(&storage))?,
                UsageLedger::open_named(Some(&storage), "aggregate_usage.jsonl")?,
            ),
            Storage::Sqlite => {
                #[cfg(feature = "storage-sqlite")]
                {
                    let database = usage_config.data_dir.join("storage.sqlite");
                    (
                        UsageLedger::open_sqlite(&database, usage_scope.clone(), "usage.jsonl")?,
                        UsageLedger::open_sqlite(&database, usage_scope, "aggregate_usage.jsonl")?,
                    )
                }
                #[cfg(not(feature = "storage-sqlite"))]
                {
                    return Err(Error::config("SQLite support is not compiled"));
                }
            }
        };
        Ok::<_, Error>((host, loaded, ledger, aggregate))
    })
    .await
    .map_err(|_| Error::protocol("Session assembly worker failed"))??;
    let catalog = loaded
        .as_ref()
        .map(|loaded| loaded.agents().to_vec())
        .unwrap_or_default();
    let agent = if let Some(name) = record.agent.as_ref() {
        Some(
            catalog
                .iter()
                .find(|agent| {
                    &agent.name == name
                        && agent
                            .modes
                            .iter()
                            .any(|mode| matches!(mode.role, AgentRole::MainAgent | AgentRole::Both))
                })
                .cloned()
                .ok_or_else(|| Error::config("main agent unavailable"))?,
        )
    } else {
        None
    };
    let wasm = loaded
        .as_ref()
        .map(|loaded| loaded.tools())
        .unwrap_or_default();
    let model_admission = super::external_speakers::admission::Admission::new(manager, &record);
    let (journal, mut recover) = match manager.config.storage {
        Storage::File => {
            #[cfg(feature = "storage-file")]
            {
                CreatorJournal::open(
                    directory
                        .to_str()
                        .ok_or_else(|| Error::config("storage path is not UTF8"))?,
                    record.id.clone(),
                    true,
                    restore,
                )
                .await?
            }
            #[cfg(not(feature = "storage-file"))]
            {
                return Err(Error::config("File storage support is not compiled"));
            }
        }
        Storage::Sqlite => {
            #[cfg(feature = "storage-sqlite")]
            {
                CreatorJournal::open_sqlite(
                    manager.config.data_dir.join("storage.sqlite"),
                    record.id.clone(),
                    restore,
                )
                .await?
            }
            #[cfg(not(feature = "storage-sqlite"))]
            {
                return Err(Error::config("SQLite support is not compiled"));
            }
        }
    };
    // Runtime history witnesses a legacy Session's first resolved configuration.
    // Child factories must use the same snapshot as restore/fork, even when the
    // older creator record has no field yet.
    let prompt_config = super::prompt_defaults::retained(
        prompt_config,
        recover.as_ref(),
        fork_source.map(|source| source.session.prompt_config()),
    );
    prompt_config.validate()?;
    let client_tools = manager.client_tools(&record.id).await?;
    if let Some(recover) = recover.as_ref() {
        client_tools
            .reconcile_catalog(&recover.context.tools)
            .await?;
    }
    let external = client_tools.registrations().await;
    let mut tools: Vec<_> = wasm.iter().cloned().map(ToolRegistration::Wasm).collect();
    tools.extend(external.iter().cloned());
    if let Some(reply) = model_admission.initial_registration().await? {
        tools.push(ToolRegistration::DialogueReply(reply));
    }
    tools.extend([
        ToolRegistration::Builtin(BuiltinTool::Expand),
        ToolRegistration::Builtin(BuiltinTool::Job),
        ToolRegistration::Builtin(BuiltinTool::Sleep),
        ToolRegistration::Builtin(BuiltinTool::ViewImage),
    ]);
    // Core retains the exact frozen definitions on restore/fork and rejects
    // unadvertised calls; this only registers the applicable native wiring.
    tools.extend(
        microcrab_app_bootstrap::agent_builtins(agent.as_ref(), &catalog)
            .map(ToolRegistration::Builtin),
    );
    let hub = manager
        .read_hubs
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(&record.id)
        .and_then(|hub| hub.upgrade())
        .unwrap_or_else(|| Arc::new(Hub::new()));
    // Runtime status changes (idle/running/closed) reach the owner's list.
    manager.watch_hub(&hub, &record.owner, &record.id);
    let presentations = Arc::new(super::presentation::Presentations::default());
    let permission_ask = super::permission_ask::PermissionAsk::new(
        record.id.clone(),
        client_tools.clone(),
        hub.clone(),
        presentations.clone(),
    );
    let round_limit =
        super::round_limit::RoundLimit::new(record.id.clone(), client_tools.clone(), hub.clone());
    let attachments =
        super::attachments::SessionAttachments::new(&manager.config.data_dir, &record.id);
    let runtime = Runtime::new(tools, permission_ask.clone())?;
    let permissions = runtime.permission_controller();
    let tool_hub = hub.clone();
    let client_tool_changes = Arc::new(tokio::sync::Notify::new());
    let tool_change = client_tool_changes.clone();
    client_tools.set_observer(Arc::new(move |event| {
        let payload = serde_json::json!({"call": event.call});
        let published = match event.phase {
            super::client_tools::ClientToolPhase::Requested => {
                super::SessionRuntimeEvent::ClientToolRequested(payload)
            }
            super::client_tools::ClientToolPhase::Prepared => {
                super::SessionRuntimeEvent::ClientToolPrepared(payload)
            }
            super::client_tools::ClientToolPhase::Paused => {
                super::SessionRuntimeEvent::ClientToolPaused(payload)
            }
            super::client_tools::ClientToolPhase::Settled => {
                super::SessionRuntimeEvent::ClientToolSettled(payload)
            }
            super::client_tools::ClientToolPhase::CancelRequested => {
                super::SessionRuntimeEvent::ClientToolCancelRequested(payload)
            }
        };
        tool_hub.publish(published);
        tool_change.notify_one();
    }));
    let selection = record
        .model_selection
        .as_ref()
        .ok_or_else(|| Error::config("session model selection is missing"))?;
    let selected_route = journal.model_selection().await;
    let legacy_cursor = if selected_route.is_none() {
        journal.cursor().await
    } else {
        None
    };
    let known_legacy = match &legacy_cursor {
        Some(cursor) => super::model::admission::legacy_selection(manager, &record, cursor)
            .await?
            .or_else(|| Some(selection.clone())),
        None => None,
    };
    let applied_selection = fork_source
        .map(|source| source.route.clone())
        .or(selected_route)
        .or(known_legacy.clone())
        .unwrap_or_else(|| selection.clone());
    let legacy_witness = legacy_cursor.zip(known_legacy);
    let auditor = NativeAuditor::open_source_with_observer(
        super::audit::source(
            &manager.config,
            &record.owner,
            &record.id,
            &record.id,
            directory.clone(),
        ),
        journal.history_anchor(),
        RecorderOptions::default(),
        Some(super::audit::observer(hub.clone())),
    )
    .await
    .map_err(|error| {
        Error::audit_with_private(
            "Session audit recorder could not be opened",
            error.to_string(),
        )
    })?;
    // Creator persists display observations outside Context and ordinary audit retention.
    let display_source = super::display::source(
        &manager.config,
        &record.owner,
        &record.id,
        &record.id,
        directory.clone(),
    );
    let inspect_display = display_source.clone();
    let display_high = tokio::task::spawn_blocking(move || inspect_display.revision())
        .await
        .map_err(|_| Error::audit("display inspection worker failed"))
        .and_then(|result| result)
        .map(|head| head.unwrap_or(0));
    let display_anchor = journal.history_anchor();
    // The Hub owns the recorder; callbacks must never own the Hub or its
    // observer-owning auditor back, otherwise the blocking writer cannot drain.
    let display_auditor = Arc::downgrade(&auditor);
    let display_hub = Arc::downgrade(&hub);
    let committed_hub = Arc::downgrade(&hub);
    let failed_hub = Arc::downgrade(&hub);
    let recorder = microcrab_creator::display::DisplayRecorder::open(
        display_source.clone(),
        Arc::new(move |observation| {
            let anchor = display_anchor.lock().unwrap_or_else(|e| e.into_inner()).clone();
            let operation = display_hub.upgrade().and_then(|hub|hub.operation()).unwrap_or_else(|| format!("inbox:{}:{}",observation.epoch,observation.turn.unwrap_or_default()));
            let identity=display_auditor.upgrade().and_then(|auditor|auditor.request_head(&observation.call_id));
            serde_json::json!({"operation_id":operation,"request_identity":identity,
                "parent_request":anchor.request_head,"history_head":anchor.history_head,"display_lineage":anchor.display_lineage})
        }),
        Arc::new(move |record| {if let Some(hub)=committed_hub.upgrade(){hub.recorded(record);}}),
        Arc::new(move |message| {if let Some(hub)=failed_hub.upgrade(){hub.display_failed(message);}}),
    ).await;
    let mut anchors = None;
    match (recorder, display_high) {
        (Ok(recorder), Ok(high)) => {
            journal.install_commit_observer(Arc::new(super::display::CheckpointDisplay {
                recorder: recorder.clone(),
                hub: Arc::downgrade(&hub),
                child: None,
            }))?;
            let display_anchors = super::display::anchors::Anchors::new(
                record.id.clone(),
                display_source,
                recorder.clone(),
            );
            let projection_hub = Arc::downgrade(&hub);
            display_anchors.set_failure_handler(Arc::new(move |cause| {
                if let Some(hub) = projection_hub.upgrade() {
                    hub.display_failed(cause);
                }
            }));
            anchors = Some(display_anchors);
            hub.install_recording(recorder, high);
        }
        (Ok(recorder), Err(error)) => {
            let _ = recorder.close().await;
            hub.display_failed(super::safe(error).message);
        }
        (Err(error), _) => hub.display_failed(super::safe(error).message),
    }
    let routing = super::model::Routing::new(
        super::model::target(manager, &record.owner, &applied_selection, hub.clone()).await?,
    );
    routing.pin_epoch(recover.as_ref().map_or(0, |recover| recover.context.epoch));
    let route_admission = super::model::admission::Admission::new(
        manager,
        &record,
        routing.clone(),
        hub.clone(),
        journal.clone(),
        legacy_witness,
    );
    if let Some(anchors) = anchors {
        routing.install_anchors(anchors);
    }
    let observed_executions = Arc::new(std::sync::Mutex::new(None));
    let observed = super::observation::ObservedAuditor {
        inner: auditor.clone(),
        hub: hub.clone(),
        calls: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        executions: observed_executions.clone(),
        presentations: presentations.clone(),
    };
    let usage = UsageAuditor::new(ledger.clone(), aggregate.clone(), Arc::new(observed));
    let children = Children::default();
    let saved_settings = manager
        .configuration
        .get(&record.owner, "session_effective", &record.id)
        .await?
        .and_then(|record| record.value)
        .map(serde_json::from_value::<crate::config::settings::SettingsOverrides>)
        .transpose()?;
    let limits = match saved_settings.as_ref() {
        Some(settings) => {
            crate::config::settings::limits(&settings.limits, &manager.config.limit_ceilings)
                .map_err(|e| Error::Config(e.into()))?
        }
        None => crate::config::settings::default_limits(),
    };
    // The approved legacy-default projection applies only when this Session did
    // not explicitly configure that field. Mutate the in-memory restore input,
    // never the selected content-addressed node or its frozen Context/State bytes.
    if let Some(recover) = recover.as_mut() {
        let explicit = |field: &str| {
            saved_settings
                .as_ref()
                .is_some_and(|settings| settings.limits.contains_key(field))
        };
        if !explicit("max_rounds") && recover.state.limits.max_rounds == 16 {
            recover.state.limits.max_rounds = crate::config::settings::default_limits()
                .max_rounds
                .min(manager.config.limit_ceilings.max_rounds);
        }
        const LEGACY_CONTEXT_BYTES: usize = 2 * 1024 * 1024;
        if !explicit("max_context_bytes")
            && recover.state.limits.max_context_bytes == LEGACY_CONTEXT_BYTES
            && recover.context.max_bytes == LEGACY_CONTEXT_BYTES
        {
            let current = crate::config::settings::default_limits()
                .max_context_bytes
                .min(manager.config.limit_ceilings.max_context_bytes);
            recover.state.limits.max_context_bytes = current;
            recover.context.max_bytes = current;
        }
    }
    // All remaining frozen custom values retain authority, constrained by the
    // administrator ceiling without silently clipping historical configuration.
    if let Some(recover) = recover.as_ref() {
        let saved = serde_json::to_value(&recover.state.limits)?;
        let ceilings = serde_json::to_value(&manager.config.limit_ceilings)?;
        for (field, value) in saved.as_object().expect("Limits serialize as object") {
            if value
                .as_u64()
                .zip(ceilings[field].as_u64())
                .is_some_and(|(value, ceiling)| value > ceiling)
            {
                return Err(Error::Config(format!("Frozen Session limits.{field} exceeds the administrator hard ceiling; explicit configuration reconciliation is required").into()));
            }
        }
    }
    let child_client = routing.client(false);
    let child_factory = Arc::new(
        NativeChildFactory::new(NativeDelegateConfig {
            children: children.clone(),
            aggregate: aggregate.clone(),
            client: child_client,
            host: host.clone(),
            wasm,
            external: external
                .into_iter()
                .filter_map(|registration| match registration {
                    ToolRegistration::External(tool) => Some(tool),
                    _ => None,
                })
                .collect(),
            catalog: catalog.clone(),
            permissions,
            limits: limits.clone(),
            directory: directory.clone(),
            disk: true,
            system: None,
            auditors: Some(Arc::new(AuditFactory {
                hub: hub.clone(),
                config: manager.config.clone(),
                owner: record.owner.clone(),
                root: record.id.clone(),
            })),
            journals: Some(Arc::new(JournalFactory {
                restore: false,
                #[cfg(feature = "storage-sqlite")]
                owner: record.owner.clone(),
                #[cfg(feature = "storage-sqlite")]
                root: record.id.clone(),
                storage: manager.config.storage,
                #[cfg(feature = "storage-sqlite")]
                database: manager.config.data_dir.join("storage.sqlite"),
            })),
        })
        .prompt_config(prompt_config.clone())
        .with_child_configuration(Arc::new(InteractionChildConfiguration {
            grants: super::settings::grants::Grants {
                owner: record.owner.clone(),
                storage: manager.configuration.clone(),
                directory: manager.directory.clone(),
                manager: manager.self_ref.clone(),
            },
            tools: client_tools.clone(),
        }))
        .with_external_source(Arc::new(ClientToolSource(client_tools.clone())))
        .with_permission_adapter(permission_ask.clone())
        .with_round_limit_callback(round_limit.clone())
        .with_attachment_resolver(attachments.clone())
        .with_display_factory(Arc::new(super::display::ChildDisplay::new(
            manager.config.clone(),
            record.owner.clone(),
            record.id.clone(),
            routing.clone(),
            hub.clone(),
            manager.child_journals.clone(),
        ))),
    );
    let delegate = microcrab_creator::delegate::OwnedDelegateExecutor::new(
        children.clone(),
        child_factory.clone(),
    );
    let runtime = runtime.with_delegate_executor(Arc::new(delegate));
    let main_client = routing.client(true);
    let naming_admission = super::naming::Admission::new(manager, main_client);
    let builder = Session::builder_arc(naming_admission.clone(), runtime)
        .prompt_config(prompt_config)
        .model_admission(model_admission)
        .turn_routing(route_admission)
        .auto_compaction(
            record
                .auto_compaction
                .clone()
                .unwrap_or_else(|| manager.config.auto_compaction.clone()),
            routing.estimator(manager),
        )
        .round_limit_callback(round_limit.clone())
        .attachment_resolver(attachments)
        .permissions(microcrab_permission::PermissionConfig {
            overrides: record.permissions.clone(),
            initialization: record.initialization,
            review: record.review,
            review_model: record.review_model.clone(),
            ..Default::default()
        })
        .session_id(record.id.clone())
        .host(host.clone())
        .limits(limits)
        .catalog(catalog)
        .is_main(true)
        .auditor(usage)
        .journal(journal.clone())
        .dialogue_reply_store_with(super::external_inbox::reply::store_factory(
            &manager.config,
            &record,
        ));
    let reference_sources: Vec<microcrab_session::state::reference::ReferenceSource> = manager
        .configuration
        .get(&record.owner, "session_references", &record.id)
        .await?
        .and_then(|r| r.value)
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    let builder = builder.markdown_references("references", reference_sources);
    let builder = if let Some(selection) = &record.review_model {
        builder.review_client(
            super::model::review_target(manager, &record.owner, selection, hub.clone()).await?,
        )
    } else {
        builder
    };
    let builder = if let Some(agent) = agent {
        builder.agent(agent)
    } else {
        builder
    };
    // Publish Managed before Session.start: effectful init may ask the existing
    // socket executor for permission. Slot owns startup admission, cancellation
    // and failure publication after this pure build/restore phase returns.
    let new_invocation_owner = fork_source.is_some() || recover.is_none();
    let initial_mode = match fork_source {
        Some(source) => source.selected_mode.as_deref(),
        None if recover.is_none() => record.mode.as_deref(),
        None => None,
    };
    let initialized = async {
        let preparation = match (fork_source, recover) {
            (Some(source), _) => {
                microcrab_app_bootstrap::session::Preparation::Fork(&source.session)
            }
            (None, Some(recover)) => {
                microcrab_app_bootstrap::session::Preparation::Restore(Box::new(recover))
            }
            (None, None) => microcrab_app_bootstrap::session::Preparation::Create,
        };
        let session = microcrab_app_bootstrap::session::prepare(builder, preparation).await?;
        // A fork retains its historical mode, even if the source's live/manual
        // selection or the copied creator configuration has since changed.
        // Restore already applies the exact saved policy in Core.
        if let Some(mode) = initial_mode
            && (fork_source.is_none() || session.state().selected_mode.as_deref() != Some(mode))
            && let Err(error) = session.select_mode(mode).await
        {
            let _ = session.close().await;
            return Err(error);
        }
        super::settings::grants::Grants {
            owner: record.owner.clone(),
            storage: manager.configuration.clone(),
            directory: manager.directory.clone(),
            manager: manager.self_ref.clone(),
        }
        .restore(&session)
        .await?;
        client_tools.install_secret_state(&session).await?;
        Ok::<Session, Error>(session)
    }
    .await;
    let session = match initialized {
        Ok(session) => session,
        Err(error) => {
            let _ = children.close().await;
            let _ = auditor.close().await;
            let _ = microcrab_session::Journal::close(&journal).await;
            return Err(error);
        }
    };
    if let Some(registrations) = client_tools.pending_registrations().await {
        let pending = registrations
            .into_iter()
            .filter_map(|entry| match entry {
                ToolRegistration::External(tool) => Some(tool),
                _ => None,
            })
            .collect();
        session.replace_external_tools(pending).await?;
    }
    *observed_executions
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(Arc::downgrade(&session.executions()));
    let slot = manager
        .registry
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&record.id)
        .cloned();
    if new_invocation_owner
        && session.state().agent_initialization_pending
        && super::agent_init::requires_deferred_start(
            record.project_id.is_none()
                && manager.conversation_flavors.get(&record.owner, &record.id)
                    == Some(super::conversation_flavors::HostFlavor::ChatOnly),
            record.initialization,
        )
        && let Some(slot) = &slot
    {
        slot.initialization_deferred.store(true, Ordering::Release);
    }
    let default_mode = default_mode(component, &record, &session);
    let managed = Arc::new(Managed {
        host_flavor: record
            .project_id
            .is_none()
            .then(|| manager.conversation_flavors.get(&record.owner, &record.id))
            .flatten(),
        session,
        slot: slot.as_ref().map(Arc::downgrade).unwrap_or_default(),
        routing,
        journal,
        host,
        children,
        auditor,
        client_tools,
        permission_ask,
        presentations: presentations.clone(),
        round_limit,
        ledger,
        aggregate,
        statistics: super::statistics::Statistics::with_metadata(
            &manager.config,
            &record.id,
            manager.model_metadata.clone(),
        )
        .scoped(&record.owner),
        statistics_events: Mutex::new(None),
        record: Mutex::new(record.clone()),
        commands: Mutex::new(()),
        worker: Mutex::new(None),
        child_drivers: Mutex::new(tokio::task::JoinSet::new()),
        child_factory,
        tool_task_events: Mutex::new(None),
        naming: Mutex::new(None),
        auto_naming: AtomicBool::new(
            saved_settings
                .as_ref()
                .and_then(|settings| settings.auto_naming)
                .unwrap_or(manager.config.auto_rename),
        ),
        naming_admission: naming_admission.clone(),
        naming_token: tokio_util::sync::CancellationToken::new(),
        hub,
        closing: AtomicBool::new(false),
        close_token: tokio_util::sync::CancellationToken::new(),
        faulted: AtomicBool::new(false),
        suspended: AtomicBool::new(false),
        component: component_digest,
        agent_wasm_fuel,
        default_mode,
        cleanup: Mutex::new(None),
    });
    naming_admission.install(&managed);
    // The Session persists task transitions and owns autonomous turns. This
    // watcher only publishes their read model, including idle answer wake-ups
    // which have no public command worker to refresh the finished transcript.
    // It holds no Managed owner while waiting and exits on close.
    let task_registry = managed.session.executions();
    let mut task_changes = task_registry.subscribe();
    let mut completed_turns = managed.session.subscribe_turns();
    let mut inbox_activity = managed.session.subscribe_activity();
    // The Context watch follows admitted input, validated replies and real tool
    // results. Streaming observations remain advisory and never enter snapshots.
    let mut conversation_changes = managed.session.context_measurement_changes();
    let mut executor_changes = managed.client_tools.executor_changes();
    let task_client_tools = Arc::downgrade(&managed.client_tools);
    let mut turn_events = managed.hub.subscribe().receiver;
    let task_hub = managed.hub.clone();
    let task_close = managed.close_token.clone();
    let task_owner = Arc::downgrade(&managed);
    let task_manager = manager.self_ref.clone();
    *managed.tool_task_events.lock().await = Some(tokio::spawn(async move {
        let mut projection = ProjectionRetry::default();
        loop {
            let mut notification = true;
            let mut lagged = false;
            tokio::select! {
                biased;
                _ = task_close.cancelled() => break,
                // Keep the timer ahead of busy event sources: an event storm must
                // not postpone the capped retry indefinitely.
                _ = ProjectionRetry::wait(projection.deadline()) => notification = false,
                changed = task_changes.changed() => {
                    if changed.is_err() { break; }
                    match super::presentation::executions(&task_registry.snapshot(), &presentations) {
                        Ok(rows) => task_hub.executions(rows),
                        Err(error) => tracing::warn!(reason = %microcrab_protocol::diagnostic::json_reason(&error), "Tool task projection failed"),
                    }
                }
                turn = completed_turns.recv() => {
                    match turn {
                        Ok(_) => {},
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => lagged = true,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                changed = inbox_activity.changed() => {
                    if changed.is_err() { break; }
                }
                changed = executor_changes.changed() => {
                    if changed.is_err() { break; }
                    let lease = executor_changes.borrow_and_update().clone();
                    if !lease.as_ref().is_some_and(|lease| task_client_tools.upgrade().is_some_and(|tools| tools.input_handoff_notified(lease))) {
                        task_hub.publish(super::SessionRuntimeEvent::ClientToolsLeaseChanged(
                            serde_json::json!({"executor_connected": lease.is_some()}),
                        ));
                    }
                    continue;
                }
                changed = conversation_changes.changed() => {
                    if changed.is_err() { break; }
                }
                event = turn_events.recv() => {
                    match event {
                        Ok(event) if matches!(&event.event, super::SessionRuntimeEvent::Turn(_)) => {},
                        Ok(_) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => lagged = true,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = client_tool_changes.notified() => {}
            }
            if notification {
                projection.changed();
            }
            let (Some(owner), Some(manager)) = (task_owner.upgrade(), task_manager.upgrade())
            else {
                break;
            };
            if owner.closing.load(Ordering::Acquire) {
                break;
            }
            if lagged {
                owner
                    .hub
                    .publish(super::SessionRuntimeEvent::ResyncRequired(
                        super::ResyncReason::SlowConsumer,
                    ));
            }
            let settlement = tokio::select! {
                biased;
                _ = task_close.cancelled() => break,
                result = async {
                    super::inbox::settle_round_stop(&manager, &owner).await?;
                    super::inbox::settle_failure(&manager, &owner).await
                } => result,
            };
            if let Err(error) = settlement {
                tracing::warn!(error_code = %super::safe(error).code, "Autonomous inbox failure receipt publication failed");
                owner
                    .hub
                    .publish(super::SessionRuntimeEvent::ResyncRequired(
                        super::ResyncReason::InboxReceiptFailed,
                    ));
            }
            let Some(generation) = projection.attempt(tokio::time::Instant::now()) else {
                continue;
            };
            let refreshed = tokio::select! {
                biased;
                _ = task_close.cancelled() => break,
                result = manager.refresh(&owner) => result,
            };
            let resync = projection.finished(
                generation,
                refreshed.as_ref().map(|_| ()),
                tokio::time::Instant::now(),
            );
            if resync && let Err(error) = refreshed {
                tracing::warn!(error_code = %super::safe(error).code, "Session transition snapshot refresh failed");
                owner
                    .hub
                    .publish(super::SessionRuntimeEvent::ResyncRequired(
                        super::ResyncReason::InboxSnapshotFailed,
                    ));
            }
        }
    }));
    let mut measurement_changes = managed.session.context_measurement_changes();
    let mut metadata_changes = manager.model_metadata.changes();
    let mut statistics_notifications = managed.hub.subscribe().receiver;
    let statistics_close = managed.close_token.clone();
    let statistics_owner = Arc::downgrade(&managed);
    let statistics_manager = manager.self_ref.clone();
    *managed.statistics_events.lock().await = Some(tokio::spawn(async move {
        loop {
            let refresh = tokio::select! {
                biased;
                _ = statistics_close.cancelled() => break,
                changed = measurement_changes.changed() => { if changed.is_err() { break; } true },
                changed = metadata_changes.changed() => { if changed.is_err() { break; } true },
                event = statistics_notifications.recv() => match event {
                    Ok(event) => matches!(&event.event, super::SessionRuntimeEvent::AuditAvailable(_)),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => true,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            };
            if !refresh {
                continue;
            }
            let (Some(owner), Some(manager)) =
                (statistics_owner.upgrade(), statistics_manager.upgrade())
            else {
                break;
            };
            let result = async {
                let record = owner.record.lock().await.clone();
                let witness = super::statistics::PublicationWitness::capture(&owner, &record)?;
                let operations = manager
                    .directory
                    .list_operations(&record.owner, &record.id)
                    .await?;
                let value = owner
                    .statistics
                    .snapshot(&owner, &record, &operations, &witness)
                    .await?;
                witness.publish(&owner, value, false).await
            };
            let result = tokio::select! {
                biased;
                _ = statistics_close.cancelled() => break,
                result = result => result,
            };
            match result {
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(error_code = %super::safe(error).code, "Statistics projection failed")
                }
            }
        }
    }));
    // A retained accepted/running receipt from a previous process is ambiguous:
    // reconcile it visibly; never rerun an external effect on restoration.
    let initialized=async {
    for mut operation in manager.directory.list_operations(&record.owner,&record.id).await? {
        if operation.result.as_ref().and_then(|value|value.get("server_instance")).and_then(serde_json::Value::as_str)!=Some(manager.instance.as_str()) && matches!(operation.status.as_str(),"accepted"|"running") {
            let revision=operation.revision;operation.revision+=1;operation.updated_at_ms=super::now().max(operation.updated_at_ms);operation.status="interrupted".into();
            let mut result=serde_json::json!({"error":{"code":"process_restarted","message":"Operation interrupted by server restart"}});
            if let Some(previous)=&operation.result { super::commands::copy_retry_link(previous,&mut result); }
            operation.result=Some(result);
            manager.directory.replace_operation(&record.owner,revision,operation).await?;
        }
    }
    view::refresh(manager.directory.as_ref(),&managed,&manager.config.provider.model).await?;
    Ok::<_,Error>(())
    }.await;
    if let Err(error) = initialized {
        let _ = manager.close_managed(&managed).await;
        return Err(error);
    }
    Ok(managed)
}
struct ClientToolSource(Arc<super::client_tools::ClientTools>);
#[async_trait]
impl microcrab_creator::delegate::ExternalToolSource for ClientToolSource {
    async fn tools(&self) -> Result<Vec<Arc<dyn microcrab_session::runtime::tool::Tool>>, Error> {
        Ok(self
            .0
            .registrations()
            .await
            .into_iter()
            .filter_map(|registration| match registration {
                ToolRegistration::External(tool) => Some(tool),
                _ => None,
            })
            .collect())
    }
}

struct InteractionChildConfiguration {
    grants: super::settings::grants::Grants,
    tools: Arc<super::client_tools::ClientTools>,
}
#[async_trait::async_trait]
impl microcrab_creator::delegate::ChildConfiguration for InteractionChildConfiguration {
    async fn apply(&self, child: &Session) -> Result<(), Error> {
        self.grants.restore(child).await?;
        self.tools.install_secret_state(child).await
    }
}

pub(super) async fn restore_child(
    manager: &Manager,
    parent: &Managed,
    directory: &Path,
    id: &str,
) -> Result<Session, Error> {
    let record = parent.record.lock().await.clone();
    let factory = JournalFactory {
        restore: true,
        storage: manager.config.storage,
        #[cfg(feature = "storage-sqlite")]
        owner: record.owner,
        #[cfg(feature = "storage-sqlite")]
        root: record.id,
        #[cfg(feature = "storage-sqlite")]
        database: manager.config.data_dir.join("storage.sqlite"),
    };
    // Register the exact existing owner/root/child audit scope through the same port.
    // Opening this Journal does not allocate a child directory or record.
    let journal = factory.open(directory, id).await?;
    let child = parent
        .child_factory
        .restore_child(&parent.session, directory, journal)
        .await?;
    Ok(child.session)
}

struct JournalFactory {
    restore: bool,
    storage: Storage,
    #[cfg(feature = "storage-sqlite")]
    owner: String,
    #[cfg(feature = "storage-sqlite")]
    root: String,
    #[cfg(feature = "storage-sqlite")]
    database: PathBuf,
}
#[async_trait]
impl ChildJournalFactory for JournalFactory {
    async fn usage(&self, session_id: &str) -> Result<Option<UsageLedger>, Error> {
        #[cfg(not(feature = "storage-sqlite"))]
        let _ = session_id;
        if self.storage == Storage::File {
            return Ok(None);
        }
        #[cfg(feature = "storage-sqlite")]
        {
            let database = self.database.clone();
            let scope =
                microcrab_storage::audit::AuditScope::new(&self.owner, &self.root, session_id);
            tokio::task::spawn_blocking(move || {
                UsageLedger::open_sqlite(&database, scope, "usage.jsonl")
            })
            .await
            .map_err(|_| Error::audit("child usage open worker failed"))?
            .map(Some)
        }
        #[cfg(not(feature = "storage-sqlite"))]
        {
            Err(Error::config("SQLite support is not compiled"))
        }
    }

    async fn open(&self, directory: &Path, session_id: &str) -> Result<CreatorJournal, Error> {
        match self.storage {
            Storage::File => {
                #[cfg(feature = "storage-file")]
                {
                    Ok(CreatorJournal::open(
                        directory
                            .to_str()
                            .ok_or_else(|| Error::config("child path is not UTF8"))?,
                        session_id.into(),
                        true,
                        self.restore,
                    )
                    .await?
                    .0)
                }
                #[cfg(not(feature = "storage-file"))]
                {
                    Err(Error::config("File storage support is not compiled"))
                }
            }
            Storage::Sqlite => {
                #[cfg(feature = "storage-sqlite")]
                {
                    let source = microcrab_storage::audit::AuditSource::Sqlite {
                        database: self.database.clone(),
                        scope: microcrab_storage::audit::AuditScope::new(
                            &self.owner,
                            &self.root,
                            session_id,
                        ),
                    };
                    // Register scope before a child Journal can commit. A failed
                    // partial assembly still belongs to its root's hard deletion.
                    tokio::task::spawn_blocking(move || source.open().map(drop))
                        .await
                        .map_err(|_| Error::audit("child scope worker failed"))??;
                    Ok(
                        CreatorJournal::open_sqlite(
                            &self.database,
                            session_id.into(),
                            self.restore,
                        )
                        .await?
                        .0,
                    )
                }
                #[cfg(not(feature = "storage-sqlite"))]
                {
                    Err(Error::config("SQLite support is not compiled"))
                }
            }
        }
    }
}
struct AuditFactory {
    hub: Arc<Hub>,
    config: Config,
    owner: String,
    root: String,
}
#[async_trait]
impl ChildAuditorFactory for AuditFactory {
    async fn create(
        &self,
        directory: &Path,
        journal: &CreatorJournal,
    ) -> Result<Arc<dyn Auditor>, Error> {
        self.create_native(directory, journal)
            .await?
            .map(|auditor| auditor as Arc<dyn Auditor>)
            .ok_or_else(|| Error::audit("Native child auditor unavailable"))
    }
    async fn create_native(
        &self,
        directory: &Path,
        journal: &CreatorJournal,
    ) -> Result<Option<Arc<NativeAuditor>>, Error> {
        let child_session_id = directory
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::config("child audit identity unavailable"))?
            .to_owned();
        let source = super::audit::source(
            &self.config,
            &self.owner,
            &self.root,
            &child_session_id,
            directory.to_owned(),
        );
        let hub = self.hub.clone();
        let observer: microcrab_creator::audit::CommitObserver = Arc::new(move |commit| {
            if let Ok(mut value) = serde_json::to_value(commit) {
                value["child_session_id"] = serde_json::json!(child_session_id);
                hub.publish(super::SessionRuntimeEvent::AuditAvailable(value));
            }
        });
        NativeAuditor::open_source_with_observer(
            source,
            journal.history_anchor(),
            RecorderOptions::default(),
            Some(observer),
        )
        .await
        .map(Some)
        .map_err(|error| {
            Error::audit_with_private(
                "Child Session audit recorder could not be opened",
                error.to_string(),
            )
        })
    }
}
