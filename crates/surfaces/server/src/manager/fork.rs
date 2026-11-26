//! Fork one idle materialized conversation into a new creator identity and root.
//! Source admission stays locked through capture; durable receipts precede effects.
use super::{Manager, commands, now, safe, unavailable, view};
use microcrab_protocol::application::{Command, WireError};
use microcrab_storage::{ModelSelection, OperationRecord, RECORD_VERSION};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::atomic::Ordering;

pub(super) struct Source {
    pub(super) session: microcrab_session::Session,
    pub(super) route: ModelSelection,
    pub(super) selected_mode: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fork {
    operation_id: String,
    #[serde(default)]
    conversation_id: Option<String>,
    #[serde(default)]
    expected_revision: Option<u64>,
    #[serde(default)]
    title: Option<String>,
}
fn busy() -> WireError {
    WireError::new(
        "busy",
        "Fork requires an open idle session with settled invocations",
    )
}
async fn replay(
    manager: &Manager,
    owner: &str,
    operation_id: &str,
    digest: &str,
) -> Result<Option<Value>, WireError> {
    let Some(operation) = manager
        .directory
        .get_operation(owner, operation_id)
        .await
        .map_err(safe)?
    else {
        return Ok(None);
    };
    if operation.method != commands::SESSION_FORK || operation.payload_hash != digest {
        return Err(WireError::new(
            "operation_conflict",
            "Operation ID was used for another request",
        ));
    }
    let operation = commands::reconcile(manager, operation).await?;
    if operation.status != "completed" {
        return Err(WireError::new(
            "operation_interrupted",
            "Session fork requires reconciliation",
        ));
    }
    operation
        .result
        .map(Some)
        .ok_or_else(|| WireError::new("operation_failed", "Fork receipt unavailable"))
}

pub(super) async fn execute(
    manager: &Manager,
    owner: &str,
    command: &Command,
) -> Result<Value, WireError> {
    let params: Fork = commands::parse(command)?;
    uuid::Uuid::parse_str(&params.operation_id)
        .map_err(|_| WireError::new("invalid_request", "Invalid operation ID"))?;
    if params
        .title
        .as_ref()
        .is_some_and(|title| title.trim().is_empty() || title.len() > 4096)
    {
        return Err(WireError::new("invalid_request", "Invalid fork title"));
    }
    let digest = commands::hash(command)?;
    // Consult the destination receipt before resolving/revision-checking source:
    // successful retries survive source advancement, close and server restart.
    if let Some(value) = replay(manager, owner, &params.operation_id, &digest).await? {
        return Ok(value);
    }
    // Match create/delete lock order: project identity precedes source runtime.
    let _identity = manager.identity_gate.lock().await;
    let source_id = command.session_id.as_deref().ok_or_else(unavailable)?;
    let record = manager.record(owner, source_id).await?;
    let slot = manager
        .registry
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(source_id)
        .cloned()
        .ok_or_else(busy)?;
    let active = slot.active.lock().await;
    let source = active.as_ref().ok_or_else(busy)?;
    let _source_command = source.commands.lock().await;
    if let Some(value) = replay(manager, owner, &params.operation_id, &digest).await? {
        return Ok(value);
    }
    if record.archived
        || source.closing.load(Ordering::Acquire)
        || source.faulted.load(Ordering::Acquire)
        || source.suspended.load(Ordering::Acquire)
        || source.hub.operation().is_some()
        || source.session.has_continuation()
        || source
            .session
            .executions()
            .snapshot()
            .iter()
            .any(|task| task.status.is_pending())
    {
        return Err(busy());
    }
    let source_record = source.record.lock().await.clone();
    if source_record.archived {
        return Err(busy());
    }
    if params
        .expected_revision
        .is_some_and(|revision| revision != source_record.revision)
    {
        return Err(WireError::new(
            "revision_conflict",
            "Session revision changed",
        ));
    }
    // Verify existing immutable execution pins and live route; never select current defaults.
    manager.session_host(&source_record).await.map_err(safe)?;
    if !manager.embedded {
        fn settled<C>(route: &super::bindings::Route<C>) -> bool {
            route.remote.unresolved_operations().is_empty()
        }
        let settled = if let Some(project) = source_record.project_id.as_deref() {
            manager
                .routes
                .lock()
                .await
                .get(project)
                .is_some_and(settled)
        } else {
            manager
                .conversation_routes
                .lock()
                .await
                .get(&source_record.id)
                .is_some_and(settled)
        };
        if !settled {
            return Err(busy());
        }
    }
    // Directory mode is mutable even when the selected conversation has not
    // changed. Read the exact Journal selection while the source command is fenced.
    let selected_mode = source
        .journal
        .head_recover()
        .await
        .map_err(safe)?
        .ok_or_else(|| {
            WireError::new(
                "history_unavailable",
                "Fork requires selected Session history",
            )
        })?
        .state
        .selected_mode;
    if selected_mode.is_none() && source.session.state().selected_mode.is_some() {
        return Err(WireError::new(
            "history_unavailable",
            "Selected history does not record a mode for this Agent",
        ));
    }
    let _project = manager
        .reserve_project_catalog_operation(&source_record)
        .await?;
    let context = source.session.context_snapshot().map_err(safe)?;
    super::external_speakers::admission::check_retired(&context.tools)?;
    let catalog = source
        .client_tools
        .fork_catalog(&context.tools)
        .await
        .map_err(safe)?;
    let destination_binding = if source_record.project_id.is_none() {
        let id = params.conversation_id.as_deref().ok_or_else(|| {
            WireError::new(
                "binding_required",
                "Fork requires a separately attached destination Conversation workspace",
            )
        })?;
        uuid::Uuid::parse_str(id).map_err(|_| unavailable())?;
        manager.conversation_host(owner, id).await?;
        Some(
            manager
                .pins
                .conversation(id)
                .await
                .map_err(safe)?
                .filter(|pin| pin.owner == owner)
                .ok_or_else(unavailable)?,
        )
    } else {
        if params.conversation_id.is_some() {
            return Err(WireError::new(
                "invalid_request",
                "Project fork cannot change execution scope",
            ));
        }
        None
    };
    let id = match destination_binding.as_ref() {
        Some(binding) => binding.conversation_id.clone(),
        None => microcrab_storage::history::content_hash(&json!([owner, params.operation_id]))
            .map_err(safe)?[..32]
            .to_owned(),
    };
    if manager
        .directory
        .get(owner, &id)
        .await
        .map_err(safe)?
        .is_some()
    {
        return Err(WireError::new(
            "operation_conflict",
            "Fork destination requires reconciliation",
        ));
    }
    let timestamp = now();
    let mut destination = source_record.clone();
    destination.id = id.clone();
    if let Some(binding) = destination_binding.as_ref() {
        destination.project_root = binding.workspace_root.clone();
    }
    destination.title = params.title.unwrap_or_else(|| source_record.title.clone());
    destination.revision = 0;
    destination.archived = false;
    destination.created_at_ms = timestamp;
    destination.updated_at_ms = timestamp;
    // Directory receipts require an existing owned destination, as in create.
    // A record without a receipt is an explicit reconciliation case, never recaptured.
    let destination = manager.directory.create(destination).await.map_err(safe)?;
    let mut operation = manager
        .directory
        .create_operation(OperationRecord {
            version: RECORD_VERSION,
            id: params.operation_id,
            session_id: id.clone(),
            owner: owner.into(),
            method: commands::SESSION_FORK.into(),
            payload_hash: digest,
            status: "accepted".into(),
            result: Some(json!({"server_instance":manager.instance})),
            revision: 0,
            created_at_ms: timestamp,
            updated_at_ms: timestamp,
        })
        .await
        .map_err(safe)?;
    let result = async {
        if manager.embedded {
            let mut pin = manager
                .pins
                .component(source_id)
                .await
                .map_err(safe)?
                .ok_or_else(unavailable)?;
            pin.session_id = id.clone();
            manager.pins.pin_component(pin).await.map_err(safe)?;
        } else if let Some(binding) = destination_binding {
            let source_pin = manager
                .pins
                .conversation_session(source_id)
                .await
                .map_err(safe)?
                .ok_or_else(unavailable)?;
            manager
                .pins
                .pin_conversation_session(
                    microcrab_storage::execution::ConversationSessionBinding {
                        session_id: id.clone(),
                        conversation: binding,
                        component_digest: source_pin.component_digest,
                        discovery_digest: source_pin.discovery_digest,
                    },
                )
                .await
                .map_err(safe)?;
        } else {
            let mut pin = manager
                .pins
                .session(source_id)
                .await
                .map_err(safe)?
                .ok_or_else(unavailable)?;
            pin.session_id = id.clone();
            manager.pins.pin_session(pin).await.map_err(safe)?;
        }
        // Fresh bridge contains only selected provider definitions and metadata.
        // Calls, leases, socket ownership and pending interactions are never copied.
        manager
            .client_tools(&id)
            .await
            .map_err(safe)?
            .register(catalog, context.epoch)
            .await
            .map_err(safe)?;
        let display_error =
            super::display::fork::record_fork(manager, owner, source, &id, &context)
                .await
                .err();
        manager
            .personal_library
            .copy_session_mounts(owner, source_id, &id)
            .await
            .map_err(super::library::library_wire_error)?;
        let managed = manager
            .activate(
                destination,
                false,
                Some(Source {
                    session: source.session.clone(),
                    route: source.routing.selection(),
                    selected_mode,
                }),
                false,
            )
            .await?;
        let record = managed.record.lock().await.clone();
        let mut value = view::session(manager, &record, &manager.config.provider.model, "idle");
        view::project_live_session(&mut value, &managed.session).map_err(safe)?;
        value["component"] = json!(managed.component);
        if let Some(error) = display_error {
            managed.hub.display_failed(error.message.clone());
            value["display"] = json!({"coverage":"unavailable","error":error});
        }
        manager.refresh(&managed).await.map_err(safe)?;
        Ok::<Value, WireError>(value)
    }
    .await;
    operation.revision = 1;
    operation.updated_at_ms = now().max(timestamp);
    match result {
        Ok(value) => {
            operation.status = "completed".into();
            operation.result = Some(value.clone());
            manager
                .directory
                .replace_operation(owner, 0, operation)
                .await
                .map_err(safe)?;
            Ok(value)
        }
        Err(error) => {
            operation.status = "failed".into();
            operation.result = Some(json!({"error":error}));
            let _ = manager
                .directory
                .replace_operation(owner, 0, operation)
                .await;
            Err(error)
        }
    }
}
