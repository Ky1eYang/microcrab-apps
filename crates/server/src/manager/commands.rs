//! Strict native business commands; retries consult durable receipts before effects.
//! A turn's owned driver keeps draining after the requesting socket disappears.
mod history;
mod inbox;
#[cfg(test)]
mod validation_tests;

use super::{Managed, Manager, now, safe, unavailable, view};
use crate::auth::Principal;
use microcrab_protocol::{
    ImageSource, InputPart,
    application::{Command, WireError},
};
use microcrab_session::{Error, SessionEvent, SessionInput, runtime::execution::ExecutionId};
use microcrab_storage::{ModelSelection, OperationRecord, RECORD_VERSION, SessionRecord, Travel};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::sync::{Arc, atomic::Ordering};

/// Query server capabilities and feature flags.
pub(crate) const SERVER_CAPABILITIES: &str = "server.capabilities";
/// Query authenticated principal identity.
pub(crate) const IDENTITY_CURRENT: &str = "identity.current";
/// List registered projects accessible to the current user.
pub(crate) const PROJECT_LIST: &str = "project.list";
/// Create a new workspace project.
pub(crate) const PROJECT_CREATE: &str = "project.create";
/// Delete a workspace project.
pub(crate) const PROJECT_DELETE: &str = "project.delete";
/// Update a project display name without changing its execution identity.
pub(crate) const PROJECT_RENAME: &str = "project.rename";
pub(crate) const PROJECT_HOST_STATE: &str = "project.host_state";
pub(crate) const PROJECT_OPERATIONS: &str = "project.operations";
pub(crate) const PROJECT_HOST_RECONCILE: &str = "project.host_reconcile";
pub(crate) const PROJECT_HOST_OUTBOUND_START: &str = "project.host_outbound_start";
pub(crate) const PROJECT_HOST_OUTBOUND_FINISH: &str = "project.host_outbound_finish";
pub(crate) const PROJECT_HOST_OUTBOUND_CANCEL: &str = "project.host_outbound_cancel";
pub(crate) const PROJECT_HOST_CONNECT: &str = "project.host_connect";
pub(crate) const PROJECT_HOST_DISCONNECT: &str = "project.host_disconnect";
pub(crate) const PROJECT_HOST_REFRESH: &str = "project.host_refresh";
pub(crate) const PROJECT_DELETE_PROVISIONAL: &str = "project.delete_provisional";
/// Query available model configurations.
pub(crate) const MODEL_LIST: &str = "model.list";
pub(crate) const MODEL_METADATA_GET: &str = "model.metadata";
pub(crate) const MODEL_METADATA_REFRESH: &str = "model.metadata.refresh";
/// Query agent catalog for a project.
pub(crate) const AGENT_CATALOG: &str = "agent.catalog";
/// List session records for the current user.
pub(crate) const SESSION_LIST: &str = "session.list";
/// Create a new session record and initialize session state.
///
/// Handler: [`create`]
pub(crate) const SESSION_CREATE: &str = "session.create";
pub(crate) const SESSION_FORK: &str = "session.fork";
/// WebSocket-only: owner-level session list increments (`sessions.changed`).
pub(crate) const SESSIONS_WATCH: &str = "sessions.watch";
/// Rename a session record with optimistic revision concurrency.
pub(crate) const SESSION_RENAME: &str = "session.rename";
/// Soft-delete (archive) a session record.
pub(crate) const SESSION_ARCHIVE: &str = "session.archive";
pub(crate) const SESSION_DELETE: &str = "session.delete";
/// Close and suspend a session runtime.
pub(crate) const SESSION_CLOSE: &str = "session.close";
/// Explicitly open and activate a session runtime.
///
/// Handler: [`Manager::active`]
pub(crate) const SESSION_OPEN: &str = "session.open";
/// Explicit owner-authorized retry of failed/unknown Agent initialization.
pub(crate) const SESSION_RETRY_AGENT_INIT: &str = "session.retry_agent_init";
pub(crate) const SESSION_RETRY_AUTO_COMPACTION: &str = "session.retry_auto_compaction";
/// WebSocket-only: page canonical blocks of the selected history lineage.
///
/// Handler: [`Manager::inspect_message_page`]
pub(crate) const SESSION_BLOCKS: &str = "session.blocks";
/// Submit a new turn to the session runtime.
pub(crate) const TURN_SUBMIT: &str = "turn.submit";
/// Retry a failed request against its current durable Context.
pub(crate) const TURN_RETRY: &str = "turn.retry";
/// Admit input into Session's shared insertion inbox.
pub(crate) const TURN_QUEUE: &str = "turn.queue";
pub(crate) const TURN_STEER: &str = "turn.steer";
pub(crate) const INBOX_UPDATE: &str = "inbox.update";
pub(crate) const INBOX_REMOVE: &str = "inbox.remove";
/// Explicitly resume the oldest pending user input after an idle pause.
pub(crate) const INBOX_RESUME: &str = "inbox.resume";
/// Query durable turn operation receipt.
pub(crate) const TURN_GET: &str = "turn.get";
/// Interrupt active session foreground turn execution.
pub(crate) const SESSION_INTERRUPT: &str = "session.interrupt";
/// Compact session conversation history.
pub(crate) const SESSION_COMPACT: &str = "session.compact";
/// Time-travel to a previous history checkpoint.
pub(crate) const HISTORY_TRAVEL: &str = "history.travel";
/// Create a manual history checkpoint.
pub(crate) const CHECKPOINT_CREATE: &str = "checkpoint.create";
/// Get session permission grants and policies.
pub(crate) const ROUND_LIMIT_REPLY: &str = "round_limit.reply";
pub(crate) const PERMISSION_REPLY: &str = "permission.reply";
pub(crate) const PERMISSION_GET: &str = "permission.get";
/// Update session permission grants and policies.
pub(crate) const PERMISSION_SET: &str = "permission.set";
/// Change session operational mode.
pub(crate) const SESSION_SET_MODE: &str = "session.set_mode";
/// Change the persistent provider, model and reasoning selection while idle.
pub(crate) const SESSION_SET_MODEL: &str = "session.set_model";
/// List workspace files matching an optional pattern.
pub(crate) const WORKSPACE_FILES: &str = "workspace.files";
pub(crate) const SESSION_CHILDREN: &str = "session.children";
pub(crate) const SESSION_CHILD: &str = "session.child";
pub(crate) const AUDIT_REQUESTS: &str = "audit.requests";
pub(crate) const DISPLAY_OBSERVATIONS: &str = "display.observations";
pub(crate) const STATE_GET: &str = "state.get";
pub(crate) const STATE_METADATA: &str = "state.metadata";
pub(crate) const STATE_INSPECTION: &str = "state.inspection";
pub(crate) const CLIENT_TOOLS_CALLS: &str = "client_tools.calls";
pub(crate) const CLIENT_TOOLS_SET: &str = "client_tools.set";
pub(crate) const CLIENT_TOOLS_BIND: &str = "client_tools.bind";
pub(crate) const CLIENT_TOOLS_RESULT: &str = "client_tools.result";
pub(crate) const CLIENT_TOOLS_UNBIND: &str = "client_tools.unbind";
pub(crate) const CLIENT_TOOLS_OUTPUT: &str = "client_tools.output";
pub(crate) const CLIENT_TOOLS_PREPARE: &str = "client_tools.prepare";
pub(crate) const EXECUTIONS_LIST: &str = "executions.list";
pub(crate) const EXECUTIONS_CANCEL: &str = "executions.cancel";
/// Read retained invocation output without consuming it or starting a runtime.
pub(crate) const EXECUTIONS_READ: &str = "executions.read";
pub(crate) const EXECUTIONS_CONTENT: &str = "executions.content";

const METHODS: &[&str] = &[
    "settings.get",
    "settings.set",
    "project.settings.get",
    "project.settings.set",
    "project.references.get",
    "project.references.set",
    "session.settings.get",
    "session.settings.set",
    "session.references.refresh",
    "session.variables.patch",
    "session.variables.operation",
    "provider.list",
    "provider.get",
    "provider.set",
    "provider.delete",
    "provider.discover",
    SERVER_CAPABILITIES,
    IDENTITY_CURRENT,
    PROJECT_LIST,
    PROJECT_CREATE,
    PROJECT_DELETE,
    PROJECT_RENAME,
    "conversation.binding",
    "conversation.attach",
    "conversation.detach",
    "host.relay_attach",
    "display.subscribe",
    PROJECT_HOST_STATE,
    PROJECT_OPERATIONS,
    PROJECT_HOST_RECONCILE,
    PROJECT_HOST_CONNECT,
    PROJECT_HOST_OUTBOUND_START,
    PROJECT_HOST_OUTBOUND_FINISH,
    PROJECT_HOST_OUTBOUND_CANCEL,
    PROJECT_HOST_DISCONNECT,
    PROJECT_HOST_REFRESH,
    PROJECT_DELETE_PROVISIONAL,
    MODEL_LIST,
    MODEL_METADATA_GET,
    MODEL_METADATA_REFRESH,
    AGENT_CATALOG,
    SESSION_LIST,
    SESSION_CREATE,
    SESSION_FORK,
    SESSIONS_WATCH,
    SESSION_RENAME,
    SESSION_ARCHIVE,
    SESSION_DELETE,
    SESSION_CLOSE,
    SESSION_OPEN,
    SESSION_RETRY_AGENT_INIT,
    SESSION_RETRY_AUTO_COMPACTION,
    SESSION_BLOCKS,
    SESSION_CHILDREN,
    SESSION_CHILD,
    TURN_SUBMIT,
    TURN_RETRY,
    TURN_QUEUE,
    TURN_STEER,
    INBOX_UPDATE,
    INBOX_REMOVE,
    INBOX_RESUME,
    TURN_GET,
    SESSION_INTERRUPT,
    SESSION_COMPACT,
    HISTORY_TRAVEL,
    CHECKPOINT_CREATE,
    ROUND_LIMIT_REPLY,
    PERMISSION_REPLY,
    PERMISSION_GET,
    PERMISSION_SET,
    SESSION_SET_MODE,
    SESSION_SET_MODEL,
    WORKSPACE_FILES,
    AUDIT_REQUESTS,
    DISPLAY_OBSERVATIONS,
    STATE_GET,
    STATE_METADATA,
    STATE_INSPECTION,
    CLIENT_TOOLS_CALLS,
    CLIENT_TOOLS_SET,
    CLIENT_TOOLS_BIND,
    CLIENT_TOOLS_RESULT,
    CLIENT_TOOLS_UNBIND,
    CLIENT_TOOLS_PREPARE,
    CLIENT_TOOLS_OUTPUT,
    EXECUTIONS_LIST,
    EXECUTIONS_CANCEL,
    EXECUTIONS_READ,
    EXECUTIONS_CONTENT,
];
fn invalid(reason: &'static str) -> WireError {
    WireError::new("invalid_request", reason)
}
fn serialization() -> WireError {
    WireError::new("internal_error", "Server response serialization failed")
}
fn validate_text(
    value: &str,
    empty: &'static str,
    budget: &'static str,
    max: usize,
) -> Result<(), WireError> {
    if value.trim().is_empty() {
        return Err(invalid(empty));
    }
    if value.len() > max {
        return Err(invalid(budget));
    }
    Ok(())
}
fn permission_validation(error: microcrab_permission::ValidationError) -> WireError {
    // String-backed errors may cross embedding boundaries. Publish only closed
    // validator causes and validated rule locations, never arbitrary text.
    let (location, cause) = error
        .0
        .split_once(": ")
        .filter(|(location, _)| {
            location.split_once(':').is_some_and(|(layer, index)| {
                matches!(layer, "provider" | "mode" | "override")
                    && !index.is_empty()
                    && index.bytes().all(|byte| byte.is_ascii_digit())
            })
        })
        .map_or((None, error.0.as_str()), |(location, cause)| {
            (Some(location), cause)
        });
    let reason = match cause {
        "matcher byte budget" | "matcher byte budget (4096) exceeded" => {
            "Permission matcher exceeds 4096 bytes"
        }
        "invalid JSON pointer" | "invalid JSON pointer or pointer depth/byte budget exceeded" => {
            "Permission JSON pointer must be empty or start with /, contain at most 512 bytes and at most 32 segments"
        }
        "invalid JSON pointer escape" => "Permission JSON pointer permits only ~0 and ~1 escapes",
        "invalid review model selection" => {
            "Review model requires a nonempty provider_id (up to 256 bytes), model (up to 1024 bytes), and optional nonempty variant (up to 64 bytes), without control characters"
        }
        "policy selector/rule count budget exceeded" => {
            "Permission policy exceeds 256 total rules or 128 bytes for agent or mode"
        }
        "agent/mode selector byte budget exceeded" => {
            "Permission agent/mode selector exceeds 128 bytes"
        }
        "predicate count budget (16) exceeded" => "Permission rule exceeds 16 argument predicates",
        "predicate input byte budget (4096) exceeded" => {
            "Permission predicate input exceeds 4096 bytes"
        }
        "matcher work budget (1000000) exceeded" => "Permission matcher exceeds 1000000 work cells",
        "tool name byte budget exceeded" => "Permission tool name exceeds 4096 bytes",
        "compiled policy node/depth budget exceeded" => {
            "Compiled permission exceeds 1024 nodes or depth 32"
        }
        "compiled policy branch budget exceeded" => "Compiled permission exceeds 4096 branches",
        "compiled source byte budget exceeded" => "Compiled permission source exceeds 4096 bytes",
        "all_of requires at least one policy" => "AllOf requires at least one policy",
        "compiled policy node budget exceeded" => "Compiled permission exceeds 1024 nodes",
        "configuration byte budget: encoded byte budget exceeded" => {
            "Permission configuration exceeds 1 MiB"
        }
        "compiled projection byte budget: encoded byte budget exceeded" => {
            "Compiled permission projection exceeds 1 MiB"
        }
        "argument byte budget: encoded byte budget exceeded"
        | "request byte budget: encoded byte budget exceeded" => {
            "Permission request exceeds 16 MiB"
        }
        "stale permission revision" => "Permission request revision is stale",
        _ => "Permission configuration failed validation; no recognized safe cause was supplied",
    };
    match location {
        Some(location) => WireError::new("invalid_request", format!("{location}: {reason}")),
        None => invalid(reason),
    }
}
fn permission_error(error: microcrab_session::policy::PermissionError) -> WireError {
    use microcrab_session::policy::PermissionError;
    match error {
        PermissionError::InvalidConfiguration(reason) => {
            let reason = reason
                .strip_prefix("invalid permission configuration: ")
                .unwrap_or(&reason);
            match reason {
                "policy lock unavailable" => invalid("Permission policy lock is unavailable"),
                "revision exhausted" => invalid("Permission revision counter is exhausted"),
                "review target must be assembled and native policy is read-only" => {
                    invalid("Review target must be assembled and native policy is read-only")
                }
                "native and provider policy is read-only; use Session mode selection" => {
                    invalid("Native and provider policy is read-only; use Session mode selection")
                }
                "review model changes require an assembled client" => {
                    invalid("Review model changes require an assembled client")
                }
                _ => permission_validation(microcrab_permission::ValidationError(reason.into())),
            }
        }
        PermissionError::PersistenceUncertain(reason) => WireError::new(
            "permission_persistence_uncertain",
            format!(
                "Permission persistence outcome requires Session recovery: {}",
                microcrab_protocol::diagnostic::sanitize(&reason, 1800)
            ),
        ),
        PermissionError::StalePolicy => {
            WireError::new("revision_conflict", "Permission revision changed")
        }
        PermissionError::Unavailable => invalid("Permission interaction is unavailable"),
        PermissionError::Review => invalid("Permission review services are unavailable"),
        PermissionError::Cancelled => invalid("Permission operation was cancelled"),
        PermissionError::Deadline => invalid("Permission operation deadline exceeded"),
        PermissionError::PolicyDenied {
            controller,
            revision,
            reason,
        } => WireError::new(
            "permission_denied",
            format!("Permission denied by controller {controller} revision {revision}: {reason}"),
        ),
        PermissionError::AlreadyDispatched => WireError::new(
            "operation_conflict",
            "Invocation admission was already dispatched; mutation replay is forbidden",
        ),
        PermissionError::ProofMismatch(reason) => WireError::new("permission_denied", reason),
        PermissionError::Denied => {
            invalid("Permission admission proof or policy owner is unavailable")
        }
    }
}
pub(super) fn parse<T: DeserializeOwned>(command: &Command) -> Result<T, WireError> {
    serde_json::from_value(command.params.clone()).map_err(|error| {
        crate::route::error::json_error(
            "Command parameters must match the command schema",
            "params",
            &error,
        )
    })
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResumePending {
    operation_id: String,
    expected_inbox_id: String,
    item_id: u64,
    expected_revision: u64,
}
impl ResumePending {
    fn validate(&self) -> Result<(), WireError> {
        inbox::validate_target(
            &self.expected_inbox_id,
            self.item_id,
            self.expected_revision,
        )
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EmptySettings {}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectParams {
    project_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectOperations {
    project_id: String,
    #[serde(default)]
    limit: Option<usize>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HostReconcile {
    project_id: String,
    #[serde(default)]
    connection_generation: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateProject {
    #[serde(default)]
    pub id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub root: Option<String>,
    #[serde(default)]
    pub host_id: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RenameProject {
    pub project_id: String,
    pub name: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeleteProject {
    pub project_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogParams {
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    conversation_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Create {
    pub(crate) project_id: Option<String>,
    #[serde(default)]
    pub(crate) conversation_id: Option<String>,
    #[serde(default)]
    pub(crate) agent: Option<String>,
    #[serde(default)]
    pub(crate) component: Option<String>,
    #[serde(default)]
    pub(crate) mode: Option<String>,
    #[serde(default)]
    pub(crate) title: Option<String>,
    #[serde(default)]
    pub(crate) model_selection: Option<ModelSelection>,
    #[serde(default)]
    pub(crate) client_tools: Vec<super::client_tools::ClientToolDefinition>,
    #[serde(default)]
    pub(crate) system_template: Option<String>,
    #[serde(default)]
    pub(crate) compact_template: Option<String>,
    #[serde(default)]
    pub(crate) auto_compact_template: Option<String>,
    #[serde(default)]
    pub(crate) rename_template: Option<String>,
    #[serde(default)]
    pub(crate) action_review_template: Option<String>,
    #[serde(default)]
    pub(crate) client_prompt: Option<String>,
    pub(crate) operation_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModelSetting {
    pub(crate) expected_revision: u64,
    pub(crate) provider_id: String,
    pub(crate) model: String,
    #[serde(default)]
    pub(crate) variant: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Submit {
    input: SessionInput,
    operation_id: String,
    #[serde(default)]
    activate_if_idle: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Retry {
    operation_id: String,
    failed_operation_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Operation {
    pub(crate) operation_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Rename {
    pub(crate) title: String,
    pub(crate) expected_revision: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Revision {
    pub(crate) expected_revision: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Direction {
    direction: String,
    #[serde(default)]
    message_id: Option<String>,
    #[serde(default)]
    expected_checkpoint_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Compact {
    #[serde(default)]
    pub(crate) instruction: String,
    #[serde(default)]
    pub(crate) operation_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Mode {
    pub(crate) mode: String,
    #[serde(default)]
    pub(crate) expected_revision: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Permission {
    pub(crate) expected_revision: u64,
    pub(crate) overrides: Vec<microcrab_permission::Permission>,
    pub(crate) review: bool,
    #[serde(default)]
    pub(crate) initialization: Option<microcrab_permission::Action>,
    #[serde(default, deserialize_with = "review_model_update")]
    pub(crate) review_model: Option<Option<microcrab_permission::ReviewModelSelection>>,
}
fn review_model_update<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<microcrab_permission::ReviewModelSelection>>, D::Error> {
    <Option<microcrab_permission::ReviewModelSelection> as serde::Deserialize>::deserialize(
        deserializer,
    )
    .map(Some)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Files {
    #[serde(default)]
    pub(crate) pattern: Option<String>,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Messages {
    #[serde(default)]
    pub(crate) lineage: Option<String>,
    #[serde(default)]
    pub(crate) before_message_id: Option<String>,
    #[serde(default)]
    pub(crate) after_message_id: Option<String>,
    #[serde(default)]
    pub(crate) before_cursor: Option<usize>,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}
/// Carry only explicit validated retry ancestry; same-position failures are unrelated.
pub(super) fn copy_retry_link(source: &Value, target: &mut Value) {
    for field in ["retry_of_operation_id", "retry_after_message_id"] {
        if let Some(value) = source.get(field) {
            target[field] = value.clone();
        }
    }
}

pub(super) async fn reconcile(
    manager: &Manager,
    mut operation: OperationRecord,
) -> Result<OperationRecord, WireError> {
    if operation
        .result
        .as_ref()
        .and_then(|value| value.get("server_instance"))
        .and_then(Value::as_str)
        != Some(manager.instance.as_str())
        && matches!(operation.status.as_str(), "accepted" | "running")
    {
        let revision = operation.revision;
        operation.revision += 1;
        operation.updated_at_ms = now().max(operation.updated_at_ms);
        operation.status = "interrupted".into();
        let mut result = json!({"error":{"code":"process_restarted","message":"Operation interrupted by server restart"}});
        if let Some(previous) = &operation.result {
            copy_retry_link(previous, &mut result);
        }
        operation.result = Some(result);
        return manager
            .directory
            .replace_operation(&operation.owner.clone(), revision, operation)
            .await
            .map_err(safe);
    }
    Ok(operation)
}
fn operation_id(id: &str) -> Result<(), WireError> {
    uuid::Uuid::parse_str(id)
        .map(|_| ())
        .map_err(|_| invalid("operation_id must be a UUID"))
}
pub(super) fn hash(command: &Command) -> Result<String, WireError> {
    microcrab_storage::history::content_hash(
        &json!({"method":command.method,"session_id":command.session_id,"params":command.params}),
    )
    .map_err(safe)
}
fn replay(record: OperationRecord, command: &Command, digest: &str) -> Result<Value, WireError> {
    if record.method != command.method
        || record.payload_hash != digest
        || command
            .session_id
            .as_ref()
            .is_some_and(|id| id != &record.session_id)
    {
        Err(WireError::new(
            "operation_conflict",
            "Operation ID was used for another request",
        ))
    } else {
        serde_json::to_value(record).map_err(|_| serialization())
    }
}

/// One `session.list` item, or `None` when its project is not visible. Shared
/// by the list command and `sessions.changed` upserts so both agree exactly.
pub(super) async fn list_item(
    manager: &Manager,
    record: SessionRecord,
) -> Result<Option<Value>, WireError> {
    if !manager.project_visible(&record).await {
        return Ok(None);
    }
    let record = manager.selected_record(record).await?;
    let slot = manager
        .registry
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&record.id)
        .cloned();
    // A slot that is activating holds its lock through assembly; the list uses
    // the directory view instead of waiting for it.
    let current = slot.and_then(|slot| {
        slot.active.try_lock().ok().and_then(|active| {
            active
                .as_ref()
                .map(|managed| managed.hub.snapshot()["session"].clone())
        })
    });
    let mut item = current.unwrap_or_else(|| {
        view::session(
            manager,
            &record,
            &manager.config.provider.model,
            if record.archived { "closed" } else { "idle" },
        )
    });
    if let Some(pin) = manager.pins.session(&record.id).await.map_err(safe)? {
        item["component"] = json!(pin.component_digest);
    }
    item["host_connected"] = json!(manager.embedded || manager.record_host(&record).await.is_ok());
    Ok(Some(item))
}
pub(super) async fn execute(
    manager: &Manager,
    principal: &Principal,
    command: &Command,
) -> Result<Value, WireError> {
    if serde_json::to_vec(&command.params)
        .map_err(|_| serialization())?
        .len()
        > 256 * 1024
    {
        return Err(WireError::new(
            "request_too_large",
            "Command parameters exceed budget",
        ));
    }
    let owner = &principal.owner;
    if command.method.starts_with("provider.") {
        return super::providers::command(manager, owner, &command.method, command.params.clone())
            .await;
    }
    if matches!(
        command.method.as_str(),
        "settings.get"
            | "settings.set"
            | "project.settings.get"
            | "project.settings.set"
            | "project.references.get"
            | "project.references.set"
            | "session.settings.get"
            | "session.settings.set"
            | "session.references.refresh"
            | "session.variables.patch"
            | "session.variables.operation"
    ) {
        return super::settings::command(manager, owner, command).await;
    }
    match command.method.as_str() {
        "server.capabilities" => {
            let _: Empty = parse(command)?;
            return Ok(
                json!({"protocol":1,"methods":METHODS,"features":{"round_limit_confirmation":true,"attachments":true,"model_observations":true,"interactive_ask":true,"remembered_grants":false,"auto_naming":manager.config.auto_rename,"history_pagination":true,"image_data":true,"image_path":false,"image_url":false,"process_execution":manager.host.tool_capabilities().iter().any(|capability| capability == "shell")},"limits":{"attachment_image_bytes":microcrab_session::admission::image::MAX_IMAGE_BYTES,"attachment_images":microcrab_session::admission::image::MAX_IMAGES_PER_INPUT,"attachment_message_bytes":microcrab_session::admission::image::MAX_INPUT_IMAGE_BYTES,"attachment_pixels":microcrab_session::admission::image::MAX_IMAGE_PIXELS,"attachment_max_side":microcrab_session::admission::image::MAX_IMAGE_DIMENSION,"command_bytes":microcrab_protocol::application::MAX_FRAME_BYTES}}),
            );
        }
        "identity.current" => {
            let _: Empty = parse(command)?;
            return Ok(
                json!({"id":owner,"subject":principal.subject,"name":principal.display_name,"expires_at":principal.expires_at}),
            );
        }
        "project.host_state" => {
            let params: ProjectParams = parse(command)?;
            return manager.host_state(principal, &params.project_id).await;
        }
        "project.operations" => {
            let params: ProjectOperations = parse(command)?;
            return manager
                .host_operations(principal, &params.project_id, params.limit.unwrap_or(100))
                .await;
        }
        "project.host_reconcile" => {
            let params: HostReconcile = parse(command)?;
            return manager
                .reconcile_host(
                    principal,
                    &params.project_id,
                    params.connection_generation.as_deref(),
                )
                .await;
        }
        "project.delete_provisional" => {
            let params: ProjectParams = parse(command)?;
            return manager
                .delete_provisional(principal, &params.project_id)
                .await;
        }
        "project.list" => {
            let _: Empty = parse(command)?;
            let mut items = vec![];
            for record in manager.projects.list(owner).await {
                let pin = manager
                    .pins
                    .project(&record.id)
                    .await
                    .map_err(safe)?
                    .filter(|pin| pin.owner == *owner);
                let connected = manager.project_host(owner, &record.id).await.is_ok();
                let status = if connected { "online" } else { "offline" };
                items.push(json!({
                    "id": record.id,
                    "name": record.name,
                    "root": pin.as_ref().map(|pin| &pin.workspace_root).unwrap_or(&record.root),
                    "host_id": pin.as_ref().map(|pin| &pin.host_id).or(record.host_id.as_ref()),
                    "workspace_id": pin.as_ref().map(|pin| &pin.workspace_id).or(record.workspace_id.as_ref()),
                    "provisional": record.provisional,
                    "host_connected": connected,
                    "status": status,
                }));
            }
            return Ok(json!({"items":items}));
        }
        "project.create" => {
            let params: CreateProject = parse(command)?;
            validate_text(
                &params.name,
                "Project name must not be empty",
                "Project name exceeds 512 bytes",
                512,
            )?;
            let id = params
                .id
                .unwrap_or_else(|| format!("p-{}", uuid::Uuid::new_v4().simple()));
            if !crate::config::valid_id(&id) {
                return Err(invalid(
                    "Project id must contain 1 to 128 ASCII letters, digits, hyphens or underscores",
                ));
            }
            if params.host_id.is_some() || params.workspace_id.is_some() {
                return Err(WireError::new(
                    "invalid_params",
                    "Host identity is established by authenticated attachment",
                ));
            }
            let root = params.root.unwrap_or_else(|| ".".into());
            validate_text(
                &root,
                "Project root must not be empty",
                "Project root exceeds 32768 bytes",
                32768,
            )?;
            if root.contains('\0') {
                return Err(invalid("Project root must not contain NUL"));
            }
            let _identity = manager.identity_gate.lock().await;
            let record = crate::project::ProjectRecord {
                id: id.clone(),
                name: params.name,
                root: root.clone(),
                host_id: None,
                workspace_id: None,
                owners: vec![owner.to_string()],
                created_at: super::now(),
                archived: false,
                provisional: true,
            };
            let created = manager
                .projects
                .create(owner, record)
                .await
                .map_err(|e| WireError::new("invalid_params", e))?;
            return Ok(json!({
                "id": created.id,
                "name": created.name,
                "root": created.root,
                "provisional": created.provisional,
                "status": "offline",
                "host_connected": false,
            }));
        }
        "project.rename" => {
            let params: RenameProject = parse(command)?;
            validate_text(
                &params.name,
                "Project name must not be empty",
                "Project name exceeds 512 bytes",
                512,
            )?;
            manager.editable_project(owner, &params.project_id).await?;
            let record = manager
                .projects
                .rename(owner, &params.project_id, params.name.trim().into())
                .await
                .map_err(|_| unavailable())?;
            let connected = manager.project_host(owner, &record.id).await.is_ok();
            let pin = manager
                .pins
                .project(&record.id)
                .await
                .map_err(safe)?
                .filter(|pin| pin.owner == *owner);
            return Ok(
                json!({"id":record.id,"name":record.name,"root":pin.as_ref().map(|pin| &pin.workspace_root).unwrap_or(&record.root),"host_id":pin.as_ref().map(|pin| &pin.host_id).or(record.host_id.as_ref()),"workspace_id":pin.as_ref().map(|pin| &pin.workspace_id).or(record.workspace_id.as_ref()),"host_connected":connected,"status":if connected {"online"} else {"offline"}}),
            );
        }
        "project.delete" => {
            let params: DeleteProject = parse(command)?;
            return super::deletion::delete_project(manager, owner, &params.project_id).await;
        }
        "model.list" => {
            let _: Empty = parse(command)?;
            return Ok(json!({
                "items": super::providers::choices(manager,owner).await?,
                "metadata_status": manager.model_metadata.status(),
                "default_model_selection": manager.effective_settings(owner,None,None).await?.0.model_selection.unwrap_or_else(||super::model::default_selection(&manager.config)),
            }));
        }
        "model.metadata" => {
            let _: Empty = parse(command)?;
            if command.session_id.is_some() {
                return Err(invalid("This command does not accept session_id"));
            }
            return Ok(
                json!({"status": manager.model_metadata.status(), "snapshot": &*manager.model_metadata.snapshot()}),
            );
        }
        "model.metadata.refresh" => {
            let _: Empty = parse(command)?;
            if command.session_id.is_some() {
                return Err(invalid("This command does not accept session_id"));
            }
            let status = manager.model_metadata.refresh().await.map_err(safe)?;
            return Ok(
                json!({"metadata_status":status,"items":super::providers::choices(manager,owner).await?,"default_model_selection":manager.effective_settings(owner,None,None).await?.0.model_selection.unwrap_or_else(||super::model::default_selection(&manager.config))}),
            );
        }
        "conversation.binding" => {
            if command.session_id.is_some() {
                return Err(invalid("This command does not accept session_id"));
            }
            let params: super::conversations::Binding = parse(command)?;
            return serde_json::to_value(manager.conversation_scope(owner, params).await?)
                .map_err(|_| serialization());
        }
        "conversation.attach" | "conversation.detach" => {
            return Err(WireError::new(
                "socket_required",
                "Browser attachment belongs to its authenticated socket",
            ));
        }
        "host.relay_attach" => {
            return Err(WireError::new(
                "socket_required",
                "WebSocket relay attachment belongs to its authenticated browser socket",
            ));
        }
        "project.host_outbound_start"
        | "project.host_outbound_finish"
        | "project.host_outbound_cancel" => {
            if command.session_id.is_some() {
                return Err(invalid("This command does not accept session_id"));
            }
            return match command.method.as_str() {
                "project.host_outbound_start" => {
                    manager
                        .start_outbound_host(principal, parse(command)?)
                        .await
                }
                "project.host_outbound_finish" => {
                    manager
                        .finish_outbound_host(principal, parse(command)?)
                        .await
                }
                _ => {
                    manager
                        .cancel_outbound_host(principal, parse(command)?)
                        .await
                }
            };
        }
        "project.host_connect" => {
            if command.session_id.is_some() {
                return Err(invalid("This command does not accept session_id"));
            }
            return manager
                .connect_direct_host(principal, parse(command)?)
                .await;
        }
        "project.host_disconnect" => {
            if command.session_id.is_some() {
                return Err(invalid("This command does not accept session_id"));
            }
            return manager
                .disconnect_direct_host(principal, parse(command)?)
                .await;
        }
        "project.host_refresh" => {
            if command.session_id.is_some() {
                return Err(invalid("This command does not accept session_id"));
            }
            let params: ProjectParams = parse(command)?;
            return manager
                .refresh_direct_host(principal, params.project_id)
                .await;
        }
        "agent.catalog" => {
            let params: CatalogParams = parse(command)?;
            let host = if manager.embedded {
                manager.host.clone()
            } else {
                match (
                    params.project_id.as_deref(),
                    params.conversation_id.as_deref(),
                ) {
                    (Some(project), None) => manager.project_host(owner, project).await?,
                    (None, Some(id)) => manager.conversation_host(owner, id).await?,
                    _ => return Err(invalid("Specify exactly one project_id or conversation_id")),
                }
            };
            let components = super::assembly::components(&manager.config, host)
                .await
                .map_err(safe)?;
            let items: Vec<_> = components
                .into_iter()
                .flat_map(|component| {
                    component.agents.into_iter().map(move |agent| {
                        let mut value =
                            serde_json::to_value(agent).expect("AgentInfo is JSON-compatible");
                        value["component"] = json!(component.digest);
                        value
                    })
                })
                .collect();
            return Ok(json!({"items":items}));
        }
        "session.list" => {
            let _: Empty = parse(command)?;
            let records = manager.directory.list_entries(owner).await.map_err(safe)?;
            let mut items = vec![];
            let mut unavailable = vec![];
            for entry in records {
                match entry {
                    microcrab_storage::directory::SessionEntry::Available { record } => {
                        if let Some(item) = list_item(manager, record).await? {
                            items.push(item);
                        }
                    }
                    microcrab_storage::directory::SessionEntry::Unavailable { id, reason } => {
                        unavailable.push(json!({"id":id,"reason":reason}));
                    }
                }
            }
            return Ok(
                json!({"items":items,"complete":unavailable.is_empty(),"unavailable":unavailable}),
            );
        }
        "session.create" => return create(manager, owner, command).await,
        "session.fork" => return super::fork::execute(manager, owner, command).await,
        _ => {}
    }
    if command.method == SESSION_CHILDREN || command.method == SESSION_CHILD {
        let id = command
            .session_id
            .as_deref()
            .ok_or_else(|| invalid("session_id is required for this command"))?;
        let mut params = command.params.clone();
        let fields = params
            .as_object_mut()
            .ok_or_else(|| invalid("Command parameters must be an object"))?;
        if fields.contains_key("session_id") {
            return Err(invalid(
                "session_id must be supplied in the command envelope, not parameters",
            ));
        }
        fields.insert("session_id".into(), json!(id));
        return if command.method == SESSION_CHILDREN {
            let request = serde_json::from_value::<super::child_inspection::ListRequest>(params)
                .map_err(|error| {
                    crate::route::error::json_error(
                        "Child inspection parameters must match the schema",
                        "params",
                        &error,
                    )
                })?;
            manager.inspect_children(owner, request).await
        } else {
            let request = serde_json::from_value::<super::child_inspection::DetailRequest>(params)
                .map_err(|error| {
                    crate::route::error::json_error(
                        "Child inspection parameters must match the schema",
                        "params",
                        &error,
                    )
                })?;
            manager.inspect_child(owner, request).await
        };
    }
    if !METHODS.contains(&command.method.as_str()) {
        return Err(WireError::new("unsupported", "Method is not supported"));
    }
    let id = command
        .session_id
        .as_deref()
        .ok_or_else(|| invalid("session_id is required for this command"))?;
    if command.method == SESSION_DELETE {
        // Deletion answers retries from its tombstone, so it runs before the
        // live-record lookup that would report an already deleted session.
        let expected_revision = parse::<Revision>(command)?.expected_revision;
        return super::deletion::delete_session(manager, owner, id, Some(expected_revision)).await;
    }
    let record = manager.record(owner, id).await?;
    match command.method.as_str() {
        EXECUTIONS_LIST => {
            let params = parse::<crate::route::session::ClientCallsQuery>(command)?;
            let cursor = params.cursor.unwrap_or(0);
            let limit = params.limit.unwrap_or(64);
            if limit == 0 || limit > 64 {
                return Err(invalid("limit must be between 1 and 64"));
            }
            let all = manager.inspect_executions(&record).await?;
            if cursor > all.len() {
                return Err(invalid("cursor exceeds the retained execution count"));
            }
            let mut items = Vec::new();
            let mut used = 256;
            for row in all.iter().skip(cursor).take(limit) {
                let size = serde_json::to_vec(row).map_err(|_| serialization())?.len();
                if used + size > 64 * 1024 {
                    if items.is_empty() {
                        return Err(WireError::new(
                            "execution_metadata_too_large",
                            format!(
                                "Retained execution {} metadata requires {size} bytes plus a 256-byte page envelope and exceeds the 65536-byte history page budget",
                                row.execution_id.0
                            ),
                        ));
                    }
                    break;
                }
                used += size;
                items.push(row.clone());
            }
            let next = cursor + items.len();
            return Ok(
                json!({"items":items,"next_cursor":if next < all.len() {Some(next)}else{None}}),
            );
        }
        EXECUTIONS_CANCEL => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct CancelExecution {
                execution_id: u64,
            }
            let params: CancelExecution = parse(command)?;
            let slot = manager
                .registry
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(id)
                .cloned()
                .ok_or_else(unavailable)?;
            let managed = slot.active.lock().await.clone().ok_or_else(|| {
                WireError::new(
                    "closed",
                    "Open the session explicitly before cancelling an execution",
                )
            })?;
            managed
                .session
                .cancel_execution(ExecutionId(params.execution_id))
                .await
                .map_err(safe)?;
            return Ok(json!({"requested":true,"execution_id":params.execution_id}));
        }
        EXECUTIONS_READ => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct ReadTask {
                execution_id: u64,
                cursor: Option<u64>,
                max_bytes: Option<usize>,
            }
            let params: ReadTask = parse(command)?;
            if params.max_bytes.is_some_and(|max| max == 0 || max > 65536) {
                return Err(invalid("max_bytes must be between 1 and 65536"));
            }
            let slot = manager
                .registry
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(id)
                .cloned()
                .ok_or_else(unavailable)?;
            let managed = slot.active.lock().await.clone().ok_or_else(|| {
                WireError::new(
                    "closed",
                    "Open the session explicitly before reading retained execution output",
                )
            })?;
            return managed
                .session
                .read_execution_output(
                    ExecutionId(params.execution_id),
                    params.cursor.unwrap_or(0),
                    params.max_bytes.unwrap_or(16384),
                )
                .and_then(|page| {
                    serde_json::to_value(page)
                        .map_err(|_| Error::protocol("execution output serialization failed"))
                })
                .map_err(safe);
        }
        EXECUTIONS_CONTENT => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct ReadContent {
                execution_id: u64,
                field: microcrab_session::runtime::execution::ExecutionContentField,
                cursor: Option<u64>,
                max_bytes: Option<usize>,
            }
            let params: ReadContent = parse(command)?;
            if params.max_bytes.is_some_and(|max| max == 0 || max > 65536) {
                return Err(invalid("max_bytes must be between 1 and 65536"));
            }
            let slot = manager
                .registry
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(id)
                .cloned()
                .ok_or_else(unavailable)?;
            let managed = slot.active.lock().await.clone().ok_or_else(|| {
                WireError::new(
                    "closed",
                    "Retained execution content is unavailable in a closed runtime",
                )
            })?;
            return managed
                .session
                .executions()
                .read_content(
                    ExecutionId(params.execution_id),
                    params.field,
                    params.cursor.unwrap_or(0),
                    params.max_bytes.unwrap_or(16384),
                )
                .and_then(|page| {
                    serde_json::to_value(page)
                        .map_err(|_| Error::protocol("execution content serialization failed"))
                })
                .map_err(safe);
        }
        DISPLAY_OBSERVATIONS => {
            return super::display::query(manager, owner, id, parse(command)?).await;
        }
        AUDIT_REQUESTS => {
            let selector = parse::<super::audit::Request>(command)?;
            return serde_json::to_value(super::audit::query(manager, owner, id, selector).await?)
                .map_err(|_| serialization());
        }
        ROUND_LIMIT_REPLY => {
            return Err(WireError::new(
                "socket_required",
                "Round confirmations require the current executor socket",
            ));
        }
        PERMISSION_REPLY => {
            return Err(WireError::new(
                "socket_required",
                "Permission replies require the current executor socket",
            ));
        }
        CLIENT_TOOLS_CALLS => {
            let params = parse::<crate::route::session::ClientCallsQuery>(command)?;
            let page = manager
                .client_tools(id)
                .await
                .map_err(safe)?
                .calls_page(params.cursor.unwrap_or(0), params.limit.unwrap_or(64))
                .await
                .map_err(safe)?;
            return serde_json::to_value(page).map_err(|_| serialization());
        }
        CLIENT_TOOLS_BIND | CLIENT_TOOLS_RESULT | CLIENT_TOOLS_UNBIND | CLIENT_TOOLS_PREPARE
        | CLIENT_TOOLS_OUTPUT => {
            return Err(WireError::new(
                "socket_required",
                "Client tool execution belongs to its authenticated socket",
            ));
        }
        SESSION_BLOCKS => {
            let params: Messages = parse(command)?;
            return manager.inspect_message_page(&record, &params).await;
        }
        _ => {}
    }
    if command.method == "turn.get" {
        let params: Operation = parse(command)?;
        operation_id(&params.operation_id)?;
        let operation = manager
            .directory
            .get_operation(owner, &params.operation_id)
            .await
            .map_err(safe)?
            .filter(|operation| operation.session_id == id)
            .ok_or_else(unavailable)?;
        return serde_json::to_value(reconcile(manager, operation).await?)
            .map_err(|_| serialization());
    }
    if matches!(
        command.method.as_str(),
        TURN_SUBMIT
            | TURN_RETRY
            | TURN_QUEUE
            | TURN_STEER
            | INBOX_UPDATE
            | INBOX_REMOVE
            | INBOX_RESUME
    ) {
        let id = if command.method == INBOX_RESUME {
            let params: ResumePending = parse(command)?;
            params.validate()?;
            params.operation_id
        } else if matches!(command.method.as_str(), INBOX_UPDATE | INBOX_REMOVE) {
            inbox::mutation_operation_id(command)?
        } else if command.method == TURN_RETRY {
            let params: Retry = parse(command)?;
            operation_id(&params.failed_operation_id)?;
            params.operation_id
        } else {
            let params: Submit = parse(command)?;
            if params.activate_if_idle && command.method != TURN_QUEUE {
                return Err(WireError::new(
                    "invalid_request",
                    "Idle activation is only valid for turn.queue",
                ));
            }
            validate_input(&params.input)?;
            params.operation_id
        };
        operation_id(&id)?;
        let digest = hash(command)?;
        if let Some(operation) = manager
            .directory
            .get_operation(owner, &id)
            .await
            .map_err(safe)?
        {
            return replay(reconcile(manager, operation).await?, command, &digest);
        }
    }
    if command.method == "session.archive" {
        let expected_revision = parse::<Revision>(command)?.expected_revision;
        return super::deletion::archive_session(manager, owner, id, Some(expected_revision)).await;
    }
    if command.method == SESSION_OPEN {
        let _: Empty = parse(command)?;
        if let Some(managed) = super::agent_init::open_deferred(manager, &record).await? {
            return manager.refresh(&managed).await.map_err(safe);
        }
    }
    if command.method == SESSION_RETRY_AUTO_COMPACTION {
        return super::auto_compaction::retry(manager, owner, record, command).await;
    }
    if command.method == SESSION_RETRY_AGENT_INIT {
        return super::agent_init::retry(manager, owner, record, command).await;
    }
    if command.method == "session.close" {
        let _: Empty = parse(command)?;
        let slot = {
            let registry = manager.registry.lock().unwrap_or_else(|e| e.into_inner());
            registry.get(id).cloned()
        };
        let active_managed = match slot {
            Some(slot) => slot.active.lock().await.clone(),
            None => None,
        };
        if let Some(managed) = active_managed {
            let closed = close_session(manager, &managed, owner, id, None).await?;
            // Project the now-dormant journal off the request path so the next
            // attach is answered from the manifest-keyed cache.
            manager.warm_dormant(record.clone());
            return Ok(closed);
        } else {
            return Ok(view::session(
                manager,
                &record,
                &manager.config.provider.model,
                "closed",
            ));
        }
    }
    if command.method == SESSION_INTERRUPT && manager.record_allowed(&record) {
        // Stop must not queue behind journal admission (an attach refresh or a
        // dormant projection holds it): an active runtime is interrupted
        // directly. Only a session without one takes the activation path.
        let _: Empty = parse(command)?;
        let slot = manager
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .cloned();
        let active = match slot {
            Some(slot) => slot.active.lock().await.clone(),
            None => None,
        };
        if let Some(managed) = active {
            managed
                .session
                .interrupt_with_reason(microcrab_session::InterruptReason::UserRequested);
            managed.hub.interrupt_operation();
            return Ok(json!({"requested":true}));
        }
    }
    if command.method == SESSION_RENAME
        && let Some(renamed) = rename_dormant(manager, owner, id, command).await?
    {
        return Ok(renamed);
    }
    let managed = if matches!(
        command.method.as_str(),
        PERMISSION_GET
            | PERMISSION_SET
            | STATE_GET
            | STATE_METADATA
            | STATE_INSPECTION
            | "session.interrupt"
    ) {
        manager.activate(record, true, None, true).await?
    } else {
        manager.active(record, true).await?
    };
    if command.method == "session.interrupt" {
        let _: Empty = parse(command)?;
        managed
            .session
            .interrupt_with_reason(microcrab_session::InterruptReason::UserRequested);
        managed.hub.interrupt_operation();
        return Ok(json!({"requested":true}));
    }
    if command.method == STATE_INSPECTION {
        let _: Empty = parse(command)?;
        let inspection = managed.session.state_handle().inspection().map_err(safe)?;
        return serde_json::to_value(inspection).map_err(|_| serialization());
    }
    if command.method == STATE_METADATA {
        let _: Empty = parse(command)?;
        return managed
            .session
            .state_handle()
            .variable_metadata()
            .map_err(safe);
    }
    if command.method == STATE_GET {
        let _: Empty = parse(command)?;
        return managed.session.state_handle().view_all().map_err(safe);
    }
    let _gate = managed.commands.lock().await;
    if command.method != PERMISSION_GET && super::agent_init::running(manager, id) {
        return Err(super::agent_init::pending_error());
    }
    if managed.suspended.load(Ordering::Acquire) {
        return Err(WireError::new(
            "host_suspended",
            "Reopen the session after Host reconciliation",
        ));
    }
    let requires_host = !matches!(
        command.method.as_str(),
        "session.open"
            | SESSION_BLOCKS
            | "permission.get"
            | STATE_GET
            | CLIENT_TOOLS_SET
            | EXECUTIONS_CANCEL
            | "session.rename"
    );
    if !manager.embedded && requires_host {
        let current_record = managed.record.lock().await.clone();
        manager.record_host(&current_record).await?;
    }
    if managed.closing.load(Ordering::Acquire) || managed.faulted.load(Ordering::Acquire) {
        return Err(WireError::new("closed", "Session requires reopening"));
    }
    match command.method.as_str() {
        CLIENT_TOOLS_SET => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct UpdateTools {
                expected_revision: u64,
                definitions: Vec<super::client_tools::ClientToolDefinition>,
            }
            let params: UpdateTools = parse(command)?;
            let mut record = managed.record.lock().await;
            if record.revision != params.expected_revision {
                return Err(WireError::new(
                    "revision_conflict",
                    "Session revision changed",
                ));
            }
            let applied = managed.client_tools.snapshot().await;
            let frozen = managed.session.context_snapshot().map_err(safe)?;
            if params.definitions.iter().any(|candidate| {
                frozen
                    .tools
                    .iter()
                    .any(|tool| tool.name == candidate.definition.name)
                    && !applied
                        .definitions
                        .iter()
                        .any(|tool| tool.definition.name == candidate.definition.name)
            }) {
                return Err(invalid(
                    "Client tool name collides with an installed native tool",
                ));
            }
            let epoch = managed
                .session
                .state()
                .epoch_id
                .checked_add(1)
                .ok_or_else(|| invalid("Epoch counter exhausted"))?;
            managed
                .client_tools
                .stage(params.definitions, epoch)
                .await
                .map_err(safe)?;
            let revision = record.revision;
            let mut next = record.clone();
            next.revision = revision
                .checked_add(1)
                .ok_or_else(|| invalid("Session revision exhausted"))?;
            next.updated_at_ms = now().max(next.updated_at_ms);
            match manager.directory.replace(owner, revision, next).await {
                Ok(next) => *record = next,
                Err(error) => {
                    managed.faulted.store(true, Ordering::Release);
                    return Err(safe(error));
                }
            }
            managed
                .client_tools
                .acknowledge_pending()
                .await
                .map_err(safe)?;
            drop(record);
            if let Some(registrations) = managed.client_tools.pending_registrations().await {
                let tools = registrations
                    .into_iter()
                    .filter_map(|entry| match entry {
                        microcrab_session::runtime::ToolRegistration::External(tool) => Some(tool),
                        _ => None,
                    })
                    .collect();
                managed
                    .session
                    .replace_external_tools(tools)
                    .await
                    .map_err(safe)?;
            }
            let mut value = serde_json::to_value(managed.client_tools.snapshot().await)
                .map_err(|_| serialization())?;
            value["pending_catalog_change"] = json!(true);
            Ok(value)
        }
        "session.open" => {
            let _: Empty = parse(command)?;
            Ok(managed.hub.snapshot())
        }
        "permission.get" => {
            let _: Empty = parse(command)?;
            permission_projection(&managed)
        }
        "workspace.files" => {
            let params: Files = parse(command)?;
            let pattern = params.pattern.unwrap_or_else(|| "**/*".into());
            if pattern.len() > 4096 {
                return Err(invalid("Workspace pattern exceeds 4096 bytes"));
            }
            let request = microcrab_host_api::GlobRequest {
                root: ".".into(),
                pattern,
                include_hidden: false,
                include_ignored: false,
                include_directories: false,
                max_results: params.limit.unwrap_or(100).clamp(1, 200),
            };
            let files = microcrab_host_api::Host::glob(
                managed.host.as_ref(),
                request,
                tokio_util::sync::CancellationToken::new(),
            )
            .await
            .map_err(|error| {
                WireError::new(
                    "workspace_unavailable",
                    error
                        .public_reason()
                        .filter(|reason| reason.len() <= 2048)
                        .unwrap_or("Workspace listing failed"),
                )
            })?;
            let items: Vec<_> = files
                .entries
                .into_iter()
                .filter(|entry| entry.kind == microcrab_host_api::GlobEntryKind::File)
                .map(|entry| entry.path)
                .collect();
            Ok(json!({"items":items,"truncated":files.truncated}))
        }
        "session.rename" => {
            let params: Rename = parse(command)?;
            validate_text(
                &params.title,
                "Session title must not be empty",
                "Session title exceeds 4096 bytes",
                4096,
            )?;
            let mut record = managed.record.lock().await;
            let mut next = record.clone();
            next.title = params.title;
            next.revision = params
                .expected_revision
                .checked_add(1)
                .ok_or_else(|| invalid("Revision counter is exhausted"))?;
            next.updated_at_ms = now();
            *record = manager
                .directory
                .replace(owner, params.expected_revision, next)
                .await
                .map_err(safe)?;
            let result = view::session(
                manager,
                &record,
                &manager.config.provider.model,
                if managed.hub.operation().is_some() {
                    "running"
                } else {
                    "idle"
                },
            );
            drop(record);
            // An explicit user title wins even when it equals the default label.
            // Cancel only after its durable CAS succeeds, outside the record lock.
            super::naming::stop(&managed).await.map_err(|error| {
                let detail = super::CleanupDetail::from_join(error);
                WireError::new(detail.code, detail.message)
            })?;
            super::naming::suppress_after_manual(&managed, manager.directory.as_ref())
                .await
                .map_err(safe)?;
            manager.refresh(&managed).await.map_err(safe)?;
            Ok(result)
        }
        "permission.set" => permission(manager, &managed, command).await,
        TURN_QUEUE | TURN_STEER => inbox::insert(manager, managed.clone(), command).await,
        INBOX_UPDATE | INBOX_REMOVE => inbox::mutate(manager, managed.clone(), command).await,
        TURN_SUBMIT | TURN_RETRY | INBOX_RESUME => submit(manager, managed.clone(), command).await,
        "checkpoint.create" => {
            let _: Empty = parse(command)?;
            idle(&managed)?;
            managed
                .session
                .checkpoint_navigation()
                .await
                .map_err(safe)?;
            manager.refresh(&managed).await.map_err(safe)?;
            Ok(json!({"cursor":managed.journal.cursor().await}))
        }
        "history.travel" => {
            let params: Direction = parse(command)?;
            idle(&managed)?;
            let display_direction = params.direction.clone();
            let (recover, restored_input) = history::prepare(&managed, params).await?;
            let tools = match managed
                .client_tools
                .registrations_for(&recover.context.tools)
                .await
            {
                Ok(tools) => tools,
                Err(error) => {
                    managed.journal.cancel_pending().await;
                    return Err(safe(error));
                }
            };
            let selection_intent =
                match super::display::begin_selection(&managed, &display_direction).await {
                    Ok(intent) => intent,
                    Err(error) => {
                        managed.journal.cancel_pending().await;
                        return Err(error);
                    }
                };
            if let Err(error) = managed
                .session
                .recover_with_external_tools(recover, tools)
                .await
            {
                managed.journal.cancel_pending().await;
                super::display::finish_selection(manager, &managed, selection_intent, false).await;
                if managed.session.is_faulted() {
                    managed.faulted.store(true, Ordering::Release);
                }
                return Err(safe(error));
            }
            super::display::finish_selection(manager, &managed, selection_intent, true).await;
            if let Some(registrations) = managed.client_tools.pending_registrations().await {
                let tools = registrations
                    .into_iter()
                    .filter_map(|entry| match entry {
                        microcrab_session::runtime::ToolRegistration::External(tool) => Some(tool),
                        _ => None,
                    })
                    .collect();
                managed
                    .session
                    .replace_external_tools(tools)
                    .await
                    .map_err(safe)?;
            }
            let mut snapshot = manager.refresh(&managed).await.map_err(safe)?;
            if let Some(input) = restored_input {
                snapshot["restored_input"] = input;
            }
            view::bound_snapshot(snapshot).map_err(safe)
        }
        "session.compact" => {
            let params: Compact = parse(command)?;
            super::compaction::start(manager, managed.clone(), command, params).await
        }
        "session.set_model" => {
            let params: ModelSetting = parse(command)?;
            super::model::select(
                manager,
                &managed,
                params.expected_revision,
                ModelSelection {
                    provider_id: params.provider_id,
                    model: params.model,
                    variant: params.variant,
                },
            )
            .await
        }
        "session.set_mode" => {
            let params: Mode = parse(command)?;
            if params.mode.is_empty() {
                return Err(invalid("Session mode must not be empty"));
            }
            if params.mode.len() > 256 {
                return Err(invalid("Session mode exceeds 256 bytes"));
            }
            let mut record = managed.record.lock().await;
            if params
                .expected_revision
                .is_some_and(|expected| expected != record.revision)
            {
                return Err(WireError::new(
                    "revision_conflict",
                    "Session revision changed",
                ));
            }
            managed.session.validate_mode(&params.mode).map_err(safe)?;
            let revision = record.revision;
            let mut next = record.clone();
            next.mode = Some(params.mode.clone());
            next.revision = revision
                .checked_add(1)
                .ok_or_else(|| invalid("Session revision exhausted"))?;
            next.updated_at_ms = now().max(record.updated_at_ms);
            *record = match manager.directory.replace(owner, revision, next).await {
                Ok(next) => next,
                Err(error) => {
                    managed.faulted.store(true, Ordering::Release);
                    return Err(safe(error));
                }
            };
            drop(record);
            if let Err(error) = managed.session.select_mode(params.mode).await {
                managed.faulted.store(true, Ordering::Release);
                managed
                    .session
                    .interrupt_with_reason(microcrab_session::InterruptReason::StateReplaced);
                return Err(safe(error));
            }
            let record = managed.record.lock().await;
            let mut snapshot = managed.hub.snapshot();
            let status =
                if managed.session.inbox_snapshot().active || managed.hub.operation().is_some() {
                    "running"
                } else {
                    "idle"
                };
            let mut session =
                view::session(manager, &record, &manager.config.provider.model, status);
            super::model::project_route(&managed, &record, &mut session);
            snapshot["session"] = session.clone();
            managed.hub.session(session);
            Ok(snapshot)
        }
        _ => Err(WireError::new(
            "unsupported",
            "Method is handled by the WebSocket subscription owner",
        )),
    }
}
pub(super) async fn close_session(
    manager: &Manager,
    managed: &Managed,
    owner: &str,
    id: &str,
    expected_revision: Option<u64>,
) -> Result<Value, WireError> {
    finish_session(
        manager,
        managed,
        owner,
        id,
        expected_revision,
        expected_revision.is_some(),
        false,
    )
    .await
}
pub(super) async fn archive_managed(
    manager: &Manager,
    managed: &Managed,
    owner: &str,
    id: &str,
    expected_revision: Option<u64>,
) -> Result<Value, WireError> {
    finish_session(manager, managed, owner, id, expected_revision, true, false).await
}
/// Close a live runtime without changing archive state, so a caller that removes
/// the record owns the metadata transition. Shares the ordinary close fences.
pub(super) async fn settle_for_delete(
    manager: &Manager,
    managed: &Managed,
    owner: &str,
    id: &str,
) -> Result<(), WireError> {
    // The directory record is already removed; the seal, not a revision,
    // excludes further edits.
    finish_session(manager, managed, owner, id, None, false, true)
        .await
        .map(|_| ())
}
async fn finish_session(
    manager: &Manager,
    managed: &Managed,
    owner: &str,
    id: &str,
    expected_revision: Option<u64>,
    archive: bool,
    discard_snapshot: bool,
) -> Result<Value, WireError> {
    {
        let _gate = managed.commands.lock().await;
        if archive && managed.closing.load(Ordering::Acquire) {
            return Err(WireError::new("closed", "Session is closing"));
        }
        if let Some(expected) = expected_revision
            && managed.record.lock().await.revision != expected
        {
            return Err(WireError::new(
                "revision_conflict",
                "Session revision changed",
            ));
        }
        managed.closing.store(true, Ordering::Release);
        managed
            .session
            .interrupt_with_reason(microcrab_session::InterruptReason::SessionClosed);
        managed.close_token.cancel();
        managed.hub.interrupt_operation();
        super::naming::stop(managed).await.map_err(|error| {
            let detail = super::CleanupDetail::from_join(error);
            WireError::new(detail.code, detail.message)
        })?;
    }
    // Keep a containment failure quarantined. Successful cleanup permits removal
    // even when the subsequent archive metadata publication fails.
    let cleanup = if discard_snapshot {
        manager.close_managed_for_delete(managed).await
    } else {
        manager
            .close_managed_owned(managed, !archive && expected_revision.is_some())
            .await
    };
    cleanup.map_err(|failure| failure.wire_error())?;
    let result = async {
        if archive {
            let mut current = managed.record.lock().await;
            // Owned turn settlement may have advanced this revision after seal;
            // ordinary edits are now excluded by closing, so preserve that result.
            let revision = current.revision;
            let mut next = current.clone();
            next.archived = true;
            next.revision = revision
                .checked_add(1)
                .ok_or_else(|| invalid("Session revision counter is exhausted"))?;
            next.updated_at_ms = now().max(current.updated_at_ms);
            *current = manager
                .directory
                .replace(owner, revision, next)
                .await
                .map_err(safe)?;
            let session =
                view::session(manager, &current, &manager.config.provider.model, "closed");
            managed.hub.session(session.clone());
            Ok(session)
        } else {
            Ok(json!({"closed":true}))
        }
    }
    .await;
    let slot = {
        manager
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(id)
            .cloned()
    };
    if let Some(slot) = slot {
        let active = slot.active.lock().await;
        if active
            .as_ref()
            .is_some_and(|session| std::ptr::eq(session.as_ref(), managed))
        {
            let mut registry = manager
                .registry
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if registry
                .get(id)
                .is_some_and(|current| Arc::ptr_eq(current, &slot))
            {
                registry.remove(id);
            }
        }
    }
    result
}
fn idle(managed: &Managed) -> Result<(), WireError> {
    if managed.hub.operation().is_some() || managed.session.inbox_snapshot().active {
        Err(WireError::new("busy", "Session is busy"))
    } else {
        Ok(())
    }
}
pub(super) fn validate_input(input: &SessionInput) -> Result<(), WireError> {
    for part in &input.parts {
        if let InputPart::Image { source, .. } = part
            && matches!(source, ImageSource::Path { .. } | ImageSource::Url { .. })
        {
            return Err(WireError::new(
                "unsupported_image_source",
                "Browser images require inline data",
            ));
        }
    }
    Ok(())
}
pub(super) async fn create(
    manager: &Manager,
    owner: &str,
    command: &Command,
) -> Result<Value, WireError> {
    let params: Create = parse(command)?;
    operation_id(&params.operation_id)?;
    let digest = hash(command)?;
    if let Some(operation) = manager
        .directory
        .get_operation(owner, &params.operation_id)
        .await
        .map_err(safe)?
    {
        let operation = reconcile(manager, operation).await?;
        let _ = replay(operation.clone(), command, &digest)?;
        if operation.status != "completed" {
            return Err(WireError::new(
                "operation_interrupted",
                "Session creation requires reconciliation",
            ));
        }
        return operation
            .result
            .ok_or_else(|| WireError::new("operation_failed", "Creation receipt unavailable"));
    }
    // Project deletion holds this gate through its child enumeration/publication.
    let _identity = manager.identity_gate.lock().await;
    let defaults = manager
        .effective_settings(owner, params.project_id.as_deref(), None)
        .await?
        .0;
    let prompt_overrides = crate::config::settings::SettingsOverrides {
        system_template: params.system_template.clone(),
        compact_template: params.compact_template.clone(),
        auto_compact_template: params.auto_compact_template.clone(),
        rename_template: params.rename_template.clone(),
        action_review_template: params.action_review_template.clone(),
        client_prompt: params.client_prompt.clone(),
        ..Default::default()
    };
    let mut prompt_settings = defaults.clone();
    prompt_settings.overlay(&prompt_overrides);
    // Deterministic identity also locates an interrupted creation's frozen inputs.
    let id = if let Some(id) = params.conversation_id.clone() {
        id
    } else {
        microcrab_storage::history::content_hash(&json!([owner, params.operation_id]))
            .map_err(safe)?[..32]
            .to_owned()
    };
    let prior_record = manager.directory.get(owner, &id).await.map_err(safe)?;
    let prompt_config = if let Some(record) = &prior_record {
        record.prompt_config.clone().unwrap_or_default()
    } else if let Some(snapshot) = manager
        .configuration
        .get(owner, "session_effective", &id)
        .await
        .map_err(safe)?
        .and_then(|record| record.value)
    {
        let snapshot: crate::config::settings::SettingsOverrides = serde_json::from_value(snapshot)
            .map_err(|error| {
                WireError::new(
                    "invalid_params",
                    microcrab_protocol::diagnostic::json_reason(&error),
                )
            })?;
        super::prompt_defaults::resolve(&snapshot)
    } else {
        super::prompt_defaults::resolve(&prompt_settings)
    };
    // Validate every slot before pins, settings snapshots, records, tools or receipts.
    prompt_config.validate().map_err(safe)?;
    let model_selection = super::model::normalize(
        params
            .model_selection
            .clone()
            .or_else(|| defaults.model_selection.clone())
            .unwrap_or_else(|| super::model::default_selection(&manager.config)),
    );
    super::providers::resolve(manager, owner, &model_selection).await?;
    let (project_id, project_root_configured, host) = match (
        params.project_id.as_deref(),
        params.conversation_id.as_deref(),
    ) {
        (Some(project), None) => {
            let registered = manager
                .projects
                .find(project)
                .await
                .filter(|p| p.allows(owner))
                .ok_or_else(unavailable)?;
            let host = manager.project_host(owner, project).await?;
            (Some(registered.id), registered.root, host)
        }
        (None, Some(id)) if !manager.embedded => {
            uuid::Uuid::parse_str(id).map_err(|_| invalid("conversation_id must be a UUID"))?;
            let binding = manager
                .pins
                .conversation(id)
                .await
                .map_err(safe)?
                .filter(|pin| pin.owner == owner)
                .ok_or_else(unavailable)?;
            (
                None,
                binding.workspace_root,
                manager.conversation_host(owner, id).await?,
            )
        }
        _ => return Err(invalid("Specify exactly one project_id or conversation_id")),
    };
    let components = super::assembly::components(&manager.config, host)
        .await
        .map_err(safe)?;
    let selected = if let Some(digest) = params.component.as_ref() {
        Some(
            components
                .iter()
                .find(|component| &component.digest == digest)
                .ok_or_else(|| {
                    WireError::new("component_unavailable", "Selected component is unavailable")
                })?,
        )
    } else if components.len() > 1 {
        return Err(WireError::new(
            "component_required",
            "Select the exact agent component digest",
        ));
    } else {
        components.first()
    };
    if !manager.embedded && selected.is_none() {
        return Err(WireError::new(
            "component_unavailable",
            "No backend agent component is configured",
        ));
    }
    let catalog = selected
        .map(|component| component.agents.as_slice())
        .unwrap_or_default();
    let main_role = |agent: &&microcrab_agent_types::AgentInfo| {
        agent.modes.iter().any(|mode| {
            matches!(
                mode.role,
                microcrab_agent_types::AgentRole::MainAgent
                    | microcrab_agent_types::AgentRole::Both
            )
        })
    };
    let agent = params
        .agent
        .or_else(|| defaults.agent.clone())
        .or_else(|| selected.and_then(|component| component.config.name.clone()))
        .or_else(|| {
            catalog
                .iter()
                .find(main_role)
                .map(|agent| agent.name.clone())
        });
    let selected_agent = match agent.as_ref() {
        Some(name) => Some(
            catalog
                .iter()
                .find(|entry| &entry.name == name)
                .ok_or_else(|| {
                    WireError::new("agent_unavailable", "Selected agent is unavailable")
                })?,
        ),
        None => None,
    };
    let mode = params
        .mode
        .or_else(|| defaults.mode.clone())
        .or_else(|| selected.and_then(|component| component.config.mode.clone()))
        .or_else(|| {
            selected_agent
                .and_then(|agent| {
                    agent.modes.iter().find(|mode| {
                        matches!(
                            mode.role,
                            microcrab_agent_types::AgentRole::MainAgent
                                | microcrab_agent_types::AgentRole::Both
                        )
                    })
                })
                .map(|mode| mode.name.clone())
        });
    if let Some(selected_agent) = selected_agent
        && !selected_agent.modes.iter().any(|entry| {
            Some(&entry.name) == mode.as_ref()
                && matches!(
                    entry.role,
                    microcrab_agent_types::AgentRole::MainAgent
                        | microcrab_agent_types::AgentRole::Both
                )
        })
    {
        return Err(WireError::new(
            "agent_unavailable",
            "Selected main agent mode is unavailable",
        ));
    }
    if prior_record.is_none() {
        prompt_config.render_system(selected_agent).map_err(safe)?;
    }
    let component_digest = selected.map(|component| component.digest.clone());
    let discovery_digest = microcrab_storage::history::content_hash(&catalog).map_err(safe)?;
    let title = params.title.unwrap_or_else(|| "New session".into());
    validate_text(
        &title,
        "Session title must not be empty",
        "Session title exceeds 4096 bytes",
        4096,
    )?;
    // Pins follow complete template validation and selected-Agent rendering.
    let project_root = if manager.embedded {
        manager
            .pins
            .pin_component(microcrab_storage::execution::SessionComponent {
                session_id: id.clone(),
                component_digest: component_digest.clone(),
                discovery_digest: discovery_digest.clone(),
            })
            .await
            .map_err(|_| {
                WireError::new("component_mismatch", "Session component identity differs")
            })?;
        project_root_configured
    } else if project_id.is_none() {
        let binding = manager
            .pins
            .conversation(&id)
            .await
            .map_err(safe)?
            .ok_or_else(unavailable)?;
        let root = binding.workspace_root.clone();
        manager
            .pins
            .pin_conversation_session(microcrab_storage::execution::ConversationSessionBinding {
                session_id: id.clone(),
                conversation: binding,
                component_digest: component_digest.clone(),
                discovery_digest,
            })
            .await
            .map_err(|_| {
                WireError::new(
                    "binding_mismatch",
                    "Conversation execution identity differs",
                )
            })?;
        root
    } else {
        let binding = manager
            .pins
            .project(
                project_id
                    .as_deref()
                    .ok_or_else(|| invalid("project_id is required for project binding"))?,
            )
            .await
            .map_err(safe)?
            .ok_or_else(unavailable)?;
        let root = binding.workspace_root.clone();
        manager
            .pins
            .pin_session(microcrab_storage::execution::SessionBinding {
                session_id: id.clone(),
                project: binding,
                component_digest: component_digest.clone(),
                discovery_digest,
            })
            .await
            .map_err(|_| {
                WireError::new("binding_mismatch", "Session execution identity differs")
            })?;
        root
    };
    let existing = manager.directory.get(owner, &id).await.map_err(safe)?;
    let restore = existing.is_some();
    let record = if let Some(record) = existing {
        if record.project_id != project_id
            || record.project_root != project_root
            || record.agent != agent
            || record.mode != mode
            || record
                .model_selection
                .as_ref()
                .is_some_and(|selection| selection != &model_selection)
        {
            return Err(WireError::new(
                "operation_conflict",
                "Session creation requires reconciliation",
            ));
        }
        record
    } else {
        let timestamp = now();
        let snapshot = manager
            .configuration
            .get(owner, "session_effective", &id)
            .await
            .map_err(safe)?;
        if snapshot.is_none() {
            let mut frozen = defaults.clone();
            frozen.system_template = Some(prompt_config.system_template.clone());
            frozen.compact_template = Some(prompt_config.compact_template.clone());
            frozen.auto_compact_template = Some(prompt_config.auto_compact_template.clone());
            frozen.rename_template = Some(prompt_config.rename_template.clone());
            frozen.action_review_template = Some(prompt_config.action_review_template.clone());
            frozen.client_prompt = Some(prompt_config.client_prompt.clone());
            frozen.model_selection = Some(model_selection.clone());
            frozen.agent = agent.clone();
            frozen.mode = mode.clone();
            frozen.auto_compaction = Some(
                defaults
                    .auto_compaction
                    .clone()
                    .unwrap_or_else(|| manager.config.auto_compaction.clone()),
            );
            frozen.auto_naming = Some(defaults.auto_naming.unwrap_or(manager.config.auto_rename));
            manager
                .configuration
                .compare_exchange(owner, "session_effective", &id, 0, Some(json!(frozen)))
                .await
                .map_err(safe)?;
        }
        if manager
            .configuration
            .get(owner, "session_references", &id)
            .await
            .map_err(safe)?
            .is_none()
        {
            let sources = if let Some(project) = project_id.as_deref() {
                manager
                    .configuration
                    .get(owner, "project_references", project)
                    .await
                    .map_err(safe)?
                    .and_then(|r| r.value)
                    .unwrap_or_else(|| json!([]))
            } else {
                json!([])
            };
            manager
                .configuration
                .compare_exchange(owner, "session_references", &id, 0, Some(sources))
                .await
                .map_err(safe)?;
        }
        let has_prompt_overrides = prompt_overrides.system_template.is_some()
            || prompt_overrides.compact_template.is_some()
            || prompt_overrides.auto_compact_template.is_some()
            || prompt_overrides.rename_template.is_some()
            || prompt_overrides.action_review_template.is_some()
            || prompt_overrides.client_prompt.is_some();
        if has_prompt_overrides
            && manager
                .configuration
                .get(owner, "session_settings", &id)
                .await
                .map_err(safe)?
                .is_none()
        {
            manager
                .configuration
                .compare_exchange(
                    owner,
                    "session_settings",
                    &id,
                    0,
                    Some(json!(prompt_overrides)),
                )
                .await
                .map_err(safe)?;
        }
        manager
            .directory
            .create(SessionRecord {
                version: RECORD_VERSION,
                id: id.clone(),
                owner: owner.into(),
                project_id: project_id.clone(),
                project_root,
                title,
                agent,
                mode,
                model_selection: Some(model_selection),
                auto_compaction: Some(
                    defaults
                        .auto_compaction
                        .clone()
                        .unwrap_or_else(|| manager.config.auto_compaction.clone()),
                ),
                prompt_config: Some(prompt_config.clone()),
                permissions: defaults.permissions.clone().unwrap_or_default(),
                initialization: defaults
                    .initialization
                    .unwrap_or(microcrab_permission::Action::Allow),
                review: defaults.review.unwrap_or(true),
                review_model: defaults.review_model.clone().flatten(),
                revision: 0,
                archived: false,
                created_at_ms: timestamp,
                updated_at_ms: timestamp,
            })
            .await
            .map_err(safe)?
    };
    let record = manager.selected_record(record).await?;
    if !restore {
        manager
            .client_tools(&id)
            .await
            .map_err(safe)?
            .register(params.client_tools, 0)
            .await
            .map_err(safe)?;
    }
    let timestamp = now();
    let mut operation = manager
        .directory
        .create_operation(OperationRecord {
            version: RECORD_VERSION,
            id: params.operation_id,
            session_id: id.clone(),
            owner: owner.into(),
            method: command.method.clone(),
            payload_hash: digest,
            status: "accepted".into(),
            result: Some(json!({"server_instance":manager.instance})),
            revision: 0,
            created_at_ms: timestamp,
            updated_at_ms: timestamp,
        })
        .await
        .map_err(safe)?;
    match manager.active(record, restore).await {
        Ok(managed) => {
            let record = managed.record.lock().await;
            let mut result =
                view::session(manager, &record, &manager.config.provider.model, "idle");
            result["component"] = json!(component_digest);
            result["agent_initialization_deferred"] = json!(super::agent_init::deferred(&managed));
            result["agent_init_pending"] = json!(
                super::agent_init::deferred(&managed)
                    || super::agent_init::pending(&managed.session)
            );
            drop(record);
            operation.status = "completed".into();
            operation.revision += 1;
            operation.updated_at_ms = now().max(operation.updated_at_ms);
            operation.result = Some(result.clone());
            manager
                .directory
                .replace_operation(owner, 0, operation)
                .await
                .map_err(safe)?;
            manager.refresh(&managed).await.map_err(safe)?;
            Ok(result)
        }
        Err(error) => {
            operation.status = "failed".into();
            operation.revision += 1;
            operation.updated_at_ms = now().max(operation.updated_at_ms);
            operation.result = Some(json!({"error":error}));
            let _ = manager
                .directory
                .replace_operation(owner, 0, operation)
                .await;
            Err(error)
        }
    }
}
fn permission_projection(managed: &Managed) -> Result<Value, WireError> {
    let controller = managed.session.permission_controller();
    let policy = controller.snapshot().map_err(permission_error)?;
    let inventory = managed
        .session
        .permission_inventory()
        .map_err(|_| WireError::new("revision_conflict", "Permission policy changed"))?;
    if controller.snapshot().map_err(permission_error)?.revision != policy.revision {
        return Err(WireError::new(
            "revision_conflict",
            "Permission policy changed",
        ));
    }
    let mut value = serde_json::to_value(policy).map_err(|_| serialization())?;
    value["tool_inventory"] = inventory;
    Ok(value)
}
pub(super) async fn permission(
    manager: &Manager,
    managed: &Managed,
    command: &Command,
) -> Result<Value, WireError> {
    let params: Permission = parse(command)?;
    if params.overrides.len() > 256 {
        return Err(invalid("Permission overrides exceed 256 rules"));
    }
    let controller = managed.session.permission_controller();
    let (old, old_review_client) = controller.review_binding().map_err(permission_error)?;
    if old.revision != params.expected_revision {
        return Err(WireError::new(
            "revision_conflict",
            "Permission revision changed",
        ));
    }
    let mut config = old.config.clone();
    config.overrides = params.overrides;
    config.review = params.review;
    if let Some(initialization) = params.initialization {
        config.initialization = initialization;
    }
    let mut review_client = old_review_client.clone();
    if let Some(selection) = params.review_model {
        config.review_model = selection.map(|selection| {
            let normalized = super::model::normalize(microcrab_storage::ModelSelection {
                provider_id: selection.provider_id,
                model: selection.model,
                variant: selection.variant,
            });
            microcrab_permission::ReviewModelSelection {
                provider_id: normalized.provider_id,
                model: normalized.model,
                variant: normalized.variant,
            }
        });
        microcrab_permission::validate(&config).map_err(permission_validation)?;
        review_client = if let Some(selection) = config.review_model.as_ref() {
            let owner = managed.record.lock().await.owner.clone();
            super::providers::resolve(
                manager,
                &owner,
                &microcrab_storage::ModelSelection {
                    provider_id: selection.provider_id.clone(),
                    model: selection.model.clone(),
                    variant: selection.variant.clone(),
                },
            )
            .await?;
            Some(
                super::model::review_target(manager, &owner, selection, managed.hub.clone())
                    .await
                    .map_err(safe)?,
            )
        } else {
            None
        };
    }
    microcrab_permission::validate(&config).map_err(permission_validation)?;
    // Persist user configuration before it can authorize a later invocation.
    // Core catalog/mode changes may advance native policy while storage awaits;
    // rebase only these user fields, never overwrite newly published defaults.
    let mut record = managed.record.lock().await;
    let revision = record.revision;
    let mut next = record.clone();
    next.permissions = config.overrides.clone();
    next.initialization = config.initialization;
    next.review = config.review;
    next.review_model = config.review_model.clone();
    next.revision = revision
        .checked_add(1)
        .ok_or_else(|| invalid("Session revision exhausted"))?;
    next.updated_at_ms = now().max(record.updated_at_ms);
    *record = match manager
        .directory
        .replace(&record.owner, revision, next)
        .await
    {
        Ok(next) => next,
        Err(error) => {
            managed.faulted.store(true, Ordering::Release);
            return Err(safe(error));
        }
    };
    drop(record);
    let mut applied = false;
    for _ in 0..3 {
        let mut current = controller.snapshot().map_err(permission_error)?;
        current.config.overrides = config.overrides.clone();
        current.config.initialization = config.initialization;
        current.config.review = config.review;
        current.config.review_model = config.review_model.clone();
        match managed.session.replace_permissions_with_review(
            current.revision,
            current.config,
            review_client.clone(),
        ) {
            Ok(_) => {
                applied = true;
                break;
            }
            Err(microcrab_session::policy::PermissionError::StalePolicy) => continue,
            Err(error) => {
                managed.faulted.store(true, Ordering::Release);
                managed
                    .session
                    .interrupt_with_reason(microcrab_session::InterruptReason::StateReplaced);
                return Err(permission_error(error));
            }
        }
    }
    if !applied {
        managed.faulted.store(true, Ordering::Release);
        managed
            .session
            .interrupt_with_reason(microcrab_session::InterruptReason::StateReplaced);
        return Err(WireError::new(
            "revision_conflict",
            "Permission changed during durable publication; reopen to reconcile",
        ));
    }
    permission_projection(managed)
}
pub(super) async fn submit(
    manager: &Manager,
    managed: Arc<Managed>,
    command: &Command,
) -> Result<Value, WireError> {
    let resume = if command.method == INBOX_RESUME {
        let params: ResumePending = parse(command)?;
        params.validate()?;
        Some(params)
    } else {
        None
    };
    let (operation, input, failed_operation_id) = if let Some(params) = &resume {
        (params.operation_id.clone(), None, None)
    } else if command.method == TURN_RETRY {
        let params: Retry = parse(command)?;
        operation_id(&params.failed_operation_id)?;
        (params.operation_id, None, Some(params.failed_operation_id))
    } else {
        let params: Submit = parse(command)?;
        validate_input(&params.input)?;
        (params.operation_id, Some(params.input), None)
    };
    operation_id(&operation)?;
    let digest = hash(command)?;
    let record = managed.record.lock().await.clone();
    if let Some(operation) = manager
        .directory
        .get_operation(&record.owner, &operation)
        .await
        .map_err(safe)?
    {
        return replay(reconcile(manager, operation).await?, command, &digest);
    }
    idle(&managed)?;
    let mut display_retry = None;
    let retry_link = if let Some(failed_id) = failed_operation_id {
        let failures = manager
            .directory
            .list_operations(&record.owner, &record.id)
            .await
            .map_err(safe)?;
        let witness = view::retry_witness(
            &managed.session.context_snapshot().map_err(safe)?,
            &managed.session.state(),
        )
        .map_err(safe)?;
        if !managed.session.retry_ready()
            || view::retry_operation_id(&failures, &witness).as_deref() != Some(failed_id.as_str())
        {
            return Err(WireError::new(
                "retry_unavailable",
                "The failed request is no longer current",
            ));
        }
        let failed = failures
            .iter()
            .find(|failure| failure.id == failed_id)
            .ok_or_else(|| {
                WireError::new(
                    "retry_unavailable",
                    "The failed request is no longer current",
                )
            })?;
        display_retry = failed.result.as_ref().and_then(|result| {
            result["partial_output"]["display_ref"]["attempt_id"]
                .as_str()
                .map(str::to_owned)
        });
        Some(json!({
            "retry_of_operation_id":failed_id,
            "retry_after_message_id":failed.result.as_ref().and_then(|result| result.get("after_message_id")).cloned().unwrap_or(Value::Null),
        }))
    } else {
        None
    };
    // Optional naming has its own helper lane and continues across ordinary turns.
    if manager.record_host(&record).await.is_err() {
        return Err(WireError::new(
            "project_offline",
            "项目宿主机执行端离线，会话暂不可用",
        ));
    }
    let anchor = managed
        .journal
        .cursor()
        .await
        .map(|cursor| cursor.checkpoint_id);
    managed
        .client_tools
        .set_history_anchor(&record.id, anchor)
        .await
        .map_err(safe)?;
    let timestamp = if command.method == TURN_RETRY {
        manager
            .directory
            .list_operations(&record.owner, &record.id)
            .await
            .map_err(safe)?
            .iter()
            .map(|operation| operation.created_at_ms)
            .max()
            .map_or(now(), |last| now().max(last.saturating_add(1)))
    } else {
        now()
    };
    let mut accepted_result = json!({"server_instance":manager.instance});
    if let Some(link) = &retry_link {
        copy_retry_link(link, &mut accepted_result);
    }
    let mut operation = manager
        .directory
        .create_operation(OperationRecord {
            version: RECORD_VERSION,
            id: operation.clone(),
            session_id: record.id,
            owner: record.owner,
            method: command.method.clone(),
            payload_hash: digest,
            status: "accepted".into(),
            result: Some(accepted_result),
            revision: 0,
            created_at_ms: timestamp,
            updated_at_ms: timestamp,
        })
        .await
        .map_err(safe)?;
    managed.hub.set_operation(Some(operation.id.clone()));
    let cancellation = managed
        .hub
        .operation_cancellation()
        .expect("turn admission installs cancellation under the command gate");
    if let Err(error) = manager.refresh(&managed).await {
        managed.hub.set_operation(None);
        operation.status = "failed".into();
        operation.revision = 1;
        operation.updated_at_ms = now().max(operation.updated_at_ms);
        operation.result =
            Some(json!({"error":{"code":"snapshot_failed","message":"Turn was not started"}}));
        let _ = manager
            .directory
            .replace_operation(&operation.owner.clone(), 0, operation)
            .await;
        return Err(safe(error));
    }
    // The accepted admission callback consumes this exact receipt before any
    // new provider content; rejection clears it without selecting another head.
    managed.routing.prepare_display_retry(display_retry);
    managed.naming_admission.arm(input.clone());
    // [!important] Preserve paused queue semantics: only this explicit witnessed
    // action resumes the original entry; queue/steer never bypass the pause.
    let started = if let Some(params) = resume {
        managed
            .session
            .resume_pending_input_with_settlement(
                &params.expected_inbox_id,
                params.item_id,
                params.expected_revision,
                cancellation,
            )
            .await
    } else {
        match input {
            Some(input) => {
                managed
                    .session
                    .stream_submission_with_settlement(input, operation.id.clone(), cancellation)
                    .await
            }
            None => {
                managed
                    .session
                    .retry_stream_with_settlement(cancellation)
                    .await
            }
        }
    };
    let (stream, settlement_guard) = match started {
        Ok(stream) => stream,
        Err(error) => {
            managed.naming_admission.arm(None);
            managed.routing.prepare_display_retry(None);
            managed.hub.set_operation(None);
            operation.status = "failed".into();
            operation.revision = 1;
            operation.updated_at_ms = now().max(operation.updated_at_ms);
            operation.result = Some(json!({"error":safe(error)}));
            let receipt = match manager
                .directory
                .replace_operation(&operation.owner.clone(), 0, operation)
                .await
            {
                Ok(receipt) => receipt,
                Err(error) => {
                    managed.faulted.store(true, Ordering::Release);
                    managed
                        .hub
                        .publish(super::SessionRuntimeEvent::ResyncRequired(
                            super::ResyncReason::ReceiptPublicationFailed,
                        ));
                    return Err(safe(error));
                }
            };
            managed.hub.publish(super::SessionRuntimeEvent::Turn(
                serde_json::to_value(&receipt).map_err(|_| serialization())?,
            ));
            if manager.refresh(&managed).await.is_err() {
                managed
                    .hub
                    .publish(super::SessionRuntimeEvent::ResyncRequired(
                        super::ResyncReason::SnapshotFailed,
                    ));
            }
            return serde_json::to_value(receipt).map_err(|_| serialization());
        }
    };
    let response = serde_json::to_value(&operation).map_err(|_| serialization())?;
    let directory = manager.directory.clone();
    let model = manager.config.provider.model.clone();
    let owned = managed.clone();
    let worker = tokio::spawn(async move {
        let mut stream = stream;
        let mut outcome: Result<Value, Error> =
            Err(Error::protocol("turn driver ended without terminal event"));
        loop {
            match stream.recv().await {
                Some(Ok(SessionEvent::Finished {
                    outcome: mut finished,
                })) => {
                    finished.usage.provider_details.clear();
                    let truncated = finished.content.len() > 128 * 1024;
                    if truncated {
                        let mut end = 128 * 1024;
                        while !finished.content.is_char_boundary(end) {
                            end -= 1;
                        }
                        finished.content.truncate(end);
                    }
                    outcome = serde_json::to_value(finished)
                        .map(|mut value| {
                            value["content_truncated"] = json!(truncated);
                            value
                        })
                        .map_err(Error::from);
                    break;
                }
                Some(Ok(SessionEvent::Model { .. })) => {}
                Some(Err(error)) => {
                    outcome = Err(error);
                    break;
                }
                None => break,
            }
        }
        owned.naming_admission.arm(None);
        let status = match &outcome {
            Ok(_) => "completed",
            Err(Error::Cancelled) => "cancelled",
            Err(_) => "failed",
        };
        operation.status = status.into();
        operation.revision = 1;
        operation.updated_at_ms = now().max(operation.updated_at_ms);
        // The hub still owns this invocation until publication below, preventing
        // a subsequent turn from changing these canonical admission witnesses.
        let state = owned.session.state();
        let endpoint = view::messages(
            owned.session.messages(),
            state.epoch_id,
            &operation.session_id,
        )
        .into_iter()
        .last();
        let position = if state.turn_id.is_some() && state.round_id.is_some() {
            json!({"epoch":state.epoch_id,"turn":state.turn_id,"round":state.round_id})
        } else {
            endpoint
                .as_ref()
                .map(|message| message["position"].clone())
                .unwrap_or(Value::Null)
        };
        let after_message_id = endpoint.as_ref().map(|message| message["id"].clone());
        operation.result = Some(match outcome {
            Ok(mut value) => {
                value["position"] = position;
                value["after_message_id"] = json!(after_message_id);
                value
            }
            Err(error) => {
                let partial = owned.hub.failed_partial(
                    state.epoch_id,
                    state.turn_id,
                    state.round_id,
                    matches!(&error, Error::Limit(limit) if limit.is_pre_effect()),
                );
                let error = if matches!(&error, Error::Cancelled) {
                    super::cancellation::wire(stream.interrupt_reason().as_ref())
                } else {
                    safe(error)
                };
                json!({"error":error, "position":position, "partial_output":partial,
                    "after_message_id":after_message_id, "retry_ready":owned.session.retry_ready()})
            }
        });
        if let Some(link) = &retry_link
            && let Some(result) = operation.result.as_mut()
        {
            copy_retry_link(link, result);
        }
        let persisted = async {
            // Close releases this gate before joining the driver. Keep the latest
            // metadata locked through publication so rename/permission edits are
            // preserved, and no history command can move the acknowledged HEAD.
            let _gate = owned.commands.lock().await;
            let mut current = owned.record.lock().await;
            let expected_session_revision = current.revision;
            let mut next = current.clone();
            next.revision = next
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::protocol("session revision exhausted"))?;
            next.updated_at_ms = now().max(current.updated_at_ms);
            operation.updated_at_ms = operation.updated_at_ms.max(next.updated_at_ms);
            let journal_cursor = owned.journal.cursor().await;
            if let Some(Value::Object(result)) = operation.result.as_mut() {
                if result.get("retry_ready") == Some(&Value::Bool(true)) {
                    result.insert(
                        "retry_witness".into(),
                        json!(view::retry_witness(
                            &owned.session.context_snapshot()?,
                            &state,
                        )?),
                    );
                }
                result.insert(
                    "journal_cursor".into(),
                    serde_json::to_value(&journal_cursor)?,
                );
            }
            let settlement = microcrab_storage::directory::OperationSettlement {
                expected_session_revision,
                session: next,
                expected_operation_revision: 0,
                operation: operation.clone(),
                journal_cursor,
            };
            let (session, receipt) = directory
                .settle_operation(&operation.owner, settlement)
                .await?;
            *current = session;
            operation = receipt;
            Ok::<_, Error>(())
        }
        .await;
        if persisted.is_ok() {
            super::display::bind_failure(&owned, &operation).await;
        }
        owned.hub.set_operation(None);
        if persisted.is_err() {
            owned.faulted.store(true, Ordering::Release);
            owned
                .hub
                .publish(super::SessionRuntimeEvent::ResyncRequired(
                    super::ResyncReason::ReceiptPublicationFailed,
                ));
        } else {
            owned.hub.publish(super::SessionRuntimeEvent::Turn(
                serde_json::to_value(&operation).unwrap_or(Value::Null),
            ));
        }
        drop(settlement_guard);
        if let Err(error) = view::refresh(directory.as_ref(), &owned, &model).await {
            // A later admitted turn may advance Context after settlement releases
            // its reservation. That read conflict cannot invalidate this receipt.
            if !matches!(&error, Error::Busy) {
                owned.faulted.store(true, Ordering::Release);
            }
            tracing::warn!(error_code = %safe(error).code, "Settled turn snapshot refresh failed");
            owned
                .hub
                .publish(super::SessionRuntimeEvent::ResyncRequired(
                    super::ResyncReason::SnapshotFailed,
                ));
        }
    });
    *managed.worker.lock().await = Some(worker);
    // Admission is already durable and the driver owns the running turn. A stale
    // observational projection cannot turn that accepted operation into a command
    // rejection; consumers reconcile through the receipt and existing resync path.
    if let Err(error) = manager.refresh(&managed).await {
        tracing::warn!(error_code = %safe(error).code, "Accepted turn snapshot refresh failed");
        managed
            .hub
            .publish(super::SessionRuntimeEvent::ResyncRequired(
                super::ResyncReason::SnapshotFailed,
            ));
    }
    Ok(response)
}

/// A title is directory metadata. Renaming a dormant session is one CAS under
/// the activation gate and never restores a runtime; an already active session
/// returns `None` so the live path can stop naming and publish its view.
async fn rename_dormant(
    manager: &Manager,
    owner: &str,
    id: &str,
    command: &Command,
) -> Result<Option<Value>, WireError> {
    let params: Rename = parse(command)?;
    validate_text(
        &params.title,
        "Session title must not be empty",
        "Session title exceeds 4096 bytes",
        4096,
    )?;
    // Lock order matches activation: journal gate, then Slot.active.
    let _activation = manager.journal_open_gate(id).lock_owned().await;
    let slot = manager
        .registry
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(id)
        .cloned();
    if let Some(slot) = slot
        && slot.active.lock().await.is_some()
    {
        return Ok(None);
    }
    let record = manager
        .directory
        .get(owner, id)
        .await
        .map_err(safe)?
        .ok_or_else(unavailable)?;
    if record.archived {
        return Err(WireError::new("archived", "Session is archived"));
    }
    let mut next = record.clone();
    next.title = params.title;
    next.revision = params
        .expected_revision
        .checked_add(1)
        .ok_or_else(|| invalid("Revision counter is exhausted"))?;
    next.updated_at_ms = now().max(record.updated_at_ms);
    let saved = manager
        .directory
        .replace(owner, params.expected_revision, next)
        .await
        .map_err(safe)?;
    super::naming::suppress_record(&saved, manager.directory.as_ref())
        .await
        .map_err(safe)?;
    Ok(Some(view::session(
        manager,
        &saved,
        &manager.config.provider.model,
        "idle",
    )))
}
