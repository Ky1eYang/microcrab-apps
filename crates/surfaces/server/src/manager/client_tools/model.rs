//! Wire-neutral records. No browser tool identity or payload schema lives here.
use microcrab_permission::{Matcher, Permission};
use microcrab_protocol::ToolDefinition;
use microcrab_session::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) const MAX_JSON_BYTES: usize = 64 * 1024;
pub(super) const MAX_SNAPSHOT_BYTES: usize = 192 * 1024;
pub(super) const MAX_PAGE_BYTES: usize = 2 * 1024 * 1024;
pub(super) const MAX_STORE_BYTES: usize = 32 * 1024 * 1024;
pub(super) const MAX_CALLS: usize = 8192;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ClientToolDefinition {
    #[serde(flatten)]
    pub definition: ToolDefinition,
    #[serde(default)]
    pub permissions: Vec<Permission>,
    /// A pending human interaction may be displayed and answered after reconnect.
    /// False means disconnect settles the call; effects must never be replayed.
    #[serde(default)]
    pub reclaimable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction: Option<ClientInteraction>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClientInteraction {
    /// Client-selected JSON pointer containing the mode.
    pub mode_pointer: String,
    #[serde(default = "default_wait_timeout")]
    pub wait_timeout_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_writes: Option<SecretWrites>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SecretWrites {
    pub keys_pointer: String,
    pub max_keys: usize,
}
fn default_wait_timeout() -> u64 {
    300_000
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InteractionMode {
    Blocking,
    Deferred,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BindRequest {
    pub client_id: String,
    pub catalog_digest: String,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResultRequest {
    #[serde(default)]
    pub lease_id: String,
    pub invocation_id: String,
    #[serde(default, deserialize_with = "present_json")]
    pub result: Option<Value>,
    pub error: Option<String>,
}
/// Explicit producer messages are control data, never inferred from tool text.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutputRequest {
    #[serde(default)]
    pub lease_id: String,
    pub invocation_id: String,
    pub event: ProducerEvent,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ProducerEvent {
    Final {
        content: String,
        failure: Option<microcrab_session::runtime::execution::ToolFailure>,
    },
    Buffered {
        #[serde(default)]
        channels: Vec<String>,
        #[serde(default)]
        initial_output: bool,
    },
    Chunk {
        channel: Option<String>,
        content: String,
    },
    Finish {
        failure: Option<microcrab_session::runtime::execution::ToolFailure>,
    },
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrepareRequest {
    #[serde(default)]
    pub lease_id: String,
    pub invocation_id: String,
    #[serde(default, deserialize_with = "present_json")]
    pub timeout_result: Option<Value>,
}
fn present_json<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClientRole {
    Executor,
    Participant,
    Observer,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CallStatus {
    Pending,
    Paused,
    Completed,
    Failed,
    Interrupted,
    Cancelled,
    Deadline,
}
impl CallStatus {
    pub fn unsettled(self) -> bool {
        matches!(self, Self::Pending | Self::Paused)
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct CallRecord {
    pub invocation_id: String,
    pub parent_session_id: String,
    pub session_id: String,
    pub call_id: String,
    #[serde(default)]
    #[serde(alias = "runtime_task_id")]
    pub execution_id: Option<u64>,
    pub tool: String,
    pub arguments: Value,
    pub epoch: u64,
    pub turn_id: Option<u64>,
    pub round_id: Option<u64>,
    /// Set by the Manager from its authoritative journal cursor when available.
    pub history_anchor: Option<String>,
    pub catalog_digest: String,
    pub created_at_ms: u64,
    pub deadline_ms: u64,
    pub settled_at_ms: Option<u64>,
    pub status: CallStatus,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_json"
    )]
    pub result: Option<Value>,
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<microcrab_session::runtime::execution::ToolFailure>,
    pub reclaimable: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub interaction: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub responder_socket: Option<String>,
    #[serde(default)]
    pub blocking_mode: Option<InteractionMode>,
    #[serde(default)]
    pub prepared: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_json"
    )]
    pub timeout_result: Option<Value>,
    #[serde(default)]
    pub timeout_applied: bool,
}
#[derive(Clone, Debug, Serialize)]
pub(crate) struct BindSnapshot {
    pub role: ClientRole,
    pub can_interact: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub participation_id: Option<String>,
    pub lease_id: Option<String>,
    pub catalog_digest: String,
    pub definitions: Vec<ClientToolDefinition>,
    pub pending: Vec<CallRecord>,
    /// Durable invocation order, including settled historical branches.
    pub calls: Vec<CallRecord>,
    pub calls_truncated: bool,
    pub pending_truncated: bool,
    pub next_cursor: Option<usize>,
    pub total_calls: usize,
}
#[derive(Clone, Debug, Serialize)]
pub(crate) struct CallPage {
    pub calls: Vec<CallRecord>,
    pub next_cursor: Option<usize>,
    pub total_calls: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClientToolPhase {
    Requested,
    Prepared,
    Paused,
    Settled,
    CancelRequested,
}
#[derive(Clone, Debug, Serialize)]
pub(crate) struct ClientToolEvent {
    pub phase: ClientToolPhase,
    pub call: CallRecord,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Durable {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configured: Option<Catalog>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged: Option<Catalog>,
    pub catalogs: Vec<Catalog>,
    pub calls: Vec<CallRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secret_operations: Vec<SecretOperation>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SecretOperation {
    pub operation_id: String,
    pub invocation_id: String,
    pub status: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SecretRequest {
    pub invocation_id: String,
    pub operation_id: String,
    pub values: std::collections::BTreeMap<String, String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Catalog {
    pub epoch: u64,
    pub digest: String,
    pub definitions: Vec<ClientToolDefinition>,
}

pub(super) fn invalid(message: &'static str) -> Error {
    Error::protocol(message)
}
pub(super) fn bounded<T: Serialize>(value: &T, max: usize) -> Result<Vec<u8>, Error> {
    let bytes = serde_json::to_vec(value).map_err(|_| invalid("invalid client tool JSON"))?;
    if bytes.len() > max {
        return Err(invalid("client tool JSON exceeds size limit"));
    }
    Ok(bytes)
}
pub(super) fn identifier(value: &str) -> Result<(), Error> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
    {
        return Err(invalid("invalid client tool identifier"));
    }
    Ok(())
}
pub(in crate::manager) fn validate_catalog(definitions: &[ClientToolDefinition]) -> Result<(), Error> {
    bounded(&definitions, MAX_JSON_BYTES)?;
    if definitions.len() > 128 {
        return Err(invalid("too many client tools"));
    }
    let mut names = BTreeSet::new();
    for entry in definitions {
        let definition = &entry.definition;
        identifier(&definition.name)?;
        if definition.name.len() > 64
            || !names.insert(&definition.name)
            || microcrab_session::runtime::BuiltinTool::from_name(&definition.name).is_some()
            || crate::manager::external_speakers::admission::reserved(&definition.name)
        {
            return Err(invalid("duplicate or reserved client tool name"));
        }
        if !definition.parameters.is_object() || definition.description.len() > 32 * 1024 {
            return Err(invalid("invalid client tool definition"));
        }
        if let Some(interaction) = &entry.interaction {
            if let Some(secret) = &interaction.secret_writes
                && (!entry.reclaimable
                    || !secret.keys_pointer.starts_with('/')
                    || secret.keys_pointer.len() > 256
                    || secret.max_keys == 0
                    || secret.max_keys > 8)
            {
                return Err(invalid("invalid private interaction write capability"));
            }
            if !interaction.mode_pointer.starts_with('/')
                || interaction.mode_pointer.len() > 256
                || interaction.wait_timeout_ms == 0
                || interaction.wait_timeout_ms > 86_400_000
            {
                return Err(invalid("invalid client interaction pointer or wait budget"));
            }
            let mut chars = interaction.mode_pointer.chars();
            while let Some(character) = chars.next() {
                if character == '~' && !matches!(chars.next(), Some('0' | '1')) {
                    return Err(invalid("invalid client interaction JSON pointer escape"));
                }
            }
        }
        // Provider defaults are scoped to their exact registered tool.
        for permission in &entry.permissions {
            if permission.tool != Some(Matcher::Exact(definition.name.clone())) {
                return Err(invalid(
                    "client provider permissions must target only their own tool invocation",
                ));
            }
        }
    }
    Ok(())
}
impl ClientToolDefinition {
    pub(super) fn interaction_mode(
        &self,
        arguments: &Value,
    ) -> Result<Option<InteractionMode>, Error> {
        let Some(interaction) = &self.interaction else {
            return Ok(None);
        };
        match arguments.pointer(&interaction.mode_pointer) {
            None => Ok(Some(InteractionMode::Blocking)),
            Some(Value::String(mode)) if mode == "blocking" => Ok(Some(InteractionMode::Blocking)),
            Some(Value::String(mode)) if mode == "deferred" => Ok(Some(InteractionMode::Deferred)),
            _ => Err(invalid("invalid client interaction mode")),
        }
    }
}
pub(super) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}
