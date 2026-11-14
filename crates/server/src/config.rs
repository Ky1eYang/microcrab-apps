//! Startup configuration owns server paths and secret-bearing provider/auth
//! settings. Durable project metadata belongs to the project registry.
//! Public capability/session projections never serialize this structure.
use crate::auth::AuthConfig;
mod jsonc;
mod provider;
pub(crate) mod settings;

/// Static startup diagnostics: never retain parser text, paths or secret values.
#[derive(Debug)]
pub struct ConfigError {
    pub stage: &'static str,
    pub code: &'static str,
    pub cause: String,
}
impl ConfigError {
    pub(super) fn new(stage: &'static str, code: &'static str) -> Self {
        Self {
            stage,
            code,
            cause: code.replace('_', " "),
        }
    }
    pub(super) fn with_cause(stage: &'static str, code: &'static str, cause: &str) -> Self {
        Self {
            stage,
            code,
            cause: microcrab_protocol::diagnostic::sanitize(cause, 2048),
        }
    }
    pub(super) fn io_source(stage: &'static str, source: &std::io::Error) -> Self {
        let mut error = Self::io(stage, source.kind());
        error.cause = format!(
            "{}; kind={:?}; os_error={:?}",
            microcrab_protocol::diagnostic::describe(source, 1800),
            source.kind(),
            source.raw_os_error()
        );
        error
    }
    pub(super) fn io(stage: &'static str, kind: std::io::ErrorKind) -> Self {
        Self::new(
            stage,
            match kind {
                std::io::ErrorKind::NotFound => "file_not_found",
                std::io::ErrorKind::PermissionDenied => "permission_denied",
                std::io::ErrorKind::InvalidData => "invalid_encoding",
                _ => "file_unavailable",
            },
        )
    }
}
impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}: {}", self.stage, self.code, self.cause)
    }
}
impl std::error::Error for ConfigError {}
#[cfg(test)]
mod tests;
use microcrab_transport::{AuthMode, OpenAiCompatibleConfig, Protocol, TransportConfig};
use serde::Deserialize;
use std::path::{Path, PathBuf};

pub const DEFAULT_CONFIG_PATH: &str = ".microcrab/config.jsonc";
pub const LEGACY_CONFIG_PATH: &str = ".microcrab/config.json";
pub const DEFAULT_AGENT_PATH: &str = ".microcrab/agents/coding.wasm";

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "listen")]
    pub listen: String,
    #[serde(default = "data_dir")]
    pub data_dir: PathBuf,
    #[serde(default)]
    pub storage: Storage,
    #[serde(default = "frontend_dir")]
    pub frontend_dir: PathBuf,
    pub auth: AuthConfig,
    pub provider: Provider,
    #[serde(default)]
    pub available_models: Vec<ModelChoice>,
    #[serde(default)]
    pub agent: Option<Agent>,
    /// Additional backend-owned components; selection uses exact content digests.
    #[serde(default)]
    pub agents: Vec<Agent>,
    #[serde(default = "enabled")]
    pub auto_rename: bool,
    #[serde(default = "microcrab_session::context::AutoCompactionPolicy::server_default")]
    pub auto_compaction: microcrab_session::context::AutoCompactionPolicy,
    /// Public model metadata is cached privately; network refresh never gates startup.
    #[serde(default)]
    pub model_metadata: ModelMetadataSettings,
    #[serde(default)]
    pub session_defaults: settings::SettingsOverrides,
    #[serde(default = "settings::hard_limits")]
    pub limit_ceilings: microcrab_session::Limits,
    #[serde(default = "max_sessions")]
    pub max_active_sessions: usize,
    #[serde(skip)]
    pub startup_dir: PathBuf,
    #[serde(skip)]
    pub(crate) private_paths: Vec<PathBuf>,
    #[serde(skip)]
    pub(crate) provider_catalog: Option<std::sync::Arc<provider::ResolvedCatalog>>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ModelMetadataSettings {
    pub enabled: bool,
    pub refresh_interval_secs: u64,
    pub request_timeout_ms: u64,
    pub max_response_bytes: usize,
}
impl Default for ModelMetadataSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            refresh_interval_secs: 6 * 60 * 60,
            request_timeout_ms: 20_000,
            max_response_bytes: 32 * 1024 * 1024,
        }
    }
}
impl ModelMetadataSettings {
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if !(60..=7 * 24 * 60 * 60).contains(&self.refresh_interval_secs)
            || !(100..=120_000).contains(&self.request_timeout_ms)
            || !(1024..=64 * 1024 * 1024).contains(&self.max_response_bytes)
        {
            return Err(ConfigError::new("model_metadata", "invalid_refresh_policy"));
        }
        Ok(())
    }
}
fn enabled() -> bool {
    true
}
fn listen() -> String {
    "127.0.0.1:4096".into()
}
fn data_dir() -> PathBuf {
    ".microcrab".into()
}
fn frontend_dir() -> PathBuf {
    "public".into()
}
fn max_sessions() -> usize {
    64
}
fn agent_path() -> PathBuf {
    DEFAULT_AGENT_PATH.into()
}
#[derive(Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Storage {
    #[cfg_attr(not(feature = "storage-sqlite"), default)]
    File,
    #[cfg_attr(feature = "storage-sqlite", default)]
    Sqlite,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, Deserialize)]
pub struct ModelChoice {
    pub id: String,
    pub name: String,
    pub provider_id: String,
    /// Effective wire protocol; endpoints and credentials remain private.
    #[serde(default = "provider_protocol")]
    pub protocol: Protocol,
    #[serde(default)]
    pub variants: Vec<String>,
    /// Explicit model capacity metadata, never an estimate of current occupancy.
    #[serde(
        default,
        deserialize_with = "deserialize_context_window_tokens",
        skip_serializing_if = "Option::is_none"
    )]
    pub context_window_tokens: Option<u64>,
}

fn deserialize_context_window_tokens<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<std::num::NonZeroU64>::deserialize(deserializer)
        .map(|value| value.map(std::num::NonZeroU64::get))
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provider {
    #[serde(default = "provider_protocol")]
    pub protocol: Protocol,
    #[serde(default)]
    pub auth: AuthMode,
    pub endpoint: String,
    pub model: String,
    #[serde(default)]
    pub api_key: Option<String>,
    /// Resettable network inactivity window; preparation and user waits are excluded.
    #[serde(default = "timeout")]
    pub timeout_ms: u64,
    /// Safety ceiling for one provider response; reception warns before it.
    #[serde(default = "response_budget")]
    pub max_response_bytes: usize,
}
fn provider_protocol() -> Protocol {
    Protocol::OpenaiCompatible
}
fn timeout() -> u64 {
    60_000
}
fn response_budget() -> usize {
    // Safety ceiling, not a reception budget: ordinary large responses are accepted.
    1024 * 1024 * 1024
}
impl Provider {
    pub fn client_transport_config(&self) -> TransportConfig {
        TransportConfig {
            protocol: self.protocol,
            auth: self.auth,
            endpoint: self.endpoint.clone(),
            model: self.model.clone(),
            api_key: self.api_key.clone(),
            timeout_ms: self.timeout_ms,
            max_response_bytes: self.max_response_bytes,
        }
    }

    pub fn client_config(&self) -> OpenAiCompatibleConfig {
        OpenAiCompatibleConfig {
            endpoint: self.endpoint.clone(),
            model: self.model.clone(),
            api_key: self.api_key.clone(),
            timeout_ms: self.timeout_ms,
            max_response_bytes: self.max_response_bytes,
        }
    }
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Agent {
    #[serde(default = "agent_path")]
    pub path: PathBuf,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
    /// Retained bytes may reopen pinned sessions but are not offered for creation.
    #[serde(default)]
    pub restore_only: bool,
}
pub(crate) struct ResolvedProviderSelection {
    pub provider: Provider,
    pub options: microcrab_transport::OpenAiCompatibleRequestOptions,
}

impl Config {
    /// Startup discovery precedes binary logging. Replay its bounded safe diagnostics
    /// once the embedding application's subscriber is installed.
    pub fn log_provider_discovery_warnings(&self) {
        if let Some(catalog) = &self.provider_catalog {
            for (provider_id, error) in &catalog.warnings {
                let provider_id = microcrab_protocol::diagnostic::sanitize(provider_id, 128);
                tracing::warn!(provider_id = %provider_id, stage = error.stage,
                    code = error.code, cause = %error.cause,
                    "provider model discovery did not supply models; configured models remain available");
            }
        }
    }

    pub(crate) fn shared_providers(&self) -> Vec<(String, Provider)> {
        self.provider_catalog
            .as_ref()
            .map(|catalog| {
                catalog
                    .providers
                    .iter()
                    .map(|(id, p)| (id.clone(), p.clone()))
                    .collect()
            })
            .unwrap_or_else(|| vec![("configured".into(), self.provider.clone())])
    }
    pub(crate) fn discover_provider(
        provider: Provider,
        id: &str,
    ) -> Result<Vec<ModelChoice>, ConfigError> {
        provider::discover_models(provider, id)
    }
    pub(crate) fn default_provider_id(&self) -> &str {
        self.provider_catalog
            .as_ref()
            .map(|catalog| catalog.default_provider_id.as_str())
            .unwrap_or("configured")
    }

    pub(crate) fn model_choices(&self) -> Vec<ModelChoice> {
        self.provider_catalog
            .as_ref()
            .map(|catalog| catalog.models.clone())
            .unwrap_or_else(|| {
                vec![ModelChoice {
                    id: self.provider.model.clone(),
                    name: self.provider.model.clone(),
                    provider_id: "configured".into(),
                    protocol: self.provider.protocol,
                    variants: vec!["default".into()],
                    context_window_tokens: self
                        .available_models
                        .iter()
                        .find(|choice| {
                            choice.provider_id == "configured" && choice.id == self.provider.model
                        })
                        .and_then(|choice| choice.context_window_tokens),
                }]
            })
    }

    pub(crate) fn resolve_selection(
        &self,
        provider_id: &str,
        model: &str,
        variant: Option<&str>,
    ) -> Result<ResolvedProviderSelection, ConfigError> {
        let variant = variant.filter(|value| !value.is_empty() && *value != "default");
        let choices = self.model_choices();
        let choice = choices
            .iter()
            .find(|choice| choice.provider_id == provider_id && choice.id == model)
            .ok_or_else(|| ConfigError::new("model_selection", "model_not_found"))?;
        if variant.is_some_and(|variant| !choice.variants.iter().any(|allowed| allowed == variant))
        {
            return Err(ConfigError::new("model_selection", "unsupported_variant"));
        }
        let mut provider = match &self.provider_catalog {
            Some(catalog) => catalog
                .routes
                .get(&(provider_id.to_owned(), model.to_owned()))
                .cloned()
                .ok_or_else(|| ConfigError::new("model_selection", "model_route_not_found"))?,
            None => self.provider.clone(),
        };
        if variant.is_some() && provider.protocol != Protocol::OpenaiCompatible {
            return Err(ConfigError::new("model_selection", "unsupported_variant"));
        }
        provider.model = model.into();
        let options = microcrab_transport::OpenAiCompatibleRequestOptions {
            reasoning_effort: variant.map(str::to_owned),
        };
        options
            .validate()
            .map_err(|_| ConfigError::new("model_selection", "unsupported_variant"))?;
        Ok(ResolvedProviderSelection { provider, options })
    }

    /// Prefer JSONC; only an absent default permits legacy JSON discovery.
    /// Explicit --config paths never fall back, and malformed files fail closed.
    pub fn read_default() -> Result<Self, Box<dyn std::error::Error>> {
        let startup = std::env::current_dir()
            .map_err(|error| ConfigError::io_source("configuration", &error))?;
        Self::read_default_at(&startup)
    }
    fn read_default_at(startup: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let preferred = startup.join(DEFAULT_CONFIG_PATH);
        let path = match std::fs::symlink_metadata(&preferred) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                startup.join(LEGACY_CONFIG_PATH)
            }
            _ => preferred,
        };
        Self::read_at(&path, startup)
    }
    pub fn read(path: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let startup = std::env::current_dir()
            .map_err(|error| ConfigError::io_source("configuration", &error))?;
        Self::read_at(path.as_ref(), &startup)
    }
    fn read_at(path: &Path, startup: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let path = path
            .canonicalize()
            .map_err(|error| ConfigError::io_source("configuration", &error))?;
        let text = provider::read_private(&path, "configuration")?;
        let mut value = jsonc::parse(&text)?;
        let catalog = provider::resolve(&mut value, startup)?;
        let mut config: Self = serde_json::from_value(value).map_err(|error| {
            ConfigError::with_cause(
                "configuration",
                "invalid_fields",
                &microcrab_protocol::diagnostic::json_reason(&error),
            )
        })?;
        config.private_paths.push(path);
        if let Some((path, catalog)) = catalog {
            config.private_paths.push(path);
            config.provider_catalog = Some(std::sync::Arc::new(catalog));
        }
        config.auth.jwt_secret = resolve_secret(&config.auth.jwt_secret)?;
        if let Some(secret) = &mut config.auth.client_secret {
            *secret = resolve_secret(secret)?;
        }
        if let Some(secret) = &mut config.provider.api_key {
            *secret = resolve_secret(secret)?;
        }
        if config.provider_catalog.is_none() || !config.provider.model.is_empty() {
            microcrab_transport::TransportClient::new(config.provider.client_transport_config())
                .map_err(|error| {
                    ConfigError::with_cause(
                        "provider",
                        "invalid_options",
                        &error.diagnostic_reason(),
                    )
                })?;
        }
        config.resolve(startup).map_err(|error| {
            ConfigError::with_cause(
                "configuration",
                "invalid_layout",
                &microcrab_protocol::diagnostic::describe(error.as_ref(), 2048),
            )
        })?;
        config.validate_storage_namespace()?;
        Ok(config)
    }
    /// A backend switch never imports, rewrites or silently hides old evidence.
    /// Metadata-only bounded traversal deliberately does not read private bodies.
    pub(crate) fn validate_storage_namespace(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.storage != Storage::Sqlite {
            return Ok(());
        }
        if self
            .data_dir
            .join("directory/directory.json")
            .try_exists()?
        {
            return Err("SQLite backend would hide an existing file directory; explicitly select storage=file or request a verified migration; source bytes are unchanged".into());
        }
        let root = self.data_dir.join("sessions");
        if !root.try_exists()? {
            return Ok(());
        }
        let mut pending = vec![(root, 0usize)];
        let mut remaining = 65536usize;
        while let Some((directory, depth)) = pending.pop() {
            if depth > 16 {
                return Err("storage namespace inspection depth exceeded; select the existing backend explicitly".into());
            }
            for entry in std::fs::read_dir(directory)? {
                let entry = entry?;
                remaining = remaining
                    .checked_sub(1)
                    .ok_or("storage namespace inspection budget exceeded")?;
                let kind = entry.file_type()?;
                if kind.is_symlink() {
                    return Err("storage namespace contains a link; explicit backend/migration inspection is required".into());
                }
                if !kind.is_dir() {
                    continue;
                }
                let session = entry.path();
                for name in [
                    "audit.jsonl",
                    "usage.jsonl",
                    "aggregate_usage.jsonl",
                    "journal",
                    "history",
                    "events.jsonl",
                    "plugin-audit.jsonl",
                    "plugin-audit",
                    "request",
                ] {
                    let exists = match std::fs::symlink_metadata(session.join(name)) {
                        Ok(_) => true,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                        Err(error) => return Err(error.into()),
                    };
                    if exists {
                        return Err("SQLite backend found historical file audit/usage/Journal evidence; select storage=file for a file namespace or request an explicit byte-preserving migration for mixed SQLite history; no data was imported or changed".into());
                    }
                }
                let children = session.join("subsessions");
                if children.try_exists()? {
                    if std::fs::symlink_metadata(&children)?
                        .file_type()
                        .is_symlink()
                    {
                        return Err("storage child namespace is a link".into());
                    }
                    pending.push((children, depth + 1));
                }
            }
        }
        Ok(())
    }
    pub fn resolve(&mut self, startup: &Path) -> Result<(), Box<dyn std::error::Error>> {
        self.auto_compaction
            .validate()
            .map_err(|_| ConfigError::new("auto_compaction", "invalid_thresholds"))?;
        if self
            .available_models
            .iter()
            .any(|choice| choice.context_window_tokens == Some(0))
        {
            return Err(ConfigError::new("configuration", "invalid_context_window").into());
        }
        self.startup_dir = startup.canonicalize()?;
        let startup = self.startup_dir.clone();
        let private = startup.join(".microcrab");
        std::fs::create_dir_all(&private)?;
        let private = private.canonicalize()?;
        if !self.private_paths.contains(&private) {
            self.private_paths.push(private);
        }
        // Reserve the retired private name without reading or migrating its data.
        let retired_private = startup.join(".micro-crab");
        if retired_private.exists() && !self.private_paths.contains(&retired_private) {
            self.private_paths.push(retired_private);
        }
        if !self.data_dir.is_absolute() {
            self.data_dir = startup.join(&self.data_dir);
        }
        std::fs::create_dir_all(&self.data_dir)?;
        self.data_dir = self.data_dir.canonicalize()?;
        if !self.private_paths.contains(&self.data_dir) {
            self.private_paths.push(self.data_dir.clone());
        }
        if !self.frontend_dir.is_absolute() {
            self.frontend_dir = startup.join(&self.frontend_dir);
        }
        std::fs::create_dir_all(&self.frontend_dir)?;
        self.frontend_dir = self.frontend_dir.canonicalize()?;
        if self
            .private_paths
            .iter()
            .any(|private| private.starts_with(&self.frontend_dir))
            || self.frontend_dir.starts_with(&self.data_dir)
        {
            return Err("static asset directory overlaps server-private paths".into());
        }
        for agent in self.agent.iter_mut().chain(self.agents.iter_mut()) {
            if !agent.path.is_absolute() {
                agent.path = startup.join(&agent.path);
            }
            if agent
                .path
                .canonicalize()
                .is_ok_and(|path| path.starts_with(&self.frontend_dir))
            {
                return Err("selected component overlaps public static assets".into());
            }
        }
        if self.max_active_sessions == 0 || self.max_active_sessions > 4096 {
            return Err("invalid session capacity".into());
        }
        Ok(())
    }
}
fn resolve_secret(value: &str) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(name) = value.strip_prefix("env:") {
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(
                ConfigError::new("secret_reference", "invalid_environment_reference").into(),
            );
        }
        return std::env::var(name).map_err(|_| {
            ConfigError::new("secret_reference", "environment_reference_absent").into()
        });
    }
    Ok(value.to_owned())
}
pub(crate) fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
}
