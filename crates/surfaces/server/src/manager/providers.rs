//! Owner Provider catalogs are private records. Discovery replaces facts only;
//! manual overlays and admitted clients remain immutable across catalog edits.
use super::{Manager, safe};
use crate::config::{Config, ModelChoice, Provider, ResolvedProviderSelection};
use microcrab_protocol::application::WireError;
use microcrab_storage::{ModelSelection, configuration::ConfigurationRecord};
use microcrab_transport::{AuthMode, Protocol};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModelOverlay {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default)]
    pub variants: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<Protocol>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthMode>,
    #[serde(default)]
    pub disabled: bool,
}
const OVERRIDE_KIND: &str = "provider_override";
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UserProvider {
    pub name: String,
    pub protocol: Protocol,
    pub endpoint: String,
    pub auth: AuthMode,
    pub timeout_ms: u64,
    pub max_response_bytes: usize,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub models: Vec<ModelOverlay>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrivateProvider {
    provider: UserProvider,
    api_key: Option<String>,
    #[serde(default)]
    discovered: Vec<ModelOverlay>,
}
/// Per-owner model catalog override and manual `/models` refresh snapshot for a
/// shared administrator provider; connection endpoints and credentials stay server-owned.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdminProviderOverride {
    #[serde(default)]
    models: Vec<ModelOverlay>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    discovered: Option<Vec<ModelOverlay>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Key {
    provider_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Revision {
    provider_id: String,
    expected_revision: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Write {
    provider_id: String,
    expected_revision: u64,
    provider: UserProvider,
    #[serde(default)]
    api_key: SecretChange,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedWrite {
    provider_id: String,
    expected_revision: u64,
    models: Vec<ModelOverlay>,
}
#[derive(Default)]
enum SecretChange {
    #[default]
    Keep,
    Clear,
    Set(String),
}
impl<'de> Deserialize<'de> for SecretChange {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Option::<String>::deserialize(d).map(|s| s.map(Self::Set).unwrap_or(Self::Clear))
    }
}
fn configuration_error(error: microcrab_session::Error) -> WireError {
    if matches!(&error,microcrab_session::Error::Protocol(reason) if reason.public_reason() == Some("configuration revision conflict; reload before saving"))
    {
        WireError::new("revision_conflict", error.diagnostic_reason())
    } else {
        safe(error)
    }
}
fn invalid(message: impl Into<String>) -> WireError {
    WireError::new("invalid_params", message)
}
fn id(id: &str) -> Result<&str, WireError> {
    let Some(key) = id.strip_prefix("user:") else {
        return Err(invalid("Private Provider ids must start with user:"));
    };
    if key.is_empty()
        || key.len() > 128
        || !key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
    {
        return Err(invalid(
            "Provider id must be user: followed by 1..128 letters, digits, - or _",
        ));
    }
    Ok(id)
}
fn decode(record: &ConfigurationRecord) -> Result<PrivateProvider, WireError> {
    serde_json::from_value(
        record
            .value
            .clone()
            .ok_or_else(|| WireError::new("not_found", "Provider removed"))?,
    )
    .map_err(|_| {
        WireError::new(
            "configuration_unavailable",
            "Stored Provider schema cannot be decoded",
        )
    })
}
fn decode_override(value: &Value) -> Result<AdminProviderOverride, WireError> {
    serde_json::from_value(value.clone()).map_err(|_| {
        WireError::new(
            "configuration_unavailable",
            "Stored shared Provider override schema cannot be decoded",
        )
    })
}
fn shared_provider(config: &Config, id: &str) -> Option<Provider> {
    config
        .shared_providers()
        .into_iter()
        .find(|(shared, _)| shared == id)
        .map(|(_, provider)| provider)
}
fn choice_to_overlay(m: ModelChoice) -> ModelOverlay {
    ModelOverlay {
        id: m.id,
        name: Some(m.name),
        variants: m.variants,
        context_window_tokens: m.context_window_tokens,
        protocol: Some(m.protocol),
        endpoint: None,
        auth: None,
        disabled: false,
    }
}
fn startup_admin_models(config: &Config, provider_id: &str) -> Vec<ModelOverlay> {
    config
        .model_choices()
        .into_iter()
        .filter(|m| m.provider_id == provider_id)
        .map(choice_to_overlay)
        .collect()
}
fn admin_baseline(config: &Config, id: &str, ov: &AdminProviderOverride) -> Vec<ModelOverlay> {
    ov.discovered
        .clone()
        .unwrap_or_else(|| startup_admin_models(config, id))
}
fn admin_variants(mut variants: Vec<String>) -> Vec<String> {
    if !variants.iter().any(|v| v == "default") {
        variants.insert(0, "default".into());
    }
    variants
}
fn merge_overlays(baseline: &[ModelOverlay], manual: &[ModelOverlay]) -> Vec<ModelOverlay> {
    let mut models: BTreeMap<String, ModelOverlay> = baseline
        .iter()
        .cloned()
        .map(|m| (m.id.clone(), m))
        .collect();
    for m in manual {
        let mut next = models.get(&m.id).cloned().unwrap_or_else(|| m.clone());
        if m.name.is_some() {
            next.name = m.name.clone();
        }
        if !m.variants.is_empty() {
            next.variants = m.variants.clone();
        }
        if m.context_window_tokens.is_some() {
            next.context_window_tokens = m.context_window_tokens;
        }
        if m.protocol.is_some() {
            next.protocol = m.protocol;
        }
        if m.endpoint.is_some() {
            next.endpoint = m.endpoint.clone();
        }
        if m.auth.is_some() {
            next.auth = m.auth;
        }
        next.disabled = m.disabled;
        models.insert(m.id.clone(), next);
    }
    models.into_values().collect()
}
fn transport(provider: &PrivateProvider, model: Option<&ModelOverlay>) -> Provider {
    Provider {
        // Owner Provider UI does not expose verified native media capability.
        google_function_response_media: false,
        protocol: model
            .and_then(|m| m.protocol)
            .unwrap_or(provider.provider.protocol),
        auth: model.and_then(|m| m.auth).unwrap_or(provider.provider.auth),
        endpoint: model
            .and_then(|m| m.endpoint.clone())
            .unwrap_or_else(|| provider.provider.endpoint.clone()),
        model: model
            .map(|m| m.id.clone())
            .unwrap_or_else(|| "discovery".into()),
        api_key: provider.api_key.clone(),
        timeout_ms: provider.provider.timeout_ms,
        max_response_bytes: provider.provider.max_response_bytes,
        headers: provider.provider.headers.clone(),
    }
}
fn validate_overlay_list(models: &[ModelOverlay], discovered_len: usize) -> Result<(), WireError> {
    if models.len() > 2048 || discovered_len > 2048 {
        return Err(invalid("Provider model catalog exceeds 2048 entries"));
    }
    let mut ids = std::collections::BTreeSet::new();
    for m in models {
        if m.id.trim().is_empty()
            || m.id.len() > 512
            || m.id.chars().any(char::is_control)
            || !ids.insert(&m.id)
        {
            return Err(invalid(
                "Manual model ids must be unique nonempty strings within 512 bytes",
            ));
        }
        if m.context_window_tokens == Some(0)
            || m.name.as_ref().is_some_and(|s| s.len() > 512)
            || m.variants.len() > 64
            || m.variants.iter().any(|v| v.is_empty() || v.len() > 128)
        {
            return Err(invalid(
                "Model name, variants or context_window_tokens exceeds its bound",
            ));
        }
    }
    Ok(())
}
fn validate_shared_override(
    config: &Config,
    id: &str,
    base: &Provider,
    ov: &AdminProviderOverride,
) -> Result<(), WireError> {
    validate_overlay_list(&ov.models, ov.discovered.as_ref().map_or(0, Vec::len))?;
    for m in &ov.models {
        if m.endpoint.is_some() || m.auth.is_some() {
            return Err(invalid(
                "Shared Provider model overrides cannot change endpoint or auth",
            ));
        }
        let mut route = config
            .shared_provider_route(id, &m.id)
            .unwrap_or_else(|| base.clone());
        if let Some(protocol) = m.protocol
            && protocol != route.protocol
        {
            return Err(invalid(
                "Shared Provider model overrides cannot change protocol",
            ));
        }
        route.model = m.id.clone();
        route
            .validate_media_capability()
            .and_then(|_| {
                route
                    .client_transport_config()
                    .validate()
                    .map_err(|_| crate::config::ConfigError::new("provider", "invalid_transport"))
            })
            .map_err(|_| {
                invalid("Provider protocol, endpoint or authentication configuration is inconsistent")
            })?;
    }
    Ok(())
}
fn validate(provider: &PrivateProvider) -> Result<(), WireError> {
    let p = &provider.provider;
    if p.name.trim().is_empty() || p.name.len() > 256 {
        return Err(invalid("Provider name must contain 1..256 bytes"));
    }
    if !(100..=3_600_000).contains(&p.timeout_ms)
        || !(1024..=1024 * 1024 * 1024).contains(&p.max_response_bytes)
    {
        return Err(invalid(
            "Provider timeout_ms must be 100..3600000 and max_response_bytes 1024..1073741824",
        ));
    }
    if provider
        .api_key
        .as_ref()
        .is_some_and(|s| s.is_empty() || s.len() > 16 * 1024 || s.chars().any(char::is_control))
    {
        return Err(invalid(
            "API key must contain 1..16384 bytes without control characters",
        ));
    }
    microcrab_transport::parse_custom_headers(&p.headers)
        .map_err(|error| invalid(format!("Provider headers: {error}")))?;
    validate_overlay_list(&p.models, provider.discovered.len())?;
    for model in std::iter::once(None).chain(
        merge_overlays(&provider.discovered, &p.models)
            .iter()
            .map(Some),
    ) {
        let route = transport(provider, model);
        let url = url::Url::parse(&route.endpoint)
            .map_err(|_| invalid("Provider endpoint must be an absolute HTTP(S) URL"))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid(
                "Provider endpoint requires HTTP(S), host, and no embedded credentials/query/fragment",
            ));
        }
        route.client_transport_config().validate().map_err(|_| {
            invalid("Provider protocol, endpoint or authentication configuration is inconsistent")
        })?;
    }
    Ok(())
}
fn public_admin(
    id: &str,
    p: &Provider,
    baseline: Vec<ModelOverlay>,
    models: Vec<ModelOverlay>,
    revision: u64,
) -> Value {
    json!({
        "id": id,
        "scope": "admin",
        "revision": revision,
        "read_only": true,
        "name": id,
        "protocol": p.protocol,
        "endpoint": p.endpoint,
        "auth": p.auth,
        "timeout_ms": p.timeout_ms,
        "max_response_bytes": p.max_response_bytes,
        "api_key_configured": p.api_key.is_some(),
        "models": models,
        "discovered": baseline,
    })
}
fn public(record: &ConfigurationRecord, p: &PrivateProvider) -> Value {
    let mut value = serde_json::to_value(&p.provider).expect("Provider DTO is JSON serializable");
    value["id"] = json!(record.target);
    value["scope"] = json!("user");
    value["revision"] = json!(record.revision);
    value["read_only"] = json!(false);
    value["api_key_configured"] = json!(p.api_key.is_some());
    value["discovered"] = json!(p.discovered);
    value
}
async fn admin_override_map(
    manager: &Manager,
    owner: &str,
) -> Result<BTreeMap<String, (u64, AdminProviderOverride)>, WireError> {
    let mut map = BTreeMap::new();
    for r in manager
        .configuration
        .list(owner, OVERRIDE_KIND)
        .await
        .map_err(configuration_error)?
    {
        if let Some(value) = r.value.as_ref() {
            map.insert(r.target, (r.revision, decode_override(value)?));
        }
    }
    Ok(map)
}
async fn discover_models(mut route: Provider, target: String) -> Result<Vec<ModelOverlay>, WireError> {
    route.model = "discovery".into();
    let models = tokio::task::spawn_blocking(move || Config::discover_provider(route, &target))
        .await
        .map_err(|_| WireError::new("discovery_failed", "Provider discovery worker failed"))?
        .map_err(|e| WireError::new("discovery_failed", format!("{}: {}", e.code, e.cause)))?;
    Ok(models.into_iter().map(choice_to_overlay).collect())
}
pub(crate) async fn command(
    manager: &Manager,
    owner: &str,
    method: &str,
    params: Value,
) -> Result<Value, WireError> {
    match method {
        "provider.list" => {
            let _: super::commands::EmptySettings = serde_json::from_value(params)
                .map_err(|_| invalid("Provider list accepts no parameters"))?;
            let overrides = admin_override_map(manager, owner).await?;
            let mut items = Vec::new();
            for (id, p) in manager.config.shared_providers() {
                let (revision, ov) = overrides.get(&id).cloned().unwrap_or_default();
                let baseline = admin_baseline(&manager.config, &id, &ov);
                items.push(public_admin(&id, &p, baseline, ov.models, revision));
            }
            for r in manager
                .configuration
                .list(owner, "provider")
                .await
                .map_err(configuration_error)?
            {
                let p = decode(&r)?;
                items.push(public(&r, &p));
            }
            Ok(json!({"items":items}))
        }
        "provider.get" => {
            let p: Key =
                serde_json::from_value(params).map_err(|_| invalid("Expected provider_id"))?;
            if let Some(base) = shared_provider(&manager.config, &p.provider_id) {
                let record = manager
                    .configuration
                    .get(owner, OVERRIDE_KIND, &p.provider_id)
                    .await
                    .map_err(configuration_error)?;
                let revision = record.as_ref().map_or(0, |r| r.revision);
                let ov = record
                    .as_ref()
                    .and_then(|r| r.value.as_ref())
                    .map(decode_override)
                    .transpose()?
                    .unwrap_or_default();
                let baseline = admin_baseline(&manager.config, &p.provider_id, &ov);
                return Ok(public_admin(&p.provider_id, &base, baseline, ov.models, revision));
            }
            if !p.provider_id.starts_with("user:") {
                return Err(WireError::new("not_found", "Provider not found"));
            }
            let r = manager
                .configuration
                .get(owner, "provider", id(&p.provider_id)?)
                .await
                .map_err(configuration_error)?
                .ok_or_else(|| WireError::new("not_found", "Provider not found"))?;
            Ok(public(&r, &decode(&r)?))
        }
        "provider.set" => {
            if let Some(pid) = params.get("provider_id").and_then(Value::as_str)
                && let Some(base) = shared_provider(&manager.config, pid)
            {
                if params.get("provider").is_some() || params.get("api_key").is_some() {
                    return Err(WireError::new(
                        "read_only",
                        "Shared administrator Provider configuration and credentials are read-only; submit only models overrides",
                    ));
                }
                let p: SharedWrite = serde_json::from_value(params).map_err(|error| {
                    invalid(format!(
                        "Shared Provider override write schema: {}",
                        microcrab_protocol::diagnostic::json_reason(&error)
                    ))
                })?;
                let old = manager
                    .configuration
                    .get(owner, OVERRIDE_KIND, &p.provider_id)
                    .await
                    .map_err(configuration_error)?;
                let old = old
                    .as_ref()
                    .and_then(|r| r.value.as_ref())
                    .map(decode_override)
                    .transpose()?
                    .unwrap_or_default();
                let next = AdminProviderOverride {
                    models: p.models,
                    discovered: old.discovered,
                };
                validate_shared_override(&manager.config, &p.provider_id, &base, &next)?;
                let r = manager
                    .configuration
                    .compare_exchange(
                        owner,
                        OVERRIDE_KIND,
                        &p.provider_id,
                        p.expected_revision,
                        Some(
                            serde_json::to_value(&next)
                                .map_err(|_| invalid("Cannot encode shared Provider override"))?,
                        ),
                    )
                    .await
                    .map_err(configuration_error)?;
                let baseline = admin_baseline(&manager.config, &p.provider_id, &next);
                return Ok(public_admin(
                    &p.provider_id,
                    &base,
                    baseline,
                    next.models,
                    r.revision,
                ));
            }
            let p: Write = serde_json::from_value(params).map_err(|error| {
                invalid(format!(
                    "Provider write schema: {}",
                    microcrab_protocol::diagnostic::json_reason(&error)
                ))
            })?;
            id(&p.provider_id)?;
            let old = manager
                .configuration
                .get(owner, "provider", &p.provider_id)
                .await
                .map_err(configuration_error)?;
            let old = old
                .as_ref()
                .filter(|r| r.value.is_some())
                .map(decode)
                .transpose()?;
            let key = match p.api_key {
                SecretChange::Keep => old.as_ref().and_then(|p| p.api_key.clone()),
                SecretChange::Clear => None,
                SecretChange::Set(k) => Some(k),
            };
            let next = PrivateProvider {
                provider: p.provider,
                api_key: key,
                discovered: old.map(|p| p.discovered).unwrap_or_default(),
            };
            validate(&next)?;
            let r = manager
                .configuration
                .compare_exchange(
                    owner,
                    "provider",
                    &p.provider_id,
                    p.expected_revision,
                    Some(
                        serde_json::to_value(&next)
                            .map_err(|_| invalid("Cannot encode Provider"))?,
                    ),
                )
                .await
                .map_err(configuration_error)?;
            Ok(public(&r, &next))
        }
        "provider.delete" => {
            let p: Revision = serde_json::from_value(params)
                .map_err(|_| invalid("Expected provider_id and expected_revision"))?;
            if shared_provider(&manager.config, &p.provider_id).is_some() {
                return Err(WireError::new(
                    "read_only",
                    "Shared administrator Provider identity is read-only and cannot be deleted",
                ));
            }
            id(&p.provider_id)?;
            let r = manager
                .configuration
                .compare_exchange(owner, "provider", &p.provider_id, p.expected_revision, None)
                .await
                .map_err(configuration_error)?;
            Ok(json!({"id":r.target,"revision":r.revision,"deleted":true}))
        }
        "provider.discover" => {
            let p: Revision = serde_json::from_value(params)
                .map_err(|_| invalid("Expected provider_id and expected_revision"))?;
            if let Some(base) = shared_provider(&manager.config, &p.provider_id) {
                let record = manager
                    .configuration
                    .get(owner, OVERRIDE_KIND, &p.provider_id)
                    .await
                    .map_err(configuration_error)?;
                let current_rev = record.as_ref().map_or(0, |r| r.revision);
                if current_rev != p.expected_revision {
                    return Err(WireError::new(
                        "revision_conflict",
                        "Provider changed before discovery",
                    ));
                }
                let mut next = record
                    .as_ref()
                    .and_then(|r| r.value.as_ref())
                    .map(decode_override)
                    .transpose()?
                    .unwrap_or_default();
                next.discovered = Some(discover_models(base.clone(), p.provider_id.clone()).await?);
                validate_shared_override(&manager.config, &p.provider_id, &base, &next)?;
                let r = manager
                    .configuration
                    .compare_exchange(
                        owner,
                        OVERRIDE_KIND,
                        &p.provider_id,
                        p.expected_revision,
                        Some(
                            serde_json::to_value(&next)
                                .map_err(|_| invalid("Cannot encode discovered catalog"))?,
                        ),
                    )
                    .await
                    .map_err(configuration_error)?;
                return Ok(public_admin(
                    &p.provider_id,
                    &base,
                    next.discovered.unwrap_or_default(),
                    next.models,
                    r.revision,
                ));
            }
            id(&p.provider_id)?;
            let record = manager
                .configuration
                .get(owner, "provider", &p.provider_id)
                .await
                .map_err(configuration_error)?
                .ok_or_else(|| WireError::new("not_found", "Provider not found"))?;
            if record.revision != p.expected_revision {
                return Err(WireError::new(
                    "revision_conflict",
                    "Provider changed before discovery",
                ));
            }
            let mut next = decode(&record)?;
            next.discovered =
                discover_models(transport(&next, None), p.provider_id.clone()).await?;
            validate(&next)?;
            let r = manager
                .configuration
                .compare_exchange(
                    owner,
                    "provider",
                    &p.provider_id,
                    p.expected_revision,
                    Some(
                        serde_json::to_value(&next)
                            .map_err(|_| invalid("Cannot encode discovered catalog"))?,
                    ),
                )
                .await
                .map_err(configuration_error)?;
            Ok(public(&r, &next))
        }
        _ => Err(invalid("Unknown Provider operation")),
    }
}
pub(super) async fn choices(manager: &Manager, owner: &str) -> Result<Vec<Value>, WireError> {
    let snapshot = manager.model_metadata.snapshot();
    let overrides = admin_override_map(manager, owner).await?;
    let mut choices = Vec::new();
    for (shared_id, base) in manager.config.shared_providers() {
        let ov = overrides
            .get(&shared_id)
            .map(|(_, ov)| ov.clone())
            .unwrap_or_default();
        let baseline = admin_baseline(&manager.config, &shared_id, &ov);
        for m in merge_overlays(&baseline, &ov.models)
            .into_iter()
            .filter(|m| !m.disabled)
        {
            let protocol = m
                .protocol
                .or_else(|| {
                    manager
                        .config
                        .shared_provider_route(&shared_id, &m.id)
                        .map(|p| p.protocol)
                })
                .unwrap_or(base.protocol);
            let choice = ModelChoice {
                id: m.id.clone(),
                name: m.name.unwrap_or(m.id),
                provider_id: shared_id.clone(),
                protocol,
                variants: admin_variants(m.variants),
                context_window_tokens: m.context_window_tokens,
            };
            choices.push(manager.model_metadata.enrich_choice(choice, &snapshot));
        }
    }
    for r in manager
        .configuration
        .list(owner, "provider")
        .await
        .map_err(configuration_error)?
    {
        let p = decode(&r)?;
        for m in merge_overlays(&p.discovered, &p.provider.models)
            .into_iter()
            .filter(|m| !m.disabled)
        {
            choices.push(json!(ModelChoice {
                id: m.id.clone(),
                name: m.name.unwrap_or(m.id),
                provider_id: r.target.clone(),
                protocol: m.protocol.unwrap_or(p.provider.protocol),
                variants: m.variants,
                context_window_tokens: m.context_window_tokens
            }));
        }
    }
    Ok(choices)
}

#[derive(Clone)]
pub(crate) struct RelayCandidate {
    pub selection: ModelSelection,
    pub resolved: ResolvedProviderSelection,
}

/// Enumerate all active model routes visible to `owner` under the server relay
/// policy and optional per-key `allowed_providers` scope. Strictly isolates
/// private `user:*` providers and per-owner shared overrides by `owner`.
pub(crate) async fn relay_candidates(
    manager: &Manager,
    owner: &str,
    key_allowed_providers: Option<&[String]>,
) -> Result<Vec<RelayCandidate>, WireError> {
    let config = &manager.config;
    if !config.relay.enabled {
        return Ok(Vec::new());
    }
    let provider_allowed = |provider_id: &str| {
        config.relay.allows_provider(provider_id)
            && key_allowed_providers.is_none_or(|list| list.iter().any(|item| item == provider_id))
    };
    let overrides = admin_override_map(manager, owner).await?;
    let mut out = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for (shared_id, _) in config.shared_providers() {
        if !provider_allowed(&shared_id) {
            continue;
        }
        let ov = overrides
            .get(&shared_id)
            .map(|(_, ov)| ov.clone())
            .unwrap_or_default();
        let baseline = admin_baseline(config, &shared_id, &ov);
        for m in merge_overlays(&baseline, &ov.models)
            .into_iter()
            .filter(|m| !m.disabled)
        {
            if !seen.insert((shared_id.clone(), m.id.clone())) {
                continue;
            }
            let Some(mut provider) = config.shared_provider_route(&shared_id, &m.id) else {
                continue;
            };
            provider.model = m.id.clone();
            if provider.validate_media_capability().is_err() {
                continue;
            }
            let transport_cfg = provider.client_transport_config();
            if config.relay.check_transport(config, &transport_cfg).is_err() {
                continue;
            }
            out.push(RelayCandidate {
                selection: ModelSelection {
                    provider_id: shared_id.clone(),
                    model: m.id,
                    variant: None,
                },
                resolved: ResolvedProviderSelection {
                    provider,
                    options: Default::default(),
                },
            });
        }
    }
    for r in manager
        .configuration
        .list(owner, "provider")
        .await
        .map_err(configuration_error)?
    {
        if !provider_allowed(&r.target) {
            continue;
        }
        let p = decode(&r)?;
        for m in merge_overlays(&p.discovered, &p.provider.models)
            .into_iter()
            .filter(|m| !m.disabled)
        {
            if !seen.insert((r.target.clone(), m.id.clone())) {
                continue;
            }
            let provider = transport(&p, Some(&m));
            let transport_cfg = provider.client_transport_config();
            if config.relay.check_transport(config, &transport_cfg).is_err() {
                continue;
            }
            out.push(RelayCandidate {
                selection: ModelSelection {
                    provider_id: r.target.clone(),
                    model: m.id,
                    variant: None,
                },
                resolved: ResolvedProviderSelection {
                    provider,
                    options: Default::default(),
                },
            });
        }
    }
    Ok(out)
}

fn validate_selected_model(
    provider: Provider,
    model_id: &str,
    allowed_variants: &[String],
    variant: Option<&str>,
    revision: u64,
    context_window_tokens: Option<u64>,
) -> Result<(ResolvedProviderSelection, u64, Option<u64>), WireError> {
    let mut provider = provider;
    provider
        .validate_media_capability()
        .map_err(|e| WireError::new("model_unavailable", e.to_string()))?;
    let variant = variant.filter(|v| *v != "default" && !v.is_empty());
    if variant.is_some_and(|v| !allowed_variants.iter().any(|allowed| allowed == v)) {
        return Err(invalid("Variant is not in this model catalog"));
    }
    if variant.is_some()
        && !matches!(
            provider.protocol,
            Protocol::OpenaiCompatible | Protocol::OpenaiResponses
        )
    {
        return Err(invalid(
            "Reasoning variant requires openai_compatible or openai_responses protocol",
        ));
    }
    provider.model = model_id.to_owned();
    let options = microcrab_transport::OpenAiCompatibleRequestOptions {
        reasoning_effort: variant.map(str::to_owned),
    };
    options
        .validate()
        .map_err(|_| invalid("Unsupported reasoning variant"))?;
    Ok((
        ResolvedProviderSelection { provider, options },
        revision,
        context_window_tokens,
    ))
}

pub(super) async fn resolve(
    manager: &Manager,
    owner: &str,
    selection: &ModelSelection,
) -> Result<(ResolvedProviderSelection, u64, Option<u64>), WireError> {
    if !selection.provider_id.starts_with("user:") {
        if shared_provider(&manager.config, &selection.provider_id).is_none() {
            return Err(WireError::new("model_unavailable", "model_not_found"));
        }
        let record = manager
            .configuration
            .get(owner, OVERRIDE_KIND, &selection.provider_id)
            .await
            .map_err(configuration_error)?;
        let revision = record.as_ref().map_or(0, |r| r.revision);
        let ov = record
            .as_ref()
            .and_then(|r| r.value.as_ref())
            .map(decode_override)
            .transpose()?
            .unwrap_or_default();
        let baseline = admin_baseline(&manager.config, &selection.provider_id, &ov);
        let m = merge_overlays(&baseline, &ov.models)
            .into_iter()
            .find(|m| m.id == selection.model && !m.disabled)
            .ok_or_else(|| {
                WireError::new("model_unavailable", "Model absent or disabled in catalog")
            })?;
        let provider = manager
            .config
            .shared_provider_route(&selection.provider_id, &selection.model)
            .ok_or_else(|| WireError::new("model_unavailable", "model_route_not_found"))?;
        return validate_selected_model(
            provider,
            &selection.model,
            &admin_variants(m.variants),
            selection.variant.as_deref(),
            revision,
            m.context_window_tokens,
        );
    }
    let r = manager
        .configuration
        .get(owner, "provider", &selection.provider_id)
        .await
        .map_err(configuration_error)?
        .ok_or_else(|| WireError::new("model_unavailable", "Owner Provider not found"))?;
    let p = decode(&r)?;
    let m = merge_overlays(&p.discovered, &p.provider.models)
        .into_iter()
        .find(|m| m.id == selection.model && !m.disabled)
        .ok_or_else(|| {
            WireError::new(
                "model_unavailable",
                "Model absent or disabled in owner catalog",
            )
        })?;
    validate_selected_model(
        transport(&p, Some(&m)),
        &selection.model,
        &m.variants,
        selection.variant.as_deref(),
        r.revision,
        m.context_window_tokens,
    )
}
