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
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UserProvider {
    pub name: String,
    pub protocol: Protocol,
    pub endpoint: String,
    pub auth: AuthMode,
    pub timeout_ms: u64,
    pub max_response_bytes: usize,
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
        return Err(WireError::new(
            "read_only",
            "Administrator Providers are read-only; user Provider ids start with user:",
        ));
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
fn merged(provider: &PrivateProvider) -> Vec<ModelOverlay> {
    let mut models: BTreeMap<String, ModelOverlay> = provider
        .discovered
        .iter()
        .cloned()
        .map(|m| (m.id.clone(), m))
        .collect();
    for m in &provider.provider.models {
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
    }
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
    if p.models.len() > 2048 || provider.discovered.len() > 2048 {
        return Err(invalid("Provider model catalog exceeds 2048 entries"));
    }
    let mut ids = std::collections::BTreeSet::new();
    for m in &p.models {
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
    for model in std::iter::once(None).chain(merged(provider).iter().map(Some)) {
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
pub(super) async fn command(
    manager: &Manager,
    owner: &str,
    method: &str,
    params: Value,
) -> Result<Value, WireError> {
    if matches!(
        method,
        "provider.set" | "provider.delete" | "provider.discover"
    ) {
        if let Some(id) = params.get("provider_id").and_then(Value::as_str) {
            if manager
                .config
                .shared_providers()
                .iter()
                .any(|(shared, _)| shared == id)
            {
                return Err(WireError::new(
                    "read_only",
                    "Shared administrator Provider identity is read-only and cannot be shadowed by an owner record",
                ));
            }
        }
    }
    match method {
        "provider.list" => {
            let _: super::commands::EmptySettings = serde_json::from_value(params)
                .map_err(|_| invalid("Provider list accepts no parameters"))?;
            let mut items = Vec::new();
            for (id, p) in manager.config.shared_providers() {
                items.push(json!({"id":id,"scope":"admin","revision":0,"read_only":true,"name":id,"protocol":p.protocol,"endpoint":p.endpoint,"auth":p.auth,"timeout_ms":p.timeout_ms,"max_response_bytes":p.max_response_bytes,"api_key_configured":p.api_key.is_some(),"models":manager.config.model_choices().into_iter().filter(|m|m.provider_id==id).map(|m|ModelOverlay{id:m.id,name:Some(m.name),variants:m.variants,context_window_tokens:m.context_window_tokens,protocol:Some(m.protocol),endpoint:None,auth:None,disabled:false}).collect::<Vec<_>>(),"discovered":[]}));
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
            if !p.provider_id.starts_with("user:") {
                let list = Box::pin(command(manager, owner, "provider.list", json!({}))).await?;
                return list["items"]
                    .as_array()
                    .and_then(|items| items.iter().find(|v| v["id"] == p.provider_id))
                    .cloned()
                    .ok_or_else(|| WireError::new("not_found", "Provider not found"));
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
            let route = transport(&next, None);
            let target = p.provider_id.clone();
            let models =
                tokio::task::spawn_blocking(move || Config::discover_provider(route, &target))
                    .await
                    .map_err(|_| {
                        WireError::new("discovery_failed", "Provider discovery worker failed")
                    })?
                    .map_err(|e| {
                        WireError::new("discovery_failed", format!("{}: {}", e.code, e.cause))
                    })?;
            next.discovered = models
                .into_iter()
                .map(|m| ModelOverlay {
                    id: m.id,
                    name: Some(m.name),
                    variants: m.variants,
                    context_window_tokens: m.context_window_tokens,
                    protocol: Some(m.protocol),
                    endpoint: None,
                    auth: None,
                    disabled: false,
                })
                .collect();
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
    let mut choices = manager.model_metadata.choices(&manager.config);
    for r in manager
        .configuration
        .list(owner, "provider")
        .await
        .map_err(configuration_error)?
    {
        let p = decode(&r)?;
        for m in merged(&p).into_iter().filter(|m| !m.disabled) {
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
pub(super) async fn resolve(
    manager: &Manager,
    owner: &str,
    selection: &ModelSelection,
) -> Result<(ResolvedProviderSelection, u64, Option<u64>), WireError> {
    if !selection.provider_id.starts_with("user:") {
        return manager
            .config
            .resolve_selection(
                &selection.provider_id,
                &selection.model,
                selection.variant.as_deref(),
            )
            .map(|r| (r, 0, None))
            .map_err(|e| WireError::new("model_unavailable", e.to_string()));
    }
    let r = manager
        .configuration
        .get(owner, "provider", &selection.provider_id)
        .await
        .map_err(configuration_error)?
        .ok_or_else(|| WireError::new("model_unavailable", "Owner Provider not found"))?;
    let p = decode(&r)?;
    let m = merged(&p)
        .into_iter()
        .find(|m| m.id == selection.model && !m.disabled)
        .ok_or_else(|| {
            WireError::new(
                "model_unavailable",
                "Model absent or disabled in owner catalog",
            )
        })?;
    let variant = selection
        .variant
        .as_deref()
        .filter(|v| *v != "default" && !v.is_empty());
    if variant.is_some_and(|v| !m.variants.iter().any(|allowed| allowed == v)) {
        return Err(invalid("Variant is not in this model catalog"));
    }
    let provider = transport(&p, Some(&m));
    if variant.is_some() && provider.protocol != Protocol::OpenaiCompatible {
        return Err(invalid(
            "Reasoning variant requires openai_compatible protocol",
        ));
    }
    let options = microcrab_transport::OpenAiCompatibleRequestOptions {
        reasoning_effort: variant.map(str::to_owned),
    };
    options
        .validate()
        .map_err(|_| invalid("Unsupported reasoning variant"))?;
    Ok((
        ResolvedProviderSelection { provider, options },
        r.revision,
        m.context_window_tokens,
    ))
}
