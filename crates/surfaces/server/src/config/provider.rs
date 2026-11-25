//! Private provider catalog resolution happens once before application assembly.
//! Inputs and parser errors may contain credentials; public errors use static codes.
use super::{ConfigError, ModelChoice, Provider};
mod discovery;
use microcrab_transport::{AuthMode, Protocol};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
};

pub(crate) fn discover_models(
    provider: Provider,
    provider_id: &str,
) -> Result<Vec<ModelChoice>, ConfigError> {
    let protocol = provider.protocol;
    discovery::discover(provider).map(|models| {
        models
            .into_iter()
            .map(|(id, model)| ModelChoice {
                name: model.name.unwrap_or_else(|| id.clone()),
                id,
                provider_id: provider_id.into(),
                protocol: model.protocol.unwrap_or(protocol),
                variants: model.variants,
                context_window_tokens: model.context_window_tokens,
            })
            .collect()
    })
}

pub(super) const MAX_CONFIG_BYTES: usize = 1024 * 1024;

pub(super) fn read_private(path: &Path, stage: &'static str) -> Result<String, ConfigError> {
    let file = std::fs::File::open(path).map_err(|error| ConfigError::io_source(stage, &error))?;
    let mut text = String::new();
    file.take((MAX_CONFIG_BYTES + 1) as u64)
        .read_to_string(&mut text)
        .map_err(|error| ConfigError::io_source(stage, &error))?;
    if text.len() > MAX_CONFIG_BYTES {
        return Err(ConfigError::new(stage, "too_large"));
    }
    Ok(text)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    config_file: PathBuf,
    #[serde(default)]
    name: Option<String>,
}
#[derive(Deserialize)]
struct Catalog {
    #[serde(default)]
    provider: BTreeMap<String, toml::Value>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    protocol: Protocol,
    options: Options,
    #[serde(default)]
    models: BTreeMap<String, ModelMeta>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelMeta {
    #[serde(default)]
    google_function_response_media: bool,
    #[serde(default)]
    protocol: Option<Protocol>,
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    auth: Option<AuthMode>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    variants: Vec<String>,
    #[serde(default, deserialize_with = "super::deserialize_context_window_tokens")]
    context_window_tokens: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    #[serde(default)]
    auth: AuthMode,
    #[serde(default, rename = "baseURL")]
    base_url: Option<String>,
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default, rename = "apiKey")]
    api_key: Option<String>,
    #[serde(default = "super::timeout", rename = "timeoutMs")]
    timeout_ms: u64,
    #[serde(default = "super::response_budget", rename = "maxResponseBytes")]
    max_response_bytes: usize,
}

/// Immutable credential-bearing routing data; never serialize or debug-print it.
pub(crate) struct ResolvedCatalog {
    pub(super) default_provider_id: String,
    pub(super) providers: BTreeMap<String, Provider>,
    pub(super) routes: BTreeMap<(String, String), Provider>,
    pub(super) models: Vec<ModelChoice>,
    pub(super) warnings: Vec<(String, ConfigError)>,
}

impl std::fmt::Debug for ResolvedCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedCatalog")
            .field("provider_count", &self.providers.len())
            .finish_non_exhaustive()
    }
}

pub(super) fn resolve(
    value: &mut serde_json::Value,
    startup: &Path,
) -> Result<Option<(PathBuf, ResolvedCatalog)>, ConfigError> {
    let Some(provider_val) = value.get("provider") else {
        return Ok(None);
    };
    if provider_val.get("config_file").is_none() {
        return Ok(None);
    }
    let reference: Reference = serde_json::from_value(provider_val.clone()).map_err(|error| {
        ConfigError::with_cause(
            "provider_catalog",
            "invalid_reference",
            &microcrab_protocol::diagnostic::json_reason(&error),
        )
    })?;
    if reference.config_file.as_os_str().is_empty() {
        return Err(ConfigError::new("provider_catalog", "invalid_reference"));
    }
    let path = if reference.config_file.is_absolute() {
        reference.config_file
    } else {
        startup.join(reference.config_file)
    };
    let path = path
        .canonicalize()
        .map_err(|error| ConfigError::io_source("provider_catalog", &error))?;
    let text = read_private(&path, "provider_catalog")?;
    let catalog: Catalog = toml::from_str(&text).map_err(|error: toml::de::Error| {
        ConfigError::with_cause(
            "provider_catalog",
            "invalid_format",
            &format!("{}; byte_range={:?}", error.message(), error.span()),
        )
    })?;
    if catalog.provider.keys().any(|id| id.starts_with("user:")) {
        return Err(ConfigError::with_cause(
            "provider_catalog",
            "reserved_provider_namespace",
            "Shared startup Provider IDs must not start with user:; that namespace belongs to authenticated owner Providers",
        ));
    }
    let name = match reference.name {
        Some(name) if !name.is_empty() => name,
        None if catalog.provider.len() == 1 => catalog
            .provider
            .keys()
            .next()
            .cloned()
            .ok_or_else(|| ConfigError::new("provider_catalog", "empty"))?,
        None if catalog.provider.is_empty() => {
            return Err(ConfigError::new("provider_catalog", "empty"));
        }
        None => return Err(ConfigError::new("provider_catalog", "selection_required")),
        Some(_) => return Err(ConfigError::new("provider_catalog", "invalid_reference")),
    };
    let selected = catalog
        .provider
        .get(&name)
        .ok_or_else(|| ConfigError::new("provider_catalog", "provider_not_found"))?;
    if protocol(selected).is_none() {
        return Err(ConfigError::new("provider_catalog", "unsupported_protocol"));
    }
    let mut providers = BTreeMap::new();
    let mut routes = BTreeMap::new();
    let mut models = Vec::new();
    let mut warnings = Vec::new();
    for (provider_id, raw_entry) in catalog.provider {
        // Unknown future protocols remain unavailable; supported transports share this catalog.
        if protocol(&raw_entry).is_none() {
            continue;
        }
        let entry: Entry = raw_entry.try_into().map_err(|error: toml::de::Error| {
            ConfigError::with_cause(
                "provider_catalog",
                "invalid_format",
                &format!("{}; byte_range={:?}", error.message(), error.span()),
            )
        })?;
        if provider_id.trim().is_empty() {
            return Err(ConfigError::new("provider_catalog", "invalid_provider"));
        }
        let options = entry.options;
        let resolved_endpoint = endpoint_for_protocol(
            entry.protocol,
            options.endpoint.clone(),
            options.base_url.clone(),
        )?;
        let api_key = options
            .api_key
            .as_deref()
            .map(super::resolve_secret)
            .transpose()
            .map_err(|error| {
                ConfigError::with_cause(
                    "provider_catalog",
                    "invalid_secret_reference",
                    &microcrab_protocol::diagnostic::describe(&error, 2048),
                )
            })?;
        let discovery_provider = Provider {
            protocol: entry.protocol,
            google_function_response_media: false,
            auth: options.auth,
            endpoint: resolved_endpoint.clone(),
            // Discovery validates common transport options without inferring a model route.
            model: "discovery".into(),
            api_key: api_key.clone(),
            timeout_ms: options.timeout_ms,
            max_response_bytes: options.max_response_bytes,
        };
        discovery_provider
            .client_transport_config()
            .validate()
            .map_err(|error| {
                ConfigError::with_cause(
                    "provider_catalog",
                    "invalid_options",
                    &error.diagnostic_reason(),
                )
            })?;
        let mut merged = match discovery::discover(discovery_provider.clone()) {
            Ok(models) => models,
            Err(error) => {
                warnings.push((provider_id.clone(), error));
                BTreeMap::new()
            }
        };
        let mut order: Vec<String> = merged.keys().cloned().collect();
        let mut configured = std::collections::BTreeSet::new();
        for (key, mut meta) in entry.models {
            let id = meta.id.take().unwrap_or_else(|| key.clone());
            if id.trim().is_empty() || !configured.insert(id.clone()) {
                return Err(ConfigError::new("provider_catalog", "invalid_models"));
            }
            if let Some(discovered) = merged.get_mut(&id) {
                discovered.google_function_response_media = meta.google_function_response_media;
                discovered.protocol = meta.protocol;
                discovered.endpoint = meta.endpoint;
                discovered.auth = meta.auth;
                if meta.name.is_some() {
                    discovered.name = meta.name;
                }
                if !meta.variants.is_empty() {
                    discovered.variants = meta.variants;
                }
                if meta.context_window_tokens.is_some() {
                    discovered.context_window_tokens = meta.context_window_tokens;
                }
            } else {
                meta.name = meta.name.or(Some(key));
                order.push(id.clone());
                merged.insert(id, meta);
            }
        }
        if let Some(model) = &options.model {
            if model.trim().is_empty() {
                return Err(ConfigError::new("provider_catalog", "missing_model"));
            }
            if !merged.contains_key(model) {
                order.push(model.clone());
                merged.insert(
                    model.clone(),
                    ModelMeta {
                        google_function_response_media: false,
                        protocol: None,
                        endpoint: None,
                        auth: None,
                        id: Some(model.clone()),
                        name: None,
                        variants: vec![],
                        context_window_tokens: None,
                    },
                );
            }
        }
        if order.is_empty() && !warnings.iter().any(|(id, _)| id == &provider_id) {
            warnings.push((provider_id.clone(), ConfigError::with_cause(
                "provider_catalog", "models_empty",
                "gateway discovery and configured models provide no selectable model; provider remains unavailable until models are configured or discovered",
            )));
        }
        let start = models.len();
        for id in order {
            let meta = merged
                .remove(&id)
                .expect("model order contains each merged identity once");
            let model_failure = |code, cause: &str| {
                ConfigError::with_cause(
                    "provider_catalog",
                    code,
                    &format!(
                        "provider={:?}, model={:?}: {cause}",
                        microcrab_protocol::diagnostic::sanitize(&provider_id, 128),
                        microcrab_protocol::diagnostic::sanitize(&id, 128)
                    ),
                )
            };
            let effective_protocol = meta.protocol.unwrap_or(entry.protocol);
            let effective_endpoint = match meta.endpoint {
                Some(endpoint) => endpoint,
                None if effective_protocol == entry.protocol => resolved_endpoint.clone(),
                None => {
                    let base = options.base_url.clone().ok_or_else(|| {
                        model_failure("model_endpoint_required",
                            "a model using a different protocol from a fixed provider endpoint requires an explicit model endpoint")
                    })?;
                    if (effective_protocol == Protocol::Google)
                        != (entry.protocol == Protocol::Google)
                    {
                        return Err(model_failure(
                            "model_endpoint_required",
                            "routing between Google and another protocol requires an explicit model endpoint; protocol base versions cannot be inferred",
                        ));
                    }
                    endpoint_for_protocol(effective_protocol, None, Some(base))
                        .map_err(|error| model_failure(error.code, &error.cause))?
                }
            };
            let route = Provider {
                protocol: effective_protocol,
                google_function_response_media: meta.google_function_response_media,
                auth: meta.auth.unwrap_or(options.auth),
                endpoint: effective_endpoint,
                model: id.clone(),
                // Explicit model endpoints deliberately inherit provider credentials.
                api_key: api_key.clone(),
                timeout_ms: options.timeout_ms,
                max_response_bytes: options.max_response_bytes,
            };
            route
                .validate_media_capability()
                .map_err(|error| model_failure(error.code, &error.cause))?;
            microcrab_transport::TransportClient::new(route.client_transport_config()).map_err(
                |error| model_failure("invalid_model_route", &error.diagnostic_reason()),
            )?;
            routes.insert((provider_id.clone(), id.clone()), route);
            let mut variants = vec!["default".to_string()];
            for variant in meta.variants {
                if variant == "default" {
                    continue;
                }
                if !matches!(
                    effective_protocol,
                    Protocol::OpenaiCompatible | Protocol::OpenaiResponses
                ) {
                    return Err(model_failure(
                        "invalid_variants",
                        "explicit reasoning-effort variants require the effective openai-compatible or openai-responses protocol",
                    ));
                }
                let options = microcrab_transport::OpenAiCompatibleRequestOptions {
                    reasoning_effort: Some(variant.clone()),
                };
                options.validate().map_err(|error| {
                    model_failure(
                        "invalid_variants",
                        &microcrab_protocol::diagnostic::describe(&error, 2048),
                    )
                })?;
                if variants.contains(&variant) {
                    return Err(model_failure(
                        "invalid_variants",
                        "duplicate configured reasoning-effort variant",
                    ));
                }
                variants.push(variant);
            }
            models.push(ModelChoice {
                id: id.clone(),
                name: meta.name.unwrap_or_else(|| id.clone()),
                provider_id: provider_id.clone(),
                protocol: effective_protocol,
                variants,
                context_window_tokens: meta.context_window_tokens,
            });
        }
        let model = options.model.unwrap_or_else(|| {
            models
                .get(start)
                .map(|choice| choice.id.clone())
                .unwrap_or_default()
        });
        let resolved = if model.is_empty() {
            Provider {
                model,
                ..discovery_provider
            }
        } else {
            routes
                .get(&(provider_id.clone(), model))
                .cloned()
                .expect("a configured or discovered default has a validated model route")
        };
        providers.insert(provider_id, resolved);
    }
    let resolved = providers
        .get(&name)
        .ok_or_else(|| ConfigError::new("provider_catalog", "provider_not_found"))?;
    value["provider"] = serde_json::json!({"protocol":resolved.protocol,"auth":resolved.auth,"endpoint":resolved.endpoint,"model":resolved.model,
        "api_key":resolved.api_key,"timeout_ms":resolved.timeout_ms,"max_response_bytes":resolved.max_response_bytes});
    if resolved.google_function_response_media {
        value["provider"]["google_function_response_media"] = serde_json::json!(true);
    }
    value["available_models"] = serde_json::to_value(&models).map_err(|error| {
        ConfigError::with_cause(
            "provider_catalog",
            "invalid_models",
            &microcrab_protocol::diagnostic::json_reason(&error),
        )
    })?;
    Ok(Some((
        path,
        ResolvedCatalog {
            default_provider_id: name,
            providers,
            routes,
            models,
            warnings,
        },
    )))
}

fn protocol(entry: &toml::Value) -> Option<Protocol> {
    entry.get("protocol")?.as_str()?.parse().ok()
}

#[cfg(test)]
fn endpoint(explicit: Option<String>, base: Option<String>) -> Result<String, ConfigError> {
    endpoint_for_protocol(Protocol::OpenaiCompatible, explicit, base)
}

fn endpoint_for_protocol(
    protocol: Protocol,
    explicit: Option<String>,
    base: Option<String>,
) -> Result<String, ConfigError> {
    match (explicit, base) {
        (Some(endpoint), None) => Ok(endpoint),
        (None, Some(base)) => {
            let mut url = url::Url::parse(&base).map_err(|error| {
                ConfigError::with_cause(
                    "provider_catalog",
                    "invalid_endpoint",
                    &microcrab_protocol::diagnostic::describe(&error, 2048),
                )
            })?;
            if url.query().is_some()
                || url.fragment().is_some()
                || !url.username().is_empty()
                || url.password().is_some()
            {
                return Err(ConfigError::new("provider_catalog", "invalid_endpoint"));
            }
            let path = url.path().trim_end_matches('/');
            let known_suffix = ["/chat/completions", "/responses", "/messages"]
                .into_iter()
                .find(|suffix| path.ends_with(*suffix));
            let google_method = path.ends_with(":generateContent")
                || path.ends_with(":streamGenerateContent")
                || path.contains("/models/");
            if (protocol == Protocol::Google && known_suffix.is_some())
                || (protocol != Protocol::Google && google_method)
            {
                return Err(ConfigError::with_cause(
                    "provider_catalog",
                    "model_endpoint_required",
                    "baseURL ends in another protocol family's endpoint; configure an explicit endpoint instead of inferring a version",
                ));
            }
            let suffix = match protocol {
                Protocol::OpenaiCompatible => "/chat/completions",
                Protocol::OpenaiResponses => "/responses",
                Protocol::Anthropic => "/messages",
                // Google resolves /models/{model}:action at request time.
                Protocol::Google => return Ok(url.into()),
            };
            // A known endpoint is a suffix, never a base to append another suffix to.
            let path = format!(
                "{}{suffix}",
                known_suffix
                    .and_then(|suffix| path.strip_suffix(suffix))
                    .unwrap_or(path)
            );
            url.set_path(&path);
            Ok(url.into())
        }
        _ => Err(ConfigError::new(
            "provider_catalog",
            "choose_endpoint_or_base_url",
        )),
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod namespace_tests {
    use super::*;
    #[test]
    fn startup_owner_namespace_is_rejected_before_protocol_or_discovery() {
        let root = std::env::temp_dir().join(format!(
            "microcrab-provider-namespace-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("providers.toml");
        // Even an unsupported protocol cannot hide an identity reserved for user
        // Providers. This must reject before constructing any discovery request.
        std::fs::write(
            &path,
            "[provider.\"user:collision\"]\nprotocol='future-unsupported'\n",
        )
        .unwrap();
        let mut configuration =
            serde_json::json!({"provider":{"config_file":path,"name":"user:collision"}});
        let error = match resolve(&mut configuration, &root) {
            Ok(_) => panic!("reserved Provider namespace accepted"),
            Err(error) => error,
        };
        assert_eq!(error.code, "reserved_provider_namespace");
        assert!(error.cause.contains("must not start with user:"));
    }
}
