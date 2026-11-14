//! Bounded gateway model discovery; the parent owns fallback and catalog overlays.
//! Only safe diagnostics cross this boundary; bodies, credentials and URLs stay private.
use super::{ConfigError, ModelMeta, Provider};
use microcrab_transport::Protocol;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

const MAX_PAGES: usize = 16;
const MAX_MODELS: usize = 2048;
const MAX_TOKEN_BYTES: usize = 8192;
const MAX_DISCOVERY_BYTES: usize = 1024 * 1024;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Page {
    #[serde(default)]
    models: Vec<GoogleModel>,
    #[serde(default)]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoogleModel {
    name: String,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    supported_generation_methods: Vec<String>,
    #[serde(default)]
    input_token_limit: Option<std::num::NonZeroU64>,
}

#[derive(Deserialize)]
struct DataPage {
    data: Vec<ListedModel>,
    #[serde(default)]
    has_more: bool,
    #[serde(default)]
    last_id: Option<String>,
}

#[derive(Deserialize)]
struct ListedModel {
    id: String,
    #[serde(default)]
    display_name: Option<String>,
}

fn error(code: &'static str) -> ConfigError {
    ConfigError::new("provider_catalog", code)
}

fn models_url(base: &str, protocol: Protocol) -> Result<url::Url, ConfigError> {
    let mut url = url::Url::parse(base).map_err(|failure| {
        ConfigError::with_cause(
            "provider_catalog",
            "invalid_endpoint",
            &microcrab_protocol::diagnostic::describe(&failure, 2048),
        )
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path().contains(':')
        || (protocol == Protocol::Google && url.path().trim_end_matches('/').contains("/models/"))
    {
        return Err(error("invalid_endpoint"));
    }
    let path = url.path().trim_end_matches('/');
    // Strip only the exact request suffix belonging to this protocol. Model IDs
    // never participate in discovery routing or diagnostics.
    let suffix = match protocol {
        Protocol::OpenaiCompatible => "/chat/completions",
        Protocol::OpenaiResponses => "/responses",
        Protocol::Anthropic => "/messages",
        Protocol::Google => "",
    };
    let path = if suffix.is_empty() {
        path
    } else {
        path.strip_suffix(suffix).unwrap_or(path)
    };
    let path = if path.ends_with("/models") {
        path.to_owned()
    } else {
        format!("{path}/models")
    };
    url.set_path(&path);
    Ok(url)
}

fn parse_page(bytes: &[u8]) -> Result<(BTreeMap<String, ModelMeta>, Option<String>), ConfigError> {
    let page: Page = serde_json::from_slice(bytes).map_err(|failure| {
        ConfigError::with_cause(
            "provider_catalog",
            "discovery_invalid_format",
            &microcrab_protocol::diagnostic::json_reason(&failure),
        )
    })?;
    let mut models = BTreeMap::new();
    for model in page.models {
        if !model
            .supported_generation_methods
            .iter()
            .any(|method| method == "generateContent")
        {
            continue;
        }
        let id = model.name.strip_prefix("models/").unwrap_or(&model.name);
        if id.is_empty()
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            || models.contains_key(id)
        {
            return Err(error("discovery_invalid_models"));
        }
        let id = id.to_owned();
        models.insert(
            id.clone(),
            ModelMeta {
                protocol: None,
                endpoint: None,
                auth: None,
                id: Some(id.clone()),
                name: model
                    .display_name
                    .filter(|name| !name.trim().is_empty())
                    .or(Some(id)),
                variants: vec![],
                context_window_tokens: model.input_token_limit.map(std::num::NonZeroU64::get),
            },
        );
    }
    let token = page.next_page_token.filter(|token| !token.is_empty());
    if token
        .as_ref()
        .is_some_and(|token| token.len() > MAX_TOKEN_BYTES)
    {
        return Err(error("discovery_limit"));
    }
    Ok((models, token))
}

fn parse_data_page(
    bytes: &[u8],
    protocol: Protocol,
) -> Result<(BTreeMap<String, ModelMeta>, Option<String>), ConfigError> {
    let page: DataPage = serde_json::from_slice(bytes).map_err(|failure| {
        ConfigError::with_cause(
            "provider_catalog",
            "discovery_invalid_format",
            &microcrab_protocol::diagnostic::json_reason(&failure),
        )
    })?;
    if page.data.len() > MAX_MODELS {
        return Err(error("discovery_limit"));
    }
    let mut models = BTreeMap::new();
    for model in page.data {
        if model.id.trim().is_empty()
            || model.id.len() > MAX_TOKEN_BYTES
            || model.id.chars().any(char::is_control)
            || models.contains_key(&model.id)
        {
            return Err(error("discovery_invalid_models"));
        }
        models.insert(
            model.id.clone(),
            ModelMeta {
                protocol: None,
                endpoint: None,
                auth: None,
                id: Some(model.id.clone()),
                name: model
                    .display_name
                    .filter(|name| !name.trim().is_empty())
                    .or(Some(model.id)),
                variants: vec![],
                context_window_tokens: None,
            },
        );
    }
    let next = if protocol == Protocol::Anthropic && page.has_more {
        Some(page.last_id.filter(|id| !id.is_empty()).ok_or_else(|| {
            ConfigError::with_cause(
                "provider_catalog",
                "discovery_invalid_format",
                "Anthropic has_more requires a nonempty last_id",
            )
        })?)
    } else {
        None
    };
    if next
        .as_ref()
        .is_some_and(|token| token.len() > MAX_TOKEN_BYTES)
    {
        return Err(error("discovery_limit"));
    }
    Ok((models, next))
}

fn request_error(failure: reqwest::Error) -> ConfigError {
    // Error Display/source strings may contain URLs, credentials or response
    // data. Preserve the known transport category and concrete I/O kind only.
    let code = if failure.is_timeout() {
        "discovery_timeout"
    } else {
        "discovery_unavailable"
    };
    let category = if failure.is_timeout() {
        "model discovery request timed out"
    } else if failure.is_connect() {
        "model discovery connection failed"
    } else if failure.is_builder() {
        "model discovery request construction failed (check authentication header)"
    } else if failure.is_body() || failure.is_decode() {
        "model discovery response body read failed"
    } else {
        "model discovery HTTP request failed"
    };
    let mut cause = std::error::Error::source(&failure);
    while let Some(source) = cause {
        if let Some(io) = source.downcast_ref::<std::io::Error>() {
            return ConfigError::with_cause(
                "provider_catalog",
                code,
                &format!("{category}: I/O {:?}", io.kind()),
            );
        }
        cause = source.source();
    }
    ConfigError::with_cause("provider_catalog", code, category)
}

pub(super) fn discover(provider: Provider) -> Result<BTreeMap<String, ModelMeta>, ConfigError> {
    // Config::read is synchronous and may itself run inside Tokio. A dedicated
    // startup thread owns its runtime, avoiding nested block_on on a caller runtime.
    let worker = std::thread::Builder::new()
        .name("model-discovery".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|failure| {
                    ConfigError::with_cause(
                        "provider_catalog",
                        "discovery_unavailable",
                        &microcrab_protocol::diagnostic::describe(&failure, 2048),
                    )
                })?;
            runtime.block_on(async move {
                tokio::time::timeout(
                    std::time::Duration::from_millis(provider.timeout_ms),
                    fetch(provider),
                )
                .await
                .map_err(|_| error("discovery_timeout"))?
            })
        })
        .map_err(|failure| {
            ConfigError::with_cause(
                "provider_catalog",
                "discovery_unavailable",
                &microcrab_protocol::diagnostic::describe(&failure, 2048),
            )
        })?;
    worker.join().map_err(|_| {
        ConfigError::with_cause(
            "provider_catalog",
            "discovery_unavailable",
            "model discovery worker panicked; payload withheld because it may contain private data",
        )
    })?
}

async fn fetch(provider: Provider) -> Result<BTreeMap<String, ModelMeta>, ConfigError> {
    provider
        .client_transport_config()
        .validate()
        .map_err(|failure| {
            ConfigError::with_cause(
                "provider_catalog",
                "invalid_options",
                &microcrab_protocol::diagnostic::describe(&failure, 2048),
            )
        })?;
    let base = models_url(&provider.endpoint, provider.protocol)?;
    let client = microcrab_transport::http::client_builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_millis(provider.timeout_ms))
        .build()
        .map_err(request_error)?;
    let mut models = BTreeMap::new();
    let mut token: Option<String> = None;
    let mut seen = BTreeSet::new();
    let mut remaining = provider.max_response_bytes.min(MAX_DISCOVERY_BYTES);
    for _ in 0..MAX_PAGES {
        let mut url = base.clone();
        match provider.protocol {
            Protocol::Google => {
                url.query_pairs_mut().append_pair("pageSize", "1000");
                if let Some(token) = &token {
                    url.query_pairs_mut().append_pair("pageToken", token);
                }
            }
            Protocol::Anthropic => {
                url.query_pairs_mut().append_pair("limit", "1000");
                if let Some(token) = &token {
                    url.query_pairs_mut().append_pair("after_id", token);
                }
            }
            Protocol::OpenaiCompatible | Protocol::OpenaiResponses => {}
        }
        let request = provider.auth.apply(
            client.get(url),
            provider.protocol,
            provider.api_key.as_deref(),
        );
        let mut response = request.send().await.map_err(request_error)?;
        if !response.status().is_success() {
            return Err(ConfigError::with_cause(
                "provider_catalog",
                "discovery_status",
                &format!(
                    "{} model discovery rejected with HTTP {}",
                    provider.protocol.as_str(),
                    response.status().as_u16()
                ),
            ));
        }
        if response
            .content_length()
            .is_some_and(|length| length > remaining as u64)
        {
            return Err(error("discovery_limit"));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(request_error)? {
            if chunk.len() > remaining {
                return Err(error("discovery_limit"));
            }
            remaining -= chunk.len();
            bytes.extend_from_slice(&chunk);
        }
        let (page, next) = match provider.protocol {
            Protocol::Google => parse_page(&bytes)?,
            protocol => parse_data_page(&bytes, protocol)?,
        };
        for (id, model) in page {
            if models.insert(id, model).is_some() || models.len() > MAX_MODELS {
                return Err(error("discovery_limit"));
            }
        }
        match next {
            None => return Ok(models),
            Some(next) if !seen.insert(next.clone()) => return Err(error("discovery_limit")),
            Some(next) => token = Some(next),
        }
    }
    Err(error("discovery_limit"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn startup_discovery_paginates_with_header_auth_and_native_model_filtering() {
        use axum::{
            Json, Router,
            extract::{Query, State},
            http::HeaderMap,
            routing::get,
        };
        use std::sync::{Arc, Mutex};
        #[derive(Default)]
        struct RequestLog {
            rows: Mutex<Vec<(String, BTreeMap<String, String>)>>,
        }
        async fn page(
            State(requests): State<Arc<RequestLog>>,
            Query(query): Query<BTreeMap<String, String>>,
            headers: HeaderMap,
        ) -> Json<serde_json::Value> {
            let key = headers
                .get("x-goog-api-key")
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned();
            requests.rows.lock().unwrap().push((key, query.clone()));
            if !query.contains_key("pageToken") {
                Json(
                    json!({"models":[{"name":"models/gemini-first","supportedGenerationMethods":["generateContent"]},{"name":"models/embedding","supportedGenerationMethods":["embedContent"]}],"nextPageToken":"second-page"}),
                )
            } else {
                assert_eq!(query["pageToken"], "second-page");
                Json(
                    json!({"models":[{"name":"models/gemini-second","displayName":"Second","supportedGenerationMethods":["generateContent"],"inputTokenLimit":32768}]}),
                )
            }
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/v1beta", listener.local_addr().unwrap());
        let requests = Arc::new(RequestLog::default());
        let app = Router::new()
            .route("/v1beta/models", get(page))
            .with_state(requests.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let provider = Provider {
            protocol: microcrab_transport::Protocol::Google,
            auth: microcrab_transport::AuthMode::Native,
            endpoint: base.clone(),
            model: "discovery".into(),
            api_key: Some("synthetic-private-key".into()),
            timeout_ms: 2000,
            max_response_bytes: 16384,
        };
        let models = tokio::task::spawn_blocking(move || discover(provider))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(models.len(), 2);
        assert_eq!(models["gemini-second"].context_window_tokens, Some(32768));
        {
            let requests = requests.rows.lock().unwrap();
            assert_eq!(requests.len(), 2);
            for (key, query) in requests.iter() {
                assert_eq!(key, "synthetic-private-key");
                assert_eq!(query["pageSize"], "1000");
                assert!(!query.contains_key("key"));
                assert!(!query.contains_key("apiKey"));
            }
        }
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn discovery_rejects_status_redirect_oversize_and_repeated_pagination_safely() {
        use axum::{Router, response::IntoResponse, routing::get};
        for scenario in ["status", "redirect", "oversize", "repeat", "malformed"] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/v1beta", listener.local_addr().unwrap());
            let app = Router::new().route(
                "/v1beta/models",
                get(move || async move {
                    match scenario {
                        "status" => (
                            axum::http::StatusCode::UNAUTHORIZED,
                            "synthetic-private-error",
                        )
                            .into_response(),
                        "redirect" => axum::response::Redirect::temporary(
                            "https://provider.invalid/never-follow",
                        )
                        .into_response(),
                        "oversize" => "x".repeat(129).into_response(),
                        "repeat" => {
                            axum::Json(json!({"models":[],"nextPageToken":"same"})).into_response()
                        }
                        _ => "{synthetic-private-malformed".into_response(),
                    }
                }),
            );
            let server = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let provider = Provider {
                protocol: microcrab_transport::Protocol::Google,
                auth: microcrab_transport::AuthMode::Native,
                endpoint,
                model: "discovery".into(),
                api_key: Some("synthetic-private-key".into()),
                timeout_ms: 2000,
                max_response_bytes: if scenario == "oversize" { 128 } else { 16384 },
            };
            let failure = tokio::task::spawn_blocking(move || discover(provider).err().unwrap())
                .await
                .unwrap();
            assert_eq!(
                failure.code,
                match scenario {
                    "status" | "redirect" => "discovery_status",
                    "malformed" => "discovery_invalid_format",
                    _ => "discovery_limit",
                }
            );
            assert!(!format!("{failure:?}").contains("synthetic-private"));
            server.abort();
            let _ = server.await;
        }
    }

    #[tokio::test]
    async fn discovery_enforces_one_total_deadline_across_pages() {
        use axum::{Json, Router, extract::Query, routing::get};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1beta", listener.local_addr().unwrap());
        let app = Router::new().route("/v1beta/models", get(|Query(query): Query<BTreeMap<String, String>>| async move {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            Json(json!({"models":[],"nextPageToken":format!("next-{}", query.get("pageToken").map(String::as_str).unwrap_or("first"))}))
        }));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let provider = Provider {
            protocol: microcrab_transport::Protocol::Google,
            auth: microcrab_transport::AuthMode::Native,
            endpoint,
            model: "discovery".into(),
            api_key: None,
            timeout_ms: 250,
            max_response_bytes: 16384,
        };
        let failure = tokio::task::spawn_blocking(move || discover(provider).err().unwrap())
            .await
            .unwrap();
        assert_eq!(failure.code, "discovery_timeout");
        server.abort();
        let _ = server.await;
    }

    fn provider(protocol: Protocol, endpoint: String) -> Provider {
        Provider {
            protocol,
            auth: microcrab_transport::AuthMode::Native,
            endpoint,
            // This routing identity must never be appended to the discovery URL.
            model: "private/model:encoded%2Fidentity".into(),
            api_key: Some("synthetic-private-key".into()),
            timeout_ms: 2000,
            max_response_bytes: 16384,
        }
    }

    #[tokio::test]
    async fn openai_discovery_uses_bearer_and_strips_only_the_actual_request_suffix() {
        use axum::{Json, Router, extract::OriginalUri, http::HeaderMap, routing::get};
        for (protocol, suffix) in [
            (Protocol::OpenaiCompatible, "/chat/completions"),
            (Protocol::OpenaiResponses, "/responses"),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!(
                "http://{}/gateway/v1{suffix}",
                listener.local_addr().unwrap()
            );
            let app = Router::new().route("/gateway/v1/models", get(
                |OriginalUri(uri): OriginalUri, headers: HeaderMap| async move {
                    assert_eq!(uri.path(), "/gateway/v1/models");
                    assert!(uri.query().is_none());
                    assert_eq!(headers["authorization"], "Bearer synthetic-private-key");
                    assert!(!headers.contains_key("x-api-key"));
                    Json(json!({"data":[{"id":"vendor/model-one","object":"model"},{"id":"model-two"}]}))
                },
            ));
            let server = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let models =
                tokio::task::spawn_blocking(move || discover(provider(protocol, endpoint)))
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(models.len(), 2);
            assert_eq!(
                models["vendor/model-one"].id.as_deref(),
                Some("vendor/model-one")
            );
            server.abort();
            let _ = server.await;
        }
    }

    #[tokio::test]
    async fn native_protocol_discovery_can_use_bearer_without_native_key_headers() {
        use axum::{Json, Router, http::HeaderMap, routing::get};
        for protocol in [Protocol::Google, Protocol::Anthropic] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
            let app = Router::new().route("/v1/models", get(move |headers: HeaderMap| async move {
                assert_eq!(headers["authorization"], "Bearer synthetic-private-key");
                assert!(!headers.contains_key("x-api-key"));
                assert!(!headers.contains_key("x-goog-api-key"));
                if protocol == Protocol::Anthropic {
                    assert_eq!(headers["anthropic-version"], "2023-06-01");
                    assert_eq!(headers.get_all("anthropic-version").iter().count(), 1);
                }
                Json(if protocol == Protocol::Google {
                    json!({"models":[{"name":"models/gemini","supportedGenerationMethods":["generateContent"]}]})
                } else { json!({"data":[{"id":"claude"}]}) })
            }));
            let server = tokio::spawn(async move {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async move {
                    axum::serve(listener, app).await.unwrap();
                })
                .await;
            });
            let mut configured = provider(protocol, endpoint);
            configured.auth = microcrab_transport::AuthMode::Bearer;
            let models = tokio::task::spawn_blocking(move || discover(configured))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(models.len(), 1);
            server.abort();
            let _ = server.await;
        }
    }

    #[tokio::test]
    async fn anthropic_discovery_paginates_with_version_and_key_headers() {
        use axum::{Json, Router, extract::Query, http::HeaderMap, routing::get};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/gateway/v1/messages",
            listener.local_addr().unwrap()
        );
        let app = Router::new().route("/gateway/v1/models", get(
            |Query(query): Query<BTreeMap<String, String>>, headers: HeaderMap| async move {
                assert_eq!(headers["x-api-key"], "synthetic-private-key");
                assert_eq!(headers["anthropic-version"], "2023-06-01");
                assert_eq!(headers.get_all("anthropic-version").iter().count(), 1);
                assert!(!headers.contains_key("authorization"));
                assert_eq!(query["limit"], "1000");
                assert!(!query.contains_key("key"));
                if let Some(after) = query.get("after_id") {
                    assert_eq!(after, "page/one+cursor");
                    Json(json!({"data":[{"id":"claude-second","display_name":"Second"}],"has_more":false,"last_id":"claude-second"}))
                } else {
                    Json(json!({"data":[{"id":"claude-first"}],"has_more":true,"last_id":"page/one+cursor"}))
                }
            },
        ));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let models =
            tokio::task::spawn_blocking(move || discover(provider(Protocol::Anthropic, endpoint)))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(models.len(), 2);
        assert_eq!(models["claude-second"].name.as_deref(), Some("Second"));
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn successful_empty_catalogs_are_not_discovery_errors() {
        use axum::{Json, Router, routing::get};
        for protocol in [
            Protocol::OpenaiCompatible,
            Protocol::OpenaiResponses,
            Protocol::Anthropic,
            Protocol::Google,
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/v1/models", listener.local_addr().unwrap());
            let app = Router::new().route("/v1/models", get(move || async move {
                Json(if protocol == Protocol::Google {
                    json!({"models":[{"name":"models/embedding","supportedGenerationMethods":["embedContent"]}]})
                } else {
                    json!({"data":[],"has_more":false})
                })
            }));
            let server = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let models =
                tokio::task::spawn_blocking(move || discover(provider(protocol, endpoint)))
                    .await
                    .unwrap()
                    .unwrap();
            assert!(models.is_empty());
            server.abort();
            let _ = server.await;
        }
    }

    #[tokio::test]
    async fn anthropic_pagination_rejects_missing_repeated_and_oversized_cursors_and_page_overflow()
    {
        use axum::{Json, Router, extract::Query, routing::get};
        for scenario in ["missing", "repeat", "token", "pages"] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/v1/messages", listener.local_addr().unwrap());
            let app = Router::new().route(
                "/v1/models",
                get(
                    move |Query(query): Query<BTreeMap<String, String>>| async move {
                        let last = match scenario {
                            "missing" => None,
                            "repeat" => Some("same".to_owned()),
                            "token" => Some("x".repeat(MAX_TOKEN_BYTES + 1)),
                            _ => Some(format!(
                                "next-{}",
                                query.get("after_id").map(String::as_str).unwrap_or("first")
                            )),
                        };
                        Json(json!({"data":[],"has_more":true,"last_id":last}))
                    },
                ),
            );
            let server = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let failure = tokio::task::spawn_blocking(move || {
                discover(provider(Protocol::Anthropic, endpoint))
                    .err()
                    .unwrap()
            })
            .await
            .unwrap();
            assert_eq!(
                failure.code,
                if scenario == "missing" {
                    "discovery_invalid_format"
                } else {
                    "discovery_limit"
                }
            );
            assert!(!format!("{failure:?}").contains("synthetic-private"));
            server.abort();
            let _ = server.await;
        }
    }

    #[test]
    fn data_catalog_rejects_model_overflow_and_malformed_shape_without_body_details() {
        let entries = (0..=MAX_MODELS)
            .map(|index| json!({"id":format!("model-{index}")}))
            .collect::<Vec<_>>();
        let bytes = serde_json::to_vec(&json!({"data":entries})).unwrap();
        let failure = parse_data_page(&bytes, Protocol::OpenaiCompatible)
            .err()
            .unwrap();
        assert_eq!(failure.code, "discovery_limit");
        for bytes in [
            b"{\"data\":\"synthetic-private\"}".as_slice(),
            b"{\"data\":[{}]}",
        ] {
            let failure = parse_data_page(bytes, Protocol::Anthropic).err().unwrap();
            assert_eq!(failure.code, "discovery_invalid_format");
            assert!(!format!("{failure:?}").contains("synthetic-private"));
        }
    }

    #[test]
    fn request_suffix_removal_preserves_the_origin_and_gateway_path() {
        for (protocol, endpoint, expected) in [
            (
                Protocol::OpenaiCompatible,
                "https://provider.invalid/gateway/v1/chat/completions",
                "https://provider.invalid/gateway/v1/models",
            ),
            (
                Protocol::OpenaiResponses,
                "https://provider.invalid/gateway/v1/responses/",
                "https://provider.invalid/gateway/v1/models",
            ),
            (
                Protocol::Anthropic,
                "https://provider.invalid/gateway/v1/messages",
                "https://provider.invalid/gateway/v1/models",
            ),
            (
                Protocol::OpenaiResponses,
                "https://provider.invalid/responses-proxy/v1/responses",
                "https://provider.invalid/responses-proxy/v1/models",
            ),
            (
                Protocol::OpenaiCompatible,
                "https://provider.invalid/tenant%20one/v1/chat/completions",
                "https://provider.invalid/tenant%20one/v1/models",
            ),
        ] {
            assert_eq!(models_url(endpoint, protocol).unwrap().as_str(), expected);
        }
        let failure = models_url(
            "https://provider.invalid/v1beta/models/private%3AgenerateContent",
            Protocol::Google,
        )
        .unwrap_err();
        assert_eq!(failure.code, "invalid_endpoint");
        assert!(!format!("{failure:?}").contains("private"));
    }

    #[test]
    fn google_discovery_url_preserves_base_and_rejects_secret_bearing_queries() {
        for base in [
            "https://provider.invalid/v1beta",
            "https://provider.invalid/v1beta/",
            "https://provider.invalid/v1beta/models",
        ] {
            assert_eq!(
                models_url(base, Protocol::Google).unwrap().as_str(),
                "https://provider.invalid/v1beta/models"
            );
        }
        for base in [
            "https://provider.invalid/v1beta?key=synthetic-private",
            "https://provider.invalid/models/test:generateContent",
            "https://user:synthetic-private@provider.invalid/v1beta",
        ] {
            let error = models_url(base, Protocol::Google).unwrap_err();
            assert_eq!(error.code, "invalid_endpoint");
            assert!(!format!("{error:?}").contains("synthetic-private"));
        }
    }
}
