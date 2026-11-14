use super::*;
use serde_json::json;

fn catalog(root: &Path, text: &str) -> PathBuf {
    let path = root.join("config.toml");
    std::fs::write(&path, text).unwrap();
    path
}
fn compatible() -> &'static str {
    "[provider.grok]\nprotocol='openai-compatible'\n[provider.grok.options]\napiKey='synthetic-literal-key'\nbaseURL='http://127.0.0.1:1/v1'\nmodel='grok'\ntimeoutMs=1234\n"
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_model_overrides_resolve_defaults_and_discovered_models_without_probing() {
    let gateway = catalog_gateway(vec![(
        "/gateway/v1/models",
        200,
        json!({"data":[{"id":"messages-wire","display_name":"Discovered"},{"id":"remote"}]})
            .to_string(),
    )])
    .await;
    let root = tempfile::tempdir().unwrap();
    catalog(
        root.path(),
        &format!(
            r#"
[provider.mixed]
protocol='openai-compatible'
[provider.mixed.options]
baseURL='{base}/gateway/v1/chat/completions'
apiKey='synthetic-private-inherited-key'
auth='bearer'
model='messages-wire'
timeoutMs=1000
[provider.mixed.models.a_messages]
id='messages-wire'
protocol='anthropic'
name='Configured messages'
[provider.mixed.models.b_responses]
id='responses-wire'
protocol='openai'
auth='native'
[provider.mixed.models.c_google]
id='gemini-wire'
protocol='google'
endpoint='{base}/native/v1beta'
[provider.mixed.models.d_chat]
id='chat-wire'
endpoint='{base}/custom-chat'
variants=['high']
"#,
            base = gateway.base
        ),
    );
    let config = read_catalog_config(root.path(), "mixed");
    // Discovery happens once on the provider default, never on the model endpoints.
    assert_catalog_gets(&gateway, &["/gateway/v1/models"]);
    assert_eq!(config.provider.model, "messages-wire");
    assert_eq!(config.provider.protocol, Protocol::Anthropic);
    assert_eq!(
        config.provider.endpoint,
        format!("{}/gateway/v1/messages", gateway.base)
    );
    assert_eq!(config.provider.auth, AuthMode::Bearer);
    let cases = [
        (
            "messages-wire",
            Protocol::Anthropic,
            "/gateway/v1/messages",
            AuthMode::Bearer,
        ),
        (
            "responses-wire",
            Protocol::OpenaiResponses,
            "/gateway/v1/responses",
            AuthMode::Native,
        ),
        (
            "gemini-wire",
            Protocol::Google,
            "/native/v1beta",
            AuthMode::Bearer,
        ),
        (
            "chat-wire",
            Protocol::OpenaiCompatible,
            "/custom-chat",
            AuthMode::Bearer,
        ),
        (
            "remote",
            Protocol::OpenaiCompatible,
            "/gateway/v1/chat/completions",
            AuthMode::Bearer,
        ),
    ];
    for (model, protocol, path, auth) in cases {
        let selected = config.resolve_selection("mixed", model, None).unwrap();
        assert_eq!(selected.provider.model, model);
        assert_eq!(selected.provider.protocol, protocol);
        assert_eq!(selected.provider.auth, auth);
        assert_eq!(
            selected.provider.endpoint,
            format!("{}{path}", gateway.base)
        );
        assert_eq!(
            selected.provider.api_key.as_deref(),
            Some("synthetic-private-inherited-key")
        );
        let choice = config
            .model_choices()
            .into_iter()
            .find(|choice| choice.id == model)
            .unwrap();
        assert_eq!(choice.protocol, protocol);
        let public = serde_json::to_value(choice).unwrap();
        assert_eq!(public["protocol"], protocol.as_str());
        assert!(public.get("endpoint").is_none());
        assert!(public.get("auth").is_none());
        assert!(!public.to_string().contains("synthetic-private"));
    }
    assert_eq!(
        config
            .resolve_selection("mixed", "chat-wire", Some("high"))
            .unwrap()
            .options
            .reasoning_effort
            .as_deref(),
        Some("high")
    );
    assert!(
        config
            .resolve_selection("mixed", "messages-wire", Some("high"))
            .is_err()
    );
    assert_eq!(
        config
            .model_choices()
            .iter()
            .find(|choice| choice.id == "messages-wire")
            .unwrap()
            .name,
        "Configured messages"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixed_endpoints_require_explicit_cross_protocol_routes_but_allow_same_protocol_overrides()
{
    let gateway = catalog_gateway(vec![("/v1/models", 503, "unavailable".into())]).await;
    let root = tempfile::tempdir().unwrap();
    let header = format!(
        "[provider.one]\nprotocol='openai-compatible'\n[provider.one.options]\nendpoint='{}/v1/chat/completions'\nmodel='wire'\ntimeoutMs=1000\n",
        gateway.base
    );
    let mut value = json!({"provider":{"config_file":"config.toml"}});
    catalog(
        root.path(),
        &format!("{header}[provider.one.models.alias]\nid='wire'\nprotocol='anthropic'\n"),
    );
    let error = resolve(&mut value, root.path()).unwrap_err();
    assert_eq!(error.code, "model_endpoint_required");
    assert!(error.cause.contains("fixed provider endpoint"));
    for protocol in ["anthropic", "openai-compatible"] {
        catalog(
            root.path(),
            &format!(
                "{header}[provider.one.models.alias]\nid='wire'\nprotocol='{protocol}'\nendpoint='{}/custom'\n",
                gateway.base
            ),
        );
        let config = read_catalog_config(root.path(), "one");
        assert_eq!(config.provider.endpoint, format!("{}/custom", gateway.base));
        assert_eq!(config.provider.protocol.as_str(), protocol);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn google_cross_family_routes_require_explicit_endpoints_and_validate_every_real_model() {
    let gateway = catalog_gateway(vec![("/v1/models", 200, json!({"data":[]}).to_string())]).await;
    let root = tempfile::tempdir().unwrap();
    let header = format!(
        "[provider.one]\nprotocol='openai-compatible'\n[provider.one.options]\nbaseURL='{}/v1'\nmodel='default-good'\ntimeoutMs=1000\n",
        gateway.base
    );
    for (model, endpoint, expected, cause) in [
        (
            "gemini",
            String::new(),
            "model_endpoint_required",
            "base versions cannot be inferred",
        ),
        (
            "bad/model",
            format!("endpoint='{}/v1beta'\n", gateway.base),
            "invalid_model_route",
            "Google model identifier",
        ),
        (
            "gemini",
            format!(
                "endpoint='{}/v1beta/models/other:generateContent'\n",
                gateway.base
            ),
            "invalid_model_route",
            "differs from selected model",
        ),
    ] {
        catalog(
            root.path(),
            &format!(
                "{header}[provider.one.models.alias]\nid='{model}'\nprotocol='google'\n{endpoint}"
            ),
        );
        let mut value = json!({"provider":{"config_file":"config.toml"}});
        let error = resolve(&mut value, root.path()).unwrap_err();
        assert_eq!(error.code, expected);
        assert!(error.cause.contains(cause), "{error:?}");
    }
    // A fixed Google generation endpoint is validated with its actual model, not "discovery".
    catalog(
        root.path(),
        &format!(
            "[provider.one]\nprotocol='google'\n[provider.one.options]\nendpoint='{}/v1beta/models/gemini:generateContent'\nmodel='gemini'\ntimeoutMs=1000\n",
            gateway.base
        ),
    );
    let config = read_catalog_config(root.path(), "one");
    assert_eq!(config.provider.model, "gemini");
    assert_eq!(config.provider.protocol, Protocol::Google);
    assert_eq!(
        config.provider_catalog.as_ref().unwrap().warnings[0].1.code,
        "invalid_endpoint"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn variants_follow_effective_protocol_in_both_override_directions() {
    let gateway = catalog_gateway(vec![("/v1/models", 200, json!({"data":[]}).to_string())]).await;
    let root = tempfile::tempdir().unwrap();
    for (provider_protocol, model_protocol, succeeds) in [
        ("anthropic", "openai-compatible", true),
        ("openai-compatible", "anthropic", false),
        ("openai-compatible", "openai", false),
    ] {
        catalog(
            root.path(),
            &format!(
                "[provider.one]\nprotocol='{provider_protocol}'\n[provider.one.options]\nbaseURL='{}/v1'\ntimeoutMs=1000\n[provider.one.models.alias]\nid='wire'\nprotocol='{model_protocol}'\nvariants=['high']\n",
                gateway.base
            ),
        );
        let mut value = json!({"provider":{"config_file":"config.toml"}});
        let result = resolve(&mut value, root.path());
        if succeeds {
            let (_, resolved) = result.unwrap().unwrap();
            assert_eq!(resolved.models[0].variants, ["default", "high"]);
            assert_eq!(resolved.models[0].protocol, Protocol::OpenaiCompatible);
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.code, "invalid_variants");
            assert!(error.cause.contains("effective openai-compatible"));
        }
    }
}

#[test]
fn recognized_suffixes_are_replaced_without_guessing_google_versions() {
    assert_eq!(
        endpoint_for_protocol(
            Protocol::Anthropic,
            None,
            Some("https://provider.invalid/gateway/v1/chat/completions/".into())
        )
        .unwrap(),
        "https://provider.invalid/gateway/v1/messages"
    );
    assert_eq!(
        endpoint_for_protocol(
            Protocol::OpenaiResponses,
            None,
            Some("https://provider.invalid/gateway/v1/messages".into())
        )
        .unwrap(),
        "https://provider.invalid/gateway/v1/responses"
    );
    for (protocol, base) in [
        (Protocol::Google, "https://provider.invalid/v1/messages"),
        (
            Protocol::Anthropic,
            "https://provider.invalid/v1beta/models/gemini:generateContent",
        ),
    ] {
        assert_eq!(
            endpoint_for_protocol(protocol, None, Some(base.into()))
                .unwrap_err()
                .code,
            "model_endpoint_required"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn effective_model_routes_send_native_wire_requests_to_exact_endpoints() {
    use microcrab_auditor::{EmptyVariableView, NoopAuditor};
    use microcrab_protocol::InputPart;
    use microcrab_transport::{Client, Content, ContentItem, TransportClient};
    use std::sync::{Arc, Mutex};
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let app = axum::Router::new().fallback(move |method: axum::http::Method,
        uri: axum::http::Uri, headers: axum::http::HeaderMap, bytes: axum::body::Bytes| {
        let recorded = recorded.clone();
        async move {
            if method == axum::http::Method::GET {
                assert_eq!(uri.path(), "/v1/models");
                assert_eq!(headers["authorization"], "Bearer synthetic-routing-key");
                return axum::Json(json!({"data":[]}));
            }
            assert_eq!(method, axum::http::Method::POST);
            assert_eq!(headers["authorization"], "Bearer synthetic-routing-key");
            assert!(!headers.contains_key("x-api-key"));
            assert!(!headers.contains_key("x-goog-api-key"));
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let response = match uri.path() {
                "/v1/chat/completions" => {
                    assert_eq!(body["model"], "chat-wire");
                    assert_eq!(body["reasoning_effort"], "high");
                    assert!(body["messages"].is_array());
                    json!({"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]})
                }
                "/v1/responses" => {
                    assert_eq!(body["model"], "responses-wire");
                    assert!(body["input"].is_array());
                    json!({"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]})
                }
                "/v1/messages" => {
                    assert_eq!(body["model"], "messages-wire");
                    assert_eq!(headers["anthropic-version"], "2023-06-01");
                    assert!(body["messages"].is_array());
                    json!({"type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn"})
                }
                "/v1beta/models/gemini-wire:generateContent" => {
                    assert!(body["contents"].is_array());
                    assert!(body.get("messages").is_none());
                    json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"ok"}]},"finishReason":"STOP"}]})
                }
                other => panic!("unexpected model route: {other}"),
            };
            recorded.lock().unwrap().push(uri.path().to_owned());
            axum::Json(response)
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let worker = tokio::spawn(async move {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), async move {
            axum::serve(listener, app).await.unwrap();
        })
        .await;
    });
    let root = tempfile::tempdir().unwrap();
    catalog(
        root.path(),
        &format!(
            r#"
[provider.mixed]
protocol='openai-compatible'
[provider.mixed.options]
baseURL='{base}/v1'
model='messages-wire'
apiKey='synthetic-routing-key'
timeoutMs=1000
[provider.mixed.models.chat]
id='chat-wire'
variants=['high']
[provider.mixed.models.messages]
id='messages-wire'
protocol='anthropic'
auth='bearer'
[provider.mixed.models.responses]
id='responses-wire'
protocol='openai'
[provider.mixed.models.google]
id='gemini-wire'
protocol='google'
endpoint='{base}/v1beta'
auth='bearer'
"#
        ),
    );
    let config = read_catalog_config(root.path(), "mixed");
    let content = Content {
        epoch: 1,
        subagent: None,
        system: "Synthetic system".into(),
        items: vec![ContentItem::User {
            parts: vec![InputPart::Text {
                text: "begin".into(),
            }],
            source: None,
        }],
        tools: vec![],
        max_output_tokens: 64,
        max_bytes: 16384,
        turn_id: None,
        round: None,
    };
    for model in [
        "chat-wire",
        "responses-wire",
        "messages-wire",
        "gemini-wire",
    ] {
        let variant = (model == "chat-wire").then_some("high");
        let selection = config.resolve_selection("mixed", model, variant).unwrap();
        let client = TransportClient::new(selection.provider.client_transport_config()).unwrap();
        let client = match client {
            TransportClient::OpenAiCompatible(chat) => TransportClient::OpenAiCompatible(
                chat.with_request_options(selection.options).unwrap(),
            ),
            other => other,
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.complete(
                &content,
                &NoopAuditor,
                &EmptyVariableView,
                tokio_util::sync::CancellationToken::new(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
    }
    assert_eq!(
        *requests.lock().unwrap(),
        [
            "/v1/chat/completions",
            "/v1/responses",
            "/v1/messages",
            "/v1beta/models/gemini-wire:generateContent"
        ]
    );
    worker.abort();
    let _ = worker.await;
}

#[test]
fn public_provider_template_uses_supported_catalog_schema_without_network() {
    let catalog: Catalog =
        toml::from_str(include_str!("../../../../../config.example.toml")).unwrap();
    assert_eq!(catalog.provider.len(), 4);
    for raw in catalog.provider.into_values() {
        let entry: Entry = raw.try_into().unwrap();
        endpoint_for_protocol(
            entry.protocol,
            entry.options.endpoint,
            entry.options.base_url,
        )
        .unwrap();
    }
}

#[test]
fn catalog_is_startup_relative_and_literal_credentials_are_preserved() {
    let root = tempfile::tempdir().unwrap();
    let path = catalog(root.path(), compatible());
    let mut value = json!({"provider":{"config_file":"config.toml","name":"grok"}});
    assert_eq!(
        resolve(&mut value, root.path())
            .unwrap()
            .map(|(path, _)| path),
        Some(path.canonicalize().unwrap())
    );
    assert_eq!(
        value["provider"]["endpoint"],
        "http://127.0.0.1:1/v1/chat/completions"
    );
    assert_eq!(value["provider"]["model"], "grok");
    assert_eq!(value["provider"]["api_key"], "synthetic-literal-key");
    assert_eq!(value["provider"]["timeout_ms"], 1234);
}
#[test]
fn endpoint_selection_is_exact_and_never_appends_twice() {
    for base in [
        "https://provider.invalid/v1",
        "https://provider.invalid/v1/",
        "https://provider.invalid/v1/chat/completions",
        "https://provider.invalid/v1/chat/completions/",
    ] {
        assert_eq!(
            endpoint(None, Some(base.into())).unwrap(),
            "https://provider.invalid/v1/chat/completions"
        );
    }
    assert_eq!(
        endpoint(Some("https://provider.invalid/custom/chat".into()), None).unwrap(),
        "https://provider.invalid/custom/chat"
    );
    assert!(
        endpoint(
            Some("https://provider.invalid/explicit".into()),
            Some("https://provider.invalid/v1".into())
        )
        .is_err()
    );
    assert!(endpoint(None, None).is_err());
    assert!(
        endpoint(
            None,
            Some("https://provider.invalid/v1?apiKey=synthetic".into())
        )
        .is_err()
    );
}
#[test]
fn single_provider_selects_without_name_but_multiple_requires_selection() {
    let root = tempfile::tempdir().unwrap();
    catalog(root.path(), compatible());
    let mut value = json!({"provider":{"config_file":"config.toml"}});
    resolve(&mut value, root.path()).unwrap();
    catalog(
        root.path(),
        &format!(
            "{}\n{}",
            compatible(),
            compatible().replace("provider.grok", "provider.other")
        ),
    );
    let mut ambiguous = json!({"provider":{"config_file":"config.toml"}});
    assert_eq!(
        resolve(&mut ambiguous, root.path()).unwrap_err().code,
        "selection_required"
    );
    let mut selected = json!({"provider":{"config_file":"config.toml","name":"other"}});
    resolve(&mut selected, root.path()).unwrap();
    let mut missing = json!({"provider":{"config_file":"config.toml","name":"missing"}});
    assert_eq!(
        resolve(&mut missing, root.path()).unwrap_err().code,
        "provider_not_found"
    );
}
#[test]
fn rejects_mixed_source_unknown_options_and_unsupported_protocol_without_input_echo() {
    let root = tempfile::tempdir().unwrap();
    let mut mixed = json!({"provider":{"config_file":"config.toml","endpoint":"https://provider.invalid","api_key":"DO-NOT-ECHO"}});
    let error = resolve(&mut mixed, root.path()).unwrap_err();
    assert_eq!(error.code, "invalid_reference");
    assert!(!error.to_string().contains("DO-NOT-ECHO"));
    catalog(
        root.path(),
        &compatible().replace("openai-compatible", "DO-NOT-ECHO"),
    );
    let mut value = json!({"provider":{"config_file":"config.toml"}});
    assert_eq!(
        resolve(&mut value, root.path()).unwrap_err().code,
        "unsupported_protocol"
    );
    catalog(
        root.path(),
        &format!("{}wrongField='DO-NOT-ECHO'\n", compatible()),
    );
    let error = resolve(&mut value, root.path()).unwrap_err();
    assert_eq!(error.code, "invalid_format");
    assert!(!error.to_string().contains("DO-NOT-ECHO"));
}
#[test]
fn bounded_read_and_missing_source_produce_safe_actionable_codes() {
    let root = tempfile::tempdir().unwrap();
    let mut value = json!({"provider":{"config_file":"config.toml"}});
    let error = resolve(&mut value, root.path()).unwrap_err();
    assert_eq!(error.stage, "provider_catalog");
    assert_eq!(error.code, "file_not_found");
    catalog(root.path(), &"x".repeat(MAX_CONFIG_BYTES + 1));
    assert_eq!(
        resolve(&mut value, root.path()).unwrap_err().code,
        "too_large"
    );
}
#[test]
fn loaded_catalog_is_private_and_cannot_overlap_public_assets() {
    let root = tempfile::tempdir().unwrap();
    let source = catalog(root.path(), compatible());
    let mut value: serde_json::Value =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    value["auth"]["jwt_secret"] = json!("synthetic-signing-key-with-at-least-thirty-two-bytes");
    // This privacy test assembles no agent component; a default example path is not installed here.
    value.as_object_mut().unwrap().remove("agent");
    value["provider"] = json!({"config_file":"config.toml","name":"grok"});
    let path = root.path().join("server.json");
    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    let config = super::super::Config::read_at(&path, root.path()).unwrap();
    assert!(
        config
            .private_paths
            .contains(&source.canonicalize().unwrap())
    );
    let host = crate::host::startup_local_host(&config).unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let result = runtime.block_on(host.read_file(
        source.canonicalize().unwrap(),
        1024,
        tokio_util::sync::CancellationToken::new(),
    ));
    let error = result.unwrap_err();
    assert!(matches!(
        &error,
        microcrab_host_api::ToolError::Host(cause)
            if cause.category == microcrab_host_api::ErrorCategory::Permission
    ));
    assert_eq!(
        error.public_reason(),
        Some("target path is protected by Host private-directory policy")
    );
    assert!(!error.to_string().contains("synthetic-literal-key"));
    value["frontend_dir"] = json!(root.path());
    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(super::super::Config::read_at(&path, root.path()).is_err());
}

#[test]
fn retained_catalog_preserves_provider_identity_and_explicit_default_model_metadata() {
    let root = tempfile::tempdir().unwrap();
    let first = format!(
        "{}\n[provider.grok.models.alias]\nid='grok'\nname='Named'\nvariants=['low','high']\n",
        compatible()
    );
    let second = compatible()
        .replace("provider.grok", "provider.other")
        .replace("synthetic-literal-key", "other-key");
    let unsupported =
        "\n[provider.future]\nprotocol='future'\n[provider.future.options]\nmodel='unavailable'\n";
    catalog(root.path(), &format!("{first}\n{second}{unsupported}"));
    let mut value = json!({"provider":{"config_file":"config.toml","name":"grok"}});
    let (_, resolved) = resolve(&mut value, root.path()).unwrap().unwrap();
    assert_eq!(resolved.providers.len(), 2);
    assert_eq!(resolved.models.len(), 2);
    assert_eq!(resolved.models[0].name, "Named");
    assert_eq!(resolved.models[0].variants, ["default", "low", "high"]);
    assert_eq!(resolved.models[1].variants, ["default"]);
    assert_eq!(
        resolved.providers["other"].api_key.as_deref(),
        Some("other-key")
    );
    assert!(!format!("{resolved:?}").contains("other-key"));
    let mut config: super::super::Config =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    config.provider_catalog = Some(std::sync::Arc::new(resolved));
    assert_eq!(config.default_provider_id(), "grok");
    let selected = config.resolve_selection("other", "grok", None).unwrap();
    assert_eq!(selected.provider.api_key.as_deref(), Some("other-key"));
    assert!(selected.options.reasoning_effort.is_none());
    for variant in [None, Some(""), Some("default")] {
        assert!(
            config
                .resolve_selection("grok", "grok", variant)
                .unwrap()
                .options
                .reasoning_effort
                .is_none()
        );
    }
    assert_eq!(
        config
            .resolve_selection("grok", "grok", Some("high"))
            .unwrap()
            .options
            .reasoning_effort
            .as_deref(),
        Some("high")
    );
    assert!(
        config
            .resolve_selection("other", "grok", Some("high"))
            .is_err()
    );
    assert!(config.resolve_selection("missing", "grok", None).is_err());
    assert!(config.resolve_selection("grok", "missing", None).is_err());
}

#[test]
fn default_model_uses_wire_id_and_malformed_variants_are_safe_errors() {
    let root = tempfile::tempdir().unwrap();
    let text = "[provider.one]\nprotocol='openai-compatible'\n[provider.one.options]\nbaseURL='http://127.0.0.1:1/v1'\n[provider.one.models.alias]\nid='wire-model'\nvariants=['xhigh']\n";
    catalog(root.path(), text);
    let mut value = json!({"provider":{"config_file":"config.toml"}});
    resolve(&mut value, root.path()).unwrap();
    assert_eq!(value["provider"]["model"], "wire-model");
    for variants in ["['high','high']", "['DO NOT ECHO']", "['']"] {
        catalog(root.path(), &text.replace("['xhigh']", variants));
        let mut value = json!({"provider":{"config_file":"config.toml"}});
        let error = resolve(&mut value, root.path()).unwrap_err();
        assert_eq!(error.code, "invalid_variants");
        assert!(!error.to_string().contains("DO NOT ECHO"));
    }
}

#[test]
fn model_context_windows_propagate_through_private_catalog_without_changing_routing() {
    let root = tempfile::tempdir().unwrap();
    let first = format!(
        "{}\n[provider.grok.models.alias]\nid='grok'\ncontext_window_tokens=131072\n",
        compatible()
    );
    let second = compatible().replace("provider.grok", "provider.other");
    catalog(
        root.path(),
        &format!(
            "{first}\n{second}\n[provider.other.models.alias]\nid='grok'\ncontext_window_tokens=262144\n"
        ),
    );
    let mut value: serde_json::Value =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    value["provider"] = json!({"config_file":"config.toml","name":"grok"});
    let path = root.path().join("server.json");
    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    let config = super::super::Config::read_at(&path, root.path()).unwrap();
    let choices = config.model_choices();
    assert_eq!(choices, config.available_models);
    assert_eq!(choices.len(), 2);
    for (provider_id, expected) in [("grok", 131072), ("other", 262144)] {
        let choice = choices
            .iter()
            .find(|choice| choice.provider_id == provider_id && choice.id == "grok")
            .unwrap();
        assert_eq!(choice.context_window_tokens, Some(expected));
        assert_eq!(
            serde_json::to_value(choice).unwrap()["context_window_tokens"],
            expected
        );
        let selection = config.resolve_selection(provider_id, "grok", None).unwrap();
        assert_eq!(selection.provider.model, "grok");
        assert!(selection.options.reasoning_effort.is_none());
    }
}

#[test]
fn catalog_missing_model_context_is_unknown_for_explicit_and_default_only_models() {
    let root = tempfile::tempdir().unwrap();
    for text in [
        compatible().to_owned(),
        format!(
            "{}\n[provider.grok.models.alias]\nid='grok'\n",
            compatible()
        ),
    ] {
        catalog(root.path(), &text);
        let mut value = json!({"provider":{"config_file":"config.toml"}});
        let (_, resolved) = resolve(&mut value, root.path()).unwrap().unwrap();
        assert_eq!(resolved.models.len(), 1);
        assert_eq!(resolved.models[0].context_window_tokens, None);
        assert!(
            value["available_models"][0]
                .get("context_window_tokens")
                .is_none()
        );
    }
}

#[test]
fn invalid_catalog_model_context_is_rejected_without_echoing_private_input() {
    let root = tempfile::tempdir().unwrap();
    for invalid in ["0", "-1", "1.5", "'synthetic-private-token'", "true"] {
        catalog(
            root.path(),
            &format!(
                "{}\n[provider.grok.models.alias]\nid='grok'\ncontext_window_tokens={invalid}\n",
                compatible()
            ),
        );
        let mut value = json!({"provider":{"config_file":"config.toml"}});
        let error = resolve(&mut value, root.path()).unwrap_err();
        assert_eq!(error.stage, "provider_catalog");
        assert_eq!(error.code, "invalid_format");
        assert!(error.cause.contains("expected a nonzero u64"), "{error:?}");
        assert!(
            error.cause.contains("invalid value") || error.cause.contains("invalid type"),
            "{error:?}"
        );
        assert_eq!(
            error.to_string(),
            format!("provider_catalog: invalid_format: {}", error.cause)
        );
        assert!(error.cause.len() <= 2048);
        for private in ["synthetic-private", "synthetic-literal-key"] {
            assert!(!format!("{error:?}").contains(private));
        }
    }
}

#[test]
fn native_protocol_models_remain_visible_selectable_and_use_native_adapters() {
    use microcrab_protocol::{InputPart, Message, Request};
    use microcrab_transport::{Protocol, TransportClient};

    let root = tempfile::tempdir().unwrap();
    let native = r#"
[provider.gemini]
protocol='google'
[provider.gemini.options]
baseURL='http://127.0.0.1:1/v1beta'
apiKey='synthetic-google-key'
[provider.gemini.models.alias]
id='gemini-model'
name='Gemini model'
context_window_tokens=1048576
[provider.responses]
protocol='openai'
[provider.responses.options]
baseURL='http://127.0.0.1:1/v1'
model='responses-model'
[provider.claude]
protocol='anthropic'
[provider.claude.options]
baseURL='http://127.0.0.1:1/v1'
model='claude-model'
"#;
    catalog(root.path(), &format!("{}{native}", compatible()));
    for default in ["grok", "gemini", "responses", "claude"] {
        let mut value = json!({"provider":{"config_file":"config.toml","name":default}});
        let (_, resolved) = resolve(&mut value, root.path()).unwrap().unwrap();
        assert_eq!(resolved.providers.len(), 4);
        assert_eq!(resolved.models.len(), 4);
        assert_eq!(
            value["provider"]["protocol"],
            resolved.providers[default].protocol.as_str()
        );
        assert_eq!(resolved.providers["gemini"].protocol, Protocol::Google);
        assert_eq!(
            resolved.providers["gemini"].endpoint,
            "http://127.0.0.1:1/v1beta"
        );
        assert_eq!(
            resolved.providers["responses"].endpoint,
            "http://127.0.0.1:1/v1/responses"
        );
        assert_eq!(
            resolved.providers["claude"].endpoint,
            "http://127.0.0.1:1/v1/messages"
        );
        let mut config: super::super::Config =
            serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
        config.provider_catalog = Some(std::sync::Arc::new(resolved));
        let choice = config
            .model_choices()
            .into_iter()
            .find(|choice| choice.provider_id == "gemini")
            .unwrap();
        assert_eq!(choice.id, "gemini-model");
        assert_eq!(choice.context_window_tokens, Some(1048576));
        assert_eq!(choice.variants, ["default"]);
        assert!(
            config
                .resolve_selection("gemini", "gemini-model", Some("thinking-high"))
                .is_err()
        );
        for variant in [None, Some(""), Some("default")] {
            let selected = config
                .resolve_selection("gemini", "gemini-model", variant)
                .unwrap();
            assert!(selected.options.reasoning_effort.is_none());
            let TransportClient::Google(client) =
                TransportClient::new(selected.provider.client_transport_config()).unwrap()
            else {
                panic!("Google catalog selection must construct the native Google client");
            };
            let request = Request {
                epoch: 1,
                messages: vec![Message::User {
                    parts: vec![InputPart::Text {
                        text: "synthetic input".into(),
                    }],
                }],
                tools: vec![],
                max_output_tokens: 64,
            };
            let adapted = client.adapt(&request).unwrap();
            assert_eq!(
                adapted["contents"][0]["parts"][0]["text"],
                "synthetic input"
            );
            assert!(adapted.get("messages").is_none());
        }
    }
}

#[test]
fn protocol_aliases_and_native_endpoint_suffixes_are_preserved() {
    use microcrab_transport::Protocol;
    for (raw, expected) in [
        ("gemini", Protocol::Google),
        ("claude", Protocol::Anthropic),
        ("openai-response", Protocol::OpenaiResponses),
        ("openai-responses", Protocol::OpenaiResponses),
    ] {
        let root = tempfile::tempdir().unwrap();
        catalog(root.path(), &compatible().replace("openai-compatible", raw));
        let mut value = json!({"provider":{"config_file":"config.toml"}});
        let (_, resolved) = resolve(&mut value, root.path()).unwrap().unwrap();
        assert_eq!(resolved.providers["grok"].protocol, expected);
    }
    for (protocol, path) in [
        (Protocol::OpenaiCompatible, "chat/completions"),
        (Protocol::OpenaiResponses, "responses"),
        (Protocol::Anthropic, "messages"),
    ] {
        let expected = format!("https://provider.invalid/v1/{path}");
        for base in [expected.clone(), format!("{expected}/")] {
            assert_eq!(
                endpoint_for_protocol(protocol, None, Some(base)).unwrap(),
                expected
            );
        }
    }
    assert_eq!(
        endpoint_for_protocol(
            Protocol::Google,
            None,
            Some("https://provider.invalid/v1beta/".into())
        )
        .unwrap(),
        "https://provider.invalid/v1beta/"
    );
}

#[test]
fn direct_provider_defaults_to_chat_completions_and_accepts_explicit_native_protocol() {
    use microcrab_transport::{Protocol, TransportClient};
    let legacy: Provider = serde_json::from_value(
        json!({"endpoint":"https://provider.invalid/v1/chat/completions","model":"legacy"}),
    )
    .unwrap();
    assert_eq!(legacy.protocol, Protocol::OpenaiCompatible);
    assert!(matches!(
        TransportClient::new(legacy.client_transport_config()).unwrap(),
        TransportClient::OpenAiCompatible(_)
    ));
    let google: Provider = serde_json::from_value(
        json!({"protocol":"google","endpoint":"https://provider.invalid/v1beta","model":"gemini"}),
    )
    .unwrap();
    assert!(matches!(
        TransportClient::new(google.client_transport_config()).unwrap(),
        TransportClient::Google(_)
    ));
}

#[test]
fn legacy_direct_choices_do_not_authorize_unresolved_display_models() {
    let mut config: super::super::Config =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    config.available_models.push(ModelChoice {
        id: "invented".into(),
        name: "invented".into(),
        provider_id: "configured".into(),
        protocol: Protocol::OpenaiCompatible,
        variants: vec!["high".into()],
        context_window_tokens: None,
    });
    assert_eq!(config.model_choices().len(), 1);
    assert!(
        config
            .resolve_selection("configured", "invented", None)
            .is_err()
    );
    assert!(
        config
            .resolve_selection("configured", &config.provider.model, Some("high"))
            .is_err()
    );
    assert!(
        config
            .resolve_selection("configured", &config.provider.model, Some("default"))
            .is_ok()
    );
}

struct CatalogGateway {
    base: String,
    requests: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
    worker: tokio::task::JoinHandle<()>,
}

impl Drop for CatalogGateway {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

async fn catalog_gateway(responses: Vec<(&str, u16, String)>) -> CatalogGateway {
    let responses: BTreeMap<String, (u16, String)> = responses
        .into_iter()
        .map(|(path, status, body)| (path.to_owned(), (status, body)))
        .collect();
    let responses = std::sync::Arc::new(responses);
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let app =
        axum::Router::new().fallback(move |method: axum::http::Method, uri: axum::http::Uri| {
            let responses = responses.clone();
            let recorded = recorded.clone();
            async move {
                recorded
                    .lock()
                    .unwrap()
                    .push((method.to_string(), uri.to_string()));
                let (status, body) = responses
                    .get(uri.path())
                    .cloned()
                    .unwrap_or((404, "unexpected fixture route".into()));
                (axum::http::StatusCode::from_u16(status).unwrap(), body)
            }
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    // Synchronous resolution waits on its own runtime. The second Tokio worker
    // serves these requests; both a deadline and Drop bound the fixture lifetime.
    let worker = tokio::spawn(async move {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), async move {
            axum::serve(listener, app).await.unwrap();
        })
        .await;
    });
    CatalogGateway {
        base,
        requests,
        worker,
    }
}

fn assert_catalog_gets(gateway: &CatalogGateway, expected: &[&str]) {
    let requests = gateway.requests.lock().unwrap();
    let mut paths = Vec::new();
    for (method, uri) in requests.iter() {
        assert_eq!(method, "GET");
        assert!(!uri.contains("synthetic-private"));
        paths.push(uri.split('?').next().unwrap().to_owned());
    }
    paths.sort();
    let mut expected: Vec<String> = expected.iter().map(|path| (*path).to_owned()).collect();
    expected.sort();
    assert_eq!(paths, expected);
}

fn assert_discovery_warnings(resolved: &ResolvedCatalog, expected: &[(&str, &str)]) {
    assert_eq!(resolved.warnings.len(), expected.len());
    for (provider_id, code) in expected {
        let (_, warning) = resolved
            .warnings
            .iter()
            .find(|(id, _)| id.as_str() == *provider_id)
            .unwrap();
        assert_eq!(warning.stage, "provider_catalog");
        assert_eq!(warning.code, *code);
        for private in [
            "synthetic-private-key",
            "synthetic-private-body",
            "DO-NOT-ECHO",
        ] {
            assert!(!format!("{warning:?}").contains(private));
            assert!(!warning.to_string().contains(private));
        }
    }
}

fn read_catalog_config(root: &Path, provider_id: &str) -> super::super::Config {
    let mut value: serde_json::Value =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    value["auth"]["jwt_secret"] = json!("synthetic-signing-key-with-at-least-thirty-two-bytes");
    value.as_object_mut().unwrap().remove("agent");
    value["provider"] = json!({"config_file":"config.toml","name":provider_id});
    let path = root.join("server.json");
    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    super::super::Config::read_at(&path, root).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_discovery_precedes_configured_overlay_and_default_model_append() {
    let gateway = catalog_gateway(vec![
        (
            "/alpha/models",
            200,
            json!({"data":[
                {"id":"remote","display_name":"Remote gateway name"},
                {"id":"shared","display_name":"Gateway shared name"}
            ]})
            .to_string(),
        ),
        (
            "/beta/models",
            200,
            json!({"data":[
                {"id":"shared","display_name":"Other gateway name"}
            ]})
            .to_string(),
        ),
    ])
    .await;
    let root = tempfile::tempdir().unwrap();
    catalog(
        root.path(),
        &format!(
            r#"
[provider.alpha]
protocol='openai-compatible'
[provider.alpha.options]
baseURL='{base}/alpha'
apiKey='synthetic-private-key-alpha'
model='options-only'
timeoutMs=2000
[provider.alpha.models.a_remote]
id='remote'
variants=['low']
[provider.alpha.models.b_shared]
id='shared'
name='Configured shared name'
variants=['high']
context_window_tokens=131072
[provider.alpha.models.c_local]
id='configured-only'
name='Configured only'
[provider.beta]
protocol='openai-compatible'
[provider.beta.options]
baseURL='{base}/beta'
apiKey='synthetic-private-key-beta'
timeoutMs=2000
"#,
            base = gateway.base
        ),
    );
    let mut value = json!({"provider":{"config_file":"config.toml","name":"alpha"}});
    let (_, resolved) = resolve(&mut value, root.path()).unwrap().unwrap();
    assert_catalog_gets(&gateway, &["/alpha/models", "/beta/models"]);
    assert_discovery_warnings(&resolved, &[]);
    assert_eq!(resolved.providers.len(), 2);
    let alpha: Vec<_> = resolved
        .models
        .iter()
        .filter(|choice| choice.provider_id == "alpha")
        .collect();
    assert_eq!(
        alpha
            .iter()
            .map(|choice| choice.id.as_str())
            .collect::<Vec<_>>(),
        ["remote", "shared", "configured-only", "options-only"]
    );
    assert_eq!(alpha[0].name, "Remote gateway name");
    assert_eq!(alpha[0].variants, ["default", "low"]);
    assert_eq!(alpha[1].name, "Configured shared name");
    assert_eq!(alpha[1].variants, ["default", "high"]);
    assert_eq!(alpha[1].context_window_tokens, Some(131072));
    assert_eq!(alpha[2].name, "Configured only");
    assert_eq!(alpha[3].name, "options-only");
    assert_eq!(alpha[3].variants, ["default"]);
    assert_eq!(alpha[3].context_window_tokens, None);
    assert_eq!(value["provider"]["model"], "options-only");
    assert_eq!(value["available_models"].as_array().unwrap().len(), 5);
    let mut config: super::super::Config =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    config.provider_catalog = Some(std::sync::Arc::new(resolved));
    let alpha_selection = config
        .resolve_selection("alpha", "shared", Some("high"))
        .unwrap();
    let beta_selection = config.resolve_selection("beta", "shared", None).unwrap();
    assert_eq!(
        alpha_selection.provider.api_key.as_deref(),
        Some("synthetic-private-key-alpha")
    );
    assert_eq!(
        beta_selection.provider.api_key.as_deref(),
        Some("synthetic-private-key-beta")
    );
    assert_eq!(
        alpha_selection.provider.endpoint,
        format!("{}/alpha/chat/completions", gateway.base)
    );
    assert_eq!(
        beta_selection.provider.endpoint,
        format!("{}/beta/chat/completions", gateway.base)
    );
    assert!(
        config
            .resolve_selection("beta", "shared", Some("high"))
            .is_err()
    );
    assert!(
        config
            .resolve_selection("beta", "configured-only", None)
            .is_err()
    );
    let beta = config
        .model_choices()
        .into_iter()
        .find(|choice| choice.provider_id == "beta")
        .unwrap();
    assert_eq!(beta.name, "Other gateway name");
    assert_eq!(beta.variants, ["default"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn google_configured_models_preserve_omitted_discovered_metadata() {
    let gateway = catalog_gateway(vec![(
        "/v1beta/models",
        200,
        json!({"models":[
            {"name":"models/gemini-keep","displayName":"Gateway retained name",
             "supportedGenerationMethods":["generateContent"],"inputTokenLimit":32768},
            {"name":"models/gemini-shared","displayName":"Gateway replaced name",
             "supportedGenerationMethods":["generateContent"],"inputTokenLimit":65536}
        ]})
        .to_string(),
    )])
    .await;
    let root = tempfile::tempdir().unwrap();
    catalog(
        root.path(),
        &format!(
            r#"
[provider.google]
protocol='google'
[provider.google.options]
baseURL='{base}/v1beta'
apiKey='synthetic-private-key'
timeoutMs=2000
[provider.google.models.a_keep]
id='gemini-keep'
[provider.google.models.b_shared]
id='gemini-shared'
name='Configured Google name'
context_window_tokens=1048576
"#,
            base = gateway.base
        ),
    );
    let mut value = json!({"provider":{"config_file":"config.toml"}});
    let (_, resolved) = resolve(&mut value, root.path()).unwrap().unwrap();
    assert_catalog_gets(&gateway, &["/v1beta/models"]);
    assert_discovery_warnings(&resolved, &[]);
    assert_eq!(resolved.models.len(), 2);
    let retained = resolved
        .models
        .iter()
        .find(|choice| choice.id == "gemini-keep")
        .unwrap();
    assert_eq!(retained.name, "Gateway retained name");
    assert_eq!(retained.context_window_tokens, Some(32768));
    let overridden = resolved
        .models
        .iter()
        .find(|choice| choice.id == "gemini-shared")
        .unwrap();
    assert_eq!(overridden.name, "Configured Google name");
    assert_eq!(overridden.context_window_tokens, Some(1048576));
    assert_eq!(value["provider"]["model"], "gemini-keep");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovery_failures_and_google_empty_do_not_abort_selected_healthy_provider() {
    let gateway = catalog_gateway(vec![
        (
            "/healthy/models",
            200,
            json!({"data":[{"id":"shared"}]}).to_string(),
        ),
        (
            "/status/models",
            503,
            "synthetic-private-body DO-NOT-ECHO".into(),
        ),
        (
            "/invalid/models",
            200,
            "{synthetic-private-body DO-NOT-ECHO".into(),
        ),
        ("/empty/models", 200, json!({"models":[]}).to_string()),
    ])
    .await;
    let root = tempfile::tempdir().unwrap();
    let mut text = String::new();
    for (id, protocol) in [
        ("healthy", "openai-compatible"),
        ("status", "openai-compatible"),
        ("invalid", "openai-compatible"),
        ("empty", "google"),
    ] {
        text.push_str(&format!("[provider.{id}]\nprotocol='{protocol}'\n[provider.{id}.options]\nbaseURL='{}/{id}'\napiKey='synthetic-private-key'\ntimeoutMs=2000\n", gateway.base));
    }
    catalog(root.path(), &text);
    let config = read_catalog_config(root.path(), "healthy");
    assert_catalog_gets(
        &gateway,
        &[
            "/healthy/models",
            "/status/models",
            "/invalid/models",
            "/empty/models",
        ],
    );
    let resolved = config.provider_catalog.as_ref().unwrap();
    assert_discovery_warnings(
        resolved,
        &[
            ("status", "discovery_status"),
            ("invalid", "discovery_invalid_format"),
            ("empty", "models_empty"),
        ],
    );
    assert_eq!(resolved.providers.len(), 4);
    assert_eq!(config.provider.model, "shared");
    assert_eq!(config.model_choices().len(), 1);
    assert!(config.resolve_selection("healthy", "shared", None).is_ok());
    for id in ["status", "invalid", "empty"] {
        assert_eq!(resolved.providers[id].model, "");
        let error = config.resolve_selection(id, "shared", None).err().unwrap();
        assert_eq!(error.code, "model_not_found");
    }
    let status = &resolved
        .warnings
        .iter()
        .find(|(id, _)| id == "status")
        .unwrap()
        .1;
    assert!(status.cause.contains("503"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovery_errors_preserve_configured_and_options_model_fallbacks() {
    let gateway = catalog_gateway(vec![
        (
            "/configured/models",
            503,
            "synthetic-private-body DO-NOT-ECHO".into(),
        ),
        (
            "/defaulted/models",
            200,
            "{synthetic-private-body DO-NOT-ECHO".into(),
        ),
    ])
    .await;
    let root = tempfile::tempdir().unwrap();
    catalog(
        root.path(),
        &format!(
            r#"
[provider.configured]
protocol='openai-compatible'
[provider.configured.options]
baseURL='{base}/configured'
apiKey='synthetic-private-key-configured'
timeoutMs=2000
[provider.configured.models.alias]
id='shared'
name='Configured fallback'
variants=['high']
context_window_tokens=65536
[provider.defaulted]
protocol='openai-compatible'
[provider.defaulted.options]
baseURL='{base}/defaulted'
apiKey='synthetic-private-key-defaulted'
model='shared'
timeoutMs=2000
"#,
            base = gateway.base
        ),
    );
    let config = read_catalog_config(root.path(), "configured");
    assert_catalog_gets(&gateway, &["/configured/models", "/defaulted/models"]);
    assert_discovery_warnings(
        config.provider_catalog.as_ref().unwrap(),
        &[
            ("configured", "discovery_status"),
            ("defaulted", "discovery_invalid_format"),
        ],
    );
    assert_eq!(config.model_choices().len(), 2);
    assert_eq!(config.provider.model, "shared");
    let configured = config
        .resolve_selection("configured", "shared", Some("high"))
        .unwrap();
    let defaulted = config
        .resolve_selection("defaulted", "shared", None)
        .unwrap();
    assert_eq!(
        configured.provider.api_key.as_deref(),
        Some("synthetic-private-key-configured")
    );
    assert_eq!(
        defaulted.provider.api_key.as_deref(),
        Some("synthetic-private-key-defaulted")
    );
    let choices = config.model_choices();
    let configured_choice = choices
        .iter()
        .find(|choice| choice.provider_id == "configured")
        .unwrap();
    assert_eq!(configured_choice.name, "Configured fallback");
    assert_eq!(configured_choice.context_window_tokens, Some(65536));
    assert!(
        config
            .resolve_selection("defaulted", "shared", Some("high"))
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_empty_provider_starts_and_keeps_other_provider_choices() {
    let gateway = catalog_gateway(vec![
        ("/empty/models", 200, json!({"models":[]}).to_string()),
        (
            "/healthy/models",
            200,
            json!({"data":[{"id":"remote"}]}).to_string(),
        ),
    ])
    .await;
    let root = tempfile::tempdir().unwrap();
    catalog(
        root.path(),
        &format!(
            "[provider.empty]\nprotocol='google'\n[provider.empty.options]\nbaseURL='{base}/empty'\ntimeoutMs=2000\n[provider.healthy]\nprotocol='openai-compatible'\n[provider.healthy.options]\nbaseURL='{base}/healthy'\ntimeoutMs=2000\n",
            base = gateway.base
        ),
    );
    let config = read_catalog_config(root.path(), "empty");
    assert_catalog_gets(&gateway, &["/empty/models", "/healthy/models"]);
    assert_eq!(config.default_provider_id(), "empty");
    assert_eq!(config.provider.model, "");
    let resolved = config.provider_catalog.as_ref().unwrap();
    assert_eq!(resolved.providers.len(), 2);
    assert_discovery_warnings(resolved, &[("empty", "models_empty")]);
    assert_eq!(config.model_choices().len(), 1);
    assert!(config.resolve_selection("healthy", "remote", None).is_ok());
    for model in ["", "remote", "nonexistent"] {
        assert_eq!(
            config
                .resolve_selection("empty", model, None)
                .err()
                .unwrap()
                .code,
            "model_not_found"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_empty_provider_catalog_starts_without_inventing_model_choices() {
    let gateway = catalog_gateway(vec![
        ("/google/models", 200, json!({"models":[]}).to_string()),
        ("/openai/models", 200, json!({"data":[]}).to_string()),
    ])
    .await;
    let root = tempfile::tempdir().unwrap();
    catalog(
        root.path(),
        &format!(
            "[provider.google]\nprotocol='google'\n[provider.google.options]\nbaseURL='{base}/google'\ntimeoutMs=2000\n[provider.openai]\nprotocol='openai-compatible'\n[provider.openai.options]\nbaseURL='{base}/openai'\ntimeoutMs=2000\n",
            base = gateway.base
        ),
    );
    let config = read_catalog_config(root.path(), "google");
    assert_catalog_gets(&gateway, &["/google/models", "/openai/models"]);
    assert_eq!(config.provider.model, "");
    assert!(config.model_choices().is_empty());
    assert!(config.available_models.is_empty());
    let resolved = config.provider_catalog.as_ref().unwrap();
    assert_eq!(resolved.providers.len(), 2);
    assert_discovery_warnings(
        resolved,
        &[("google", "models_empty"), ("openai", "models_empty")],
    );
    for provider_id in ["google", "openai"] {
        assert_eq!(resolved.providers[provider_id].model, "");
        for model in ["", "nonexistent"] {
            let error = config
                .resolve_selection(provider_id, model, None)
                .err()
                .unwrap();
            assert_eq!(error.stage, "model_selection");
            assert_eq!(error.code, "model_not_found");
        }
    }
}
