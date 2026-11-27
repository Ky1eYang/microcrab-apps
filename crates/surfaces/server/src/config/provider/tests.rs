#[path = "media_tests.rs"]
mod media_tests;
use super::*;
use serde_json::json;

fn compatible_entry() -> serde_json::Value {
    json!({
        "protocol": "openai-compatible",
        "api_key": "synthetic-literal-key",
        "base_url": "http://127.0.0.1:1/v1",
        "model": "grok",
        "timeout_ms": 1234
    })
}

fn read_multi_config(
    root: &Path,
    default_provider: &str,
    providers: serde_json::Value,
) -> super::super::Config {
    let mut value: serde_json::Value =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    value["auth"]["jwt_secret"] = json!("synthetic-signing-key-with-at-least-thirty-two-bytes");
    value.as_object_mut().unwrap().remove("agent");
    value.as_object_mut().unwrap().remove("provider");
    value["default_provider"] = json!(default_provider);
    value["providers"] = providers;
    let path = root.join("server.json");
    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    super::super::Config::read_at(&path, root).unwrap()
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
    let config = read_multi_config(
        root.path(),
        "mixed",
        json!({
            "mixed": {
                "protocol": "openai-compatible",
                "base_url": format!("{}/gateway/v1/chat/completions", gateway.base),
                "api_key": "synthetic-private-inherited-key",
                "auth": "bearer",
                "model": "messages-wire",
                "timeout_ms": 1000,
                "models": {
                    "a_messages": {
                        "id": "messages-wire",
                        "protocol": "anthropic",
                        "name": "Configured messages"
                    },
                    "b_responses": {
                        "id": "responses-wire",
                        "protocol": "openai",
                        "auth": "native"
                    },
                    "c_google": {
                        "id": "gemini-wire",
                        "protocol": "google",
                        "endpoint": format!("{}/native/v1beta", gateway.base)
                    },
                    "d_chat": {
                        "id": "chat-wire",
                        "endpoint": format!("{}/custom-chat", gateway.base),
                        "variants": ["high"]
                    }
                }
            }
        }),
    );
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
    let mut value = json!({
        "providers": {
            "one": {
                "protocol": "openai-compatible",
                "endpoint": format!("{}/v1/chat/completions", gateway.base),
                "model": "wire",
                "timeout_ms": 1000,
                "models": {
                    "alias": {
                        "id": "wire",
                        "protocol": "anthropic"
                    }
                }
            }
        }
    });
    let error = resolve(&mut value).unwrap_err();
    assert_eq!(error.code, "model_endpoint_required");
    assert!(error.cause.contains("fixed provider endpoint"));
    for protocol in ["anthropic", "openai-compatible"] {
        let config = read_multi_config(
            root.path(),
            "one",
            json!({
                "one": {
                    "protocol": "openai-compatible",
                    "endpoint": format!("{}/v1/chat/completions", gateway.base),
                    "model": "wire",
                    "timeout_ms": 1000,
                    "models": {
                        "alias": {
                            "id": "wire",
                            "protocol": protocol,
                            "endpoint": format!("{}/custom", gateway.base)
                        }
                    }
                }
            }),
        );
        assert_eq!(config.provider.endpoint, format!("{}/custom", gateway.base));
        assert_eq!(config.provider.protocol.as_str(), protocol);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn google_cross_family_routes_require_explicit_endpoints_and_validate_every_real_model() {
    let gateway = catalog_gateway(vec![("/v1/models", 200, json!({"data":[]}).to_string())]).await;
    let root = tempfile::tempdir().unwrap();
    for (model, endpoint, expected, cause) in [
        (
            "gemini",
            None,
            "model_endpoint_required",
            "base versions cannot be inferred",
        ),
        (
            "bad/model",
            Some(format!("{}/v1beta", gateway.base)),
            "invalid_model_route",
            "Google model identifier",
        ),
        (
            "gemini",
            Some(format!("{}/v1beta/models/other:generateContent", gateway.base)),
            "invalid_model_route",
            "differs from selected model",
        ),
    ] {
        let mut model_cfg = json!({"id": model, "protocol": "google"});
        if let Some(ep) = endpoint {
            model_cfg["endpoint"] = json!(ep);
        }
        let mut value = json!({
            "providers": {
                "one": {
                    "protocol": "openai-compatible",
                    "base_url": format!("{}/v1", gateway.base),
                    "model": "default-good",
                    "timeout_ms": 1000,
                    "models": { "alias": model_cfg }
                }
            }
        });
        let error = resolve(&mut value).unwrap_err();
        assert_eq!(error.code, expected);
        assert!(error.cause.contains(cause), "{error:?}");
    }
    let config = read_multi_config(
        root.path(),
        "one",
        json!({
            "one": {
                "protocol": "google",
                "endpoint": format!("{}/v1beta/models/gemini:generateContent", gateway.base),
                "model": "gemini",
                "timeout_ms": 1000
            }
        }),
    );
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
    for (provider_protocol, model_protocol, expected_protocol) in [
        ("anthropic", "openai-compatible", Some(Protocol::OpenaiCompatible)),
        ("openai-compatible", "openai", Some(Protocol::OpenaiResponses)),
        ("openai-compatible", "anthropic", None),
    ] {
        let mut value = json!({
            "providers": {
                "one": {
                    "protocol": provider_protocol,
                    "base_url": format!("{}/v1", gateway.base),
                    "timeout_ms": 1000,
                    "models": {
                        "alias": {
                            "id": "wire",
                            "protocol": model_protocol,
                            "variants": ["high"]
                        }
                    }
                }
            }
        });
        let result = resolve(&mut value);
        if let Some(protocol) = expected_protocol {
            let resolved = result.unwrap().unwrap();
            assert_eq!(resolved.models[0].variants, ["default", "high"]);
            assert_eq!(resolved.models[0].protocol, protocol);
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.code, "invalid_variants");
            assert!(error.cause.contains("openai-compatible or openai-responses"));
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
async fn effective_model_routes_send_native_wire_requests_and_custom_headers_to_exact_endpoints() {
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
            assert_eq!(headers["user-agent"], "pi");
            assert_eq!(headers["x-gateway-client"], "microcrab-test");
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
    let config = read_multi_config(
        root.path(),
        "mixed",
        json!({
            "mixed": {
                "protocol": "openai-compatible",
                "base_url": format!("{base}/v1"),
                "model": "messages-wire",
                "api_key": "synthetic-routing-key",
                "timeout_ms": 1000,
                "headers": {
                    "User-Agent": "pi",
                    "X-Gateway-Client": "microcrab-test"
                },
                "models": {
                    "chat": {"id": "chat-wire", "variants": ["high"]},
                    "messages": {"id": "messages-wire", "protocol": "anthropic", "auth": "bearer"},
                    "responses": {"id": "responses-wire", "protocol": "openai"},
                    "google": {"id": "gemini-wire", "protocol": "google", "endpoint": format!("{base}/v1beta"), "auth": "bearer"}
                }
            }
        }),
    );
    let content = Content {
        epoch: 1,
        subagent: None,
        system: "Synthetic system".into(),
        items: vec![ContentItem::User {
            envelope: None,
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
fn literal_credentials_and_headers_are_preserved_in_jsonc_multi_provider() {
    let mut entry = compatible_entry();
    entry["headers"] = json!({"User-Agent": "pi"});
    let mut value = json!({
        "default_provider": "grok",
        "providers": { "grok": entry }
    });
    resolve(&mut value).unwrap().unwrap();
    assert_eq!(
        value["provider"]["endpoint"],
        "http://127.0.0.1:1/v1/chat/completions"
    );
    assert_eq!(value["provider"]["model"], "grok");
    assert_eq!(value["provider"]["api_key"], "synthetic-literal-key");
    assert_eq!(value["provider"]["timeout_ms"], 1234);
    assert_eq!(value["provider"]["headers"], json!({"User-Agent": "pi"}));
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
fn single_provider_selects_without_default_but_multiple_requires_selection() {
    let mut single = json!({ "providers": { "grok": compatible_entry() } });
    resolve(&mut single).unwrap().unwrap();

    let mut ambiguous = json!({
        "providers": {
            "grok": compatible_entry(),
            "other": compatible_entry()
        }
    });
    assert_eq!(
        resolve(&mut ambiguous).unwrap_err().code,
        "selection_required"
    );

    let mut selected = json!({
        "default_provider": "other",
        "providers": {
            "grok": compatible_entry(),
            "other": compatible_entry()
        }
    });
    let resolved = resolve(&mut selected).unwrap().unwrap();
    assert_eq!(resolved.default_provider_id, "other");

    let mut missing = json!({
        "default_provider": "missing",
        "providers": {
            "grok": compatible_entry(),
            "other": compatible_entry()
        }
    });
    assert_eq!(
        resolve(&mut missing).unwrap_err().code,
        "provider_not_found"
    );
}

#[test]
fn rejects_mixed_source_removed_toml_config_file_reserved_headers_and_bad_protocol() {
    let root = tempfile::tempdir().unwrap();
    let mut mixed = json!({
        "provider": {"endpoint": "https://provider.invalid", "model": "m"},
        "providers": {"one": compatible_entry()}
    });
    assert_eq!(
        resolve(&mut mixed).unwrap_err().code,
        "conflicting_provider_configuration"
    );

    let mut base: serde_json::Value =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    base["provider"] = json!({"config_file": "providers.toml", "name": "one"});
    let path = root.path().join("legacy-toml.jsonc");
    std::fs::write(&path, serde_json::to_vec(&base).unwrap()).unwrap();
    assert!(super::super::Config::read_at(&path, root.path()).is_err());

    let mut bad_proto = compatible_entry();
    bad_proto["protocol"] = json!("DO-NOT-ECHO");
    let mut value = json!({"providers": {"grok": bad_proto}});
    let err = resolve(&mut value).unwrap_err();
    assert_eq!(err.code, "unsupported_protocol");
    assert!(!err.to_string().contains("DO-NOT-ECHO"));

    let mut unknown_field = compatible_entry();
    unknown_field["wrong_field"] = json!("DO-NOT-ECHO");
    let mut value = json!({"providers": {"grok": unknown_field}});
    let err = resolve(&mut value).unwrap_err();
    assert_eq!(err.code, "invalid_format");
    assert!(!err.to_string().contains("DO-NOT-ECHO"));

    let mut bad_header = compatible_entry();
    bad_header["headers"] = json!({"Authorization": "DO-NOT-ECHO"});
    let mut value = json!({"providers": {"grok": bad_header}});
    let err = resolve(&mut value).unwrap_err();
    assert_eq!(err.code, "invalid_options");
    assert!(!err.to_string().contains("DO-NOT-ECHO"));
}

#[test]
fn retained_catalog_preserves_provider_identity_and_explicit_default_model_metadata() {
    let mut first = compatible_entry();
    first["models"] = json!({
        "alias": {
            "id": "grok",
            "name": "Named",
            "variants": ["low", "high"]
        }
    });
    let mut second = compatible_entry();
    second["api_key"] = json!("other-key");
    let mut value = json!({
        "default_provider": "grok",
        "providers": {
            "grok": first,
            "other": second,
            "future": {
                "protocol": "future",
                "model": "unavailable"
            }
        }
    });
    let resolved = resolve(&mut value).unwrap().unwrap();
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
    let mut value = json!({
        "providers": {
            "one": {
                "protocol": "openai-compatible",
                "base_url": "http://127.0.0.1:1/v1",
                "models": {
                    "alias": {
                        "id": "wire-model",
                        "variants": ["xhigh"]
                    }
                }
            }
        }
    });
    resolve(&mut value).unwrap();
    assert_eq!(value["provider"]["model"], "wire-model");
    for variants in [json!(["high", "high"]), json!(["DO NOT ECHO"]), json!([""])] {
        let mut value = json!({
            "providers": {
                "one": {
                    "protocol": "openai-compatible",
                    "base_url": "http://127.0.0.1:1/v1",
                    "models": {
                        "alias": {
                            "id": "wire-model",
                            "variants": variants
                        }
                    }
                }
            }
        });
        let error = resolve(&mut value).unwrap_err();
        assert_eq!(error.code, "invalid_variants");
        assert!(!error.to_string().contains("DO NOT ECHO"));
    }
}

#[test]
fn model_context_windows_propagate_through_jsonc_catalog_without_changing_routing() {
    let root = tempfile::tempdir().unwrap();
    let mut first = compatible_entry();
    first["models"] = json!({"alias": {"id": "grok", "context_window_tokens": 131072}});
    let mut second = compatible_entry();
    second["models"] = json!({"alias": {"id": "grok", "context_window_tokens": 262144}});
    let config = read_multi_config(
        root.path(),
        "grok",
        json!({"grok": first, "other": second}),
    );
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
    let mut with_alias = compatible_entry();
    with_alias["models"] = json!({"alias": {"id": "grok"}});
    for entry in [compatible_entry(), with_alias] {
        let mut value = json!({"providers": {"grok": entry}});
        let resolved = resolve(&mut value).unwrap().unwrap();
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
    for invalid in [
        json!(0),
        json!(-1),
        json!(1.5),
        json!("synthetic-private-token"),
        json!(true),
    ] {
        let mut entry = compatible_entry();
        entry["models"] = json!({"alias": {"id": "grok", "context_window_tokens": invalid}});
        let mut value = json!({"providers": {"grok": entry}});
        let error = resolve(&mut value).unwrap_err();
        assert_eq!(error.stage, "provider_catalog");
        assert_eq!(error.code, "invalid_format");
        assert!(error.cause.contains("expected a nonzero u64"), "{error:?}");
        for private in ["synthetic-private", "synthetic-literal-key"] {
            assert!(!format!("{error:?}").contains(private));
        }
    }
}

#[test]
fn native_protocol_models_remain_visible_selectable_and_use_native_adapters() {
    use microcrab_protocol::{InputPart, Message, Request};
    use microcrab_transport::{Protocol, TransportClient};

    let providers = json!({
        "grok": compatible_entry(),
        "gemini": {
            "protocol": "google",
            "base_url": "http://127.0.0.1:1/v1beta",
            "api_key": "synthetic-google-key",
            "models": {
                "alias": {
                    "id": "gemini-model",
                    "name": "Gemini model",
                    "context_window_tokens": 1048576
                }
            }
        },
        "responses": {
            "protocol": "openai",
            "base_url": "http://127.0.0.1:1/v1",
            "model": "responses-model"
        },
        "claude": {
            "protocol": "anthropic",
            "base_url": "http://127.0.0.1:1/v1",
            "model": "claude-model"
        }
    });
    for default in ["grok", "gemini", "responses", "claude"] {
        let mut value = json!({
            "default_provider": default,
            "providers": providers.clone()
        });
        let resolved = resolve(&mut value).unwrap().unwrap();
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
        let mut entry = compatible_entry();
        entry["protocol"] = json!(raw);
        let mut value = json!({"providers": {"grok": entry}});
        let resolved = resolve(&mut value).unwrap().unwrap();
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
    let mut value = json!({
        "default_provider": "alpha",
        "providers": {
            "alpha": {
                "protocol": "openai-compatible",
                "base_url": format!("{}/alpha", gateway.base),
                "api_key": "synthetic-private-key-alpha",
                "model": "options-only",
                "timeout_ms": 2000,
                "models": {
                    "a_remote": {"id": "remote", "variants": ["low"]},
                    "b_shared": {
                        "id": "shared",
                        "name": "Configured shared name",
                        "variants": ["high"],
                        "context_window_tokens": 131072
                    },
                    "c_local": {"id": "configured-only", "name": "Configured only"}
                }
            },
            "beta": {
                "protocol": "openai-compatible",
                "base_url": format!("{}/beta", gateway.base),
                "api_key": "synthetic-private-key-beta",
                "timeout_ms": 2000
            }
        }
    });
    let resolved = resolve(&mut value).unwrap().unwrap();
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
    drop(root);
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
    let mut value = json!({
        "providers": {
            "google": {
                "protocol": "google",
                "base_url": format!("{}/v1beta", gateway.base),
                "api_key": "synthetic-private-key",
                "timeout_ms": 2000,
                "models": {
                    "a_keep": {"id": "gemini-keep"},
                    "b_shared": {
                        "id": "gemini-shared",
                        "name": "Configured Google name",
                        "context_window_tokens": 1048576
                    }
                }
            }
        }
    });
    let resolved = resolve(&mut value).unwrap().unwrap();
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
    let config = read_multi_config(
        root.path(),
        "healthy",
        json!({
            "healthy": {
                "protocol": "openai-compatible",
                "base_url": format!("{}/healthy", gateway.base),
                "api_key": "synthetic-private-key",
                "timeout_ms": 2000
            },
            "status": {
                "protocol": "openai-compatible",
                "base_url": format!("{}/status", gateway.base),
                "api_key": "synthetic-private-key",
                "timeout_ms": 2000
            },
            "invalid": {
                "protocol": "openai-compatible",
                "base_url": format!("{}/invalid", gateway.base),
                "api_key": "synthetic-private-key",
                "timeout_ms": 2000
            },
            "empty": {
                "protocol": "google",
                "base_url": format!("{}/empty", gateway.base),
                "api_key": "synthetic-private-key",
                "timeout_ms": 2000
            }
        }),
    );
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
    let config = read_multi_config(
        root.path(),
        "configured",
        json!({
            "configured": {
                "protocol": "openai-compatible",
                "base_url": format!("{}/configured", gateway.base),
                "api_key": "synthetic-private-key-configured",
                "timeout_ms": 2000,
                "models": {
                    "alias": {
                        "id": "shared",
                        "name": "Configured fallback",
                        "variants": ["high"],
                        "context_window_tokens": 65536
                    }
                }
            },
            "defaulted": {
                "protocol": "openai-compatible",
                "base_url": format!("{}/defaulted", gateway.base),
                "api_key": "synthetic-private-key-defaulted",
                "model": "shared",
                "timeout_ms": 2000
            }
        }),
    );
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
    let config = read_multi_config(
        root.path(),
        "empty",
        json!({
            "empty": {
                "protocol": "google",
                "base_url": format!("{}/empty", gateway.base),
                "timeout_ms": 2000
            },
            "healthy": {
                "protocol": "openai-compatible",
                "base_url": format!("{}/healthy", gateway.base),
                "timeout_ms": 2000
            }
        }),
    );
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
    let config = read_multi_config(
        root.path(),
        "google",
        json!({
            "google": {
                "protocol": "google",
                "base_url": format!("{}/google", gateway.base),
                "timeout_ms": 2000
            },
            "openai": {
                "protocol": "openai-compatible",
                "base_url": format!("{}/openai", gateway.base),
                "timeout_ms": 2000
            }
        }),
    );
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
