use super::*;
use serde_json::json;

#[test]
fn inline_provider_capability_defaults_false_and_rejects_non_google_opt_in() {
    for protocol in ["openai-compatible", "openai", "anthropic", "google"] {
        let value = json!({"protocol":protocol,"endpoint":"https://example.invalid/v1beta","model":"fixture"});
        let provider: Provider = serde_json::from_value(value.clone()).unwrap();
        assert!(!provider.google_function_response_media);
        provider.validate_media_capability().unwrap();
        let mut enabled = value;
        enabled["google_function_response_media"] = json!(true);
        let provider: Provider = serde_json::from_value(enabled).unwrap();
        assert_eq!(
            provider.validate_media_capability().is_ok(),
            protocol == "google"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_capability_is_exact_model_opt_in_and_discovery_never_grants_it() {
    let gateway = super::catalog_gateway(vec![(
        "/google/models",
        200,
        json!({"models":[
            {"name":"models/explicit","supportedGenerationMethods":["generateContent"]},
            {"name":"models/implicit","supportedGenerationMethods":["generateContent"],"google_function_response_media":true}
        ]})
        .to_string(),
    )])
    .await;
    let root = tempfile::tempdir().unwrap();
    let config = super::read_multi_config(
        root.path(),
        "google",
        json!({
            "google": {
                "protocol": "google",
                "base_url": format!("{}/google", gateway.base),
                "model": "explicit",
                "timeout_ms": 1000,
                "models": {
                    "exact": {
                        "id": "explicit",
                        "google_function_response_media": true
                    },
                    "other": {
                        "id": "configured-off"
                    }
                }
            }
        }),
    );
    assert!(config.provider.google_function_response_media);
    for (model, expected) in [
        ("explicit", true),
        ("implicit", false),
        ("configured-off", false),
    ] {
        assert_eq!(
            config
                .resolve_selection("google", model, None)
                .unwrap()
                .provider
                .google_function_response_media,
            expected
        );
    }
    super::assert_catalog_gets(&gateway, &["/google/models"]);
}

#[test]
fn non_google_catalog_media_capability_is_rejected_before_route_http() {
    let mut value = json!({
        "default_provider": "one",
        "providers": {
            "one": {
                "protocol": "openai-compatible",
                "endpoint": "http://127.0.0.1:1/chat/completions",
                "model": "fixture",
                "models": {
                    "fixture": {
                        "google_function_response_media": true
                    }
                }
            }
        }
    });
    let error = resolve(&mut value).unwrap_err();
    assert_eq!(error.code, "google_media_requires_google_protocol");
}
