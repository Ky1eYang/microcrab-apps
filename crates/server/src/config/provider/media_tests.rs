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
    let gateway=super::catalog_gateway(vec![("/google/models",200,json!({"models":[
        {"name":"models/explicit","supportedGenerationMethods":["generateContent"]},
        {"name":"models/implicit","supportedGenerationMethods":["generateContent"],"google_function_response_media":true}
    ]}).to_string())]).await;
    let root = tempfile::tempdir().unwrap();
    super::catalog(
        root.path(),
        &format!(
            "[provider.google]\nprotocol='google'\n[provider.google.options]\nbaseURL='{}/google'\nmodel='explicit'\ntimeoutMs=1000\n[provider.google.models.exact]\nid='explicit'\ngoogle_function_response_media=true\n[provider.google.models.other]\nid='configured-off'\n",
            gateway.base
        ),
    );
    let config = super::read_catalog_config(root.path(), "google");
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
    let root = tempfile::tempdir().unwrap();
    super::catalog(
        root.path(),
        "[provider.one]\nprotocol='openai-compatible'\n[provider.one.options]\nendpoint='http://127.0.0.1:1/chat/completions'\nmodel='fixture'\n[provider.one.models.fixture]\ngoogle_function_response_media=true\n",
    );
    let mut reference = json!({"provider":{"config_file":"config.toml","name":"one"}});
    let error = resolve(&mut reference, root.path()).unwrap_err();
    assert_eq!(error.code, "google_media_requires_google_protocol");
}
