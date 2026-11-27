use super::*;
use crate::manager::{
    dashboard::{Filter, Range, Window},
    preferences::PreferencesUpdate,
    providers::command as provider_command,
};
use axum::{body::Body, http::Request, routing::post as axum_post};
use microcrab_transport::Protocol;
use tokio::net::TcpListener;
use tower::ServiceExt as _;

struct FixtureClient {
    fail: bool,
}

#[async_trait::async_trait]
impl Client for FixtureClient {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn microcrab_auditor::Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        unreachable!("the relay consumes the streaming transport for both client formats")
    }

    async fn stream(
        &self,
        _: &Content,
        _: &dyn microcrab_auditor::Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: CancellationToken,
        events: Option<mpsc::Sender<ModelEvent>>,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        events
            .unwrap()
            .send(ModelEvent::TextDelta {
                text: "fixture answer".into(),
            })
            .await
            .unwrap();
        if self.fail {
            return Err(microcrab_transport::Error::ProviderStatus { status: 502 });
        }
        Ok(microcrab_protocol::Response {
            content: "fixture answer".into(),
            tool_calls: Vec::new(),
            usage: Usage {
                input_tokens: Some(2),
                output_tokens: Some(3),
                total_tokens: Some(5),
                samples: 1,
                ..Usage::default()
            },
            provider_metadata: None,
        })
    }
}

fn test_config(root: &std::path::Path) -> crate::config::Config {
    let mut raw: Value =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    raw["agent"] = Value::Null;
    raw["auth"]["dev_oidc"] = json!(false);
    raw["auth"]["discovery"] = json!(false);
    raw["auth"]["issuer"] = json!("http://127.0.0.1:19091");
    raw["auth"]["authorization_endpoint"] = json!("http://127.0.0.1:19091/authorize");
    raw["auth"]["token_endpoint"] = json!("http://127.0.0.1:19091/token");
    raw["auth"]["jwks_uri"] = json!("http://127.0.0.1:19091/jwks");
    raw["auth"]["jwt_secret"] = json!("test-only-secret-at-least-thirty-two-characters");
    let mut config: crate::config::Config = serde_json::from_value(raw).unwrap();
    config.resolve(root).unwrap();
    config.provider.endpoint = "https://provider.example.org/v1/chat/completions".into();
    config.provider.model = "test-model".into();
    config.relay.enabled = true;
    config.relay.validate(&config).unwrap();
    config
}

#[test]
fn chat_subset_accepts_standard_openai_parameters_and_multimodal_images() {
    use chat::Chat;
    let rich = json!({
        "model": "user:proxy/vision-model",
        "temperature": 0.4,
        "top_p": 0.9,
        "stop": ["END"],
        "tool_choice": "auto",
        "reasoning_effort": "medium",
        "n": 1,
        "user": "alice",
        "messages": [
            {"role": "system", "content": "instructions"},
            {"role": "user", "content": [
                {"type": "text", "text": "describe"},
                {"type": "image_url", "image_url": {"url": "https://example.org/a.png"}}
            ]},
            {"role": "assistant", "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "f", "arguments": "{}"}}]},
            {"role": "tool", "tool_call_id": "call_1", "content": "result"}
        ],
        "tools": [{"type": "function", "function": {"name": "f", "parameters": {"type": "object"}}}]
    });
    let parsed: Chat = serde_json::from_value(rich.clone()).unwrap();
    assert_eq!(parsed.request_options().unwrap().reasoning_effort.as_deref(), Some("medium"));
    let content = parsed.content(16384).unwrap();
    assert_eq!(content.system, "instructions");
    assert_eq!(content.items.len(), 3);

    // Rejects invalid image URL schemes, n > 1, and unmatched tool calls.
    for bad in [
        json!({"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"file:///etc/passwd"}}]}]}),
        json!({"messages":[{"role":"user","content":"hi"}],"n":2}),
        json!({"messages":[{"role":"user","content":"hi"}],"temperature":3.0}),
    ] {
        assert!(serde_json::from_value::<Chat>(bad).unwrap().content(16384).is_err());
    }
    let mut incomplete = rich;
    incomplete["messages"].as_array_mut().unwrap().pop();
    assert!(
        serde_json::from_value::<Chat>(incomplete)
            .unwrap()
            .content(16384)
            .is_err()
    );
    assert!(
        usage(&Usage {
            samples: 1,
            ..Usage::default()
        })
        .is_none()
    );
}

#[tokio::test]
async fn relay_serves_owner_providers_plus_server_and_isolates_owners_and_records_usage() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock_upstream = Router::new().route(
        "/v1/chat/completions",
        axum_post(|| async {
            Json(json!({
                "id": "upstream-1",
                "object": "chat.completion",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "from-user-provider"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18}
            }))
        }),
    );
    let upstream_task = tokio::spawn(async move {
        let _ = axum::serve(listener, mock_upstream).await;
    });

    let root = tempfile::tempdir().unwrap();
    let mut config = test_config(root.path());
    config.relay.allow_http_loopback = true;
    let auth = crate::auth::Auth::new(config.auth.clone()).unwrap();
    let manager = crate::manager::Manager::new(config).await.unwrap();

    // Owner A registers a private provider; Owner B registers another private provider.
    let register_provider = |owner: String, id: String, model: String| {
        let mgr = &manager;
        async move {
            provider_command(
                mgr,
                &owner,
                "provider.set",
                json!({
                    "provider_id": id,
                    "expected_revision": 0,
                    "provider": {
                        "name": id,
                        "protocol": "openai-compatible",
                        "endpoint": format!("http://{addr}/v1/chat/completions"),
                        "auth": "native",
                        "timeout_ms": 5000,
                        "max_response_bytes": 1048576,
                        "models": [{"id": model}]
                    },
                    "api_key": null
                }),
            )
            .await
            .unwrap();
        }
    };
    register_provider("owner-a".into(), "user:alpha".into(), "alpha-model".into()).await;
    register_provider("owner-b".into(), "user:beta".into(), "beta-model".into()).await;

    let key_a = manager
        .relay_keys
        .create("owner-a", Default::default())
        .await
        .unwrap();
    let app = router(Arc::new(Application {
        auth,
        manager: manager.clone(),
    }));

    // 1. Owner A's /v1/models sees configured + user:alpha, NEVER user:beta.
    let models_res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .header(header::AUTHORIZATION, format!("Bearer {}", key_a.secret))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(models_res.status(), StatusCode::OK);
    let models_json: Value =
        serde_json::from_slice(&to_bytes(models_res.into_body(), 16 * 1024).await.unwrap())
            .unwrap();
    let model_ids: Vec<_> = models_json["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert!(model_ids.contains(&"configured/test-model"));
    assert!(model_ids.contains(&"user:alpha/alpha-model"));
    assert!(!model_ids.contains(&"user:beta/beta-model"));

    // 2. Owner A cannot invoke Owner B's private provider even with explicit ID.
    let cross_owner = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::AUTHORIZATION, format!("Bearer {}", key_a.secret))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"model":"user:beta/beta-model","messages":[{"role":"user","content":"hi"}]})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cross_owner.status(), StatusCode::NOT_FOUND);

    // 3. Mixed session cookie + bearer key is rejected; cross-origin with valid key is allowed.
    let cookie_mix = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .header(header::AUTHORIZATION, format!("Bearer {}", key_a.secret))
                .header(header::COOKIE, "microcrab_session=anything")
                .header(header::ORIGIN, "https://app.example.com")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cookie_mix.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        cookie_mix
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|v| v.to_str().ok()),
        Some("*")
    );

    let preflight = app
        .clone()
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/v1/chat/completions")
                .header(header::ORIGIN, "https://app.example.com")
                .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                .header(
                    header::ACCESS_CONTROL_REQUEST_HEADERS,
                    "authorization, content-type",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(preflight.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        preflight
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|v| v.to_str().ok()),
        Some("*")
    );
    assert_eq!(
        preflight
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
            .and_then(|v| v.to_str().ok()),
        Some("authorization, content-type")
    );

    let cors_models = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .header(header::AUTHORIZATION, format!("Bearer {}", key_a.secret))
                .header(header::ORIGIN, "https://app.example.com")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cors_models.status(), StatusCode::OK);
    assert_eq!(
        cors_models
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|v| v.to_str().ok()),
        Some("*")
    );

    // 4. Account preference accepts Owner A's private `user:alpha/alpha-model` as default relay model.
    let pref_update = PreferencesUpdate::parse(
        json!({
            "expected_revision": 0,
            "dashboard_default_range": "7d",
            "low_cache_hit_warn_pct": null,
            "relay_default_selection": {"provider_id": "user:alpha", "model": "alpha-model"}
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    manager
        .update_preferences("owner-a", pref_update)
        .await
        .unwrap();

    // 5. Relay call omitting `model` resolves Owner A's account default (`user:alpha/alpha-model`),
    // forwards upstream, records into Owner A's Dashboard usage, and updates key usage stats.
    let chat_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::AUTHORIZATION, format!("Bearer {}", key_a.secret))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "temperature": 0.3,
                        "messages": [{"role": "user", "content": "hello"}]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(chat_res.status(), StatusCode::OK);
    let chat_json: Value =
        serde_json::from_slice(&to_bytes(chat_res.into_body(), 16 * 1024).await.unwrap()).unwrap();
    assert_eq!(chat_json["model"], "user:alpha/alpha-model");
    assert_eq!(
        chat_json["choices"][0]["message"]["content"],
        "from-user-provider"
    );
    assert_eq!(chat_json["usage"]["total_tokens"], 18);

    // Key stats updated without bumping config revision.
    let listed = manager.relay_keys.list("owner-a").await.unwrap();
    assert_eq!(listed[0].request_count, 1);
    assert!(listed[0].last_used_at_ms.is_some());
    assert_eq!(listed[0].revision, 1);

    // Usage recorded strictly in Owner A's Dashboard summary & logs, invisible to Owner B.
    let filter = Filter::default();
    let window = Window::new(Range::Days7, crate::manager::now(), None, None).unwrap();
    let summary_a = manager
        .dashboard_summary("owner-a", &filter, &window)
        .await
        .unwrap();
    let summary_b = manager
        .dashboard_summary("owner-b", &filter, &window)
        .await
        .unwrap();
    assert_eq!(summary_a["kpi"]["requests"], 1);
    assert_eq!(summary_b["kpi"]["requests"], 0);

    // 6. Test Call streaming error and fixture recording directly.
    let state = Arc::new(Application {
        auth: crate::auth::Auth::new(manager.config.auth.clone()).unwrap(),
        manager: manager.clone(),
    });
    let permit = state.manager.relay_runtime.global.clone().acquire_owned().await.unwrap();
    let fail_call = Call {
        admission: Admission {
            state: state.clone(),
            credential: Credential {
                owner: "owner-a".into(),
                key: listed[0].clone(),
            },
            _slot: permit,
        },
        id: "chatcmpl-fail".into(),
        route_id: "user:alpha/alpha-model".into(),
        model: Model {
            provider_id: "user:alpha".into(),
            model: "alpha-model".into(),
            protocol: Some(Protocol::OpenaiCompatible),
        },
        client: Box::new(FixtureClient { fail: true }),
        content: Content {
            epoch: 0,
            subagent: None,
            system: String::new(),
            items: Vec::new(),
            tools: Vec::new(),
            max_output_tokens: 32,
            max_bytes: 1024,
            turn_id: None,
            round: None,
        },
        cancellation: CancellationToken::new(),
        deadline: Instant::now() + Duration::from_secs(5),
        stream: false,
        include_usage: false,
        created: 0,
        used_at_ms: crate::manager::now(),
    };
    let fail_res = fail_call.non_streaming().await.unwrap_err();
    assert_eq!(fail_res.0, StatusCode::BAD_GATEWAY);

    upstream_task.abort();
    manager.close().await.unwrap();
}
