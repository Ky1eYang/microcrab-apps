//! Request diagnostics preserve closed facts without publishing request values.
use super::*;

#[tokio::test]
async fn capabilities_negotiate_relay_and_display_without_cross_socket_authority() {
    use crate::route::socket::{GlobalMethod, SessionMethod, decode};
    let fixture = Fixture::new(false).await;
    let response = fixture.request("GET", "/api/v1/capabilities", None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let capabilities = body(response).await;
    let methods = capabilities["methods"].as_array().unwrap();
    for method in ["host.relay_attach", "display.subscribe"] {
        assert!(
            methods.iter().any(|value| value == method),
            "cannot negotiate {method}"
        );
        let frame = json!({
            "v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),
            "method":method,"params":{}
        })
        .to_string();
        if method == "host.relay_attach" {
            assert!(matches!(
                decode::<GlobalMethod>(&frame),
                Ok((_, GlobalMethod::HostRelayAttach))
            ));
            assert!(decode::<SessionMethod>(&frame).is_err());
        } else {
            assert!(matches!(
                decode::<SessionMethod>(&frame),
                Ok((_, SessionMethod::DisplaySubscribe))
            ));
            assert!(decode::<GlobalMethod>(&frame).is_err());
        }
    }
    assert!(
        !methods
            .iter()
            .any(|value| value == "session.attach" || value == "session.subscribe")
    );
    fixture.state.manager.close().await.unwrap();
}

#[tokio::test]
async fn capabilities_expose_build_identity_only_after_authentication() {
    let fixture = Fixture::new(false).await;
    let unauthenticated = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/capabilities")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    assert!(body(unauthenticated).await.get("build").is_none());

    let response = fixture.request("GET", "/api/v1/capabilities", None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let capabilities = body(response).await;
    let build = capabilities["build"].as_object().unwrap();
    assert_eq!(build.len(), 2);
    assert_eq!(build["version"], env!("CARGO_PKG_VERSION"));
    #[cfg(target_os = "linux")]
    {
        let hash = build["sha256"].as_str().unwrap();
        assert_eq!(hash.len(), 64);
        assert!(
            hash.bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        );
    }
    #[cfg(not(target_os = "linux"))]
    assert_eq!(build.get("sha256"), Some(&Value::Null));

    for path in ["/api/health", "/api/v1/health"] {
        let health = fixture
            .router
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        assert!(body(health).await.get("build").is_none());
    }
    fixture.state.manager.close().await.unwrap();
}

#[tokio::test]
async fn invalid_path_query_and_json_report_safe_actionable_facts() {
    let fixture = Fixture::new(false).await;
    for (path, source, kind, field, cause) in [
        (
            "/api/v1/sessions/s/images/m/private-secret-index",
            "path",
            None,
            None,
            "unsigned integer part_index",
        ),
        (
            "/api/v1/sessions/s/client-tools/calls?limit=private-secret-limit",
            "query",
            Some("expected_integer"),
            Some("limit"),
            "invalid digit found in string",
        ),
        (
            "/api/v1/sessions/s/client-tools/calls?limit=9999999999999999999999999999999999999999",
            "query",
            Some("integer_out_of_range"),
            Some("limit"),
            "number too large to fit in target type",
        ),
        (
            "/api/v1/sessions?private-secret-field=private-secret-value",
            "query",
            Some("unknown_field"),
            None,
            "unknown field in query parameters",
        ),
    ] {
        let response = fixture.request("GET", path, None).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error = body(response).await["error"].clone();
        assert_eq!(error["code"], "invalid_request");
        assert_eq!(error["retryable"], false);
        assert_eq!(error["details"]["source"], source);
        assert_eq!(error["details"]["category"], "data");
        assert!(
            error["message"].as_str().unwrap().contains(cause),
            "{path}: {error}"
        );
        if let Some(kind) = kind {
            assert_eq!(error["details"]["reason"]["kind"], kind);
            assert_eq!(error["details"]["reason"]["field"].as_str(), field);
            let concrete = error["details"]["cause"].as_str().unwrap();
            let expected_cause = match field {
                Some(field) => format!("{field}: {cause}"),
                None => cause.to_owned(),
            };
            assert_eq!(concrete, expected_cause);
            assert_eq!(
                error["message"],
                format!("Query parameters do not match the endpoint schema: {expected_cause}")
            );
            assert!(concrete.len() <= 2048);
        }
        assert!(
            !error.to_string().contains("private-secret"),
            "query/path diagnostic leaked request data for {path}: {error}"
        );
        assert!(error["message"].as_str().unwrap().len() <= 4096);
    }
    for (data, category, kind, reason) in [
        (
            "{\n\"name\":\"private-secret\",}",
            "syntax",
            "trailing_comma",
            "trailing comma",
        ),
        ("", "eof", "missing_value", "EOF while parsing a value"),
        (
            "{\"name\":\"private-secret\"",
            "eof",
            "incomplete_object",
            "EOF while parsing an object",
        ),
    ] {
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/projects")
            .header("cookie", format!("microcrab_session={}", fixture.token))
            .header("origin", "http://127.0.0.1:4096")
            .header("content-type", "application/json")
            .body(Body::from(data))
            .unwrap();
        let response = fixture.router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error = body(response).await["error"].clone();
        assert_eq!(error["details"]["source"], "json");
        assert_eq!(error["code"], "invalid_request");
        assert_eq!(error["details"]["category"], category);
        assert_eq!(error["details"]["reason"]["kind"], kind);
        assert!(error["details"]["line"].as_u64().unwrap() >= 1);
        assert!(error["details"]["column"].is_u64());
        assert!(error["details"]["cause"].as_str().unwrap().contains(reason));
        assert!(error["details"]["cause"].as_str().unwrap().len() <= 2048);
        assert!(
            error["message"].as_str().unwrap().contains(reason),
            "{error}"
        );
        assert!(!error.to_string().contains("private-secret"));
    }
    fixture.state.manager.close().await.unwrap();
}

#[tokio::test]
async fn project_validation_reports_empty_name_invalid_id_and_root_separately() {
    let fixture = Fixture::new(false).await;
    for (params, expected, kind, field) in [
        (
            json!({}),
            "missing field name",
            Some("missing_field"),
            Some("name"),
        ),
        (
            json!({"name":17}),
            "expected a string",
            Some("invalid_type"),
            None,
        ),
        (
            json!({"name":"   "}),
            "Project name must not be empty",
            None,
            None,
        ),
        (
            json!({"name":"ok","id":"private-secret/invalid"}),
            "Project id must contain",
            None,
            None,
        ),
        (
            json!({"name":"ok","root":" "}),
            "Project root must not be empty",
            None,
            None,
        ),
        (
            json!({"name":"ok","root":"private-secret\u{0000}"}),
            "Project root must not contain NUL",
            None,
            None,
        ),
        (
            json!({"name":"ok","private-secret-field":"private-secret-value"}),
            "unknown field",
            Some("unknown_field"),
            None,
        ),
    ] {
        let response = fixture
            .request("POST", "/api/v1/projects", Some(params))
            .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error = body(response).await["error"].clone();
        assert_eq!(error["code"], "invalid_request");
        assert_eq!(error["retryable"], false);
        if let Some(kind) = kind {
            assert_eq!(error["details"]["source"], "params");
            assert_eq!(error["details"]["category"], "data");
            assert_eq!(error["details"]["reason"]["kind"], kind);
            assert_eq!(error["details"]["reason"]["field"].as_str(), field);
            assert!(error["details"]["line"].is_u64());
            assert!(error["details"]["column"].is_u64());
            assert!(
                error["details"]["cause"]
                    .as_str()
                    .unwrap()
                    .contains(expected)
            );
        }
        assert!(error["message"].as_str().unwrap().len() <= 4096);
        assert!(
            error["message"].as_str().unwrap().contains(expected),
            "{error}"
        );
        assert!(!error.to_string().contains("private-secret"));
    }
    fixture.state.manager.close().await.unwrap();
}
