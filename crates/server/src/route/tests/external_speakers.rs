use super::*;
const BASE: &str = "/api/v1/projects/workspace/external-speakers";
#[tokio::test]
async fn external_inbox_checks_supplied_origin_for_enqueue_and_observation() {
    let fixture = Fixture::new(false).await;
    let operation = uuid::Uuid::new_v4().to_string();
    let key = "a".repeat(43);
    for method in ["POST", "GET"] {
        for origin in [
            None,
            Some("http://127.0.0.1:4096"),
            Some("https://untrusted.example"),
        ] {
            let uri = if method == "POST" {
                "/api/v1/sessions/absent/inbox".to_owned()
            } else {
                format!("/api/v1/sessions/absent/inbox/operations/{operation}")
            };
            let mut request = Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", format!("Bearer {key}"));
            if let Some(origin) = origin {
                request = request.header("origin", origin);
            }
            let request = if method == "POST" {
                request
                    .header("idempotency-key", &operation)
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"body":"hello"}"#))
                    .unwrap()
            } else {
                request.body(Body::empty()).unwrap()
            };
            let response = fixture.router.clone().oneshot(request).await.unwrap();
            let expected = if origin == Some("https://untrusted.example") {
                StatusCode::FORBIDDEN
            } else {
                // Absent/allowed Origin reaches bearer authorization for the absent Session.
                StatusCode::UNAUTHORIZED
            };
            assert_eq!(response.status(), expected, "{method} Origin={origin:?}");
            let response = body(response).await;
            assert_eq!(
                response["error"]["code"],
                if expected == StatusCode::FORBIDDEN {
                    "origin_not_allowed"
                } else {
                    "unauthenticated"
                },
                "{method} Origin={origin:?}"
            );
        }
    }
    fixture.state.manager.close().await.unwrap();
}

struct InboxClient;
#[async_trait::async_trait]
impl microcrab_session::Client for InboxClient {
    async fn complete(
        &self,
        _: &microcrab_transport::Content,
        _: &dyn microcrab_auditor::Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        Ok(microcrab_protocol::Response {
            provider_metadata: None,
            content: "inbox fixture completed".into(),
            tool_calls: vec![],
            usage: Default::default(),
        })
    }
}

#[tokio::test]
async fn external_inbox_http_exposes_only_authorized_operation_metadata() {
    let fixture = Fixture::with_client(true, Some(Arc::new(InboxClient))).await;
    let session = body(
        fixture
            .request(
                "POST",
                "/api/v1/sessions",
                Some(json!({
                    "project_id":"workspace", "operation_id":uuid::Uuid::new_v4().to_string()
                })),
            )
            .await,
    )
    .await;
    let id = session["id"].as_str().unwrap();
    let key = "a".repeat(43);
    let other = "b".repeat(43);
    let set =
        async |revision| {
            let response = fixture.request("PUT", BASE, Some(json!({
            "operation_id":uuid::Uuid::new_v4().to_string(), "expected_revision":revision,
            "speakers":{
                "monitor":{"action":"set","key":{"kind":"custom","value":key}},
                "other":{"action":"set","key":{"kind":"custom","value":other}}
            }
        }))).await;
            assert_eq!(response.status(), StatusCode::OK);
        };
    set(0).await;
    let operation = uuid::Uuid::new_v4().to_string();
    let inbox = format!("/api/v1/sessions/{id}/inbox");
    let get = format!("{inbox}/operations/{operation}");
    let request = async |method: &str, bearer: &str| {
        let mut request = Request::builder()
            .method(method)
            .uri(if method == "POST" { &inbox } else { &get })
            .header("authorization", format!("Bearer {bearer}"));
        let input = if method == "POST" {
            request = request
                .header("idempotency-key", &operation)
                .header("content-type", "application/json");
            Body::from(r#"{"body":"sensitive external text"}"#)
        } else {
            Body::empty()
        };
        fixture
            .router
            .clone()
            .oneshot(request.body(input).unwrap())
            .await
            .unwrap()
    };
    let created = request("POST", &key).await;
    assert_eq!(created.status(), StatusCode::ACCEPTED);
    let created = body(created).await;
    let alias = created["dialogue_id"].as_str().unwrap();
    assert_eq!(alias.len(), 8);
    assert!(
        alias
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    let fields = created.as_object().unwrap();
    assert_eq!(fields.len(), 4);
    assert!(
        ["dialogue_id", "operation_id", "ordinal", "status"]
            .iter()
            .all(|key| fields.contains_key(*key))
    );
    assert_eq!(created["operation_id"], operation);
    for method in ["POST", "GET"] {
        let response = request(method, &key).await;
        assert!(response.status().is_success());
        let response = body(response).await;
        assert_eq!(response["dialogue_id"], alias);
        assert_eq!(response["ordinal"], created["ordinal"]);
    }
    assert_eq!(request("GET", &other).await.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        request("GET", &"c".repeat(43)).await.status(),
        StatusCode::UNAUTHORIZED
    );
    set(1).await; // literal bytes unchanged, but not the original credential incarnation
    assert_eq!(request("GET", &key).await.status(), StatusCode::NOT_FOUND);
    let replay = request("POST", &key).await;
    assert_eq!(replay.status(), StatusCode::CONFLICT);
    assert!(body(replay).await.get("dialogue_id").is_none());
    fixture.state.manager.close().await.unwrap();
}

#[tokio::test]
async fn speaker_http_management_is_owner_authenticated_redacted_and_reconcilable() {
    let fixture = Fixture::new(false).await;
    let operation = uuid::Uuid::new_v4().to_string();
    let write = json!({"operation_id":operation,"expected_revision":0,"speakers":{"monitor":{"action":"set","key":{"kind":"generated"}}}});
    let unauthenticated = fixture
        .router
        .clone()
        .oneshot(Request::builder().uri(BASE).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    let no_origin = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(BASE)
                .header("cookie", format!("microcrab_session={}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from(write.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(no_origin.status(), StatusCode::FORBIDDEN);
    let initial = body(fixture.request("GET", BASE, None).await).await;
    assert_eq!(initial["revision"], 0);
    let response = fixture.request("PUT", BASE, Some(write.clone())).await;
    assert_eq!(response.status(), StatusCode::OK);
    let saved = body(response).await;
    assert_eq!(saved["revision"], 1);
    let retry = body(fixture.request("PUT", BASE, Some(write)).await).await;
    assert_eq!(saved, retry);
    let receipt = fixture
        .request("GET", &format!("{BASE}/operations/{operation}"), None)
        .await;
    assert_eq!(receipt.status(), StatusCode::OK);
    assert_eq!(body(receipt).await, saved);
    let unknown = fixture
        .request(
            "GET",
            &format!("{BASE}/operations/{}", uuid::Uuid::new_v4()),
            None,
        )
        .await;
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
    assert_eq!(body(unknown).await["error"]["code"], "operation_not_found");
    let reveal = fixture
        .request(
            "POST",
            &format!("{BASE}/key"),
            Some(json!({"identity":"monitor"})),
        )
        .await;
    assert_eq!(reveal.status(), StatusCode::OK);
    let reveal = body(reveal).await;
    let secret = reveal["key"].as_str().unwrap();
    assert_eq!(secret.len(), 43);
    assert!(!saved.to_string().contains(secret));
    assert!(
        !body(fixture.request("GET", BASE, None).await)
            .await
            .to_string()
            .contains(secret)
    );
    let intruder = fixture.state.auth.login_for_test("intruder");
    let forbidden = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("{BASE}/key"))
                .header("cookie", format!("microcrab_session={intruder}"))
                .header("origin", "http://127.0.0.1:4096")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"identity":"monitor"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forbidden.status(), StatusCode::NOT_FOUND);
    assert!(!body(forbidden).await.to_string().contains(secret));
    let duplicate = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(BASE)
                .header("cookie", format!("microcrab_session={}", fixture.token))
                .header("origin", "http://127.0.0.1:4096")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"expected_revision":1,"expected_revision":0,"speakers":{}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(duplicate.status(), StatusCode::BAD_REQUEST);
    // The dedicated inbox now uses the canonical Core port and rejects ambient owner cookies.
    let inbox = fixture
        .request("POST", "/api/v1/sessions/absent/inbox", Some(json!({})))
        .await;
    assert_eq!(inbox.status(), StatusCode::UNAUTHORIZED);
    fixture.state.manager.close().await.unwrap();
}
