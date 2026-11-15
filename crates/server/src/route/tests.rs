//! Transport regression checks: owner isolation, browser mutation admission,
//! durable create reconciliation and logical deletion remain Manager contracts.
mod attachments;
mod citations;
mod children;
mod host_connection;
mod images;
mod model_metadata;
mod permission_ask;
mod provenance;
mod v018;
mod validation;
mod variables;

use super::*;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
    response::Response,
};
use serde_json::{Value, json};
use tower::ServiceExt;

struct Fixture {
    router: Router,
    state: Arc<Application>,
    token: String,
    _root: tempfile::TempDir,
}
impl Fixture {
    async fn new(embedded: bool) -> Self {
        Self::with_client(embedded, None).await
    }
    async fn with_client(
        embedded: bool,
        client: Option<Arc<dyn microcrab_session::Client>>,
    ) -> Self {
        let root = tempfile::tempdir().unwrap();
        let mut value: Value =
            serde_json::from_str(include_str!("../../config.example.json")).unwrap();
        value["agent"] = Value::Null;
        value["auto_rename"] = json!(false);
        value["provider"]["endpoint"] = json!("http://127.0.0.1:1/v1/chat/completions");
        // Transport tests mint application cookies directly; they do not serve
        // an OIDC listener or perform discovery. Keep endpoint resolution local.
        value["auth"]["dev_oidc"] = json!(false);
        value["auth"]["discovery"] = json!(false);
        value["auth"]["issuer"] = json!("http://127.0.0.1:19091");
        value["auth"]["authorization_endpoint"] = json!("http://127.0.0.1:19091/authorize");
        value["auth"]["token_endpoint"] = json!("http://127.0.0.1:19091/token");
        value["auth"]["jwks_uri"] = json!("http://127.0.0.1:19091/jwks");
        value["auth"]["jwt_secret"] = json!("test-only-secret-at-least-thirty-two-characters");
        let mut config: crate::config::Config = serde_json::from_value(value).unwrap();
        config.resolve(root.path()).unwrap();
        #[cfg(not(feature = "storage-sqlite"))]
        {
            config.storage = crate::config::Storage::File;
        }
        let (router, state) = if let Some(client) = client {
            assert!(
                embedded,
                "test Client injection requires explicit embedding"
            );
            let deployment =
                crate::deployment::DeploymentGuard::acquire(&config.data_dir, &config.startup_dir)
                    .unwrap();
            let auth = crate::auth::Auth::new(config.auth.clone()).unwrap();
            let frontend = config.frontend_dir.clone();
            let manager = crate::manager::Manager::with_client(config, client)
                .await
                .unwrap();
            let state = Arc::new(Application {
                auth,
                manager,
                _deployment: deployment,
            });
            (crate::route::router(state.clone(), &frontend), state)
        } else if embedded {
            let host = crate::host::startup_local_host(&config).unwrap();
            crate::application_with_host(config, host).await.unwrap()
        } else {
            crate::application(config).await.unwrap()
        };
        let token = state.auth.login_for_test("owner");
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "cookie",
            format!("microcrab_session={token}").parse().unwrap(),
        );
        let owner = state.auth.authenticate(&headers).unwrap().owner;
        state
            .manager
            .projects
            .create(
                &owner,
                serde_json::from_value(json!({
                    "id":"workspace", "name":"Workspace", "root":".", "owners":[owner.clone()]
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        Self {
            router,
            state,
            token,
            _root: root,
        }
    }
    /// Owner-scoped full inspection for assertions (replaces the retired
    /// `GET /sessions/{id}` read model).
    async fn inspect(&self, id: &str) -> Result<Value, microcrab_protocol::application::WireError> {
        let owner = self.owner_of(&self.token);
        self.state.manager.inspect_owned(&owner, id).await
    }
    fn owner_of(&self, token: &str) -> String {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "cookie",
            format!("microcrab_session={token}").parse().unwrap(),
        );
        self.state.auth.authenticate(&headers).unwrap().owner
    }
    async fn request(&self, method: &str, path: &str, body: Option<Value>) -> Response {
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header("cookie", format!("microcrab_session={}", self.token));
        let body = if let Some(body) = body {
            request = request
                .header("origin", "http://127.0.0.1:4096")
                .header("content-type", "application/json");
            Body::from(body.to_string())
        } else {
            Body::empty()
        };
        self.router
            .clone()
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap()
    }
}
async fn body(response: Response) -> Value {
    assert_eq!(response.headers()["cache-control"], "no-store");
    serde_json::from_slice(
        &to_bytes(response.into_body(), 2 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn http_request_ids_correlate_success_and_failure_without_echoing_untrusted_headers() {
    let fixture = Fixture::new(false).await;
    let correlation = uuid::Uuid::new_v4().to_string();
    for authenticated in [true, false] {
        let mut request = Request::builder()
            .uri("/api/v1/sessions")
            .header("x-request-id", &correlation);
        if authenticated {
            request = request.header("cookie", format!("microcrab_session={}", fixture.token));
        }
        let response = fixture
            .router
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if authenticated {
                StatusCode::OK
            } else {
                StatusCode::UNAUTHORIZED
            }
        );
        assert_eq!(response.headers()["x-request-id"], correlation);
    }
    for untrusted in [
        None,
        Some("private-input-not-a-trace-id"),
        Some("../../private/path"),
    ] {
        let mut request = Request::builder().uri("/api/v1/sessions");
        if let Some(value) = untrusted {
            request = request.header("x-request-id", value);
        }
        let response = fixture
            .router
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let id = response.headers()["x-request-id"].to_str().unwrap();
        assert!(uuid::Uuid::parse_str(id).is_ok());
        assert_eq!(id.len(), 36);
        assert_ne!(Some(id), untrusted);
    }
    fixture.state.manager.close().await.unwrap();
}

#[tokio::test]
async fn api_errors_are_json_and_unsafe_requests_require_cookie_origin_json_and_budget() {
    let fixture = Fixture::new(false).await;
    let health = fixture.request("GET", "/api/v1/health", None).await;
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(body(health).await["status"], "ok");
    let unauth = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/sessions")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(body(unauth).await["error"]["retryable"], false);
    for path in ["/api", "/api/v1", "/api/v1/missing", "/api/missing"] {
        let response = fixture.request("GET", path, None).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body(response).await["error"]["code"], "not_found");
    }
    let unsupported = fixture
        .request("POST", "/api/v1/models", Some(json!({})))
        .await;
    assert_eq!(unsupported.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        body(unsupported).await["error"]["code"],
        "method_not_allowed"
    );
    for (origin, content_type, data, status, code) in [
        (
            None,
            "application/json",
            "{}".into(),
            StatusCode::FORBIDDEN,
            "origin_not_allowed",
        ),
        (
            Some("http://untrusted.invalid"),
            "application/json",
            "{}".into(),
            StatusCode::FORBIDDEN,
            "origin_not_allowed",
        ),
        (
            Some("http://127.0.0.1:4096"),
            "text/plain",
            "{}".into(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
        ),
        (
            Some("http://127.0.0.1:4096"),
            "application/json",
            "[]".into(),
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ),
        (
            Some("http://127.0.0.1:4096"),
            "application/json",
            "{".into(),
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ),
        (
            Some("http://127.0.0.1:4096"),
            "application/json",
            " ".repeat(microcrab_protocol::application::MAX_FRAME_BYTES + 1),
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
        ),
    ] {
        let mut request = Request::builder()
            .method("POST")
            .uri("/api/v1/sessions")
            .header("cookie", format!("microcrab_session={}", fixture.token))
            .header("content-type", content_type);
        if let Some(origin) = origin {
            request = request.header("origin", origin);
        }
        let response = fixture
            .router
            .clone()
            .oneshot(request.body(Body::from(data)).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(body(response).await["error"]["code"], code);
    }
    let unsupported_query = fixture
        .request("GET", "/api/v1/sessions?owner=intruder", None)
        .await;
    assert_eq!(unsupported_query.status(), StatusCode::BAD_REQUEST);
    let no_host = fixture
        .request(
            "POST",
            "/api/v1/sessions",
            Some(json!({"project_id":"workspace","operation_id":uuid::Uuid::new_v4().to_string()})),
        )
        .await;
    assert_eq!(no_host.status(), StatusCode::CONFLICT);
    assert_eq!(body(no_host).await["error"]["code"], "host_detached");
    fixture.state.manager.close().await.unwrap();
}

#[tokio::test]
async fn http_fork_returns_native_session_and_new_root_availability() {
    let fixture = Fixture::new(true).await;
    let response = fixture.request("POST", "/api/v1/sessions", Some(json!({
        "project_id":"workspace", "title":"Original", "operation_id":uuid::Uuid::new_v4().to_string()
    }))).await;
    assert_eq!(response.status(), StatusCode::OK);
    let source = body(response).await;
    let id = source["id"].as_str().unwrap();
    let path = format!("/api/v1/sessions/{id}/fork");
    let request = json!({"operation_id":uuid::Uuid::new_v4().to_string(), "expected_revision":source["revision"], "title":"Forked"});
    let response = fixture.request("POST", &path, Some(request.clone())).await;
    assert_eq!(response.status(), StatusCode::OK);
    let fork = body(response).await;
    assert_ne!(fork["id"], source["id"]);
    assert_eq!(fork["title"], "Forked");
    assert_eq!(fork["project_id"], source["project_id"]);
    let response = fixture.request("POST", &path, Some(request)).await;
    assert_eq!(body(response).await, fork);
    let snapshot = fixture.inspect(fork["id"].as_str().unwrap()).await.unwrap();
    assert_eq!(
        snapshot["history_available"],
        json!({"undo":false,"redo":false})
    );
    assert!(snapshot["history"].is_object());
    let invalid = fixture.request("POST", &path, Some(json!({"operation_id":uuid::Uuid::new_v4().to_string(),"checkpoint_id":"not accepted"}))).await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    fixture.state.manager.close().await.unwrap();
}

#[tokio::test]
async fn http_delete_requires_the_current_revision_and_leaves_no_session_behind() {
    let fixture = Fixture::new(true).await;
    let create = json!({"project_id":"workspace","title":"HTTP delete","operation_id":uuid::Uuid::new_v4().to_string()});
    let session = body(
        fixture
            .request("POST", "/api/v1/sessions", Some(create))
            .await,
    )
    .await;
    let id = session["id"].as_str().unwrap().to_owned();
    let revision = session["revision"].as_u64().unwrap();
    let stale = fixture
        .request(
            "DELETE",
            &format!("/api/v1/sessions/{id}"),
            Some(json!({"expected_revision": revision + 7})),
        )
        .await;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    assert_eq!(body(stale).await["error"]["code"], "revision_conflict");
    let deleted = fixture
        .request(
            "DELETE",
            &format!("/api/v1/sessions/{id}"),
            Some(json!({"expected_revision": revision})),
        )
        .await;
    assert_eq!(deleted.status(), StatusCode::OK);
    let deleted = body(deleted).await;
    assert_eq!(deleted["deleted"], true);
    assert_eq!(deleted["session_id"], id);
    assert!(deleted["removed_operation_receipts"].as_u64().unwrap() >= 1);
    let inspect = fixture.inspect(&id).await;
    assert_eq!(inspect.unwrap_err().code, "not_found");
    let again = fixture
        .request(
            "DELETE",
            &format!("/api/v1/sessions/{id}"),
            Some(json!({"expected_revision": revision})),
        )
        .await;
    // Deletion is idempotent for its owner: a retried DELETE reports the
    // tombstone instead of failing.
    assert_eq!(again.status(), StatusCode::OK);
    let again = body(again).await;
    assert_eq!(again["deleted"], true);
    assert_eq!(again["session_id"], id);
    // Archive is a different route with different semantics.
    let archive = fixture
        .request(
            "POST",
            &format!("/api/v1/sessions/{id}/archive"),
            Some(json!({"expected_revision": revision})),
        )
        .await;
    assert_eq!(archive.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn http_creation_receipt_owner_boundary_runtime_and_archive_keep_existing_payloads() {
    let fixture = Fixture::new(true).await;
    let operation = uuid::Uuid::new_v4().to_string();
    let create = json!({"project_id":"workspace","title":"HTTP session","operation_id":operation});
    let response = fixture
        .request("POST", "/api/v1/sessions", Some(create.clone()))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let session = body(response).await;
    let id = session["id"].as_str().unwrap();
    let replay = fixture
        .request("POST", "/api/v1/sessions", Some(create))
        .await;
    assert_eq!(body(replay).await, session);
    let conflict = fixture
        .request(
            "POST",
            "/api/v1/sessions",
            Some(json!({"project_id":"workspace","title":"different","operation_id":operation})),
        )
        .await;
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    assert_eq!(body(conflict).await["error"]["code"], "operation_conflict");
    let receipt = fixture
        .request(
            "GET",
            &format!("/api/v1/sessions/{id}/operations/{operation}"),
            None,
        )
        .await;
    assert_eq!(body(receipt).await["status"], "completed");
    let intruder = fixture.state.auth.login_for_test("intruder");
    assert_eq!(
        fixture
            .state
            .manager
            .inspect_owned(&fixture.owner_of(&intruder), id)
            .await
            .unwrap_err()
            .code,
        "not_found"
    );
    for path in [format!("/api/v1/sessions/{id}/operations/{operation}")] {
        let response = fixture
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("cookie", format!("microcrab_session={intruder}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body(response).await["error"]["code"], "not_found");
    }
    let unknown = fixture
        .request(
            "PATCH",
            &format!("/api/v1/sessions/{id}"),
            Some(json!({"title":"rename","expected_revision":0,"owner":"intruder"})),
        )
        .await;
    assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);
    let renamed = fixture
        .request(
            "PATCH",
            &format!("/api/v1/sessions/{id}"),
            Some(json!({"title":"renamed","expected_revision":0})),
        )
        .await;
    assert_eq!(body(renamed).await["title"], "renamed");
    let close = fixture
        .request(
            "DELETE",
            &format!("/api/v1/sessions/{id}/runtime"),
            Some(json!({})),
        )
        .await;
    assert_eq!(body(close).await, json!({"closed":true}));
    let inspect = fixture.inspect(id).await.unwrap();
    assert_eq!(inspect["session"]["title"], "renamed");
    let open = fixture
        .request(
            "PUT",
            &format!("/api/v1/sessions/{id}/runtime"),
            Some(json!({})),
        )
        .await;
    assert_eq!(body(open).await["session"]["id"], id);
    let archive = fixture
        .request(
            "POST",
            &format!("/api/v1/sessions/{id}/archive"),
            Some(json!({"expected_revision":1})),
        )
        .await;
    assert_eq!(body(archive).await["archived"], true);
    let inspect = fixture.inspect(id).await.unwrap();
    assert_eq!(inspect["session"]["archived"], true);
    let open = fixture
        .request(
            "PUT",
            &format!("/api/v1/sessions/{id}/runtime"),
            Some(json!({})),
        )
        .await;
    assert_eq!(open.status(), StatusCode::CONFLICT);
    assert_eq!(body(open).await["error"]["code"], "archived");
    let receipt = fixture
        .request(
            "GET",
            &format!("/api/v1/sessions/{id}/operations/{operation}"),
            None,
        )
        .await;
    assert_eq!(body(receipt).await["status"], "completed");
    fixture.state.manager.close().await.unwrap();
}

#[tokio::test]
async fn model_selection_http_preserves_session_identity_and_requires_current_revision() {
    let fixture = Fixture::new(true).await;
    let catalog = body(fixture.request("GET", "/api/v1/models", None).await).await;
    let selected = catalog["default_model_selection"].clone();
    let created = body(
        fixture
            .request(
                "POST",
                "/api/v1/sessions",
                Some(json!({
                    "project_id":"workspace", "operation_id":uuid::Uuid::new_v4().to_string(),
                    "model_selection":selected,
                })),
            )
            .await,
    )
    .await;
    let id = created["id"].as_str().unwrap();
    assert_eq!(created["model_selection"], selected);
    let path = format!("/api/v1/sessions/{id}/model");
    let params = json!({
        "expected_revision":created["revision"],
        "provider_id":selected["provider_id"],"model":selected["model"],"variant":"default",
    });
    let response = fixture.request("PUT", &path, Some(params.clone())).await;
    assert_eq!(response.status(), StatusCode::OK);
    let updated = body(response).await;
    assert_eq!(updated["session"]["id"], id);
    assert_eq!(updated["session"]["model_selection"], selected);
    assert_eq!(updated["session"]["model"], selected["model"]);
    let stale = fixture.request("PUT", &path, Some(params)).await;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    assert_eq!(body(stale).await["error"]["code"], "revision_conflict");
    let inspect = fixture.inspect(id).await.unwrap();
    assert_eq!(inspect["session"]["id"], id);
    assert_eq!(
        inspect["session"]["revision"],
        updated["session"]["revision"]
    );
    fixture.state.manager.close().await.unwrap();
}

#[tokio::test]
async fn auth_loss_after_admission_marks_only_mutations_as_unknown() {
    let fixture = Fixture::new(false).await;
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "cookie",
        format!("microcrab_session={}", fixture.token)
            .parse()
            .unwrap(),
    );
    let principal = fixture.state.auth.authenticate(&headers).unwrap();
    let logout = fixture
        .request("POST", "/api/auth/logout", Some(json!({})))
        .await;
    assert_eq!(logout.status(), StatusCode::NO_CONTENT);
    let mutation = http::check_completion_auth(&fixture.state.auth, &principal, true).unwrap_err();
    assert_eq!(mutation.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(body(mutation).await["error"]["code"], "outcome_unknown");
    let read = http::check_completion_auth(&fixture.state.auth, &principal, false).unwrap_err();
    assert_eq!(read.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(body(read).await["error"]["code"], "unauthenticated");
    let admission = fixture
        .request("POST", "/api/v1/sessions", Some(json!({})))
        .await;
    assert_eq!(admission.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(body(admission).await["error"]["code"], "unauthenticated");
    fixture.state.manager.close().await.unwrap();
}
