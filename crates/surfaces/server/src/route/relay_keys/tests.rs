use super::*;
use axum::http::Request;
use axum::{
    Router,
    body::{Body, to_bytes},
    routing::get,
};
use serde_json::{Value, json};
use tower::ServiceExt as _;

async fn body(response: Response, status: StatusCode) -> Value {
    assert_eq!(response.status(), status);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    serde_json::from_slice(&to_bytes(response.into_body(), 32 * 1024).await.unwrap()).unwrap()
}

#[tokio::test]
async fn relay_key_management_is_cookie_origin_and_owner_scoped() {
    let root = tempfile::tempdir().unwrap();
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
    config.resolve(root.path()).unwrap();
    #[cfg(feature = "storage-sqlite")]
    {
        config.storage = crate::config::Storage::Sqlite;
    }
    #[cfg(not(feature = "storage-sqlite"))]
    {
        config.storage = crate::config::Storage::File;
    }
    let auth = crate::auth::Auth::new(config.auth.clone()).unwrap();
    let manager = crate::manager::Manager::new(config).await.unwrap();
    let us = format!("microcrab_session={}", auth.login_for_test("owner"));
    let other = format!("microcrab_session={}", auth.login_for_test("other"));
    let mut headers = HeaderMap::new();
    headers.insert(header::COOKIE, us.parse().unwrap());
    let owner = auth.authenticate(&headers).unwrap().owner;
    let state = Arc::new(Application { auth, manager });
    let router = Router::new()
        .route("/api/v1/users/{user_id}/api-keys", get(list).post(create))
        .route(
            "/api/v1/users/{user_id}/api-keys/{key_id}",
            axum::routing::patch(update).delete(revoke),
        )
        .with_state(state.clone());
    let base = format!("/api/v1/users/{owner}/api-keys");
    let success = json!({"name":"Test client", "allowed_providers":["configured"]});
    let send = |method: &str,
                path: &str,
                auth: &str,
                origin: Option<&str>,
                content_type: &str,
                json: Option<Value>| {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header(header::CONTENT_TYPE, content_type);
        let request = if auth.starts_with("Bearer ") {
            request.header(header::AUTHORIZATION, auth)
        } else {
            request.header(header::COOKIE, auth)
        };
        let request = if let Some(origin) = origin {
            request.header(header::ORIGIN, origin)
        } else {
            request
        };
        router.clone().oneshot(
            request
                .body(json.map_or_else(Body::empty, |value| Body::from(value.to_string())))
                .unwrap(),
        )
    };
    let origin = Some("http://127.0.0.1:4096");
    let mime = "application/json";
    body(
        send("GET", &base, &other, None, mime, None).await.unwrap(),
        StatusCode::FORBIDDEN,
    )
    .await;
    body(
        send("GET", &base, "Bearer mc_sk_wrong", None, mime, None)
            .await
            .unwrap(),
        StatusCode::UNAUTHORIZED,
    )
    .await;
    body(
        send("POST", &base, &us, None, mime, Some(success.clone()))
            .await
            .unwrap(),
        StatusCode::FORBIDDEN,
    )
    .await;
    body(
        send(
            "POST",
            &base,
            &us,
            origin,
            "text/plain",
            Some(success.clone()),
        )
        .await
        .unwrap(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
    )
    .await;
    let created = body(
        send("POST", &base, &us, origin, mime, Some(success))
            .await
            .unwrap(),
        StatusCode::CREATED,
    )
    .await;
    let secret = created["secret"].as_str().unwrap();
    assert!(
        state
            .manager
            .relay_keys
            .lookup(secret)
            .await
            .unwrap()
            .is_some()
    );
    let mixed = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(&base)
                .header(header::COOKIE, &us)
                .header(header::AUTHORIZATION, format!("Bearer {secret}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    body(mixed, StatusCode::UNAUTHORIZED).await;
    let url = format!("{base}/{}", created["key"]["id"].as_str().unwrap());
    body(
        send(
            "DELETE",
            &url,
            &us,
            origin,
            mime,
            Some(json!({"expected_revision": 0})),
        )
        .await
        .unwrap(),
        StatusCode::CONFLICT,
    )
    .await;
    let response = body(
        send(
            "DELETE",
            &url,
            &us,
            origin,
            mime,
            Some(json!({"expected_revision": 1})),
        )
        .await
        .unwrap(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(response["key"]["revoked"], true);
    assert!(response.get("secret").is_none());
    assert!(
        state
            .manager
            .relay_keys
            .lookup(secret)
            .await
            .unwrap()
            .is_none()
    );
    let listed = body(
        send("GET", &base, &us, None, mime, None).await.unwrap(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(listed["keys"][0], response["key"]);
    assert!(!listed.to_string().contains(secret));
    state.manager.close().await.unwrap();
}
