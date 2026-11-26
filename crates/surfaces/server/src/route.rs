//! Central transport assembly. Business effects stay owned by Manager; routes own
//! HTTP authentication and socket delivery, with one API fallback/error vocabulary.
mod attachment;
mod children;
mod citations;
mod dashboard;
mod endpoint;
pub(crate) mod error;
mod external_inbox;
mod external_speakers;
mod host;
mod http;
mod images;
mod library;
mod preferences;
pub(crate) mod project;
mod prompt_history;
mod provenance;
mod question_images;
pub(crate) mod relay;
mod relay_keys;
pub(crate) mod session;
mod settings;
mod socket;
pub(crate) mod system;
#[cfg(all(test, feature = "storage-file"))]
mod tests;

use crate::{Application, auth};
use axum::{Router, middleware, routing::get};
use std::{path::Path, sync::Arc};
use tower_http::services::{ServeDir, ServeFile};

pub(crate) fn router(state: Arc<Application>, frontend: &Path) -> Router {
    #[cfg(feature = "dev")]
    let provider = auth::dev::router(state.auth.clone());
    let api = http::router(state.clone()) // include /api/v1/*
        .route("/api/v1/host/outbound", get(host::outbound))
        .route(
            "/api/v1/users/{user_id}/api-keys",
            get(relay_keys::list).post(relay_keys::create),
        )
        .route(
            "/api/v1/users/{user_id}/api-keys/{key_id}",
            axum::routing::patch(relay_keys::update).delete(relay_keys::revoke),
        )
        .route(
            "/api/v1/users/{user_id}/dashboard/summary",
            get(dashboard::summary),
        )
        .route(
            "/api/v1/users/{user_id}/dashboard/cache-diagnostics",
            get(dashboard::cache_diagnostics),
        )
        .route(
            "/api/v1/users/{user_id}/dashboard/logs",
            get(dashboard::logs),
        )
        .route(
            "/api/v1/users/{user_id}/dashboard/logs.csv",
            get(dashboard::export_logs),
        )
        .route(
            "/api/v1/users/{user_id}/preferences",
            get(preferences::read).put(preferences::update),
        )
        .route("/api/v1/ws", get(socket::global))
        .route("/api/v1/sessions/{session_id}/ws", get(socket::session))
        .route("/api/health", get(http::health))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            provenance::request,
        ))
        .with_state(state.clone())
        .merge(auth::router(state.auth.clone()))
        .route("/api", axum::routing::any(error::not_found))
        .route("/api/{*path}", axum::routing::any(error::not_found))
        .method_not_allowed_fallback(error::method_not_allowed)
        .layer(middleware::from_fn(error::no_store));
    #[cfg(feature = "dev")]
    let api = api.merge(provider);
    Router::new()
        .merge(api)
        .merge(relay::router(state))
        .fallback_service(
            ServeDir::new(frontend).not_found_service(ServeFile::new(frontend.join("index.html"))),
        )
        .layer(middleware::from_fn(normalize_loopback_host))
}

async fn normalize_loopback_host(
    req: axum::extract::Request,
    next: middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if !matches!(req.uri().path(), "/v1")
        && !req.uri().path().starts_with("/v1/")
        && req.method() == axum::http::Method::GET
        && let Some(host) = req
            .headers()
            .get(axum::http::header::HOST)
            .and_then(|h| h.to_str().ok())
        && host.starts_with("localhost")
    {
        let port = host.strip_prefix("localhost").unwrap_or("");
        let uri = req.uri();
        let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
        let target = format!("http://127.0.0.1{port}{path_and_query}");
        return axum::response::Redirect::temporary(&target).into_response();
    }
    next.run(req).await
}
