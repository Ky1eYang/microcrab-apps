//! Privacy boundary for transport diagnostics: only closed labels and validated
//! identities cross into spans. Request bodies and URLs never reach the logger.
#[cfg(test)]
pub(crate) mod testing;

use crate::Application;
use axum::{
    extract::{MatchedPath, Request, State},
    middleware::Next,
    response::Response,
};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Instant};
use tracing::Instrument;

#[derive(Clone)]
pub(super) struct RequestId(pub(super) String);
#[derive(Clone)]
pub(super) struct ErrorCode(pub(super) String);

pub(super) fn identity(value: &str) -> &str {
    if !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        value
    } else {
        "invalid"
    }
}
pub(super) fn owner_prefix(owner: &str) -> String {
    let digest = Sha256::digest(owner.as_bytes());
    digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
pub(super) fn tab_id(headers: &axum::http::HeaderMap) -> String {
    match headers.get("x-client-tab-id") {
        None => "absent".into(),
        Some(value) => value
            .to_str()
            .ok()
            .and_then(|value| uuid::Uuid::parse_str(value).ok())
            .map_or_else(|| "invalid".into(), |id| id.to_string()),
    }
}
fn trigger(headers: &axum::http::HeaderMap) -> &'static str {
    let Some(value) = headers.get("x-client-trigger") else {
        return "absent";
    };
    const ALLOWED: &[&str] = &[
        "resume_project:bootstrap",
        "resume_project:attach",
        "resume_project:select_project",
        "resume_project:create_session",
        "action:connect_host",
        "action:disconnect_host",
        "action:logout",
        "action:refresh_host",
        "relay_lost",
        "connect_error_cleanup",
    ];
    value
        .to_str()
        .ok()
        .and_then(|value| ALLOWED.iter().copied().find(|allowed| *allowed == value))
        .unwrap_or("invalid")
}
pub(super) fn actions(method: &str, params: &serde_json::Value) {
    use crate::manager::{
        commands,
        direct::{Connect, Disconnect},
    };
    use serde::Deserialize;
    let span = tracing::Span::current();
    // Deserialize only the closed lifecycle DTOs before observing their safe
    // fields. Unknown keys and arbitrary values are never diagnostic material.
    match method {
        commands::PROJECT_HOST_CONNECT | "host.relay_attach" => {
            if let Ok(connect) = Connect::deserialize(params) {
                span.record("project_id", identity(&connect.project_id));
                span.record(
                    "has_token",
                    connect
                        .token
                        .as_ref()
                        .is_some_and(|value| !value.is_empty()),
                );
                span.record("allow_legacy_binding", connect.allow_legacy_binding);
                let carrier = url::Url::parse(&connect.url)
                    .ok()
                    .map(|url| match url.scheme() {
                        "grpc" | "grpcs" | "http" | "https" => "grpc",
                        "ws" | "wss" => "ws",
                        _ => "invalid",
                    })
                    .unwrap_or("invalid");
                span.record("carrier", carrier);
            }
        }
        commands::PROJECT_HOST_DISCONNECT => {
            if let Ok(disconnect) = Disconnect::deserialize(params) {
                span.record("project_id", identity(&disconnect.project_id));
                span.record("revoke", disconnect.revoke);
            }
        }
        commands::SESSION_CREATE => {
            if let Some(value) = params.get("project_id").and_then(serde_json::Value::as_str) {
                span.record("project_id", identity(value));
            }
        }
        _ => {}
    }
}

/// One span covers command, attachment and upgrade handlers, including extractor
/// rejection. Upgraded socket futures receive their own lifetime span explicitly.
pub(super) async fn request(
    State(state): State<Arc<Application>>,
    mut request: Request,
    next: Next,
) -> Response {
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| uuid::Uuid::parse_str(value).ok())
        .unwrap_or_else(uuid::Uuid::new_v4)
        .to_string();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("unmatched", MatchedPath::as_str)
        .to_string();
    let method = super::http::all_endpoints()
        .find(|endpoint| {
            endpoint.path == route && endpoint.verb.name() == request.method().as_str()
        })
        .map_or("transport", |endpoint| endpoint.command);
    let path = request.uri().path().to_string();
    let segments: Vec<_> = path.split('/').collect();
    let scoped = |resource| {
        segments
            .windows(2)
            .find(|pair| pair[0] == resource)
            .map_or("", |pair| identity(pair[1]))
    };
    let owner = state
        .auth
        .authenticate(request.headers())
        .ok()
        .map(|principal| owner_prefix(&principal.owner))
        .unwrap_or_default();
    let span = tracing::info_span!("http_api", request_id = %request_id, method, http_method = %request.method(), route = %route, owner_prefix = %owner, project_id = scoped("projects"), session_id = scoped("sessions"), tab_id = %tab_id(request.headers()), trigger = trigger(request.headers()), carrier = tracing::field::Empty, has_token = tracing::field::Empty, revoke = tracing::field::Empty, allow_legacy_binding = tracing::field::Empty);
    request
        .extensions_mut()
        .insert(RequestId(request_id.clone()));
    let started = Instant::now();
    let mut response = async {
        tracing::info!(event = "request.start", "HTTP API request started");
        let response = next.run(request).await;
        let code = response.extensions().get::<ErrorCode>().map_or_else(
            || match response.status().as_u16() {
                101 | 200..=299 => "none",
                401 => "unauthenticated",
                403 => "forbidden",
                400 => "invalid_request",
                404 => "not_found",
                405 => "method_not_allowed",
                _ => "transport_rejected",
            },
            |code| code.0.as_str(),
        );
        if response.status().is_client_error()
            && (route.ends_with("/ws")
                || route.contains("/images/")
                || route.contains("/attachments"))
        {
            tracing::warn!(
                event = "request.reject",
                status = response.status().as_u16(),
                error_code = code,
                "Transport request rejected"
            );
        }
        tracing::info!(
            event = "request.completed",
            status = response.status().as_u16(),
            error_code = code,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "HTTP API request completed"
        );
        response
    }
    .instrument(span)
    .await;
    response
        .headers_mut()
        .insert("x-request-id", request_id.parse().expect("UUID header"));
    response
}

pub(super) fn auth_error(error: crate::auth::AuthError) -> Response {
    use axum::response::IntoResponse;
    let code = match error {
        crate::auth::AuthError::Configuration
        | crate::auth::AuthError::Unavailable
        | crate::auth::AuthError::ConfigurationReason(_)
        | crate::auth::AuthError::UnavailableReason(_) => "auth_unavailable",
        crate::auth::AuthError::Unauthorized | crate::auth::AuthError::UnauthorizedReason(_) => {
            "unauthenticated"
        }
        crate::auth::AuthError::Forbidden => "origin_not_allowed",
        crate::auth::AuthError::InvalidRequest => "invalid_request",
    };
    let mut response = error.into_response();
    response.extensions_mut().insert(ErrorCode(code.into()));
    response
}
