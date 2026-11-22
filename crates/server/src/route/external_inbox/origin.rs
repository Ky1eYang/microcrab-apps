//! Route-local bearer CORS; do not grant cross-origin access to owner APIs.
use super::{Application, Error};
use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::sync::Arc;

fn forbidden() -> Error {
    Error::new(
        StatusCode::FORBIDDEN,
        "origin_not_allowed",
        "Origin is not allowed",
    )
}

pub(super) fn check(state: &Application, headers: &HeaderMap) -> Result<(), Error> {
    if headers.contains_key(header::ORIGIN) {
        state.auth.check_origin(headers).map_err(|_| forbidden())?;
    }
    Ok(())
}

pub(crate) async fn cors(
    State(state): State<Arc<Application>>,
    request: Request,
    next: Next,
) -> Response {
    let origin = if request.headers().contains_key(header::ORIGIN) {
        if let Err(error) = check(&state, request.headers()) {
            return error.into_response();
        }
        request.headers().get(header::ORIGIN).cloned()
    } else {
        None
    };
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    if let Some(origin) = origin {
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }
    headers.append(header::VARY, header::HeaderValue::from_static("Origin"));
    response
}

fn preflight(headers: &HeaderMap, post_allowed: bool) -> Result<Response, Error> {
    let method =
        super::single(headers, "access-control-request-method").map_err(|_| forbidden())?;
    let (method, permitted) = match method {
        "GET" => (
            "GET",
            "authorization, accept, x-microcrab-dialogue-id, last-event-id",
        ),
        "POST" if post_allowed => ("POST", "authorization, content-type, idempotency-key"),
        _ => return Err(forbidden()),
    };
    for requested in headers.get_all(header::ACCESS_CONTROL_REQUEST_HEADERS) {
        let requested = requested.to_str().map_err(|_| forbidden())?;
        if !requested.split(',').all(|request| {
            permitted
                .split(", ")
                .any(|header| header.eq_ignore_ascii_case(request.trim()))
        }) {
            return Err(forbidden());
        }
    }
    Ok((
        StatusCode::NO_CONTENT,
        [
            (header::ACCESS_CONTROL_ALLOW_METHODS, method),
            (header::ACCESS_CONTROL_ALLOW_HEADERS, permitted),
            (
                header::VARY,
                "Access-Control-Request-Method, Access-Control-Request-Headers",
            ),
        ],
    )
        .into_response())
}

pub(crate) async fn inbox_preflight(headers: HeaderMap) -> Result<Response, Error> {
    if !headers.contains_key(header::ORIGIN) {
        return Err(forbidden());
    }
    preflight(&headers, true)
}

pub(crate) async fn observe_preflight(headers: HeaderMap) -> Result<Response, Error> {
    if !headers.contains_key(header::ORIGIN) {
        return Err(forbidden());
    }
    preflight(&headers, false)
}
