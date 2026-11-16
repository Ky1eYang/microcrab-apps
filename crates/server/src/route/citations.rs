//! Bounded on-click delivery. No browser-provided registration grants authority.
use super::{error::Error, http::check_completion_auth};
use crate::{Application, manager::citations::Request as CitationRequest};
use axum::{
    extract::{Path, Query, Request, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(super) async fn read(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String)>, axum::extract::rejection::PathRejection>,
    query: Result<Query<CitationRequest>, axum::extract::rejection::QueryRejection>,
    request: Request,
) -> Result<Response, Response> {
    let principal = state
        .auth
        .authenticate(request.headers())
        .map_err(IntoResponse::into_response)?;
    state
        .auth
        .check_active(&principal)
        .map_err(IntoResponse::into_response)?;
    let Path((session, message)) = path.map_err(|_| {
        Error::path("Citation requires valid session and message identities").into_response()
    })?;
    let Query(query) = query.map_err(|error| Error::query(error).into_response())?;
    let cancellation = CancellationToken::new();
    // A disconnected waiter cancels the canonical Host operation, while Manager
    // continues owning its admission/shutdown task until cleanup completes.
    let _cancel = cancellation.clone().drop_guard();
    let result = state
        .manager
        .citation(&principal, &session, &message, query, cancellation)
        .await;
    check_completion_auth(&state.auth, &principal, false)?;
    let content = result.map_err(|error| Error::from(error).into_response())?;
    response(content)
}

pub(super) fn response(content: crate::manager::citations::Content) -> Result<Response, Response> {
    let filename: String =
        url::form_urlencoded::byte_serialize(content.filename.as_bytes()).collect();
    let disposition = format!(
        "{}; filename=\"download\"; filename*=UTF-8''{filename}",
        if content.media_type.starts_with("image/") {
            "inline"
        } else {
            "attachment"
        }
    );
    let mut response = (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content.media_type),
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (
                header::CONTENT_SECURITY_POLICY,
                "default-src 'none'; sandbox",
            ),
        ],
        content.data,
    )
        .into_response();
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&disposition)
            .map_err(|_| Error::internal("Invalid download metadata").into_response())?,
    );
    Ok(response)
}
