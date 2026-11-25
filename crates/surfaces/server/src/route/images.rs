//! Read-only image delivery; Manager owns authorization and durable selection.
use super::{error::Error, http::check_completion_auth};
use crate::Application;
use axum::{
    extract::{Path, Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

pub(super) async fn read(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String, usize)>, axum::extract::rejection::PathRejection>,
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
    let Path((session_id, message_id, part_index)) =
        path.map_err(|_| Error::path("Image path requires valid UTF-8 session_id and message_id and an unsigned integer part_index").into_response())?;
    if request.uri().query().is_some_and(|query| !query.is_empty()) {
        return Err(
            Error::invalid("Image endpoint does not accept query parameters").into_response(),
        );
    }
    let result = state
        .manager
        .image(&principal, &session_id, &message_id, part_index)
        .await;
    // Do not return historical bytes after logout/revocation during storage IO.
    check_completion_auth(&state.auth, &principal, false)?;
    let image = result.map_err(|error| Error::from(error).into_response())?;
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, image.media_type),
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (
                header::CONTENT_SECURITY_POLICY,
                "default-src 'none'; sandbox",
            ),
        ],
        image.data,
    )
        .into_response())
}
