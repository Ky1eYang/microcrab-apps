//! Authenticated, Session-scoped raw image upload, separate from JSON control frames.
use super::{error::Error, http::check_completion_auth};
use crate::Application;
use axum::{
    Json,
    extract::{Path, Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;
use microcrab_session::admission::image::{MAX_IMAGE_BYTES, image_type};
use std::sync::Arc;

pub(super) async fn upload(
    State(state): State<Arc<Application>>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
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
    state
        .auth
        .check_origin(request.headers())
        .map_err(IntoResponse::into_response)?;
    let Path(session_id) = path.map_err(|_| {
        Error::path("Attachment path parameters must be valid UTF-8").into_response()
    })?;
    if request.uri().query().is_some_and(|query| !query.is_empty()) {
        return Err(
            Error::invalid("Attachment endpoint does not accept query parameters").into_response(),
        );
    }
    let mut types = request.headers().get_all(header::CONTENT_TYPE).iter();
    let media_type = types
        .next()
        .and_then(|value| value.to_str().ok())
        .filter(|value| image_type(value).is_some())
        .ok_or_else(|| {
            Error::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "invalid_media_type",
                "Upload requires a supported raw image MIME type",
            )
            .into_response()
        })?
        .to_owned();
    if types.next().is_some() {
        return Err(
            Error::invalid("Upload requires exactly one Content-Type header").into_response(),
        );
    }
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .is_some_and(|value| {
            value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .is_some_and(|length| length > MAX_IMAGE_BYTES as u64)
        })
    {
        return Err(too_large());
    }
    // Resolve owner/project visibility before buffering even the first body chunk.
    // Capacity remains held by the tracked storage owner after transfer completes.
    let permit = state
        .manager
        .authorize_attachment(&principal, &session_id)
        .await
        .map_err(|error| Error::from(error).into_response())?;
    let mut stream = request.into_body().into_data_stream();
    let mut data = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| {
            Error::invalid("Attachment request body could not be read").into_response()
        })?;
        if chunk.len() > MAX_IMAGE_BYTES.saturating_sub(data.len()) {
            return Err(too_large());
        }
        data.extend_from_slice(&chunk);
    }
    // Revocation during transfer refuses storage rather than silently admitting bytes.
    state
        .auth
        .check_active(&principal)
        .map_err(IntoResponse::into_response)?;
    let result = state
        .manager
        .upload_attachment(&principal, &session_id, &media_type, data, permit)
        .await;
    check_completion_auth(&state.auth, &principal, true)?;
    Ok(Json(result.map_err(|error| Error::from(error).into_response())?).into_response())
}
pub(super) async fn read(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String)>, axum::extract::rejection::PathRejection>,
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
    let Path((session_id, attachment_id)) = path.map_err(|_| {
        Error::path("Attachment path parameters must be valid UTF-8").into_response()
    })?;
    if request.uri().query().is_some_and(|query| !query.is_empty()) {
        return Err(
            Error::invalid("Attachment endpoint does not accept query parameters").into_response(),
        );
    }
    let result = state
        .manager
        .read_attachment(&principal, &session_id, &attachment_id)
        .await;
    check_completion_auth(&state.auth, &principal, false)?;
    let image = result.map_err(|error| Error::from(error).into_response())?;
    Ok((
        [
            (header::CONTENT_TYPE, image.media_type.as_str()),
            (header::CACHE_CONTROL, "private, no-store"),
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

fn too_large() -> Response {
    Error::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        "request_too_large",
        "Image exceeds the 20 MiB attachment byte limit",
    )
    .into_response()
}
