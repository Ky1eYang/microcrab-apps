//! Raw ingress for Session-owned image variables. There is deliberately no GET.
use super::{error::Error, http::check_completion_auth, provenance::auth_error};
use crate::{Application, auth::Principal};
use axum::{
    Json,
    extract::{Path, Request, State, rejection::PathRejection},
    http::{HeaderMap, HeaderName, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;
use microcrab_session::{
    admission::image::{MAX_IMAGE_BYTES, image_type},
    state::VariableWitness,
};
use std::{sync::Arc, time::Duration};

const RUNTIME_INSTANCE_ID: HeaderName = HeaderName::from_static("x-microcrab-runtime-instance-id");
const VARIABLES_REVISION: HeaderName = HeaderName::from_static("x-microcrab-variables-revision");
const BODY_TIMEOUT: Duration = Duration::from_secs(30);

fn single(headers: &HeaderMap, name: HeaderName) -> Option<&str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    values.next().is_none().then_some(value)
}

fn decimal(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit())
}

type ResourcePath = Result<Path<(String, String)>, PathRejection>;

struct Target {
    session: String,
    name: String,
    expected: VariableWitness,
    principal: Principal,
}

#[expect(
    clippy::result_large_err,
    reason = "Raw route preflight returns the shared Axum error envelope"
)]
fn target(state: &Application, path: ResourcePath, request: &Request) -> Result<Target, Response> {
    let headers = request.headers();
    let principal = state.auth.authenticate(headers).map_err(auth_error)?;
    state.auth.check_origin(headers).map_err(auth_error)?;
    let Path((session, name)) = path.map_err(|_| {
        Error::path("Byte variable path parameters must be valid UTF-8").into_response()
    })?;
    if request.uri().query().is_some() {
        return Err(
            Error::invalid("Byte variable endpoint does not accept query parameters")
                .into_response(),
        );
    }
    let invalid = || {
        Error::invalid("One runtime instance ID and decimal variables revision are required")
            .into_response()
    };
    let instance = single(headers, RUNTIME_INSTANCE_ID)
        .filter(|value| uuid::Uuid::parse_str(value).is_ok())
        .ok_or_else(invalid)?;
    let revision = single(headers, VARIABLES_REVISION)
        .filter(|value| decimal(value) && value.parse::<u64>().is_ok())
        .ok_or_else(invalid)?;
    Ok(Target {
        session,
        name,
        expected: VariableWitness {
            runtime_instance_id: instance.to_owned(),
            variables_revision: revision.to_owned(),
        },
        principal,
    })
}

pub(super) async fn upload(
    State(state): State<Arc<Application>>,
    path: ResourcePath,
    request: Request,
) -> Result<Response, Response> {
    let target = target(&state, path, &request)?;
    let headers = request.headers();
    let media_type = single(headers, header::CONTENT_TYPE)
        .and_then(image_type)
        .filter(|_| !headers.contains_key(header::CONTENT_ENCODING))
        .ok_or_else(|| {
            Error::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "invalid_media_type",
                "Upload requires one canonical raw image Content-Type and no Content-Encoding",
            )
            .into_response()
        })?;
    let length = single(headers, header::CONTENT_LENGTH)
        .filter(|value| decimal(value))
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|length| *length != 0 && !headers.contains_key(header::TRANSFER_ENCODING))
        .ok_or_else(|| {
            Error::invalid("Upload requires one positive Content-Length and no Transfer-Encoding")
                .into_response()
        })?;
    if length > MAX_IMAGE_BYTES as u64 {
        return Err(Error::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
            "Upload exceeds the image byte limit",
        )
        .into_response());
    }
    // Authenticate and reserve both request and Core byte capacity before the
    // first data poll, without holding a Manager lock while the client streams.
    let mut upload = state
        .manager
        .prepare_bytes_upload(
            &target.principal,
            &target.session,
            &target.name,
            media_type,
            length,
            target.expected,
        )
        .await
        .map_err(wire_error)?;
    state
        .auth
        .check_active(&target.principal)
        .map_err(auth_error)?;
    let closed = upload.closed.clone();
    let mut stream = request.into_body().into_data_stream();
    let read = async {
        let mut received = 0_u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| Error::invalid("Image upload body could not be read"))?;
            if chunk.len() as u64 > length - received {
                return Err(Error::invalid("Image upload does not match Content-Length"));
            }
            received += chunk.len() as u64;
            upload.append(&chunk).map_err(Error::from)?;
        }
        if received != length {
            return Err(Error::invalid("Image upload does not match Content-Length"));
        }
        Ok(())
    };
    tokio::select! {
        biased;
        _ = closed.cancelled() => return Err(Error::from(microcrab_protocol::application::WireError::new("closed", "Session closed during image upload")).into_response()),
        result = tokio::time::timeout(BODY_TIMEOUT, read) => {
            result.map_err(|_| timed_out())?.map_err(IntoResponse::into_response)?;
        }
    }
    let result = state
        .manager
        .commit_bytes_upload(&state.auth, &target.principal, &target.session, upload)
        .await;
    let receipt = result.map_err(wire_error)?;
    check_completion_auth(&state.auth, &target.principal, true)?;
    Ok(Json(receipt).into_response())
}

pub(super) async fn remove(
    State(state): State<Arc<Application>>,
    path: ResourcePath,
    request: Request,
) -> Result<Response, Response> {
    let target = target(&state, path, &request)?;
    // DELETE accepts no body; do not consume a stream just to validate emptiness.
    use axum::body::HttpBody;
    if !request.body().is_end_stream()
        || request.headers().contains_key(header::TRANSFER_ENCODING)
        || (request.headers().contains_key(header::CONTENT_LENGTH)
            && single(request.headers(), header::CONTENT_LENGTH) != Some("0"))
    {
        return Err(Error::invalid("Byte variable removal does not accept a body").into_response());
    }
    let result = state
        .manager
        .delete_bytes_variable(
            &state.auth,
            &target.principal,
            &target.session,
            target.name,
            target.expected,
        )
        .await;
    let receipt = result.map_err(wire_error)?;
    check_completion_auth(&state.auth, &target.principal, true)?;
    Ok(Json(receipt).into_response())
}

fn wire_error(error: microcrab_protocol::application::WireError) -> Response {
    Error::from(error).into_response()
}

fn timed_out() -> Response {
    Error::new(
        StatusCode::REQUEST_TIMEOUT,
        "upload_timeout",
        "Image upload timed out before commit",
    )
    .into_response()
}
