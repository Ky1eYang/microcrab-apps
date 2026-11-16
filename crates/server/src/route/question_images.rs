//! A question grants only its own Markdown image references while its exact
//! browser executor lease and pending invocation remain live.
use super::{error::Error, http::check_completion_auth};
use crate::{Application, manager::question_images::Request as ImageRequest};
use axum::{
    extract::{Path, Query, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(super) async fn read(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String)>, axum::extract::rejection::PathRejection>,
    query: Result<Query<ImageRequest>, axum::extract::rejection::QueryRejection>,
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
    let Path((root, invocation)) = path.map_err(|_| {
        Error::path("Question image requires valid Session and invocation identities")
            .into_response()
    })?;
    let Query(query) = query.map_err(|error| Error::query(error).into_response())?;
    let mut leases = request
        .headers()
        .get_all("x-microcrab-executor-lease")
        .iter();
    let lease = leases
        .next()
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .ok_or_else(|| {
            Error::new(
                StatusCode::FORBIDDEN,
                "permission_denied",
                "Question image requires the current executor lease",
            )
            .into_response()
        })?
        .to_owned();
    if leases.next().is_some() {
        return Err(Error::new(
            StatusCode::FORBIDDEN,
            "permission_denied",
            "Question image requires one exact executor lease",
        )
        .into_response());
    }
    let cancellation = CancellationToken::new();
    let _cancel = cancellation.clone().drop_guard();
    let result = state
        .manager
        .question_image(&principal, &root, &invocation, query, lease, cancellation)
        .await;
    check_completion_auth(&state.auth, &principal, false)?;
    super::citations::response(result.map_err(|error| Error::from(error).into_response())?)
}
