//! Pull-only projection of the owner-selected frozen prompt. Never a socket payload.
use super::{error::Error, http::check_completion_auth};
use crate::{Application, manager::prompt_history::Selection};
use axum::{
    extract::{Path, Query, Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

pub(super) async fn read(
    State(state): State<Arc<Application>>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    query: Result<Query<Selection>, axum::extract::rejection::QueryRejection>,
    request: Request,
) -> Result<Response, Response> {
    let principal = state
        .auth
        .authenticate(request.headers())
        .map_err(super::provenance::auth_error)?;
    state
        .auth
        .check_active(&principal)
        .map_err(super::provenance::auth_error)?;
    let Path(session_id) =
        path.map_err(|_| Error::path("Session ID must be valid UTF-8").into_response())?;
    let Query(query) = query.map_err(|error| Error::query(error).into_response())?;
    let result = state
        .manager
        .frozen_prompt(&principal, session_id, query)
        .await;
    check_completion_auth(&state.auth, &principal, false)?;
    let body = result.map_err(|wire| {
        if wire.code == "stale_history" {
            Error::new(StatusCode::CONFLICT, &wire.code, &wire.message)
        } else {
            Error::from(wire)
        }
        .into_response()
    })?;
    Ok((
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        body,
    )
        .into_response())
}
