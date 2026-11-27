//! Authenticated user-scoped remote Git Skills repository routes.
use super::{error::Error, http::check_completion_auth};
use crate::Application;
use crate::manager::git_skills::AddGitSkillRepoInput;
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::json;
use std::sync::Arc;

const MAX_BODY: usize = 64 * 1024;

pub(super) fn router() -> Router<Arc<Application>> {
    Router::new()
        .route(
            "/api/v1/users/{user_id}/skills/repos",
            get(list_repos).post(add_repo),
        )
        .route(
            "/api/v1/users/{user_id}/skills/repos/{repo_id}/refresh",
            post(refresh_repo),
        )
        .route(
            "/api/v1/users/{user_id}/skills/repos/{repo_id}",
            axum::routing::delete(delete_repo),
        )
}

#[allow(clippy::result_large_err)]
fn authorize_user(
    state: &Application,
    headers: &axum::http::HeaderMap,
    path_user_id: &str,
    check_origin: bool,
) -> Result<crate::auth::Principal, Response> {
    let principal = state
        .auth
        .authenticate(headers)
        .map_err(super::provenance::auth_error)?;
    state
        .auth
        .check_active(&principal)
        .map_err(super::provenance::auth_error)?;
    if check_origin {
        state
            .auth
            .check_origin(headers)
            .map_err(super::provenance::auth_error)?;
    }
    if principal.owner != path_user_id {
        return Err(Error::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            "Path user_id does not match authenticated user",
        )
        .into_response());
    }
    Ok(principal)
}

async fn list_repos(
    State(state): State<Arc<Application>>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path(user_id) =
        path.map_err(|_| Error::path("user_id must be valid UTF-8").into_response())?;
    let principal = authorize_user(&state, request.headers(), &user_id, false)?;
    let repos = state
        .manager
        .git_skill_repos_for_owner(&principal.owner)
        .await
        .map_err(|e| Error::from(e).into_response())?;
    check_completion_auth(&state.auth, &principal, false)?;
    Ok(Json(json!({ "items": repos })).into_response())
}

async fn add_repo(
    State(state): State<Arc<Application>>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path(user_id) =
        path.map_err(|_| Error::path("user_id must be valid UTF-8").into_response())?;
    let principal = authorize_user(&state, request.headers(), &user_id, true)?;
    let bytes = to_bytes(request.into_body(), MAX_BODY)
        .await
        .map_err(|_| Error::invalid("Request body too large").into_response())?;
    let input: AddGitSkillRepoInput =
        serde_json::from_slice(&bytes).map_err(|e| Error::json(e).into_response())?;
    let created = state
        .manager
        .add_git_skill_repo(&principal.owner, input)
        .await
        .map_err(|e| Error::from(e).into_response())?;
    check_completion_auth(&state.auth, &principal, true)?;
    Ok(Json(created).into_response())
}

async fn refresh_repo(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String)>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path((user_id, repo_id)) =
        path.map_err(|_| Error::path("user_id and repo_id must be valid UTF-8").into_response())?;
    let principal = authorize_user(&state, request.headers(), &user_id, true)?;
    let updated = state
        .manager
        .refresh_git_skill_repo(&principal.owner, &repo_id)
        .await
        .map_err(|e| Error::from(e).into_response())?;
    check_completion_auth(&state.auth, &principal, true)?;
    Ok(Json(updated).into_response())
}

async fn delete_repo(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String)>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path((user_id, repo_id)) =
        path.map_err(|_| Error::path("user_id and repo_id must be valid UTF-8").into_response())?;
    let principal = authorize_user(&state, request.headers(), &user_id, true)?;
    let deleted = state
        .manager
        .remove_git_skill_repo(&principal.owner, &repo_id)
        .await
        .map_err(|e| Error::from(e).into_response())?;
    if !deleted {
        return Err(Error::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "Git skill repository not found",
        )
        .into_response());
    }
    check_completion_auth(&state.auth, &principal, true)?;
    Ok(Json(json!({ "deleted": true, "id": repo_id })).into_response())
}
