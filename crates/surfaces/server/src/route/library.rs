//! Authenticated user-scoped Personal Content Library HTTP routes.
//!
//! Every handler authenticates the caller AND verifies `user_id == principal.owner`
//! so one authenticated principal can never read or mutate another user's library.
use super::{endpoint::parse_query, error::Error, http::check_completion_auth};
use crate::Application;
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Path, Query, Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, put},
};
use base64::Engine as _;
use futures_util::StreamExt;
use microcrab_session::state::reference::{
    CreateLibraryObject, LocalBlobOrigin, ShortcutTarget, UpdateLibraryObject, canonical_mime,
};
use serde::Deserialize;
use std::sync::Arc;

const MAX_JSON_BODY: usize = 32 * 1024 * 1024 + 1024 * 1024;

pub(super) fn router() -> Router<Arc<Application>> {
    Router::new()
        .route("/api/v1/users/{user_id}/library/quota", get(get_quota))
        .route(
            "/api/v1/users/{user_id}/library/objects",
            get(list_objects).post(create_object),
        )
        .route(
            "/api/v1/users/{user_id}/library/objects/{ref_id}",
            get(get_object).patch(update_object).delete(delete_object),
        )
        .route(
            "/api/v1/users/{user_id}/library/objects/{ref_id}/content",
            get(read_content),
        )
        .route(
            "/api/v1/users/{user_id}/library/objects/{ref_id}/mounts/sessions/{session_id}",
            put(mount_session).delete(unmount_session),
        )
        .route(
            "/api/v1/users/{user_id}/library/objects/{ref_id}/mounts/projects/{project_id}",
            put(mount_project).delete(unmount_project),
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

#[derive(Default, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct EmptyQuery {}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListQuery {
    #[serde(default)]
    source_kind: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    q: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadQuery {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    mount_session_id: Option<String>,
    #[serde(default)]
    mount_project_id: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContentQuery {
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonCreateBody {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    target: Option<ShortcutTarget>,
    #[serde(default)]
    media_type: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    base64: Option<String>,
    #[serde(default)]
    mount_session_id: Option<String>,
    #[serde(default)]
    mount_project_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonUpdateBody {
    #[serde(default)]
    expected_revision: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    media_type: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    base64: Option<String>,
    #[serde(default)]
    target: Option<ShortcutTarget>,
}

async fn get_quota(
    State(state): State<Arc<Application>>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path(user_id) =
        path.map_err(|_| Error::path("user_id path parameter must be valid UTF-8").into_response())?;
    let principal = authorize_user(&state, request.headers(), &user_id, false)?;
    parse_query::<EmptyQuery>(request.uri()).map_err(IntoResponse::into_response)?;
    let quota = state
        .manager
        .library_quota(&principal)
        .await
        .map_err(|e| Error::from(e).into_response())?;
    check_completion_auth(&state.auth, &principal, false)?;
    Ok(Json(quota).into_response())
}

async fn list_objects(
    State(state): State<Arc<Application>>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path(user_id) =
        path.map_err(|_| Error::path("user_id path parameter must be valid UTF-8").into_response())?;
    let principal = authorize_user(&state, request.headers(), &user_id, false)?;
    let Query(query) =
        Query::<ListQuery>::try_from_uri(request.uri()).map_err(|e| Error::query(e).into_response())?;
    let list = state
        .manager
        .library_list(
            &principal,
            query.source_kind.as_deref(),
            query.session_id.as_deref(),
            query.project_id.as_deref(),
            query.q.as_deref(),
        )
        .await
        .map_err(|e| Error::from(e).into_response())?;
    check_completion_auth(&state.auth, &principal, false)?;
    Ok(Json(list).into_response())
}

async fn get_object(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String)>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path((user_id, ref_id)) = path.map_err(|_| {
        Error::path("user_id and ref_id path parameters must be valid UTF-8").into_response()
    })?;
    let principal = authorize_user(&state, request.headers(), &user_id, false)?;
    parse_query::<EmptyQuery>(request.uri()).map_err(IntoResponse::into_response)?;
    let object = state
        .manager
        .library_get(&principal, &ref_id)
        .await
        .map_err(|e| Error::from(e).into_response())?;
    check_completion_auth(&state.auth, &principal, false)?;
    Ok(Json(object).into_response())
}

async fn create_object(
    State(state): State<Arc<Application>>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path(user_id) =
        path.map_err(|_| Error::path("user_id path parameter must be valid UTF-8").into_response())?;
    let principal = authorize_user(&state, request.headers(), &user_id, true)?;
    let permit = state
        .manager
        .library_permit()
        .map_err(|e| Error::from(e).into_response())?;
    let mut types = request.headers().get_all(header::CONTENT_TYPE).iter();
    let content_type = types
        .next()
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| {
            Error::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "Upload or shortcut creation requires Content-Type",
            )
            .into_response()
        })?;
    if types.next().is_some() {
        return Err(Error::invalid("Exactly one Content-Type header is required").into_response());
    }
    let mime_base = content_type
        .split(';')
        .next()
        .unwrap_or(&content_type)
        .trim()
        .to_ascii_lowercase();
    if mime_base == "application/json" {
        parse_query::<EmptyQuery>(request.uri()).map_err(IntoResponse::into_response)?;
        let bytes = to_bytes(request.into_body(), MAX_JSON_BODY)
            .await
            .map_err(|_| {
                Error::new(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request_too_large",
                    "JSON library create request exceeds body limit",
                )
                .into_response()
            })?;
        let body: JsonCreateBody =
            serde_json::from_slice(&bytes).map_err(|e| Error::json(e).into_response())?;
        let req = match (body.target, body.text, body.base64) {
            (Some(target), None, None) => CreateLibraryObject::Shortcut {
                title: body.title.unwrap_or_default(),
                target,
            },
            (None, Some(text), None) => CreateLibraryObject::LocalBlob {
                id: None,
                title: body.title.unwrap_or_else(|| "Untitled text".into()),
                media_type: body.media_type.unwrap_or_else(|| "text/plain".into()),
                data: text.into_bytes(),
                origin: LocalBlobOrigin::Upload,
                origin_execution_id: None,
            },
            (None, None, Some(b64)) => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(b64.as_bytes())
                    .map_err(|_| Error::invalid("Invalid base64 payload").into_response())?;
                CreateLibraryObject::LocalBlob {
                    id: None,
                    title: body.title.unwrap_or_else(|| "Uploaded binary".into()),
                    media_type: body
                        .media_type
                        .unwrap_or_else(|| "application/octet-stream".into()),
                    data: decoded,
                    origin: LocalBlobOrigin::Upload,
                    origin_execution_id: None,
                }
            }
            _ => {
                return Err(Error::invalid(
                    "JSON library create requires exactly one of target, text, or base64",
                )
                .into_response());
            }
        };
        let created = state
            .manager
            .library_create(
                &principal,
                req,
                body.mount_session_id,
                body.mount_project_id,
                permit,
            )
            .await
            .map_err(|e| Error::from(e).into_response())?;
        check_completion_auth(&state.auth, &principal, true)?;
        return Ok(Json(created).into_response());
    }

    let Query(query) = Query::<UploadQuery>::try_from_uri(request.uri())
        .map_err(|e| Error::query(e).into_response())?;
    let media_type = canonical_mime(&mime_base)
        .map_err(|_| Error::invalid("Unsupported Content-Type for library upload").into_response())?;
    let max_object = state.manager.config.personal_library.max_object_bytes;
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .is_some_and(|v| {
            v.to_str()
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .is_some_and(|len| len > max_object as u64)
        })
    {
        return Err(Error::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
            "Upload exceeds maximum personal library object size",
        )
        .into_response());
    }
    let mut stream = request.into_body().into_data_stream();
    let mut data = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| {
            Error::invalid("Personal library upload body could not be read").into_response()
        })?;
        if chunk.len() > max_object.saturating_sub(data.len()) {
            return Err(Error::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                "Upload exceeds maximum personal library object size",
            )
            .into_response());
        }
        data.extend_from_slice(&chunk);
    }
    state
        .auth
        .check_active(&principal)
        .map_err(super::provenance::auth_error)?;
    let title = query
        .title
        .filter(|t| !t.trim().is_empty())
        .unwrap_or_else(|| "Uploaded file".into());
    let created = state
        .manager
        .library_create(
            &principal,
            CreateLibraryObject::LocalBlob {
                id: None,
                title,
                media_type,
                data,
                origin: LocalBlobOrigin::Upload,
                origin_execution_id: None,
            },
            query.mount_session_id,
            query.mount_project_id,
            permit,
        )
        .await
        .map_err(|e| Error::from(e).into_response())?;
    check_completion_auth(&state.auth, &principal, true)?;
    Ok(Json(created).into_response())
}

async fn update_object(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String)>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path((user_id, ref_id)) = path.map_err(|_| {
        Error::path("user_id and ref_id path parameters must be valid UTF-8").into_response()
    })?;
    let principal = authorize_user(&state, request.headers(), &user_id, true)?;
    parse_query::<EmptyQuery>(request.uri()).map_err(IntoResponse::into_response)?;
    let bytes = to_bytes(request.into_body(), MAX_JSON_BODY)
        .await
        .map_err(|_| {
            Error::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                "Update body exceeds budget",
            )
            .into_response()
        })?;
    let body: JsonUpdateBody =
        serde_json::from_slice(&bytes).map_err(|e| Error::json(e).into_response())?;
    let data = match (body.text, body.base64) {
        (Some(text), None) => Some(text.into_bytes()),
        (None, Some(b64)) => Some(
            base64::engine::general_purpose::STANDARD
                .decode(b64.as_bytes())
                .map_err(|_| Error::invalid("Invalid base64 payload").into_response())?,
        ),
        (None, None) => None,
        (Some(_), Some(_)) => {
            return Err(Error::invalid("Specify at most one of text or base64").into_response());
        }
    };
    let updated = state
        .manager
        .library_update(
            &principal,
            &ref_id,
            UpdateLibraryObject {
                expected_revision: body.expected_revision,
                title: body.title,
                media_type: body.media_type,
                data,
                target: body.target,
            },
        )
        .await
        .map_err(|e| Error::from(e).into_response())?;
    check_completion_auth(&state.auth, &principal, true)?;
    Ok(Json(updated).into_response())
}

async fn delete_object(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String)>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path((user_id, ref_id)) = path.map_err(|_| {
        Error::path("user_id and ref_id path parameters must be valid UTF-8").into_response()
    })?;
    let principal = authorize_user(&state, request.headers(), &user_id, true)?;
    parse_query::<EmptyQuery>(request.uri()).map_err(IntoResponse::into_response)?;
    let deleted = state
        .manager
        .library_delete(&principal, &ref_id)
        .await
        .map_err(|e| Error::from(e).into_response())?;
    check_completion_auth(&state.auth, &principal, true)?;
    Ok(Json(deleted).into_response())
}

async fn read_content(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String)>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path((user_id, ref_id)) = path.map_err(|_| {
        Error::path("user_id and ref_id path parameters must be valid UTF-8").into_response()
    })?;
    let principal = authorize_user(&state, request.headers(), &user_id, false)?;
    let Query(query) = Query::<ContentQuery>::try_from_uri(request.uri())
        .map_err(|e| Error::query(e).into_response())?;
    let content = state
        .manager
        .library_read_content(
            &principal,
            &ref_id,
            query.session_id.as_deref(),
            query.project_id.as_deref(),
        )
        .await
        .map_err(|e| Error::from(e).into_response())?;
    check_completion_auth(&state.auth, &principal, false)?;
    let disposition = if content.inline_safe {
        "inline"
    } else {
        "attachment"
    };
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content.media_type),
            (header::CONTENT_DISPOSITION, disposition.to_owned()),
            (header::CACHE_CONTROL, "no-store".to_owned()),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff".to_owned()),
            (
                header::CONTENT_SECURITY_POLICY,
                "default-src 'none'; sandbox".to_owned(),
            ),
        ],
        content.data,
    )
        .into_response())
}

async fn mount_session(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String, String)>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path((user_id, ref_id, session_id)) = path
        .map_err(|_| Error::path("Mount path parameters must be valid UTF-8").into_response())?;
    let principal = authorize_user(&state, request.headers(), &user_id, true)?;
    let updated = state
        .manager
        .library_mount_session(&principal, &ref_id, &session_id, true)
        .await
        .map_err(|e| Error::from(e).into_response())?;
    check_completion_auth(&state.auth, &principal, true)?;
    Ok(Json(updated).into_response())
}

async fn unmount_session(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String, String)>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path((user_id, ref_id, session_id)) = path
        .map_err(|_| Error::path("Mount path parameters must be valid UTF-8").into_response())?;
    let principal = authorize_user(&state, request.headers(), &user_id, true)?;
    let updated = state
        .manager
        .library_mount_session(&principal, &ref_id, &session_id, false)
        .await
        .map_err(|e| Error::from(e).into_response())?;
    check_completion_auth(&state.auth, &principal, true)?;
    Ok(Json(updated).into_response())
}

async fn mount_project(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String, String)>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path((user_id, ref_id, project_id)) = path
        .map_err(|_| Error::path("Mount path parameters must be valid UTF-8").into_response())?;
    let principal = authorize_user(&state, request.headers(), &user_id, true)?;
    let updated = state
        .manager
        .library_mount_project(&principal, &ref_id, &project_id, true)
        .await
        .map_err(|e| Error::from(e).into_response())?;
    check_completion_auth(&state.auth, &principal, true)?;
    Ok(Json(updated).into_response())
}

async fn unmount_project(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String, String)>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path((user_id, ref_id, project_id)) = path
        .map_err(|_| Error::path("Mount path parameters must be valid UTF-8").into_response())?;
    let principal = authorize_user(&state, request.headers(), &user_id, true)?;
    let updated = state
        .manager
        .library_mount_project(&principal, &ref_id, &project_id, false)
        .await
        .map_err(|e| Error::from(e).into_response())?;
    check_completion_auth(&state.auth, &principal, true)?;
    Ok(Json(updated).into_response())
}
