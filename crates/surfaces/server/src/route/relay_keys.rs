//! Cookie-authenticated key management. Relay bearer credentials are never accepted here.
use super::{
    endpoint::parse_query, error::Error, http::check_completion_auth, provenance::auth_error,
};
use crate::{
    Application,
    auth::Principal,
    manager::relay_keys::{CreateKey, KeyError, KeyMetadata, UpdateKey},
};
use axum::{
    Json,
    extract::{Path, Request, State, rejection::PathRejection},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::sync::Arc;

#[cfg(test)]
mod tests;

const MAX_JSON_BYTES: usize = 16 * 1024;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct EmptyQuery {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Revoke {
    expected_revision: u64,
}

#[derive(Serialize)]
struct Keys {
    keys: Vec<KeyMetadata>,
}

#[derive(Serialize)]
struct Key {
    key: KeyMetadata,
}

#[derive(Serialize)]
struct DeletedKey {
    deleted: bool,
    id: String,
}

pub(super) async fn list(
    State(state): State<Arc<Application>>,
    path: Result<Path<String>, PathRejection>,
    request: Request,
) -> Response {
    no_store(read_keys(&state, path, request).await)
}

pub(super) async fn create(
    State(state): State<Arc<Application>>,
    path: Result<Path<String>, PathRejection>,
    request: Request,
) -> Response {
    no_store(create_key(&state, path, request).await)
}

pub(super) async fn update(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String)>, PathRejection>,
    request: Request,
) -> Response {
    no_store(change_key(&state, path, request, false).await)
}

pub(super) async fn revoke(
    State(state): State<Arc<Application>>,
    path: Result<Path<(String, String)>, PathRejection>,
    request: Request,
) -> Response {
    no_store(change_key(&state, path, request, true).await)
}

async fn read_keys(
    state: &Application,
    path: Result<Path<String>, PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path(owner) = path.map_err(path_error)?;
    let principal = authorize(state, request.headers(), &owner, false)?;
    parse_query::<EmptyQuery>(request.uri()).map_err(IntoResponse::into_response)?;
    let keys = state
        .manager
        .relay_keys
        .list(&owner)
        .await
        .map_err(|error| key_error(error, false))?;
    check_completion_auth(&state.auth, &principal, false)?;
    Ok(Json(Keys { keys }).into_response())
}

async fn create_key(
    state: &Application,
    path: Result<Path<String>, PathRejection>,
    request: Request,
) -> Result<Response, Response> {
    let Path(owner) = path.map_err(path_error)?;
    let principal = authorize(state, request.headers(), &owner, true)?;
    let request = json::<CreateKey>(request).await?;
    state.auth.check_active(&principal).map_err(auth_error)?;
    let created = state
        .manager
        .relay_keys
        .create(&owner, request)
        .await
        .map_err(|error| key_error(error, true))?;
    check_completion_auth(&state.auth, &principal, true)?;
    Ok((StatusCode::CREATED, Json(created)).into_response())
}

async fn change_key(
    state: &Application,
    path: Result<Path<(String, String)>, PathRejection>,
    request: Request,
    revoke: bool,
) -> Result<Response, Response> {
    let Path((owner, id)) = path.map_err(path_error)?;
    let principal = authorize(state, request.headers(), &owner, true)?;
    let store = &state.manager.relay_keys;
    if revoke {
        let request = json::<Revoke>(request).await?;
        state.auth.check_active(&principal).map_err(auth_error)?;
        store
            .revoke(&owner, &id, request.expected_revision)
            .await
            .map_err(|error| key_error(error, true))?;
        check_completion_auth(&state.auth, &principal, true)?;
        Ok(Json(DeletedKey { deleted: true, id }).into_response())
    } else {
        let request = json::<UpdateKey>(request).await?;
        state.auth.check_active(&principal).map_err(auth_error)?;
        let key = store
            .update(&owner, &id, request)
            .await
            .map_err(|error| key_error(error, true))?;
        check_completion_auth(&state.auth, &principal, true)?;
        Ok(Json(Key { key }).into_response())
    }
}

#[expect(
    clippy::result_large_err,
    reason = "The canonical OIDC error response is passed through without reserialization"
)]
fn authorize(
    state: &Application,
    headers: &HeaderMap,
    owner: &str,
    mutation: bool,
) -> Result<Principal, Response> {
    // Do not silently accept a mixed authentication attempt.
    if headers.contains_key(header::AUTHORIZATION) {
        return Err(Error::new(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "Key management requires browser cookie authentication only",
        )
        .into_response());
    }
    let principal = state.auth.authenticate(headers).map_err(auth_error)?;
    state.auth.check_active(&principal).map_err(auth_error)?;
    if principal.owner != owner {
        return Err(Error::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            "Path user_id does not match authenticated user",
        )
        .into_response());
    }
    if mutation {
        state.auth.check_origin(headers).map_err(auth_error)?;
    }
    Ok(principal)
}

async fn json<T: DeserializeOwned>(request: Request) -> Result<T, Response> {
    parse_query::<EmptyQuery>(request.uri()).map_err(IntoResponse::into_response)?;
    let mut types = request.headers().get_all(header::CONTENT_TYPE).iter();
    let is_json = types
        .next()
        .and_then(|header| header.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"));
    if !is_json || types.next().is_some() {
        return Err(Error::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "Expected exactly one application/json Content-Type",
        )
        .into_response());
    }
    use futures_util::StreamExt as _;
    let mut body = request.into_body().into_data_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|_| {
            Error::invalid("Relay key request body could not be read").into_response()
        })?;
        if chunk.len() > MAX_JSON_BYTES - bytes.len() {
            return Err(Error::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                "Relay key request exceeds the JSON body limit",
            )
            .into_response());
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|error| Error::json(error).into_response())
}

fn no_store(result: Result<Response, Response>) -> Response {
    let mut response = result.unwrap_or_else(|response| response);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    response
}

fn path_error(_: PathRejection) -> Response {
    Error::path("Relay key path parameters must be valid UTF-8").into_response()
}

fn key_error(error: KeyError, mutation: bool) -> Response {
    match error {
        KeyError::Invalid(message) => Error::parameters("json", message),
        KeyError::NotFound => Error::new(StatusCode::NOT_FOUND, "not_found", "Relay key not found"),
        KeyError::Conflict => Error::new(StatusCode::CONFLICT, "revision_conflict", "Relay key revision changed; reload before retrying"),
        KeyError::Capacity => Error::new(StatusCode::CONFLICT, "key_capacity", "Relay key retention limit reached"),
        KeyError::Busy => Error::new(StatusCode::SERVICE_UNAVAILABLE, "busy", "Relay key authority is busy"),
        KeyError::Closed => Error::new(StatusCode::SERVICE_UNAVAILABLE, "closed", "Relay key authority is closed"),
        KeyError::Random => Error::internal("Relay key random generation failed"),
        error => {
            tracing::warn!(reason = %error, "Relay key authority operation failed");
            if mutation {
                Error::new(StatusCode::INTERNAL_SERVER_ERROR, "outcome_unknown", "Relay key mutation could not be acknowledged; reload keys and reconcile before retrying")
            } else {
                Error::internal("Relay key metadata is unavailable")
            }
        }
    }.into_response()
}
