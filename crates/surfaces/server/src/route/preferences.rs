//! Cookie-authenticated account preference management. Relay keys confer no write authority.
use super::{
    endpoint::parse_query, error::Error, http::check_completion_auth, provenance::auth_error,
};
use crate::{
    Application,
    manager::preferences::{PreferencesError, PreferencesSnapshot, PreferencesUpdate},
};
use axum::{
    Json,
    extract::{Path, Request, State, rejection::PathRejection},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt as _;
use microcrab_storage::ModelSelection;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const MAX_JSON_BYTES: usize = 8192;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct EmptyQuery {}

#[derive(Serialize)]
struct AccountPreferences {
    #[serde(flatten)]
    preferences: PreferencesSnapshot,
    relay_models: Vec<ModelSelection>,
}

pub(super) async fn read(
    State(state): State<Arc<Application>>,
    path: Result<Path<String>, PathRejection>,
    request: Request,
) -> Response {
    respond(&state, path, request, false).await
}

pub(super) async fn update(
    State(state): State<Arc<Application>>,
    path: Result<Path<String>, PathRejection>,
    request: Request,
) -> Response {
    respond(&state, path, request, true).await
}

async fn respond(
    state: &Application,
    path: Result<Path<String>, PathRejection>,
    request: Request,
    mutation: bool,
) -> Response {
    let mut response = handle(state, path, request, mutation)
        .await
        .unwrap_or_else(|error| error);
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn handle(
    state: &Application,
    path: Result<Path<String>, PathRejection>,
    request: Request,
    mutation: bool,
) -> Result<Response, Response> {
    let Path(owner) =
        path.map_err(|_| Error::path("Path user_id must be valid UTF-8").into_response())?;
    if request.headers().contains_key(header::AUTHORIZATION) {
        return Err(Error::new(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "Account preferences require browser cookie authentication only",
        )
        .into_response());
    }
    let principal = state
        .auth
        .authenticate(request.headers())
        .map_err(auth_error)?;
    state.auth.check_active(&principal).map_err(auth_error)?;
    if principal.owner != owner {
        return Err(Error::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            "Path user_id does not match authenticated user",
        )
        .into_response());
    }
    parse_query::<EmptyQuery>(request.uri()).map_err(IntoResponse::into_response)?;
    let manager = &state.manager;
    let result = if mutation {
        state
            .auth
            .check_origin(request.headers())
            .map_err(auth_error)?;
        let input = parse_update(request).await?;
        state.auth.check_active(&principal).map_err(auth_error)?;
        manager.update_preferences(&owner, input).await
    } else {
        manager.preferences(&owner).await
    };
    let preferences = result.map_err(|error| preference_error(error).into_response())?;
    let relay_models = manager
        .preference_relay_models(&owner)
        .await
        .map_err(|error| preference_error(error).into_response())?;
    check_completion_auth(&state.auth, &principal, mutation)?;
    Ok(Json(AccountPreferences {
        preferences,
        relay_models,
    })
    .into_response())
}

async fn parse_update(request: Request) -> Result<PreferencesUpdate, Response> {
    let mut types = request.headers().get_all(header::CONTENT_TYPE).iter();
    let is_json = types
        .next()
        .and_then(|value| value.to_str().ok())
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
    let mut body = request.into_body().into_data_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|_| {
            Error::invalid("Preferences request body could not be read").into_response()
        })?;
        if chunk.len() > MAX_JSON_BYTES - bytes.len() {
            return Err(Error::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                "Preferences request exceeds the JSON body limit",
            )
            .into_response());
        }
        bytes.extend_from_slice(&chunk);
    }
    PreferencesUpdate::parse(&bytes).map_err(|error| Error::json(error).into_response())
}

fn preference_error(error: PreferencesError) -> Error {
    match error {
        PreferencesError::Invalid(reason) => Error::parameters("json", reason),
        PreferencesError::Conflict => Error::new(
            StatusCode::CONFLICT,
            "revision_conflict",
            "Account preferences changed; read the latest revision before saving",
        ),
        PreferencesError::Mutation(source) => {
            tracing::warn!(reason = %source.diagnostic_reason(), "Account preference mutation could not be acknowledged");
            Error::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "outcome_unknown",
                "Account preferences could not be acknowledged; reload to reconcile before saving again",
            )
        }
        PreferencesError::Read(source) => {
            tracing::warn!(reason = %source.diagnostic_reason(), "Account preference read failed");
            Error::internal("Account preferences storage is unavailable")
        }
        PreferencesError::Schema(source) => {
            tracing::warn!(category = ?source.classify(), "Account preference schema could not be decoded");
            Error::internal("Stored account preferences cannot be decoded")
        }
    }
}
