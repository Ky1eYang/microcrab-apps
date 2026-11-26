//! Fixed HTTP operations delegate to the same canonical Manager commands as the
//! runtime protocol. The endpoint table also excludes these commands from WS.
use super::{
    endpoint::{Endpoint, Verb, parse_query},
    error::Error,
    project, session, system,
};
use crate::Application;
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{MatchedPath, Path, Request, State},
    http::{Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, on},
};
use microcrab_protocol::application::{Command, CommandKind, VERSION};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};

/// One command body bound per request. The WebSocket frame is the transport's real
/// bound, so this mirrors it instead of introducing a smaller arbitrary ceiling
/// that rejected ordinary long input with `413 request_too_large`.
const MAX_BODY_BYTES: usize = microcrab_protocol::application::MAX_FRAME_BYTES;

/// Complete iterator over all declared HTTP endpoints across resource modules.
pub(crate) fn all_endpoints() -> impl Iterator<Item = &'static Endpoint> {
    system::ENDPOINTS
        .iter()
        .chain(project::ENDPOINTS.iter())
        .chain(session::ENDPOINTS.iter())
        .chain(super::children::ENDPOINTS.iter())
        .chain(super::settings::ENDPOINTS.iter())
        .chain(super::external_speakers::ENDPOINTS.iter())
}

pub(super) fn router(state: Arc<Application>) -> Router<Arc<Application>> {
    use super::external_inbox as inbox;
    let bearer_routes = Router::new()
        .route(
            "/api/v1/sessions/{session_id}/inbox",
            axum::routing::post(inbox::enqueue)
                .get(inbox::reply::by_header)
                .options(inbox::inbox_preflight),
        )
        .route(
            "/api/v1/sessions/{session_id}/inbox/operations/{operation_id}",
            get(inbox::observe).options(inbox::observe_preflight),
        )
        .route(
            "/api/v1/sessions/{session_id}/inbox/dialogues/{dialogue_id}/reply",
            get(inbox::reply::by_path).options(inbox::observe_preflight),
        )
        .route_layer(axum::middleware::from_fn_with_state(state, inbox::cors));
    let mut router = bearer_routes
        .route(
            "/api/v1/sessions/{session_id}/attachments",
            axum::routing::post(super::attachment::upload),
        )
        .merge(super::library::router())
        .route(
            "/api/v1/sessions/{session_id}/attachments/{attachment_id}",
            get(super::attachment::read),
        )
        .route("/api/v1/health", get(health))
        .route(
            "/api/v1/sessions/{session_id}/history/prompt",
            get(super::prompt_history::read),
        )
        .route(
            "/api/v1/sessions/{session_id}/files/{message_id}",
            get(super::citations::read),
        )
        .route(
            "/api/v1/sessions/{session_id}/questions/{invocation_id}/image",
            get(super::question_images::read),
        )
        .route(
            "/api/v1/sessions/{session_id}/images/{message_id}/{part_index}",
            get(super::images::read),
        );
    for endpoint in all_endpoints() {
        router = router.route(endpoint.path, on(endpoint.verb.filter(), execute));
    }
    router
}

pub(super) async fn health() -> Json<Value> {
    let mut host_transports = vec!["ws"];
    #[cfg(feature = "grpc")]
    host_transports.push("grpc");
    Json(json!({"status":"ok","protocol":VERSION,"host_transports":host_transports}))
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct EmptyQuery {}

async fn execute(
    State(state): State<Arc<Application>>,
    matched: MatchedPath,
    path: Result<Path<HashMap<String, String>>, axum::extract::rejection::PathRejection>,
    request: Request,
) -> Response {
    // The middleware's validated id is propagated to Manager independently of
    // request headers, including when the client did not supply a correlation id.
    let request_id = request
        .extensions()
        .get::<super::provenance::RequestId>()
        .map(|id| id.0.clone())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    match dispatch(state, matched, path, request, request_id).await {
        Ok(value) if value["method"] == "child.turn.submit" && value["status"] == "accepted" => {
            (StatusCode::ACCEPTED, Json(value)).into_response()
        }
        Ok(value) => Json(value).into_response(),
        Err(response) => response,
    }
}

async fn dispatch(
    state: Arc<Application>,
    matched: MatchedPath,
    path: Result<Path<HashMap<String, String>>, axum::extract::rejection::PathRejection>,
    request: Request,
    request_id: String,
) -> Result<Value, Response> {
    let principal = state
        .auth
        .authenticate(request.headers())
        .map_err(super::provenance::auth_error)?;
    state
        .auth
        .check_active(&principal)
        .map_err(super::provenance::auth_error)?;
    let endpoint = all_endpoints()
        .find(|endpoint| {
            endpoint.path == matched.as_str() && endpoint.verb.name() == request.method().as_str()
        })
        .ok_or_else(|| {
            Error::new(
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "HTTP method is not supported for this route",
            )
            .into_response()
        })?;
    let path = if matched.as_str().contains('{') {
        path.map_err(|_| Error::path("Path parameters must be valid UTF-8").into_response())?
            .0
    } else {
        HashMap::new()
    };
    let mut params = if request.method() == Method::GET {
        if let Some(parse_query) = endpoint.query_parser {
            parse_query(request.uri()).map_err(IntoResponse::into_response)?
        } else {
            parse_query::<EmptyQuery>(request.uri()).map_err(IntoResponse::into_response)?
        }
    } else {
        state
            .auth
            .check_origin(request.headers())
            .map_err(super::provenance::auth_error)?;
        // Even same-site browser forms cannot invoke an effect with a simple MIME
        // type. Origin is mandatory: there is no implicit native-client bypass.
        let mut content_types = request.headers().get_all(header::CONTENT_TYPE).iter();
        let json_type = content_types
            .next()
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .split(';')
                    .next()
                    .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
            });
        if !json_type || content_types.next().is_some() {
            return Err(Error::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "Expected application/json",
            )
            .into_response());
        }
        parse_query::<EmptyQuery>(request.uri()).map_err(IntoResponse::into_response)?;
        let bytes = to_bytes(request.into_body(), MAX_BODY_BYTES)
            .await
            .map_err(|_| {
                Error::new(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request_too_large",
                    "Request body exceeds budget",
                )
                .into_response()
            })?;
        let params: Value = if endpoint.command.contains(".external_speakers.") {
            super::external_speakers::parse(&bytes)
        } else {
            serde_json::from_slice(&bytes)
        }
        .map_err(|error| Error::json(error).into_response())?;
        if !params.is_object() {
            return Err(
                Error::parameters("json", "JSON request body must be an object").into_response(),
            );
        }
        params
    };
    if let Some(map_path) = endpoint.path_mapper {
        map_path(&path, &mut params);
    }
    super::provenance::actions(endpoint.command, &params);
    let command = Command {
        v: VERSION,
        kind: CommandKind::Command,
        request_id,
        method: endpoint.command.into(),
        session_id: path.get("session_id").cloned(),
        params,
    };
    // Manager tracks the effect owner before awaiting it. A dropped HTTP future
    // drops only its waiter; neither disconnect nor timeout replays the mutation.
    let result = state.manager.command(&principal, &command).await;
    check_completion_auth(&state.auth, &principal, !matches!(endpoint.verb, Verb::Get))?;
    result.map_err(|error| Error::from(error).into_response())
}

/// Authentication loss after Manager accepted a mutation cannot prove that its
/// effects were rejected. Withhold the result while requiring reconciliation.
#[expect(
    clippy::result_large_err,
    reason = "Routes propagate Axum responses directly; boxing this completed authentication response adds allocation and changes the shared route result contract"
)]
pub(super) fn check_completion_auth(
    auth: &crate::auth::Auth,
    principal: &crate::auth::Principal,
    mutation: bool,
) -> Result<(), Response> {
    auth.check_active(principal).map_err(|error| {
        if mutation {
            Error::new(
                StatusCode::UNAUTHORIZED,
                "outcome_unknown",
                "Authentication changed after operation admission; sign in and reconcile before retrying",
            ).into_response()
        } else {
            super::provenance::auth_error(error)
        }
    })
}
