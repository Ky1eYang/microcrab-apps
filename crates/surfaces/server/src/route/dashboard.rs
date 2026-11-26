//! Owner-only dashboard read routes. Relay bearer keys are not browser principals.
use super::{error::Error, provenance::auth_error};
use crate::{
    Application,
    manager::dashboard::{Filter, Window},
};
use axum::{
    Json,
    extract::{Path, Query, State, rejection::QueryRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

fn prepare(query: Result<Query<Filter>, QueryRejection>) -> Result<(Filter, Window), Error> {
    let Query(filter) = query.map_err(Error::query)?;
    let window = filter
        .window()
        .map_err(|message| Error::parameters("query", message))?;
    Ok((filter, window))
}

fn authorize(state: &Application, headers: &HeaderMap, owner: &str) -> Result<(), Response> {
    let principal = state.auth.authenticate(headers).map_err(auth_error)?;
    state.auth.check_active(&principal).map_err(auth_error)?;
    if principal.owner == owner {
        Ok(())
    } else {
        Err(Error::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            "Path user_id does not match authenticated user",
        )
        .into_response())
    }
}

fn storage_error(error: microcrab_session::Error) -> Error {
    if matches!(error, microcrab_session::Error::Busy) {
        return Error::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "busy",
            "Dashboard projection is busy; try again",
        );
    }
    // Private storage details and provider metadata are not sent to the browser.
    tracing::warn!("Dashboard read failed");
    Error::internal("Dashboard usage projection is unavailable")
}

async fn read(
    state: Arc<Application>,
    owner: String,
    headers: HeaderMap,
    query: Result<Query<Filter>, QueryRejection>,
    endpoint: Endpoint,
) -> Response {
    let mut response = match authorize(&state, &headers, &owner) {
        Ok(()) => match prepare(query) {
            Ok((filter, window)) => endpoint.respond(&state, &owner, &filter, &window).await,
            Err(error) => error.into_response(),
        },
        Err(response) => response,
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

enum Endpoint {
    Summary,
    Diagnostics,
    Logs,
    Export,
}
impl Endpoint {
    async fn respond(
        self,
        state: &Application,
        owner: &str,
        filter: &Filter,
        window: &Window,
    ) -> Response {
        let result = match self {
            Self::Summary => state.manager.dashboard_summary(owner, filter, window).await,
            Self::Diagnostics => {
                state
                    .manager
                    .dashboard_diagnostics(owner, filter, window)
                    .await
            }
            Self::Logs => state.manager.dashboard_logs(owner, filter, window).await,
            Self::Export => {
                return match state.manager.dashboard_csv(owner, filter, window).await {
                    Ok((csv, truncated)) => (
                        [
                            (header::CONTENT_TYPE.as_str(), "text/csv; charset=utf-8"),
                            (
                                header::CONTENT_DISPOSITION.as_str(),
                                "attachment; filename=\"microcrab-usage.csv\"",
                            ),
                            (
                                "x-microcrab-export-truncated",
                                if truncated { "true" } else { "false" },
                            ),
                        ],
                        csv,
                    )
                        .into_response(),
                    Err(error) => storage_error(error).into_response(),
                };
            }
        };
        match result {
            Ok(json) => Json(json).into_response(),
            Err(error) => storage_error(error).into_response(),
        }
    }
}

pub(super) async fn summary(
    State(state): State<Arc<Application>>,
    Path(owner): Path<String>,
    headers: HeaderMap,
    query: Result<Query<Filter>, QueryRejection>,
) -> Response {
    read(state, owner, headers, query, Endpoint::Summary).await
}
pub(super) async fn cache_diagnostics(
    State(state): State<Arc<Application>>,
    Path(owner): Path<String>,
    headers: HeaderMap,
    query: Result<Query<Filter>, QueryRejection>,
) -> Response {
    read(state, owner, headers, query, Endpoint::Diagnostics).await
}
pub(super) async fn logs(
    State(state): State<Arc<Application>>,
    Path(owner): Path<String>,
    headers: HeaderMap,
    query: Result<Query<Filter>, QueryRejection>,
) -> Response {
    read(state, owner, headers, query, Endpoint::Logs).await
}
pub(super) async fn export_logs(
    State(state): State<Arc<Application>>,
    Path(owner): Path<String>,
    headers: HeaderMap,
    query: Result<Query<Filter>, QueryRejection>,
) -> Response {
    read(state, owner, headers, query, Endpoint::Export).await
}
