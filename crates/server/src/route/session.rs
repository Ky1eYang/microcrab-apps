//! Session lifecycle, inspection, and history HTTP route descriptors.
//!
//! Provides durable CRUD, checkpointing, compaction, permission tuning,
//! background job inspection, and workspace queries for sessions.
use super::endpoint::{Endpoint, parse_query};
use crate::manager::commands;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;

/// Query parameters for `GET /api/v1/sessions/{session_id}/workspace/files`.
#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FilesQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pattern: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) limit: Option<usize>,
}

fn parse_files_query(uri: &axum::http::Uri) -> Result<Value, super::error::Error> {
    parse_query::<FilesQuery>(uri)
}

fn map_turn_get_path(path: &HashMap<String, String>, params: &mut Value) {
    params["operation_id"] = json!(path.get("operation_id"));
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClientCallsQuery {
    pub(crate) cursor: Option<usize>,
    pub(crate) limit: Option<usize>,
}
fn parse_client_calls_query(uri: &axum::http::Uri) -> Result<Value, super::error::Error> {
    parse_query::<ClientCallsQuery>(uri)
}

fn parse_executions_query(uri: &axum::http::Uri) -> Result<Value, super::error::Error> {
    parse_query::<crate::manager::execution_history::Query>(uri)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionContentQuery {
    pub(crate) field: microcrab_session::runtime::execution::ExecutionContentField,
    pub(crate) cursor: Option<u64>,
    pub(crate) max_bytes: Option<usize>,
}
fn parse_execution_content_query(uri: &axum::http::Uri) -> Result<Value, super::error::Error> {
    parse_query::<ExecutionContentQuery>(uri)
}

fn map_execution_path(path: &HashMap<String, String>, params: &mut Value) {
    params["execution_id"] = path
        .get("execution_id")
        .and_then(|id| id.parse::<u64>().ok())
        .map(Value::from)
        .unwrap_or(Value::Null);
}

/// All session lifecycle, state inspection, and operation endpoints.
pub(crate) const ENDPOINTS: &[Endpoint] = &[
    Endpoint::get(
        "/api/v1/sessions/{session_id}/state/inspection",
        commands::STATE_INSPECTION,
    ),
    Endpoint::get(
        "/api/v1/sessions/{session_id}/executions",
        commands::EXECUTIONS_LIST,
    )
    .with_query(parse_executions_query),
    Endpoint::get(
        "/api/v1/sessions/{session_id}/executions/{execution_id}/content",
        commands::EXECUTIONS_CONTENT,
    )
    .with_query(parse_execution_content_query)
    .with_path_mapper(map_execution_path),
    Endpoint::post(
        "/api/v1/sessions/{session_id}/display/observations/query",
        commands::DISPLAY_OBSERVATIONS,
    ),
    Endpoint::post(
        "/api/v1/sessions/{session_id}/audit/requests",
        commands::AUDIT_REQUESTS,
    ),
    Endpoint::get("/api/v1/sessions/{session_id}/state", commands::STATE_GET),
    Endpoint::get(
        "/api/v1/sessions/{session_id}/state/metadata",
        commands::STATE_METADATA,
    ),
    Endpoint::get(
        "/api/v1/sessions/{session_id}/client-tools/calls",
        commands::CLIENT_TOOLS_CALLS,
    )
    .with_query(parse_client_calls_query),
    Endpoint::post(
        "/api/v1/sessions/{session_id}/client-tools",
        commands::CLIENT_TOOLS_SET,
    ),
    // List sessions belonging to the caller.
    Endpoint::get("/api/v1/sessions", commands::SESSION_LIST),
    // Create a new session record and initialize session state.
    // Request Body: [`commands::Create`]
    Endpoint::post("/api/v1/sessions", commands::SESSION_CREATE),
    Endpoint::post("/api/v1/sessions/{session_id}/fork", commands::SESSION_FORK),
    // Rename session title with optimistic revision check.
    // Request Body: [`commands::Rename`]
    Endpoint::patch("/api/v1/sessions/{session_id}", commands::SESSION_RENAME),
    // Logical deletion (archive): the record and its bytes are retained.
    // Request Body: [`commands::Revision`]
    Endpoint::post(
        "/api/v1/sessions/{session_id}/archive",
        commands::SESSION_ARCHIVE,
    ),
    // Irreversible deletion of one session, its byte trees and its receipts.
    // Request Body: [`commands::Revision`]
    Endpoint::delete("/api/v1/sessions/{session_id}", commands::SESSION_DELETE),
    // Explicitly open and activate a session runtime.
    Endpoint::put(
        "/api/v1/sessions/{session_id}/runtime",
        commands::SESSION_OPEN,
    ),
    Endpoint::post(
        "/api/v1/sessions/{session_id}/auto-compaction/retry",
        commands::SESSION_RETRY_AUTO_COMPACTION,
    ),
    // Explicit initialization retry; accepted operations are never automatically replayed.
    Endpoint::post(
        "/api/v1/sessions/{session_id}/agent-init/retry",
        commands::SESSION_RETRY_AGENT_INIT,
    ),
    // Close and suspend a session runtime.
    Endpoint::delete(
        "/api/v1/sessions/{session_id}/runtime",
        commands::SESSION_CLOSE,
    ),
    // Time-travel to a previous history checkpoint revision.
    // Request Body: [`commands::Revision`]
    Endpoint::post(
        "/api/v1/sessions/{session_id}/history/travel",
        commands::HISTORY_TRAVEL,
    ),
    // Create an explicit manual history checkpoint.
    Endpoint::post(
        "/api/v1/sessions/{session_id}/checkpoints",
        commands::CHECKPOINT_CREATE,
    ),
    // Compact session conversation history.
    // Request Body: [`commands::Compact`]
    Endpoint::post(
        "/api/v1/sessions/{session_id}/compact",
        commands::SESSION_COMPACT,
    ),
    // Query active permissions and policy overrides.
    Endpoint::get(
        "/api/v1/sessions/{session_id}/permission",
        commands::PERMISSION_GET,
    ),
    // Update session permissions with optimistic revision check.
    // Request Body: [`commands::Permission`]
    Endpoint::put(
        "/api/v1/sessions/{session_id}/permission",
        commands::PERMISSION_SET,
    ),
    // Set runtime execution mode (e.g. read-only, workspace-write).
    // Request Body: [`commands::Mode`]
    Endpoint::put(
        "/api/v1/sessions/{session_id}/mode",
        commands::SESSION_SET_MODE,
    ),
    // Update provider/model/reasoning while preserving the conversation epoch.
    // Request Body: [`commands::ModelSetting`]
    Endpoint::put(
        "/api/v1/sessions/{session_id}/model",
        commands::SESSION_SET_MODEL,
    ),
    // List workspace files matching an optional pattern.
    Endpoint::get(
        "/api/v1/sessions/{session_id}/workspace/files",
        commands::WORKSPACE_FILES,
    )
    .with_query(parse_files_query),
    // Query durable receipt and status of an asynchronous turn operation.
    Endpoint::get(
        "/api/v1/sessions/{session_id}/operations/{operation_id}",
        commands::TURN_GET,
    )
    .with_path_mapper(map_turn_get_path),
];
