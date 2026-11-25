//! Project and agent catalog HTTP route descriptors.
//!
//! Provides workspace project discovery, lifecycle management and project-scoped agent component catalogs.
use super::endpoint::{Endpoint, parse_query};
use crate::manager::commands;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;

fn map_agent_catalog_path(path: &HashMap<String, String>, params: &mut Value) {
    params["project_id"] = json!(path.get("project_id"));
}

fn map_conversation_agent_catalog_path(path: &HashMap<String, String>, params: &mut Value) {
    params["conversation_id"] = json!(path.get("conversation_id"));
}

fn map_project_id_path(path: &HashMap<String, String>, params: &mut Value) {
    params["project_id"] = json!(path.get("project_id"));
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OperationsQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<usize>,
}
fn parse_operations_query(uri: &axum::http::Uri) -> Result<Value, super::error::Error> {
    parse_query::<OperationsQuery>(uri)
}

/// Project-scoped HTTP endpoints.
pub(crate) const ENDPOINTS: &[Endpoint] = &[
    Endpoint::post(
        "/api/v1/projects/{project_id}/host/outbound/start",
        commands::PROJECT_HOST_OUTBOUND_START,
    )
    .with_path_mapper(map_project_id_path),
    Endpoint::post(
        "/api/v1/projects/{project_id}/host/outbound/finish",
        commands::PROJECT_HOST_OUTBOUND_FINISH,
    )
    .with_path_mapper(map_project_id_path),
    Endpoint::post(
        "/api/v1/projects/{project_id}/host/outbound/cancel",
        commands::PROJECT_HOST_OUTBOUND_CANCEL,
    )
    .with_path_mapper(map_project_id_path),
    Endpoint::post(
        "/api/v1/projects/{project_id}/host/connect",
        commands::PROJECT_HOST_CONNECT,
    )
    .with_path_mapper(map_project_id_path),
    Endpoint::post(
        "/api/v1/projects/{project_id}/host/disconnect",
        commands::PROJECT_HOST_DISCONNECT,
    )
    .with_path_mapper(map_project_id_path),
    Endpoint::post(
        "/api/v1/projects/{project_id}/host/refresh",
        commands::PROJECT_HOST_REFRESH,
    )
    .with_path_mapper(map_project_id_path),
    Endpoint::get(
        "/api/v1/projects/{project_id}/host-state",
        commands::PROJECT_HOST_STATE,
    )
    .with_path_mapper(map_project_id_path),
    Endpoint::get(
        "/api/v1/projects/{project_id}/operations",
        commands::PROJECT_OPERATIONS,
    )
    .with_query(parse_operations_query)
    .with_path_mapper(map_project_id_path),
    Endpoint::post(
        "/api/v1/projects/{project_id}/host-reconcile",
        commands::PROJECT_HOST_RECONCILE,
    )
    .with_path_mapper(map_project_id_path),
    Endpoint::delete(
        "/api/v1/projects/{project_id}/provisional",
        commands::PROJECT_DELETE_PROVISIONAL,
    )
    .with_path_mapper(map_project_id_path),
    // List accessible workspace projects for the current user.
    Endpoint::get("/api/v1/projects", commands::PROJECT_LIST),
    // Create a new workspace project for the current user.
    Endpoint::post("/api/v1/projects", commands::PROJECT_CREATE),
    // Rename presentation metadata independently from Host/workspace identity.
    Endpoint::patch("/api/v1/projects/{project_id}", commands::PROJECT_RENAME)
        .with_path_mapper(map_project_id_path),
    // Delete a workspace project.
    Endpoint::delete("/api/v1/projects/{project_id}", commands::PROJECT_DELETE)
        .with_path_mapper(map_project_id_path),
    // Discover available agent components and tools for a given project.
    Endpoint::get(
        "/api/v1/projects/{project_id}/agents",
        commands::AGENT_CATALOG,
    )
    .with_path_mapper(map_agent_catalog_path),
    // Discover agents for the exact owner-authorized Conversation Host binding.
    Endpoint::get(
        "/api/v1/conversations/{conversation_id}/agents",
        commands::AGENT_CATALOG,
    )
    .with_path_mapper(map_conversation_agent_catalog_path),
];
