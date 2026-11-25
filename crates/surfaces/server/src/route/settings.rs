//! HTTP settings contracts. Transport supplies identity; payloads never carry owner.
use super::endpoint::Endpoint;
use serde_json::{Value, json};
use std::collections::HashMap;
fn project(path: &HashMap<String, String>, params: &mut Value) {
    params["project_id"] = json!(path.get("project_id"));
}
fn operation(path: &HashMap<String, String>, params: &mut Value) {
    params["operation_id"] = json!(path.get("operation_id"));
}
fn provider(path: &HashMap<String, String>, params: &mut Value) {
    params["provider_id"] = json!(path.get("provider_id"));
}
pub(crate) const ENDPOINTS: &[Endpoint] = &[
    Endpoint::get("/api/v1/settings", "settings.get"),
    Endpoint::put("/api/v1/settings", "settings.set"),
    Endpoint::get(
        "/api/v1/projects/{project_id}/settings",
        "project.settings.get",
    )
    .with_path_mapper(project),
    Endpoint::put(
        "/api/v1/projects/{project_id}/settings",
        "project.settings.set",
    )
    .with_path_mapper(project),
    Endpoint::get(
        "/api/v1/projects/{project_id}/references",
        "project.references.get",
    )
    .with_path_mapper(project),
    Endpoint::put(
        "/api/v1/projects/{project_id}/references",
        "project.references.set",
    )
    .with_path_mapper(project),
    Endpoint::get(
        "/api/v1/sessions/{session_id}/settings",
        "session.settings.get",
    ),
    Endpoint::put(
        "/api/v1/sessions/{session_id}/settings",
        "session.settings.set",
    ),
    Endpoint::post(
        "/api/v1/sessions/{session_id}/references/refresh",
        "session.references.refresh",
    ),
    Endpoint::patch(
        "/api/v1/sessions/{session_id}/variables",
        "session.variables.patch",
    ),
    Endpoint::get(
        "/api/v1/sessions/{session_id}/variables/operations/{operation_id}",
        "session.variables.operation",
    )
    .with_path_mapper(operation),
    Endpoint::get("/api/v1/providers", "provider.list"),
    Endpoint::get("/api/v1/providers/{provider_id}", "provider.get").with_path_mapper(provider),
    Endpoint::put("/api/v1/providers/{provider_id}", "provider.set").with_path_mapper(provider),
    Endpoint::delete("/api/v1/providers/{provider_id}", "provider.delete")
        .with_path_mapper(provider),
    Endpoint::post(
        "/api/v1/providers/{provider_id}/discover",
        "provider.discover",
    )
    .with_path_mapper(provider),
];
