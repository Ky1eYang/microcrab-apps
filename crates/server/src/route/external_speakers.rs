//! Owner-authenticated dedicated speaker resources; never reuse prompt settings DTOs.
mod json;
use super::endpoint::Endpoint;
pub(super) fn parse(bytes: &[u8]) -> Result<Value, serde_json::Error> {
    json::parse(bytes)
}
use serde_json::{Value, json};
use std::collections::HashMap;
fn project(path: &HashMap<String, String>, params: &mut Value) {
    params["project_id"] = json!(path.get("project_id"));
}
fn operation(path: &HashMap<String, String>, params: &mut Value) {
    params["operation_id"] = json!(path.get("operation_id"));
}
fn project_operation(path: &HashMap<String, String>, params: &mut Value) {
    project(path, params);
    operation(path, params);
}
pub(crate) const ENDPOINTS: &[Endpoint] = &[
    Endpoint::get(
        "/api/v1/projects/{project_id}/external-speakers/operations/{operation_id}",
        "project.external_speakers.operation",
    )
    .with_path_mapper(project_operation),
    Endpoint::get(
        "/api/v1/sessions/{session_id}/external-speakers/operations/{operation_id}",
        "session.external_speakers.operation",
    )
    .with_path_mapper(operation),
    Endpoint::get(
        "/api/v1/projects/{project_id}/external-speakers",
        "project.external_speakers.get",
    )
    .with_path_mapper(project),
    Endpoint::put(
        "/api/v1/projects/{project_id}/external-speakers",
        "project.external_speakers.set",
    )
    .with_path_mapper(project),
    Endpoint::post(
        "/api/v1/projects/{project_id}/external-speakers/key",
        "project.external_speakers.key",
    )
    .with_path_mapper(project),
    Endpoint::get(
        "/api/v1/sessions/{session_id}/external-speakers",
        "session.external_speakers.get",
    ),
    Endpoint::put(
        "/api/v1/sessions/{session_id}/external-speakers",
        "session.external_speakers.set",
    ),
    Endpoint::post(
        "/api/v1/sessions/{session_id}/external-speakers/key",
        "session.external_speakers.key",
    ),
];
