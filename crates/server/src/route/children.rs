//! Parent-authorized child reads and explicitly targeted current-mode user submissions.
use super::endpoint::{Endpoint, parse_query};
use crate::manager::commands;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ListQuery {
    limit: Option<usize>,
    cursor: Option<String>,
}
#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DetailQuery {
    limit: Option<usize>,
    before_cursor: Option<usize>,
}
fn list_query(uri: &axum::http::Uri) -> Result<Value, super::error::Error> {
    parse_query::<ListQuery>(uri)
}
fn detail_query(uri: &axum::http::Uri) -> Result<Value, super::error::Error> {
    parse_query::<DetailQuery>(uri)
}
fn detail_path(path: &HashMap<String, String>, params: &mut Value) {
    params["child_id"] = json!(path.get("child_id"));
}
fn operation_path(path: &HashMap<String, String>, params: &mut Value) {
    detail_path(path, params);
    params["operation_id"] = json!(path.get("operation_id"));
}
pub(crate) const ENDPOINTS: &[Endpoint] = &[
    Endpoint::post(
        "/api/v1/sessions/{session_id}/children/{child_id}/enter",
        "child.enter",
    )
    .with_path_mapper(detail_path),
    Endpoint::post(
        "/api/v1/sessions/{session_id}/children/{child_id}/turns",
        "child.turn.submit",
    )
    .with_path_mapper(detail_path),
    Endpoint::get(
        "/api/v1/sessions/{session_id}/children/{child_id}/turns/{operation_id}",
        "child.turn.get",
    )
    .with_path_mapper(operation_path),
    Endpoint::get(
        "/api/v1/sessions/{session_id}/children",
        commands::SESSION_CHILDREN,
    )
    .with_query(list_query),
    Endpoint::get(
        "/api/v1/sessions/{session_id}/children/{child_id}",
        commands::SESSION_CHILD,
    )
    .with_query(detail_query)
    .with_path_mapper(detail_path),
];
