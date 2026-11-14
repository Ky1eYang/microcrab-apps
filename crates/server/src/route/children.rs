//! Bounded parent-authorized child read resources; never child execution routes.
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
pub(crate) const ENDPOINTS: &[Endpoint] = &[
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
