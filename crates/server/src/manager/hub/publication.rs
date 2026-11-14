//! Incremental publication of the settled conversation view.
//!
//! The Hub keeps one server-side materialized view so attach can answer from a
//! coherent barrier, but subscribers never receive that whole value again.
//! Each replacement is reduced to ordered deltas: metadata patches, canonical
//! message upserts keyed by stable message id, statistics, and a history reset
//! only when the selected lineage changed or a canonical row disappeared. A
//! turn or round boundary therefore produces at most the rows it appended.
use super::SessionRuntimeEvent;
use serde_json::{Map, Value, json};
use std::collections::HashMap;

/// Keys carried by `blocks.*` events rather than `session.meta`.
const CANONICAL: [&str; 4] = [
    "messages",
    "messages_truncated",
    "next_cursor",
    "history_view",
];
/// Keys sent once after attach and then by their own events.
pub(crate) const DEFERRED: [&str; 3] = ["statistics", "state", "usage"];

pub(crate) fn lineage(view: &Value) -> Option<&str> {
    view["history_view"]["lineage"].as_str()
}

/// One canonical page as delivered by `blocks.page`.
pub(crate) fn page(view: &Value, messages: Value, reason: &str) -> Value {
    json!({
        "reason": reason,
        "messages": messages,
        "messages_truncated": view["messages_truncated"],
        "next_cursor": view["next_cursor"],
        "history_view": view["history_view"],
    })
}

/// Attach metadata: everything except canonical rows and deferred bulk fields.
pub(crate) fn meta(view: &Value) -> Map<String, Value> {
    let mut meta = view.as_object().cloned().unwrap_or_default();
    for key in ["messages", "messages_truncated", "next_cursor"]
        .iter()
        .chain(DEFERRED.iter())
    {
        meta.remove(*key);
    }
    meta
}

/// Ordered events that move a subscriber from `previous` to `next`.
pub(crate) fn diff(previous: &Value, next: &Value) -> Vec<SessionRuntimeEvent> {
    let mut events = vec![];
    if !previous.is_object() || previous.get("session").is_none() {
        // A fresh Hub has no subscriber holding an older view.
        return events;
    }
    let mut patch = Map::new();
    let keys: std::collections::BTreeSet<&String> = previous
        .as_object()
        .into_iter()
        .chain(next.as_object())
        .flat_map(Map::keys)
        .collect();
    for key in keys {
        if CANONICAL.contains(&key.as_str()) || key == "statistics" {
            continue;
        }
        let before = previous.get(key).unwrap_or(&Value::Null);
        let after = next.get(key).unwrap_or(&Value::Null);
        if before != after {
            patch.insert(key.clone(), after.clone());
        }
    }
    if !patch.is_empty() {
        events.push(SessionRuntimeEvent::SessionMeta(Value::Object(patch)));
    }
    let empty = vec![];
    let before = previous["messages"].as_array().unwrap_or(&empty);
    let after = next["messages"].as_array().unwrap_or(&empty);
    let reset = match (lineage(previous), lineage(next)) {
        (Some(old), Some(new)) => old != new,
        _ => false,
    } || removed(before, after);
    if reset {
        events.push(SessionRuntimeEvent::HistoryReset(json!({
            "lineage": lineage(next),
            "epoch": next["history_view"]["epoch"],
            "reason": if lineage(previous) != lineage(next) { "lineage" } else { "rewritten" },
        })));
        events.push(SessionRuntimeEvent::BlocksPage(page(
            next,
            json!(after),
            "reset",
        )));
    } else {
        let known: HashMap<&str, &Value> = before
            .iter()
            .filter_map(|message| message["id"].as_str().map(|id| (id, message)))
            .collect();
        let changed: Vec<&Value> = after
            .iter()
            .filter(|message| {
                message["id"]
                    .as_str()
                    .is_none_or(|id| known.get(id).is_none_or(|old| *old != *message))
            })
            .collect();
        let ids: Vec<&Value> = after.iter().map(|message| &message["id"]).collect();
        let order_changed =
            ids.len() != before.len() || ids.iter().zip(before).any(|(id, old)| **id != old["id"]);
        let window_changed = CANONICAL[1..]
            .iter()
            .any(|key| previous.get(*key) != next.get(*key));
        if !changed.is_empty() || order_changed || window_changed {
            events.push(SessionRuntimeEvent::BlocksUpsert(json!({
                "ids": ids,
                "messages": changed,
                "messages_truncated": next["messages_truncated"],
                "next_cursor": next["next_cursor"],
                "history_view": next["history_view"],
            })));
        }
    }
    let statistics = &next["statistics"];
    if !statistics.is_null() && previous.get("statistics") != Some(statistics) {
        events.push(SessionRuntimeEvent::StatisticsUpdated(statistics.clone()));
    }
    events
}

/// Reconnect hints never authorize last-id suffix recovery: earlier rows can
/// change while retaining ids. Every opening sends the complete bounded page.
#[derive(Clone, Debug, Default)]
pub(crate) struct Resume {
    pub history_lineage: Option<String>,
}

pub(crate) fn attach(view: &Value, resume: &Resume) -> (&'static str, Vec<SessionRuntimeEvent>) {
    let reset = resume
        .history_lineage
        .as_deref()
        .is_some_and(|old| lineage(view).is_some_and(|current| current != old));
    let mut events = vec![];
    if reset {
        events.push(SessionRuntimeEvent::HistoryReset(json!({
            "lineage":lineage(view), "epoch":view["history_view"]["epoch"], "reason":"lineage"
        })));
    }
    events.push(SessionRuntimeEvent::BlocksPage(page(
        view,
        view["messages"].clone(),
        if reset { "reset" } else { "attach" },
    )));
    let mut deferred = Map::new();
    for key in ["state", "usage"] {
        if let Some(value) = view.get(key).filter(|value| !value.is_null()) {
            deferred.insert(key.into(), value.clone());
        }
    }
    if !deferred.is_empty() {
        events.push(SessionRuntimeEvent::SessionMeta(Value::Object(deferred)));
    }
    if !view["statistics"].is_null() {
        events.push(SessionRuntimeEvent::StatisticsUpdated(
            view["statistics"].clone(),
        ));
    }
    (if reset { "reset" } else { "fresh" }, events)
}

/// Reference reducer for the incremental protocol: applies one event to a
/// materialized view. Returns whether the event changed settled view state.
/// The browser store implements the same rules; server tests use this one.
pub(crate) fn apply(view: &mut Value, event: &str, data: &Value) -> bool {
    if !view.is_object() {
        *view = json!({});
    }
    match event {
        "model_observation" => {
            // Durable observations advance the private subscription barrier too.
            // The forwarder reduces these same frames before bounding catch-up;
            // waiting for a later metadata patch would retain the attach watermark.
            let Some(high) = data["archive_sequence"].as_u64() else {
                return false;
            };
            let current = view["display"]["high_water"].as_u64().unwrap_or(0);
            if high > current {
                if !view["display"].is_object() {
                    view["display"] = json!({"coverage":"available"});
                }
                view["display"]["high_water"] = json!(high);
            }
        }
        "session.meta" => {
            for (key, value) in data.as_object().into_iter().flatten() {
                view[key] = value.clone();
            }
        }
        "session" => view["session"] = data.clone(),
        "statistics.updated" => view["statistics"] = data.clone(),
        "executions.updated" => {
            view["executions"] = data["items"].clone();
            view["executions_omitted"] = json!(false);
        }
        "blocks.page" | "blocks.upsert" => {
            let rows = if event == "blocks.page" {
                data["messages"].clone()
            } else {
                let mut known: HashMap<String, Value> = HashMap::new();
                for row in view["messages"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .chain(data["messages"].as_array().into_iter().flatten())
                {
                    if let Some(id) = row["id"].as_str() {
                        known.insert(id.to_owned(), row.clone());
                    }
                }
                json!(
                    data["ids"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|id| id.as_str().and_then(|id| known.get(id)).cloned())
                        .collect::<Vec<_>>()
                )
            };
            view["messages"] = rows;
            for key in ["messages_truncated", "next_cursor", "history_view"] {
                view[key] = data[key].clone();
            }
        }
        _ => return false,
    }
    true
}

/// A row inside the overlapping window that vanished means the canonical
/// history was rewritten in place; only then may a client replace its rows.
fn removed(before: &[Value], after: &[Value]) -> bool {
    let Some(first) = after.first().and_then(|message| message["id"].as_str()) else {
        return !before.is_empty() && after.is_empty();
    };
    let present: std::collections::HashSet<&str> = after
        .iter()
        .filter_map(|message| message["id"].as_str())
        .collect();
    // A window that starts after every published row is an append or slide.
    let Some(start) = before
        .iter()
        .position(|message| message["id"].as_str() == Some(first))
    else {
        return false;
    };
    before[start..]
        .iter()
        .filter_map(|message| message["id"].as_str())
        .any(|id| !present.contains(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn view(lineage: &str, ids: &[(&str, &str)]) -> Value {
        json!({
            "session": {"id": "s", "revision": 1},
            "messages": ids.iter().map(|(id, text)| json!({"id": id, "text": text})).collect::<Vec<_>>(),
            "messages_truncated": false,
            "next_cursor": null,
            "history_view": {"lineage": lineage, "epoch": 0},
            "statistics": {"v": 1},
        })
    }
    #[test]
    fn detached_history_updates_do_not_recreate_or_overwrite_current_state() {
        for state in [
            Value::Null,
            json!({"custom":7,"executions":[{"execution_id":2}]}),
        ] {
            let mut snapshot = view("l", &[]);
            snapshot["state"] = state.clone();
            let history = json!([{"execution_id":1,"status":"succeeded"}]);
            assert!(apply(
                &mut snapshot,
                "executions.updated",
                &json!({"items":history})
            ));
            assert_eq!(snapshot["state"], state);
            assert_eq!(snapshot["executions"], history);
        }
    }
    #[test]
    fn durable_observation_frames_advance_catchup_barrier_without_metadata() {
        let mut snapshot = view("l", &[("a", "1")]);
        snapshot["display"] = json!({"high_water":3,"coverage":"available"});
        let observed = |data| SessionRuntimeEvent::ModelObservation(data);
        let durable = observed(json!({
            "phase":"reasoning_delta", "archive_sequence":7, "text":"thinking"
        }));
        assert!(apply(&mut snapshot, durable.name(), durable.payload()));
        assert_eq!(snapshot["display"]["high_water"], 7);
        assert_eq!(snapshot["messages"], json!([{"id":"a","text":"1"}]));
        let stale = observed(json!({"archive_sequence":5}));
        apply(&mut snapshot, stale.name(), stale.payload());
        assert_eq!(snapshot["display"]["high_water"], 7);
        let live = observed(json!({"phase":"started"}));
        assert!(!apply(&mut snapshot, live.name(), live.payload()));
    }
    #[test]
    fn appended_rows_publish_only_new_bodies_and_no_whole_view() {
        let old = view("l", &[("a", "1"), ("b", "2")]);
        let new = view("l", &[("a", "1"), ("b", "2"), ("c", "3")]);
        let events = diff(&old, &new);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name(), "blocks.upsert");
        assert_eq!(
            events[0].payload()["messages"],
            json!([{"id":"c","text":"3"}])
        );
        assert_eq!(events[0].payload()["ids"], json!(["a", "b", "c"]));
        assert!(events.iter().all(|event| event.name() != "snapshot"));
    }
    #[test]
    fn unchanged_view_publishes_nothing_and_meta_is_a_patch() {
        let old = view("l", &[("a", "1")]);
        assert!(diff(&old, &old).is_empty());
        let mut new = old.clone();
        new["session"]["revision"] = json!(2);
        new["statistics"] = json!({"v": 2});
        let events = diff(&old, &new);
        assert_eq!(events[0].name(), "session.meta");
        assert_eq!(
            *events[0].payload(),
            json!({"session": {"id": "s", "revision": 2}})
        );
        assert_eq!(events[1].name(), "statistics.updated");
    }
    #[test]
    fn sliding_window_is_not_a_reset_but_rewrite_and_lineage_are() {
        let old = view("l", &[("a", "1"), ("b", "2")]);
        let slid = view("l", &[("b", "2"), ("c", "3")]);
        assert!(
            diff(&old, &slid)
                .iter()
                .all(|event| event.name() != "history.reset")
        );
        let rewritten = view("l", &[("a", "1"), ("c", "3")]);
        assert_eq!(diff(&old, &rewritten)[0].name(), "history.reset");
        let travelled = view("m", &[("a", "1")]);
        let events = diff(&old, &travelled);
        assert_eq!(events[0].name(), "history.reset");
        assert_eq!(events[1].name(), "blocks.page");
    }
    #[test]
    fn attach_always_sends_full_page_even_when_lineage_matches() {
        let current = view("l", &[("a", "changed prefix"), ("b", "2"), ("c", "3")]);
        for resume in [
            Resume::default(),
            Resume {
                history_lineage: Some("l".into()),
            },
        ] {
            let (mode, events) = attach(&current, &resume);
            assert_eq!(mode, "fresh");
            assert_eq!(events[0].name(), "blocks.page");
            assert_eq!(events[0].payload()["messages"], current["messages"]);
        }
        let (mode, events) = attach(
            &current,
            &Resume {
                history_lineage: Some("old".into()),
            },
        );
        assert_eq!(mode, "reset");
        assert_eq!(events[0].name(), "history.reset");
        assert_eq!(events[1].name(), "blocks.page");
    }
}
