//! Exact request ancestry and persistent display navigation certificates.
use super::error;
use microcrab_protocol::application::WireError;
use microcrab_storage::display::ArchiveRecord;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
/// Follow exact request identities, never matching a body hash, position or starting
/// history head. A sibling with the same parent cannot enter this selected chain.
pub(in crate::manager) fn selected_attempts(
    records: &[ArchiveRecord],
    heads: &HashSet<String>,
) -> HashSet<String> {
    let mut requests: HashMap<String, Vec<(&str, Option<&str>)>> = HashMap::new();
    for record in records {
        if let Some(identity) = record.event["request_identity"].as_str() {
            requests.entry(identity.into()).or_default().push((
                record.attempt_id.as_str(),
                record.event["parent_request"].as_str(),
            ));
        }
    }
    let mut pending: Vec<String> = heads.iter().cloned().collect();
    let mut visited = HashSet::new();
    let mut attempts = HashSet::new();
    while let Some(identity) = pending.pop() {
        if !visited.insert(identity.clone()) {
            continue;
        }
        if let Some(rows) = requests.get(&identity) {
            for (attempt, parent) in rows {
                attempts.insert((*attempt).to_owned());
                if let Some(parent) = parent {
                    pending.push((*parent).to_owned());
                }
            }
        }
    }
    attempts
}

pub(in crate::manager) fn unadmitted_heads(
    records: &[ArchiveRecord],
    checkpoints: &HashSet<String>,
    selected_failure: Option<&(String, String)>,
) -> (HashSet<String>, bool) {
    let (scopes, current, pending) = selection_scopes(records);
    let mut heads = HashSet::new();
    if pending {
        return (heads, true);
    }
    for record in records {
        if scopes.get(&record.sequence) != Some(&current) {
            continue;
        }
        let certified = record.event["phase"] == "admission_binding"
            && record.event["checkpoint_id"]
                .as_str()
                .is_some_and(|checkpoint| checkpoints.contains(checkpoint));
        let reconciled = record.event["phase"] == "started"
            && selected_failure.is_some_and(|(_, attempt)| &record.attempt_id == attempt);
        if (certified || reconciled)
            && let Some(identity) = record.event["request_identity"].as_str()
        {
            heads.insert(identity.to_owned());
        }
    }
    (heads, false)
}

/// Replay only acknowledged navigation, with independent display tokens. A redo
/// restores its explicit LIFO departure token only after Journal acknowledges the
/// exact target cursor. Equal checkpoints alone never choose a previous token.
/// Journal clears redo on every ordinary commit (history/{memory,disk,sqlite});
/// a changed departure cursor proves such a commit before the next travel.
fn selection_scopes(records: &[ArchiveRecord]) -> (HashMap<u64, String>, String, bool) {
    let mut scopes = HashMap::new();
    let mut current = "initial".to_owned();
    let mut cursor: Option<Value> = None;
    let mut redo: Vec<(String, Value)> = Vec::new();
    let mut intents: HashMap<String, Value> = HashMap::new();
    let mut unresolved = false;
    for record in records {
        match record.event["phase"].as_str() {
            Some("selection_intent") => {
                intents.insert(record.attempt_id.clone(), record.event.clone());
            }
            Some("selection_abort") => {
                intents.remove(&record.attempt_id);
            }
            Some("selection_commit") => {
                let Some(intent) = intents.remove(&record.attempt_id) else {
                    unresolved = true;
                    continue;
                };
                let old = intent["old_cursor"].clone();
                let target = record.event["cursor"].clone();
                if old.is_null() || target.is_null() {
                    unresolved = true;
                    continue;
                }
                if cursor.as_ref().is_some_and(|cursor| cursor != &old) {
                    redo.clear();
                }
                match intent["direction"].as_str() {
                    Some("undo") => {
                        redo.push((current, old));
                        current = record.attempt_id.clone();
                    }
                    Some("redo") => match redo.pop() {
                        Some((token, expected)) if expected == target => current = token,
                        _ => {
                            current = record.attempt_id.clone();
                            unresolved = true;
                            redo.clear();
                        }
                    },
                    _ => {
                        current = record.attempt_id.clone();
                        unresolved = true;
                        redo.clear();
                    }
                }
                cursor = Some(target);
            }
            _ => {
                scopes.insert(record.sequence, current.clone());
            }
        }
    }
    (scopes, current, unresolved || !intents.is_empty())
}
pub(in crate::manager) async fn begin_selection(
    managed: &super::super::Managed,
    direction: &str,
) -> Result<Option<String>, WireError> {
    let Some(recorder) = managed.hub.recording() else {
        return Ok(None);
    };
    let intent = uuid::Uuid::new_v4().to_string();
    recorder
        .append(
            intent.clone(),
            json!({"phase":"selection_intent","direction":direction,"old_cursor":managed.journal.cursor().await}),
        )
        .await
        .map_err(|message| {
            managed.hub.display_failed(message);
            error("Display selection fence could not be saved")
        })?;
    Ok(Some(intent))
}
pub(in crate::manager) async fn finish_selection(
    manager: &super::super::Manager,
    managed: &super::super::Managed,
    intent: Option<String>,
    committed: bool,
) {
    let Some(intent) = intent else {
        return;
    };
    let Some(recorder) = managed.hub.recording() else {
        return;
    };
    let cursor = managed.journal.cursor().await;
    let mut head = None;
    if committed {
        match navigation_head(
            manager,
            managed,
            &intent,
            &serde_json::to_value(&cursor).unwrap_or(Value::Null),
        )
        .await
        {
            Ok(Some(selected)) => {
                if let Err(message) = recorder
                    .append(
                        "display-selection".into(),
                        json!({"phase":"display_head_selected", "parent_block_id":selected.head,
                        "selected_head_known":selected.known}),
                    )
                    .await
                {
                    managed.hub.display_failed(message);
                    return;
                }
                head = Some(selected);
            }
            Ok(None) => {}
            Err(message) => {
                managed.hub.display_failed(message);
                return;
            }
        }
    }
    let mut event =
        json!({"phase":if committed {"selection_commit"}else{"selection_abort"},"cursor":cursor});
    if let Some(head) = head {
        event["selected_head"] = json!(head.head);
        event["selected_head_known"] = json!(head.known);
    }
    if let Err(message) = recorder.append(intent, event).await {
        managed.hub.display_failed(message);
    }
}

/// Resolve only explicit graph checkpoint and navigation certificates. Neither
/// request ancestry nor model turn/round counters can select a display parent.
#[derive(Clone, Debug, PartialEq, Eq)]
struct NavigationHead {
    head: Option<String>,
    known: bool,
}
impl NavigationHead {
    fn from_event(event: &Value) -> Self {
        let known = event["selected_head_known"].as_bool().unwrap_or(true);
        Self {
            head: if known {
                event["selected_head"].as_str().map(str::to_owned)
            } else {
                None
            },
            known,
        }
    }
}

async fn navigation_head(
    manager: &super::super::Manager,
    managed: &super::super::Managed,
    intent: &str,
    target: &Value,
) -> Result<Option<NavigationHead>, String> {
    let record = managed.record.lock().await.clone();
    let source = super::source(
        &manager.config,
        &record.owner,
        &record.id,
        &record.id,
        manager.config.data_dir.join("sessions").join(&record.id),
    );
    let intent = intent.to_owned();
    let target = target.clone();
    tokio::task::spawn_blocking(move || {
        let mut rows = Vec::new();
        let mut after = 0;
        let mut high = None;
        loop {
            let Some(page) = source
                .query(after, high, 256)
                .map_err(|cause| cause.to_string())?
            else {
                return Ok(None);
            };
            high = Some(page.high_water);
            rows.extend(page.records);
            let Some(next) = page.next_cursor else {
                break;
            };
            after = next;
        }
        resolve_navigation(&rows, &intent, &target)
    })
    .await
    .map_err(|_| "Display navigation worker failed".to_owned())?
}

fn resolve_navigation(
    rows: &[ArchiveRecord],
    intent_id: &str,
    target: &Value,
) -> Result<Option<NavigationHead>, String> {
    if !rows.iter().any(|row| {
        row.event.get("display_block").is_some() || row.event["phase"] == "display_graph_import"
    }) {
        return Ok(None);
    }
    let mut intents = HashMap::new();
    let mut redo: Vec<(Value, NavigationHead)> = Vec::new();
    let mut cursor: Option<Value> = None;
    for row in rows {
        match row.event["phase"].as_str() {
            Some("selection_intent") => {
                intents.insert(row.attempt_id.as_str(), &row.event);
            }
            Some("selection_abort") => {
                intents.remove(row.attempt_id.as_str());
            }
            Some("selection_commit") => {
                let Some(departure) = intents.remove(row.attempt_id.as_str()) else {
                    continue;
                };
                let old = departure["old_cursor"].clone();
                if cursor.as_ref().is_some_and(|cursor| cursor != &old) {
                    redo.clear();
                }
                match departure["direction"].as_str() {
                    Some("undo") => redo.push((old, NavigationHead::from_event(departure))),
                    Some("redo") => {
                        redo.pop();
                    }
                    _ => {
                        redo.clear();
                    }
                }
                cursor = Some(row.event["cursor"].clone());
            }
            _ => {}
        }
    }
    let departure = intents
        .get(intent_id)
        .ok_or("Display navigation intent is missing")?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor != &departure["old_cursor"])
    {
        redo.clear();
    }
    if !departure
        .as_object()
        .is_some_and(|fields| fields.contains_key("selected_head"))
    {
        return Err("Display navigation departure has no graph certificate".into());
    }
    if departure["direction"] == "redo" {
        return match redo.pop() {
            Some((expected, head)) if expected == *target => Ok(Some(head)),
            _ => Err("Display redo target does not match its acknowledged departure".into()),
        };
    }
    let checkpoint = target["checkpoint_id"]
        .as_str()
        .ok_or("Display navigation target has no checkpoint identity")?;
    let binding = rows.iter().rev().find(|row| {
        row.event["phase"] == "display_checkpoint"
            && row.event["checkpoint_id"].as_str() == Some(checkpoint)
    });
    let Some(binding) = binding else {
        // The Journal has already accepted this target. Historical checkpoints
        // have no display placement certificate; absence is unknown, not root.
        return Ok(Some(NavigationHead {
            head: None,
            known: false,
        }));
    };
    if !binding
        .event
        .as_object()
        .is_some_and(|fields| fields.contains_key("selected_head"))
    {
        return Err("Display checkpoint certificate has no selected graph head".into());
    }
    Ok(Some(NavigationHead::from_event(&binding.event)))
}

/// A failure is bound only after Directory has acknowledged its exact operation
/// receipt and Journal cursor. This is an explicit certificate, never an inference
/// from equal turn/round counters or a shared starting checkpoint.
pub(in crate::manager) async fn bind_failure(
    managed: &super::super::Managed,
    operation: &microcrab_storage::OperationRecord,
) {
    let Some(result) = &operation.result else {
        return;
    };
    let Some(attempt) = result["partial_output"]["display_ref"]["attempt_id"].as_str() else {
        return;
    };
    let Some(checkpoint) = result["journal_cursor"]["checkpoint_id"].as_str() else {
        return;
    };
    let Some(recorder) = managed.hub.recording() else {
        return;
    };
    let identity = managed.hub.request_identity(attempt);
    if let Err(message)=recorder.append(attempt.to_owned(),json!({"phase":"admission_binding","admission":"unadmitted",
        "operation_id":operation.id,"operation_revision":operation.revision,"checkpoint_id":checkpoint,"request_identity":identity})).await {
        managed.hub.display_failed(message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_checkpoint_selection_is_unknown_and_redo_keeps_exact_departure() {
        let row = |sequence, attempt: &str, event| ArchiveRecord {
            sequence,
            attempt_id: attempt.into(),
            event,
        };
        let old = json!({"checkpoint_id":"legacy"});
        let new = json!({"checkpoint_id":"current"});
        let mut rows = vec![
            row(
                1,
                "current",
                json!({"display_block":{"block_id":"exact-head"}}),
            ),
            row(
                2,
                "undo",
                json!({"phase":"selection_intent","direction":"undo","old_cursor":new,"selected_head":"exact-head","selected_head_known":true}),
            ),
        ];
        assert_eq!(
            resolve_navigation(&rows, "undo", &old).unwrap(),
            Some(NavigationHead {
                head: None,
                known: false
            })
        );
        rows.push(row(3, "undo", json!({"phase":"selection_commit","cursor":old,"selected_head":null,"selected_head_known":false})));
        assert!(!selection_scopes(&rows).2);
        rows.push(row(4, "redo", json!({"phase":"selection_intent","direction":"redo","old_cursor":old,"selected_head":null,"selected_head_known":false})));
        assert_eq!(
            resolve_navigation(&rows, "redo", &new).unwrap(),
            Some(NavigationHead {
                head: Some("exact-head".into()),
                known: true
            })
        );
        rows.push(row(5, "redo", json!({"phase":"selection_abort"})));
        assert!(!selection_scopes(&rows).2);
    }
    #[test]
    fn cancelled_suffix_survives_restart_but_travel_excludes_common_checkpoint_sibling() {
        let row = |sequence, attempt: &str, event: Value| ArchiveRecord {
            sequence,
            attempt_id: attempt.into(),
            event,
        };
        let mut records = vec![
            row(
                1,
                "a",
                json!({"phase":"started","request_identity":"request-a"}),
            ),
            row(
                2,
                "a",
                json!({"phase":"admission_binding","checkpoint_id":"same","request_identity":"request-a","operation_id":"cancel-a"}),
            ),
        ];
        let checkpoints = HashSet::from(["same".to_owned()]);
        assert_eq!(
            unadmitted_heads(&records, &checkpoints, None),
            (HashSet::from(["request-a".into()]), false)
        );
        records.push(row(
            3,
            "travel",
            json!({"phase":"selection_intent","direction":"undo","old_cursor":{"checkpoint_id":"same"}}),
        ));
        assert_eq!(
            unadmitted_heads(&records, &checkpoints, None),
            (HashSet::new(), true)
        );
        records.push(row(
            4,
            "travel",
            json!({"phase":"selection_commit","cursor":{"checkpoint_id":"same"}}),
        ));
        records.push(row(
            5,
            "b",
            json!({"phase":"started","request_identity":"request-b"}),
        ));
        records.push(row(6,"b",json!({"phase":"admission_binding","checkpoint_id":"same","request_identity":"request-b","operation_id":"cancel-b"})));
        assert_eq!(
            unadmitted_heads(&records, &checkpoints, None),
            (HashSet::from(["request-b".into()]), false)
        );
        records.push(row(7, "aborted", json!({"phase":"selection_intent"})));
        records.push(row(8, "aborted", json!({"phase":"selection_abort"})));
        assert_eq!(
            unadmitted_heads(&records, &checkpoints, None),
            (HashSet::from(["request-b".into()]), false)
        );
    }
    #[test]
    fn redo_restores_exact_departure_token_even_when_checkpoint_ids_are_equal() {
        let row = |sequence, attempt: &str, event: Value| ArchiveRecord {
            sequence,
            attempt_id: attempt.into(),
            event,
        };
        let records = vec![
            row(
                1,
                "a",
                json!({"phase":"admission_binding","checkpoint_id":"same","request_identity":"request-a"}),
            ),
            row(
                2,
                "undo",
                json!({"phase":"selection_intent","direction":"undo","old_cursor":{"checkpoint_id":"same"}}),
            ),
            row(
                3,
                "undo",
                json!({"phase":"selection_commit","cursor":{"checkpoint_id":"same"}}),
            ),
            row(
                4,
                "b",
                json!({"phase":"admission_binding","checkpoint_id":"same","request_identity":"request-b"}),
            ),
            row(
                5,
                "redo",
                json!({"phase":"selection_intent","direction":"redo","old_cursor":{"checkpoint_id":"same"}}),
            ),
            row(
                6,
                "redo",
                json!({"phase":"selection_commit","cursor":{"checkpoint_id":"same"}}),
            ),
        ];
        assert_eq!(
            unadmitted_heads(&records[..4], &HashSet::from(["same".into()]), None),
            (HashSet::from(["request-b".into()]), false)
        );
        assert_eq!(
            unadmitted_heads(&records, &HashSet::from(["same".into()]), None),
            (HashSet::from(["request-a".into()]), false)
        );
    }
    #[test]
    fn ordinary_journal_commit_invalidates_stale_redo_tokens() {
        let row = |sequence, attempt: &str, event: Value| ArchiveRecord {
            sequence,
            attempt_id: attempt.into(),
            event,
        };
        let records = vec![
            row(
                1,
                "a",
                json!({"phase":"admission_binding","checkpoint_id":"a","request_identity":"request-a"}),
            ),
            row(
                2,
                "undo-a",
                json!({"phase":"selection_intent","direction":"undo","old_cursor":{"checkpoint_id":"a"}}),
            ),
            row(
                3,
                "undo-a",
                json!({"phase":"selection_commit","cursor":{"checkpoint_id":"b"}}),
            ),
            row(
                4,
                "branch-c",
                json!({"phase":"admission_binding","checkpoint_id":"c","request_identity":"request-c"}),
            ),
            row(
                5,
                "undo-c",
                json!({"phase":"selection_intent","direction":"undo","old_cursor":{"checkpoint_id":"c"}}),
            ),
            row(
                6,
                "undo-c",
                json!({"phase":"selection_commit","cursor":{"checkpoint_id":"b"}}),
            ),
            row(
                7,
                "redo-c",
                json!({"phase":"selection_intent","direction":"redo","old_cursor":{"checkpoint_id":"b"}}),
            ),
            row(
                8,
                "redo-c",
                json!({"phase":"selection_commit","cursor":{"checkpoint_id":"c"}}),
            ),
            row(
                9,
                "invalid-redo-a",
                json!({"phase":"selection_intent","direction":"redo","old_cursor":{"checkpoint_id":"c"}}),
            ),
            row(
                10,
                "invalid-redo-a",
                json!({"phase":"selection_commit","cursor":{"checkpoint_id":"a"}}),
            ),
        ];
        assert_eq!(
            unadmitted_heads(
                &records[..8],
                &HashSet::from(["b".into(), "c".into()]),
                None
            ),
            (HashSet::from(["request-c".into()]), false)
        );
        assert_eq!(
            unadmitted_heads(&records, &HashSet::from(["a".into()]), None),
            (HashSet::new(), true)
        );
    }
    #[test]
    fn acknowledged_failure_receipt_reconciles_crash_before_binding_without_selecting_sibling() {
        let records = vec![
            ArchiveRecord {
                sequence: 1,
                attempt_id: "a".into(),
                event: json!({"phase":"started","request_identity":"request-a"}),
            },
            ArchiveRecord {
                sequence: 2,
                attempt_id: "b".into(),
                event: json!({"phase":"started","request_identity":"request-b"}),
            },
        ];
        assert_eq!(
            unadmitted_heads(
                &records,
                &HashSet::new(),
                Some(&("operation-b".into(), "b".into()))
            ),
            (HashSet::from(["request-b".into()]), false)
        );
    }
    #[test]
    fn selected_request_chain_excludes_same_position_sibling() {
        let row = |sequence, attempt: &str, request: &str, parent: Option<&str>| ArchiveRecord {
            sequence,
            attempt_id: attempt.into(),
            event: json!({"request_identity":request,"parent_request":parent,"epoch":0,"turn":1,"round":1}),
        };
        let records = vec![
            row(1, "first", "a", None),
            row(2, "sibling", "b", Some("a")),
            row(3, "selected", "c", Some("a")),
        ];
        assert_eq!(
            selected_attempts(&records, &HashSet::from(["c".into()])),
            HashSet::from(["first".into(), "selected".into()])
        );
    }
}
