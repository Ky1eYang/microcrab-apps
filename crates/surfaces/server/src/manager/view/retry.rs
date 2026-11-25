//! Retry feedback ancestry is projected from exact admitted links or command hashes.
//! Directory records and immutable Journal history are never rewritten here.
use microcrab_protocol::application::Command;
use microcrab_session::Error;
use microcrab_storage::OperationRecord;
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap, HashSet};

fn failed(operation: &OperationRecord) -> bool {
    matches!(operation.status.as_str(), "failed" | "cancelled")
        && operation
            .result
            .as_ref()
            .is_some_and(|result| result["retry_ready"] == true)
        && matches!(
            operation.method.as_str(),
            "turn.submit" | "turn.retry" | "turn.inbox" | "inbox.resume"
        )
}
fn witness(result: &Value) -> Option<&str> {
    result["retry_witness"]
        .as_str()
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

/// Consume projection-owned records before paging. The returned selected-branch
/// set remains authoritative even when the retry receipt itself leaves the page.
pub(crate) fn retry_supersession(
    operations: &mut [OperationRecord],
    messages: &[Value],
) -> Result<Vec<String>, Error> {
    let anchors: HashSet<_> = messages
        .iter()
        .filter_map(|message| message["id"].as_str())
        .collect();
    let mut by_id = HashMap::new();
    let mut legacy_candidates: HashMap<_, Vec<usize>> = HashMap::new();
    for (index, operation) in operations.iter().enumerate() {
        by_id.insert(
            (
                operation.owner.as_str(),
                operation.session_id.as_str(),
                operation.id.as_str(),
            ),
            index,
        );
        if failed(operation)
            && let Some(result) = &operation.result
            && let (Some(witness), Some(anchor)) =
                (witness(result), result["after_message_id"].as_str())
            && anchors.contains(anchor)
        {
            legacy_candidates
                .entry((
                    operation.owner.as_str(),
                    operation.session_id.as_str(),
                    witness,
                    anchor,
                ))
                .or_default()
                .push(index);
        }
    }
    let mut links = Vec::new();
    for (index, retry) in operations.iter().enumerate() {
        if retry.method != "turn.retry"
            || !matches!(
                retry.status.as_str(),
                "accepted" | "running" | "completed" | "failed" | "cancelled" | "interrupted"
            )
        {
            continue;
        }
        let Some(result) = &retry.result else {
            continue;
        };
        let has_explicit = result.get("retry_of_operation_id").is_some()
            || result.get("retry_after_message_id").is_some();
        let selected = if has_explicit {
            match (
                result["retry_of_operation_id"].as_str(),
                result["retry_after_message_id"].as_str(),
            ) {
                (Some(previous), Some(anchor)) if anchors.contains(anchor) => by_id
                    .get(&(retry.owner.as_str(), retry.session_id.as_str(), previous))
                    .copied()
                    .filter(|&previous| {
                        let previous = &operations[previous];
                        failed(previous)
                            && previous.created_at_ms < retry.created_at_ms
                            && previous
                                .result
                                .as_ref()
                                .and_then(|result| result["after_message_id"].as_str())
                                == Some(anchor)
                    }),
                _ => None,
            }
        } else if matches!(retry.status.as_str(), "failed" | "cancelled")
            && result["retry_ready"] == true
        {
            if let (Some(witness), Some(anchor)) =
                (witness(result), result["after_message_id"].as_str())
            {
                let candidates = legacy_candidates.get(&(
                    retry.owner.as_str(),
                    retry.session_id.as_str(),
                    witness,
                    anchor,
                ));
                let mut matched = None;
                let mut ambiguous = false;
                for &previous in candidates.into_iter().flatten() {
                    let previous_operation = &operations[previous];
                    if previous_operation.created_at_ms >= retry.created_at_ms {
                        continue;
                    }
                    let command: Command = serde_json::from_value(json!({
                        "v":1,"type":"command","request_id":retry.id,
                        "method":"turn.retry","session_id":retry.session_id,
                        "params":{"operation_id":retry.id,"failed_operation_id":previous_operation.id},
                    })).map_err(|_| Error::protocol("retry projection command invalid"))?;
                    if super::super::commands::hash(&command)
                        .map_err(|_| Error::protocol("retry projection hash failed"))?
                        == retry.payload_hash
                    {
                        if matched.is_some() {
                            ambiguous = true;
                            break;
                        }
                        matched = Some(previous);
                    }
                }
                if ambiguous { None } else { matched }
            } else {
                None
            }
        } else {
            None
        };
        if let Some(previous) = selected {
            let previous = &operations[previous];
            let anchor = previous
                .result
                .as_ref()
                .and_then(|result| result["after_message_id"].as_str())
                .expect("selected retry has a validated message anchor");
            links.push((index, previous.id.clone(), anchor.to_owned()));
        }
    }
    // Drop invalid/unselected relationship fields as well: a raw field alone is
    // never authority to hide a failure in the browser.
    for operation in operations.iter_mut() {
        if let Some(Value::Object(result)) = operation.result.as_mut() {
            result.remove("retry_of_operation_id");
            result.remove("retry_after_message_id");
        }
    }
    let mut superseded = BTreeSet::new();
    for (index, previous, anchor) in links {
        if let Some(result) = operations[index].result.as_mut() {
            result["retry_of_operation_id"] = json!(previous);
            result["retry_after_message_id"] = json!(anchor);
            superseded.insert(previous);
        }
    }
    Ok(superseded.into_iter().collect())
}

#[cfg(test)]
mod tests;
