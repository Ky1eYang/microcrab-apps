//! Read-only history presentation. Journal owns lineage; Context owns messages.
use super::{Managed, display_messages, link_client_results};
use microcrab_protocol::application::WireError;
use microcrab_session::Error;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashSet};
#[cfg(test)]
mod tests;

/// IO may overlap a round. Revalidate both owners rather than attach a later
/// cursor to earlier messages; callers can retry a stale read without mutation.
pub(in crate::manager) async fn coherent_messages(
    managed: &Managed,
    id: &str,
) -> Result<(super::DisplayMessages, String, u64), Error> {
    for _ in 0..3 {
        let journal = managed.journal.display_witness().await;
        let before = managed.session.context_measurement()?;
        let state = managed.session.state();
        let marker = managed.session.compaction();
        let request_head = managed
            .journal
            .history_anchor()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .request_head
            .clone();
        let mut display = display_messages(
            &managed.journal,
            marker.as_ref(),
            managed.session.messages(),
            state.epoch_id,
            id,
        )
        .await?;
        link_client_results(&managed.client_tools, id, &mut display.messages).await;
        if let Err(error) = managed
            .routing
            .annotate_display_messages(&mut display.messages)
            .await
        {
            tracing::warn!(reason = %microcrab_protocol::diagnostic::sanitize(&error, 2048), "canonical display source projection unavailable");
        }
        let after = managed.session.context_measurement()?;
        let selected_head = managed
            .journal
            .history_anchor()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .request_head
            .clone();
        if request_head == selected_head
            && before.fingerprint == after.fingerprint
            && before.epoch == after.epoch
            && managed.journal.display_witness().await == journal
        {
            return Ok((display, journal.lineage, state.epoch_id));
        }
    }
    Err(Error::Busy)
}

pub(in crate::manager) async fn live_message_page(
    managed: &Managed,
    id: &str,
    params: &crate::manager::commands::Messages,
) -> Result<Value, WireError> {
    let witness = managed.journal.display_witness().await;
    let measurement = managed
        .session
        .context_measurement()
        .map_err(crate::manager::safe)?;
    let head = managed
        .journal
        .history_anchor()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .request_head
        .clone();
    let (display, lineage, epoch) = coherent_messages(managed, id)
        .await
        .map_err(crate::manager::safe)?;
    let messages = display.messages;
    let mut page = message_page(
        messages.clone(),
        &lineage,
        epoch,
        params.before_cursor,
        params.limit.unwrap_or(64),
        params.lineage.as_deref(),
        params.before_message_id.as_deref(),
        params.after_message_id.as_deref(),
    )?;
    if let Some(items) = page["items"].as_array_mut() {
        managed
            .statistics
            .message_usage(head.clone(), &messages, items)
            .await;
    }
    managed
        .statistics
        .turn_usage(
            head.clone(),
            &display.historical_heads,
            &messages,
            &mut page["history_view"],
        )
        .await;
    let after = managed
        .session
        .context_measurement()
        .map_err(crate::manager::safe)?;
    let selected_head = managed
        .journal
        .history_anchor()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .request_head
        .clone();
    if selected_head != head
        || managed.journal.display_witness().await != witness
        || measurement.fingerprint != after.fingerprint
        || measurement.epoch != after.epoch
    {
        return Err(crate::manager::safe(Error::Busy));
    }
    Ok(page)
}

fn scope(message: &Value) -> Option<(u64, u64)> {
    Some((
        message["position"]["epoch"].as_u64()?,
        message["position"]["turn"].as_u64()?,
    ))
}

pub(in crate::manager) fn history_view(
    messages: &[Value],
    page: &Value,
    lineage: &str,
    epoch: u64,
    active: Option<(u64, u64)>,
) -> Value {
    let mut wanted: BTreeSet<_> = page["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(scope)
        .collect();
    wanted.extend(active);
    let mut counts: BTreeMap<_, (usize, usize, HashSet<String>)> = wanted
        .into_iter()
        .map(|key| (key, (0, 0, HashSet::new())))
        .collect();
    for message in messages {
        let Some(key) = scope(message) else { continue };
        let Some((ordinary, delegates, seen)) = counts.get_mut(&key) else {
            continue;
        };
        if message["role"] != "assistant" {
            continue;
        }
        for call in message["tool_calls"].as_array().into_iter().flatten() {
            let Some(id) = call["id"].as_str() else {
                continue;
            };
            if !seen.insert(id.to_owned()) {
                continue;
            }
            if call["builtin"] == "delegate" {
                *delegates += 1
            } else {
                *ordinary += 1
            }
        }
    }
    let unscoped_calls = messages.iter().any(|message| {
        message["role"] == "assistant"
            && message["tool_calls"].as_array().is_some_and(|calls| {
                !calls.is_empty()
                    && (scope(message).is_none()
                        || calls.iter().any(|call| call["id"].as_str().is_none()))
            })
    });
    if unscoped_calls {
        counts.clear();
    }
    let turns: Vec<_> = counts.into_iter().map(|((epoch,turn),(ordinary,delegates,_))| json!({"epoch":epoch,"turn":turn,"tool_calls":ordinary+delegates,"ordinary_tool_calls":ordinary,"delegate_calls":delegates,"usage":{"input_tokens":null,"output_tokens":null,"audit_sequence":null}})).collect();
    let end = page["end_cursor"].as_u64().unwrap_or(messages.len() as u64);
    let start = page["start_cursor"]
        .as_u64()
        .unwrap_or_else(|| end.saturating_sub(page["items"].as_array().map_or(0, Vec::len) as u64));
    json!({"lineage":lineage,"epoch":epoch,"message_count":messages.len(),"start_cursor":start,"end_cursor":end,"turns":turns})
}

#[allow(clippy::too_many_arguments)]
pub(in crate::manager) fn message_page(
    messages: Vec<Value>,
    lineage: &str,
    epoch: u64,
    before: Option<usize>,
    limit: usize,
    expected: Option<&str>,
    before_id: Option<&str>,
    after_id: Option<&str>,
) -> Result<Value, WireError> {
    let stale = || {
        WireError::new(
            "stale_history",
            "The selected history no longer contains this page boundary",
        )
    };
    if expected.is_some_and(|expected| expected != lineage) {
        return Err(stale());
    }
    // Stable boundaries are only meaningful together with the incarnation proof.
    if (before_id.is_some() || after_id.is_some()) && expected.is_none() {
        return Err(stale());
    }
    let end = match before_id {
        Some(id) => messages
            .iter()
            .position(|message| message["id"] == id)
            .ok_or_else(stale)?,
        None => before.unwrap_or(messages.len()).min(messages.len()),
    };
    let mut result = if let Some(id) = after_id {
        let start = messages
            .iter()
            .position(|message| message["id"] == id)
            .ok_or_else(stale)?
            + 1;
        if start > end {
            return Err(stale());
        }
        let mut items = vec![];
        let mut used = 0;
        for message in messages[start..end].iter().take(limit.clamp(1, 128)) {
            let single = super::page(vec![message.clone()], None, 1);
            let Some(projected) = single["items"]
                .as_array()
                .and_then(|items| items.first())
                .cloned()
            else {
                break;
            };
            let size = serde_json::to_vec(&projected).map_err(|_| stale())?.len();
            if used + size > 96 * 1024 {
                break;
            }
            used += size;
            items.push(projected);
        }
        let loaded_end = start + items.len();
        json!({"items":items,"truncated":loaded_end<end,"next_cursor":if loaded_end<end{Some(loaded_end)}else{None},"start_cursor":start,"end_cursor":loaded_end})
    } else {
        let mut page = super::page(messages.clone(), Some(end), limit);
        let count = page["items"].as_array().map_or(0, Vec::len);
        page["start_cursor"] = json!(end - count);
        page["end_cursor"] = json!(end);
        page
    };
    result["history_view"] = history_view(&messages, &result, lineage, epoch, None);
    Ok(result)
}
