//! Per-answer display facts. Journal selects the request chain; recorded State
//! positions join it to Context. Missing or ambiguous evidence stays unknown.
use super::{Statistics, clean_usage};
use microcrab_creator::audit::{
    AuditAttempt, AuditQueryResult, AuditStatus, query_selected_attempt_metadata,
    query_selected_turn_metadata_with_heads,
};
use microcrab_protocol::Usage;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

impl Statistics {
    pub(in crate::manager) async fn turn_usage(
        &self,
        head: Option<String>,
        historical_heads: &[(u64, Option<String>)],
        canonical: &[Value],
        history: &mut Value,
    ) {
        let Some(turns) = history["turns"].as_array_mut() else {
            return;
        };
        for turn in turns.iter_mut() {
            turn["usage"] =
                serde_json::json!({"input_tokens":null,"output_tokens":null,"audit_sequence":null});
        }
        let mut heads: Vec<_> = head.into_iter().collect();
        heads.extend(historical_heads.iter().filter_map(|(_, head)| head.clone()));
        if heads.is_empty() {
            return;
        }
        let scopes: Vec<_> = turns
            .iter()
            .filter_map(|turn| Some((turn["epoch"].as_u64()?, turn["turn"].as_u64()?)))
            .collect();
        if scopes.is_empty() || scopes.len() > 129 {
            return;
        }
        let directory = self.audit_source();
        let Ok(Ok(result)) = tokio::task::spawn_blocking(move || {
            query_selected_turn_metadata_with_heads(directory, &heads, &scopes)
        })
        .await
        else {
            return;
        };
        project_turns(turns, canonical, &result);
        // A historical display context with no durable request anchor cannot be
        // certified by an unrelated current epoch, even if positions coincide.
        for turn in turns {
            if historical_heads
                .iter()
                .any(|(epoch, head)| head.is_none() && turn["epoch"].as_u64() == Some(*epoch))
            {
                turn["usage"]["input_tokens"] = Value::Null;
                turn["usage"]["output_tokens"] = Value::Null;
            }
        }
    }

    pub(in crate::manager) async fn message_usage(
        &self,
        head: Option<String>,
        canonical: &[Value],
        messages: &mut [Value],
        legacy_completions: &super::super::display::anchors::LegacyCompletions,
    ) {
        for message in messages
            .iter_mut()
            .filter(|message| message["role"] == "assistant")
        {
            message["usage"] = Value::Null;
        }
        let Some(head) = head else { return };
        let positions: Vec<_> = messages
            .iter()
            .filter(|message| message["role"] == "assistant")
            .filter_map(position)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if positions.is_empty() || positions.len() > 128 {
            return;
        }
        let directory = self.audit_source();
        // Reuse the verified reader's scan/ancestry/response caps. A missing,
        // corrupt or over-budget audit must not make conversation unreadable.
        let Ok(Ok(result)) = tokio::task::spawn_blocking(move || {
            query_selected_attempt_metadata(directory, &head, &positions)
        })
        .await
        else {
            return;
        };
        project(messages, &result.attempts, canonical);
        super::super::display::anchors::project_legacy_attempts(
            legacy_completions,
            &result,
            canonical,
            messages,
        );
    }
}

type Position = (u64, u64, u64);
fn position(message: &Value) -> Option<Position> {
    let scope = &message["position"];
    Some((
        scope["epoch"].as_u64()?,
        scope["turn"].as_u64()?,
        scope["round"].as_u64()?,
    ))
}

fn project(messages: &mut [Value], attempts: &[AuditAttempt], canonical: &[Value]) {
    let mut counts = HashMap::<Position, usize>::new();
    for message in canonical
        .iter()
        .filter(|message| message["role"] == "assistant")
    {
        if let Some(scope) = position(message) {
            *counts.entry(scope).or_default() += 1;
        }
    }
    let mut accepted = HashMap::<Position, &AuditAttempt>::new();
    let mut ambiguous = HashSet::new();
    let mut requests = HashMap::new();
    let mut calls = HashMap::new();
    for attempt in attempts {
        *requests
            .entry(attempt.request_hash.as_str())
            .or_insert(0usize) += 1;
        *calls.entry(attempt.call_id.as_str()).or_insert(0usize) += 1;
    }
    for attempt in attempts {
        if attempt.subagent.is_some()
            || attempt.completion != "succeeded"
            || attempt.outcome.as_ref().and_then(Value::as_str) != Some("accepted")
        {
            continue;
        }
        let (Some(turn), Some(round)) = (attempt.turn, attempt.round) else {
            continue;
        };
        let scope = (attempt.epoch, turn, round);
        if requests.get(attempt.request_hash.as_str()) != Some(&1)
            || calls.get(attempt.call_id.as_str()) != Some(&1)
        {
            ambiguous.insert(scope);
        }
        if accepted.insert(scope, attempt).is_some() {
            ambiguous.insert(scope);
        }
    }
    for message in messages
        .iter_mut()
        .filter(|message| message["role"] == "assistant")
    {
        message["usage"] = Value::Null;
        let Some(scope) = position(message) else {
            continue;
        };
        if counts.get(&scope) != Some(&1) || ambiguous.contains(&scope) {
            continue;
        }
        let Some(usage) = accepted
            .get(&scope)
            .and_then(|attempt| attempt.usage.as_ref())
        else {
            continue;
        };
        if let Ok(usage) = serde_json::from_value::<Usage>(usage.clone()) {
            // No normalization, accumulation or total fallback: real zero and
            // each unavailable component retain the provider's persisted meaning.
            message["usage"] = serde_json::to_value(clean_usage(&usage)).unwrap_or(Value::Null);
        }
    }
}

/// Only exact canonical references can repeat a round. Distinct native assistant
/// identities at the same State position are ambiguous, not extra fragments.
/// Unknown evidence never becomes a partial whole-Turn sum.
fn project_turns(turns: &mut [Value], canonical: &[Value], result: &AuditQueryResult) {
    const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
    let mut positions = HashMap::<(u64, u64), HashSet<Position>>::new();
    let mut identities = HashMap::<&str, (&Value, Position)>::new();
    let mut round_identities = HashMap::<Position, &str>::new();
    let mut ambiguous = HashSet::new();
    let mut canonical_complete = true;
    for message in canonical
        .iter()
        .filter(|message| message["role"] == "assistant")
    {
        let Some(position) = position(message) else {
            // Its Turn cannot be attributed safely, so no requested Turn can
            // claim complete canonical coverage in this display lineage.
            canonical_complete = false;
            continue;
        };
        let scope = (position.0, position.1);
        positions.entry(scope).or_default().insert(position);
        let Some(id) = message["id"].as_str().filter(|id| !id.is_empty()) else {
            ambiguous.insert(scope);
            continue;
        };
        if let Some(previous) = round_identities.insert(position, id)
            && previous != id
        {
            ambiguous.insert(scope);
        }
        // An identifiable conflict affects only the involved Turns. Unrelated
        // requested Turns retain their independently complete canonical evidence.
        if let Some((previous, previous_position)) = identities.insert(id, (message, position))
            && previous != message
        {
            ambiguous.insert(scope);
            ambiguous.insert((previous_position.0, previous_position.1));
        }
    }
    for turn in turns {
        let Some(scope) = turn["epoch"].as_u64().zip(turn["turn"].as_u64()) else {
            continue;
        };
        let attempts: Vec<_> = result
            .attempts
            .iter()
            .filter(|attempt| {
                attempt.subagent.is_none()
                    && (attempt.epoch, attempt.turn) == (scope.0, Some(scope.1))
            })
            .collect();
        // Audit identity completeness is deliberately chain-wide: a reused or
        // missing request identity can hide which Turn owns the transport facts.
        let trustworthy = result.selected_ancestry_complete && result.selected_identities_complete;
        let first = attempts
            .iter()
            .filter(|attempt| {
                matches!(
                    attempt.status,
                    AuditStatus::Omitted | AuditStatus::Available | AuditStatus::Unchanged
                )
            })
            .min_by_key(|attempt| attempt.transmitted_sequence);
        let input = trustworthy
            .then(|| {
                first
                    .and_then(|attempt| attempt.usage.as_ref())
                    .and_then(|usage| usage["input_tokens"].as_u64())
                    .filter(|tokens| *tokens <= MAX_SAFE_INTEGER)
            })
            .flatten();
        let mut accepted = HashMap::<Position, &AuditAttempt>::new();
        let mut complete = trustworthy && canonical_complete && !ambiguous.contains(&scope);
        for attempt in &attempts {
            if attempt.completion != "succeeded"
                || attempt.outcome.as_ref().and_then(Value::as_str) != Some("accepted")
            {
                continue;
            }
            let Some(round) = attempt.round else {
                complete = false;
                continue;
            };
            if !matches!(
                attempt.status,
                AuditStatus::Omitted | AuditStatus::Available | AuditStatus::Unchanged
            ) || accepted
                .insert((scope.0, scope.1, round), attempt)
                .is_some()
            {
                complete = false;
            }
        }
        let canonical_positions = positions.get(&scope);
        if canonical_positions.is_none_or(|positions| {
            positions.len() != accepted.len()
                || positions
                    .iter()
                    .any(|position| !accepted.contains_key(position))
        }) {
            complete = false;
        }
        let output = if complete && !accepted.is_empty() {
            accepted.values().try_fold(0u64, |sum, attempt| {
                sum.checked_add(attempt.usage.as_ref()?["output_tokens"].as_u64()?)
                    .filter(|tokens| *tokens <= MAX_SAFE_INTEGER)
            })
        } else {
            None
        };
        turn["usage"] = serde_json::json!({
            "input_tokens":input,"output_tokens":output,"audit_sequence":result.committed_sequence
        });
    }
}

#[cfg(all(test, feature = "storage-file"))]
mod tests;
