//! Response completion certificates live in display metadata, never in Context.
#[cfg(test)]
mod tests;
use microcrab_creator::audit::{AuditQueryResult, AuditStatus, query_selected_attempt_metadata};
use microcrab_storage::{
    audit::AuditSource,
    display::{ArchiveRecord, DisplayGraph},
};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

const JS_DATE_LIMIT: u64 = 8_640_000_000_000_000;
const FIELD: &str = "response_completed_at_ms";
type Position = (u64, u64, u64);

#[derive(Clone, Default)]
pub(in crate::manager::display) struct ReplayCompletion {
    durable: HashMap<String, DurableCompletion>,
    legacy: LegacyCompletions,
}

#[derive(Clone)]
pub(super) struct DurableCompletion {
    pub(super) ms: u64,
    pub(super) native: String,
    producer: Option<String>,
}

impl ReplayCompletion {
    pub(super) fn new(
        durable: HashMap<String, DurableCompletion>,
        legacy: LegacyCompletions,
    ) -> Self {
        Self { durable, legacy }
    }

    pub(in crate::manager::display) fn sanitize(
        &self,
        record: &mut ArchiveRecord,
        local_session: &str,
        inherited: bool,
    ) {
        if record.event["phase"] == "display_binding" {
            if let Some(message) = record.event.get_mut("message") {
                strip(message);
            }
            let certified =
                text(&record.event, "canonical_source_id").and_then(|id| self.durable.get(id));
            let valid = certified.is_some_and(|fact| {
                fact.producer.as_deref() == Some(record.attempt_id.as_str())
                    && text(&record.event, "canonical_message_id") == Some(fact.native.as_str())
                    && completion_millis(&record.event[FIELD]) == Some(fact.ms)
            }) && record.event["producer_call_id"].as_str()
                == Some(record.attempt_id.as_str());
            if inherited || !valid {
                strip(&mut record.event);
            }
            if inherited {
                record.event["response_admission_version"] = json!(1);
            }
        } else if record.event["phase"] == "display_canonical" {
            if let Some(message) = record
                .event
                .get_mut("content")
                .and_then(|content| content.get_mut("message"))
            {
                strip(message);
            }
        } else if record.event["phase"] == "display_fork_remap" {
            let local = record.event["target_session_id"].as_str() == Some(local_session);
            if let Some(mappings) = record.event["mappings"].as_array_mut() {
                for mapping in mappings {
                    let certified =
                        text(mapping, "canonical_source_id").and_then(|id| self.durable.get(id));
                    if !local
                        || !certified.is_some_and(|fact| {
                            fact.producer.is_none()
                                && text(mapping, "canonical_message_id")
                                    == Some(fact.native.as_str())
                                && completion_millis(&mapping["message"][FIELD]) == Some(fact.ms)
                        })
                    {
                        if let Some(message) = mapping.get_mut("message") {
                            strip(message);
                        }
                    }
                }
            }
        }
    }

    pub(in crate::manager::display) fn has_legacy(&self) -> bool {
        !self.legacy.1.is_empty()
    }

    pub(in crate::manager::display) async fn project_legacy(
        &self,
        directory: AuditSource,
        head: Option<String>,
        records: &mut [ArchiveRecord],
    ) {
        let mut indexes = Vec::new();
        let mut messages = Vec::new();
        for (index, record) in records.iter().enumerate() {
            if record.event["phase"] == "display_binding"
                && record.event.get("response_admission_version").is_none()
                && let Some(candidate) = candidate(&self.legacy, &record.event["message"])
                && candidate.producer == record.attempt_id
                && record.event["producer_call_id"].as_str() == Some(candidate.producer.as_str())
            {
                indexes.push(index);
                messages.push(record.event["message"].clone());
            }
        }
        if indexes.is_empty() {
            return;
        }
        // Every selected display occurrence is counted, including legacy
        // bindings outside the current replay page.
        let canonical: Vec<_> = self.legacy.1.iter().map(|&(epoch, turn, round)| {
            json!({"role":"assistant","position":{"epoch":epoch,"turn":turn,"round":round}})
        }).collect();
        project_legacy_completion(&self.legacy, directory, head, &canonical, &mut messages).await;
        for (index, message) in indexes.into_iter().zip(messages) {
            if let Some(ms) = completion_millis(&message[FIELD]) {
                records[index].event[FIELD] = json!(ms);
            }
        }
    }
}

fn strip(event: &mut Value) {
    if let Some(object) = event.as_object_mut() {
        object.remove(FIELD);
    }
}

pub(super) fn valid_millis(ms: u64) -> bool {
    (1..=JS_DATE_LIMIT).contains(&ms)
}

fn completion_millis(value: &Value) -> Option<u64> {
    value.as_u64().filter(|ms| valid_millis(*ms))
}

fn text<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value[field].as_str().filter(|value| !value.is_empty())
}

fn position(message: &Value) -> Option<Position> {
    let scope = &message["position"];
    Some((
        scope["epoch"].as_u64()?,
        scope["turn"].as_u64()?,
        scope["round"].as_u64()?,
    ))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LegacyProducer {
    producer: String,
    request: String,
    native: String,
    position: Position,
}

#[derive(Clone, Default)]
pub(in crate::manager) struct LegacyCompletions(HashMap<String, LegacyProducer>, Vec<Position>);

#[derive(PartialEq, Eq)]
struct Binding {
    source: String,
    native: String,
    blocks: Vec<String>,
    placement: Option<String>,
    timing: Timing,
}

#[derive(PartialEq, Eq)]
enum Timing {
    Legacy(Option<Position>),
    Durable(u64),
    Unavailable,
}

#[derive(PartialEq, Eq)]
struct Remap {
    native: String,
    blocks: Vec<String>,
    ms: u64,
}

#[derive(Default)]
pub(super) struct CompletionIndex {
    // A duplicate exact fact is idempotent. Conflicting archive witnesses are
    // never allowed to choose a different completion or restore an old one.
    bindings: HashMap<String, Option<Binding>>,
    bound_sources: HashMap<String, Option<String>>,
    remaps: HashMap<String, Option<Remap>>,
    positions: HashMap<String, Option<Option<Position>>>,
    request_ids: HashMap<String, Option<String>>,
}

fn immutable<T: PartialEq>(map: &mut HashMap<String, Option<T>>, identity: &str, value: T) {
    map.entry(identity.to_owned())
        .and_modify(|prior| {
            if prior.as_ref() != Some(&value) {
                *prior = None;
            }
        })
        .or_insert_with(|| Some(value));
}

impl CompletionIndex {
    pub(super) fn observe(
        &mut self,
        record: &ArchiveRecord,
        graph: &DisplayGraph,
        canonical_blocks: &HashMap<String, (String, String)>,
    ) {
        let event = &record.event;
        if event["phase"] == "started" {
            if let Some(request) = text(event, "request_identity")
                && event["call_id"].as_str() == Some(record.attempt_id.as_str())
            {
                immutable(
                    &mut self.request_ids,
                    &record.attempt_id,
                    request.to_owned(),
                );
            } else {
                self.request_ids.insert(record.attempt_id.clone(), None);
            }
            return;
        }
        if event["phase"] == "display_fork_remap" {
            for mapping in event["mappings"].as_array().into_iter().flatten() {
                let Some(source) = text(mapping, "canonical_source_id") else {
                    continue;
                };
                if mapping["message"]["role"] == "assistant" {
                    let pos = position(&mapping["message"]);
                    immutable(&mut self.positions, source, pos);
                }
                if let Some(remap) = remap(mapping) {
                    immutable(&mut self.remaps, source, remap);
                } else if mapping["message"]["role"] == "assistant" {
                    self.remaps.insert(source.to_owned(), None);
                }
            }
            return;
        }
        let content = match event["phase"].as_str() {
            Some("display_canonical") => &event["content"],
            Some("display_binding") => event,
            _ => return,
        };
        if content["message"]["role"] == "assistant"
            && let Some(source) = text(content, "canonical_source_id")
        {
            immutable(&mut self.positions, source, position(&content["message"]));
        }
        if event["phase"] != "display_binding" {
            return;
        }
        let Some(producer) = text(event, "producer_call_id") else {
            return;
        };
        let binding = binding(record, graph, canonical_blocks);
        if let Some(binding) = binding {
            immutable(
                &mut self.bound_sources,
                &binding.source,
                producer.to_owned(),
            );
            immutable(&mut self.bindings, producer, binding);
        } else {
            if let Some(source) = text(event, "canonical_source_id") {
                self.bound_sources.insert(source.to_owned(), None);
            }
            self.bindings.insert(producer.to_owned(), None);
        }
    }

    pub(super) fn selected(
        &self,
        graph: &DisplayGraph,
        canonical: &HashMap<String, String>,
        ancestry: &HashMap<String, usize>,
        producer_blocks: &HashMap<String, Vec<(String, String)>>,
        remaps: &HashMap<String, (String, Vec<String>)>,
    ) -> (HashMap<String, DurableCompletion>, LegacyCompletions) {
        let mut durable = HashMap::new();
        let mut legacy = HashMap::new();
        if !graph.selection_known || graph.pending_selection.is_some() {
            return (durable, LegacyCompletions::default());
        }
        for (producer, binding) in &self.bindings {
            let Some(binding) = binding else { continue };
            let selected_blocks = producer_blocks.get(producer);
            let same_producer_chain = selected_blocks.is_some_and(|selected| {
                selected
                    .iter()
                    .filter(|(_, kind)| kind != "canonical")
                    .map(|(id, _)| id)
                    .eq(binding.blocks.iter())
            });
            if canonical.get(&binding.native) != Some(&binding.source)
                || self
                    .bound_sources
                    .get(&binding.source)
                    .and_then(Option::as_ref)
                    != Some(producer)
                || (!binding.blocks.is_empty() && !same_producer_chain)
                || binding
                    .placement
                    .as_ref()
                    .is_some_and(|id| !ancestry.contains_key(id))
            {
                continue;
            }
            match &binding.timing {
                Timing::Durable(ms) => {
                    durable.insert(
                        binding.source.clone(),
                        DurableCompletion {
                            ms: *ms,
                            native: binding.native.clone(),
                            producer: Some(producer.clone()),
                        },
                    );
                }
                Timing::Legacy(Some(position))
                    if !graph.projection_incomplete && graph.legacy_content_records == 0 =>
                {
                    let Some(Some(request)) = self.request_ids.get(producer) else {
                        continue;
                    };
                    legacy.insert(
                        binding.source.clone(),
                        LegacyProducer {
                            native: binding.native.clone(),
                            producer: producer.clone(),
                            request: request.clone(),
                            position: *position,
                        },
                    );
                }
                _ => {}
            }
        }
        for (source, remap) in &self.remaps {
            let Some(remap) = remap else { continue };
            if canonical.get(&remap.native) == Some(source)
                && !self.bound_sources.contains_key(source)
                && remaps.get(source).is_some_and(|(native, blocks)| {
                    native == &remap.native && blocks == &remap.blocks
                })
                && remap.blocks.iter().all(|id| ancestry.contains_key(id))
            {
                durable.insert(
                    source.clone(),
                    DurableCompletion {
                        ms: remap.ms,
                        native: remap.native.clone(),
                        producer: None,
                    },
                );
            }
        }
        let mut positions = Vec::new();
        if !legacy.is_empty() {
            for source in canonical.values() {
                match self.positions.get(source) {
                    Some(Some(Some(scope))) => positions.push(*scope),
                    Some(_) => {
                        // Unscoped selected Assistant: replay cannot prove it
                        // isn't another occurrence of the candidate position.
                        positions.clear();
                        break;
                    }
                    None => {}
                }
            }
        }
        (durable, LegacyCompletions(legacy, positions))
    }
}

fn remap(mapping: &Value) -> Option<Remap> {
    let source = text(mapping, "canonical_source_id")?;
    let native = text(mapping, "canonical_message_id")?;
    let origin = text(mapping, "source_canonical_message_id")?;
    let message = &mapping["message"];
    let blocks = blocks(mapping)?;
    if native == origin || blocks.is_empty() || !matching_assistant(message, native, source) {
        return None;
    }
    Some(Remap {
        native: native.to_owned(),
        blocks,
        ms: completion_millis(&message[FIELD])?,
    })
}

fn matching_assistant(message: &Value, native: &str, source: &str) -> bool {
    message["role"] == "assistant"
        && text(message, "id") == Some(native)
        && text(message, "canonical_source_id") == Some(source)
}

fn blocks(event: &Value) -> Option<Vec<String>> {
    let ids = event["producer_block_ids"].as_array()?;
    let mut seen = HashSet::new();
    ids.iter()
        .map(|value| {
            let id = value.as_str().filter(|id| !id.is_empty())?;
            seen.insert(id).then(|| id.to_owned())
        })
        .collect()
}

fn binding(
    record: &ArchiveRecord,
    graph: &DisplayGraph,
    canonical_blocks: &HashMap<String, (String, String)>,
) -> Option<Binding> {
    let event = &record.event;
    let producer = text(event, "producer_call_id")?;
    let source = text(event, "canonical_source_id")?;
    let native = text(event, "canonical_message_id")?;
    let message = &event["message"];
    let blocks = blocks(event)?;
    if producer != record.attempt_id
        || !matching_assistant(message, native, source)
        || !blocks.iter().all(|id| {
            graph.blocks.get(id).is_some_and(|block| {
                block.attempt_id == producer
                    && block.sequence < record.sequence
                    && block.block_type != "canonical"
            })
        })
    {
        return None;
    }
    let placement = if blocks.is_empty()
        || !blocks
            .iter()
            .any(|id| matches!(graph.blocks[id].block_type.as_str(), "text" | "tool_call"))
    {
        // Non-streaming and reasoning-only attempts have an exact canonical
        // anchor emitted just before the binding. It must represent this row.
        let id = graph.selected_head.as_ref()?;
        let (old_native, old_source) = canonical_blocks.get(id)?;
        if old_native != native
            || old_source != source
            || graph.blocks[id].block_type != "canonical"
        {
            return None;
        }
        Some(id.clone())
    } else {
        None
    };
    let timing = match (event.get("response_admission_version"), event.get(FIELD)) {
        (Some(version), Some(ms)) if version == 1 => {
            completion_millis(ms).map_or(Timing::Unavailable, Timing::Durable)
        }
        (None, None) => Timing::Legacy(position(message)),
        _ => Timing::Unavailable,
    };
    Some(Binding {
        source: source.to_owned(),
        native: native.to_owned(),
        blocks,
        placement,
        timing,
    })
}

/// Scope one metadata scan to the bounded page, only if it has a legacy
/// admission certificate. The Journal head selects the audit chain and the
/// Display source selects the canonical producer; positions only corroborate.
pub(in crate::manager) async fn project_legacy_completion(
    evidence: &LegacyCompletions,
    directory: AuditSource,
    head: Option<String>,
    canonical: &[Value],
    messages: &mut [Value],
) {
    let Some(head) = head else { return };
    let positions: HashSet<_> = messages
        .iter()
        .filter_map(|message| candidate(evidence, message).map(|candidate| candidate.position))
        .collect();
    if positions.is_empty() || positions.len() > 128 {
        return;
    }
    let positions: Vec<_> = positions.into_iter().collect();
    let Ok(Ok(result)) = tokio::task::spawn_blocking(move || {
        query_selected_attempt_metadata(directory, &head, &positions)
    })
    .await
    else {
        return;
    };
    project(evidence, &result, canonical, messages);
}

fn candidate<'a>(evidence: &'a LegacyCompletions, message: &Value) -> Option<&'a LegacyProducer> {
    if message["role"] != "assistant" || message.get(FIELD).is_some() {
        return None;
    }
    let candidate = evidence.0.get(text(message, "canonical_source_id")?)?;
    (text(message, "id") == Some(candidate.native.as_str())
        && position(message) == Some(candidate.position))
    .then_some(candidate)
}

pub(in crate::manager) fn project(
    evidence: &LegacyCompletions,
    result: &AuditQueryResult,
    canonical: &[Value],
    messages: &mut [Value],
) {
    if result.incomplete_tail
        || !result.selected_ancestry_complete
        || !result.selected_identities_complete
    {
        return;
    }
    let mut canonical_counts = HashMap::new();
    for row in canonical.iter().filter(|row| row["role"] == "assistant") {
        let Some(scope) = position(row) else { return };
        *canonical_counts.entry(scope).or_insert(0usize) += 1;
    }
    let requests: HashSet<_> = result
        .attempts
        .iter()
        .map(|attempt| &attempt.request_hash)
        .collect();
    let calls: HashSet<_> = result
        .attempts
        .iter()
        .map(|attempt| &attempt.call_id)
        .collect();
    if requests.len() != result.attempts.len() || calls.len() != result.attempts.len() {
        return;
    }
    for message in messages {
        let Some(producer) = candidate(evidence, message) else {
            continue;
        };
        if canonical_counts.get(&producer.position) != Some(&1) {
            continue;
        }
        let (epoch, turn, round) = producer.position;
        let mut successful = result.attempts.iter().filter(|attempt| {
            attempt.subagent.is_none()
                && (attempt.epoch, attempt.turn, attempt.round) == (epoch, Some(turn), Some(round))
                && attempt.completion == "succeeded"
                && attempt.outcome.as_ref().and_then(Value::as_str) == Some("accepted")
        });
        if let Some(attempt) = successful.next()
            && successful.next().is_none()
            && attempt.call_id == producer.producer
            && attempt.request_hash == producer.request
            && attempt.transmitted_sequence.is_some()
            && matches!(
                attempt.status,
                AuditStatus::Omitted | AuditStatus::Available | AuditStatus::Unchanged
            )
            && let Some(ms) = attempt
                .response_completed_at_ms
                .filter(|ms| valid_millis(*ms))
        {
            message[FIELD] = json!(ms);
        }
    }
}
