//! Exact display certificates projected from actual Session admission. Context
//! bytes stay frozen; a separate source identity belongs to the display archive.
//! The Session serializes admission; this leaf awaits durable writes, and reports
//! display failure without revoking an already admitted response or retrying it.
use microcrab_creator::display::DisplayRecorder;
use microcrab_storage::display::DisplaySource;
use microcrab_transport::ContentItem;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
mod fork;
#[cfg(test)]
mod tests;
pub(in crate::manager) use fork::fork_remap_event;

pub(in crate::manager) struct Anchors {
    session_id: String,
    source: DisplaySource,
    recorder: Arc<DisplayRecorder>,
    retry: Mutex<Option<String>>,
    index: Arc<Mutex<Option<Index>>>,
    #[expect(
        clippy::type_complexity,
        reason = "Display failure reporting retains one optional thread-safe observer behind a short registration lock"
    )]
    failure: Mutex<Option<Arc<dyn Fn(String) + Send + Sync>>>,
    failure_reported: std::sync::atomic::AtomicBool,
}
struct Selected {
    canonical: HashMap<String, String>,
    submissions: HashMap<String, String>,
    producer_blocks: HashMap<String, Vec<(String, String)>>,
    placements: HashMap<String, Vec<String>>,
    own_assistants: HashMap<String, Arc<[String]>>,
    executions: HashMap<String, ExecutionLink>,
    executed_calls: HashMap<String, HashMap<usize, ExecutedCall>>,
}

struct AdmittedExecution {
    assistant_index: usize,
    call_index: usize,
    execution_id: u64,
    terminal_status: Option<&'static str>,
}

struct ExecutedCall {
    call_id: String,
    execution_id: u64,
    terminal_status: Option<&'static str>,
}

fn certified_terminal_status(value: &str) -> Option<&'static str> {
    match value {
        "failed" => Some("failed"),
        "cancelled" => Some("cancelled"),
        "interrupted" => Some("interrupted"),
        _ => None,
    }
}

#[derive(Clone, PartialEq, Eq)]
struct ExecutionLink {
    id: u64,
    session: String,
    assistant_source: String,
    slot: usize,
    call_id: String,
    terminal_status: Option<&'static str>,
}
impl ExecutionLink {
    fn annotate_result(&self, message: &mut Value) {
        message["execution_id"] = json!(self.id);
        message["execution_session_id"] = json!(self.session);
        message["execution_call_source_id"] = json!(self.assistant_source);
        message["execution_call_index"] = json!(self.slot);
        if let Some(status) = self.terminal_status {
            message["execution_status"] = json!(status);
        }
    }
}
impl Anchors {
    pub(in crate::manager) fn new(
        session_id: String,
        source: DisplaySource,
        recorder: Arc<DisplayRecorder>,
    ) -> Arc<Self> {
        Arc::new(Self {
            session_id,
            source,
            recorder,
            retry: Mutex::new(None),
            index: Arc::new(Mutex::new(None)),
            failure: Mutex::new(None),
            failure_reported: std::sync::atomic::AtomicBool::new(false),
        })
    }
    pub(in crate::manager) fn set_failure_handler(
        &self,
        handler: Arc<dyn Fn(String) + Send + Sync>,
    ) {
        *self.failure.lock().unwrap_or_else(|e| e.into_inner()) = Some(handler);
    }
    pub(in crate::manager) async fn annotate_messages(
        &self,
        messages: &mut [Value],
    ) -> Result<(), String> {
        annotate(&self.selected().await?, messages)
    }
    async fn selected(&self) -> Result<Selected, String> {
        let source = self.source.clone();
        let cache = self.index.clone();
        tokio::task::spawn_blocking(move || {
            let mut cache = cache.lock().unwrap_or_else(|e| e.into_inner());
            let index = cache.get_or_insert_with(|| Index::new(&source));
            index.refresh(&source)?;
            index.selected()
        })
        .await
        .map_err(|error| format!("canonical display inspection worker failed: {error}"))?
    }
    pub(in crate::manager) fn prepare_retry(&self, attempt: String) {
        *self.retry.lock().unwrap_or_else(|e| e.into_inner()) = Some(attempt);
    }
    pub(in crate::manager) fn clear_retry(&self) {
        self.retry.lock().unwrap_or_else(|e| e.into_inner()).take();
    }
    pub(in crate::manager) async fn observe(
        &self,
        epoch: u64,
        items: &[ContentItem],
        producer: Option<&str>,
    ) {
        self.observe_with_submissions(epoch, items, producer, &[])
            .await;
    }
    pub(in crate::manager) async fn observe_input(
        &self,
        epoch: u64,
        items: &[ContentItem],
        submissions: &[(usize, String)],
    ) {
        self.observe_with_submissions(epoch, items, None, submissions)
            .await;
    }
    pub(in crate::manager) async fn observe_tool_result(
        &self,
        epoch: u64,
        items: &[ContentItem],
        assistant_index: usize,
        call_index: usize,
        execution_id: u64,
        terminal_status: Option<&str>,
    ) {
        let execution = AdmittedExecution {
            assistant_index,
            call_index,
            execution_id,
            terminal_status: terminal_status.and_then(certified_terminal_status),
        };
        if let Err(error) = self
            .record_admission(epoch, items, None, &[], Some(execution))
            .await
        {
            self.report_failure(error).await;
        }
    }
    async fn observe_with_submissions(
        &self,
        epoch: u64,
        items: &[ContentItem],
        producer: Option<&str>,
        submissions: &[(usize, String)],
    ) {
        let result = if submissions.is_empty() {
            self.record(epoch, items, producer).await
        } else {
            self.record_admission(epoch, items, producer, submissions, None)
                .await
        };
        if let Err(error) = result {
            self.report_failure(error).await;
        }
    }
    async fn report_failure(&self, error: String) {
        // Context remains admitted. Even a healthy archive can lack its
        // canonical certificate after a projection/read failure, so fence
        // completeness independently of the writer's own failure channel.
        tracing::warn!(reason = %microcrab_protocol::diagnostic::sanitize(&error, 2048), "canonical display admission unavailable");
        if !self
            .failure_reported
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            let handler = self
                .failure
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Some(handler) = handler {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(error)));
            }
            // A later successful admission cannot repair the chronology of a
            // missing earlier certificate. Only explicit repair may clear it.
            let _ = self.recorder.append("display-projection".into(), json!({
                    "phase":"display_projection_unavailable", "reason":"canonical_admission_certificate_unavailable"
                })).await;
        }
    }
    async fn record(
        &self,
        epoch: u64,
        items: &[ContentItem],
        producer: Option<&str>,
    ) -> Result<(), String> {
        self.record_admission(epoch, items, producer, &[], None)
            .await
    }
    async fn record_admission(
        &self,
        epoch: u64,
        items: &[ContentItem],
        producer: Option<&str>,
        submissions: &[(usize, String)],
        execution: Option<AdmittedExecution>,
    ) -> Result<(), String> {
        let retry = self.retry.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(attempt) = retry {
            self.recorder.select_attempt_parent(attempt).await?;
        }
        let selected = self.selected().await?;
        let submissions: HashMap<_, _> = submissions
            .iter()
            .filter_map(|(index, operation)| {
                let item = items.get(*index)?;
                matches!(item, ContentItem::User { source: None, .. }).then(|| {
                    (
                        super::super::images::message_id(&self.session_id, epoch, *index, item),
                        operation,
                    )
                })
            })
            .collect();
        let execution = execution
            .map(|execution| self.execution_link(&selected, epoch, items, execution))
            .transpose();
        let messages = super::super::view::messages(items.to_vec(), epoch, &self.session_id);
        // Only the last admitted assistant can belong to the producer supplied by
        // this callback. Prior assistants are imported as canonical snapshot rows,
        // never guessed to belong to the current request.
        let admitted =
            producer.and_then(|_| messages.iter().rposition(|m| m["role"] == "assistant"));
        for (index, mut message) in messages.into_iter().enumerate() {
            let id = message["id"]
                .as_str()
                .ok_or("canonical projection has no native id")?
                .to_owned();
            if selected.canonical.contains_key(&id) {
                continue;
            }
            let source_id = uuid::Uuid::new_v4().to_string();
            message["canonical_source_id"] = json!(source_id);
            if let Some(operation) = submissions.get(&id) {
                message["submission_operation_id"] = json!(operation);
            }
            if let Ok(Some((result, evidence))) = &execution
                && &id == result
            {
                evidence.annotate_result(&mut message);
            }
            let content = json!({"canonical_message_id":id,"canonical_source_id":source_id,"message":message});
            let producer = (admitted == Some(index)).then_some(producer).flatten();
            let blocks = producer.and_then(|p| selected.producer_blocks.get(p));
            let represented = blocks.is_some_and(|blocks| {
                blocks
                    .iter()
                    .any(|(_, kind)| kind == "text" || kind == "tool_call")
            });
            if !represented {
                self.recorder
                    .append_block(source_id.clone(), "canonical".into(), content.clone())
                    .await?;
            }
            if let Some(producer) = producer {
                let tool_call_ids: Vec<_> = message["tool_calls"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|call| call["id"].as_str())
                    .collect();
                let producer_block_ids: Vec<_> =
                    blocks.into_iter().flatten().map(|(id, _)| id).collect();
                self.recorder
                    .append(
                        producer.to_owned(),
                        json!({
                            "phase":"display_binding", "producer_call_id":producer,
                            "producer_block_ids":producer_block_ids,
                            "canonical_message_id":id,"canonical_source_id":source_id,
                            "tool_call_ids":tool_call_ids,"message":message,
                        }),
                    )
                    .await?;
            }
        }
        execution.map(|_| ())
    }
    fn execution_link(
        &self,
        selected: &Selected,
        epoch: u64,
        items: &[ContentItem],
        execution: AdmittedExecution,
    ) -> Result<(String, ExecutionLink), String> {
        let (result_index, result) = items
            .iter()
            .enumerate()
            .next_back()
            .ok_or("tool result admission has no Context items")?;
        let ContentItem::Tool { call_id, .. } = result else {
            return Err("tool result admission does not end in a tool result".into());
        };
        let Some(assistant @ ContentItem::Assistant { tool_calls, .. }) = items
            .get(execution.assistant_index)
            .filter(|_| execution.assistant_index < result_index)
        else {
            return Err("tool result admission does not identify a preceding assistant".into());
        };
        if execution.execution_id == 0
            || items[execution.assistant_index + 1..result_index]
                .iter()
                .any(|item| matches!(item, ContentItem::Assistant { .. }))
            || !tool_calls
                .get(execution.call_index)
                .is_some_and(|call| &call.id == call_id)
        {
            return Err(
                "tool result admission does not match the original call slot and execution".into(),
            );
        }
        let native = super::super::images::message_id(
            &self.session_id,
            epoch,
            execution.assistant_index,
            assistant,
        );
        let source = selected
            .canonical
            .get(&native)
            .and_then(|source| selected.own_assistants.get_key_value(source))
            .filter(|(_, calls)| {
                calls
                    .get(execution.call_index)
                    .is_some_and(|call| call == call_id)
            })
            .map(|(source, _)| source)
            .ok_or(
                "tool result admission has no selected own-session assistant call certificate",
            )?;
        let result_id =
            super::super::images::message_id(&self.session_id, epoch, result_index, result);
        let evidence = ExecutionLink {
            id: execution.execution_id,
            session: self.session_id.clone(),
            assistant_source: source.clone(),
            slot: execution.call_index,
            call_id: call_id.clone(),
            terminal_status: execution.terminal_status,
        };
        if let Some(result_source) = selected.canonical.get(&result_id) {
            // Certificates are immutable. In particular we do not fill in a
            // legacy slot even if this later notification has exact identity.
            if selected.executions.get(result_source) != Some(&evidence) {
                return Err(
                    "tool result admission was already archived without this execution certificate"
                        .into(),
                );
            }
        } else if selected
            .executed_calls
            .get(source)
            .is_some_and(|calls| calls.contains_key(&execution.call_index))
        {
            return Err("tool result admission repeats a selected assistant call slot".into());
        }
        Ok((result_id, evidence))
    }
}
/// Read-only projection for live and dormant history. Never use a previously
/// cached sibling's native id as evidence of the current occurrence.
pub(in crate::manager) async fn annotate_messages(
    source: DisplaySource,
    messages: &mut [Value],
) -> Result<(), String> {
    let selected = tokio::task::spawn_blocking(move || selected(&source))
        .await
        .map_err(|error| format!("canonical source inspection worker failed: {error}"))??;
    annotate(&selected, messages)
}
fn annotate(selected: &Selected, messages: &mut [Value]) -> Result<(), String> {
    for message in messages {
        let object = message
            .as_object_mut()
            .ok_or("canonical DTO is not an object")?;
        for field in [
            "canonical_source_id",
            "submission_operation_id",
            "execution_id",
            "execution_session_id",
            "execution_call_source_id",
            "execution_call_index",
            "execution_status",
        ] {
            object.remove(field);
        }
        let source = message["id"]
            .as_str()
            .and_then(|id| selected.canonical.get(id));
        if let Some(source) = source {
            message["canonical_source_id"] = json!(source);
            if let Some(operation) = selected.submissions.get(source) {
                message["submission_operation_id"] = json!(operation);
            }
            if message["role"] == "tool"
                && let Some(evidence) = selected.executions.get(source)
                && message["call_id"].as_str() == Some(evidence.call_id.as_str())
            {
                evidence.annotate_result(message);
            }
        }
        if message["role"] == "assistant"
            && let Some(calls) = message["tool_calls"].as_array_mut()
        {
            let executions = source.and_then(|source| selected.executed_calls.get(source));
            for (slot, call) in calls.iter_mut().enumerate() {
                if let Some(call) = call.as_object_mut() {
                    call.remove("execution_id");
                    call.remove("execution_status");
                    if let Some(execution) = executions.and_then(|calls| calls.get(&slot))
                        && execution.execution_id != 0
                        && call.get("id").and_then(Value::as_str)
                            == Some(execution.call_id.as_str())
                    {
                        call.insert("execution_id".into(), json!(execution.execution_id));
                        if let Some(status) = execution.terminal_status {
                            call.insert("execution_status".into(), json!(status));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
fn selected(source: &DisplaySource) -> Result<Selected, String> {
    let mut index = Index::new(source);
    index.refresh(source)?;
    index.selected()
}
/// Bodies are discarded immediately after suffix replay. Only graph metadata
/// and exact source certificates survive; the archive remains sole authority.
struct Index {
    graph: microcrab_storage::display::DisplayGraph,
    session: String,
    canonical_blocks: HashMap<String, (String, String)>,
    submissions: HashMap<String, String>,
    bindings: HashMap<String, (String, String)>,
    remaps: HashMap<String, (String, Vec<String>)>,
    assistants: HashMap<String, AssistantCertificate>,
    execution_blocks: HashMap<String, ExecutionLink>,
}
struct AssistantCertificate {
    calls: Arc<[String]>,
    blocks: HashSet<String>,
}
impl Index {
    fn new(source: &DisplaySource) -> Self {
        let scope = match source {
            DisplaySource::File { scope, .. } | DisplaySource::Sqlite { scope, .. } => {
                scope.clone()
            }
        };
        Self {
            session: scope.session.clone(),
            graph: microcrab_storage::display::DisplayGraph::empty(scope),
            canonical_blocks: HashMap::new(),
            submissions: HashMap::new(),
            bindings: HashMap::new(),
            remaps: HashMap::new(),
            assistants: HashMap::new(),
            execution_blocks: HashMap::new(),
        }
    }
    fn refresh(&mut self, source: &DisplaySource) -> Result<(), String> {
        let high = source
            .revision()
            .map_err(|e| e.to_string())?
            .ok_or("display archive disappeared during canonical admission")?;
        if high < self.graph.high_water {
            return Err("display archive revision moved backwards".into());
        }
        while self.graph.high_water < high {
            let page = source
                .query(self.graph.high_water, Some(high), 128)
                .map_err(|e| e.to_string())?
                .ok_or("display archive disappeared while refreshing canonical certificates")?;
            if page.records.is_empty() {
                return Err("display suffix made no progress".into());
            }
            for record in page.records {
                self.graph.apply(&record).map_err(|e| e.to_string())?;
                self.certificate(&record.event)?;
            }
        }
        Ok(())
    }
    fn certificate(&mut self, event: &Value) -> Result<(), String> {
        if event["phase"] == "display_fork_remap" {
            for mapping in event["mappings"]
                .as_array()
                .ok_or("fork remap mappings missing")?
            {
                let source = mapping["canonical_source_id"]
                    .as_str()
                    .ok_or("fork remap source identity missing")?;
                let native = mapping["canonical_message_id"]
                    .as_str()
                    .ok_or("fork remap native identity missing")?;
                let blocks: Vec<String> =
                    serde_json::from_value(mapping["producer_block_ids"].clone())
                        .map_err(|e| format!("fork remap graph witness invalid: {e}"))?;
                if blocks.is_empty() {
                    return Err("fork remap has no graph placement witness".into());
                }
                self.remaps
                    .insert(source.to_owned(), (native.to_owned(), blocks));
            }
            return Ok(());
        }
        let (content, key, binding) = match event["phase"].as_str() {
            Some("display_canonical") => (
                &event["content"],
                event["display_block"]["block_id"].as_str(),
                false,
            ),
            Some("display_binding") => (event, event["producer_call_id"].as_str(), true),
            _ => return Ok(()),
        };
        if let (Some(key), Some(id), Some(source_id)) = (
            key,
            content["canonical_message_id"].as_str(),
            content["canonical_source_id"].as_str(),
        ) {
            if content["message"]["role"] == "user"
                && content["message"]["source"] != "framework"
                && let Some(operation) = content["message"]["submission_operation_id"].as_str()
            {
                self.submissions
                    .insert(source_id.to_owned(), operation.to_owned());
            }
            let message = &content["message"];
            if message["role"] == "assistant"
                && let Some(calls) = message["tool_calls"].as_array().and_then(|calls| {
                    calls
                        .iter()
                        .map(|call| call["id"].as_str().map(str::to_owned))
                        .collect::<Option<Vec<_>>>()
                })
            {
                let assistant = self
                    .assistants
                    .entry(source_id.to_owned())
                    .or_insert_with(|| AssistantCertificate {
                        calls: calls.into(),
                        blocks: HashSet::new(),
                    });
                if binding {
                    assistant.blocks.extend(
                        event["producer_block_ids"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(Value::as_str)
                            .map(str::to_owned),
                    );
                } else {
                    assistant.blocks.insert(key.to_owned());
                }
            }
            if !binding
                && message["role"] == "tool"
                && message["execution_session_id"].as_str() == Some(self.session.as_str())
                && let (Some(execution_id @ 1..), Some(assistant_source), Some(slot), Some(call_id)) = (
                    message["execution_id"].as_u64(),
                    message["execution_call_source_id"].as_str(),
                    message["execution_call_index"]
                        .as_u64()
                        .and_then(|index| usize::try_from(index).ok()),
                    message["call_id"].as_str(),
                )
                && self
                    .assistants
                    .get(assistant_source)
                    .and_then(|assistant| assistant.calls.get(slot))
                    .is_some_and(|call| call == call_id)
            {
                self.execution_blocks.insert(
                    key.to_owned(),
                    ExecutionLink {
                        id: execution_id,
                        session: self.session.clone(),
                        assistant_source: assistant_source.to_owned(),
                        slot,
                        call_id: call_id.to_owned(),
                        terminal_status: message["execution_status"]
                            .as_str()
                            .and_then(certified_terminal_status),
                    },
                );
            }
            let certificate = (id.to_owned(), source_id.to_owned());
            if binding {
                self.bindings.insert(key.to_owned(), certificate);
            } else {
                self.canonical_blocks.insert(key.to_owned(), certificate);
            }
        }
        Ok(())
    }
    fn selected(&self) -> Result<Selected, String> {
        let mut ancestry = HashMap::<String, usize>::new();
        let mut canonical = HashMap::new();
        let mut placements = HashMap::<String, Vec<String>>::new();
        let mut execution_candidates = Vec::new();
        let mut producer_blocks = HashMap::<String, Vec<(String, String)>>::new();
        let mut head = self.graph.selected_head.as_ref();
        while let Some(id) = head {
            let position = ancestry.len();
            if ancestry.insert(id.clone(), position).is_some() {
                return Err("display ancestry cycle".into());
            }
            let block = self
                .graph
                .blocks
                .get(id)
                .ok_or("selected display ancestor missing")?;
            producer_blocks
                .entry(block.attempt_id.clone())
                .or_default()
                .push((id.clone(), block.block_type.clone()));
            if let Some((native, source)) = self.canonical_blocks.get(id) {
                if let Some(execution) = self.execution_blocks.get(id) {
                    execution_candidates.push((native, source, position, execution));
                }
                placements
                    .entry(native.clone())
                    .or_default()
                    .push(id.clone());
                canonical
                    .entry(native.clone())
                    .or_insert_with(|| source.clone());
            }
            if let Some((native, source)) = self.bindings.get(&block.attempt_id) {
                placements
                    .entry(native.clone())
                    .or_default()
                    .push(id.clone());
                canonical
                    .entry(native.clone())
                    .or_insert_with(|| source.clone());
            }
            head = block.parent_block_id.as_ref();
        }
        for blocks in producer_blocks.values_mut() {
            blocks.reverse();
        }
        for (source, (native, blocks)) in &self.remaps {
            if blocks.iter().all(|id| ancestry.contains_key(id)) {
                canonical.insert(native.clone(), source.clone());
                placements.insert(native.clone(), blocks.clone());
            }
        }
        let mut own_assistants = HashMap::new();
        let mut executions = HashMap::new();
        let mut executed_calls = HashMap::<String, HashMap<usize, ExecutedCall>>::new();
        if self.graph.selection_known && self.graph.pending_selection.is_none() {
            for source in canonical.values() {
                if let Some(assistant) = self.assistants.get(source)
                    && assistant.blocks.iter().any(|id| ancestry.contains_key(id))
                {
                    own_assistants.insert(source.clone(), assistant.calls.clone());
                }
            }
            for (native, source, result_position, evidence) in execution_candidates {
                // The result and assistant must both be the current canonical
                // occurrence; the assistant's certified placement must precede
                // the result on this exact branch (walk positions run backwards).
                if canonical.get(native) != Some(source)
                    || !own_assistants.contains_key(&evidence.assistant_source)
                    || !self.assistants[&evidence.assistant_source]
                        .blocks
                        .iter()
                        .any(|id| {
                            ancestry
                                .get(id)
                                .is_some_and(|&position| position > result_position)
                        })
                {
                    continue;
                }
                executions.insert(source.clone(), evidence.clone());
                executed_calls
                    .entry(evidence.assistant_source.clone())
                    .or_default()
                    .entry(evidence.slot)
                    .and_modify(|call| {
                        if call.execution_id != evidence.id
                            || call.terminal_status != evidence.terminal_status
                        {
                            call.execution_id = 0; // Conflicting witnesses cannot number the call.
                        }
                    })
                    .or_insert_with(|| ExecutedCall {
                        call_id: evidence.call_id.clone(),
                        execution_id: evidence.id,
                        terminal_status: evidence.terminal_status,
                    });
            }
        }
        // If a slot has conflicting surviving witnesses, omit all of them.
        executions.retain(|_, evidence| {
            executed_calls[&evidence.assistant_source][&evidence.slot].execution_id != 0
        });
        Ok(Selected {
            canonical,
            submissions: self.submissions.clone(),
            producer_blocks,
            placements,
            own_assistants,
            executions,
            executed_calls,
        })
    }
}
