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
            self.record_submissions(epoch, items, producer, submissions)
                .await
        };
        if let Err(error) = result {
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
                    let _ =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(error)));
                }
                // A later successful admission cannot repair the chronology of a
                // missing earlier certificate. Only explicit repair may clear it.
                let _ = self.recorder.append("display-projection".into(), json!({
                    "phase":"display_projection_unavailable", "reason":"canonical_admission_certificate_unavailable"
                })).await;
            }
        }
    }
    async fn record(
        &self,
        epoch: u64,
        items: &[ContentItem],
        producer: Option<&str>,
    ) -> Result<(), String> {
        self.record_submissions(epoch, items, producer, &[]).await
    }
    async fn record_submissions(
        &self,
        epoch: u64,
        items: &[ContentItem],
        producer: Option<&str>,
        submissions: &[(usize, String)],
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
        Ok(())
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
        message
            .as_object_mut()
            .ok_or("canonical DTO is not an object")?
            .remove("canonical_source_id");
        message
            .as_object_mut()
            .expect("validated message object")
            .remove("submission_operation_id");
        if let Some(source) = message["id"]
            .as_str()
            .and_then(|id| selected.canonical.get(id))
        {
            message["canonical_source_id"] = json!(source);
            if let Some(operation) = selected.submissions.get(source) {
                message["submission_operation_id"] = json!(operation);
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
    canonical_blocks: HashMap<String, (String, String)>,
    submissions: HashMap<String, String>,
    bindings: HashMap<String, (String, String)>,
    remaps: HashMap<String, (String, Vec<String>)>,
}
impl Index {
    fn new(source: &DisplaySource) -> Self {
        let scope = match source {
            DisplaySource::File { scope, .. } | DisplaySource::Sqlite { scope, .. } => {
                scope.clone()
            }
        };
        Self {
            graph: microcrab_storage::display::DisplayGraph::empty(scope),
            canonical_blocks: HashMap::new(),
            submissions: HashMap::new(),
            bindings: HashMap::new(),
            remaps: HashMap::new(),
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
        let mut ancestry = HashSet::new();
        let mut canonical = HashMap::new();
        let mut placements = HashMap::<String, Vec<String>>::new();
        let mut producer_blocks = HashMap::<String, Vec<(String, String)>>::new();
        let mut head = self.graph.selected_head.as_ref();
        while let Some(id) = head {
            if !ancestry.insert(id.clone()) {
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
            if blocks.iter().all(|id| ancestry.contains(id)) {
                canonical.insert(native.clone(), source.clone());
                placements.insert(native.clone(), blocks.clone());
            }
        }
        Ok(Selected {
            canonical,
            submissions: self.submissions.clone(),
            producer_blocks,
            placements,
        })
    }
}
