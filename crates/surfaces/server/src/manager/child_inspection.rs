//! Parent-authorized read models never assemble a child Session or acquire its writer.
#[cfg(test)]
mod tests;

use super::{Manager, safe, unavailable, view};
use crate::config::Storage;
use microcrab_protocol::application::WireError;
use microcrab_session::{Recover, context::ContentItem, runtime::execution::ExecutionSnapshot};
use microcrab_storage::{SessionRecord, history::RestorePoint};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    io::Read,
    path::{Path, PathBuf},
};

const MAX_CHILDREN: usize = 4096;

/// A runtime certificate never outlives its exact selected checkpoint. Keeping a
/// journal clone retains authority through child finalization without acquiring a
/// child writer or reconstructing a Session.
pub(super) struct ChildSelection {
    pub(super) lineage: String,
    pub(super) prefix_len: usize,
    journal: microcrab_storage::CreatorJournal,
    witness: microcrab_storage::history::DisplayWitness,
}
impl ChildSelection {
    pub(super) fn display_lineage(&self) -> &str {
        &self.witness.lineage
    }
    pub(super) async fn current(&self) -> bool {
        self.journal.display_witness().await == self.witness
    }
}

impl Manager {
    pub(super) async fn verified_child_path(
        &self,
        owner: &str,
        parent: &str,
        child: &str,
    ) -> Result<PathBuf, WireError> {
        if !valid_child(child) {
            return Err(WireError::new(
                "invalid_params",
                "Invalid child Session identity",
            ));
        }
        self.child_parent(owner, parent).await?;
        let sessions = self.config.data_dir.join("sessions");
        let parent = parent.to_owned();
        let child = child.to_owned();
        tokio::task::spawn_blocking(move || child_path(&parent_path(&sessions, &parent)?, &child))
            .await
            .map_err(|_| read_error())?
    }
    async fn child_communication_metadata(
        &self,
        value: &mut Value,
        parent: &str,
        child: &str,
        target: &str,
        point: Option<&RestorePoint>,
    ) -> Result<(), WireError> {
        view::communication_identity(
            value,
            point
                .and_then(|point| point.checkpoint.recover.state.communication.as_ref())
                .map(|communication| &communication.identity),
        );
        value["parent_session_id"] = json!(parent);
        let slot = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(parent)
            .cloned();
        if let Some(slot) = slot {
            let active = slot.active.lock().await.clone();
            if let Some(parent) = active
                && let Ok(child_session) = parent.session.inspect_child(child, target)
            {
                let accepts = parent
                    .session
                    .child_accepts_user_input(child)
                    .map_err(safe)?;
                view::communication_session(value, &child_session, Some(accepts)).map_err(safe)?;
            }
        }
        Ok(())
    }
    pub(super) async fn child_selection(
        &self,
        owner: &str,
        root: &str,
        child: &str,
        point: &RestorePoint,
    ) -> Result<Option<ChildSelection>, WireError> {
        let journal = self
            .child_journals
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&(owner.to_owned(), root.to_owned(), child.to_owned()))
            .cloned();
        let Some(journal) = journal else {
            return Ok(None);
        };
        let Some((witness, admission)) = journal.display_projection(&point.cursor).await else {
            return Err(WireError::new(
                "stale_lineage",
                "Child selection changed while loading",
            ));
        };
        let prefix_len = match admission {
            Some(admission) => admission
                .validate(&point.checkpoint.recover)
                .map_err(safe)?,
            None => point.checkpoint.recover.context.items.len(),
        };
        Ok(Some(ChildSelection {
            lineage: format!("child:{}:{}:{}", self.instance, child, witness.lineage),
            prefix_len,
            journal,
            witness,
        }))
    }
}

const MAX_PAGE_BYTES: usize = 32 * 1024;
const MAX_USAGE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ListRequest {
    pub session_id: String,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DetailRequest {
    pub session_id: String,
    pub child_id: String,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub before_cursor: Option<usize>,
}
fn valid_child(id: &str) -> bool {
    id.len() == 16
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn limit(value: Option<usize>) -> Result<usize, WireError> {
    let value = value.unwrap_or(100);
    if value == 0 || value > 100 {
        return Err(WireError::new(
            "invalid_params",
            "Child page limit must be between 1 and 100",
        ));
    }
    Ok(value)
}
fn short_name(name: &str) -> &str {
    let mut end = name.len().min(512);
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    &name[..end]
}
fn read_error() -> WireError {
    WireError::new("child_unavailable", "Child snapshot is unavailable")
}

/// Resolve each parent/child ancestor independently, rejecting symlink/junction escapes.
fn linked(path: &Path) -> Result<bool, WireError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| read_error())?;
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x400 != 0 {
            return Ok(true);
        }
    }
    Ok(metadata.file_type().is_symlink())
}
fn parent_path(sessions: &Path, id: &str) -> Result<PathBuf, WireError> {
    let root = sessions.canonicalize().map_err(|_| read_error())?;
    if linked(&sessions.join(id))? {
        return Err(read_error());
    }
    let directory = sessions.join(id).canonicalize().map_err(|_| read_error())?;
    if directory.parent() != Some(root.as_path()) {
        return Err(read_error());
    }
    Ok(directory)
}
fn children_path(parent: &Path) -> Result<Option<PathBuf>, WireError> {
    let requested = parent.join("subsessions");
    if !requested.try_exists().map_err(|_| read_error())? {
        return Ok(None);
    }
    if linked(&requested)? {
        return Err(read_error());
    }
    let actual = requested.canonicalize().map_err(|_| read_error())?;
    if actual.parent() != Some(parent) {
        return Err(read_error());
    }
    Ok(Some(actual))
}
fn child_path(parent: &Path, id: &str) -> Result<PathBuf, WireError> {
    if !valid_child(id) {
        return Err(WireError::new(
            "invalid_params",
            "Invalid child Session identity",
        ));
    }
    let children = children_path(parent)?.ok_or_else(unavailable)?;
    if linked(&children.join(id))? {
        return Err(read_error());
    }
    let path = children
        .join(id)
        .canonicalize()
        .map_err(|_| unavailable())?;
    if path.parent() != Some(children.as_path()) {
        return Err(read_error());
    }
    let metadata_path = path.join("creator.json");
    let metadata = std::fs::symlink_metadata(&metadata_path).map_err(|_| read_error())?;
    if linked(&metadata_path)? || !metadata.is_file() || metadata.len() > 4096 {
        return Err(read_error());
    }
    let mut bytes = Vec::new();
    std::fs::File::open(metadata_path)
        .map_err(|_| read_error())?
        .take(4097)
        .read_to_end(&mut bytes)
        .map_err(|_| read_error())?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| read_error())?;
    if value["version"] != 1 || value["session_id"].as_str() != Some(id) {
        return Err(read_error());
    }
    Ok(path)
}

struct ChildExecution {
    target: String,
    status: String,
}
fn child_executions(
    parent_id: &str,
    recover: &Recover,
    rows: &[ExecutionSnapshot],
) -> BTreeMap<String, ChildExecution> {
    // The selected Context must witness this exact framework execution. A live
    // row left over from another history branch cannot authorize a selected card.
    let calls: HashSet<_> = recover
        .context
        .items
        .iter()
        .filter_map(|item| match item {
            ContentItem::Assistant { tool_calls, .. } => {
                Some(tool_calls.iter().map(|call| call.id.as_str()))
            }
            _ => None,
        })
        .flatten()
        .collect();
    let selected: HashSet<_> = recover
        .context
        .items
        .iter()
        .filter_map(|item| {
            let ContentItem::Tool {
                call_id, content, ..
            } = item
            else {
                return None;
            };
            if !calls.contains(call_id.as_str()) || content.len() > 65536 {
                return None;
            }
            let value: Value = serde_json::from_str(content).ok()?;
            if value["session_id"].as_str() != Some(parent_id) {
                return None;
            }
            Some((call_id.as_str(), value["execution_id"].as_u64()?))
        })
        .collect();
    rows.iter()
        .filter_map(|row| {
            let child = row.delegate_child.as_ref()?;
            if row.session_id != parent_id
                || !valid_child(&child.session_id)
                || !selected.contains(&(row.call_id.as_str(), row.execution_id.0))
            {
                return None;
            }
            let status = serde_json::to_value(row.status).ok()?.as_str()?.to_owned();
            Some((
                child.session_id.clone(),
                ChildExecution {
                    target: child.target.clone(),
                    status,
                },
            ))
        })
        .collect()
}
// The ledger is creator-owned consumption, read without opening its writer.
// An absent legacy ledger permits receipt fallback; corrupt/linked ledgers do not.
fn child_usage(path: &Path) -> Result<Option<Value>, WireError> {
    let usage = path.join("usage.jsonl");
    let metadata = match std::fs::symlink_metadata(&usage) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(read_error()),
    };
    if !metadata.is_file() || metadata.len() > MAX_USAGE_BYTES || linked(&usage)? {
        return Err(read_error());
    }
    let total = microcrab_creator::usage::UsageLedger::read_total_named(path, "usage.jsonl")
        .map_err(|_| read_error())?;
    Ok(Some(serde_json::to_value(total).map_err(|_| read_error())?))
}

enum ChildScope {
    Directory,
    SelectedHistory,
}

impl Manager {
    async fn parent_child_executions(
        &self,
        parent_id: &str,
        point: Option<&RestorePoint>,
    ) -> BTreeMap<String, ChildExecution> {
        let Some(point) = point else {
            return BTreeMap::new();
        };
        let recover = &point.checkpoint.recover;
        // Observe an already active parent only when its selected cursor still
        // matches this read. Never activate a writer for inspection metadata.
        let slot = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(parent_id)
            .cloned();
        if let Some(slot) = slot {
            let active = slot.active.lock().await;
            if let Some(managed) = active.as_ref()
                && managed.journal.cursor().await.as_ref() == Some(&point.cursor)
            {
                return child_executions(
                    parent_id,
                    recover,
                    &managed.session.execution_snapshots(),
                );
            }
        }
        child_executions(parent_id, recover, &recover.state.executions)
    }
    async fn child_parent(&self, owner: &str, id: &str) -> Result<SessionRecord, WireError> {
        if !crate::config::valid_id(id) {
            return Err(unavailable());
        }
        let record = self
            .directory
            .get(owner, id)
            .await
            .map_err(safe)?
            .ok_or_else(unavailable)?;
        if !self.project_visible(&record).await {
            return Err(unavailable());
        }
        Ok(record)
    }
    pub(super) async fn child_checkpoint(
        &self,
        path: &Path,
        id: &str,
    ) -> Result<Option<RestorePoint>, WireError> {
        let result = match self.config.storage {
            Storage::File => {
                #[cfg(feature = "storage-file")]
                {
                    microcrab_storage::history::FileJournal::inspect_head(path).await
                }
                #[cfg(not(feature = "storage-file"))]
                {
                    let _ = path;
                    Err(microcrab_session::Error::config(
                        "File storage is not compiled",
                    ))
                }
            }
            Storage::Sqlite => {
                #[cfg(feature = "storage-sqlite")]
                {
                    microcrab_storage::history::SqliteJournal::inspect_head(
                        self.config.data_dir.join("storage.sqlite"),
                        id.to_owned(),
                    )
                    .await
                }
                #[cfg(not(feature = "storage-sqlite"))]
                {
                    Err(microcrab_session::Error::config(
                        "SQLite storage is not compiled",
                    ))
                }
            }
        };
        let selected = result.map_err(|_| read_error())?;
        if selected
            .as_ref()
            .is_some_and(|point| point.cursor.session_id != id || point.checkpoint.session_id != id)
        {
            return Err(read_error());
        }
        Ok(selected)
    }
    pub(super) async fn inspect_children(
        &self,
        owner: &str,
        request: ListRequest,
    ) -> Result<Value, WireError> {
        let page_limit = limit(request.limit)?;
        if request.cursor.as_ref().is_some_and(|id| !valid_child(id)) {
            return Err(WireError::new(
                "invalid_params",
                "Invalid child page cursor",
            ));
        }
        let record = self.child_parent(owner, &request.session_id).await?;
        let sessions = self.config.data_dir.join("sessions");
        let id = record.id.clone();
        let cursor = request.cursor.clone();
        let (parent, entries) = tokio::task::spawn_blocking(move || {
            let parent = parent_path(&sessions, &id)?;
            let Some(children) = children_path(&parent)? else {
                return Ok((parent, Vec::new()));
            };
            let mut entries = BTreeMap::new();
            for (index, entry) in std::fs::read_dir(children)
                .map_err(|_| read_error())?
                .enumerate()
            {
                if index >= MAX_CHILDREN {
                    return Err(WireError::new(
                        "child_limit",
                        "Child directory exceeds bounded inspection limit",
                    ));
                }
                let entry = entry.map_err(|_| read_error())?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if valid_child(&name) && cursor.as_ref().is_none_or(|cursor| &name > cursor) {
                    let valid = child_path(&parent, &name).is_ok();
                    entries.insert(name, valid);
                    if entries.len() > page_limit + 1 {
                        entries.pop_last();
                    }
                }
            }
            Ok((parent, entries.into_iter().collect::<Vec<_>>()))
        })
        .await
        .map_err(|_| read_error())??;
        let parent_point = self
            .child_checkpoint(&parent, &record.id)
            .await
            .ok()
            .flatten();
        let receipts = parent_point
            .as_ref()
            .map(|point| receipts(&point.checkpoint.recover))
            .unwrap_or_default();
        let bindings = self
            .parent_child_executions(&record.id, parent_point.as_ref())
            .await;
        let mut more = entries.len() > page_limit;
        let mut children = Vec::new();
        let mut list_bytes = 0;
        for (id, valid) in entries.into_iter().take(page_limit) {
            let receipt = receipts.get(&id);
            let binding = bindings.get(&id);
            let selected = if valid {
                self.child_checkpoint(&parent.join("subsessions").join(&id), &id)
                    .await
            } else {
                Err(read_error())
            };
            let (availability, point) = match selected {
                Ok(Some(point)) => ("persisted", Some(point)),
                Ok(None) => ("pending", None),
                Err(_) => ("unavailable", None),
            };
            let name = point
                .as_ref()
                .and_then(|point| point.checkpoint.recover.agent.as_ref())
                .and_then(|agent| agent.name())
                .or_else(|| binding.map(|binding| binding.target.as_str()))
                .or_else(|| receipt.and_then(|r| r["subagent"].as_str()))
                .unwrap_or(&id);
            let mut summary = json!({"id":id,"name":short_name(name),"status":binding.map(|binding| binding.status.as_str()).unwrap_or(if receipt.is_some(){"completed"}else{"unknown"}),"availability":availability});
            self.child_communication_metadata(&mut summary, &record.id, &id, name, point.as_ref())
                .await?;
            let bytes = serde_json::to_vec(&summary)
                .map_err(|_| read_error())?
                .len();
            if list_bytes + bytes > 48 * 1024 {
                more = true;
                break;
            }
            list_bytes += bytes;
            children.push(summary);
        }
        let next = if more {
            children
                .last()
                .map(|child| child["id"].clone())
                .unwrap_or(Value::Null)
        } else {
            Value::Null
        };
        Ok(
            json!({"parent_session_id":record.id,"children":children,"next_cursor":next,"limit":page_limit}),
        )
    }
    pub(super) async fn inspect_child(
        &self,
        owner: &str,
        request: DetailRequest,
    ) -> Result<Value, WireError> {
        self.child_detail(owner, request, ChildScope::Directory)
            .await
    }
    /// Resource eligibility requires a canonical child link in the selected parent
    /// history; a child directory alone permits inspection, never Host reads.
    pub(super) async fn inspect_selected_child(
        &self,
        owner: &str,
        request: DetailRequest,
    ) -> Result<Value, WireError> {
        self.child_detail(owner, request, ChildScope::SelectedHistory)
            .await
    }
    async fn child_detail(
        &self,
        owner: &str,
        request: DetailRequest,
        scope: ChildScope,
    ) -> Result<Value, WireError> {
        let page_limit = limit(request.limit)?;
        if !valid_child(&request.child_id) {
            return Err(WireError::new(
                "invalid_params",
                "Invalid child Session identity",
            ));
        }
        let record = self.child_parent(owner, &request.session_id).await?;
        let sessions = self.config.data_dir.join("sessions");
        let parent_id = record.id.clone();
        let child_id = request.child_id.clone();
        let (parent, child) = tokio::task::spawn_blocking(move || {
            let parent = parent_path(&sessions, &parent_id)?;
            let child = child_path(&parent, &child_id)?;
            Ok::<_, WireError>((parent, child))
        })
        .await
        .map_err(|_| read_error())??;
        let parent_point = match self.child_checkpoint(&parent, &record.id).await {
            Ok(point) => point,
            Err(error) if matches!(scope, ChildScope::SelectedHistory) => return Err(error),
            Err(_) => None,
        };
        let mut receipt = parent_point
            .as_ref()
            .and_then(|point| receipts(&point.checkpoint.recover).remove(&request.child_id));
        let binding = self
            .parent_child_executions(&record.id, parent_point.as_ref())
            .await
            .remove(&request.child_id);
        if matches!(scope, ChildScope::SelectedHistory) && receipt.is_none() && binding.is_none() {
            return Err(WireError::new(
                "permission_denied",
                "Child is not linked from the selected parent history",
            ));
        }
        let point = self.child_checkpoint(&child, &request.child_id).await;
        let (availability, selected) = match point {
            Ok(Some(point)) => ("persisted", Some(point)),
            Ok(None) => ("pending", None),
            Err(_) => ("unavailable", None),
        };
        let name = selected
            .as_ref()
            .and_then(|point| point.checkpoint.recover.agent.as_ref())
            .and_then(|agent| agent.name().map(str::to_owned))
            .or_else(|| binding.as_ref().map(|binding| binding.target.clone()))
            .or_else(|| {
                receipt
                    .as_ref()
                    .and_then(|r| r["subagent"].as_str())
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| request.child_id.clone());
        let legacy_usage = receipt
            .as_ref()
            .and_then(|receipt| receipt.get("usage"))
            .filter(|value| serde_json::to_vec(value).is_ok_and(|bytes| bytes.len() <= 4096))
            .cloned()
            .unwrap_or(Value::Null);
        let display_source = super::display::source(
            &self.config,
            owner,
            &record.id,
            &request.child_id,
            child.clone(),
        );
        let inspect_display = display_source.clone();
        let display = tokio::task::spawn_blocking(move || inspect_display.revision())
            .await
            .map_err(|_| read_error())?
            .map(|revision| match revision {
                Some(high_water) => json!({"high_water":high_water,"coverage":"available"}),
                None => json!({"high_water":0,"coverage":"missing"}),
            })
            .unwrap_or_else(|_| json!({"high_water":0,"coverage":"unavailable"}));
        let audit_source = super::audit::source(
            &self.config,
            owner,
            &record.id,
            &request.child_id,
            child.clone(),
        );
        let usage = tokio::task::spawn_blocking(move || child_usage(&child))
            .await
            .map_err(|_| read_error())?
            .map(|usage| usage.unwrap_or(legacy_usage))
            .unwrap_or(Value::Null);
        let mut tasks = selected
            .as_ref()
            .and_then(|point| {
                let metadata: Vec<_> = point
                    .checkpoint
                    .recover
                    .state
                    .executions
                    .iter()
                    .map(microcrab_session::runtime::execution::ExecutionSnapshot::metadata)
                    .collect();
                serde_json::to_value(metadata).ok()
            })
            .unwrap_or_else(|| json!([]));
        let tasks_truncated = bounded_tasks(&mut tasks);
        if receipt
            .as_ref()
            .is_some_and(|value| serde_json::to_vec(value).is_ok_and(|bytes| bytes.len() > 8192))
        {
            let original = receipt.take().expect("receipt exists");
            receipt = Some(
                json!({"child_id":request.child_id,"subagent":original["subagent"],"outcome":null,"truncated":true}),
            );
        }
        let child_selection = match selected.as_ref() {
            Some(point) => {
                self.child_selection(owner, &record.id, &request.child_id, point)
                    .await?
            }
            None => None,
        };
        let mut full_messages = selected
            .as_ref()
            .map(|point| {
                let mut items = point.checkpoint.recover.context.items.clone();
                if let Some(selection) = &child_selection {
                    items.truncate(selection.prefix_len);
                }
                view::messages(
                    items,
                    point.checkpoint.recover.state.epoch_id,
                    &request.child_id,
                )
            })
            .unwrap_or_default();
        let mut legacy_completions = Default::default();
        if !full_messages.is_empty() {
            match super::display::anchors::annotate_messages(display_source, &mut full_messages)
                .await
            {
                Ok(evidence) => legacy_completions = evidence,
                Err(cause) => {
                    tracing::debug!(error = %cause, "child canonical display identity unavailable")
                }
            }
        }
        let (mut messages, next, truncated) = if selected.is_some() {
            message_page(full_messages.clone(), request.before_cursor, page_limit)?
        } else {
            (vec![], None, false)
        };
        super::display::anchors::project_legacy_completion(
            &legacy_completions,
            audit_source,
            selected
                .as_ref()
                .and_then(|point| point.checkpoint.request_head.clone()),
            &full_messages,
            &mut messages,
        )
        .await;
        let end = request
            .before_cursor
            .unwrap_or(full_messages.len())
            .min(full_messages.len());
        let child_page = json!({"items":messages,"start_cursor":end.saturating_sub(messages.len()),"end_cursor":end});
        let lineage = child_selection
            .as_ref()
            .map(|selection| selection.lineage.clone())
            .unwrap_or_else(|| {
                format!(
                    "child:{}:{}:{}",
                    self.instance,
                    request.child_id,
                    selected
                        .as_ref()
                        .map(|point| point.cursor.checkpoint_id.as_str())
                        .unwrap_or("empty")
                )
            });
        let epoch = selected
            .as_ref()
            .map_or(0, |point| point.checkpoint.recover.state.epoch_id);
        let history_view = view::history_view(&full_messages, &child_page, &lineage, epoch, None);
        if let Some(selection) = &child_selection
            && !selection.current().await
        {
            return Err(WireError::new(
                "stale_lineage",
                "Child selection changed while projecting history",
            ));
        }
        let mut communication = json!({});
        self.child_communication_metadata(
            &mut communication,
            &record.id,
            &request.child_id,
            &name,
            selected.as_ref(),
        )
        .await?;
        let mut result = json!({"display":display,"history_view":history_view,"parent_session_id":record.id,"id":request.child_id,"name":short_name(&name),"status":binding.as_ref().map(|binding| binding.status.as_str()).unwrap_or(if receipt.is_some(){"completed"}else{"unknown"}),"availability":availability,"messages":messages,"next_cursor":next,"messages_truncated":truncated,"receipt":receipt,"usage":usage,"executions":tasks,"executions_truncated":tasks_truncated,"history":selected.map(|point|point.cursor)});
        if let (Some(result), Some(communication)) =
            (result.as_object_mut(), communication.as_object())
        {
            result.extend(communication.clone());
        }
        Ok(result)
    }
}

/// Only actual delegate Tool results or succeeded canonical delegate tasks are
/// receipts. User/assistant prose and JSON-looking arbitrary tool text are ignored.
fn receipts(recover: &Recover) -> BTreeMap<String, Value> {
    let mut calls = HashSet::new();
    let mut found = BTreeMap::new();
    let mut task_receipts = BTreeMap::new();
    let mut accept = |text: &str| {
        if text.len() > 65536 {
            return;
        }
        let Ok(value) = serde_json::from_str::<Value>(text) else {
            return;
        };
        let Some(id) = value["child_id"].as_str().filter(|id| valid_child(id)) else {
            return;
        };
        if value["subagent"]
            .as_str()
            .is_some_and(|name| name.len() <= 512)
            && value.get("outcome").is_some()
        {
            found.insert(id.to_owned(), value);
        }
    };
    for item in &recover.context.items {
        match item {
            ContentItem::Assistant { tool_calls, .. } => {
                for call in tool_calls {
                    if call.name == "delegate" {
                        calls.insert(call.id.clone());
                    }
                }
            }
            ContentItem::Tool {
                call_id, content, ..
            } if calls.contains(call_id) => {
                accept(content);
                if content.len() <= 65536
                    && let Ok(value) = serde_json::from_str::<Value>(content)
                    && let (Some(task_id), Some(session_id)) =
                        (value["execution_id"].as_u64(), value["session_id"].as_str())
                {
                    task_receipts.insert(call_id.clone(), (task_id, session_id.to_owned()));
                }
            }
            _ => {}
        }
    }
    for task in &recover.state.executions {
        if task.tool == "delegate"
            && task.status == microcrab_session::runtime::execution::ExecutionStatus::Succeeded
            && task_receipts
                .get(&task.call_id)
                .is_some_and(|(id, session)| {
                    *id == task.execution_id.0 && session == &task.session_id
                })
            && let Some(result) = &task.result
        {
            accept(result);
        }
    }
    found
}
fn bounded_tasks(tasks: &mut Value) -> bool {
    let Some(rows) = tasks.as_array_mut() else {
        *tasks = json!([]);
        return true;
    };
    let mut bytes = 0;
    let mut keep = 0;
    for row in rows.iter_mut().take(100) {
        if let Some(value) = row.get_mut("result")
            && value.as_str().is_some_and(|text| text.len() > 1024)
        {
            *value = Value::Null;
            row["result_truncated"] = json!(true);
        }
        let size = serde_json::to_vec(row).map_or(8193, |bytes| bytes.len());
        if bytes + size > 8192 {
            break;
        }
        bytes += size;
        keep += 1;
    }
    let truncated = keep < rows.len();
    rows.truncate(keep);
    truncated
}
fn message_page(
    messages: Vec<Value>,
    before: Option<usize>,
    limit: usize,
) -> Result<(Vec<Value>, Option<usize>, bool), WireError> {
    let end = before.unwrap_or(messages.len());
    if end > messages.len() {
        return Err(WireError::new(
            "invalid_params",
            "Child message cursor is outside the persisted snapshot",
        ));
    }
    let mut start = end;
    let mut bytes = 0;
    let mut page = Vec::new();
    for index in (0..end).rev().take(limit) {
        let mut message = messages[index].clone();
        let size = serde_json::to_vec(&message).map_or(MAX_PAGE_BYTES + 1, |bytes| bytes.len());
        if size > MAX_PAGE_BYTES {
            message = json!({"id":message["id"],"role":message["role"],"text":null,"parts":[],"tool_calls":[],"position":null,"content_omitted":true});
        }
        let size = serde_json::to_vec(&message).map_or(MAX_PAGE_BYTES + 1, |bytes| bytes.len());
        if bytes + size > MAX_PAGE_BYTES {
            break;
        }
        bytes += size;
        start = index;
        page.push(message);
    }
    page.reverse();
    let truncated = start > 0
        || page
            .iter()
            .any(|message| message["content_omitted"] == true);
    Ok((page, (start > 0).then_some(start), truncated))
}
