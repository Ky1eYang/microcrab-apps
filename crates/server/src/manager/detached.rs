//! History inspection never assembles an agent or touches a remote workspace.
use super::{Manager, hub::Hub, safe, view};
use crate::config::Storage;
use microcrab_protocol::application::WireError;
use microcrab_session::{Error, Journal};
use microcrab_storage::{CreatorJournal, SessionRecord};
use serde_json::{Value, json};
use std::sync::Arc;

/// Projection budget: at most this many dormant sessions, and only heads whose
/// checkpoint file is at most `DORMANT_NODE_BYTES`, stay resident.
const DORMANT_VIEWS: usize = 8;
const DORMANT_NODE_BYTES: u64 = 16 * 1024 * 1024;

/// The journal-derived part of a dormant view at one published manifest.
/// Every field is a pure function of the manifest bytes and the immutable,
/// content-addressed checkpoint nodes they name, so a byte-equal manifest proves
/// an entry current without opening (restoring, hashing and projecting) the
/// journal again. Undo/redo availability also depends only on those nodes and
/// the manifest's redo stack; it is filled lazily.
pub(super) struct Dormant {
    session_id: String,
    manifest: Option<Vec<u8>>,
    recover: Option<microcrab_session::Recover>,
    history: Option<microcrab_storage::JournalCursor>,
    model_selection: Option<microcrab_storage::ModelSelection>,
    request_head: Option<String>,
    measurement: Option<microcrab_session::context::ContextMeasurement>,
    messages: Vec<Value>,
    historical_heads: Vec<(u64, Option<String>)>,
    history_available: std::sync::OnceLock<microcrab_storage::history::HistoryAvailable>,
}
/// History page identity for a non-active Session, based on the published head.
pub(super) fn detached_lineage(
    instance: &impl std::fmt::Display,
    history: Option<&microcrab_storage::JournalCursor>,
) -> String {
    format!(
        "detached:{}:{}",
        instance,
        history.map_or("empty", |cursor| cursor.checkpoint_id.as_str())
    )
}

impl Dormant {
    fn lineage(&self, instance: &impl std::fmt::Display) -> String {
        detached_lineage(instance, self.history.as_ref())
    }
    fn display(&self) -> view::DisplayMessages {
        view::DisplayMessages {
            messages: self.messages.clone(),
            historical_heads: self.historical_heads.clone(),
        }
    }
}

impl Manager {
    #[cfg(test)]
    pub(super) async fn inspect_messages(
        &self,
        record: &SessionRecord,
    ) -> Result<Vec<Value>, WireError> {
        let _journal_open = self.journal_open_gate(&record.id).lock_owned().await;
        Ok(self.inspect_message_source(record).await?.0.messages)
    }
    pub(super) async fn inspect_message_page(
        &self,
        record: &SessionRecord,
        params: &super::commands::Messages,
    ) -> Result<Value, WireError> {
        let _journal_open = self.journal_open_gate(&record.id).lock_owned().await;
        let slot = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&record.id)
            .cloned();
        if let Some(slot) = slot {
            let active = slot.active.lock().await;
            if let Some(managed) = active.as_ref() {
                return view::live_message_page(managed, &record.id, params).await;
            }
        }
        let (display, lineage, epoch, head) = self.inspect_message_source(record).await?;
        let items = display.messages;
        let mut page = view::message_page(
            items.clone(),
            &lineage,
            epoch,
            params.before_cursor,
            params.limit.unwrap_or(64),
            params.lineage.as_deref(),
            params.before_message_id.as_deref(),
            params.after_message_id.as_deref(),
        )?;
        if let Some(page_items) = page["items"].as_array_mut() {
            super::statistics::Statistics::new(&self.config, &record.id)
                .scoped(&record.owner)
                .message_usage(head.clone(), &items, page_items)
                .await;
        }
        super::statistics::Statistics::new(&self.config, &record.id)
            .scoped(&record.owner)
            .turn_usage(
                head,
                &display.historical_heads,
                &items,
                &mut page["history_view"],
            )
            .await;
        Ok(page)
    }
    pub(super) async fn inspect_message_source(
        &self,
        record: &SessionRecord,
    ) -> Result<(view::DisplayMessages, String, u64, Option<String>), WireError> {
        let slot = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&record.id)
            .cloned();
        let active = match slot {
            Some(slot) => slot.active.lock().await.clone(),
            None => None,
        };
        if let Some(managed) = active.as_ref() {
            let (messages, lineage, epoch) = view::coherent_messages(managed, &record.id)
                .await
                .map_err(safe)?;
            let head = managed
                .journal
                .history_anchor()
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .request_head
                .clone();
            return Ok((messages, lineage, epoch, head));
        }
        let dormant = self.dormant(record, false).await.map_err(safe)?;
        let lineage = dormant.lineage(&self.instance);
        let epoch = dormant
            .recover
            .as_ref()
            .map_or(0, |recover| recover.state.epoch_id);
        let mut display = dormant.display();
        self.annotate_detached_display(record, &mut display.messages)
            .await;
        let request_head = dormant.request_head.clone();
        let bridge = self.client_tools(&record.id).await.map_err(safe)?;
        view::link_client_results(&bridge, &record.id, &mut display.messages).await;
        Ok((display, lineage, epoch, request_head))
    }
    async fn annotate_detached_display(&self, record: &SessionRecord, messages: &mut [Value]) {
        if messages.is_empty() {
            return;
        }
        let source = super::display::source(
            &self.config,
            &record.owner,
            &record.id,
            &record.id,
            self.config.data_dir.join("sessions").join(&record.id),
        );
        if let Err(cause) = super::display::anchors::annotate_messages(source, messages).await {
            tracing::debug!(error = %cause, "dormant canonical display identity unavailable");
        }
    }
    /// Published manifest bytes of a file journal; `None` (never cached) for
    /// SQLite storage, a missing journal, or a linked manifest.
    async fn dormant_manifest(&self, id: &str) -> Option<Vec<u8>> {
        if !matches!(self.config.storage, Storage::File) {
            return None;
        }
        let path = self
            .config
            .data_dir
            .join("sessions")
            .join(id)
            .join("journal")
            .join("manifest.json");
        let metadata = tokio::fs::symlink_metadata(&path).await.ok()?;
        if !metadata.is_file() || metadata.len() > 64 * 1024 {
            return None;
        }
        tokio::fs::read(path).await.ok()
    }
    /// Drop a session's resident projection (deletion, privacy).
    pub(super) fn forget_dormant(&self, id: &str) {
        self.dormant_views
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|entry| entry.session_id != id);
    }
    /// The dormant journal projection, from the manifest-keyed cache when the
    /// published manifest is byte-equal; otherwise one journal open computes it.
    /// With `history`, undo/redo availability is resolved too. Callers own
    /// journal-open admission and have already excluded an active runtime.
    pub(super) async fn dormant(
        &self,
        record: &SessionRecord,
        history: bool,
    ) -> Result<Arc<Dormant>, Error> {
        let manifest = self.dormant_manifest(&record.id).await;
        let cached = manifest.as_ref().and_then(|manifest| {
            let mut views = self
                .dormant_views
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let index = views.iter().position(|entry| {
                entry.session_id == record.id && entry.manifest.as_ref() == Some(manifest)
            })?;
            let entry = views.remove(index)?;
            views.push_front(entry.clone());
            Some(entry)
        });
        if let Some(entry) = cached.as_ref()
            && (!history || entry.history_available.get().is_some())
        {
            return Ok(entry.clone());
        }
        let (journal, recover) = self.inspect_journal(record).await?;
        // The opened journal holds its exclusive lock: these bytes name exactly
        // the head that was restored.
        let opened = self.dormant_manifest(&record.id).await;
        let entry = match cached {
            Some(entry) if opened.is_some() && entry.manifest == opened => entry,
            _ => {
                let history_cursor = journal.cursor().await;
                let request_head = journal
                    .history_anchor()
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .request_head
                    .clone();
                // Measurement and display projection both walk the whole
                // Context; run measurement on a blocking worker meanwhile.
                let measuring = recover.as_ref().map(|recover| {
                    let context = recover.context.clone();
                    tokio::task::spawn_blocking(move || {
                        microcrab_session::context::Context::from_snapshot(context)
                            .and_then(|context| context.measure())
                    })
                });
                let display = match recover.as_ref() {
                    Some(recover) => {
                        view::display_messages(
                            &journal,
                            recover.context.compaction.as_ref(),
                            recover.context.items.clone(),
                            recover.state.epoch_id,
                            &record.id,
                        )
                        .await?
                    }
                    None => view::DisplayMessages::default(),
                };
                let measurement = match measuring {
                    Some(measuring) => Some(
                        measuring
                            .await
                            .map_err(|_| Error::protocol("context measurement worker failed"))??,
                    ),
                    None => None,
                };
                let resident = match (opened.as_ref(), history_cursor.as_ref()) {
                    (Some(_), Some(cursor)) => tokio::fs::metadata(
                        self.config
                            .data_dir
                            .join("sessions")
                            .join(&record.id)
                            .join("journal")
                            .join("nodes")
                            .join(format!("{}.json", cursor.checkpoint_id)),
                    )
                    .await
                    .is_ok_and(|metadata| metadata.len() <= DORMANT_NODE_BYTES),
                    (Some(_), None) => true,
                    _ => false,
                };
                Arc::new(Dormant {
                    session_id: record.id.clone(),
                    manifest: if resident { opened } else { None },
                    recover,
                    history: history_cursor,
                    model_selection: journal.model_selection().await,
                    request_head,
                    measurement,
                    messages: display.messages,
                    historical_heads: display.historical_heads,
                    history_available: std::sync::OnceLock::new(),
                })
            }
        };
        if history && entry.history_available.get().is_none() {
            let available = journal.history_available().await?;
            let _ = entry.history_available.set(available);
        }
        journal.close().await?;
        if entry.manifest.is_some() {
            let mut views = self
                .dormant_views
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            views.retain(|resident| resident.session_id != record.id);
            views.push_front(entry.clone());
            views.truncate(DORMANT_VIEWS);
        }
        Ok(entry)
    }
    /// After a session goes dormant, project it in the background so the next
    /// attach answers from the cache; availability follows the lazy probe.
    pub(super) fn warm_dormant(&self, record: SessionRecord) {
        let Some(manager) = self.self_ref.upgrade() else {
            return;
        };
        let _ = self.track(async move {
            let _journal_open = manager.journal_open_gate(&record.id).lock_owned().await;
            let slot = manager
                .registry
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&record.id)
                .cloned();
            if let Some(slot) = slot
                && slot.active.lock().await.is_some()
            {
                return;
            }
            if let Err(error) = manager.dormant(&record, false).await {
                tracing::debug!(reason=%microcrab_protocol::diagnostic::describe(&error,2048), "dormant projection warm-up failed");
            }
        });
    }
    /// Test-only owner-scoped full inspection (the retired `session.get`).
    #[cfg(test)]
    pub(crate) async fn inspect_owned(&self, owner: &str, id: &str) -> Result<Value, WireError> {
        let record = self.record(owner, id).await?;
        self.inspect(&record).await
    }
    pub(super) async fn inspect(&self, record: &SessionRecord) -> Result<Value, WireError> {
        let _journal_open = self.journal_open_gate(&record.id).lock_owned().await;
        self.inspect_locked(record).await
    }
    /// Caller owns journal-open admission through inspection and any read-Hub
    /// publication. A subscription must not recursively acquire the same gate.
    async fn inspect_locked(&self, record: &SessionRecord) -> Result<Value, WireError> {
        self.inspect_view(record, false).await
    }
    /// With `lazy_history`, a dormant view reports `history_available: null`;
    /// the attach path probes undo/redo (one full parent read each) after the
    /// subscription is already answered.
    async fn inspect_view(
        &self,
        record: &SessionRecord,
        lazy_history: bool,
    ) -> Result<Value, WireError> {
        // Archive owns journal-open admission through cleanup and publication.
        // A read may have captured its record before waiting for that owner;
        // reload only after the fence so reconciliation observes the settled fact.
        let current = self.record(&record.owner, &record.id).await?;
        let record = &current;
        let slot = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&record.id)
            .cloned();
        // Release Slot.active before the O(history) refresh; the journal gate
        // still excludes activation, archive and deletion for this session.
        let active = match slot {
            Some(slot) => slot.active.lock().await.clone(),
            None => None,
        };
        {
            if let Some(managed) = active.as_ref() {
                let mut snapshot = self.refresh(managed).await.map_err(safe)?;
                snapshot["host_connected"] =
                    json!(self.embedded || self.record_host(record).await.is_ok());
                return Ok(snapshot);
            }
        }
        // Journal projection, pins, client tools, operation receipts and display
        // metadata are independent reads; answer from whichever is slowest.
        let display_source = super::display::source(
            &self.config,
            &record.owner,
            &record.id,
            &record.id,
            self.config.data_dir.join("sessions").join(&record.id),
        );
        let (dormant, pin, conversation_pin, bridge, operations, display_revision) = tokio::try_join!(
            async { self.dormant(record, !lazy_history).await.map_err(safe) },
            async { self.pins.session(&record.id).await.map_err(safe) },
            async {
                self.pins
                    .conversation_session(&record.id)
                    .await
                    .map_err(safe)
            },
            async { self.client_tools(&record.id).await.map_err(safe) },
            async {
                self.directory
                    .list_operations(&record.owner, &record.id)
                    .await
                    .map_err(safe)
            },
            async {
                tokio::task::spawn_blocking(move || display_source.revision())
                    .await
                    .map_err(|_| {
                        WireError::new("display_unavailable", "Display metadata worker failed")
                    })
            },
        )?;
        let mut operations = operations;
        let recover = dormant.recover.as_ref();
        let history = dormant.history.clone();
        let history_available = dormant
            .history_available
            .get()
            .map_or(Value::Null, |available| json!(available));
        let request_head = dormant.request_head.clone();
        let marker = recover.and_then(|recover| recover.context.compaction.clone());
        let measurement = dormant.measurement.as_ref();
        let display = dormant.display();
        let mut messages = display.messages;
        self.annotate_detached_display(record, &mut messages).await;
        let mut session = view::session(
            self,
            record,
            &self.config.provider.model,
            if record.archived { "closed" } else { "idle" },
        );
        session["mode"] = json!(recover.and_then(|recover| recover.state.selected_mode.as_deref()));
        view::communication_identity(
            &mut session,
            recover
                .and_then(|recover| recover.state.communication.as_ref())
                .map(|communication| &communication.identity),
        );
        if let Some(kind) = recover.and_then(|recover| recover.state.session_kind.as_ref()) {
            session["kind"] = json!(kind);
        }
        session["component"] = json!(
            pin.and_then(|pin| pin.component_digest)
                .or_else(|| conversation_pin.and_then(|pin| pin.component_digest))
        );
        let executions: Vec<_> = recover
            .as_ref()
            .map(|recover| {
                recover
                    .state
                    .executions
                    .iter()
                    .map(|row| {
                        let mut row = row.metadata();
                        if row.status.is_pending() {
                            row.status =
                                microcrab_session::runtime::execution::ExecutionStatus::Interrupted;
                        }
                        row
                    })
                    .collect()
            })
            .unwrap_or_default();
        view::link_client_results(&bridge, &record.id, &mut messages).await;
        let selected_invocation_ids = view::selected_client_invocations(&messages);
        let compactions = view::compactions(marker.as_ref(), &messages);
        let statistics = if let Some(measurement) = measurement {
            super::statistics::Statistics::with_metadata(
                &self.config,
                &record.id,
                self.model_metadata.clone(),
            )
            .scoped(&record.owner)
            .detached(
                record,
                measurement,
                request_head.clone(),
                &operations,
                dormant.model_selection.as_ref(),
            )
            .await
            .map_err(safe)?
        } else {
            Value::Null
        };
        view::retain_context_failures(&mut operations, &messages);
        let superseded_failure_operation_ids =
            view::retry_supersession(&mut operations, &messages).map_err(safe)?;
        let retry_operation = if !record.archived
            && let Some(recover) = recover.as_ref()
            && recover
                .continuation
                .as_ref()
                .is_some_and(|continuation| continuation.request_prepared)
        {
            view::retry_operation_id(
                &operations,
                &view::retry_witness(&recover.context, &recover.state).map_err(safe)?,
            )
        } else {
            None
        };
        session["retry_ready"] = json!(retry_operation.is_some());
        session["auto_compaction_retry_available"] = json!(
            record
                .auto_compaction
                .as_ref()
                .unwrap_or(&self.config.auto_compaction)
                .enabled
                && recover
                    .as_ref()
                    .is_some_and(|recover| recover.context.auto_compaction_retry
                        && !recover
                            .continuation
                            .as_ref()
                            .is_some_and(|continuation| continuation.request_prepared))
        );
        session["retry_operation_id"] = json!(retry_operation);
        let agent_init = recover
            .as_ref()
            .map(|recover| recover.state.agent_init.clone())
            .unwrap_or_default();
        let agent_initialization_pending = recover
            .as_ref()
            .is_some_and(|recover| recover.state.agent_initialization_pending);
        session["agent_initialization_deferred"] = json!(false);
        session["agent_init_pending"] = json!(false);
        session["agent_init_retry_required"] =
            json!(agent_initialization_pending || super::agent_init::blocked(&agent_init));
        let mut page = view::page(messages.clone(), None, 64);
        if let Some(items) = page["items"].as_array_mut() {
            super::statistics::Statistics::new(&self.config, &record.id)
                .scoped(&record.owner)
                .message_usage(request_head.clone(), &messages, items)
                .await;
        }
        let lineage = dormant.lineage(&self.instance);
        let epoch = recover.map_or(0, |recover| recover.state.epoch_id);
        let mut history_view = view::history_view(&messages, &page, &lineage, epoch, None);
        super::statistics::Statistics::new(&self.config, &record.id)
            .scoped(&record.owner)
            .turn_usage(
                request_head,
                &display.historical_heads,
                &messages,
                &mut history_view,
            )
            .await;
        let (operations, truncated) = view::operation_page(operations, retry_operation.as_deref());
        let operations = view::project_operation_results(
            operations,
            &superseded_failure_operation_ids,
            retry_operation.as_deref(),
        );
        let mut client_tools = serde_json::to_value(bridge.snapshot().await)
            .map_err(|_| safe(Error::protocol("client tool snapshot serialization failed")))?;
        client_tools["selected_invocation_ids"] = json!(selected_invocation_ids);
        let display = display_revision
            .map(|revision| match revision {
                Some(high) => json!({"high_water":high,"coverage":"available"}),
                None => json!({"high_water":0,"coverage":"missing"}),
            })
            .unwrap_or_else(|_| json!({"high_water":0,"coverage":"unavailable"}));
        let mut state = dormant_current_state(recover).map_err(safe)?;
        state["agent_init"] = json!(agent_init);
        state["agent_initialization_pending"] = json!(agent_initialization_pending);
        view::bound_snapshot(
            json!({"display":display,"inbox":{"items":[],"active":false,"paused":false},"statistics":statistics,"compactions":compactions,"history_available":history_available,"client_tools":client_tools,"session":session,"messages":page["items"],"messages_truncated":page["truncated"],"next_cursor":page["next_cursor"],"history_view":history_view,"history":history,"operations":operations,"superseded_failure_operation_ids":superseded_failure_operation_ids,"operations_truncated":truncated,"state":state,"executions":executions,"host_connected":false}),
        )
        .map_err(safe)
    }
    /// Inspect a selected head without opening or creating a journal. Callers
    /// hold journal-open admission and verify the checkpoint's Session identity.
    pub(super) async fn inspect_detached_head(
        &self,
        id: &str,
    ) -> Result<Option<microcrab_storage::history::RestorePoint>, Error> {
        match self.config.storage {
            Storage::File => {
                #[cfg(feature = "storage-file")]
                {
                    let directory = self.config.data_dir.join("sessions").join(id);
                    // Owner records without a journal directory are valid and
                    // don't authorize constructing one on a history read.
                    if !tokio::fs::try_exists(&directory).await? {
                        return Ok(None);
                    }
                    microcrab_storage::history::FileJournal::inspect_head(directory).await
                }
                #[cfg(not(feature = "storage-file"))]
                Err(Error::config("File storage is not compiled"))
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
                Err(Error::config("SQLite storage is not compiled"))
            }
        }
    }

    /// Both callers retain journal-open admission until the returned Journal is
    /// closed. This helper must not be used by unguarded transport handlers.
    pub(super) async fn inspect_journal(
        &self,
        record: &SessionRecord,
    ) -> Result<(CreatorJournal, Option<microcrab_session::Recover>), Error> {
        match self.config.storage {
            Storage::File => {
                #[cfg(feature = "storage-file")]
                {
                    let directory = self.config.data_dir.join("sessions").join(&record.id);
                    CreatorJournal::open(
                        directory
                            .to_str()
                            .ok_or_else(|| Error::config("storage path is not UTF8"))?,
                        record.id.clone(),
                        true,
                        true,
                    )
                    .await
                }
                #[cfg(not(feature = "storage-file"))]
                {
                    Err(Error::config("File storage is not compiled"))
                }
            }
            Storage::Sqlite => {
                #[cfg(feature = "storage-sqlite")]
                {
                    CreatorJournal::open_sqlite(
                        self.config.data_dir.join("storage.sqlite"),
                        record.id.clone(),
                        true,
                    )
                    .await
                }
                #[cfg(not(feature = "storage-sqlite"))]
                {
                    Err(Error::config("SQLite storage is not compiled"))
                }
            }
        }
    }
    pub(super) async fn inspect_subscription(
        &self,
        record: &SessionRecord,
    ) -> Result<super::Subscription, WireError> {
        let _journal_open = self.journal_open_gate(&record.id).lock_owned().await;
        let slot = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&record.id)
            .cloned();
        let active = match slot {
            Some(slot) => slot.active.lock().await.clone(),
            None => None,
        };
        if let Some(managed) = active.as_ref() {
            // Attach must include admitted foreground content even when its
            // asynchronous snapshot publisher has not consumed the Context watch
            // yet. Refresh uses the existing optimistic publication witness;
            // snapshot capture and event registration remain atomic in the Hub.
            self.refresh(managed).await.map_err(super::safe)?;
            return Ok(managed.hub.subscribe());
        }
        let snapshot = self.inspect_view(record, true).await?;
        let deferred = snapshot["history_available"].is_null();
        let (hub, subscription) = {
            let mut hubs = self
                .read_hubs
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            // The registry never owns conversation bytes. Serialize pruning,
            // reuse and lease creation so a last disconnect cannot retire another
            // subscriber's Hub or admit more concurrent owners than the cap.
            let mut owners = Vec::new();
            hubs.retain(|_, hub| match hub.upgrade() {
                Some(hub) => {
                    owners.push(hub);
                    true
                }
                None => false,
            });
            let existing = hubs.get(&record.id).and_then(std::sync::Weak::upgrade);
            if existing.is_none() && hubs.len() >= self.config.max_active_sessions {
                return Err(WireError::new("capacity", "Observation capacity reached"));
            }
            let hub = existing.unwrap_or_else(|| Arc::new(Hub::new()));
            hubs.insert(record.id.clone(), Arc::downgrade(&hub));
            hub.replace(snapshot);
            let mut subscription = hub.subscribe();
            subscription._observation_owner = Some(hub.clone());
            (hub, subscription)
        };
        if deferred && let Some(manager) = self.self_ref.upgrade() {
            self.probe_history(manager, record.clone(), hub);
        }
        Ok(subscription)
    }
    /// Undo/redo availability costs one full parent read per direction, so a
    /// dormant attach answers first and publishes it as a `session.meta` patch.
    /// The probe re-enters journal-open admission, so it is ordered against
    /// activation, archive and deletion; a hub that is no longer the session's
    /// read hub (activated or retired meanwhile) is left untouched.
    fn probe_history(&self, manager: Arc<Manager>, record: SessionRecord, hub: Arc<Hub>) {
        let _ = self.track(async move {
            let _journal_open = manager.journal_open_gate(&record.id).lock_owned().await;
            let current = manager
                .read_hubs
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&record.id)
                .and_then(std::sync::Weak::upgrade)
                .is_some_and(|current| Arc::ptr_eq(&current, &hub));
            if !current {
                return;
            }
            let available = manager.dormant(&record, true).await.and_then(|dormant| {
                dormant
                    .history_available
                    .get()
                    .copied()
                    .ok_or_else(|| Error::protocol("history availability is unresolved"))
            });
            match available {
                Ok(available) => {
                    let mut fields = serde_json::Map::new();
                    fields.insert("history_available".into(), json!(available));
                    hub.patch(fields);
                }
                Err(error) => tracing::debug!(reason=%error.diagnostic_reason(), "deferred history availability failed"),
            }
        });
    }
}

/// A dormant Session has no live resource producer. Reuse execution recovery's
/// delivery semantics to distinguish retained buffered output from Final history;
/// this creates no Session, starts no effects and never calls reference/Host code.
fn dormant_current_state(recover: Option<&microcrab_session::Recover>) -> Result<Value, Error> {
    let Some(recover) = recover else {
        return Ok(json!({}));
    };
    let mut values = recover.state.variables.as_value();
    for key in [
        "subagents",
        "delegatable_agents",
        "permission",
        "jobs",
        "tool_tasks",
        "executions",
        "mode",
        "mode_prompt",
        "session",
        "time",
    ] {
        values
            .as_object_mut()
            .expect("Variables is an object")
            .remove(key);
    }
    let registry =
        microcrab_session::runtime::execution::ExecutionRegistry::new(recover.state.limits.clone());
    registry.restore(&recover.state.executions)?;
    let executions = registry.model_projection();
    if !executions.is_empty() {
        values["executions"] = json!(executions);
    }
    if let Some(mode) = &recover.state.selected_mode {
        values["mode"] = json!(mode);
    }
    values["session"] = json!({"epoch":recover.state.epoch_id,"turn":recover.state.turn_id,"round":recover.state.round_id});
    if let Some(kind) = &recover.state.session_kind {
        values["session"]["kind"] = json!(kind);
    }
    Ok(values)
}
