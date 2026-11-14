//! One bridge per parent session. The short state lock serializes durable
//! transitions and event publication; client-result waiting never holds it.
//! The bounded synchronous observer must not reenter the bridge. Child adapters
//! share this bridge while preserving the invoking ToolContext session identity.
use super::{model::*, store::Store};
use async_trait::async_trait;
use microcrab_permission::Permission;
use microcrab_protocol::ToolDefinition;
use microcrab_session::{
    Error,
    runtime::{
        ToolRegistration,
        execution::{
            ExecutionController, OutputOptions, OutputWriter, ToolFailure, ToolFailureKind,
            ToolOutput,
        },
        tool::{Tool, ToolContext},
    },
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::sync::{Mutex as AsyncMutex, oneshot};

#[cfg(test)]
#[path = "settlement_tests.rs"]
mod settlement_tests;

struct Lease {
    socket: String,
    client: String,
    id: String,
}
struct DeferredCompletion {
    record: CallRecord,
    finished: bool,
}
struct State {
    durable: Durable,
    lease: Option<Lease>,
    waiters: HashMap<String, oneshot::Sender<CallRecord>>,
    anchors: HashMap<String, Option<String>>,
    contexts: HashMap<String, ToolContext>,
    buffered: HashMap<String, oneshot::Sender<ToolOutput>>,
    writers: HashMap<String, OutputWriter>,
    failures: HashMap<String, ToolFailure>,
    implicit_buffers: HashSet<String>,
    deferred_completions: HashMap<String, DeferredCompletion>,
}
pub(crate) struct ClientTools {
    parent_session_id: String,
    store: Store,
    state: AsyncMutex<State>,
    #[expect(
        clippy::type_complexity,
        reason = "The bridge shares one optional synchronous event callback behind a short registration lock; preserve the erased Send + Sync observer port"
    )]
    observer: Mutex<Option<Arc<dyn Fn(ClientToolEvent) + Send + Sync>>>,
    executor_changes: tokio::sync::watch::Sender<Option<(String, String)>>,
}
impl ClientTools {
    pub async fn open(data_dir: &Path, parent_session_id: &str) -> Result<Arc<Self>, Error> {
        let (store, mut durable) = Store::open(data_dir, parent_session_id)?;
        for (index, catalog) in durable.catalogs.iter().enumerate() {
            validate_catalog(&catalog.definitions).map_err(|error| {
                Error::Config(
                    format!("client tool catalogs[{index}] failed validation: {error}").into(),
                )
            })?;
            if catalog.digest != digest(&catalog.definitions)? {
                return Err(Error::Config(
                    format!("client tool catalogs[{index}].digest mismatch").into(),
                ));
            }
        }
        if durable.calls.len() > MAX_CALLS {
            return Err(invalid("too many durable client tool calls"));
        }
        let mut changed = false;
        for call in &mut durable.calls {
            if call.parent_session_id != parent_session_id {
                return Err(invalid("client tool store session mismatch"));
            }
            if call.status.unsettled() {
                finish(
                    call,
                    CallStatus::Interrupted,
                    None,
                    Some("server restarted".into()),
                );
                changed = true;
            }
        }
        if changed {
            store.commit(&durable)?;
        }
        Ok(Arc::new(Self {
            parent_session_id: parent_session_id.into(),
            store,
            state: AsyncMutex::new(State {
                durable,
                lease: None,
                waiters: HashMap::new(),
                anchors: HashMap::new(),
                contexts: HashMap::new(),
                buffered: HashMap::new(),
                writers: HashMap::new(),
                failures: HashMap::new(),
                implicit_buffers: HashSet::new(),
                deferred_completions: HashMap::new(),
            }),
            observer: Mutex::new(None),
            executor_changes: tokio::sync::watch::channel(None).0,
        }))
    }
    /// The callback is synchronous and must not reenter ClientTools. Holding the
    /// lease lock through approval settlement prevents detach/rebind races.
    pub(crate) async fn with_executor<R>(
        &self,
        callback: impl FnOnce(Option<(&str, &str)>) -> R,
    ) -> R {
        let state = self.state.lock().await;
        callback(
            state
                .lease
                .as_ref()
                .map(|lease| (lease.socket.as_str(), lease.id.as_str())),
        )
    }
    pub(crate) fn executor_changes(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<(String, String)>> {
        self.executor_changes.subscribe()
    }
    pub fn set_observer(&self, observer: Arc<dyn Fn(ClientToolEvent) + Send + Sync>) {
        *self
            .observer
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(observer);
    }
    fn emit(&self, phase: ClientToolPhase, call: CallRecord) {
        let observer = self
            .observer
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        if let Some(observer) = observer {
            observer(ClientToolEvent { phase, call });
        }
    }
    /// Stage a validated catalog without revoking active invocation leases.
    /// A separate acknowledgement follows Directory CAS; interrupted unacknowledged
    /// candidates are retained but cannot become executable on reopen.
    pub async fn stage(
        &self,
        mut definitions: Vec<ClientToolDefinition>,
        epoch: u64,
    ) -> Result<(), Error> {
        definitions.sort_by(|a, b| a.definition.name.cmp(&b.definition.name));
        validate_catalog(&definitions)?;
        let catalog = Catalog {
            epoch,
            digest: digest(&definitions)?,
            definitions,
        };
        let mut state = self.state.lock().await;
        let mut next = state.durable.clone();
        for entry in &catalog.definitions {
            if state
                .durable
                .catalogs
                .iter()
                .flat_map(|old| &old.definitions)
                .any(|old| old.definition == entry.definition && old != entry)
            {
                return Err(invalid(
                    "client tool provider metadata changes require changed frozen definition bytes",
                ));
            }
        }
        next.staged = Some(catalog);
        bounded(&next, MAX_STORE_BYTES - MAX_JSON_BYTES - 1024)?;
        self.store.commit(&next)?;
        state.durable = next;
        Ok(())
    }
    pub async fn acknowledge_pending(&self) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        let mut next = state.durable.clone();
        if let Some(staged) = next.staged.take() {
            next.configured = Some(staged);
        }
        self.store.commit(&next)?;
        state.durable = next;
        Ok(())
    }
    pub async fn pending_registrations(self: &Arc<Self>) -> Option<Vec<ToolRegistration>> {
        let state = self.state.lock().await;
        state
            .durable
            .configured
            .as_ref()
            .filter(|catalog| {
                state
                    .durable
                    .catalogs
                    .last()
                    .is_none_or(|active| active.digest != catalog.digest)
            })
            .map(|catalog| {
                catalog
                    .definitions
                    .iter()
                    .map(|entry| {
                        ToolRegistration::External(Arc::new(Adapter {
                            entry: entry.clone(),
                            catalog_digest: catalog.digest.clone(),
                            bridge: self.clone(),
                        }))
                    })
                    .collect()
            })
    }
    /// The core's detached catalog candidate certifies the application boundary.
    pub async fn apply_configured(
        &self,
        context: &microcrab_session::context::ContextSnapshot,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        let Some(pending) = state.durable.configured.as_ref() else {
            return Ok(());
        };
        if state
            .durable
            .catalogs
            .last()
            .is_some_and(|active| active.digest == pending.digest)
        {
            return Ok(());
        }
        let names: HashSet<_> = state
            .durable
            .catalogs
            .iter()
            .flat_map(|c| &c.definitions)
            .chain(&pending.definitions)
            .map(|d| d.definition.name.as_str())
            .collect();
        let external: Vec<_> = context
            .tools
            .iter()
            .filter(|tool| names.contains(tool.name.as_str()))
            .collect();
        if !pending
            .definitions
            .iter()
            .map(|d| &d.definition)
            .eq(external)
        {
            return Ok(());
        }
        let mut next = state.durable.clone();
        let mut catalog = pending.clone();
        catalog.epoch = context
            .epoch
            .checked_add(u64::from(context.pending_epoch))
            .ok_or_else(|| invalid("client tool epoch overflow"))?;
        next.catalogs.push(catalog);
        self.store.commit(&next)?;
        state.durable = next;
        // The same executor remains owner of already issued old-catalog calls.
        // A new bind validates the new advertised digest without changing its lease.
        Ok(())
    }

    /// The caller must already hold the Session epoch/lifecycle admission gate.
    /// Idempotence is allowed only for the identical catalog in the same epoch.
    pub async fn register(
        &self,
        definitions: Vec<ClientToolDefinition>,
        epoch: u64,
    ) -> Result<(), Error> {
        self.register_catalog(definitions, epoch, false).await
    }
    /// Fixture helper for retained same-epoch catalog candidates. Production
    /// configuration admission uses stage plus acknowledgement instead.
    #[cfg(test)]
    pub async fn register_pending(
        &self,
        definitions: Vec<ClientToolDefinition>,
        epoch: u64,
    ) -> Result<(), Error> {
        self.register_catalog(definitions, epoch, true).await
    }
    async fn register_catalog(
        &self,
        mut definitions: Vec<ClientToolDefinition>,
        epoch: u64,
        pending: bool,
    ) -> Result<(), Error> {
        definitions.sort_by(|a, b| a.definition.name.cmp(&b.definition.name));
        validate_catalog(&definitions)?;
        let catalog_digest = digest(&definitions)?;
        let mut state = self.state.lock().await;
        for entry in &definitions {
            if state
                .durable
                .catalogs
                .iter()
                .flat_map(|catalog| &catalog.definitions)
                .any(|old| old.definition == entry.definition && old != entry)
            {
                return Err(invalid(
                    "client tool provider metadata changes require changed frozen definition bytes",
                ));
            }
        }
        if state
            .durable
            .calls
            .iter()
            .any(|call| call.status.unsettled())
        {
            return Err(invalid("client tool calls are still pending"));
        }
        if let Some(current) = state.durable.catalogs.last() {
            if epoch < current.epoch
                || (epoch == current.epoch && current.digest != catalog_digest && !pending)
            {
                return Err(invalid(
                    "client tool catalog changes require a new explicit epoch",
                ));
            }
            if epoch == current.epoch && current.digest == catalog_digest {
                return Ok(());
            }
        }
        let mut durable = state.durable.clone();
        durable.catalogs.push(Catalog {
            epoch,
            digest: catalog_digest,
            definitions,
        });
        bounded(&durable, MAX_STORE_BYTES - MAX_JSON_BYTES - 1024)?;
        self.store.commit(&durable)?;
        state.durable = durable;
        // An old socket must explicitly bind to the newly admitted digest.
        state.lease = None;
        self.executor_changes.send_replace(None);
        Ok(())
    }
    /// Reconcile the file candidate against the Journal's committed frozen tools
    /// after a crash or failed epoch admission. Identical frozen definitions have
    /// immutable provider metadata, so pending-epoch checkpoints are unambiguous.
    /// The caller serializes this operation with lifecycle/epoch admission and
    /// must not pass a synthetic pre-initialization empty Context at creation.
    pub async fn reconcile_catalog(&self, tools: &[ToolDefinition]) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        if state
            .durable
            .calls
            .iter()
            .any(|call| call.status.unsettled())
        {
            return Err(invalid("cannot reconcile a catalog with pending calls"));
        }
        let names: std::collections::BTreeSet<_> = state
            .durable
            .catalogs
            .iter()
            .flat_map(|catalog| {
                catalog
                    .definitions
                    .iter()
                    .map(|entry| entry.definition.name.as_str())
            })
            .collect();
        let mut external: Vec<_> = tools
            .iter()
            .filter(|definition| names.contains(definition.name.as_str()))
            .cloned()
            .collect();
        external.sort_by(|a, b| a.name.cmp(&b.name));
        let selected = state
            .durable
            .catalogs
            .iter()
            .rev()
            .find(|catalog| {
                catalog
                    .definitions
                    .iter()
                    .map(|entry| &entry.definition)
                    .eq(external.iter())
            })
            .cloned();
        let catalog = match selected {
            Some(catalog) => catalog,
            None if external.is_empty() => Catalog {
                epoch: 0,
                digest: digest(&[])?,
                definitions: vec![],
            },
            None => {
                return Err(invalid(
                    "journal client tool catalog has no retained definition match",
                ));
            }
        };
        if state.durable.catalogs.last().is_some_and(|current| {
            current.digest == catalog.digest && current.epoch == catalog.epoch
        }) {
            return Ok(());
        }
        let mut durable = state.durable.clone();
        durable.catalogs.push(catalog);
        self.store.commit(&durable)?;
        state.durable = durable;
        state.lease = None;
        self.executor_changes.send_replace(None);
        Ok(())
    }
    /// Read the exact frozen catalog without moving source selection, leases or calls.
    pub async fn fork_catalog(
        &self,
        tools: &[ToolDefinition],
    ) -> Result<Vec<ClientToolDefinition>, Error> {
        self.catalog_for(tools, true).await
    }
    async fn catalog_for(
        &self,
        tools: &[ToolDefinition],
        require_settled: bool,
    ) -> Result<Vec<ClientToolDefinition>, Error> {
        let state = self.state.lock().await;
        if require_settled
            && state
                .durable
                .calls
                .iter()
                .any(|call| call.status.unsettled())
        {
            return Err(invalid(
                "cannot fork a client tool catalog with pending calls",
            ));
        }
        let names: std::collections::BTreeSet<_> = state
            .durable
            .catalogs
            .iter()
            .flat_map(|catalog| {
                catalog
                    .definitions
                    .iter()
                    .map(|entry| entry.definition.name.as_str())
            })
            .collect();
        let mut external: Vec<_> = tools
            .iter()
            .filter(|definition| names.contains(definition.name.as_str()))
            .collect();
        external.sort_by(|a, b| a.name.cmp(&b.name));
        match state.durable.catalogs.iter().rev().find(|catalog| {
            catalog
                .definitions
                .iter()
                .map(|entry| &entry.definition)
                .eq(external.iter().copied())
        }) {
            Some(catalog) => Ok(catalog.definitions.clone()),
            None if external.is_empty() => Ok(vec![]),
            None => Err(invalid("frozen client tool catalog is unavailable")),
        }
    }
    /// Resolve retained immutable adapter metadata without moving active selection
    /// or rejecting live calls. The replacement owner handles their cleanup ACK.
    pub async fn registrations_for(
        self: &Arc<Self>,
        tools: &[ToolDefinition],
    ) -> Result<Vec<Arc<dyn Tool>>, Error> {
        let entries = self.catalog_for(tools, false).await?;
        let catalog_digest = digest(&entries)?;
        Ok(entries
            .into_iter()
            .map(|entry| {
                Arc::new(Adapter {
                    entry,
                    catalog_digest: catalog_digest.clone(),
                    bridge: self.clone(),
                }) as Arc<dyn Tool>
            })
            .collect())
    }
    pub async fn registrations(self: &Arc<Self>) -> Vec<ToolRegistration> {
        let state = self.state.lock().await;
        state
            .durable
            .catalogs
            .last()
            .map(|catalog| {
                catalog
                    .definitions
                    .iter()
                    .map(|entry| {
                        ToolRegistration::External(Arc::new(Adapter {
                            entry: entry.clone(),
                            catalog_digest: catalog.digest.clone(),
                            bridge: self.clone(),
                        }))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
    pub async fn snapshot(&self) -> BindSnapshot {
        let state = self.state.lock().await;
        snapshot(&state, ClientRole::Observer, None)
    }
    pub async fn calls_page(&self, offset: usize, limit: usize) -> Result<CallPage, Error> {
        let state = self.state.lock().await;
        let total_calls = state.durable.calls.len();
        if offset > total_calls || limit == 0 || limit > 64 {
            return Err(invalid("invalid client tool call page cursor or limit"));
        }
        let mut page = CallPage {
            calls: vec![],
            next_cursor: None,
            total_calls,
        };
        let mut used = 256;
        for call in state.durable.calls.iter().skip(offset).take(limit) {
            let bytes = bounded(call, MAX_PAGE_BYTES)?.len() + 1;
            if used + bytes > MAX_PAGE_BYTES {
                break;
            }
            used += bytes;
            page.calls.push(call.clone());
        }
        let next = offset + page.calls.len();
        if next < total_calls {
            page.next_cursor = Some(next);
        }
        Ok(page)
    }
    /// Supply an immutable Journal identity before admitting calls at this cursor.
    /// Numeric turn/round values alone cannot distinguish history branches.
    pub async fn set_history_anchor(
        &self,
        session_id: &str,
        anchor: Option<String>,
    ) -> Result<(), Error> {
        identifier(session_id)?;
        if anchor.as_ref().is_some_and(|value| value.len() > 256) {
            return Err(invalid("history anchor too long"));
        }
        self.state
            .lock()
            .await
            .anchors
            .insert(session_id.into(), anchor);
        Ok(())
    }
    pub async fn bind(
        &self,
        socket_id: &str,
        request: &BindRequest,
    ) -> Result<BindSnapshot, Error> {
        identifier(socket_id)?;
        identifier(&request.client_id)?;
        let mut state = self.state.lock().await;
        let current = state
            .durable
            .catalogs
            .last()
            .ok_or_else(|| invalid("client tool catalog not registered"))?;
        if request.catalog_digest != current.digest {
            return Err(invalid("client tool catalog digest mismatch"));
        }
        if let Some(lease) = &state.lease {
            if lease.socket == socket_id && lease.client == request.client_id {
                return Ok(snapshot(
                    &state,
                    ClientRole::Executor,
                    Some(lease.id.clone()),
                ));
            }
            return Ok(snapshot(&state, ClientRole::Observer, None));
        }
        let lease_id = uuid::Uuid::new_v4().to_string();
        state.lease = Some(Lease {
            socket: socket_id.into(),
            client: request.client_id.clone(),
            id: lease_id.clone(),
        });
        self.executor_changes
            .send_replace(Some((socket_id.to_owned(), lease_id.clone())));
        Ok(snapshot(&state, ClientRole::Executor, Some(lease_id)))
    }
    pub async fn result(
        &self,
        socket_id: &str,
        request: &ResultRequest,
    ) -> Result<CallRecord, Error> {
        bounded(&request.result, MAX_JSON_BYTES)?;
        if request
            .error
            .as_ref()
            .is_some_and(|error| error.len() > 16 * 1024)
            || request.result.is_some() == request.error.is_some()
        {
            return Err(invalid(
                "client tool result requires exactly one result or error",
            ));
        }
        let mut state = self.state.lock().await;
        let lease = state
            .lease
            .as_ref()
            .ok_or_else(|| invalid("client tool executor is detached"))?;
        if lease.socket != socket_id || lease.id != request.lease_id {
            return Err(invalid("stale client tool executor lease"));
        }
        let index = state
            .durable
            .calls
            .iter()
            .position(|call| call.invocation_id == request.invocation_id)
            .ok_or_else(|| invalid("unknown client tool invocation"))?;
        let current = &state.durable.calls[index];
        if state.writers.contains_key(&request.invocation_id)
            && !state.implicit_buffers.contains(&request.invocation_id)
        {
            return Err(invalid(
                "buffered producer must finish through typed output protocol",
            ));
        }
        if !current.status.unsettled() {
            if matches!(current.status, CallStatus::Completed | CallStatus::Failed)
                && current.result == request.result
                && current.error == request.error
            {
                return Ok(current.clone());
            }
            return Err(invalid("client tool invocation already settled"));
        }
        let status = if request.error.is_some() {
            CallStatus::Failed
        } else {
            CallStatus::Completed
        };
        self.commit_finish(
            &mut state,
            index,
            status,
            request.result.clone(),
            request.error.clone(),
            false,
        )
    }
    /// The client computes and validates its own complete fallback. The server
    /// stores opaque JSON only; preparation cannot change mode or wait budget.
    pub async fn prepare(
        &self,
        socket_id: &str,
        request: &PrepareRequest,
    ) -> Result<CallRecord, Error> {
        bounded(&request.timeout_result, MAX_JSON_BYTES)?;
        let mut state = self.state.lock().await;
        let lease = state
            .lease
            .as_ref()
            .ok_or_else(|| invalid("client tool executor is detached"))?;
        if lease.socket != socket_id || lease.id != request.lease_id {
            return Err(invalid("stale client tool executor lease"));
        }
        let index = state
            .durable
            .calls
            .iter()
            .position(|call| call.invocation_id == request.invocation_id)
            .ok_or_else(|| invalid("unknown client tool invocation"))?;
        let call = &state.durable.calls[index];
        if call.blocking_mode.is_none() {
            return Err(invalid("client tool is not an interaction"));
        }
        if call.prepared {
            if call.timeout_result != request.timeout_result {
                return Err(invalid(
                    "client interaction already prepared with different fallback",
                ));
            }
            if call.status.unsettled()
                && call.blocking_mode == Some(InteractionMode::Blocking)
                && call.deadline_ms <= now_ms()
            {
                self.expire_locked(&mut state, index)?;
            }
            return Ok(state.durable.calls[index].clone());
        }
        if !call.status.unsettled() {
            return Err(invalid("client tool invocation already settled"));
        }
        let mut durable = state.durable.clone();
        durable.calls[index].prepared = true;
        durable.calls[index].timeout_result = request.timeout_result.clone();
        self.store.commit(&durable)?;
        state.durable = durable;
        self.emit(
            ClientToolPhase::Prepared,
            state.durable.calls[index].clone(),
        );
        if state.durable.calls[index].blocking_mode == Some(InteractionMode::Blocking)
            && state.durable.calls[index].deadline_ms <= now_ms()
        {
            self.expire_locked(&mut state, index)?;
        }
        Ok(state.durable.calls[index].clone())
    }
    /// Link canonical call identities or exact completed output before truncation.
    /// Runtime execution identity was supplied at admission, never parsed from text.
    #[expect(
        clippy::too_many_arguments,
        reason = "Matching needs each canonical session/call/tool and epoch/turn/round identity separately before comparing untruncated content"
    )]
    pub async fn match_canonical_output(
        &self,
        session_id: &str,
        call_id: &str,
        toolname: &str,
        epoch: Option<u64>,
        turn: Option<u64>,
        round: Option<u64>,
        content: &str,
    ) -> Option<String> {
        let state = self.state.lock().await;
        state
            .durable
            .calls
            .iter()
            .rev()
            .find(|call| {
                call.session_id == session_id
                    && call.call_id == call_id
                    && call.tool == toolname
                    && epoch.is_none_or(|epoch| call.epoch == epoch)
                    && call.turn_id == turn
                    && call.round_id == round
                    && (call.execution_id.is_some()
                        || (call.status == CallStatus::Completed
                            && call.result.as_ref().is_some_and(|value| match value {
                                Value::String(text) => text == content,
                                value => {
                                    serde_json::to_string(value).is_ok_and(|text| text == content)
                                }
                            })))
            })
            .map(|call| call.invocation_id.clone())
    }
    /// The execution registry owns completion and its single terminal notice.
    /// Keep the writer until both that transition and durable settlement succeed.
    /// Progress is bounded by existing implicit writers, never a second inbox.
    fn finish_deferred(
        &self,
        state: &mut State,
        proposed: &CallRecord,
    ) -> Result<CallRecord, Error> {
        let id = &proposed.invocation_id;
        let lifecycle = matches!(
            proposed.status,
            CallStatus::Cancelled | CallStatus::Interrupted
        );
        if let Some(progress) = state.deferred_completions.get(id) {
            if progress.finished {
                // Completion won before a storage failure. A retry or close must
                // reconcile that outcome, never overwrite an already-issued Wake.
                if !lifecycle && !same_settlement(&progress.record, proposed) {
                    return Err(invalid(
                        "deferred answer already accepted with different outcome",
                    ));
                }
                return Ok(progress.record.clone());
            }
            if !lifecycle && !same_settlement(&progress.record, proposed) {
                return Err(invalid("deferred answer output already appended"));
            }
        }
        let writer = state
            .writers
            .get(id)
            .ok_or_else(|| invalid("deferred interaction lost its output writer"))?;
        let native_lifecycle = proposed.status == CallStatus::Interrupted
            || (proposed.status == CallStatus::Cancelled && proposed.failure.is_none());
        let outcome = if proposed.status == CallStatus::Completed || native_lifecycle {
            Ok(())
        } else {
            let failure = state
                .failures
                .get(id)
                .cloned()
                .unwrap_or_else(|| ToolFailure {
                    kind: if proposed.status == CallStatus::Cancelled {
                        ToolFailureKind::Cancelled
                    } else {
                        ToolFailureKind::Failed
                    },
                    code: "external_failed".into(),
                    message: proposed
                        .error
                        .clone()
                        .unwrap_or_else(|| "External producer failed".into()),
                    retryable: false,
                });
            // Validate before appending: rejected outcomes must not leak answer
            // bytes into an execution that remains pending and can be retried.
            validate_failure(&failure)?;
            Err(failure)
        };
        if !state.deferred_completions.contains_key(id)
            && let Some(result) = &proposed.result
        {
            let text = match result {
                Value::String(text) => text.clone(),
                value => value.to_string(),
            };
            writer.append(None, &text)?;
            state.deferred_completions.insert(
                id.clone(),
                DeferredCompletion {
                    record: proposed.clone(),
                    finished: false,
                },
            );
        }
        if proposed.status == CallStatus::Cancelled && proposed.failure.is_none() {
            writer.confirm_cancelled()?;
        } else if proposed.status == CallStatus::Interrupted {
            writer.interrupt("External producer disconnected; effects may be unknown")?;
        } else {
            writer.finish(outcome)?;
        }
        state.deferred_completions.insert(
            id.clone(),
            DeferredCompletion {
                record: proposed.clone(),
                finished: true,
            },
        );
        Ok(proposed.clone())
    }
    fn complete_producer(&self, state: &mut State, call: &CallRecord) {
        state.contexts.remove(&call.invocation_id);
        if state
            .deferred_completions
            .get(&call.invocation_id)
            .is_some_and(|progress| progress.finished)
        {
            state.deferred_completions.remove(&call.invocation_id);
            state.implicit_buffers.remove(&call.invocation_id);
            state.writers.remove(&call.invocation_id);
            state.failures.remove(&call.invocation_id);
            return;
        }
        // Keep the initial handoff sender until the invocation waiter receives its
        // settled record; dropping it first would race the Final response.
        if let Some(writer) = state.writers.remove(&call.invocation_id) {
            if state.implicit_buffers.remove(&call.invocation_id)
                && let Some(result) = &call.result
            {
                let text = match result {
                    Value::String(text) => text.clone(),
                    value => value.to_string(),
                };
                if let Err(error) = writer.append(None, &text) {
                    tracing::warn!(error_code = %super::super::safe(error).code, "External interaction output rejected");
                }
            }
            if call.status == CallStatus::Cancelled && call.failure.is_none() {
                if let Err(error) = writer.confirm_cancelled() {
                    tracing::warn!(error_code = %super::super::safe(error).code, "External waiting cancellation rejected");
                }
                return;
            }
            if call.status == CallStatus::Interrupted {
                if let Err(error) =
                    writer.interrupt("External producer disconnected; effects may be unknown")
                {
                    tracing::warn!(error_code = %super::super::safe(error).code, "External interruption rejected");
                }
                return;
            }
            let outcome = if call.status == CallStatus::Completed {
                Ok(())
            } else {
                Err(state
                    .failures
                    .remove(&call.invocation_id)
                    .unwrap_or_else(|| ToolFailure {
                        kind: if call.status == CallStatus::Cancelled {
                            ToolFailureKind::Cancelled
                        } else {
                            ToolFailureKind::Failed
                        },
                        code: if call.status == CallStatus::Interrupted {
                            "external_interrupted"
                        } else {
                            "external_failed"
                        }
                        .into(),
                        message: call
                            .error
                            .clone()
                            .unwrap_or_else(|| "External producer failed".into()),
                        retryable: false,
                    }))
            };
            if let Err(error) = writer.finish(outcome) {
                tracing::warn!(error_code = %super::super::safe(error).code, "External producer completion rejected");
            }
        }
    }
    /// Exact socket lease and durable invocation identity scope every producer event.
    pub async fn output(
        self: &Arc<Self>,
        socket_id: &str,
        request: &OutputRequest,
    ) -> Result<CallRecord, Error> {
        let mut state = self.state.lock().await;
        if !state
            .lease
            .as_ref()
            .is_some_and(|lease| lease.socket == socket_id && lease.id == request.lease_id)
        {
            return Err(invalid("stale client tool executor lease"));
        }
        let index = state
            .durable
            .calls
            .iter()
            .position(|call| call.invocation_id == request.invocation_id)
            .ok_or_else(|| invalid("unknown client tool invocation"))?;
        if !state.durable.calls[index].status.unsettled() {
            return Err(invalid("client tool invocation already settled"));
        }
        match &request.event {
            ProducerEvent::Buffered {
                channels,
                initial_output,
            } => {
                if state.writers.contains_key(&request.invocation_id) {
                    return Err(invalid("producer already opened"));
                }
                let context = state
                    .contexts
                    .get(&request.invocation_id)
                    .ok_or_else(|| invalid("invocation no longer owns a producer factory"))?;
                let (writer, handle) = context.open_output(
                    OutputOptions {
                        channels: channels.clone(),
                    },
                    Arc::new(ClientController {
                        bridge: self.clone(),
                        invocation_id: request.invocation_id.clone(),
                    }),
                )?;
                let sender = state
                    .buffered
                    .remove(&request.invocation_id)
                    .ok_or_else(|| invalid("producer handoff unavailable"))?;
                state.writers.insert(request.invocation_id.clone(), writer);
                state.contexts.remove(&request.invocation_id);
                // Keep the foreground waiter alive until the buffered handoff is
                // consumed. Closing it here races its receiver against the ready
                // producer handle and can discard an otherwise valid buffer.
                sender
                    .send(ToolOutput::Buffered(
                        handle.with_initial_output(*initial_output),
                    ))
                    .map_err(|_| invalid("invocation no longer accepts producer"))?;
            }
            ProducerEvent::Chunk { channel, content } => {
                if content.len() > MAX_JSON_BYTES {
                    return Err(invalid("producer chunk exceeds limit"));
                }
                state
                    .writers
                    .get(&request.invocation_id)
                    .ok_or_else(|| invalid("producer not opened"))?
                    .append(channel.as_deref(), content)?;
            }
            ProducerEvent::Final { content, failure } => {
                if state.writers.contains_key(&request.invocation_id) {
                    return Err(invalid("buffered producer must finish explicitly"));
                }
                if content.len() > MAX_JSON_BYTES {
                    return Err(invalid("final output exceeds limit"));
                }
                if let Some(failure) = failure {
                    validate_failure(failure)?;
                    state
                        .failures
                        .insert(request.invocation_id.clone(), failure.clone());
                }
                let status = match failure.as_ref().map(|failure| failure.kind) {
                    None => CallStatus::Completed,
                    Some(ToolFailureKind::Cancelled) => CallStatus::Cancelled,
                    Some(ToolFailureKind::Timeout) => CallStatus::Deadline,
                    Some(ToolFailureKind::Failed) => CallStatus::Failed,
                };
                let error = failure.as_ref().map(|failure| failure.message.clone());
                return self.commit_finish(
                    &mut state,
                    index,
                    status,
                    Some(Value::String(content.clone())),
                    error,
                    false,
                );
            }
            ProducerEvent::Finish { failure } => {
                if !state.writers.contains_key(&request.invocation_id) {
                    return Err(invalid("producer not opened"));
                }
                if let Some(failure) = failure {
                    validate_failure(failure)?;
                    state
                        .failures
                        .insert(request.invocation_id.clone(), failure.clone());
                }
                let status = match failure.as_ref().map(|failure| failure.kind) {
                    None => CallStatus::Completed,
                    Some(ToolFailureKind::Cancelled) => CallStatus::Cancelled,
                    Some(ToolFailureKind::Timeout) => CallStatus::Deadline,
                    Some(ToolFailureKind::Failed) => CallStatus::Failed,
                };
                return self.commit_finish(
                    &mut state,
                    index,
                    status,
                    None,
                    failure.as_ref().map(|failure| failure.message.clone()),
                    false,
                );
            }
        }
        Ok(state.durable.calls[index].clone())
    }
    async fn interaction_expired(&self, invocation_id: &str) -> Result<bool, Error> {
        let mut state = self.state.lock().await;
        let Some(index) = state
            .durable
            .calls
            .iter()
            .position(|call| call.invocation_id == invocation_id && call.status.unsettled())
        else {
            return Ok(false);
        };
        self.expire_locked(&mut state, index)
    }
    fn expire_locked(&self, state: &mut State, index: usize) -> Result<bool, Error> {
        let call = &state.durable.calls[index];
        if call.blocking_mode != Some(InteractionMode::Blocking) {
            return Ok(false);
        }
        if let Some(fallback) = call.timeout_result.clone() {
            self.commit_finish(
                state,
                index,
                CallStatus::Completed,
                Some(fallback),
                None,
                true,
            )?;
            return Ok(false);
        }
        if call.status != CallStatus::Paused {
            let mut durable = state.durable.clone();
            durable.calls[index].status = CallStatus::Paused;
            self.store.commit(&durable)?;
            state.durable = durable;
            self.emit(ClientToolPhase::Paused, state.durable.calls[index].clone());
        }
        Ok(true)
    }
    /// Explicit detach validates the exact generation; a stale same-socket
    /// message must never revoke a more recently acquired executor lease.
    pub async fn unbind(&self, socket_id: &str, lease_id: &str) -> Result<bool, Error> {
        self.release(socket_id, Some(lease_id)).await
    }
    /// Socket teardown owns its socket identity and therefore needs no wire
    /// lease token. Other sockets and observer teardown cannot revoke a lease.
    pub async fn detach(&self, socket_id: &str) -> Result<(), Error> {
        self.release(socket_id, None).await?;
        Ok(())
    }
    async fn release(&self, socket_id: &str, lease_id: Option<&str>) -> Result<bool, Error> {
        let mut state = self.state.lock().await;
        if !state.lease.as_ref().is_some_and(|lease| {
            lease.socket == socket_id && lease_id.is_none_or(|id| id == lease.id)
        }) {
            return Ok(false);
        }
        let indices: Vec<_> = state
            .durable
            .calls
            .iter()
            .enumerate()
            .filter(|(_, call)| {
                call.status.unsettled()
                    && (!call.reclaimable
                        || (state.writers.contains_key(&call.invocation_id)
                            && !state.implicit_buffers.contains(&call.invocation_id)))
            })
            .map(|(index, _)| index)
            .collect();
        self.finish_many(
            &mut state,
            &indices,
            CallStatus::Interrupted,
            "client tool executor disconnected",
        )?;
        state.lease = None;
        self.executor_changes.send_replace(None);
        Ok(true)
    }
    pub async fn settle_all(&self, reason: &str) -> Result<(), Error> {
        if reason.len() > 16 * 1024 {
            return Err(invalid("client tool settlement reason too long"));
        }
        let mut state = self.state.lock().await;
        let indices: Vec<_> = state
            .durable
            .calls
            .iter()
            .enumerate()
            .filter(|(_, call)| call.status.unsettled())
            .map(|(index, _)| index)
            .collect();
        self.finish_many(&mut state, &indices, CallStatus::Interrupted, reason)?;
        Ok(())
    }
    fn finish_many(
        &self,
        state: &mut State,
        indices: &[usize],
        status: CallStatus,
        reason: &str,
    ) -> Result<Vec<CallRecord>, Error> {
        let mut durable = state.durable.clone();
        for &index in indices {
            finish(&mut durable.calls[index], status, None, Some(reason.into()));
            if state
                .implicit_buffers
                .contains(&durable.calls[index].invocation_id)
            {
                durable.calls[index] = self.finish_deferred(state, &durable.calls[index])?;
            }
        }
        self.store.commit(&durable)?;
        state.durable = durable;
        let records: Vec<_> = indices
            .iter()
            .map(|&index| state.durable.calls[index].clone())
            .collect();
        for call in &records {
            self.complete_producer(state, call);
            self.emit(ClientToolPhase::Settled, call.clone());
            if let Some(sender) = state.waiters.remove(&call.invocation_id) {
                let _ = sender.send(call.clone());
            }
        }
        Ok(records)
    }
    fn commit_finish(
        &self,
        state: &mut State,
        index: usize,
        status: CallStatus,
        result: Option<Value>,
        error: Option<String>,
        timeout_applied: bool,
    ) -> Result<CallRecord, Error> {
        let mut durable = state.durable.clone();
        finish(&mut durable.calls[index], status, result, error);
        durable.calls[index].timeout_applied = timeout_applied;
        durable.calls[index].failure = state
            .failures
            .get(&durable.calls[index].invocation_id)
            .cloned();
        if state
            .implicit_buffers
            .contains(&durable.calls[index].invocation_id)
        {
            durable.calls[index] = self.finish_deferred(state, &durable.calls[index])?;
        }
        self.store.commit(&durable)?;
        state.durable = durable;
        let record = state.durable.calls[index].clone();
        self.complete_producer(state, &record);
        self.emit(ClientToolPhase::Settled, record.clone());
        if let Some(sender) = state.waiters.remove(&record.invocation_id) {
            let _ = sender.send(record.clone());
        }
        Ok(record)
    }
    async fn settle_one(
        &self,
        invocation_id: &str,
        status: CallStatus,
        reason: &str,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        let Some(index) = state
            .durable
            .calls
            .iter()
            .position(|call| call.invocation_id == invocation_id && call.status.unsettled())
        else {
            return Ok(());
        };
        self.commit_finish(&mut state, index, status, None, Some(reason.into()), false)?;
        Ok(())
    }
    async fn execute(
        self: &Arc<Self>,
        entry: &ClientToolDefinition,
        catalog_digest: &str,
        arguments: Value,
        context: ToolContext,
    ) -> Result<ToolOutput, Error> {
        bounded(&arguments, MAX_JSON_BYTES)?;
        identifier(&context.session_id)?;
        if context.call_id.is_empty() || context.call_id.len() > 1024 {
            return Err(invalid("invalid client tool call identity"));
        }
        if context.cancellation.is_cancelled() {
            return Err(invalid("client tool invocation cancelled"));
        }
        let created_at_ms = now_ms();
        let blocking_mode = entry.interaction_mode(&arguments)?;
        let timeout_ms = entry
            .interaction
            .as_ref()
            .map(|interaction| {
                interaction
                    .wait_timeout_ms
                    .min(context.interaction_timeout_ms)
            })
            .unwrap_or(0);
        let deadline = if blocking_mode == Some(InteractionMode::Blocking) {
            Some(
                tokio::time::Instant::now()
                    .checked_add(std::time::Duration::from_millis(timeout_ms))
                    .ok_or_else(|| invalid("client interaction deadline is out of range"))?,
            )
        } else {
            None
        };
        let invocation_id = uuid::Uuid::new_v4().to_string();
        let mut state = self.state.lock().await;
        if state.lease.is_none() {
            return Err(invalid("client tool executor unavailable"));
        }
        if state
            .durable
            .catalogs
            .iter()
            .all(|catalog| catalog.digest != catalog_digest)
        {
            return Err(invalid("client tool adapter belongs to an old catalog"));
        }
        if state.durable.calls.len() >= MAX_CALLS {
            return Err(invalid("client tool call retention limit reached"));
        }
        let epoch = context.state.snapshot().epoch_id;
        // A duplicate runtime invocation is an error, not permission to repeat
        // a possibly effectful browser operation. Epoch and history distinguish
        // legitimate identical model call IDs on another branch.
        let anchor = state.anchors.get(&context.session_id).cloned().flatten();
        if state.durable.calls.iter().any(|call| {
            call.session_id == context.session_id
                && call.call_id == context.call_id
                && call.epoch == epoch
                && call.turn_id == context.turn_id
                && call.round_id == context.round_id
                && call.history_anchor == anchor
        }) {
            return Err(invalid("duplicate client tool invocation"));
        }
        let record = CallRecord {
            invocation_id: invocation_id.clone(),
            parent_session_id: self.parent_session_id.clone(),
            session_id: context.session_id.clone(),
            call_id: context.call_id.clone(),
            execution_id: context.execution_id.map(|id| id.0),
            tool: entry.definition.name.clone(),
            arguments,
            epoch,
            turn_id: context.turn_id,
            round_id: context.round_id,
            history_anchor: anchor,
            catalog_digest: catalog_digest.into(),
            created_at_ms,
            deadline_ms: if deadline.is_some() {
                created_at_ms.saturating_add(timeout_ms)
            } else {
                0
            },
            settled_at_ms: None,
            status: CallStatus::Pending,
            result: None,
            error: None,
            failure: None,
            reclaimable: entry.reclaimable,
            blocking_mode,
            prepared: false,
            timeout_result: None,
            timeout_applied: false,
        };
        let mut durable = state.durable.clone();
        durable.calls.push(record.clone());
        let pending_count = durable
            .calls
            .iter()
            .filter(|call| call.status.unsettled())
            .count();
        let reserve = pending_count
            .saturating_mul((16 * 1024 * 6).max(MAX_JSON_BYTES) + MAX_JSON_BYTES + 1024);
        bounded(
            &durable,
            MAX_STORE_BYTES.saturating_sub(reserve + MAX_JSON_BYTES + 1024),
        )?;
        self.store.commit(&durable)?;
        state.durable = durable;
        let (sender, receiver) = oneshot::channel();
        state.waiters.insert(invocation_id.clone(), sender);
        let (buffered_sender, mut buffered_receiver) = oneshot::channel();
        state
            .buffered
            .insert(invocation_id.clone(), buffered_sender);
        state
            .contexts
            .insert(invocation_id.clone(), context.clone());
        let mut guard = PendingGuard {
            bridge: self.clone(),
            invocation_id: invocation_id.clone(),
            armed: true,
        };
        // Deferred interaction is an adapter-owned producer. The Runtime never
        // chooses its observation window or interprets its business timeout.
        if blocking_mode == Some(InteractionMode::Deferred) && context.execution_id.is_some() {
            let (writer, output) = context.open_output(
                OutputOptions::default(),
                Arc::new(ClientController {
                    bridge: self.clone(),
                    invocation_id: invocation_id.clone(),
                }),
            )?;
            state.writers.insert(invocation_id.clone(), writer);
            state.implicit_buffers.insert(invocation_id.clone());
            state.contexts.remove(&invocation_id);
            state.waiters.remove(&invocation_id);
            state.buffered.remove(&invocation_id);
            guard.armed = false;
            self.emit(ClientToolPhase::Requested, record);
            return Ok(ToolOutput::Buffered(output));
        }
        self.emit(ClientToolPhase::Requested, record);
        drop(state);
        let mut receiver = receiver;
        let mut wait_deadline = deadline;
        let outcome = loop {
            let budget = async {
                match wait_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                // A producer can finish in the same scheduling slice as its
                // initial handoff. Adopt its retained bytes before final wakeups.
                biased;
                response = &mut buffered_receiver => {
                    let output = response.map_err(|_| invalid("client producer handoff closed"))?;
                    self.state.lock().await.waiters.remove(&invocation_id);
                    guard.armed = false;
                    return Ok(output);
                }
                response = &mut receiver => break response.map_err(|_| invalid("client tool waiter closed")),
                _ = context.cancellation.cancelled() => {
                    {
                        let state = self.state.lock().await;
                        if let Some(call) = state.durable.calls.iter().find(|call| call.invocation_id == invocation_id) {
                            self.emit(ClientToolPhase::CancelRequested, call.clone());
                        }
                    }
                    // A socket request cannot prove that an in-flight remote
                    // effect stopped. End ownership honestly without replay.
                    self.settle_one(&invocation_id, CallStatus::Interrupted, "Cancellation requested; external effects may be unknown").await?;
                    break receiver.await.map_err(|_| invalid("client tool waiter closed"));
                }
                _ = budget => {
                    wait_deadline = None;
                    if self.interaction_expired(&invocation_id).await?
                        && let Some(progress) = context.execution_progress.as_ref()
                    {
                        progress.mark_paused_waiting();
                    }
                }
            }
        };
        guard.armed = false;
        let settled = outcome?;
        self.state.lock().await.buffered.remove(&invocation_id);
        match settled.status {
            CallStatus::Interrupted => Err(Error::ExecutionInterrupted),
            CallStatus::Completed => match settled.result {
                Some(Value::String(text)) => Ok(ToolOutput::text(text)),
                Some(value) => serde_json::to_string(&value)
                    .map(ToolOutput::text)
                    .map_err(|_| invalid("invalid client tool result")),
                None => Err(invalid("client tool result missing")),
            },
            _ => {
                let failure = settled
                    .failure
                    .clone()
                    .or(self.state.lock().await.failures.remove(&invocation_id))
                    .unwrap_or_else(|| ToolFailure {
                        kind: if settled.status == CallStatus::Cancelled {
                            ToolFailureKind::Cancelled
                        } else {
                            ToolFailureKind::Failed
                        },
                        code: if settled.status == CallStatus::Interrupted {
                            "external_interrupted"
                        } else {
                            "external_failed"
                        }
                        .into(),
                        message: settled
                            .error
                            .unwrap_or_else(|| "Client tool invocation interrupted".into()),
                        retryable: false,
                    });
                Ok(ToolOutput::failed(
                    settled
                        .result
                        .and_then(|value| value.as_str().map(str::to_owned))
                        .unwrap_or_default(),
                    failure,
                ))
            }
        }
    }
}

struct Adapter {
    entry: ClientToolDefinition,
    catalog_digest: String,
    bridge: Arc<ClientTools>,
}
#[async_trait]
impl Tool for Adapter {
    fn definition(&self) -> ToolDefinition {
        self.entry.definition.clone()
    }
    fn permission_defaults(&self) -> Vec<Permission> {
        self.entry.permissions.clone()
    }
    async fn execute(&self, arguments: Value, context: ToolContext) -> Result<ToolOutput, Error> {
        self.bridge
            .execute(&self.entry, &self.catalog_digest, arguments, context)
            .await
    }
}
/// Dropping an invocation before explicit producer handoff settles its durable
/// waiter. Returning Buffered transfers that lifetime to its controller instead.
struct PendingGuard {
    bridge: Arc<ClientTools>,
    invocation_id: String,
    armed: bool,
}
impl Drop for PendingGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let bridge = self.bridge.clone();
        let invocation_id = self.invocation_id.clone();
        let status = CallStatus::Interrupted;
        let reason = "External invocation future dropped; effects may be unknown";
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Err(error) = bridge.settle_one(&invocation_id, status, reason).await {
                    tracing::error!(error_code = %super::super::safe(error).code, "Could not persist client tool cancellation");
                }
            });
        }
    }
}
fn snapshot(state: &State, role: ClientRole, lease_id: Option<String>) -> BindSnapshot {
    let catalog = state.durable.catalogs.last();
    let mut snapshot = BindSnapshot {
        role,
        lease_id,
        catalog_digest: catalog
            .map(|catalog| catalog.digest.clone())
            .unwrap_or_default(),
        definitions: catalog
            .map(|catalog| catalog.definitions.clone())
            .unwrap_or_default(),
        pending: vec![],
        calls: vec![],
        calls_truncated: false,
        pending_truncated: false,
        next_cursor: None,
        total_calls: state.durable.calls.len(),
    };
    // Reserve room for changing false->true flags and the page cursor. Catalogs
    // are validated at admission; serialization failure conservatively omits calls.
    let mut used = serde_json::to_vec(&snapshot)
        .map(|bytes| bytes.len() + 64)
        .unwrap_or(MAX_SNAPSHOT_BYTES);
    for call in state
        .durable
        .calls
        .iter()
        .filter(|call| call.status.unsettled())
    {
        let size = serde_json::to_vec(call)
            .map(|bytes| bytes.len() + 1)
            .unwrap_or(MAX_SNAPSHOT_BYTES);
        if used + size <= MAX_SNAPSHOT_BYTES {
            used += size;
            snapshot.pending.push(call.clone());
        } else {
            snapshot.pending_truncated = true;
        }
    }
    // Prefer recent history for the common UI; full reconstruction uses pages.
    for call in state.durable.calls.iter().rev() {
        let size = serde_json::to_vec(call)
            .map(|bytes| bytes.len() + 1)
            .unwrap_or(MAX_SNAPSHOT_BYTES);
        if used + size <= MAX_SNAPSHOT_BYTES {
            used += size;
            snapshot.calls.push(call.clone());
        } else {
            snapshot.calls_truncated = true;
        }
    }
    snapshot.calls.reverse();
    if snapshot.calls_truncated || snapshot.pending_truncated {
        snapshot.next_cursor = Some(0);
    }
    snapshot
}
fn finish(call: &mut CallRecord, status: CallStatus, result: Option<Value>, error: Option<String>) {
    call.status = status;
    call.result = result;
    call.error = error;
    call.settled_at_ms = Some(now_ms());
}
fn digest(definitions: &[ClientToolDefinition]) -> Result<String, Error> {
    Ok(format!(
        "{:x}",
        Sha256::digest(bounded(&definitions, MAX_JSON_BYTES)?)
    ))
}

fn same_settlement(left: &CallRecord, right: &CallRecord) -> bool {
    left.status == right.status
        && left.result == right.result
        && left.error == right.error
        && left.failure == right.failure
        && left.timeout_applied == right.timeout_applied
}

fn validate_failure(failure: &ToolFailure) -> Result<(), Error> {
    if failure.code.is_empty() || failure.code.len() > 128 || failure.message.len() > 4096 {
        return Err(invalid("invalid external failure bounds"));
    }
    Ok(())
}
struct ClientController {
    bridge: Arc<ClientTools>,
    invocation_id: String,
}
#[async_trait]
impl ExecutionController for ClientController {
    async fn cancel(&self, _: tokio_util::sync::CancellationToken) -> Result<(), Error> {
        let mut state = self.bridge.state.lock().await;
        if state.implicit_buffers.contains(&self.invocation_id) {
            if let Some(index) = state.durable.calls.iter().position(|call| {
                call.invocation_id == self.invocation_id && call.status.unsettled()
            }) {
                self.bridge.commit_finish(
                    &mut state,
                    index,
                    CallStatus::Cancelled,
                    None,
                    Some("Waiting interaction cancelled".into()),
                    false,
                )?;
            }
            return Ok(());
        }
        let call = state
            .durable
            .calls
            .iter()
            .find(|call| call.invocation_id == self.invocation_id)
            .ok_or_else(|| invalid("unknown external producer"))?;
        if call.status.unsettled() {
            self.bridge
                .emit(ClientToolPhase::CancelRequested, call.clone());
        }
        Ok(())
    }
    async fn release(&self, _: tokio_util::sync::CancellationToken) -> Result<(), Error> {
        self.bridge
            .settle_one(
                &self.invocation_id,
                CallStatus::Interrupted,
                "External producer released",
            )
            .await
    }
}
