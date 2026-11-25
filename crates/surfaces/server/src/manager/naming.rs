//! One durable naming claim per session. Optional naming never changes the primary
//! turn receipt, never overwrites a concurrent user edit and never retries on restart.
use super::{Managed, now};
use async_trait::async_trait;
use microcrab_creator::naming::{AutoNamingStore, maybe_rename};
use microcrab_session::{Error, SessionInput};
use microcrab_storage::{Directory, OperationRecord, RECORD_VERSION, SessionRecord};
use serde_json::json;
use std::sync::{Arc, atomic::Ordering};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Submit intent is not admission. Only the primary runtime's existing admission
/// callback consumes this exact seed; retries and restored history never arm it.
/// Weak runtime ownership avoids a Session -> Client -> Managed reference cycle.
pub(super) struct Admission {
    client: Arc<dyn microcrab_session::Client>,
    manager: std::sync::Weak<super::Manager>,
    managed: std::sync::Mutex<std::sync::Weak<Managed>>,
    pending: std::sync::Mutex<Option<SessionInput>>,
}
impl Admission {
    pub(super) fn new(
        manager: &super::Manager,
        client: Arc<dyn microcrab_session::Client>,
    ) -> Arc<Self> {
        Arc::new(Self {
            client,
            manager: manager.self_ref.clone(),
            managed: std::sync::Mutex::new(std::sync::Weak::new()),
            pending: std::sync::Mutex::new(None),
        })
    }
    pub(super) fn install(&self, managed: &Arc<Managed>) {
        *self.managed.lock().unwrap_or_else(|e| e.into_inner()) = Arc::downgrade(managed);
    }
    pub(super) fn arm(&self, input: Option<SessionInput>) {
        *self.pending.lock().unwrap_or_else(|e| e.into_inner()) = input;
    }
}
#[async_trait]
impl microcrab_session::Client for Admission {
    async fn observe_input_admitted(
        &self,
        epoch: u64,
        items: &[microcrab_transport::ContentItem],
        submissions: &[(usize, String)],
    ) {
        self.client
            .observe_input_admitted(epoch, items, submissions)
            .await;
    }
    async fn observe_admitted(
        &self,
        epoch: u64,
        items: &[microcrab_transport::ContentItem],
        producer: Option<&str>,
    ) {
        // The callback precedes provider issuance and follows actual Context
        // admission. Keep display publication with its original client owner.
        self.client.observe_admitted(epoch, items, producer).await;
        let input = self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let managed = self
            .managed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade();
        if let (Some(input), Some(managed), Some(manager)) =
            (input, managed, self.manager.upgrade())
            && managed.auto_naming.load(Ordering::Acquire)
        {
            start_admitted(
                managed,
                manager.directory.clone(),
                manager.config.provider.model.clone(),
                input,
                manager.instance.clone(),
            )
            .await;
        }
    }
    async fn observe_response_admitted(
        &self,
        epoch: u64,
        items: &[microcrab_transport::ContentItem],
        assistant_index: usize,
        attempt: Option<&microcrab_transport::ModelAttempt>,
    ) {
        self.client
            .observe_response_admitted(epoch, items, assistant_index, attempt)
            .await;
    }
    async fn observe_tool_result_admitted(
        &self,
        epoch: u64,
        items: &[microcrab_transport::ContentItem],
        assistant_index: usize,
        call_index: usize,
        execution_id: u64,
        terminal_status: Option<&str>,
    ) {
        self.client
            .observe_tool_result_admitted(
                epoch,
                items,
                assistant_index,
                call_index,
                execution_id,
                terminal_status,
            )
            .await;
    }
    async fn complete(
        &self,
        content: &microcrab_transport::Content,
        auditor: &dyn microcrab_auditor::Auditor,
        variables: &dyn microcrab_auditor::VariableView,
        cancellation: CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        self.client
            .complete(content, auditor, variables, cancellation)
            .await
    }
    async fn stream(
        &self,
        content: &microcrab_transport::Content,
        auditor: &dyn microcrab_auditor::Auditor,
        variables: &dyn microcrab_auditor::VariableView,
        cancellation: CancellationToken,
        events: Option<tokio::sync::mpsc::Sender<microcrab_transport::ModelEvent>>,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        self.client
            .stream(content, auditor, variables, cancellation, events)
            .await
    }
    async fn stream_admitted(
        &self,
        content: &microcrab_transport::Content,
        auditor: &dyn microcrab_auditor::Auditor,
        variables: &dyn microcrab_auditor::VariableView,
        cancellation: CancellationToken,
        events: Option<tokio::sync::mpsc::Sender<microcrab_transport::ModelEvent>>,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        self.stream_admitted_outcome(content, auditor, variables, cancellation, events)
            .await
            .map(|outcome| outcome.response)
    }
    async fn stream_admitted_outcome(
        &self,
        content: &microcrab_transport::Content,
        auditor: &dyn microcrab_auditor::Auditor,
        variables: &dyn microcrab_auditor::VariableView,
        cancellation: CancellationToken,
        events: Option<tokio::sync::mpsc::Sender<microcrab_transport::ModelEvent>>,
    ) -> Result<microcrab_transport::ResponseOutcome, microcrab_transport::Error> {
        let managed = self
            .managed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade();
        if let Some(managed) = managed {
            // Core applies pending native mode changes after the admitted batch.
            // Observe the live mode before sending the next provider request.
            // Serialize with manual replies and asynchronous snapshot publication.
            let _record = managed.record.lock().await;
            if !managed.closing.load(Ordering::Acquire) {
                let state = managed.session.state();
                managed.hub.applied_mode(state.selected_mode.as_deref());
            }
        }
        self.client
            .stream_admitted_outcome(content, auditor, variables, cancellation, events)
            .await
    }
}

/// Optional naming owns cancellation and durable receipt settlement. Its task
/// slot serializes registration/join; submitted Directory writes always settle.
pub(super) struct Task {
    cancellation: CancellationToken,
    worker: tokio::task::JoinHandle<()>,
    #[cfg(test)]
    title_waiting: Arc<tokio::sync::Notify>,
}

pub(super) async fn stop(managed: &Managed) -> Result<(), tokio::task::JoinError> {
    managed.naming_token.cancel();
    let mut slot = managed.naming.lock().await;
    if let Some(task) = slot.take() {
        task.cancellation.cancel();
        task.worker.await?;
    }
    Ok(())
}

fn operation_id(session_id: &str) -> Result<String, Error> {
    microcrab_storage::history::content_hash(&format!("{session_id}:auto-rename"))
}

// Manual suppression and automatic admission share the same durable receipt schema.
// A running instance marks a claim that must persist before provider issuance.
fn new_operation(
    record: &SessionRecord,
    id: String,
    running_instance: Option<&str>,
) -> Result<OperationRecord, Error> {
    let timestamp = now();
    Ok(OperationRecord {
        version: RECORD_VERSION,
        id,
        session_id: record.id.clone(),
        owner: record.owner.clone(),
        method: "session.auto_rename".into(),
        payload_hash: microcrab_storage::history::content_hash(
            &json!({"session_id":record.id,"purpose":"auto_rename"}),
        )?,
        status: if running_instance.is_some() {
            "running"
        } else {
            "completed"
        }
        .into(),
        result: Some(match running_instance {
            Some(instance) => json!({"server_instance":instance}),
            None => json!({"skipped":"custom_title"}),
        }),
        revision: 0,
        created_at_ms: timestamp,
        updated_at_ms: timestamp,
    })
}

/// Caller owns the command gate and has already settled any admitted naming task.
/// Persist manual intent even before a delayed automatic naming task is registered.
pub(super) async fn suppress_after_manual(
    managed: &Managed,
    directory: &dyn Directory,
) -> Result<(), Error> {
    let record = managed.record.lock().await.clone();
    suppress_record(&record, directory).await
}

/// Dormant rename has no runtime naming worker to stop. Persisting the same
/// suppression receipt keeps a later activation from auto-renaming over the
/// explicit user title.
pub(super) async fn suppress_record(
    record: &SessionRecord,
    directory: &dyn Directory,
) -> Result<(), Error> {
    let id = operation_id(&record.id)?;
    if directory.get_operation(&record.owner, &id).await?.is_some() {
        return Ok(());
    }
    directory
        .create_operation(new_operation(record, id, None)?)
        .await?;
    Ok(())
}

#[cfg(test)]
pub(super) async fn start(
    managed: Arc<Managed>,
    directory: Arc<dyn Directory>,
    model: String,
    input: SessionInput,
    instance: String,
) {
    let _gate = managed.commands.lock().await;
    start_admitted(managed.clone(), directory, model, input, instance).await;
}

/// Registration is serialized by the task slot, including terminal cancellation.
/// Admission callers may already own the command gate; never acquire it here.
pub(super) async fn start_admitted(
    managed: Arc<Managed>,
    directory: Arc<dyn Directory>,
    model: String,
    input: SessionInput,
    instance: String,
) {
    if !input.parts.iter().any(|part| matches!(part, microcrab_protocol::InputPart::Text { text } if !text.trim().is_empty())) {
        return;
    }
    let mut slot = managed.naming.lock().await;
    if slot.is_some()
        || managed.naming_token.is_cancelled()
        || managed.closing.load(std::sync::atomic::Ordering::Acquire)
    {
        return;
    }
    let cancellation = managed.naming_token.child_token();
    let token = cancellation.clone();
    let owner = managed.clone();
    #[cfg(test)]
    let title_waiting = Arc::new(tokio::sync::Notify::new());
    #[cfg(test)]
    let waiting = title_waiting.clone();
    let worker = tokio::spawn(async move {
        run_cancellable(
            owner,
            directory,
            model,
            input,
            instance,
            token,
            #[cfg(test)]
            waiting,
        )
        .await;
    });
    *slot = Some(Task {
        cancellation,
        worker,
        #[cfg(test)]
        title_waiting,
    });
}

#[cfg(test)]
pub(super) async fn finish(managed: &Managed) {
    let task = managed.naming.lock().await.take().unwrap();
    task.worker.await.unwrap();
}

#[cfg(test)]
pub(super) async fn wait_for_title(managed: &Managed) {
    let waiting = managed
        .naming
        .lock()
        .await
        .as_ref()
        .unwrap()
        .title_waiting
        .clone();
    waiting.notified().await;
}

struct Store {
    managed: Arc<Managed>,
    directory: Arc<dyn Directory>,
    model: String,
    instance: String,
    claim: Arc<Mutex<Option<OperationRecord>>>,
    claim_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    title_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    cancellation: CancellationToken,
    #[cfg(test)]
    title_waiting: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl AutoNamingStore for Store {
    async fn claim(&self) -> Result<bool, Error> {
        let _gate = tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => return Err(Error::Cancelled),
            gate = self.managed.commands.lock() => gate,
        };
        if self
            .managed
            .closing
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(false);
        }
        let record = self.managed.record.lock().await.clone();
        if self
            .directory
            .get(&record.owner, &record.id)
            .await?
            .is_none()
        {
            return Ok(false);
        }
        let id = operation_id(&record.id)?;
        if self
            .directory
            .get_operation(&record.owner, &id)
            .await?
            .is_some()
        {
            return Ok(false);
        }
        let enabled = record.title == "New session" && !record.archived;
        let operation = new_operation(&record, id, enabled.then_some(self.instance.as_str()))?;
        let directory = self.directory.clone();
        let claimed = self.claim.clone();
        let (reply, receipt) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(async move {
            let result = match directory.create_operation(operation).await {
                Ok(operation) => {
                    *claimed.lock().await = Some(operation);
                    Ok(enabled)
                }
                Err(error) => Err(error),
            };
            let _ = reply.send(result);
        });
        *self
            .claim_task
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(worker);
        receipt
            .await
            .map_err(|_| Error::protocol("naming claim owner failed"))?
    }
    async fn set_title(&self, title: &str) -> Result<(), Error> {
        if title.trim().is_empty() || title.len() > 4096 {
            return Err(Error::config("generated title is invalid"));
        }
        let managed = self.managed.clone();
        let directory = self.directory.clone();
        let model = self.model.clone();
        let claim = self.claim.lock().await.clone();
        let title = title.to_owned();
        let cancellation = self.cancellation.clone();
        #[cfg(test)]
        self.title_waiting.notify_one();
        let (reply, published) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(async move {
            let result = async {
                // Close/model transitions can hold the gate while joining naming.
                // Cancel before acquiring it; a submitted Directory write still settles.
                let _gate = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(Error::Cancelled),
                    gate = managed.commands.lock() => gate,
                };
                if managed.closing.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(Error::Closed);
                }
                let mut record = managed.record.lock().await;
                if directory.get(&record.owner, &record.id).await?.is_none() {
                    return Ok(());
                }
                if record.title != "New session"
                    || record.archived
                    || !claim.as_ref().is_some_and(|claim| {
                        claim.session_id == record.id && claim.owner == record.owner
                    })
                {
                    return Ok(());
                }
                let revision = record.revision;
                let mut next = record.clone();
                next.title = title;
                next.revision = revision
                    .checked_add(1)
                    .ok_or_else(|| Error::protocol("session revision exhausted"))?;
                next.updated_at_ms = now().max(record.updated_at_ms);
                *record = directory.replace(&record.owner, revision, next).await?;
                drop(record);
                let _ = super::view::refresh(directory.as_ref(), &managed, &model).await;
                Ok(())
            }
            .await;
            let _ = reply.send(result);
        });
        *self
            .title_task
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(worker);
        published
            .await
            .map_err(|_| Error::protocol("naming title owner failed"))?
    }
}
#[cfg(test)]
pub(super) async fn run(
    managed: Arc<Managed>,
    directory: Arc<dyn Directory>,
    model: String,
    input: SessionInput,
    instance: String,
) {
    let cancellation = managed.naming_token.child_token();
    run_cancellable(
        managed,
        directory,
        model,
        input,
        instance,
        cancellation,
        Arc::new(tokio::sync::Notify::new()),
    )
    .await;
}

async fn run_cancellable(
    managed: Arc<Managed>,
    directory: Arc<dyn Directory>,
    model: String,
    input: SessionInput,
    instance: String,
    cancellation: CancellationToken,
    #[cfg(test)] title_waiting: Arc<tokio::sync::Notify>,
) {
    let store = Store {
        managed: managed.clone(),
        directory: directory.clone(),
        model,
        instance,
        claim: Arc::new(Mutex::new(None)),
        claim_task: std::sync::Mutex::new(None),
        title_task: std::sync::Mutex::new(None),
        cancellation: cancellation.clone(),
        #[cfg(test)]
        title_waiting,
    };
    let mut result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(Error::Cancelled),
        result = maybe_rename(&managed.session, &input, &store) => result,
    };
    // A blocking publication already submitted to Directory outlives a dropped
    // waiter. Its owned acknowledgment must settle before closing the session.
    let claim_task = {
        store
            .claim_task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    };
    if let Some(worker) = claim_task
        && worker.await.is_err()
    {
        result = Err(Error::protocol("naming claim owner failed"));
    }
    let title_task = {
        store
            .title_task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    };
    if let Some(worker) = title_task
        && worker.await.is_err()
    {
        result = Err(Error::protocol("naming title owner failed"));
    }
    let claimed = store.claim.lock().await.take();
    if let Some(mut operation) = claimed
        && operation.status == "running"
    {
        operation.revision = 1;
        operation.updated_at_ms = now().max(operation.updated_at_ms);
        operation.status = match &result {
            Ok(_) => "completed",
            Err(Error::Cancelled) => "interrupted",
            Err(_) => "failed",
        }
        .into();
        operation.result = Some(match result {
            Ok(_) => json!({"attempted":true}),
            Err(_) => {
                json!({"error":{"code":"auto_naming_failed","message":"Automatic naming failed"}})
            }
        });
        if directory
            .replace_operation(&operation.owner.clone(), 0, operation)
            .await
            .is_err()
        {
            managed
                .hub
                .publish(super::SessionRuntimeEvent::ResyncRequired(
                    super::ResyncReason::NamingReceiptFailed,
                ));
        }
    }
}
