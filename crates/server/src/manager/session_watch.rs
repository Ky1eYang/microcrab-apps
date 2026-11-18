//! Owner-level session list increments. A browser loads `GET /sessions` once and
//! then applies `sessions.changed` events from its `sessions.watch` subscription.
//!
//! Producers only mark `(owner, session)` dirty: directory record writes (create,
//! rename, archive, naming, settlement, removal), runtime status changes seen by
//! a session Hub, and deletion cleanup. One publisher drains the dirty set and
//! reads the *current* list item for each, so a burst coalesces and a late
//! publication can never resurrect an older view.
use super::Manager;
use async_trait::async_trait;
use microcrab_session::Error;
use microcrab_storage::{
    Directory, OperationRecord, SessionRecord, directory::OperationSettlement,
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{broadcast, mpsc};

/// One owner-scoped list event.
#[derive(Debug)]
pub(crate) struct SessionChange {
    pub owner: String,
    pub event: &'static str,
    pub data: Value,
}

/// Cheap, clonable producer handle.
#[derive(Clone)]
pub(crate) struct SessionDirty {
    sender: mpsc::Sender<(String, String)>,
    events: broadcast::Sender<Arc<SessionChange>>,
}
impl SessionDirty {
    pub(crate) fn mark(&self, owner: &str, id: &str) {
        if let Err(mpsc::error::TrySendError::Full(_)) =
            self.sender.try_send((owner.to_owned(), id.to_owned()))
        {
            let _ = self.events.send(Arc::new(SessionChange {
                owner: owner.to_owned(),
                event: "resync_required",
                data: json!({"reason":"slow_consumer"}),
            }));
        }
    }
}

pub(crate) struct SessionWatch {
    sender: broadcast::Sender<Arc<SessionChange>>,
    dirty: SessionDirty,
    receiver: Mutex<Option<mpsc::Receiver<(String, String)>>>,
}
impl SessionWatch {
    pub(crate) fn new() -> Self {
        let (sender, _) = broadcast::channel(256);
        let (dirty, receiver) = mpsc::channel(256);
        Self {
            dirty: SessionDirty {
                sender: dirty,
                events: sender.clone(),
            },
            sender,
            receiver: Mutex::new(Some(receiver)),
        }
    }
    pub(crate) fn dirty(&self) -> SessionDirty {
        self.dirty.clone()
    }
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<Arc<SessionChange>> {
        self.sender.subscribe()
    }
    /// Publish an event that is not a list item (for example a cleanup failure).
    pub(crate) fn publish(&self, change: SessionChange) {
        let _ = self.sender.send(Arc::new(change));
    }
    /// Start the single publisher. It holds only a weak Manager reference and
    /// ends with it.
    pub(crate) fn start(&self, manager: Weak<Manager>) {
        let Some(mut receiver) = self
            .receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        else {
            return;
        };
        tokio::spawn(async move {
            while let Some(first) = receiver.recv().await {
                let mut batch = vec![first];
                while let Ok(next) = receiver.try_recv() {
                    if !batch.contains(&next) {
                        batch.push(next);
                    }
                }
                let Some(manager) = manager.upgrade() else {
                    break;
                };
                if manager.session_watch.sender.receiver_count() == 0 {
                    continue;
                }
                for (owner, id) in batch {
                    match manager.session_change(&owner, &id).await {
                        Ok(change) => manager.session_watch.publish(change),
                        Err(error) => {
                            tracing::debug!(error_code = %error.code, "Session list publication requires resync");
                            manager.session_watch.publish(SessionChange {
                                owner,
                                                event: "resync_required",
                                data: json!({"reason":"list_publication_failed","code":error.code,"message":error.message}),
                            });
                        }
                    }
                }
            }
        });
    }
}

impl Manager {
    /// Subscribe to owner-level list increments (the socket filters by owner).
    pub(crate) fn watch_sessions(&self) -> broadcast::Receiver<Arc<SessionChange>> {
        self.session_watch.subscribe()
    }
    /// Mark one session's list item for republication.
    pub(crate) fn session_dirty(&self, owner: &str, id: &str) {
        self.session_watch.dirty.mark(owner, id);
    }
    /// Report a failed deletion cleanup to the owner's list watchers.
    pub(crate) fn session_cleanup_failed(&self, owner: &str, id: &str, stage: &str, cause: Value) {
        self.session_watch.publish(SessionChange {
            owner: owner.to_owned(),
            event: "session.cleanup_failed",
            data: json!({"session_id":id,"stage":stage,"cause":cause}),
        });
    }
    /// Let a session Hub mark the owner list dirty when its session view changes.
    pub(super) fn watch_hub(&self, hub: &super::hub::Hub, owner: &str, id: &str) {
        let dirty = self.session_watch.dirty();
        let (owner, id) = (owner.to_owned(), id.to_owned());
        hub.watch_session(move || dirty.mark(&owner, &id));
    }
    /// The current list increment for one session: an `upsert` with the same
    /// item `session.list` returns, or a `remove` once it is gone or hidden.
    async fn session_change(
        &self,
        owner: &str,
        id: &str,
    ) -> Result<SessionChange, microcrab_protocol::application::WireError> {
        let record = match self.directory.get(owner, id).await {
            Ok(record) => record,
            Err(error) => {
                let failure = super::safe(error);
                return Ok(SessionChange {
                    owner: owner.to_owned(),
                    event: "sessions.changed",
                    data: json!({"op":"unavailable","session_id":id,
                        "error":{"code":"session_configuration_unavailable","message":failure.message}}),
                });
            }
        };
        let item = match record {
            Some(record) => super::commands::list_item(self, record).await?,
            None => None,
        };
        let data = match item {
            Some(item) => json!({"op":"upsert","session":item}),
            None => {
                let state = super::deletion::tombstone::read(self, id)
                    .await
                    .filter(|stone| stone.owner == owner)
                    .map(|stone| stone.state);
                json!({"op":"remove","session_id":id,"state":state})
            }
        };
        Ok(SessionChange {
            owner: owner.to_owned(),
            event: "sessions.changed",
            data,
        })
    }
}

/// Directory decorator: every successful session-record write marks it dirty.
pub(crate) struct WatchedDirectory {
    pub inner: Arc<dyn Directory>,
    pub dirty: SessionDirty,
}
#[async_trait]
impl Directory for WatchedDirectory {
    async fn session_owner(&self, id: &str) -> Result<Option<String>, Error> {
        self.inner.session_owner(id).await
    }
    async fn unavailable_unowned_count(&self) -> Result<usize, Error> {
        self.inner.unavailable_unowned_count().await
    }
    async fn get(&self, owner: &str, id: &str) -> Result<Option<SessionRecord>, Error> {
        self.inner.get(owner, id).await
    }
    async fn list_entries(
        &self,
        owner: &str,
    ) -> Result<Vec<microcrab_storage::directory::SessionEntry>, Error> {
        self.inner.list_entries(owner).await
    }
    async fn list(&self, owner: &str) -> Result<Vec<SessionRecord>, Error> {
        self.inner.list(owner).await
    }
    async fn create(&self, record: SessionRecord) -> Result<SessionRecord, Error> {
        let created = self.inner.create(record).await?;
        self.dirty.mark(&created.owner, &created.id);
        Ok(created)
    }
    async fn replace(
        &self,
        owner: &str,
        expected_revision: u64,
        record: SessionRecord,
    ) -> Result<SessionRecord, Error> {
        let replaced = self.inner.replace(owner, expected_revision, record).await?;
        self.dirty.mark(&replaced.owner, &replaced.id);
        Ok(replaced)
    }
    async fn get_operation(&self, owner: &str, id: &str) -> Result<Option<OperationRecord>, Error> {
        self.inner.get_operation(owner, id).await
    }
    async fn list_operations(
        &self,
        owner: &str,
        session_id: &str,
    ) -> Result<Vec<OperationRecord>, Error> {
        self.inner.list_operations(owner, session_id).await
    }
    async fn create_operation(&self, record: OperationRecord) -> Result<OperationRecord, Error> {
        self.inner.create_operation(record).await
    }
    async fn replace_operation(
        &self,
        owner: &str,
        expected_revision: u64,
        record: OperationRecord,
    ) -> Result<OperationRecord, Error> {
        self.inner
            .replace_operation(owner, expected_revision, record)
            .await
    }
    async fn settle_operation(
        &self,
        owner: &str,
        settlement: OperationSettlement,
    ) -> Result<(SessionRecord, OperationRecord), Error> {
        let settled = self.inner.settle_operation(owner, settlement).await?;
        self.dirty.mark(&settled.0.owner, &settled.0.id);
        Ok(settled)
    }
    async fn remove(&self, owner: &str, id: &str, expected_revision: u64) -> Result<usize, Error> {
        let removed = self.inner.remove(owner, id, expected_revision).await?;
        self.dirty.mark(owner, id);
        Ok(removed)
    }
    async fn close(&self) -> Result<(), Error> {
        self.inner.close().await
    }
}
