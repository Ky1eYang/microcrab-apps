//! Read-only observations and persistent reply-store composition for root Sessions.
use super::{Manager, auth_error, denied};
use crate::config::{Config, Storage};
use microcrab_protocol::application::WireError;
use microcrab_session::{
    Error,
    state::communication::{
        AgentId, SessionInteraction,
        inbox::{DeliveryStatus, ExternalCredentialIncarnation},
        reply::{
            DialogueAdmissionKey, DialogueReplyMutation, DialogueReplyStore, DurableDialogueReply,
            ReplyWrite,
        },
    },
};
use microcrab_storage::{SessionRecord, dialogue_reply::DialogueReplyScope};
use std::{path::PathBuf, sync::Arc};
use tokio::{sync::OnceCell, sync::OwnedSemaphorePermit};
use tokio_util::sync::CancellationToken;

/// Only an actual Core mutation acquires a writer lease. The adapter is owned by
/// the Session runtime; neither a reader nor Manager retains the writer on close.
pub(super) struct Store {
    storage: Storage,
    path: PathBuf,
    scope: DialogueReplyScope,
    writer: OnceCell<Box<dyn DialogueReplyStore>>,
}

impl Store {
    fn root(config: &Config, owner: &str, id: &str, agent: AgentId) -> Result<Self, Error> {
        Ok(Self {
            storage: config.storage,
            path: Self::root_path(config, id),
            scope: DialogueReplyScope::new(owner, id, id, agent)?,
            writer: OnceCell::new(),
        })
    }

    fn root_path(config: &Config, id: &str) -> PathBuf {
        match config.storage {
            Storage::File => config.data_dir.join("sessions").join(id),
            Storage::Sqlite => config.data_dir.join("storage.sqlite"),
        }
    }

    async fn open(&self) -> Result<Box<dyn DialogueReplyStore>, Error> {
        match self.storage {
            #[cfg(feature = "storage-file")]
            Storage::File => Ok(Box::new(
                microcrab_storage::dialogue_reply::FileDialogueReplies::open(
                    &self.path,
                    self.scope.clone(),
                )
                .await?,
            )),
            #[cfg(feature = "storage-sqlite")]
            Storage::Sqlite => Ok(Box::new(
                microcrab_storage::dialogue_reply::SqliteDialogueReplies::open(
                    &self.path,
                    self.scope.clone(),
                )
                .await?,
            )),
            #[allow(unreachable_patterns)]
            _ => Err(Error::config(
                "External dialogue storage backend is not compiled",
            )),
        }
    }
}

impl Store {
    async fn inspect(
        &self,
        agent: &AgentId,
        key: &DialogueAdmissionKey,
    ) -> Result<Option<DurableDialogueReply>, Error> {
        let _ = key;
        if self.scope.agent_id() != agent {
            return Err(Error::protocol("External dialogue reply owner mismatch"));
        }
        match self.storage {
            #[cfg(feature = "storage-file")]
            Storage::File => {
                microcrab_storage::dialogue_reply::FileDialogueReplies::inspect(
                    &self.path,
                    self.scope.clone(),
                    key,
                )
                .await
            }
            #[cfg(feature = "storage-sqlite")]
            Storage::Sqlite => {
                microcrab_storage::dialogue_reply::SqliteDialogueReplies::inspect(
                    &self.path,
                    self.scope.clone(),
                    key,
                )
                .await
            }
            #[allow(unreachable_patterns)]
            _ => Err(Error::config(
                "External dialogue storage backend is not compiled",
            )),
        }
    }
}

#[async_trait::async_trait]
impl DialogueReplyStore for Store {
    async fn load(
        &self,
        agent: &AgentId,
        key: &DialogueAdmissionKey,
    ) -> Result<Option<DurableDialogueReply>, Error> {
        self.inspect(agent, key).await
    }

    async fn apply(&self, mutation: DialogueReplyMutation) -> Result<ReplyWrite, Error> {
        if mutation.binding().agent_id() != self.scope.agent_id() {
            return Err(Error::protocol("External dialogue reply owner mismatch"));
        }
        self.writer
            .get_or_try_init(|| self.open())
            .await?
            .apply(mutation)
            .await
    }
}

pub(in crate::manager) fn store_factory(
    config: &Config,
    record: &SessionRecord,
) -> impl FnOnce(&AgentId) -> Result<Arc<dyn DialogueReplyStore>, Error> + use<> {
    let (owner, id) = (record.owner.clone(), record.id.clone());
    let storage = config.storage;
    let path = Store::root_path(config, &id);
    move |agent| {
        Ok(Arc::new(Store {
            storage,
            path,
            scope: DialogueReplyScope::new(&owner, &id, &id, agent.clone())?,
            writer: OnceCell::new(),
        }))
    }
}

/// Private input to the wire projector; do not serialize Core records or bindings.
pub(crate) struct ReplySnapshot {
    pub(crate) consumed: bool,
    pub(crate) speaker_identity: String,
    pub(crate) reply: Option<DurableDialogueReply>,
}

/// Owns only its admission slot; dropping the body cannot interrupt or replay a Turn.
pub(crate) struct ReplyReader {
    manager: Arc<Manager>,
    session_id: String,
    bearer: String,
    pub(crate) dialogue_id: String,
    _permit: OwnedSemaphorePermit,
}

fn missing() -> WireError {
    WireError::new(
        "not_found",
        "External dialogue has no matching committed admission",
    )
}

fn backend(_: Error) -> WireError {
    WireError::new(
        "storage_unavailable",
        "External dialogue storage is unavailable",
    )
}

impl Manager {
    /// End HTTP reply observers before the server waits on graceful connection drain.
    pub fn stop_external_reply_reads(&self) {
        self.external_reply_stop.cancel();
    }

    pub(crate) fn external_reply_reader(
        self: &Arc<Self>,
        session_id: String,
        bearer: String,
        dialogue_id: String,
    ) -> Result<ReplyReader, WireError> {
        if self.external_reply_stop.is_cancelled() {
            return Err(WireError::new("closed", "Server is shutting down"));
        }
        let permit = self
            .external_reply_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| WireError::new("capacity", "External dialogue read capacity reached"))?;
        Ok(ReplyReader {
            manager: self.clone(),
            session_id,
            bearer,
            dialogue_id,
            _permit: permit,
        })
    }
}

impl ReplyReader {
    pub(crate) fn stop(&self) -> CancellationToken {
        self.manager.external_reply_stop.clone()
    }

    /// Emits a single observation under the same owner credential fence as revoke,
    /// and the same exact Session gate as delete. Never hold either across a wait
    /// for future updates or across HTTP backpressure.
    pub(crate) async fn observe<T>(
        &self,
        project: impl FnOnce(ReplySnapshot) -> T,
    ) -> Result<T, WireError> {
        let manager = &self.manager;
        let id = &self.session_id;
        tokio::select! {
            biased;
            _ = manager.external_reply_stop.cancelled() => Err(WireError::new("closed", "Server is shutting down")),
            result = async {
                if !crate::config::valid_id(id) {
                    return Err(denied());
                }
                let _lifecycle = manager.journal_open_gate(id).lock_owned().await;
                let (record, _speaker) = manager.lock_external_speaker_session(id).await.map_err(auth_error)?;
                let credential = manager.external_speaker_credential_locked(
                    &record, &self.bearer, super::CredentialUse::Observe,
                ).await.map_err(auth_error)?;
                let credential = credential.ok_or_else(missing)?;
                if crate::manager::deletion::tombstone::read(manager, id).await.is_some() {
                    return Err(denied());
                }
                let path = manager.config.data_dir.join("sessions").join(id);
                let identity = manager.child_checkpoint(&path, id).await?
                    .and_then(|point| point.checkpoint.recover.state.communication)
                    .map(|state| state.identity)
                    .filter(|identity| identity.interaction == SessionInteraction::Persistent)
                    .ok_or_else(missing)?;
                let inbox = manager.inspect_communication(&path, id, identity.agent_id.clone())
                    .await?.ok_or_else(missing)?;
                let proof = ExternalCredentialIncarnation::new(*credential.witness());
                let (receipt, key) = inbox.authenticated_external_dialogue(
                    &self.dialogue_id, &credential.identity, &proof,
                ).map_err(backend)?.ok_or_else(missing)?;
                let consumed = receipt.receipt.status == DeliveryStatus::Consumed;
                let reply = if consumed {
                    Store::root(&manager.config, &record.owner, id, identity.agent_id.clone())
                        .map_err(backend)?.inspect(&identity.agent_id, &key).await.map_err(backend)?
                } else {
                    None
                };
                Ok(project(ReplySnapshot { consumed, speaker_identity: credential.identity, reply }))
            } => result,
        }
    }
}
