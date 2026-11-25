//! Bearer-only admission and observation of exact operations in the canonical Core inbox.
use super::{Manager, external_speakers::CredentialUse, safe};
use microcrab_protocol::application::WireError;
use microcrab_session::state::communication::{
    AgentId,
    inbox::{
        DurableInbox, ExternalCredentialIncarnation, ExternalDialogueReceipt, ExternalReplay,
        QueuedReceipt,
    },
};
use std::{path::Path, sync::Arc};

pub(crate) mod reply;

#[derive(Debug)]
pub(crate) enum ExternalInboxReceipt {
    Legacy(QueuedReceipt),
    Dialogue(ExternalDialogueReceipt),
}

pub(super) fn denied() -> WireError {
    WireError::new(
        "unauthenticated",
        "External speaker key is not bound to the target Session",
    )
}
fn missing() -> WireError {
    WireError::new(
        "operation_not_found",
        "External operation has no committed receipt; this does not prove a pending append failed",
    )
}
fn backend(_: microcrab_session::Error) -> WireError {
    WireError::new(
        "storage_unavailable",
        "External inbox storage is unavailable",
    )
}
fn auth_error(error: WireError) -> WireError {
    match error.code.as_str() {
        "unauthenticated" | "not_found" => denied(),
        "io" | "protocol" | "configuration_unavailable" | "storage_unavailable" => WireError::new(
            "storage_unavailable",
            "External speaker configuration is unavailable",
        ),
        _ => error,
    }
}
fn enqueue_error(error: microcrab_session::Error, operation: &str) -> WireError {
    use microcrab_session::{Error, observation::diagnostics::EffectKnowledge};
    if matches!(&error, Error::Diagnostic(diagnostic) if diagnostic.effects == EffectKnowledge::MayHaveOccurred && diagnostic.retryable == Some(false))
    {
        let mut uncertain = WireError::new(
            "outcome_unknown",
            "External enqueue requires receipt observation before retrying",
        );
        uncertain.details = Some(Box::new(serde_json::json!({"operation_id":operation})));
        return uncertain;
    }
    if matches!(&error, Error::Config(reason) if reason.public_reason() == Some("operation id already names a different communication admission"))
        || matches!(&error, Error::Denied(reason) if reason.public_reason() == Some("no matching legacy external admission"))
    {
        return WireError::new(
            "operation_conflict",
            "Operation does not match the committed external admission",
        );
    }
    safe(error)
}

impl Manager {
    async fn inspect_external_inbox(&self, id: &str) -> Result<Option<DurableInbox>, WireError> {
        let path = self.config.data_dir.join("sessions").join(id);
        let Some(point) = self.child_checkpoint(&path, id).await? else {
            return Ok(None);
        };
        let identity = point
            .checkpoint
            .recover
            .state
            .communication
            .ok_or_else(|| {
                WireError::new(
                    "permission_denied",
                    "Legacy Session has no communication inbox",
                )
            })?
            .identity
            .agent_id;
        self.inspect_communication(&path, id, identity).await
    }

    async fn inspect_communication(
        &self,
        path: &Path,
        id: &str,
        identity: AgentId,
    ) -> Result<Option<DurableInbox>, WireError> {
        let _ = (path, id, &identity);
        match self.config.storage {
            crate::config::Storage::File => {
                #[cfg(feature = "storage-file")]
                {
                    microcrab_storage::history::FileJournal::inspect_communication(
                        path,
                        id.to_owned(),
                        identity,
                    )
                    .await
                    .map_err(backend)
                }
                #[cfg(not(feature = "storage-file"))]
                {
                    Err(WireError::new(
                        "storage_unavailable",
                        "File storage is not compiled",
                    ))
                }
            }
            crate::config::Storage::Sqlite => {
                #[cfg(feature = "storage-sqlite")]
                {
                    microcrab_storage::history::SqliteJournal::inspect_communication(
                        self.config.data_dir.join("storage.sqlite"),
                        id.to_owned(),
                        identity,
                    )
                    .await
                    .map_err(backend)
                }
                #[cfg(not(feature = "storage-sqlite"))]
                {
                    Err(WireError::new(
                        "storage_unavailable",
                        "SQLite storage is not compiled",
                    ))
                }
            }
        }
    }

    pub(crate) async fn external_inbox(
        &self,
        session_id: String,
        bearer: String,
        operation_id: String,
        body: Option<String>,
    ) -> Result<ExternalInboxReceipt, WireError> {
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| WireError::new("capacity", "Command capacity reached"))?;
        let manager = self
            .self_ref
            .upgrade()
            .ok_or_else(|| WireError::new("closed", "Manager unavailable"))?;
        self.track(async move {
            let _permit = permit;
            manager
                .external_inbox_owned(&session_id, &bearer, operation_id, body)
                .await
        })?
        .await
        .map_err(|_| {
            WireError::new(
                "outcome_unknown",
                "External admission owner stopped; observe the original operation before retrying",
            )
        })?
    }

    async fn external_inbox_owned(
        self: Arc<Self>,
        id: &str,
        bearer: &str,
        operation: String,
        body: Option<String>,
    ) -> Result<ExternalInboxReceipt, WireError> {
        if !crate::config::valid_id(id) {
            return Err(denied());
        }
        let (record, _marker) = {
            let _lifecycle = self.journal_open_gate(id).lock_owned().await;
            let (record, _owner) = self
                .lock_external_speaker_session(id)
                .await
                .map_err(auth_error)?;
            if let Some(receipt) = self
                .observe_external_operation_locked(&record, bearer, &operation, body.as_deref())
                .await?
            {
                return Ok(receipt);
            }
            if body.is_none() {
                return Err(missing());
            }
            let project = record.project_id.as_deref().ok_or_else(denied)?;
            let marker = self.external_messaging_marker(&record.owner, project);
            (record, marker)
        };
        let Some(body) = body else {
            return Err(missing());
        };
        // Never nest the speaker gate into active() or a Core restore. A project
        // marker spans this interval, and keys/deletion are revalidated after it.
        let managed = self.active(record.clone(), true).await?;
        let _lifecycle = self.journal_open_gate(id).lock_owned().await;
        let (current, _owner) = self
            .lock_external_speaker_session(id)
            .await
            .map_err(auth_error)?;
        if current.owner != record.owner
            || current.project_id != record.project_id
            || current.archived
            || managed.closing.load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(denied());
        }
        if let Some(receipt) = self
            .observe_external_operation_locked(&current, bearer, &operation, Some(&body))
            .await?
        {
            return Ok(receipt);
        }
        let proof = self
            .external_speaker_credential_locked(&current, bearer, CredentialUse::Admit)
            .await
            .map_err(auth_error)?
            .ok_or_else(denied)?;
        let incarnation = ExternalCredentialIncarnation::new(*proof.witness());
        self.synchronize_external_speakers_locked(
            &current.owner,
            current.project_id.as_deref(),
            id,
            &managed.session,
        )
        .await?;
        // The tracked operation owns the authority fence through Core's durable
        // enqueue ACK. Only the autonomous dispatcher (on its own task) claims it.
        managed
            .session
            .enqueue_authenticated_external(operation.clone(), proof.identity, body, incarnation)
            .await
            .map(ExternalInboxReceipt::Dialogue)
            .map_err(|error| enqueue_error(error, &operation))
    }

    async fn observe_external_operation_locked(
        &self,
        record: &microcrab_storage::SessionRecord,
        bearer: &str,
        operation: &str,
        body: Option<&str>,
    ) -> Result<Option<ExternalInboxReceipt>, WireError> {
        let speaker = self
            .authenticate_external_speaker_at(
                &record.owner,
                record.project_id.as_deref(),
                &record.id,
                bearer,
            )
            .await
            .map_err(auth_error)?;
        if super::deletion::tombstone::read(self, &record.id)
            .await
            .is_some()
        {
            return Err(denied());
        }
        let Some(inbox) = self.inspect_external_inbox(&record.id).await? else {
            return Ok(None);
        };
        if body.is_none()
            && let Some(legacy) = inbox.receipt(operation)
        {
            return Ok(Some(ExternalInboxReceipt::Legacy(legacy)));
        }
        let proof = self
            .external_speaker_credential_locked(record, bearer, CredentialUse::Observe)
            .await
            .map_err(auth_error)?;
        let incarnation = proof
            .as_ref()
            .map(|proof| ExternalCredentialIncarnation::new(*proof.witness()));
        if let Some(body) = body {
            return inbox
                .replay_external(operation, &speaker.identity, body, incarnation.as_ref())
                .map(|receipt| {
                    receipt.map(|receipt| match receipt {
                        ExternalReplay::Legacy(receipt) => ExternalInboxReceipt::Legacy(receipt),
                        ExternalReplay::Dialogue(receipt) => {
                            ExternalInboxReceipt::Dialogue(receipt)
                        }
                    })
                })
                .map_err(|error| enqueue_error(error, operation));
        }
        let Some(incarnation) = incarnation.as_ref() else {
            return Ok(None);
        };
        inbox
            .authenticated_external_receipt(operation, &speaker.identity, incarnation)
            .map(|receipt| receipt.map(ExternalInboxReceipt::Dialogue))
            .map_err(backend)
    }
}
