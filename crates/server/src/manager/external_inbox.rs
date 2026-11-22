//! Bearer-only admission and observation of exact operations in the canonical Core inbox.
use super::{Manager, external_speakers::CredentialUse, safe};
use microcrab_protocol::application::WireError;
use microcrab_session::state::communication::{
    AgentId,
    inbox::{DurableInbox, ExternalCredentialIncarnation, ExternalDialogueReceipt, QueuedReceipt},
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
        let (record, _gate) = self
            .lock_external_speaker_session(id)
            .await
            .map_err(auth_error)?;
        // Prove the effective bearer before any inbox reads. Legacy compatibility is
        // deliberately non-migrating, even on POST of an already admitted legacy row.
        let speaker = self
            .authenticate_external_speaker_at(
                &record.owner,
                record.project_id.as_deref(),
                id,
                bearer,
            )
            .await
            .map_err(auth_error)?;
        let slot = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(id)
            .cloned();
        let active = if let Some(slot) = slot {
            slot.active.lock().await.clone()
        } else {
            None
        };
        let dormant = if active.is_none() {
            self.inspect_external_inbox(id).await?
        } else {
            None
        };
        let legacy = if let Some(active) = &active {
            active
                .session
                .delivery_receipt(&operation)
                .await
                .map_err(backend)?
        } else {
            dormant.as_ref().and_then(|inbox| inbox.receipt(&operation))
        };

        let Some(body) = body else {
            if let Some(legacy) = legacy {
                return Ok(ExternalInboxReceipt::Legacy(legacy));
            }
            let proof = self
                .external_speaker_credential_locked(&record, bearer, CredentialUse::Observe)
                .await
                .map_err(auth_error)?
                .ok_or_else(missing)?;
            let incarnation = ExternalCredentialIncarnation::new(*proof.witness());
            let receipt = if let Some(active) = active {
                active
                    .session
                    .authenticated_external_receipt(&operation, &proof.identity, &incarnation)
                    .await
            } else if let Some(inbox) = dormant {
                inbox.authenticated_external_receipt(&operation, &proof.identity, &incarnation)
            } else {
                return Err(missing());
            };
            return receipt
                .map_err(backend)?
                .map(ExternalInboxReceipt::Dialogue)
                .ok_or_else(missing);
        };

        if legacy.is_some() {
            // Core checks exact body/source under its communication fence. This port
            // MUST NOT append on absence, even across activation or history changes.
            let managed = self.active(record, true).await?;
            return managed
                .session
                .replay_legacy_external(operation.clone(), speaker.identity, body)
                .await
                .map(ExternalInboxReceipt::Legacy)
                .map_err(|error| enqueue_error(error, &operation));
        }

        let proof = self
            .external_speaker_credential_locked(&record, bearer, CredentialUse::Admit)
            .await
            .map_err(auth_error)?
            .ok_or_else(denied)?;
        let incarnation = ExternalCredentialIncarnation::new(*proof.witness());
        let owner = record.owner.clone();
        let managed = self.active(record, true).await?;
        self.synchronize_external_speakers(
            &owner,
            managed.record.lock().await.project_id.as_deref(),
            id,
            &managed.session,
        )
        .await?;
        // One exact enqueue, never a fallback or retry after an uncertain result.
        managed
            .session
            .enqueue_authenticated_external(operation.clone(), proof.identity, body, incarnation)
            .await
            .map(ExternalInboxReceipt::Dialogue)
            .map_err(|error| enqueue_error(error, &operation))
    }
}
