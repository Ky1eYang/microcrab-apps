//! Bearer-only external admission; no browser Principal, second queue or credential index.
use super::{Manager, safe};
use microcrab_protocol::application::WireError;
use microcrab_session::state::communication::inbox::QueuedReceipt;
use std::sync::Arc;

pub(super) fn denied() -> WireError {
    WireError::new(
        "unauthenticated",
        "External speaker key is not bound to the target Session",
    )
}
fn backend(error: microcrab_session::Error) -> WireError {
    let cause = safe(error);
    let mut error = WireError::new("storage_unavailable", &cause.message);
    error.details = Some(Box::new(serde_json::json!({"cause":cause})));
    error
}
impl Manager {
    async fn inspect_external_receipt(
        &self,
        id: &str,
        operation: &str,
    ) -> Result<QueuedReceipt, WireError> {
        let path = self.config.data_dir.join("sessions").join(id);
        let point = self.child_checkpoint(&path, id).await?.ok_or_else(|| {
            WireError::new(
                "operation_not_found",
                "Session has no selected durable communication checkpoint",
            )
        })?;
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
        let inbox = match self.config.storage {
            crate::config::Storage::File => {
                #[cfg(feature = "storage-file")]
                {
                    microcrab_storage::history::FileJournal::inspect_communication(
                        &path,
                        id.to_owned(),
                        identity,
                    )
                    .await
                    .map_err(backend)?
                }
                #[cfg(not(feature = "storage-file"))]
                {
                    return Err(WireError::new(
                        "storage_unavailable",
                        "File storage is not compiled",
                    ));
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
                    .map_err(backend)?
                }
                #[cfg(not(feature = "storage-sqlite"))]
                {
                    return Err(WireError::new(
                        "storage_unavailable",
                        "SQLite storage is not compiled",
                    ));
                }
            }
        };
        inbox.and_then(|inbox|inbox.receipt(operation)).ok_or_else(||WireError::new("operation_not_found","External operation has no committed receipt; this does not prove a pending append failed"))
    }
    pub(crate) async fn external_inbox(
        &self,
        session_id: String,
        bearer: String,
        operation_id: String,
        body: Option<String>,
    ) -> Result<QueuedReceipt, WireError> {
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
    ) -> Result<QueuedReceipt, WireError> {
        if !crate::config::valid_id(id) {
            return Err(denied());
        }
        // The canonical exact index selects an owner; only effective bearer proof grants admission.
        let owner = self
            .directory
            .session_owner(id)
            .await
            .map_err(backend)?
            .ok_or_else(denied)?;
        let _admission = self.speaker_gate(&owner).lock_owned().await;
        let record = self
            .directory
            .get(&owner, id)
            .await
            .map_err(backend)?
            .ok_or_else(denied)?;
        let identity = self
            .authenticate_external_speaker(&owner, id, bearer)
            .await
            .map_err(|error| {
                if matches!(error.code.as_str(), "unauthenticated" | "not_found") {
                    denied()
                } else if matches!(
                    error.code.as_str(),
                    "io" | "protocol" | "configuration_unavailable" | "storage_unavailable"
                ) {
                    let mut unavailable = WireError::new("storage_unavailable", &error.message);
                    unavailable.details = Some(Box::new(serde_json::json!({"cause":error})));
                    unavailable
                } else {
                    error
                }
            })?;
        let managed = if body.is_some() {
            self.active(record, true).await?
        } else {
            // Observation never activates dormant Sessions or starts a dispatcher.
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
            match active {
                Some(managed) => managed,
                None => return self.inspect_external_receipt(id, &operation).await,
            }
        };
        if let Some(body) = body {
            self.synchronize_external_speakers(
                &owner,
                managed.record.lock().await.project_id.as_deref(),
                id,
                &managed.session,
            )
            .await?;
            match managed
                .session
                .enqueue_external(operation.clone(), identity.identity, body)
                .await
            {
                Ok(receipt) => Ok(receipt),
                Err(error) => {
                    let uncertain = matches!(&error, microcrab_session::Error::Diagnostic(diagnostic) if diagnostic.effects == microcrab_session::observation::diagnostics::EffectKnowledge::MayHaveOccurred && diagnostic.retryable == Some(false));
                    let cause = safe(error);
                    if !uncertain {
                        return Err(cause);
                    }
                    let mut uncertain = WireError::new(
                        "outcome_unknown",
                        format!(
                            "External enqueue requires receipt observation: {}",
                            cause.message
                        ),
                    );
                    uncertain.details = Some(Box::new(
                        serde_json::json!({"operation_id":operation,"cause":cause}),
                    ));
                    Err(uncertain)
                }
            }
        } else {
            managed.session.delivery_receipt(&operation).await.map_err(backend)?.ok_or_else(|| WireError::new("operation_not_found", "External operation has no committed receipt; this does not prove a pending append failed"))
        }
    }
}
