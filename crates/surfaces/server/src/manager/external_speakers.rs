//! Dedicated owner-managed credentials. Never include these records in prompt settings or State.
//!
//! Project defaults and sparse Session add/override/disable layers share one owner-scoped
//! CAS revision. A PUT replaces its layer; `keep` preserves a local key without revealing
//! it. Only explicit owner POST key requests disclose secrets. Read-only paths never
//! activate Sessions, generate keys, write configuration or create missing records.
//!
//! Mutation UUIDs and typed request digests bind immutable redacted receipts to the
//! atomic write. Observing a missing receipt does not settle a racing mutation. Retain
//! receipts until the bounded resource is full, then reject rather than lose deduplication.
//! Limits: 128 entries per layer/effective set, 4096 layers and receipts, existing 1 MiB
//! Configuration budget. Custom format validates alphabet and length, not statistical entropy.
//!
//! Witnessed admission (not legacy inbox auth or observation) lazily upgrades the matched
//! legacy key origin by CAS, bumping the owner revision. No proof is issued before durable
//! acknowledgement. Neither historic management receipts nor legacy inbox receipts are
//! migrated. A Session that resumes inheritance after a local shadow starts a new effective
//! incarnation without changing the project bearer or affecting siblings.
pub(super) mod admission;
mod model;
#[cfg(test)]
mod publication_tests;
mod store;
#[cfg(test)]
mod tests;

use super::{Manager, safe};
use microcrab_protocol::application::{Command, WireError};
use model::{Configuration, Layer, SpeakerChange, SpeakerProjection};
pub(crate) use model::{SpeakerCredential, SpeakerIdentity};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Read {
    #[serde(default)]
    project_id: Option<String>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SessionWrite {
    // Retain the legacy typed Request serialization (including null project_id)
    // so an exact replay of a previously committed Session operation still works.
    #[serde(default)]
    project_id: Option<String>,
    expected_revision: u64,
    operation_id: String,
    speakers: BTreeMap<String, SpeakerChange>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProjectWrite {
    project_id: String,
    expected_revision: u64,
    operation_id: String,
    speakers: BTreeMap<String, SpeakerChange>,
    enabled: bool,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DisabledReason {
    ProjectDisabled,
    NoProject,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reveal {
    #[serde(default)]
    project_id: Option<String>,
    identity: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Observe {
    #[serde(default)]
    project_id: Option<String>,
    operation_id: String,
}
#[derive(Clone, Serialize, Deserialize)]
struct View {
    #[serde(skip_serializing_if = "Option::is_none")]
    operation_id: Option<String>,
    /// One owner resource revision covers both defaults and sparse overrides.
    revision: u64,
    /// Missing only in pre-opt-in receipts. Historical evidence is not live state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    disabled_reason: Option<DisabledReason>,
    speakers: Vec<SpeakerProjection>,
    overrides: BTreeMap<String, model::SpeakerOverride>,
}
fn invalid(reason: &'static str) -> WireError {
    WireError::new("invalid_params", reason)
}
fn parse<T: serde::de::DeserializeOwned>(params: Value) -> Result<T, WireError> {
    serde_json::from_value(params).map_err(|error| {
        crate::route::error::json_error(
            "Speaker parameters must match the schema",
            "params",
            &error,
        )
    })
}
fn encode(value: impl Serialize) -> Result<Value, WireError> {
    serde_json::to_value(value)
        .map_err(|_| WireError::new("serialization_error", "Cannot encode external speakers"))
}
/// The caller must explicitly choose a non-writing read or a new witnessed admission.
#[derive(Clone, Copy)]
pub(crate) enum CredentialUse {
    Observe,
    Admit,
}

impl Manager {
    /// File Configuration can quarantine its writer while retaining a last-ACKed
    /// read cache after an ambiguous replace. That cache is not authority. Block
    /// model/inbox/reply/key actions until this Manager reopens durable storage.
    fn mark_external_messaging_uncertain(&self, owner: &str) {
        self.external_messaging_quarantine
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(owner.to_owned());
    }

    fn ensure_external_messaging_available(&self, owner: &str) -> Result<(), WireError> {
        if self
            .external_messaging_quarantine
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(owner)
        {
            return Err(WireError::new(
                "configuration_unavailable",
                "External messaging requires durable configuration recovery",
            ));
        }
        Ok(())
    }

    /// Caller holds the owner gate until Project removal is published.
    pub(super) async fn retire_project_external_messaging_locked(
        &self,
        owner: &str,
        project: &str,
    ) -> Result<(), WireError> {
        self.ensure_external_messaging_available(owner)?;
        let result = store::retire_project(self.configuration.as_ref(), owner, project).await;
        if result
            .as_ref()
            .is_err_and(|error| error.code == "outcome_unknown")
        {
            self.mark_external_messaging_uncertain(owner);
        }
        result
    }

    pub(super) fn speaker_gate(&self, owner: &str) -> std::sync::Arc<tokio::sync::Mutex<()>> {
        let mut gates = self
            .speaker_gates
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        gates.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = gates.get(owner).and_then(std::sync::Weak::upgrade) {
            return gate;
        }
        let gate = std::sync::Arc::new(tokio::sync::Mutex::new(()));
        gates.insert(owner.into(), std::sync::Arc::downgrade(&gate));
        gate
    }
    /// Call under the owner fence when a durable binding must be synchronized
    /// with an immediate enqueue. Fanout and activation use the same path.
    pub(super) async fn synchronize_external_speakers_locked(
        &self,
        owner: &str,
        project: Option<&str>,
        id: &str,
        session: &microcrab_session::Session,
    ) -> Result<(), WireError> {
        if !session.communication_identity().is_some_and(|identity| {
            identity.interaction
                == microcrab_session::state::communication::SessionInteraction::Persistent
        }) {
            return Ok(());
        }
        let (_, config) = store::load(self.configuration.as_ref(), owner).await?;
        let speakers = if config.enabled(project) {
            config.effective(project, Some(id))?
        } else {
            BTreeMap::new()
        };
        session
            .replace_external_speakers(
                speakers
                    .into_keys()
                    .map(
                        |identity| microcrab_session::state::communication::ExternalSpeaker {
                            identity,
                        },
                    )
                    .collect(),
            )
            .await
            .map_err(safe)
    }
    async fn synchronize_live_speakers(&self, owner: &str, toggled_project: Option<&str>) {
        let slots: Vec<_> = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect();
        for slot in slots {
            let active = slot.active.lock().await.clone();
            if let Some(managed) = active {
                let record = managed.record.lock().await.clone();
                if record.owner == owner
                    && !managed.closing.load(std::sync::atomic::Ordering::Acquire)
                {
                    let speaker = self.speaker_gate(owner).lock_owned().await;
                    let result = self
                        .synchronize_external_speakers_locked(
                            owner,
                            record.project_id.as_deref(),
                            &record.id,
                            &managed.session,
                        )
                        .await;
                    drop(speaker);
                    if let Err(error) = result {
                        tracing::warn!(session = %record.id, code = %error.code, "External messaging bindings require reconciliation");
                    }
                    if toggled_project.is_some() && record.project_id.as_deref() == toggled_project
                    {
                        if let Err(error) = managed.session.reconcile_model_catalog().await {
                            tracing::warn!(session = %record.id, code = %safe(error).code, "External messaging catalog requires reconciliation");
                        }
                    }
                }
            }
        }
    }
    /// Metadata-only lookup: reads durable ownership and configuration without activating a Session.
    #[cfg(test)]
    pub(crate) async fn effective_external_speakers(
        &self,
        owner: &str,
        session_id: &str,
    ) -> Result<Vec<SpeakerIdentity>, WireError> {
        let project = self.speaker_session_project(owner, session_id).await?;
        let (_, config) = store::load(self.configuration.as_ref(), owner).await?;
        Ok(config
            .effective(project.as_deref(), Some(session_id))?
            .into_iter()
            .map(|(identity, speaker)| SpeakerIdentity {
                identity,
                fingerprint: speaker.key.fingerprint(),
            })
            .collect())
    }

    #[cfg(test)]
    pub(crate) async fn authenticate_external_speaker(
        &self,
        owner: &str,
        session_id: &str,
        bearer: &str,
    ) -> Result<SpeakerIdentity, WireError> {
        let project = self.speaker_session_project(owner, session_id).await?;
        self.authenticate_external_speaker_at(owner, project.as_deref(), session_id, bearer)
            .await
    }

    pub(super) async fn authenticate_external_speaker_at(
        &self,
        owner: &str,
        project: Option<&str>,
        session_id: &str,
        bearer: &str,
    ) -> Result<SpeakerIdentity, WireError> {
        self.ensure_external_messaging_available(owner)?;
        let (_, config) = store::load(self.configuration.as_ref(), owner).await?;
        config.authenticate(project, session_id, bearer)
    }

    /// Resolve the owner from the exact Session index, then re-read that Session
    /// in its owner namespace under the same gate as credential mutations.
    pub(super) async fn lock_external_speaker_session(
        &self,
        session_id: &str,
    ) -> Result<
        (
            microcrab_storage::directory::SessionRecord,
            tokio::sync::OwnedMutexGuard<()>,
        ),
        WireError,
    > {
        use super::external_inbox::denied;
        if !crate::config::valid_id(session_id) {
            return Err(denied());
        }
        let owner = self
            .directory
            .session_owner(session_id)
            .await
            .map_err(safe)?
            .ok_or_else(denied)?;
        let gate = self.speaker_gate(&owner).lock_owned().await;
        let record = self
            .directory
            .get(&owner, session_id)
            .await
            .map_err(safe)?
            .ok_or_else(denied)?;
        if let Some(project) = record.project_id.as_deref() {
            let project = self
                .editable_project(&owner, project)
                .await
                .map_err(|error| {
                    if error.code == "not_found" {
                        denied()
                    } else {
                        error
                    }
                })?;
            if project.provisional {
                return Err(denied());
            }
        }
        Ok((record, gate))
    }

    /// Requires the owner speaker gate and the exact record from the canonical directory.
    pub(super) async fn external_speaker_credential_locked(
        &self,
        record: &microcrab_storage::directory::SessionRecord,
        bearer: &str,
        usage: CredentialUse,
    ) -> Result<Option<SpeakerCredential>, WireError> {
        self.ensure_external_messaging_available(&record.owner)?;
        let result = store::authenticate_witnessed(
            self.configuration.as_ref(),
            &record.owner,
            record.project_id.as_deref(),
            &record.id,
            bearer,
            matches!(usage, CredentialUse::Admit),
        )
        .await;
        if matches!(usage, CredentialUse::Admit)
            && result
                .as_ref()
                .is_err_and(|error| error.code == "storage_unavailable")
        {
            self.mark_external_messaging_uncertain(&record.owner);
        }
        result
    }

    async fn speaker_session_project(
        &self,
        owner: &str,
        id: &str,
    ) -> Result<Option<String>, WireError> {
        if !crate::config::valid_id(id) {
            return Err(super::unavailable());
        }
        let record = self
            .directory
            .get(owner, id)
            .await
            .map_err(safe)?
            .ok_or_else(|| {
                WireError::new(
                    "not_found",
                    "Session not found in authenticated owner namespace",
                )
            })?;
        if let Some(project) = record.project_id.as_deref() {
            self.editable_project(owner, project).await?;
        }
        Ok(record.project_id)
    }
}

fn operation_id(value: &str) -> Result<(), WireError> {
    if !uuid::Uuid::parse_str(value).is_ok_and(|id| id.to_string() == value) {
        return Err(invalid(
            "operation_id must be a canonical lowercase hyphenated UUID",
        ));
    }
    Ok(())
}
fn receipt<'a>(
    config: &'a Configuration,
    operation: &str,
    project: Option<&str>,
    session: Option<&str>,
) -> Result<Option<&'a model::Operation>, WireError> {
    let Some(receipt) = config.operations.get(operation) else {
        return Ok(None);
    };
    if receipt.project_id.as_deref() != project || receipt.session_id.as_deref() != session {
        return Err(WireError::new(
            "operation_conflict",
            "Speaker operation belongs to another resource",
        ));
    }
    Ok(Some(receipt))
}
fn snapshot(
    config: &Configuration,
    revision: u64,
    project: Option<&str>,
    session: Option<&str>,
    operation: Option<String>,
) -> Result<View, WireError> {
    Ok(View {
        operation_id: operation,
        revision,
        enabled: Some(config.enabled(project)),
        disabled_reason: (!config.enabled(project)).then_some(if project.is_some() {
            DisabledReason::ProjectDisabled
        } else {
            DisabledReason::NoProject
        }),
        speakers: config.project(project, session)?,
        overrides: config.overrides(project, session),
    })
}

pub(super) async fn command(
    manager: &Manager,
    owner: &str,
    command: &Command,
) -> Result<Value, WireError> {
    let write = command.method.ends_with(".set");
    let reveal = command.method.ends_with(".key");
    let observe = command.method.ends_with(".operation");
    let session_scope = command.method.starts_with("session.");
    let admission = manager.speaker_gate(owner).lock_owned().await;
    let (project, changes, expected, identity, operation, request_fingerprint, enabled) = if write {
        let fingerprint = |bytes: Vec<u8>| {
            use sha2::{Digest, Sha256};
            format!("{:x}", Sha256::digest(bytes))
        };
        if session_scope {
            let params: SessionWrite = parse(command.params.clone())?;
            operation_id(&params.operation_id)?;
            let bytes = serde_json::to_vec(&params)
                .map_err(|_| invalid("Cannot encode speaker mutation"))?;
            (
                params.project_id,
                Some(params.speakers),
                Some(params.expected_revision),
                None,
                Some(params.operation_id),
                Some(fingerprint(bytes)),
                None,
            )
        } else {
            let params: ProjectWrite = parse(command.params.clone())?;
            operation_id(&params.operation_id)?;
            let bytes = serde_json::to_vec(&params)
                .map_err(|_| invalid("Cannot encode speaker mutation"))?;
            (
                Some(params.project_id),
                Some(params.speakers),
                Some(params.expected_revision),
                None,
                Some(params.operation_id),
                Some(fingerprint(bytes)),
                Some(params.enabled),
            )
        }
    } else if reveal {
        let params: Reveal = parse(command.params.clone())?;
        (
            params.project_id,
            None,
            None,
            Some(params.identity),
            None,
            None,
            None,
        )
    } else if observe {
        let params: Observe = parse(command.params.clone())?;
        operation_id(&params.operation_id)?;
        (
            params.project_id,
            None,
            None,
            None,
            Some(params.operation_id),
            None,
            None,
        )
    } else {
        let params: Read = parse(command.params.clone())?;
        (params.project_id, None, None, None, None, None, None)
    };
    let session = if session_scope {
        if project.is_some() {
            return Err(invalid("Session speakers do not accept project_id"));
        }
        Some(
            command
                .session_id
                .as_deref()
                .ok_or_else(|| invalid("Session path required"))?,
        )
    } else {
        None
    };
    let project = if let Some(session) = session {
        manager.speaker_session_project(owner, session).await?
    } else {
        let project = project.ok_or_else(|| invalid("Project path required"))?;
        if !crate::config::valid_id(&project) {
            return Err(super::unavailable());
        }
        let record = manager.editable_project(owner, &project).await?;
        if record.provisional && enabled == Some(true) {
            return Err(invalid(
                "Attach this Project before enabling external messaging",
            ));
        }
        Some(project)
    };
    // Read-only reconciliation of a UUID may prove a committed operation; a
    // missing receipt never proves failure while this owner is quarantined.
    if !observe {
        manager.ensure_external_messaging_available(owner)?;
    }
    let (revision, mut config) = store::load(manager.configuration.as_ref(), owner).await?;
    if let Some(operation) = operation.as_deref() {
        if let Some(receipt) = receipt(&config, operation, project.as_deref(), session)? {
            if request_fingerprint
                .as_ref()
                .is_some_and(|fingerprint| fingerprint != &receipt.request_fingerprint)
            {
                return Err(WireError::new(
                    "operation_conflict",
                    "Speaker operation_id was already used with different parameters",
                ));
            }
            return encode(&receipt.result);
        }
        if observe {
            return Err(WireError::new(
                "operation_not_found",
                "Speaker operation has no committed receipt; this does not prove a pending write failed",
            ));
        }
    }
    if let Some(changes) = changes {
        if expected != Some(revision) {
            return Err(store::conflict());
        }
        let toggle = enabled.is_some_and(|enabled| enabled != config.enabled(project.as_deref()));
        if toggle && let Some(project) = project.as_deref() {
            manager.require_quiet_external_messaging(owner, project)?;
        }
        if config.operations.len() >= 4096 {
            return Err(WireError::new(
                "capacity",
                "External speaker operation receipt capacity reached",
            ));
        }
        let layer = Layer::from_changes(
            changes,
            session.is_some(),
            config.layer(project.as_deref(), session),
        )?;
        if let Some(session) = session {
            config.replace_session_layer(project.clone(), session.into(), layer);
        } else {
            let project = project
                .as_ref()
                .ok_or_else(|| invalid("Project path required"))?;
            config.replace_project_layer(project.clone(), layer);
            if enabled == Some(true) {
                config.enabled_projects.insert(project.clone());
            } else {
                config.enabled_projects.remove(project);
            }
        }
        config.validate()?;
        let operation = operation.ok_or_else(|| invalid("operation_id required"))?;
        let next = revision.checked_add(1).ok_or_else(store::conflict)?;
        let result = snapshot(
            &config,
            next,
            project.as_deref(),
            session,
            Some(operation.clone()),
        )?;
        config.operations.insert(
            operation.clone(),
            model::Operation {
                project_id: project.clone(),
                session_id: session.map(str::to_owned),
                request_fingerprint: request_fingerprint
                    .clone()
                    .ok_or_else(|| invalid("Mutation fingerprint required"))?,
                result: result.clone(),
            },
        );
        match store::save(
            manager.configuration.as_ref(),
            owner,
            revision,
            &config,
            &operation,
        )
        .await
        {
            Ok(_) => {
                drop(admission);
                // Acknowledging the durable CAS is independent of checkpointing
                // each live catalog. Fanout reloads the current configuration;
                // admission remains fail-closed if catalog apply cannot complete.
                manager
                    .synchronize_live_speakers(
                        owner,
                        toggle.then_some(project.as_deref()).flatten(),
                    )
                    .await;
                return encode(result);
            }
            Err(error) if error.code == "revision_conflict" => {
                // Observe a concurrent same-operation winner; never regenerate or replay uncertain writes.
                let (_, committed) = store::load(manager.configuration.as_ref(), owner).await?;
                if let Some(receipt) = receipt(&committed, &operation, project.as_deref(), session)?
                {
                    if request_fingerprint.as_ref() == Some(&receipt.request_fingerprint) {
                        return encode(&receipt.result);
                    }
                    return Err(WireError::new(
                        "operation_conflict",
                        "Speaker operation_id was already used with different parameters",
                    ));
                }
                return Err(error);
            }
            Err(error) => {
                if error.code == "outcome_unknown" {
                    manager.mark_external_messaging_uncertain(owner);
                }
                return Err(error);
            }
        }
    }
    if let Some(identity) = identity {
        if !config.enabled(project.as_deref()) {
            return Err(WireError::new(
                "permission_denied",
                "External messaging is disabled for this resource; keys cannot be revealed",
            ));
        }
        model::validate_identity(&identity)?;
        let effective = config.effective(project.as_deref(), session)?;
        let speaker = effective.get(&identity).ok_or_else(|| {
            WireError::new("not_found", "External speaker is not effectively bound")
        })?;
        return encode(
            serde_json::json!({"identity":identity,"fingerprint":speaker.key.fingerprint(),"key":speaker.key.expose()}),
        );
    }
    encode(snapshot(
        &config,
        revision,
        project.as_deref(),
        session,
        None,
    )?)
}
