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
mod model;
#[cfg(test)]
mod publication_tests;
mod store;
#[cfg(test)]
mod tests;

use super::{Manager, safe};
use microcrab_protocol::application::{Command, WireError};
pub(crate) use model::SpeakerIdentity;
use model::{Configuration, Layer, SpeakerChange, SpeakerProjection};
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
struct Write {
    #[serde(default)]
    project_id: Option<String>,
    expected_revision: u64,
    operation_id: String,
    speakers: BTreeMap<String, SpeakerChange>,
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
impl Manager {
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
    pub(super) async fn synchronize_external_speakers(
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
        session
            .replace_external_speakers(
                config
                    .effective(project, Some(id))?
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
    async fn synchronize_live_speakers(&self, owner: &str) -> Result<(), WireError> {
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
                    self.synchronize_external_speakers(
                        owner,
                        record.project_id.as_deref(),
                        &record.id,
                        &managed.session,
                    )
                    .await?;
                }
            }
        }
        Ok(())
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

    /// The inbox adapter supplies the exact target's owner; bearer data never chooses an owner or identity.
    /// Re-evaluate on every append: disabled, overridden and revoked keys fail immediately.
    pub(crate) async fn authenticate_external_speaker(
        &self,
        owner: &str,
        session_id: &str,
        bearer: &str,
    ) -> Result<SpeakerIdentity, WireError> {
        let project = self.speaker_session_project(owner, session_id).await?;
        let (_, config) = store::load(self.configuration.as_ref(), owner).await?;
        config.authenticate(project.as_deref(), session_id, bearer)
    }

    async fn speaker_session_project(
        &self,
        owner: &str,
        id: &str,
    ) -> Result<Option<String>, WireError> {
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
    let _admission = if write {
        Some(manager.speaker_gate(owner).lock_owned().await)
    } else {
        None
    };
    let reveal = command.method.ends_with(".key");
    let observe = command.method.ends_with(".operation");
    let (project, changes, expected, identity, operation, request_fingerprint) = if write {
        use sha2::{Digest, Sha256};
        let params: Write = parse(command.params.clone())?;
        operation_id(&params.operation_id)?;
        let bytes =
            serde_json::to_vec(&params).map_err(|_| invalid("Cannot encode speaker mutation"))?;
        let fingerprint: String = Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        (
            params.project_id,
            Some(params.speakers),
            Some(params.expected_revision),
            None,
            Some(params.operation_id),
            Some(fingerprint),
        )
    } else if reveal {
        let params: Reveal = parse(command.params.clone())?;
        (
            params.project_id,
            None,
            None,
            Some(params.identity),
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
        )
    } else {
        let params: Read = parse(command.params.clone())?;
        (params.project_id, None, None, None, None, None)
    };
    let session = if command.method.starts_with("session.") {
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
        manager.editable_project(owner, &project).await?;
        Some(project)
    };
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
            config.sessions.insert(
                session.into(),
                model::SessionLayer {
                    project_id: project.clone(),
                    speakers: layer,
                },
            );
        } else {
            config.projects.insert(
                project
                    .clone()
                    .ok_or_else(|| invalid("Project path required"))?,
                layer,
            );
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
                if let Err(cause) = manager.synchronize_live_speakers(owner).await {
                    let mut error = WireError::new(
                        "outcome_unknown",
                        "External speakers were saved; live projection requires reconciliation",
                    );
                    error.details = Some(Box::new(
                        serde_json::json!({"operation_id":operation,"cause":cause}),
                    ));
                    return Err(error);
                }
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
            Err(error) => return Err(error),
        }
    }
    if let Some(identity) = identity {
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
