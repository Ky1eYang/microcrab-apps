//! Future grants publish through one owner-scoped targeted durable transaction.
//! The authenticated owner is captured by assembly, never supplied by a proposal.
use async_trait::async_trait;
use microcrab_permission::PolicySnapshot;
use microcrab_session::policy::{FutureGrantPersistence, PermissionError};
use microcrab_storage::configuration::Configuration;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
#[derive(Clone)]
pub(crate) struct Grants {
    pub(crate) owner: String,
    pub(crate) storage: Arc<dyn Configuration>,
    pub(crate) directory: Arc<dyn microcrab_storage::Directory>,
    pub(crate) manager: std::sync::Weak<super::super::Manager>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurablePolicy {
    controller_id: u64,
    policy: PolicySnapshot,
}
fn write_failure(error: microcrab_session::Error) -> PermissionError {
    if matches!(&error,microcrab_session::Error::Protocol(reason) if matches!(reason.public_reason(), Some("configuration revision conflict; reload before saving" | "creator directory revision or ownership conflict")))
    {
        PermissionError::StalePolicy
    } else {
        PermissionError::PersistenceUncertain(error.diagnostic_reason())
    }
}
fn failure(error: impl std::fmt::Display) -> PermissionError {
    PermissionError::InvalidConfiguration(format!(
        "future grant durable publication failed: {}",
        microcrab_protocol::diagnostic::sanitize(&error.to_string(), 1800)
    ))
}
#[async_trait]
impl FutureGrantPersistence for Grants {
    async fn persist(
        &self,
        owning_session_id: &str,
        controller_id: u64,
        expected: &PolicySnapshot,
        next: &PolicySnapshot,
    ) -> Result<(), PermissionError> {
        if owning_session_id.is_empty()
            || next.revision
                != expected
                    .revision
                    .checked_add(1)
                    .ok_or(PermissionError::StalePolicy)?
            || next.config.overrides.len() != expected.config.overrides.len() + 1
            || !next
                .config
                .overrides
                .starts_with(&expected.config.overrides)
        {
            return Err(PermissionError::StalePolicy);
        }
        let mut unchanged = next.config.clone();
        unchanged.overrides = expected.config.overrides.clone();
        if unchanged != expected.config {
            return Err(PermissionError::StalePolicy);
        }
        if let Some(mut record) = self
            .directory
            .get(&self.owner, owning_session_id)
            .await
            .map_err(failure)?
        {
            if record.permissions != expected.config.overrides
                || record.initialization != expected.config.initialization
                || record.review != expected.config.review
                || record.review_model != expected.config.review_model
            {
                return Err(PermissionError::StalePolicy);
            }
            let revision = record.revision;
            record.permissions = next.config.overrides.clone();
            record.revision = revision
                .checked_add(1)
                .ok_or(PermissionError::StalePolicy)?;
            record.updated_at_ms = super::super::now().max(record.updated_at_ms);
            let record = self
                .directory
                .replace(&self.owner, revision, record)
                .await
                .map_err(write_failure)?;
            if let Some(manager) = self.manager.upgrade()
                && let Some(managed) = super::active(&manager, owning_session_id).await
            {
                let mut cached = managed.record.lock().await;
                if cached.owner != self.owner || cached.id != owning_session_id {
                    return Err(PermissionError::PersistenceUncertain("Grant directory publication committed, but the active cache owner or Session witness differs; reconcile the owning Session without updating that cache".into()));
                }
                if cached.revision <= record.revision {
                    *cached = record.clone();
                }
                let mut session = super::super::view::session(
                    &manager,
                    &cached,
                    &manager.config.provider.model,
                    if managed.session.inbox_snapshot().active {
                        "running"
                    } else {
                        "idle"
                    },
                );
                let _ = super::super::view::project_live_session(&mut session, &managed.session);
                super::super::model::project_route(&managed, &cached, &mut session);
                managed.hub.session(session);
            }
            return Ok(());
        }
        let record = self
            .storage
            .get(&self.owner, "session_policy", owning_session_id)
            .await
            .map_err(failure)?;
        if let Some(value) = record.as_ref().and_then(|r| r.value.clone()) {
            let stored: DurablePolicy = serde_json::from_value(value).map_err(failure)?;
            // A recovered controller has a new process identity. Its policy must
            // first be restored from the saved grant; unrelated edits are never rebased.
            if stored.policy.config != expected.config {
                return Err(PermissionError::StalePolicy);
            }
            if stored.controller_id == controller_id && stored.policy.revision != expected.revision
            {
                return Err(PermissionError::StalePolicy);
            }
        }
        self.storage
            .compare_exchange(
                &self.owner,
                "session_policy",
                owning_session_id,
                record.as_ref().map_or(0, |r| r.revision),
                Some(
                    serde_json::to_value(DurablePolicy {
                        controller_id,
                        policy: next.clone(),
                    })
                    .map_err(failure)?,
                ),
            )
            .await
            .map_err(write_failure)?;
        Ok(())
    }
}
#[async_trait]
impl microcrab_creator::delegate::ChildConfiguration for Grants {
    async fn apply(
        &self,
        child: &microcrab_session::Session,
    ) -> Result<(), microcrab_session::Error> {
        self.restore(child).await
    }
}
impl Grants {
    /// Load only the targeted owner Session. Controller identity is a live witness,
    /// never copied from a previous process or from its parent transport Session.
    pub(crate) async fn restore(
        &self,
        session: &microcrab_session::Session,
    ) -> Result<(), microcrab_session::Error> {
        let id = session.process_control().session_id().to_owned();
        let root = self.directory.get(&self.owner, &id).await?;
        let saved = if let Some(root) = root {
            Some(root.permissions)
        } else {
            self.storage
                .get(&self.owner, "session_policy", &id)
                .await?
                .and_then(|record| record.value)
                .map(serde_json::from_value::<DurablePolicy>)
                .transpose()?
                .map(|saved| saved.policy.config.overrides)
        };
        if let Some(overrides) = saved {
            let controller = session.permission_controller();
            let current = controller
                .snapshot()
                .map_err(|e| microcrab_session::Error::Config(e.to_string().into()))?;
            if current.config.overrides != overrides {
                let mut next = current.config.clone();
                next.overrides = overrides;
                session
                    .replace_permissions(current.revision, next)
                    .map_err(|e| microcrab_session::Error::Config(e.to_string().into()))?;
            }
        }
        session
            .permission_controller()
            .set_future_grant_persistence(Arc::new(self.clone()))
            .map_err(|e| microcrab_session::Error::Config(e.to_string().into()))?;
        Ok(())
    }
}
