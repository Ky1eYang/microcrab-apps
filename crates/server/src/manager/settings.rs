//! Durable defaults and explicit Session application. Saves never mutate other
//! live Sessions; desired/applied revisions remain separate on a safe deferral.
pub(crate) mod grants;
mod variables;
use super::{Managed, Manager, safe};
use crate::config::settings::{SettingsOverrides, limits};
use microcrab_protocol::application::{Command, WireError};
use microcrab_session::state::reference::markdown::ReferenceSource;
use microcrab_storage::configuration::ConfigurationRecord;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Read {
    #[serde(default)]
    project_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Write {
    expected_revision: u64,
    overrides: SettingsOverrides,
    #[serde(default)]
    project_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReferencesWrite {
    project_id: String,
    expected_revision: u64,
    sources: Vec<ReferenceSource>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Refresh {
    project_revision: u64,
}
fn configuration_error(error: microcrab_session::Error) -> WireError {
    if matches!(&error,microcrab_session::Error::Protocol(reason) if reason.public_reason() == Some("configuration revision conflict; reload before saving"))
    {
        WireError::new("revision_conflict", error.diagnostic_reason())
    } else {
        safe(error)
    }
}
fn invalid(message: impl Into<String>) -> WireError {
    WireError::new("invalid_params", message)
}
fn parse<T: serde::de::DeserializeOwned>(params: Value) -> Result<T, WireError> {
    serde_json::from_value(params)
        .map_err(|e| invalid(microcrab_protocol::diagnostic::json_reason(&e)))
}
fn overrides(record: Option<&ConfigurationRecord>) -> Result<SettingsOverrides, WireError> {
    record
        .and_then(|r| r.value.clone())
        .map(serde_json::from_value)
        .transpose()
        .map(|v| v.unwrap_or_default())
        .map_err(|_| {
            WireError::new(
                "configuration_unavailable",
                "Stored settings schema cannot be decoded",
            )
        })
}
impl Manager {
    pub(super) async fn effective_settings(
        &self,
        owner: &str,
        project: Option<&str>,
        session: Option<&str>,
    ) -> Result<(SettingsOverrides, BTreeMap<String, String>), WireError> {
        let mut result = self.config.session_defaults.clone();
        let mut sources = BTreeMap::new();
        for key in serde_json::to_value(crate::config::settings::default_limits())
            .map_err(|_| invalid("Cannot encode limits"))?
            .as_object()
            .expect("Limits is object")
            .keys()
        {
            sources.insert(format!("limits.{key}"), "admin".into());
        }
        for key in [
            "model_selection",
            "agent",
            "mode",
            "permissions",
            "initialization",
            "review",
            "review_model",
            "auto_compaction",
            "auto_naming",
        ] {
            sources.insert(key.into(), "admin".into());
        }
        let mut records = vec![(
            "user",
            self.configuration
                .get(owner, "settings", "user")
                .await
                .map_err(configuration_error)?,
        )];
        if let Some(project) = project {
            records.push((
                "project",
                self.configuration
                    .get(owner, "project_settings", project)
                    .await
                    .map_err(configuration_error)?,
            ));
        }
        if let Some(session) = session {
            records.push((
                "session",
                self.configuration
                    .get(owner, "session_settings", session)
                    .await
                    .map_err(configuration_error)?,
            ));
        }
        for (source, record) in records {
            let value = overrides(record.as_ref())?;
            for key in value.limits.keys() {
                sources.insert(format!("limits.{key}"), source.into());
            }
            let json =
                serde_json::to_value(&value).map_err(|_| invalid("Cannot encode settings"))?;
            for (key, _) in json.as_object().expect("Settings is object") {
                if key != "limits" {
                    sources.insert(key.clone(), source.into());
                }
            }
            result.overlay(&value);
        }
        result
            .validate(&self.config.limit_ceilings)
            .map_err(invalid)?;
        Ok((result, sources))
    }
}
fn values(manager: &Manager, settings: &SettingsOverrides) -> Result<Value, WireError> {
    let mut value =
        serde_json::to_value(settings).map_err(|_| invalid("Cannot encode settings"))?;
    value["agent"] = json!(settings.agent);
    value["mode"] = json!(settings.mode);
    value["review_model"] = json!(settings.review_model.clone().flatten());
    value["limits"] =
        json!(limits(&settings.limits, &manager.config.limit_ceilings).map_err(invalid)?);
    if settings.model_selection.is_none() {
        value["model_selection"] = json!(super::model::default_selection(&manager.config));
    }
    if settings.review.is_none() {
        value["review"] = json!(true);
    }
    if settings.initialization.is_none() {
        value["initialization"] = json!(microcrab_permission::Action::Allow);
    }
    if settings.permissions.is_none() {
        value["permissions"] = json!([]);
    }
    if settings.auto_compaction.is_none() {
        value["auto_compaction"] = json!(manager.config.auto_compaction);
    }
    if settings.auto_naming.is_none() {
        value["auto_naming"] = json!(manager.config.auto_rename);
    }
    Ok(value)
}
async fn active(manager: &Manager, id: &str) -> Option<Arc<Managed>> {
    let slot = manager
        .registry
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(id)
        .cloned();
    if let Some(slot) = slot {
        slot.active.lock().await.clone()
    } else {
        None
    }
}
async fn session_record(
    manager: &Manager,
    owner: &str,
    id: &str,
) -> Result<microcrab_storage::SessionRecord, WireError> {
    manager
        .directory
        .get(owner, id)
        .await
        .map_err(configuration_error)?
        .ok_or_else(|| {
            WireError::new(
                "not_found",
                "Session not found in authenticated owner namespace",
            )
        })
}
pub(super) async fn command(
    manager: &Manager,
    owner: &str,
    command: &Command,
) -> Result<Value, WireError> {
    let method = command.method.as_str();
    if method.starts_with("project.references.") {
        let (project, sources, expected) = if method.ends_with(".set") {
            let p: ReferencesWrite = parse(command.params.clone())?;
            (p.project_id, Some(p.sources), Some(p.expected_revision))
        } else {
            let p: Read = parse(command.params.clone())?;
            (
                p.project_id.ok_or_else(|| invalid("project_id required"))?,
                None,
                None,
            )
        };
        manager.editable_project(owner, &project).await?;
        let record = if let Some(sources) = sources {
            validate_references(&sources)?;
            Some(
                manager
                    .configuration
                    .compare_exchange(
                        owner,
                        "project_references",
                        &project,
                        expected.expect("write has revision"),
                        Some(json!(sources)),
                    )
                    .await
                    .map_err(configuration_error)?,
            )
        } else {
            manager
                .configuration
                .get(owner, "project_references", &project)
                .await
                .map_err(configuration_error)?
        };
        return Ok(
            json!({"revision":record.as_ref().map_or(0,|r|r.revision),"sources":record.and_then(|r|r.value).unwrap_or_else(||json!([]))}),
        );
    }
    if method == "session.references.refresh" {
        let p: Refresh = parse(command.params.clone())?;
        let id = command
            .session_id
            .as_deref()
            .ok_or_else(|| invalid("Session path required"))?;
        let record = session_record(manager, owner, id).await?;
        let project = record
            .project_id
            .ok_or_else(|| invalid("Project references require a Project Session"))?;
        manager.editable_project(owner, &project).await?;
        let refs = manager
            .configuration
            .get(owner, "project_references", &project)
            .await
            .map_err(configuration_error)?;
        if refs.as_ref().map_or(0, |r| r.revision) != p.project_revision {
            return Err(WireError::new(
                "revision_conflict",
                "Project references changed; reload before refresh",
            ));
        }
        let sources: Vec<ReferenceSource> =
            parse(refs.and_then(|r| r.value).unwrap_or_else(|| json!([])))?;
        validate_references(&sources)?;
        let managed = active(manager, id).await.ok_or_else(|| {
            WireError::new(
                "closed",
                "Open the Session explicitly before refreshing references",
            )
        })?;
        let _gate = managed.commands.lock().await;
        ensure_mutable(&managed)?;
        managed
            .session
            .replace_markdown_references(
                "references".into(),
                sources.clone(),
                microcrab_session::state::reference::markdown::ReferenceLimits::default(),
            )
            .await
            .map_err(configuration_error)?;
        let publication = async {
            let saved = manager
                .configuration
                .get(owner, "session_references", id)
                .await
                .map_err(configuration_error)?;
            manager
                .configuration
                .compare_exchange(
                    owner,
                    "session_references",
                    id,
                    saved.as_ref().map_or(0, |r| r.revision),
                    Some(json!(sources)),
                )
                .await
                .map_err(configuration_error)?;
            Ok::<(), WireError>(())
        }
        .await;
        if let Err(error) = publication {
            return Err(quarantine(
                &managed,
                p.project_revision,
                &json!({}),
                "reference source metadata publication",
                error,
            )
            .await);
        }
        return Ok(
            json!({"project_revision":p.project_revision,"applied":true,"boundary":"next_state_patch"}),
        );
    }
    if matches!(
        method,
        "session.variables.patch" | "session.variables.operation"
    ) {
        return variables::command(manager, owner, command).await;
    }
    let is_session = method.starts_with("session.");
    let is_project = method.starts_with("project.");
    let write = method.ends_with(".set");
    let (project_id, write) = if write {
        let p: Write = parse(command.params.clone())?;
        (p.project_id, Some((p.expected_revision, p.overrides)))
    } else {
        let p: Read = parse(command.params.clone())?;
        (p.project_id, None)
    };
    let id = if is_session {
        Some(
            command
                .session_id
                .as_deref()
                .ok_or_else(|| invalid("Session path required"))?,
        )
    } else {
        None
    };
    let record = if let Some(id) = id {
        Some(session_record(manager, owner, id).await?)
    } else {
        None
    };
    let project = if is_project {
        Some(
            project_id
                .as_deref()
                .ok_or_else(|| invalid("project_id required"))?,
        )
    } else {
        record.as_ref().and_then(|r| r.project_id.as_deref())
    };
    if let Some(project) = project {
        manager.editable_project(owner, project).await?;
    }
    let (kind, target) = if let Some(id) = id {
        ("session_settings", id)
    } else if is_project {
        ("project_settings", project.expect("project supplied"))
    } else {
        ("settings", "user")
    };
    if write.is_some() {
        if let Some(id) = id {
            if let Some(managed) = active(manager, id).await {
                ensure_mutable(&managed)?;
            }
        }
    }
    let mut application = "defaults_saved";
    let mut reason = None;
    let stored = if let Some((expected, next)) = write {
        next.validate(&manager.config.limit_ceilings)
            .map_err(invalid)?;
        let mut candidate = if is_project {
            manager.effective_settings(owner, None, None).await?.0
        } else if is_session {
            manager.effective_settings(owner, project, None).await?.0
        } else {
            manager.config.session_defaults.clone()
        };
        candidate.overlay(&next);
        let effective = values(manager, &candidate)?;
        if let Some(selection) = &candidate.model_selection {
            super::providers::resolve(manager, owner, selection).await?;
        }
        let applying = if let Some(id) = id {
            active(manager, id).await
        } else {
            None
        };
        let _application_gate = if let Some(managed) = applying.as_ref() {
            let gate = managed.commands.lock().await;
            ensure_mutable(managed)?;
            Some(gate)
        } else {
            None
        };
        let stored = manager
            .configuration
            .compare_exchange(owner, kind, target, expected, Some(json!(next)))
            .await
            .map_err(configuration_error)?;
        if let Some(id) = id {
            if let Some(managed) = applying.as_ref() {
                match apply(manager, managed, &candidate, &effective).await {
                    Ok(()) => {
                        let publication = async {
                            let old = manager
                                .configuration
                                .get(owner, "session_settings_applied", id)
                                .await
                                .map_err(configuration_error)?;
                            manager
                                .configuration
                                .compare_exchange(
                                    owner,
                                    "session_settings_applied",
                                    id,
                                    old.as_ref().map_or(0, |r| r.revision),
                                    Some(json!({"revision":stored.revision})),
                                )
                                .await
                                .map_err(configuration_error)?;
                            Ok::<(), WireError>(())
                        }
                        .await;
                        if let Err(error) = publication {
                            return Err(quarantine(
                                &managed,
                                stored.revision,
                                &effective,
                                "applied revision publication",
                                error,
                            )
                            .await);
                        }
                        application = "applied";
                    }
                    Err(failure) => {
                        if failure.live_changed
                            || managed.faulted.load(std::sync::atomic::Ordering::Acquire)
                        {
                            return Err(quarantine(
                                &managed,
                                stored.revision,
                                &effective,
                                "live application or metadata publication",
                                failure.error,
                            )
                            .await);
                        }
                        application = "deferred";
                        reason = Some(failure.error.message);
                    }
                }
            } else {
                application = "deferred";
                reason = Some("Session is closed; open it explicitly and save Session settings again to apply".into());
            }
        }
        Some(stored)
    } else {
        manager
            .configuration
            .get(owner, kind, target)
            .await
            .map_err(configuration_error)?
    };
    let (effective, sources) = manager.effective_settings(owner, project, id).await?;
    let desired = values(manager, &effective)?;
    let mut actual = if let Some(id) = id {
        let saved = manager
            .configuration
            .get(owner, "session_effective", id)
            .await
            .map_err(configuration_error)?;
        values(manager, &overrides(saved.as_ref())?)?
    } else {
        desired.clone()
    };
    if let Some(record) = record.as_ref() {
        actual["agent"] = json!(record.agent);
        actual["mode"] = json!(record.mode);
        actual["permissions"] = json!(record.permissions);
        actual["initialization"] = json!(record.initialization);
        actual["review"] = json!(record.review);
        actual["review_model"] = json!(record.review_model);
        actual["model_selection"] = json!(record.model_selection);
        actual["auto_compaction"] = json!(
            record
                .auto_compaction
                .clone()
                .unwrap_or_else(|| manager.config.auto_compaction.clone())
        );
    }
    let applied = if let Some(id) = id {
        let applied = manager
            .configuration
            .get(owner, "session_settings_applied", id)
            .await
            .map_err(configuration_error)?
            .and_then(|r| r.value)
            .and_then(|v| v["revision"].as_u64())
            .unwrap_or(0);
        if let Some(managed) = active(manager, id).await {
            if managed.faulted.load(std::sync::atomic::Ordering::Acquire) {
                application = "deferred";
                reason=Some("Session is quarantined after a configuration or persistence failure. These are current observations, not an applied acknowledgement. Close and reopen explicitly, inspect current and desired settings, and submit a new revisioned edit; do not replay the previous mutation.".into());
            }
            actual["limits"] = json!(managed.session.state().limits);
            actual["mode"] = json!(managed.session.state().selected_mode);
            actual["auto_naming"] = json!(
                managed
                    .auto_naming
                    .load(std::sync::atomic::Ordering::Acquire)
            );
            let live_record = managed.record.lock().await;
            actual["agent"] = json!(live_record.agent);
            actual["auto_compaction"] =
                json!(managed.session.auto_compaction_policy().unwrap_or_default());
            drop(live_record);
            let snapshot = managed
                .session
                .permission_controller()
                .snapshot()
                .map_err(|_| WireError::new("revision_conflict", "Permission changed"))?;
            actual["permissions"] = json!(snapshot.config.overrides);
            actual["initialization"] = json!(snapshot.config.initialization);
            actual["review"] = json!(snapshot.config.review);
            actual["review_model"] = json!(snapshot.config.review_model);
            actual["model_selection"] = json!(managed.routing.selection());
        }
        applied
    } else {
        stored.as_ref().map_or(0, |r| r.revision)
    };
    Ok(
        json!({"revision":stored.as_ref().map_or(0,|r|r.revision),"overrides":overrides(stored.as_ref())?,"effective":actual,"desired_effective":desired,"sources":sources,"constraints":{"limits":manager.config.limit_ceilings},"applied_revision":applied,"application":application,"reason":reason}),
    )
}
struct ApplyFailure {
    error: WireError,
    live_changed: bool,
}
impl From<WireError> for ApplyFailure {
    fn from(error: WireError) -> Self {
        Self {
            error,
            live_changed: false,
        }
    }
}
fn ensure_mutable(managed: &Managed) -> Result<(), WireError> {
    if managed.faulted.load(std::sync::atomic::Ordering::Acquire)
        || managed.closing.load(std::sync::atomic::Ordering::Acquire)
    {
        return Err(WireError::new(
            "settings_reconciliation_required",
            "Session configuration is quarantined or closing; close and reopen the Session explicitly, inspect current and desired settings, then submit a new revisioned edit. Do not replay the previous mutation.",
        ));
    }
    Ok(())
}
async fn quarantine(
    managed: &Managed,
    desired_revision: u64,
    _desired: &Value,
    phase: &str,
    cause: WireError,
) -> WireError {
    managed
        .faulted
        .store(true, std::sync::atomic::Ordering::Release);
    managed
        .session
        .interrupt_with_reason(microcrab_session::InterruptReason::StateReplaced);
    let cause_code = microcrab_protocol::diagnostic::sanitize(&cause.code, 128);
    let cause_message = microcrab_protocol::diagnostic::sanitize(&cause.message, 1800);
    let mut error = WireError::new(
        "settings_reconciliation_required",
        format!(
            "Session settings may already be partially applied; {phase} failed ({cause_code}): {cause_message}. Session quarantined: inspect current and desired values using GET Session settings, close and reopen explicitly, and submit a new revisioned edit instead of replaying this mutation."
        ),
    );
    let affected: &[&str] = if phase == "reference source metadata publication" {
        &["references"]
    } else {
        &[
            "limits",
            "auto_compaction",
            "model_selection",
            "permissions",
            "mode",
            "auto_naming",
        ]
    };
    error.details = Some(Box::new(
        json!({"reconciliation_required":true,"desired_revision":desired_revision,"phase":phase,"cause":{"code":cause_code,"message":cause_message},"potentially_changed_owners":affected,"recovery":"read_session_settings_and_state_then_explicit_close_reopen_no_replay"}),
    ));
    error
}
async fn apply(
    manager: &Manager,
    managed: &Managed,
    settings: &SettingsOverrides,
    effective: &Value,
) -> Result<(), ApplyFailure> {
    ensure_mutable(managed)?;
    let current = managed.record.lock().await.clone();
    if settings
        .agent
        .as_ref()
        .is_some_and(|agent| current.agent.as_ref() != Some(agent))
    {
        return Err(WireError::new(
            "configuration_rebuild_required",
            "Changing Agent identity requires an explicit environment rebuild",
        )
        .into());
    }
    if let Some(mode) = &settings.mode {
        managed
            .session
            .validate_mode(mode)
            .map_err(configuration_error)?;
    }
    let before = managed.session.state().limits;
    let desired_limits =
        limits(&settings.limits, &manager.config.limit_ceilings).map_err(invalid)?;
    if let Err(error) = managed.session.apply_limits(desired_limits).await {
        return Err(ApplyFailure {
            error: configuration_error(error),
            live_changed: managed.session.state().limits != before,
        });
    }
    apply_after_limits(manager, managed, settings, effective)
        .await
        .map_err(|error| ApplyFailure {
            error,
            live_changed: true,
        })
}
async fn apply_after_limits(
    manager: &Manager,
    managed: &Managed,
    settings: &SettingsOverrides,
    effective: &Value,
) -> Result<(), WireError> {
    let policy = settings
        .auto_compaction
        .clone()
        .unwrap_or_else(|| manager.config.auto_compaction.clone());
    managed
        .session
        .set_auto_compaction(policy.clone())
        .await
        .map_err(configuration_error)?;
    let mut cached = managed.record.lock().await;
    let revision = cached.revision;
    let mut next = cached.clone();
    next.auto_compaction = Some(policy);
    next.revision = revision
        .checked_add(1)
        .ok_or_else(|| invalid("Session revision exhausted"))?;
    next.updated_at_ms = super::now().max(next.updated_at_ms);
    *cached = manager
        .directory
        .replace(&cached.owner, revision, next)
        .await
        .map_err(configuration_error)?;
    drop(cached);
    let mut record = managed.record.lock().await.clone();
    if let Some(selection) = &settings.model_selection {
        super::model::select(manager, managed, record.revision, selection.clone()).await?;
        record = managed.record.lock().await.clone();
    }
    let command = Command {
        v: 1,
        kind: microcrab_protocol::application::CommandKind::Command,
        request_id: uuid::Uuid::new_v4().to_string(),
        method: "permission.set".into(),
        session_id: Some(record.id),
        params: json!({"expected_revision":managed.session.permission_controller().snapshot().map_err(|_|WireError::new("revision_conflict","Permission changed"))?.revision,"overrides":effective["permissions"],"initialization":effective["initialization"],"review":effective["review"],"review_model":effective["review_model"]}),
    };
    super::commands::permission(manager, managed, &command).await?;
    if let Some(mode) = &settings.mode {
        managed
            .session
            .validate_mode(mode)
            .map_err(configuration_error)?;
        let mut cached = managed.record.lock().await;
        let revision = cached.revision;
        let mut next = cached.clone();
        next.mode = Some(mode.clone());
        next.revision = revision
            .checked_add(1)
            .ok_or_else(|| invalid("Session revision exhausted"))?;
        next.updated_at_ms = super::now().max(next.updated_at_ms);
        *cached = manager
            .directory
            .replace(&cached.owner, revision, next)
            .await
            .map_err(configuration_error)?;
        drop(cached);
        managed
            .session
            .select_mode(mode.clone())
            .await
            .map_err(configuration_error)?;
    }
    managed.auto_naming.store(
        settings.auto_naming.unwrap_or(manager.config.auto_rename),
        std::sync::atomic::Ordering::Release,
    );
    let record = managed.record.lock().await.clone();
    let saved = manager
        .configuration
        .get(&record.owner, "session_effective", &record.id)
        .await
        .map_err(configuration_error)?;
    manager
        .configuration
        .compare_exchange(
            &record.owner,
            "session_effective",
            &record.id,
            saved.as_ref().map_or(0, |r| r.revision),
            Some(json!(settings)),
        )
        .await
        .map_err(configuration_error)?;
    Ok(())
}
fn validate_references(sources: &[ReferenceSource]) -> Result<(), WireError> {
    if sources.len() > 256 {
        return Err(invalid("Reference sources exceed 256 entries"));
    }
    for source in sources {
        match source {
            ReferenceSource::HostPath { path } | ReferenceSource::HostGlob { root: path, .. } => {
                if path.as_os_str().is_empty()
                    || path.to_string_lossy().len() > 4096
                    || path.is_absolute()
                    || path.components().any(|c| {
                        matches!(
                            c,
                            std::path::Component::ParentDir | std::path::Component::Prefix(_)
                        )
                    })
                {
                    return Err(invalid(
                        "Reference paths must stay relative to the injected Host workspace without parent traversal",
                    ));
                }
                if let ReferenceSource::HostGlob { pattern, .. } = source
                    && (pattern.is_empty() || pattern.len() > 4096 || pattern.contains(".."))
                {
                    return Err(invalid(
                        "Reference glob must be nonempty, bounded and exclude parent traversal",
                    ));
                }
            }
            ReferenceSource::Url { url } | ReferenceSource::DirectoryUrl { url } => {
                let parsed =
                    url::Url::parse(url).map_err(|_| invalid("Reference URL is malformed"))?;
                if url.len() > 8192
                    || !matches!(parsed.scheme(), "http" | "https")
                    || parsed.host_str().is_none()
                    || !parsed.username().is_empty()
                    || parsed.password().is_some()
                    || parsed.query().is_some()
                    || parsed.fragment().is_some()
                {
                    return Err(invalid(
                        "Reference URL requires bounded HTTP(S) with host and no credentials/query/fragment",
                    ));
                }
            }
        }
    }
    Ok(())
}
