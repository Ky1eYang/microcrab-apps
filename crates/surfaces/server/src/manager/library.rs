//! Server-owned Personal Content Library adapter and Session mount synchronization.
//!
//! `PersonalLibrary` stores owner-isolated blobs and shortcuts in persistent
//! Server storage (`FilePersonalLibrary` / `SqlitePersonalLibrary`).
//! `SessionLibraryAdapter` exposes the session's currently mounted slice to Core.
use super::{Manager, safe, settings::active, unavailable};
use crate::auth::Principal;
use async_trait::async_trait;
use microcrab_host_api::Host;
use microcrab_protocol::application::WireError;
use microcrab_session::{
    Error,
    ReferenceLibrary,
    admission::image::{image_type, validate_image},
    budget::LimitKind,
    state::reference::{
        CreateLibraryObject, LibraryObjectSummary, LibraryQuota, LibrarySource,
        MarkdownReferences, Reference, ReferenceContent, ReferencePayload, ReferenceProjection,
        ReferenceSource, ShortcutTarget, UpdateLibraryObject, content_from_bytes,
        resolve_shortcut_content, valid_library_id,
    },
};
use microcrab_storage::library::{LibraryLimits, LibraryObjectRecord, PersonalLibrary};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::{Arc, RwLock, Weak},
};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) struct ResolvedLibraryContent {
    pub(crate) media_type: String,
    pub(crate) data: Vec<u8>,
    pub(crate) inline_safe: bool,
}

#[derive(Default)]
struct CollectionCache {
    by_ref: BTreeMap<String, Arc<MarkdownReferences>>,
    section_owner: HashMap<String, String>,
}

pub(crate) struct SessionLibraryAdapter {
    manager: Weak<Manager>,
    owner: String,
    session_id: String,
    project_id: Option<String>,
    host: Arc<dyn Host>,
    store: Arc<dyn PersonalLibrary>,
    limits: LibraryLimits,
    collections: RwLock<CollectionCache>,
}

impl SessionLibraryAdapter {
    pub(crate) fn new(
        manager: Weak<Manager>,
        owner: String,
        session_id: String,
        project_id: Option<String>,
        host: Arc<dyn Host>,
        store: Arc<dyn PersonalLibrary>,
        limits: LibraryLimits,
    ) -> Self {
        Self {
            manager,
            owner,
            session_id,
            project_id,
            host,
            store,
            limits,
            collections: RwLock::new(CollectionCache::default()),
        }
    }

    fn verify_shortcut_scope(&self, target: &ShortcutTarget) -> Result<(), Error> {
        let bound_project = match target {
            ShortcutTarget::HostPath { project_id, .. }
            | ShortcutTarget::HostGlob { project_id, .. } => project_id.as_deref(),
            ShortcutTarget::PublicHttps { .. } | ShortcutTarget::DirectoryUrl { .. } => None,
        };
        if let Some(expected_project) = bound_project
            && self.project_id.as_deref() != Some(expected_project)
        {
            return Err(Error::reference(
                "shortcut host target belongs to a different project workspace",
            ));
        }
        Ok(())
    }

    fn shortcut_to_markdown_source(target: &ShortcutTarget) -> Option<ReferenceSource> {
        match target {
            ShortcutTarget::HostGlob { root, pattern, .. } => Some(ReferenceSource::HostGlob {
                root: PathBuf::from(root),
                pattern: pattern.clone(),
            }),
            ShortcutTarget::DirectoryUrl { url } => {
                Some(ReferenceSource::DirectoryUrl { url: url.clone() })
            }
            ShortcutTarget::HostPath { path, .. } => {
                let lower = path.to_ascii_lowercase();
                if lower.ends_with(".md") || lower.ends_with(".markdown") {
                    Some(ReferenceSource::HostPath {
                        path: PathBuf::from(path),
                    })
                } else {
                    None
                }
            }
            ShortcutTarget::PublicHttps { .. } => None,
        }
    }

    async fn load_collection(
        &self,
        summary: &LibraryObjectSummary,
        source: ReferenceSource,
    ) -> Result<(Option<Vec<Value>>, String, Arc<MarkdownReferences>), Error> {
        let md = Arc::new(MarkdownReferences::new(
            summary.id.clone(),
            vec![source],
            Some(self.host.clone()),
        ));
        md.load().await?;
        let subjects = md
            .change()?
            .and_then(|value| value.as_array().cloned());
        let list = md.list()?;
        let revision = format!("{}x{}", summary.revision.split('_').next().unwrap_or("1"), list.len());
        Ok((subjects, revision, md))
    }

    async fn ensure_collection_loaded(&self) -> Result<(), Error> {
        let mounted = self
            .store
            .list_session_mounts(&self.owner, &self.session_id)
            .await?;
        for summary in mounted {
            let LibrarySource::Shortcut { target } = &summary.source else {
                continue;
            };
            let Some(source) = Self::shortcut_to_markdown_source(target) else {
                continue;
            };
            if self.verify_shortcut_scope(target).is_err() {
                continue;
            }
            let already = self
                .collections
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .by_ref
                .contains_key(&summary.id);
            if already {
                continue;
            }
            if let Ok((_subjects, _rev, md)) = self.load_collection(&summary, source).await {
                if let Ok(list) = md.list() {
                    let mut cache = self.collections.write().unwrap_or_else(|e| e.into_inner());
                    for item in list {
                        cache.section_owner.insert(item.id, summary.id.clone());
                    }
                    cache.by_ref.insert(summary.id.clone(), md);
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ReferenceLibrary for SessionLibraryAdapter {
    async fn quota(&self) -> Result<LibraryQuota, Error> {
        self.store.quota(&self.owner, self.limits).await
    }

    async fn add(
        &self,
        request: CreateLibraryObject,
        cancel: CancellationToken,
    ) -> Result<LibraryObjectSummary, Error> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let normalized_request = match request {
            CreateLibraryObject::LocalBlob {
                id,
                title,
                media_type,
                data,
                origin,
                origin_execution_id,
            } => CreateLibraryObject::LocalBlob {
                id,
                title,
                media_type,
                data,
                origin,
                origin_execution_id,
            },
            CreateLibraryObject::Shortcut { title, target } => {
                let scoped = match target {
                    ShortcutTarget::HostPath {
                        path,
                        project_id,
                        host_id,
                        workspace_id,
                    } => ShortcutTarget::HostPath {
                        path,
                        project_id: project_id.or_else(|| self.project_id.clone()),
                        host_id,
                        workspace_id,
                    },
                    ShortcutTarget::HostGlob {
                        root,
                        pattern,
                        project_id,
                        host_id,
                        workspace_id,
                    } => ShortcutTarget::HostGlob {
                        root,
                        pattern,
                        project_id: project_id.or_else(|| self.project_id.clone()),
                        host_id,
                        workspace_id,
                    },
                    other => other,
                };
                self.verify_shortcut_scope(&scoped)?;
                CreateLibraryObject::Shortcut {
                    title,
                    target: scoped,
                }
            }
        };
        let created = self
            .store
            .create(
                &self.owner,
                self.limits,
                normalized_request,
                Some(&self.session_id),
                None,
            )
            .await?;
        let mut summary = created.summary;
        if let LibrarySource::Shortcut { target } = &summary.source
            && let Some(source) = Self::shortcut_to_markdown_source(target)
        {
            if let Ok((subjects, rev, md)) = self.load_collection(&summary, source).await {
                if let Some(subjects_val) = subjects.clone() {
                    let _ = self
                        .store
                        .update_shortcut_metadata(
                            &self.owner,
                            &summary.id,
                            summary.byte_length,
                            rev.clone(),
                            Some(subjects_val.clone()),
                        )
                        .await;
                    summary.subjects = Some(subjects_val);
                    summary.revision = rev;
                }
                if let Ok(list) = md.list() {
                    let mut cache = self.collections.write().unwrap_or_else(|e| e.into_inner());
                    for item in list {
                        cache.section_owner.insert(item.id, summary.id.clone());
                    }
                    cache.by_ref.insert(summary.id.clone(), md);
                }
            }
        }
        Ok(summary)
    }

    async fn update(
        &self,
        id: &str,
        update: UpdateLibraryObject,
        cancel: CancellationToken,
    ) -> Result<LibraryObjectSummary, Error> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let Some(existing) = self.store.get(&self.owner, id).await? else {
            return Err(Error::reference("requested reference was not found"));
        };
        if !existing.mounted_sessions.contains(&self.session_id) {
            return Err(Error::reference("reference is not mounted in this Session"));
        }
        if let Some(target) = &update.target {
            self.verify_shortcut_scope(target)?;
        }
        let updated = self
            .store
            .update(&self.owner, self.limits, id, update)
            .await?;
        Ok(updated.summary)
    }

    async fn remove_mount(&self, id: &str, cancel: CancellationToken) -> Result<bool, Error> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let removed = self
            .store
            .unmount_session(&self.owner, id, &self.session_id)
            .await?;
        if removed {
            let mut cache = self.collections.write().unwrap_or_else(|e| e.into_inner());
            cache.by_ref.remove(id);
            cache.section_owner.retain(|_, owner_id| owner_id != id);
        }
        Ok(removed)
    }

    async fn read(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, Error> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if !valid_library_id(id) {
            let existing_md = {
                let cache = self.collections.read().unwrap_or_else(|e| e.into_inner());
                cache
                    .section_owner
                    .get(id)
                    .and_then(|owner| cache.by_ref.get(owner))
                    .cloned()
            };
            if let Some(md) = existing_md {
                return md.read(id);
            }
            self.ensure_collection_loaded().await?;
            let retry_md = {
                let cache = self.collections.read().unwrap_or_else(|e| e.into_inner());
                cache
                    .section_owner
                    .get(id)
                    .and_then(|owner| cache.by_ref.get(owner))
                    .cloned()
            };
            return match retry_md {
                Some(md) => md.read(id),
                None => Ok(None),
            };
        }

        let Some(record) = self.store.get(&self.owner, id).await? else {
            return Ok(None);
        };
        if !record.mounted_sessions.contains(&self.session_id) {
            return Ok(None);
        }
        match &record.summary.source {
            LibrarySource::LocalBlob { .. } => {
                let data = self
                    .store
                    .read_blob(&self.owner, id)
                    .await?
                    .unwrap_or_default();
                content_from_bytes(id, Some(&record.summary.media_type), data).map(Some)
            }
            LibrarySource::Shortcut { target } => {
                self.verify_shortcut_scope(target)?;
                let host = if let Some(manager) = self.manager.upgrade()
                    && let Some(project) = match target {
                        ShortcutTarget::HostPath { project_id, .. }
                        | ShortcutTarget::HostGlob { project_id, .. } => project_id.as_deref(),
                        _ => None,
                    }
                {
                    manager
                        .project_host(&self.owner, project)
                        .await
                        .unwrap_or_else(|_| self.host.clone())
                } else {
                    self.host.clone()
                };
                resolve_shortcut_content(
                    id,
                    target,
                    Some(host.as_ref()),
                    self.limits.max_object_bytes,
                    &cancel,
                )
                .await
                .map(Some)
            }
        }
    }

    async fn projections(&self, cancel: CancellationToken) -> Result<ReferenceProjection, Error> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let mounted = self
            .store
            .list_session_mounts(&self.owner, &self.session_id)
            .await?;
        if mounted.is_empty() {
            return Ok(ReferenceProjection {
                exact: None,
                short_model: None,
            });
        }
        Ok(ReferenceProjection {
            exact: Some(Value::Array(
                mounted.iter().map(|s| s.projection_value(false)).collect(),
            )),
            short_model: Some(Value::Array(
                mounted.iter().map(|s| s.projection_value(true)).collect(),
            )),
        })
    }

    async fn refresh(&self, cancel: CancellationToken) -> Result<(), Error> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if let Some(project_id) = &self.project_id {
            self.store
                .mount_project_to_session(&self.owner, project_id, &self.session_id)
                .await?;
        }
        let mounted = self
            .store
            .list_session_mounts(&self.owner, &self.session_id)
            .await?;
        let mut next_cache = CollectionCache::default();
        for summary in mounted {
            let LibrarySource::Shortcut { target } = &summary.source else {
                continue;
            };
            let Some(source) = Self::shortcut_to_markdown_source(target) else {
                continue;
            };
            if self.verify_shortcut_scope(target).is_err() {
                continue;
            }
            if let Ok((subjects, revision, md)) = self.load_collection(&summary, source).await {
                let _ = self
                    .store
                    .update_shortcut_metadata(
                        &self.owner,
                        &summary.id,
                        summary.byte_length,
                        revision,
                        subjects,
                    )
                    .await;
                if let Ok(list) = md.list() {
                    for item in list {
                        next_cache
                            .section_owner
                            .insert(item.id, summary.id.clone());
                    }
                }
                next_cache.by_ref.insert(summary.id, md);
            }
        }
        *self.collections.write().unwrap_or_else(|e| e.into_inner()) = next_cache;
        Ok(())
    }
}

pub(crate) fn library_wire_error(error: Error) -> WireError {
    match error {
        Error::Limit(limit)
            if matches!(
                limit.kind,
                LimitKind::LibraryStorageBytes | LimitKind::LibraryStorageEntries
            ) =>
        {
            let mut wire = WireError::new("user_storage_quota_exceeded", limit.safe_message());
            wire.details = Some(Box::new(json!({
                "reason": limit.kind.as_str(),
                "used_bytes": limit.required_bytes,
                "quota_bytes": limit.budget_bytes,
                "required_count": limit.required_count,
                "budget_count": limit.budget_count,
            })));
            wire
        }
        Error::Protocol(ref reason) if reason.public_reason().is_some_and(|r| r.contains("conflict")) => {
            WireError::new("revision_conflict", error.diagnostic_reason())
        }
        Error::Config(_) | Error::Reference(_) => {
            WireError::new("invalid_request", error.diagnostic_reason())
        }
        other => safe(other),
    }
}

fn record_json(record: &LibraryObjectRecord) -> Value {
    let mut obj = serde_json::to_value(&record.summary).unwrap_or_else(|_| json!({}));
    if let Some(map) = obj.as_object_mut() {
        map.insert("mounted_sessions".into(), json!(record.mounted_sessions));
        map.insert("mounted_projects".into(), json!(record.mounted_projects));
    }
    obj
}

impl Manager {
    pub(crate) async fn library_quota(&self, principal: &Principal) -> Result<Value, WireError> {
        let quota = self
            .personal_library
            .quota(&principal.owner, self.config.personal_library.limits())
            .await
            .map_err(library_wire_error)?;
        Ok(json!(quota))
    }

    pub(crate) async fn library_list(
        &self,
        principal: &Principal,
        source_kind: Option<&str>,
        session_id: Option<&str>,
        project_id: Option<&str>,
        query: Option<&str>,
    ) -> Result<Value, WireError> {
        let records = self
            .personal_library
            .list(&principal.owner)
            .await
            .map_err(library_wire_error)?;
        let q_lower = query.map(|q| q.trim().to_ascii_lowercase());
        let mut items = Vec::new();
        for record in records {
            if let Some(kind) = source_kind
                && !kind.is_empty()
                && record.summary.source_kind != kind
            {
                continue;
            }
            if let Some(sid) = session_id
                && !sid.is_empty()
                && !record.mounted_sessions.iter().any(|s| s == sid)
            {
                continue;
            }
            if let Some(pid) = project_id
                && !pid.is_empty()
                && !record.mounted_projects.iter().any(|p| p == pid)
            {
                continue;
            }
            if let Some(q) = &q_lower
                && !q.is_empty()
                && !record.summary.title.to_ascii_lowercase().contains(q)
                && !record.summary.id.to_ascii_lowercase().contains(q)
                && !record.summary.media_type.to_ascii_lowercase().contains(q)
            {
                continue;
            }
            items.push(record_json(&record));
        }
        let quota = self
            .personal_library
            .quota(&principal.owner, self.config.personal_library.limits())
            .await
            .map_err(library_wire_error)?;
        Ok(json!({ "items": items, "quota": quota }))
    }

    pub(crate) async fn library_get(
        &self,
        principal: &Principal,
        id: &str,
    ) -> Result<Value, WireError> {
        if !valid_library_id(id) {
            return Err(unavailable());
        }
        let record = self
            .personal_library
            .get(&principal.owner, id)
            .await
            .map_err(library_wire_error)?
            .ok_or_else(unavailable)?;
        Ok(record_json(&record))
    }

    pub(crate) fn library_permit(&self) -> Result<OwnedSemaphorePermit, WireError> {
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(WireError::new("closed", "Manager unavailable"));
        }
        self.library_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| WireError::new("capacity", "Personal library request capacity reached"))
    }

    async fn validate_optional_mounts(
        &self,
        owner: &str,
        mount_session_id: Option<&str>,
        mount_project_id: Option<&str>,
    ) -> Result<(), WireError> {
        if let Some(sid) = mount_session_id {
            let record = self.record(owner, sid).await?;
            if record.archived {
                return Err(WireError::new("archived", "Session is archived"));
            }
        }
        if let Some(pid) = mount_project_id
            && !self.project_allowed(owner, pid)
        {
            return Err(unavailable());
        }
        Ok(())
    }

    async fn try_refresh_session(&self, session_id: &str) {
        if let Some(managed) = active(self, session_id).await {
            if managed.commands.try_lock().is_ok() {
                let _ = managed.session.refresh_reference_library().await;
            }
        }
    }

    pub(crate) async fn library_create(
        &self,
        principal: &Principal,
        request: CreateLibraryObject,
        mount_session_id: Option<String>,
        mount_project_id: Option<String>,
        permit: OwnedSemaphorePermit,
    ) -> Result<Value, WireError> {
        let owner = principal.owner.clone();
        self.validate_optional_mounts(
            &owner,
            mount_session_id.as_deref(),
            mount_project_id.as_deref(),
        )
        .await?;
        let limits = self.config.personal_library.limits();
        let store = self.personal_library.clone();
        let created = self
            .track(async move {
                let _permit = permit;
                store
                    .create(
                        &owner,
                        limits,
                        request,
                        mount_session_id.as_deref(),
                        mount_project_id.as_deref(),
                    )
                    .await
                    .map_err(library_wire_error)
            })?
            .await
            .map_err(|_| WireError::new("operation_failed", "Personal library writer failed"))??;
        for sid in &created.mounted_sessions {
            self.try_refresh_session(sid).await;
        }
        Ok(record_json(&created))
    }

    pub(crate) async fn library_update(
        &self,
        principal: &Principal,
        id: &str,
        update: UpdateLibraryObject,
    ) -> Result<Value, WireError> {
        if !valid_library_id(id) {
            return Err(unavailable());
        }
        let updated = self
            .personal_library
            .update(
                &principal.owner,
                self.config.personal_library.limits(),
                id,
                update,
            )
            .await
            .map_err(library_wire_error)?;
        for sid in &updated.mounted_sessions {
            self.try_refresh_session(sid).await;
        }
        Ok(record_json(&updated))
    }

    pub(crate) async fn library_delete(
        &self,
        principal: &Principal,
        id: &str,
    ) -> Result<Value, WireError> {
        if !valid_library_id(id) {
            return Err(unavailable());
        }
        let existing = self
            .personal_library
            .get(&principal.owner, id)
            .await
            .map_err(library_wire_error)?;
        let affected_sessions = existing
            .as_ref()
            .map(|r| r.mounted_sessions.clone())
            .unwrap_or_default();
        let deleted = self
            .personal_library
            .delete(&principal.owner, id)
            .await
            .map_err(library_wire_error)?;
        if !deleted {
            return Err(unavailable());
        }
        for sid in affected_sessions {
            self.try_refresh_session(&sid).await;
        }
        let quota = self.library_quota(principal).await?;
        Ok(json!({ "deleted": true, "id": id, "quota": quota }))
    }

    pub(crate) async fn library_mount_session(
        &self,
        principal: &Principal,
        id: &str,
        session_id: &str,
        mount: bool,
    ) -> Result<Value, WireError> {
        if !valid_library_id(id) {
            return Err(unavailable());
        }
        let record = self.record(&principal.owner, session_id).await?;
        if mount && record.archived {
            return Err(WireError::new("archived", "Session is archived"));
        }
        let changed = if mount {
            self.personal_library
                .mount_session(&principal.owner, id, session_id)
                .await
                .map_err(library_wire_error)?
        } else {
            self.personal_library
                .unmount_session(&principal.owner, id, session_id)
                .await
                .map_err(library_wire_error)?
        };
        if !changed {
            return Err(unavailable());
        }
        self.try_refresh_session(session_id).await;
        let updated = self.library_get(principal, id).await?;
        Ok(updated)
    }

    pub(crate) async fn library_mount_project(
        &self,
        principal: &Principal,
        id: &str,
        project_id: &str,
        mount: bool,
    ) -> Result<Value, WireError> {
        if !valid_library_id(id) || !self.project_allowed(&principal.owner, project_id) {
            return Err(unavailable());
        }
        let changed = if mount {
            self.personal_library
                .mount_project(&principal.owner, id, project_id)
                .await
                .map_err(library_wire_error)?
        } else {
            self.personal_library
                .unmount_project(&principal.owner, id, project_id)
                .await
                .map_err(library_wire_error)?
        };
        if !changed {
            return Err(unavailable());
        }
        self.library_get(principal, id).await
    }

    pub(crate) async fn library_read_content(
        &self,
        principal: &Principal,
        id: &str,
        session_hint: Option<&str>,
        project_hint: Option<&str>,
    ) -> Result<ResolvedLibraryContent, WireError> {
        if !valid_library_id(id) {
            return Err(unavailable());
        }
        let record = self
            .personal_library
            .get(&principal.owner, id)
            .await
            .map_err(library_wire_error)?
            .ok_or_else(unavailable)?;
        match &record.summary.source {
            LibrarySource::LocalBlob { .. } => {
                let data = self
                    .personal_library
                    .read_blob(&principal.owner, id)
                    .await
                    .map_err(library_wire_error)?
                    .ok_or_else(unavailable)?;
                let inline_safe = image_type(&record.summary.media_type).is_some()
                    && validate_image(&record.summary.media_type, &data).is_ok();
                Ok(ResolvedLibraryContent {
                    media_type: record.summary.media_type,
                    data,
                    inline_safe,
                })
            }
            LibrarySource::Shortcut { target } => {
                let host: Option<Arc<dyn Host>> = match target {
                    ShortcutTarget::HostPath { project_id, .. }
                    | ShortcutTarget::HostGlob { project_id, .. } => {
                        if let Some(pid) = project_hint.or(project_id.as_deref()) {
                            if let Some(bound) = project_id.as_deref()
                                && bound != pid
                            {
                                return Err(WireError::new(
                                    "invalid_request",
                                    "Shortcut belongs to another project workspace",
                                ));
                            }
                            Some(self.project_host(&principal.owner, pid).await?)
                        } else if let Some(sid) = session_hint
                            .or_else(|| record.mounted_sessions.first().map(String::as_str))
                        {
                            let rec = self.record(&principal.owner, sid).await?;
                            Some(
                                self.session_host(&rec)
                                    .await
                                    .map_err(library_wire_error)?,
                            )
                        } else if let Some(pid) = record.mounted_projects.first() {
                            Some(self.project_host(&principal.owner, pid).await?)
                        } else {
                            None
                        }
                    }
                    ShortcutTarget::PublicHttps { .. } | ShortcutTarget::DirectoryUrl { .. } => {
                        None
                    }
                };
                let resolved = resolve_shortcut_content(
                    id,
                    target,
                    host.as_deref(),
                    self.config.personal_library.max_object_bytes,
                    &CancellationToken::new(),
                )
                .await
                .map_err(library_wire_error)?;
                let (media_type, data, inline_safe) = match resolved.payload {
                    ReferencePayload::Text(text) => (resolved.media_type, text.into_bytes(), false),
                    ReferencePayload::Binary { data, .. } => {
                        let safe_img = image_type(&resolved.media_type).is_some()
                            && validate_image(&resolved.media_type, &data).is_ok();
                        (resolved.media_type, data, safe_img)
                    }
                };
                Ok(ResolvedLibraryContent {
                    media_type,
                    data,
                    inline_safe,
                })
            }
        }
    }
}
