//! Server-owned Personal Content Library `Reference` implementations (`references` & `media`)
//! and Session mount synchronization.
use super::{Manager, safe, settings::active, unavailable};
use crate::auth::Principal;
use async_trait::async_trait;
use microcrab_host_api::{
    Host, HostSkillsReference,
    markdown::{
        DocumentRevision, compact_markdown_node, is_markdown_path, markdown_metadata_json,
        parse_markdown,
    },
};
use microcrab_protocol::{ReferenceLimitDetails, application::WireError};
use microcrab_session::{
    Error, Reference, ReferenceCapabilities, ReferenceContent, ReferenceEntry, ReferenceError,
    ReferenceNodeKind, ReferencePatch, ReferencePayload, ReferenceProjection, ReferenceQuota,
    ReferenceWrite,
    admission::image::{MAX_IMAGE_BYTES, image_type, validate_image},
    budget::{LimitKind, check as check_budget},
    state::reference::{content_from_bytes, valid_library_id},
};
use microcrab_storage::library::{
    CreateLibraryObject, LibraryLimits, LibraryObjectRecord, LibraryObjectSummary, LibrarySource,
    LocalBlobOrigin, PersonalLibrary, ShortcutTarget, UpdateLibraryObject,
};
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

fn ref_failed(error: impl std::fmt::Display) -> Error {
    Error::reference_with_private("reference operation failed", error.to_string())
}

fn session_err_to_ref(error: Error) -> ReferenceError {
    match error {
        Error::Cancelled => ReferenceError::cancelled(),
        Error::Denied(r) => ReferenceError::denied(r.to_string()),
        Error::Config(r) => ReferenceError::config(r.to_string()),
        Error::Protocol(r) => ReferenceError::conflict(r.to_string()),
        Error::Limit(limit) => {
            let mut err = ReferenceError::limit(limit.safe_message());
            err.limit_details = Some(ReferenceLimitDetails {
                kind: limit.kind.as_str(),
                required_bytes: limit.required_bytes.unwrap_or(0) as u64,
                budget_bytes: limit.budget_bytes.unwrap_or(0) as u64,
                required_count: limit.required_count.unwrap_or(0) as u64,
                budget_count: limit.budget_count.unwrap_or(0) as u64,
            });
            err
        }
        other => ReferenceError::failed(other.to_string()),
    }
}

fn is_image_summary(summary: &LibraryObjectSummary) -> bool {
    matches!(&summary.source, LibrarySource::LocalBlob { .. })
        && image_type(&summary.media_type).is_some()
}

fn summary_to_entry(summary: &LibraryObjectSummary) -> ReferenceEntry {
    let location = match &summary.source {
        LibrarySource::Shortcut { target } => Some(target.location()),
        LibrarySource::LocalBlob { .. } => None,
    };
    ReferenceEntry {
        id: summary.id.clone(),
        node_kind: ReferenceNodeKind::File,
        parent_id: None,
        title: summary.title.clone(),
        location,
        description: None,
        media_type: Some(summary.media_type.clone()),
        revision: summary.revision.clone(),
        writable: !summary.read_only,
        metadata: None,
    }
}

#[derive(Default)]
struct ShortcutCollectionCache {
    host_skills: BTreeMap<String, Arc<HostSkillsReference>>,
    dir_urls: BTreeMap<String, Arc<DirectoryUrlMarkdownCollection>>,
    section_owner: HashMap<String, String>,
}

struct DirectoryUrlMarkdownCollection {
    subjects: Option<Vec<Value>>,
    bodies: BTreeMap<String, String>,
    section_ids: Vec<String>,
}

impl DirectoryUrlMarkdownCollection {
    async fn load(kind: &str, url: &str) -> Result<Self, Error> {
        let base = reqwest::Url::parse(url).map_err(ref_failed)?;
        if base.scheme() != "https" || !base.path().ends_with('/') {
            return Err(Error::reference(
                "directory_url must be HTTPS and end with /",
            ));
        }
        let client = microcrab_transport::http::client_builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(20))
            .build()
            .map_err(ref_failed)?;
        let html = fetch_https_text(&client, &base, 1024 * 1024).await?;
        let links = extract_directory_links(&base, &html)?;
        let mut bodies = BTreeMap::new();
        let mut subjects = Vec::new();
        let mut section_ids = Vec::new();
        for link in links.into_iter().take(64) {
            let md_text = fetch_https_text(&client, &link, 1024 * 1024).await?;
            let location = link.to_string();
            let ident = format!("{}:{}:{}", kind.len(), kind, location);
            let doc = parse_markdown(&ident, &md_text).map_err(ref_failed)?;
            let mut row = compact_markdown_node(&doc.root);
            if let Some(map) = row.as_object_mut() {
                let rev = DocumentRevision::from_bytes(md_text.as_bytes());
                map.insert("path".into(), json!(location));
                map.insert("revision".into(), json!(rev.label));
                if let Ok(Some(meta)) = markdown_metadata_json(&doc.metadata) {
                    map.insert("metadata".into(), meta);
                }
            }
            subjects.push(row);
            for (sid, body) in doc.bodies {
                section_ids.push(sid.clone());
                bodies.insert(sid, body);
            }
        }
        Ok(Self {
            subjects: (!subjects.is_empty()).then_some(subjects),
            bodies,
            section_ids,
        })
    }
}

async fn fetch_https_text(
    client: &reqwest::Client,
    url: &reqwest::Url,
    limit: usize,
) -> Result<String, Error> {
    let mut resp = client
        .get(url.clone())
        .send()
        .await
        .map_err(ref_failed)?
        .error_for_status()
        .map_err(ref_failed)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(ref_failed)? {
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(Error::reference("directory URL document exceeds size budget"));
        }
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes).map_err(ref_failed)
}

fn extract_directory_links(base: &reqwest::Url, html: &str) -> Result<Vec<reqwest::Url>, Error> {
    let mut links = Vec::new();
    let mut rest = html;
    while let Some(idx) = rest.find("href=") {
        rest = &rest[idx + 5..];
        let quote = rest.as_bytes().first().copied();
        let Some(q) = quote.filter(|b| matches!(b, b'"' | b'\'')) else {
            continue;
        };
        let after = &rest[1..];
        let Some(end) = after.find(q as char) else {
            break;
        };
        let raw = &after[..end];
        rest = &after[end + 1..];
        if raw.is_empty() || raw.starts_with('#') || raw.starts_with('?') {
            continue;
        }
        if let Ok(joined) = base.join(raw)
            && joined.origin() == base.origin()
            && is_markdown_path(joined.path())
            && !links.contains(&joined)
        {
            links.push(joined);
        }
    }
    Ok(links)
}

pub(crate) async fn resolve_shortcut_content(
    id: &str,
    target: &ShortcutTarget,
    host: Option<&dyn Host>,
    max_bytes: usize,
    cancel: &CancellationToken,
) -> Result<ReferenceContent, Error> {
    match target {
        ShortcutTarget::HostPath { path, .. } => {
            let host = host.ok_or_else(|| {
                Error::tool("shortcut host_path requires a connected bound Host")
            })?;
            let path_buf = PathBuf::from(path);
            if host.supports_private_file_read() {
                let op = microcrab_host_api::OpContext {
                    cancel: cancel.clone(),
                    principal: "library".into(),
                    trace_id: id.into(),
                    deadline: None,
                };
                let buffer = host
                    .read_private_file(
                        path_buf,
                        microcrab_host_api::PrivateFileBuffer::new(max_bytes, ()),
                        &op,
                    )
                    .await
                    .map_err(Error::from_host_error)?;
                let data = buffer.into_bytes();
                content_from_bytes(id, Some(path), data)
            } else {
                let output = host
                    .read_text(
                        microcrab_host_api::ReadRequest {
                            path: path_buf,
                            offset_line: 1,
                            max_lines: usize::MAX,
                            max_bytes,
                        },
                        cancel.clone(),
                    )
                    .await
                    .map_err(Error::from_host_error)?;
                if output.truncated {
                    return Err(check_budget(
                        LimitKind::ReferenceBytes,
                        max_bytes.saturating_add(1),
                        max_bytes,
                    )
                    .unwrap_err());
                }
                Ok(ReferenceContent {
                    id: id.into(),
                    media_type: microcrab_session::state::reference::detect_mime_from_bytes(
                        Some(path),
                        output.content.as_bytes(),
                    ),
                    payload: ReferencePayload::Text(output.content),
                })
            }
        }
        ShortcutTarget::PublicHttps { url } => {
            let parsed = microcrab_transport::public_https::PublicHttpsUrl::parse(url)
                .map_err(|error| Error::tool_dynamic(error.to_string()))?;
            let target = parsed.admit(cancel).await.map_err(|error| {
                if cancel.is_cancelled() {
                    Error::Cancelled
                } else {
                    Error::tool_dynamic(error.to_string())
                }
            })?;
            let max_fetch = max_bytes.min(MAX_IMAGE_BYTES.max(8 * 1024 * 1024));
            let fetched = target.fetch(max_fetch, cancel).await.map_err(|error| {
                if cancel.is_cancelled() {
                    Error::Cancelled
                } else {
                    Error::tool_dynamic(error.to_string())
                }
            })?;
            content_from_bytes(id, Some(&fetched.mime_type), fetched.bytes)
        }
        ShortcutTarget::HostGlob { .. } | ShortcutTarget::DirectoryUrl { .. } => Err(
            Error::reference("collection shortcuts expose Markdown section IDs; expand those section IDs directly"),
        ),
    }
}

/// Shared session-scoped backing over persistent `PersonalLibrary`, consumed by
/// both `PersonalLibraryReference` (`references`) and `MediaReference` (`media`).
pub(crate) struct SessionLibraryBacking {
    manager: Weak<Manager>,
    owner: String,
    session_id: String,
    project_id: Option<String>,
    host: Arc<dyn Host>,
    store: Arc<dyn PersonalLibrary>,
    limits: LibraryLimits,
    mounted_snapshot: RwLock<Vec<LibraryObjectSummary>>,
    collections: RwLock<ShortcutCollectionCache>,
}

impl SessionLibraryBacking {
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
            mounted_snapshot: RwLock::new(Vec::new()),
            collections: RwLock::new(ShortcutCollectionCache::default()),
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

    async fn sync_mounted_snapshot(&self) -> Result<(), Error> {
        let mounted = self
            .store
            .list_session_mounts(&self.owner, &self.session_id)
            .await?;
        *self
            .mounted_snapshot
            .write()
            .unwrap_or_else(|e| e.into_inner()) = mounted;
        Ok(())
    }

    async fn load_shortcut_collection(
        &self,
        summary: &LibraryObjectSummary,
        target: &ShortcutTarget,
        cancel: &CancellationToken,
    ) -> Result<Option<(Option<Vec<Value>>, String, Vec<String>)>, Error> {
        match target {
            ShortcutTarget::HostGlob { root, .. } => {
                let skills = Arc::new(HostSkillsReference::from_host(
                    summary.id.clone(),
                    self.host.clone(),
                    vec![PathBuf::from(root)],
                ));
                skills
                    .load(cancel.clone())
                    .await
                    .map_err(ref_failed)?;
                let subjects = skills
                    .projections()
                    .ok()
                    .and_then(|p| p.exact)
                    .and_then(|v| v.as_array().cloned());
                let ids: Vec<String> = skills
                    .list()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|e| e.id)
                    .collect();
                let rev = format!(
                    "{}x{}",
                    summary.revision.split('_').next().unwrap_or("1"),
                    ids.len()
                );
                let mut cache = self.collections.write().unwrap_or_else(|e| e.into_inner());
                for id in &ids {
                    cache.section_owner.insert(id.clone(), summary.id.clone());
                }
                cache.host_skills.insert(summary.id.clone(), skills);
                Ok(Some((subjects, rev, ids)))
            }
            ShortcutTarget::DirectoryUrl { url } => {
                let coll = Arc::new(DirectoryUrlMarkdownCollection::load(&summary.id, url).await?);
                let subjects = coll.subjects.clone();
                let ids = coll.section_ids.clone();
                let rev = format!(
                    "{}x{}",
                    summary.revision.split('_').next().unwrap_or("1"),
                    ids.len()
                );
                let mut cache = self.collections.write().unwrap_or_else(|e| e.into_inner());
                for id in &ids {
                    cache.section_owner.insert(id.clone(), summary.id.clone());
                }
                cache.dir_urls.insert(summary.id.clone(), coll);
                Ok(Some((subjects, rev, ids)))
            }
            ShortcutTarget::HostPath { .. } | ShortcutTarget::PublicHttps { .. } => Ok(None),
        }
    }

    async fn refresh_all(&self, cancel: &CancellationToken) -> Result<(), Error> {
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
        *self
            .collections
            .write()
            .unwrap_or_else(|e| e.into_inner()) = ShortcutCollectionCache::default();
        for summary in &mounted {
            if let LibrarySource::Shortcut { target } = &summary.source
                && self.verify_shortcut_scope(target).is_ok()
                && let Ok(Some((subjects, rev, _))) =
                    self.load_shortcut_collection(summary, target, cancel).await
            {
                let _ = self
                    .store
                    .update_shortcut_metadata(
                        &self.owner,
                        &summary.id,
                        summary.byte_length,
                        rev,
                        subjects,
                    )
                    .await;
            }
        }
        self.sync_mounted_snapshot().await?;
        Ok(())
    }

    fn snapshot_filtered(&self, want_images: bool) -> Vec<LibraryObjectSummary> {
        self.mounted_snapshot
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|s| is_image_summary(s) == want_images)
            .cloned()
            .collect()
    }

    fn projections_filtered(&self, want_images: bool) -> ReferenceProjection {
        let items = self.snapshot_filtered(want_images);
        if items.is_empty() {
            return ReferenceProjection::empty();
        }
        ReferenceProjection {
            exact: Some(Value::Array(
                items.iter().map(|s| s.projection_value(false)).collect(),
            )),
            short_model: Some(Value::Array(
                items.iter().map(|s| s.projection_value(true)).collect(),
            )),
        }
    }

    async fn owns_filtered(
        &self,
        id: &str,
        want_images: bool,
        cancel: &CancellationToken,
    ) -> Result<bool, ReferenceError> {
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        if !valid_library_id(id) {
            if want_images {
                return Ok(false);
            }
            return Ok(self
                .collections
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .section_owner
                .contains_key(id));
        }
        let Some(record) = self
            .store
            .get(&self.owner, id)
            .await
            .map_err(session_err_to_ref)?
        else {
            return Ok(false);
        };
        if !record.mounted_sessions.contains(&self.session_id) {
            return Ok(false);
        }
        Ok(is_image_summary(&record.summary) == want_images)
    }

    async fn expand_filtered(
        &self,
        id: &str,
        want_images: bool,
        cancel: &CancellationToken,
    ) -> Result<Option<ReferenceContent>, ReferenceError> {
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        if !valid_library_id(id) {
            if want_images {
                return Ok(None);
            }
            let (hs, du) = {
                let cache = self.collections.read().unwrap_or_else(|e| e.into_inner());
                let owner = cache.section_owner.get(id).cloned();
                (
                    owner.as_ref().and_then(|o| cache.host_skills.get(o).cloned()),
                    owner.as_ref().and_then(|o| cache.dir_urls.get(o).cloned()),
                )
            };
            if let Some(skills) = hs {
                return skills.expand(id, cancel.clone()).await;
            }
            if let Some(coll) = du {
                return Ok(coll
                    .bodies
                    .get(id)
                    .map(|b| ReferenceContent::text_with_mime(id, "text/markdown", b.clone())));
            }
            return Ok(None);
        }

        let Some(record) = self
            .store
            .get(&self.owner, id)
            .await
            .map_err(session_err_to_ref)?
        else {
            return Ok(None);
        };
        if !record.mounted_sessions.contains(&self.session_id)
            || is_image_summary(&record.summary) != want_images
        {
            return Ok(None);
        }
        match &record.summary.source {
            LibrarySource::LocalBlob { .. } => {
                let data = self
                    .store
                    .read_blob(&self.owner, id)
                    .await
                    .map_err(session_err_to_ref)?
                    .unwrap_or_default();
                content_from_bytes(id, Some(&record.summary.media_type), data)
                    .map(Some)
                    .map_err(session_err_to_ref)
            }
            LibrarySource::Shortcut { target } => {
                self.verify_shortcut_scope(target)
                    .map_err(session_err_to_ref)?;
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
                    cancel,
                )
                .await
                .map(Some)
                .map_err(session_err_to_ref)
            }
        }
    }

    fn write_to_create_request(&self, input: ReferenceWrite) -> Result<CreateLibraryObject, Error> {
        let title = input.title.unwrap_or_default();
        if let Some(hint) = input.locator_hint.as_deref() {
            if let Some(exec_id) = hint.strip_prefix("job_redirect:") {
                let data = match input.payload {
                    ReferencePayload::Text(t) => t.into_bytes(),
                    ReferencePayload::Binary { data, .. } => data,
                };
                return Ok(CreateLibraryObject::LocalBlob {
                    id: None,
                    title,
                    media_type: input.media_type,
                    data,
                    origin: LocalBlobOrigin::JobRedirect,
                    origin_execution_id: Some(exec_id.to_owned()),
                });
            }
            if let Some(exec_id) = hint.strip_prefix("tool_capture:") {
                let data = match input.payload {
                    ReferencePayload::Text(t) => t.into_bytes(),
                    ReferencePayload::Binary { data, .. } => data,
                };
                return Ok(CreateLibraryObject::LocalBlob {
                    id: None,
                    title,
                    media_type: input.media_type,
                    data,
                    origin: LocalBlobOrigin::ToolCapture,
                    origin_execution_id: Some(exec_id.to_owned()),
                });
            }
            if hint.starts_with("https://") {
                let target = ShortcutTarget::PublicHttps {
                    url: hint.to_owned(),
                };
                return Ok(CreateLibraryObject::Shortcut { title, target });
            }
            if let Some(path) = hint.strip_prefix("host:") {
                let target = ShortcutTarget::HostPath {
                    path: path.to_owned(),
                    project_id: self.project_id.clone(),
                    host_id: None,
                    workspace_id: None,
                };
                self.verify_shortcut_scope(&target)?;
                return Ok(CreateLibraryObject::Shortcut { title, target });
            }
        }

        let data = match input.payload {
            ReferencePayload::Text(t) => t.into_bytes(),
            ReferencePayload::Binary { data, .. } => data,
        };
        Ok(CreateLibraryObject::LocalBlob {
            id: None,
            title: if title.is_empty() {
                format!("literal ({})", input.media_type)
            } else {
                title
            },
            media_type: input.media_type,
            data,
            origin: LocalBlobOrigin::AgentLiteral,
            origin_execution_id: None,
        })
    }

    async fn add_item(
        &self,
        input: ReferenceWrite,
        cancel: &CancellationToken,
    ) -> Result<ReferenceEntry, ReferenceError> {
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        let req = self.write_to_create_request(input).map_err(session_err_to_ref)?;
        let created = self
            .store
            .create(
                &self.owner,
                self.limits,
                req,
                Some(&self.session_id),
                None,
            )
            .await
            .map_err(session_err_to_ref)?;
        let mut summary = created.summary;
        if let LibrarySource::Shortcut { target } = &summary.source
            && let Ok(Some((subjects, rev, _))) =
                self.load_shortcut_collection(&summary, target, cancel).await
        {
            let _ = self
                .store
                .update_shortcut_metadata(
                    &self.owner,
                    &summary.id,
                    summary.byte_length,
                    rev.clone(),
                    subjects.clone(),
                )
                .await;
            summary.subjects = subjects;
            summary.revision = rev;
        }
        let _ = self.sync_mounted_snapshot().await;
        Ok(summary_to_entry(&summary))
    }

    async fn update_item(
        &self,
        id: &str,
        want_images: bool,
        patch: ReferencePatch,
        cancel: &CancellationToken,
    ) -> Result<ReferenceEntry, ReferenceError> {
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        let Some(existing) = self
            .store
            .get(&self.owner, id)
            .await
            .map_err(session_err_to_ref)?
        else {
            return Err(ReferenceError::not_found("requested reference was not found"));
        };
        if !existing.mounted_sessions.contains(&self.session_id)
            || is_image_summary(&existing.summary) != want_images
        {
            return Err(ReferenceError::not_found(
                "reference is not mounted under this kind in this Session",
            ));
        }
        let data = patch.payload.map(|p| match p {
            ReferencePayload::Text(t) => t.into_bytes(),
            ReferencePayload::Binary { data, .. } => data,
        });
        let target = if let Some(hint) = patch.locator_hint.as_deref() {
            if hint.starts_with("https://") {
                Some(ShortcutTarget::PublicHttps {
                    url: hint.to_owned(),
                })
            } else if let Some(path) = hint.strip_prefix("host:") {
                let t = ShortcutTarget::HostPath {
                    path: path.to_owned(),
                    project_id: self.project_id.clone(),
                    host_id: None,
                    workspace_id: None,
                };
                self.verify_shortcut_scope(&t).map_err(session_err_to_ref)?;
                Some(t)
            } else {
                None
            }
        } else {
            None
        };
        let updated = self
            .store
            .update(
                &self.owner,
                self.limits,
                id,
                UpdateLibraryObject {
                    expected_revision: patch.expected_revision,
                    title: patch.title,
                    media_type: patch.media_type,
                    data,
                    target,
                },
            )
            .await
            .map_err(session_err_to_ref)?;
        let _ = self.sync_mounted_snapshot().await;
        Ok(summary_to_entry(&updated.summary))
    }

    async fn remove_item(
        &self,
        id: &str,
        want_images: bool,
        cancel: &CancellationToken,
    ) -> Result<bool, ReferenceError> {
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        let Some(existing) = self
            .store
            .get(&self.owner, id)
            .await
            .map_err(session_err_to_ref)?
        else {
            return Ok(false);
        };
        if is_image_summary(&existing.summary) != want_images {
            return Ok(false);
        }
        let removed = self
            .store
            .unmount_session(&self.owner, id, &self.session_id)
            .await
            .map_err(session_err_to_ref)?;
        if removed {
            {
                let mut cache = self.collections.write().unwrap_or_else(|e| e.into_inner());
                cache.host_skills.remove(id);
                cache.dir_urls.remove(id);
                cache.section_owner.retain(|_, owner_id| owner_id != id);
            }
            let _ = self.sync_mounted_snapshot().await;
        }
        Ok(removed)
    }

    async fn quota(&self, cancel: &CancellationToken) -> Result<Option<ReferenceQuota>, ReferenceError> {
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        self.store
            .quota(&self.owner, self.limits)
            .await
            .map(Some)
            .map_err(session_err_to_ref)
    }
}

/// Server-owned Personal Content Library `Reference` (`kind = "references"`):
/// non-image blobs, external shortcuts, and default writable Session reference store.
pub(crate) struct PersonalLibraryReference {
    backing: Arc<SessionLibraryBacking>,
}

impl PersonalLibraryReference {
    pub(crate) fn new(backing: Arc<SessionLibraryBacking>) -> Self {
        Self { backing }
    }
}

#[async_trait]
impl Reference for PersonalLibraryReference {
    fn kind(&self) -> &str {
        "references"
    }

    fn capabilities(&self) -> ReferenceCapabilities {
        ReferenceCapabilities::read_write()
    }

    async fn load(&self, cancel: CancellationToken) -> Result<(), ReferenceError> {
        self.backing
            .refresh_all(&cancel)
            .await
            .map_err(session_err_to_ref)
    }

    fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
        Ok(self
            .backing
            .snapshot_filtered(false)
            .iter()
            .map(summary_to_entry)
            .collect())
    }

    fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
        Ok(self.backing.projections_filtered(false))
    }

    async fn owns(&self, id: &str, cancel: CancellationToken) -> Result<bool, ReferenceError> {
        self.backing.owns_filtered(id, false, &cancel).await
    }

    async fn expand(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, ReferenceError> {
        self.backing.expand_filtered(id, false, &cancel).await
    }

    async fn refresh(&self, cancel: CancellationToken) -> Result<(), ReferenceError> {
        self.backing
            .refresh_all(&cancel)
            .await
            .map_err(session_err_to_ref)
    }

    async fn add(
        &self,
        input: ReferenceWrite,
        cancel: CancellationToken,
    ) -> Result<ReferenceEntry, ReferenceError> {
        self.backing.add_item(input, &cancel).await
    }

    async fn update(
        &self,
        id: &str,
        patch: ReferencePatch,
        cancel: CancellationToken,
    ) -> Result<ReferenceEntry, ReferenceError> {
        self.backing.update_item(id, false, patch, &cancel).await
    }

    async fn remove(&self, id: &str, cancel: CancellationToken) -> Result<bool, ReferenceError> {
        self.backing.remove_item(id, false, &cancel).await
    }

    async fn quota(
        &self,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceQuota>, ReferenceError> {
        self.backing.quota(&cancel).await
    }
}

/// Server-owned image media `Reference` (`kind = "media"`).
pub(crate) struct MediaReference {
    backing: Arc<SessionLibraryBacking>,
}

impl MediaReference {
    pub(crate) fn new(backing: Arc<SessionLibraryBacking>) -> Self {
        Self { backing }
    }
}

#[async_trait]
impl Reference for MediaReference {
    fn kind(&self) -> &str {
        "media"
    }

    fn capabilities(&self) -> ReferenceCapabilities {
        ReferenceCapabilities::read_write()
    }

    async fn load(&self, cancel: CancellationToken) -> Result<(), ReferenceError> {
        self.backing
            .sync_mounted_snapshot()
            .await
            .map_err(session_err_to_ref)?;
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        Ok(())
    }

    fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
        Ok(self
            .backing
            .snapshot_filtered(true)
            .iter()
            .map(summary_to_entry)
            .collect())
    }

    fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
        Ok(self.backing.projections_filtered(true))
    }

    async fn owns(&self, id: &str, cancel: CancellationToken) -> Result<bool, ReferenceError> {
        self.backing.owns_filtered(id, true, &cancel).await
    }

    async fn expand(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, ReferenceError> {
        self.backing.expand_filtered(id, true, &cancel).await
    }

    async fn refresh(&self, cancel: CancellationToken) -> Result<(), ReferenceError> {
        self.backing
            .sync_mounted_snapshot()
            .await
            .map_err(session_err_to_ref)?;
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        Ok(())
    }

    async fn add(
        &self,
        input: ReferenceWrite,
        cancel: CancellationToken,
    ) -> Result<ReferenceEntry, ReferenceError> {
        if image_type(&input.media_type).is_none() {
            return Err(ReferenceError::config(
                "media reference kind currently accepts image/* payloads only",
            ));
        }
        self.backing.add_item(input, &cancel).await
    }

    async fn update(
        &self,
        id: &str,
        patch: ReferencePatch,
        cancel: CancellationToken,
    ) -> Result<ReferenceEntry, ReferenceError> {
        if let Some(mime) = patch.media_type.as_deref()
            && image_type(mime).is_none()
        {
            return Err(ReferenceError::config(
                "media reference kind currently accepts image/* payloads only",
            ));
        }
        self.backing.update_item(id, true, patch, &cancel).await
    }

    async fn remove(&self, id: &str, cancel: CancellationToken) -> Result<bool, ReferenceError> {
        self.backing.remove_item(id, true, &cancel).await
    }

    async fn quota(
        &self,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceQuota>, ReferenceError> {
        self.backing.quota(&cancel).await
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
                let _ = managed.session.refresh_references().await;
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
