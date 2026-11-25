//! Project admission and the Server-owned native reply tool.
//!
//! The production deployment lease excludes a second Manager sharing File or SQLite.
//! Under it, the owner fence orders configuration CAS, model/activation reservations,
//! bearer checks, and append through durable ACK. A project marker (not the owner
//! lock) survives activation/checkpointing or the Model Turn. The settings writer
//! only checks markers; it never enters a Core operation while holding the lock.
//!
//! When needed, the Server journal-open fence precedes the owner fence (activate,
//! delete and authorized reads). External admission authenticates before activation,
//! releases the owner fence for activation, then reacquires and revalidates before
//! enqueue. Enqueue and reply append take the owner fence before Core's respective
//! communication/reply-store fence; Core prepare must run before either one.
use super::{Manager, safe, store};
use microcrab_permission::{Action, Matcher, Permission};
use microcrab_protocol::{ToolDefinition, application::WireError};
use microcrab_session::{
    Error, ModelAdmission, PreparedModelAdmission,
    runtime::{
        AdmittedDialogueReply, DialogueReplyAdapter, DialogueReplyPosition, DialogueReplyReceipt,
        DialogueReplyRegistration, DialogueReplyRequest,
    },
};
use microcrab_storage::SessionRecord;
use serde_json::json;
use std::sync::{Arc, Weak};

const NAME: &str = "reply_external_dialogue";
const RETIRED: &str = "reply_external_speaker";
const RETIRED_NAMES: &[&str] = &[RETIRED];

pub(in crate::manager) fn reserved(name: &str) -> bool {
    matches!(name, NAME | RETIRED)
}

pub(in crate::manager) fn current_definition_matches(
    tools: &[ToolDefinition],
    enabled: bool,
) -> bool {
    let found = tools.iter().find(|tool| tool.name == NAME);
    check_retired(tools).is_ok()
        && match found {
            Some(tool) => enabled && *tool == definition(),
            None => !enabled,
        }
}

pub(in crate::manager) fn check_retired(tools: &[ToolDefinition]) -> Result<(), WireError> {
    if tools.iter().any(|tool| tool.name == RETIRED) {
        return Err(WireError::new(
            "legacy_external_dialogue_catalog",
            "This Session uses a retired external dialogue catalog. Start a new Session.",
        ));
    }
    Ok(())
}

fn definition() -> ToolDefinition {
    ToolDefinition {
        name: NAME.into(),
        description: "Append a reply to this Turn's admitted external dialogue. Normal assistant text is not relayed. `is_final` closes only this dialogue's append stream; the Turn owns terminal settlement.".into(),
        parameters: json!({
            "type":"object",
            "properties":{
                "dialogue_id":{"type":"string","pattern":"^[0-9a-f]{8}$","minLength":8,"maxLength":8},
                "message":{"type":"string","pattern":"\\S","minLength":1,"maxLength":8192,"description":"Nonblank reply text, at most 8192 UTF-8 bytes."},
                "is_final":{"type":"boolean","default":false}
            },
            "required":["dialogue_id","message"],
            "additionalProperties":false
        }),
    }
}

#[derive(Clone)]
struct Scope {
    manager: Weak<Manager>,
    owner: String,
    session: String,
    project: Option<String>,
}

impl Scope {
    fn manager(&self) -> Result<Arc<Manager>, Error> {
        self.manager.upgrade().ok_or(Error::Closed)
    }

    /// Called only under the owner's speaker fence. A selected catalog must not
    /// survive a removed/rebound Session, including projectless root sessions.
    async fn enabled(&self, manager: &Manager) -> Result<bool, WireError> {
        let record = manager
            .directory
            .get(&self.owner, &self.session)
            .await
            .map_err(safe)?
            .filter(|record| !record.archived && record.project_id == self.project)
            .ok_or_else(super::super::unavailable)?;
        if super::super::deletion::tombstone::read(manager, &self.session)
            .await
            .is_some()
        {
            return Err(super::super::unavailable());
        }
        if let Some(project) = record.project_id.as_deref() {
            manager
                .project_external_messaging_enabled(&record.owner, project)
                .await
        } else {
            Ok(false)
        }
    }
}

pub(in crate::manager) struct Admission {
    scope: Scope,
    reply: DialogueReplyRegistration,
}

impl Admission {
    pub(in crate::manager) fn new(manager: &Manager, record: &SessionRecord) -> Arc<Self> {
        let scope = Scope {
            manager: manager.self_ref.clone(),
            owner: record.owner.clone(),
            session: record.id.clone(),
            project: record.project_id.clone(),
        };
        let reply = DialogueReplyRegistration::new(
            definition(),
            vec![Permission {
                agent: None,
                mode: None,
                tool: Some(Matcher::Exact(NAME.into())),
                args: vec![],
                action: Action::Allow,
            }],
            Arc::new(scope.clone()),
        );
        Arc::new(Self { scope, reply })
    }

    pub(in crate::manager) async fn initial_registration(
        &self,
    ) -> Result<Option<DialogueReplyRegistration>, Error> {
        let manager = self.scope.manager()?;
        let _guard = manager.speaker_gate(&self.scope.owner).lock_owned().await;
        self.scope
            .enabled(&manager)
            .await
            .map(|enabled| enabled.then(|| self.reply.clone()))
            .map_err(|cause| {
                Error::config_with_private(
                    "Project external messaging catalog is unavailable",
                    cause.message,
                )
            })
    }
}

#[async_trait::async_trait]
impl ModelAdmission for Admission {
    fn dialogue_reply(&self) -> &DialogueReplyRegistration {
        &self.reply
    }

    fn retired_tool_names(&self) -> &[&str] {
        RETIRED_NAMES
    }

    async fn prepare(&self) -> Result<PreparedModelAdmission, Error> {
        let manager = self.scope.manager()?;
        let _guard = manager.speaker_gate(&self.scope.owner).lock_owned().await;
        let enabled = self.scope.enabled(&manager).await.map_err(|cause| {
            Error::config_with_private(
                "Project external messaging catalog is unavailable",
                cause.message,
            )
        })?;
        let marker = self
            .scope
            .project
            .as_deref()
            .map(|project| manager.external_messaging_marker(&self.scope.owner, project));
        Ok(PreparedModelAdmission::new(
            Box::new(marker),
            enabled.then(|| self.reply.clone()),
        ))
    }
}

#[async_trait::async_trait]
impl DialogueReplyAdapter for Scope {
    async fn execute(
        &self,
        admitted: AdmittedDialogueReply,
        request: DialogueReplyRequest,
    ) -> Result<DialogueReplyReceipt, Error> {
        let manager = self.manager()?;
        let authority = manager.speaker_gate(&self.owner).lock_owned().await;
        match self.enabled(&manager).await {
            Ok(true) => admitted.commit(request, authority).await,
            Ok(false) => Err(Error::denied("Project external messaging is disabled")),
            Err(cause) => Err(Error::config_with_private(
                "Project external messaging reply authority is unavailable",
                cause.message,
            )),
        }
    }

    fn format_receipt(&self, position: &DialogueReplyPosition) -> String {
        json!({
            "status":"committed", "dialogue_id":position.dialogue_id(),
            "chunk_index":position.chunk_index(), "is_final":position.is_final()
        })
        .to_string()
    }
}

impl Manager {
    pub(in crate::manager) async fn reserve_project_catalog_operation(
        &self,
        record: &SessionRecord,
    ) -> Result<Option<Arc<()>>, WireError> {
        let Some(project) = record.project_id.as_deref() else {
            return Ok(None);
        };
        let _owner = self.speaker_gate(&record.owner).lock_owned().await;
        self.editable_project(&record.owner, project).await?;
        Ok(Some(self.external_messaging_marker(&record.owner, project)))
    }

    /// Only under the owner fence. All clones of the marker witness an in-flight
    /// activation/admission for the same owner/project. No drop path takes a lock.
    pub(in crate::manager) fn external_messaging_marker(
        &self,
        owner: &str,
        project: &str,
    ) -> Arc<()> {
        let mut markers = self
            .external_messaging_markers
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        markers.retain(|_, marker| marker.strong_count() > 0);
        let key = (owner.to_owned(), project.to_owned());
        if let Some(marker) = markers.get(&key).and_then(Weak::upgrade) {
            return marker;
        }
        let marker = Arc::new(());
        markers.insert(key, Arc::downgrade(&marker));
        marker
    }

    pub(in crate::manager) fn require_quiet_external_messaging(
        &self,
        owner: &str,
        project: &str,
    ) -> Result<(), WireError> {
        if self
            .external_messaging_markers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(owner.to_owned(), project.to_owned()))
            .is_some_and(|marker| marker.strong_count() > 0)
        {
            return Err(WireError::new(
                "project_external_messaging_busy",
                "Retry after affected Session Turns and activations have settled; no external messaging settings were written.",
            ));
        }
        Ok(())
    }

    pub(in crate::manager) async fn project_external_messaging_enabled(
        &self,
        owner: &str,
        project: &str,
    ) -> Result<bool, WireError> {
        self.ensure_external_messaging_available(owner)?;
        let project = self.editable_project(owner, project).await?;
        let (_, config) = store::load(self.configuration.as_ref(), owner).await?;
        Ok(!project.provisional && config.enabled(Some(&project.id)))
    }
}
