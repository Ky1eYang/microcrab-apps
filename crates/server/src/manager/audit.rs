//! Authorized audit reads do not assemble a runtime or require a connected Host.
#[cfg(test)]
mod tests;

use super::{Manager, hub::Hub};
use microcrab_creator::audit::{AuditQuery, AuditQueryResult, CommitObserver};
use microcrab_protocol::application::WireError;
use std::sync::Arc;

pub(super) struct Request {
    pub selector: AuditQuery,
    pub child_session_id: Option<String>,
    pub internal_assistants: bool,
}
impl<'de> serde::Deserialize<'de> for Request {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // serde flatten can swallow unknown fields instead of applying the inner
        // selector's deny_unknown_fields. Remove only the server-owned scope and
        // let the authoritative creator DTO validate every remaining selector.
        let mut fields =
            <serde_json::Map<String, serde_json::Value> as serde::Deserialize>::deserialize(
                deserializer,
            )?;
        let child_session_id = fields
            .remove("child_session_id")
            .map(serde_json::from_value::<Option<String>>)
            .transpose()
            .map_err(serde::de::Error::custom)?
            .flatten();
        let internal_assistants = fields
            .remove("internal_assistants")
            .map(serde_json::from_value::<bool>)
            .transpose()
            .map_err(serde::de::Error::custom)?
            .unwrap_or(false);
        let selector = serde_json::from_value(serde_json::Value::Object(fields))
            .map_err(serde::de::Error::custom)?;
        Ok(Self {
            selector,
            child_session_id,
            internal_assistants,
        })
    }
}

pub(super) async fn query(
    manager: &Manager,
    owner: &str,
    session_id: &str,
    request: Request,
) -> Result<AuditQueryResult, WireError> {
    let Request {
        selector,
        child_session_id,
        internal_assistants,
    } = request;
    if child_session_id.as_ref().is_some_and(|id| {
        id.len() != 16
            || !id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }) {
        return Err(WireError::new(
            "invalid_params",
            "Invalid child Session identity",
        ));
    }
    // Preserve canonical visibility without selected_record's legacy migration.
    if !crate::config::valid_id(session_id) {
        return Err(super::unavailable());
    }
    let record = manager
        .directory
        .get(owner, session_id)
        .await
        .map_err(super::safe)?
        .ok_or_else(super::unavailable)?;
    if !manager.project_visible(&record).await {
        return Err(super::unavailable());
    }
    let sessions = manager.config.data_dir.join("sessions");
    let directory = sessions.join(record.id);
    let is_child = child_session_id.is_some();
    let audit_id = child_session_id
        .clone()
        .unwrap_or_else(|| session_id.to_owned());
    let directory = tokio::task::spawn_blocking(move || {
        if directory.try_exists().map_err(|_| {
            microcrab_auditor::AuditorError("audit directory metadata failed".into())
        })? {
            let root = sessions.canonicalize().map_err(|_| {
                microcrab_auditor::AuditorError("audit sessions root unavailable".into())
            })?;
            let actual = directory.canonicalize().map_err(|_| {
                microcrab_auditor::AuditorError("audit session directory unavailable".into())
            })?;
            if actual.parent() != Some(root.as_path()) {
                return Err(microcrab_auditor::AuditorError(
                    "audit session directory escapes startup sessions root".into(),
                ));
            }
        }
        let directory = if let Some(child) = child_session_id {
            let children = directory.join("subsessions");
            let child_directory = children.join(child);
            if child_directory.try_exists().map_err(|_| {
                microcrab_auditor::AuditorError("audit child metadata failed".into())
            })? {
                let parent_root = directory.canonicalize().map_err(|_| {
                    microcrab_auditor::AuditorError("audit parent directory unavailable".into())
                })?;
                let children_root = children.canonicalize().map_err(|_| {
                    microcrab_auditor::AuditorError("audit children directory unavailable".into())
                })?;
                let child_root = child_directory.canonicalize().map_err(|_| {
                    microcrab_auditor::AuditorError("audit child directory unavailable".into())
                })?;
                if children_root.parent() != Some(parent_root.as_path())
                    || child_root.parent() != Some(children_root.as_path())
                {
                    return Err(microcrab_auditor::AuditorError(
                        "audit child directory escapes authorized parent".into(),
                    ));
                }
            }
            child_directory
        } else {
            directory
        };
        Ok::<_, microcrab_auditor::AuditorError>(directory)
    })
    .await
    .map_err(|_| WireError::new("audit_unavailable", "Audit path worker failed"))?
    .map_err(|error| WireError::new("audit_unavailable", error.to_string()))?;
    let audit_source = source(
        &manager.config,
        owner,
        session_id,
        &audit_id,
        directory.clone(),
    );
    if manager.config.storage == crate::config::Storage::Sqlite {
        let inspect = audit_source.clone();
        let scoped = tokio::task::spawn_blocking(move || inspect.revision())
            .await
            .map_err(|_| WireError::new("audit_unavailable", "Audit scope worker failed"))?
            .map_err(|error| WireError::new("audit_unavailable", error.to_string()))?
            .is_some();
        if !scoped {
            // A foreign/missing child must not cause even detached Journal reads
            // outside the authorized parent scope. The common reader supplies the
            // ordinary Missing response without materializing another session.
            return tokio::task::spawn_blocking(move || {
                if internal_assistants {
                    microcrab_creator::audit::query_internal(audit_source, &selector)
                } else {
                    microcrab_creator::audit::query(audit_source, &selector)
                }
            })
            .await
            .map_err(|_| WireError::new("audit_unavailable", "Audit query worker failed"))?
            .map_err(|error| WireError::new("audit_unavailable", error.to_string()));
        }
    }
    let mut anchor = microcrab_storage::history::AuditAnchor::default();
    {
        // Inspection never opens a writer or activates a Session. A live parent
        // supplies the exact in-flight anchor; detached/child reads use storage.
        let slot = if is_child {
            None
        } else {
            manager
                .registry
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(session_id)
                .cloned()
        };
        let mut live = false;
        if let Some(slot) = slot {
            let active = slot.active.lock().await;
            if let Some(managed) = active.as_ref() {
                anchor = managed
                    .journal
                    .history_anchor()
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone();
                live = true;
            }
        }
        if !live && let Some(point) = manager.child_checkpoint(&directory, &audit_id).await? {
            anchor.history_head = Some(point.cursor.checkpoint_id);
            anchor.request_head = point.checkpoint.request_head;
        }
    }
    let needs_ancestry = internal_assistants || selector.subagent.is_some();
    let history_ancestry = if needs_ancestry && let Some(head) = anchor.history_head.clone() {
        let ancestry = match manager.config.storage {
            crate::config::Storage::File => {
                #[cfg(feature = "storage-file")]
                {
                    microcrab_storage::history::FileJournal::inspect_ancestry(
                        &directory,
                        audit_id.clone(),
                        head,
                    )
                    .await
                }
                #[cfg(not(feature = "storage-file"))]
                {
                    Err(microcrab_session::Error::config(
                        "File storage is not compiled",
                    ))
                }
            }
            crate::config::Storage::Sqlite => {
                #[cfg(feature = "storage-sqlite")]
                {
                    microcrab_storage::history::SqliteJournal::inspect_ancestry(
                        manager.config.data_dir.join("storage.sqlite"),
                        audit_id.clone(),
                        head,
                    )
                    .await
                }
                #[cfg(not(feature = "storage-sqlite"))]
                {
                    Err(microcrab_session::Error::config(
                        "SQLite storage is not compiled",
                    ))
                }
            }
        };
        ancestry.map_err(|_| {
            WireError::new(
                "audit_unavailable",
                "Selected Journal ancestry is unavailable",
            )
        })?
    } else {
        Vec::new()
    };
    let directory = audit_source;
    tokio::task::spawn_blocking(move || {
        if internal_assistants {
            microcrab_creator::audit::query_internal_selected_with_ancestry(
                directory,
                &selector,
                anchor.request_head.as_deref(),
                anchor.history_head.as_deref(),
                &history_ancestry,
            )
        } else if needs_ancestry && (anchor.request_head.is_some() || anchor.history_head.is_some())
        {
            microcrab_creator::audit::query_selected_with_ancestry(
                directory,
                &selector,
                anchor.request_head.as_deref(),
                anchor.history_head.as_deref(),
                &history_ancestry,
            )
        } else if anchor.request_head.is_some() || anchor.history_head.is_some() {
            microcrab_creator::audit::query_selected(
                directory,
                &selector,
                anchor.request_head.as_deref(),
                anchor.history_head.as_deref(),
            )
        } else {
            microcrab_creator::audit::query(directory, &selector)
        }
    })
    .await
    .map_err(|_| WireError::new("audit_unavailable", "Audit query worker failed"))?
    .map_err(|error| WireError::new("audit_unavailable", error.to_string()))
}

/// Writer callbacks occur after sync_all, rather than before the writer queue.
pub(super) fn observer(hub: Arc<Hub>) -> CommitObserver {
    Arc::new(move |commit| {
        if let Ok(value) = serde_json::to_value(commit) {
            hub.publish(super::SessionRuntimeEvent::AuditAvailable(value));
        }
    })
}

/// One resolved backend supplies every audit consumer; owner/root pins are never
/// inferred from a child-controlled path.
pub(super) fn source(
    config: &crate::config::Config,
    owner: &str,
    root: &str,
    session: &str,
    directory: std::path::PathBuf,
) -> microcrab_storage::audit::AuditSource {
    match config.storage {
        crate::config::Storage::File => microcrab_storage::audit::AuditSource::File(directory),
        crate::config::Storage::Sqlite => microcrab_storage::audit::AuditSource::Sqlite {
            database: config.data_dir.join("storage.sqlite"),
            scope: microcrab_storage::audit::AuditScope::new(owner, root, session),
        },
    }
}

#[cfg(all(test, feature = "storage-sqlite"))]
mod sqlite_tests;
