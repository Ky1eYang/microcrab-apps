//! Owner-scoped soft deletion. Project publication follows child cleanup, so a
//! failed cascade leaves a visible project that can be retried without orphaning
//! conversations. Creation/fork admission shares the identity gate with deletion.
use super::{Manager, commands, now, safe, unavailable, view};
use microcrab_protocol::application::WireError;
use serde_json::{Value, json};

pub(super) async fn archived_view(
    manager: &Manager,
    record: &microcrab_storage::SessionRecord,
) -> Result<Value, WireError> {
    let mut value = view::session(manager, record, &manager.config.provider.model, "closed");
    let path = manager.config.data_dir.join("sessions").join(&record.id);
    let point = manager.child_checkpoint(&path, &record.id).await?;
    value["mode"] = json!(
        point
            .as_ref()
            .and_then(|point| point.checkpoint.recover.state.selected_mode.as_deref())
    );
    view::communication_identity(
        &mut value,
        point
            .as_ref()
            .and_then(|point| point.checkpoint.recover.state.communication.as_ref())
            .map(|communication| &communication.identity),
    );
    Ok(value)
}

pub(super) async fn archive_session(
    manager: &Manager,
    owner: &str,
    id: &str,
    expected_revision: Option<u64>,
) -> Result<Value, WireError> {
    // Activation owns the same gate through runtime publication. Without this
    // fence an inactive archive could race a restore and leave a live writer.
    let _activation = manager.journal_open_gate(id).lock_owned().await;
    let record = manager
        .directory
        .get(owner, id)
        .await
        .map_err(safe)?
        .ok_or_else(unavailable)?;
    // Archive is idempotent on its target state: a retried or duplicated request
    // returns the settled view, never an error and never another revision.
    if record.archived {
        return archived_view(manager, &record).await;
    }
    if expected_revision.is_some_and(|expected| record.revision != expected) {
        return Err(WireError::new(
            "revision_conflict",
            "Session revision changed",
        ));
    }
    let slot = manager
        .registry
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(id)
        .cloned();
    let active = match slot {
        Some(slot) => slot.active.lock().await.clone(),
        None => None,
    };
    if let Some(managed) = active {
        return commands::archive_managed(manager, &managed, owner, id, expected_revision).await;
    }
    let revision = record.revision;
    let mut next = record;
    next.archived = true;
    next.revision = revision
        .checked_add(1)
        .ok_or_else(|| WireError::new("invalid_request", "Session revision exhausted"))?;
    next.updated_at_ms = now().max(next.updated_at_ms);
    let saved = manager
        .directory
        .replace(owner, revision, next)
        .await
        .map_err(safe)?;
    archived_view(manager, &saved).await
}

/// Irreversible removal with an idempotent, fast acknowledgement.
///
/// The request writes an owner-scoped tombstone, removes the directory record
/// (so list/attach/commands see the session as gone) and returns. Runtime
/// release, audit purge and byte removal run in a tracked background task; the
/// destructive close skips the full terminal checkpoint. A tombstone left in
/// `cleanup_pending` is resumed at startup, and a repeated DELETE for the same
/// owner answers from the tombstone instead of 404.
pub(super) async fn delete_session(
    manager: &Manager,
    owner: &str,
    id: &str,
    expected_revision: Option<u64>,
) -> Result<Value, WireError> {
    if !crate::config::valid_id(id) {
        return Err(unavailable());
    }
    // The same fence archive uses: deletion cannot race a restore or activation.
    let _activation = manager.journal_open_gate(id).lock_owned().await;
    let Some(record) = manager.directory.get(owner, id).await.map_err(safe)? else {
        return match tombstone::read(manager, id).await {
            Some(stone) if stone.owner == owner => {
                // A retry of an unfinished cleanup resumes it; a settled one is
                // answered from the receipt with the same result.
                if stone.state == tombstone::CLEANUP_PENDING {
                    schedule_cleanup(manager, owner, id);
                }
                Ok(json!({
                    "deleted": true,
                    "session_id": id,
                    "state": stone.state,
                    "failure": stone.failure,
                    "removed_operation_receipts": 0,
                }))
            }
            _ => Err(unavailable()),
        };
    };
    if !manager.project_visible(&record).await {
        return Err(unavailable());
    }
    if expected_revision.is_some_and(|expected| record.revision != expected) {
        return Err(WireError::new(
            "revision_conflict",
            "Session revision changed",
        ));
    }
    let active = {
        let slot = manager
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(id)
            .cloned();
        match slot {
            Some(slot) => slot.active.lock().await.clone(),
            None => None,
        }
    };
    // Activation excludes restore. Seal foreground before waiting for commands:
    // an already submitted naming CAS may own that gate while Directory stalls.
    if let Some(managed) = active.as_ref() {
        managed
            .closing
            .store(true, std::sync::atomic::Ordering::Release);
        managed
            .session
            .interrupt_with_reason(microcrab_session::InterruptReason::SessionClosed);
        managed.close_token.cancel();
        managed.hub.interrupt_operation();
    }
    let _commands = match active.as_ref() {
        Some(managed) => Some(managed.commands.lock().await),
        None => None,
    };
    if let Some(managed) = active.as_ref() {
        super::naming::stop(managed).await.map_err(|error| {
            let detail = super::CleanupDetail::from_join(error);
            WireError::new(detail.code, detail.message)
        })?;
    }
    // A started reply can retain the owner guard beyond its waiter. Fence
    // tombstone publication after that ACK, without waiting for a Turn here.
    let _owner = manager.speaker_gate(owner).lock_owned().await;
    tombstone::write(manager, id, owner, tombstone::CLEANUP_PENDING, None)
        .await
        .map_err(|_| removal_failed())?;
    manager.forget_dormant(id);
    manager
        .read_hubs
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(id);
    let receipts = match remove_record(manager, owner, id, record.revision).await {
        Ok(receipts) => receipts,
        Err(error) => {
            // The durable intent is already published. A retry or restart must
            // finish metadata removal before it can purge any session bytes.
            schedule_cleanup(manager, owner, id);
            return Err(error);
        }
    };
    schedule_cleanup(manager, owner, id);
    Ok(json!({
        "deleted": true,
        "session_id": id,
        "state": tombstone::CLEANUP_PENDING,
        "removed_operation_receipts": receipts,
    }))
}

/// Cleanup ownership is scoped to one Manager, so cancellation and independent
/// data roots cannot suppress a later recovery attempt.
static RUNNING: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<(String, String)>>> =
    std::sync::LazyLock::new(Default::default);

struct CleanupOwner((String, String));
impl Drop for CleanupOwner {
    fn drop(&mut self) {
        RUNNING
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.0);
    }
}

/// Remove the current metadata revision while admission is fenced. An owned
/// turn can still advance its revision while observing the deletion seal.
async fn remove_record(
    manager: &Manager,
    owner: &str,
    id: &str,
    mut revision: u64,
) -> Result<usize, WireError> {
    for _ in 0..8 {
        match manager.directory.remove(owner, id, revision).await {
            Ok(count) => return Ok(count),
            Err(error) => match manager.directory.get(owner, id).await.map_err(safe)? {
                Some(current) if current.revision != revision => revision = current.revision,
                Some(_) => return Err(safe(error)),
                None => return Ok(0),
            },
        }
    }
    Err(WireError::new(
        "revision_conflict",
        "Session deletion could not settle the changing directory revision",
    ))
}

/// Reconcile durable intent before any byte deletion. The gate prevents a
/// surviving directory record from being activated during restart recovery.
async fn reconcile_record(manager: &Manager, owner: &str, id: &str) -> Result<(), WireError> {
    let _activation = manager.journal_open_gate(id).lock_owned().await;
    manager.forget_dormant(id);
    manager
        .read_hubs
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(id);
    if let Some(record) = manager.directory.get(owner, id).await.map_err(safe)? {
        let slot = manager
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(id)
            .cloned();
        if let Some(slot) = slot
            && let Some(managed) = slot.active.lock().await.as_ref()
        {
            managed
                .closing
                .store(true, std::sync::atomic::Ordering::Release);
            managed
                .session
                .interrupt_with_reason(microcrab_session::InterruptReason::SessionClosed);
            managed.close_token.cancel();
            managed.hub.interrupt_operation();
            let _commands = managed.commands.lock().await;
            super::naming::stop(managed).await.map_err(|error| {
                let detail = super::CleanupDetail::from_join(error);
                WireError::new(detail.code, detail.message)
            })?;
        }
        let _owner = manager.speaker_gate(owner).lock_owned().await;
        remove_record(manager, owner, id, record.revision).await?;
    }
    Ok(())
}

/// Release the sealed runtime (keeping quarantine on failure), then purge audit
/// rows and bytes. Any failure stays in the tombstone with its sanitized cause
/// so a later DELETE reports it and retries.
fn schedule_cleanup(manager: &Manager, owner: &str, id: &str) {
    let Some(this) = manager.self_ref.upgrade() else {
        return;
    };
    let key = (manager.instance.clone(), id.to_owned());
    if !RUNNING
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(key.clone())
    {
        return;
    }
    let cleanup_owner = CleanupOwner(key);
    let (owner, id) = (owner.to_owned(), id.to_owned());
    let task = async move {
        let _cleanup_owner = cleanup_owner;
        let mut outcome = reconcile_record(&this, &owner, &id).await;
        let mut stage = "directory";
        let active = {
            let slot = this
                .registry
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&id)
                .cloned();
            match slot {
                Some(slot) => slot.active.lock().await.clone(),
                None => None,
            }
        };
        if outcome.is_ok()
            && let Some(managed) = active
        {
            stage = "runtime";
            outcome = commands::settle_for_delete(&this, &managed, &owner, &id).await;
        }
        if outcome.is_ok() {
            stage = "storage";
            outcome = cleanup_bytes(&this, &owner, &id).await;
        }
        let written = match &outcome {
            Ok(()) => tombstone::write(&this, &id, &owner, tombstone::DELETED, None).await,
            Err(error) => {
                tracing::warn!(error_code = %error.code, "Deleted session cleanup failed; retained for retry");
                tombstone::write(
                    &this,
                    &id,
                    &owner,
                    tombstone::CLEANUP_PENDING,
                    serde_json::to_value(error).ok(),
                )
                .await
            }
        };
        if written.is_err() {
            tracing::warn!("Deleted session receipt update failed");
        }
        // The owner's list learns the settled result: a final `remove` with the
        // receipt state, or the failed stage and its sanitized cause.
        match &outcome {
            Ok(()) => this.session_dirty(&owner, &id),
            Err(error) => this.session_cleanup_failed(
                &owner,
                &id,
                stage,
                serde_json::to_value(error).unwrap_or(serde_json::Value::Null),
            ),
        }
    };
    if manager.track(task).is_err() {
        tracing::warn!("Deleted session cleanup deferred until restart");
    }
}

async fn cleanup_bytes(manager: &Manager, owner: &str, id: &str) -> Result<(), WireError> {
    if manager.config.storage == crate::config::Storage::Sqlite {
        #[cfg(feature = "storage-sqlite")]
        {
            let path = manager.config.data_dir.join("storage.sqlite");
            let owner = owner.to_owned();
            let id = id.to_owned();
            tokio::task::spawn_blocking(move || {
                microcrab_storage::audit::SqliteAuditStore::purge_root(&path, &owner, &id)
            })
            .await
            .map_err(|_| removal_failed())?
            .map_err(safe)?;
        }
    }
    #[cfg(not(feature = "storage-sqlite"))]
    let _ = owner;
    remove_session_directory(manager, id).await
}

/// Startup convergence for deletions acknowledged before a crash or shutdown.
pub(super) async fn resume_pending_cleanup(
    manager: &std::sync::Arc<Manager>,
) -> Result<(), WireError> {
    for (id, stone) in tombstone::pending(manager).await? {
        reconcile_record(manager, &stone.owner, &id).await?;
        schedule_cleanup(manager, &stone.owner, &id);
    }
    Ok(())
}

/// Tests observe the acknowledged background cleanup through its tombstone.
#[cfg(test)]
pub(super) async fn cleanup_outcome(manager: &Manager, id: &str) -> Option<tombstone::Tombstone> {
    for _ in 0..1000 {
        let running = RUNNING
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains(&(manager.instance.clone(), id.to_owned()));
        if !running {
            return tombstone::read(manager, id).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    None
}

#[cfg(test)]
pub(super) async fn cleanup_settled(manager: &Manager, id: &str) -> bool {
    cleanup_outcome(manager, id)
        .await
        .is_some_and(|stone| stone.state == tombstone::DELETED)
}

/// Owner-scoped deletion receipts. They live outside the session tree and the
/// directory so they survive both removals; they hold no conversation bytes.
pub(super) mod tombstone {
    use super::Manager;
    use serde::{Deserialize, Serialize};

    pub(crate) const CLEANUP_PENDING: &str = "cleanup_pending";
    pub(crate) const DELETED: &str = "deleted";

    #[derive(Serialize, Deserialize)]
    pub(crate) struct Tombstone {
        pub owner: String,
        pub state: String,
        pub deleted_at_ms: u64,
        /// Sanitized wire error of the last failed cleanup attempt.
        #[serde(default)]
        pub failure: Option<serde_json::Value>,
    }

    fn root(manager: &Manager) -> std::path::PathBuf {
        manager.config.data_dir.join("deleted-sessions")
    }

    pub(crate) async fn read(manager: &Manager, id: &str) -> Option<Tombstone> {
        let path = root(manager).join(format!("{id}.json"));
        let bytes = tokio::fs::read(path).await.ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Atomic replace (write temp, fsync, rename) so a crash never leaves a
    /// partial receipt that would turn an idempotent retry into a 404.
    pub(crate) async fn write(
        manager: &Manager,
        id: &str,
        owner: &str,
        state: &str,
        failure: Option<serde_json::Value>,
    ) -> std::io::Result<()> {
        let dir = root(manager);
        let stone = Tombstone {
            owner: owner.to_owned(),
            state: state.to_owned(),
            deleted_at_ms: super::now(),
            failure,
        };
        let bytes = serde_json::to_vec(&stone).map_err(std::io::Error::other)?;
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            use std::io::Write;
            std::fs::create_dir_all(&dir)?;
            let target = dir.join(format!("{id}.json"));
            let temp = dir.join(format!(".{id}.{}.tmp", uuid::Uuid::new_v4()));
            let mut file = std::fs::File::create(&temp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&temp, &target)?;
            #[cfg(unix)]
            std::fs::File::open(&dir)?.sync_all()?;
            // Windows has no portable std directory fsync. The receipt file was
            // flushed before its atomic rename, matching storage durability.
            Ok(())
        })
        .await
        .map_err(std::io::Error::other)?
    }

    pub(crate) async fn pending(
        manager: &Manager,
    ) -> Result<Vec<(String, Tombstone)>, microcrab_protocol::application::WireError> {
        use microcrab_protocol::application::WireError;
        let dir = root(manager);
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(error) => return Err(receipt_io("enumerate deletion receipts", error)),
        };
        let mut pending = vec![];
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|error| receipt_io("enumerate deletion receipt entry", error))?
        {
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(id) = name.strip_suffix(".json") else {
                continue;
            };
            if !crate::config::valid_id(id) {
                continue;
            }
            let bytes = tokio::fs::read(entry.path())
                .await
                .map_err(|error| receipt_io("read deletion receipt", error))?;
            let stone: Tombstone = serde_json::from_slice(&bytes).map_err(|error| {
                WireError::new(
                    "storage",
                    format!(
                        "Deletion receipt JSON is invalid ({:?} at line {}, column {})",
                        error.classify(),
                        error.line(),
                        error.column(),
                    ),
                )
            })?;
            if stone.state == CLEANUP_PENDING {
                pending.push((id.to_owned(), stone));
            } else if stone.state != DELETED {
                return Err(WireError::new(
                    "storage",
                    "Deletion receipt contains an unsupported cleanup state",
                ));
            }
        }
        Ok(pending)
    }

    fn receipt_io(
        action: &str,
        error: std::io::Error,
    ) -> microcrab_protocol::application::WireError {
        microcrab_protocol::application::WireError::new(
            "storage",
            format!(
                "Cannot {action} (I/O kind {:?}, OS code {:?})",
                error.kind(),
                error.raw_os_error(),
            ),
        )
    }
}

/// Delete one session directory tree under the startup sessions root. The root is
/// canonicalized and the target must be a real child of it, so no link redirects
/// removal outside the data directory.
async fn remove_session_directory(manager: &Manager, id: &str) -> Result<(), WireError> {
    let root = manager.config.data_dir.join("sessions");
    let target = root.join(id);
    tokio::task::spawn_blocking(move || {
        if !root.try_exists().map_err(|error| removal_io("inspect sessions root", error))? {
            return Ok(());
        }
        let root = root.canonicalize().map_err(|error| removal_io("resolve sessions root", error))?;
        match std::fs::symlink_metadata(&target) {
            Ok(metadata) => {
                #[cfg(windows)]
                let linked = {
                    use std::os::windows::fs::MetadataExt;
                    metadata.file_attributes() & 0x400 != 0
                };
                #[cfg(not(windows))]
                let linked = metadata.file_type().is_symlink();
                if linked || !metadata.is_dir() {
                    return Err(WireError::new("operation_failed", "Session removal target must be an ordinary directory, not a link or reparse point"));
                }
                let actual = target.canonicalize().map_err(|error| removal_io("resolve session directory", error))?;
                if actual.parent() != Some(root.as_path()) {
                    return Err(WireError::new("operation_failed", "Session removal target is outside the sessions root"));
                }
                std::fs::remove_dir_all(&actual).map_err(|error| removal_io("remove session directory", error))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(removal_io("inspect session directory", error)),
        }
    })
    .await
    .map_err(|error| WireError::new("operation_failed", if error.is_panic() {
        "Session directory removal worker panicked"
    } else { "Session directory removal worker was cancelled" }))?
}

fn removal_io(action: &'static str, error: std::io::Error) -> WireError {
    WireError::new(
        "operation_failed",
        format!(
            "Cannot {action} (I/O kind {:?}, OS code {:?})",
            error.kind(),
            error.raw_os_error()
        ),
    )
}

fn removal_failed() -> WireError {
    WireError::new("operation_failed", "Session directory removal failed")
}

pub(super) async fn delete_project(
    manager: &Manager,
    owner: &str,
    project_id: &str,
) -> Result<Value, WireError> {
    let _identity = manager.identity_gate.lock().await;
    manager.editable_project(owner, project_id).await?;
    let records = manager.directory.list(owner).await.map_err(safe)?;
    let mut deleted_session_ids = Vec::new();
    for record in records
        .into_iter()
        .filter(|record| record.project_id.as_deref() == Some(project_id))
    {
        // Project removal is irreversible too, so its sessions are deleted rather
        // than archived: no orphaned archived records or retained bytes remain.
        delete_session(manager, owner, &record.id, Some(record.revision)).await?;
        deleted_session_ids.push(record.id);
    }
    // Only publish the parent removal after every owned conversation is gone, so a
    // failed cascade leaves a visible project that can be retried.
    {
        let _owner = manager.speaker_gate(owner).lock_owned().await;
        manager
            .retire_project_external_messaging_locked(owner, project_id)
            .await?;
        manager
            .projects
            .delete(owner, project_id)
            .await
            .map_err(|_| unavailable())?;
    }
    if let Some(route) = manager.routes.lock().await.remove(project_id) {
        route.remote.detach(route.generation);
    }
    manager.project_host_changed(project_id);
    manager.suspend_project(project_id).await;
    manager.retire_project(project_id);
    Ok(json!({"deleted":true,"project_id":project_id,"deleted_session_ids":deleted_session_ids}))
}
