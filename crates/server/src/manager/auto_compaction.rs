//! Explicit owner-admitted compaction retry. Durable operation intent fences HTTP
//! replay; this command sends no main request and never renews Turn allowances.
use super::{Manager, commands, now, safe, view};
use microcrab_protocol::application::{Command, WireError};
use microcrab_storage::{OperationRecord, RECORD_VERSION, SessionRecord};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::atomic::Ordering;

struct ReceiptOwner(std::sync::Arc<super::hub::Hub>);
impl Drop for ReceiptOwner {
    fn drop(&mut self) {
        self.0.set_operation(None);
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Retry {
    pub operation_id: String,
    pub expected_revision: u64,
}
async fn receipt(
    manager: &Manager,
    owner: &str,
    record: &SessionRecord,
    params: &Retry,
    digest: &str,
) -> Result<bool, WireError> {
    let Some(operation) = manager
        .directory
        .get_operation(owner, &params.operation_id)
        .await
        .map_err(safe)?
    else {
        return Ok(false);
    };
    if operation.session_id != record.id
        || operation.method != commands::SESSION_RETRY_AUTO_COMPACTION
        || operation.payload_hash != digest
    {
        return Err(WireError::new(
            "operation_conflict",
            "Operation ID belongs to another request",
        ));
    }
    match operation.status.as_str() {
        "completed" => Ok(true),
        "failed" => Err(operation
            .result
            .and_then(|result| result.get("error").cloned())
            .and_then(|error| serde_json::from_value(error).ok())
            .unwrap_or_else(|| {
                WireError::new(
                    "auto_compaction_failed",
                    "Compaction retry failed; inspect retained Session context",
                )
            })),
        _ => Err(WireError::new(
            "operation_interrupted",
            "Compaction retry was already admitted; inspect its outcome before another explicit attempt",
        )),
    }
}
pub(super) async fn retry(
    manager: &Manager,
    owner: &str,
    record: SessionRecord,
    command: &Command,
) -> Result<Value, WireError> {
    let params: Retry = commands::parse(command)?;
    uuid::Uuid::parse_str(&params.operation_id)
        .map_err(|_| WireError::new("invalid_request", "Invalid operation ID"))?;
    let digest = commands::hash(command)?;
    if receipt(manager, owner, &record, &params, &digest).await? {
        return manager.inspect(&record).await;
    }
    let managed = manager.active(record, true).await?;
    let _command = managed.commands.lock().await;
    let record = managed.record.lock().await.clone();
    if receipt(manager, owner, &record, &params, &digest).await? {
        // Archive/delete own Journal admission before waiting for commands.
        // A duplicate lookup must release commands before inspecting history.
        drop(_command);
        return manager.inspect(&record).await;
    }
    if record.archived
        || managed.closing.load(Ordering::Acquire)
        || managed.faulted.load(Ordering::Acquire)
        || managed.suspended.load(Ordering::Acquire)
    {
        return Err(WireError::new(
            "closed",
            "Session is unavailable for compaction retry",
        ));
    }
    if record.revision != params.expected_revision {
        return Err(WireError::new(
            "revision_conflict",
            "Session revision changed",
        ));
    }
    if managed.hub.operation().is_some() || managed.session.inbox_snapshot().active {
        return Err(WireError::new("busy", "Session is busy"));
    }
    if managed.session.retry_ready() {
        return Err(WireError::new(
            "prepared_request",
            "Retry the prepared failed model request before compacting",
        ));
    }
    manager.record_host(&record).await?;
    let timestamp = now();
    let mut operation = manager
        .directory
        .create_operation(OperationRecord {
            version: RECORD_VERSION,
            id: params.operation_id,
            session_id: record.id.clone(),
            owner: owner.into(),
            method: commands::SESSION_RETRY_AUTO_COMPACTION.into(),
            payload_hash: digest,
            status: "accepted".into(),
            result: Some(json!({"server_instance":manager.instance})),
            revision: 0,
            created_at_ms: timestamp,
            updated_at_ms: timestamp,
        })
        .await
        .map_err(safe)?;
    managed.hub.set_operation(Some(operation.id.clone()));
    let receipt_owner = ReceiptOwner(managed.hub.clone());
    // Durable intent and Hub ownership now fence competing mutations. Close must
    // acquire this gate to signal Session cancellation while the helper awaits IO.
    drop(_command);
    let result = managed.session.retry_auto_compaction().await.map_err(safe);
    operation.revision = 1;
    operation.updated_at_ms = now().max(timestamp);
    operation.status = if result.is_ok() {
        "completed"
    } else {
        "failed"
    }
    .into();
    operation.result = Some(match &result {
        Ok(compacted) => json!({"compacted":compacted}),
        Err(error) => json!({"error":error}),
    });
    manager
        .directory
        .replace_operation(owner, 0, operation)
        .await
        .map_err(safe)?;
    drop(receipt_owner);
    let snapshot = manager.refresh(&managed).await.map_err(safe)?;
    result?;
    view::bound_snapshot(snapshot).map_err(safe)
}
