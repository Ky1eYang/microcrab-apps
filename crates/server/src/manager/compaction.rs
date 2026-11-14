//! Session-owned manual compaction. HTTP admits once; receipt settlement survives
//! requester disconnects and publishes success only after durable summary install.
use super::{Managed, Manager, commands, now, safe, view};
use futures_util::FutureExt;
use microcrab_protocol::application::{Command, WireError};
use microcrab_session::Error;
use microcrab_storage::{OperationRecord, RECORD_VERSION, directory::OperationSettlement};
use serde_json::{Value, json};
use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, atomic::Ordering},
};

const DEFAULT_INSTRUCTION: &str = "Preserve the user's objective, decisions, constraints, exact relevant paths, verified work, unresolved failures and the immediate next action so work can continue accurately.";

/// Caller holds Managed.commands through admission, not through model execution.
pub(super) async fn start(
    manager: &Manager,
    managed: Arc<Managed>,
    command: &Command,
    params: commands::Compact,
) -> Result<Value, WireError> {
    if params.instruction.len() > 32768 {
        return Err(WireError::new(
            "invalid_request",
            "Compaction instruction exceeds budget",
        ));
    }
    let id = params
        .operation_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    uuid::Uuid::parse_str(&id)
        .map_err(|_| WireError::new("invalid_request", "Invalid operation identity"))?;
    let digest = commands::hash(command)?;
    let record = managed.record.lock().await.clone();
    if let Some(receipt) = manager
        .directory
        .get_operation(&record.owner, &id)
        .await
        .map_err(safe)?
    {
        if receipt.session_id != record.id
            || receipt.method != command.method
            || receipt.payload_hash != digest
        {
            return Err(WireError::new(
                "operation_conflict",
                "Operation ID was used for another request",
            ));
        }
        commands::reconcile(manager, receipt).await?;
        return manager.refresh(&managed).await.map_err(safe);
    }
    if managed.hub.operation().is_some() || managed.session.inbox_snapshot().active {
        return Err(WireError::new(
            "busy",
            "Finish or recover the current turn before compaction",
        ));
    }
    let instruction = if params.instruction.trim().is_empty() {
        DEFAULT_INSTRUCTION.to_owned()
    } else {
        params.instruction
    };
    let timestamp = now();
    let operation = manager
        .directory
        .create_operation(OperationRecord {
            version: RECORD_VERSION,
            id,
            session_id: record.id,
            owner: record.owner,
            method: command.method.clone(),
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
    let cancellation = managed
        .hub
        .operation_cancellation()
        .expect("operation admission installs its cancellation token under the command gate");
    let directory = manager.directory.clone();
    let model = manager.config.provider.model.clone();
    let owned = managed.clone();
    let worker = tokio::spawn(async move {
        owned.hub.publish(super::SessionRuntimeEvent::Compaction(
            json!({"operation_id":operation.id,"status":"running"}),
        ));
        let result = AssertUnwindSafe(async {
            if owned.close_token.is_cancelled() || cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            owned
                .session
                .compact_with_model_cancellation(instruction, json!({}), cancellation)
                .await
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(Error::lifecycle("compaction worker panicked")));
        settle(&owned, directory.as_ref(), &model, operation, result).await;
    });
    *managed.worker.lock().await = Some(worker);
    // The operation and worker already have owners. A failed response is an
    // unknown delivery outcome; it must not cancel or replay the effect.
    manager.refresh(&managed).await.map_err(safe)
}

async fn settle(
    managed: &Managed,
    directory: &dyn microcrab_storage::Directory,
    model: &str,
    mut operation: OperationRecord,
    result: Result<microcrab_protocol::Response, Error>,
) {
    let status = match &result {
        Ok(_) => "completed",
        Err(Error::Cancelled) => "cancelled",
        Err(_) => "failed",
    };
    operation.status = status.into();
    operation.revision = 1;
    operation.updated_at_ms = now().max(operation.updated_at_ms);
    operation.result = Some(match result {
        Ok(mut response) => {
            response.usage.provider_details.clear();
            json!({"usage":response.usage,"compaction_id":managed.session.compaction().map(|marker| marker.id)})
        }
        Err(error) => json!({"error":safe(error)}),
    });
    let persisted = async {
        let _gate = managed.commands.lock().await;
        let mut current = managed.record.lock().await;
        let expected_session_revision = current.revision;
        let mut next = current.clone();
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::protocol("session revision exhausted"))?;
        next.updated_at_ms = now().max(next.updated_at_ms);
        operation.updated_at_ms = operation.updated_at_ms.max(next.updated_at_ms);
        let journal_cursor = managed.journal.cursor().await;
        if let Some(Value::Object(result)) = operation.result.as_mut() {
            result.insert(
                "journal_cursor".into(),
                serde_json::to_value(&journal_cursor)?,
            );
        }
        let (session, receipt) = directory
            .settle_operation(
                &operation.owner,
                OperationSettlement {
                    expected_session_revision,
                    session: next,
                    expected_operation_revision: 0,
                    operation: operation.clone(),
                    journal_cursor,
                },
            )
            .await?;
        *current = session;
        operation = receipt;
        Ok::<_, Error>(())
    }
    .await;
    managed.hub.set_operation(None);
    if persisted.is_err() {
        managed.faulted.store(true, Ordering::Release);
        managed
            .hub
            .publish(super::SessionRuntimeEvent::ResyncRequired(
                super::ResyncReason::ReceiptPublicationFailed,
            ));
    } else {
        managed
            .hub
            .publish(super::SessionRuntimeEvent::Compaction(json!({
                "operation_id": operation.id,
                "status": operation.status,
                "error": operation.result.as_ref().and_then(|value| value.get("error")),
            })));
        managed.hub.publish(super::SessionRuntimeEvent::Turn(
            serde_json::to_value(&operation).unwrap_or(Value::Null),
        ));
    }
    if view::refresh(directory, managed, model).await.is_err() {
        managed.faulted.store(true, Ordering::Release);
        managed
            .hub
            .publish(super::SessionRuntimeEvent::ResyncRequired(
                super::ResyncReason::SnapshotFailed,
            ));
    }
}
