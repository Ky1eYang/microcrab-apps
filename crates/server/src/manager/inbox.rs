//! Observe autonomous Session outcomes without owning, persisting or replaying
//! the insertion queue. Naming is scheduled by durable input admission.
mod round_stop;
use super::*;
use microcrab_session::InboxFailureKind;
use microcrab_storage::{OperationRecord, RECORD_VERSION};
pub(super) use round_stop::settle as settle_round_stop;
use serde_json::json;

pub(super) async fn settle_failure(manager: &Manager, managed: &Managed) -> Result<(), Error> {
    let _gate = managed.commands.lock().await;
    if managed.hub.operation().is_some() || managed.session.inbox_snapshot().active {
        return Ok(());
    }
    let failure = managed
        .session
        .subscribe_activity()
        .borrow()
        .latest_inbox_failure
        .clone();
    let Some(failure) = failure else {
        return Ok(());
    };
    let record = managed.record.lock().await.clone();
    let operations = manager
        .directory
        .list_operations(&record.owner, &record.id)
        .await?;
    if operations.iter().any(|operation| {
        operation.method == "turn.inbox"
            && operation.result.as_ref().is_some_and(|result| {
                result["server_instance"].as_str() == Some(manager.instance.as_str())
                    && result["inbox_failure_sequence"].as_u64() == Some(failure.sequence)
            })
    }) {
        return Ok(());
    }
    let state = managed.session.state();
    let retry_ready = failure.retry_ready
        && managed.session.retry_ready()
        && failure.epoch == state.epoch_id
        && failure.turn == state.turn_id
        && failure.round == state.round_id;
    let witness = if retry_ready {
        Some(view::retry_witness(
            &managed.session.context_snapshot()?,
            &state,
        )?)
    } else {
        None
    };
    if witness
        .as_deref()
        .is_some_and(|witness| view::retry_operation_id(&operations, witness).is_some())
    {
        return Ok(());
    }
    let messages = view::messages(managed.session.messages(), state.epoch_id, &record.id);
    let endpoint = messages.iter().rev().find(|message| {
        message["position"]["epoch"].as_u64() == Some(failure.epoch)
            && message["position"]["turn"].as_u64() == failure.turn
            && message["position"]["round"].as_u64() <= failure.round
    });
    // History can select another branch before this observer acquires the
    // command gate. A retained failure cannot create an unanchored ghost error.
    if failure.turn.is_some() && endpoint.is_none() {
        return Ok(());
    }
    // JSON and I/O have distinct direct wire contracts. Preserve them before
    // reconstructing a body-free Session error for local settlement decisions.
    let wire_override = match &failure.kind {
        InboxFailureKind::Cancelled { reason } => Some(super::cancellation::wire(reason.as_ref())),
        InboxFailureKind::Json(diagnostic) => {
            let mut wire = WireError::new("invalid_request", diagnostic.summary());
            wire.details = Some(Box::new(
                json!({"reason": diagnostic.summary(), "line": diagnostic.json_line, "column": diagnostic.json_column}),
            ));
            Some(wire)
        }
        InboxFailureKind::Io {
            kind,
            raw_os_error,
            message,
        } => {
            // Reconstruct by kind, not OS code: a retained raw code need not map
            // to the same ErrorKind on a different platform.
            let mut wire = safe(Error::Io(std::io::Error::new(*kind, message.clone())));
            if let Some(code) = raw_os_error {
                wire.details
                    .as_mut()
                    .expect("I/O wire projection supplies details")["raw_os_error"] = json!(code);
            }
            Some(wire)
        }
        _ => None,
    };
    let reason = |value: Option<String>| {
        value
            .map(microcrab_session::ErrorReason::from)
            .unwrap_or_else(|| String::new().into())
    };
    let error = match failure.kind {
        InboxFailureKind::ProviderStatus { status } => Error::ProviderStatus { status },
        InboxFailureKind::Transport => Error::Transport,
        InboxFailureKind::TransportFailure(message) => Error::TransportFailure(message.into()),
        InboxFailureKind::ProviderTimeout => Error::ProviderTimeout,
        InboxFailureKind::ProviderIdleTimeout(details) => Error::ProviderIdleTimeout(details),
        InboxFailureKind::Provider {
            protocol_reason,
            reason,
            failure,
            empty_completion,
        } => Error::Provider(match (empty_completion, protocol_reason, failure) {
            (Some(facts), _, _) => microcrab_transport::ProviderError::empty_completion(facts),
            (None, Some(reason), _) => microcrab_transport::ProviderError::protocol(
                microcrab_transport::ProtocolError::from(reason),
            ),
            (None, None, Some(failure)) => microcrab_transport::ProviderError::failure(failure),
            (None, None, None) => reason.into(),
        }),
        InboxFailureKind::Protocol(reason) => match reason {
            Some(reason) => Error::Protocol(reason.into()),
            None => Error::Protocol(String::new().into()),
        },
        InboxFailureKind::Limit(error) => Error::Limit(error),
        InboxFailureKind::Cancelled { .. } => Error::Cancelled,
        InboxFailureKind::Config(value) => Error::Config(reason(value)),
        InboxFailureKind::Denied(value) => Error::Denied(reason(value)),
        InboxFailureKind::Tool(value) => Error::Tool(reason(value)),
        InboxFailureKind::Lifecycle(value) => Error::Lifecycle(reason(value)),
        InboxFailureKind::Audit(value) => Error::Audit(reason(value)),
        InboxFailureKind::Wasm(value) => Error::Wasm(reason(value)),
        InboxFailureKind::Reference(value) => Error::Reference(reason(value)),
        InboxFailureKind::Diagnostic(diagnostic) | InboxFailureKind::Json(diagnostic) => {
            Error::Diagnostic(diagnostic)
        }
        InboxFailureKind::Io { kind, .. } => Error::Io(std::io::Error::new(kind, "")),
        InboxFailureKind::ToolFailure {
            kind,
            retryable,
            code,
            message,
        } => Error::ToolFailure(microcrab_session::runtime::execution::ToolFailure {
            kind,
            retryable,
            code,
            message,
        }),
        InboxFailureKind::PendingInput(value) => Error::PendingInput(value),
        InboxFailureKind::Busy => Error::Busy,
        InboxFailureKind::Closed => Error::Closed,
        InboxFailureKind::ExecutionInterrupted => Error::ExecutionInterrupted,
        InboxFailureKind::Other => Error::Tool(String::new().into()),
    };
    let status = if matches!(error, Error::Cancelled) {
        "cancelled"
    } else {
        "failed"
    };
    let timestamp = operations
        .iter()
        .map(|value| value.created_at_ms)
        .max()
        .map_or(now(), |last| now().max(last.saturating_add(1)));
    let partial = managed.hub.failed_partial(
        failure.epoch,
        failure.turn,
        failure.round,
        matches!(&error, Error::Limit(limit) if limit.is_pre_effect()),
    );
    let journal_cursor = managed.journal.cursor().await;
    let receipt = manager
        .directory
        .create_operation(OperationRecord {
            version: RECORD_VERSION,
            id: uuid::Uuid::new_v4().to_string(),
            owner: record.owner,
            session_id: record.id,
            method: "turn.inbox".into(),
            payload_hash: microcrab_storage::history::content_hash(&(
                manager.instance.as_str(),
                failure.sequence,
            ))?,
            status: status.into(),
            revision: 0,
            created_at_ms: timestamp,
            updated_at_ms: timestamp,
            result: Some(json!({
                "error":wire_override.unwrap_or_else(|| safe(error)),
                "partial_output": partial,
                "position":{"epoch":failure.epoch,"turn":failure.turn,"round":failure.round},
                "after_message_id":endpoint.map(|message| message["id"].clone()),
                "retry_ready":retry_ready,"retry_witness":witness,
                "server_instance":manager.instance,"inbox_failure_sequence":failure.sequence,
                "journal_cursor":journal_cursor,
            })),
        })
        .await?;
    super::display::bind_failure(managed, &receipt).await;
    managed
        .hub
        .publish(super::SessionRuntimeEvent::Turn(serde_json::to_value(
            receipt,
        )?));
    Ok(())
}
