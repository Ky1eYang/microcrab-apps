//! Parent-authorized child turns keep Core child settlement separate from parent metadata receipts.
use super::{Manager, now, safe};
use microcrab_protocol::application::{Command, WireError};
use microcrab_session::{Error, SessionEvent, SessionInput};
use microcrab_storage::{OperationRecord, RECORD_VERSION};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Submit {
    child_id: String,
    operation_id: String,
    input: SessionInput,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Observe {
    child_id: String,
    operation_id: String,
}
fn invalid(reason: &'static str) -> WireError {
    WireError::new("invalid_params", reason)
}
fn operation(value: &str) -> Result<(), WireError> {
    if !uuid::Uuid::parse_str(value).is_ok_and(|id| id.to_string() == value) {
        return Err(invalid(
            "operation_id must be a canonical lowercase hyphenated UUID",
        ));
    }
    Ok(())
}
fn publication(error: Error, operation_id: &str) -> WireError {
    if matches!(&error,Error::Protocol(reason) if reason.public_reason()==Some("creator directory revision or ownership conflict"))
    {
        return WireError::new(
            "operation_conflict",
            "Child operation receipt changed during publication",
        );
    }
    let cause = safe(error);
    let mut error = WireError::new(
        "outcome_unknown",
        format!(
            "Child operation publication requires observation: {}",
            cause.message
        ),
    );
    error.details = Some(Box::new(json!({"operation_id":operation_id,"cause":cause})));
    error
}
pub(super) async fn observe_prior(
    manager: &Manager,
    mut prior: OperationRecord,
    observe: bool,
) -> Result<OperationRecord, WireError> {
    if prior.status == "accepted"
        && prior
            .result
            .as_ref()
            .and_then(|result| result["server_instance"].as_str())
            != Some(manager.instance.as_str())
    {
        if !observe {
            let mut error = WireError::new(
                "outcome_unknown",
                "Observe the previous child operation before any further submission",
            );
            error.details = Some(Box::new(
                json!({"operation_id":prior.id,"admission":"unknown"}),
            ));
            return Err(error);
        }
        let expected = prior.revision;
        let child = prior
            .result
            .as_ref()
            .map(|result| result["child_id"].clone())
            .unwrap_or(Value::Null);
        prior.status = "failed".into();
        prior.revision += 1;
        prior.updated_at_ms = now().max(prior.updated_at_ms);
        let mut error = WireError::new(
            "operation_interrupted",
            "Server restarted before the child terminal receipt was acknowledged; admission remains unknown and is never replayed",
        );
        error.details = Some(Box::new(
            json!({"admission":"unknown","operation_id":prior.id}),
        ));
        prior.result = Some(json!({"child_id":child,"error":error}));
        let operation = prior.id.clone();
        return manager
            .directory
            .replace_operation(&prior.owner.clone(), expected, prior)
            .await
            .map_err(|error| publication(error, &operation));
    }
    Ok(prior)
}

fn scoped(record: &OperationRecord, parent: &str, child: &str) -> Result<(), WireError> {
    if record.session_id != parent
        || record.method != "child.turn.submit"
        || record
            .result
            .as_ref()
            .and_then(|result| result["child_id"].as_str())
            != Some(child)
    {
        return Err(WireError::new(
            "operation_conflict",
            "Operation does not belong to this exact parent and child",
        ));
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn receipt_authority_never_crosses_parent_child_or_operation_kind() {
        let record = OperationRecord {
            version: RECORD_VERSION,
            id: uuid::Uuid::new_v4().to_string(),
            session_id: "parent".into(),
            owner: "owner".into(),
            method: "child.turn.submit".into(),
            payload_hash: "f".repeat(64),
            status: "accepted".into(),
            result: Some(json!({"child_id":"0123456789abcdef","server_instance":"instance"})),
            revision: 0,
            created_at_ms: 1,
            updated_at_ms: 1,
        };
        assert!(scoped(&record, "parent", "0123456789abcdef").is_ok());
        assert!(scoped(&record, "other", "0123456789abcdef").is_err());
        assert!(scoped(&record, "parent", "fedcba9876543210").is_err());
        let mut root = record;
        root.method = "turn.submit".into();
        assert!(scoped(&root, "parent", "0123456789abcdef").is_err());
        assert!(serde_json::from_value::<Submit>(json!({"operation_id":uuid::Uuid::new_v4().to_string(),"child_id":"0123456789abcdef","input":{"parts":[],"variables":{}},"owner":"foreign"})).is_err());
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Enter {
    child_id: String,
}
pub(super) async fn enter(
    manager: &Manager,
    owner: &str,
    command: &Command,
) -> Result<Value, WireError> {
    let params: Enter = super::commands::parse(command)?;
    let parent_id = command
        .session_id
        .as_deref()
        .ok_or_else(|| invalid("Parent Session path required"))?;
    let selected = manager
        .inspect_selected_child(
            owner,
            super::child_inspection::DetailRequest {
                session_id: parent_id.into(),
                child_id: params.child_id.clone(),
                limit: Some(1),
                before_cursor: None,
            },
        )
        .await?;
    let parent = manager
        .active(manager.record(owner, parent_id).await?, true)
        .await?;
    let _gate = parent.commands.lock().await;
    ensure_child(
        manager,
        owner,
        parent_id,
        &params.child_id,
        &parent,
        &selected,
    )
    .await?;
    manager
        .inspect_selected_child(
            owner,
            super::child_inspection::DetailRequest {
                session_id: parent_id.into(),
                child_id: params.child_id,
                limit: None,
                before_cursor: None,
            },
        )
        .await
}
async fn ensure_child(
    manager: &Manager,
    owner: &str,
    parent_id: &str,
    child: &str,
    parent: &super::Managed,
    selected: &Value,
) -> Result<(), WireError> {
    let target = selected["name"]
        .as_str()
        .ok_or_else(|| invalid("Selected child Agent identity unavailable"))?;
    if parent.session.inspect_child(child, target).is_ok() {
        return Ok(());
    }
    if selected["interaction"] != "persistent" || selected["depth"] != 1 {
        return Err(WireError::new(
            "permission_denied",
            "Only a recorded persistent child can be reattached",
        ));
    }
    let path = manager.verified_child_path(owner, parent_id, child).await?;
    super::assembly::restore_child(manager, parent, &path, child)
        .await
        .map_err(safe)?;
    Ok(())
}

pub(super) async fn command(
    manager: &Manager,
    owner: &str,
    command: &Command,
) -> Result<Value, WireError> {
    let parent_id = command
        .session_id
        .as_deref()
        .ok_or_else(|| invalid("Parent Session path required"))?;
    let (child, operation_id, input) = if command.method == "child.turn.submit" {
        let params: Submit = super::commands::parse(command)?;
        super::commands::validate_input(&params.input)?;
        (params.child_id, params.operation_id, Some(params.input))
    } else {
        let params: Observe = super::commands::parse(command)?;
        (params.child_id, params.operation_id, None)
    };
    operation(&operation_id)?;
    manager
        .directory
        .get(owner, parent_id)
        .await
        .map_err(safe)?
        .ok_or_else(super::unavailable)?;
    // Retained selected parent evidence, never a directory name or inspection unlock, certifies eligibility.
    let selected = manager
        .inspect_selected_child(
            owner,
            super::child_inspection::DetailRequest {
                session_id: parent_id.into(),
                child_id: child.clone(),
                limit: Some(1),
                before_cursor: None,
            },
        )
        .await?;
    let digest =
        microcrab_storage::history::content_hash(&(parent_id, &child, &operation_id, &input))
            .map_err(safe)?;
    if let Some(prior) = manager
        .directory
        .get_operation(owner, &operation_id)
        .await
        .map_err(safe)?
    {
        scoped(&prior, parent_id, &child)?;
        if input.is_some() && prior.payload_hash != digest {
            return Err(WireError::new(
                "operation_conflict",
                "Child operation_id was used with different parameters",
            ));
        }
        let prior = observe_prior(manager, prior, input.is_none()).await?;
        return serde_json::to_value(prior).map_err(|error| safe(error.into()));
    }
    let Some(input) = input else {
        return Err(WireError::new(
            "operation_not_found",
            "Child operation has no durable receipt; a pending admission may still complete",
        ));
    };
    let record = manager.record(owner, parent_id).await?;
    let parent = manager.active(record, true).await?;
    let _commands = parent.commands.lock().await;
    if parent.closing.load(std::sync::atomic::Ordering::Acquire)
        || parent.faulted.load(std::sync::atomic::Ordering::Acquire)
    {
        return Err(WireError::new(
            "closed",
            "Parent requires cleanup or operation reconciliation before child input",
        ));
    }
    {
        let mut drivers = parent.child_drivers.lock().await;
        while let Some(joined) = drivers.try_join_next() {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    return Err(WireError::new(
                        "operation_failed",
                        "Prior child driver failed before cleanup acknowledgement",
                    ));
                }
            }
        }
    }
    if let Some(prior) = manager
        .directory
        .get_operation(owner, &operation_id)
        .await
        .map_err(safe)?
    {
        scoped(&prior, parent_id, &child)?;
        if prior.payload_hash != digest {
            return Err(WireError::new(
                "operation_conflict",
                "Child operation_id was used with different parameters",
            ));
        }
        return serde_json::to_value(prior).map_err(|error| safe(error.into()));
    }
    for prior in manager
        .directory
        .list_operations(owner, parent_id)
        .await
        .map_err(safe)?
    {
        if prior.method == "child.turn.submit"
            && prior.status == "accepted"
            && prior
                .result
                .as_ref()
                .and_then(|result| result["child_id"].as_str())
                == Some(child.as_str())
            && prior
                .result
                .as_ref()
                .and_then(|result| result["server_instance"].as_str())
                != Some(manager.instance.as_str())
        {
            observe_prior(manager, prior, false).await?;
        }
    }
    ensure_child(manager, owner, parent_id, &child, &parent, &selected).await?;
    if !parent
        .session
        .child_accepts_user_input(&child)
        .map_err(safe)?
    {
        return Err(WireError::new(
            "permission_denied",
            "Current child mode does not accept targeted user input",
        ));
    }
    let timestamp = now();
    let mut receipt = manager
        .directory
        .create_operation(OperationRecord {
            version: RECORD_VERSION,
            id: operation_id.clone(),
            session_id: parent_id.into(),
            owner: owner.into(),
            method: "child.turn.submit".into(),
            payload_hash: digest,
            status: "accepted".into(),
            result: Some(json!({"child_id":child,"server_instance":manager.instance})),
            revision: 0,
            created_at_ms: timestamp,
            updated_at_ms: timestamp,
        })
        .await
        .map_err(|error| publication(error, &operation_id))?;
    let started = parent
        .session
        .stream_child_user_with_settlement(
            &child,
            input,
            Some(operation_id.clone()),
            parent.close_token.child_token(),
        )
        .await;
    let (stream, settlement) = match started {
        Ok(value) => value,
        Err(error) => {
            receipt.status = "failed".into();
            receipt.revision = 1;
            receipt.updated_at_ms = now().max(timestamp);
            receipt.result = Some(json!({"child_id":child,"error":safe(error)}));
            let saved = manager
                .directory
                .replace_operation(owner, 0, receipt)
                .await
                .map_err(|error| publication(error, &operation_id))?;
            return serde_json::to_value(saved).map_err(|error| safe(error.into()));
        }
    };
    let response = serde_json::to_value(&receipt).map_err(|error| safe(error.into()))?;
    let directory = manager.directory.clone();
    let mut drivers = parent.child_drivers.lock().await;
    drivers.spawn(drive_child(
        directory,
        parent.clone(),
        child,
        receipt,
        stream,
        settlement,
    ));
    Ok(response)
}

pub(super) async fn drive_child(
    directory: std::sync::Arc<dyn microcrab_storage::Directory>,
    parent: std::sync::Arc<super::Managed>,
    child: String,
    mut receipt: OperationRecord,
    mut stream: microcrab_session::SessionStream,
    settlement: microcrab_session::SettlementGuard,
) -> Result<(), WireError> {
    let timestamp = receipt.updated_at_ms;
    let mut result: Result<Value, Error> =
        Err(Error::protocol("child driver ended without terminal event"));
    while let Some(event) = stream.recv().await {
        match event {
            Ok(SessionEvent::Finished { mut outcome }) => {
                outcome.usage.provider_details.clear();
                let truncated = outcome.content.len() > 128 * 1024;
                if truncated {
                    let mut end = 128 * 1024;
                    while !outcome.content.is_char_boundary(end) {
                        end -= 1;
                    }
                    outcome.content.truncate(end);
                }
                result = serde_json::to_value(outcome)
                    .map(|mut value| {
                        value["content_truncated"] = json!(truncated);
                        value
                    })
                    .map_err(Error::from);
                break;
            }
            Ok(SessionEvent::Model { .. }) => {}
            Err(error) => {
                result = Err(error);
                break;
            }
        }
    }
    receipt.status = match &result {
        Ok(_) => "completed",
        Err(Error::Cancelled) => "cancelled",
        Err(_) => "failed",
    }
    .into();
    receipt.revision = 1;
    receipt.updated_at_ms = now().max(timestamp);
    receipt.result = Some(match result {
        Ok(outcome) => json!({"child_id":child,"outcome":outcome}),
        Err(error) => {
            json!({"child_id":child,"error":if matches!(&error,Error::Cancelled){super::cancellation::wire(stream.interrupt_reason().as_ref())}else{safe(error)}})
        }
    });
    // Core's child Journal and settlement reservation own the exact child Context.
    // Only the independent parent-scoped operation receipt is published here.
    let saved = directory
        .replace_operation(&receipt.owner, 0, receipt.clone())
        .await
        .map_err(|error| publication(error, &receipt.id));
    if saved.is_err() {
        parent
            .faulted
            .store(true, std::sync::atomic::Ordering::Release);
    }
    parent.hub.publish(super::SessionRuntimeEvent::Turn(
        json!({"child_id":child,"operation":saved.as_ref().ok()}),
    ));
    drop(settlement);
    saved.map(|_| ())
}
