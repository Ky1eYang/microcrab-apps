//! Browser insertion owns durable admission receipts, never a copied runtime queue.
use super::*;

pub(super) async fn insert(
    manager: &Manager,
    managed: Arc<Managed>,
    command: &Command,
) -> Result<Value, WireError> {
    let params: Submit = parse(command)?;
    if params.activate_if_idle && command.method != TURN_QUEUE {
        return Err(WireError::new(
            "invalid_request",
            "Idle activation is only valid for turn.queue",
        ));
    }
    operation_id(&params.operation_id)?;
    validate_input(&params.input)?;
    let digest = hash(command)?;
    let record = managed.record.lock().await.clone();
    if let Some(operation) = manager
        .directory
        .get_operation(&record.owner, &params.operation_id)
        .await
        .map_err(safe)?
    {
        return replay(reconcile(manager, operation).await?, command, &digest);
    }
    let timestamp = now();
    let mut operation = manager
        .directory
        .create_operation(OperationRecord {
            version: RECORD_VERSION,
            id: params.operation_id,
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
    // No await separates the existing inbox mutation and recording its outcome.
    // A process loss before durable terminal publication is interrupted/unknown,
    // never a reason to replay this insertion automatically.
    let naming_input = params.input.clone();
    let result = managed.session.queue_submission(
        params.input,
        operation.id.clone(),
        command.method == TURN_STEER,
        params.activate_if_idle,
    );
    operation.status = if result.is_ok() {
        "completed"
    } else {
        "failed"
    }
    .into();
    operation.result = Some(match result {
        Ok(()) => json!({"admitted":true,"storage":"session_memory"}),
        Err(microcrab_session::Error::PendingInput(
            microcrab_session::PendingInputError::Continuation,
        )) if params.activate_if_idle => {
            json!({"error": WireError::new("continuation_pending", "Retry or resume the retained continuation before sending new input; your draft was not admitted")})
        }
        Err(error) => json!({"error":safe(error)}),
    });
    operation.revision = 1;
    operation.updated_at_ms = now().max(timestamp);
    let receipt = match manager
        .directory
        .replace_operation(&operation.owner.clone(), 0, operation)
        .await
    {
        Ok(receipt) => receipt,
        Err(_) => {
            managed.faulted.store(true, Ordering::Release);
            managed
                .hub
                .publish(super::super::SessionRuntimeEvent::ResyncRequired(
                    super::super::ResyncReason::InboxReceiptFailed,
                ));
            return Err(WireError::new(
                "outcome_unknown",
                "Input admission outcome requires reconciliation",
            ));
        }
    };
    if receipt.status == "completed" && managed.auto_naming.load(Ordering::Acquire) {
        super::super::naming::start_admitted(
            managed.clone(),
            manager.directory.clone(),
            manager.config.provider.model.clone(),
            naming_input,
            manager.instance.clone(),
        )
        .await;
    }
    managed.hub.publish(super::super::SessionRuntimeEvent::Turn(
        serde_json::to_value(&receipt).map_err(|_| serialization())?,
    ));
    // The lifecycle-owned Session observer subscribes to this terminal `turn`
    // publication and inbox activity. Its snapshot refresh must not delay the
    // already durable admission receipt or hold this command's admission slot.
    serde_json::to_value(receipt).map_err(|_| serialization())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Update {
    operation_id: String,
    expected_inbox_id: String,
    item_id: u64,
    expected_revision: u64,
    input: Option<microcrab_session::SessionInput>,
    mode: Option<microcrab_session::PendingInputMode>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Remove {
    operation_id: String,
    expected_inbox_id: String,
    item_id: u64,
    expected_revision: u64,
}
pub(super) fn validate_target(inbox_id: &str, item: u64, revision: u64) -> Result<(), WireError> {
    const MAX_SAFE_INTEGER: u64 = (1u64 << 53) - 1;
    if inbox_id.len() != 32 || !inbox_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid(
            "expected_inbox_id must contain exactly 32 hexadecimal digits",
        ));
    }
    if item == 0 || item > MAX_SAFE_INTEGER {
        return Err(invalid("item_id must be between 1 and 9007199254740991"));
    }
    if revision > MAX_SAFE_INTEGER {
        return Err(invalid(
            "expected_revision must not exceed 9007199254740991",
        ));
    }
    Ok(())
}
pub(super) fn mutation_operation_id(command: &Command) -> Result<String, WireError> {
    if command.method == INBOX_UPDATE {
        let params: Update = parse(command)?;
        validate_target(
            &params.expected_inbox_id,
            params.item_id,
            params.expected_revision,
        )?;
        if params.input.is_none() && params.mode.is_none() {
            return Err(invalid("Inbox update requires input or mode"));
        }
        if let Some(input) = &params.input {
            validate_input(input)?;
        }
        Ok(params.operation_id)
    } else {
        let params: Remove = parse(command)?;
        validate_target(
            &params.expected_inbox_id,
            params.item_id,
            params.expected_revision,
        )?;
        Ok(params.operation_id)
    }
}
pub(super) async fn mutate(
    manager: &Manager,
    managed: Arc<Managed>,
    command: &Command,
) -> Result<Value, WireError> {
    let id = mutation_operation_id(command)?;
    operation_id(&id)?;
    let digest = hash(command)?;
    let record = managed.record.lock().await.clone();
    if let Some(operation) = manager
        .directory
        .get_operation(&record.owner, &id)
        .await
        .map_err(safe)?
    {
        return replay(reconcile(manager, operation).await?, command, &digest);
    }
    let timestamp = now();
    let mut operation = manager
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
    // The Session mutex is the only mutation authority. No await or copied queue
    // separates its compare-and-swap from capturing the exact removed/updated item.
    let result = if command.method == INBOX_UPDATE {
        let params: Update = parse(command)?;
        managed
            .session
            .update_pending_input(
                &params.expected_inbox_id,
                params.item_id,
                params.expected_revision,
                params.input,
                params.mode,
            )
            .map(|mut item| {
                item.input.variables = managed
                    .session
                    .state_handle()
                    .receipt_change(&item.input.variables);
                json!({"inbox_id":params.expected_inbox_id,"item":item})
            })
    } else {
        let params: Remove = parse(command)?;
        managed.session.remove_pending_input(&params.expected_inbox_id, params.item_id,
            params.expected_revision)
            .map(|mut item| {
                item.input.variables = managed.session.state_handle().receipt_change(&item.input.variables);
                json!({"removed":true,"item_id":params.item_id,"inbox_id":params.expected_inbox_id,"item":item})
            })
    };
    operation.status = if result.is_ok() {
        "completed"
    } else {
        "failed"
    }
    .into();
    operation.result = Some(match result {
        Ok(value) => value,
        Err(error) => json!({"error":safe(error)}),
    });
    operation.revision = 1;
    operation.updated_at_ms = now().max(timestamp);
    let receipt = match manager
        .directory
        .replace_operation(&operation.owner.clone(), 0, operation)
        .await
    {
        Ok(receipt) => receipt,
        Err(_) => {
            managed.faulted.store(true, Ordering::Release);
            managed
                .hub
                .publish(super::super::SessionRuntimeEvent::ResyncRequired(
                    super::super::ResyncReason::InboxReceiptFailed,
                ));
            return Err(WireError::new(
                "outcome_unknown",
                "Pending input mutation outcome requires reconciliation",
            ));
        }
    };
    managed.hub.publish(super::super::SessionRuntimeEvent::Turn(
        serde_json::to_value(&receipt).map_err(|_| serialization())?,
    ));
    // The lifecycle-owned Session observer subscribes to this terminal `turn`
    // publication and inbox activity. Its snapshot refresh must not delay the
    // already durable admission receipt or hold this command's admission slot.
    serde_json::to_value(receipt).map_err(|_| serialization())
}
