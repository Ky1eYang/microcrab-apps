//! Governed variable transport uses the Session State queue as its mutation owner.
//! The shared directory stores body-free intent/results, never a replayable patch.
use super::{active, ensure_mutable, invalid, parse, session_record};
use crate::manager::{Managed, Manager, commands, now, safe};
use futures_util::FutureExt;
use microcrab_protocol::application::{Command, WireError};
use microcrab_session::{VariablesChange, state::VariableWitness};
use microcrab_storage::{OperationRecord, RECORD_VERSION};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{panic::AssertUnwindSafe, sync::atomic::Ordering};

const PATCH: &str = "session.variables.patch";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Checked {
    operation_id: String,
    expected: VariableWitness,
    change: VariablesChange,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Observation {
    operation_id: String,
}

fn operation_id(id: &str) -> Result<(), WireError> {
    uuid::Uuid::parse_str(id)
        .map(|_| ())
        .map_err(|_| invalid("operation_id must be a UUID"))
}

fn mutable(manager: &Manager, managed: &Managed, id: &str) -> Result<(), WireError> {
    ensure_mutable(managed)?;
    if super::super::agent_init::running(manager, id) {
        return Err(super::super::agent_init::pending_error());
    }
    if let Some(error) = super::super::agent_init::failure(&managed.session) {
        return Err(error);
    }
    if managed.suspended.load(Ordering::Acquire) {
        return Err(WireError::new(
            "host_suspended",
            "Reopen the Session after Host reconciliation before changing variables",
        ));
    }
    Ok(())
}

/// A durable receipt alone cannot certify a State commit survived restart or
/// history/activation replacement. `applied` certifies this exact past commit in
/// the current live lineage, not that later edits left its values unchanged.
fn projection(operation: &OperationRecord, server: &str, incarnation: Option<&str>) -> Value {
    let result = operation.result.as_ref();
    let same_server = result.and_then(|v| v["server_instance"].as_str()) == Some(server);
    let same_live = incarnation.is_some()
        && result.and_then(|v| v["runtime_instance_id"].as_str()) == incarnation;
    let mut response = json!({"operation_id":operation.id,"status":"unknown"});
    match operation.status.as_str() {
        "completed" if same_server && same_live => {
            response["status"] = json!("applied");
            response["commit"] = result.map(|v| v["commit"].clone()).unwrap_or(Value::Null);
            response["boundary"] = json!("next_state_patch");
        }
        "failed" => {
            response["status"] = json!("rejected");
            response["reason"] = result.map(|v| v["error"].clone()).unwrap_or(Value::Null);
        }
        "accepted" | "running" if same_server && same_live => {
            response["status"] = json!("queued");
        }
        _ => {
            response["reason"] = json!({"code":"outcome_unknown","message":"The exact State commit cannot be certified in this live Session; inspect current State and explicitly save a fresh edit if needed. Do not replay this operation."});
        }
    }
    response
}

fn reply(response: Value) -> Result<Value, WireError> {
    if response["status"] == "rejected" {
        let mut error: WireError = serde_json::from_value(response["reason"].clone())
            .unwrap_or_else(|_| {
                WireError::new(
                    "variable_rejected",
                    "Variable edit was rejected; inspect its exact receipt",
                )
            });
        error.details = Some(Box::new(
            json!({"operation_id":response["operation_id"],"status":"rejected"}),
        ));
        Err(error)
    } else {
        Ok(response)
    }
}

async fn observe(manager: &Manager, id: &str, operation: &OperationRecord) -> Value {
    let incarnation = match active(manager, id).await {
        Some(managed) => managed
            .session
            .state_handle()
            .inspection()
            .ok()
            .map(|inspection| inspection.witness.runtime_instance_id),
        None => None,
    };
    projection(operation, &manager.instance, incarnation.as_deref())
}

fn validate_receipt(
    operation: &OperationRecord,
    id: &str,
    digest: Option<&str>,
) -> Result<(), WireError> {
    if operation.session_id != id || operation.method != PATCH {
        return Err(WireError::new(
            "operation_conflict",
            "Operation ID belongs to another Session or action",
        ));
    }
    // Identity is the caller's operation UUID; digest only certifies exact input.
    if digest.is_some_and(|digest| operation.payload_hash != digest) {
        return Err(WireError::new(
            "operation_conflict",
            "Operation ID was used with a different variable edit; inspect its receipt and use a fresh ID for a new explicit save",
        ));
    }
    Ok(())
}

async fn existing(
    manager: &Manager,
    owner: &str,
    id: &str,
    operation_id: &str,
    digest: Option<&str>,
) -> Result<Option<Value>, WireError> {
    let Some(operation) = manager
        .directory
        .get_operation(owner, operation_id)
        .await
        .map_err(safe)?
    else {
        return Ok(None);
    };
    validate_receipt(&operation, id, digest)?;
    Ok(Some(observe(manager, id, &operation).await))
}

pub(super) async fn command(
    manager: &Manager,
    owner: &str,
    command: &Command,
) -> Result<Value, WireError> {
    let id = command
        .session_id
        .as_deref()
        .ok_or_else(|| invalid("Session path required"))?;
    let record = session_record(manager, owner, id).await?;
    if command.method == "session.variables.operation" {
        let params: Observation = parse(command.params.clone())?;
        operation_id(&params.operation_id)?;
        return Ok(existing(manager, owner, id, &params.operation_id, None).await?
            .unwrap_or_else(|| json!({"operation_id":params.operation_id,"status":"unknown","reason":{"code":"operation_unknown","message":"No exact variable operation receipt is available; inspect current State before a fresh explicit save"}})));
    }
    let checked = command.params.get("operation_id").is_some()
        || command.params.get("expected").is_some()
        || command.params.get("change").is_some();
    let params: Option<Checked> = if checked {
        Some(parse(command.params.clone())?)
    } else {
        None
    };
    let legacy: Option<VariablesChange> = if checked {
        None
    } else {
        Some(parse(command.params.clone())?)
    };
    let digest = commands::hash(command)?;
    if let Some(params) = &params {
        operation_id(&params.operation_id)?;
        if let Some(result) =
            existing(manager, owner, id, &params.operation_id, Some(&digest)).await?
        {
            return reply(result);
        }
    }
    if record.archived {
        return Err(WireError::new(
            "archived",
            "Open an unarchived Session explicitly before changing variables",
        ));
    }
    let managed = active(manager, id).await.ok_or_else(|| {
        WireError::new(
            "closed",
            "Open the Session explicitly before changing variables",
        )
    })?;
    let gate = managed.commands.lock().await;
    if let Some(params) = &params {
        if let Some(result) =
            existing(manager, owner, id, &params.operation_id, Some(&digest)).await?
        {
            return reply(result);
        }
    }
    mutable(manager, &managed, id)?;
    let Some(params) = params else {
        // Legacy unconditional calls still use the public Client-bound State ACL.
        managed
            .session
            .state_handle()
            .change(legacy.expect("legacy shape was parsed"))
            .await
            .map_err(safe)?;
        return Ok(json!({"applied":true,"boundary":"next_state_patch"}));
    };
    let incarnation = managed
        .session
        .state_handle()
        .inspection()
        .map_err(safe)?
        .witness
        .runtime_instance_id;
    let timestamp = now();
    let operation = OperationRecord {
        version: RECORD_VERSION,
        id: params.operation_id,
        session_id: id.into(),
        owner: owner.into(),
        method: PATCH.into(),
        payload_hash: digest,
        status: "accepted".into(),
        result: Some(json!({"server_instance":manager.instance,"runtime_instance_id":incarnation})),
        revision: 0,
        created_at_ms: timestamp,
        updated_at_ms: timestamp,
    };
    let directory = manager.directory.clone();
    let server = manager.instance.clone();
    let owned = managed.clone();
    // Spawn before durable admission: requester cancellation must not strand a
    // queued receipt with no worker. State, rather than a second Server queue,
    // owns CAS. Concurrent admissions resolve only through the shared directory.
    let worker = tokio::spawn(async move {
        let operation = match directory.create_operation(operation.clone()).await {
            Ok(operation) => operation,
            Err(error) => {
                if let Some(receipt) = directory
                    .get_operation(&operation.owner, &operation.id)
                    .await
                    .map_err(safe)?
                {
                    validate_receipt(
                        &receipt,
                        &operation.session_id,
                        Some(&operation.payload_hash),
                    )?;
                    let current = owned
                        .session
                        .state_handle()
                        .inspection()
                        .ok()
                        .map(|inspection| inspection.witness.runtime_instance_id);
                    return Ok(projection(&receipt, &server, current.as_deref()));
                }
                return Err(safe(error));
            }
        };
        let state = owned.session.state_handle();
        let result = AssertUnwindSafe(state.change_checked(params.change, params.expected))
            .catch_unwind()
            .await;
        let mut settled = operation.clone();
        settled.revision += 1;
        settled.updated_at_ms = now().max(settled.updated_at_ms);
        match result {
            Ok(Ok(commit)) => {
                settled.status = "completed".into();
                settled.result = Some(
                    json!({"server_instance":server,"runtime_instance_id":incarnation,"commit":commit}),
                );
            }
            Ok(Err(error))
                if matches!(
                    &error,
                    microcrab_session::Error::VariableConflict
                        | microcrab_session::Error::VariableForbidden(_)
                        | microcrab_session::Error::Denied(_)
                        | microcrab_session::Error::Config(_)
                ) =>
            {
                settled.status = "failed".into();
                settled.result = Some(json!({"error":safe(error)}));
            }
            // Cancellation/IO/panic may race publication. Never certify rejection
            // when the State effect could already have happened.
            _ => {
                settled.status = "interrupted".into();
                settled.result = None;
            }
        }
        match directory
            .replace_operation(&settled.owner.clone(), operation.revision, settled.clone())
            .await
        {
            Ok(_) => {
                let current = owned
                    .session
                    .state_handle()
                    .inspection()
                    .ok()
                    .map(|inspection| inspection.witness.runtime_instance_id);
                Ok(projection(&settled, &server, current.as_deref()))
            }
            Err(_) => Ok(
                json!({"operation_id":operation.id,"status":"unknown","reason":{"code":"outcome_unknown","message":"Variable edit receipt publication failed; inspect current State and do not replay the operation"}}),
            ),
        }
    });
    drop(gate);
    let response = worker.await.map_err(|_| WireError::new("outcome_unknown", "Variable operation worker interrupted; inspect its exact receipt and current State without replaying"))??;
    reply(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_schema_rejects_authority_spoofing_and_mixed_shapes() {
        let valid = json!({"operation_id":uuid::Uuid::new_v4().to_string(),"expected":{"runtime_instance_id":"live","variables_revision":"0"},"change":{"set":{"x":1}}});
        assert!(serde_json::from_value::<Checked>(valid.clone()).is_ok());
        for field in ["owner", "role", "session_id", "set"] {
            let mut spoofed = valid.clone();
            spoofed[field] = json!("client");
            assert!(
                serde_json::from_value::<Checked>(spoofed).is_err(),
                "{field}"
            );
        }
        let mut witness_spoof = valid.clone();
        witness_spoof["expected"]["role"] = json!("runtime");
        assert!(serde_json::from_value::<Checked>(witness_spoof).is_err());
        let mut spoofed = valid;
        spoofed["change"]["role"] = json!("runtime");
        assert!(serde_json::from_value::<Checked>(spoofed).is_err());
    }

    #[test]
    fn receipts_require_live_commit_evidence_and_never_expose_digest_or_body() {
        let mut receipt = OperationRecord {
            version: RECORD_VERSION,
            id: "id".into(),
            session_id: "s".into(),
            owner: "o".into(),
            method: PATCH.into(),
            payload_hash: "private-digest".into(),
            status: "accepted".into(),
            result: Some(
                json!({"server_instance":"server","runtime_instance_id":"live","commit":{"changed":true}}),
            ),
            revision: 0,
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        assert_eq!(
            projection(&receipt, "server", Some("live"))["status"],
            "queued"
        );
        receipt.status = "completed".into();
        assert_eq!(
            projection(&receipt, "server", Some("live"))["status"],
            "applied"
        );
        for (server, live) in [
            ("restart", Some("live")),
            ("server", Some("reopened")),
            ("server", None),
        ] {
            assert_eq!(projection(&receipt, server, live)["status"], "unknown");
        }
        assert!(
            !projection(&receipt, "server", Some("live"))
                .to_string()
                .contains("private-digest")
        );
        receipt.status = "interrupted".into();
        assert_eq!(
            projection(&receipt, "server", Some("live"))["status"],
            "unknown"
        );
        receipt.status = "failed".into();
        assert_eq!(projection(&receipt, "restart", None)["status"], "rejected");
    }
}
