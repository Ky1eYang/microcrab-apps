//! Durable, body-free terminal receipts for autonomous round-limit stops.
//! The Session supplies exact completion identity; this observer never owns a queue.
use super::super::*;
use microcrab_storage::{OperationRecord, RECORD_VERSION};
use serde_json::json;

pub(crate) async fn settle(manager: &Manager, managed: &Managed) -> Result<(), Error> {
    let _gate = managed.commands.lock().await;
    let stop = managed
        .session
        .subscribe_activity()
        .borrow()
        .latest_round_limit_stop
        .clone();
    let Some(stop) = stop else {
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
                    && result["round_limit_stop_sequence"].as_u64() == Some(stop.sequence)
            })
    }) {
        return Ok(());
    }
    let messages = view::messages(
        managed.session.messages(),
        managed.session.state().epoch_id,
        &record.id,
    );
    let endpoint = messages.iter().rev().find(|message| {
        message["position"]["epoch"].as_u64() == Some(stop.epoch)
            && message["position"]["turn"].as_u64() == Some(stop.turn)
            && message["position"]["round"]
                .as_u64()
                .is_some_and(|round| round <= stop.rounds as u64)
    });
    // An intervening undo/travel can remove the stopped branch. Never invent a
    // notice attached to the newly selected conversation's unrelated endpoint.
    let Some(endpoint) = endpoint else {
        return Ok(());
    };
    let timestamp = operations
        .iter()
        .map(|value| value.created_at_ms)
        .max()
        .map_or(now(), |last| now().max(last.saturating_add(1)));
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
                "round_limit_stop",
                stop.sequence,
            ))?,
            status: "completed".into(),
            revision: 0,
            created_at_ms: timestamp,
            updated_at_ms: timestamp,
            result: Some(json!({"stop_reason":"round_limit","rounds":stop.rounds,
            "position":{"epoch":stop.epoch,"turn":stop.turn,"round":stop.rounds},
            "after_message_id":endpoint["id"],"server_instance":manager.instance,
            "round_limit_stop_sequence":stop.sequence})),
        })
        .await?;
    managed
        .hub
        .publish(crate::manager::SessionRuntimeEvent::Turn(
            serde_json::to_value(receipt)?,
        ));
    Ok(())
}
