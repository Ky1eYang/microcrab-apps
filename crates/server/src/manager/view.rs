//! Native browser projections are explicit: State frames and raw wire audit bodies
//! never become UI messages. Socket subscribe clones only the settled hub snapshot.
use super::Managed;
use microcrab_session::{Error, context::ContentItem};
use microcrab_storage::{Directory, OperationRecord, SessionRecord};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::atomic::Ordering};
mod history;
pub(super) use history::{coherent_messages, history_view, live_message_page, message_page};
mod operation_results;
mod retry;
pub(super) use operation_results::project_operation_results;
pub(super) use retry::retry_supersession;

/// Only model-facing bytes and their turn identity identify a prepared request.
/// Execution observations can advance Journal HEAD without changing this witness.
pub(super) fn retry_witness(
    context: &microcrab_session::context::ContextSnapshot,
    state: &microcrab_session::SessionState,
) -> Result<String, Error> {
    microcrab_storage::history::content_hash(&(
        context,
        state.epoch_id,
        state.turn_id,
        state.round_id,
    ))
}

/// Select the latest failure for this exact prepared request.
pub(super) fn retry_operation_id(operations: &[OperationRecord], witness: &str) -> Option<String> {
    operations
        .iter()
        .filter(|operation| {
            matches!(operation.status.as_str(), "failed" | "cancelled")
                && matches!(
                    operation.method.as_str(),
                    "turn.submit" | "turn.retry" | "turn.inbox" | "inbox.resume"
                )
                && operation.result.as_ref().is_some_and(|result| {
                    result["retry_ready"] == true
                        && result["retry_witness"].as_str() == Some(witness)
                })
        })
        .max_by_key(|operation| operation.created_at_ms)
        .map(|operation| operation.id.clone())
}

/// Bound receipts by time rather than UUID order. Retry readiness must always
/// identify a receipt in the same snapshot, even after unrelated later commands.
pub(super) fn operation_page(
    mut operations: Vec<OperationRecord>,
    retry_operation: Option<&str>,
) -> (Vec<OperationRecord>, bool) {
    operations.retain(|operation| operation.method != "model.route.legacy");
    const LIMIT: usize = 32;
    let truncated = operations.len() > LIMIT;
    operations.sort_by(|left, right| {
        (right.created_at_ms, right.updated_at_ms, &right.id).cmp(&(
            left.created_at_ms,
            left.updated_at_ms,
            &left.id,
        ))
    });
    if let Some(index) =
        retry_operation.and_then(|id| operations.iter().position(|operation| operation.id == id))
        && index >= LIMIT
    {
        let retry = operations.remove(index);
        operations.truncate(LIMIT - 1);
        operations.push(retry);
    } else {
        operations.truncate(LIMIT);
    }
    (operations, truncated)
}

pub(super) fn retain_context_failures(operations: &mut Vec<OperationRecord>, messages: &[Value]) {
    operations.retain(|operation| {
        operation
            .result
            .as_ref()
            .and_then(|result| result.get("after_message_id"))
            .and_then(Value::as_str)
            .is_none_or(|anchor| messages.iter().any(|message| message["id"] == anchor))
    });
}

pub(super) fn session(
    manager: &super::Manager,
    record: &SessionRecord,
    legacy_model: &str,
    status: &str,
) -> Value {
    let mut value = base_session(record, legacy_model, status);
    communication_identity(&mut value, None);
    let slot = manager
        .registry
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&record.id)
        .cloned();
    if let Some(slot) = slot
        && let Ok(active) = slot.active.try_lock()
        && let Some(managed) = active.as_ref()
    {
        let _ = communication_session(&mut value, &managed.session, None);
    }
    if record.project_id.is_none()
        && let Some(flavor) = manager.conversation_flavors.get(&record.owner, &record.id)
    {
        value["host_flavor"] = json!(flavor);
    }
    value
}
pub(super) fn communication_identity(
    value: &mut Value,
    identity: Option<&microcrab_session::state::communication::ConversationIdentity>,
) {
    value["agent_id"] = json!(identity.map(|identity| identity.agent_id.as_str()));
    value["depth"] = json!(identity.map(|identity| identity.depth));
    value["interaction"] = json!(identity.map(|identity| identity.interaction));
    value["parent_session_id"] =
        json!(identity.and_then(|identity| identity.parent_session_id.as_deref()));
    value["current_role"] = Value::Null;
    value["accepts_user_input"] = json!(false);
}
pub(super) fn communication_session(
    value: &mut Value,
    session: &microcrab_session::Session,
    targeted: Option<bool>,
) -> Result<(), Error> {
    communication_identity(value, session.communication_identity().as_ref());
    value["current_role"] = json!(session.current_agent_role()?);
    value["accepts_user_input"] = json!(targeted.unwrap_or_else(|| session.accepts_user_input()));
    Ok(())
}
fn base_session(record: &SessionRecord, legacy_model: &str, status: &str) -> Value {
    let model = record
        .model_selection
        .as_ref()
        .map(|selection| selection.model.as_str())
        .unwrap_or(legacy_model);
    json!({"id":record.id,"title":record.title,"project_id":record.project_id,"status":status,"agent":record.agent,"mode":record.mode,"model":model,"model_selection":record.model_selection,"created_at_ms":record.created_at_ms,"updated_at_ms":record.updated_at_ms,"revision":record.revision,"archived":record.archived})
}
/// Internal display evidence travels with the exact selected compaction contexts.
/// Audit anchors never become user message fields or frozen Context content.
#[derive(Default)]
pub(super) struct DisplayMessages {
    pub messages: Vec<Value>,
    pub historical_heads: Vec<(u64, Option<String>)>,
}
pub(super) async fn display_messages(
    journal: &microcrab_storage::CreatorJournal,
    marker: Option<&microcrab_session::context::CompactionMarker>,
    current: Vec<ContentItem>,
    epoch: u64,
    session_id: &str,
) -> Result<DisplayMessages, Error> {
    fn marked(
        items: Vec<ContentItem>,
        epoch: u64,
        id: &str,
        marker: Option<&microcrab_session::context::CompactionMarker>,
    ) -> Vec<Value> {
        let mut projected = messages(items, epoch, id);
        if let Some(marker) = compactions(marker, &projected)
            .as_array()
            .and_then(|markers| markers.first())
            && let Some(message) = projected
                .iter_mut()
                .find(|message| message["id"] == marker["message_id"])
        {
            message["compaction"] = marker.clone();
        }
        projected
    }
    let mut display = vec![];
    let mut historical_heads = vec![];
    for checkpoint in journal.compacted_history_with_evidence(marker).await? {
        let recover = checkpoint.recover;
        historical_heads.push((recover.state.epoch_id, checkpoint.request_head));
        let mut previous = marked(
            recover.context.items,
            recover.state.epoch_id,
            session_id,
            recover.context.compaction.as_ref(),
        );
        for message in &mut previous {
            message["historical"] = json!(true);
        }
        display.extend(previous);
    }
    display.extend(marked(current, epoch, session_id, marker));
    // Environment/catalog changes retain earlier epoch rows without a compaction
    // marker. Resolve their anchors from exact selected Journal checkpoints too.
    let missing_epochs: Vec<_> = display
        .iter()
        .filter_map(|message| message["position"]["epoch"].as_u64())
        .filter(|represented| {
            *represented != epoch
                && !historical_heads
                    .iter()
                    .any(|(known, _)| known == represented)
        })
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    if !missing_epochs.is_empty() {
        let additional = journal
            .display_request_heads(&missing_epochs)
            .await
            .unwrap_or_else(|_| missing_epochs.iter().map(|epoch| (*epoch, None)).collect());
        historical_heads.extend(additional);
    }
    Ok(DisplayMessages {
        messages: display,
        historical_heads,
    })
}
pub(super) fn compactions(
    marker: Option<&microcrab_session::context::CompactionMarker>,
    messages: &[Value],
) -> Value {
    let retained: Vec<_> = messages
        .iter()
        .filter_map(|message| message.get("compaction").cloned())
        .collect();
    if !retained.is_empty() {
        return json!(retained);
    }
    let Some(marker) = marker else {
        return json!([]);
    };
    let message = messages.iter().find(|message| message["role"] == "user");
    let summary: String = marker.summary.chars().take(8192).collect();
    json!([{"id":marker.id,"epoch":marker.epoch,"summary":summary,"summary_truncated":summary.len() < marker.summary.len(),"message_id":message.and_then(|message| message["id"].as_str())}])
}

pub(super) fn messages(items: Vec<ContentItem>, epoch: u64, session_id: &str) -> Vec<Value> {
    let mut calls = HashMap::<String, (String, Value)>::new();
    let mut messages: Vec<Value> = vec![];
    let mut projection = serde_json::Map::new();
    let mut pending_users = Vec::<usize>::new();
    for (index, item) in items.into_iter().enumerate() {
        // IDs remain compatible with existing browser history. Position comes from
        // the recorded State frames, never a parsed ID or inferred round count.
        let position = |projection: &serde_json::Map<String, Value>| {
            let session = projection.get("session");
            json!({"epoch":session.and_then(|s| s.get("epoch")),"turn":session.and_then(|s| s.get("turn")),"round":session.and_then(|s| s.get("round"))})
        };
        // State frames carry no message id; hashing is skipped for them.
        let id = match &item {
            ContentItem::State(frame) => {
                let mut value = Value::Object(std::mem::take(&mut projection));
                microcrab_transport::apply_state_patch(&mut value, frame)
                    .expect("published Context State frames are validated");
                let Value::Object(state) = value else {
                    unreachable!("State is an object")
                };
                projection = state;
                let scope = position(&projection);
                if scope["turn"].is_u64() && scope["round"].is_u64() {
                    for user in pending_users.drain(..) {
                        messages[user]["position"] = scope.clone();
                    }
                }
                continue;
            }
            _ => super::images::message_id(session_id, epoch, index, &item),
        };
        let mut message = match item {
            ContentItem::State(_) => unreachable!("State frames are projected above"),
            ContentItem::User { parts, source, .. } => {
                let text = parts
                    .iter()
                    .filter_map(|part| match part {
                        microcrab_protocol::InputPart::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                let images_omitted = parts.iter().any(|part| {
                    matches!(part, microcrab_protocol::InputPart::Image { source } if !super::images::available(source))
                });
                let parts: Vec<_> = parts
                    .into_iter()
                    .enumerate()
                    .map(|(part_index, part)| match part {
                        microcrab_protocol::InputPart::Text { text } => {
                            json!({"type":"text","text":text})
                        }
                        microcrab_protocol::InputPart::Image { source } => {
                            super::images::reference(session_id, &id, part_index, &source)
                        }
                    })
                    .collect();
                pending_users.push(messages.len());
                let mut message = json!({"id":id,"role":"user","text":text,"parts":parts,"tool_calls":[],"images_omitted":images_omitted,"position":null});
                if let Some(source) = source {
                    message["source"] = serde_json::to_value(source)
                        .expect("framework provenance enum is serializable");
                }
                message
            }
            ContentItem::Assistant {
                content,
                tool_calls,
                ..
            } => {
                let scope = position(&projection);
                for call in &tool_calls {
                    calls.insert(call.id.clone(), (call.name.clone(), scope.clone()));
                }
                let tool_calls: Vec<Value> = tool_calls
                    .into_iter()
                    .map(|call| {
                        // Runtime registration rejects reserved builtin names for
                        // WASM/external tools. Only this canonical server witness
                        // gives the browser framework identity; payloads do not.
                        let builtin = matches!(
                            microcrab_session::runtime::BuiltinTool::from_name(&call.name),
                            Some(microcrab_session::runtime::BuiltinTool::Delegate)
                        )
                        .then_some("delegate");
                        json!({"id":call.id,"name":call.name,"arguments":call.arguments,"builtin":builtin})
                    })
                    .collect();
                json!({"id":id,"role":"assistant","text":content,"parts":[],"tool_calls":tool_calls,"position":scope})
            }
            ContentItem::Tool {
                call_id,
                content,
                parts,
            } => {
                let images_omitted = parts.iter().any(|part| matches!(part,microcrab_protocol::InputPart::Image {source} if !super::images::available(source)));
                let image_parts: Vec<_> = parts
                    .iter()
                    .enumerate()
                    .filter_map(|(index, part)| match part {
                        microcrab_protocol::InputPart::Image { source } => {
                            Some(super::images::reference(session_id, &id, index, source))
                        }
                        _ => None,
                    })
                    .collect();
                let call = calls.get(&call_id);
                let builtin = call
                    .filter(|(name, _)| {
                        matches!(
                            microcrab_session::runtime::BuiltinTool::from_name(name),
                            Some(microcrab_session::runtime::BuiltinTool::Delegate)
                        )
                    })
                    .map(|_| "delegate");
                json!({"id":id,"role":"tool","text":content,"parts":image_parts,"images_omitted":images_omitted,"tool_calls":[],"tool_name":call.map(|c| &c.0),"tool_builtin":builtin,"call_id":call_id,"position":call.map(|c| &c.1)})
            }
        };
        // Historical records without State frames expose unknown positions instead
        // of manufacturing turn/round identities for audit queries.
        if message.get("position").is_none() {
            message["position"] = Value::Null;
        }
        messages.push(message);
    }
    messages
}
/// Link canonical completed outputs or real task receipts before pagination.
/// Exact output/task identity and recorded position prevent a later branch's result from
/// being mistaken for the selected history. Durable bodies remain on demand.
pub(super) async fn link_client_results(
    bridge: &super::client_tools::ClientTools,
    session_id: &str,
    messages: &mut [Value],
) {
    for message in messages {
        if message["role"] != "tool" {
            continue;
        }
        let (Some(call_id), Some(tool_name), Some(content)) = (
            message["call_id"].as_str(),
            message["tool_name"].as_str(),
            message["text"].as_str(),
        ) else {
            continue;
        };
        let position = &message["position"];
        if let Some(id) = bridge
            .match_canonical_output(
                session_id,
                call_id,
                tool_name,
                position["epoch"].as_u64(),
                position["turn"].as_u64(),
                position["round"].as_u64(),
                content,
            )
            .await
        {
            message["client_invocation_id"] = json!(id);
        }
    }
}
pub(super) fn selected_client_invocations(messages: &[Value]) -> Vec<String> {
    messages
        .iter()
        .filter(|message| message["historical"] != true)
        .filter_map(|message| message["client_invocation_id"].as_str().map(str::to_owned))
        .collect()
}
pub(super) async fn snapshot(
    managed: &Managed,
    record: &SessionRecord,
    model: &str,
    operations: Vec<OperationRecord>,
    witness: &super::statistics::PublicationWitness,
) -> Result<Value, Error> {
    let status = if managed.closing.load(Ordering::Acquire) {
        "closed"
    } else if managed.faulted.load(Ordering::Acquire) {
        "faulted"
    } else if managed.hub.operation().is_some() || managed.session.inbox_snapshot().active {
        "running"
    } else {
        "idle"
    };
    let policy = managed
        .session
        .permission_controller()
        .snapshot()
        .map_err(|_| Error::protocol("permission snapshot unavailable"))?;
    let (display, lineage, display_epoch) = coherent_messages(managed, &record.id).await?;
    let visible_messages = display.messages;
    let selected_invocation_ids = selected_client_invocations(&visible_messages);
    let compactions = compactions(managed.session.compaction().as_ref(), &visible_messages);
    let statistics = managed
        .statistics
        .snapshot(managed, record, &operations, witness)
        .await?;
    let mut operations = operations;
    retain_context_failures(&mut operations, &visible_messages);
    let superseded_failure_operation_ids = retry_supersession(&mut operations, &visible_messages)?;
    let mut page = page(visible_messages.clone(), None, 64);
    let head = managed
        .journal
        .history_anchor()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .request_head
        .clone();
    if let Some(items) = page["items"].as_array_mut() {
        managed
            .statistics
            .message_usage(head.clone(), &visible_messages, items)
            .await;
    }
    let active_state = managed.session.state();
    let mut history_view = history_view(
        &visible_messages,
        &page,
        &lineage,
        display_epoch,
        active_state
            .turn_id
            .map(|turn| (active_state.epoch_id, turn)),
    );
    managed
        .statistics
        .turn_usage(
            head,
            &display.historical_heads,
            &visible_messages,
            &mut history_view,
        )
        .await;
    let retry_operation = if status == "idle" && managed.session.retry_ready() {
        retry_operation_id(
            &operations,
            &retry_witness(
                &managed.session.context_snapshot()?,
                &managed.session.state(),
            )?,
        )
    } else {
        None
    };
    let (operations, operations_truncated) = operation_page(operations, retry_operation.as_deref());
    let mut usage = managed.aggregate.total();
    usage.provider_details.clear();
    let operations = project_operation_results(
        operations,
        &superseded_failure_operation_ids,
        retry_operation.as_deref(),
    );
    let mut projection = base_session(record, model, status);
    communication_session(&mut projection, &managed.session, None)?;
    if let Some(flavor) = managed.host_flavor {
        projection["host_flavor"] = json!(flavor);
    }
    super::model::project_route(managed, record, &mut projection);
    projection["component"] = json!(managed.component);
    projection["kind"] = json!(managed.session.kind());
    projection["retry_ready"] = json!(retry_operation.is_some());
    projection["auto_compaction_retry_available"] =
        json!(managed.session.auto_compaction_retry_available());
    projection["retry_operation_id"] = json!(retry_operation);
    let init_deferred = super::agent_init::deferred(managed);
    let init_pending = init_deferred || super::agent_init::pending(&managed.session);
    projection["agent_initialization_deferred"] = json!(init_deferred);
    projection["agent_init_pending"] = json!(init_pending);
    projection["agent_init_retry_required"] =
        json!(!init_pending && super::agent_init::failure(&managed.session).is_some());
    let mut state = managed.session.state_view()?;
    state["agent_initialization_pending"] = json!(active_state.agent_initialization_pending);
    state["agent_init"] = json!(active_state.agent_init);
    let executions = serde_json::to_value(managed.session.execution_snapshots())
        .map_err(|_| Error::protocol("execution metadata serialization failed"))?;
    let mut client_tools = serde_json::to_value(managed.client_tools.snapshot().await)
        .map_err(|_| Error::protocol("client tool snapshot serialization failed"))?;
    client_tools["selected_invocation_ids"] = json!(selected_invocation_ids);
    let history_available = managed.journal.history_available().await?;
    let snapshot = json!({"inbox":managed.session.inbox_snapshot(),"statistics":statistics,"compactions":compactions,"history_available":history_available,"state":state,"executions":executions,"client_tools":client_tools,"session":projection,"messages":page["items"],"messages_truncated":page["truncated"],"next_cursor":page["next_cursor"],"permission":policy,"operations":operations,"superseded_failure_operation_ids":superseded_failure_operation_ids,"operations_truncated":operations_truncated,"history_view":history_view,"history":managed.journal.cursor().await,"usage":usage});
    bound_snapshot(snapshot)
}

/// Apply the transport bound after every response-specific snapshot addition.
/// State remains available through state.get when omitted from this projection.
pub(super) fn bound_snapshot(mut snapshot: Value) -> Result<Value, Error> {
    if snapshot.get("executions").is_some_and(Value::is_array) {
        snapshot["executions_omitted"] = json!(false);
    }
    // Full State remains authoritative through state.get. The subscription must
    // still deliver canonical history when custom values exceed its frame cap.
    let frame_budget = microcrab_protocol::application::MAX_FRAME_BYTES.saturating_sub(16 * 1024);
    if serde_json::to_vec(&snapshot)
        .map_err(|_| Error::protocol("snapshot serialization failed"))?
        .len()
        > frame_budget
    {
        snapshot["state"] = Value::Null;
        snapshot["state_omitted"] = json!(true);
    }
    if serde_json::to_vec(&snapshot)
        .map_err(|_| Error::protocol("snapshot serialization failed"))?
        .len()
        > frame_budget
        && snapshot
            .get("executions")
            .is_some_and(|value| !value.is_null())
    {
        // Null is unavailable, never an empty history assertion. The separate
        // executions.list route retains its own bounded paging contract.
        snapshot["executions"] = Value::Null;
        snapshot["executions_omitted"] = json!(true);
    }
    if serde_json::to_vec(&snapshot)
        .map_err(|_| Error::protocol("snapshot serialization failed"))?
        .len()
        > frame_budget
    {
        return Err(Error::Limit(
            "subscription snapshot metadata exceeds transport budget".into(),
        ));
    }
    Ok(snapshot)
}
pub(super) fn page(messages: Vec<Value>, before: Option<usize>, limit: usize) -> Value {
    let end = before.unwrap_or(messages.len()).min(messages.len());
    let mut start = end;
    let mut budget = 96 * 1024;
    let mut items = vec![];
    for original in messages[..end].iter().rev().take(limit.clamp(1, 128)) {
        let mut message = original.clone();
        let size = serde_json::to_vec(&message)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        if size > 32 * 1024 {
            let text = message["text"]
                .as_str()
                .unwrap_or_default()
                .chars()
                .take(8192)
                .collect::<String>();
            message["text"] = json!(text);
            message["parts"] = super::images::references(&message["parts"]);
            message["tool_calls"] = json!([]);
            message["content_truncated"] = json!(true);
        }
        let size = serde_json::to_vec(&message)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        if size > budget {
            break;
        }
        budget -= size;
        items.push(message);
        start -= 1;
    }
    items.reverse();
    json!({"items":items,"truncated":start>0,"next_cursor":if start>0{Some(start)}else{None}})
}
pub(super) async fn refresh(
    directory: &dyn Directory,
    managed: &Managed,
    model: &str,
) -> Result<Value, Error> {
    for _ in 0..3 {
        let record = managed.record.lock().await.clone();
        let witness = super::statistics::PublicationWitness::capture(managed, &record)?;
        let operations = directory.list_operations(&record.owner, &record.id).await?;
        let value = snapshot(managed, &record, model, operations, &witness).await?;
        if let Some(value) = witness.publish(managed, value, true).await? {
            return Ok(value);
        }
    }
    // A continuously changing foreground Context will publish its next semantic
    // observation. Return the canonical value, never the rejected old calculation.
    Ok(managed.hub.snapshot())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn snapshot_bounds_history_independently_from_current_state() {
        let snapshot = bound_snapshot(json!({
            "state":{"custom":7},
            "executions":[{"execution_id":1,"tool":"x".repeat(microcrab_protocol::application::MAX_FRAME_BYTES)}],
            "session":{"id":"s"}, "messages":[{"id":"m","text":"visible"}]
        })).unwrap();
        assert!(snapshot["state"].is_null());
        assert_eq!(snapshot["state_omitted"], true);
        assert!(snapshot["executions"].is_null());
        assert_eq!(snapshot["executions_omitted"], true);
        assert_eq!(snapshot["messages"][0]["text"], "visible");
        let small = bound_snapshot(json!({"state":{},"executions":[]})).unwrap();
        assert_eq!(small["executions"], json!([]));
        assert_eq!(small["executions_omitted"], false);
    }
    #[test]
    fn framework_source_is_explicit_and_ordinary_message_shape_is_unchanged() {
        let user = |source| ContentItem::User {
            envelope: None,
            parts: vec![microcrab_protocol::InputPart::Text {
                text: "Framework execution notice".into(),
            }],
            source,
        };
        let projected = messages(
            vec![
                user(None),
                user(Some(microcrab_transport::UserSource::Framework)),
            ],
            0,
            "session",
        );
        assert!(projected[0].get("source").is_none());
        assert_eq!(projected[1]["source"], "framework");
        assert_ne!(projected[0]["id"], projected[1]["id"]);
    }
    #[test]
    fn communication_sources_preserve_identity_without_state_conversation_blocks() {
        use microcrab_transport::{CarrierEnvelopeVersion, StateFrame, UserSource};
        let source = |source| ContentItem::User {
            source: Some(source),
            envelope: Some(CarrierEnvelopeVersion::V1),
            parts: vec![microcrab_protocol::InputPart::Text {
                text: "carrier".into(),
            }],
        };
        let messages = messages(
            vec![
                ContentItem::State(StateFrame::StateSnapshot {
                    state: json!({"external_speakers":[{"identity":"monitor"}]}),
                }),
                source(UserSource::External {
                    identity: "monitor".into(),
                }),
                source(UserSource::Agent {
                    agent_id: "agt_0123456789abcdef".into(),
                    exchange: None,
                }),
            ],
            0,
            "session",
        );
        assert_eq!(messages.len(), 2);
        assert_eq!(
            messages[0]["source"],
            json!({"external":{"identity":"monitor"}})
        );
        assert_eq!(
            messages[1]["source"],
            json!({"agent":{"agent_id":"agt_0123456789abcdef"}})
        );
        assert!(
            messages
                .iter()
                .all(|message| message.get("state").is_none())
        );
    }
    #[test]
    fn messages_keep_recorded_positions_across_tool_rounds_and_epochs() {
        use microcrab_transport::StateFrame;
        let items = vec![
            ContentItem::User {
                envelope: None,
                source: None,
                parts: vec![microcrab_protocol::InputPart::Text { text: "go".into() }],
            },
            ContentItem::State(StateFrame::StateSnapshot {
                state: json!({"session":{"epoch":2,"turn":7,"round":1}}),
            }),
            ContentItem::Assistant {
                provider_metadata: None,
                content: String::new(),
                tool_calls: vec![microcrab_protocol::ToolCall {
                    provider_metadata: Some(microcrab_protocol::ToolCallProviderMetadata::Google {
                        thought_signature: "b3BhcXVlLWNhbm9uaWNhbC12aWV3".into(),
                    }),
                    id: "c".into(),
                    name: "read".into(),
                    arguments: json!({"path":"a"}),
                }],
            },
            ContentItem::Tool {
                parts: vec![],
                call_id: "c".into(),
                content: "ok".into(),
            },
            ContentItem::State(StateFrame::StatePatch {
                set: serde_json::from_value(json!({"session":{"epoch":2,"turn":7,"round":2}}))
                    .unwrap(),
                remove: vec![],
                execution_delta: None,
                task_delta: None,
                job_delta: None,
            }),
            ContentItem::Assistant {
                provider_metadata: None,
                content: "done".into(),
                tool_calls: vec![],
            },
        ];
        let result = messages(items, 2, "s");
        assert!(
            !serde_json::to_string(&result)
                .unwrap()
                .contains("b3BhcXVlLWNhbm9uaWNhbC12aWV3")
        );
        assert!(
            result[1]["tool_calls"][0]
                .get("provider_metadata")
                .is_none()
        );
        assert_eq!(result[1]["tool_calls"][0]["arguments"], json!({"path":"a"}));
        assert_eq!(result[0]["position"], json!({"epoch":2,"turn":7,"round":1}));
        assert_eq!(result[2]["position"], result[1]["position"]);
        assert_eq!(result[3]["position"]["round"], 2);
        assert!(
            result
                .iter()
                .all(|item| item["id"].as_str().unwrap().starts_with("message-"))
        );
    }

    #[test]
    fn messages_expose_delegate_identity_only_for_canonical_reserved_calls() {
        let result = messages(
            vec![
                ContentItem::Assistant {
                    provider_metadata: None,
                    content: String::new(),
                    tool_calls: vec![microcrab_protocol::ToolCall {
                        provider_metadata: None,
                        id: "delegate-call".into(),
                        name: "delegate".into(),
                        arguments: json!({}),
                    }],
                },
                ContentItem::Tool {
                    parts: vec![],
                    call_id: "delegate-call".into(),
                    content: "opaque receipt".into(),
                },
                ContentItem::Tool {
                    parts: vec![],
                    call_id: "orphan".into(),
                    content: json!({"child_id":"0123456789abcdef","builtin":"delegate"})
                        .to_string(),
                },
            ],
            2,
            "session",
        );
        assert_eq!(result[0]["tool_calls"][0]["builtin"], "delegate");
        assert_eq!(result[1]["tool_builtin"], "delegate");
        assert!(result[2]["tool_builtin"].is_null());
        assert!(result[2].get("child_session_id").is_none());
    }
}
