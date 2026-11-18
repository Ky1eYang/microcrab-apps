//! Explicit owner-admitted Agent initialization retry. Durable receipts fence HTTP
//! retries before effects; blocked Sessions remain inspectable without resetting
//! failed/unknown attempts during ordinary open or turn submission.
use super::{Manager, commands, now, safe, view};
use microcrab_protocol::application::{Command, WireError};
use microcrab_session::{
    Session,
    runtime::journal::{AgentInitRecord, AgentInitStatus},
};
use microcrab_storage::{OperationRecord, RECORD_VERSION, SessionRecord};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::{Arc, atomic::Ordering};
/// Slot owns initialization admission. Dropping a command never authorizes
/// replay: Session's already-durable Intent/Failed evidence remains authoritative.
pub(super) struct Admission(Arc<super::Slot>);
impl Admission {
    pub(super) fn acquire(slot: Arc<super::Slot>) -> Result<Self, WireError> {
        slot.initializing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| pending_error())?;
        Ok(Self(slot))
    }
}
impl Drop for Admission {
    fn drop(&mut self) {
        self.0.initializing.store(false, Ordering::Release);
    }
}
struct ReceiptOwner(Arc<super::hub::Hub>);
impl Drop for ReceiptOwner {
    fn drop(&mut self) {
        self.0.set_operation(None);
    }
}
pub(super) fn pending_error() -> WireError {
    WireError::new(
        "agent_init_pending",
        "Agent initialization is running; inspect or answer its permission request",
    )
}
pub(super) fn running(manager: &Manager, id: &str) -> bool {
    manager
        .registry
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(id)
        .is_some_and(|slot| slot.initializing.load(Ordering::Acquire))
}
pub(super) fn pending(session: &Session) -> bool {
    session.execution_snapshots().iter().any(|row| {
        matches!(
            row.subject,
            microcrab_session::policy::InvocationSubject::AgentInit { .. }
        ) && row.status.is_pending()
    })
}

pub(super) fn blocked(records: &[AgentInitRecord]) -> bool {
    records
        .iter()
        .any(|record| !matches!(record.status, AgentInitStatus::Ready))
}
pub(super) fn deferred(managed: &super::Managed) -> bool {
    managed
        .slot
        .upgrade()
        .is_some_and(|slot| slot.initialization_deferred.load(Ordering::Acquire))
}
/// A capability-empty Server Host needs no browser compute route to initialize.
/// This controls startup scheduling only; Session.start still enforces actual policy.
pub(super) fn requires_deferred_start(
    chat_only: bool,
    initialization: microcrab_permission::Action,
) -> bool {
    !(chat_only && initialization == microcrab_permission::Action::Allow)
}

pub(super) fn deferred_error() -> WireError {
    WireError::new(
        "agent_init_deferred",
        "Bind an owner interaction client, then explicitly open the session to initialize its Agent; execution Hosts also require their executor",
    )
}
pub(super) async fn open_deferred(
    manager: &Manager,
    record: &SessionRecord,
) -> Result<Option<Arc<super::Managed>>, WireError> {
    let slot = manager
        .registry
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&record.id)
        .cloned();
    let Some(slot) = slot else {
        return Ok(None);
    };
    if !slot.initialization_deferred.load(Ordering::Acquire) {
        return Ok(None);
    }
    let Some(managed) = slot.active.lock().await.clone() else {
        return Ok(None);
    };
    let command = managed.commands.lock().await;
    if !slot.initialization_deferred.load(Ordering::Acquire) {
        return Ok(None);
    }
    if record.archived
        || managed.closing.load(Ordering::Acquire)
        || managed.faulted.load(Ordering::Acquire)
        || managed.suspended.load(Ordering::Acquire)
    {
        return Err(WireError::new(
            "closed",
            "Session is unavailable for initialization",
        ));
    }
    if !manager.embedded {
        manager.record_host(record).await?;
    }
    let chat_only = managed.host_flavor == Some(super::conversation_flavors::HostFlavor::ChatOnly);
    let interactive = if chat_only {
        managed
            .client_tools
            .with_interaction_audience(|present| present)
            .await
    } else {
        managed
            .client_tools
            .with_executor(|executor| executor.is_some())
            .await
    };
    if !interactive {
        return Err(if chat_only {
            WireError::new(
                "participant_required",
                "Bind an owner participant before initializing this Chat Only Agent",
            )
        } else {
            WireError::new(
                "executor_required",
                "Bind the session executor before initializing its Agent",
            )
        });
    }
    let admission = Admission::acquire(slot.clone())?;
    slot.initialization_deferred.store(false, Ordering::Release);
    drop(command);
    let result = managed.session.start().await;
    drop(admission);
    match result {
        Ok(()) => Ok(Some(managed)),
        Err(error) => Err(failure(&managed.session).unwrap_or_else(|| safe(error))),
    }
}
pub(super) fn failure(session: &Session) -> Option<WireError> {
    if session.agent_initialization_complete() {
        return None;
    }
    let state = session.state();
    failure_evidence(&state.agent_init, state.agent_initialization_pending)
}
#[cfg(test)]
pub(super) fn failure_records(records: &[AgentInitRecord]) -> Option<WireError> {
    failure_evidence(records, false)
}
fn failure_evidence(
    records: &[AgentInitRecord],
    initialization_pending: bool,
) -> Option<WireError> {
    if !initialization_pending && !blocked(records) {
        return None;
    }
    let mut error = WireError::new(
        "agent_init_failed",
        "Agent initialization requires inspection and an explicit retry",
    );
    error.details = Some(Box::new(
        json!({"retry_required":true,"agent_initialization_pending":initialization_pending,"agent_init":records}),
    ));
    if initialization_pending
        && !blocked(records)
        && let Some(details) = error.details.as_mut()
    {
        details["reason"] = json!("init_outcome_unknown");
    }
    Some(error)
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
        || operation.method != commands::SESSION_RETRY_AGENT_INIT
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
                    "agent_init_failed",
                    "Initialization attempt failed; inspect Session state",
                )
            })),
        _ => Err(WireError::new(
            "operation_interrupted",
            "Initialization retry was already admitted; inspect its outcome before a new explicit attempt",
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
    let managed = manager.activate(record, true, None, true).await?;
    let _command = managed.commands.lock().await;
    let record = managed.record.lock().await.clone();
    if receipt(manager, owner, &record, &params, &digest).await? {
        // Match archive/delete's Journal-before-command order. A completed
        // duplicate needs no command ownership while projecting its snapshot.
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
            "Session is unavailable for initialization retry",
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
    if deferred(&managed) {
        return Err(deferred_error());
    }
    if failure(&managed.session).is_none() {
        return Err(WireError::new(
            "agent_init_ready",
            "Agent initialization does not require retry",
        ));
    }
    manager.record_host(&record).await?;
    let slot = manager
        .registry
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&record.id)
        .cloned()
        .ok_or_else(|| WireError::new("closed", "Session is unavailable"))?;
    let initialization = Admission::acquire(slot)?;
    let timestamp = now();
    let mut operation = manager
        .directory
        .create_operation(OperationRecord {
            version: RECORD_VERSION,
            id: params.operation_id,
            session_id: record.id.clone(),
            owner: owner.into(),
            method: commands::SESSION_RETRY_AGENT_INIT.into(),
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
    let operation_owner = ReceiptOwner(managed.hub.clone());
    drop(_command);
    let result = async {
        managed
            .session
            .retry_agent_init()
            .await
            .map_err(|error| failure(&managed.session).unwrap_or_else(|| safe(error)))?;
        if let Some(error) = failure(&managed.session) {
            return Err(error);
        }
        Ok::<(), WireError>(())
    }
    .await;
    operation.revision = 1;
    operation.updated_at_ms = now().max(timestamp);
    operation.status = if result.is_ok() {
        "completed"
    } else {
        "failed"
    }
    .into();
    operation.result = Some(match &result {
        Ok(()) => json!({"ready":true}),
        Err(error) => json!({"error":error}),
    });
    manager
        .directory
        .replace_operation(owner, 0, operation)
        .await
        .map_err(safe)?;
    // Refresh publishes settled failure details too. No automatic retry path is
    // attached to socket reconnect or failed ordinary activation.
    drop(operation_owner);
    drop(initialization);
    let snapshot = manager.refresh(&managed).await.map_err(safe)?;
    result?;
    view::bound_snapshot(snapshot).map_err(safe)
}

#[cfg(test)]
mod startup_policy_tests {
    use super::*;
    #[test]
    fn only_chat_only_allow_starts_without_a_bound_browser_client() {
        use microcrab_permission::Action;
        assert!(!requires_deferred_start(true, Action::Allow));
        assert!(requires_deferred_start(true, Action::Ask));
        assert!(requires_deferred_start(true, Action::Deny));
        for policy in [Action::Allow, Action::Ask, Action::Deny] {
            assert!(requires_deferred_start(false, policy));
        }
    }
}
