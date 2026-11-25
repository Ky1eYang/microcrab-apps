//! Best-effort transport of safe diagnostic facts. Local persistence happens
//! before enqueue; telemetry cannot block replies, cancellation or lease pings.
use microcrab_host_protocol::{BridgeDiagnostic, BridgeDiagnosticPhase, WireFrame};
use tokio::sync::mpsc;

pub(crate) fn event(
    bridge: &crate::Bridge,
    generation: &str,
    phase: BridgeDiagnosticPhase,
) -> BridgeDiagnostic {
    BridgeDiagnostic {
        event_id: uuid::Uuid::new_v4().to_string(),
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|time| time.as_millis().min(u128::from(u64::MAX)) as u64)
            .unwrap_or(0),
        bridge_instance_id: bridge.descriptor.bridge_instance_id.clone(),
        request_id: None,
        operation_id: None,
        lease_generation: generation.into(),
        method: None,
        phase,
        error_code: None,
        elapsed_ms: None,
    }
}
pub(crate) fn safe_id(value: &str) -> &str {
    if uuid::Uuid::parse_str(value).is_ok() {
        value
    } else {
        "invalid_id"
    }
}
pub(crate) fn emit(outgoing: &mpsc::Sender<WireFrame>, mut event: BridgeDiagnostic) {
    event.request_id = event
        .request_id
        .filter(|id| uuid::Uuid::parse_str(id).is_ok());
    event.operation_id = event
        .operation_id
        .filter(|id| uuid::Uuid::parse_str(id).is_ok());
    // This typed payload contains identifiers/categories only, never requests,
    // native messages, command arguments, credentials or process output.
    match serde_json::to_string(&event) {
        Ok(encoded) => tracing::warn!(bridge_diagnostic=%encoded, "Host diagnostic event"),
        Err(_) => tracing::warn!(
            reason = "diagnostic_encoding_failed",
            "Host diagnostic observation lost"
        ),
    }
    if let Err(error) = outgoing.try_send(WireFrame::Diagnostic { event }) {
        let reason = match error {
            mpsc::error::TrySendError::Full(_) => "diagnostic_queue_full",
            mpsc::error::TrySendError::Closed(_) => "diagnostic_connection_closed",
        };
        tracing::warn!(
            reason,
            "Host diagnostic transport unavailable; local evidence retained"
        );
    }
}
