//! Cancellation receipts project only operation-owned closed causes. They never
//! infer a cause from a later Session status or copy arbitrary Host error bodies.
use microcrab_protocol::application::WireError;
use microcrab_session::{HostDetachCode, InterruptReason};
use serde_json::json;

pub(super) fn host_code(code: &microcrab_host_protocol::ErrorCode) -> HostDetachCode {
    use microcrab_host_protocol::ErrorCode;
    match code {
        ErrorCode::Disconnected => HostDetachCode::Disconnected,
        ErrorCode::LeaseExpired => HostDetachCode::LeaseExpired,
        ErrorCode::BindingMismatch => HostDetachCode::BindingMismatch,
        _ => HostDetachCode::Unavailable,
    }
}

pub(super) fn wire(reason: Option<&InterruptReason>) -> WireError {
    let (code, detail, host) = match reason {
        Some(InterruptReason::UserRequested) => (Some("user_requested"), "Stopped by user", None),
        Some(InterruptReason::HostDetached { code }) => (
            Some("host_detached"),
            match code {
                HostDetachCode::Disconnected => "Interrupted: Host connection disconnected",
                HostDetachCode::Replaced => "Interrupted: Host connection replaced",
                HostDetachCode::LeaseExpired => "Interrupted: Host lease expired",
                HostDetachCode::BindingMismatch => "Interrupted: Host binding no longer matches",
                HostDetachCode::Unavailable => "Interrupted: Host connection unavailable",
            },
            Some(code),
        ),
        Some(InterruptReason::SessionClosed) => {
            (Some("session_closed"), "Interrupted: Session closed", None)
        }
        Some(InterruptReason::StateReplaced) => (
            Some("state_replaced"),
            "Interrupted: Session state replaced",
            None,
        ),
        Some(InterruptReason::ClientToolDisconnected) => (
            Some("client_tool_disconnected"),
            "Interrupted: Client tool executor disconnected",
            None,
        ),
        None => (None, "Operation cancelled", None),
    };
    let mut error = WireError::new("cancelled", detail);
    let mut details = json!({"cancel_reason":code,"detail":detail});
    if let Some(host) = host {
        details["host_detach_code"] = json!(host);
    }
    error.details = Some(Box::new(details));
    error
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn receipts_preserve_closed_causes_without_inventing_unknown_user_actions() {
        for (reason, code) in [
            (InterruptReason::UserRequested, "user_requested"),
            (
                InterruptReason::HostDetached {
                    code: HostDetachCode::Replaced,
                },
                "host_detached",
            ),
            (InterruptReason::SessionClosed, "session_closed"),
            (
                InterruptReason::ClientToolDisconnected,
                "client_tool_disconnected",
            ),
        ] {
            let result = wire(Some(&reason));
            assert_eq!(result.code, "cancelled");
            assert_eq!(result.details.as_ref().unwrap()["cancel_reason"], code);
            assert_ne!(result.message, "Operation cancelled");
        }
        assert!(wire(None).details.unwrap()["cancel_reason"].is_null());
        assert_eq!(
            wire(Some(&InterruptReason::HostDetached {
                code: HostDetachCode::LeaseExpired
            }))
            .details
            .unwrap()["host_detach_code"],
            "lease_expired"
        );
    }
}
