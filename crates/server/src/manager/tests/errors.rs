//! Diagnostic categories survive publication without leaking upstream private text.
use super::*;

#[test]
fn failure_receipts_retain_safe_categories_without_private_provider_details() {
    let private = "private-provider-body-secret";
    let failures = [
        (Error::ProviderStatus { status: 503 }, "provider_status"),
        (Error::Transport, "transport"),
        (Error::ProviderTimeout, "provider_timeout"),
        (
            Error::Provider(format!("concrete provider rejection; response_body={private}").into()),
            "provider",
        ),
        (
            Error::Protocol(format!("concrete protocol mismatch; token={private}").into()),
            "protocol",
        ),
        (Error::Limit(private.into()), "limit"),
    ];
    for (failure, category) in failures {
        let wire = safe(failure);
        assert_eq!(wire.code, category);
        let encoded = serde_json::to_string(&wire).unwrap();
        assert!(!encoded.contains(private));
        assert!(!encoded.contains("Operation failed"));
    }
}

#[test]
fn io_publication_retains_typed_causes_without_private_error_text() {
    let wire = safe(Error::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "missing cache file; token=private",
    )));
    assert_eq!(wire.code, "io");
    assert!(wire.message.contains("missing cache file"));
    assert_eq!(wire.details.as_ref().unwrap()["kind"], "NotFound");
    assert!(!serde_json::to_string(&wire).unwrap().contains("private"));
    let wire = safe(Error::Io(std::io::Error::from_raw_os_error(5)));
    assert_eq!(wire.details.unwrap()["raw_os_error"], 5);
}

#[test]
fn authored_protocol_cause_survives_transport_session_and_wire() {
    let transport = microcrab_transport::validate_request_rewrite(
        "private malformed authored body",
        "{}",
        4096,
    )
    .unwrap_err();
    assert!(matches!(
        &transport,
        microcrab_transport::Error::Protocol(_)
    ));
    let session = Error::from(transport);
    assert_eq!(
        session.public_protocol_reason(),
        Some("invalid authored provider body")
    );
    let wire = safe(session);
    assert_eq!(wire.code, "protocol");
    assert_eq!(wire.message, "invalid authored provider body");
    assert_eq!(
        wire.details.unwrap()["reason"],
        "invalid authored provider body"
    );
}

#[test]
fn nested_provider_protocol_cause_publishes_only_the_safe_reason() {
    let private = "private-call-id provider-body /private/storage";
    let transport =
        microcrab_transport::Error::Provider(microcrab_transport::ProviderError::protocol(
            microcrab_transport::ProtocolError::with_private_reason(
                "invalid tool arguments JSON",
                format!("invalid tool arguments JSON for call {private}"),
            ),
        ));
    let session = Error::from(transport);
    assert!(matches!(&session, Error::Provider(_)));
    let wire = safe(session);
    assert_eq!(wire.code, "protocol");
    assert_eq!(wire.message, "invalid tool arguments JSON");
    assert_eq!(
        wire.details.as_ref().unwrap()["reason"],
        "invalid tool arguments JSON"
    );
    assert!(!serde_json::to_string(&wire).unwrap().contains(private));
    for private in [private, "invalid tool arguments JSON"] {
        let wire = safe(Error::Protocol(
            format!("concrete protocol mismatch; token={private}").into(),
        ));
        assert_eq!(wire.code, "protocol");
        assert!(wire.message.contains("concrete protocol mismatch"));
        assert!(wire.details.is_some());
    }
}

#[test]
fn empty_completion_wire_retains_closed_facts_without_fabricating_an_answer() {
    use microcrab_transport::{EmptyCompletionFacts, EmptyCompletionFinish, ProviderError};
    for (finish, expected) in [
        (EmptyCompletionFinish::Stop, "stop"),
        (EmptyCompletionFinish::Length, "length"),
        (EmptyCompletionFinish::Unknown, "unknown"),
    ] {
        for reasoning_present in [false, true] {
            let transport = microcrab_transport::Error::Provider(ProviderError::empty_completion(
                EmptyCompletionFacts {
                    reasoning_present,
                    finish,
                },
            ));
            let wire = safe(Error::from(transport));
            assert_eq!(wire.code, "provider");
            let details = wire.details.unwrap();
            assert_eq!(details["provider_code"], "empty_assistant_response");
            assert_eq!(details["reasoning_present"], reasoning_present);
            assert_eq!(details["finish_reason"], expected);
            assert!(details.get("content").is_none());
            assert!(details.get("reasoning").is_none());
        }
    }
}

#[derive(Clone, Default)]
struct ProtocolLogCapture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for ProtocolLogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn protocol_logs_publish_known_causes_and_suppress_private_details() {
    let capture = ProtocolLogCapture::default();
    let writer = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(move || writer.clone())
        .without_time()
        .with_ansi(false)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        safe(Error::protocol_with_private_reason(
            "journal commit worker failed",
            "private-storage-worker-panic".into(),
        ));
        safe(Error::Provider(
            microcrab_transport::ProviderError::protocol(
                microcrab_transport::ProtocolError::with_private_reason(
                    "invalid tool arguments JSON",
                    "private-tool-call-id".into(),
                ),
            ),
        ));
        safe(Error::Protocol(
            "concrete protocol mismatch; response_body=private-custom-protocol-body".into(),
        ));
        safe(Error::Provider(
            "concrete provider rejection; response_body=private-custom-provider-body".into(),
        ));
    });
    let bytes = capture.0.lock().unwrap();
    let log = std::str::from_utf8(&bytes).unwrap();
    assert!(log.contains("journal commit worker failed"));
    assert!(log.contains("invalid tool arguments JSON"));
    assert!(log.contains("concrete protocol mismatch"));
    assert!(!log.contains("private-"), "{log}");
}

#[test]
fn idle_timeout_wire_details_preserve_measurements_and_omit_unsafe_numbers() {
    let details = microcrab_session::ProviderTimeoutDetails {
        timeout_ms: 60000,
        elapsed_ms: 180000,
        first_token_ms: Some(1000),
        streamed_bytes: 9000,
        streamed_chars: 7000,
    };
    let wire = safe(Error::ProviderIdleTimeout(details.clone()));
    assert_eq!(wire.code, "provider_timeout");
    assert_eq!(
        *wire.details.unwrap(),
        serde_json::json!({"reason":"idle_timeout", "timeout_ms":60000,
        "elapsed_ms":180000, "first_token_ms":1000, "streamed_bytes":9000, "streamed_chars":7000})
    );
    let wire = safe(Error::ProviderIdleTimeout(
        microcrab_session::ProviderTimeoutDetails {
            elapsed_ms: u64::MAX,
            first_token_ms: None,
            ..details
        },
    ));
    let details = wire.details.unwrap();
    assert!(details.get("elapsed_ms").is_none());
    assert!(details.get("first_token_ms").is_none());
    assert_eq!(safe(Error::Cancelled).code, "cancelled");
}

#[test]
fn limit_wire_details_are_bounded_and_do_not_export_private_display_text() {
    use microcrab_session::{LimitError, LimitKind};
    let private = "C:/private/config-secret provider-body";
    let generic = safe(Error::Limit(private.into()));
    assert_eq!(generic.code, "limit");
    assert_eq!(generic.details.as_ref().unwrap()["reason"], "other");
    assert_eq!(generic.details.as_ref().unwrap()["pre_effect"], false);
    assert!(!serde_json::to_string(&generic).unwrap().contains(private));
    let mut limit: LimitError = private.into();
    limit.kind = LimitKind::ToolResultReservation;
    limit.calls = Some(8);
    limit.required_bytes = Some(3 * 1024 * 1024);
    limit.budget_bytes = Some(2 * 1024 * 1024);
    let wire = safe(Error::Limit(limit.clone()));
    let details = wire.details.unwrap();
    assert_eq!(details["reason"], "tool_result_reservation");
    assert_eq!(details["calls"], 8);
    assert_eq!(details["required_bytes"], 3 * 1024 * 1024);
    assert_eq!(details["budget_bytes"], 2 * 1024 * 1024);
    assert_eq!(details["pre_effect"], false);
    assert!(!wire.message.contains(private));
    limit.required_bytes = Some(usize::MAX);
    limit.required_count = Some(usize::MAX);
    let large = safe(Error::Limit(limit));
    if usize::BITS > 53 {
        let details = large.details.unwrap();
        assert!(details.get("required_bytes").is_none());
        assert!(details.get("required_count").is_none());
    }
    let count_error = microcrab_session::budget::check(LimitKind::Processes, 3, 2)
        .expect_err("process admission must reject before capacity is exceeded");
    let count_wire = safe(count_error);
    assert_eq!(
        *count_wire.details.unwrap(),
        serde_json::json!({
            "reason":"processes", "pre_effect":false, "required_count":3, "budget_count":2
        })
    );
}
