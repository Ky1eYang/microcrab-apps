//! Command parsing keeps trusted schema facts and withholds arbitrary serde values.
use super::*;

fn command(params: Value) -> Command {
    Command {
        v: 1,
        kind: microcrab_protocol::application::CommandKind::Command,
        request_id: "validation-test".into(),
        method: PROJECT_CREATE.into(),
        session_id: None,
        params,
    }
}
fn failure<T: DeserializeOwned>(params: Value) -> WireError {
    match parse::<T>(&command(params)) {
        Ok(_) => panic!("invalid fixture unexpectedly parsed"),
        Err(error) => error,
    }
}

#[test]
fn command_parse_never_echoes_unknown_enum_variants_or_bad_values() {
    #[derive(Deserialize)]
    enum Choice {
        Known,
    }
    for secret in [
        "private-secret",
        "missing field `name`",
        "value, expected a string",
        "line\nprivate-secret",
    ] {
        let error = failure::<Choice>(json!(secret));
        assert_eq!(
            error.details.as_ref().unwrap()["reason"]["kind"],
            "unknown_variant"
        );
        assert!(!serde_json::to_string(&error).unwrap().contains(secret));
    }
}

#[test]
fn known_validation_rules_keep_their_causes() {
    let error = operation_id("private-secret").unwrap_err();
    assert_eq!(error.message, "operation_id must be a UUID");
    for (inbox, item, revision, expected) in [
        ("private-secret", 1, 0, "expected_inbox_id"),
        ("0123456789abcdef0123456789abcdef", 0, 0, "item_id"),
        (
            "0123456789abcdef0123456789abcdef",
            1,
            1 << 53,
            "expected_revision",
        ),
    ] {
        let error = inbox::validate_target(inbox, item, revision).unwrap_err();
        assert!(error.message.contains(expected));
        assert!(!error.message.contains("private-secret"));
    }
    for (cause, expected) in [
        ("matcher byte budget", "4096 bytes"),
        ("invalid JSON pointer", "start with /"),
        ("invalid JSON pointer escape", "~0 and ~1"),
        (
            "predicate count budget (16) exceeded",
            "16 argument predicates",
        ),
        ("private-secret", "no recognized safe cause"),
    ] {
        let error = permission_validation(microcrab_permission::ValidationError(cause.into()));
        assert!(error.message.contains(expected));
        assert!(!error.message.contains("private-secret"));
    }
}
