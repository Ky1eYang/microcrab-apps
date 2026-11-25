//! Safe transport errors never include request bodies or storage/provider detail.
use axum::{
    Json,
    extract::Request,
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use microcrab_protocol::application::WireError;
use serde_json::json;

pub(crate) struct Error {
    status: StatusCode,
    wire: WireError,
}
impl Error {
    pub(crate) fn new(status: StatusCode, code: &str, message: &str) -> Self {
        Self {
            status,
            wire: WireError::new(code, message),
        }
    }
    pub(crate) fn invalid(reason: &'static str) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", reason)
    }
    pub(crate) fn parameters(source: &'static str, reason: &'static str) -> Self {
        let mut error = Self::invalid(reason);
        error.wire.details = Some(Box::new(json!({"source":source, "category":"data"})));
        error
    }
    pub(crate) fn path(reason: &'static str) -> Self {
        Self::parameters("path", reason)
    }
    pub(crate) fn query(error: impl std::error::Error) -> Self {
        let mut result = Self::invalid("Query parameters do not match the endpoint schema");
        // Inspect only closed serde templates; never publish rejection Display.
        let mut cause: &dyn std::error::Error = &error;
        for _ in 0..8 {
            let Some(source) = cause.source() else {
                break;
            };
            cause = source;
        }
        let facts = safe_serde_reason(&cause.to_string());
        // The path wrapper may contain an arbitrary request key. Render only
        // the closed parser rule and schema facts extracted above; sanitizing
        // the wrapper's Display cannot establish that a path is public.
        let rule = match facts["kind"].as_str() {
            Some("expected_integer") => "invalid digit found in string",
            Some("integer_out_of_range") => "number too large to fit in target type",
            Some("unknown_field") => "unknown field in query parameters",
            Some("unknown_variant") => "unknown query parameter variant",
            Some("missing_field") => "missing query parameter",
            Some("duplicate_field") => "duplicate query parameter",
            Some("invalid_type") => "invalid type",
            Some("invalid_value") => "invalid value",
            Some("invalid_length") => "invalid length",
            _ => "query parameter schema or encoding mismatch",
        };
        let mut cause = match facts["field"].as_str() {
            Some(field) => format!("{field}: {rule}"),
            None => rule.to_owned(),
        };
        if let Some(expected) = facts["expected"].as_str() {
            cause.push_str(", expected ");
            cause.push_str(expected);
        }
        result.wire.message = format!(
            "Query parameters do not match the endpoint schema: {}",
            cause
        );
        result.wire.details = Some(Box::new(
            json!({"source":"query", "category":"data", "reason":facts, "cause":cause}),
        ));
        result
    }
    pub(crate) fn json(error: serde_json::Error) -> Self {
        Self::from(json_error(
            "JSON body must match the endpoint schema",
            "json",
            &error,
        ))
    }
    pub(crate) fn internal(reason: &'static str) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", reason)
    }
}
/// JSON diagnostics carry only closed categories, positions, and trusted schema facts.
/// The display string is inspected privately because serde may embed arbitrary values.
pub(crate) fn json_error(
    reason: &'static str,
    source: &'static str,
    error: &serde_json::Error,
) -> WireError {
    let category = match error.classify() {
        serde_json::error::Category::Io => "io",
        serde_json::error::Category::Syntax => "syntax",
        serde_json::error::Category::Data => "data",
        serde_json::error::Category::Eof => "eof",
    };
    let facts = safe_serde_reason(&error.to_string());
    let cause = microcrab_protocol::diagnostic::json_reason(error);
    let message = format!(
        "{reason}: {cause} ({category}, line {}, column {})",
        error.line(),
        error.column()
    );
    let mut result = WireError::new("invalid_request", message);
    result.details = Some(Box::new(json!({
        "source":source, "category":category, "line":error.line(), "column":error.column(),
        "reason":facts, "cause":cause
    })));
    result
}

fn safe_serde_reason(message: &str) -> serde_json::Value {
    // Missing/duplicate fields are authored by derives, but custom deserializers
    // may manufacture them. Accept only this explicit server-schema vocabulary.
    const FIELDS: &[&str] = &[
        "operation_id",
        "failed_operation_id",
        "project_id",
        "conversation_id",
        "session_id",
        "name",
        "root",
        "id",
        "title",
        "expected_revision",
        "enabled",
        "expected_inbox_id",
        "item_id",
        "input",
        "parts",
        "text",
        "source",
        "type",
        "media_type",
        "data",
        "mode",
        "agent",
        "component",
        "provider_id",
        "model",
        "variant",
        "model_selection",
        "client_tools",
        "definitions",
        "parameters",
        "description",
        "permissions",
        "overrides",
        "review",
        "review_model",
        "tool",
        "resource",
        "capability",
        "action",
        "conditions",
        "arguments",
        "pointer",
        "matcher",
        "kind",
        "value",
        "direction",
        "message_id",
        "expected_checkpoint_id",
        "execution_id",
        "field",
        "cursor",
        "limit",
        "max_bytes",
        "epoch",
        "turn",
        "round",
        "lineage",
        "before_message_id",
        "after_message_id",
        "before_cursor",
        "activate_if_idle",
        "url",
        "token",
        "directory",
        "allow_legacy_binding",
        "revoke",
        "relay_release",
        "browser_id",
        "workspace_id",
        "descriptor",
        "lease_generation",
        "display_watermark",
        "history_lineage",
        "child_id",
        "instruction",
        "checkpoint_id",
        "pattern",
        "reply",
        "allow",
        "remember",
    ];
    // serde_path_to_error may terminate the source chain at its path wrapper.
    // Inspect its cause using closed templates, but publish only known schema
    // fields: unknown query keys are request data and must remain private.
    if let Some((field, cause)) = message.split_once(": ") {
        let mut facts = safe_serde_reason_without_path(cause, FIELDS);
        if facts["kind"] != "schema_or_encoding_mismatch" {
            if FIELDS.contains(&field) {
                facts["field"] = json!(field);
            }
            return facts;
        }
    }
    safe_serde_reason_without_path(message, FIELDS)
}

fn safe_serde_reason_without_path(message: &str, fields: &[&str]) -> serde_json::Value {
    for (prefix, kind) in [
        ("missing field `", "missing_field"),
        ("duplicate field `", "duplicate_field"),
    ] {
        if let Some(rest) = message.strip_prefix(prefix)
            && let Some((field, suffix)) = rest.split_once('`')
            && (suffix.is_empty() || suffix.starts_with(" at line "))
            && fields.contains(&field)
        {
            return json!({"kind":kind, "field":field});
        }
    }
    let syntax = message
        .rsplit_once(" at line ")
        .map_or(message, |(text, _)| text);
    let syntax_kind = match syntax {
        "key must be a string" => Some("object_key_must_be_string"),
        "expected `:`" => Some("expected_colon"),
        "expected `,` or `}`" => Some("expected_comma_or_object_end"),
        "expected `,` or `]`" => Some("expected_comma_or_array_end"),
        "expected value" => Some("expected_value"),
        "trailing characters" => Some("trailing_characters"),
        "trailing comma" => Some("trailing_comma"),
        "invalid escape" => Some("invalid_escape"),
        "invalid unicode code point" => Some("invalid_unicode_code_point"),
        "control character (\\u0000-\\u001F) found while parsing a string" => {
            Some("unescaped_control_character")
        }
        "EOF while parsing an object" => Some("incomplete_object"),
        "EOF while parsing a list" => Some("incomplete_array"),
        "EOF while parsing a string" => Some("incomplete_string"),
        "EOF while parsing a value" => Some("missing_value"),
        _ => None,
    };
    if let Some(kind) = syntax_kind {
        return json!({"kind":kind});
    }
    let kind = if message.starts_with("unknown field `") {
        "unknown_field"
    } else if message.starts_with("unknown variant `") {
        "unknown_variant"
    } else if message.starts_with("invalid type: ") {
        "invalid_type"
    } else if message.starts_with("invalid value: ") {
        "invalid_value"
    } else if message.starts_with("invalid length ") {
        "invalid_length"
    } else if message == "invalid digit found in string" {
        "expected_integer"
    } else if message == "number too large to fit in target type" {
        "integer_out_of_range"
    } else {
        "schema_or_encoding_mismatch"
    };
    // Expected types are copied only from a closed list; enum alternatives and
    // custom expectations may contain private strings and stay withheld.
    let expected = message.rsplit_once(", expected ").and_then(|(_, value)| {
        let value = value.split(" at line ").next().unwrap_or(value);
        [
            "a string",
            "a boolean",
            "a sequence",
            "a map",
            "u64",
            "u32",
            "u16",
            "u8",
            "usize",
            "i64",
            "i32",
            "f64",
        ]
        .into_iter()
        .find(|expected| *expected == value)
    });
    json!({"kind":kind, "expected":expected})
}

impl From<WireError> for Error {
    fn from(wire: WireError) -> Self {
        let status = match wire.code.as_str() {
            "unauthenticated" => StatusCode::UNAUTHORIZED,
            "internal_error" => StatusCode::INTERNAL_SERVER_ERROR,
            "permission_denied" | "variable_forbidden" => StatusCode::FORBIDDEN,
            "not_found" | "operation_not_found" | "unavailable" | "inbox_item_not_found" => {
                StatusCode::NOT_FOUND
            }
            "request_too_large" => StatusCode::PAYLOAD_TOO_LARGE,
            "capacity" | "command_limit" | "subscription_limit" => StatusCode::TOO_MANY_REQUESTS,
            "inbox_changed"
            | "inbox_revision_conflict"
            | "inbox_item_claimed"
            | "inbox_item_not_first"
            | "inbox_continuation_pending"
            | "execution_history_changed"
            | "revision_conflict"
            | "variable_conflict"
            | "operation_conflict"
            | "busy"
            | "project_external_messaging_busy"
            | "legacy_external_dialogue_catalog"
            | "external_messaging_continuation_mismatch"
            | "closed"
            | "archived"
            | "host_detached"
            | "host_suspended"
            | "host_reconciliation_required"
            | "host_route_observation_changed"
            | "workspace_conflict"
            | "operation_interrupted"
            | "component_mismatch"
            | "binding_mismatch"
            | "legacy_binding_required"
            | "project_in_use"
            | "project_not_provisional"
            | "component_required"
            | "route_replaced" => StatusCode::CONFLICT,
            "provider_timeout" => StatusCode::GATEWAY_TIMEOUT,
            "provider_status" | "provider" | "protocol" => StatusCode::BAD_GATEWAY,
            "transport"
            | "operation_failed"
            | "workspace_unavailable"
            | "host_verification_failed"
            | "outcome_unknown"
            | "configuration_unavailable"
            | "storage_unavailable" => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::BAD_REQUEST,
        };
        Self { status, wire }
    }
}
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let code = super::provenance::ErrorCode(self.wire.code.clone());
        let mut response = (
            self.status,
            [(header::CACHE_CONTROL, "no-store")],
            Json(json!({"error":self.wire})),
        )
            .into_response();
        response.extensions_mut().insert(code);
        response
    }
}
pub(super) async fn not_found() -> Error {
    Error::new(StatusCode::NOT_FOUND, "not_found", "API route unavailable")
}
pub(super) async fn method_not_allowed() -> Error {
    Error::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "HTTP method is not supported for this route",
    )
}
pub(super) async fn no_store(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
}
