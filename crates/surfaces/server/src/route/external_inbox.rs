//! External inbox has its own bearer capability. Browser cookies never authorize this route.
use super::{error::Error, external_speakers};
use crate::{Application, manager::external_inbox::ExternalInboxReceipt};
use axum::{
    Json,
    body::to_bytes,
    extract::{Path, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use microcrab_session::state::communication::inbox::{
    DeliveryStatus, MAX_BODY_BYTES, QueuedReceipt,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

mod origin;
pub(super) mod reply;
pub(super) use origin::{cors, inbox_preflight, observe_preflight};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    body: String,
}

/// HTTP-specific metadata: do not serialize Core's durable receipt or sender proof.
#[derive(Serialize)]
struct Receipt {
    operation_id: String,
    ordinal: u64,
    status: DeliveryStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    dialogue_id: Option<String>,
}
impl From<ExternalInboxReceipt> for Receipt {
    fn from(value: ExternalInboxReceipt) -> Self {
        let (receipt, dialogue_id) = match value {
            ExternalInboxReceipt::Legacy(receipt) => (receipt, None),
            ExternalInboxReceipt::Dialogue(dialogue) => {
                (dialogue.receipt, Some(dialogue.dialogue_id))
            }
        };
        let QueuedReceipt {
            operation_id,
            ordinal,
            status,
        } = receipt;
        Self {
            operation_id,
            ordinal,
            status,
            dialogue_id,
        }
    }
}

fn unauthorized() -> Error {
    Error::new(
        StatusCode::UNAUTHORIZED,
        "unauthenticated",
        "External speaker key is not bound to the target Session",
    )
}
fn single<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, Error> {
    let mut values = headers.get_all(name).iter();
    let value = values
        .next()
        .and_then(|value| value.to_str().ok())
        .ok_or_else(unauthorized)?;
    if values.next().is_some() {
        return Err(unauthorized());
    }
    Ok(value)
}
fn bearer(headers: &HeaderMap) -> Result<String, Error> {
    let header = single(headers, "authorization")?;
    let (scheme, key) = header.split_once(' ').ok_or_else(unauthorized)?;
    if !scheme.eq_ignore_ascii_case("Bearer")
        || !(43..=128).contains(&key.len())
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(unauthorized());
    }
    Ok(key.into())
}
fn operation(value: &str) -> Result<String, Error> {
    if !uuid::Uuid::parse_str(value).is_ok_and(|id| id.to_string() == value) {
        return Err(Error::invalid(
            "Idempotency key must be a canonical lowercase hyphenated UUID",
        ));
    }
    Ok(value.into())
}
pub(super) async fn enqueue(
    State(state): State<Arc<Application>>,
    Path(id): Path<String>,
    request: Request,
) -> Response {
    async {
        let key = bearer(request.headers())?;
        let operation = operation(
            single(request.headers(), "idempotency-key")
                .map_err(|_| Error::invalid("Exactly one Idempotency-Key is required"))?,
        )?;
        let mime = single(request.headers(), "content-type").map_err(|_| {
            Error::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "Expected application/json",
            )
        })?;
        if !mime
            .split(';')
            .next()
            .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
        {
            return Err(Error::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "Expected application/json",
            ));
        }
        // Explicit bearer authorization supports non-browser senders. If an Origin is supplied,
        // it still follows the application's existing exact browser-origin policy.
        origin::check(&state, request.headers())?;
        let bytes = to_bytes(request.into_body(), MAX_BODY_BYTES * 6 + 1024)
            .await
            .map_err(|_| {
                Error::new(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request_too_large",
                    "External body exceeds budget",
                )
            })?;
        let value = external_speakers::parse(&bytes).map_err(Error::json)?;
        let input: Input = serde_json::from_value(value).map_err(Error::json)?;
        if input.body.trim().is_empty() || input.body.len() > MAX_BODY_BYTES {
            return Err(Error::invalid(
                "External body requires 1..8192 UTF-8 bytes and non-whitespace text",
            ));
        }
        let receipt = state
            .manager
            .external_inbox(id, key, operation, Some(input.body))
            .await
            .map_err(Error::from)?;
        let receipt = Receipt::from(receipt);
        let status = if receipt.status == DeliveryStatus::Queued {
            StatusCode::ACCEPTED
        } else {
            StatusCode::OK
        };
        Ok((status, Json(receipt)).into_response())
    }
    .await
    .unwrap_or_else(IntoResponse::into_response)
}
pub(super) async fn observe(
    State(state): State<Arc<Application>>,
    Path((id, operation_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    async {
        let key = bearer(&headers)?;
        let operation = operation(&operation_id)?;
        origin::check(&state, &headers)?;
        let receipt = state
            .manager
            .external_inbox(id, key, operation, None)
            .await
            .map_err(Error::from)?;
        Ok::<Response, Error>(Json(Receipt::from(receipt)).into_response())
    }
    .await
    .unwrap_or_else(IntoResponse::into_response)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_receipt_preserves_wire_shape() {
        let receipt = Receipt::from(ExternalInboxReceipt::Legacy(QueuedReceipt {
            operation_id: "legacy".into(),
            ordinal: 1,
            status: DeliveryStatus::Consumed,
        }));
        assert_eq!(
            serde_json::to_value(receipt).unwrap(),
            serde_json::json!({
                "operation_id":"legacy", "ordinal":1, "status":"consumed"
            })
        );
    }

    #[test]
    fn strict_bearer_has_no_cookie_fallback_and_rejects_ambiguous_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("cookie", "microcrab_session=browser".parse().unwrap());
        assert!(bearer(&headers).is_err());
        headers.insert(
            "authorization",
            format!("Bearer {}", "a".repeat(43)).parse().unwrap(),
        );
        assert!(bearer(&headers).is_ok());
        headers.append(
            "authorization",
            format!("Bearer {}", "b".repeat(43)).parse().unwrap(),
        );
        assert!(bearer(&headers).is_err());
    }
}
