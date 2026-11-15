//! Citations grant eligibility, never filesystem authority. Each click reselects
//! durable canonical assistant Markdown and resolves the exact existing Host pin.
mod path;
#[cfg(test)]
mod tests;
use super::{Manager, now, safe, unavailable};
use crate::auth::Principal;
use microcrab_protocol::application::WireError;
use serde::Deserialize;
use serde_json::Value;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

/// Existing typed ReadFile replies use the bounded Host TransferRef mechanism
/// (64 MiB, 16 KiB chunks), including remote carrier digest verification.
pub(crate) const MAX_FILE_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const READ_TIMEOUT: Duration = Duration::from_secs(15);
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub(crate) history_lineage: String,
    pub(crate) href: String,
    pub(crate) child_session_id: Option<String>,
}
pub(crate) struct Content {
    pub(crate) media_type: &'static str,
    pub(crate) filename: String,
    pub(crate) data: Vec<u8>,
}
fn denied() -> WireError {
    WireError::new(
        "permission_denied",
        "No eligible file reference in the selected assistant message",
    )
}
fn host_error(error: microcrab_host_api::ToolError) -> WireError {
    use microcrab_host_api::{ErrorCategory, ToolError};
    if matches!(error, ToolError::FileLimit | ToolError::OutputLimit) {
        return WireError::new("request_too_large", "Cited file exceeds the 8 MiB limit");
    }
    let error = error.into_host_error();
    let (code, message) = match error.category {
        ErrorCategory::NotFound => ("not_found", "Cited file does not exist"),
        ErrorCategory::Permission => (
            "permission_denied",
            "Pinned Host filesystem policy denied this file",
        ),
        ErrorCategory::Timeout => ("provider_timeout", "Pinned Host file read timed out"),
        ErrorCategory::Cancelled => ("cancelled", "Pinned Host file read was cancelled"),
        ErrorCategory::Conflict => (
            "workspace_conflict",
            "Pinned Host workspace changed during file read",
        ),
        ErrorCategory::Unsupported => (
            "unsupported",
            "Pinned Host does not support bounded file reads",
        ),
        ErrorCategory::Io => ("file_unavailable", "Pinned Host file read failed"),
    };
    WireError::new(code, message)
}
fn select(messages: &[Value], id: &str, href: &str) -> Result<String, WireError> {
    let message = messages
        .iter()
        .find(|message| message["id"] == id && message["role"] == "assistant")
        .ok_or_else(denied)?;
    path::eligible(message["text"].as_str().unwrap_or(""), href).ok_or_else(denied)
}
fn media_type(data: &[u8]) -> &'static str {
    // Byte signature plus canonical metadata admission, never file extension.
    for mime in ["image/png", "image/jpeg", "image/gif", "image/webp"] {
        if let Ok(metadata) = microcrab_session::admission::image::validate_image(mime, data)
            && metadata.width <= 4096
            && metadata.height <= 4096
            && u64::from(metadata.width) * u64::from(metadata.height) <= 16_000_000
        {
            return mime;
        }
    }
    "application/octet-stream"
}
impl Manager {
    pub(crate) async fn citation(
        &self,
        principal: &Principal,
        id: &str,
        message: &str,
        request: Request,
        cancel: CancellationToken,
    ) -> Result<Content, WireError> {
        if principal.expires_at <= now() / 1000 {
            return Err(WireError::new("unauthenticated", "Authentication expired"));
        }
        if request.history_lineage.is_empty()
            || request.history_lineage.len() > 1024
            || message.len() > 256
            || path::path(&request.href).is_none()
        {
            return Err(WireError::new(
                "invalid_params",
                "Invalid citation destination or history identity",
            ));
        }
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| WireError::new("capacity", "Command capacity reached"))?;
        let file_permit = self
            .citation_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| WireError::new("capacity", "File read capacity reached"))?;
        let manager = self
            .self_ref
            .upgrade()
            .ok_or_else(|| WireError::new("closed", "Manager unavailable"))?;
        let owner = principal.owner.clone();
        let id = id.to_owned();
        let message = message.to_owned();
        self.track(async move {
            let (_permit, _file_permit) = (permit, file_permit);
            let timeout_cancel = cancel.clone();
            let result = tokio::time::timeout(
                READ_TIMEOUT,
                manager.citation_owned(&owner, &id, &message, &request, cancel),
            )
            .await;
            match result {
                Ok(result) => result,
                Err(_) => {
                    timeout_cancel.cancel();
                    Err(WireError::new("timed_out", "File read timed out"))
                }
            }
        })?
        .await
        .map_err(|_| WireError::new("operation_failed", "File reader failed"))?
    }
    async fn citation_path(
        &self,
        owner: &str,
        id: &str,
        message: &str,
        request: &Request,
    ) -> Result<String, WireError> {
        let record = self.record(owner, id).await?;
        if let Some(child) = &request.child_session_id {
            // Reuse exact parent ownership, admitted direct-child directory and
            // selected-child history policy; never activate a Session or infer
            // arbitrary ancestor/descendant navigation authority.
            let mut before = None;
            for _ in 0..64 {
                let page = self
                    .inspect_child(
                        owner,
                        super::child_inspection::DetailRequest {
                            session_id: id.into(),
                            child_id: child.clone(),
                            limit: Some(100),
                            before_cursor: before,
                        },
                    )
                    .await?;
                if page["history_view"]["lineage"].as_str()
                    != Some(request.history_lineage.as_str())
                {
                    return Err(WireError::new(
                        "stale_history",
                        "Selected child history changed",
                    ));
                }
                let messages = page["messages"].as_array().ok_or_else(unavailable)?;
                if messages.iter().any(|item| item["id"] == message) {
                    return select(messages, message, &request.href);
                }
                before = page["next_cursor"].as_u64().map(|cursor| cursor as usize);
                if before.is_none() {
                    break;
                }
            }
            return Err(denied());
        }
        let _open = self.journal_open_gate(id).lock_owned().await;
        let (display, lineage, _, _) = self.inspect_message_source(&record).await?;
        if lineage != request.history_lineage {
            return Err(WireError::new("stale_history", "Selected history changed"));
        }
        select(&display.messages, message, &request.href)
    }
    async fn citation_owned(
        &self,
        owner: &str,
        id: &str,
        message: &str,
        request: &Request,
        cancel: CancellationToken,
    ) -> Result<Content, WireError> {
        let path = self.citation_path(owner, id, message, request).await?;
        let record = self.record(owner, id).await?;
        // NativeDelegateConfig in assembly passes this exact Host Arc to direct
        // children (creator/delegate builder uses config.host.clone()). Children
        // have no independently rebindable execution pin. inspect_child above owns
        // their existing parent/history access; do not invent ancestor authority.
        let host = self.session_host(&record).await.map_err(safe)?;
        let data = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(WireError::new("cancelled", "File read cancelled")),
            result = host.read_file(PathBuf::from(&path), MAX_FILE_BYTES, cancel.clone()) => result.map_err(host_error)?,
        };
        if data.len() > MAX_FILE_BYTES {
            return Err(WireError::new(
                "request_too_large",
                "Cited file exceeds the 8 MiB limit",
            ));
        }
        // Owner/project deletion, history travel, pin/route replacement and caller
        // cancellation all invalidate completion. Bytes are current, not snapshots.
        if cancel.is_cancelled() {
            return Err(WireError::new("cancelled", "File read cancelled"));
        }
        if self.citation_path(owner, id, message, request).await? != path {
            return Err(denied());
        }
        let current_record = self.record(owner, id).await?;
        let current = self.session_host(&current_record).await.map_err(safe)?;
        if !Arc::ptr_eq(&host, &current) {
            return Err(WireError::new(
                "stale_host",
                "Pinned Host route changed during file read",
            ));
        }
        let filename = path.rsplit('/').next().unwrap_or("download").to_owned();
        Ok(Content {
            media_type: media_type(&data),
            filename,
            data,
        })
    }
}
