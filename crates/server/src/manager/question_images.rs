//! Pending question image authorization is independent of assistant citations.
//! The canonical invocation and exact live participant reader token own eligibility;
//! the existing immutable Session Host pin remains filesystem authority.
use super::{Manager, citations, client_tools::CallRecord, now, safe};
use crate::auth::Principal;
use microcrab_protocol::application::WireError;
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub(crate) history_lineage: String,
    pub(crate) href: String,
    pub(crate) target_session_id: String,
}
fn denied() -> WireError {
    WireError::new(
        "permission_denied",
        "No eligible image in the current pending question or executor lease expired",
    )
}
fn select(call: &CallRecord, root: &str, target: &str, href: &str) -> Result<String, WireError> {
    if call.parent_session_id != root
        || call.session_id != target
        || call.tool != "ask_user_question"
    {
        return Err(denied());
    }
    call.arguments["questions"]
        .as_array()
        .and_then(|questions| {
            questions
                .iter()
                .filter_map(|question| question["question"].as_str())
                .find_map(|markdown| citations::path::eligible_image(markdown, href))
        })
        .ok_or_else(denied)
}
impl Manager {
    pub(crate) async fn question_image(
        &self,
        principal: &Principal,
        root: &str,
        invocation: &str,
        request: Request,
        lease: String,
        cancel: CancellationToken,
    ) -> Result<citations::Content, WireError> {
        if principal.expires_at <= now() / 1000 {
            return Err(WireError::new("unauthenticated", "Authentication expired"));
        }
        if request.history_lineage.is_empty()
            || request.history_lineage.len() > 1024
            || root.is_empty()
            || root.len() > 128
            || invocation.is_empty()
            || invocation.len() > 128
            || request.target_session_id.is_empty()
            || request.target_session_id.len() > 128
            || lease.is_empty()
            || lease.len() > 128
            || citations::path::path(&request.href).is_none()
        {
            return Err(WireError::new(
                "invalid_params",
                "Invalid question image destination or identity",
            ));
        }
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| WireError::new("capacity", "Command capacity reached"))?;
        // Share the existing four-read budget with assistant citations.
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
        let root = root.to_owned();
        let invocation = invocation.to_owned();
        self.track(async move {
            let (_permit, _file_permit) = (permit, file_permit);
            let operation = cancel.child_token();
            let _cancel = operation.clone().drop_guard();
            tokio::time::timeout(
                citations::READ_TIMEOUT,
                manager.question_image_owned(
                    &owner,
                    &root,
                    &invocation,
                    &request,
                    &lease,
                    operation,
                ),
            )
            .await
            .map_err(|_| WireError::new("timed_out", "Question image read timed out"))?
        })?
        .await
        .map_err(|_| WireError::new("operation_failed", "Question image reader failed"))?
    }
    /// Inspect the selected root history and (for a direct child) canonical
    /// parent execution membership. Neither operation activates a Session.
    async fn question_image_history(
        &self,
        owner: &str,
        root: &str,
        request: &Request,
    ) -> Result<Option<String>, WireError> {
        let record = self.record(owner, root).await?;
        let _open = self.journal_open_gate(root).lock_owned().await;
        let (_, lineage, _, _) = self.inspect_message_source(&record).await?;
        if lineage != request.history_lineage {
            return Err(WireError::new(
                "stale_history",
                "Selected question history changed",
            ));
        }
        if request.target_session_id == root {
            return Ok(None);
        }
        let child = self
            .inspect_selected_child(
                owner,
                super::child_inspection::DetailRequest {
                    session_id: root.into(),
                    child_id: request.target_session_id.clone(),
                    limit: Some(1),
                    before_cursor: None,
                },
            )
            .await?;
        child["history_view"]["lineage"]
            .as_str()
            .map(|value| Some(value.to_owned()))
            .ok_or_else(|| WireError::new("stale_history", "Selected child history is unavailable"))
    }
    async fn question_image_owned(
        &self,
        owner: &str,
        root: &str,
        invocation: &str,
        request: &Request,
        lease: &str,
        cancel: CancellationToken,
    ) -> Result<citations::Content, WireError> {
        // Resolve owner before opening any browser registry or Host.
        let record = self.record(owner, root).await?;
        let bridge = self.client_tools(root).await.map_err(safe)?;
        let mut changes = bridge.invocation_changes();
        let mut membership = bridge.membership_changes();
        let call = bridge
            .leased_pending_call(invocation, lease)
            .await
            .ok_or_else(denied)?;
        let path = select(&call, root, &request.target_session_id, &request.href)?;
        let child_history = self.question_image_history(owner, root, request).await?;
        let host = self.session_host(&record).await.map_err(safe)?;
        let read = host.read_file(
            PathBuf::from(&path),
            citations::MAX_FILE_BYTES,
            cancel.clone(),
        );
        tokio::pin!(read);
        let data = loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(WireError::new("cancelled", "Question image read cancelled")),
                changed = membership.changed() => {
                    if changed.is_err() || bridge.leased_pending_call(invocation, lease).await.is_none() {
                        return Err(denied());
                    }
                },
                changed = changes.changed() => {
                    if changed.is_err() { return Err(denied()); }
                    let cancellation_requested = changes.borrow().as_ref().is_some_and(|event|
                        event.call.invocation_id == invocation && event.phase == super::client_tools::ClientToolPhase::CancelRequested);
                    if cancellation_requested || bridge.leased_pending_call(invocation, lease).await.is_none() {
                        return Err(denied());
                    }
                },
                result = &mut read => break result.map_err(citations::host_error)?,
            }
        };
        if data.len() > citations::MAX_FILE_BYTES {
            return Err(WireError::new(
                "request_too_large",
                "Question image exceeds the 8 MiB limit",
            ));
        }
        let media_type = citations::media_type(&data);
        if !media_type.starts_with("image/") {
            return Err(WireError::new(
                "unsupported_media_type",
                "Question preview requires a supported raster image within dimension limits",
            ));
        }
        if cancel.is_cancelled() {
            return Err(WireError::new("cancelled", "Question image read cancelled"));
        }
        let current_call = bridge
            .leased_pending_call(invocation, lease)
            .await
            .ok_or_else(denied)?;
        if current_call.arguments != call.arguments
            || current_call.catalog_digest != call.catalog_digest
            || select(
                &current_call,
                root,
                &request.target_session_id,
                &request.href,
            )? != path
            || self.question_image_history(owner, root, request).await? != child_history
        {
            return Err(denied());
        }
        let current = self
            .session_host(&self.record(owner, root).await?)
            .await
            .map_err(safe)?;
        if !Arc::ptr_eq(&host, &current) {
            return Err(WireError::new(
                "stale_host",
                "Pinned Host route changed during question image read",
            ));
        }
        // Recheck the live lease/call after the asynchronous owner/history/pin checks.
        bridge
            .leased_pending_call(invocation, lease)
            .await
            .ok_or_else(denied)?;
        Ok(citations::Content {
            media_type,
            filename: path.rsplit('/').next().unwrap_or("image").into(),
            data,
        })
    }
}
