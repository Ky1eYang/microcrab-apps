use crate::{Bridge, receipts, transfers};
use microcrab_host_api::{FsApi, Host, OpContext, ProcessApi, ShellApi, ToolError};
use microcrab_host_protocol::*;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    path::Path,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
fn encoded(value: impl Serialize) -> Result<Value, WireError> {
    serde_json::to_value(value).map_err(|_| WireError::new(ErrorCode::Io, "cannot encode result"))
}
pub(crate) fn tool_error(error: ToolError) -> WireError {
    if let ToolError::Host(error) = error {
        return host_error(error);
    }
    let cause = error
        .public_reason()
        .and_then(HostCause::from_public_reason);
    let code = match error {
        ToolError::Unsupported(_) => ErrorCode::Unsupported,
        ToolError::PathOutsideRoot | ToolError::CwdOutsideRoot => ErrorCode::Denied,
        ToolError::FileLimit | ToolError::OutputLimit | ToolError::OperationLimit(_) => {
            ErrorCode::Limit
        }
        ToolError::Cancelled => ErrorCode::Cancelled,
        ToolError::Timeout | ToolError::OperationDeadline => ErrorCode::Timeout,
        ToolError::WorkspaceConflict | ToolError::FileConflict => ErrorCode::Conflict,
        ToolError::InvalidRequest(_) => ErrorCode::InvalidRequest,
        ToolError::Io(ref error) => match error.kind() {
            std::io::ErrorKind::NotFound => ErrorCode::NotFound,
            std::io::ErrorKind::PermissionDenied => ErrorCode::Denied,
            std::io::ErrorKind::TimedOut => ErrorCode::Timeout,
            _ => ErrorCode::Io,
        },
        _ => ErrorCode::Io,
    };
    let message = match &error {
        // This is the authorized operation result, not a diagnostic log. Keep
        // the original OS/custom feedback alongside machine-readable facts.
        ToolError::Io(source) => format!(
            "Host I/O failed: {source}; kind={:?}; os_error={:?}",
            source.kind(),
            source.raw_os_error()
        ),
        _ => error.to_string(),
    };
    WireError::new(code, message).with_cause(cause)
}
pub(crate) fn host_error(error: microcrab_host_api::HostError) -> WireError {
    use microcrab_host_api::ErrorCategory;
    let code = match error.category {
        ErrorCategory::NotFound => ErrorCode::NotFound,
        ErrorCategory::Permission => ErrorCode::Denied,
        ErrorCategory::Timeout => ErrorCode::Timeout,
        ErrorCategory::Cancelled => ErrorCode::Cancelled,
        ErrorCategory::Conflict => ErrorCode::Conflict,
        ErrorCategory::Unsupported => ErrorCode::Unsupported,
        ErrorCategory::Io => ErrorCode::Io,
    };
    WireError::new(code, error.message).with_cause(
        error
            .public_reason
            .as_deref()
            .and_then(HostCause::from_public_reason),
    )
}
pub(crate) fn unknown_outcome(reason: &WireError) -> WireError {
    WireError::new(
        ErrorCode::OutcomeUnknown,
        format!(
            "Host outcome needs reconciliation after {:?}: {}; effects may have occurred; do not replay automatically",
            reason.code, reason.message
        ),
    )
}
struct ActiveEffect<'a> {
    bridge: &'a Bridge,
    operation_id: &'a str,
    active: bool,
}
impl Drop for ActiveEffect<'_> {
    fn drop(&mut self) {
        if self.active {
            self.bridge
                .executing
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(self.operation_id);
        }
    }
}
impl Bridge {
    pub(crate) async fn execute(
        &self,
        operation_id: String,
        generation: String,
        budget: u32,
        method: HostRequest,
        cancel: CancellationToken,
    ) -> (HostReply, Vec<WireFrame>) {
        let deadline = Instant::now() + Duration::from_millis(u64::from(budget.min(30000)));
        let mutation = method.is_mutation();
        let consumption_key = method.consumption_key();
        let stream_read = matches!(&method, HostRequest::ProcessStreamRead { .. });
        // Only effects need serial receipt admission. Reads and selected cache
        // points must remain usable while a long shell effect is running.
        let independent_process = method.is_process_operation();
        let known_process_effect = independent_process || method.is_start();
        let _admission = if (mutation && !independent_process)
            || matches!(&method, HostRequest::SettleUnknown { .. })
        {
            let admitted = tokio::select! {
                biased;
                _ = cancel.cancelled() => return (Err(WireError::new(ErrorCode::Cancelled, "Host admission cancelled").with_cause(Some(HostCause::AdmissionCancelled))), vec![]),
                admitted = tokio::time::timeout_at(deadline.into(), self.admission.lock()) => admitted,
            };
            match admitted {
                Ok(guard) => Some(guard),
                Err(_) => {
                    return (
                        Err(
                            WireError::new(ErrorCode::Timeout, "Host admission deadline exceeded")
                                .with_cause(Some(HostCause::AdmissionTimeout)),
                        ),
                        vec![],
                    );
                }
            }
        } else {
            None
        };
        let scope = match self.check_lease(&generation).await {
            Ok(scope) => scope,
            Err(error) => return (Err(error), vec![]),
        };
        if cancel.is_cancelled() || budget == 0 || Instant::now() >= deadline {
            return (
                Err(WireError::new(
                    ErrorCode::Cancelled,
                    "Host operation cancelled before admission",
                )),
                vec![],
            );
        }
        if let Err(error) = self.policy.check(&method) {
            return (Err(error), vec![]);
        }
        let shell = matches!(
            &method,
            HostRequest::ProcessSpawn(_)
                | HostRequest::ShellLaunch(_)
                | HostRequest::ProcessWrite { .. }
                | HostRequest::ShellWrite { .. }
        );
        let digest = match payload_digest(&method) {
            Ok(digest) => digest,
            Err(error) => return (Err(error), vec![]),
        };
        if mutation {
            if operation_id.is_empty() || operation_id.len() > 256 {
                return (
                    Err(WireError::new(
                        ErrorCode::InvalidRequest,
                        "mutation needs bounded operation id",
                    )),
                    vec![],
                );
            }
            let mut records = self.receipts.lock().await;
            if let Err(error) = records.ready() {
                return (Err(error), vec![]);
            }
            if let Some(fence) = records.fence(&operation_id) {
                if fence.scope != scope || fence.payload_digest != digest {
                    return (
                        Err(WireError::new(
                            ErrorCode::OperationConflict,
                            "operation id reused with another payload or scope",
                        )),
                        vec![],
                    );
                }
                let receipt = match records.get(&operation_id) {
                    Ok(Some(receipt)) => receipt,
                    Ok(None) => {
                        return (
                            Err(WireError::new(
                                ErrorCode::Io,
                                "original receipt unavailable",
                            )),
                            vec![],
                        );
                    }
                    Err(error) => return (Err(error), vec![]),
                };
                return match receipt.state {
                    ReceiptState::Completed(result) => (result, vec![]),
                    ReceiptState::FailedUnknown { reason, .. } => (Err(reason), vec![]),
                    _ => (
                        Err(WireError::new(
                            ErrorCode::OutcomeUnknown,
                            "operation needs reconciliation; replay prohibited",
                        )),
                        vec![],
                    ),
                };
            }
            if records.fences().any(|receipt| {
                receipt.phase.unresolved()
                    && match &receipt.consumption_key {
                        Some(key) => consumption_key.as_ref() == Some(key),
                        None => !self
                            .executing
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .get(&receipt.operation_id)
                            .is_some_and(|raw_read| known_process_effect || *raw_read),
                    }
            }) {
                return (
                    Err(WireError::new(
                        ErrorCode::OutcomeUnknown,
                        "unresolved durable mutation fences new effects",
                    )
                    .with_cause(Some(HostCause::UnreconciledMutation))),
                    vec![],
                );
            }
            if records.len() >= receipts::MAX_RECEIPTS {
                return (
                    Err(
                        WireError::new(ErrorCode::Limit, "durable receipt capacity reached")
                            .with_cause(Some(HostCause::ReceiptCapacity)),
                    ),
                    vec![],
                );
            }
            let receipt = OperationReceipt {
                operation_id: operation_id.clone(),
                payload_digest: digest.clone(),
                scope: scope.clone(),
                state: ReceiptState::Intent,
            };
            if let Err(error) = records.intent_scoped(&receipt, consumption_key.clone()) {
                return (Err(error), vec![]);
            }
            self.executing
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(operation_id.clone(), stream_read);
        }
        let _effect = ActiveEffect {
            bridge: self,
            operation_id: &operation_id,
            active: mutation,
        };
        let ctx = OpContext {
            deadline: if stream_read { None } else { Some(deadline) },
            cancel: cancel.clone(),
            ..OpContext::default()
        };
        // Cancellation is signalled without dropping publication futures. LocalHost owns commit/compensation.
        let timer_cancel = cancel.clone();
        // A quiet raw pipe keeps exactly one pending bounded read. Its owner
        // cancels it on route loss; an RPC timer would turn silence into failure.
        let timer = (!stream_read).then(|| {
            tokio::spawn(async move {
                tokio::time::sleep_until(deadline.into()).await;
                timer_cancel.cancel();
            })
        });
        let result = self.dispatch(method, &ctx, &scope).await;
        if let Some(timer) = timer {
            timer.abort();
        }
        if mutation {
            let mut records = self.receipts.lock().await;
            let mut receipt = OperationReceipt {
                operation_id: operation_id.clone(),
                payload_digest: digest,
                scope,
                state: ReceiptState::Intent,
            };
            let unknown = result.as_ref().is_err_and(|error| {
                error.code == ErrorCode::Io
                    || error.code == ErrorCode::OutcomeUnknown
                    || (shell && matches!(error.code, ErrorCode::Cancelled | ErrorCode::Timeout))
            });
            if unknown {
                let reason = result.as_ref().err().cloned().unwrap_or_else(|| {
                    WireError::new(
                        ErrorCode::OutcomeUnknown,
                        "indeterminate: owner did not publish an outcome",
                    )
                });
                let feedback = unknown_outcome(&reason);
                if records.save_unknown(&receipt, reason).is_err() {
                    return (
                        Err(WireError::new(
                            ErrorCode::OutcomeUnknown,
                            format!(
                                "cannot persist original unknown reason; {}",
                                feedback.message
                            ),
                        )),
                        vec![],
                    );
                }
                return (Err(feedback), vec![]);
            }
            receipt.state = ReceiptState::Completed(
                result
                    .as_ref()
                    .map(|(value, _)| value.clone())
                    .map_err(Clone::clone),
            );
            if records.complete(&receipt).is_err() {
                return (
                    Err(WireError::new(
                        ErrorCode::OutcomeUnknown,
                        "effect finished but durable completion failed",
                    )),
                    vec![],
                );
            }
        }
        match result {
            Ok((value, chunks)) => (Ok(value), chunks),
            Err(error) => (Err(error), vec![]),
        }
    }
    async fn dispatch(
        &self,
        method: HostRequest,
        ctx: &OpContext,
        scope: &BindingScope,
    ) -> Result<(Value, Vec<WireFrame>), WireError> {
        let token = ctx.cancel.clone();
        let value = match method {
            HostRequest::CachePoint(request) => encoded(
                Host::cache_point(&self.host, request, token)
                    .await
                    .map_err(tool_error)?,
            )?,
            HostRequest::Read(request) => encoded(
                Host::read_text(&self.host, request, token)
                    .await
                    .map_err(tool_error)?,
            )?,
            HostRequest::Write(request) => encoded(
                Host::write(&self.host, request, token)
                    .await
                    .map_err(tool_error)?,
            )?,
            HostRequest::PreflightMutations { requests } => encoded(
                Host::preflight_mutations(&self.host, requests, token)
                    .await
                    .map_err(tool_error)?,
            )?,
            HostRequest::Mutate(request) => encoded(
                Host::mutate(&self.host, request, token)
                    .await
                    .map_err(tool_error)?,
            )?,
            HostRequest::Glob(request) => encoded(
                Host::glob(&self.host, request, token)
                    .await
                    .map_err(tool_error)?,
            )?,
            HostRequest::Grep(request) => encoded(
                Host::grep(&self.host, request, token)
                    .await
                    .map_err(tool_error)?,
            )?,
            HostRequest::Normalize { path } => encoded(
                Host::normalize_resource(&self.host, path.into(), token)
                    .await
                    .map_err(tool_error)?,
            )?,
            HostRequest::ReadFile { path, max_bytes } => {
                return transfers::outgoing(
                    Host::read_file(
                        &self.host,
                        path.into(),
                        (max_bytes as usize).min(MAX_TRANSFER_BYTES),
                        token,
                    )
                    .await
                    .map_err(tool_error)?,
                    "bytes",
                );
            }
            method @ (HostRequest::ProcessSpawn(_) | HostRequest::ShellLaunch(_)) => {
                encoded(self.start_owned_process(method, scope, ctx).await?)?
            }
            HostRequest::ProcessInventory => encoded(self.process_inventory(scope).await)?,
            HostRequest::ProcessStatus { handle } => {
                let owned = self.owned_process(handle, scope).await?;
                encoded(
                    self.host
                        .status(ctx, owned.handle)
                        .await
                        .map_err(host_error)?,
                )?
            }
            HostRequest::ProcessPid { handle } => {
                let owned = self.owned_process(handle, scope).await?;
                encoded(self.host.pid(ctx, owned.handle).await.map_err(host_error)?)?
            }
            HostRequest::ProcessWait { handle } => {
                let owned = self.owned_process(handle, scope).await?;
                encoded(
                    self.host
                        .wait(ctx, owned.handle)
                        .await
                        .map_err(host_error)?,
                )?
            }
            HostRequest::ProcessSignal { handle, signal } => {
                let owned = self.owned_process(handle, scope).await?;
                self.host
                    .signal(ctx, owned.handle, signal)
                    .await
                    .map_err(host_error)?;
                Value::Null
            }
            HostRequest::ProcessKill { handle, force } => {
                let owned = self.owned_process(handle, scope).await?;
                self.host
                    .kill_tree(ctx, owned.handle, force)
                    .await
                    .map_err(host_error)?;
                Value::Null
            }
            HostRequest::ProcessRelease { handle } => {
                self.release_owned_process(handle, scope).await?;
                Value::Null
            }
            HostRequest::ProcessWrite { handle, data } => {
                if data.len() > MAX_CHUNK_BYTES {
                    return Err(WireError::new(
                        ErrorCode::Limit,
                        "process stdin chunk too large",
                    ));
                }
                let owned = self.owned_process(handle, scope).await?;
                self.host
                    .write_stdin(ctx, owned.handle, data.into())
                    .await
                    .map_err(host_error)?;
                Value::Null
            }
            HostRequest::ProcessCloseStdin { handle } => {
                let owned = self.owned_process(handle, scope).await?;
                self.host
                    .close_stdin(ctx, owned.handle)
                    .await
                    .map_err(host_error)?;
                Value::Null
            }
            HostRequest::ProcessResize { handle, rows, cols } => {
                let owned = self.owned_process(handle, scope).await?;
                self.host
                    .resize_pty(ctx, owned.handle, rows, cols)
                    .await
                    .map_err(host_error)?;
                Value::Null
            }
            HostRequest::ProcessStreamOpen { handle, stderr } => {
                self.open_process_stream(handle, scope, stderr).await?;
                Value::Null
            }
            HostRequest::ProcessStreamRead {
                handle,
                stderr,
                max_bytes,
            } => encoded(
                self.read_process_stream(handle, scope, stderr, max_bytes, ctx)
                    .await?,
            )?,
            HostRequest::ShellCaptureStatus { handle } => {
                let owned = self.owned_process(handle, scope).await?;
                encoded(
                    self.host
                        .capture_status(ctx, owned.handle)
                        .await
                        .map_err(host_error)?,
                )?
            }
            HostRequest::ShellRead { handle, from } => {
                let owned = self.owned_process(handle, scope).await?;
                encoded(
                    ShellApi::read(&self.host, ctx, owned.handle, from)
                        .await
                        .map_err(host_error)?,
                )?
            }
            HostRequest::ShellDrain { handle, max_bytes } => {
                if max_bytes as usize > MAX_TRANSFER_BYTES {
                    return Err(WireError::new(
                        ErrorCode::Limit,
                        "shell drain byte budget exceeds relay limit",
                    ));
                }
                let owned = self.owned_process(handle, scope).await?;
                encoded(
                    self.host
                        .drain(ctx, owned.handle, max_bytes as usize)
                        .await
                        .map_err(host_error)?,
                )?
            }
            HostRequest::ShellWrite { handle, data, eof } => {
                if data.len() > MAX_CHUNK_BYTES {
                    return Err(WireError::new(
                        ErrorCode::Limit,
                        "shell stdin chunk too large",
                    ));
                }
                let owned = self.owned_process(handle, scope).await?;
                ShellApi::write(&self.host, ctx, owned.handle, data, eof)
                    .await
                    .map_err(host_error)?;
                Value::Null
            }
            HostRequest::Stat { path } => {
                let meta = FsApi::stat(&self.host, ctx, Path::new(&path))
                    .await
                    .map_err(host_error)?;
                json!({"kind":match meta.kind { microcrab_host_api::filesystem::FileKind::File=>"file",microcrab_host_api::filesystem::FileKind::Directory=>"directory",microcrab_host_api::filesystem::FileKind::Symlink=>"symlink",_=>"other"},"size":meta.size.to_string()})
            }
            HostRequest::SettleUnknown {
                operation_id,
                payload_digest,
                scope: original_scope,
            } => {
                // execute holds admission until dispatch and receipt publication finish.
                // Old controllers cannot admit another effect after lease replacement.
                // Waiting for this fence joins the original owner, including cancellation.
                if original_scope != *scope || operation_id.is_empty() || operation_id.len() > 256 {
                    return Err(WireError::new(
                        ErrorCode::OperationConflict,
                        "settlement scope mismatch",
                    ));
                }
                if self
                    .executing
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .contains_key(&operation_id)
                {
                    return Err(WireError::new(
                        ErrorCode::OutcomeUnknown,
                        "original job operation is still owned",
                    ));
                }
                let mut records = self.receipts.lock().await;
                if payload_digest.len() != 64
                    || !payload_digest.bytes().all(|b| b.is_ascii_hexdigit())
                {
                    return Err(WireError::new(
                        ErrorCode::InvalidRequest,
                        "invalid original payload digest",
                    ));
                }
                records.ready()?;
                let missing = records.fence(&operation_id).is_none();
                if missing {
                    let intent = OperationReceipt {
                        operation_id: operation_id.clone(),
                        payload_digest: payload_digest.clone(),
                        scope: original_scope.clone(),
                        state: ReceiptState::Intent,
                    };
                    // Authenticated server owns the immutable original payload. A missing
                    // local intent is recorded only as failed, never executed or successful.
                    records.intent(&intent)?;
                }
                let mut receipt = records
                    .get(&operation_id)?
                    .ok_or_else(|| WireError::new(ErrorCode::Io, "original receipt unavailable"))?;
                if receipt.scope != original_scope || receipt.payload_digest != payload_digest {
                    return Err(WireError::new(
                        ErrorCode::OperationConflict,
                        "settlement payload or scope mismatch",
                    ));
                }
                if matches!(
                    receipt.state,
                    ReceiptState::Intent | ReceiptState::Indeterminate
                ) {
                    let fallback = if missing {
                        "receipt_missing: original durable receipt unavailable; effects may have occurred"
                    } else {
                        "indeterminate: original operation has no durable outcome; effects may have occurred"
                    };
                    let reason = records
                        .unknown_reason(&receipt)?
                        .unwrap_or_else(|| WireError::new(ErrorCode::OutcomeUnknown, fallback));
                    records.save_unknown(&receipt, reason.clone())?;
                    receipt.state = ReceiptState::FailedUnknown {
                        reason,
                        effects_may_have_occurred: true,
                    };
                    records.settle(&receipt)?;
                }
                encoded(receipt)?
            }
            HostRequest::Recovery => {
                let mut records = self.receipts.lock().await;
                records.ready()?;
                let ids = records
                    .fences()
                    .filter(|fence| &fence.scope == scope && fence.phase != receipts::Phase::Done)
                    .map(|fence| fence.operation_id.clone())
                    .collect::<Vec<_>>();
                let mut recovered = Vec::with_capacity(ids.len());
                for id in ids {
                    recovered.push(records.get(&id)?.ok_or_else(|| {
                        WireError::new(ErrorCode::Io, "recovery receipt unavailable")
                    })?);
                }
                encoded(recovered)?
            }
            HostRequest::Unsupported { .. } => {
                return Err(WireError::new(
                    ErrorCode::Unsupported,
                    "requested owner is not implemented by filesystem bridge",
                ));
            }
        };
        let bytes = serde_json::to_vec(&value)
            .map_err(|_| WireError::new(ErrorCode::Io, "cannot encode result"))?;
        if bytes.len() > MAX_FRAME_BYTES / 2 {
            transfers::outgoing(bytes, "json")
        } else {
            Ok((value, vec![]))
        }
    }
}

#[cfg(test)]
mod failure_cause_tests {
    use super::*;
    #[test]
    fn bridge_preserves_closed_facts_separately_from_private_feedback() {
        for error in [
            ToolError::PathOutsideRoot,
            ToolError::CwdOutsideRoot,
            ToolError::FileLimit,
            ToolError::OutputLimit,
            ToolError::Timeout,
            ToolError::OperationDeadline,
            ToolError::WorkspaceConflict,
            ToolError::FileConflict,
            ToolError::invalid_request("path must be Unicode"),
            ToolError::unsupported("read"),
        ] {
            let expected = error.public_reason().map(str::to_owned);
            let wire = tool_error(error);
            let wire: WireError =
                serde_json::from_value(serde_json::to_value(wire).unwrap()).unwrap();
            assert_eq!(
                wire.cause.map(HostCause::public_reason),
                expected.as_deref()
            );
        }
        let wire = tool_error(ToolError::InvalidRequest("private-host-secret".into()));
        assert_eq!(wire.cause, None);
        assert!(wire.message.contains("private-host-secret"));
        let wire = host_error(
            microcrab_host_api::HostError::new(
                microcrab_host_api::ErrorCategory::Io,
                "private-host-secret",
            )
            .with_public_reason("filesystem worker failed"),
        );
        assert_eq!(
            wire.cause.unwrap().public_reason(),
            "filesystem worker failed"
        );
    }
}
