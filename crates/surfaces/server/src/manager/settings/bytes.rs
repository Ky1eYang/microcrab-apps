//! Private image ingress. State owns the only staged allocation and the CAS;
//! Manager owns request capacity and the authenticated Session lifecycle fence.
use super::{active, session_record, variables};
use crate::{
    auth::{Auth, Principal},
    manager::{Managed, Manager, safe, unavailable},
};
use microcrab_protocol::application::WireError;
use microcrab_session::{
    BytesWriteReceipt, Error, LimitKind, PreparedBytesWrite,
    state::{VariableCommit, VariableWitness},
};
use std::sync::{Arc, atomic::Ordering};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;

pub(crate) struct BytesUpload {
    prepared: PreparedBytesWrite,
    permit: OwnedSemaphorePermit,
    pub(crate) closed: CancellationToken,
}

impl BytesUpload {
    pub(crate) fn append(&mut self, chunk: &[u8]) -> Result<(), WireError> {
        self.prepared.append(chunk).map_err(bytes_error)
    }
}

fn check_active(auth: &Auth, principal: &Principal) -> Result<(), WireError> {
    auth.check_active(principal)
        .map_err(|_| WireError::new("unauthenticated", "Authentication is no longer active"))
}

fn bytes_error(error: Error) -> WireError {
    // Do not propagate diagnostic strings from raw input or authorization checks.
    match error {
        Error::Config(_) => WireError::new(
            "invalid_image_variable",
            "Invalid image data or byte variable",
        ),
        Error::Limit(limit) if limit.kind == LimitKind::AttachmentBytes => WireError::new(
            "request_too_large",
            "Upload exceeds the Session image byte limit",
        ),
        Error::Limit(_) => WireError::new("capacity", "Session byte variable capacity exceeded"),
        Error::VariableForbidden(_) => {
            WireError::new("variable_forbidden", "Byte variable edit is not allowed")
        }
        _ => safe(error),
    }
}

impl Manager {
    async fn bytes_session(&self, owner: &str, id: &str) -> Result<Arc<Managed>, WireError> {
        if !crate::config::valid_id(id) {
            return Err(unavailable());
        }
        let record = session_record(self, owner, id).await?;
        if !self.record_allowed(&record) || !self.project_visible(&record).await {
            return Err(unavailable());
        }
        if record.archived {
            return Err(WireError::new("archived", "Session is archived"));
        }
        // Ingress never opens a runtime or requires a new Host/Agent route.
        active(self, id).await.ok_or_else(|| {
            WireError::new(
                "closed",
                "Open the Session explicitly before changing variables",
            )
        })
    }

    fn bytes_permit(&self) -> Result<OwnedSemaphorePermit, WireError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(WireError::new("closed", "Manager unavailable"));
        }
        // Slow uploads must not occupy the ordinary command lane (including
        // the client's ability to close the Session to cancel the upload).
        self.bytes_ingress_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| WireError::new("capacity", "Byte variable request capacity reached"))
    }

    pub(crate) async fn prepare_bytes_upload(
        &self,
        principal: &Principal,
        id: &str,
        name: &str,
        media_type: &str,
        length: u64,
        expected: VariableWitness,
    ) -> Result<BytesUpload, WireError> {
        let permit = self.bytes_permit()?;
        let managed = self.bytes_session(&principal.owner, id).await?;
        let _commands = managed.commands.lock().await;
        variables::mutable(self, &managed, id)?;
        let prepared = managed
            .session
            .state_handle()
            .prepare_bytes_write(name, media_type, length, expected)
            .map_err(bytes_error)?;
        Ok(BytesUpload {
            prepared,
            permit,
            closed: managed.close_token.clone(),
        })
    }

    async fn mutate_bytes<T: Send + 'static, F>(
        self: &Arc<Self>,
        auth: &Auth,
        principal: &Principal,
        id: &str,
        permit: OwnedSemaphorePermit,
        mutate: impl FnOnce(microcrab_session::State) -> F + Send + 'static,
    ) -> Result<T, WireError>
    where
        F: Future<Output = Result<T, Error>> + Send,
    {
        check_active(auth, principal)?;
        // Do not allocate a lifecycle gate for an unowned or inactive id. Check
        // again under that gate so archive/delete cannot race the mutation.
        self.bytes_session(&principal.owner, id).await?;
        let manager = self.clone();
        let auth = auth.clone();
        let principal = principal.clone();
        let id = id.to_owned();
        self.track(async move {
            let _permit = permit;
            let _lifecycle = manager.journal_open_gate(&id).lock_owned().await;
            let managed = manager.bytes_session(&principal.owner, &id).await?;
            let _commands = managed.commands.lock().await;
            variables::mutable(&manager, &managed, &id)?;
            check_active(&auth, &principal)?;
            // Core's fenced result is definitive. Losing this worker's reply is
            // not: neither Manager nor the HTTP contract replays it.
            mutate(managed.session.state_handle()).await.map_err(bytes_error)
        })?
        .await
        .map_err(|_| WireError::new("outcome_unknown", "Byte variable mutation outcome is unknown; inspect current State and do not replay"))?
    }

    pub(crate) async fn commit_bytes_upload(
        self: &Arc<Self>,
        auth: &Auth,
        principal: &Principal,
        id: &str,
        upload: BytesUpload,
    ) -> Result<BytesWriteReceipt, WireError> {
        let BytesUpload {
            prepared, permit, ..
        } = upload;
        self.mutate_bytes(auth, principal, id, permit, async move |state| {
            state.commit_bytes(prepared).await
        })
        .await
    }

    pub(crate) async fn delete_bytes_variable(
        self: &Arc<Self>,
        auth: &Auth,
        principal: &Principal,
        id: &str,
        name: String,
        expected: VariableWitness,
    ) -> Result<VariableCommit, WireError> {
        let permit = self.bytes_permit()?;
        self.mutate_bytes(auth, principal, id, permit, async move |state| {
            state.remove_bytes_checked(&name, expected).await
        })
        .await
    }
}
