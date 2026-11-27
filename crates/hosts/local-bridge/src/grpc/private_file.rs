//! Authenticated, backpressured gRPC private binary lane. Not a HostFrame method.
use super::{Sessions, same_origin};
use microcrab_host_api::{Host, OpContext, PrivateFileBuffer};
use microcrab_host_protocol::{
    BindingScope, ErrorCode, GRPC_EXECUTE_TOKEN_HEADER, GRPC_PRIVATE_FILE_HEADER,
    GRPC_PRIVATE_FILE_V1, HostCause, MAX_CHUNK_BYTES, MAX_FRAME_BYTES, MAX_PRIVATE_FILE_BUDGET_MS,
    MAX_PRIVATE_FILE_BYTES, MAX_PRIVATE_FILE_PATH_BYTES, WireError,
    pb::{
        self,
        private_file_frame::Body,
        private_files_server::{PrivateFiles, PrivateFilesServer},
    },
};
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request, Response, Status,
    metadata::{MetadataMap, MetadataValue},
};
use uuid::Uuid;

pub(super) struct Lane {
    pub(super) origin: String,
    pub(super) sessions: Sessions,
    /// Process-wide admission, including workers whose callers dropped.
    pub(super) reads: Arc<Semaphore>,
    pub(super) bytes: Arc<Semaphore>,
}
impl Lane {
    pub(super) fn service(self) -> PrivateFilesServer<Self> {
        PrivateFilesServer::new(self)
            .max_decoding_message_size(MAX_FRAME_BYTES)
            .max_encoding_message_size(MAX_FRAME_BYTES)
    }
}

pub(super) fn offered(metadata: &MetadataMap) -> bool {
    let all = metadata.get_all(GRPC_PRIVATE_FILE_HEADER);
    let mut values = all.iter();
    values
        .next()
        .is_some_and(|value| value == GRPC_PRIVATE_FILE_V1)
        && values.next().is_none()
}

pub(super) fn acknowledge(metadata: &mut MetadataMap) {
    metadata.insert(
        GRPC_PRIVATE_FILE_HEADER,
        MetadataValue::from_static(GRPC_PRIVATE_FILE_V1),
    );
}

fn status_for(error: WireError) -> Status {
    let reason = error.cause.map(HostCause::public_reason);
    if error.cause == Some(HostCause::FileByteLimitExceeded) {
        return Status::resource_exhausted("file byte limit exceeded");
    }
    match error.code {
        ErrorCode::Denied => {
            Status::permission_denied(reason.unwrap_or("private file access denied"))
        }
        ErrorCode::LeaseExpired | ErrorCode::BindingMismatch => {
            Status::failed_precondition("private file binding lease expired or changed")
        }
        ErrorCode::InvalidRequest => {
            Status::invalid_argument(reason.unwrap_or("invalid private file request"))
        }
        ErrorCode::NotFound => Status::not_found("private file was not found"),
        ErrorCode::Timeout => Status::deadline_exceeded("private file read deadline exceeded"),
        ErrorCode::Cancelled => Status::cancelled("private file read cancelled"),
        ErrorCode::Conflict => Status::failed_precondition(
            reason.unwrap_or("private file changed or is not an ordinary file"),
        ),
        ErrorCode::Unsupported => {
            Status::unimplemented("Runtime does not support confined file reads")
        }
        _ => Status::internal(reason.unwrap_or("private file read failed")),
    }
}

#[tonic::async_trait]
impl PrivateFiles for Lane {
    type ReadStream = ReceiverStream<Result<pb::PrivateFileFrame, Status>>;

    async fn read(
        &self,
        request: Request<pb::PrivateFileRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        same_origin(request.metadata(), &self.origin)?;
        let capability = request
            .metadata()
            .get(GRPC_EXECUTE_TOKEN_HEADER)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| {
                Status::unauthenticated("private file read requires authenticated Control")
            })?;
        let controller = self
            .sessions
            .lock()
            .await
            .get(capability)
            .cloned()
            .filter(|controller| !controller.stop.is_cancelled())
            .ok_or_else(|| Status::unauthenticated("private file Control is unknown or closed"))?;
        if !controller.private_files {
            return Err(Status::unimplemented(
                "confined file reads were not negotiated on Control",
            ));
        }
        let request = request.into_inner();
        let (bridge, generation) = controller.live.private_file_binding();
        if request.lease_generation != generation {
            return Err(Status::failed_precondition(
                "private file lease generation does not match Control",
            ));
        }
        let scope = request
            .scope
            .ok_or_else(|| Status::invalid_argument("private file Project binding is missing"))?;
        let scope = BindingScope::project(scope.backend_id, scope.binding_id, scope.project_id);
        let lease = {
            let authority = bridge.authority.lock().await;
            if !scope.valid() || &scope != authority.check_lease(generation).map_err(status_for)? {
                return Err(Status::permission_denied(
                    "private file scope does not match Control",
                ));
            }
            authority.private_file_stop.clone()
        };
        let operation_id = request.operation_id;
        if !Uuid::parse_str(&operation_id).is_ok_and(|id| id.to_string() == operation_id) {
            return Err(Status::invalid_argument(
                "private file operation identity is invalid",
            ));
        }
        if !(1..=MAX_PRIVATE_FILE_BUDGET_MS).contains(&request.remaining_budget_ms) {
            return Err(Status::deadline_exceeded(
                "private file time budget is out of bounds",
            ));
        }
        let max_bytes = request.max_bytes as usize;
        if !(1..=MAX_PRIVATE_FILE_BYTES).contains(&max_bytes) {
            return Err(Status::resource_exhausted(
                "private file byte budget is out of bounds",
            ));
        }
        if request.path.is_empty()
            || request.path.len() > MAX_PRIVATE_FILE_PATH_BYTES
            || request.path.contains('\0')
        {
            return Err(Status::invalid_argument("unsupported private file path"));
        }
        let path = PathBuf::from(request.path);
        if path.components().any(|component| {
            component == std::path::Component::ParentDir
                || (!path.is_absolute()
                    && matches!(
                        component,
                        std::path::Component::Prefix(_) | std::path::Component::RootDir
                    ))
        }) {
            return Err(Status::invalid_argument(
                "private file path must not contain traversal or unrooted prefixes",
            ));
        }
        let read_permit = self
            .reads
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("too many pending private file reads"))?;
        let byte_permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(request.max_bytes)
            .map_err(|_| Status::resource_exhausted("private file transfer capacity exhausted"))?;
        let buffer = PrivateFileBuffer::new(max_bytes, (read_permit, byte_permit));
        let deadline = Instant::now() + Duration::from_millis(request.remaining_budget_ms.into());
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let (bridge, generation) = controller.live.private_file_binding();
            let cancel = lease.child_token();
            let cancel_on_drop = cancel.clone().drop_guard();
            let context = OpContext {
                deadline: Some(deadline),
                cancel: cancel.clone(),
                ..OpContext::default()
            };
            let transfer = async {
                let buffer = bridge
                    .host
                    .read_private_file(path, buffer, &context)
                    .await
                    .map_err(|error| status_for(crate::execution::tool_error(error)))?;
                let data = buffer.as_bytes();
                let chunks = data.len().div_ceil(MAX_CHUNK_BYTES).max(1);
                let declaration = Body::Transfer(pb::PrivateFileTransfer {
                    operation_id: operation_id.clone(),
                    byte_length: data.len() as u32,
                    sha256: Sha256::digest(data).to_vec(),
                });
                let frames = std::iter::once(declaration).chain((0..chunks).map(|sequence| {
                    let start = sequence * MAX_CHUNK_BYTES;
                    let end = (start + MAX_CHUNK_BYTES).min(data.len());
                    Body::Chunk(pb::TransferChunk {
                        transfer_id: operation_id.clone(),
                        sequence: sequence as u32,
                        data: data[start..end].to_vec(),
                        eof: sequence + 1 == chunks,
                    })
                }));
                for body in frames {
                    let slot = tx
                        .reserve()
                        .await
                        .map_err(|_| Status::cancelled("private file consumer closed"))?;
                    // Revalidate at egress as well as at admission. One frame
                    // can already be in transport when the lease is retired.
                    let authority = bridge.authority.lock().await;
                    if &scope != authority.check_lease(generation).map_err(status_for)? {
                        return Err(Status::permission_denied("private file scope changed"));
                    }
                    context
                        .check()
                        .map_err(|error| status_for(crate::execution::host_error(error)))?;
                    slot.send(Ok(pb::PrivateFileFrame { body: Some(body) }));
                    drop(authority);
                }
                Ok(())
            };
            let result = tokio::select! {
                biased;
                _ = tx.closed() => return,
                _ = controller.stop.cancelled() => Err(Status::cancelled("private file Control retired")),
                _ = cancel.cancelled() => Err(Status::cancelled("private file lease retired")),
                _ = tokio::time::sleep_until(deadline.into()) => Err(Status::deadline_exceeded("private file read deadline exceeded")),
                result = transfer => result,
            };
            drop(cancel_on_drop);
            if let Err(status) = result {
                // Do not wait on a stalled consumer once the budget is over;
                // incomplete length/EOF is also rejected by the Server.
                let _ = tx.try_send(Err(status));
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
