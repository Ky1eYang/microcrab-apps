//! A verified Server-direct Control installs this reader on just its RemoteHost
//! route generation. The dedicated protobuf stream never enters HostFrame.
use microcrab_host_api::{ErrorCategory, HostError, PrivateFileBuffer, ToolError};
use microcrab_host_protocol::{
    GRPC_EXECUTE_TOKEN_HEADER, GRPC_PRIVATE_FILE_HEADER, GRPC_PRIVATE_FILE_V1, MAX_FRAME_BYTES,
    pb::private_files_client::PrivateFilesClient,
};
use microcrab_host_remote::{PrivateFileReader, PrivateFileTransfer};
use tonic::{
    Code, Request, Status,
    metadata::{Ascii, MetadataMap, MetadataValue},
    transport::Channel,
};

pub(super) fn offer(metadata: &mut MetadataMap) {
    metadata.insert(
        GRPC_PRIVATE_FILE_HEADER,
        MetadataValue::from_static(GRPC_PRIVATE_FILE_V1),
    );
}

pub(super) fn acknowledged(metadata: &MetadataMap) -> bool {
    let all = metadata.get_all(GRPC_PRIVATE_FILE_HEADER);
    let mut values = all.iter();
    values
        .next()
        .is_some_and(|value| value == GRPC_PRIVATE_FILE_V1)
        && values.next().is_none()
}

pub(super) struct Reader {
    client: PrivateFilesClient<Channel>,
    origin: String,
    capability: MetadataValue<Ascii>,
}
impl Reader {
    pub(super) fn new(channel: Channel, origin: &str, capability: MetadataValue<Ascii>) -> Self {
        Self {
            client: PrivateFilesClient::new(channel)
                .max_encoding_message_size(MAX_FRAME_BYTES)
                .max_decoding_message_size(MAX_FRAME_BYTES),
            origin: origin.to_owned(),
            capability,
        }
    }
}

fn operation_failure(status: Status) -> ToolError {
    let category = match status.code() {
        Code::PermissionDenied | Code::Unauthenticated => ErrorCategory::Permission,
        Code::NotFound => ErrorCategory::NotFound,
        Code::DeadlineExceeded => ErrorCategory::Timeout,
        Code::Cancelled => ErrorCategory::Cancelled,
        Code::FailedPrecondition | Code::Aborted | Code::AlreadyExists => ErrorCategory::Conflict,
        Code::Unimplemented => return ToolError::unsupported("read_private_file"),
        Code::ResourceExhausted => {
            return ToolError::OperationLimit(status.message().to_owned().into());
        }
        Code::InvalidArgument => {
            return ToolError::InvalidRequest(status.message().to_owned().into());
        }
        _ => ErrorCategory::Io,
    };
    HostError::new(category, status.message()).into()
}

#[async_trait::async_trait]
impl PrivateFileReader for Reader {
    async fn read(
        &self,
        mut transfer: PrivateFileTransfer,
    ) -> Result<PrivateFileBuffer, ToolError> {
        let mut request = Request::new(transfer.request().clone());
        request.metadata_mut().insert(
            "origin",
            MetadataValue::try_from(self.origin.as_str())
                .map_err(|_| ToolError::worker("Server origin is not valid gRPC metadata"))?,
        );
        request
            .metadata_mut()
            .insert(GRPC_EXECUTE_TOKEN_HEADER, self.capability.clone());
        let mut client = self.client.clone();
        let mut stream = client
            .read(request)
            .await
            .map_err(operation_failure)?
            .into_inner();
        // Dropping this future drops the RPC, including on cancellation, timeout,
        // invalid transfer, or route replacement. Never retry a pending read.
        while let Some(frame) = stream.message().await.map_err(operation_failure)? {
            transfer.receive(frame)?;
        }
        transfer.finish()
    }
}
