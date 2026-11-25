//! Bounded outgoing result transfers; uploads have no current operation consumer.
use microcrab_host_protocol::*;
use sha2::{Digest, Sha256};

pub(crate) fn outgoing(
    bytes: Vec<u8>,
    kind: &str,
) -> Result<(serde_json::Value, Vec<WireFrame>), WireError> {
    if bytes.len() > MAX_TRANSFER_BYTES {
        return Err(WireError::new(
            ErrorCode::Limit,
            "result transfer limit exceeded",
        ));
    }
    let reference = TransferRef {
        transfer_id: uuid::Uuid::new_v4().to_string(),
        byte_length: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        kind: kind.into(),
    };
    let mut chunks: Vec<WireFrame> = bytes
        .chunks(MAX_CHUNK_BYTES)
        .enumerate()
        .map(|(sequence, data)| WireFrame::TransferChunk {
            transfer_id: reference.transfer_id.clone(),
            sequence: sequence as u32,
            data: data.to_vec(),
            eof: (sequence + 1) * MAX_CHUNK_BYTES >= bytes.len(),
        })
        .collect();
    if bytes.is_empty() {
        chunks.push(WireFrame::TransferChunk {
            transfer_id: reference.transfer_id.clone(),
            sequence: 0,
            data: vec![],
            eof: true,
        });
    }
    Ok((
        serde_json::to_value(reference)
            .map_err(|_| WireError::new(ErrorCode::Io, "cannot encode transfer reference"))?,
        chunks,
    ))
}
