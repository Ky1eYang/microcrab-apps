//! Bounded binary framing and atomic projection publication. These functions run
//! under the receipt owner; no second writer or independently cached authority.
use super::{MAX_BODY_BYTES, Record, StorageResult};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, Metadata, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

pub(super) const HEADER_BYTES: u64 = 88;
pub(super) const FRAME_BYTES: u64 = 88;
const LOG_MAGIC: &[u8; 8] = b"MCRLOG01";
const FRAME_MAGIC: &[u8; 8] = b"MCRREC01";
const MAX_INDEX_BYTES: u64 = 16 * 1024 * 1024;

pub(super) fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(super) fn ordinary_file(path: &Path) -> StorageResult<Option<Metadata>> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if !crate::linked(&metadata) && metadata.is_file() => Ok(Some(metadata)),
        Ok(_) => Err("receipt storage must be an ordinary file".into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}
pub(super) fn ensure_dir(path: &Path) -> StorageResult<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if !crate::linked(&metadata) && metadata.is_dir() => Ok(()),
        Ok(_) => Err("receipt directory must be an ordinary directory".into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(path)?;
            sync_parent(path)?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}
pub(super) fn sync_parent(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    File::open(
        path.parent()
            .ok_or_else(|| std::io::Error::other("record parent missing"))?,
    )?
    .sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
pub(super) fn temporary_path(dir: &Path, purpose: &str) -> PathBuf {
    dir.join(format!(".{purpose}-{}.tmp", Uuid::new_v4()))
}

pub(super) struct Header {
    pub(super) log_id: [u8; 16],
    workspace_hash: [u8; 32],
}
impl Header {
    pub(super) fn new(workspace_id: &str) -> Self {
        Self {
            log_id: *Uuid::new_v4().as_bytes(),
            workspace_hash: hash(workspace_id.as_bytes()),
        }
    }
}
pub(super) fn write_header(file: &mut File, header: &Header) -> StorageResult<()> {
    let mut bytes = Vec::with_capacity(HEADER_BYTES as usize);
    bytes.extend_from_slice(LOG_MAGIC);
    bytes.extend_from_slice(&header.log_id);
    bytes.extend_from_slice(&header.workspace_hash);
    bytes.extend_from_slice(&hash(&bytes));
    file.write_all(&bytes)?;
    Ok(())
}
pub(super) fn read_header(file: &mut File, workspace_id: &str) -> StorageResult<Header> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = [0; HEADER_BYTES as usize];
    file.read_exact(&mut bytes)?;
    if &bytes[..8] != LOG_MAGIC
        || bytes[24..56] != hash(workspace_id.as_bytes())
        || bytes[56..88] != hash(&bytes[..56])
    {
        return Err("invalid receipt log identity or header".into());
    }
    Ok(Header {
        log_id: bytes[8..24].try_into()?,
        workspace_hash: bytes[24..56].try_into()?,
    })
}

#[derive(Clone, Copy, Debug, serde::Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct BodyRef {
    pub(super) offset: u64,
    pub(super) length: u64,
    pub(super) sequence: u64,
    pub(super) checksum: [u8; 32],
}
impl BodyRef {
    pub(super) fn end(self) -> StorageResult<u64> {
        self.offset
            .checked_add(FRAME_BYTES)
            .and_then(|offset| offset.checked_add(self.length))
            .ok_or_else(|| "receipt frame length overflow".into())
    }
}
pub(super) fn encode_frame(
    record: &Record,
    offset: u64,
    sequence: u64,
) -> StorageResult<(Vec<u8>, BodyRef)> {
    let body = serde_json::to_vec(record)?;
    if body.is_empty() || body.len() > MAX_BODY_BYTES {
        return Err("receipt body exceeds durable limit".into());
    }
    let reference = BodyRef {
        offset,
        length: body.len() as u64,
        sequence,
        checksum: hash(&body),
    };
    reference.end()?;
    let mut bytes = Vec::with_capacity(FRAME_BYTES as usize + body.len());
    bytes.extend_from_slice(FRAME_MAGIC);
    bytes.extend_from_slice(&sequence.to_le_bytes());
    bytes.extend_from_slice(&reference.length.to_le_bytes());
    bytes.extend_from_slice(&reference.checksum);
    bytes.extend_from_slice(&hash(&bytes));
    bytes.extend_from_slice(&body);
    Ok((bytes, reference))
}
pub(super) enum ReadFrame {
    Complete(Box<Record>, BodyRef),
    Incomplete { header: Option<BodyRef> },
}
pub(super) fn read_frame(file: &mut File, offset: u64, size: u64) -> StorageResult<ReadFrame> {
    let remaining = size
        .checked_sub(offset)
        .ok_or("receipt offset exceeds file")?;
    if remaining < FRAME_BYTES {
        return Ok(ReadFrame::Incomplete { header: None });
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut header = [0; FRAME_BYTES as usize];
    file.read_exact(&mut header)?;
    if &header[..8] != FRAME_MAGIC || header[56..88] != hash(&header[..56]) {
        return Err("invalid complete receipt frame header".into());
    }
    let reference = BodyRef {
        offset,
        sequence: u64::from_le_bytes(header[8..16].try_into()?),
        length: u64::from_le_bytes(header[16..24].try_into()?),
        checksum: header[24..56].try_into()?,
    };
    if reference.sequence == 0 || reference.length == 0 || reference.length > MAX_BODY_BYTES as u64
    {
        return Err("invalid complete receipt frame length or sequence".into());
    }
    if reference.end()? > size {
        return Ok(ReadFrame::Incomplete {
            header: Some(reference),
        });
    }
    let mut body = vec![0; reference.length as usize];
    file.read_exact(&mut body)?;
    if hash(&body) != reference.checksum {
        return Err("invalid complete receipt frame checksum".into());
    }
    let record: Record = serde_json::from_slice(&body)?;
    record.validate()?;
    Ok(ReadFrame::Complete(Box::new(record), reference))
}
pub(super) fn read_reference(
    file: &mut File,
    expected: BodyRef,
    watermark: u64,
) -> StorageResult<Record> {
    if expected.offset < HEADER_BYTES || expected.end()? > watermark {
        return Err("receipt reference exceeds checked watermark".into());
    }
    match read_frame(file, expected.offset, watermark)? {
        ReadFrame::Complete(record, actual) if actual == expected => Ok(*record),
        _ => Err("receipt frame does not match checked reference".into()),
    }
}
pub(super) fn append_frame(file: &mut File, offset: u64, bytes: &[u8]) -> StorageResult<()> {
    if file.metadata()?.len() != offset {
        return Err("receipt log changed outside its owner".into());
    }
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

/// A malformed snapshot is never authority. Its log is rebuilt instead. I/O and
/// unsafe path failures propagate rather than masquerading as a missing index.
pub(super) fn read_index<T: DeserializeOwned>(path: &Path) -> StorageResult<Option<T>> {
    let Some(metadata) = ordinary_file(path)? else {
        return Ok(None);
    };
    if metadata.len() < 32 || metadata.len() > MAX_INDEX_BYTES {
        return Ok(None);
    }
    let bytes = std::fs::read(path)?;
    if bytes[..32] != hash(&bytes[32..]) {
        return Ok(None);
    }
    Ok(serde_json::from_slice(&bytes[32..]).ok())
}
pub(super) fn publish_index(dir: &Path, index: &impl Serialize) -> StorageResult<()> {
    let body = serde_json::to_vec(index)?;
    if body.len() as u64 + 32 > MAX_INDEX_BYTES {
        return Err("receipt index exceeds durable limit".into());
    }
    let path = temporary_path(dir, "index");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    file.write_all(&hash(&body))?;
    file.write_all(&body)?;
    file.sync_all()?;
    drop(file);
    let destination = dir.join("fence.index");
    ordinary_file(&destination)?;
    // std::fs::rename replaces an existing ordinary file atomically on supported
    // local filesystems. Never delete the old checkpoint before replacement.
    std::fs::rename(path, &destination)?;
    sync_parent(&destination)?;
    Ok(())
}
