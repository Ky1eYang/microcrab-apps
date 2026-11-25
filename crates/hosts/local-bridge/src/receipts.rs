//! One receipt owner serializes durable declarations, never the effects themselves.
//! The index is a checked projection; result bodies are read only when requested.
mod legacy;
mod storage;
#[cfg(test)]
mod tests;

use microcrab_host_protocol::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use storage::{BodyRef, Header, ReadFrame};

pub(crate) const MAX_RECEIPTS: usize = 4096;
type StorageResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const MAX_BODY_BYTES: usize = 262144 + 8192;

pub(crate) fn create_durable(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    storage::sync_parent(path)
}

fn conflict(message: &'static str) -> WireError {
    WireError::new(ErrorCode::OperationConflict, message)
}
fn io_error() -> WireError {
    WireError::new(ErrorCode::Io, "cannot persist durable receipt")
        .with_cause(Some(HostCause::ReceiptStoreUnavailable))
}
fn valid_identity(receipt: &OperationReceipt) -> bool {
    !receipt.operation_id.is_empty()
        && receipt.operation_id.len() <= 256
        && receipt.payload_digest.len() == 64
        && receipt
            .payload_digest
            .bytes()
            .all(|b| b.is_ascii_hexdigit())
        && receipt.scope.valid()
        && receipt.scope.is_project()
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    Intent,
    Done,
    Unknown,
    Failed,
}
impl Phase {
    pub(crate) fn unresolved(self) -> bool {
        matches!(self, Self::Intent | Self::Unknown)
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(try_from = "DiskRecord", into = "DiskRecord")]
struct Record {
    version: u32,
    phase: Phase,
    receipt: OperationReceipt,
    original_reason: Option<WireError>,
    consumption_key: Option<String>,
}

/// Identity and terminal phase survive independently from the optional body.
/// This version always writes Some(body); decoding omitted compacted bodies is
/// refused until the later compaction/replay contract is implemented.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DiskRecord {
    version: u32,
    phase: Phase,
    operation_id: String,
    payload_digest: String,
    scope: BindingScope,
    body: Option<ReceiptState>,
    original_reason: Option<WireError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    consumption_key: Option<String>,
}
impl From<Record> for DiskRecord {
    fn from(record: Record) -> Self {
        Self {
            version: record.version,
            phase: record.phase,
            operation_id: record.receipt.operation_id,
            payload_digest: record.receipt.payload_digest,
            scope: record.receipt.scope,
            body: Some(record.receipt.state),
            original_reason: record.original_reason,
            consumption_key: record.consumption_key,
        }
    }
}
impl TryFrom<DiskRecord> for Record {
    type Error = &'static str;
    fn try_from(record: DiskRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            version: record.version,
            phase: record.phase,
            receipt: OperationReceipt {
                operation_id: record.operation_id,
                payload_digest: record.payload_digest,
                scope: record.scope,
                state: record
                    .body
                    .ok_or("compacted receipt bodies are not supported")?,
            },
            original_reason: record.original_reason,
            consumption_key: record.consumption_key,
        })
    }
}
impl Record {
    fn validate(&self) -> StorageResult<()> {
        if self.version != 1 || !valid_identity(&self.receipt) {
            return Err("invalid receipt identity or version".into());
        }
        let valid_state = matches!(
            (self.phase, &self.receipt.state),
            (Phase::Intent, ReceiptState::Intent)
                | (Phase::Done, ReceiptState::Completed(_))
                | (Phase::Unknown, ReceiptState::Indeterminate)
                | (
                    Phase::Failed,
                    ReceiptState::FailedUnknown {
                        effects_may_have_occurred: true,
                        ..
                    }
                )
        );
        if !valid_state || (self.phase != Phase::Unknown && self.original_reason.is_some()) {
            return Err("invalid receipt phase or state".into());
        }
        if self.original_reason.as_ref().is_some_and(|reason| {
            serde_json::to_vec(reason).map_or(true, |bytes| bytes.len() > 4096)
        }) {
            return Err("unknown reason exceeds durable limit".into());
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Fence {
    pub(crate) operation_id: String,
    pub(crate) payload_digest: String,
    pub(crate) scope: BindingScope,
    pub(crate) phase: Phase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) consumption_key: Option<String>,
    // Optional on disk so future compaction can omit a terminal body without
    // changing the envelope. This writer always retains bodies; open rejects an
    // omitted body until compacted-body replay semantics are implemented.
    body: Option<BodyRef>,
    reason: Option<BodyRef>,
    reason_hash: Option<[u8; 32]>,
}
impl Fence {
    fn matches(&self, receipt: &OperationReceipt) -> bool {
        self.operation_id == receipt.operation_id
            && self.payload_digest == receipt.payload_digest
            && self.scope == receipt.scope
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Index {
    version: u32,
    log_id: [u8; 16],
    watermark: u64,
    sequence: u64,
    anchor: Option<BodyRef>,
    entries: BTreeMap<String, Fence>,
}
impl Index {
    fn empty(header: &Header) -> Self {
        Self {
            version: 1,
            log_id: header.log_id,
            watermark: storage::HEADER_BYTES,
            sequence: 0,
            anchor: None,
            entries: BTreeMap::new(),
        }
    }
    fn validate(&self, header: &Header) -> StorageResult<()> {
        if self.version != 1 || self.log_id != header.log_id || self.entries.len() > MAX_RECEIPTS {
            return Err("invalid receipt index identity or capacity".into());
        }
        match self.anchor {
            Some(anchor) if anchor.end()? == self.watermark && anchor.sequence == self.sequence => {
            }
            None if self.watermark == storage::HEADER_BYTES
                && self.sequence == 0
                && self.entries.is_empty() => {}
            _ => return Err("invalid receipt index watermark".into()),
        }
        for (id, entry) in &self.entries {
            let identity = OperationReceipt {
                operation_id: entry.operation_id.clone(),
                payload_digest: entry.payload_digest.clone(),
                scope: entry.scope.clone(),
                state: ReceiptState::Intent,
            };
            if id != &entry.operation_id || !valid_identity(&identity) {
                return Err("invalid fence identity".into());
            }
            let body = entry
                .body
                .ok_or("compacted receipt bodies are not supported")?;
            for reference in [Some(body), entry.reason].into_iter().flatten() {
                if reference.offset < storage::HEADER_BYTES
                    || reference.length > MAX_BODY_BYTES as u64
                    || reference.sequence == 0
                    || reference.sequence > self.sequence
                    || reference.end()? > self.watermark
                {
                    return Err("receipt reference exceeds durable watermark".into());
                }
            }
            if (entry.reason.is_some() && entry.phase == Phase::Intent)
                || entry.reason.is_some() != entry.reason_hash.is_some()
            {
                return Err("invalid unknown reason projection".into());
            }
        }
        Ok(())
    }
    fn fold(&mut self, record: &Record, reference: BodyRef) -> StorageResult<()> {
        record.validate()?;
        if reference.offset != self.watermark
            || reference.sequence
                != self
                    .sequence
                    .checked_add(1)
                    .ok_or("receipt sequence overflow")?
        {
            return Err("receipt log sequence mismatch".into());
        }
        let id = &record.receipt.operation_id;
        let previous = self.entries.get(id);
        match previous {
            None if record.phase == Phase::Intent => {
                if self.entries.len() >= MAX_RECEIPTS {
                    return Err("receipt retention capacity exceeded".into());
                }
            }
            Some(entry) if entry.matches(&record.receipt) && entry.phase.unresolved() => {
                if record.phase == Phase::Intent
                    || (entry.reason.is_some() && record.original_reason.is_some())
                {
                    return Err("duplicate immutable receipt phase or reason".into());
                }
            }
            _ => return Err("receipt phase does not match original intent".into()),
        }
        let reason = if record.original_reason.is_some() {
            Some(reference)
        } else {
            previous.and_then(|entry| entry.reason)
        };
        if let (Some(original), ReceiptState::FailedUnknown { reason, .. }) = (
            previous.and_then(|entry| entry.reason_hash),
            &record.receipt.state,
        ) && storage::hash(&serde_json::to_vec(reason)?) != original
        {
            return Err("settlement changed original unknown reason".into());
        }
        let reason_hash = match &record.original_reason {
            Some(reason) => Some(storage::hash(&serde_json::to_vec(reason)?)),
            None => previous.and_then(|entry| entry.reason_hash),
        };
        self.entries.insert(
            id.clone(),
            Fence {
                operation_id: id.clone(),
                payload_digest: record.receipt.payload_digest.clone(),
                scope: record.receipt.scope.clone(),
                phase: record.phase,
                consumption_key: record
                    .consumption_key
                    .clone()
                    .or_else(|| previous.and_then(|entry| entry.consumption_key.clone())),
                body: Some(reference),
                reason,
                reason_hash,
            },
        );
        self.watermark = reference.end()?;
        self.sequence = reference.sequence;
        self.anchor = Some(reference);
        Ok(())
    }
}

pub(crate) struct ReceiptStore {
    dir: PathBuf,
    log: File,
    index: Index,
    fresh_intents: HashSet<String>,
    poisoned: bool,
    #[cfg(test)]
    body_reads: usize,
}
impl ReceiptStore {
    /// Caller holds the workspace instance lock. A published log always wins over
    /// legacy files, even when its checked index was never published or is lost.
    pub(crate) fn open(state_dir: &Path, workspace_id: &str) -> StorageResult<Self> {
        let dir = state_dir.join("receipts");
        storage::ensure_dir(&dir)?;
        let log_path = dir.join("active.log");
        let index_path = dir.join("fence.index");
        let log_exists = storage::ordinary_file(&log_path)?.is_some();
        let index_exists = storage::ordinary_file(&index_path)?.is_some();
        if !log_exists {
            if index_exists {
                return Err("receipt index exists without authoritative log".into());
            }
            Self::migrate(state_dir, &dir, workspace_id)?;
        }
        let mut log = OpenOptions::new().read(true).write(true).open(&log_path)?;
        let header = storage::read_header(&mut log, workspace_id)?;
        let saved = storage::read_index::<Index>(&index_path)?;
        let checked = saved.filter(|index| index.validate(&header).is_ok());
        let size = log.metadata()?.len();
        // A valid projection is durable evidence of its watermark even when its
        // anchor was subsequently damaged. Never truncate inside that boundary.
        let protected = checked
            .as_ref()
            .map_or(storage::HEADER_BYTES, |index| index.watermark);
        if size < protected {
            return Err("receipt log is shorter than its durable watermark".into());
        }
        let anchored = match &checked {
            Some(index) => Self::check_anchor(&mut log, index).is_ok(),
            None => false,
        };
        let mut index = if anchored {
            checked.ok_or("receipt checkpoint unavailable")?
        } else {
            Index::empty(&header)
        };
        let mut changed = !anchored;
        while index.watermark < size {
            match storage::read_frame(&mut log, index.watermark, size)? {
                ReadFrame::Complete(record, reference) => {
                    index.fold(&record, reference)?;
                    changed = true;
                }
                ReadFrame::Incomplete { header } => {
                    // With no valid checkpoint a partial header supplies no safe
                    // frame boundary. A verified full header plus short body does,
                    // under the append-prefix crash model documented for this log.
                    let invalid_sequence = header.is_some_and(|reference| {
                        index.sequence.checked_add(1) != Some(reference.sequence)
                    });
                    if index.watermark < protected
                        || (!anchored && header.is_none())
                        || invalid_sequence
                    {
                        return Err("cannot establish a safe incomplete receipt tail".into());
                    }
                    log.set_len(index.watermark)?;
                    log.sync_all()?;
                    changed = true;
                    break;
                }
            }
        }
        if changed {
            storage::publish_index(&dir, &index)?;
        }
        Ok(Self {
            dir,
            log,
            index,
            fresh_intents: HashSet::new(),
            poisoned: false,
            #[cfg(test)]
            body_reads: 0,
        })
    }
    fn check_anchor(log: &mut File, index: &Index) -> StorageResult<()> {
        if let Some(reference) = index.anchor {
            let record = storage::read_reference(log, reference, index.watermark)?;
            record.validate()?;
            let fence = index
                .entries
                .get(&record.receipt.operation_id)
                .ok_or("anchor fence missing")?;
            if !fence.matches(&record.receipt)
                || fence.body != Some(reference)
                || fence.phase != record.phase
            {
                return Err("receipt anchor does not match checked fence".into());
            }
        }
        Ok(())
    }
    fn migrate(state_dir: &Path, dir: &Path, workspace_id: &str) -> StorageResult<()> {
        let legacy = legacy::load(state_dir)?;
        let header = Header::new(workspace_id);
        let path = storage::temporary_path(dir, "import");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        storage::write_header(&mut file, &header)?;
        let mut index = Index::empty(&header);
        for (receipt, reason) in legacy {
            let mut intent = receipt.clone();
            intent.state = ReceiptState::Intent;
            Self::import_record(
                &mut file,
                &mut index,
                Record {
                    version: 1,
                    phase: Phase::Intent,
                    receipt: intent,
                    original_reason: None,
                    consumption_key: None,
                },
            )?;
            if reason.is_some() || matches!(receipt.state, ReceiptState::Indeterminate) {
                let mut unknown = receipt.clone();
                unknown.state = ReceiptState::Indeterminate;
                Self::import_record(
                    &mut file,
                    &mut index,
                    Record {
                        version: 1,
                        phase: Phase::Unknown,
                        receipt: unknown,
                        original_reason: reason,
                        consumption_key: None,
                    },
                )?;
            }
            let phase = match &receipt.state {
                ReceiptState::Completed(_) => Some(Phase::Done),
                ReceiptState::FailedUnknown { .. } => Some(Phase::Failed),
                _ => None,
            };
            if let Some(phase) = phase {
                Self::import_record(
                    &mut file,
                    &mut index,
                    Record {
                        version: 1,
                        phase,
                        receipt,
                        original_reason: None,
                        consumption_key: None,
                    },
                )?;
            }
        }
        file.sync_all()?;
        drop(file);
        // active.log is published only after the entire legacy import is durable.
        // A crash before index publication therefore needs no migration marker.
        std::fs::rename(&path, dir.join("active.log"))?;
        storage::sync_parent(&dir.join("active.log"))?;
        storage::publish_index(dir, &index)
    }
    fn import_record(file: &mut File, index: &mut Index, record: Record) -> StorageResult<()> {
        let (bytes, reference) =
            storage::encode_frame(&record, index.watermark, index.sequence + 1)?;
        index.fold(&record, reference)?;
        file.write_all(&bytes)?;
        Ok(())
    }
    pub(crate) fn len(&self) -> usize {
        self.index.entries.len()
    }
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.index.entries.is_empty()
    }
    pub(crate) fn fence(&self, id: &str) -> Option<&Fence> {
        self.index.entries.get(id)
    }
    pub(crate) fn fences(&self) -> impl Iterator<Item = &Fence> {
        self.index.entries.values()
    }
    pub(crate) fn ready(&self) -> Result<(), WireError> {
        if self.poisoned {
            return Err(WireError::new(
                ErrorCode::OutcomeUnknown,
                "receipt storage requires reopen",
            ));
        }
        Ok(())
    }
    fn read_record(&mut self, reference: BodyRef) -> Result<Record, WireError> {
        #[cfg(test)]
        {
            self.body_reads += 1;
        }
        let result = storage::read_reference(&mut self.log, reference, self.index.watermark)
            .and_then(|record| {
                record.validate()?;
                Ok(record)
            });
        match result {
            Ok(record) => Ok(record),
            Err(error) => {
                self.poisoned = true;
                Err(WireError::new(
                    ErrorCode::OperationConflict,
                    format!("invalid referenced receipt body: {error}"),
                ))
            }
        }
    }
    pub(crate) fn get(&mut self, id: &str) -> Result<Option<OperationReceipt>, WireError> {
        self.ready()?;
        let Some(fence) = self.fence(id).cloned() else {
            return Ok(None);
        };
        let record = self.read_record(
            fence
                .body
                .ok_or_else(|| conflict("receipt body unavailable"))?,
        )?;
        let bad_reason = if let (Some(original), ReceiptState::FailedUnknown { reason, .. }) =
            (fence.reason_hash, &record.receipt.state)
        {
            serde_json::to_vec(reason).map_or(true, |bytes| storage::hash(&bytes) != original)
        } else {
            false
        };
        if !fence.matches(&record.receipt) || fence.phase != record.phase || bad_reason {
            self.poisoned = true;
            return Err(conflict("receipt body does not match fence"));
        }
        let mut receipt = record.receipt;
        if fence.phase == Phase::Intent && !self.fresh_intents.contains(id) {
            receipt.state = ReceiptState::Indeterminate;
        }
        Ok(Some(receipt))
    }
    pub(crate) fn unknown_reason(
        &mut self,
        receipt: &OperationReceipt,
    ) -> Result<Option<WireError>, WireError> {
        self.ready()?;
        let fence = self
            .fence(&receipt.operation_id)
            .cloned()
            .ok_or_else(|| conflict("original receipt unavailable"))?;
        if !fence.matches(receipt) {
            return Err(conflict("unknown reason scope mismatch"));
        }
        let Some(reference) = fence.reason else {
            return Ok(None);
        };
        let record = self.read_record(reference)?;
        let bad_reason = record.original_reason.as_ref().is_none_or(|reason| {
            serde_json::to_vec(reason).map_or(true, |bytes| {
                Some(storage::hash(&bytes)) != fence.reason_hash
            })
        });
        if !fence.matches(&record.receipt) || record.phase != Phase::Unknown || bad_reason {
            self.poisoned = true;
            return Err(conflict("unknown reason does not match fence"));
        }
        Ok(record.original_reason)
    }
    fn append(&mut self, record: Record) -> Result<(), WireError> {
        self.ready()?;
        record.validate().map_err(|error| {
            WireError::new(
                ErrorCode::OperationConflict,
                format!("invalid receipt declaration: {error}"),
            )
        })?;
        let sequence = self.index.sequence.checked_add(1).ok_or_else(io_error)?;
        let (bytes, reference) = storage::encode_frame(&record, self.index.watermark, sequence)
            .map_err(|error| {
                WireError::new(
                    ErrorCode::Io,
                    format!("cannot encode durable receipt: {error}"),
                )
                .with_cause(Some(HostCause::ReceiptStoreUnavailable))
            })?;
        let mut next = self.index.clone();
        next.fold(&record, reference).map_err(|error| {
            WireError::new(
                ErrorCode::OperationConflict,
                format!("receipt declaration conflicts with durable fence: {error}"),
            )
        })?;
        // Seek and write happen under Bridge.receipts, including both syncs and
        // checkpoint publication. No caller can dispatch from partial intent.
        let result = storage::append_frame(&mut self.log, self.index.watermark, &bytes)
            .and_then(|()| storage::publish_index(&self.dir, &next));
        if let Err(error) = result {
            self.poisoned = true;
            return Err(WireError::new(
                ErrorCode::Io,
                format!("cannot persist durable receipt: {error}"),
            )
            .with_cause(Some(HostCause::ReceiptStoreUnavailable)));
        }
        if record.phase == Phase::Intent {
            self.fresh_intents
                .insert(record.receipt.operation_id.clone());
        } else {
            self.fresh_intents.remove(&record.receipt.operation_id);
        }
        self.index = next;
        Ok(())
    }
    pub(crate) fn intent(&mut self, receipt: &OperationReceipt) -> Result<(), WireError> {
        self.intent_scoped(receipt, None)
    }
    pub(crate) fn intent_scoped(
        &mut self,
        receipt: &OperationReceipt,
        consumption_key: Option<String>,
    ) -> Result<(), WireError> {
        if self.len() >= MAX_RECEIPTS && self.fence(&receipt.operation_id).is_none() {
            return Err(WireError::new(
                ErrorCode::Limit,
                "durable receipt capacity reached",
            ));
        }
        self.append(Record {
            version: 1,
            phase: Phase::Intent,
            receipt: receipt.clone(),
            original_reason: None,
            consumption_key,
        })
    }
    pub(crate) fn complete(&mut self, receipt: &OperationReceipt) -> Result<(), WireError> {
        self.append(Record {
            version: 1,
            phase: Phase::Done,
            receipt: receipt.clone(),
            original_reason: None,
            consumption_key: None,
        })
    }
    pub(crate) fn save_unknown(
        &mut self,
        receipt: &OperationReceipt,
        reason: WireError,
    ) -> Result<(), WireError> {
        if let Some(original) = self.unknown_reason(receipt)? {
            if original != reason {
                return Err(conflict("original unknown reason is immutable"));
            }
            return Ok(());
        }
        let mut unknown = receipt.clone();
        unknown.state = ReceiptState::Indeterminate;
        self.append(Record {
            version: 1,
            phase: Phase::Unknown,
            receipt: unknown,
            original_reason: Some(reason),
            consumption_key: None,
        })
    }
    pub(crate) fn settle(&mut self, receipt: &OperationReceipt) -> Result<(), WireError> {
        if let ReceiptState::FailedUnknown { reason, .. } = &receipt.state
            && self
                .unknown_reason(receipt)?
                .is_some_and(|original| original != *reason)
        {
            return Err(conflict("settlement must preserve original unknown reason"));
        }
        self.append(Record {
            version: 1,
            phase: Phase::Failed,
            receipt: receipt.clone(),
            original_reason: None,
            consumption_key: None,
        })
    }
}
