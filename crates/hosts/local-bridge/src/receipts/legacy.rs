//! Read-only first-open import of the retired per-operation files. Existing
//! completion decoding keeps its conservative indeterminate fallback. Originals
//! remain untouched and are never consulted after active.log is published.
use super::{MAX_RECEIPTS, StorageResult, storage};
use microcrab_host_protocol::*;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path};

fn key(operation_id: &str) -> String {
    format!("{:x}", Sha256::digest(operation_id.as_bytes()))
}
#[derive(serde::Serialize, serde::Deserialize)]
struct UnknownDebug {
    operation_id: String,
    payload_digest: String,
    scope: BindingScope,
    reason: WireError,
}
fn read_bounded(path: &Path, max: u64) -> StorageResult<Vec<u8>> {
    let metadata = storage::ordinary_file(path)?.ok_or("legacy receipt disappeared")?;
    if metadata.len() > max {
        return Err("legacy receipt exceeds durable limit".into());
    }
    Ok(std::fs::read(path)?)
}
fn unknown_reason(dir: &Path, receipt: &OperationReceipt) -> StorageResult<Option<WireError>> {
    let path = dir.join(format!("{}.unknown.json", key(&receipt.operation_id)));
    if storage::ordinary_file(&path)?.is_none() {
        return Ok(None);
    }
    let debug: UnknownDebug = serde_json::from_slice(&read_bounded(&path, 4096)?)?;
    if debug.operation_id != receipt.operation_id
        || debug.payload_digest != receipt.payload_digest
        || debug.scope != receipt.scope
    {
        return Err("legacy unknown reason does not match receipt".into());
    }
    Ok(Some(debug.reason))
}
pub(super) fn load(dir: &Path) -> StorageResult<Vec<(OperationReceipt, Option<WireError>)>> {
    let mut result = BTreeMap::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.ends_with(".intent.json") {
            continue;
        }
        if result.len() >= MAX_RECEIPTS {
            return Err("receipt retention capacity exceeded".into());
        }
        let mut receipt: OperationReceipt =
            serde_json::from_slice(&read_bounded(&entry.path(), 262144)?)?;
        if name.as_ref() != format!("{}.intent.json", key(&receipt.operation_id)) {
            return Err("receipt identity does not match its storage key".into());
        }
        let completion = dir.join(format!("{}.done.json", key(&receipt.operation_id)));
        if storage::ordinary_file(&completion)?.is_some() {
            match serde_json::from_slice::<OperationReceipt>(&read_bounded(&completion, 262144)?) {
                Ok(done)
                    if done.operation_id == receipt.operation_id
                        && done.payload_digest == receipt.payload_digest
                        && done.scope == receipt.scope =>
                {
                    receipt = done
                }
                _ => receipt.state = ReceiptState::Indeterminate,
            }
        } else {
            receipt.state = ReceiptState::Indeterminate;
        }
        let settlement = dir.join(format!("{}.failed.json", key(&receipt.operation_id)));
        if storage::ordinary_file(&settlement)?.is_some() {
            let failed: OperationReceipt =
                serde_json::from_slice(&read_bounded(&settlement, 262144)?)?;
            if failed.operation_id != receipt.operation_id
                || failed.payload_digest != receipt.payload_digest
                || failed.scope != receipt.scope
                || !matches!(
                    failed.state,
                    ReceiptState::FailedUnknown {
                        effects_may_have_occurred: true,
                        ..
                    }
                )
                || !matches!(
                    receipt.state,
                    ReceiptState::Intent | ReceiptState::Indeterminate
                )
            {
                return Err("settlement does not match original unknown intent".into());
            }
            receipt = failed;
        }
        let reason = unknown_reason(dir, &receipt)?;
        if let (
            Some(original),
            ReceiptState::FailedUnknown {
                reason: settled, ..
            },
        ) = (&reason, &receipt.state)
            && original != settled
        {
            return Err("legacy settlement changed original unknown reason".into());
        }
        if result
            .insert(receipt.operation_id.clone(), (receipt, reason))
            .is_some()
        {
            return Err("duplicate legacy receipt identity".into());
        }
    }
    Ok(result.into_values().collect())
}

#[cfg(test)]
pub(super) fn save_fixture(dir: &Path, receipt: &OperationReceipt, suffix: &str) {
    super::create_durable(
        &dir.join(format!("{}.{suffix}.json", key(&receipt.operation_id))),
        &serde_json::to_vec(receipt).expect("encode legacy receipt"),
    )
    .expect("write legacy receipt");
}
#[cfg(test)]
pub(super) fn save_reason_fixture(dir: &Path, receipt: &OperationReceipt, reason: WireError) {
    let debug = UnknownDebug {
        operation_id: receipt.operation_id.clone(),
        payload_digest: receipt.payload_digest.clone(),
        scope: receipt.scope.clone(),
        reason,
    };
    super::create_durable(
        &dir.join(format!("{}.unknown.json", key(&receipt.operation_id))),
        &serde_json::to_vec(&debug).expect("encode legacy reason"),
    )
    .expect("write legacy reason");
}
