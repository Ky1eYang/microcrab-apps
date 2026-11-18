//! Outcome-only durable receipts. Pending approvals are never restored.
#[cfg(test)]
#[path = "decision_tests.rs"]
mod tests;
use super::model::{MAX_STORE_BYTES, bounded, invalid};
use microcrab_session::Error;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Mutex,
};

pub(super) struct DecisionStore {
    path: PathBuf,
    receipts: Mutex<BTreeMap<String, Value>>,
}
impl DecisionStore {
    pub fn open(directory: &Path) -> Result<Self, Error> {
        let path = directory.join("interaction_outcomes.json");
        super::store::reject_symlink(&path)?;
        let receipts: BTreeMap<String, Value> = match std::fs::File::open(&path) {
            Ok(file) => {
                use std::io::Read;
                let mut bytes = Vec::new();
                file.take(MAX_STORE_BYTES as u64 + 1)
                    .read_to_end(&mut bytes)?;
                if bytes.len() > MAX_STORE_BYTES {
                    return Err(invalid("interaction receipt store exceeds limit"));
                }
                serde_json::from_slice(&bytes)?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(error) => return Err(error.into()),
        };
        if receipts.len() > 8192 {
            return Err(invalid("interaction receipt retention limit exceeded"));
        }
        for (key, value) in &receipts {
            if key.len() > 256 {
                return Err(invalid("interaction receipt identity exceeds limit"));
            }
            bounded(value, 16 * 1024)?;
        }
        Ok(Self {
            path,
            receipts: Mutex::new(receipts),
        })
    }
    pub fn record(&self, kind: &str, id: &str, value: &Value) -> Result<(), Error> {
        use std::io::Write;
        let key = format!("{kind}:{id}");
        let mut receipts = self
            .receipts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(previous) = receipts.get(&key) {
            return if previous == value {
                Ok(())
            } else {
                Err(invalid("interaction outcome is immutable"))
            };
        }
        if receipts.len() >= 8192 {
            return Err(invalid("interaction receipt retention limit reached"));
        }
        bounded(value, 16 * 1024)?;
        let mut candidate = receipts.clone();
        candidate.insert(key, value.clone());
        let bytes = bounded(&candidate, MAX_STORE_BYTES)?;
        super::store::reject_symlink(&self.path)?;
        let temporary = self.path.with_extension("json.tmp");
        super::store::reject_symlink(&temporary)?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, &self.path)?;
        *receipts = candidate;
        Ok(())
    }
    pub fn get(&self, kind: &str, id: &str) -> Option<Value> {
        self.receipts
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&format!("{kind}:{id}"))
            .cloned()
    }
}
