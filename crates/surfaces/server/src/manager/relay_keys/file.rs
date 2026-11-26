//! One exact digest file is the authority. Owner indexes only locate records
//! for management; a staged index entry without a record grants nothing.
use super::{Change, Credential, KeyError, KeyMetadata, MAX_KEYS_PER_OWNER, MAX_RECORD_BYTES};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

const MAX_INDEX_BYTES: usize = 32 * 1024;

pub(super) struct Store {
    records: PathBuf,
    owners: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    hash: [u8; 32],
    credential: Credential,
}

impl Store {
    pub(super) fn open(data_dir: &Path) -> Result<Self, KeyError> {
        let root = data_dir.join("relay-keys");
        let records = root.join("by-digest");
        let owners = root.join("by-owner");
        fs::create_dir_all(&records)?;
        fs::create_dir_all(&owners)?;
        sync_dir(&root)?;
        sync_dir(data_dir)?;
        Ok(Self { records, owners })
    }

    fn digest_path(&self, hash: &[u8; 32]) -> PathBuf {
        self.records.join(hex(hash))
    }

    fn owner_path(&self, owner: &str) -> PathBuf {
        let hash: [u8; 32] = Sha256::digest(owner.as_bytes()).into();
        self.owners.join(hex(&hash))
    }

    fn index(&self, owner: &str) -> Result<BTreeMap<String, [u8; 32]>, KeyError> {
        let index = read_bounded(&self.owner_path(owner), MAX_INDEX_BYTES)?
            .as_deref()
            .map(serde_json::from_slice::<BTreeMap<String, [u8; 32]>>)
            .transpose()?
            .unwrap_or_default();
        if index.len() > MAX_KEYS_PER_OWNER {
            return Err(KeyError::Storage("oversized owner index"));
        }
        Ok(index)
    }

    pub(super) fn lookup(&self, hash: &[u8; 32]) -> Result<Option<Credential>, KeyError> {
        let Some(bytes) = read_bounded(&self.digest_path(hash), MAX_RECORD_BYTES)? else {
            return Ok(None);
        };
        let record: Record = serde_json::from_slice(&bytes)?;
        if &record.hash != hash {
            return Err(KeyError::Storage(
                "record digest does not match lookup identity",
            ));
        }
        Ok(Some(record.credential))
    }

    fn owned(
        &self,
        owner: &str,
        id: &str,
        hash: &[u8; 32],
    ) -> Result<Option<Credential>, KeyError> {
        let credential = self.lookup(hash)?;
        if credential
            .as_ref()
            .is_some_and(|found| found.owner != owner || found.key.id != id)
        {
            return Err(KeyError::Storage(
                "owner index does not match record identity",
            ));
        }
        Ok(credential)
    }

    pub(super) fn list(&self, owner: &str) -> Result<Vec<KeyMetadata>, KeyError> {
        let mut keys = Vec::new();
        for (id, hash) in self.index(owner)? {
            if let Some(found) = self.owned(owner, &id, &hash)? {
                keys.push(found.key);
            }
        }
        Ok(keys)
    }

    pub(super) fn create(&self, hash: &[u8; 32], credential: &Credential) -> Result<(), KeyError> {
        let mut index = self.index(&credential.owner)?;
        if index.len() >= MAX_KEYS_PER_OWNER {
            return Err(KeyError::Capacity);
        }
        if index.contains_key(&credential.key.id) || self.lookup(hash)?.is_some() {
            return Err(KeyError::Storage("generated key identity collision"));
        }
        let record = record_bytes(hash, credential)?;
        index.insert(credential.key.id.clone(), *hash);
        let index_bytes = serde_json::to_vec(&index)?;
        if index_bytes.len() > MAX_INDEX_BYTES {
            return Err(KeyError::Storage(
                "encoded owner index exceeds storage limits",
            ));
        }
        // Store the directory hint durably before publishing the authority.
        // An unacknowledged create is never replayed. A crash can reserve an
        // index slot without a record, but can never create an unlisted key.
        atomic_write(&self.owner_path(&credential.owner), &index_bytes)?;
        atomic_write(&self.digest_path(hash), &record)
    }

    pub(super) fn change(
        &self,
        owner: &str,
        id: &str,
        change: Change,
    ) -> Result<KeyMetadata, KeyError> {
        let index = self.index(owner)?;
        let hash = index.get(id).ok_or(KeyError::NotFound)?;
        let mut found = self.owned(owner, id, hash)?.ok_or(KeyError::NotFound)?;
        change.apply(&mut found.key)?;
        let record = record_bytes(hash, &found)?;
        atomic_write(&self.digest_path(hash), &record)?;
        Ok(found.key)
    }

    pub(super) fn record_use(&self, owner: &str, id: &str, used_at_ms: u64) -> Result<(), KeyError> {
        let index = self.index(owner)?;
        let Some(hash) = index.get(id) else {
            return Ok(());
        };
        let Some(mut found) = self.owned(owner, id, hash)? else {
            return Ok(());
        };
        found.key.last_used_at_ms = Some(
            found
                .key
                .last_used_at_ms
                .map_or(used_at_ms, |prev| prev.max(used_at_ms)),
        );
        found.key.request_count = found.key.request_count.saturating_add(1);
        let record = record_bytes(hash, &found)?;
        atomic_write(&self.digest_path(hash), &record)
    }
}

fn hex(hash: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut result = String::with_capacity(64);
    for byte in hash {
        // Writing to a String is infallible.
        let _ = write!(result, "{byte:02x}");
    }
    result
}

fn record_bytes(hash: &[u8; 32], credential: &Credential) -> Result<Vec<u8>, KeyError> {
    #[derive(Serialize)]
    struct RecordRef<'a> {
        hash: &'a [u8; 32],
        credential: &'a Credential,
    }
    let bytes = serde_json::to_vec(&RecordRef { hash, credential })?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(KeyError::Invalid(
            "Relay key metadata exceeds storage limits",
        ));
    }
    Ok(bytes)
}

fn read_bounded(path: &Path, limit: usize) -> Result<Option<Vec<u8>>, KeyError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !file.metadata()?.is_file() {
        return Err(KeyError::Storage("unexpected type for relay key file"));
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(KeyError::Storage("oversized file"));
    }
    Ok(Some(bytes))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), KeyError> {
    let parent = path.parent().ok_or(KeyError::Storage("missing parent"))?;
    // One writer owns each deployment. The fixed staging filename safely
    // overwrites only its own interrupted write for this digest or owner.
    let temp = path.with_extension("tmp");
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temp, path)?;
    sync_dir(parent)?;
    Ok(())
}

fn sync_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
