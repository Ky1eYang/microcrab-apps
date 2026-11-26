//! Durable relay-only credential authority. Browser principals never enter this store.
mod file;
#[cfg(feature = "storage-sqlite")]
mod sqlite;
#[cfg(test)]
mod tests;

use crate::config::{Config, Storage};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use microcrab_session::Error;
use microcrab_storage::ModelSelection;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

const KEY_PREFIX: &str = "mc_sk_";
const SECRET_BYTES: usize = 32;
const MAX_KEYS_PER_OWNER: usize = 128;
const MAX_RECORD_BYTES: usize = 16 * 1024;
const MAX_PROVIDERS: usize = 32;

#[derive(Debug, thiserror::Error)]
pub(crate) enum KeyError {
    #[error("{0}")]
    Invalid(&'static str),
    #[error("Relay key not found")]
    NotFound,
    #[error("Relay key revision changed")]
    Conflict,
    #[error("Revoked keys cannot be modified")]
    Revoked,
    #[error("Relay key retention limit reached")]
    Capacity,
    #[error("Relay key authority is busy")]
    Busy,
    #[error("Relay key authority is closed")]
    Closed,
    #[error("Relay key random generation failed")]
    Random,
    #[error("Relay key worker failed")]
    Worker,
    #[error("Relay key storage invariant failed: {0}")]
    Storage(&'static str),
    #[error("Relay key file operation failed")]
    Io(#[from] std::io::Error),
    #[error("Relay key storage JSON failed")]
    Json(#[from] serde_json::Error),
    #[cfg(feature = "storage-sqlite")]
    #[error("Relay key SQLite operation failed")]
    Sqlite(#[from] rusqlite::Error),
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KeyMetadata {
    pub id: String,
    pub name: String,
    pub key_prefix: String,
    pub enabled: bool,
    pub revoked: bool,
    pub allowed_providers: Option<Vec<String>>,
    pub default_model_selection: Option<ModelSelection>,
    pub created_at_ms: u64,
    #[serde(default)]
    pub last_used_at_ms: Option<u64>,
    #[serde(default)]
    pub request_count: u64,
    pub revision: u64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Credential {
    pub owner: String,
    pub key: KeyMetadata,
}

#[derive(Serialize)]
pub(crate) struct CreatedKey {
    pub key: KeyMetadata,
    pub secret: String,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateKey {
    name: Option<String>,
    enabled: Option<bool>,
    allowed_providers: Option<Vec<String>>,
    default_model_selection: Option<ModelSelection>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateKey {
    expected_revision: u64,
    name: Option<String>,
    enabled: Option<bool>,
    #[serde(default, deserialize_with = "nullable_patch")]
    allowed_providers: Option<Option<Vec<String>>>,
    #[serde(default, deserialize_with = "nullable_patch")]
    default_model_selection: Option<Option<ModelSelection>>,
}

fn nullable_patch<'de, D, T>(input: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(input).map(Some)
}

enum Change {
    Update(UpdateKey),
    Revoke(u64),
}
impl Change {
    fn expected(&self) -> u64 {
        match self {
            Self::Update(key) => key.expected_revision,
            Self::Revoke(revision) => *revision,
        }
    }

    fn apply(self, key: &mut KeyMetadata) -> Result<(), KeyError> {
        if key.revision != self.expected() {
            return Err(KeyError::Conflict);
        }
        if key.revoked {
            return Err(KeyError::Revoked);
        }
        match self {
            Self::Update(update) => {
                if let Some(name) = update.name {
                    key.name = name;
                }
                if let Some(enabled) = update.enabled {
                    key.enabled = enabled;
                }
                if let Some(allowed_providers) = update.allowed_providers {
                    key.allowed_providers = allowed_providers;
                }
                if let Some(default_model_selection) = update.default_model_selection {
                    key.default_model_selection = default_model_selection;
                }
                validate_metadata(key)?;
            }
            Self::Revoke(_) => {
                key.enabled = false;
                key.revoked = true;
            }
        }
        key.revision = key.revision.checked_add(1).ok_or(KeyError::Conflict)?;
        Ok(())
    }
}

fn valid_label(value: &str, max_bytes: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= max_bytes
        && !value.contains(KEY_PREFIX)
        && !value.chars().any(char::is_control)
}

fn valid_provider_id(value: &str) -> bool {
    valid_label(value, 128) && !value.contains('/')
}

fn validate_metadata(key: &KeyMetadata) -> Result<(), KeyError> {
    if !valid_label(&key.name, 128) {
        return Err(KeyError::Invalid("Invalid relay key name"));
    }
    if let Some(providers) = &key.allowed_providers
        && (providers.len() > MAX_PROVIDERS
            || providers.iter().enumerate().any(|(index, provider)| {
                !valid_provider_id(provider) || providers[..index].contains(provider)
            }))
    {
        return Err(KeyError::Invalid("Invalid allowed provider list"));
    }
    if let Some(selection) = &key.default_model_selection {
        if !valid_provider_id(&selection.provider_id)
            || !valid_label(&selection.model, 256)
            || selection
                .variant
                .as_deref()
                .is_some_and(|v| v != "default" && !valid_label(v, 64))
        {
            return Err(KeyError::Invalid("Invalid default relay model selection"));
        }
        if key
            .allowed_providers
            .as_ref()
            .is_some_and(|providers| !providers.contains(&selection.provider_id))
        {
            return Err(KeyError::Invalid(
                "Default relay provider is outside the key scope",
            ));
        }
    }
    Ok(())
}

fn digest(secret: &str) -> Option<[u8; 32]> {
    let encoded = secret.strip_prefix(KEY_PREFIX)?;
    if encoded.len() != 43 {
        return None;
    }
    let mut bytes = [0; SECRET_BYTES];
    // The strict decoder rejects padding, non-alphabet bytes and trailing bits.
    if URL_SAFE_NO_PAD.decode_slice(encoded, &mut bytes).ok()? != SECRET_BYTES {
        return None;
    }
    Some(Sha256::digest(secret.as_bytes()).into())
}

#[cfg(feature = "storage-sqlite")]
fn encode(value: &Credential) -> Result<Vec<u8>, KeyError> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(KeyError::Invalid(
            "Relay key metadata exceeds storage limits",
        ));
    }
    Ok(bytes)
}

#[cfg(feature = "storage-sqlite")]
fn decode(bytes: &[u8]) -> Result<Credential, KeyError> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(KeyError::Storage("oversized record"));
    }
    Ok(serde_json::from_slice(bytes)?)
}

enum Backend {
    File(file::Store),
    #[cfg(feature = "storage-sqlite")]
    Sqlite(sqlite::Store),
}

#[derive(Clone)]
pub(crate) struct RelayKeys {
    permits: Arc<Semaphore>,
    store: Arc<Mutex<Option<Backend>>>,
}

impl RelayKeys {
    pub(super) async fn open(config: &Config) -> Result<Self, Error> {
        Self::open_at(config.storage, config.data_dir.clone())
            .await
            .map_err(|error| {
                Error::config_with_private(
                    "Relay key authority could not open",
                    format!("{error:#?}"),
                )
            })
    }

    async fn open_at(storage: Storage, data_dir: std::path::PathBuf) -> Result<Self, KeyError> {
        let store = tokio::task::spawn_blocking(move || match storage {
            Storage::File => file::Store::open(&data_dir).map(Backend::File),
            #[cfg(feature = "storage-sqlite")]
            Storage::Sqlite => sqlite::Store::open(&data_dir).map(Backend::Sqlite),
            #[cfg(not(feature = "storage-sqlite"))]
            Storage::Sqlite => Err(KeyError::Storage("SQLite support is not compiled")),
        })
        .await
        .map_err(|_| KeyError::Worker)??;
        Ok(Self {
            permits: Arc::new(Semaphore::new(16)),
            store: Arc::new(Mutex::new(Some(store))),
        })
    }

    async fn run<T: Send + 'static>(
        &self,
        job: impl FnOnce(&mut Backend) -> Result<T, KeyError> + Send + 'static,
    ) -> Result<T, KeyError> {
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => KeyError::Closed,
                tokio::sync::TryAcquireError::NoPermits => KeyError::Busy,
            })?;
        // Move the guard into the worker: a dropped HTTP response cannot cancel
        // an in-flight write or let a later operation race it.
        let mut store = self.store.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            job(store.as_mut().ok_or(KeyError::Closed)?)
        })
        .await
        .map_err(|_| KeyError::Worker)?
    }

    pub(super) async fn close(&self) -> Result<(), Error> {
        self.permits.close();
        let store = self.store.lock().await.take();
        tokio::task::spawn_blocking(move || drop(store))
            .await
            .map_err(|_| Error::lifecycle("Relay key authority failed to close"))
    }

    pub(crate) async fn lookup(&self, secret: &str) -> Result<Option<Credential>, KeyError> {
        let Some(hash) = digest(secret) else {
            return Ok(None);
        };
        self.run(move |store| {
            let credential = match store {
                Backend::File(store) => store.lookup(&hash),
                #[cfg(feature = "storage-sqlite")]
                Backend::Sqlite(store) => store.lookup(&hash),
            }?;
            Ok(credential.filter(|found| found.key.enabled && !found.key.revoked))
        })
        .await
    }

    pub(crate) async fn list(&self, owner: &str) -> Result<Vec<KeyMetadata>, KeyError> {
        let owner = owner.to_owned();
        self.run(move |store| {
            let mut keys = match store {
                Backend::File(store) => store.list(&owner),
                #[cfg(feature = "storage-sqlite")]
                Backend::Sqlite(store) => store.list(&owner),
            }?;
            keys.sort_unstable_by(|a, b| (b.created_at_ms, &b.id).cmp(&(a.created_at_ms, &a.id)));
            Ok(keys)
        })
        .await
    }

    pub(crate) async fn create(
        &self,
        owner: &str,
        request: CreateKey,
    ) -> Result<CreatedKey, KeyError> {
        let owner = owner.to_owned();
        self.run(move |store| {
            let mut random = [0u8; SECRET_BYTES];
            getrandom::fill(&mut random).map_err(|_| KeyError::Random)?;
            let secret = format!("{KEY_PREFIX}{}", URL_SAFE_NO_PAD.encode(random));
            let hash: [u8; 32] = Sha256::digest(secret.as_bytes()).into();
            let key = KeyMetadata {
                id: uuid::Uuid::new_v4().to_string(),
                name: request.name.unwrap_or_else(|| "Relay key".into()),
                key_prefix: secret[..12].to_owned(),
                enabled: request.enabled.unwrap_or(true),
                revoked: false,
                allowed_providers: request.allowed_providers,
                default_model_selection: request.default_model_selection,
                created_at_ms: super::now(),
                last_used_at_ms: None,
                request_count: 0,
                revision: 1,
            };
            validate_metadata(&key)?;
            let credential = Credential { owner, key };
            match store {
                Backend::File(store) => store.create(&hash, &credential),
                #[cfg(feature = "storage-sqlite")]
                Backend::Sqlite(store) => store.create(&hash, &credential),
            }?;
            Ok(CreatedKey {
                key: credential.key,
                secret,
            })
        })
        .await
    }

    pub(crate) async fn update(
        &self,
        owner: &str,
        id: &str,
        update: UpdateKey,
    ) -> Result<KeyMetadata, KeyError> {
        self.change(owner, id, Change::Update(update)).await
    }

    pub(crate) async fn revoke(
        &self,
        owner: &str,
        id: &str,
        expected_revision: u64,
    ) -> Result<KeyMetadata, KeyError> {
        self.change(owner, id, Change::Revoke(expected_revision))
            .await
    }

    /// Record a non-revision-bumping usage telemetry update for a key.
    pub(crate) async fn record_use(
        &self,
        owner: &str,
        id: &str,
        used_at_ms: u64,
    ) -> Result<(), KeyError> {
        let (owner, id) = (owner.to_owned(), id.to_owned());
        self.run(move |store| match store {
            Backend::File(store) => store.record_use(&owner, &id, used_at_ms),
            #[cfg(feature = "storage-sqlite")]
            Backend::Sqlite(store) => store.record_use(&owner, &id, used_at_ms),
        })
        .await
    }

    async fn change(&self, owner: &str, id: &str, change: Change) -> Result<KeyMetadata, KeyError> {
        if uuid::Uuid::parse_str(id)
            .ok()
            .is_none_or(|value| value.to_string() != id)
        {
            return Err(KeyError::NotFound);
        }
        let (owner, id) = (owner.to_owned(), id.to_owned());
        self.run(move |store| match store {
            Backend::File(store) => store.change(&owner, &id, change),
            #[cfg(feature = "storage-sqlite")]
            Backend::Sqlite(store) => store.change(&owner, &id, change),
        })
        .await
    }
}
