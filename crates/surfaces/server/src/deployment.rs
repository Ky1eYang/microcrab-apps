//! One deployment owns persisted sessions and their startup Host namespace.
//! A namespace marker rejects old project-relative histories instead of applying
//! their snapshots to a different directory. The deployment lock covers admission.
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Error, ErrorKind, Write},
    path::{Path, PathBuf},
};
pub(crate) struct DeploymentGuard {
    _file: File,
}
#[derive(Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Namespace {
    version: u32,
    startup: PathBuf,
}
impl DeploymentGuard {
    pub(crate) fn acquire(directory: &Path, startup: &Path) -> Result<Self, Error> {
        let guard = Self::acquire_ownership(directory)?;
        pin_namespace(directory, startup)?;
        Ok(guard)
    }

    /// Direct Manager embeddings own their namespace policy, but may not share
    /// a data root concurrently with any other Manager or Server application.
    pub(crate) fn acquire_ownership(directory: &Path) -> Result<Self, Error> {
        std::fs::create_dir_all(directory)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join(".server.lock"))?;
        file.try_lock()
            .map_err(|_| Error::other("server data directory is already in use"))?;
        Ok(Self { _file: file })
    }
}
fn pin_namespace(directory: &Path, startup: &Path) -> Result<(), Error> {
    let expected = Namespace {
        version: 1,
        startup: startup.canonicalize()?,
    };
    let marker = directory.join(".host-namespace.json");
    match std::fs::read(&marker) {
        Ok(bytes) => {
            let actual: Namespace = serde_json::from_slice(&bytes)
                .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid Host namespace marker"))?;
            if actual != expected {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "persisted Host namespace differs from startup directory",
                ));
            }
            return Ok(());
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    for name in ["directory", "sessions"] {
        let path = directory.join(name);
        if path.exists() && std::fs::read_dir(path)?.next().is_some() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "legacy session storage requires explicit namespace migration",
            ));
        }
    }
    if directory.join("storage.sqlite").exists() {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "legacy SQLite storage requires explicit namespace migration",
        ));
    }
    // The deployment lock excludes another writer. Publish only a synced complete
    // marker; an interrupted pre-publication write leaves no admitted history.
    let temporary = directory.join(format!(".host-namespace-{}.tmp", uuid::Uuid::new_v4()));
    let published = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        let bytes = serde_json::to_vec(&expected).map_err(Error::other)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, &marker)
    })();
    if published.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    published
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deployment_lock_prevents_two_live_session_owners_and_releases_on_drop() {
        let directory = tempfile::tempdir().unwrap();
        let first = DeploymentGuard::acquire(directory.path(), directory.path()).unwrap();
        assert!(DeploymentGuard::acquire(directory.path(), directory.path()).is_err());
        drop(first);
        assert!(DeploymentGuard::acquire(directory.path(), directory.path()).is_ok());
    }
    #[test]
    fn persisted_namespace_cannot_be_reinterpreted_at_a_different_root() {
        let data = tempfile::tempdir().unwrap();
        let original = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        drop(DeploymentGuard::acquire(data.path(), original.path()).unwrap());
        std::fs::create_dir_all(data.path().join("sessions")).unwrap();
        std::fs::write(data.path().join("sessions/history"), b"retained").unwrap();
        assert!(DeploymentGuard::acquire(data.path(), other.path()).is_err());
        assert_eq!(
            std::fs::read(data.path().join("sessions/history")).unwrap(),
            b"retained"
        );
        assert!(DeploymentGuard::acquire(data.path(), original.path()).is_ok());
    }
    #[test]
    fn legacy_history_is_refused_before_creating_a_namespace_marker() {
        for sqlite in [false, true] {
            let data = tempfile::tempdir().unwrap();
            if sqlite {
                std::fs::write(data.path().join("storage.sqlite"), b"legacy").unwrap();
            } else {
                std::fs::create_dir_all(data.path().join("sessions")).unwrap();
                std::fs::write(data.path().join("sessions/history"), b"legacy").unwrap();
            }
            assert!(DeploymentGuard::acquire(data.path(), data.path()).is_err());
            assert!(!data.path().join(".host-namespace.json").exists());
        }
    }
}
