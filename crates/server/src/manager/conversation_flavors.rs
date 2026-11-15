//! Client-owned immutable conversation flavor, bound to the complete Core grant.
//! Both file and SQLite deployments already keep execution identities on disk.
use microcrab_session::Error;
use microcrab_storage::execution::ConversationGrant;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostFlavor {
    ChatOnly,
    BrowserSandbox,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pin {
    grant: ConversationGrant,
    host_flavor: HostFlavor,
}
pub(super) struct Flavors {
    root: PathBuf,
    pins: Mutex<HashMap<String, Pin>>,
}
impl Flavors {
    pub(super) fn open(data: &Path) -> Result<Self, Error> {
        let root = data.join("conversation-flavors");
        let mut pins = HashMap::new();
        if root.try_exists()? {
            directory(&root)?;
            for entry in std::fs::read_dir(&root)? {
                let path = entry?.path();
                if path.extension().is_some_and(|ext| ext == "json") {
                    let pin: Pin = read(&path)?;
                    uuid::Uuid::parse_str(&pin.grant.conversation_id)
                        .map_err(|_| Error::config("invalid conversation flavor identity"))?;
                    if path.file_stem().and_then(|v| v.to_str())
                        != Some(pin.grant.conversation_id.as_str())
                    {
                        return Err(Error::config("invalid conversation flavor pin"));
                    }
                    pins.insert(pin.grant.conversation_id.clone(), pin);
                }
            }
        }
        // Only an actual browser grant is historical browser evidence. A
        // projectless delegated/one-shot Session alone is not such evidence.
        let execution = data.join("execution");
        if execution.try_exists()? {
            directory(&execution)?;
            for entry in std::fs::read_dir(execution)? {
                let path = entry?.path();
                if path
                    .file_name()
                    .and_then(|v| v.to_str())
                    .is_some_and(|v| v.starts_with("conversation-grant-") && v.ends_with(".json"))
                {
                    let grant: ConversationGrant = read(&path)?;
                    if let Some(pin) = pins.get(&grant.conversation_id) {
                        if pin.grant != grant {
                            return Err(Error::config("conversation flavor grant differs"));
                        }
                    } else {
                        pins.insert(
                            grant.conversation_id.clone(),
                            Pin {
                                grant,
                                host_flavor: HostFlavor::BrowserSandbox,
                            },
                        );
                    }
                }
            }
        }
        Ok(Self {
            root,
            pins: Mutex::new(pins),
        })
    }
    pub(super) fn get(&self, owner: &str, id: &str) -> Option<HostFlavor> {
        self.pins
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .filter(|p| p.grant.owner == owner)
            .map(|p| p.host_flavor)
    }
    pub(super) fn resolve(&self, grant: &ConversationGrant) -> Result<HostFlavor, Error> {
        let path = self.root.join(format!("{}.json", grant.conversation_id));
        let current = if path.try_exists()? {
            let pin: Pin = read(&path)?;
            if pin.grant != *grant {
                return Err(Error::config("conversation flavor grant differs"));
            }
            pin
        } else {
            Pin {
                grant: grant.clone(),
                host_flavor: HostFlavor::BrowserSandbox,
            }
        };
        let flavor = current.host_flavor;
        self.pins
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(grant.conversation_id.clone(), current);
        Ok(flavor)
    }
    /// Reuse a staged grant after a crash between flavor and Core grant
    /// publication, retaining the original binding ID instead of creating a new one.
    pub(super) fn grant(&self, id: &str) -> Option<ConversationGrant> {
        self.pins
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .map(|p| p.grant.clone())
    }
    /// Caller holds Manager's identity gate. Atomic complete-byte publication
    /// never replaces a pin, including across processes. No IO lock crosses await.
    pub(super) fn pin(
        &self,
        grant: ConversationGrant,
        host_flavor: HostFlavor,
    ) -> Result<(), Error> {
        uuid::Uuid::parse_str(&grant.conversation_id)
            .map_err(|_| Error::config("invalid conversation identity"))?;
        let value = Pin { grant, host_flavor };
        let id = &value.grant.conversation_id;
        let mut pins = self.pins.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(old) = pins.get(id) {
            if old != &value {
                return Err(Error::config("conversation flavor is immutable"));
            }
        }
        std::fs::create_dir_all(&self.root)?;
        directory(&self.root)?;
        let target = self.root.join(format!("{id}.json"));
        let staging = self.root.join(format!("pending-{}", uuid::Uuid::new_v4()));
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&staging)?;
        let written = (|| -> Result<(), Error> {
            file.write_all(&serde_json::to_vec(&value)?)?;
            file.sync_all()?;
            Ok(())
        })();
        drop(file);
        if let Err(error) = written {
            std::fs::remove_file(&staging)?;
            return Err(error);
        }
        let published = std::fs::hard_link(&staging, &target);
        std::fs::remove_file(staging)?;
        match published {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let current = read::<Pin>(&target)?;
                if current != value {
                    pins.insert(id.clone(), current);
                    return Err(Error::config("conversation flavor is immutable"));
                }
            }
            Err(e) => return Err(e.into()),
        }
        pins.insert(id.clone(), value);
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn grant() -> ConversationGrant {
        ConversationGrant {
            conversation_id: uuid::Uuid::new_v4().to_string(),
            browser_id: uuid::Uuid::new_v4().to_string(),
            workspace_id: uuid::Uuid::new_v4().to_string(),
            owner: "owner".into(),
            backend_id: "backend".into(),
            binding_id: "binding".into(),
        }
    }
    #[test]
    fn staged_grant_survives_crash_without_becoming_legacy_sandbox() {
        let root = tempfile::tempdir().unwrap();
        let store = Flavors::open(root.path()).unwrap();
        let grant = grant();
        store.pin(grant.clone(), HostFlavor::ChatOnly).unwrap();
        // Deliberately no Core grant: this is the crash boundary.
        let restarted = Flavors::open(root.path()).unwrap();
        assert_eq!(restarted.grant(&grant.conversation_id), Some(grant.clone()));
        assert_eq!(
            restarted.get("owner", &grant.conversation_id),
            Some(HostFlavor::ChatOnly)
        );
        assert!(
            restarted
                .pin(grant.clone(), HostFlavor::BrowserSandbox)
                .is_err()
        );
        let mut changed = grant.clone();
        changed.workspace_id = uuid::Uuid::new_v4().to_string();
        assert!(restarted.pin(changed, HostFlavor::ChatOnly).is_err());
        let mut changed = grant.clone();
        changed.owner = "other".into();
        assert!(restarted.pin(changed, HostFlavor::ChatOnly).is_err());
        assert_eq!(restarted.get("other", &grant.conversation_id), None);
        restarted.pin(grant, HostFlavor::ChatOnly).unwrap();
    }
    #[test]
    fn independent_writers_publish_one_complete_immutable_winner() {
        let root = tempfile::tempdir().unwrap();
        let a = Flavors::open(root.path()).unwrap();
        let b = Flavors::open(root.path()).unwrap();
        let grant = grant();
        let other = grant.clone();
        let gate = std::sync::Arc::new(std::sync::Barrier::new(2));
        let other_gate = gate.clone();
        let first = std::thread::spawn(move || {
            gate.wait();
            a.pin(grant, HostFlavor::ChatOnly)
        });
        let second = std::thread::spawn(move || {
            other_gate.wait();
            b.pin(other, HostFlavor::BrowserSandbox)
        });
        let outcomes = [
            first.join().unwrap().is_ok(),
            second.join().unwrap().is_ok(),
        ];
        assert_eq!(outcomes.into_iter().filter(|won| *won).count(), 1);
        let reopened = Flavors::open(root.path()).unwrap();
        assert_eq!(reopened.pins.lock().unwrap().len(), 1);
        assert!(
            std::fs::read_dir(root.path().join("conversation-flavors"))
                .unwrap()
                .all(|entry| entry
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|ext| ext == "json"))
        );
    }
    #[tokio::test]
    async fn only_actual_legacy_conversation_grants_classify_as_sandbox() {
        let root = tempfile::tempdir().unwrap();
        let grant = grant();
        microcrab_storage::execution::ExecutionPins::new(root.path().join("execution"))
            .pin_conversation_grant(grant.clone())
            .await
            .unwrap();
        let store = Flavors::open(root.path()).unwrap();
        assert_eq!(
            store.get("owner", &grant.conversation_id),
            Some(HostFlavor::BrowserSandbox)
        );
        assert_eq!(store.get("owner", "unrelated-projectless-session"), None);
        assert!(store.pin(grant, HostFlavor::ChatOnly).is_err());
    }
}
fn redirected(meta: &std::fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        meta.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        meta.file_type().is_symlink()
    }
}
fn directory(path: &Path) -> Result<(), Error> {
    let meta = std::fs::symlink_metadata(path)?;
    if redirected(&meta) || !meta.is_dir() {
        return Err(Error::config("invalid conversation identity directory"));
    }
    Ok(())
}
fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, Error> {
    let meta = std::fs::symlink_metadata(path)?;
    if redirected(&meta) || !meta.is_file() {
        return Err(Error::config("invalid conversation identity file"));
    }
    let mut bytes = vec![];
    std::fs::File::open(path)?
        .take(128 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 128 * 1024 {
        return Err(Error::config("conversation flavor exceeds budget"));
    }
    Ok(serde_json::from_slice(&bytes)?)
}
