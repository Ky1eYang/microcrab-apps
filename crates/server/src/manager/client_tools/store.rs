//! Bounded per-session durable file, shared by every Journal storage mode.
//! Replacement is written and synced before rename; memory advances only after
//! commit, so a persistence failure never acknowledges an unrecorded result.
use super::model::{Catalog, Durable, MAX_STORE_BYTES, bounded, identifier, invalid};
use microcrab_session::Error;
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    path::{Path, PathBuf},
};

pub(super) struct Store {
    path: PathBuf,
}
impl Store {
    pub fn open(root: &Path, session_id: &str) -> Result<(Self, Durable), Error> {
        identifier(session_id)?;
        let sessions = root.join("sessions");
        reject_symlink(&sessions)?;
        let directory = sessions.join(session_id);
        reject_symlink(&directory)?;
        std::fs::create_dir_all(&directory)?;
        let store = Self {
            path: directory.join("client_tools.json"),
        };
        reject_symlink(&store.path)?;
        let durable = match std::fs::File::open(&store.path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(MAX_STORE_BYTES as u64 + 1)
                    .read_to_end(&mut bytes)?;
                if bytes.len() > MAX_STORE_BYTES {
                    return Err(invalid("client tool store exceeds size limit"));
                }
                decode(&bytes)?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Durable::default(),
            Err(error) => return Err(error.into()),
        };
        Ok((store, durable))
    }
    pub fn commit(&self, durable: &Durable) -> Result<(), Error> {
        let bytes = bounded(durable, MAX_STORE_BYTES)?;
        // A fixed temporary name is safe: one bridge is owned by one Manager
        // per session, and its state mutex serializes every transaction.
        let temporary = self.path.with_extension("json.tmp");
        reject_symlink(&self.path)?;
        reject_symlink(&temporary)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, &self.path)?;
        Ok(())
    }
}

/// Decode only the current catalog schema. Historical failures leave the entire
/// per-session store untouched; callers isolate that Session rather than dropping
/// a catalog, replaying a call, or acknowledging replacement bytes.
fn decode(bytes: &[u8]) -> Result<Durable, Error> {
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|error| Error::Config(format!("client_tools.json: {error}").into()))?;
    for field in ["configured", "staged"] {
        if let Some(catalog) = value.get(field).filter(|v| !v.is_null()) {
            decode_catalog(catalog, field)?;
        }
    }
    if let Some(catalogs) = value.get("catalogs").and_then(|v| v.as_array()) {
        for (index, catalog) in catalogs.iter().enumerate() {
            decode_catalog(catalog, &format!("catalogs[{index}]"))?;
        }
    }
    serde_json::from_value(value)
        .map_err(|error| Error::Config(format!("client_tools.json: {error}").into()))
}
fn decode_catalog(value: &serde_json::Value, location: &str) -> Result<(), Error> {
    // Decode individual rules first so unknown legacy fields carry their exact
    // frozen location instead of a blanket catalog/store error.
    if let Some(definitions) = value.get("definitions").and_then(|v| v.as_array()) {
        for (index, definition) in definitions.iter().enumerate() {
            if let Some(permissions) = definition.get("permissions").and_then(|v| v.as_array()) {
                for (rule_index, permission) in permissions.iter().enumerate() {
                    serde_json::from_value::<microcrab_permission::Permission>(permission.clone())
                        .map_err(|error| Error::Config(format!(
                            "client_tools.json.{location}.definitions[{index}].permissions[{rule_index}]: {error}"
                        ).into()))?;
                }
            }
        }
    }
    let catalog: Catalog = serde_json::from_value(value.clone())
        .map_err(|error| Error::Config(format!("client_tools.json.{location}: {error}").into()))?;
    super::model::validate_catalog(&catalog.definitions).map_err(|error| {
        Error::Config(format!("client_tools.json.{location}.definitions: {error}").into())
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn legacy_store() -> Vec<u8> {
        serde_json::to_vec(&json!({
            "catalogs": [{"epoch": 0, "digest": "a".repeat(64), "definitions": [{
                "name": "lookup", "description": "lookup", "parameters": {},
                "permissions": [{"capability": "tool_invoke", "action": "allow"}]
            }]}], "calls": []
        }))
        .unwrap()
    }

    #[test]
    fn legacy_catalog_error_names_the_exact_rule_and_field() {
        let error = decode(&legacy_store()).unwrap_err().to_string();
        assert!(error.contains("catalogs[0].definitions[0].permissions[0]"));
        assert!(error.contains("capability"));
        assert!(decode(br#"{"catalogs":[],"calls":[]}"#).is_ok());
    }

    #[test]
    fn incompatible_session_store_is_not_rewritten_and_does_not_poison_neighbor() {
        let root = std::env::temp_dir().join(format!(
            "microcrab-client-tools-history-{}",
            uuid::Uuid::new_v4()
        ));
        let directory = root.join("sessions").join("old-session");
        std::fs::create_dir_all(&directory).unwrap();
        let bytes = legacy_store();
        let path = directory.join("client_tools.json");
        std::fs::write(&path, &bytes).unwrap();
        assert!(Store::open(&root, "old-session").is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert!(Store::open(&root, "healthy-session").is_ok());
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn reject_symlink(path: &Path) -> Result<(), Error> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(invalid(
            "client tool storage paths cannot be symbolic links",
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}
