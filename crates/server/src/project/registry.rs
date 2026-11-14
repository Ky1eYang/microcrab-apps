//! Durable project metadata. Readers never wait for disk IO; mutations publish
//! only after serialized atomic persistence succeeds.
use super::types::ProjectRecord;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use tokio::sync::Mutex;

#[derive(Clone)]
pub(crate) struct ProjectRegistry {
    path: PathBuf,
    records: Arc<RwLock<Vec<ProjectRecord>>>,
    writer: Arc<Mutex<()>>,
}
impl ProjectRegistry {
    pub(crate) fn new(data_dir: &Path) -> Result<Self, std::io::Error> {
        let path = data_dir.join("projects.json");
        let records = match std::fs::read(&path) {
            Ok(content) => serde_json::from_slice(&content)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error),
        };
        Ok(Self {
            path,
            records: Arc::new(RwLock::new(records)),
            writer: Arc::new(Mutex::new(())),
        })
    }
    fn snapshot(&self) -> Vec<ProjectRecord> {
        self.records
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    async fn publish(&self, records: Vec<ProjectRecord>) -> Result<(), String> {
        let content = serde_json::to_vec_pretty(&records).map_err(|e| e.to_string())?;
        let parent = self.path.parent().ok_or("Project registry has no parent")?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| e.to_string())?;
        let temporary = parent.join(format!(".projects-{}.tmp", uuid::Uuid::new_v4()));
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .await
            .map_err(|e| e.to_string())?;
        use tokio::io::AsyncWriteExt;
        file.write_all(&content).await.map_err(|e| e.to_string())?;
        file.sync_all().await.map_err(|e| e.to_string())?;
        drop(file);
        tokio::fs::rename(&temporary, &self.path)
            .await
            .map_err(|e| e.to_string())?;
        *self.records.write().unwrap_or_else(|e| e.into_inner()) = records;
        Ok(())
    }
    pub(crate) async fn list(&self, owner: &str) -> Vec<ProjectRecord> {
        self.snapshot()
            .into_iter()
            .filter(|r| r.allows(owner))
            .collect()
    }
    pub(crate) fn find_sync(&self, id: &str) -> Option<ProjectRecord> {
        self.records
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .find(|r| r.id == id)
            .cloned()
    }
    pub(crate) async fn find(&self, id: &str) -> Option<ProjectRecord> {
        self.find_sync(id)
    }
    pub(crate) async fn create(
        &self,
        owner: &str,
        mut record: ProjectRecord,
    ) -> Result<ProjectRecord, String> {
        let _writer = self.writer.lock().await;
        let mut records = self.snapshot();
        if records.iter().any(|r| r.id == record.id) {
            return Err("Project already exists".into());
        }
        if record.host_id.is_some()
            && records
                .iter()
                .any(|r| r.allows(owner) && r.host_id == record.host_id && r.root == record.root)
        {
            return Err("This Host and workspace already belong to a project".into());
        }
        if !record.allows(owner) {
            record.owners.push(owner.to_owned());
        }
        records.push(record.clone());
        self.publish(records).await?;
        Ok(record)
    }
    pub(crate) async fn rename(
        &self,
        owner: &str,
        id: &str,
        name: String,
    ) -> Result<ProjectRecord, String> {
        let _writer = self.writer.lock().await;
        let mut records = self.snapshot();
        let record = records
            .iter_mut()
            .find(|r| r.id == id && r.allows(owner))
            .ok_or("Project not found or not permitted")?;
        record.name = name;
        let renamed = record.clone();
        self.publish(records).await?;
        Ok(renamed)
    }
    pub(crate) async fn finalize(&self, owner: &str, id: &str) -> Result<(), String> {
        let _writer = self.writer.lock().await;
        let mut records = self.snapshot();
        let record = records
            .iter_mut()
            .find(|r| r.id == id && r.allows(owner))
            .ok_or("Project not found or not permitted")?;
        if !record.provisional {
            return Ok(());
        }
        record.provisional = false;
        self.publish(records).await
    }
    pub(crate) async fn remove_provisional(&self, owner: &str, id: &str) -> Result<(), String> {
        let _writer = self.writer.lock().await;
        let mut records = self.snapshot();
        let index = records
            .iter()
            .position(|r| r.id == id && r.allows(owner) && r.provisional)
            .ok_or("Project is not removable provisional metadata")?;
        records.remove(index);
        self.publish(records).await
    }
    pub(crate) async fn delete(&self, owner: &str, id: &str) -> Result<(), String> {
        let _writer = self.writer.lock().await;
        let mut records = self.snapshot();
        let index = records
            .iter()
            .position(|r| r.id == id && r.allows(owner))
            .ok_or("Project not found or not permitted")?;
        records[index].archived = true;
        self.publish(records).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(id: &str) -> ProjectRecord {
        ProjectRecord {
            id: id.into(),
            name: "Display name".into(),
            root: "/fixed".into(),
            host_id: Some(format!("host-{id}")),
            workspace_id: Some("workspace".into()),
            owners: vec!["owner".into()],
            created_at: 1,
            archived: false,
            provisional: false,
        }
    }
    #[tokio::test]
    async fn concurrent_metadata_writes_preserve_identity_and_owner_access() {
        let root = tempfile::tempdir().unwrap();
        let registry = ProjectRegistry::new(root.path()).unwrap();
        registry.create("owner", record("a")).await.unwrap();
        let writer = registry.writer.lock().await;
        assert!(
            registry
                .find_sync("a")
                .is_some_and(|record| record.allows("owner"))
        );
        assert!(
            !registry
                .find_sync("a")
                .is_some_and(|record| record.allows("other"))
        );
        drop(writer);
        let (a, b) = tokio::join!(
            registry.rename("owner", "a", "Custom title".into()),
            registry.create("owner", record("b"))
        );
        assert_eq!(a.unwrap().root, "/fixed");
        b.unwrap();
        let reopened = ProjectRegistry::new(root.path()).unwrap();
        assert_eq!(reopened.list("owner").await.len(), 2);
        assert_eq!(reopened.find("a").await.unwrap().name, "Custom title");
        assert!(registry.delete("other", "a").await.is_err());
        registry.delete("owner", "a").await.unwrap();
        assert!(
            !ProjectRegistry::new(root.path())
                .unwrap()
                .find_sync("a")
                .is_some_and(|record| record.allows("owner"))
        );
    }
    #[test]
    fn corrupt_registry_fails_instead_of_erasing_projects() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("projects.json"), b"invalid").unwrap();
        assert!(ProjectRegistry::new(root.path()).is_err());
    }
}
