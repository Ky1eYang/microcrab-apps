//! Private resume-capability storage. File publication is atomic and serialized
//! independently from Project connection gates; no execution or socket IO uses it.
use super::*;

impl Manager {
    async fn read_direct_credentials(&self) -> Result<HashMap<String, Credential>, WireError> {
        let path = self.config.data_dir.join("host-connections.json");
        let bytes = match tokio::fs::read(path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
            Err(error) => {
                return Err(WireError::new(
                    "host_credentials_unavailable",
                    format!("Cannot read native Host resume storage: {:?}", error.kind()),
                ));
            }
        };
        if bytes.len() > 1024 * 1024 {
            return Err(WireError::new(
                "host_credentials_unavailable",
                "Native Host resume storage exceeds 1 MiB",
            ));
        }
        serde_json::from_slice(&bytes).map_err(|_| {
            WireError::new(
                "host_credentials_unavailable",
                "Native Host resume storage does not match its schema",
            )
        })
    }
    pub(super) async fn load_direct_credential(
        &self,
        project: &str,
    ) -> Result<Option<Credential>, WireError> {
        let _serial = self.direct_hosts.persistence.lock().await;
        Ok(self.read_direct_credentials().await?.remove(project))
    }
    pub(super) async fn save_direct_credential(
        &self,
        project: &str,
        record: Option<Credential>,
    ) -> Result<(), WireError> {
        let _serial = self.direct_hosts.persistence.lock().await;
        let mut records = self.read_direct_credentials().await?;
        if let Some(record) = record {
            records.insert(project.into(), record);
        } else {
            records.remove(project);
        }
        let bytes = serde_json::to_vec(&records)
            .map_err(|_| failure("Cannot encode native Host resume storage"))?;
        if bytes.len() > 1024 * 1024 {
            return Err(failure("Native Host resume storage capacity reached"));
        }
        let temporary = self
            .config
            .data_dir
            .join(format!("host-connections-{}.tmp", uuid::Uuid::new_v4()));
        tokio::fs::write(&temporary, bytes).await.map_err(|error| {
            WireError::new(
                "host_credentials_unavailable",
                format!(
                    "Cannot write native Host resume storage: {:?}",
                    error.kind()
                ),
            )
        })?;
        tokio::fs::rename(
            &temporary,
            self.config.data_dir.join("host-connections.json"),
        )
        .await
        .map_err(|error| {
            WireError::new(
                "host_credentials_unavailable",
                format!(
                    "Cannot publish native Host resume storage: {:?}",
                    error.kind()
                ),
            )
        })
    }
}
