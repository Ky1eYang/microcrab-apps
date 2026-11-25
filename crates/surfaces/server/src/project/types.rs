//! Project domain models representing durable Host + Directory (workspace) context.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectRecord {
    pub id: String,
    pub name: String,
    pub root: String,
    #[serde(default)]
    pub host_id: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub owners: Vec<String>,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub archived: bool,
    /// Unauthenticated creation metadata; only canonical bridge attach finalizes it.
    #[serde(default)]
    pub provisional: bool,
}

impl ProjectRecord {
    pub fn allows(&self, owner: &str) -> bool {
        !self.archived
            && self
                .owners
                .iter()
                .any(|entry| entry == "*" || entry == owner)
    }
}
