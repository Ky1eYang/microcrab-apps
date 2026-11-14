//! Workspace project authority: Project = Host + Directory (workspace).
//!
//! Projects establish durable execution identity, access boundaries and workspace mappings.
pub(crate) mod registry;
pub(crate) mod types;

pub(crate) use registry::ProjectRegistry;
pub(crate) use types::ProjectRecord;

/// Test fixtures seed the real durable registry before Manager construction.
#[cfg(test)]
pub(crate) fn seed_test_projects(data_dir: &std::path::Path, records: serde_json::Value) {
    let records: Vec<ProjectRecord> = serde_json::from_value(records).unwrap();
    std::fs::create_dir_all(data_dir).unwrap();
    std::fs::write(
        data_dir.join("projects.json"),
        serde_json::to_vec(&records).unwrap(),
    )
    .unwrap();
}
