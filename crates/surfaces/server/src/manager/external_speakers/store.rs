//! One owner-scoped CAS record makes project edits and all sparse layers atomic on both backends.
use super::model::Configuration;
use microcrab_protocol::application::WireError;
use microcrab_storage::configuration::Configuration as Storage;

const KIND: &str = "external_speakers";
const TARGET: &str = "resources";
pub(super) fn conflict() -> WireError {
    WireError::new(
        "revision_conflict",
        "External speakers changed; reload before saving",
    )
}
fn error(error: microcrab_session::Error) -> WireError {
    if matches!(&error, microcrab_session::Error::Protocol(reason) if reason.public_reason() == Some("configuration revision conflict; reload before saving"))
    {
        conflict()
    } else {
        super::super::safe(error)
    }
}
fn publication_error(source: microcrab_session::Error, operation_id: &str) -> WireError {
    let cause = error(source);
    if cause.code == "revision_conflict" {
        return cause;
    }
    let mut uncertain = WireError::new(
        "outcome_unknown",
        format!(
            "External speaker publication outcome requires receipt observation: {}",
            cause.message
        ),
    );
    uncertain.details = Some(Box::new(serde_json::json!({
        "operation_id": operation_id,
        "cause": cause,
        "reconciliation": "observe_external_speaker_operation"
    })));
    uncertain
}
pub(super) async fn load(
    storage: &dyn Storage,
    owner: &str,
) -> Result<(u64, Configuration), WireError> {
    let record = storage.get(owner, KIND, TARGET).await.map_err(error)?;
    let revision = record.as_ref().map_or(0, |record| record.revision);
    let config: Configuration = record
        .and_then(|record| record.value)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| {
            WireError::new(
                "configuration_unavailable",
                "Stored external speaker schema cannot be decoded",
            )
        })?
        .unwrap_or_default();
    config.validate()?;
    Ok((revision, config))
}
pub(super) async fn save(
    storage: &dyn Storage,
    owner: &str,
    revision: u64,
    config: &Configuration,
    operation_id: &str,
) -> Result<u64, WireError> {
    let value = configuration_value(config)?;
    Ok(storage
        .compare_exchange(owner, KIND, TARGET, revision, Some(value))
        .await
        .map_err(|source| publication_error(source, operation_id))?
        .revision)
}

/// Internal retirement has no management receipt; repeat after an uncertain
/// result by rereading under the owner fence and checking the exact scope.
pub(super) async fn retire_project(
    storage: &dyn Storage,
    owner: &str,
    project: &str,
) -> Result<(), WireError> {
    let (revision, mut config) = load(storage, owner).await?;
    if config.remove_project(project) {
        storage
            .compare_exchange(
                owner,
                KIND,
                TARGET,
                revision,
                Some(configuration_value(&config)?),
            )
            .await
            .map_err(|source| {
                let mut cause = error(source);
                if cause.code != "revision_conflict" {
                    cause.code = "outcome_unknown".into();
                }
                cause
            })?;
    }
    Ok(())
}

fn configuration_value(config: &Configuration) -> Result<serde_json::Value, WireError> {
    let value = serde_json::to_value(config).map_err(|_| {
        WireError::new(
            "serialization_error",
            "Cannot encode external speaker resource",
        )
    })?;
    if serde_json::to_vec(&value)
        .map_err(|_| super::invalid("Cannot measure external speaker resource"))?
        .len()
        > microcrab_storage::configuration::MAX_CONFIGURATION_BYTES
    {
        return Err(WireError::new(
            "capacity",
            "External speaker configuration and retained receipts exceed the 1 MiB durable resource budget",
        ));
    }
    Ok(value)
}

/// Caller holds the owner speaker gate and has resolved the exact Session/project.
/// Return only a witnessed credential that was already stored or whose CAS succeeded.
/// On a cross-manager conflict or uncertain publication, fail closed for this call;
/// fresh authentication must reload durable state before making another decision.
pub(super) async fn authenticate_witnessed(
    storage: &dyn Storage,
    owner: &str,
    project: Option<&str>,
    session: &str,
    bearer: &str,
    allow_migration: bool,
) -> Result<Option<super::model::SpeakerCredential>, WireError> {
    let (revision, mut config) = load(storage, owner).await?;
    let Some((credential, migrated)) =
        config.authenticate_witnessed(project, session, bearer, allow_migration)?
    else {
        return Ok(None);
    };
    if migrated {
        config.validate()?;
        let value = configuration_value(&config)?;
        storage
            .compare_exchange(owner, KIND, TARGET, revision, Some(value))
            .await
            .map_err(|_| {
                WireError::new(
                    "storage_unavailable",
                    "Cannot confirm durable external speaker credential migration",
                )
            })?;
    }
    Ok(Some(credential))
}
