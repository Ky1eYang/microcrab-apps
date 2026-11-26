//! Account-scoped user preferences with optimistic revision checks.
//! Covers console defaults, relay default selection, default conversation flavor,
//! and cross-device synced theme/locale preferences.
use super::{Manager, dashboard::Range, providers};
use microcrab_session::Error;
use microcrab_storage::{ModelSelection, configuration::Configuration};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

const KIND: &str = "user_preferences";
const TARGET: &str = "default";
const MAX_CLIENT_REVISION: u64 = (1 << 53) - 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConversationFlavor {
    ChatOnly,
    BrowserSandbox,
    Project,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ThemePreference {
    System,
    Light,
    Dark,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) enum LocalePreference {
    #[serde(rename = "system")]
    System,
    #[serde(rename = "zh-CN")]
    ZhCn,
    #[serde(rename = "en")]
    En,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PreferencesError {
    #[error("{0}")]
    Invalid(&'static str),
    #[error("Account preferences changed; read the latest revision before saving")]
    Conflict,
    #[error("Account preferences could not be acknowledged")]
    Mutation(Error),
    #[error("Account preferences storage is unavailable")]
    Read(Error),
    #[error("Stored account preferences cannot be decoded")]
    Schema(serde_json::Error),
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Preferences {
    pub dashboard_default_range: Range,
    #[serde(deserialize_with = "nullable")]
    pub low_cache_hit_warn_pct: Option<u8>,
    #[serde(deserialize_with = "nullable")]
    pub relay_default_selection: Option<ModelSelection>,
    #[serde(deserialize_with = "nullable")]
    pub default_conversation_flavor: Option<ConversationFlavor>,
    #[serde(deserialize_with = "nullable")]
    pub theme: Option<ThemePreference>,
    #[serde(deserialize_with = "nullable")]
    pub locale: Option<LocalePreference>,
}

fn nullable<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(d)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    expected_revision: u64,
    operation_id: Option<Uuid>,
    dashboard_default_range: Range,
    #[serde(deserialize_with = "nullable")]
    low_cache_hit_warn_pct: Option<u8>,
    #[serde(deserialize_with = "nullable")]
    relay_default_selection: Option<ModelSelection>,
    #[serde(default, deserialize_with = "nullable")]
    default_conversation_flavor: Option<ConversationFlavor>,
    #[serde(default, deserialize_with = "nullable")]
    theme: Option<ThemePreference>,
    #[serde(default, deserialize_with = "nullable")]
    locale: Option<LocalePreference>,
}

pub(crate) struct PreferencesUpdate {
    expected_revision: u64,
    operation_id: Option<Uuid>,
    values: Preferences,
}

impl PreferencesUpdate {
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let input: Request = serde_json::from_slice(bytes)?;
        Ok(Self {
            expected_revision: input.expected_revision,
            operation_id: input.operation_id,
            values: Preferences {
                dashboard_default_range: input.dashboard_default_range,
                low_cache_hit_warn_pct: input.low_cache_hit_warn_pct,
                relay_default_selection: input.relay_default_selection,
                default_conversation_flavor: input.default_conversation_flavor,
                theme: input.theme,
                locale: input.locale,
            },
        })
    }
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Document {
    values: Preferences,
    operation_id: Option<Uuid>,
}

#[derive(Serialize)]
pub(crate) struct PreferencesSnapshot {
    revision: u64,
    #[serde(flatten)]
    pub values: Preferences,
    last_operation_id: Option<Uuid>,
}

impl Manager {
    pub(crate) async fn preferences(
        &self,
        owner: &str,
    ) -> Result<PreferencesSnapshot, PreferencesError> {
        load(self.configuration.as_ref(), owner).await
    }

    pub(crate) async fn preference_relay_models(
        &self,
        owner: &str,
    ) -> Result<Vec<ModelSelection>, PreferencesError> {
        providers::relay_candidates(self, owner, None)
            .await
            .map(|items| items.into_iter().map(|c| c.selection).collect())
            .map_err(|_| PreferencesError::Invalid("Relay model catalog is unavailable"))
    }

    pub(crate) async fn update_preferences(
        &self,
        owner: &str,
        input: PreferencesUpdate,
    ) -> Result<PreferencesSnapshot, PreferencesError> {
        let available = if input.values.relay_default_selection.is_some() {
            self.preference_relay_models(owner).await?
        } else {
            Vec::new()
        };
        save(self.configuration.as_ref(), owner, input, &available).await
    }
}

async fn load(
    storage: &dyn Configuration,
    owner: &str,
) -> Result<PreferencesSnapshot, PreferencesError> {
    let row = storage
        .get(owner, KIND, TARGET)
        .await
        .map_err(PreferencesError::Read)?;
    let revision = row.as_ref().map_or(0, |row| row.revision);
    let document: Document = row
        .and_then(|row| row.value)
        .map(serde_json::from_value)
        .transpose()
        .map_err(PreferencesError::Schema)?
        .unwrap_or_default();
    Ok(PreferencesSnapshot {
        revision,
        values: document.values,
        last_operation_id: document.operation_id,
    })
}

async fn save(
    storage: &dyn Configuration,
    owner: &str,
    input: PreferencesUpdate,
    available: &[ModelSelection],
) -> Result<PreferencesSnapshot, PreferencesError> {
    if input.expected_revision >= MAX_CLIENT_REVISION {
        return Err(PreferencesError::Invalid(
            "expected_revision must permit a safe next revision",
        ));
    }
    if input
        .values
        .low_cache_hit_warn_pct
        .is_some_and(|p| !(1..=99).contains(&p))
    {
        return Err(PreferencesError::Invalid(
            "Cache threshold must be between 1 and 99",
        ));
    }
    let mut values = input.values;
    if let Some(selection) = values.relay_default_selection.as_mut() {
        if selection.variant.as_deref() == Some("default") {
            selection.variant = None;
        }
        if !available.contains(selection) {
            // Preserve an unchanged retired/disabled route on unrelated edits,
            // but never allow adopting an unavailable route as a new selection.
            let current = load(storage, owner).await?;
            if current.revision != input.expected_revision {
                return Err(PreferencesError::Conflict);
            }
            if current.values.relay_default_selection.as_ref() != Some(selection) {
                return Err(PreferencesError::Invalid(
                    "Default relay model must be an active relay-eligible provider model",
                ));
            }
        }
    }
    let document = Document {
        values,
        operation_id: input.operation_id,
    };
    let value = serde_json::to_value(&document).map_err(PreferencesError::Schema)?;
    let row = storage
        .compare_exchange(owner, KIND, TARGET, input.expected_revision, Some(value))
        .await
        .map_err(|error| match &error {
            Error::Protocol(reason)
                if reason.public_reason()
                    == Some("configuration revision conflict; reload before saving") =>
            {
                PreferencesError::Conflict
            }
            _ => PreferencesError::Mutation(error),
        })?;
    Ok(PreferencesSnapshot {
        revision: row.revision,
        values: document.values,
        last_operation_id: document.operation_id,
    })
}

#[cfg(test)]
mod tests;
