//! Serializable ordinary Session defaults; deployment handles are never writable.
use microcrab_permission::{Action, Permission, ReviewModelSelection};
use microcrab_session::{Limits, context::AutoCompactionPolicy};
use microcrab_storage::ModelSelection;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SettingsOverrides {
    pub limits: BTreeMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_selection: Option<ModelSelection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permissions: Option<Vec<Permission>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initialization: Option<Action>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub review: Option<bool>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nullable_override"
    )]
    pub review_model: Option<Option<ReviewModelSelection>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_compaction: Option<AutoCompactionPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_naming: Option<bool>,
}
pub fn default_limits() -> Limits {
    Limits {
        max_tool_output_bytes: 64 * 1024,
        ..Limits::default()
    }
}
/// Default administrator ceilings deliberately keep independent execution/process
/// capacities. Deployments can lower any ceiling in startup configuration.
pub fn hard_limits() -> Limits {
    Limits {
        max_rounds: 4096,
        max_tool_calls_per_round: 1024,
        max_context_bytes: 1024 * 1024 * 1024,
        max_tool_output_bytes: 16 * 1024 * 1024,
        max_output_tokens: 1_048_576,
        interaction_timeout_ms: 3_600_000,
        max_pending_executions: 1024,
        max_retained_executions: 4096,
        lifecycle_timeout_ms: 300_000,
        max_processes: 1024,
        max_execution_buffers: 1024,
        max_retained_processes: 4096,
        output_buffer_bytes: 64 * 1024 * 1024,
        sleep_max_ms: 3_600_000,
    }
}
pub fn limits(merged: &BTreeMap<String, u64>, ceilings: &Limits) -> Result<Limits, String> {
    let mut result =
        serde_json::to_value(default_limits()).map_err(|_| "cannot encode default limits")?;
    let ceiling =
        serde_json::to_value(ceilings).map_err(|_| "cannot encode administrator ceilings")?;
    for (key, value) in merged {
        let Some(max) = ceiling.get(key).and_then(serde_json::Value::as_u64) else {
            return Err(format!("unknown Limits field: {key}"));
        };
        if (*value == 0 && key != "max_retained_processes")
            || *value > max
            || (key == "sleep_max_ms" && *value > 3_600_000)
        {
            return Err(format!(
                "limits.{key} must be positive and at most {}",
                max.min(if key == "sleep_max_ms" {
                    3_600_000
                } else {
                    u64::MAX
                })
            ));
        }
        result[key] = (*value).into();
    }
    // Administrator ceilings also apply when inherited library defaults exceed them.
    for (key, value) in result.as_object_mut().ok_or("limits must be an object")? {
        let max = ceiling[key]
            .as_u64()
            .ok_or("invalid administrator limit ceiling")?;
        if max == 0 && key != "max_retained_processes" {
            return Err(format!("administrator ceiling {key} must be positive"));
        }
        if value.as_u64().is_some_and(|value| value > max) {
            *value = max.into();
        }
    }
    serde_json::from_value(result).map_err(|_| "Limits value exceeds platform capacity".into())
}
fn nullable_override<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<ReviewModelSelection>>, D::Error> {
    Option::<ReviewModelSelection>::deserialize(deserializer).map(Some)
}
impl SettingsOverrides {
    pub fn validate(&self, ceilings: &Limits) -> Result<(), String> {
        limits(&self.limits, ceilings)?;
        if self
            .agent
            .as_ref()
            .is_some_and(|s| s.is_empty() || s.len() > 256)
            || self
                .mode
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > 256)
        {
            return Err("agent/mode must contain 1..256 bytes".into());
        }
        if self.permissions.as_ref().is_some_and(|p| p.len() > 256) {
            return Err("permissions exceed 256 rules".into());
        }
        microcrab_permission::validate(&microcrab_permission::PermissionConfig {
            overrides: self.permissions.clone().unwrap_or_default(),
            initialization: self.initialization.unwrap_or(Action::Allow),
            review: self.review.unwrap_or(true),
            review_model: self.review_model.clone().flatten(),
            ..Default::default()
        })
        .map_err(|e| format!("permission defaults: {e}"))?;
        if let Some(policy) = &self.auto_compaction {
            policy.validate().map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    pub fn overlay(&mut self, other: &Self) {
        self.limits.extend(other.limits.clone());
        macro_rules! apply{($($field:ident),*)=>{$(if other.$field.is_some(){self.$field=other.$field.clone();})*};}
        apply!(
            model_selection,
            agent,
            mode,
            permissions,
            initialization,
            review,
            review_model,
            auto_compaction,
            auto_naming
        );
    }
}
