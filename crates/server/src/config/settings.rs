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
    /// Omitted/null inherit; an empty template selects its embedded baseline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compact_template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_compact_template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rename_template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_review_template: Option<String>,
    /// An explicit empty string disables the inherited client instructions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_prompt: Option<String>,
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
        max_tool_image_bytes: 1024 * 1024,
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
    let defaults = default_limits();
    let mut result = serde_json::to_value(&defaults).map_err(|_| "cannot encode default limits")?;
    let mut ceiling =
        serde_json::to_value(ceilings).map_err(|_| "cannot encode administrator ceilings")?;
    result["max_tool_image_bytes"] = serde_json::json!(defaults.max_tool_image_bytes);
    ceiling["max_tool_image_bytes"] = serde_json::json!(ceilings.max_tool_image_bytes);
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
    let result: Limits = serde_json::from_value(result)
        .map_err(|_| "Limits value exceeds platform capacity".to_owned())?;
    result
        .validate()
        .map_err(|error| error.diagnostic_reason())?;
    Ok(result)
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
        let mut prompts = microcrab_session::context::system::PromptConfig::default();
        macro_rules! prompt_override {
            ($($field:ident),*) => {$(
                if let Some(value) = &self.$field {
                    prompts.$field = value.clone();
                }
            )*};
        }
        prompt_override!(
            system_template,
            compact_template,
            auto_compact_template,
            rename_template,
            action_review_template,
            client_prompt
        );
        prompts.validate().map_err(|error| error.to_string())?;
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
            auto_naming,
            system_template,
            compact_template,
            auto_compact_template,
            rename_template,
            action_review_template,
            client_prompt
        );
    }
}

#[cfg(test)]
mod prompt_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn typed_media_limits_override_and_clamp_independently_of_historical_serde_omission() {
        let ceilings = hard_limits();
        let explicit = BTreeMap::from([("max_tool_image_bytes".into(), 512 * 1024)]);
        assert_eq!(
            limits(&explicit, &ceilings).unwrap().max_tool_image_bytes,
            512 * 1024
        );
        let mut low = ceilings.clone();
        low.max_tool_image_bytes = 256 * 1024;
        assert_eq!(
            limits(&BTreeMap::new(), &low).unwrap().max_tool_image_bytes,
            256 * 1024
        );
        assert!(limits(&explicit, &low).is_err());
        assert!(limits(&BTreeMap::from([("not_a_limit".into(), 1)]), &ceilings).is_err());
        let mut project = SettingsOverrides::default();
        project
            .limits
            .insert("max_tool_image_bytes".into(), 512 * 1024);
        let mut session = project.clone();
        session.overlay(
            &serde_json::from_value(json!({"limits":{"max_tool_image_bytes":128*1024}})).unwrap(),
        );
        assert_eq!(
            limits(&project.limits, &ceilings)
                .unwrap()
                .max_tool_image_bytes,
            512 * 1024
        );
        assert_eq!(
            limits(&session.limits, &ceilings)
                .unwrap()
                .max_tool_image_bytes,
            128 * 1024
        );
    }
    #[test]
    fn omitted_and_null_prompts_inherit_but_empty_overrides_are_retained() {
        let mut settings: SettingsOverrides = serde_json::from_value(json!({
            "system_template": "authored system\n",
            "client_prompt": "inherited instructions"
        }))
        .unwrap();
        settings.overlay(
            &serde_json::from_value(json!({
                "system_template": null,
                "client_prompt": null
            }))
            .unwrap(),
        );
        assert_eq!(
            settings.system_template.as_deref(),
            Some("authored system\n")
        );
        assert_eq!(
            settings.client_prompt.as_deref(),
            Some("inherited instructions")
        );
        settings.overlay(
            &serde_json::from_value(json!({
                "system_template": "",
                "client_prompt": ""
            }))
            .unwrap(),
        );
        assert_eq!(settings.system_template.as_deref(), Some(""));
        assert_eq!(settings.client_prompt.as_deref(), Some(""));
        settings.validate(&hard_limits()).unwrap();
        assert_eq!(serde_json::to_value(settings).unwrap()["client_prompt"], "");
    }

    #[test]
    fn helper_overrides_preserve_authored_bytes_and_literal_binding_contracts() {
        let mut settings = SettingsOverrides::default();
        settings.overlay(&serde_json::from_value(json!({
            "compact_template": "Compact\r\n{{ user_input }}\r\n",
            "auto_compact_template": "Automatic\n",
            "rename_template": "Name {{ user_input }}\n",
            "action_review_template": "Review {{ tool_name }} {{ tool_args }} {{ user_input }}\n"
        })).unwrap());
        assert_eq!(
            settings.compact_template.as_deref(),
            Some("Compact\r\n{{ user_input }}\r\n")
        );
        assert_eq!(
            settings.auto_compact_template.as_deref(),
            Some("Automatic\n")
        );
        assert_eq!(
            settings.rename_template.as_deref(),
            Some("Name {{ user_input }}\n")
        );
        assert!(settings.action_review_template.is_some());
        settings.validate(&hard_limits()).unwrap();
        settings.rename_template = Some("{{ user_input | safe }}".into());
        assert!(settings.validate(&hard_limits()).is_err());
    }

    #[test]
    fn prompt_validation_rejects_system_syntax_before_configuration_save() {
        let settings: SettingsOverrides = serde_json::from_value(json!({
            "system_template": "{% if agent is defined %}unclosed"
        }))
        .unwrap();
        assert!(settings.validate(&hard_limits()).is_err());
    }
}
