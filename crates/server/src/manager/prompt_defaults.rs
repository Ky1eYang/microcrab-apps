//! Server-owned client baseline; Core only owns template engines and assets.
use crate::config::settings::SettingsOverrides;
use microcrab_session::context::system::PromptConfig;

pub(super) const DEFAULT_CLIENT_PROMPT: &str = "The frontend client environment renders rich GitHub-Flavored Markdown (GFM), inline SVG graphics, code visualizers (```svg, ```mermaid, ```echarts), and KaTeX math ($...$, $$...$$). Format structured replies with appropriate headings, lists, tables, and visual code blocks.\n\nWhen facing tool capability bottlenecks or when specialized integrations are requested by the user, proactively check available frontend plugins to discover relevant client tools that satisfy the user's requirements.";

/// Presence of a selected recovery is authoritative even when its old schema
/// omitted the snapshot: Core restore deliberately chooses its legacy baseline.
pub(super) fn retained(
    record: PromptConfig,
    recover: Option<&microcrab_session::Recover>,
    fork: Option<&PromptConfig>,
) -> PromptConfig {
    fork.cloned()
        .or_else(|| recover.map(|recover| recover.prompt_config.clone().unwrap_or_default()))
        .unwrap_or(record)
        .resolved()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_defaults_cover_rich_markdown_and_plugin_discovery() {
        let config = resolve(&SettingsOverrides::default());
        config.validate().unwrap();
        for guidance in [
            "GitHub-Flavored Markdown (GFM)",
            "inline SVG graphics",
            "```svg",
            "```mermaid",
            "```echarts",
            "KaTeX math ($...$, $$...$$)",
            "headings, lists, tables, and visual code blocks",
            "tool capability bottlenecks",
            "specialized integrations are requested by the user",
            "proactively check available frontend plugins",
            "discover relevant client tools",
        ] {
            assert!(
                config.client_prompt.contains(guidance),
                "missing {guidance}"
            );
        }
    }

    #[test]
    fn prompt_defaults_do_not_append_to_explicit_client_instructions() {
        for client_prompt in ["", "  {{ literal }}\n", "custom client instructions"] {
            let config = resolve(&SettingsOverrides {
                client_prompt: Some(client_prompt.into()),
                ..Default::default()
            });
            assert_eq!(config.client_prompt, client_prompt);
        }
    }

    fn checkpoint(config: Option<PromptConfig>) -> microcrab_session::Recover {
        let limits = microcrab_session::Limits::default();
        let context = microcrab_session::context::Context::new(
            "exact frozen SYSTEM".into(),
            vec![],
            limits.max_context_bytes,
        )
        .unwrap();
        microcrab_session::Recover {
            prompt_config: config,
            state: microcrab_session::SessionState::new(limits),
            context: context.snapshot().unwrap(),
            continuation: None,
            workspace: None,
            cache_report: None,
            agent: None,
        }
    }
    #[test]
    fn prompt_retained_authority_matches_restore_and_fork_including_legacy_absence() {
        let record = PromptConfig {
            system_template: "RECORD".into(),
            client_prompt: "record client".into(),
            rename_template: "RECORD NAME".into(),
            ..Default::default()
        };
        let original = PromptConfig {
            system_template: "RECOVER".into(),
            client_prompt: "  {{ literal }}\n".into(),
            rename_template: "ORIGINAL NAME".into(),
            ..Default::default()
        };
        let fork = PromptConfig {
            system_template: "FORK".into(),
            client_prompt: String::new(),
            compact_template: "FORK COMPACT".into(),
            ..Default::default()
        };
        assert_eq!(retained(record.clone(), None, None), record);
        let snapshot = checkpoint(Some(original.clone()));
        assert_eq!(retained(record.clone(), Some(&snapshot), None), original);
        assert_eq!(
            retained(PromptConfig::default(), Some(&snapshot), None),
            original
        );
        let legacy = checkpoint(None);
        assert_eq!(
            retained(record.clone(), Some(&legacy), None),
            PromptConfig::default()
        );
        assert_eq!(retained(record.clone(), Some(&snapshot), Some(&fork)), fork);
        assert_eq!(retained(record.clone(), Some(&legacy), Some(&fork)), fork);
        assert_eq!(retained(record, None, Some(&fork)), fork);
    }
}

pub(super) fn resolve(settings: &SettingsOverrides) -> PromptConfig {
    let mut config = PromptConfig {
        client_prompt: DEFAULT_CLIENT_PROMPT.into(),
        ..Default::default()
    };
    macro_rules! apply {
        ($($field:ident),*) => {$(if let Some(value) = &settings.$field {
            config.$field = value.clone();
        })*};
    }
    apply!(
        system_template,
        compact_template,
        auto_compact_template,
        rename_template,
        action_review_template,
        client_prompt
    );
    config.resolved()
}
