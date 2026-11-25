//! Native Session assembly helpers for Client deliveries.
//!
//! The caller owns Host binding, permission policy, frozen history, and Session
//! lifecycle. This crate neither publishes Sessions nor starts initialization.

pub mod agent;

use microcrab_agent_types::{AgentInfo, AgentRole};
use microcrab_session::runtime::BuiltinTool;

pub mod session;

/// Plan Agent-dependent native builtins in registration order. `root` is the
/// already-selected root Agent; other catalog entries do not grant it modes.
/// This is runtime wiring, not authority to change a frozen tool catalog.
pub fn agent_builtins(
    root: Option<&AgentInfo>,
    catalog: &[AgentInfo],
) -> impl Iterator<Item = BuiltinTool> {
    let switch_mode = root.is_some_and(|agent| {
        let mut modes = agent
            .modes
            .iter()
            .filter(|mode| matches!(mode.role, AgentRole::MainAgent | AgentRole::Both));
        modes
            .next()
            .is_some_and(|first| modes.any(|mode| mode.name != first.name))
    });
    let delegate = catalog.iter().any(|agent| {
        agent
            .modes
            .iter()
            .any(|mode| matches!(mode.role, AgentRole::SubAgent | AgentRole::Both))
    });
    [
        switch_mode.then_some(BuiltinTool::SwitchMode),
        delegate.then_some(BuiltinTool::Delegate),
    ]
    .into_iter()
    .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use microcrab_agent_types::AgentMode;

    fn agent(name: &str, modes: impl IntoIterator<Item = (&'static str, AgentRole)>) -> AgentInfo {
        AgentInfo {
            name: name.into(),
            tool_media_output_bytes: Default::default(),
            modes: modes
                .into_iter()
                .map(|(name, role)| AgentMode {
                    name: name.into(),
                    role,
                    description: String::new(),
                    prompt: String::new(),
                    permissions: vec![],
                })
                .collect(),
            tools: vec![],
            variables: Default::default(),
        }
    }

    #[test]
    fn switching_requires_distinct_main_modes_on_the_selected_agent() {
        use AgentRole::*;

        let root = agent(
            "root",
            [
                ("main", MainAgent),
                ("main", MainAgent),
                ("child", SubAgent),
            ],
        );
        let other = agent("other", [("one", MainAgent), ("two", MainAgent)]);
        let catalog = [root, other];
        assert_eq!(agent_builtins(None, &[]).count(), 0);
        assert!(matches!(
            agent_builtins(None, &catalog)
                .collect::<Vec<_>>()
                .as_slice(),
            [BuiltinTool::Delegate]
        ));
        assert!(matches!(
            agent_builtins(Some(&catalog[0]), &catalog)
                .collect::<Vec<_>>()
                .as_slice(),
            [BuiltinTool::Delegate]
        ));
        assert!(matches!(
            agent_builtins(Some(&catalog[1]), &catalog[1..])
                .collect::<Vec<_>>()
                .as_slice(),
            [BuiltinTool::SwitchMode]
        ));
    }

    #[test]
    fn both_role_contributes_to_switching_and_delegation_in_order() {
        use AgentRole::*;

        let catalog = [agent("root", [("main", MainAgent), ("both", Both)])];
        assert!(matches!(
            agent_builtins(Some(&catalog[0]), &catalog)
                .collect::<Vec<_>>()
                .as_slice(),
            [BuiltinTool::SwitchMode, BuiltinTool::Delegate]
        ));
    }
}
