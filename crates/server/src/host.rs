//! Startup-only local backend assembly. Projects retain access metadata while
//! every session shares this execution namespace and its concurrency controls.
use crate::config::Config;
use microcrab_host_api::Host;
use microcrab_host_local::{LocalHost, ShellLimits};
use microcrab_session::Error;
use std::sync::Arc;

/// Assemble the server's local Host after configuration paths are resolved.
/// Private paths are registered before publication; filesystem-only assembly
/// preserves the server's process-disabled policy without probing interpreters.
/// Injected backends are responsible for equivalent deployment path protections.
pub fn startup_local_host(config: &Config) -> Result<Arc<dyn Host>, Error> {
    let host = LocalHost::new_filesystem(&config.startup_dir, ShellLimits::default())
        .map_err(|_| Error::config("startup Host unavailable"))?;
    host.protect_workspace_path(config.data_dir.clone())
        .map_err(|_| Error::config("private storage protection unavailable"))?;
    for path in &config.private_paths {
        host.protect_workspace_path(path.clone())
            .map_err(|_| Error::config("private configuration protection unavailable"))?;
    }
    host.protect_workspace_path(config.frontend_dir.clone())
        .map_err(|_| Error::config("served frontend protection unavailable"))?;
    for agent in config.agent.iter().chain(config.agents.iter()) {
        host.protect_workspace_path(agent.path.clone())
            .map_err(|_| Error::config("selected component protection unavailable"))?;
    }
    Ok(Arc::new(host))
}
