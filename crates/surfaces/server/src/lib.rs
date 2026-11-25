//! Axum application assembly. The manager owns live Sessions; sockets only own
//! subscriptions. No request-body/authorization-URI tracing layer is installed.
pub mod auth;
pub mod config;
mod deployment;
pub mod host;
pub mod manager;
mod noop;
pub(crate) mod project;
mod route;

use axum::Router;
use std::sync::Arc;

pub struct Application {
    pub auth: auth::Auth,
    pub manager: Arc<manager::Manager>,
}
pub async fn application(
    config: config::Config,
) -> Result<(Router, Arc<Application>), Box<dyn std::error::Error>> {
    assemble_application(config, None, None).await
}

/// Assemble for an already-bound listener. The development issuer uses this
/// trusted address rather than request headers or an unresolved configured port.
pub async fn application_at(
    config: config::Config,
    address: std::net::SocketAddr,
) -> Result<(Router, Arc<Application>), Box<dyn std::error::Error>> {
    assemble_application(config, None, Some(address)).await
}

/// Assemble an application with a caller-owned backend shared by all sessions.
/// The embedding application owns backend namespace and private-path policy.
pub async fn application_with_host(
    config: config::Config,
    host: Arc<dyn microcrab_host_api::Host>,
) -> Result<(Router, Arc<Application>), Box<dyn std::error::Error>> {
    assemble_application(config, Some(host), None).await
}
async fn assemble_application(
    config: config::Config,
    host: Option<Arc<dyn microcrab_host_api::Host>>,
    address: Option<std::net::SocketAddr>,
) -> Result<(Router, Arc<Application>), Box<dyn std::error::Error>> {
    let deployment = deployment::DeploymentGuard::acquire(&config.data_dir, &config.startup_dir)?;
    let authentication = auth::Auth::for_listener(config.auth.clone(), address).await?;
    let manager = manager::Manager::for_application(config.clone(), host, deployment).await?;
    #[cfg(not(test))]
    manager.start_model_metadata();
    let state = Arc::new(Application {
        auth: authentication.clone(),
        manager,
    });
    let router = route::router(state.clone(), &config.frontend_dir);
    Ok((router, state))
}
