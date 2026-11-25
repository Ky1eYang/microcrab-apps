//! Owner-scoped inspection never exposes original request bodies or completed values.
use super::{
    Manager,
    bindings::{ProjectCarrier, ProjectCarrierMode, Route},
    safe, unavailable,
};
use crate::auth::Principal;
use microcrab_host_protocol::{BindingScope, HostDescriptor};
use microcrab_protocol::application::WireError;
use serde_json::{Value, json};

/// Apply the Manager's admission gate even if the RemoteHost is already ready.
fn project_host_view(
    project: &str,
    embedded: bool,
    route: Option<&Route<ProjectCarrier>>,
    readiness: Option<&Value>,
    scope: Option<BindingScope>,
    diagnostics: bool,
) -> Value {
    let connected = readiness.is_some_and(|state| state["connected"] == true);
    let carrier = route.filter(|_| connected).map(|route| &route.carrier);
    let mode = if embedded {
        json!("embedded")
    } else if let Some(carrier) = carrier {
        json!(carrier.mode)
    } else {
        json!("detached")
    };
    let descriptor = carrier.map(|carrier| {
        let HostDescriptor {
            host_id,
            workspace_id,
            workspace_root,
            platform,
            tool_capabilities,
            supported_primitives,
            ..
        } = &carrier.descriptor;
        json!({"host_id":host_id,"workspace_id":workspace_id,"workspace_root":workspace_root,
            "platform":platform,"tool_capabilities":tool_capabilities,
            "supported_primitives":supported_primitives})
    });
    json!({
        "project_id": project,
        "connection_generation": route.map(|r| r.generation.to_string()),
        "carrier_mode": mode,
        "descriptor": descriptor,
        "connected": embedded || connected,
        "reconciling": connected && readiness.is_some_and(|state| state["reconciling"] == true),
        "ready": embedded || connected && route.is_some_and(|r| r.ready)
            && readiness.is_some_and(|state| state["ready"] == true),
        "scope": scope,
        "reconciliation_error": readiness.map(|state| &state["cause"]),
        "readiness": readiness,
        "diagnostics": route.filter(|_| diagnostics).map(|r| r.remote.diagnostics(64)).unwrap_or_default(),
    })
}

impl Manager {
    async fn inspect_project_owner(
        &self,
        principal: &Principal,
        project: &str,
    ) -> Result<Option<BindingScope>, WireError> {
        if !self.allows_project(principal, project) {
            return Err(unavailable());
        }
        let grant = self.pins.grant(project).await.map_err(safe)?;
        if grant
            .as_ref()
            .is_some_and(|grant| grant.owner != principal.owner)
            || self
                .pins
                .project(project)
                .await
                .map_err(safe)?
                .is_some_and(|pin| pin.owner != principal.owner)
        {
            return Err(unavailable());
        }
        Ok(grant.map(|grant| {
            BindingScope::project(grant.backend_id, grant.binding_id, grant.project_id)
        }))
    }
    pub(crate) fn watch_project_hosts(&self) -> tokio::sync::broadcast::Receiver<String> {
        self.project_host_changes.subscribe()
    }

    pub(super) fn project_host_changed(&self, project: &str) {
        if self.project_host_changes.receiver_count() > 0 {
            let _ = self.project_host_changes.send(project.to_owned());
        }
    }

    pub(crate) async fn authorize_project_host_watch(
        &self,
        principal: &Principal,
        project: &str,
    ) -> Result<(), WireError> {
        // Do not wait for the route publication gate from the socket writer:
        // direct/relay attachment may still need that same writer to make progress.
        self.inspect_project_owner(principal, project).await?;
        if !self.allows_project(principal, project) {
            return Err(unavailable());
        }
        Ok(())
    }

    pub(crate) async fn project_host_sample(
        &self,
        principal: &Principal,
        project: &str,
    ) -> Result<(Value, Option<tokio::sync::watch::Receiver<()>>), WireError> {
        let _identity = self.identity_gate.lock().await;
        let scope = self.inspect_project_owner(principal, project).await?;
        let routes = self.routes.lock().await;
        if !self.allows_project(principal, project) {
            return Err(unavailable());
        }
        let route = routes
            .get(project)
            .filter(|route| !self.embedded && route.owner == principal.owner);
        // Subscribe before sampling; RemoteHost may change independently of the Manager lock.
        let receiver = route.map(|route| route.remote.watch_readiness());
        let readiness = route.map(|route| {
            let mut value = route.remote.readiness();
            if let Some(fields) = value.as_object_mut() {
                fields.remove("transport_warning");
            }
            value
        });
        Ok((
            project_host_view(
                project,
                self.embedded,
                route,
                readiness.as_ref(),
                scope,
                false,
            ),
            receiver,
        ))
    }

    pub(super) async fn host_state(
        &self,
        principal: &Principal,
        project: &str,
    ) -> Result<Value, WireError> {
        let _identity = self.identity_gate.lock().await;
        let scope = self.inspect_project_owner(principal, project).await?;
        let routes = self.routes.lock().await;
        if !self.allows_project(principal, project) {
            return Err(unavailable());
        }
        let route = routes
            .get(project)
            .filter(|route| !self.embedded && route.owner == principal.owner);
        let readiness = route.map(|route| route.remote.readiness());
        Ok(project_host_view(
            project,
            self.embedded,
            route,
            readiness.as_ref(),
            scope,
            true,
        ))
    }

    /// Keep the existing connect response shape, using the same route sample for
    /// the full accepted descriptor, legacy transport label and readiness.
    pub(super) async fn observe_direct_route(
        &self,
        principal: &Principal,
        project: &str,
    ) -> Result<Option<Value>, WireError> {
        let _identity = self.identity_gate.lock().await;
        let scope = self.inspect_project_owner(principal, project).await?;
        let routes = self.routes.lock().await;
        if !self.allows_project(principal, project) {
            return Err(unavailable());
        }
        let Some(route) = routes.get(project).filter(|route| {
            route.owner == principal.owner && route.carrier.mode != ProjectCarrierMode::BrowserRelay
        }) else {
            return Ok(None);
        };
        let readiness = route.remote.readiness();
        if readiness["connected"] != true {
            return Ok(None);
        }
        let mut view =
            project_host_view(project, false, Some(route), Some(&readiness), scope, true);
        view["descriptor"] = json!(route.carrier.descriptor);
        view["transport"] = json!(route.carrier.mode.transport());
        view["observed_existing"] = json!(true);
        Ok(Some(view))
    }
    pub(super) async fn host_operations(
        &self,
        principal: &Principal,
        project: &str,
        limit: usize,
    ) -> Result<Value, WireError> {
        self.inspect_project_owner(principal, project).await?;
        if limit == 0 || limit > 100 {
            return Err(WireError::new(
                "invalid_request",
                "Operation limit must be between 1 and 100",
            ));
        }
        let routes = self.routes.lock().await;
        let route = routes
            .get(project)
            .filter(|route| route.owner == principal.owner);
        Ok(
            json!({"project_id":project,"connection_generation":route.map(|r| r.generation.to_string()),
            "operations":route.map(|r| r.remote.operations(limit)).unwrap_or_default(),"limit":limit}),
        )
    }
    pub(super) async fn reconcile_host(
        &self,
        principal: &Principal,
        project: &str,
        expected: Option<&str>,
    ) -> Result<Value, WireError> {
        self.inspect_project_owner(principal, project).await?;
        let (remote, generation) = {
            let mut routes = self.routes.lock().await;
            let route = routes
                .get_mut(project)
                .filter(|r| r.owner == principal.owner && r.remote.is_connected())
                .ok_or_else(|| WireError::new("host_detached", "Project Host is disconnected"))?;
            if expected.is_some_and(|expected| expected != route.generation.to_string()) {
                return Err(WireError::new(
                    "route_replaced",
                    "Host connection generation changed",
                ));
            }
            route.ready = false;
            self.project_host_changed(project);
            (route.remote.clone(), route.generation)
        };
        let _ = remote.reconcile_for_generation(generation).await;
        {
            let mut routes = self.routes.lock().await;
            let route = routes
                .get_mut(project)
                .filter(|r| r.owner == principal.owner && r.generation == generation)
                .ok_or_else(|| {
                    WireError::new("route_replaced", "Host route changed during reconciliation")
                })?;
            if !self.allows_project(principal, project) {
                return Err(unavailable());
            }
            route.ready = remote.is_ready();
            self.project_host_changed(project);
        }
        self.host_state(principal, project).await
    }
    pub(super) async fn delete_provisional(
        &self,
        principal: &Principal,
        project: &str,
    ) -> Result<Value, WireError> {
        let _identity = self.identity_gate.lock().await;
        self.inspect_project_owner(principal, project).await?;
        let record = self
            .projects
            .find(project)
            .await
            .filter(|r| r.allows(&principal.owner) && r.provisional)
            .ok_or_else(|| {
                WireError::new(
                    "project_not_provisional",
                    "Project is not removable provisional metadata",
                )
            })?;
        if self.pins.project(&record.id).await.map_err(safe)?.is_some()
            || self.routes.lock().await.contains_key(project)
            || self
                .directory
                .list(&principal.owner)
                .await
                .map_err(safe)?
                .iter()
                .any(|r| r.project_id.as_deref() == Some(project))
        {
            return Err(WireError::new(
                "project_in_use",
                "Provisional project has a binding, route or sessions",
            ));
        }
        // The durable grant is deliberately retained: only unbound metadata is removed.
        // Retire staged settings before publishing removal; ID reuse cannot inherit them.
        let _owner = self.speaker_gate(&principal.owner).lock_owned().await;
        self.retire_project_external_messaging_locked(&principal.owner, project)
            .await?;
        self.projects
            .remove_provisional(&principal.owner, project)
            .await
            .map_err(|_| unavailable())?;
        self.project_host_changed(project);
        Ok(json!({"project_id":project,"deleted":true}))
    }
}
