//! Owner-scoped inspection never exposes original request bodies or completed values.
use super::{Manager, safe, unavailable};
use crate::auth::Principal;
use microcrab_protocol::application::WireError;
use serde_json::{Value, json};

impl Manager {
    async fn inspect_project_owner(
        &self,
        principal: &Principal,
        project: &str,
    ) -> Result<(), WireError> {
        if !self.allows_project(principal, project) {
            return Err(unavailable());
        }
        if self
            .pins
            .grant(project)
            .await
            .map_err(safe)?
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
        Ok(())
    }
    pub(super) async fn host_state(
        &self,
        principal: &Principal,
        project: &str,
    ) -> Result<Value, WireError> {
        self.inspect_project_owner(principal, project).await?;
        let grant = self.pins.grant(project).await.map_err(safe)?;
        let routes = self.routes.lock().await;
        let route = routes
            .get(project)
            .filter(|route| route.owner == principal.owner);
        let readiness = route.map(|route| route.remote.readiness());
        Ok(
            json!({"project_id":project,"connection_generation":route.map(|r| r.generation.to_string()),
            "connected":self.embedded || readiness.as_ref().is_some_and(|state| state["connected"] == true),
            "reconciling":readiness.as_ref().is_some_and(|state| state["reconciling"] == true),
            "ready":self.embedded || route.is_some_and(|r| r.ready) && readiness.as_ref().is_some_and(|state| state["ready"] == true),
            "scope":grant.map(|g| microcrab_host_protocol::BindingScope::project(g.backend_id,g.binding_id,g.project_id)),
            "reconciliation_error":readiness.as_ref().map(|state| &state["cause"]),
            "readiness":readiness,
            "diagnostics":route.map(|r| r.remote.diagnostics(64)).unwrap_or_default()}),
        )
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
        self.projects
            .remove_provisional(&principal.owner, project)
            .await
            .map_err(|_| unavailable())?;
        Ok(json!({"project_id":project,"deleted":true}))
    }
}
