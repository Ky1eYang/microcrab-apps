//! Browser Conversations have no Project registry membership. Immutable owner,
//! browser and workspace pins constrain every reconnect; socket generations own
//! transient leases. Route replacement interrupts admission and never replays effects.
use super::{Manager, bindings::Route, safe, unavailable};
use crate::auth::Principal;
use microcrab_host_api::Host;
use microcrab_host_protocol::{BindingScope, HostDescriptor, HostFrame};
use microcrab_host_remote::RemoteHost;
use microcrab_protocol::application::WireError;
use microcrab_storage::execution::{ConversationBinding, ConversationGrant};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::{Arc, atomic::Ordering};
use tokio::sync::mpsc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Binding {
    pub conversation_id: String,
    pub browser_id: String,
    pub workspace_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Attach {
    pub conversation_id: String,
    pub descriptor: HostDescriptor,
    pub lease_generation: String,
}
impl Manager {
    pub(super) async fn conversation_scope(
        &self,
        owner: &str,
        params: Binding,
    ) -> Result<BindingScope, WireError> {
        if self.embedded || self.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        for id in [
            &params.conversation_id,
            &params.browser_id,
            &params.workspace_id,
        ] {
            uuid::Uuid::parse_str(id).map_err(|_| {
                WireError::new(
                    "invalid_request",
                    "Expected UUID Conversation, browser and workspace identities",
                )
            })?;
        }
        let _identity = self.identity_gate.lock().await;
        // A different owner cannot acquire an existing Conversation ID, even
        // before its first Session record is created.
        let grant = match self
            .pins
            .conversation_grant(&params.conversation_id)
            .await
            .map_err(safe)?
        {
            Some(grant) => grant,
            None => self
                .pins
                .pin_conversation_grant(ConversationGrant {
                    conversation_id: params.conversation_id.clone(),
                    browser_id: params.browser_id.clone(),
                    workspace_id: params.workspace_id.clone(),
                    owner: owner.into(),
                    backend_id: self.pins.backend_id().await.map_err(safe)?,
                    binding_id: uuid::Uuid::new_v4().to_string(),
                })
                .await
                .map_err(safe)?,
        };
        if grant.owner != owner
            || grant.browser_id != params.browser_id
            || grant.workspace_id != params.workspace_id
        {
            return Err(unavailable());
        }
        Ok(BindingScope::conversation(
            grant.backend_id,
            grant.binding_id,
            grant.conversation_id,
        ))
    }
    pub(crate) async fn allows_conversation(&self, principal: &Principal, id: &str) -> bool {
        !self.closed.load(Ordering::Acquire)
            && principal.expires_at > super::now() / 1000
            && self
                .pins
                .conversation_grant(id)
                .await
                .ok()
                .flatten()
                .is_some_and(|grant| grant.owner == principal.owner)
    }
    pub(crate) async fn attach_conversation(
        &self,
        principal: &Principal,
        socket: &str,
        params: Attach,
        outbound: mpsc::Sender<HostFrame>,
        generation: u64,
    ) -> Result<Value, WireError> {
        if self.embedded
            || !self
                .allows_conversation(principal, &params.conversation_id)
                .await
        {
            return Err(unavailable());
        }
        if generation == 0
            || params.lease_generation.is_empty()
            || params.lease_generation.len() > 1024
            || params.descriptor.workspace_root.trim().is_empty()
            || params.descriptor.workspace_root.len() > 32768
            || params.descriptor.workspace_root.contains('\0')
        {
            return Err(WireError::new(
                "invalid_host",
                "Invalid browser Host lease or workspace",
            ));
        }
        let identity = self.identity_gate.lock().await;
        let grant = self
            .pins
            .conversation_grant(&params.conversation_id)
            .await
            .map_err(safe)?
            .ok_or_else(unavailable)?;
        if grant.owner != principal.owner
            || grant.browser_id != params.descriptor.host_id
            || grant.workspace_id != params.descriptor.workspace_id
        {
            return Err(WireError::new(
                "binding_mismatch",
                "Browser or workspace differs from the Conversation grant",
            ));
        }
        let scope =
            BindingScope::conversation(grant.backend_id, grant.binding_id, grant.conversation_id);
        let candidate = RemoteHost::new(params.descriptor.clone())
            .map_err(|_| WireError::new("invalid_host", "Invalid browser Host descriptor"))?;
        candidate
            .set_scope(scope.clone())
            .map_err(|_| unavailable())?;
        let binding = ConversationBinding {
            conversation_id: params.conversation_id.clone(),
            owner: principal.owner.clone(),
            host_id: params.descriptor.host_id.clone(),
            workspace_id: params.descriptor.workspace_id.clone(),
            workspace_root: params.descriptor.workspace_root.clone(),
            descriptor_digest: params.descriptor.descriptor_digest.clone(),
        };
        if let Some(existing) = self
            .pins
            .conversation(&params.conversation_id)
            .await
            .map_err(safe)?
            && (existing != binding
                || !microcrab_host_protocol::descriptor_matches_pin(
                    &params.descriptor,
                    &existing.descriptor_digest,
                )
                .map_err(|_| unavailable())?)
        {
            return Err(WireError::new(
                "binding_mismatch",
                "Conversation execution identity is immutable",
            ));
        }
        // A workspace cannot be concurrently leased by another Conversation.
        // Durable binding cannot later move it to a different Conversation either.
        let workspace_key = microcrab_storage::history::content_hash(&json!([
            binding.host_id,
            binding.workspace_id
        ]))
        .map_err(safe)?;
        self.pins
            .pin_conversation_workspace(&workspace_key, &binding)
            .await
            .map_err(|_| {
                WireError::new(
                    "workspace_in_use",
                    "Browser workspace belongs to another Conversation",
                )
            })?;
        self.pins.pin_conversation(binding).await.map_err(|_| {
            WireError::new("binding_mismatch", "Conversation workspace pin differs")
        })?;
        {
            let mut routes = self.conversation_routes.lock().await;
            if let Some(old) = routes.get_mut(&params.conversation_id) {
                if generation <= old.generation {
                    return Err(WireError::new(
                        "route_replaced",
                        "Stale browser attachment generation",
                    ));
                }
                old.ready = false;
                old.remote.detach(old.generation);
            }
        }
        self.suspend_conversation(&params.conversation_id).await;
        let remote = {
            let mut routes = self.conversation_routes.lock().await;
            let remote = routes
                .get(&params.conversation_id)
                .map(|route| route.remote.clone())
                .unwrap_or(candidate);
            remote
                .attach_with_bridge_instance(
                    outbound,
                    generation,
                    params.lease_generation,
                    params.descriptor.bridge_instance_id,
                )
                .map_err(|_| WireError::new("host_detached", "Browser Host attachment failed"))?;
            routes.insert(
                params.conversation_id.clone(),
                Route {
                    owner: principal.owner.clone(),
                    socket: socket.into(),
                    generation,
                    remote: remote.clone(),
                    ready: false,
                    diagnostics: None,
                },
            );
            remote
        };
        drop(identity);
        let _ = remote.reconcile_for_generation(generation).await;
        if !self
            .allows_conversation(principal, &params.conversation_id)
            .await
        {
            return Err(unavailable());
        }
        let mut routes = self.conversation_routes.lock().await;
        let route = routes
            .get_mut(&params.conversation_id)
            .filter(|route| route.socket == socket && route.generation == generation)
            .ok_or_else(|| WireError::new("route_replaced", "Browser attachment replaced"))?;
        let readiness = remote.readiness();
        route.ready = readiness["ready"] == true;
        Ok(
            json!({"conversation_id":params.conversation_id,"connection_generation":generation.to_string(),"connected":readiness["connected"],"ready":route.ready,"reconciling":readiness["reconciling"],"scope":scope,"reconciliation_error":readiness["cause"],"readiness":readiness}),
        )
    }
    pub(crate) async fn receive_conversation_host(
        &self,
        principal: &Principal,
        socket: &str,
        id: &str,
        generation: u64,
        frame: HostFrame,
    ) -> Result<(), WireError> {
        if !self.allows_conversation(principal, id).await {
            return Err(unavailable());
        }
        if !super::bindings::is_reply(&frame) {
            return Err(WireError::new(
                "invalid_host_frame",
                "Expected browser Host reply",
            ));
        }
        let routes = self.conversation_routes.lock().await;
        let route = routes
            .get(id)
            .filter(|route| {
                route.owner == principal.owner
                    && route.socket == socket
                    && route.generation == generation
            })
            .ok_or_else(unavailable)?;
        route
            .remote
            .receive(generation, frame)
            .map_err(|_| WireError::new("invalid_host_frame", "Browser Host reply rejected"))
    }
    pub(crate) async fn detach_conversation_socket(&self, socket: &str, id: Option<&str>) {
        let detached: Vec<_> = {
            let mut routes = self.conversation_routes.lock().await;
            routes
                .iter_mut()
                .filter_map(|(key, route)| {
                    if route.socket == socket && id.is_none_or(|id| id == key) {
                        route.ready = false;
                        route.remote.detach(route.generation);
                        Some(key.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };
        for id in detached {
            self.suspend_conversation(&id).await;
        }
    }
    pub(super) async fn conversation_host(
        &self,
        owner: &str,
        id: &str,
    ) -> Result<Arc<dyn Host>, WireError> {
        if self.embedded
            || self
                .pins
                .conversation_grant(id)
                .await
                .map_err(safe)?
                .is_none_or(|grant| grant.owner != owner)
        {
            return Err(unavailable());
        }
        let routes = self.conversation_routes.lock().await;
        let route = routes.get(id).filter(|route| route.owner == owner);
        let readiness = route.map(|route| route.remote.readiness());
        if let Some(route) = route
            && route.ready
            && readiness
                .as_ref()
                .is_some_and(|state| state["ready"] == true)
        {
            return Ok(route.remote.clone());
        }
        let message = readiness.as_ref()
            .and_then(|state| state["cause"]["message"].as_str())
            .unwrap_or("The exact Conversation browser workspace has no ready route. Inspect its connection and operation receipts, then explicitly retry; actions are never replayed");
        let mut error = WireError::new("host_detached", message);
        error.details = Some(Box::new(
            json!({"conversation_id":id,"readiness":readiness}),
        ));
        Err(error)
    }
    pub(super) async fn record_host(
        &self,
        record: &microcrab_storage::SessionRecord,
    ) -> Result<Arc<dyn Host>, WireError> {
        match record.project_id.as_deref() {
            Some(project) => self.project_host(&record.owner, project).await,
            None => self.conversation_host(&record.owner, &record.id).await,
        }
    }
    pub(super) fn record_allowed(&self, record: &microcrab_storage::SessionRecord) -> bool {
        record
            .project_id
            .as_deref()
            .is_none_or(|project| self.project_allowed(&record.owner, project))
    }
    pub(super) async fn suspend_conversation(&self, id: &str) {
        self.suspend_slots(None, Some(id)).await;
    }
}
