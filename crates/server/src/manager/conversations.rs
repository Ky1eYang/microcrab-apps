//! Browser Conversations have no Project registry membership. Immutable owner,
//! browser and workspace pins constrain every reconnect; socket generations own
//! transient leases. Route replacement interrupts admission and never replays effects.
mod server_host;
use super::conversation_flavors::HostFlavor;
use super::{Manager, bindings::Route, safe, unavailable};
use crate::auth::Principal;
use microcrab_host_api::Host;
use microcrab_host_protocol::{BindingScope, HostDescriptor, HostFrame};
use microcrab_host_remote::RemoteHost;
use microcrab_protocol::application::WireError;
use microcrab_storage::execution::{ConversationBinding, ConversationGrant};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use server_host::ServerHost;
use std::sync::{Arc, atomic::Ordering};
use tokio::sync::mpsc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Binding {
    pub conversation_id: String,
    pub browser_id: String,
    pub workspace_id: String,
    #[serde(default)]
    pub host_flavor: Option<HostFlavor>,
}
#[derive(Debug, PartialEq, Eq, Serialize)]
pub(super) struct ConversationScope {
    #[serde(flatten)]
    scope: BindingScope,
    host_flavor: HostFlavor,
    server_owned: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Attach {
    pub conversation_id: String,
    pub descriptor: HostDescriptor,
    pub lease_generation: String,
    #[serde(default)]
    pub host_flavor: Option<HostFlavor>,
    #[serde(default)]
    pub force_takeover: bool,
}
fn chat_only_descriptor(descriptor: &HostDescriptor) -> bool {
    descriptor.platform == "browser"
        && descriptor.workspace_root == "/chat"
        && descriptor.info.capabilities.is_empty()
        && descriptor.info.virtual_shell.is_none()
        && descriptor.tool_capabilities.is_empty()
        && descriptor.supported_primitives.is_empty()
}
impl Manager {
    /// Caller holds identity_gate. The grant and immutable flavor, not a socket,
    /// authorize only this capability-empty compatibility descriptor.
    async fn pin_chat_only(&self, grant: &ConversationGrant) -> Result<HostDescriptor, WireError> {
        let descriptor = ServerHost::descriptor(grant)
            .map_err(|_| WireError::new("invalid_host", "Chat Only descriptor encoding failed"))?;
        let binding = ConversationBinding {
            conversation_id: grant.conversation_id.clone(),
            owner: grant.owner.clone(),
            host_id: descriptor.host_id.clone(),
            workspace_id: descriptor.workspace_id.clone(),
            workspace_root: descriptor.workspace_root.clone(),
            descriptor_digest: descriptor.descriptor_digest.clone(),
        };
        if self
            .pins
            .conversation(&grant.conversation_id)
            .await
            .map_err(safe)?
            .is_some_and(|existing| existing != binding)
        {
            return Err(WireError::new(
                "binding_mismatch",
                "Chat Only execution identity is immutable",
            ));
        }
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
            WireError::new("binding_mismatch", "Chat Only execution identity differs")
        })?;
        Ok(descriptor)
    }
    pub(super) async fn conversation_scope(
        &self,
        owner: &str,
        params: Binding,
    ) -> Result<ConversationScope, WireError> {
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
        let existing = self
            .pins
            .conversation_grant(&params.conversation_id)
            .await
            .map_err(safe)?;
        let grant = match existing
            .clone()
            .or_else(|| self.conversation_flavors.grant(&params.conversation_id))
        {
            Some(grant) => grant,
            None => ConversationGrant {
                conversation_id: params.conversation_id.clone(),
                browser_id: params.browser_id.clone(),
                workspace_id: params.workspace_id.clone(),
                owner: owner.into(),
                backend_id: self.pins.backend_id().await.map_err(safe)?,
                binding_id: uuid::Uuid::new_v4().to_string(),
            },
        };
        if grant.owner != owner
            || grant.browser_id != params.browser_id
            || grant.workspace_id != params.workspace_id
        {
            return Err(unavailable());
        }
        let pinned = self
            .conversation_flavors
            .get(owner, &params.conversation_id)
            .or_else(|| existing.as_ref().map(|_| HostFlavor::BrowserSandbox));
        let flavor = params
            .host_flavor
            .or(pinned)
            .unwrap_or(HostFlavor::BrowserSandbox);
        if pinned.is_some_and(|old| old != flavor) {
            return Err(WireError::new(
                "binding_mismatch",
                "Conversation execution flavor is immutable",
            ));
        }
        // Flavor is committed before the Core grant or any runtime preparation.
        // A crash in between is resumed with the sidecar's exact binding identity.
        let flavors = self.conversation_flavors.clone();
        let flavor_grant = grant.clone();
        tokio::task::spawn_blocking(move || flavors.pin(flavor_grant, flavor))
            .await
            .map_err(|_| {
                WireError::new(
                    "storage_error",
                    "Conversation flavor publication worker failed",
                )
            })?
            .map_err(safe)?;
        let grant = self
            .pins
            .pin_conversation_grant(grant)
            .await
            .map_err(safe)?;
        if flavor == HostFlavor::ChatOnly {
            self.pin_chat_only(&grant).await?;
        }
        Ok(ConversationScope {
            scope: BindingScope::conversation(
                grant.backend_id,
                grant.binding_id,
                grant.conversation_id,
            ),
            host_flavor: flavor,
            server_owned: flavor == HostFlavor::ChatOnly,
        })
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
        let flavors = self.conversation_flavors.clone();
        let flavor_grant = grant.clone();
        let flavor = tokio::task::spawn_blocking(move || flavors.resolve(&flavor_grant))
            .await
            .map_err(|_| WireError::new("storage_error", "Conversation flavor read worker failed"))?
            .map_err(safe)?;
        if params
            .host_flavor
            .is_some_and(|requested| requested != flavor)
        {
            return Err(WireError::new(
                "binding_mismatch",
                "Conversation execution flavor is immutable",
            ));
        }
        if flavor == HostFlavor::ChatOnly && !chat_only_descriptor(&params.descriptor) {
            return Err(WireError::new(
                "binding_mismatch",
                "Chat Only cannot advertise execution capabilities",
            ));
        }
        if flavor == HostFlavor::ChatOnly {
            let expected = self.pin_chat_only(&grant).await?;
            if !microcrab_host_protocol::descriptor_matches_pin(
                &params.descriptor,
                &expected.descriptor_digest,
            )
            .map_err(|_| WireError::new("invalid_host", "Invalid browser Host descriptor"))?
            {
                return Err(WireError::new(
                    "binding_mismatch",
                    "Chat Only descriptor differs from its immutable pin",
                ));
            }
            return Ok(json!({
                "conversation_id":params.conversation_id,"host_flavor":flavor,
                "attached":false,"server_owned":true,"role":"participant",
                "connected":true,"ready":true,"reconciling":false,
                "scope":BindingScope::conversation(grant.backend_id,grant.binding_id,grant.conversation_id),
                "readiness":{"connected":true,"ready":true,"reconciling":false,"host_flavor":flavor,"server_owned":true}
            }));
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
        let slot = self
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .find(|slot| slot.project_id.is_none() && slot.session_id == params.conversation_id)
            .cloned();
        let managed = if params.force_takeover
            && let Some(slot) = &slot
        {
            if slot.initializing.load(Ordering::Acquire) {
                return Err(WireError::new(
                    "host_takeover_busy",
                    "Conversation initialization is active",
                ));
            }
            slot.active
                .try_lock()
                .map_err(|_| {
                    WireError::new("host_takeover_busy", "Conversation lifecycle is active")
                })?
                .clone()
        } else {
            None
        };
        let command_guard = if params.force_takeover {
            managed
                .as_ref()
                .map(|managed| {
                    managed.commands.try_lock().map_err(|_| {
                        WireError::new("host_takeover_busy", "Conversation command is active")
                    })
                })
                .transpose()?
        } else {
            None
        };
        let remote = {
            let mut routes = self.conversation_routes.lock().await;
            if let Some(old) = routes.get(&params.conversation_id) {
                if generation <= old.generation {
                    return Err(WireError::new(
                        "route_replaced",
                        "Stale browser attachment generation",
                    ));
                }
                let readiness = old.remote.readiness();
                if readiness["connected"] == true && !params.force_takeover {
                    return Ok(json!({
                        "conversation_id":params.conversation_id,"host_flavor":flavor,
                        "attached":false,"server_owned":false,"role":"participant",
                        "connection_generation":old.generation.to_string(),
                        "connected":true,"ready":old.ready && readiness["ready"] == true,
                        "reconciling":readiness["reconciling"],"scope":scope,
                        "reconciliation_error":readiness["cause"],"readiness":readiness
                    }));
                }
                if params.force_takeover
                    && readiness["connected"] == true
                    && (!old.ready
                        || readiness["ready"] != true
                        || readiness["reconciling"] == true)
                {
                    return Err(WireError::new(
                        "host_takeover_reconciling",
                        "The existing Host must finish reconciliation before takeover",
                    ));
                }
            }
            let replace = || -> Result<Arc<RemoteHost>, WireError> {
                // Deferred initialization releases the command lock before Session.start.
                // Recheck its admission while holding that lock, at the replacement boundary.
                if params.force_takeover
                    && slot
                        .as_ref()
                        .is_some_and(|slot| slot.initializing.load(Ordering::Acquire))
                {
                    return Err(WireError::new(
                        "host_takeover_busy",
                        "Conversation initialization is active",
                    ));
                }
                if params.force_takeover
                    && let Some(managed) = &managed
                {
                    let mut drivers = managed.child_drivers.try_lock().map_err(|_| {
                        WireError::new("host_takeover_busy", "Child execution ownership is active")
                    })?;
                    while let Some(joined) = drivers.try_join_next() {
                        if !matches!(joined, Ok(Ok(()))) {
                            managed.faulted.store(true, Ordering::Release);
                            return Err(WireError::new(
                                "host_takeover_busy",
                                "Child execution cleanup requires reconciliation",
                            ));
                        }
                    }
                    if managed.faulted.load(Ordering::Acquire)
                        || managed.closing.load(Ordering::Acquire)
                        || managed.session.is_faulted()
                        || managed.hub.operation().is_some()
                        || managed
                            .session
                            .execution_snapshots()
                            .iter()
                            .any(|execution| execution.status.is_pending())
                        || !drivers.is_empty()
                    {
                        return Err(WireError::new(
                            "host_takeover_busy",
                            "Conversation execution is active",
                        ));
                    }
                }
                let remote = routes
                    .get(&params.conversation_id)
                    .map(|route| route.remote.clone())
                    .unwrap_or(candidate);
                if let Some(old) = routes.get_mut(&params.conversation_id) {
                    old.ready = false;
                    old.remote.detach(old.generation);
                }
                remote
                    .attach_with_bridge_instance(
                        outbound,
                        generation,
                        params.lease_generation,
                        params.descriptor.bridge_instance_id,
                    )
                    .map_err(|_| {
                        WireError::new("host_detached", "Browser Host attachment failed")
                    })?;
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
                Ok(remote)
            };
            if params.force_takeover
                && let Some(managed) = &managed
            {
                // Lock order: conversation route -> ClientTools -> Session inbox.
                // The callback is synchronous: no new producer or turn can race
                // the exact idle observation and route generation replacement.
                managed
                    .client_tools
                    .with_execution_idle(|idle| {
                        if !idle {
                            return Err(WireError::new(
                                "host_takeover_busy",
                                "Browser execution is active",
                            ));
                        }
                        managed.session.with_inbox_snapshot(|inbox| {
                            if inbox.active {
                                Err(WireError::new(
                                    "host_takeover_busy",
                                    "Conversation turn is active",
                                ))
                            } else {
                                replace()
                            }
                        })
                    })
                    .await?
            } else {
                replace()?
            }
        };
        drop(command_guard);
        self.suspend_conversation(&params.conversation_id).await;
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
        let mut readiness = remote.readiness();
        readiness["host_flavor"] = json!(flavor);
        route.ready = readiness["ready"] == true;
        Ok(
            json!({"conversation_id":params.conversation_id,"host_flavor":flavor,"attached":true,"server_owned":false,"connection_generation":generation.to_string(),"connected":readiness["connected"],"ready":route.ready,"reconciling":readiness["reconciling"],"scope":scope,"reconciliation_error":readiness["cause"],"readiness":readiness}),
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
        if self.embedded {
            return Err(unavailable());
        }
        let grant = self
            .pins
            .conversation_grant(id)
            .await
            .map_err(safe)?
            .filter(|grant| grant.owner == owner)
            .ok_or_else(unavailable)?;
        let flavors = self.conversation_flavors.clone();
        let flavor_grant = grant.clone();
        let flavor = tokio::task::spawn_blocking(move || flavors.resolve(&flavor_grant))
            .await
            .map_err(|_| WireError::new("storage_error", "Conversation flavor read worker failed"))?
            .map_err(safe)?;
        if flavor == HostFlavor::ChatOnly {
            let descriptor = ServerHost::descriptor(&grant).map_err(|_| {
                WireError::new("invalid_host", "Chat Only descriptor encoding failed")
            })?;
            let binding = self
                .pins
                .conversation(id)
                .await
                .map_err(safe)?
                .ok_or_else(|| {
                    WireError::new("binding_mismatch", "Chat Only execution pin missing")
                })?;
            if binding.owner != grant.owner
                || binding.conversation_id != grant.conversation_id
                || binding.host_id != descriptor.host_id
                || binding.workspace_id != descriptor.workspace_id
                || binding.workspace_root != descriptor.workspace_root
                || binding.descriptor_digest != descriptor.descriptor_digest
            {
                return Err(WireError::new(
                    "binding_mismatch",
                    "Chat Only execution pin differs",
                ));
            }
            return self
                .conversation_flavors
                .chat_host(&grant, Arc::new(ServerHost::new(&descriptor)))
                .map_err(safe);
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
