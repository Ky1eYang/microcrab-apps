//! Per-project remote route authority. Durable pins outlive sockets; controller
//! generations and readiness remain transient. Route loss interrupts real turns.
use super::{Manager, unavailable};
use crate::auth::Principal;
use microcrab_host_api::Host;
use microcrab_host_protocol::{HostDescriptor, HostFrame};
use microcrab_host_remote::RemoteHost;
use microcrab_protocol::application::WireError;
use microcrab_storage::execution::ProjectBinding;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::{Arc, atomic::Ordering};
use tokio::sync::mpsc;

/// Session assembly accepts typed readiness evidence, never arbitrary Host text.
fn host_route_error(error: WireError, detached: &'static str) -> microcrab_session::Error {
    let cause = error
        .details
        .as_ref()
        .map(|details| &details["readiness"]["cause"]);
    let typed = cause
        .and_then(|cause| cause.get("diagnostic_cause"))
        .and_then(|value| {
            serde_json::from_value::<microcrab_host_protocol::HostCause>(value.clone()).ok()
        });
    let reason = if let Some(cause) = typed {
        cause.public_reason()
    } else {
        match cause
            .and_then(|cause| cause["code"].as_str())
            .unwrap_or(&error.code)
        {
            "reconciliation_timeout" => "Host reconciliation timed out",
            "reconciliation_failed" => "Host reconciliation failed",
            "process_cleanup_failed" => "Host process cleanup failed",
            "reconciliation_cancelled" => "Host reconciliation was cancelled",
            "disconnected" => "Host route is disconnected",
            "lease_expired" => "Host route lease expired",
            "outcome_unknown" => {
                "Host operation outcome is unknown; inspect operation receipts before retrying"
            }
            "operation_conflict" => "Host operation conflicts with its durable receipt",
            "binding_mismatch" => "Host route binding does not match its execution pin",
            "workspace_denied" => "Host workspace attachment was denied",
            "pairing_denied" => "Host pairing authorization was denied",
            "unavailable" | "not_found" => "Session execution Host is unavailable to this owner",
            "host_detached" => detached,
            _ => {
                "Host route is not ready; inspect connection and operation receipts before retrying"
            }
        }
    };
    microcrab_session::Error::config_with_private(reason, error.message)
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;

    #[test]
    fn route_errors_publish_only_closed_readiness_evidence() {
        let mut wire = WireError::new("host_detached", "private-host-secret");
        wire.details = Some(Box::new(json!({"readiness":{"cause":{
            "code":"denied", "message":"private-host-secret",
            "diagnostic_cause":"file_path_is_outside_the_configured_root"
        }}})));
        let error = host_route_error(wire, "Project Host has no ready route");
        assert_eq!(
            error.public_reason(),
            Some("file path is outside the configured root")
        );
        let mut wire = WireError::new("host_detached", "private-host-secret");
        wire.details = Some(Box::new(json!({"readiness":{"cause":{
            "code":"reconciliation_timeout", "message":"private-host-secret",
            "diagnostic_cause":"private-host-secret"
        }}})));
        let error = host_route_error(wire, "Project Host has no ready route");
        assert_eq!(error.public_reason(), Some("Host reconciliation timed out"));
    }
}

pub(super) fn is_reply(frame: &HostFrame) -> bool {
    matches!(
        frame,
        HostFrame::Response { .. }
            | HostFrame::Receipt { .. }
            | HostFrame::TransferChunk { .. }
            | HostFrame::ProcessChanged { .. }
            | HostFrame::Diagnostic { .. }
            | HostFrame::Pong { .. }
    )
}
pub(super) struct Route {
    pub owner: String,
    pub socket: String,
    pub generation: u64,
    pub remote: Arc<RemoteHost>,
    pub ready: bool,
    // Last authenticated carrier witness; never consulted for authorization.
    pub diagnostics: Option<super::direct::Correlation>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Attach {
    pub project_id: String,
    pub descriptor: HostDescriptor,
    pub lease_generation: String,
    #[serde(default)]
    pub allow_legacy_binding: bool,
}
impl Manager {
    // Caller holds identity_gate; retained pins for deleted projects do not reserve
    // a workspace. Descriptor capability changes never bypass canonical identity.
    pub(super) async fn canonical_project(
        &self,
        owner: &str,
        requested: &str,
        descriptor: &HostDescriptor,
    ) -> Result<Option<String>, WireError> {
        for project in self.projects.list(owner).await {
            if project.id == requested {
                continue;
            }
            if let Some(existing) = self.pins.project(&project.id).await.map_err(super::safe)?
                && existing.owner == owner
                && existing.host_id == descriptor.host_id
                && (existing.workspace_id == descriptor.workspace_id
                    || existing.workspace_root == descriptor.workspace_root)
            {
                return Ok(Some(existing.project_id));
            }
        }
        Ok(None)
    }
    /// Caller holds identity_gate and has established that no Project pin exists.
    /// Inspection and publication use the same legacy-history admission policy.
    pub(super) async fn validate_unbound_project(
        &self,
        owner: &str,
        project: &str,
        registered: Option<&crate::project::ProjectRecord>,
        allow_legacy_binding: bool,
    ) -> Result<(), WireError> {
        if registered.is_some_and(|record| record.provisional) {
            return Ok(());
        }
        if !allow_legacy_binding {
            return Err(WireError::new(
                "legacy_binding_required",
                "Explicit compatibility binding is required for this unbound Project",
            ));
        }
        if self
            .directory
            .list(owner)
            .await
            .map_err(super::safe)?
            .iter()
            .any(|record| record.project_id.as_deref() == Some(project))
        {
            return Err(WireError::new(
                "binding_mismatch",
                "Legacy sessions cannot adopt a new execution identity",
            ));
        }
        Ok(())
    }
    pub(super) fn project_allowed(&self, owner: &str, project: &str) -> bool {
        self.projects
            .find_sync(project)
            .is_some_and(|record| record.allows(owner))
    }
    pub(super) async fn editable_project(
        &self,
        owner: &str,
        id: &str,
    ) -> Result<crate::project::ProjectRecord, WireError> {
        // Durable owner pins constrain edits even if registry metadata is shared.
        let bound_owner = match self.pins.grant(id).await.map_err(super::safe)? {
            Some(grant) => Some(grant.owner),
            None => self
                .pins
                .project(id)
                .await
                .map_err(super::safe)?
                .map(|pin| pin.owner),
        };
        if bound_owner.is_some_and(|bound_owner| bound_owner != owner) {
            return Err(unavailable());
        }
        self.projects
            .find(id)
            .await
            .filter(|record| record.allows(owner))
            .ok_or_else(unavailable)
    }
    pub(crate) fn allows_project(&self, principal: &Principal, project: &str) -> bool {
        !self.closed.load(Ordering::Acquire)
            && principal.expires_at > super::now() / 1000
            && self.project_allowed(&principal.owner, project)
    }
    pub(crate) fn next_connection_generation(&self) -> u64 {
        self.next_route.fetch_add(1, Ordering::AcqRel)
    }
    pub(super) async fn binding_scope(
        &self,
        owner: &str,
        project: &str,
    ) -> Result<microcrab_host_protocol::BindingScope, WireError> {
        let allowed = self.project_allowed(owner, project);
        if self.embedded || !allowed {
            return Err(unavailable());
        }
        let grant = match self.pins.grant(project).await.map_err(super::safe)? {
            Some(grant) => grant,
            None => {
                let grant = microcrab_storage::execution::ProjectGrant {
                    project_id: project.into(),
                    owner: owner.into(),
                    backend_id: self.pins.backend_id().await.map_err(super::safe)?,
                    binding_id: uuid::Uuid::new_v4().to_string(),
                };
                match self.pins.pin_grant(grant).await {
                    Ok(grant) => grant,
                    Err(error) => self
                        .pins
                        .grant(project)
                        .await
                        .map_err(super::safe)?
                        .ok_or_else(|| super::safe(error))?,
                }
            }
        };
        if grant.owner != owner {
            return Err(unavailable());
        }
        Ok(microcrab_host_protocol::BindingScope::project(
            grant.backend_id,
            grant.binding_id,
            grant.project_id,
        ))
    }
    pub(crate) async fn attach_project(
        &self,
        principal: &Principal,
        socket: &str,
        params: Attach,
        outbound: mpsc::Sender<HostFrame>,
        generation: u64,
    ) -> Result<Value, WireError> {
        if self.embedded {
            return Err(WireError::new(
                "unsupported",
                "Explicit Host embedding owns execution",
            ));
        }
        if principal.expires_at <= super::now() / 1000 {
            return Err(WireError::new("unauthenticated", "Authentication expired"));
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(WireError::new("closed", "Server is shutting down"));
        }
        if generation == 0
            || params.lease_generation.is_empty()
            || params.descriptor.workspace_root.trim().is_empty()
            || params.descriptor.workspace_root.len() > 32768
            || params.lease_generation.len() > 1024
        {
            return Err(WireError::new(
                "invalid_host",
                "Host metadata exceeds budget",
            ));
        }
        let identity = self.identity_gate.lock().await;
        let registered = self.projects.find(&params.project_id).await;
        let allowed = self.project_allowed(&principal.owner, &params.project_id);
        if !allowed {
            return Err(unavailable());
        }
        if self
            .routes
            .lock()
            .await
            .get(&params.project_id)
            .is_some_and(|route| generation <= route.generation)
        {
            return Err(WireError::new(
                "route_replaced",
                "Stale Host attachment generation",
            ));
        }
        if registered.as_ref().is_some_and(|project| {
            project
                .host_id
                .as_ref()
                .is_some_and(|id| id != &params.descriptor.host_id)
                || project
                    .workspace_id
                    .as_ref()
                    .is_some_and(|id| id != &params.descriptor.workspace_id)
        }) {
            return Err(WireError::new(
                "binding_mismatch",
                "Project Host or workspace identity differs",
            ));
        }
        let candidate = RemoteHost::new(params.descriptor.clone())
            .map_err(|_| WireError::new("invalid_host", "Host descriptor is invalid"))?;
        // Project registry membership owns canonicalization. Historical pins are
        // retained for Sessions, but a deleted Project cannot reserve a workspace.
        if let Some(existing) = self
            .canonical_project(&principal.owner, &params.project_id, &params.descriptor)
            .await?
        {
            return Ok(
                json!({"status":"existing_project","existing_project_id":existing,
                "project_id":params.project_id,"connected":false,"ready":false,"pairing_token_consumed":true}),
            );
        }
        let scope = self
            .binding_scope(&principal.owner, &params.project_id)
            .await?;
        candidate
            .set_scope(scope.clone())
            .map_err(|_| WireError::new("binding_mismatch", "Host scope rejected"))?;
        let pin = ProjectBinding {
            project_id: params.project_id.clone(),
            owner: principal.owner.clone(),
            host_id: params.descriptor.host_id.clone(),
            workspace_id: params.descriptor.workspace_id.clone(),
            workspace_root: params.descriptor.workspace_root.clone(),
            descriptor_digest: params.descriptor.descriptor_digest.clone(),
        };
        let pin = match self
            .pins
            .project(&params.project_id)
            .await
            .map_err(super::safe)?
        {
            Some(existing) => {
                if existing.owner != pin.owner
                    || existing.host_id != pin.host_id
                    || existing.workspace_id != pin.workspace_id
                    || existing.workspace_root != pin.workspace_root
                    || !microcrab_host_protocol::descriptor_matches_pin(
                        &params.descriptor,
                        &existing.descriptor_digest,
                    )
                    .map_err(|_| WireError::new("invalid_host", "Host descriptor is invalid"))?
                {
                    return Err(WireError::new(
                        "binding_mismatch",
                        "Project is pinned to another owner, Host or workspace",
                    ));
                }
                existing
            }
            None => {
                self.validate_unbound_project(
                    &principal.owner,
                    &params.project_id,
                    registered.as_ref(),
                    params.allow_legacy_binding,
                )
                .await?;
                pin
            }
        };
        // Pin publication is immutable and atomic even across concurrent owner claims.
        self.pins.pin_project(pin).await.map_err(|_| {
            WireError::new(
                "binding_mismatch",
                "Project is pinned to another owner, Host or workspace",
            )
        })?;
        if registered.is_some() {
            self.projects
                .finalize(&principal.owner, &params.project_id)
                .await
                .map_err(|_| {
                    WireError::new(
                        "project_persistence_failed",
                        "Canonical binding metadata could not be finalized",
                    )
                })?;
        }
        // Stop old admission and fail pending RPC before waiting for activation
        // guards. A guard may itself be awaiting one of those old-route replies.
        {
            let mut routes = self.routes.lock().await;
            if let Some(route) = routes.get_mut(&params.project_id) {
                if generation <= route.generation {
                    return Err(WireError::new(
                        "route_replaced",
                        "Stale Host attachment generation",
                    ));
                }
                route.ready = false;
                route.remote.detach(route.generation);
            }
        }
        self.suspend_project_with_reason(
            &params.project_id,
            microcrab_session::HostDetachCode::Replaced,
        )
        .await;
        let remote = {
            let mut routes = self.routes.lock().await;
            if !self.project_allowed(&principal.owner, &params.project_id) {
                return Err(unavailable());
            }
            let remote = match routes.get(&params.project_id) {
                Some(route) => route.remote.clone(),
                None => candidate,
            };
            remote
                .set_scope(scope.clone())
                .map_err(|_| WireError::new("binding_mismatch", "Host scope rejected"))?;
            let diagnostics = super::direct::Correlation::new(
                &params.project_id,
                socket,
                generation,
                &params.lease_generation,
                &params.descriptor.bridge_instance_id,
            );
            remote
                .attach_with_bridge_instance(
                    outbound,
                    generation,
                    params.lease_generation,
                    params.descriptor.bridge_instance_id,
                )
                .map_err(|_| WireError::new("host_detached", "Host route attachment failed"))?;
            routes.insert(
                params.project_id.clone(),
                Route {
                    owner: principal.owner.clone(),
                    socket: socket.into(),
                    generation,
                    remote: remote.clone(),
                    ready: false,
                    diagnostics: Some(diagnostics.clone()),
                },
            );
            diagnostics.route_changed("attach", None, "authenticated");
            remote
        };
        drop(identity);
        // Replies are demultiplexed by the socket reader while reconciliation waits.
        // A failed outcome inspection keeps this authenticated route available.
        let _ = remote.reconcile_for_generation(generation).await;
        let mut routes = self.routes.lock().await;
        let route = routes
            .get_mut(&params.project_id)
            .filter(|route| route.socket == socket && route.generation == generation)
            .ok_or_else(|| WireError::new("route_replaced", "Host route was replaced"))?;
        let readiness = remote.readiness();
        route.ready = readiness["ready"] == true;
        Ok(
            json!({"project_id":params.project_id,"connection_generation":generation.to_string(),
            "connected":readiness["connected"],"reconciling":readiness["reconciling"],"ready":route.ready,
            "scope":scope,"reconciliation_error":readiness["cause"],"readiness":readiness}),
        )
    }
    #[cfg(test)]
    pub(crate) async fn receive_host(
        &self,
        principal: &Principal,
        socket: &str,
        project: &str,
        generation: u64,
        frame: HostFrame,
    ) -> Result<(), WireError> {
        let routes = self.routes.lock().await;
        let route = routes
            .get(project)
            .filter(|route| {
                route.owner == principal.owner
                    && route.socket == socket
                    && route.generation == generation
            })
            .ok_or_else(unavailable)?;
        if !self.allows_project(principal, project) {
            return Err(unavailable());
        }
        // The application socket only carries bridge replies/streams, never requests,
        // credentials, invented session identities or controller commands.
        if !matches!(
            &frame,
            HostFrame::Response { .. }
                | HostFrame::Receipt { .. }
                | HostFrame::TransferChunk { .. }
                | HostFrame::ProcessChanged { .. }
                | HostFrame::Diagnostic { .. }
                | HostFrame::Pong { .. }
        ) {
            return Err(WireError::new(
                "invalid_host_frame",
                "Expected a bridge response",
            ));
        }
        route
            .remote
            .receive(generation, frame)
            .map_err(|_| WireError::new("invalid_host_frame", "Host reply was rejected"))
    }
    pub(crate) async fn poll_socket_routes(&self, socket: &str) {
        let failed_conversations: Vec<_> = self
            .conversation_routes
            .lock()
            .await
            .iter()
            .filter(|(_, route)| {
                route.socket == socket && route.ready && !route.remote.is_connected()
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in failed_conversations {
            self.detach_conversation_socket(socket, Some(&id)).await;
        }
    }
    #[cfg(test)]
    pub(crate) async fn detach_socket(&self, socket: &str, project: Option<&str>) {
        self.detach_socket_with_cause(
            socket,
            project,
            microcrab_host_protocol::ErrorCode::Disconnected,
            "Host disconnected; mutation outcome may be unknown",
        )
        .await;
    }
    pub(crate) async fn detach_socket_with_cause(
        &self,
        socket: &str,
        project: Option<&str>,
        code: microcrab_host_protocol::ErrorCode,
        message: &'static str,
    ) {
        let detached: Vec<String> = {
            let mut routes = self.routes.lock().await;
            routes
                .iter_mut()
                .filter_map(|(id, route)| {
                    if route.socket == socket && project.is_none_or(|project| project == id) {
                        route.ready = false;
                        route
                            .remote
                            .detach_with_cause(route.generation, code.clone(), message);
                        if let Some(diagnostics) = &route.diagnostics {
                            diagnostics.route_changed("detach", Some(&code), "carrier_detached");
                        }
                        Some(id.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };
        if project.is_none() {
            self.detach_conversation_socket(socket, None).await;
        }
        for project in detached {
            self.suspend_project_with_reason(&project, super::cancellation::host_code(&code))
                .await;
        }
    }
    pub(super) async fn suspend_project(&self, project: &str) {
        self.suspend_project_with_reason(project, microcrab_session::HostDetachCode::Unavailable)
            .await;
    }
    pub(super) async fn suspend_project_with_reason(
        &self,
        project: &str,
        code: microcrab_session::HostDetachCode,
    ) {
        self.suspend_slots_with_reason(
            Some(project),
            None,
            microcrab_session::InterruptReason::HostDetached { code },
        )
        .await;
    }
    pub(super) async fn suspend_slots(&self, project: Option<&str>, conversation: Option<&str>) {
        self.suspend_slots_with_reason(
            project,
            conversation,
            microcrab_session::InterruptReason::HostDetached {
                code: microcrab_session::HostDetachCode::Disconnected,
            },
        )
        .await;
    }
    async fn suspend_slots_with_reason(
        &self,
        project: Option<&str>,
        conversation: Option<&str>,
        reason: microcrab_session::InterruptReason,
    ) {
        // Suspension gates Session admission; it does not itself prove transport loss.
        // Carry the existing remote owner's generation-scoped facts to the subscriber.
        let readiness = if let Some(project) = project {
            self.routes
                .lock()
                .await
                .get(project)
                .map(|route| route.remote.readiness())
        } else if let Some(conversation) = conversation {
            self.conversation_routes
                .lock()
                .await
                .get(conversation)
                .map(|route| route.remote.readiness())
        } else {
            None
        };
        let cause = super::cancellation::wire(Some(&reason));
        let notice = json!({"suspended":true,"readiness":readiness,"error":cause});
        let slots: Vec<_> = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter(|slot| {
                project.is_some_and(|project| slot.project_id.as_deref() == Some(project))
                    || conversation
                        .is_some_and(|id| slot.project_id.is_none() && slot.session_id == id)
            })
            .cloned()
            .collect();
        for slot in slots {
            // Mark admission synchronously; an activation awaiting IO must observe
            // this before publishing a usable runtime. The socket reader must not
            // wait on that activation: it may need replies from this same socket.
            slot.pending_suspension
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get_or_insert(reason);
            if let Ok(active) = slot.active.try_lock() {
                if slot.take_pending_suspension(active.as_deref())
                    && let Some(managed) = active.as_ref()
                {
                    managed
                        .hub
                        .publish(super::SessionRuntimeEvent::HostDetached(notice.clone()));
                }
                continue;
            }
            let notice = notice.clone();
            let _ = self.track(async move {
                let active = slot.active.lock().await;
                if slot.take_pending_suspension(active.as_deref())
                    && let Some(managed) = active.as_ref()
                {
                    managed
                        .hub
                        .publish(super::SessionRuntimeEvent::HostDetached(notice.clone()));
                }
            });
        }
    }
    pub(super) fn retire_project(&self, project: &str) {
        let slots: Vec<_> = self
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(_, slot)| slot.project_id.as_deref() == Some(project))
            .map(|(id, slot)| (id.clone(), slot.clone()))
            .collect();
        let Some(manager) = self.self_ref.upgrade() else {
            return;
        };
        let _ = self.track(async move {
            for (id, slot) in slots {
                let managed = slot.active.lock().await.clone();
                if let Some(managed) = managed
                    && manager.close_managed(&managed).await.is_err()
                {
                    tracing::warn!("Removed project runtime cleanup failed");
                    continue;
                }
                let mut registry = manager.registry.lock().unwrap_or_else(|e| e.into_inner());
                if registry
                    .get(&id)
                    .is_some_and(|current| Arc::ptr_eq(current, &slot))
                {
                    registry.remove(&id);
                }
            }
        });
    }
    pub(super) async fn project_host(
        &self,
        owner: &str,
        project: &str,
    ) -> Result<Arc<dyn Host>, WireError> {
        if !self.project_allowed(owner, project) {
            return Err(unavailable());
        }
        if self.embedded {
            return Ok(self.host.clone());
        }
        let routes = self.routes.lock().await;
        let route = routes.get(project).filter(|route| route.owner == owner);
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
            .unwrap_or("The pinned project Host has no ready route. Inspect its connection and operation receipts, then explicitly retry the rejected action. Reconnection does not replay actions.");
        let mut error = WireError::new("host_detached", message);
        error.details = Some(Box::new(
            json!({"project_id":project,"readiness":readiness}),
        ));
        Err(error)
    }
    pub(super) async fn session_host(
        &self,
        record: &microcrab_storage::SessionRecord,
    ) -> Result<Arc<dyn Host>, microcrab_session::Error> {
        if self.embedded {
            return Ok(self.host.clone());
        }
        if record.project_id.is_none() {
            let pin = self
                .pins
                .conversation_session(&record.id)
                .await?
                .ok_or_else(|| {
                    microcrab_session::Error::config("Conversation execution pin missing")
                })?;
            if pin.session_id != record.id
                || pin.conversation.owner != record.owner
                || pin.conversation.conversation_id != record.id
                || pin.conversation.workspace_root != record.project_root
                || self.pins.conversation(&record.id).await?.as_ref() != Some(&pin.conversation)
            {
                return Err(microcrab_session::Error::config(
                    "Conversation execution identity mismatch",
                ));
            }
            return self
                .conversation_host(&record.owner, &record.id)
                .await
                .map_err(|error| host_route_error(error, "Conversation Host has no ready route; inspect its connection and operation receipts before explicitly retrying"));
        }
        let project = record
            .project_id
            .as_deref()
            .ok_or_else(|| microcrab_session::Error::config("Project identity missing"))?;
        let pin = self.pins.session(&record.id).await?.ok_or_else(|| {
            microcrab_session::Error::config(
                "session has no execution identity; legacy history cannot be rebound",
            )
        })?;
        if pin.project.owner != record.owner
            || Some(pin.project.project_id.as_str()) != record.project_id.as_deref()
            || pin.project.workspace_root != record.project_root
            || self.pins.project(project).await?.as_ref() != Some(&pin.project)
        {
            return Err(microcrab_session::Error::config(
                "session execution identity mismatch",
            ));
        }
        self.project_host(&record.owner, project)
            .await
            .map_err(|error| host_route_error(error, "Project Host has no ready route; inspect its connection and operation receipts before explicitly retrying"))
    }
    pub(super) async fn project_visible(&self, record: &microcrab_storage::SessionRecord) -> bool {
        let Some(project) = record.project_id.as_deref() else {
            return self
                .pins
                .conversation_session(&record.id)
                .await
                .ok()
                .flatten()
                .is_some_and(|pin| {
                    pin.session_id == record.id
                        && pin.conversation.owner == record.owner
                        && pin.conversation.conversation_id == record.id
                        && pin.conversation.workspace_root == record.project_root
                });
        };
        let allowed = self.projects.find(project).await.is_some_and(|project| {
            // Project archival stops new execution; retained session history
            // remains readable to its existing owners without reviving access.
            project
                .owners
                .iter()
                .any(|owner| owner == "*" || owner == &record.owner)
        });
        if !allowed {
            return false;
        }
        if self.embedded {
            return true;
        }
        match self.pins.session(&record.id).await {
            Ok(Some(pin)) => {
                pin.project.owner == record.owner
                    && Some(pin.project.project_id.as_str()) == record.project_id.as_deref()
                    && pin.project.workspace_root == record.project_root
            }
            // Legacy records may be inspected but can never be activated remotely.
            Ok(None) => true,
            Err(_) => false,
        }
    }
}
