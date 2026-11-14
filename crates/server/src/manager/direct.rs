//! Native Host carriers share protobuf frames. Outbound WSS leaves the browser
//! after authenticated bootstrap; gRPC dials a reachable Bridge. Explicit browser
//! relay retains its socket lifetime. Project gates serialize publication only;
//! IO never holds the route registry gate. Pairing capabilities stay private.
mod credentials;
mod diagnostics;
pub(crate) mod outbound;
pub(super) use diagnostics::Correlation;
use diagnostics::StopReason;
use tracing::{Instrument, instrument::WithSubscriber};
#[cfg(feature = "grpc")]
mod grpc;
use super::{Manager, bindings};
use crate::auth::Principal;
use microcrab_host_protocol::{
    HostDescriptor, HostFrame, MAX_FRAME_BYTES, decode_frame, encode_frame,
};
use microcrab_protocol::application::WireError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(super) struct Hosts {
    entries: Mutex<HashMap<String, Arc<AsyncMutex<Connection>>>>,
    persistence: AsyncMutex<()>,
    outbound: outbound::Attempts,
}
#[derive(Default)]
struct Connection {
    pending_outbound: Option<uuid::Uuid>,
    stop: Option<CancellationToken>,
    worker: Option<tokio::task::JoinHandle<()>>,
    detach: Option<(mpsc::Sender<HostFrame>, String)>,
    relay: Option<RelayIdentity>,
    // Last authenticated witness survives worker retirement for diagnostics only.
    diagnostics: Option<Correlation>,
}
struct RelayIdentity {
    app_socket: String,
    id: uuid::Uuid,
    generation: u64,
}
impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(stop) = &self.stop {
            stop.cancel();
        }
    }
}
/// One browser relay intake. Binary frames are copied into `sender` only while
/// this id is still installed, so a replacement relay cannot clear the new pipe.
pub(crate) struct RelayBytes {
    pub id: uuid::Uuid,
    pub sender: mpsc::Sender<Vec<u8>>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Credential {
    owner: String,
    url: String,
    directory: Option<String>,
    credential: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Connect {
    pub project_id: String,
    pub url: String,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub directory: Option<String>,
    #[serde(default)]
    pub allow_legacy_binding: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Disconnect {
    pub project_id: String,
    #[serde(default)]
    pub revoke: bool,
    #[serde(default)]
    pub relay_release: Option<RelayRelease>,
}
/// Conditional cleanup targets one authenticated relay publication. This is
/// separate from diagnostic metadata and cannot authorize Project access.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RelayRelease {
    pub relay_id: uuid::Uuid,
    pub relay_generation: String,
}
fn failure(message: impl Into<String>) -> WireError {
    WireError::new("host_transport_unavailable", message)
}
/// Carrier loops only fail. Naming the success type keeps those `Err` branches inferable.
fn carrier_stop(message: &'static str) -> Result<(), &'static str> {
    Err(message)
}
enum Carrier {
    /// Browser opens this loopback bridge. The server does not dial it.
    Relay(String),
    /// Server dials this reachable bridge.
    Grpc(String),
}
fn classify(value: &str) -> Result<Carrier, WireError> {
    let mut url = url::Url::parse(value)
        .map_err(|error| failure(format!("Host address is not a URL: {error}")))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(failure(
            "Host address cannot include credentials, a query, or a fragment",
        ));
    }
    let host = url.host_str().unwrap_or("").trim_matches(['[', ']']);
    let loopback = host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    match url.scheme() {
        "ws" | "wss" => {
            if !loopback {
                return Err(failure(
                    "Host WebSocket relay requires a loopback address because the bridge socket stays local to the client",
                ));
            }
            if url.path() == "/" {
                url.set_path("/host");
            }
            if url.path() != "/host" {
                return Err(failure("Host WebSocket relay path must be /host"));
            }
            Ok(Carrier::Relay(url.into()))
        }
        "http" | "https" => {
            if url.path() != "/" && !url.path().is_empty() {
                return Err(failure("gRPC host address cannot include a path"));
            }
            url.set_path("");
            let dial = url.as_str().trim_end_matches('/').to_owned();
            Ok(Carrier::Grpc(dial))
        }
        _ => Err(failure(
            "Host address must be a ws:// relay URL or an http:// gRPC URL",
        )),
    }
}
impl Hosts {
    fn entry(&self, project: &str) -> Arc<AsyncMutex<Connection>> {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(project.into())
            .or_default()
            .clone()
    }
}
fn server_origin(redirect_uri: &str) -> Result<String, WireError> {
    Ok(url::Url::parse(redirect_uri)
        .map_err(|_| failure("Server authentication redirect URI has no valid origin"))?
        .origin()
        .ascii_serialization())
}
struct AcceptedHello {
    descriptor: HostDescriptor,
    credential: String,
    lease_generation: String,
}
fn accept_hello(
    hello: HostFrame,
    pin: Option<&microcrab_storage::execution::ProjectBinding>,
) -> Result<AcceptedHello, WireError> {
    let HostFrame::Hello {
        descriptor,
        credential,
        lease_generation,
    } = hello
    else {
        return Err(failure("Native Host must return authenticated Hello"));
    };
    if descriptor.protocol_version != microcrab_host_protocol::PROTOCOL_VERSION
        || microcrab_host_protocol::descriptor_fingerprint(&descriptor)
            .map_err(|_| failure("Host descriptor cannot be fingerprinted"))?
            != descriptor.descriptor_digest
    {
        return Err(failure(
            "Native Host descriptor fingerprint or protocol version differs",
        ));
    }
    if let Some(pin) = pin {
        if descriptor.host_id != pin.host_id
            || descriptor.workspace_id != pin.workspace_id
            || descriptor.workspace_root != pin.workspace_root
            || !microcrab_host_protocol::descriptor_matches_pin(&descriptor, &pin.descriptor_digest)
                .map_err(|_| failure("Native Host descriptor digest is invalid"))?
        {
            return Err(WireError::new(
                "binding_mismatch",
                "Native Host identity or descriptor differs from the immutable Project pin",
            ));
        }
    }
    Ok(AcceptedHello {
        descriptor,
        credential,
        lease_generation,
    })
}
async fn retire_connection(connection: &mut Connection) {
    connection.relay = None;
    connection.detach = None;
    if let Some(stop) = connection.stop.take() {
        stop.cancel();
    }
    if let Some(worker) = connection.worker.take() {
        if let Err(error) = worker.await {
            let error_code = if error.is_cancelled() {
                "worker_cancelled"
            } else {
                "worker_panicked"
            };
            tracing::warn!(
                error_code,
                "Native Host worker could not complete retirement"
            );
            if let Some(diagnostics) = &connection.diagnostics {
                diagnostics.stopped(StopReason::StopCancelled, error_code);
            }
        }
    }
}
struct RelayGuard<'a> {
    relay: &'a Mutex<Option<RelayBytes>>,
    id: uuid::Uuid,
    armed: bool,
}
impl RelayGuard<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for RelayGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            clear_relay(self.relay, self.id);
        }
    }
}
fn clear_relay(relay: &Mutex<Option<RelayBytes>>, id: uuid::Uuid) {
    if let Ok(mut guard) = relay.lock() {
        if guard.as_ref().is_some_and(|item| item.id == id) {
            *guard = None;
        }
    }
}
fn spawn_relay_worker(
    outbound_bytes: mpsc::Sender<Vec<u8>>,
    mut inbound_bytes: mpsc::Receiver<Vec<u8>>,
    mut outgoing: mpsc::Receiver<HostFrame>,
    stop: CancellationToken,
    weak: std::sync::Weak<super::Manager>,
    owner: String,
    project: String,
    socket_id: String,
    generation: u64,
    relay: Arc<Mutex<Option<RelayBytes>>>,
    relay_id: uuid::Uuid,
    diagnostics: Correlation,
) -> tokio::task::JoinHandle<()> {
    let parent = tracing::Span::current();
    tokio::spawn(async move {
        let writer = async {
            while let Some(frame) = outgoing.recv().await {
                let bytes = match encode_frame(&frame) {
                    Ok(bytes) if bytes.len() <= MAX_FRAME_BYTES => bytes,
                    Ok(_) => {
                        return carrier_stop(
                            "Outgoing Host relay frame exceeds the protocol byte limit",
                        );
                    }
                    Err(_) => {
                        tracing::warn!(error_code = "frame_encode_failed", "Cannot encode Host relay frame");
                        return carrier_stop("Cannot encode outgoing Host relay frame");
                    }
                };
                tokio::time::timeout(Duration::from_secs(10), outbound_bytes.send(bytes))
                    .await
                    .map_err(|_| "Host relay socket send exceeded 10 seconds")?
                    .map_err(|_| "Host relay socket send failed")?;
            }
            carrier_stop("Host relay outgoing route closed")
        };
        let reader = async {
            while let Some(bytes) = inbound_bytes.recv().await {
                let frame = decode_frame(&bytes)
                    .map_err(|_| "Host relay frame does not match the protocol schema")?;
                if !bindings::is_reply(&frame) {
                    return carrier_stop(
                        "Host relay sent a controller request instead of a response",
                    );
                }
                let Some(manager) = weak.upgrade() else {
                    return carrier_stop("Server native Host owner closed");
                };
                let routes = manager.routes.lock().await;
                if let Some(route) = routes.get(&project).filter(|route| {
                    route.socket == socket_id
                        && route.generation == generation
                        && route.owner == owner
                }) {
                    route.remote.receive(generation, frame).map_err(
                        |_| "Host relay reply correlation, receipt or transfer validation failed",
                    )?;
                }
            }
            carrier_stop("Host relay transport reached end of stream")
        };
        let health = async {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                let Some(manager) = weak.upgrade() else {
                    return;
                };
                let routes = manager.routes.lock().await;
                match routes.get(&project) {
                    Some(route) if route.socket == socket_id && route.generation == generation => {
                        if !route.remote.is_connected() {
                            return;
                        }
                    }
                    Some(route) if route.generation > generation => return,
                    _ => continue,
                }
            }
        };
        let (stop_reason, reason) = tokio::select! {
            biased;
            _ = stop.cancelled() => (StopReason::StopCancelled, Some("Native Host connection was explicitly disconnected or replaced")),
            result = writer => (StopReason::WriterErr, result.err()),
            result = reader => {
                let reason = result.err();
                let stopped = if reason == Some("Host relay transport reached end of stream") {
                    StopReason::ReaderEof
                } else {
                    StopReason::ReaderErr
                };
                (stopped, reason)
            },
            _ = health => (StopReason::HealthUnhealthy, None),
        };
        diagnostics.stopped(stop_reason, reason.unwrap_or("Host route is no longer healthy"));
        clear_relay(&relay, relay_id);
        if let Some(manager) = weak.upgrade() {
            if let Some(reason) = reason {
                manager
                    .detach_socket_with_cause(
                        &socket_id,
                        Some(&project),
                        microcrab_host_protocol::ErrorCode::Disconnected,
                        reason,
                    )
                    .await;
            } else {
                let current = manager
                    .routes
                    .lock()
                    .await
                    .get(&project)
                    .is_some_and(|route| {
                        route.socket == socket_id && route.generation == generation
                    });
                if current {
                    manager.suspend_project(&project).await;
                }
            }
        }
    }.instrument(parent).with_current_subscriber())
}

impl Manager {
    pub(crate) async fn connect_direct_host(
        &self,
        principal: &Principal,
        params: Connect,
    ) -> Result<Value, WireError> {
        self.editable_project(&principal.owner, &params.project_id)
            .await?;
        let entry = self.direct_hosts.entry(&params.project_id);
        let mut connection = entry.lock().await;
        self.connect_direct_host_locked(principal, params, &mut connection, false)
            .await
    }
    async fn connect_direct_host_locked(
        &self,
        principal: &Principal,
        params: Connect,
        connection: &mut Connection,
        force: bool,
    ) -> Result<Value, WireError> {
        let carrier = classify(&params.url)?;
        connection.pending_outbound = None;
        match carrier {
            Carrier::Relay(_) => Err(failure(
                "Host WebSocket is a client relay. Open the bridge from this browser and attach it with host.relay_attach; the server does not dial that address.",
            )),
            Carrier::Grpc(url) => {
                #[cfg(feature = "grpc")]
                {
                    self.connect_grpc_locked(principal, params, url, connection, force)
                        .await
                }
                #[cfg(not(feature = "grpc"))]
                {
                    let _ = (principal, params, url, connection, force);
                    Err(failure(
                        "This server build does not include the gRPC host carrier",
                    ))
                }
            }
        }
    }
    #[cfg(feature = "grpc")]
    async fn connect_grpc_locked(
        &self,
        principal: &Principal,
        params: Connect,
        url: String,
        connection: &mut Connection,
        force: bool,
    ) -> Result<Value, WireError> {
        let saved = self.matching_credential(principal, &params, &url).await?;
        if !force && params.token.as_ref().is_none_or(|token| token.is_empty()) && saved.is_some() {
            if let Some(mut state) = self
                .observe_direct_route(principal, &params.project_id)
                .await?
            {
                state["transport"] = json!(if self
                    .routes
                    .lock()
                    .await
                    .get(&params.project_id)
                    .is_some_and(|route| route.socket.starts_with("direct:wss:"))
                {
                    "outbound_wss"
                } else {
                    "grpc"
                });
                if let Some(diagnostics) = &connection.diagnostics {
                    diagnostics.connect("observe_existing", false, force);
                }
                return Ok(state);
            }
        }
        let (handshake, pin) = self.handshake(principal, &params, saved.as_ref()).await?;
        let origin = server_origin(&self.config.auth.redirect_uri)?;
        let (link, hello) = grpc::dial(&url, &origin, &handshake).await?;
        let accepted = accept_hello(hello, pin.as_ref())?;
        self.save_direct_credential(
            &params.project_id,
            Some(Credential {
                owner: principal.owner.clone(),
                url,
                directory: params.directory.clone(),
                credential: accepted.credential.clone(),
            }),
        )
        .await?;
        retire_connection(connection).await;
        let generation = self.next_connection_generation();
        let socket_id = format!("direct:{}", uuid::Uuid::new_v4());
        let diagnostics = Correlation::new(
            &params.project_id,
            &socket_id,
            generation,
            &accepted.lease_generation,
            &accepted.descriptor.bridge_instance_id,
        );
        diagnostics.connect(
            "dial_new",
            params.token.as_ref().is_some_and(|token| !token.is_empty()),
            force,
        );
        connection.diagnostics = Some(diagnostics.clone());
        let stop = CancellationToken::new();
        let (outbound, outgoing) = mpsc::channel::<HostFrame>(64);
        let worker = grpc::spawn_worker(
            link,
            outgoing,
            stop.clone(),
            self.self_ref.clone(),
            principal.owner.clone(),
            params.project_id.clone(),
            socket_id.clone(),
            generation,
            diagnostics,
        );
        self.publish_carrier(
            principal, connection, params, accepted, outbound, stop, worker, generation, socket_id,
            "grpc",
        )
        .await
    }
    /// Browser relay. The server emits Pair or Resume on the caller's socket and
    /// never dials the loopback bridge. Closing that socket drops the relay.
    pub(crate) async fn attach_client_relay(
        &self,
        principal: &Principal,
        app_socket: &str,
        params: Connect,
        outbound_bytes: mpsc::Sender<Vec<u8>>,
        mut inbound_bytes: mpsc::Receiver<Vec<u8>>,
        relay: Arc<Mutex<Option<RelayBytes>>>,
        relay_id: uuid::Uuid,
    ) -> Result<Value, WireError> {
        let mut intake = RelayGuard {
            relay: &relay,
            id: relay_id,
            armed: true,
        };
        let Carrier::Relay(url) = classify(&params.url)? else {
            return Err(failure(
                "host.relay_attach accepts only a WebSocket bridge address",
            ));
        };
        self.editable_project(&principal.owner, &params.project_id)
            .await?;
        let entry = self.direct_hosts.entry(&params.project_id);
        let mut connection = entry.lock().await;
        connection.pending_outbound = None;
        let saved = self.matching_credential(principal, &params, &url).await?;
        let (handshake, pin) = self.handshake(principal, &params, saved.as_ref()).await?;
        let bytes = encode_frame(&handshake)
            .map_err(|error| failure(format!("Cannot encode Host handshake: {}", error.message)))?;
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(failure("Host handshake exceeds the protocol byte limit"));
        }
        tokio::time::timeout(Duration::from_secs(10), outbound_bytes.send(bytes))
            .await
            .map_err(|_| failure("Host relay handshake exceeded 10 seconds"))?
            .map_err(|_| failure("Host relay disconnected while sending handshake"))?;
        let hello_bytes = tokio::time::timeout(Duration::from_secs(10), inbound_bytes.recv())
            .await
            .map_err(|_| failure("Host relay handshake exceeded 10 seconds"))?
            .ok_or_else(|| failure("Host relay closed before authenticated Hello"))?;
        let hello = decode_frame(&hello_bytes).map_err(|error| {
            failure(format!(
                "Host relay returned an invalid handshake frame: {}",
                error.message
            ))
        })?;
        let accepted = accept_hello(hello, pin.as_ref())?;
        self.save_direct_credential(
            &params.project_id,
            Some(Credential {
                owner: principal.owner.clone(),
                url,
                directory: params.directory.clone(),
                credential: accepted.credential.clone(),
            }),
        )
        .await?;
        retire_connection(&mut connection).await;
        let generation = self.next_connection_generation();
        let socket_id = format!("relay:{app_socket}");
        let diagnostics = Correlation::new(
            &params.project_id,
            &socket_id,
            generation,
            &accepted.lease_generation,
            &accepted.descriptor.bridge_instance_id,
        );
        diagnostics.relay_attach();
        connection.diagnostics = Some(diagnostics.clone());
        let stop = CancellationToken::new();
        let (outbound, outgoing) = mpsc::channel::<HostFrame>(64);
        let worker = spawn_relay_worker(
            outbound_bytes,
            inbound_bytes,
            outgoing,
            stop.clone(),
            self.self_ref.clone(),
            principal.owner.clone(),
            params.project_id.clone(),
            socket_id.clone(),
            generation,
            Arc::clone(&relay),
            relay_id,
            diagnostics,
        );
        // Install the exact worker witness before attach_project can yield. A
        // canceled attach clears its byte slot, but socket cleanup still owns
        // this identity and must be able to retire an unfinished publication.
        connection.relay = Some(RelayIdentity {
            app_socket: app_socket.to_owned(),
            id: relay_id,
            generation,
        });
        let mut published = self
            .publish_carrier(
                principal,
                &mut connection,
                params,
                accepted,
                outbound,
                stop,
                worker,
                generation,
                socket_id,
                "relay",
            )
            .await?;
        if published["status"] != "existing_project" {
            published["relay_release"] = json!(RelayRelease {
                relay_id,
                relay_generation: generation.to_string(),
            });
        }
        intake.disarm();
        Ok(published)
    }
    /// A stale browser may close after another browser has replaced this Project's
    /// carrier. Only the exact published relay may retire its worker.
    pub(crate) async fn stop_client_relay(
        &self,
        project: &str,
        app_socket: &str,
        relay_id: uuid::Uuid,
    ) {
        let entry = self.direct_hosts.entry(project);
        let mut connection = entry.lock().await;
        let Some(identity) = connection
            .relay
            .as_ref()
            .filter(|identity| identity.app_socket == app_socket && identity.id == relay_id)
        else {
            return;
        };
        let relay = self.routes.lock().await.get(project).is_none_or(|route| {
            // Publication can be canceled before installing its route. The
            // connection still owns that new worker; an absent/older route must
            // not prevent cleanup. A current/newer route needs the exact witness.
            route.generation < identity.generation
                || (route.socket == format!("relay:{app_socket}")
                    && route.generation == identity.generation)
        });
        if relay {
            retire_connection(&mut connection).await;
        }
    }
    pub(super) async fn matching_credential(
        &self,
        principal: &Principal,
        params: &Connect,
        url: &str,
    ) -> Result<Option<Credential>, WireError> {
        let Some(saved) = self.load_direct_credential(&params.project_id).await? else {
            return Ok(None);
        };
        if saved.owner != principal.owner || saved.url != url {
            return Ok(None);
        }
        if saved.directory == params.directory {
            return Ok(Some(saved));
        }
        // Hello fixes the Project's canonical workspace root. Bootstrap uses
        // that root after the original request may have selected it with ".".
        // Only this owned immutable pin certifies correspondence; arbitrary
        // directory aliases never borrow a saved capability. Preserve its bytes.
        let pin = self
            .pins
            .project(&params.project_id)
            .await
            .map_err(super::safe)?;
        if pin.as_ref().is_some_and(|pin| {
            pin.owner == principal.owner
                && params.directory.as_deref() == Some(pin.workspace_root.as_str())
        }) {
            Ok(Some(saved))
        } else {
            Ok(None)
        }
    }
    async fn observe_direct_route(
        &self,
        principal: &Principal,
        project: &str,
    ) -> Result<Option<Value>, WireError> {
        let descriptor = {
            let routes = self.routes.lock().await;
            routes
                .get(project)
                .filter(|route| {
                    route.owner == principal.owner
                        && route.socket.starts_with("direct:")
                        && route.remote.is_connected()
                })
                .map(|route| route.remote.descriptor().clone())
        };
        let Some(descriptor) = descriptor else {
            return Ok(None);
        };
        let mut state = self.host_state(principal, project).await?;
        state["descriptor"] = json!(descriptor);
        Ok(Some(state))
    }
    async fn handshake(
        &self,
        principal: &Principal,
        params: &Connect,
        saved: Option<&Credential>,
    ) -> Result<
        (
            HostFrame,
            Option<microcrab_storage::execution::ProjectBinding>,
        ),
        WireError,
    > {
        let scope = self
            .binding_scope(&principal.owner, &params.project_id)
            .await?;
        let pin = self
            .pins
            .project(&params.project_id)
            .await
            .map_err(super::safe)?;
        let expected_host_id = pin.as_ref().map(|pin| pin.host_id.clone());
        let expected_workspace_id = pin.as_ref().map(|pin| pin.workspace_id.clone());
        let token = params.token.clone().filter(|token| !token.is_empty());
        let frame = if let Some(token) = token {
            HostFrame::Pair {
                token,
                scope,
                directory: params.directory.clone(),
                expected_host_id,
                expected_workspace_id,
            }
        } else if let Some(saved) = saved {
            HostFrame::Resume {
                credential: saved.credential.clone(),
                scope,
                directory: params.directory.clone(),
                expected_host_id,
                expected_workspace_id,
            }
        } else {
            HostFrame::Pair {
                token: String::new(),
                scope,
                directory: params.directory.clone(),
                expected_host_id,
                expected_workspace_id,
            }
        };
        Ok((frame, pin))
    }
    async fn publish_carrier(
        &self,
        principal: &Principal,
        connection: &mut Connection,
        params: Connect,
        accepted: AcceptedHello,
        outbound: mpsc::Sender<HostFrame>,
        stop: CancellationToken,
        worker: tokio::task::JoinHandle<()>,
        generation: u64,
        socket_id: String,
        transport: &str,
    ) -> Result<Value, WireError> {
        connection.stop = Some(stop);
        connection.worker = Some(worker);
        connection.detach = Some((outbound.clone(), accepted.lease_generation.clone()));
        let result = self
            .attach_project(
                principal,
                &socket_id,
                bindings::Attach {
                    project_id: params.project_id,
                    descriptor: accepted.descriptor.clone(),
                    lease_generation: accepted.lease_generation,
                    allow_legacy_binding: params.allow_legacy_binding,
                },
                outbound,
                generation,
            )
            .await;
        match result {
            Ok(mut result) if result["status"] != "existing_project" => {
                result["descriptor"] = json!(accepted.descriptor);
                result["transport"] = json!(transport);
                Ok(result)
            }
            result => {
                retire_connection(connection).await;
                result
            }
        }
    }
    pub(crate) async fn disconnect_direct_host(
        &self,
        principal: &Principal,
        params: Disconnect,
    ) -> Result<Value, WireError> {
        self.editable_project(&principal.owner, &params.project_id)
            .await?;
        let entry = self.direct_hosts.entry(&params.project_id);
        let mut connection = entry.lock().await;
        if let Some(expected) = &params.relay_release {
            // Compare under the same Project gate as connect/refresh. A delayed
            // tab cleanup must never retire another tab's replacement carrier.
            if expected.relay_generation.is_empty()
                || expected.relay_generation.len() > 20
                || !expected
                    .relay_generation
                    .bytes()
                    .all(|byte| byte.is_ascii_digit())
            {
                return Err(WireError::new(
                    "invalid_request",
                    "Relay release generation must be a bounded unsigned decimal integer",
                ));
            }
            let generation = expected.relay_generation.parse::<u64>().map_err(|_| {
                WireError::new(
                    "invalid_request",
                    "Relay release generation must be an unsigned integer",
                )
            })?;
            if generation == 0 {
                return Err(WireError::new(
                    "invalid_request",
                    "Relay release generation must be nonzero",
                ));
            }
            let matches = connection.relay.as_ref().is_some_and(|relay| {
                relay.id == expected.relay_id && relay.generation == generation
            });
            if !matches {
                tracing::info!(event = "host.disconnect", project_id = %params.project_id,
                    mode = "conditional_relay_release", disconnected = false,
                    "Stale native Host relay release ignored");
                return Ok(
                    json!({"project_id":params.project_id,"disconnected":false,"detach_requested":false}),
                );
            }
            if params.revoke {
                return Err(WireError::new(
                    "invalid_request",
                    "Conditional relay release cannot revoke credentials",
                ));
            }
        }
        connection.pending_outbound = None;
        let had_connection = connection.stop.is_some() || connection.worker.is_some();
        let mut detach_sent = false;
        if params.revoke {
            if let Some((outgoing, lease_generation)) = connection.detach.take() {
                detach_sent = tokio::time::timeout(
                    Duration::from_secs(2),
                    outgoing.send(HostFrame::Detach { lease_generation }),
                )
                .await
                .is_ok_and(|result| result.is_ok());
            }
            if detach_sent {
                if let Some(worker) = connection.worker.as_mut() {
                    if tokio::time::timeout(Duration::from_secs(3), worker)
                        .await
                        .is_ok()
                    {
                        connection.worker.take();
                    }
                }
            }
        }
        connection.detach = None;
        if let Some(stop) = connection.stop.take() {
            stop.cancel();
        }
        if let Some(worker) = connection.worker.take() {
            let _ = worker.await;
        }
        tracing::info!(event = "host.disconnect", project_id = %params.project_id,
            revoke = params.revoke, had_connection, detach_sent,
            old_lease_generation = connection.diagnostics.as_ref().map(Correlation::lease),
            "Native Host disconnect completed");
        if params.revoke {
            self.save_direct_credential(&params.project_id, None)
                .await?;
        }
        Ok(
            json!({"project_id":params.project_id,"connected":false,"ready":false,"disconnected":true,"detach_requested":detach_sent}),
        )
    }
    pub(crate) async fn refresh_direct_host(
        &self,
        principal: &Principal,
        project: String,
    ) -> Result<Value, WireError> {
        self.editable_project(&principal.owner, &project).await?;
        let entry = self.direct_hosts.entry(&project);
        let mut connection = entry.lock().await;
        let saved = self
            .load_direct_credential(&project)
            .await?
            .filter(|r| r.owner == principal.owner)
            .ok_or_else(|| {
                WireError::new("pairing_required", "No saved native Host connection exists")
            })?;
        let url = saved.url.clone();
        let directory = saved.directory.clone();
        match classify(&url)? {
            Carrier::Relay(_) => Err(WireError::new(
                "socket_required",
                "WebSocket relay refresh requires this browser to reattach the bridge",
            )),
            Carrier::Grpc(_) => {
                self.connect_direct_host_locked(
                    principal,
                    Connect {
                        project_id: project,
                        url,
                        token: None,
                        directory,
                        allow_legacy_binding: false,
                    },
                    &mut connection,
                    true,
                )
                .await
            }
        }
    }
    pub(super) async fn stop_direct_hosts(&self) {
        self.direct_hosts.outbound.stop();
        let entries: Vec<_> = self
            .direct_hosts
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        for entry in entries {
            let mut connection = entry.lock().await;
            if let Some(stop) = connection.stop.take() {
                stop.cancel();
            }
            if let Some(worker) = connection.worker.take() {
                let _ = worker.await;
            }
        }
    }
}
