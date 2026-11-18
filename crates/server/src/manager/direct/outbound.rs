//! Bounded bootstrap. The native socket owns route lifetime, never the browser.
use super::*;
use axum::{
    extract::ws::{Message, WebSocket},
    http::HeaderMap,
};
use futures_util::{SinkExt, StreamExt};
use std::time::Instant;
use tokio::sync::watch;
#[cfg(test)]
mod tests;

const TTL: Duration = Duration::from_secs(30);
#[derive(Default)]
pub(super) struct Attempts(Mutex<HashMap<uuid::Uuid, Arc<Attempt>>>);
impl Attempts {
    pub(super) fn stop(&self) {
        for attempt in self.0.lock().unwrap_or_else(|e| e.into_inner()).values() {
            attempt.fail("Server stopped before outbound authentication completed");
        }
    }
}
pub(crate) struct Attempt {
    id: uuid::Uuid,
    principal: Principal,
    params: Connect,
    deadline: Instant,
    capability: Mutex<Option<String>>,
    stop: CancellationToken,
    phase_gate: Mutex<bool>,
    auth_sent: std::sync::atomic::AtomicBool,
    result: watch::Sender<Option<Result<Value, WireError>>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AttemptParams {
    project_id: String,
    attempt_id: uuid::Uuid,
}
fn unavailable(message: impl Into<String>) -> WireError {
    let mut error = WireError::new("host_outbound_unavailable", message);
    error.details = Some(Box::new(
        json!({"phase":"pre_auth","pairing_token_consumed":false}),
    ));
    error
}
fn unknown() -> WireError {
    let mut error = WireError::new(
        "host_pairing_outcome_unknown",
        "Host authentication was sent but Hello was not confirmed; obtain a fresh local pairing token before retrying. Do not replay the one-time token",
    );
    error.details = Some(Box::new(
        json!({"phase":"auth_unknown","pairing_token_consumed":"unknown"}),
    ));
    error
}
impl Attempt {
    fn begin_auth(&self) -> Result<(), WireError> {
        let _phase = self.phase_gate.lock().unwrap_or_else(|e| e.into_inner());
        if self.stop.is_cancelled()
            || Instant::now() >= self.deadline
            || self.result.borrow().is_some()
        {
            return Err(self.failure("Outbound attempt cancelled or expired before authentication"));
        }
        self.auth_sent
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    /// Claim persistence/publication while holding the Project gate. Once Hello
    /// has won this boundary, expiry is no longer an authentication failure;
    /// observers must receive the actual persistence/publication outcome.
    fn begin_commit(&self) -> Result<(), WireError> {
        let mut committing = self.phase_gate.lock().unwrap_or_else(|e| e.into_inner());
        if self.stop.is_cancelled()
            || Instant::now() >= self.deadline
            || self.result.borrow().is_some()
        {
            return Err(
                self.failure("Outbound attempt expired or was cancelled before committing Hello")
            );
        }
        *committing = true;
        Ok(())
    }
    fn fail(&self, message: &str) -> bool {
        let committing = self.phase_gate.lock().unwrap_or_else(|e| e.into_inner());
        if *committing || self.result.borrow().is_some() {
            return false;
        }
        self.stop.cancel();
        self.complete(Err(self.failure(message)));
        true
    }
    fn failure(&self, message: &str) -> WireError {
        if self.auth_sent.load(std::sync::atomic::Ordering::SeqCst) {
            unknown()
        } else {
            unavailable(message)
        }
    }
    async fn wait_result(&self) -> Result<Value, WireError> {
        let mut results = self.result.subscribe();
        loop {
            if let Some(result) = results.borrow_and_update().clone() {
                return result;
            }
            tokio::select! {
                _ = tokio::time::sleep_until(self.deadline.into()) => {
                    if !self.fail("Outbound Bridge connection did not arrive before expiry") {
                        results.changed().await.map_err(|_| self.failure("Outbound commit owner closed"))?;
                    }
                }
                changed = results.changed() => { if changed.is_err() { return Err(self.failure("Outbound attempt owner closed")); } }
            }
        }
    }
    fn complete(&self, result: Result<Value, WireError>) {
        self.result.send_if_modified(|current| {
            if current.is_some() {
                false
            } else {
                *current = Some(result);
                true
            }
        });
    }
}
impl Manager {
    pub(crate) async fn start_outbound_host(
        &self,
        principal: &Principal,
        params: Connect,
    ) -> Result<Value, WireError> {
        self.editable_project(&principal.owner, &params.project_id)
            .await?;
        let Carrier::Relay(locator) = classify(&params.url)? else {
            return Err(unavailable(
                "Outbound signaling requires a loopback WebSocket bridge locator",
            ));
        };
        let mut url = url::Url::parse(&server_origin(&self.config.auth.redirect_uri)?)
            .map_err(|_| unavailable("Server origin is invalid"))?;
        let local = url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        let scheme = match url.scheme() {
            "https" => "wss",
            "http" if local => "ws",
            _ => {
                return Err(unavailable(
                    "Outbound Host transport requires HTTPS on the configured public Server origin",
                ));
            }
        };
        url.set_scheme(scheme)
            .map_err(|_| unavailable("Cannot construct Server WebSocket origin"))?;
        url.set_path("/api/v1/host/outbound");
        let entry = self.direct_hosts.entry(&params.project_id);
        let mut connection = entry.lock().await;
        if params.token.as_ref().is_none_or(|token| token.is_empty())
            && self
                .matching_credential(principal, &params, &locator)
                .await?
                .is_some()
            && let Some(mut state) = self
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
            return Ok(state);
        }
        let mut attempts = self
            .direct_hosts
            .outbound
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        attempts.retain(|_, a| {
            if Instant::now() >= a.deadline {
                if a.result.borrow().is_some() {
                    return false;
                }
                !a.fail("Outbound attempt expired before authentication")
            } else {
                true
            }
        });
        if attempts.len() >= 64
            || attempts
                .values()
                .filter(|a| a.principal.owner == principal.owner && a.result.borrow().is_none())
                .count()
                >= 4
        {
            return Err(unavailable(
                "Outbound attempt capacity reached; wait for a pending attempt to finish",
            ));
        }
        for old in attempts.values().filter(|a| {
            a.principal.owner == principal.owner
                && a.params.project_id == params.project_id
                && a.result.borrow().is_none()
        }) {
            old.fail("Outbound attempt superseded before authentication");
        }
        let id = uuid::Uuid::new_v4();
        connection.pending_outbound = Some(id);
        let capability = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let (result, _) = watch::channel(None);
        attempts.insert(
            id,
            Arc::new(Attempt {
                id,
                principal: principal.clone(),
                params,
                deadline: Instant::now() + TTL,
                capability: Mutex::new(Some(capability.clone())),
                stop: CancellationToken::new(),
                phase_gate: Mutex::new(false),
                auth_sent: std::sync::atomic::AtomicBool::new(false),
                result,
            }),
        );
        Ok(
            json!({"attempt_id":id,"capability":capability,"url":url.as_str(),"expires_in_ms":TTL.as_millis() as u64}),
        )
    }
    fn outbound_attempt(
        &self,
        principal: &Principal,
        params: &AttemptParams,
    ) -> Result<Arc<Attempt>, WireError> {
        self.direct_hosts
            .outbound
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&params.attempt_id)
            .filter(|a| {
                a.principal.owner == principal.owner && a.params.project_id == params.project_id
            })
            .cloned()
            .ok_or_else(|| {
                let mut error = unknown();
                error.message = "Outbound attempt evidence is absent or belongs to another Project or owner; safe token reuse cannot be certified. Obtain a fresh local pairing token".into();
                error
            })
    }
    pub(crate) async fn finish_outbound_host(
        &self,
        principal: &Principal,
        params: AttemptParams,
    ) -> Result<Value, WireError> {
        self.editable_project(&principal.owner, &params.project_id)
            .await?;
        let attempt = self.outbound_attempt(principal, &params)?;
        attempt.wait_result().await
    }
    pub(crate) async fn cancel_outbound_host(
        &self,
        principal: &Principal,
        params: AttemptParams,
    ) -> Result<Value, WireError> {
        self.editable_project(&principal.owner, &params.project_id)
            .await?;
        let attempt = self.outbound_attempt(principal, &params)?;
        // Authentication holds the Project publication gate across network IO.
        // The phase gate alone arbitrates cancellation against Hello commit.
        let cancelled = attempt.fail("Outbound attempt cancelled before authentication completed");
        Ok(
            json!({"cancelled":cancelled,"phase":if attempt.auth_sent.load(std::sync::atomic::Ordering::SeqCst) {"auth_unknown"} else {"pre_auth"}}),
        )
    }
    pub(crate) fn claim_outbound_host(
        &self,
        headers: &HeaderMap,
    ) -> Result<Arc<Attempt>, WireError> {
        if headers.get("origin").and_then(|v| v.to_str().ok())
            != Some(server_origin(&self.config.auth.redirect_uri)?.as_str())
        {
            return Err(unavailable(
                "Outbound Host Origin differs from configured Server origin",
            ));
        }
        let id = headers
            .get("x-host-attempt-id")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| uuid::Uuid::parse_str(v).ok())
            .ok_or_else(|| unavailable("Outbound attempt identity is missing or malformed"))?;
        let attempts = self
            .direct_hosts
            .outbound
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let attempt = attempts
            .get(&id)
            .filter(|a| Instant::now() < a.deadline && !a.stop.is_cancelled())
            .ok_or_else(|| unavailable("Outbound attempt expired or is absent"))?;
        let supplied = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or_else(|| unavailable("Outbound capability is missing"))?;
        let mut capability = attempt.capability.lock().unwrap_or_else(|e| e.into_inner());
        if capability.as_deref() != Some(supplied) {
            return Err(unavailable(
                "Outbound capability is invalid or already consumed",
            ));
        }
        *capability = None;
        Ok(attempt.clone())
    }
    pub(crate) async fn accept_outbound_host(&self, attempt: Arc<Attempt>, socket: WebSocket) {
        let result = self.authenticate_outbound(&attempt, socket).await;
        attempt.complete(result);
    }
    async fn authenticate_outbound(
        &self,
        attempt: &Attempt,
        mut socket: WebSocket,
    ) -> Result<Value, WireError> {
        let entry = self.direct_hosts.entry(&attempt.params.project_id);
        let mut connection = entry.lock().await;
        if attempt.stop.is_cancelled()
            || Instant::now() >= attempt.deadline
            || connection.pending_outbound != Some(attempt.id)
        {
            return Err(attempt.failure(
                "Outbound attempt cancelled, expired or superseded by another connection intent",
            ));
        }
        self.editable_project(&attempt.principal.owner, &attempt.params.project_id)
            .await?;
        let Carrier::Relay(url) = classify(&attempt.params.url)? else {
            return Err(unavailable("Invalid outbound locator"));
        };
        let saved = self
            .matching_credential(&attempt.principal, &attempt.params, &url)
            .await?;
        let (mut handshake, pin) = self
            .handshake(&attempt.principal, &attempt.params, saved.as_ref())
            .await?;
        let (request_id, inspect) = preflight::inspect(&handshake)?;
        let bytes =
            encode_frame(&inspect).map_err(|_| unavailable("Cannot encode Host inspection"))?;
        let probe = async {
            socket
                .send(Message::Binary(bytes.into()))
                .await
                .map_err(|_| unavailable("Outbound inspection send failed"))?;
            match socket.recv().await {
                Some(Ok(Message::Binary(bytes))) => decode_frame(&bytes)
                    .map_err(|_| unavailable("Outbound inspection reply is invalid")),
                _ => Err(unavailable(
                    "Outbound Host closed before inspection; pairing was not sent",
                )),
            }
        };
        let reply = tokio::select! {
            _ = attempt.stop.cancelled() => return Err(unavailable("Outbound inspection cancelled before pairing")),
            reply = tokio::time::timeout(Duration::from_secs(10), probe) => reply.map_err(|_| unavailable("Outbound inspection exceeded 10 seconds"))??,
        };
        let descriptor = preflight::accept(reply, &request_id, pin.as_ref())?;
        let _workspace = self
            .direct_hosts
            .admit_workspace(&attempt.principal.owner, &descriptor)
            .await;
        if let Some(existing) = self
            .preflight_candidate(&attempt.principal, &attempt.params, &descriptor)
            .await?
        {
            return Ok(existing);
        }
        preflight::guard(&mut handshake, &descriptor)?;
        let bytes = encode_frame(&handshake).map_err(|error| {
            unavailable(format!("Cannot encode Host handshake: {}", error.message))
        })?;
        // Mark before write: a partial write can have delivered a complete Pair.
        attempt.begin_auth()?;
        let exchange = async {
            socket
                .send(Message::Binary(bytes.into()))
                .await
                .map_err(|_| unknown())?;
            match socket.recv().await {
                Some(Ok(Message::Binary(bytes))) => decode_frame(&bytes).map_err(|_| unknown()),
                _ => Err(unknown()),
            }
        };
        let hello = tokio::select! { _ = attempt.stop.cancelled() => return Err(unknown()), result = tokio::time::timeout(Duration::from_secs(10), exchange) => result.map_err(|_| unknown())?? };
        let accepted = accept_hello(hello, pin.as_ref(), &descriptor)?;
        attempt.begin_commit()?;
        self.save_direct_credential(
            &attempt.params.project_id,
            Some(Credential {
                owner: attempt.principal.owner.clone(),
                url,
                directory: attempt.params.directory.clone(),
                credential: accepted.credential.clone(),
            }),
        )
        .await?;
        retire_connection(&mut connection).await;
        let generation = self.next_connection_generation();
        let socket_id = format!("direct:wss:{}", uuid::Uuid::new_v4());
        let diagnostics = Correlation::new(
            &attempt.params.project_id,
            &socket_id,
            generation,
            &accepted.lease_generation,
            &accepted.descriptor.bridge_instance_id,
        );
        diagnostics.connect(
            "outbound",
            attempt.params.token.as_ref().is_some_and(|v| !v.is_empty()),
            false,
        );
        connection.diagnostics = Some(diagnostics.clone());
        let stop = CancellationToken::new();
        let (outbound, outgoing) = mpsc::channel(64);
        let (outbound_bytes, mut bytes_rx) = mpsc::channel::<Vec<u8>>(8);
        let (bytes_tx, inbound_bytes) = mpsc::channel::<Vec<u8>>(8);
        let relay = Arc::new(Mutex::new(None));
        let worker = spawn_relay_worker(
            outbound_bytes,
            inbound_bytes,
            outgoing,
            stop.clone(),
            self.self_ref.clone(),
            attempt.principal.owner.clone(),
            attempt.params.project_id.clone(),
            socket_id.clone(),
            generation,
            relay,
            uuid::Uuid::new_v4(),
            diagnostics,
        );
        let wire_stop = stop.clone();
        tokio::spawn(async move {
            let (mut sink, mut stream) = socket.split();
            let writer = async {
                while let Some(bytes) = bytes_rx.recv().await {
                    tokio::time::timeout(
                        Duration::from_secs(10),
                        sink.send(Message::Binary(bytes.into())),
                    )
                    .await
                    .map_err(|_| ())?
                    .map_err(|_| ())?;
                }
                Ok::<(), ()>(())
            };
            let reader = async {
                while let Some(message) = stream.next().await {
                    match message {
                        Ok(Message::Binary(bytes)) if bytes.len() <= MAX_FRAME_BYTES => {
                            tokio::time::timeout(
                                Duration::from_secs(10),
                                bytes_tx.send(bytes.to_vec()),
                            )
                            .await
                            .map_err(|_| ())?
                            .map_err(|_| ())?;
                        }
                        Ok(Message::Ping(_) | Message::Pong(_)) => (),
                        _ => break,
                    }
                }
                Ok::<(), ()>(())
            };
            tokio::select! { _ = wire_stop.cancelled() => (), _ = writer => (), _ = reader => () }
            wire_stop.cancel();
        });
        let params = Connect {
            project_id: attempt.params.project_id.clone(),
            url: attempt.params.url.clone(),
            token: None,
            directory: attempt.params.directory.clone(),
            allow_legacy_binding: attempt.params.allow_legacy_binding,
        };
        let result = self
            .publish_carrier(
                &attempt.principal,
                &mut connection,
                params,
                accepted,
                outbound,
                stop,
                worker,
                generation,
                socket_id,
                "outbound_wss",
            )
            .await;
        attempt.complete(result.clone());
        result
    }
}
