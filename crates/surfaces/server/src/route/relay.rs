//! Stateless Bearer model relay; never assembles a Session or Host.
//! Supports both owner-scoped `user:*` providers and Server shared providers.
mod chat;
#[cfg(all(test, feature = "storage-file"))]
mod tests;

use crate::{
    Application,
    config::relay::RelayConfig,
    manager::{
        dashboard::{Model, Outcome, Terminal},
        providers::{RelayCandidate, relay_candidates},
        relay_keys::Credential,
    },
};
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware,
    response::{
        IntoResponse, Response,
        sse::{Event, Sse},
    },
    routing::{get, post},
};
use microcrab_auditor::{EmptyVariableView, NoopAuditor};
use microcrab_protocol::Usage;
use microcrab_transport::{
    Client, Content, ModelEvent, OpenAiCompatibleRequestOptions, OpenAiResponseRequestOptions,
    TransportClient, TransportConfig,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

pub(crate) struct Runtime {
    stop: CancellationToken,
    global: Arc<Semaphore>,
    accounts: Mutex<HashMap<String, usize>>,
}

impl Runtime {
    pub(crate) fn new(config: &RelayConfig) -> Self {
        Self {
            stop: CancellationToken::new(),
            global: Arc::new(Semaphore::new(config.max_concurrent)),
            accounts: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn stop(&self) {
        self.stop.cancel();
    }
}

struct Admission {
    state: Arc<Application>,
    credential: Credential,
    _slot: OwnedSemaphorePermit,
}

impl Drop for Admission {
    fn drop(&mut self) {
        let mut accounts = self
            .state
            .manager
            .relay_runtime
            .accounts
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let std::collections::hash_map::Entry::Occupied(mut entry) =
            accounts.entry(self.credential.owner.clone())
        {
            if *entry.get() == 1 {
                entry.remove();
            } else {
                *entry.get_mut() -= 1;
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct Failure(StatusCode, &'static str, &'static str);
impl Failure {
    fn bad(message: &'static str) -> Self {
        Self(StatusCode::BAD_REQUEST, "invalid_request", message)
    }

    fn key() -> Self {
        Self(
            StatusCode::UNAUTHORIZED,
            "invalid_api_key",
            "A valid relay Bearer is required without a browser session cookie",
        )
    }

    fn value(&self) -> Value {
        let kind = match self.0 {
            StatusCode::UNAUTHORIZED => "invalid_request_error",
            StatusCode::TOO_MANY_REQUESTS => "rate_limit_error",
            status if status.is_server_error() => "server_error",
            _ => "invalid_request_error",
        };
        json!({"error":{"message":self.2,"type":kind,"code":self.1}})
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (self.0, Json(self.value())).into_response()
    }
}

pub(super) fn router(state: Arc<Application>) -> Router {
    Router::new()
        .route("/v1/models", get(models))
        .route("/v1/chat/completions", post(completions))
        .route("/v1", axum::routing::any(missing))
        .route("/v1/{*path}", axum::routing::any(missing))
        .method_not_allowed_fallback(|| async {
            Failure(
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "Method is not supported",
            )
        })
        .with_state(state)
        .layer(middleware::from_fn(super::error::no_store))
}

async fn missing() -> Failure {
    Failure(
        StatusCode::NOT_FOUND,
        "not_found",
        "Relay endpoint does not exist",
    )
}

async fn authenticate(state: &Application, headers: &HeaderMap) -> Result<Credential, Failure> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let value = values
        .next()
        .and_then(|value| value.to_str().ok())
        .ok_or_else(Failure::key)?;
    let (scheme, secret) = value.split_once(' ').ok_or_else(Failure::key)?;
    if values.next().is_some()
        || !scheme.eq_ignore_ascii_case("bearer")
        || !secret.starts_with("mc_sk_")
        || crate::auth::has_session_cookie(headers).map_err(|_| Failure::key())?
    {
        return Err(Failure::key());
    }
    if headers.contains_key(header::ORIGIN) {
        return Err(Failure(
            StatusCode::FORBIDDEN,
            "origin_not_allowed",
            "Browser cross-origin relay is not supported",
        ));
    }
    let manager = &state.manager;
    if !manager.config.relay.enabled || manager.relay_runtime.stop.is_cancelled() {
        return Err(Failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "relay_unavailable",
            "Relay is not available",
        ));
    }
    // Account suspension and IdP deprovisioning checks are deliberately not part of v0.3.2.
    manager
        .relay_keys
        .lookup(secret)
        .await
        .map_err(|_| {
            Failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "key_store_unavailable",
                "Relay key authority is unavailable",
            )
        })?
        .ok_or_else(Failure::key)
}

fn route_id(candidate: &RelayCandidate) -> String {
    format!(
        "{}/{}",
        candidate.selection.provider_id, candidate.selection.model
    )
}

async fn models(
    State(state): State<Arc<Application>>,
    headers: HeaderMap,
) -> Result<Json<Value>, Failure> {
    let credential = authenticate(&state, &headers).await?;
    let candidates = relay_candidates(
        &state.manager,
        &credential.owner,
        credential.key.allowed_providers.as_deref(),
    )
    .await
    .map_err(|_| {
        Failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "catalog_unavailable",
            "Model catalog is unavailable",
        )
    })?;
    let data: Vec<_> = candidates
        .iter()
        .map(|candidate| {
            json!({
                "id": route_id(candidate),
                "object": "model",
                "created": 0,
                "owned_by": candidate.selection.provider_id,
            })
        })
        .collect();
    Ok(Json(json!({"object":"list","data":data})))
}

async fn completions(
    State(state): State<Arc<Application>>,
    request: Request,
) -> Result<Response, Failure> {
    let credential = authenticate(&state, request.headers()).await?;
    let outcome = prepare(state, credential, request).await?;
    if outcome.stream {
        Ok(outcome.streaming())
    } else {
        outcome.non_streaming().await
    }
}

struct Call {
    admission: Admission,
    id: String,
    route_id: String,
    model: Model,
    client: Box<dyn Client>,
    content: Content,
    cancellation: CancellationToken,
    deadline: Instant,
    stream: bool,
    include_usage: bool,
    created: u64,
    used_at_ms: u64,
}

fn select_candidate<'a>(
    candidates: &'a [RelayCandidate],
    requested_model: Option<&str>,
    key_default: Option<&microcrab_storage::ModelSelection>,
    account_default: Option<&microcrab_storage::ModelSelection>,
) -> Result<&'a RelayCandidate, Failure> {
    if let Some(model) = requested_model {
        if let Some(exact) = candidates
            .iter()
            .find(|candidate| route_id(candidate) == model)
        {
            return Ok(exact);
        }
        let mut matches = candidates
            .iter()
            .filter(|candidate| candidate.selection.model == model);
        let first = matches.next();
        if matches.next().is_some() {
            return Err(Failure::bad(
                "Model ID is ambiguous; use the provider/model ID returned by /v1/models",
            ));
        }
        first.ok_or(Failure(
            StatusCode::NOT_FOUND,
            "model_not_found",
            "No allowed model matches; supply an ID from /v1/models or set a key default",
        ))
    } else {
        key_default
            .or(account_default)
            .and_then(|selection| {
                candidates.iter().find(|candidate| {
                    candidate.selection.provider_id == selection.provider_id
                        && candidate.selection.model == selection.model
                })
            })
            .ok_or(Failure(
                StatusCode::NOT_FOUND,
                "model_not_found",
                "No allowed model matches; supply an ID from /v1/models or set a key default",
            ))
    }
}

fn build_client(
    mut transport: TransportConfig,
    google_function_response_media: bool,
    options: OpenAiCompatibleRequestOptions,
) -> Result<Box<dyn Client>, Failure> {
    transport.max_response_bytes = transport.max_response_bytes.min(8 * 1024 * 1024);
    let client = TransportClient::new(transport).map_err(|_| {
        Failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "provider_unavailable",
            "Provider route is unavailable",
        )
    })?;
    let client = match client {
        TransportClient::OpenAiCompatible(inner) => TransportClient::OpenAiCompatible(
            inner.with_request_options(options).map_err(|_| {
                Failure::bad("Unsupported reasoning_effort for OpenAI-compatible route")
            })?,
        ),
        TransportClient::OpenAiResponses(inner) => TransportClient::OpenAiResponses(
            inner
                .with_request_options(OpenAiResponseRequestOptions {
                    reasoning_effort: options.reasoning_effort,
                    prompt_cache_key: None,
                })
                .map_err(|_| {
                    Failure::bad("Unsupported reasoning_effort for OpenAI Responses route")
                })?,
        ),
        TransportClient::Google(inner) => {
            TransportClient::Google(inner.with_function_response_media(google_function_response_media))
        }
        TransportClient::Anthropic(inner) => TransportClient::Anthropic(inner),
    };
    Ok(Box::new(client))
}

async fn prepare(
    state: Arc<Application>,
    credential: Credential,
    request: Request,
) -> Result<Call, Failure> {
    let config = &state.manager.config;
    let runtime = &state.manager.relay_runtime;
    let limit = &config.relay;
    let overloaded = || {
        Failure(
            StatusCode::TOO_MANY_REQUESTS,
            "relay_busy",
            "Relay request capacity is exhausted",
        )
    };
    let slot = runtime
        .global
        .clone()
        .try_acquire_owned()
        .map_err(|_| overloaded())?;
    {
        let mut accounts = runtime
            .accounts
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let active = accounts.entry(credential.owner.clone()).or_default();
        if *active >= limit.max_concurrent_per_owner {
            return Err(overloaded());
        }
        *active += 1;
    }
    let admission = Admission {
        state: state.clone(),
        credential,
        _slot: slot,
    };
    if !request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(';')
                .next()
                .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
        })
    {
        return Err(Failure(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "JSON body required",
        ));
    }
    let headers = request.headers().clone();
    let deadline = Instant::now() + Duration::from_millis(limit.deadline_ms);
    let body = tokio::select! {
        _ = runtime.stop.cancelled() => return Err(Failure(StatusCode::SERVICE_UNAVAILABLE, "relay_unavailable", "Relay is shutting down")),
        result = tokio::time::timeout_at(deadline.into(), to_bytes(request.into_body(), limit.max_request_bytes)) => result
            .map_err(|_| Failure(StatusCode::REQUEST_TIMEOUT,"request_timeout","Relay request body timed out"))?
            .map_err(|_| Failure(StatusCode::PAYLOAD_TOO_LARGE,"request_too_large","Relay request body is unreadable or exceeds its limit"))?,
    };
    let chat: chat::Chat = serde_json::from_slice(&body).map_err(|_| {
        Failure::bad(
            "Invalid request or unsupported Chat Completions field; see the relay support matrix",
        )
    })?;
    let mut request_options = chat.request_options()?;
    // Revalidate after bounded body admission; a slow upload cannot use a key
    // revoked while its request was still waiting to become a provider call.
    let credential = authenticate(&state, &headers).await?;
    let mut admission = admission;
    admission.credential = credential;
    let key = &admission.credential;
    let account_default = if chat.model.is_none() && key.key.default_model_selection.is_none() {
        state
            .manager
            .preferences(&key.owner)
            .await
            .map_err(|_| {
                Failure(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "preferences_unavailable",
                    "Relay preferences are unavailable",
                )
            })?
            .values
            .relay_default_selection
    } else {
        None
    };
    let candidates = relay_candidates(
        &state.manager,
        &key.owner,
        key.key.allowed_providers.as_deref(),
    )
    .await
    .map_err(|_| {
        Failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "catalog_unavailable",
            "Model catalog is unavailable",
        )
    })?;
    let selected = select_candidate(
        &candidates,
        chat.model.as_deref(),
        key.key.default_model_selection.as_ref(),
        account_default.as_ref(),
    )?;
    if request_options.reasoning_effort.is_none() {
        let default_variant = key
            .key
            .default_model_selection
            .as_ref()
            .or(account_default.as_ref())
            .filter(|sel| {
                sel.provider_id == selected.selection.provider_id
                    && sel.model == selected.selection.model
            })
            .and_then(|sel| sel.variant.as_deref())
            .filter(|v| !v.is_empty() && *v != "default")
            .map(str::to_owned);
        request_options.reasoning_effort =
            default_variant.or_else(|| selected.resolved.options.reasoning_effort.clone());
    }
    let transport = selected.resolved.provider.client_transport_config();
    let protocol = transport.protocol;
    let google_media = selected.resolved.provider.google_function_response_media;
    let client = build_client(transport, google_media, request_options)?;
    let model = Model {
        provider_id: selected.selection.provider_id.clone(),
        model: selected.selection.model.clone(),
        protocol: Some(protocol),
    };
    let stream = chat.stream;
    let include_usage = chat
        .stream_options
        .as_ref()
        .is_some_and(|options| options.include_usage);
    let content = chat.content(limit.max_request_bytes)?;
    let route = route_id(selected);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let created = now.as_secs();
    let used_at_ms = u64::try_from(now.as_millis()).unwrap_or(u64::MAX);
    Ok(Call {
        admission,
        id: format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()),
        route_id: route,
        model,
        client,
        content,
        cancellation: runtime.stop.child_token(),
        deadline,
        stream,
        include_usage,
        created,
        used_at_ms,
    })
}

fn provider_failure(error: microcrab_transport::Error) -> (Failure, Outcome, Option<u16>) {
    use microcrab_transport::Error;
    let (status, code, message, outcome, upstream) = match error {
        Error::Cancelled => (
            StatusCode::SERVICE_UNAVAILABLE,
            "relay_cancelled",
            "Provider request cancelled",
            Outcome::Cancelled,
            None,
        ),
        Error::ProviderTimeout | Error::ProviderIdleTimeout(_) => (
            StatusCode::GATEWAY_TIMEOUT,
            "provider_timeout",
            "Provider response timed out",
            Outcome::TimedOut,
            None,
        ),
        Error::Limit(_) => (
            StatusCode::BAD_GATEWAY,
            "provider_limit",
            "Provider response exceeded relay limits",
            Outcome::LimitExceeded,
            None,
        ),
        Error::Protocol(_) | Error::Json(_) => (
            StatusCode::BAD_GATEWAY,
            "provider_invalid_response",
            "Provider returned an invalid response",
            Outcome::InvalidResponse,
            None,
        ),
        Error::ProviderStatus { status } => (
            StatusCode::BAD_GATEWAY,
            "provider_error",
            "Provider returned an HTTP error",
            Outcome::Unavailable,
            Some(status),
        ),
        Error::ProviderUnavailable(failure) => (
            StatusCode::BAD_GATEWAY,
            "provider_unavailable",
            "Provider is unavailable",
            Outcome::Unavailable,
            failure.status(),
        ),
        Error::Provider(failure) => (
            StatusCode::BAD_GATEWAY,
            "provider_error",
            "Provider did not complete the request",
            Outcome::InvalidResponse,
            failure
                .failure_details()
                .and_then(|failure| failure.status()),
        ),
        _ => (
            StatusCode::BAD_GATEWAY,
            "provider_error",
            "Provider transport could not complete the request",
            Outcome::Unavailable,
            None,
        ),
    };
    (Failure(status, code, message), outcome, upstream)
}

fn usage(usage: &Usage) -> Option<Value> {
    let (prompt, output, total) = (
        usage.input_tokens?,
        usage.output_tokens?,
        usage.total_tokens?,
    );
    let mut value = json!({"prompt_tokens":prompt,"completion_tokens":output,"total_tokens":total});
    if let Some(n) = usage.cache_read_tokens {
        value["prompt_tokens_details"] = json!({"cached_tokens":n});
    }
    if let Some(n) = usage.reasoning_tokens {
        value["completion_tokens_details"] = json!({"reasoning_tokens":n});
    }
    Some(value)
}

struct TerminalGuard<'a> {
    call: &'a Call,
    started: Instant,
    terminal: Option<Terminal>,
    usage_recorded: bool,
}

impl Drop for TerminalGuard<'_> {
    fn drop(&mut self) {
        self.call.cancellation.cancel();
        let key = &self.call.admission.credential;
        let row = self.terminal.take().unwrap_or(Terminal {
            outcome: Outcome::Cancelled,
            status_code: None,
            latency_ms: Some(elapsed(self.started)),
            ttft_ms: None,
            usage: None,
        });
        self.call.admission.state.manager.dashboard.record_relay(
            &key.owner,
            &key.key.id,
            &key.key.name,
            &self.call.id,
            self.call.model.clone(),
            row,
        );
        if !self.usage_recorded
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            let keys = self.call.admission.state.manager.relay_keys.clone();
            let owner = key.owner.clone();
            let key_id = key.key.id.clone();
            let used_at_ms = self.call.used_at_ms;
            handle.spawn(async move {
                let _ = keys.record_use(&owner, &key_id, used_at_ms).await;
            });
        }
    }
}

fn elapsed(start: Instant) -> u64 {
    start.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

impl Call {
    fn terminal(&self) -> TerminalGuard<'_> {
        TerminalGuard {
            call: self,
            started: Instant::now(),
            terminal: None,
            usage_recorded: false,
        }
    }

    async fn persist_key_use(&self, guard: &mut TerminalGuard<'_>) {
        if !guard.usage_recorded {
            guard.usage_recorded = true;
            let key = &self.admission.credential;
            let _ = self
                .admission
                .state
                .manager
                .relay_keys
                .record_use(&key.owner, &key.key.id, self.used_at_ms)
                .await;
        }
    }

    fn chunk(&self, choice: Value) -> Value {
        json!({"id":self.id,"object":"chat.completion.chunk","created":self.created,"model":self.route_id,"choices":[choice]})
    }

    async fn non_streaming(self) -> Result<Response, Failure> {
        let mut terminal = self.terminal();
        let result = self.forward(&mut terminal, None).await;
        self.persist_key_use(&mut terminal).await;
        let result = result?;
        let calls: Vec<_> = result
            .tool_calls
            .into_iter()
            .map(|call| {
                json!({
                    "id": call.id,
                    "type": "function",
                    "function": {"name": call.name, "arguments": call.arguments.to_string()}
                })
            })
            .collect();
        let mut message = json!({"role":"assistant","content":result.content});
        if !calls.is_empty() {
            message["tool_calls"] = json!(calls);
        }
        Ok(Json(json!({
            "id": self.id,
            "object": "chat.completion",
            "created": self.created,
            "model": self.route_id,
            "choices": [{"index": 0, "message": message, "finish_reason": null}],
            "usage": usage(&result.usage)
        }))
        .into_response())
    }

    fn streaming(self) -> Response {
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            let mut terminal = self.terminal();
            let result = self.forward(&mut terminal, Some(&tx)).await;
            self.persist_key_use(&mut terminal).await;
            let tail = async {
                match result {
                    Ok(response) => {
                        if self.include_usage
                            && let Some(usage) = usage(&response.usage)
                        {
                            let frame = json!({
                                "id": self.id,
                                "object": "chat.completion.chunk",
                                "created": self.created,
                                "model": self.route_id,
                                "choices": [],
                                "usage": usage
                            });
                            if tx
                                .send(Event::default().data(frame.to_string()))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        let _ = tx.send(Event::default().data("[DONE]")).await;
                    }
                    Err(error) => {
                        let _ = tx
                            .send(Event::default().data(error.value().to_string()))
                            .await;
                    }
                }
            };
            tokio::select! {
                _ = self.cancellation.cancelled() => {},
                _ = tokio::time::timeout_at(self.deadline.into(), tail) => {},
            }
        });
        Sse::new(futures_util::stream::unfold(rx, |mut rx| async {
            rx.recv()
                .await
                .map(|value| (Ok::<_, Infallible>(value), rx))
        }))
        .into_response()
    }

    async fn forward(
        &self,
        terminal: &mut TerminalGuard<'_>,
        output: Option<&mpsc::Sender<Event>>,
    ) -> Result<microcrab_protocol::Response, Failure> {
        let (tx, mut rx) = mpsc::channel(16);
        let cancel = &self.cancellation;
        let upstream = self.client.stream(
            &self.content,
            &NoopAuditor,
            &EmptyVariableView,
            cancel.clone(),
            Some(tx),
        );
        tokio::pin!(upstream);
        let deadline = tokio::time::sleep_until(self.deadline.into());
        tokio::pin!(deadline);
        let mut first = None;
        let mut ended = false;
        let mut upstream_status = None;
        let mut upstream_outcome = None;
        let send = |value: Value| async move {
            if let Some(output) = output {
                output
                    .send(Event::default().data(value.to_string()))
                    .await
                    .map_err(|_| {
                        Failure(
                            StatusCode::BAD_GATEWAY,
                            "client_closed",
                            "Client closed the response",
                        )
                    })?;
            }
            Ok::<_, Failure>(())
        };
        let poll = async {
            let mut emit = async |event| {
                if matches!(
                    &event,
                    ModelEvent::TextDelta { .. } | ModelEvent::ToolCallDelta { .. }
                ) {
                    first.get_or_insert_with(|| elapsed(terminal.started));
                }
                match event {
                    ModelEvent::TextDelta { text } => {
                        send(self.chunk(
                            json!({"index":0,"delta":{"content":text},"finish_reason":null}),
                        ))
                        .await?
                    }
                    ModelEvent::ToolCallDelta {
                        index,
                        id,
                        name,
                        arguments,
                    } => {
                        let mut delta = json!({"index":index,"function":{"arguments":arguments}});
                        if let Some(id) = id {
                            delta["id"] = json!(id);
                            delta["type"] = json!("function");
                        }
                        if let Some(name) = name {
                            delta["function"]["name"] = json!(name);
                        }
                        send(self.chunk(
                            json!({"index":0,"delta":{"tool_calls":[delta]},"finish_reason":null}),
                        ))
                        .await?;
                    }
                    ModelEvent::ReasoningDelta { .. } | ModelEvent::Finished { .. } => {}
                }
                Ok::<_, Failure>(())
            };
            send(self.chunk(json!({"index":0,"delta":{"role":"assistant"},"finish_reason":null})))
                .await?;
            loop {
                tokio::select! {
                    event = rx.recv(), if !ended => match event {
                        Some(event) => emit(event).await?,
                        None => ended = true,
                    },
                    result = &mut upstream => {
                        while let Ok(event) = rx.try_recv() {
                            emit(event).await?;
                        }
                        return result.map_err(|error| {
                            let (failure, outcome, upstream) = provider_failure(error);
                            upstream_status = upstream;
                            upstream_outcome = Some(outcome);
                            failure
                        });
                    }
                }
            }
        };
        let wait_closed = async {
            if let Some(output) = output {
                output.closed().await
            } else {
                std::future::pending().await
            }
        };
        let (outcome, result) = tokio::select! {
            biased;
            _ = cancel.cancelled() => (
                Outcome::Cancelled,
                Err(Failure(StatusCode::SERVICE_UNAVAILABLE, "relay_cancelled", "Relay request cancelled")),
            ),
            _ = wait_closed => (
                Outcome::Cancelled,
                Err(Failure(StatusCode::BAD_GATEWAY, "client_closed", "Client closed the response")),
            ),
            _ = &mut deadline => (
                Outcome::TimedOut,
                Err(Failure(StatusCode::GATEWAY_TIMEOUT, "relay_timeout", "Relay request exceeded its deadline")),
            ),
            result = poll => (
                if result.is_ok() {
                    Outcome::Accepted
                } else {
                    upstream_outcome.unwrap_or(Outcome::Cancelled)
                },
                result,
            ),
        };
        terminal.terminal = Some(Terminal {
            outcome,
            status_code: upstream_status,
            latency_ms: Some(elapsed(terminal.started)),
            ttft_ms: first,
            usage: result.as_ref().ok().map(|result| result.usage.clone()),
        });
        result
    }
}
