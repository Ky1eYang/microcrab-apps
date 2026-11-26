//! Bounded creator-side projection of terminal upstream facts. It does not read
//! historical audit/usage bodies and is not an authority for Session or billing.
mod report;
mod store;
#[cfg(test)]
mod tests;

use microcrab_auditor::{
    AuditBody, AuditOutcome, Auditor, AuditorError, BoxFuture, HookContext, Injection,
};
use microcrab_protocol::{Moment, Usage};
use microcrab_transport::Protocol;
pub(crate) use report::{Filter, Range, Window};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
pub(crate) use store::Dashboard;
use uuid::Uuid;

pub(crate) const DAY_MS: u64 = 86_400_000;
const RETENTION_MS: u64 = 30 * DAY_MS;
const CAPACITY: usize = 4096;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Source {
    Session,
    Relay,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Outcome {
    Accepted,
    Unavailable,
    InvalidResponse,
    LimitExceeded,
    Cancelled,
    TimedOut,
}

impl From<AuditOutcome> for Outcome {
    fn from(value: AuditOutcome) -> Self {
        match value {
            AuditOutcome::Accepted => Self::Accepted,
            AuditOutcome::Unavailable => Self::Unavailable,
            AuditOutcome::InvalidResponse => Self::InvalidResponse,
            AuditOutcome::LimitExceeded => Self::LimitExceeded,
            AuditOutcome::Cancelled => Self::Cancelled,
            AuditOutcome::TimedOut => Self::TimedOut,
        }
    }
}

impl Outcome {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Unavailable => "unavailable",
            Self::InvalidResponse => "invalid_response",
            Self::LimitExceeded => "limit_exceeded",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
        }
    }
}

/// Only public route identifiers, never an endpoint, auth information or variant data.
#[derive(Clone)]
pub(crate) struct Model {
    pub provider_id: String,
    pub model: String,
    pub protocol: Option<Protocol>,
}

pub(crate) struct Terminal {
    pub outcome: Outcome,
    pub status_code: Option<u16>,
    pub latency_ms: Option<u64>,
    pub ttft_ms: Option<u64>,
    pub usage: Option<Usage>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Event {
    event_id: Uuid,
    ts_ms: u64,
    request_id: Option<String>,
    source: Source,
    source_id: String,
    source_label: Option<String>,
    provider_id: Option<String>,
    model: Option<String>,
    protocol: Option<Protocol>,
    outcome: Outcome,
    status_code: Option<u16>,
    latency_ms: Option<u64>,
    ttft_ms: Option<u64>,
    usage: Option<Counters>,
}

/// Explicit allowlist: Core Usage also includes opaque provider_details.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Counters {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cache_read_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
    uncached_input_tokens: Option<u64>,
    non_reasoning_output_tokens: Option<u64>,
    total_tokens: Option<u64>,
    samples: u64,
}
impl From<&Usage> for Counters {
    fn from(value: &Usage) -> Self {
        Self {
            input_tokens: value.input_tokens,
            output_tokens: value.output_tokens,
            cache_read_tokens: value.cache_read_tokens,
            cache_write_tokens: value.cache_write_tokens,
            reasoning_tokens: value.reasoning_tokens,
            uncached_input_tokens: value.uncached_input_tokens,
            non_reasoning_output_tokens: value.non_reasoning_output_tokens,
            total_tokens: value.total_tokens,
            samples: value.samples.max(1),
        }
    }
}
impl Event {
    fn key(&self) -> (u64, Uuid) {
        (self.ts_ms, self.event_id)
    }
}

/// Do not truncate or otherwise misidentify an overlong route. Public IDs use
/// non-whitespace, control-free strings and must not resemble URLs or relay keys.
fn identifier(text: &str) -> Option<String> {
    (!text.is_empty()
        && text.len() <= 256
        && !text.chars().any(|c| c.is_whitespace() || c.is_control())
        && !text.contains("://")
        && !text.contains("mc_sk_"))
    .then(|| text.to_owned())
}

fn key_label(text: &str) -> Option<String> {
    (!text.is_empty()
        && text.len() <= 256
        && !text.chars().any(char::is_control)
        && !text.contains("mc_sk_"))
    .then(|| text.to_owned())
}

fn owner_key(owner: &str) -> String {
    format!("{:x}", Sha256::digest(owner.as_bytes()))
}

fn event(
    source: Source,
    source_id: &str,
    source_label: Option<String>,
    request_id: &str,
    route: &Model,
    terminal: &Terminal,
    counters: Option<&Usage>,
) -> Option<Event> {
    Some(Event {
        event_id: Uuid::new_v4(),
        ts_ms: super::now(),
        request_id: identifier(request_id),
        source,
        source_id: identifier(source_id)?,
        source_label,
        provider_id: identifier(&route.provider_id),
        model: identifier(&route.model),
        protocol: route.protocol,
        outcome: terminal.outcome,
        status_code: terminal
            .status_code
            .filter(|code| (100..=599).contains(code)),
        latency_ms: terminal.latency_ms,
        ttft_ms: terminal.ttft_ms,
        usage: counters.map(Counters::from),
    })
}

impl Dashboard {
    // Contract for the independent relay dispatcher. Exactly one terminal per
    // admitted logical request; preflight and internal retries are not rows.
    #[allow(
        dead_code,
        reason = "Independent relay dispatcher is integrated separately"
    )]
    pub(crate) fn record_relay(
        &self,
        owner: &str,
        key_id: &str,
        key_name: &str,
        request_id: &str,
        route: Model,
        terminal: Terminal,
    ) {
        if let Some(row) = event(
            Source::Relay,
            key_id,
            key_label(key_name),
            request_id,
            &route,
            &terminal,
            terminal.usage.as_ref(),
        ) {
            self.record(owner_key(owner), row);
        }
    }
}

/// The routed client installs this once around each call on its immutable target.
/// Core publishes each terminal upstream attempt, including retry and drop, here;
/// forwarding keeps the existing Session/child audit and UsageLedger intact.
#[derive(Clone)]
pub(in crate::manager) struct SessionSink {
    dashboard: Dashboard,
    owner: Arc<str>,
    session_id: Arc<str>,
}

impl SessionSink {
    pub(super) fn new(dashboard: Dashboard, owner: &str, session_id: &str) -> Self {
        Self {
            dashboard,
            owner: Arc::from(owner_key(owner)),
            session_id: Arc::from(session_id),
        }
    }

    pub(super) fn for_session(&self, session_id: &str) -> Self {
        Self {
            dashboard: self.dashboard.clone(),
            owner: self.owner.clone(),
            session_id: Arc::from(session_id),
        }
    }

    pub(super) fn wrap<'a>(
        &'a self,
        inner: &'a dyn Auditor,
        route: &'a Model,
    ) -> SessionAuditor<'a> {
        SessionAuditor {
            sink: self,
            inner,
            route,
            model_rewritten: AtomicBool::new(false),
        }
    }
}

pub(super) struct SessionAuditor<'a> {
    sink: &'a SessionSink,
    inner: &'a dyn Auditor,
    route: &'a Model,
    model_rewritten: AtomicBool,
}
impl Auditor for SessionAuditor<'_> {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, moment: Moment, body: AuditBody) {
        if moment == Moment::RoundCompleted
            && let Some((request_id, outcome)) = body.call_id.as_deref().zip(body.outcome)
            && let Some(mut row) = event(
                Source::Session,
                &self.sink.session_id,
                None,
                request_id,
                self.route,
                &Terminal {
                    outcome: outcome.into(),
                    status_code: body.response_status,
                    latency_ms: body.elapsed_ms,
                    ttft_ms: body.first_token_ms,
                    usage: None,
                },
                body.usage.as_ref(),
            )
        {
            // A request rewrite can change its model; do not inspect either body
            // or imply that an unverified model used the configured public rate.
            if self.model_rewritten.swap(false, Ordering::AcqRel) {
                row.model = None;
            }
            // Prefer the producer's certified terminal time (or measured elapsed
            // time from the known start); otherwise use notification wall time.
            row.ts_ms = body
                .response_completed_at_ms
                .or_else(|| {
                    body.started_at_unix_ms
                        .zip(body.elapsed_ms)
                        .and_then(|(start, elapsed)| start.checked_add(elapsed))
                })
                .unwrap_or(row.ts_ms);
            self.sink.dashboard.record(self.sink.owner.to_string(), row);
        }
        if self.inner.is_enabled() {
            self.inner.publish(moment, body);
        }
    }
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        context: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            let injection = self.inner.intervene(moment, context).await?;
            if moment == Moment::RequestPrepared {
                self.model_rewritten
                    .store(injection.request_body.is_some(), Ordering::Release);
            }
            Ok(injection)
        })
    }
    fn flush(&self) -> BoxFuture<'_, Result<(), AuditorError>> {
        self.inner.flush()
    }
}
