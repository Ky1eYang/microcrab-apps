//! Derived statistics only. Context, append-only ledgers, operation receipts and
//! verified audit evidence remain the respective authorities. No stream-delta IO.
use super::Managed;
use crate::config::Config;
use microcrab_auditor::{
    AuditBody, Auditor, AuditorError, BoxFuture, HookContext, Injection, Moment,
};
use microcrab_creator::{
    UsageLedger,
    audit::{AuditAttempt, AuditQuery, query_attempt_metadata},
};
use microcrab_protocol::Usage;
use microcrab_session::{Error, context::ContextMeasurement};
use microcrab_storage::{ModelSelection, OperationRecord, SessionRecord};
use microcrab_transport::Content;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
#[cfg(test)]
mod forwarding_tests;
mod message_usage;
#[cfg(all(test, feature = "storage-file"))]
mod tests;

/// Private optimistic witness: no DTO fields or secondary lifecycle authority.
pub(super) struct PublicationWitness {
    revision: u64,
    target: Option<ModelSelection>,
    applied: ModelSelection,
    request_head: Option<String>,
    display_lineage: String,
    context: Value,
    measurement: ContextMeasurement,
}
impl PublicationWitness {
    pub(super) fn capture(managed: &Managed, record: &SessionRecord) -> Result<Self, Error> {
        let anchor = managed.journal.history_anchor();
        let anchor = anchor.lock().unwrap_or_else(|error| error.into_inner());
        let measurement = managed.session.context_measurement()?;
        let context = serde_json::to_value(&measurement)?;
        Ok(Self {
            revision: record.revision,
            target: record.model_selection.clone(),
            applied: managed.routing.selection(),
            request_head: anchor.request_head.clone(),
            display_lineage: anchor.display_lineage.clone(),
            context,
            measurement,
        })
    }
    pub(super) async fn publish(
        self,
        managed: &Managed,
        mut value: Value,
        whole_snapshot: bool,
    ) -> Result<Option<Value>, Error> {
        // Revalidation and publication are synchronous while their respective real
        // owners are locked; no record/Context/HEAD lock is held across audit IO.
        let record = managed.record.lock().await;
        if record.revision != self.revision
            || record.model_selection != self.target
            || managed.routing.selection() != self.applied
        {
            return Ok(None);
        }
        let anchor = managed.journal.history_anchor();
        let anchor = anchor.lock().unwrap_or_else(|error| error.into_inner());
        if anchor.request_head != self.request_head
            || anchor.display_lineage != self.display_lineage
        {
            return Ok(None);
        }
        if whole_snapshot
            && value["history_view"]["lineage"].as_str() != Some(self.display_lineage.as_str())
        {
            return Ok(None);
        }
        managed.session.with_context_measurement(|current| {
            let candidate = if whole_snapshot {
                &value["statistics"]["context"]
            } else {
                &value["context"]
            };
            if !self.matches_context(current, candidate) {
                return Ok(None);
            }
            // Timing belongs to the real foreground owner, not a short-lived
            // queue admission receipt. Sample it under the same inbox lock as
            // active so delayed audit IO cannot publish a stale running clock.
            managed.session.with_inbox_snapshot(|inbox| {
                let execution = execution_timing(inbox);
                if whole_snapshot {
                    value["statistics"]["activity"]["execution"] = execution;
                    value["inbox"] = serde_json::to_value(inbox)?;
                    if !managed.closing.load(Ordering::Acquire)
                        && !managed.faulted.load(Ordering::Acquire)
                    {
                        value["session"]["status"] =
                            json!(if inbox.active || managed.hub.operation().is_some() {
                                "running"
                            } else {
                                "idle"
                            });
                    }
                    let value = super::view::bound_snapshot(value)?;
                    Ok(Some(managed.hub.replace(value)))
                } else {
                    value["activity"]["execution"] = execution;
                    managed.hub.statistics(value.clone(), self.revision);
                    Ok(Some(value))
                }
            })
        })?
    }
    fn matches_context(&self, current: &ContextMeasurement, candidate: &Value) -> bool {
        candidate == &self.context
            && self.context["fingerprint"].as_str() == Some(current.fingerprint.as_str())
            && self.context["epoch"].as_u64() == Some(current.epoch)
            && self.context["pending_epoch"].as_bool() == Some(current.pending_epoch)
    }
}

pub(super) struct FingerprintAuditor<'a> {
    inner: &'a dyn Auditor,
    fingerprint: String,
    target: String,
    rewritten: AtomicBool,
}
impl<'a> FingerprintAuditor<'a> {
    pub(super) fn new(
        inner: &'a dyn Auditor,
        content: &Content,
        selection: &ModelSelection,
    ) -> Result<Self, microcrab_transport::Error> {
        let measurement = ContextMeasurement::for_content(content)
            .map_err(|_| microcrab_transport::Error::protocol("context measurement failed"))?;
        Ok(Self {
            inner,
            fingerprint: measurement.fingerprint,
            target: serde_json::to_string(selection)?,
            rewritten: AtomicBool::new(false),
        })
    }
}
impl Auditor for FingerprintAuditor<'_> {
    fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }
    fn publish(&self, event: Moment, mut body: AuditBody) {
        if !self.rewritten.load(Ordering::Acquire) {
            body.context_fingerprint = Some(self.fingerprint.clone());
            body.context_target = Some(self.target.clone());
        }
        self.inner.publish(event, body);
    }
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        context: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            let injection = self.inner.intervene(moment, context).await?;
            if injection.request_body.is_some() {
                self.rewritten.store(true, Ordering::Release);
            }
            Ok(injection)
        })
    }
    fn flush(&self) -> BoxFuture<'_, Result<(), AuditorError>> {
        self.inner.flush()
    }
}

#[derive(Clone, Default)]
struct Cache {
    key: Option<(u64, Option<String>, u64)>,
    sequence: Option<u64>,
    attempt: Option<AuditAttempt>,
    timing_attempt: Option<AuditAttempt>,
    available: bool,
}
pub(super) struct Statistics {
    directory: PathBuf,
    owner: String,
    cache: Mutex<Cache>,
    config: Config,
    model_metadata: Option<std::sync::Arc<super::model_metadata::ModelMetadata>>,
}
impl Statistics {
    pub(super) fn new(config: &Config, id: &str) -> Self {
        Self {
            directory: config.data_dir.join("sessions").join(id),
            owner: String::new(),
            cache: Mutex::new(Cache::default()),
            config: config.clone(),
            model_metadata: None,
        }
    }
    pub(super) fn with_metadata(
        config: &Config,
        id: &str,
        metadata: std::sync::Arc<super::model_metadata::ModelMetadata>,
    ) -> Self {
        Self {
            model_metadata: Some(metadata),
            ..Self::new(config, id)
        }
    }
    pub(super) fn scoped(mut self, owner: &str) -> Self {
        self.owner = owner.into();
        self
    }
    fn audit_source(&self) -> microcrab_storage::audit::AuditSource {
        let id = self
            .directory
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("");
        super::audit::source(&self.config, &self.owner, id, id, self.directory.clone())
    }
    fn enrich(&self, record: &SessionRecord, mut value: Value) -> Value {
        if record.model_selection.is_some()
            && let Some(metadata) = &self.model_metadata
        {
            metadata.enrich_statistics(record, &self.config, &mut value);
        }
        value
    }
    /// Called only at semantic Context/audit/lifecycle boundaries, never deltas.
    pub(super) async fn snapshot(
        &self,
        managed: &Managed,
        record: &SessionRecord,
        operations: &[OperationRecord],
        witness: &PublicationWitness,
    ) -> Result<Value, Error> {
        if record.revision != witness.revision || record.model_selection != witness.target {
            return Err(Error::protocol(
                "statistics record does not match captured source",
            ));
        }
        let measurement = &witness.measurement;
        let path = self.directory.clone();
        let cache = self
            .read_attempt(path, witness.request_head.clone(), measurement.epoch)
            .await?;
        let main = managed.ledger.snapshot();
        let aggregate = managed.aggregate.snapshot();
        let mut applied_record = record.clone();
        applied_record.model_selection = Some(witness.applied.clone());
        let mut value = project(
            &self.config,
            &applied_record,
            measurement,
            &main.total,
            &aggregate.total,
            operations,
            cache.attempt.as_ref(),
            cache.timing_attempt.as_ref(),
            cache.sequence,
            cache.available,
        );
        reported_cache(&mut value, &main, &aggregate)?;
        Ok(self.enrich(&applied_record, value))
    }
    async fn read_attempt(
        &self,
        directory: PathBuf,
        head: Option<String>,
        epoch: u64,
    ) -> Result<Cache, Error> {
        // The query snapshot supplies its committed sequence. File lengths are
        // neither SQLite witnesses nor evidence that an append is committed.
        let source = if self.config.storage == crate::config::Storage::File {
            microcrab_storage::audit::AuditSource::File(directory)
        } else {
            self.audit_source()
        };
        let revision_source = source.clone();
        let revision = tokio::task::spawn_blocking(move || revision_source.revision())
            .await
            .map_err(|_| Error::protocol("statistics revision worker failed"))?
            .ok()
            .flatten();
        if let Some(sequence) = revision {
            let cache = self.cache.lock().unwrap_or_else(|error| error.into_inner());
            if cache.key.as_ref() == Some(&(sequence, head.clone(), epoch)) {
                return Ok(cache.clone());
            }
        }
        let timing_head = head.clone();
        let result = if let Some(head) = head {
            tokio::task::spawn_blocking(move || {
                query_attempt_metadata(
                    source,
                    &AuditQuery {
                        epoch,
                        turn: None,
                        round: None,
                        request_hash: Some(head),
                        history_head: None,
                        subagent: None,
                        call_id: None,
                        tool_call_id: None,
                    },
                )
            })
            .await
            .map_err(|_| Error::protocol("statistics audit worker failed"))?
            .ok()
        } else {
            None
        };
        let mut cache = self.cache.lock().unwrap_or_else(|error| error.into_inner());
        cache.key = Some((
            result
                .as_ref()
                .map_or(0, |result| result.committed_sequence),
            timing_head.clone(),
            epoch,
        ));
        cache.available = result.is_some();
        cache.sequence = result.as_ref().map(|result| result.committed_sequence);
        // Prompt measurement can come from a causal ancestor; timing always belongs
        // to the exact selected Journal request, including failed/pending attempts.
        cache.timing_attempt = result.as_ref().and_then(|result| {
            result
                .attempts
                .iter()
                .find(|attempt| Some(&attempt.request_hash) == timing_head.as_ref())
                .cloned()
        });
        cache.attempt = result.and_then(|result| {
            result
                .attempts
                .iter()
                .filter(|attempt| {
                    attempt
                        .usage
                        .as_ref()
                        .is_some_and(|usage| usage["input_tokens"].is_u64())
                })
                .max_by_key(|attempt| attempt.sequence)
                .cloned()
                .or_else(|| {
                    result
                        .attempts
                        .into_iter()
                        .max_by_key(|attempt| attempt.sequence)
                })
        });
        Ok(cache.clone())
    }
    pub(super) async fn detached(
        &self,
        record: &SessionRecord,
        measurement: &ContextMeasurement,
        head: Option<String>,
        operations: &[OperationRecord],
        selected_model: Option<&microcrab_storage::ModelSelection>,
    ) -> Result<Value, Error> {
        let path = self.directory.clone();
        let config = self.config.clone();
        #[cfg(feature = "storage-sqlite")]
        let scope =
            microcrab_storage::audit::AuditScope::new(&record.owner, &record.id, &record.id);
        let (main, aggregate) = tokio::task::spawn_blocking(move || {
            let (main, aggregate) = match config.storage {
                crate::config::Storage::File => (
                    UsageLedger::read_snapshot_named(&path, "usage.jsonl")?,
                    UsageLedger::read_snapshot_named(&path, "aggregate_usage.jsonl")?,
                ),
                crate::config::Storage::Sqlite => {
                    #[cfg(feature = "storage-sqlite")]
                    {
                        let database = config.data_dir.join("storage.sqlite");
                        (
                            UsageLedger::read_snapshot_sqlite(&database, &scope, "usage.jsonl")?,
                            UsageLedger::read_snapshot_sqlite(
                                &database,
                                &scope,
                                "aggregate_usage.jsonl",
                            )?,
                        )
                    }
                    #[cfg(not(feature = "storage-sqlite"))]
                    {
                        return Err(Error::config("SQLite support is not compiled"));
                    }
                }
            };
            Ok::<_, Error>((main, aggregate))
        })
        .await
        .map_err(|_| Error::protocol("statistics usage worker failed"))??;
        let cache = self
            .read_attempt(self.directory.clone(), head, measurement.epoch)
            .await?;
        let mut applied_record = record.clone();
        applied_record.model_selection = selected_model.cloned();
        let mut value = project(
            &self.config,
            &applied_record,
            measurement,
            &main.total,
            &aggregate.total,
            operations,
            cache.attempt.as_ref(),
            cache.timing_attempt.as_ref(),
            cache.sequence,
            cache.available,
        );
        reported_cache(&mut value, &main, &aggregate)?;
        Ok(self.enrich(&applied_record, value))
    }
}
fn reported_cache(
    value: &mut Value,
    main: &microcrab_creator::usage::UsageSnapshot,
    aggregate: &microcrab_creator::usage::UsageSnapshot,
) -> Result<(), Error> {
    value["usage"]["main_reported_cache"] = serde_json::to_value(&main.cache)?;
    value["usage"]["aggregate_reported_cache"] = serde_json::to_value(&aggregate.cache)?;
    Ok(())
}
fn clean_usage(usage: &Usage) -> Usage {
    let mut usage = usage.clone();
    usage.provider_details.clear();
    usage
}
fn cache_rate(usage: &Usage) -> Option<f64> {
    usage
        .cache_read_tokens
        .zip(usage.input_tokens)
        .filter(|(read, input)| *input > 0 && read <= input)
        .map(|(read, input)| read as f64 / input as f64)
}
fn execution_timing(inbox: &microcrab_session::InboxSnapshot) -> Value {
    let started = inbox.execution_started_at_ms;
    let ended = inbox.execution_ended_at_ms;
    json!({
        "active": inbox.active,
        "started_at_ms": started,
        "ended_at_ms": ended,
        "elapsed_ms": ended.zip(started).and_then(|(ended, started)| ended.checked_sub(started)),
    })
}
fn operation(operations: &[OperationRecord]) -> Value {
    operations.iter().filter(|operation| matches!(operation.method.as_str(), "turn.submit"|"turn.resume"|"turn.retry"|"inbox.resume"|"session.compact")).max_by_key(|operation| operation.created_at_ms).map_or(Value::Null, |operation| {
        let terminal = !matches!(operation.status.as_str(), "accepted"|"running");
        json!({"id":operation.id,"kind":operation.method,"status":operation.status,"started_at_ms":operation.created_at_ms,"ended_at_ms":terminal.then_some(operation.updated_at_ms),"elapsed_ms":terminal.then(|| operation.updated_at_ms.checked_sub(operation.created_at_ms)).flatten()})
    })
}
#[expect(
    clippy::too_many_arguments,
    reason = "Projection combines distinct measured authorities, not a shared mutable state bag"
)]
fn project(
    config: &Config,
    record: &SessionRecord,
    measurement: &ContextMeasurement,
    main: &Usage,
    aggregate: &Usage,
    operations: &[OperationRecord],
    attempt: Option<&AuditAttempt>,
    timing_attempt: Option<&AuditAttempt>,
    sequence: Option<u64>,
    available: bool,
) -> Value {
    let selected = record.model_selection.as_ref();
    let target = selected.and_then(|selected| serde_json::to_string(selected).ok());
    let window = selected.and_then(|selected| {
        config
            .model_choices()
            .into_iter()
            .find(|choice| {
                choice.id == selected.model && choice.provider_id == selected.provider_id
            })
            .and_then(|choice| choice.context_window_tokens)
    });
    let (freshness, reason) = if selected.is_none() {
        ("unknown", "selected_model_route_unavailable")
    } else if measurement.pending_epoch {
        ("stale", "pending_epoch")
    } else if let Some(attempt) = attempt {
        if attempt.epoch != measurement.epoch {
            ("stale", "epoch_changed")
        } else if attempt.context_target.is_none() || attempt.context_fingerprint.is_none() {
            ("unknown", "request_provenance_unavailable")
        } else if attempt.context_target != target {
            ("stale", "model_selection_changed")
        } else if attempt.context_fingerprint.as_deref() != Some(measurement.fingerprint.as_str()) {
            ("stale", "context_changed_since_request")
        } else {
            ("current", "exact_request_context_match")
        }
    } else {
        ("unknown", "no_selected_request_measurement")
    };
    let input = attempt
        .and_then(|attempt| attempt.usage.as_ref())
        .and_then(|usage| usage["input_tokens"].as_u64());
    let headroom = (freshness == "current")
        .then_some(())
        .and(window.zip(input))
        .and_then(|(window, input)| window.checked_sub(input));
    let latest = attempt.map(|attempt| json!({"request_hash":attempt.request_hash,"call_id":attempt.call_id,"epoch":attempt.epoch,"turn":attempt.turn,"round":attempt.round,"input_tokens":input,"started_at_ms":attempt.started_at_ms,"ended_at_ms":attempt.ended_at_ms,"freshness":freshness,"freshness_reason":reason}));
    let timing = timing_attempt.map(|attempt| {
        let elapsed = attempt.started_at_ms.zip(attempt.ended_at_ms).and_then(|(start,end)| end.checked_sub(start));
        let output = attempt.usage.as_ref().and_then(|usage| usage["output_tokens"].as_u64());
        let request_rate = elapsed.filter(|elapsed| *elapsed > 0).zip(output).map(|(elapsed,output)| output as f64 * 1000.0 / elapsed as f64);
        let rate = attempt.generation_ms.filter(|generation| *generation > 0).zip(output).map(|(generation,output)| output as f64 * 1000.0 / generation as f64);
        json!({"request_hash":attempt.request_hash,"call_id":attempt.call_id,"started_at_ms":attempt.started_at_ms,"ended_at_ms":attempt.ended_at_ms,"elapsed_ms":elapsed,"generation_ms":attempt.generation_ms,"first_token_ms":attempt.first_token_ms,"timing_source":attempt.timing_source,"output_tokens":output,"tokens_per_second":rate,"rate_basis":attempt.generation_ms.map(|_| "generation_interval"),"request_tokens_per_second":request_rate})
    });
    json!({"context":measurement,"activity":{"scope":"selected_context_history","turns":measurement.counts.turns,"steps":measurement.counts.steps,"tool_calls":measurement.counts.tool_calls,"operation":operation(operations)},"usage":{"scope":"append_only_consumption","main":clean_usage(main),"aggregate":clean_usage(aggregate),"main_cache_hit_rate":cache_rate(main),"aggregate_cache_hit_rate":cache_rate(aggregate)},"provider":{"scope":"latest_main_provider_attempt","latest_prompt":latest,"timing":timing,"audit_sequence":sequence,"available":available},"model":{"context_window_tokens":window,"measured_headroom_tokens":headroom,"headroom_reason":if window.is_none(){"model_window_unknown"}else if input.is_none(){"provider_input_tokens_unavailable"}else if headroom.is_some(){"measured_current_request"}else if freshness == "current"{"measured_input_exceeds_model_window"}else{reason}}})
}

#[cfg(all(test, feature = "storage-sqlite"))]
mod sqlite_tests;
