pub(super) mod admission;
// Session-local provider routing. Manager owns mutation under its command gate;
// requests retain an immutable target before awaiting so retries never change it.
use super::{Managed, Manager, hub::Hub};
use crate::config::Config;
use async_trait::async_trait;
use microcrab_auditor::{Auditor, VariableView};
use microcrab_creator::{RetryPolicy, RetryingClient};
use microcrab_protocol::{Response, application::WireError};
use microcrab_session::{Client, Error};
use microcrab_storage::{ModelSelection, SessionRecord};
use microcrab_transport::{Content, ModelEvent};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub(super) struct Target {
    selection: ModelSelection,
    witness: String,
    main: Arc<dyn Client>,
    child: Arc<dyn Client>,
    recordable: Option<RetryingClient>,
    context_window_tokens: Option<u64>,
}
#[derive(Clone)]
pub(super) struct Routing {
    target: Arc<Mutex<Arc<Target>>>,
    epoch_targets: Arc<Mutex<std::collections::BTreeMap<u64, Arc<Target>>>>,
    anchors: Arc<Mutex<Option<Arc<super::display::anchors::Anchors>>>>,
}
#[derive(Clone)]
struct RoutedClient {
    routing: Routing,
    main: bool,
    recording: Option<Arc<microcrab_creator::display::DisplayRecorder>>,
    anchors: Option<Arc<super::display::anchors::Anchors>>,
}
struct RouteEstimator {
    routing: Routing,
    config: Config,
    metadata: Arc<super::model_metadata::ModelMetadata>,
}
impl microcrab_session::context::TokenEstimator for RouteEstimator {
    fn estimate(
        &self,
        content: &Content,
    ) -> Result<Option<microcrab_session::context::TokenEstimate>, Error> {
        let target = self
            .routing
            .target
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let selection = target.selection.clone();
        let Some(window) = target
            .context_window_tokens
            .or_else(|| self.metadata.context_window(&self.config, &selection))
        else {
            return Ok(None);
        };
        microcrab_session::context::TokenEstimate::conservative_text(
            content,
            format!(
                "{}:{}:{}",
                selection.provider_id,
                selection.model,
                selection.variant.as_deref().unwrap_or("default")
            ),
            window,
        )
    }
}
impl Routing {
    pub(super) fn witness(&self) -> String {
        self.target
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .witness
            .clone()
    }

    pub(super) fn estimator(
        &self,
        manager: &Manager,
    ) -> Arc<dyn microcrab_session::context::TokenEstimator> {
        Arc::new(RouteEstimator {
            routing: self.clone(),
            config: manager.config.clone(),
            metadata: manager.model_metadata.clone(),
        })
    }
    pub(super) fn new(target: Target) -> Self {
        Self {
            target: Arc::new(Mutex::new(Arc::new(target))),
            epoch_targets: Arc::default(),
            anchors: Arc::new(Mutex::new(None)),
        }
    }
    pub(super) fn install_anchors(&self, anchors: Arc<super::display::anchors::Anchors>) {
        *self.anchors.lock().unwrap_or_else(|e| e.into_inner()) = Some(anchors);
    }
    pub(super) async fn annotate_display_messages(
        &self,
        messages: &mut [serde_json::Value],
    ) -> Result<(), String> {
        let anchors = self
            .anchors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(anchors) = anchors {
            anchors.annotate_messages(messages).await?;
        }
        Ok(())
    }
    pub(super) fn prepare_display_retry(&self, attempt: Option<String>) {
        if let Some(anchors) = self
            .anchors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            match attempt {
                Some(attempt) => anchors.prepare_retry(attempt),
                None => anchors.clear_retry(),
            }
        }
    }
    pub(super) fn pin_epoch(&self, epoch: u64) {
        let target = self
            .target
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        self.epoch_targets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(epoch, target);
    }
    pub(super) fn frozen_for_epoch(&self, epoch: u64) -> Self {
        let mut captured = self.frozen();
        if let Some(target) = self
            .epoch_targets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&epoch)
            .cloned()
        {
            captured.target = Arc::new(Mutex::new(target));
        }
        captured
    }
    pub(super) fn epoch_client(&self, epoch: u64) -> Arc<dyn Client> {
        let target = self
            .epoch_targets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&epoch)
            .cloned();
        match target {
            Some(target) => target.child.clone(),
            None => self.client(false),
        }
    }
    pub(super) fn selection(&self) -> ModelSelection {
        self.target
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .selection
            .clone()
    }
    fn frozen(&self) -> Self {
        Self {
            target: Arc::new(Mutex::new(
                self.target
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone(),
            )),
            anchors: self.anchors.clone(),
            epoch_targets: self.epoch_targets.clone(),
        }
    }
    pub(super) fn client(&self, main: bool) -> Arc<dyn Client> {
        Arc::new(RoutedClient {
            routing: if main { self.clone() } else { self.frozen() },
            main,
            recording: None,
            anchors: None,
        })
    }
    pub(super) fn recorded_child(
        &self,
        recording: Arc<microcrab_creator::display::DisplayRecorder>,
        id: String,
        source: microcrab_storage::display::DisplaySource,
        failure: Arc<dyn Fn(String) + Send + Sync>,
    ) -> Arc<dyn Client> {
        let anchors = super::display::anchors::Anchors::new(id, source, recording.clone());
        anchors.set_failure_handler(failure);
        Arc::new(RoutedClient {
            routing: self.frozen(),
            main: false,
            anchors: Some(anchors),
            recording: Some(recording),
        })
    }
    /// Infallible publication after acknowledged Directory CAS. Never await here.
    fn publish(&self, target: Target) {
        *self
            .target
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Arc::new(target);
    }
}
impl RoutedClient {
    fn snapshot(&self, visible: bool) -> (Arc<dyn Client>, ModelSelection) {
        let target = self
            .routing
            .target
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !visible {
            (target.child.clone(), target.selection.clone())
        } else if self.main {
            (target.main.clone(), target.selection.clone())
        } else if let (Some(recording), Some(client)) = (&self.recording, &target.recordable) {
            (
                Arc::new(client.clone().with_recording(recording.clone())),
                target.selection.clone(),
            )
        } else {
            (target.child.clone(), target.selection.clone())
        }
    }
}
#[async_trait]
impl Client for RoutedClient {
    async fn observe_input_admitted(
        &self,
        epoch: u64,
        items: &[microcrab_transport::ContentItem],
        submissions: &[(usize, String)],
    ) {
        let anchors = if self.main {
            self.routing
                .anchors
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
        } else {
            self.anchors.clone()
        };
        if let Some(anchors) = anchors {
            anchors.observe_input(epoch, items, submissions).await;
        }
    }
    async fn observe_admitted(
        &self,
        epoch: u64,
        items: &[microcrab_transport::ContentItem],
        producer: Option<&str>,
    ) {
        let anchors = if self.main {
            self.routing
                .anchors
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
        } else {
            self.anchors.clone()
        };
        if let Some(anchors) = anchors {
            anchors.observe(epoch, items, producer).await;
        }
    }
    async fn stream_admitted(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
        events: Option<mpsc::Sender<ModelEvent>>,
    ) -> Result<Response, microcrab_transport::Error> {
        let (client, selection) = self.snapshot(true);
        let measured = super::statistics::FingerprintAuditor::new(auditor, content, &selection)?;
        client
            .stream(content, &measured, variables, cancellation, events)
            .await
    }
    async fn complete(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let (client, selection) = self.snapshot(false);
        let measured = super::statistics::FingerprintAuditor::new(auditor, content, &selection)?;
        client
            .complete(content, &measured, variables, cancellation)
            .await
    }
    async fn stream(
        &self,
        content: &Content,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
        events: Option<mpsc::Sender<ModelEvent>>,
    ) -> Result<Response, microcrab_transport::Error> {
        let (client, selection) = self.snapshot(false);
        let measured = super::statistics::FingerprintAuditor::new(auditor, content, &selection)?;
        client
            .stream(content, &measured, variables, cancellation, events)
            .await
    }
}

pub(super) fn normalize(mut selection: ModelSelection) -> ModelSelection {
    if selection
        .variant
        .as_deref()
        .is_some_and(|variant| variant.is_empty() || variant == "default")
    {
        selection.variant = None;
    }
    selection
}
pub(super) fn default_selection(config: &Config) -> ModelSelection {
    ModelSelection {
        provider_id: config.default_provider_id().to_owned(),
        model: config.provider.model.clone(),
        variant: None,
    }
}
pub(super) fn validate(config: &Config, selection: &ModelSelection) -> Result<(), WireError> {
    config
        .resolve_selection(
            &selection.provider_id,
            &selection.model,
            selection.variant.as_deref(),
        )
        .map(|_| ())
        .map_err(|error| {
            WireError::new(
                "model_unavailable",
                microcrab_protocol::diagnostic::describe(&error, 1800),
            )
        })
}
fn retry_policy(timeout_ms: u64) -> RetryPolicy {
    RetryPolicy {
        attempts: 6,
        backoff_ms: 1_000,
        deadline_ms: timeout_ms.max(1),
    }
}

pub(super) async fn target(
    manager: &Manager,
    owner: &str,
    selection: &ModelSelection,
    hub: Arc<Hub>,
) -> Result<Target, Error> {
    let (resolved, configuration_revision, context_window_tokens) =
        super::providers::resolve(manager, owner, selection)
            .await
            .map_err(|error| {
                Error::Config(
                    format!(
                        "selected model unavailable ({}): {}",
                        error.code, error.message
                    )
                    .into(),
                )
            })?;
    use sha2::Digest;
    let endpoint = resolved
        .provider
        .client_transport_config()
        .request_endpoint()?;
    let mut route = serde_json::json!({
        "version": 1,
        "protocol": resolved.provider.protocol,
        "endpoint": endpoint.as_str(),
        "auth": resolved.provider.auth,
        "model": resolved.provider.model,
        "variant": resolved.options.reasoning_effort,
        "projection": "state-first-neutral-v1",
    });
    if selection.provider_id.starts_with("user:") {
        route["owner"] = serde_json::json!(owner);
        route["provider_id"] = serde_json::json!(selection.provider_id);
        route["configuration_revision"] = serde_json::json!(configuration_revision);
        route["timeout_ms"] = serde_json::json!(resolved.provider.timeout_ms);
        route["max_response_bytes"] = serde_json::json!(resolved.provider.max_response_bytes);
        route["credential_digest"] = serde_json::json!(
            resolved
                .provider
                .api_key
                .as_ref()
                .map(|key| format!("{:x}", sha2::Sha256::digest(key.as_bytes())))
        );
    }
    let witness = format!("{:x}", sha2::Sha256::digest(serde_json::to_vec(&route)?));
    let client = RetryingClient::new_transport(
        resolved.provider.client_transport_config(),
        retry_policy(resolved.provider.timeout_ms),
    )?
    .with_request_options(resolved.options)?;
    let recordable = manager.client_override.is_none().then(|| client.clone());
    let child = manager
        .client_override
        .clone()
        .unwrap_or_else(|| Arc::new(client.clone()));
    let client = if let Some(recording) = hub.recording() {
        client.with_recording(recording)
    } else {
        client
    };
    let main = manager
        .client_override
        .clone()
        .unwrap_or_else(|| Arc::new(client.with_observer(hub)));
    Ok(Target {
        selection: selection.clone(),
        witness,
        main,
        child,
        recordable,
        context_window_tokens,
    })
}

/// Resolve a dedicated target using the existing provider catalog and retry assembly.
pub(super) async fn review_target(
    manager: &Manager,
    owner: &str,
    selection: &microcrab_permission::ReviewModelSelection,
    hub: Arc<Hub>,
) -> Result<Arc<dyn Client>, Error> {
    let selected = normalize(ModelSelection {
        provider_id: selection.provider_id.clone(),
        model: selection.model.clone(),
        variant: selection.variant.clone(),
    });
    Ok(Routing::new(target(manager, owner, &selected, hub).await?).client(false))
}

impl Manager {
    /// Legacy records adopt this startup's actual default exactly once. CAS races
    /// reread the durable authority; a storage failure never invents a selection.
    pub(super) async fn selected_record(
        &self,
        mut record: SessionRecord,
    ) -> Result<SessionRecord, WireError> {
        if record.model_selection.is_some() {
            return Ok(record);
        }
        let selection = default_selection(&self.config);
        validate(&self.config, &selection)?;
        let revision = record.revision;
        record.model_selection = Some(selection);
        record.revision = revision
            .checked_add(1)
            .ok_or_else(|| WireError::new("revision_conflict", "Session revision exhausted"))?;
        record.updated_at_ms = super::now().max(record.updated_at_ms);
        match self
            .directory
            .replace(&record.owner, revision, record.clone())
            .await
        {
            Ok(record) => Ok(record),
            Err(error) => {
                // A competing first read may already have migrated the record.
                let latest = self
                    .directory
                    .get(&record.owner, &record.id)
                    .await
                    .map_err(super::safe)?;
                if let Some(latest) = latest
                    && latest.model_selection.is_some()
                {
                    return Ok(latest);
                }
                Err(super::safe(error))
            }
        }
    }
}

/// Caller holds Managed.commands, excluding model turns and naming starts.
/// This changes creator metadata only; Context and Journal HEAD stay untouched.
pub(super) async fn select(
    manager: &Manager,
    managed: &Managed,
    expected_revision: u64,
    selection: ModelSelection,
) -> Result<serde_json::Value, WireError> {
    let selection = normalize(selection);
    let owner = managed.record.lock().await.owner.clone();
    super::providers::resolve(manager, &owner, &selection).await?;
    // Constructibility is checked at admission, but the active route stays pinned.
    let _prepared = target(manager, &owner, &selection, managed.hub.clone())
        .await
        .map_err(super::safe)?;
    {
        let record = managed.record.lock().await;
        if record.revision != expected_revision {
            return Err(WireError::new(
                "revision_conflict",
                "Session revision changed",
            ));
        }
    }
    admission::remember_legacy_route(manager, managed)
        .await
        .map_err(super::safe)?;
    let mut record = managed.record.lock().await;
    if record.revision != expected_revision {
        return Err(WireError::new(
            "revision_conflict",
            "Session revision changed",
        ));
    }
    let mut next = record.clone();
    next.model_selection = Some(selection);
    next.revision = expected_revision
        .checked_add(1)
        .ok_or_else(|| WireError::new("revision_conflict", "Session revision exhausted"))?;
    next.updated_at_ms = super::now().max(record.updated_at_ms);
    let next = match manager
        .directory
        .replace(&record.owner, expected_revision, next)
        .await
    {
        Ok(next) => next,
        Err(error) => {
            // A failed disk publication can be ambiguous. Refuse subsequent work
            // until reopening reconciles the durable selection.
            managed
                .faulted
                .store(true, std::sync::atomic::Ordering::Release);
            return Err(super::safe(error));
        }
    };

    *record = next;
    drop(record);
    let mut snapshot = managed.hub.snapshot();
    let record = managed.record.lock().await;
    let status = if managed.session.inbox_snapshot().active || managed.hub.operation().is_some() {
        "running"
    } else {
        "idle"
    };
    let mut session = super::view::session(&record, &manager.config.provider.model, status);
    project_route(managed, &record, &mut session);
    snapshot["session"] = session.clone();
    managed.hub.session(session);
    Ok(snapshot)
}

pub(super) fn project_route(
    managed: &Managed,
    record: &SessionRecord,
    value: &mut serde_json::Value,
) {
    let applied = managed.routing.selection();
    value["applied_model_selection"] = serde_json::json!(&applied);
    value["turn_model_selection"] =
        if managed.session.has_continuation() || managed.session.inbox_snapshot().active {
            serde_json::json!(&applied)
        } else {
            serde_json::Value::Null
        };
    value["pending_model_change"] =
        serde_json::json!(record.model_selection.as_ref() != Some(&applied));
}
