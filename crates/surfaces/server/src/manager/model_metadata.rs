//! Application-owned durable model catalog. Transport fetches public data; this
//! owner publishes only snapshots whose atomic private-cache write succeeded.
use crate::config::{Config, ModelMetadataSettings};
use microcrab_session::Error;
use microcrab_transport::model_metadata::{
    MetadataSnapshot, ModelMatch, ModelMetadataManager, OpenRouterModelManager,
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

const SOURCE: &str = "https://openrouter.ai/api/v1/models?output_modalities=all";
#[cfg(test)]
mod tests;

pub(super) struct ModelMetadata {
    source: Arc<dyn ModelMetadataManager>,
    current: RwLock<Arc<MetadataSnapshot>>,
    path: PathBuf,
    policy: ModelMetadataSettings,
    refresh: AsyncMutex<()>,
    generation: std::sync::atomic::AtomicU64,
    changes: tokio::sync::watch::Sender<()>,
    failure: Mutex<Option<&'static str>>,
    cancel: CancellationToken,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl ModelMetadata {
    pub(super) async fn open(config: &Config) -> Result<Arc<Self>, Error> {
        config
            .model_metadata
            .validate()
            .map_err(|_| Error::config("invalid model metadata policy"))?;
        let source = OpenRouterModelManager::with_endpoint(
            SOURCE,
            Duration::from_millis(config.model_metadata.request_timeout_ms),
            config.model_metadata.max_response_bytes,
        )
        .map_err(|_| Error::config("model metadata source unavailable"))?;
        Self::with_source(config, Arc::new(source)).await
    }
    async fn with_source(
        config: &Config,
        source: Arc<dyn ModelMetadataManager>,
    ) -> Result<Arc<Self>, Error> {
        let path = config.data_dir.join("model-metadata.json");
        let mut cached = None;
        let mut failure = None;
        match tokio::fs::metadata(&path).await {
            Ok(meta)
                if meta.len()
                    <= config.model_metadata.max_response_bytes.saturating_mul(2) as u64 =>
            {
                let bytes = tokio::fs::read(&path).await?;
                match serde_json::from_slice::<MetadataSnapshot>(&bytes) {
                    Ok(value) if value.validate().is_ok() => cached = Some(Arc::new(value)),
                    _ => failure = Some("invalid_cached_metadata"),
                }
            }
            Ok(_) => failure = Some("cached_metadata_too_large"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(Arc::new(Self {
            current: RwLock::new(cached.unwrap_or_else(|| source.snapshot())),
            source,
            path,
            policy: config.model_metadata.clone(),
            refresh: AsyncMutex::new(()),
            generation: std::sync::atomic::AtomicU64::new(0),
            changes: tokio::sync::watch::channel(()).0,
            failure: Mutex::new(failure),
            cancel: CancellationToken::new(),
            worker: Mutex::new(None),
        }))
    }
    pub(super) fn changes(&self) -> tokio::sync::watch::Receiver<()> {
        self.changes.subscribe()
    }
    pub(super) fn snapshot(&self) -> Arc<MetadataSnapshot> {
        self.current
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
    pub(super) fn context_window(
        &self,
        config: &Config,
        selection: &microcrab_storage::ModelSelection,
    ) -> Option<u64> {
        config
            .model_choices()
            .into_iter()
            .find(|choice| {
                choice.provider_id == selection.provider_id && choice.id == selection.model
            })
            .and_then(|choice| choice.context_window_tokens)
            .or_else(|| {
                self.snapshot()
                    .find(&selection.provider_id, &selection.model)
                    .as_ref()
                    .and_then(capacity)
            })
    }
    pub(super) fn enrich_choice(
        &self,
        choice: crate::config::ModelChoice,
        snapshot: &MetadataSnapshot,
    ) -> Value {
        let found = snapshot.find(&choice.provider_id, &choice.id);
        let mut value = serde_json::to_value(&choice).expect("model choices are JSON-compatible");
        if let Some(found) = found {
            if choice.context_window_tokens.is_none() {
                value["context_window_tokens"] = json!(capacity(&found));
            }
            value["metadata_match"] = json!(found.kind);
            value["metadata"] = json!(found.metadata);
        }
        value
    }
    #[cfg(test)]
    pub(super) fn choices(&self, config: &Config) -> Vec<Value> {
        let snapshot = self.snapshot();
        config
            .model_choices()
            .into_iter()
            .map(|choice| self.enrich_choice(choice, &snapshot))
            .collect()
    }
    pub(super) fn enrich_statistics(
        &self,
        record: &microcrab_storage::SessionRecord,
        config: &Config,
        value: &mut Value,
    ) {
        let selection = record
            .model_selection
            .clone()
            .unwrap_or_else(|| super::model::default_selection(config));
        let snapshot = self.snapshot();
        let found = snapshot.find(&selection.provider_id, &selection.model);
        let configured = config
            .model_choices()
            .into_iter()
            .find(|choice| {
                choice.provider_id == selection.provider_id && choice.id == selection.model
            })
            .and_then(|choice| choice.context_window_tokens);
        let model = &mut value["model"];
        let window = configured.or_else(|| found.as_ref().and_then(capacity));
        model["context_window_tokens"] = json!(window);
        model["context_window_source"] = json!(if configured.is_some() {
            Some("configured")
        } else if window.is_some() {
            Some("openrouter")
        } else {
            None
        });
        model["metadata"] = json!(found.as_ref().map(|found| &found.metadata));
        model["metadata_match"] = json!(found.as_ref().map(|found| &found.kind));
        model["metadata_fetched_at_ms"] = json!(found.as_ref().map(|_| snapshot.fetched_at_ms));
        let input = value["provider"]["latest_prompt"]["input_tokens"].as_u64();
        let freshness = value["provider"]["latest_prompt"]["freshness"]
            .as_str()
            .unwrap_or("unknown")
            .to_owned();
        let headroom = (freshness == "current")
            .then_some(())
            .and(window.zip(input))
            .and_then(|(window, input)| window.checked_sub(input));
        value["model"]["measured_headroom_tokens"] = json!(headroom);
        if window.is_none() {
            value["model"]["headroom_reason"] = json!("model_window_unknown");
        } else if input.is_none() {
            value["model"]["headroom_reason"] = json!("provider_input_tokens_unavailable");
        } else if headroom.is_some() {
            value["model"]["headroom_reason"] = json!("measured_current_request");
        } else if freshness == "current" {
            value["model"]["headroom_reason"] = json!("measured_input_exceeds_model_window");
        } else {
            value["model"]["headroom_reason"] =
                value["provider"]["latest_prompt"]["freshness_reason"].clone();
        }
    }
    pub(super) fn status(&self) -> Value {
        let snapshot = self.snapshot();
        json!({"source":"openrouter","fetched_at_ms":(snapshot.fetched_at_ms > 0).then_some(snapshot.fetched_at_ms),"models":snapshot.models.len(),"refresh_interval_secs":self.policy.refresh_interval_secs,"enabled":self.policy.enabled,"last_error":*self.failure.lock().unwrap_or_else(|error| error.into_inner())})
    }
    pub(super) async fn refresh(&self) -> Result<Value, Error> {
        // Concurrent manual/periodic callers that waited for the same fresh
        // publication reuse it rather than issuing one fetch per waiter.
        let generation = self.generation.load(std::sync::atomic::Ordering::Acquire);
        let _gate = self.refresh.lock().await;
        if self.generation.load(std::sync::atomic::Ordering::Acquire) != generation {
            let failed = self
                .failure
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_some();
            return if failed {
                Err(Error::protocol("model metadata refresh failed"))
            } else {
                Ok(self.status())
            };
        }
        let result = async {
            let snapshot = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Err(Error::Closed),
                result = self.source.refresh() => result.map_err(|_| Error::protocol("model metadata refresh failed"))?,
            };
            self.publish_snapshot(snapshot).await
        }.await;
        *self
            .failure
            .lock()
            .unwrap_or_else(|error| error.into_inner()) =
            result.as_ref().err().map(|_| "refresh_failed");
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        result?;
        Ok(self.status())
    }
    async fn publish_snapshot(&self, snapshot: Arc<MetadataSnapshot>) -> Result<(), Error> {
        snapshot
            .validate()
            .map_err(|_| Error::protocol("invalid model metadata snapshot"))?;
        let bytes = serde_json::to_vec(&*snapshot)?;
        // Typed optional fields and the snapshot envelope can add cache bytes.
        if bytes.len() > self.policy.max_response_bytes.saturating_mul(2) {
            return Err(Error::protocol("model metadata cache budget exceeded"));
        }
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || persist(&path, &bytes))
            .await
            .map_err(|_| Error::protocol("model metadata persistence worker failed"))??;
        *self
            .current
            .write()
            .unwrap_or_else(|error| error.into_inner()) = snapshot;
        // Coalescing notification only: readers reproject from the durable snapshot.
        self.changes.send_replace(());
        Ok(())
    }
    /// Start only from application assembly. Direct Manager embeddings decide
    /// whether to use external discovery, and tests never depend on public IO.
    #[cfg(not(test))]
    pub(crate) fn start(self: &Arc<Self>) {
        if !self.policy.enabled {
            return;
        }
        self.start_worker(Duration::from_secs(self.policy.refresh_interval_secs));
    }
    fn start_worker(self: &Arc<Self>, period: Duration) {
        let mut worker = self
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if worker.is_some() {
            return;
        }
        // The timer observes application ownership; it must not keep its own
        // JoinHandle owner alive forever when an embedder drops the application.
        let owner = Arc::downgrade(self);
        let cancel = self.cancel.clone();
        *worker = Some(tokio::spawn(async move {
            let mut timer = tokio::time::interval(period);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { biased; _ = cancel.cancelled() => break, _ = timer.tick() => {} }
                let Some(owner) = owner.upgrade() else {
                    break;
                };
                let cached = owner.snapshot();
                if cached.fetched_at_ms > 0
                    && super::now().saturating_sub(cached.fetched_at_ms)
                        < owner.policy.refresh_interval_secs.saturating_mul(1000)
                {
                    continue;
                }
                // Cancellation may stop a fetch, but never abandons an admitted
                // atomic persistence worker: shutdown awaits the same refresh owner.
                if owner.refresh().await.is_err() {
                    tracing::warn!(
                        code = "model_metadata_refresh_failed",
                        "Public model metadata refresh failed; retaining last cache"
                    );
                }
            }
        }));
    }
    pub(super) async fn close(&self) -> Result<(), Error> {
        self.cancel.cancel();
        let worker = self
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(worker) = worker {
            worker
                .await
                .map_err(|_| Error::protocol("model metadata refresh owner failed"))?;
        }
        Ok(())
    }
}
impl Drop for ModelMetadata {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
fn capacity(found: &ModelMatch) -> Option<u64> {
    let catalog = found.metadata.context_length.filter(|size| *size > 0);
    let top = found
        .metadata
        .top_provider
        .as_ref()
        .and_then(|provider| provider.context_length)
        .filter(|size| *size > 0);
    match (catalog, top) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}
fn persist(path: &std::path::Path, bytes: &[u8]) -> Result<(), Error> {
    use std::io::Write;
    let parent = path
        .parent()
        .ok_or_else(|| Error::config("metadata cache directory unavailable"))?;
    std::fs::create_dir_all(parent)?;
    let temporary = parent.join(format!("model-metadata-{}.tmp", uuid::Uuid::new_v4()));
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temporary, path)?;
    Ok(())
}
