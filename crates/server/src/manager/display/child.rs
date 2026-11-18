//! Actual-child display resources and owned close finalization, isolated by ID.
use super::super::hub::Hub;
use crate::config::Config;
use async_trait::async_trait;
use microcrab_creator::audit::NativeAuditor;
use microcrab_session::{Error, Session};
use microcrab_storage::CreatorJournal;
use std::{path::Path, sync::Arc};
struct ChildDisplayResources {
    journal: CreatorJournal,
    recorder: Arc<microcrab_creator::display::DisplayRecorder>,
    identities: Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
}
#[derive(Clone)]
pub(in crate::manager) struct ChildDisplay {
    config: Config,
    owner: String,
    root: String,
    routing: super::super::model::Routing,
    hub: Arc<Hub>,
    #[expect(
        clippy::type_complexity,
        reason = "Child Journals retain exact owner, root and child identity in the existing shared registry"
    )]
    journals:
        Arc<std::sync::Mutex<std::collections::HashMap<(String, String, String), CreatorJournal>>>,
    archives: Arc<std::sync::Mutex<std::collections::HashMap<String, ChildDisplayResources>>>,
}
impl ChildDisplay {
    #[expect(
        clippy::type_complexity,
        reason = "Construction accepts the same canonical owner-root-child Journal registry rather than introducing an alias owner"
    )]
    pub(in crate::manager) fn new(
        config: Config,
        owner: String,
        root: String,
        routing: super::super::model::Routing,
        hub: Arc<Hub>,
        journals: Arc<
            std::sync::Mutex<std::collections::HashMap<(String, String, String), CreatorJournal>>,
        >,
    ) -> Self {
        Self {
            config,
            owner,
            root,
            routing,
            hub,
            journals,
            archives: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }
}
#[async_trait]
impl microcrab_creator::delegate::ChildDisplayFactory for ChildDisplay {
    fn for_epoch(
        &self,
        epoch: u64,
    ) -> Option<Arc<dyn microcrab_creator::delegate::ChildDisplayFactory>> {
        let mut captured = self.clone();
        captured.routing = self.routing.frozen_for_epoch(epoch);
        Some(Arc::new(captured))
    }
    async fn create(
        &self,
        directory: &Path,
        id: &str,
        journal: &CreatorJournal,
        auditor: Option<Arc<NativeAuditor>>,
    ) -> Result<Arc<dyn microcrab_session::Client>, Error> {
        journal
            .prepare_model_route(self.routing.selection(), Some(self.routing.witness()))
            .await;
        {
            let mut journals = self
                .journals
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let key = (self.owner.clone(), self.root.clone(), id.to_owned());
            // An evicted child falls back to checkpoint-based read-only identity;
            // no historical continuity is guessed after its runtime proof is gone.
            if journals.len() >= 4096
                && !journals.contains_key(&key)
                && let Some(evicted) = journals.keys().next().cloned()
            {
                journals.remove(&evicted);
            }
            journals.insert(key, journal.clone());
        }
        let Some(auditor) = auditor else {
            return Ok(self.routing.client(false));
        };
        let source = super::source(
            &self.config,
            &self.owner,
            &self.root,
            id,
            directory.to_owned(),
        );
        let anchor = journal.history_anchor();
        let identities = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let observed_identities = identities.clone();
        let child_id = id.to_owned();
        let committed_hub = Arc::downgrade(&self.hub);
        let failed_hub = Arc::downgrade(&self.hub);
        let failed_child = id.to_owned();
        let recorder = microcrab_creator::display::DisplayRecorder::open(source.clone(),
            Arc::new(move |observation| {
                let anchor=anchor.lock().unwrap_or_else(|e|e.into_inner()).clone();
                let identity=auditor.request_head(&observation.call_id);
                if let Some(identity)=&identity {observed_identities.lock().unwrap_or_else(|e|e.into_inner()).insert(observation.call_id.clone(),identity.clone());}
                serde_json::json!({"request_identity":identity,"parent_request":anchor.request_head,"history_head":anchor.history_head,"display_lineage":anchor.display_lineage})
            }),
            Arc::new(move |record| {if let Some(hub)=committed_hub.upgrade(){hub.publish(super::super::SessionRuntimeEvent::ChildDisplayAvailable(serde_json::json!({"child_session_id":child_id,"high_water":record.sequence})));}}),
            Arc::new(move |_message| {if let Some(hub)=failed_hub.upgrade(){hub.publish(super::super::SessionRuntimeEvent::ChildDisplayUnavailable(serde_json::json!({"child_session_id":failed_child,"coverage":"unavailable"})));}}),
        ).await;
        match recorder {
            Ok(recorder) => {
                journal.install_commit_observer(Arc::new(super::CheckpointDisplay {
                    recorder: recorder.clone(),
                    hub: Arc::downgrade(&self.hub),
                    child: Some(id.to_owned()),
                }))?;
                self.archives
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(
                        id.to_owned(),
                        ChildDisplayResources {
                            journal: journal.clone(),
                            recorder: recorder.clone(),
                            identities,
                        },
                    );
                let projection_hub = Arc::downgrade(&self.hub);
                let projection_child = id.to_owned();
                let failure = Arc::new(move |_cause: String| {
                    if let Some(hub) = projection_hub.upgrade() {
                        hub.publish(super::super::SessionRuntimeEvent::ChildDisplayUnavailable(
                            serde_json::json!({"child_session_id":projection_child,"coverage":"unavailable"}),
                        ));
                    }
                });
                Ok(self
                    .routing
                    .recorded_child(recorder, id.to_owned(), source, failure))
            }
            Err(error) => {
                self.hub
                    .publish(super::super::SessionRuntimeEvent::ChildDisplayUnavailable(
                        serde_json::json!({"child_session_id":id,"coverage":"unavailable"}),
                    ));
                tracing::warn!(error_code = %super::super::safe(error).code,"Child display archive unavailable");
                Ok(self.routing.client(false))
            }
        }
    }
    async fn finalize(
        &self,
        id: &str,
        _session: &Session,
        close_succeeded: bool,
    ) -> Result<(), Error> {
        // This registry owns only child-specific archival finalization resources;
        // it is drained after the creator-owned close, outside cancellation.
        let resource = self
            .archives
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);
        let Some(resource) = resource else {
            return Ok(());
        };
        let cursor = resource.journal.cursor().await;
        let anchor = resource
            .journal
            .history_anchor()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let attempt = anchor.request_head.as_ref().and_then(|request| {
            resource
                .identities
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .find_map(|(attempt, identity)| (identity == request).then(|| attempt.clone()))
        });
        let mut failed = false;
        if close_succeeded {
            if let (Some(cursor), Some(attempt), Some(identity)) =
                (cursor, attempt, anchor.request_head)
            {
                failed=resource.recorder.append(attempt,serde_json::json!({"phase":"admission_binding","admission":"child_closed",
                    "checkpoint_id":cursor.checkpoint_id,"request_identity":identity,"child_session_id":id})).await.is_err();
            }
        } else {
            failed = true;
            let _=resource.recorder.append(format!("child-close:{id}"),serde_json::json!({"phase":"archive_boundary","coverage":"unavailable","reason":"child_close_unacknowledged"})).await;
        }
        if close_succeeded && !failed {
            failed=resource.recorder.append(format!("child-close:{id}"),serde_json::json!({"phase":"archive_boundary","coverage":"available","reason":"child_close_acknowledged"})).await.is_err();
        }
        if resource.recorder.close().await.is_err() {
            failed = true;
        }
        if failed {
            self.hub
                .publish(super::super::SessionRuntimeEvent::ChildDisplayUnavailable(
                    serde_json::json!({"child_session_id":id,"coverage":"unavailable"}),
                ));
        }
        Ok(())
    }
}
