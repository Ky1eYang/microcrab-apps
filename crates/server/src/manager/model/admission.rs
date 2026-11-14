//! Creator-owned routing admission. Checkpoints atomically contain their route
//! beside frozen core Recover; equal request bodies never identify invocations.
use super::*;
use microcrab_session::{Recover, TurnRouting};
use microcrab_storage::{CreatorJournal, JournalCursor};

pub(crate) struct Admission {
    manager: std::sync::Weak<Manager>,
    owner: String,
    id: String,
    routing: Routing,
    hub: Arc<Hub>,
    journal: CreatorJournal,
    legacy: Option<(JournalCursor, ModelSelection)>,
    prepared: Mutex<Option<(Option<u64>, Target)>>,
    restoring: Mutex<Option<(Target, bool)>>,
}
impl Admission {
    pub(crate) fn new(
        manager: &Manager,
        record: &SessionRecord,
        routing: Routing,
        hub: Arc<Hub>,
        journal: CreatorJournal,
        legacy: Option<(JournalCursor, ModelSelection)>,
    ) -> Arc<Self> {
        Arc::new(Self {
            manager: manager.self_ref.clone(),
            owner: record.owner.clone(),
            id: record.id.clone(),
            routing,
            hub,
            journal,
            legacy,
            prepared: Mutex::new(None),
            restoring: Mutex::new(None),
        })
    }
}
#[async_trait]
impl TurnRouting for Admission {
    async fn prepare(&self) -> Result<Option<microcrab_session::ConfigurationEffect>, Error> {
        let manager = self.manager.upgrade().ok_or(Error::Closed)?;
        let record = manager
            .directory
            .get(&self.owner, &self.id)
            .await?
            .ok_or_else(|| Error::config("configured Session no longer exists"))?;
        let selection = record
            .model_selection
            .ok_or_else(|| Error::config("configured model selection is missing"))?;
        let prepared = target(&manager, &self.owner, &selection, self.hub.clone()).await?;
        if selection == self.routing.selection() && prepared.witness == self.routing.witness() {
            self.prepared
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            return Ok(None);
        }
        *self.prepared.lock().unwrap_or_else(|e| e.into_inner()) = Some((None, prepared));
        Ok(Some(
            microcrab_session::ConfigurationEffect::MODEL_SELECTION,
        ))
    }
    fn apply(&self, epoch: u64) -> Result<(), Error> {
        let mut prepared = self.prepared.lock().unwrap_or_else(|e| e.into_inner());
        let (position, _) = prepared
            .as_mut()
            .ok_or_else(|| Error::protocol("model admission has no prepared route"))?;
        *position = Some(epoch);
        Ok(())
    }
    fn client(&self, epoch: u64) -> Arc<dyn Client> {
        self.routing.epoch_client(epoch)
    }
    async fn before_checkpoint(&self, recover: &Recover) -> Result<(), Error> {
        let (selection, witness) = {
            let pending = self.prepared.lock().unwrap_or_else(|e| e.into_inner());
            match pending.as_ref() {
                Some((Some(epoch), target))
                    if *epoch == recover.context.epoch && !recover.context.pending_epoch =>
                {
                    (target.selection.clone(), target.witness.clone())
                }
                _ => (self.routing.selection(), self.routing.witness()),
            }
        };
        self.journal
            .prepare_model_route(selection, Some(witness))
            .await;
        let manager = self.manager.upgrade().ok_or(Error::Closed)?;
        manager
            .client_tools(&self.id)
            .await?
            .apply_configured(&recover.context)
            .await
    }
    async fn checkpoint_committed(&self, recover: &Recover) -> Result<(), Error> {
        let selected = self.journal.model_selection().await;
        let target = {
            let mut pending = self.prepared.lock().unwrap_or_else(|e| e.into_inner());
            if pending.as_ref().is_some_and(|(epoch, _)| {
                *epoch == Some(recover.context.epoch) && !recover.context.pending_epoch
            }) {
                pending.take().map(|(_, target)| target)
            } else {
                None
            }
        };
        if let Some(target) = target {
            if selected.as_ref() != Some(&target.selection) {
                return Err(Error::protocol(
                    "acknowledged Journal route differs from the admitted target",
                ));
            }
            self.routing.publish(target);
        }
        self.routing.pin_epoch(recover.context.epoch);
        Ok(())
    }
    async fn validate_restore(&self, _: &Recover) -> Result<(), Error> {
        let historical = self.journal.restore_model_route().await;
        let (selected, witness, has_head) = match historical {
            Some((cursor, selection, witness)) => {
                let selected = selection
                    .or_else(|| {
                        self.legacy
                            .as_ref()
                            .filter(|(legacy, _)| legacy == &cursor)
                            .map(|(_, model)| model.clone())
                    })
                    .unwrap_or_else(|| self.routing.selection());
                (selected, witness, true)
            }
            None => (self.routing.selection(), None, false),
        };
        let manager = self.manager.upgrade().ok_or(Error::Closed)?;
        let prepared = target(&manager, &self.owner, &selected, self.hub.clone()).await?;
        if witness
            .as_ref()
            .is_some_and(|witness| witness != &prepared.witness)
        {
            return Err(Error::config(
                "historical model wire route differs from current protocol, endpoint or auth policy; select a new context boundary instead of retrying the witnessed route",
            ));
        }
        let refresh = has_head && witness.is_none();
        if refresh {
            tracing::info!(
                "recovery route has no durable wire witness; rebuilding live context without cache continuity guarantee"
            );
        }
        *self.restoring.lock().unwrap_or_else(|e| e.into_inner()) = Some((prepared, refresh));
        Ok(())
    }
    fn refresh_context_on_restore(&self) -> bool {
        self.restoring
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|(_, refresh)| *refresh)
    }
    async fn restore(&self, recover: &Recover) -> Result<(), Error> {
        let (prepared, _) = self
            .restoring
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| Error::protocol("creator recovery has no validated route"))?;
        let selected = self.journal.model_selection().await;
        if selected
            .as_ref()
            .is_some_and(|selection| selection != &prepared.selection)
        {
            return Err(Error::protocol(
                "selected Journal route changed during recovery",
            ));
        }
        let manager = self.manager.upgrade().ok_or(Error::Closed)?;
        manager
            .client_tools(&self.id)
            .await?
            .reconcile_catalog(&recover.context.tools)
            .await?;
        self.prepared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        self.routing.publish(prepared);
        self.routing.pin_epoch(recover.context.epoch);
        Ok(())
    }
}

fn legacy_receipt_id(cursor: &JournalCursor) -> String {
    format!("model-route-{}", cursor.checkpoint_id)
}
pub(crate) async fn legacy_selection(
    manager: &Manager,
    record: &SessionRecord,
    cursor: &JournalCursor,
) -> Result<Option<ModelSelection>, Error> {
    let Some(receipt) = manager
        .directory
        .get_operation(&record.owner, &legacy_receipt_id(cursor))
        .await?
    else {
        return Ok(None);
    };
    if receipt.session_id != record.id
        || receipt.method != "model.route.legacy"
        || receipt.payload_hash != cursor.checkpoint_id
    {
        return Err(Error::protocol(
            "legacy model route receipt identity mismatch",
        ));
    }
    let result = receipt
        .result
        .ok_or_else(|| Error::protocol("legacy model route receipt has no result"))?;
    let witnessed: JournalCursor = serde_json::from_value(result["cursor"].clone())?;
    if witnessed != *cursor {
        return Err(Error::protocol(
            "legacy model route receipt cursor mismatch",
        ));
    }
    Ok(Some(serde_json::from_value(
        result["model_selection"].clone(),
    )?))
}
pub(crate) async fn remember_legacy_route(
    manager: &Manager,
    managed: &Managed,
) -> Result<(), Error> {
    if managed.journal.model_selection().await.is_some() {
        return Ok(());
    }
    let Some(cursor) = managed.journal.cursor().await else {
        return Ok(());
    };
    let record = managed.record.lock().await.clone();
    if legacy_selection(manager, &record, &cursor).await?.is_some() {
        return Ok(());
    }
    let timestamp = super::super::now();
    manager
        .directory
        .create_operation(microcrab_storage::OperationRecord {
            version: microcrab_storage::RECORD_VERSION,
            id: legacy_receipt_id(&cursor),
            session_id: record.id,
            owner: record.owner,
            method: "model.route.legacy".into(),
            payload_hash: cursor.checkpoint_id.clone(),
            status: "completed".into(),
            revision: 0,
            result: Some(
                serde_json::json!({"cursor":cursor,"model_selection":managed.routing.selection()}),
            ),
            created_at_ms: timestamp,
            updated_at_ms: timestamp,
        })
        .await?;
    Ok(())
}
