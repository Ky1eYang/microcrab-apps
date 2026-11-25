//! Owner-scoped retained metadata pages. A cursor is an offset into one exact
//! ordered observation, not durable execution identity or a runtime command.
use super::{
    Managed, Manager, detached::detached_lineage, presentation::NativeExecution, safe, unavailable,
};
use microcrab_protocol::application::WireError;
use microcrab_session::{
    Error,
    runtime::execution::{ExecutionSnapshot, ExecutionStatus},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[cfg(test)]
mod tests;

const MAX_ITEMS: usize = 64;
const MAX_PAGE_BYTES: usize = 64 * 1024;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Query {
    cursor: Option<usize>,
    limit: Option<usize>,
    expected_revision: Option<String>,
}

struct Observation {
    lineage: String,
    epoch: u64,
    dormant: bool,
    rows: Vec<ExecutionSnapshot>,
    presentations: Option<std::sync::Arc<super::presentation::Presentations>>,
}

pub(super) async fn list(
    manager: &Manager,
    owner: &str,
    id: &str,
    query: Query,
) -> Result<Value, WireError> {
    if !crate::config::valid_id(id) {
        return Err(unavailable());
    }
    let _lifecycle = manager.journal_open_gate(id).lock_owned().await;
    // Preserve owner and project visibility without Manager::record's legacy
    // model-selection migration. No runtime, journal writer or receipt is opened.
    let record = manager
        .directory
        .get(owner, id)
        .await
        .map_err(safe)?
        .ok_or_else(unavailable)?;
    if !manager.project_visible(&record).await {
        return Err(unavailable());
    }
    let limit = query.limit.unwrap_or(MAX_ITEMS);
    if limit == 0 || limit > MAX_ITEMS {
        return Err(WireError::new(
            "invalid_request",
            "limit must be between 1 and 64",
        ));
    }
    let slot = manager
        .registry
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(id)
        .cloned();
    let mut live = None;
    if let Some(slot) = slot {
        let active = slot.active.lock().await.clone();
        if let Some(managed) = active {
            let observation = observe_live(&managed).await;
            // Ordinary close can retire this slot without the journal gate.
            // A read that waited behind close must not report a retired runtime.
            if manager
                .registry
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(id)
                .is_some_and(|current| std::sync::Arc::ptr_eq(current, &slot))
            {
                live = Some(observation?);
            }
        }
    }
    let observation = match live {
        Some(observation) => observation,
        None => observe_dormant(manager, id).await?,
    };
    if !manager.project_visible(&record).await {
        return Err(unavailable());
    }
    page(
        owner,
        id,
        observation,
        query.cursor.unwrap_or(0),
        limit,
        query.expected_revision.as_deref(),
    )
}

async fn observe_live(managed: &Managed) -> Result<Observation, WireError> {
    // History travel owns Managed.commands through both Journal publication and
    // Session replacement. The background runtime can still advance the epoch
    // or update metadata, so bracket the registry observation with selection reads.
    let _command = managed.commands.lock().await;
    for _ in 0..3 {
        let lineage = managed.journal.display_witness().await.lineage;
        let epoch = managed.session.state().epoch_id;
        let rows = managed.session.execution_snapshots();
        if managed.session.state().epoch_id == epoch
            && managed.journal.display_witness().await.lineage == lineage
        {
            return Ok(Observation {
                lineage,
                epoch,
                dormant: false,
                rows,
                presentations: Some(managed.presentations.clone()),
            });
        }
    }
    Err(safe(Error::Busy))
}

async fn observe_dormant(manager: &Manager, id: &str) -> Result<Observation, WireError> {
    let point = manager.inspect_detached_head(id).await.map_err(safe)?;
    let lineage = detached_lineage(&manager.instance, point.as_ref().map(|point| &point.cursor));
    let mut observation = Observation {
        lineage,
        epoch: 0,
        dormant: true,
        rows: vec![],
        presentations: None,
    };
    if let Some(point) = point {
        let state = point.checkpoint.recover.state;
        if point.cursor.session_id != id
            || point.checkpoint.session_id != id
            || point.cursor.epoch != state.epoch_id
        {
            return Err(WireError::new(
                "storage_unavailable",
                "Selected execution checkpoint identity does not match the Session",
            ));
        }
        observation.epoch = state.epoch_id;
        observation.rows = state
            .executions
            .iter()
            .map(|row| {
                let mut row = row.metadata();
                if row.status.is_pending() {
                    row.status = ExecutionStatus::Interrupted;
                }
                row
            })
            .collect();
    }
    Ok(observation)
}

fn serialization() -> WireError {
    WireError::new(
        "internal_error",
        "Retained execution metadata serialization failed",
    )
}

fn page(
    owner: &str,
    id: &str,
    observation: Observation,
    cursor: usize,
    limit: usize,
    expected_revision: Option<&str>,
) -> Result<Value, WireError> {
    let Observation {
        lineage,
        epoch,
        dormant,
        rows,
        presentations,
    } = observation;
    let wire_rows: Vec<_> = rows
        .iter()
        .map(|row| NativeExecution::new(row, presentations.as_deref()))
        .collect();
    // The opaque digest is independent of cursor/limit and includes every
    // serialized row (including those outside the page), in retained order.
    // Ordinary appends to the same history do not invalidate identical rows.
    let mut digest = Sha256::new();
    serde_json::to_writer(
        &mut digest,
        &(
            "execution-history-v1",
            owner,
            id,
            &lineage,
            epoch,
            dormant,
            &wire_rows,
        ),
    )
    .map_err(|_| serialization())?;
    let revision = format!("{:x}", digest.finalize());
    if expected_revision.is_some_and(|expected| expected != revision) {
        return Err(WireError::new(
            "execution_history_changed",
            "The selected execution history changed; restart paging",
        ));
    }
    if cursor > rows.len() {
        return Err(WireError::new(
            "invalid_request",
            "cursor exceeds the retained execution count",
        ));
    }
    let count = rows.len();
    let mut response = json!({"items":[], "next_cursor":null, "witness":{
        "session_id":id, "history_lineage":lineage, "history_epoch":epoch,
        "rows_revision":revision, "dormant":dormant
    }});
    // The base envelope includes [] and null. Add each serialized item, commas,
    // and the actual next cursor's replacement for null, with no guessed slack.
    let envelope = serde_json::to_vec(&response)
        .map_err(|_| serialization())?
        .len()
        - "null".len();
    let mut used = envelope;
    let mut items = Vec::new();
    let mut next_cursor = None;
    for (index, wire) in wire_rows.into_iter().enumerate().skip(cursor).take(limit) {
        let item = serde_json::to_value(wire).map_err(|_| serialization())?;
        let size = serde_json::to_vec(&item)
            .map_err(|_| serialization())?
            .len();
        let next = (index + 1 < count).then_some(index + 1);
        let cursor_size = next.map_or("null".len(), |offset| offset.to_string().len());
        let candidate = used + size + usize::from(!items.is_empty());
        if candidate + cursor_size > MAX_PAGE_BYTES {
            if items.is_empty() {
                return Err(WireError::new(
                    "execution_metadata_too_large",
                    format!(
                        "Retained execution {} metadata requires {size} bytes plus the page envelope and exceeds the {MAX_PAGE_BYTES}-byte history page budget",
                        rows[index].execution_id.0
                    ),
                ));
            }
            break;
        }
        used = candidate;
        items.push(item);
        next_cursor = next;
    }
    response["items"] = Value::Array(items);
    response["next_cursor"] = json!(next_cursor);
    Ok(response)
}
