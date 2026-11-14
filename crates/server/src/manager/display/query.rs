//! Authorized detached archive queries, paging and immutable source resolution.
use super::super::{Manager, hub::Hub};
use super::{error, selected_attempts, source, unadmitted_heads};
use crate::config::Storage;
use microcrab_protocol::application::WireError;
use microcrab_storage::{display::ArchiveRecord, history::AuditAnchor};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
};
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::manager) struct Request {
    after: Option<u64>,
    high_water: Option<u64>,
    limit: Option<usize>,
    history_lineage: Option<String>,
    child_session_id: Option<String>,
}
#[derive(Clone)]
pub(crate) struct Selection {
    lineage: String,
    high: u64,
    selected: HashSet<String>,
    sealed: HashSet<(String, u64)>,
    complete: bool,
    graph: Option<Arc<super::graph::PreparedGraph>>,
}
#[cfg(test)]
mod cache_tests {
    use super::*;

    #[test]
    fn cloning_selection_shares_prepared_graph() {
        let graph = microcrab_storage::display::DisplayGraph::empty(
            microcrab_storage::display::DisplayScope::new("owner", "root", "root"),
        );
        let selection = Selection {
            lineage: "lineage".into(),
            high: 0,
            selected: HashSet::new(),
            sealed: HashSet::new(),
            complete: true,
            graph: Some(Arc::new(
                super::super::graph::PreparedGraph::new(&graph).unwrap(),
            )),
        };
        let cloned = selection.clone();
        assert!(Arc::ptr_eq(
            selection.graph.as_ref().unwrap(),
            cloned.graph.as_ref().unwrap(),
        ));
    }
}

pub(in crate::manager) async fn query(
    manager: &Manager,
    owner: &str,
    id: &str,
    request: Request,
) -> Result<Value, WireError> {
    query_cached(manager, owner, id, request, &mut None, None).await
}
pub(in crate::manager) async fn query_cached(
    manager: &Manager,
    owner: &str,
    id: &str,
    request: Request,
    selection: &mut Option<Selection>,
    reader_permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Result<Value, WireError> {
    if !crate::config::valid_id(id) {
        return Err(super::super::unavailable());
    }
    let record = manager
        .directory
        .get(owner, id)
        .await
        .map_err(super::super::safe)?
        .ok_or_else(super::super::unavailable)?;
    if !manager.project_visible(&record).await {
        return Err(super::super::unavailable());
    }
    let child = request.child_session_id.as_deref();
    if child.is_some_and(|id| {
        id.len() != 16
            || !id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }) {
        return Err(WireError::new(
            "invalid_params",
            "Invalid child Session identity",
        ));
    }
    let actual = child.unwrap_or(id).to_owned();
    let directory = manager.config.data_dir.join("sessions").join(id);
    let directory = if child.is_some() {
        directory.join("subsessions").join(&actual)
    } else {
        directory
    };
    // The same read-only path admission protects initial and referenced scopes.
    let sessions = manager.config.data_dir.join("sessions");
    let root_id = id.to_owned();
    let child_id = child.map(str::to_owned);
    let checked_directory = directory.clone();
    tokio::task::spawn_blocking(move || {
        check_scope(
            &sessions,
            &root_id,
            child_id.as_deref(),
            &checked_directory,
            false,
        )
    })
    .await
    .map_err(|_| error("Display scope worker failed"))??;
    let source = source(&manager.config, owner, id, &actual, directory.clone());
    let child_point = if child.is_some() {
        manager.child_checkpoint(&directory, &actual).await?
    } else {
        None
    };
    let child_selection = match child_point.as_ref() {
        Some(point) => manager.child_selection(owner, id, &actual, point).await?,
        None => None,
    };
    let child_lineage = child.is_some().then(|| {
        child_selection
            .as_ref()
            .map(|selection| selection.lineage.clone())
            .unwrap_or_else(|| {
                format!(
                    "child:{}:{}:{}",
                    manager.instance,
                    actual,
                    child_point
                        .as_ref()
                        .map(|point| point.cursor.checkpoint_id.as_str())
                        .unwrap_or("empty")
                )
            })
    });
    let mut anchor = AuditAnchor::default();
    let mut hub: Option<Arc<Hub>> = None;
    let mut live_anchor = None;
    if child.is_none() {
        let slot = manager
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .cloned();
        if let Some(slot) = slot {
            let active = slot.active.lock().await;
            if let Some(managed) = active.as_ref() {
                anchor = managed
                    .journal
                    .history_anchor()
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                live_anchor = Some(managed.journal.history_anchor());
                hub = Some(managed.hub.clone());
            }
        }
    }
    // Canonical lineage is a routing fence only. Durable display graph heads
    // select replay even when the model-facing Journal is absent or unavailable.
    let lineage = hub
        .as_ref()
        .and_then(|hub| {
            hub.snapshot()["history_view"]["lineage"]
                .as_str()
                .map(str::to_owned)
        })
        .or_else(|| child_lineage.clone())
        .or_else(|| request.history_lineage.clone())
        .unwrap_or_else(|| format!("display:{}:{}", manager.instance, actual));
    if (hub.is_some() || child.is_some())
        && request
            .history_lineage
            .as_ref()
            .is_some_and(|value| value != &lineage)
    {
        return Err(WireError::new("stale_lineage", "Display selection changed"));
    }
    let limit = request.limit.unwrap_or(128);
    if limit == 0 || limit > 1024 {
        return Err(WireError::new(
            "invalid_params",
            "Display page limit must be 1..1024",
        ));
    }
    let graph_source = source.clone();
    let graph_after = request.after.unwrap_or(0);
    let graph_high = request
        .high_water
        .or_else(|| hub.as_ref().map(|hub| hub.display_high_water()));
    let graph_cached = selection
        .as_ref()
        .filter(|cached| cached.lineage == lineage && Some(cached.high) == graph_high)
        .and_then(|cached| cached.graph.clone());
    let (graph_page, reader_permit) = tokio::task::spawn_blocking(move || {
        let graph = match graph_cached {
            Some(graph) => Ok(Some(graph)),
            None => graph_source
                .graph_at(graph_high)
                .and_then(|graph| {
                    graph
                        .as_ref()
                        .map(super::graph::PreparedGraph::new)
                        .transpose()
                })
                .map(|graph| graph.map(Arc::new)),
        };
        let result = graph.and_then(|graph| {
            let page = graph
                .as_ref()
                .map(|graph| {
                    super::graph::page_from_prepared(&graph_source, graph, graph_after, limit)
                })
                .transpose()?
                .flatten();
            Ok((page, graph))
        });
        (result, reader_permit)
    })
    .await
    .map_err(|_| error("Display graph worker failed"))?;
    let (graph_page, graph) = graph_page.map_err(|cause| {
        WireError::new(
            "display_unavailable",
            format!("Display graph read failed: {cause}"),
        )
    })?;
    let graph_page = if let Some(mut page) = graph_page {
        let records: Vec<ArchiveRecord> =
            serde_json::from_value(page["records"].clone()).map_err(|cause| {
                WireError::new(
                    "display_unavailable",
                    format!("Display graph records could not be decoded: {cause}"),
                )
            })?;
        let mut resolved = Vec::with_capacity(records.len());
        for record in records {
            resolved.push(resolve_fork_record(manager, owner, record).await?);
        }
        page["records"] = json!(resolved);
        Some(page)
    } else {
        None
    };
    if graph
        .as_ref()
        .is_some_and(|graph| graph.selection_known && graph.legacy_content_records == 0)
        && let Some(mut page) = graph_page.clone()
    {
        if hub.as_ref().is_some_and(|hub| {
            hub.snapshot()["history_view"]["lineage"].as_str() != Some(lineage.as_str())
        }) {
            return Err(WireError::new(
                "stale_lineage",
                "Display selection changed while loading graph",
            ));
        }
        if child.is_some() {
            let current = manager.child_checkpoint(&directory, &actual).await?;
            let runtime_changed = if let Some(selection) = &child_selection {
                !selection.current().await
            } else {
                false
            };
            if current.as_ref().map(|point| &point.cursor)
                != child_point.as_ref().map(|point| &point.cursor)
                || runtime_changed
            {
                return Err(WireError::new(
                    "stale_lineage",
                    "Child display selection changed while loading graph",
                ));
            }
        }
        page["history_lineage"] = json!(lineage);
        *selection = Some(Selection {
            lineage,
            high: page["high_water"].as_u64().unwrap_or(0),
            selected: HashSet::new(),
            sealed: HashSet::new(),
            complete: true,
            graph,
        });
        return Ok(page);
    }
    // Legacy records retain their historical read-only projection. No graph
    // identity or placement is inferred for bytes written before this contract.
    let point = if child.is_some() {
        child_point
    } else {
        manager.child_checkpoint(&directory, &actual).await?
    };
    if hub.is_none()
        && let Some(point) = &point
    {
        anchor.history_head = Some(point.cursor.checkpoint_id.clone());
        anchor.request_head = point.checkpoint.request_head.clone();
    }
    if let Some(selection) = &child_selection {
        anchor.display_lineage = selection.display_lineage().to_owned();
    }
    let lineage = if hub.is_some() {
        lineage
    } else if let Some(child_lineage) = child_lineage {
        child_lineage
    } else {
        format!(
            "detached:{}:{}",
            manager.instance,
            anchor.history_head.as_deref().unwrap_or("empty")
        )
    };
    if request
        .history_lineage
        .as_ref()
        .is_some_and(|value| value != &lineage)
    {
        return Err(WireError::new("stale_lineage", "Display selection changed"));
    }
    let journal_lineage = anchor.display_lineage.clone();
    let mut heads: HashSet<String> = anchor.request_head.into_iter().collect();
    let mut checkpoints = HashSet::new();
    if let Some(head) = anchor.history_head {
        let ancestry = match manager.config.storage {
            Storage::File => {
                #[cfg(feature = "storage-file")]
                {
                    microcrab_storage::history::FileJournal::inspect_request_heads(
                        &directory,
                        actual.clone(),
                        head,
                    )
                    .await
                }
                #[cfg(not(feature = "storage-file"))]
                {
                    Err(microcrab_session::Error::config("File storage unavailable"))
                }
            }
            Storage::Sqlite => {
                #[cfg(feature = "storage-sqlite")]
                {
                    microcrab_storage::history::SqliteJournal::inspect_request_heads(
                        manager.config.data_dir.join("storage.sqlite"),
                        actual.clone(),
                        head,
                    )
                    .await
                }
                #[cfg(not(feature = "storage-sqlite"))]
                {
                    Err(microcrab_session::Error::config(
                        "SQLite storage unavailable",
                    ))
                }
            }
        }
        .map_err(|_| error("Selected display ancestry unavailable"))?;
        for (checkpoint, request_head) in ancestry {
            checkpoints.insert(checkpoint);
            if let Some(head) = request_head {
                heads.insert(head);
            }
        }
    }
    // Reuse the established exact prepared-request receipt authority. The archive
    // never chooses a failed sibling merely because its checkpoint is selected.
    let mut selected_failure = None;
    if child.is_none() {
        let operations = manager
            .directory
            .list_operations(owner, id)
            .await
            .map_err(super::super::safe)?;
        let selected_operation = if let Some(hub) = &hub {
            hub.snapshot()["session"]["retry_operation_id"]
                .as_str()
                .map(str::to_owned)
        } else if let Some(point) = &point {
            if point
                .checkpoint
                .recover
                .continuation
                .as_ref()
                .is_some_and(|continuation| continuation.request_prepared)
            {
                let witness = super::super::view::retry_witness(
                    &point.checkpoint.recover.context,
                    &point.checkpoint.recover.state,
                )
                .map_err(super::super::safe)?;
                super::super::view::retry_operation_id(&operations, &witness)
            } else {
                None
            }
        } else {
            None
        };
        if let Some(operation) = operations
            .iter()
            .find(|operation| Some(operation.id.as_str()) == selected_operation.as_deref())
            && let Some(attempt) = operation
                .result
                .as_ref()
                .and_then(|result| result["partial_output"]["display_ref"]["attempt_id"].as_str())
        {
            selected_failure = Some((operation.id.clone(), attempt.to_owned()));
        }
    }
    let limit = request.limit.unwrap_or(128);
    if limit == 0 || limit > 1024 {
        return Err(WireError::new(
            "invalid_params",
            "Display page limit must be 1..1024",
        ));
    }
    let after = request.after.unwrap_or(0);
    let high = request
        .high_water
        .or_else(|| hub.as_ref().map(|hub| hub.display_high_water()));
    let selection_before = hub
        .as_ref()
        .map(|hub| hub.snapshot()["history_view"]["lineage"].clone());
    let cached = selection
        .clone()
        .filter(|selection| selection.lineage == journal_lineage);
    let prepared_lineage = journal_lineage.clone();
    let (result, mut prepared) = tokio::task::spawn_blocking(move || {
        // Dropping the async waiter cannot cancel a running storage read. Keep
        // admission here until that read exits, including replaced subscriptions.
        let _reader_permit = reader_permit;
        let prepared = if let Some(cached) = cached.filter(|cached| Some(cached.high) == high) {
            cached
        } else {
            // Metadata selection is independent of audit body retention and its attempt
            // limit. Scan archive pages through a fixed verified committed boundary.
            let Some(first) = source.query(0, high, 256)? else {
                return Ok::<_, microcrab_session::Error>((None, None));
            };
            let high = first.high_water;
            let mut all: Vec<_> = first
                .records
                .into_iter()
                .filter(|row| {
                    matches!(
                        row.event["phase"].as_str(),
                        Some(
                            "started"
                                | "admission_binding"
                                | "fork_record"
                                | "fork_seal"
                                | "selection_intent"
                                | "selection_commit"
                                | "selection_abort"
                                | "archive_boundary"
                        )
                    )
                })
                .collect();
            let mut next = first.next_cursor;
            while let Some(cursor) = next {
                let page = source.query(cursor, Some(high), 256)?.ok_or_else(|| {
                    microcrab_session::Error::audit("Display archive disappeared")
                })?;
                next = page.next_cursor;
                all.extend(page.records.into_iter().filter(|row| {
                    matches!(
                        row.event["phase"].as_str(),
                        Some(
                            "started"
                                | "admission_binding"
                                | "fork_record"
                                | "fork_seal"
                                | "selection_intent"
                                | "selection_commit"
                                | "selection_abort"
                                | "archive_boundary"
                        )
                    )
                }));
            }
            let (unadmitted, selection_pending) =
                unadmitted_heads(&all, &checkpoints, selected_failure.as_ref());
            heads.extend(unadmitted);
            let mut fork_counts: HashMap<(String, u64), u64> = HashMap::new();
            for pointer in &all {
                if pointer.event["phase"] == "fork_record"
                    && let (Some(source), Some(high)) = (
                        pointer.event["source_session_id"].as_str(),
                        pointer.event["source_high_water"].as_u64(),
                    )
                {
                    *fork_counts.entry((source.to_owned(), high)).or_default() += 1;
                }
            }
            let sealed: HashSet<_> = all
                .iter()
                .filter_map(|seal| {
                    if seal.event["phase"] != "fork_seal" {
                        return None;
                    }
                    let source = seal.event["source_session_id"].as_str()?.to_owned();
                    let high = seal.event["source_high_water"].as_u64()?;
                    let count = seal.event["pointer_count"].as_u64()?;
                    (fork_counts
                        .get(&(source.clone(), high))
                        .copied()
                        .unwrap_or(0)
                        == count)
                        .then_some((source, high))
                })
                .collect();
            let boundary_available = all
                .iter()
                .rev()
                .find(|row| row.event["phase"] == "archive_boundary")
                .is_none_or(|row| row.event["coverage"] != "unavailable");
            let fork_complete = boundary_available
                && !selection_pending
                && fork_counts.keys().all(|scope| sealed.contains(scope));
            let selected = selected_attempts(&all, &heads);
            Selection {
                lineage: prepared_lineage,
                high,
                selected,
                sealed,
                complete: fork_complete,
                graph: None,
            }
        };
        let Selection {
            high,
            ref selected,
            ref sealed,
            complete,
            ..
        } = prepared;
        let page = source
            .query(after, Some(high), limit)?
            .ok_or_else(|| microcrab_session::Error::audit("Display archive disappeared"))?;
        let records: Vec<_> = page
            .records
            .into_iter()
            .filter(|row| {
                if row.event["phase"] == "fork_record" {
                    return row.event["source_session_id"]
                        .as_str()
                        .zip(row.event["source_high_water"].as_u64())
                        .is_some_and(|(source, high)| sealed.contains(&(source.to_owned(), high)));
                }
                selected.contains(&row.attempt_id)
                    && !matches!(
                        row.event["phase"].as_str(),
                        Some(
                            "admission_binding"
                                | "fork_seal"
                                | "selection_intent"
                                | "selection_commit"
                                | "selection_abort"
                                | "archive_boundary"
                        )
                    )
            })
            .collect();
        Ok((
            Some((records, high, page.next_cursor, complete)),
            Some(prepared),
        ))
    })
    .await
    .map_err(|_| error("Display read worker failed"))?
    .map_err(|_| error("Display archive read failed"))?;
    if let Some(prepared) = &mut prepared {
        prepared.graph = graph;
    }
    *selection = prepared;
    if hub
        .as_ref()
        .map(|hub| hub.snapshot()["history_view"]["lineage"].clone())
        != selection_before
    {
        return Err(WireError::new(
            "stale_lineage",
            "Display selection changed while loading",
        ));
    }
    if live_anchor.as_ref().is_some_and(|anchor| {
        anchor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .display_lineage
            != journal_lineage
    }) {
        return Err(WireError::new(
            "stale_lineage",
            "Display Journal selection changed while loading",
        ));
    }
    if hub.is_none() {
        let current = manager.child_checkpoint(&directory, &actual).await?;
        if current.as_ref().map(|point| &point.cursor) != point.as_ref().map(|point| &point.cursor)
        {
            return Err(WireError::new(
                "stale_lineage",
                "Display selection changed while loading",
            ));
        }
    }
    if let Some(selection) = &child_selection {
        if !selection.current().await {
            return Err(WireError::new(
                "stale_lineage",
                "Child display selection changed while resolving source records",
            ));
        }
    }
    let result = if let Some((records, high, next, complete)) = result {
        let mut resolved = Vec::with_capacity(records.len());
        for record in records {
            resolved.push(resolve_fork_record(manager, owner, record).await?);
        }
        Some((resolved, high, next, complete))
    } else {
        None
    };
    // Source-reference reads can await storage too; recheck selection after all
    // dereferences, immediately before handing a response to the caller.
    if live_anchor.as_ref().is_some_and(|anchor| {
        anchor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .display_lineage
            != journal_lineage
    }) || hub
        .as_ref()
        .map(|hub| hub.snapshot()["history_view"]["lineage"].clone())
        != selection_before
    {
        return Err(WireError::new(
            "stale_lineage",
            "Display selection changed while resolving source records",
        ));
    }
    if hub.is_none() {
        let current = manager.child_checkpoint(&directory, &actual).await?;
        if current.as_ref().map(|point| &point.cursor) != point.as_ref().map(|point| &point.cursor)
        {
            return Err(WireError::new(
                "stale_lineage",
                "Display selection changed while resolving source records",
            ));
        }
    }
    if let Some(selection) = &child_selection {
        if !selection.current().await {
            return Err(WireError::new(
                "stale_lineage",
                "Child display selection changed while resolving source records",
            ));
        }
    }
    let mut response = match result {
        Some((records, high, next, complete)) => {
            json!({"records":records,"high_water":high,"next_cursor":next,"history_lineage":lineage,"coverage":if complete {"available"}else{"unavailable"}})
        }
        None => {
            json!({"records":[],"high_water":0,"next_cursor":null,"history_lineage":lineage,"coverage":"missing"})
        }
    };
    if let Some(graph_page) = graph_page {
        // Historical bytes keep their established read-only selection. Only the
        // display graph chooses graph-owned records, including retry exclusions.
        let graph_attempts = selection
            .as_ref()
            .and_then(|selection| selection.graph.as_ref())
            .map(|graph| &graph.all_attempts);
        let mut records: Vec<Value> = response["records"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|row| {
                row["event"].get("display_block").is_none()
                    && row["event"]["display_graph_version"] != 1
                    && row["attempt_id"].as_str().is_none_or(|attempt| {
                        graph_attempts.is_none_or(|attempts| !attempts.contains(attempt))
                    })
            })
            .cloned()
            .chain(
                graph_page["records"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .cloned(),
            )
            .collect();
        records.sort_by_key(|row| row["sequence"].as_u64().unwrap_or(0));
        records.dedup_by_key(|row| row["sequence"].as_u64().unwrap_or(0));
        response["records"] = json!(records);
        response["graph"] = graph_page["graph"].clone();
        response["legacy_ordering"] = json!("incomplete");
    }
    if selection
        .as_ref()
        .and_then(|selection| selection.graph.as_ref())
        .is_some_and(|graph| {
            !graph.selection_known || graph.projection_incomplete || graph.selection_pending
        })
    {
        response["coverage"] = json!("unavailable");
    }
    Ok(response)
}

/// Immutable references use the target archive sequence for paging and retain
/// exact source record hashes solely as integrity evidence, never as invocation IDs.
pub(super) async fn resolve_fork_record(
    manager: &Manager,
    owner: &str,
    mut record: ArchiveRecord,
) -> Result<ArchiveRecord, WireError> {
    let mut event = record.event;
    let mut visited = HashSet::new();
    while event["phase"] == "fork_record" {
        let id = event["source_session_id"]
            .as_str()
            .filter(|id| crate::config::valid_id(id))
            .ok_or_else(|| error("Invalid display fork source"))?
            .to_owned();
        let sequence = event["source_sequence"]
            .as_u64()
            .filter(|sequence| *sequence > 0)
            .ok_or_else(|| error("Invalid display fork cursor"))?;
        let high = event["source_high_water"]
            .as_u64()
            .filter(|high| *high >= sequence)
            .ok_or_else(|| error("Invalid display fork boundary"))?;
        if !visited.insert((id.clone(), sequence)) {
            return Err(error("Display fork reference cycle"));
        }
        let source_record = manager
            .directory
            .get(owner, &id)
            .await
            .map_err(super::super::safe)?
            .ok_or_else(|| error("Display fork source missing"))?;
        if !manager.project_visible(&source_record).await {
            return Err(super::super::unavailable());
        }
        let sessions = manager.config.data_dir.join("sessions");
        let directory = sessions.join(&id);
        let checked_id = id.clone();
        let checked_directory = directory.clone();
        tokio::task::spawn_blocking(move || {
            check_scope(&sessions, &checked_id, None, &checked_directory, true)
        })
        .await
        .map_err(|_| error("Display fork scope worker failed"))??;
        let source = source(&manager.config, owner, &id, &id, directory);
        let original =
            tokio::task::spawn_blocking(move || source.query(sequence - 1, Some(high), 1))
                .await
                .map_err(|_| error("Display fork source worker failed"))?
                .map_err(|_| error("Display fork source unavailable"))?
                .and_then(|page| page.records.into_iter().next())
                .ok_or_else(|| error("Display fork source record missing"))?;
        let hash = microcrab_storage::history::content_hash(&original)
            .map_err(|_| error("Display fork source integrity unavailable"))?;
        if event["source_record_hash"].as_str() != Some(hash.as_str())
            || event["source_attempt_id"].as_str() != Some(original.attempt_id.as_str())
        {
            return Err(error("Display fork source integrity mismatch"));
        }
        event = original.event;
    }
    record.event = event;
    Ok(record)
}

/// An owner-visible Directory record does not grant authority over a linked
/// filesystem target. Check each parent component before reading either Journal
/// metadata or an immutable fork source. This function never creates a path.
fn check_scope(
    sessions: &Path,
    root_id: &str,
    child: Option<&str>,
    directory: &Path,
    required: bool,
) -> Result<(), WireError> {
    if !directory
        .try_exists()
        .map_err(|_| error("Display path metadata unavailable"))?
    {
        return if required {
            Err(error("Display fork source directory missing"))
        } else {
            Ok(())
        };
    }
    let root = sessions
        .canonicalize()
        .map_err(|_| error("Display root unavailable"))?;
    let parent_path = sessions.join(root_id);
    check_ordinary_directory(&parent_path)?;
    let parent = parent_path
        .canonicalize()
        .map_err(|_| error("Display parent unavailable"))?;
    if parent.parent() != Some(root.as_path()) {
        return Err(error("Display parent escapes authorized root"));
    }
    let actual = directory
        .canonicalize()
        .map_err(|_| error("Display directory unavailable"))?;
    if let Some(child) = child {
        let children_path = parent_path.join("subsessions");
        check_ordinary_directory(&children_path)?;
        check_ordinary_directory(&children_path.join(child))?;
        let children = children_path
            .canonicalize()
            .map_err(|_| error("Display child root unavailable"))?;
        if children.parent() != Some(parent.as_path())
            || actual.parent() != Some(children.as_path())
        {
            return Err(error("Display child escapes authorized parent"));
        }
    } else if actual != parent {
        return Err(error("Display scope mismatch"));
    }
    Ok(())
}
fn check_ordinary_directory(path: &Path) -> Result<(), WireError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| error("Display directory metadata unavailable"))?;
    #[cfg(windows)]
    let linked = {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    };
    #[cfg(not(windows))]
    let linked = metadata.file_type().is_symlink();
    if linked || !metadata.is_dir() {
        return Err(error("Linked display scope is not authorized"));
    }
    Ok(())
}
#[cfg(test)]
mod tests;
