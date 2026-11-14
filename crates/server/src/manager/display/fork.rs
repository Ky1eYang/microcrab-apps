//! Fork owns immutable display references; it never duplicates thought bodies or
//! injects them into the freshly materialized model Context.
use super::{selected_attempts, source};
use crate::manager::{Managed, Manager, safe};
use microcrab_protocol::application::WireError;
use microcrab_storage::display::ArchiveRecord;
use serde_json::json;
use std::collections::HashSet;

pub(in crate::manager) async fn record_fork(
    manager: &Manager,
    owner: &str,
    original: &Managed,
    target: &str,
    context: &microcrab_session::context::ContextSnapshot,
) -> Result<(), WireError> {
    let record = original.record.lock().await.clone();
    let mut remap =
        super::anchors::fork_remap_event(manager, owner, &record.id, target, context).await?;
    remap["display_graph_version"] = json!(1);
    let anchor = original
        .journal
        .history_anchor()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    let directory = manager.config.data_dir.join("sessions").join(&record.id);
    let graph_source = source(
        &manager.config,
        owner,
        &record.id,
        &record.id,
        directory.clone(),
    );
    let high_water = original.hub.display_high_water();
    let graph = tokio::task::spawn_blocking(move || graph_source.graph_at(Some(high_water)))
        .await
        .map_err(|_| WireError::new("display_unavailable", "Fork graph worker failed"))?
        .map_err(safe)?;
    let graph_only = graph
        .as_ref()
        .is_some_and(|graph| !graph.blocks.is_empty() && graph.legacy_content_records == 0);
    let mut heads: HashSet<String> = anchor.request_head.into_iter().collect();
    let mut checkpoints = HashSet::new();
    if !graph_only && let Some(head) = anchor.history_head {
        let ancestry = match manager.config.storage {
            crate::config::Storage::File => {
                #[cfg(feature = "storage-file")]
                {
                    microcrab_storage::history::FileJournal::inspect_request_heads(
                        &directory,
                        record.id.clone(),
                        head,
                    )
                    .await
                }
                #[cfg(not(feature = "storage-file"))]
                {
                    Err(microcrab_session::Error::config("File storage unavailable"))
                }
            }
            crate::config::Storage::Sqlite => {
                #[cfg(feature = "storage-sqlite")]
                {
                    microcrab_storage::history::SqliteJournal::inspect_request_heads(
                        manager.config.data_dir.join("storage.sqlite"),
                        record.id.clone(),
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
        .map_err(safe)?;
        for (checkpoint, request_head) in ancestry {
            checkpoints.insert(checkpoint);
            heads.extend(request_head);
        }
    }
    let source_archive = source(&manager.config, owner, &record.id, &record.id, directory);
    let target_archive = source(
        &manager.config,
        owner,
        target,
        target,
        manager.config.data_dir.join("sessions").join(target),
    );
    let target = target.to_owned();
    tokio::task::spawn_blocking(move || {
        // Old Sessions with no display archive remain a truthful missing source.
        let Some(first) = source_archive.query(0, Some(high_water), 128)? else {
            return Ok::<_, microcrab_session::Error>(());
        };
        if graph.as_ref().is_some_and(|graph| !graph.selection_known) {
            // Context fork remains supported. Persist the missing display prefix
            // before activation so reopening the destination cannot claim coverage.
            let mut writer = target_archive.open()?;
            if writer.revision()? != 0 {
                return Err(microcrab_session::Error::audit(
                    "Fork display destination requires reconciliation",
                ));
            }
            writer.append(0, &ArchiveRecord {
                sequence: 1,
                attempt_id: format!("fork-unavailable:{target}"),
                event: json!({"phase":"display_projection_unavailable", "display_graph_version":1,
                    "reason":"fork_historical_display_ordering_unavailable"}),
            })?;
            writer.flush()?;
            return Err(microcrab_session::Error::audit(
                "Fork historical display ordering is unavailable",
            ));
        }
        if graph
            .as_ref()
            .is_some_and(|graph| graph.pending_selection.is_some())
        {
            return Err(microcrab_session::Error::audit(
                "Fork graph selection requires reconciliation",
            ));
        }
        let mut starts = Vec::new();
        let mut page = first;
        loop {
            for row in page.records {
                if matches!(
                    row.event["phase"].as_str(),
                    Some(
                        "started"
                            | "fork_record"
                            | "fork_seal"
                            | "admission_binding"
                            | "selection_intent"
                            | "selection_commit"
                            | "selection_abort"
                    )
                ) {
                    starts.push(row);
                }
            }
            let Some(cursor) = page.next_cursor else {
                break;
            };
            page = source_archive
                .query(cursor, Some(high_water), 128)?
                .ok_or_else(|| {
                    microcrab_session::Error::audit("Fork display source disappeared")
                })?;
        }
        let mut pointer_counts = std::collections::HashMap::new();
        for row in &starts {
            if row.event["phase"] == "fork_record" {
                let key = (
                    row.event["source_session_id"].as_str().map(str::to_owned),
                    row.event["source_high_water"].as_u64(),
                );
                *pointer_counts.entry(key).or_insert(0_u64) += 1;
            }
        }
        for (key, count) in &pointer_counts {
            if !starts.iter().any(|row| {
                row.event["phase"] == "fork_seal"
                    && row.event["source_session_id"].as_str() == key.0.as_deref()
                    && row.event["source_high_water"].as_u64() == key.1
                    && row.event["pointer_count"].as_u64() == Some(*count)
            }) {
                return Err(microcrab_session::Error::audit(
                    "Fork source display prefix is incomplete",
                ));
            }
        }
        let (unadmitted, selection_pending) = super::unadmitted_heads(&starts, &checkpoints, None);
        if selection_pending {
            return Err(microcrab_session::Error::audit(
                "Fork display selection requires reconciliation",
            ));
        }
        heads.extend(unadmitted);
        let selected = if graph_only {
            HashSet::new()
        } else {
            selected_attempts(&starts, &heads)
        };
        let graph_envelope = graph.as_ref().map(super::graph::envelope).transpose()?;
        let graph_ids: HashSet<String> = graph_envelope
            .as_ref()
            .into_iter()
            .flat_map(|value| value["nodes"].as_array().into_iter().flatten())
            .filter_map(|node| node["block_id"].as_str().map(str::to_owned))
            .collect();
        let graph_attempts: HashSet<String> = graph_envelope
            .as_ref()
            .into_iter()
            .flat_map(|value| value["nodes"].as_array().into_iter().flatten())
            .filter_map(|node| node["attempt_id"].as_str().map(str::to_owned))
            .collect();
        let graph_all_attempts: HashSet<String> = graph
            .as_ref()
            .into_iter()
            .flat_map(|graph| graph.blocks.values())
            .map(|block| block.attempt_id.clone())
            .collect();
        let mut writer = target_archive.open()?;
        let mut sequence = writer.revision()?;
        // A fork destination must be fresh; do not recapture a newer source for
        // an interrupted receipt or append another independent inherited prefix.
        if sequence != 0 {
            return Err(microcrab_session::Error::audit(
                "Fork display destination requires reconciliation",
            ));
        }
        let mut cursor = 0;
        loop {
            let page = source_archive
                .query(cursor, Some(high_water), 128)?
                .ok_or_else(|| {
                    microcrab_session::Error::audit("Fork display source disappeared")
                })?;
            for row in page.records {
                let graph_block = row.event["display_block"]["block_id"]
                    .as_str()
                    .or_else(|| row.event["display_source_block_id"].as_str());
                let graph_selected = graph_block.is_some_and(|id| graph_ids.contains(id))
                    || (graph_block.is_none()
                        && graph_attempts.contains(&row.attempt_id)
                        && matches!(
                            row.event["phase"].as_str(),
                            Some(
                                "started"
                                    | "validated"
                                    | "invalidated"
                                    | "display_binding"
                                    | "fork_record"
                            )
                        ));
                let legacy_selected = graph_block.is_none()
                    && !graph_all_attempts.contains(&row.attempt_id)
                    && (selected.contains(&row.attempt_id)
                        || (!graph_only && row.event["phase"] == "fork_record"));
                if !graph_selected && !legacy_selected {
                    continue;
                }
                let graph_block = graph_block.map(str::to_owned);
                let next = sequence.checked_add(1).ok_or_else(|| {
                    microcrab_session::Error::audit("Fork display sequence exhausted")
                })?;
                let pointer = ArchiveRecord {
                    sequence: next,
                    attempt_id: row.attempt_id.clone(),
                    event: json!({
                        "phase":"fork_record",
                        "display_graph_version":1,
                        "source_session_id":record.id,
                        "source_sequence":row.sequence,
                        "source_high_water":high_water,
                        "source_attempt_id":row.attempt_id,
                        "display_source_block_id":graph_block,
                        "source_record_hash":microcrab_storage::history::content_hash(&row)?,
                    }),
                };
                writer.append(sequence, &pointer)?;
                sequence = next;
            }
            let Some(next) = page.next_cursor else {
                break;
            };
            cursor = next;
        }
        let seal = ArchiveRecord {
            sequence: sequence.checked_add(1).ok_or_else(|| {
                microcrab_session::Error::audit("Fork display sequence exhausted")
            })?,
            attempt_id: format!("fork:{target}"),
            event: json!({
                "phase":"fork_seal",
                "display_graph_version":1,
                "source_session_id":record.id,
                "source_high_water":high_water,
                "pointer_count":sequence,
            }),
        };
        writer.append(sequence, &seal)?;
        sequence = seal.sequence;
        if let Some(graph) = graph.filter(|graph| !graph.blocks.is_empty()) {
            let imported = ArchiveRecord {
                sequence: sequence.checked_add(1).ok_or_else(|| {
                    microcrab_session::Error::audit("Fork graph sequence exhausted")
                })?,
                attempt_id: format!("fork-graph:{target}"),
                event: graph.import_event()?,
            };
            writer.append(sequence, &imported)?;
            sequence = imported.sequence;
        }
        let remapping = ArchiveRecord {
            sequence: sequence
                .checked_add(1)
                .ok_or_else(|| microcrab_session::Error::audit("Fork remap sequence exhausted"))?,
            attempt_id: format!("fork-remap:{target}"),
            event: remap,
        };
        writer.append(sequence, &remapping)?;
        writer.flush()
    })
    .await
    .map_err(|_| WireError::new("display_unavailable", "Fork display worker failed"))?
    .map_err(safe)
}
