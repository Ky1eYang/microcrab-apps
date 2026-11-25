//! Selected display ancestry is reconstructed solely from durable display records.
use microcrab_session::Error;
use microcrab_storage::display::{DisplayGraph, DisplaySource};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
#[cfg(test)]
mod tests;

/// The wire order is the immutable parent chain, never timestamps or model rounds.
/// All nodes remain in storage; this envelope exposes only the selected branch.
pub(super) fn envelope(graph: &DisplayGraph) -> Result<Value, Error> {
    let mut nodes = Vec::new();
    let mut visited = HashSet::new();
    let mut cursor = graph
        .selection_known
        .then_some(graph.selected_head.as_deref())
        .flatten();
    while let Some(id) = cursor {
        if !visited.insert(id.to_owned()) {
            return Err(Error::audit("Display selected ancestry contains a cycle"));
        }
        let node = graph
            .blocks
            .get(id)
            .ok_or_else(|| Error::audit("Display selected ancestry references a missing block"))?;
        nodes.push(node.clone());
        cursor = node.parent_block_id.as_deref();
    }
    nodes.reverse();
    Ok(
        json!({"selected_head":if graph.selection_known { graph.selected_head.clone() } else { None },"selection_known":graph.selection_known,"revision":graph.high_water,
        "nodes":nodes,"complete":graph.selection_known && graph.legacy_content_records == 0 && graph.pending_selection.is_none() && !graph.projection_incomplete}),
    )
}

/// One immutable high-water selection per catch-up. Pages share this through Arc;
/// neither selected ancestry nor its wire nodes are rebuilt by subsequent readers.
/// The sequence index finds scanned nodes without walking the entire selected chain.
pub(super) struct PreparedGraph {
    pub(super) high_water: u64,
    pub(super) selection_known: bool,
    pub(super) legacy_content_records: u64,
    // Hybrid legacy replay excludes all graph-owned attempts, including
    // unselected siblings; the selected chain alone cannot certify ownership.
    pub(super) all_attempts: HashSet<String>,
    pub(super) projection_incomplete: bool,
    pub(super) selection_pending: bool,
    header: Value,
    nodes: Vec<Value>,
    ids: HashMap<String, usize>,
    attempts: HashSet<String>,
    sequences: Vec<(u64, usize)>,
    legacy_only: bool,
    #[cfg(test)]
    lookup_work: std::sync::atomic::AtomicUsize,
}

impl PreparedGraph {
    pub(super) fn new(graph: &DisplayGraph) -> Result<Self, Error> {
        let legacy_only =
            graph.blocks.is_empty() && graph.high_water > 0 && !graph.projection_incomplete;
        let mut header = if legacy_only {
            json!({"nodes": []})
        } else {
            envelope(graph)?
        };
        let nodes = header["nodes"]
            .as_array_mut()
            .expect("graph envelope nodes are an array");
        let nodes = std::mem::take(nodes);
        let ids = nodes
            .iter()
            .enumerate()
            .filter_map(|(index, node)| node["block_id"].as_str().map(|id| (id.to_owned(), index)))
            .collect();
        let attempts = nodes
            .iter()
            .filter_map(|node| node["attempt_id"].as_str().map(str::to_owned))
            .collect();
        let mut sequences: Vec<_> = nodes
            .iter()
            .enumerate()
            .filter_map(|(index, node)| node["sequence"].as_u64().map(|sequence| (sequence, index)))
            .collect();
        sequences.sort_unstable();
        Ok(Self {
            high_water: graph.high_water,
            selection_known: graph.selection_known,
            legacy_content_records: graph.legacy_content_records,
            all_attempts: graph
                .blocks
                .values()
                .map(|block| block.attempt_id.clone())
                .collect(),
            projection_incomplete: graph.projection_incomplete,
            selection_pending: graph.pending_selection.is_some(),
            header,
            nodes,
            ids,
            attempts,
            sequences,
            legacy_only,
            #[cfg(test)]
            lookup_work: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn sequence_end(&self, bound: u64) -> usize {
        self.sequences.partition_point(|(sequence, _)| {
            #[cfg(test)]
            self.lookup_work
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            *sequence <= bound
        })
    }

    fn emitted_nodes<'a>(
        &'a self,
        after: u64,
        scanned: u64,
        referenced: impl Iterator<Item = &'a str>,
    ) -> Vec<&'a Value> {
        let start = self.sequence_end(after);
        let end = self.sequence_end(scanned);
        let mut indices: Vec<_> = self.sequences[start..end.max(start)]
            .iter()
            .map(|(_, index)| {
                #[cfg(test)]
                self.lookup_work
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                *index
            })
            .collect();
        indices.extend(referenced.filter_map(|id| {
            #[cfg(test)]
            self.lookup_work
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.ids.get(id).copied()
        }));
        // A remapped source may precede this page, and sequence order need not
        // equal ancestry order. Emit each selected node once in parent-chain order.
        indices.sort_unstable();
        indices.dedup();
        indices
            .into_iter()
            .map(|index| &self.nodes[index])
            .collect()
    }
}

/// None means an old archive without graph identities; callers retain its read-only
/// historical query. New graph selection never asks Context or request ancestry.
#[cfg(test)]
pub(super) fn page(
    source: &DisplaySource,
    after: u64,
    high: Option<u64>,
    limit: usize,
) -> Result<Option<Value>, Error> {
    let Some(graph) = source.graph_at(high)? else {
        return Ok(None);
    };
    page_from_graph(source, &graph, after, limit)
}

#[cfg(test)]
pub(super) fn page_from_graph(
    source: &DisplaySource,
    graph: &DisplayGraph,
    after: u64,
    limit: usize,
) -> Result<Option<Value>, Error> {
    page_from_prepared(source, &PreparedGraph::new(graph)?, after, limit)
}

pub(super) fn page_from_prepared(
    source: &DisplaySource,
    graph: &PreparedGraph,
    after: u64,
    limit: usize,
) -> Result<Option<Value>, Error> {
    if graph.legacy_only {
        return Ok(None);
    }
    let page = source
        .query(after, Some(graph.high_water), limit)?
        .ok_or_else(|| Error::audit("Display graph archive disappeared"))?;
    let records: Vec<_> = page
        .records
        .into_iter()
        .filter(|record| {
            if record.event["phase"] == "display_fork_remap" {
                let (DisplaySource::File { scope, .. } | DisplaySource::Sqlite { scope, .. }) =
                    source;
                return record.event["target_session_id"].as_str() == Some(scope.session.as_str());
            }
            if let Some(id) = record.event["display_block"]["block_id"]
                .as_str()
                .or_else(|| record.event["display_source_block_id"].as_str())
            {
                graph.ids.contains_key(id)
            } else {
                // Lifecycle metadata carries no ordering authority and is included
                // only for an exact attempt already present on this selected chain.
                graph.attempts.contains(record.attempt_id.as_str())
                    && matches!(
                        record.event["phase"].as_str(),
                        Some(
                            "started"
                                | "validated"
                                | "invalidated"
                                | "display_binding"
                                | "fork_record"
                        )
                    )
            }
        })
        .collect();
    let scanned = page.next_cursor.unwrap_or(graph.high_water);
    let emitted_nodes = graph.emitted_nodes(
        after,
        scanned,
        records
            .iter()
            .filter_map(|record| record.event["display_source_block_id"].as_str()),
    );
    let mut selected = graph.header.clone();
    selected["nodes"] = json!(emitted_nodes);
    let complete = selected["complete"] == true;
    Ok(Some(
        json!({"records":records,"high_water":graph.high_water,
        "next_cursor":page.next_cursor,"graph":selected,
        "coverage":if complete {"available"} else {"unavailable"}}),
    ))
}
