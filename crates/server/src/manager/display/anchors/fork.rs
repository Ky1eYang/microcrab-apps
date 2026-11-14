//! Fork correspondence is certified at the actual immutable Context copy. Equal
//! text, hashes, rounds and provider request bodies never select a source row.
use super::*;
use crate::manager::Manager;
use microcrab_protocol::application::WireError;
use microcrab_session::context::ContextSnapshot;

pub(in crate::manager) async fn fork_remap_event(
    manager: &Manager,
    owner: &str,
    source_id: &str,
    target_id: &str,
    context: &ContextSnapshot,
) -> Result<Value, WireError> {
    let source = super::super::source(
        &manager.config,
        owner,
        source_id,
        source_id,
        manager.config.data_dir.join("sessions").join(source_id),
    );
    let (mut index, pointers) = tokio::task::spawn_blocking(move || {
        let mut index = Index::new(&source);
        if source.revision().map_err(|e| e.to_string())?.is_none() {
            return Ok::<_, String>((index, vec![]));
        }
        index.refresh(&source)?;
        let mut pointers = Vec::new();
        let mut after = 0;
        while after < index.graph.high_water {
            let page = source
                .query(after, Some(index.graph.high_water), 128)
                .map_err(|e| e.to_string())?
                .ok_or("fork canonical source disappeared")?;
            if page.records.is_empty() {
                return Err("fork canonical scan made no progress".into());
            }
            after = page
                .records
                .last()
                .ok_or("fork canonical page empty")?
                .sequence;
            pointers.extend(
                page.records
                    .into_iter()
                    .filter(|row| row.event["phase"] == "fork_record"),
            );
        }
        Ok((index, pointers))
    })
    .await
    .map_err(|error| super::super::error(&format!("Fork canonical worker failed: {error}")))?
    .map_err(|error| super::super::error(&error))?;
    // The current source Session's direct remaps supersede inherited remaps of
    // the same source UUID. Resolve pointers through the existing authenticated,
    // scope-checked and hash-verified reader, including arbitrarily nested forks.
    let local_remaps = std::mem::take(&mut index.remaps);
    for pointer in pointers {
        let resolved = super::super::query::resolve_fork_record(manager, owner, pointer).await?;
        index
            .certificate(&resolved.event)
            .map_err(|error| super::super::error(&error))?;
    }
    index.remaps.extend(local_remaps);
    let selected = index
        .selected()
        .map_err(|error| super::super::error(&error))?;
    remap(&selected, source_id, target_id, context).map_err(|error| super::super::error(&error))
}

pub(super) fn remap(
    selected: &Selected,
    source_id: &str,
    target_id: &str,
    context: &ContextSnapshot,
) -> Result<Value, String> {
    let source =
        super::super::super::view::messages(context.items.clone(), context.epoch, source_id);
    let target =
        super::super::super::view::messages(context.items.clone(), context.epoch, target_id);
    let ordinals: Vec<_> = context
        .items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| (!matches!(item, ContentItem::State(_))).then_some(index))
        .collect();
    if source.len() != target.len() || source.len() != ordinals.len() {
        return Err("fork Context projection changed item correspondence".into());
    }
    let mut mappings = Vec::new();
    for ((source, mut message), ordinal) in source.into_iter().zip(target).zip(ordinals) {
        let native = source["id"]
            .as_str()
            .ok_or("fork source DTO has no native id")?;
        let Some(source_id) = selected.canonical.get(native) else {
            // Historical archives without admission certificates stay unknown.
            // A missing source identity is never filled from matching bodies.
            continue;
        };
        let blocks = selected
            .placements
            .get(native)
            .ok_or("fork source certificate has no selected placement")?;
        if blocks.is_empty() {
            return Err("fork source certificate placement empty".into());
        }
        message["canonical_source_id"] = json!(source_id);
        mappings.push(json!({
            "context_item_index":ordinal,"context_epoch":context.epoch,
            "canonical_source_id":source_id,"source_canonical_message_id":native,
            "canonical_message_id":message["id"],"producer_block_ids":blocks,
            "message":message,
        }));
    }
    Ok(
        json!({"phase":"display_fork_remap","source_session_id":source_id,
        "target_session_id":target_id,"context_epoch":context.epoch,"mappings":mappings}),
    )
}
