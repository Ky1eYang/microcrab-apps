//! Validate exact browser input rollback before staging one Journal selection.
//! Unsupported input representations and stale selections never mutate history.
use super::*;
use microcrab_session::{Recover, context::ContentItem};

pub(super) async fn prepare(
    managed: &Managed,
    params: Direction,
) -> Result<(Recover, Option<Value>), WireError> {
    let Some(message_id) = params.message_id else {
        if params.expected_checkpoint_id.is_some() {
            return Err(invalid("expected_checkpoint_id requires message_id"));
        }
        return match params.direction.as_str() {
            "redo" => managed
                .journal
                .prepare_travel(Travel::Redo)
                .await
                .map(|recover| (recover, None))
                .map_err(safe),
            "undo" => {
                let live = managed.session.context_snapshot().map_err(safe)?;
                let (index, input) = live
                    .items
                    .iter()
                    .enumerate()
                    .rev()
                    .find(|(index, item)| {
                        matches!(item, ContentItem::User { source: None, .. })
                            && !(live.compaction.is_some()
                                && !live.items[..*index]
                                    .iter()
                                    .any(|item| !matches!(item, ContentItem::State(_))))
                    })
                    .ok_or_else(|| {
                        WireError::new(
                            "invalid_history_target",
                            "No submitted user input is available to undo",
                        )
                    })?;
                let expected = managed.journal.cursor().await.ok_or_else(unavailable)?;
                managed
                    .journal
                    .prepare_undo_input_from_context(expected, &live, index, input)
                    .await
                    .map(|recover| (recover, None))
                    .map_err(safe)
            }
            _ => Err(invalid("History direction must be undo or redo")),
        };
    };
    if params.direction != "undo" {
        return Err(invalid("message_id is supported only for undo"));
    }
    let expected_id = params
        .expected_checkpoint_id
        .ok_or_else(|| invalid("expected_checkpoint_id is required when undoing a message"))?;
    microcrab_storage::history::valid_hash(&expected_id)
        .map_err(|_| invalid("expected_checkpoint_id must be a lowercase SHA-256 digest"))?;
    let expected = managed.journal.cursor().await.ok_or_else(unavailable)?;
    if expected.checkpoint_id != expected_id {
        return Err(WireError::new(
            "history_conflict",
            "History changed; refresh before undoing this input",
        ));
    }
    let session_id = expected.session_id.clone();
    let epoch = managed.session.state().epoch_id;
    let live = managed.session.context_snapshot().map_err(safe)?;
    let (index, input) = live
        .items
        .iter()
        .cloned()
        .enumerate()
        .find(|(index, item)| {
            matches!(item, ContentItem::User { source: None, .. })
                && !(live.compaction.is_some()
                    && !live.items[..*index]
                        .iter()
                        .any(|item| !matches!(item, ContentItem::State(_))))
                && super::super::images::message_id(&session_id, epoch, *index, item) == message_id
        })
        .ok_or_else(|| {
            WireError::new(
                "invalid_history_target",
                "User input is not in selected history",
            )
        })?;
    let ContentItem::User { parts, .. } = &input else {
        unreachable!("matched canonical user input");
    };
    // Canonical Context can retain remote URLs from embedding clients. The browser
    // cannot resubmit those safely, so reject before rollback rather than omit parts.
    for part in parts {
        if let InputPart::Image { source } = part
            && !super::super::images::available(source)
        {
            return Err(WireError::new(
                "unsupported_history_input",
                "This input contains an image that cannot be restored to the browser composer",
            ));
        }
    }
    let restored = json!({"message_id":message_id,"parts":parts});
    if serde_json::to_vec(&restored)
        .map_err(|_| serialization())?
        .len()
        > 255 * 1024
    {
        return Err(WireError::new(
            "unsupported_history_input",
            "This input exceeds the browser composer restore budget",
        ));
    }
    let recover = managed
        .journal
        .prepare_undo_input_from_context(expected, &live, index, &input)
        .await
        .map_err(safe)?;
    Ok((recover, Some(restored)))
}
