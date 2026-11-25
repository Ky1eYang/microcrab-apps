//! Owner-scoped reads of images already admitted to Context and durable Journal.
//! Binary delivery never resolves historical paths/URLs or activates a Runtime.
use super::{Manager, now, safe, unavailable};
use crate::auth::Principal;
use microcrab_protocol::{ImageSource, InputPart, application::WireError};
use microcrab_session::{Journal, context::ContentItem};
use serde_json::{Value, json};

pub(crate) struct ImageContent {
    pub(crate) media_type: &'static str,
    pub(crate) data: Vec<u8>,
}

/// Keep browser IDs and binary lookup on the exact same historical identity.
pub(super) fn message_id(session_id: &str, epoch: u64, index: usize, item: &ContentItem) -> String {
    let digest =
        microcrab_storage::history::content_hash_value(json!([session_id, epoch, index, item]))
            .expect("ContentItem is JSON-compatible");
    format!("message-{digest}")
}

fn media_type(value: &str) -> Option<&'static str> {
    microcrab_session::admission::image::image_type(value)
}

pub(super) fn available(source: &ImageSource) -> bool {
    matches!(source, ImageSource::Data { media_type: mime, data } if media_type(mime).is_some() && !data.is_empty())
}

pub(super) fn reference(session_id: &str, id: &str, index: usize, source: &ImageSource) -> Value {
    if available(source) {
        json!({"type":"image","source":{"type":"url","url":format!("/api/v1/sessions/{session_id}/images/{id}/{index}")}})
    } else {
        // Never expose a path, remote URL, embedded credentials or active format.
        json!({"type":"image","source_omitted":true})
    }
}

/// Large text/tool messages keep safe image references, never binary parts.
pub(super) fn references(parts: &Value) -> Value {
    json!(
        parts
            .as_array()
            .into_iter()
            .flatten()
            .filter(|part| part["type"] == "image")
            .map(|part| {
                match (
                    part["source"]["type"].as_str(),
                    part["source"]["url"].as_str(),
                ) {
                    (Some("url"), Some(url)) if url.starts_with("/api/v1/sessions/") => {
                        json!({"type":"image","source":{"type":"url","url":url}})
                    }
                    _ => json!({"type":"image","source_omitted":true}),
                }
            })
            .collect::<Vec<_>>()
    )
}

fn select(
    items: Vec<ContentItem>,
    epoch: u64,
    session_id: &str,
    id: &str,
    part_index: usize,
) -> Result<ImageContent, WireError> {
    // Presentation merges the append-only archive into its original tool row.
    // Resolve that row's exact same ID and projected part ordinal, never a URL
    // or a different call. Existing canonical image identities remain unchanged.
    if part_index == 0 {
        for (index, item) in items.iter().enumerate() {
            let ContentItem::Tool { call_id, parts, .. } = item else {
                continue;
            };
            if !parts.is_empty() || message_id(session_id, epoch, index, item) != id {
                continue;
            }
            let archive = items[index + 1..]
                .iter()
                .take_while(|item| !matches!(item, ContentItem::Assistant { .. }))
                .find_map(|item| {
                    let ContentItem::User {
                        parts,
                        envelope:
                            Some(microcrab_transport::CarrierEnvelopeVersion::RetainedToolImage {
                                call_id: related,
                            }),
                        ..
                    } = item
                    else {
                        return None;
                    };
                    (related == call_id).then(|| parts.get(1)).flatten()
                });
            if let Some(InputPart::Image {
                source:
                    ImageSource::Data {
                        media_type: mime,
                        data,
                    },
            }) = archive
                && !data.is_empty()
            {
                let media_type = media_type(mime).ok_or_else(unavailable)?;
                return Ok(ImageContent {
                    media_type,
                    data: data.clone(),
                });
            }
            break;
        }
    }
    for (index, item) in items.into_iter().enumerate() {
        if !matches!(&item, ContentItem::User { .. } | ContentItem::Tool { .. })
            || message_id(session_id, epoch, index, &item) != id
        {
            continue;
        }
        let parts = match item {
            ContentItem::User { parts, .. } | ContentItem::Tool { parts, .. } => parts,
            _ => unreachable!("image lookup accepts only User or Tool items"),
        };
        let part = parts.into_iter().nth(part_index).ok_or_else(unavailable)?;
        return match part {
            InputPart::Image {
                source:
                    ImageSource::Data {
                        media_type: mime,
                        data,
                    },
            } if !data.is_empty() => {
                let media_type = media_type(&mime).ok_or_else(unavailable)?;
                Ok(ImageContent { media_type, data })
            }
            _ => Err(unavailable()),
        };
    }
    Err(unavailable())
}

async fn select_display(
    journal: &microcrab_storage::CreatorJournal,
    items: Vec<ContentItem>,
    epoch: u64,
    marker: Option<&microcrab_session::context::CompactionMarker>,
    session_id: &str,
    id: &str,
    part_index: usize,
) -> Result<ImageContent, WireError> {
    if let Ok(image) = select(items, epoch, session_id, id, part_index) {
        return Ok(image);
    }
    for recover in journal.compacted_history(marker).await.map_err(safe)? {
        if let Ok(image) = select(
            recover.context.items,
            recover.state.epoch_id,
            session_id,
            id,
            part_index,
        ) {
            return Ok(image);
        }
    }
    Err(unavailable())
}

impl Manager {
    pub(crate) async fn image(
        &self,
        principal: &Principal,
        session_id: &str,
        message_id: &str,
        part_index: usize,
    ) -> Result<ImageContent, WireError> {
        if principal.expires_at <= now() / 1000 {
            return Err(WireError::new("unauthenticated", "Authentication expired"));
        }
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| WireError::new("capacity", "Command capacity reached"))?;
        let manager = self
            .self_ref
            .upgrade()
            .ok_or_else(|| WireError::new("closed", "Manager unavailable"))?;
        let owner = principal.owner.clone();
        let session_id = session_id.to_owned();
        let message_id = message_id.to_owned();
        // Normal admission/shutdown tracking owns even disconnected read waiters.
        self.track(async move {
            let _permit = permit;
            manager
                .image_owned(&owner, &session_id, &message_id, part_index)
                .await
        })?
        .await
        .map_err(|_| WireError::new("operation_failed", "Image reader failed"))?
    }

    async fn image_owned(
        &self,
        owner: &str,
        session_id: &str,
        id: &str,
        part_index: usize,
    ) -> Result<ImageContent, WireError> {
        let record = self.record(owner, session_id).await?;
        let _journal_open = self.journal_open_gate(&record.id).lock_owned().await;
        let slot = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&record.id)
            .cloned();
        if let Some(slot) = slot
            && let Some(managed) = slot.active.lock().await.as_ref()
        {
            return select_display(
                &managed.journal,
                managed.session.messages(),
                managed.session.state().epoch_id,
                managed.session.compaction().as_ref(),
                &record.id,
                id,
                part_index,
            )
            .await;
        }
        let (journal, recover) = self.inspect_journal(&record).await.map_err(safe)?;
        let result = match recover {
            Some(recover) => {
                select_display(
                    &journal,
                    recover.context.items,
                    recover.state.epoch_id,
                    recover.context.compaction.as_ref(),
                    &record.id,
                    id,
                    part_index,
                )
                .await
            }
            None => Err(unavailable()),
        };
        // Release the read-only Journal before returning either bytes or errors.
        journal.close().await.map_err(safe)?;
        result
    }
}

#[cfg(test)]
mod tool_image_tests {
    use super::*;
    #[test]
    fn https_archive_is_addressed_using_the_linked_tool_row_and_not_the_archive_row() {
        let result = ContentItem::Tool {
            call_id: "image".into(),
            content: "pending".into(),
            parts: vec![],
        };
        let archive = ContentItem::User {
            parts: vec![
                InputPart::Text {
                    text: "retained".into(),
                },
                InputPart::Image {
                    source: ImageSource::Data {
                        media_type: "image/png".into(),
                        data: vec![1, 2, 3],
                    },
                },
            ],
            source: Some(microcrab_transport::UserSource::Framework),
            envelope: Some(
                microcrab_transport::CarrierEnvelopeVersion::RetainedToolImage {
                    call_id: "image".into(),
                },
            ),
        };
        let assistant = ContentItem::Assistant {
            content: String::new(),
            tool_calls: vec![microcrab_protocol::ToolCall {
                id: "image".into(),
                name: "view_image".into(),
                arguments: serde_json::json!({"image_url":"https:legacy-private-reference"}),
                provider_metadata: None,
            }],
            provider_metadata: None,
        };
        let before =
            super::super::view::messages(vec![assistant.clone(), result.clone()], 2, "owned");
        assert!(before[0]["tool_calls"][0].get("presentation").is_none());
        assert!(before[1].get("presentation").is_none());
        let items = vec![assistant, result, archive];
        let id = message_id("owned", 2, 1, &items[1]);
        let messages = super::super::view::messages(items.clone(), 2, "owned");
        let archived = super::super::view::canonical_display_messages(items.clone(), 2, "owned");
        assert!(archived[0]["tool_calls"][0].get("presentation").is_none());
        assert!(archived[1].get("presentation").is_none());
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["tool_calls"][0]["presentation"], "view-image");
        assert_eq!(
            messages[0]["tool_calls"][0]["arguments"],
            before[0]["tool_calls"][0]["arguments"]
        );
        assert_eq!(messages[1]["presentation"], "view-image");
        assert_eq!(messages[1]["id"], before[1]["id"]);
        assert_eq!(messages[1]["text"], before[1]["text"]);
        assert_eq!(
            messages[1]["parts"][0]["source"]["url"],
            format!("/api/v1/sessions/owned/images/{id}/0")
        );
        assert_eq!(
            select(items.clone(), 2, "owned", &id, 0).unwrap().data,
            vec![1, 2, 3]
        );
        assert!(select(items.clone(), 2, "other", &id, 0).is_err());
        assert!(select(items, 2, "owned", &id, 1).is_err());
    }

    #[test]
    fn exact_tool_image_resource_uses_canonical_identity_and_original_part_index() {
        let item = ContentItem::Tool {
            call_id: "image".into(),
            content: "done".into(),
            parts: vec![
                InputPart::Text {
                    text: "annotation".into(),
                },
                InputPart::Image {
                    source: ImageSource::Data {
                        media_type: "image/png".into(),
                        data: vec![1, 2, 3],
                    },
                },
            ],
        };
        let id = message_id("owned", 2, 0, &item);
        let image = select(vec![item.clone()], 2, "owned", &id, 1).unwrap();
        assert_eq!(image.data, vec![1, 2, 3]);
        assert_eq!(image.media_type, "image/png");
        assert!(select(vec![item.clone()], 2, "other", &id, 1).is_err());
        assert!(select(vec![item.clone()], 2, "owned", &id, 0).is_err());
        assert!(select(vec![item], 3, "owned", &id, 1).is_err());
    }
}
