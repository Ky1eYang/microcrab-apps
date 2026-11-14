//! Exact acknowledged Journal checkpoints certify the current durable display head.
use super::super::{SessionRuntimeEvent, hub::Hub};
use async_trait::async_trait;
use microcrab_creator::display::DisplayRecorder;
use microcrab_storage::history::{CommitObserver, JournalCursor};
use std::sync::{Arc, Weak};

pub(in crate::manager) struct CheckpointDisplay {
    pub recorder: Arc<DisplayRecorder>,
    pub hub: Weak<Hub>,
    pub child: Option<String>,
}
#[async_trait]
impl CommitObserver for CheckpointDisplay {
    async fn committed(&self, cursor: &JournalCursor) {
        // This callback runs under Journal commit serialization. It must not
        // re-enter Journal, inspect Context, or reject an acknowledged commit.
        if let Err(cause) = self.recorder.checkpoint(cursor.checkpoint_id.clone()).await {
            if let Some(hub) = self.hub.upgrade() {
                if let Some(child) = &self.child {
                    hub.publish(SessionRuntimeEvent::ChildDisplayUnavailable(
                        serde_json::json!({
                            "child_session_id":child,"coverage":"unavailable",
                            "error":{"code":"display_checkpoint_failed","message":cause}
                        }),
                    ));
                } else {
                    hub.display_failed(cause);
                }
            }
        }
    }
}
