//! Debug-only diagnostics. These shapes are not protocol validation or routing.
use super::{Inbox, Utf8Bytes};
use serde_json::Value;
use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};
use tracing::{Level, event_enabled};

const MAX_FIFO_SAMPLE: usize = 8;
const MAX_INSPECT_BYTES: usize = 4 * 1024;
// Process-local correlation for pressure diagnostics, never socket identity.
static NEXT_SNAPSHOT_ORDINAL: AtomicU64 = AtomicU64::new(1);

pub(super) struct Snapshot {
    ordinal: u64,
    fifo: Vec<Utf8Bytes>,
    omitted_fifo: usize,
    staged: Option<Utf8Bytes>,
}

impl Snapshot {
    pub(super) fn capture(inbox: &Inbox) -> Option<Self> {
        if !event_enabled!(
            Level::DEBUG,
            message,
            event,
            snapshot_ordinal,
            sampled_fifo,
            omitted_fifo,
            fifo,
            staged
        ) {
            return None;
        }
        // Only copy bounded reference-counted handles while holding the inbox
        // mutex. Parsing and dispatch happen after unlocking; no frame is moved.
        Some(Self {
            ordinal: NEXT_SNAPSHOT_ORDINAL.fetch_add(1, Ordering::Relaxed),
            fifo: inbox.frames.iter().take(MAX_FIFO_SAMPLE).cloned().collect(),
            omitted_fifo: inbox.frames.len().saturating_sub(MAX_FIFO_SAMPLE),
            staged: inbox.staged.clone(),
        })
    }

    pub(super) fn ordinal(&self) -> u64 {
        self.ordinal
    }

    pub(super) fn emit(&self) {
        tracing::debug!(
            event = "bridge_websocket_inbound_queue_snapshot",
            snapshot_ordinal = self.ordinal,
            sampled_fifo = self.fifo.len(),
            omitted_fifo = self.omitted_fifo,
            fifo = %fmt::from_fn(|f| {
                for (index, text) in self.fifo.iter().enumerate() {
                    if index != 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{index}:{}:{}", text.len(), shape(text.as_str()))?;
                }
                Ok(())
            }),
            staged = %fmt::from_fn(|f| match &self.staged {
                Some(text) => write!(f, "{}:{}", text.len(), shape(text.as_str())),
                None => f.write_str("none"),
            }),
            "Bridge WebSocket queued text snapshot (FIFO index:bytes:shape; staged bytes:shape)"
        );
    }
}

// Return only fixed labels. No identifiers (including numeric IDs), keys,
// values, parser errors or excerpts from the peer can enter the log.
fn shape(text: &str) -> &'static str {
    if text.len() > MAX_INSPECT_BYTES {
        return "unclassified";
    }
    let fields = match serde_json::from_str::<Value>(text) {
        Ok(Value::Object(fields)) => fields,
        Ok(_) => return "unclassified",
        Err(_) => return "invalid_json",
    };
    match (
        fields.get("id"),
        fields.contains_key("result"),
        fields.contains_key("error"),
        fields.get("method"),
    ) {
        (Some(Value::Number(_)), true, false, None)
        | (Some(Value::Number(_)), false, true, None) => "reply_like",
        (None, false, false, Some(Value::String(_))) => "event_like",
        _ => "unclassified",
    }
}
