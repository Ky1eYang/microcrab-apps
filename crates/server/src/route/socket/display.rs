//! Session subscription forwarder. One task owns wire order for live Hub events
//! and server-pushed archive pages. Archive producers await a two-page queue and
//! never hold a Hub lock while delivering.
use super::{
    ServerFrame,
    writer::{Delivery, send},
};
use crate::{
    Application,
    auth::Principal,
    manager::{ResyncReason, SessionRuntimeEvent, Subscription},
};
use microcrab_protocol::application::{MAX_FRAME_BYTES, VERSION};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

/// `display.subscribe` params, and the session socket's initial catch-up.
#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DisplayRequest {
    pub(super) child_session_id: Option<String>,
    pub(super) history_lineage: Option<String>,
    pub(super) high_water: Option<u64>,
    #[serde(default)]
    pub(super) display_watermark: u64,
    #[serde(skip)]
    pub(super) coverage: Option<String>,
}

/// Which page event a (possibly fragmented) archive page commits as.
#[derive(Clone, Copy)]
enum PageKind {
    Batch,
    Settled,
}
impl PageKind {
    fn event(self, page: Value) -> SessionRuntimeEvent {
        match self {
            Self::Batch => SessionRuntimeEvent::DisplayBatch(page),
            Self::Settled => SessionRuntimeEvent::DisplaySettled(page),
        }
    }
    /// The fragment's `event` field names the page it reassembles into.
    fn wire(self) -> &'static str {
        match self {
            Self::Batch => "display.batch",
            Self::Settled => "display.settled",
        }
    }
}

pub(super) struct Forward {
    pub(super) subscription: Subscription,
    pub(super) wire_sequence: u64,
    pub(super) session_id: String,
    pub(super) sender: mpsc::Sender<Delivery>,
    pub(super) cancellation: CancellationToken,
    pub(super) state: Arc<Application>,
    pub(super) principal: Principal,
    pub(super) initial: DisplayRequest,
}

pub(super) async fn forward(context: Forward, mut requests: mpsc::Receiver<DisplayRequest>) {
    let Forward {
        mut subscription,
        mut wire_sequence,
        session_id,
        sender,
        cancellation,
        state,
        principal,
        initial,
    } = context;
    let producer_cancel = cancellation.child_token();
    let _producer_guard = producer_cancel.clone().drop_guard();
    let (pages, mut incoming) = mpsc::channel::<SessionRuntimeEvent>(2);
    let mut jobs = HashMap::<String, JoinHandle<()>>::new();
    let mut lineage = lineage_of(&subscription.snapshot);
    let mut initial = Some(initial);
    let job = |jobs: &mut HashMap<String, JoinHandle<()>>, request| {
        start_display(
            jobs,
            request,
            &state,
            &principal,
            &session_id,
            &pages,
            &producer_cancel,
        )
    };
    loop {
        if let Some(mut request) = initial.take() {
            request.child_session_id = None;
            if request.history_lineage != lineage {
                request.display_watermark = 0;
            }
            request.history_lineage = lineage.clone();
            bound(&mut request, &subscription.snapshot);
            if request.high_water.is_some() {
                job(&mut jobs, request);
            }
        }
        let (event, resync) = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            request = requests.recv() => {
                let Some(mut request) = request else { break };
                if request.child_session_id.is_none() {
                    // A main catch-up can only use this subscription's snapshot.
                    if request.history_lineage != lineage { continue; }
                    bound(&mut request, &subscription.snapshot);
                }
                jobs.retain(|_, job| !job.is_finished());
                if !jobs.contains_key(request.child_session_id.as_deref().unwrap_or("")) && jobs.len() >= 8 { continue; }
                job(&mut jobs, request);
                continue;
            },
            notification = subscription.receiver.recv() => match notification {
                Ok(notification) if notification.sequence > subscription.sequence => {
                    subscription.sequence = notification.sequence;
                    // Keep the private display barrier current using the same typed
                    // increments sent to the browser; whole snapshots never cross WS.
                    crate::manager::publication::apply(&mut subscription.snapshot, notification.event.name(), notification.event.payload());
                    let next = lineage_of(&subscription.snapshot);
                    if next != lineage {
                        for (_, job) in jobs.drain() { job.abort(); }
                        lineage = next;
                        initial = Some(DisplayRequest::default());
                    }
                    (notification.event, false)
                },
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) =>
                    (SessionRuntimeEvent::ResyncRequired(ResyncReason::SlowConsumer), true),
                Err(_) => break,
            },
            page = incoming.recv() => {
                let Some(page) = page else { break };
                let data = page.payload();
                if data["child_session_id"].is_null() && data["history_lineage"].as_str() != lineage.as_deref() { continue; }
                (page, false)
            },
        };
        let Some(sequence) = wire_sequence.checked_add(1) else {
            break;
        };
        wire_sequence = sequence;
        let frame = ServerFrame::Event {
            v: VERSION,
            subscription_id: subscription.id.clone(),
            generation: subscription.generation.clone(),
            sequence,
            session_id: session_id.clone(),
            event,
        };
        let delivered = if resync {
            sender.try_send(Delivery::Last(frame)).is_ok()
        } else {
            send(&sender, frame, &cancellation).await.is_ok()
        };
        if !delivered {
            cancellation.cancel();
            break;
        }
        if resync {
            break;
        }
    }
    for (_, job) in jobs {
        job.abort();
    }
}

fn lineage_of(snapshot: &Value) -> Option<String> {
    snapshot["history_view"]["lineage"]
        .as_str()
        .map(str::to_owned)
}
fn bound(request: &mut DisplayRequest, snapshot: &Value) {
    request.high_water = snapshot["display"]["high_water"].as_u64();
    request.coverage = snapshot["display"]["coverage"].as_str().map(str::to_owned);
}

fn start_display(
    jobs: &mut HashMap<String, JoinHandle<()>>,
    request: DisplayRequest,
    state: &Arc<Application>,
    principal: &Principal,
    session: &str,
    pages: &mpsc::Sender<SessionRuntimeEvent>,
    cancellation: &CancellationToken,
) {
    let key = request.child_session_id.clone().unwrap_or_default();
    if let Some(old) = jobs.remove(&key) {
        old.abort();
    }
    let state = state.clone();
    let principal = principal.clone();
    let session = session.to_owned();
    let pages = pages.clone();
    let cancellation = cancellation.clone();
    jobs.insert(key, tokio::spawn(async move {
        let mut after = request.display_watermark;
        let Some(high) = request.high_water else { return };
        if after > high { after = 0; }
        // Even an empty suffix must read the durable selected graph: a watermark
        // certifies received archive bytes, not which immutable branch is selected.
        let mut selection = None;
        loop {
            let query = state.manager.display_page(&principal, &session, json!({
                "after":after,"high_water":high,"limit":128,
                "history_lineage":request.history_lineage,"child_session_id":request.child_session_id,
            }), &mut selection);
            let result = tokio::select! { biased; _ = cancellation.cancelled() => return, result = query => result };
            let (kind, mut page, next) = match result {
                Ok(page) => {
                    let next = page["next_cursor"].as_u64();
                    let scanned = next.unwrap_or_else(|| page["high_water"].as_u64().unwrap_or(after));
                    if scanned < after || scanned > high || next.is_some_and(|cursor| cursor <= after) { return; }
                    (if next.is_none() { PageKind::Settled } else { PageKind::Batch }, page, next)
                },
                Err(error) => (PageKind::Settled, json!({
                    "records":[],"history_lineage":request.history_lineage,"high_water":high,
                    "next_cursor":null,"coverage":"unavailable","error":{"code":error.code,"message":error.message},
                }), None),
            };
            page["after"] = json!(after);
            page["scanned_through"] = json!(if page.get("error").is_some() { after } else { next.unwrap_or_else(|| page["high_water"].as_u64().unwrap_or(after)) });
            page["child_session_id"] = json!(request.child_session_id);
            if !deliver_display(&pages, kind, page, &cancellation).await { return; }
            let Some(next) = next else { return };
            after = next;
        }
    }));
}

// Archive bodies are complete and may exceed the socket frame limit. Fragment
// the serialized page only at UTF-8 boundaries; the client commits it atomically.
async fn deliver_display(
    pages: &mpsc::Sender<SessionRuntimeEvent>,
    kind: PageKind,
    page: Value,
    cancellation: &CancellationToken,
) -> bool {
    let Ok(body) = serde_json::to_string(&page) else {
        return false;
    };
    if body.len() < MAX_FRAME_BYTES / 2 {
        return tokio::select! { biased; _ = cancellation.cancelled() => false,
        result = pages.send(kind.event(page)) => result.is_ok() };
    }
    let id = uuid::Uuid::new_v4().to_string();
    let mut offset = 0;
    let mut index = 0;
    while offset < body.len() {
        let mut end = (offset + MAX_FRAME_BYTES / 8).min(body.len());
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        let fragment = json!({"id":id,"index":index,"last":end == body.len(),
            "text":&body[offset..end],"event":kind.wire(),
            "history_lineage":page["history_lineage"],"child_session_id":page["child_session_id"]});
        let delivered = tokio::select! { biased; _ = cancellation.cancelled() => false,
        result = pages.send(SessionRuntimeEvent::DisplayFragment(fragment)) => result.is_ok() };
        if !delivered {
            return false;
        }
        offset = end;
        index += 1;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn large_display_page_obeys_backpressure_and_utf8_frame_bounds() {
        let cancellation = CancellationToken::new();
        let (sender, mut receiver) = mpsc::channel(2);
        let body = "界🦀\n".repeat(1_200_000);
        let page = json!({"records":[{"sequence":1,"event":{"text":body}}],
            "history_lineage":"selected","child_session_id":null,"high_water":1});
        let original = page.clone();
        let producer_cancel = cancellation.clone();
        let producer = tokio::spawn(async move {
            deliver_display(&sender, PageKind::Settled, page, &producer_cancel).await
        });
        let mut parts = String::new();
        let mut index = 0;
        while let Some(event) = receiver.recv().await {
            let SessionRuntimeEvent::DisplayFragment(fragment) = event else {
                panic!("large pages must be fragmented");
            };
            assert_eq!(fragment["index"], index);
            assert_eq!(fragment["event"], "display.settled");
            assert!(serde_json::to_vec(&fragment).unwrap().len() < MAX_FRAME_BYTES / 2);
            parts.push_str(fragment["text"].as_str().unwrap());
            index += 1;
        }
        assert!(producer.await.unwrap());
        assert!(index > 64);
        assert_eq!(serde_json::from_str::<Value>(&parts).unwrap(), original);
        assert!(!cancellation.is_cancelled());
    }

    #[tokio::test]
    async fn blocked_display_delivery_cancels_with_its_subscription() {
        let cancellation = CancellationToken::new();
        let (sender, _receiver) = mpsc::channel(1);
        sender
            .send(SessionRuntimeEvent::Session(Value::Null))
            .await
            .unwrap();
        cancellation.cancel();
        assert!(
            !deliver_display(
                &sender,
                PageKind::Settled,
                json!({"records":[]}),
                &cancellation
            )
            .await
        );
    }
}
