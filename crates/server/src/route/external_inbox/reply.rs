//! Project only authenticated, committed dialogue data. Never use owner streams.
use super::{Application, Error, bearer, single};
use crate::manager::external_inbox::reply::{ReplyReader, ReplySnapshot};
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, header},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
};
use microcrab_session::state::communication::reply::DialogueTerminalReason;
use serde_json::{Value, json};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::time::Instant;

const DIALOGUE_HEADER: &str = "x-microcrab-dialogue-id";
const LAST_EVENT_ID: &str = "last-event-id";
const POLL: Duration = Duration::from_millis(500);
const MAX_WAIT: Duration = Duration::from_secs(300);

pub(crate) async fn by_header(
    State(state): State<Arc<Application>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, Error> {
    let dialogue_id = single(&headers, DIALOGUE_HEADER)
        .map_err(|_| Error::invalid("Exactly one X-MicroCrab-Dialogue-Id is required"))?;
    read(&state, &headers, id, dialogue_id).await
}

pub(crate) async fn by_path(
    State(state): State<Arc<Application>>,
    Path((id, dialogue_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, Error> {
    if headers.contains_key(DIALOGUE_HEADER)
        && single(&headers, DIALOGUE_HEADER).ok() != Some(dialogue_id.as_str())
    {
        return Err(Error::invalid(
            "Dialogue path and correlation header must agree",
        ));
    }
    read(&state, &headers, id, &dialogue_id).await
}

async fn read(
    state: &Application,
    headers: &HeaderMap,
    id: String,
    alias: &str,
) -> Result<Response, Error> {
    let key = bearer(headers)?;
    if alias.len() != 8
        || !alias
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::invalid(
            "Dialogue ID must be eight lowercase hexadecimal characters",
        ));
    }
    let reader = state
        .manager
        .external_reply_reader(id, key, alias.into())
        .map_err(Error::from)?;
    if !wants_sse(headers) {
        if headers.contains_key(LAST_EVENT_ID) {
            return Err(Error::invalid(
                "Last-Event-ID requires Accept: text/event-stream",
            ));
        }
        let value = reader
            .observe(|snapshot| json_snapshot(alias, snapshot))
            .await
            .map_err(Error::from)?;
        return Ok(Json(value).into_response());
    }
    let after = if headers.contains_key(LAST_EVENT_ID) {
        let input =
            single(headers, LAST_EVENT_ID).map_err(|_| Error::invalid("Invalid Last-Event-ID"))?;
        Some(
            input
                .parse::<usize>()
                .ok()
                .filter(|index| index.to_string() == input)
                .ok_or_else(|| {
                    Error::invalid("Last-Event-ID must be a canonical decimal replay index")
                })?,
        )
    } else {
        None
    };
    let last = reader
        .observe(|snapshot| last_event_index(&snapshot))
        .await
        .map_err(Error::from)?;
    if after.is_some_and(|after| after > last) {
        return Err(Error::invalid(
            "Last-Event-ID exceeds the committed dialogue events",
        ));
    }
    let stream = futures_util::stream::unfold(
        (
            reader,
            after.map_or(0, |index| index + 1),
            Instant::now() + MAX_WAIT,
            false,
        ),
        |(reader, index, deadline, done)| async move {
            if done {
                return None;
            }
            let (event, done) = tokio::time::timeout_at(deadline, next_event(&reader, index))
                .await
                .ok()??;
            Some((
                Ok::<Event, Infallible>(event),
                (reader, index + 1, deadline, done),
            ))
        },
    );
    Ok(Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response())
}

fn wants_sse(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::ACCEPT)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .any(|value| {
            value.split(',').any(|range| {
                let mut parts = range.split(';');
                parts
                    .next()
                    .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"))
                    && parts.all(|parameter| {
                        !parameter
                            .trim()
                            .split_once('=')
                            .is_some_and(|(key, value)| {
                                key.trim().eq_ignore_ascii_case("q")
                                    && !value
                                        .trim()
                                        .parse::<f32>()
                                        .is_ok_and(|q| 0.0 < q && q <= 1.0)
                            })
                    })
            })
        })
}

async fn next_event(reader: &ReplyReader, index: usize) -> Option<(Event, bool)> {
    let stop = reader.stop();
    loop {
        // Recheck each event, including historical replay and terminal output.
        // Never buffer private chunks between observations or after backpressure.
        let next = reader
            .observe(|snapshot| {
                let terminal = snapshot
                    .reply
                    .as_ref()
                    .is_some_and(|record| record.terminal().is_some());
                match indexed_event(&reader.dialogue_id, &snapshot, index) {
                    Some((name, data)) => Some(Some((
                        Event::default()
                            .id(index.to_string())
                            .event(name)
                            .data(data.to_string()),
                        name == "done",
                    ))),
                    None if terminal => Some(None), // reconnect after already-observed done
                    None => None,
                }
            })
            .await
            .ok()?;
        if let Some(result) = next {
            return result;
        }
        tokio::select! {
            biased;
            _ = stop.cancelled() => return None,
            _ = tokio::time::sleep(POLL) => (),
        }
    }
}

fn last_event_index(snapshot: &ReplySnapshot) -> usize {
    usize::from(snapshot.consumed)
        + snapshot.reply.as_ref().map_or(0, |reply| {
            reply.chunks().len() + usize::from(reply.terminal().is_some())
        })
}

fn indexed_event(
    alias: &str,
    snapshot: &ReplySnapshot,
    index: usize,
) -> Option<(&'static str, Value)> {
    if index == 0 {
        return Some(("queued", json!({"dialogue_id": alias, "status":"queued"})));
    }
    if index == 1 && snapshot.consumed {
        return Some((
            "consumed",
            json!({"dialogue_id":alias, "status":"consumed"}),
        ));
    }
    let reply = snapshot.reply.as_ref()?;
    let chunk_index = index.checked_sub(2)?;
    if let Some(chunk) = reply.chunks().get(chunk_index) {
        let mut value = chunk_json(chunk);
        value["dialogue_id"] = json!(alias);
        return Some(("chunk", value));
    }
    if chunk_index != reply.chunks().len() {
        return None;
    }
    let (status, reason) = status_reason(snapshot);
    reply.terminal().map(|_| ("done", json!({
        "dialogue_id": alias, "status": status, "reason": reason,
        "total_chunks": reply.chunks().len(), "settled_turn": reply.binding().turn().number(),
    })))
}

fn status_reason(snapshot: &ReplySnapshot) -> (&'static str, Option<&'static str>) {
    let terminal = snapshot
        .reply
        .as_ref()
        .and_then(|record| record.terminal().map(|terminal| (record, terminal)));
    match terminal.map(|(record, terminal)| (record.chunks().is_empty(), terminal.reason())) {
        Some((true, DialogueTerminalReason::Completed)) => ("completed", Some("no_reply_invoked")),
        Some((false, DialogueTerminalReason::Completed)) => ("completed", Some("turn_settled")),
        Some((_, DialogueTerminalReason::Cancelled)) => ("cancelled", Some("cancelled")),
        Some((_, DialogueTerminalReason::Interrupted)) => ("interrupted", Some("interrupted")),
        None if snapshot.consumed => ("consumed", None),
        None => ("queued", None),
    }
}

fn chunk_json(chunk: &microcrab_session::state::communication::reply::DialogueReplyChunk) -> Value {
    json!({"index":chunk.index(), "message":chunk.message(), "is_final":chunk.is_final(), "at_ms":chunk.at_ms()})
}

fn json_snapshot(alias: &str, snapshot: ReplySnapshot) -> Value {
    let (status, reason) = status_reason(&snapshot);
    let reply = snapshot.reply.as_ref();
    let chunks = reply.map_or(&[][..], |record| record.chunks());
    json!({
        "dialogue_id":alias, "speaker_identity":snapshot.speaker_identity, "status":status,
        "settled_turn":reply.filter(|record| record.terminal().is_some()).map(|record| record.binding().turn().number()),
        "chunks":chunks.iter().map(chunk_json).collect::<Vec<_>>(),
        "aggregated_reply":(!chunks.is_empty()).then(|| chunks.iter().map(|chunk| chunk.message()).collect::<Vec<_>>().join("\n\n")),
        "reason":reason,
    })
}
