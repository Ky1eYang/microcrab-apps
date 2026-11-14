//! Native rendezvous authenticates an expiring attempt capability, not a cookie.
use crate::Application;
use axum::{
    extract::{State, WebSocketUpgrade},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use std::sync::Arc;
pub(super) async fn outbound(
    State(state): State<Arc<Application>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let attempt = match state.manager.claim_outbound_host(&headers) {
        Ok(attempt) => attempt,
        Err(_) => return StatusCode::FORBIDDEN.into_response(),
    };
    ws.max_message_size(microcrab_host_protocol::MAX_FRAME_BYTES)
        .max_frame_size(microcrab_host_protocol::MAX_FRAME_BYTES)
        .on_upgrade(move |socket| async move {
            state.manager.accept_outbound_host(attempt, socket).await;
        })
}
