//! Advisory browser presentation; never part of Context or admission.
use microcrab_auditor::{AuditBody, HookPayload, ToolPresentation};
use microcrab_session::runtime::execution::{ExecutionId, ExecutionSnapshot};
use serde::Serialize;
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Presentation {
    CodingFile,
    MessageReceipt,
    ViewImage,
}

impl Presentation {
    pub(super) fn from_audit(body: &AuditBody) -> Option<Self> {
        if !matches!(
            body.payload,
            HookPayload::ToolCall { .. } | HookPayload::ToolResult { .. }
        ) {
            return None;
        }
        match body.tool_presentation {
            Some(ToolPresentation::CodingFile) => Some(Self::CodingFile),
            Some(ToolPresentation::MessageReceipt) => Some(Self::MessageReceipt),
            Some(ToolPresentation::ViewImage) => Some(Self::ViewImage),
            None => None,
        }
    }
}

/// A bounded observation owned by one main Session runtime. Reopening creates
/// a new observer; no identity is inferred for an old execution row.
#[derive(Default)]
pub(super) struct Presentations {
    calls: Mutex<BTreeMap<u64, ObservedCall>>,
}

struct ObservedCall {
    presentation: Presentation,
    session: String,
    call: String,
    tool: String,
    position: (u64, Option<u64>, Option<u64>),
    pending: bool,
}

const MAX_PRESENTATION_OBSERVATIONS: usize = 1024;

impl Presentations {
    pub(super) fn observe(
        &self,
        body: &AuditBody,
        row: &ExecutionSnapshot,
    ) -> Option<Presentation> {
        if body.subagent.is_some()
            || row.execution_id.0 == 0
            || body.tool_execution_id != Some(row.execution_id.0)
            || body.tool_call_id.as_deref() != Some(row.call_id.as_str())
            || !row.subject.is_tool()
        {
            return None;
        }
        let position = (body.epoch, body.turn_id, body.round);
        let typed = Presentation::from_audit(body);
        let (presentation, pending) = match &body.payload {
            HookPayload::ToolCall { name, .. } if *name == row.tool => (
                // The original Core witness is still sufficient for ViewImage.
                typed.or(body
                    .authored_image_arguments
                    .then_some(Presentation::ViewImage)),
                true,
            ),
            HookPayload::ToolResult { .. } => (typed, false),
            _ => return None,
        };
        let mut calls = self.calls.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(known) = calls.get_mut(&row.execution_id.0) {
            if known.session != row.session_id
                || known.call != row.call_id
                || known.tool != row.tool
                || known.position != position
                || presentation.is_some_and(|value| value != known.presentation)
            {
                return None;
            }
            known.pending &= pending;
            return Some(known.presentation);
        }
        let presentation = presentation?;
        if calls.len() >= MAX_PRESENTATION_OBSERVATIONS {
            calls.pop_first();
        }
        calls.insert(
            row.execution_id.0,
            ObservedCall {
                presentation,
                session: row.session_id.clone(),
                call: row.call_id.clone(),
                tool: row.tool.clone(),
                position,
                pending,
            },
        );
        Some(presentation)
    }

    pub(super) fn permission(&self, session: &str, call: &str, tool: &str) -> Option<Presentation> {
        let calls = self.calls.lock().unwrap_or_else(|error| error.into_inner());
        let mut matches = calls.values().filter(|witness| {
            witness.pending
                && witness.session == session
                && witness.call == call
                && witness.tool == tool
        });
        let presentation = matches.next()?.presentation;
        matches.next().is_none().then_some(presentation)
    }

    pub(super) fn execution(
        &self,
        id: ExecutionId,
        session: &str,
        call: &str,
    ) -> Option<Presentation> {
        let calls = self.calls.lock().unwrap_or_else(|error| error.into_inner());
        calls
            .get(&id.0)
            .filter(|witness| witness.session == session && witness.call == call)
            .map(|witness| witness.presentation)
    }

    pub(super) fn decorate_messages(&self, messages: &mut [serde_json::Value]) {
        let witnesses = self.calls.lock().unwrap_or_else(|error| error.into_inner());
        if witnesses.is_empty() {
            return;
        }
        // Resolve the certified *result*, then follow its exact canonical
        // assistant source and slot. Neither a tool name nor bare call ID binds
        // a presentation to selected history after retry/travel.
        let results: std::collections::HashMap<_, _> = messages
            .iter_mut()
            .filter_map(|message| {
                if message["role"] != "tool" {
                    return None;
                }
                let session = message["execution_session_id"].as_str()?;
                let id = message["execution_id"].as_u64()?;
                let call = message["call_id"].as_str()?;
                let witness = witnesses.get(&id)?;
                let position = &message["position"];
                if witness.session != session
                    || witness.call != call
                    || position["epoch"].as_u64() != Some(witness.position.0)
                    || position["turn"].as_u64() != witness.position.1
                    || position["round"].as_u64() != witness.position.2
                {
                    return None;
                }
                let source = message["execution_call_source_id"].as_str()?.to_owned();
                let slot = message["execution_call_index"].as_u64()?;
                let key = (source, slot, id, call.to_owned());
                message["presentation"] = serde_json::json!(witness.presentation);
                Some((key, witness.presentation))
            })
            .collect();
        for message in messages {
            let Some(source) = message["canonical_source_id"].as_str().map(str::to_owned) else {
                continue;
            };
            if let Some(calls) = message["tool_calls"].as_array_mut() {
                for (slot, call) in calls.iter_mut().enumerate() {
                    let Some(id) = call["execution_id"].as_u64() else {
                        continue;
                    };
                    let Some(call_id) = call["id"].as_str() else {
                        continue;
                    };
                    if let Some(presentation) =
                        results.get(&(source.clone(), slot as u64, id, call_id.to_owned()))
                    {
                        call["presentation"] = serde_json::json!(presentation);
                    }
                }
            }
        }
    }
}

#[derive(Serialize)]
pub(super) struct NativeExecution<'a> {
    #[serde(flatten)]
    row: &'a ExecutionSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    presentation: Option<Presentation>,
}

impl<'a> NativeExecution<'a> {
    pub(super) fn new(row: &'a ExecutionSnapshot, known: Option<&Presentations>) -> Self {
        Self {
            row,
            presentation: known
                .and_then(|known| known.execution(row.execution_id, &row.session_id, &row.call_id)),
        }
    }
}

#[cfg(test)]
mod tests;

pub(super) fn executions(
    rows: &[ExecutionSnapshot],
    known: &Presentations,
) -> Result<serde_json::Value, serde_json::Error> {
    serde_json::to_value(
        rows.iter()
            .map(|row| NativeExecution::new(row, Some(known)))
            .collect::<Vec<_>>(),
    )
}
