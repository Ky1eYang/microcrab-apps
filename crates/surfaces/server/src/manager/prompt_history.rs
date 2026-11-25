//! Session-owned frozen prompt evidence. Live facts never fill missing frozen evidence.
//! A live selection with an unapplied external messaging decision is pending;
//! detached historical evidence is not treated as a usable catalog.
use super::{Managed, Manager, detached::detached_lineage, now, safe, unavailable};
use crate::auth::Principal;
use microcrab_protocol::{ToolDefinition, application::WireError};
use microcrab_session::{Error, context::ContextSnapshot};
use serde::{Deserialize, Serialize, ser::SerializeSeq};
use std::io::{self, Write};

#[cfg(test)]
mod tests;

const MAX_LINEAGE_BYTES: usize = 1024;
const MAX_RESPONSE_BYTES: usize = 256 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Selection {
    history_lineage: String,
    epoch: u64,
}

#[derive(Serialize)]
struct Projection<'a> {
    session_id: &'a str,
    history_lineage: &'a str,
    epoch: u64,
    #[serde(flatten)]
    data: Evidence<'a>,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Evidence<'a> {
    Available {
        system: &'a str,
        tools: DeclaredTools<'a>,
    },
    Unavailable {
        reason: Unavailable,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Unavailable {
    MissingEvidence,
    ResponseLimit,
}

struct DeclaredTools<'a>(&'a [ToolDefinition]);
impl Serialize for DeclaredTools<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // A definition is not evidence of origin, permission, or executability.
        #[derive(Serialize)]
        struct DeclaredTool<'a> {
            name: &'a str,
            description: &'a str,
            parameters: &'a serde_json::Value,
        }
        let mut tools = serializer.serialize_seq(Some(self.0.len()))?;
        for tool in self.0 {
            tools.serialize_element(&DeclaredTool {
                name: &tool.name,
                description: &tool.description,
                parameters: &tool.parameters,
            })?;
        }
        tools.end()
    }
}

#[derive(Default)]
struct BoundedJson(Vec<u8>);
impl Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_RESPONSE_BYTES - self.0.len() {
            return Err(io::Error::other("frozen prompt response limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn render(
    id: &str,
    selected: &Selection,
    context: Option<&ContextSnapshot>,
) -> Result<Vec<u8>, WireError> {
    let mut projection = Projection {
        session_id: id,
        history_lineage: &selected.history_lineage,
        epoch: selected.epoch,
        // A pending checkpoint may hold candidate definitions for the *next*
        // epoch. Neither those nor its historical catalog alone prove the pair.
        data: match context
            .filter(|context| context.epoch == selected.epoch && !context.pending_epoch)
        {
            Some(context) => Evidence::Available {
                system: &context.system,
                tools: DeclaredTools(&context.tools),
            },
            None => Evidence::Unavailable {
                reason: Unavailable::MissingEvidence,
            },
        },
    };
    let mut buffer = BoundedJson::default();
    match serde_json::to_writer(&mut buffer, &projection) {
        Ok(()) => Ok(buffer.0),
        Err(error) if error.is_io() => {
            projection.data = Evidence::Unavailable {
                reason: Unavailable::ResponseLimit,
            };
            serde_json::to_vec(&projection).map_err(|_| encode_failed())
        }
        Err(_) => Err(encode_failed()),
    }
}

fn encode_failed() -> WireError {
    WireError::new("internal_error", "Could not encode frozen prompt evidence")
}

fn stale() -> WireError {
    WireError::new(
        "stale_history",
        "The selected Session history or epoch changed",
    )
}

fn read_failed() -> WireError {
    WireError::new(
        "storage_unavailable",
        "Could not verify selected frozen prompt evidence",
    )
}

async fn live_checkpoint(
    managed: &Managed,
    id: &str,
    selected: &Selection,
) -> Result<Option<ContextSnapshot>, WireError> {
    for _ in 0..3 {
        let before = managed.journal.display_witness().await;
        // State is used only to check the page's selection identity. Prompt
        // text, tool definitions and their epoch provenance come from Recover.
        if before.lineage != selected.history_lineage
            || managed.session.state().epoch_id != selected.epoch
        {
            return Err(stale());
        }
        if before
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.session_id != id)
        {
            return Err(read_failed());
        }
        let result = if before
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.epoch == selected.epoch)
        {
            managed.journal.head_recover().await
        } else {
            // The live epoch can advance before its first durable checkpoint.
            Ok(None)
        };
        if managed.journal.display_witness().await != before {
            continue;
        }
        if managed.session.state().epoch_id != selected.epoch {
            return Err(stale());
        }
        return match result.map_err(|_| read_failed())? {
            Some(recover) if recover.state.epoch_id != selected.epoch => Err(read_failed()),
            recover => Ok(recover.map(|recover| recover.context)),
        };
    }
    Err(safe(Error::Busy))
}

impl Manager {
    pub(crate) async fn frozen_prompt(
        &self,
        principal: &Principal,
        id: String,
        selected: Selection,
    ) -> Result<Vec<u8>, WireError> {
        if principal.expires_at <= now() / 1000 {
            return Err(WireError::new("unauthenticated", "Authentication expired"));
        }
        if !crate::config::valid_id(&id) {
            return Err(unavailable());
        }
        if selected.history_lineage.is_empty() || selected.history_lineage.len() > MAX_LINEAGE_BYTES
        {
            return Err(WireError::new(
                "invalid_request",
                "Invalid selected history identity",
            ));
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
        self.track(async move {
            let _permit = permit;
            manager.frozen_prompt_owned(&owner, &id, &selected).await
        })?
        .await
        .map_err(|_| read_failed())?
    }

    async fn frozen_prompt_owned(
        &self,
        owner: &str,
        id: &str,
        selected: &Selection,
    ) -> Result<Vec<u8>, WireError> {
        let _gate = self.journal_open_gate(id).lock_owned().await;
        // Unlike Manager::record, this lookup never migrates legacy routing or
        // writes defaults. The lifecycle fence excludes activation and deletion.
        let record = self
            .directory
            .get(owner, id)
            .await
            .map_err(safe)?
            .ok_or_else(unavailable)?;
        if !self.project_visible(&record).await {
            return Err(unavailable());
        }
        let slot = self
            .registry
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .get(id)
            .cloned();
        let managed = match slot {
            Some(slot) => slot.active.lock().await.clone(),
            None => None,
        };
        let context = if let Some(managed) = managed {
            // History travel is a Manager command. Do not observe the interval
            // between selection publication and installation in the live Session.
            let _command = managed.commands.lock().await;
            let _speaker = self.speaker_gate(owner).lock_owned().await;
            let enabled = match record.project_id.as_deref() {
                Some(project) => self
                    .project_external_messaging_enabled(owner, project)
                    .await
                    .map_err(|_| read_failed())?,
                None => false,
            };
            let context = live_checkpoint(&managed, id, selected).await?;
            if context.as_ref().is_some_and(|context| {
                !super::external_speakers::admission::current_definition_matches(
                    &context.tools,
                    enabled,
                )
            }) {
                return render(id, selected, None);
            }
            context
        } else {
            let point = self
                .inspect_detached_head(id)
                .await
                .map_err(|_| read_failed())?;
            let cursor = point.as_ref().map(|point| &point.cursor);
            if detached_lineage(&self.instance, cursor) != selected.history_lineage
                || cursor.map_or(0, |cursor| cursor.epoch) != selected.epoch
            {
                return Err(stale());
            }
            let context = match point {
                Some(point)
                    if point.cursor.session_id == id
                        && point.checkpoint.session_id == id
                        && point.checkpoint.recover.state.epoch_id == point.cursor.epoch =>
                {
                    Some(point.checkpoint.recover.context)
                }
                Some(_) => return Err(read_failed()),
                None => None,
            };
            context
        };
        if !self.project_visible(&record).await {
            return Err(unavailable());
        }
        render(id, selected, context.as_ref())
    }
}
