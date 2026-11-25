//! Browser inventory is an input proposal, not authority to execute or replay a call.
use super::{Catalog, ClientTools, Error, Lease, MAX_JSON_BYTES, State, bounded};
use async_trait::async_trait;
use microcrab_protocol::{
    ScopedVariableSet, VariablePersistence, VariableRole, VariableVisibility,
};
use microcrab_session::{
    ImportantInput, InputAdmissionBoundary, InputAdmissionResolver, PreparedInputAdmission,
    SessionInput, VariablesChange,
};
use serde_json::Value;
use std::{collections::HashSet, sync::Arc};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod participation_tests;

const PLUGINS: &str = "plugins";
const PLUGIN_TOOL: &str = "plugin";
const INVENTORY_CHANGED: &str = "The browser plugin executor or its inventory changed. Use the current `plugins` inventory; use `plugin` list and describe again before making a new call. Do not reuse old call pins or replay calls whose effects are uncertain; observe their existing outcomes instead.";
const INVENTORY_EMPTY: &str = "The browser plugin executor or its inventory changed. No browser plugins are certified for this input. If plugins become available, use `plugin` list and describe again. Do not reuse old call pins or replay calls whose effects are uncertain; observe their existing outcomes instead.";

struct BrowserInputResolver<F> {
    bridge: Arc<ClientTools>,
    socket: Option<String>,
    proposed: Option<Vec<Value>>,
    session_state: microcrab_session::State,
    notify: Arc<F>,
}

#[derive(Clone, PartialEq, Eq)]
struct LogicalOwner {
    client: String,
    catalog_digest: String,
}

#[derive(Clone)]
pub(super) struct CommittedInventory {
    // Generation and epoch certify reuse; logical identity suppresses spurious
    // notices when the same browser rebinds and supplies the same inventory.
    lease_id: String,
    catalog_epoch: u64,
    owner: LogicalOwner,
    entries: Vec<Value>,
}

impl ClientTools {
    /// Capture an untrusted summary now; do not inspect or grant a lease until
    /// the queued input actually reaches Core's final Turn admission boundary.
    /// The notifier publishes the handoff before the provider can issue a tool
    /// request. It is synchronous and must not reenter the bridge.
    pub(crate) fn browser_input_resolver(
        self: &Arc<Self>,
        socket: Option<String>,
        input: &mut SessionInput,
        state: microcrab_session::State,
        notify: impl Fn() + Send + Sync + 'static,
    ) -> Result<Arc<dyn InputAdmissionResolver>, Error> {
        let (inventory, duplicate) = {
            let mut proposals = input
                .variables
                .scoped_set
                .extract_if(.., |entry| entry.name == PLUGINS);
            (proposals.next(), proposals.next().is_some())
        };
        Self::strip_browser_input_plugins(input);
        if duplicate {
            return Err(Error::config("duplicate scoped browser plugin inventory"));
        }
        let proposed = inventory.map(validate_inventory).transpose()?;
        Ok(Arc::new(BrowserInputResolver {
            bridge: self.clone(),
            socket,
            proposed,
            session_state: state,
            notify: Arc::new(notify),
        }))
    }

    /// Pending edits can replace user content but cannot supply a new inventory.
    pub(crate) fn strip_browser_input_plugins(input: &mut SessionInput) {
        input.variables.set.remove(PLUGINS);
        input.variables.remove.retain(|key| key != PLUGINS);
        input
            .variables
            .scoped_set
            .retain(|entry| entry.name != PLUGINS);
    }

    pub(crate) fn input_handoff_notified(&self, lease: &(String, String)) -> bool {
        self.last_input_handoff_notice
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            == Some(lease)
    }
}

fn validate_inventory(scoped: ScopedVariableSet) -> Result<Vec<Value>, Error> {
    if scoped.visibility == VariableVisibility::Visible
        && scoped.persistence == VariablePersistence::Ephemeral
    {
        return Err(Error::config(
            "model-visible ephemeral variables are not supported",
        ));
    }
    if scoped.visibility != VariableVisibility::Visible
        || scoped.persistence != VariablePersistence::Persisted
        || scoped
            .editors
            .is_some_and(|roles| roles.iter().any(|role| *role != VariableRole::Client))
    {
        return Err(Error::config(
            "browser plugin inventory must be Visible, Persisted, and Client-only",
        ));
    }
    bounded(&scoped.value, MAX_JSON_BYTES)?;
    let Value::Array(mut entries) = scoped.value else {
        return Err(Error::config("browser plugin inventory must be an array"));
    };
    if entries.len() > 32 {
        return Err(Error::config("browser plugin inventory exceeds 32 entries"));
    }
    let mut ids = HashSet::new();
    for entry in &entries {
        let fields = entry
            .as_object()
            .ok_or_else(|| Error::config("browser plugin summary must be an object"))?;
        if fields.keys().any(|key| {
            !matches!(
                key.as_str(),
                "id" | "name" | "version" | "artifact" | "description"
            )
        }) {
            return Err(Error::config("unsupported browser plugin summary field"));
        }
        let id = fields
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::config("browser plugin summary requires an id"))?;
        let id_char = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
        if id.len() > 128
            || !id.bytes().next().is_some_and(id_char)
            || !id
                .bytes()
                .all(|byte| id_char(byte) || matches!(byte, b'.' | b'_' | b'-'))
            || !ids.insert(id)
        {
            return Err(Error::config("invalid or duplicate browser plugin id"));
        }
        let artifact = fields
            .get("artifact")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::config("browser plugin summary requires an artifact"))?;
        if !matches!(artifact.len(), 16 | 64)
            || !artifact
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(Error::config("invalid browser plugin artifact reference"));
        }
        // The frontend measures display-string limits in UTF-16 code units.
        let bounded_string = |value: &Value, max| {
            value
                .as_str()
                .is_some_and(|text| text.encode_utf16().count() <= max)
        };
        if !fields
            .get("name")
            .is_some_and(|value| value.as_str() != Some("") && bounded_string(value, 128))
            || !fields
                .get("version")
                .is_some_and(|value| bounded_string(value, 128))
            || fields
                .get("description")
                .is_some_and(|value| !bounded_string(value, 256))
        {
            return Err(Error::config("invalid browser plugin summary text"));
        }
    }
    entries.sort_unstable_by(|left, right| left["id"].as_str().cmp(&right["id"].as_str()));
    Ok(entries)
}

#[async_trait]
impl<F: Fn() + Send + Sync + 'static> InputAdmissionResolver for BrowserInputResolver<F> {
    async fn prepare(
        &self,
        boundary: InputAdmissionBoundary,
        _input: &SessionInput,
        cancellation: CancellationToken,
    ) -> Result<PreparedInputAdmission, Error> {
        if matches!(boundary, InputAdmissionBoundary::Steer) {
            return Ok(PreparedInputAdmission::new(
                VariablesChange::default(),
                None,
            ));
        }
        let mut guard = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(Error::Cancelled),
            guard = self.bridge.state.clone().lock_owned() => guard,
        };
        let current = self.session_state.extract(PLUGINS);
        let candidate = select(&guard, self.socket.as_deref(), self.proposed.as_deref());
        let (change, notice) =
            candidate.overlay(guard.committed_inventory.as_ref(), current.as_ref())?;
        let bridge = self.bridge.clone();
        // This is the only mutation path. Dropping a failed/cancelled admission
        // releases the owned lock without moving any authority or inventory.
        let notify = self.notify.clone();
        Ok(
            PreparedInputAdmission::new(change, notice).with_commit(move || {
                candidate.commit(&mut guard, &bridge, || notify());
            }),
        )
    }
}

struct Candidate {
    replacement: Option<Lease>,
    inventory: Option<CommittedInventory>,
}

fn has_plugin_tool(catalog: &Catalog) -> bool {
    catalog
        .definitions
        .iter()
        .any(|entry| entry.definition.name == PLUGIN_TOOL)
}

fn select(state: &State, socket: Option<&str>, proposed: Option<&[Value]>) -> Candidate {
    let catalog = state.durable.catalogs.last();
    let sender = socket.zip(proposed).and_then(|(socket, proposed)| {
        if !state.execution_idle() {
            // Even the incumbent may have changed plugin configuration while an
            // older call is settling. Retain its existing certified inventory.
            return None;
        }
        let catalog = catalog?;
        let member = state
            .participants
            .get(socket)
            .filter(|member| member.matches_catalog(catalog))?;
        let current = state
            .lease
            .as_ref()
            .filter(|lease| lease.socket == socket && lease.client == member.client);
        let (id, replacement) = match current {
            Some(lease) => (lease.id.clone(), None),
            None => {
                let id = uuid::Uuid::new_v4().to_string();
                let lease = Lease {
                    socket: socket.into(),
                    client: member.client.clone(),
                    id: id.clone(),
                };
                (id, Some(lease))
            }
        };
        let inventory = has_plugin_tool(catalog).then(|| CommittedInventory {
            lease_id: id,
            catalog_epoch: catalog.epoch,
            owner: LogicalOwner {
                client: member.client.clone(),
                catalog_digest: catalog.digest.clone(),
            },
            entries: proposed.to_vec(),
        });
        Some(Candidate {
            replacement,
            inventory,
        })
    });
    sender.unwrap_or_else(|| {
        let inventory = state
            .committed_inventory
            .as_ref()
            .filter(|inventory| {
                let Some((lease, catalog)) = state.lease.as_ref().zip(catalog) else {
                    return false;
                };
                inventory.lease_id == lease.id
                    && state
                        .participants
                        .get(&lease.socket)
                        .is_some_and(|member| member.matches_catalog(catalog))
                    && inventory.catalog_epoch == catalog.epoch
                    && inventory.owner.catalog_digest == catalog.digest
                    && inventory.owner.client == lease.client
            })
            .cloned();
        Candidate {
            replacement: None,
            inventory,
        }
    })
}

impl Candidate {
    fn overlay(
        &self,
        previous: Option<&CommittedInventory>,
        current: Option<&Value>,
    ) -> Result<(VariablesChange, Option<ImportantInput>), Error> {
        let entries = self
            .inventory
            .as_ref()
            .map_or(&[][..], |inventory| inventory.entries.as_slice());
        let logical_owner_changed = previous.map(|inventory| &inventory.owner)
            != self.inventory.as_ref().map(|inventory| &inventory.owner);
        let previous_inventory_changed =
            previous.is_some_and(|inventory| !same_entries(&inventory.entries, entries));
        let restored_inventory_changed = match current {
            Some(Value::Array(current)) => !same_entries(current, entries),
            Some(_) => true,
            None => !entries.is_empty(),
        };
        let notice =
            (logical_owner_changed || previous_inventory_changed || restored_inventory_changed)
                .then(|| {
                    ImportantInput::new(if entries.is_empty() {
                        INVENTORY_EMPTY
                    } else {
                        INVENTORY_CHANGED
                    })
                })
                .transpose()?;
        let change = VariablesChange {
            scoped_set: vec![ScopedVariableSet {
                name: PLUGINS.into(),
                value: Value::Array(entries.to_vec()),
                visibility: VariableVisibility::Visible,
                persistence: VariablePersistence::Persisted,
                editors: Some(vec![VariableRole::Client]),
            }],
            ..Default::default()
        };
        Ok((change, notice))
    }

    fn commit(self, state: &mut State, bridge: &ClientTools, notify: impl FnOnce()) {
        state.committed_inventory = self.inventory;
        if let Some(lease) = self.replacement {
            let witness = (lease.socket.clone(), lease.id.clone());
            state.lease = Some(lease);
            *bridge
                .last_input_handoff_notice
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(witness.clone());
            notify();
            bridge.executor_changes.send_replace(Some(witness));
        }
    }
}

fn same_entries(left: &[Value], right: &[Value]) -> bool {
    left == right
        || (left.len() == right.len()
            && left.iter().all(|entry| right.contains(entry))
            && right.iter().all(|entry| left.contains(entry)))
}
