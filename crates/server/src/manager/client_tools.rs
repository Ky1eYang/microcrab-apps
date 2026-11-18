//! Generic browser-owned tool execution. Catalog admission belongs to explicit
//! session epoch boundaries; socket binding only leases an already frozen catalog.
mod bridge;
mod decisions;
mod model;
mod store;
#[cfg(test)]
mod tests;

pub(crate) use bridge::ClientTools;
pub(crate) use model::{
    BindRequest, CallRecord, ClientToolDefinition, ClientToolPhase, OutputRequest, PrepareRequest,
    ResultRequest, SecretRequest,
};
#[cfg(test)]
use model::{BindSnapshot, CallStatus, ClientRole};
