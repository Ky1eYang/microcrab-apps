//! Generic browser-owned tool execution. Catalog admission belongs to explicit
//! session epoch boundaries; socket binding only leases an already frozen catalog.
mod bridge;
mod model;
mod store;
#[cfg(test)]
mod tests;

pub(crate) use bridge::ClientTools;
pub(crate) use model::{
    BindRequest, ClientToolDefinition, ClientToolPhase, OutputRequest, PrepareRequest,
    ResultRequest,
};
#[cfg(test)]
use model::{BindSnapshot, CallRecord, CallStatus, ClientRole};
