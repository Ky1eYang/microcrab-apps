//! System and discovery HTTP route descriptors.
//!
//! Provides feature capability discovery, caller identity reflection, and
//! model catalog listing.
use super::endpoint::Endpoint;
use crate::manager::commands;

/// System and server capability HTTP endpoints.
pub(crate) const ENDPOINTS: &[Endpoint] = &[
    // Query server capabilities, limits, and runtime feature flags.
    Endpoint::get("/api/v1/capabilities", commands::SERVER_CAPABILITIES),
    // Query current authenticated identity and principal subject.
    Endpoint::get("/api/v1/identity", commands::IDENTITY_CURRENT),
    // Query configured model providers and models.
    Endpoint::get("/api/v1/models", commands::MODEL_LIST),
    Endpoint::get("/api/v1/models/metadata", commands::MODEL_METADATA_GET),
    Endpoint::post(
        "/api/v1/models/metadata/refresh",
        commands::MODEL_METADATA_REFRESH,
    ),
];
