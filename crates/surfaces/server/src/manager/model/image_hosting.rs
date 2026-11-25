//! Opt-in Rust embedding boundary; no HTTP, config file or Session grant producer.
#[cfg(all(test, feature = "storage-file"))]
mod tests;
use super::{Manager, normalize};
use microcrab_auditor::{Auditor, VariableView};
use microcrab_creator::RetryingClient;
use microcrab_protocol::Response;
use microcrab_storage::ModelSelection;
use microcrab_transport::{
    Content, Error, ImageHostingConfig, ImageHostingGrant, ImageHostingScope, ImageUrlResolver,
    ModelEvent, Protocol, ToolImagePosition,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Private startup/embedding policy for one exact owner and resolved model route.
/// Do not construct it from a request DTO or print any part of the endpoint.
/// Supplying this config attests to verified provider tool-image URL support.
pub struct ProviderImageHosting {
    pub owner: String,
    pub selection: ModelSelection,
    pub protocol: Protocol,
    /// Complete expected request endpoint, including the protocol resource path
    /// and any route query. Compared as a URL, not by string prefix or host only.
    pub request_endpoint: url::Url,
    /// Opaque upload authority; do not put credentials in this label.
    pub authority: String,
    pub allowed_hosts: Vec<String>,
    pub max_hint_ttl: Duration,
    pub uploader: Arc<dyn ImageUrlResolver>,
}

/// Immutable scoped route. Not registered as a Session client or exposed over
/// HTTP. Drop it when the selected route or policy is replaced.
pub struct PreparedImageHostingRoute {
    owner: String,
    scope: ImageHostingScope,
    client: RetryingClient,
}

impl PreparedImageHostingRoute {
    pub(super) fn new(
        policy: ProviderImageHosting,
        witness: String,
        protocol: Protocol,
        endpoint: &url::Url,
        client: RetryingClient,
    ) -> Result<Self, Error> {
        if policy.protocol != protocol || policy.request_endpoint != *endpoint {
            return Err(Error::config(
                "image hosting provider route does not match the configured policy",
            ));
        }
        // Bind an uploader's own reuse policy to the exact owner, even for
        // operator catalog routes whose underlying route witness is shared.
        let route = opaque_route(&witness, &policy.owner, &policy.selection);
        let scope = ImageHostingScope {
            route,
            authority: policy.authority,
        };
        let client = client.with_image_hosting(
            ImageHostingConfig {
                scope: scope.clone(),
                allowed_hosts: policy.allowed_hosts,
                max_hint_ttl: policy.max_hint_ttl,
            },
            policy.uploader,
        )?;
        Ok(Self {
            owner: policy.owner,
            scope,
            client,
        })
    }

    /// `authenticated_owner` and fresh provenance must come from trusted caller
    /// state. Coordinates alone do not authorize hosting. Never scan history.
    pub fn authorize<'a>(
        &'a self,
        authenticated_owner: &str,
        canonical: &'a Content,
        positions: &[ToolImagePosition],
    ) -> Result<ImageHostingGrant<'a>, Error> {
        if authenticated_owner != self.owner {
            return Err(Error::config(
                "image hosting owner does not match the prepared route",
            ));
        }
        self.client
            .authorize_image_hosting(&self.scope, canonical, positions)
    }

    pub async fn complete(
        &self,
        grant: ImageHostingGrant<'_>,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
    ) -> Result<Response, Error> {
        self.client
            .complete_with_image_hosting(grant, auditor, variables, cancellation)
            .await
    }

    pub async fn stream(
        &self,
        grant: ImageHostingGrant<'_>,
        auditor: &dyn Auditor,
        variables: &dyn VariableView,
        cancellation: CancellationToken,
        events: Option<mpsc::Sender<ModelEvent>>,
    ) -> Result<Response, Error> {
        self.client
            .stream_with_image_hosting(grant, auditor, variables, cancellation, events)
            .await
    }
}

fn opaque_route(witness: &str, owner: &str, selection: &ModelSelection) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"provider-image-hosting-v1\0");
    for label in [
        witness,
        owner,
        &selection.provider_id,
        &selection.model,
        selection.variant.as_deref().unwrap_or_default(),
    ] {
        digest.update((label.len() as u64).to_be_bytes());
        digest.update(label.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

impl Manager {
    /// Resolve the ordinary Server catalog and prepare a separate trusted hosted
    /// route. This does not configure the Manager's ordinary model traffic.
    pub async fn prepare_image_hosting_route(
        &self,
        mut policy: ProviderImageHosting,
    ) -> Result<PreparedImageHostingRoute, microcrab_session::Error> {
        if self.client_override.is_some() {
            return Err(microcrab_session::Error::config(
                "image hosting requires the real configured provider route",
            ));
        }
        if policy.owner.trim().is_empty()
            || policy.owner.len() > 256
            || policy.owner.chars().any(char::is_control)
        {
            return Err(microcrab_session::Error::config(
                "image hosting requires a bounded authenticated owner",
            ));
        }
        if policy.selection.provider_id.starts_with("user:") {
            return Err(microcrab_session::Error::config(
                "image hosting requires an operator-configured provider route",
            ));
        }
        policy.selection = normalize(policy.selection);
        let route = super::prepare_route(self, &policy.owner, &policy.selection).await?;
        Ok(PreparedImageHostingRoute::new(
            policy,
            route.witness,
            route.protocol,
            &route.endpoint,
            route.client,
        )?)
    }
}
