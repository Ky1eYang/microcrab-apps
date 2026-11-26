//! Stateless model relay policy. Allows owner-scoped `user:*` providers and
//! Server shared providers while enforcing destination and concurrency bounds.
use super::{Config, ConfigError};
use microcrab_transport::{Protocol, TransportConfig};
use serde::Deserialize;
use std::collections::HashSet;
use std::net::IpAddr;
use url::{Host, Url};

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RelayConfig {
    pub enabled: bool,
    pub allow_shared_providers: bool,
    pub allow_user_providers: bool,
    pub allowed_providers: Option<Vec<String>>,
    pub allow_http_loopback: bool,
    pub max_concurrent: usize,
    pub max_concurrent_per_owner: usize,
    pub max_request_bytes: usize,
    pub deadline_ms: u64,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            allow_shared_providers: true,
            allow_user_providers: true,
            allowed_providers: None,
            allow_http_loopback: false,
            max_concurrent: 16,
            max_concurrent_per_owner: 2,
            max_request_bytes: 256 * 1024,
            deadline_ms: 120_000,
        }
    }
}

fn invalid(code: &'static str) -> ConfigError {
    ConfigError::new("relay", code)
}

impl RelayConfig {
    pub(crate) fn validate(&self, _config: &Config) -> Result<(), ConfigError> {
        if !(1..=128).contains(&self.max_concurrent)
            || !(1..=self.max_concurrent).contains(&self.max_concurrent_per_owner)
            || !(1024..=1024 * 1024).contains(&self.max_request_bytes)
            || !(1000..=300_000).contains(&self.deadline_ms)
        {
            return Err(invalid("invalid_limits"));
        }
        if let Some(allowed) = &self.allowed_providers {
            if allowed.len() > 256 {
                return Err(invalid("invalid_allowed_providers"));
            }
            let mut seen = HashSet::new();
            for id in allowed {
                if id.trim().is_empty() || id.contains('/') || !seen.insert(id) {
                    return Err(invalid("invalid_allowed_providers"));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn allows_provider(&self, provider_id: &str) -> bool {
        let scope_ok = if provider_id.starts_with("user:") {
            self.allow_user_providers
        } else {
            self.allow_shared_providers
        };
        scope_ok
            && self
                .allowed_providers
                .as_ref()
                .is_none_or(|list| list.iter().any(|item| item == provider_id))
    }

    pub(crate) fn check_transport(
        &self,
        config: &Config,
        transport: &TransportConfig,
    ) -> Result<Url, ConfigError> {
        let endpoint = transport
            .request_endpoint()
            .map_err(|_| invalid("invalid_endpoint"))?;
        if !destination(&endpoint, transport.protocol, self.allow_http_loopback)
            || self_target(config, &endpoint)
        {
            return Err(invalid("unapproved_or_unsafe_endpoint"));
        }
        Ok(endpoint)
    }
}

fn is_loopback_host(host: &Host<&str>) -> bool {
    match host {
        Host::Domain(domain) => domain.trim_end_matches('.').eq_ignore_ascii_case("localhost"),
        Host::Ipv4(ip) => IpAddr::V4(*ip).is_loopback(),
        Host::Ipv6(ip) => IpAddr::V6(*ip).is_loopback(),
    }
}

// Deployment enforces the final DNS/network egress boundary. The Server accepts
// only public HTTPS domain destinations (plus explicit loopback HTTP only when
// `allow_http_loopback` is enabled), never non-loopback raw IPs or itself.
fn destination(url: &Url, protocol: Protocol, allow_http_loopback: bool) -> bool {
    let query_ok = if protocol == Protocol::Google {
        url.query() == Some("alt=sse")
    } else {
        url.query().is_none()
    };
    if !url.username().is_empty() || url.password().is_some() || !query_ok || url.fragment().is_some()
    {
        return false;
    }
    let Some(host) = url.host() else {
        return false;
    };
    if url.scheme() == "https" {
        return matches!(host, Host::Domain(domain) if {
            let host = domain.trim_end_matches('.');
            host.contains('.') && !host.ends_with(".localhost") && !host.ends_with(".local")
        });
    }
    url.scheme() == "http" && allow_http_loopback && is_loopback_host(&host)
}

fn self_target(config: &Config, endpoint: &Url) -> bool {
    let endpoint_host = endpoint.host_str().map(|name| name.trim_end_matches('.'));
    let endpoint_port = endpoint.port_or_known_default();
    let listen_self = Url::parse(&format!("http://{}", config.listen))
        .ok()
        .is_some_and(|listen| {
            listen.host_str().map(|h| h.trim_end_matches('.')) == endpoint_host
                && listen.port_or_known_default() == endpoint_port
        });
    listen_self
        || std::iter::once(&config.auth.redirect_uri)
            .chain(config.auth.allowed_origins.iter())
            .filter_map(|value| Url::parse(value).ok())
            .any(|url| {
                url.host_str().map(|name| name.trim_end_matches('.')) == endpoint_host
                    && url.port_or_known_default() == endpoint_port
            })
}
