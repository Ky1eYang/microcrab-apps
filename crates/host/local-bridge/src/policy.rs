//! Explicit startup restrictions; these gates are not an OS process sandbox.
use microcrab_host_protocol::{ErrorCode, HostRequest, WireError};

#[derive(Clone, Copy, Debug, Default)]
pub struct BridgePolicy {
    pub read_only: bool,
    pub no_shell: bool,
    pub no_process: bool,
}
impl BridgePolicy {
    pub(crate) fn check(&self, request: &HostRequest) -> Result<(), WireError> {
        let reason = match request {
            HostRequest::Write(_)
            | HostRequest::CachePoint(_)
            | HostRequest::Mutate(_)
            | HostRequest::PreflightMutations { .. }
                if self.read_only =>
            {
                Some("read-only bridge: filesystem writes and cache-point creation are disabled")
            }
            HostRequest::ShellLaunch(_) if self.read_only || self.no_shell => {
                Some("bridge startup policy disables shell launch")
            }
            HostRequest::ProcessSpawn(_) if self.read_only || self.no_process => {
                Some("bridge startup policy disables process spawn")
            }
            _ => None,
        };
        match reason {
            Some(reason) => Err(WireError::new(ErrorCode::Denied, reason).with_cause(
                microcrab_host_protocol::HostCause::from_public_reason(reason),
            )),
            None => Ok(()),
        }
    }
    pub(crate) fn capability(&self, name: &str) -> bool {
        match name {
            "write" | "cache_point" | "file_mutation" => !self.read_only,
            "shell" | "shell.launch" => !self.read_only && !self.no_shell,
            "process" | "process.spawn" => !self.read_only && !self.no_process,
            _ => true,
        }
    }
}
