//! Capability-empty Chat Only execution owner. Metadata is byte-compatible with
//! the historical inert browser descriptor; no browser process is needed to use it.
use microcrab_host_api::{
    FsApi, Host, HostAPI, HostInfo, NetApi, OsFamily, ProcessApi, ShellApi, ToolError,
};
use microcrab_host_protocol::HostDescriptor;
use microcrab_storage::execution::ConversationGrant;

pub(super) struct ServerHost {
    info: HostInfo,
}
impl ServerHost {
    pub(super) fn descriptor(grant: &ConversationGrant) -> Result<HostDescriptor, ToolError> {
        let mut descriptor = HostDescriptor {
            protocol_version: microcrab_host_protocol::PROTOCOL_VERSION,
            host_id: grant.browser_id.clone(),
            workspace_id: grant.workspace_id.clone(),
            bridge_instance_id: format!("chat-only:{}", grant.binding_id),
            workspace_root: "/chat".into(),
            platform: "browser".into(),
            info: HostInfo {
                os_family: OsFamily::Other,
                os: "browser".into(),
                arch: "wasm32".into(),
                shell: "none".into(),
                path_separator: "/".into(),
                newline: "\n".into(),
                capabilities: vec![],
                resources: None,
                virtual_shell: None,
                description: Some("Chat Only has no filesystem, shell, process or Host network execution. Authorized client tools and plugins remain available.".into()),
            },
            tool_capabilities: vec![],
            supported_primitives: vec![],
            descriptor_digest: String::new(),
        };
        descriptor.descriptor_digest = microcrab_host_protocol::descriptor_fingerprint(&descriptor)
            .map_err(|_| ToolError::worker("Chat Only descriptor encoding failed"))?;
        Ok(descriptor)
    }
    pub(super) fn new(descriptor: &HostDescriptor) -> Self {
        Self {
            info: descriptor.info.clone(),
        }
    }
}
impl ProcessApi for ServerHost {}
impl FsApi for ServerHost {}
impl NetApi for ServerHost {}
impl ShellApi for ServerHost {}
impl HostAPI for ServerHost {}
impl Host for ServerHost {
    fn info(&self) -> Result<HostInfo, ToolError> {
        Ok(self.info.clone())
    }
}
