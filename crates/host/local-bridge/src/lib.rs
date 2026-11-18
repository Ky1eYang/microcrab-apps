//! One isolated workspace; BridgeService owns device-level directory selection.
mod diagnostics;
mod execution;
#[cfg(feature = "grpc")]
mod grpc;
#[cfg(feature = "ws")]
mod outbound;
mod policy;
mod processes;
pub use policy::BridgePolicy;
mod receipts;
mod service;
mod session;
#[cfg(feature = "ws")]
mod socket;
pub use service::BridgeService;
#[cfg(test)]
mod tests;
mod transfers;
use microcrab_host_api::{Host, HostAPI};
use microcrab_host_local::{LocalHost, ShellLimits};
use microcrab_host_protocol::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct Bridge {
    pub descriptor: HostDescriptor,
    pub(crate) host: LocalHost,
    pub(crate) policy: BridgePolicy,
    legacy_host_id: String,
    pub(crate) state_dir: PathBuf,
    pub(crate) authority: Mutex<Authority>,
    /// Active receipt ownership. The value marks raw reads, which consume pipe
    /// bytes without excluding unrelated workspace effects while still active.
    pub(crate) executing: std::sync::Mutex<HashMap<String, bool>>,
    /// Serial effect admission keeps durable mutation receipts ordered.
    pub(crate) admission: Mutex<()>,
    /// Orders result declarations/chunks only, never execution.
    pub(crate) egress: Mutex<()>,
    /// One owner orders log/index publication; effect dispatch releases this lock.
    pub(crate) receipts: Mutex<receipts::ReceiptStore>,
    pub(crate) processes: processes::Processes,
    _workspace_lock: std::fs::File,
}
pub(crate) struct Authority {
    token: Option<String>,
    require_token: bool,
    pair_deadline: Instant,
    credential: Option<String>,
    /// Current capability/lease ownership only; backend owns durable registration.
    scope: Option<BindingScope>,
    /// Host selected by this credential, including explicitly pinned legacy IDs.
    bound_host_id: Option<String>,
    generation: u64,
    lease_deadline: Instant,
    credential_deadline: Instant,
}
#[derive(Serialize, Deserialize)]
struct Identity {
    host_id: String,
    workspace_id: String,
    canonical_root: PathBuf,
}
impl Bridge {
    pub fn open(
        root: &Path,
    ) -> Result<(Arc<Self>, String), Box<dyn std::error::Error + Send + Sync>> {
        let (bridge, token) = Self::open_with_options(root, true, None)?;
        Ok((bridge, token.unwrap_or_default()))
    }
    pub fn open_with_options(
        root: &Path,
        require_token: bool,
        custom_token: Option<String>,
    ) -> Result<(Arc<Self>, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
        Self::open_with_policy(root, require_token, custom_token, BridgePolicy::default())
    }
    pub fn open_with_policy(
        root: &Path,
        require_token: bool,
        custom_token: Option<String>,
        policy: BridgePolicy,
    ) -> Result<(Arc<Self>, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
        let root = std::fs::canonicalize(root)?;
        let state_dir = root.join(".microcrab").join("host-bridge");
        // Refuse symlink/reparse private storage rather than redirect credentials/receipts.
        for path in [root.join(".microcrab"), state_dir.clone()] {
            if path.exists() && linked(&std::fs::symlink_metadata(&path)?) {
                return Err("bridge private storage must not be a link".into());
            }
            std::fs::create_dir_all(&path)?;
        }
        let lock_path = state_dir.join("instance.lock");
        if lock_path.exists() && linked(&std::fs::symlink_metadata(&lock_path)?) {
            return Err("bridge lock must not be a link".into());
        }
        let workspace_lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        workspace_lock
            .try_lock()
            .map_err(|_| "another bridge already owns this workspace")?;
        let identity_path = state_dir.join("identity.json");
        if identity_path.exists() && linked(&std::fs::symlink_metadata(&identity_path)?) {
            return Err("bridge identity must not be a link".into());
        }
        let identity: Identity = if identity_path.exists() {
            let identity: Identity = serde_json::from_slice(&std::fs::read(&identity_path)?)?;
            if identity.canonical_root != root {
                return Err("durable workspace identity belongs to another root".into());
            }
            identity
        } else {
            let identity = Identity {
                host_id: service::device_id()?,
                workspace_id: Uuid::new_v4().to_string(),
                canonical_root: root.clone(),
            };
            receipts::create_durable(&identity_path, &serde_json::to_vec(&identity)?)?;
            identity
        };
        let host = LocalHost::new_with_filesystem_scope(
            &root,
            ShellLimits::default(),
            policy.unrestricted_fs,
        )
        .or_else(|_| {
            LocalHost::new_filesystem_with_scope(
                &root,
                ShellLimits::default(),
                policy.unrestricted_fs,
            )
        })?;
        host.protect_workspace_path(root.join(".microcrab"))?;
        let retired_private = root.join(".micro-crab");
        if retired_private.exists() {
            host.protect_workspace_path(retired_private)?;
        }
        let mut tool_capabilities: Vec<String> = host
            .tool_capabilities()
            .into_iter()
            .filter(|name| {
                matches!(
                    name.as_str(),
                    "read"
                        | "write"
                        | "glob"
                        | "grep"
                        | "shell"
                        | "process"
                        | "read_file"
                        | "file_mutation"
                        | "cache_point"
                )
            })
            .collect();
        // The file facade does not advertise independent execution owner ports.
        // Derive these gates from the actual native primitives available here.
        for (operation, capability) in [("process.spawn", "process"), ("shell.launch", "shell")] {
            if host.capabilities().supports(operation)
                && !tool_capabilities.iter().any(|name| name == capability)
            {
                tool_capabilities.push(capability.into());
            }
        }
        tool_capabilities.retain(|name| policy.capability(name));
        let mut info = host.info()?;
        info.capabilities = tool_capabilities.clone();
        let mut descriptor = HostDescriptor {
            protocol_version: PROTOCOL_VERSION,
            host_id: service::device_id()?,
            workspace_id: identity.workspace_id,
            bridge_instance_id: Uuid::new_v4().to_string(),
            workspace_root: root
                .to_str()
                .ok_or("workspace root must be Unicode")?
                .into(),
            platform: std::env::consts::OS.into(),
            info,
            tool_capabilities,
            supported_primitives: host
                .capabilities()
                .supported_operations
                .iter()
                .filter(|name| {
                    matches!(
                        name.as_str(),
                        "file_mutation"
                            | "filesystem.stat"
                            | "filesystem.open"
                            | "filesystem.read"
                            | "filesystem.close"
                            | "process.spawn"
                            | "process.status"
                            | "process.pid"
                            | "process.wait"
                            | "process.watch"
                            | "process.signal"
                            | "process.kill-tree"
                            | "process.stdout"
                            | "process.stderr"
                            | "process.write-stdin"
                            | "process.close-stdin"
                            | "process.resize-pty"
                            | "process.release"
                            | "shell.launch"
                            | "shell.capture-status"
                            | "shell.read"
                            | "shell.drain"
                            | "shell.write"
                    )
                })
                .filter(|name| policy.capability(name))
                .cloned()
                .collect(),
            descriptor_digest: String::new(),
        };
        descriptor.descriptor_digest = descriptor_fingerprint(&descriptor)?;
        let has_custom = custom_token.is_some();
        let token = if require_token {
            Some(custom_token.unwrap_or_else(|| Uuid::new_v4().to_string()))
        } else {
            None
        };
        let pair_deadline = if has_custom || !require_token {
            Instant::now() + Duration::from_secs(315360000)
        } else {
            Instant::now() + Duration::from_secs(120)
        };
        let receipts = receipts::ReceiptStore::open(&state_dir, &descriptor.workspace_id)?;
        // Backend owns Project -> Host/workspace registration. Older releases
        // persisted binding.json and binding-host.json here; leave those files
        // untouched, but never treat their stale Project IDs as authorization.
        Ok((
            Arc::new(Self {
                descriptor,
                host,
                policy,
                legacy_host_id: identity.host_id,
                state_dir,
                authority: Mutex::new(Authority {
                    token: token.clone(),
                    require_token,
                    pair_deadline,
                    credential: None,
                    scope: None,
                    bound_host_id: None,
                    generation: 0,
                    lease_deadline: Instant::now(),
                    credential_deadline: Instant::now() + Duration::from_secs(43200),
                }),
                executing: std::sync::Mutex::new(HashMap::new()),
                admission: Mutex::new(()),
                egress: Mutex::new(()),
                receipts: Mutex::new(receipts),
                processes: processes::Processes::default(),
                _workspace_lock: workspace_lock,
            }),
            token,
        ))
    }
    pub async fn authenticate(&self, frame: WireFrame) -> Result<WireFrame, WireError> {
        self.authenticate_mode(frame, None).await
    }
    fn authorize(auth: &Authority, frame: &WireFrame) -> Result<(), WireError> {
        let scope = match frame {
            WireFrame::Pair { scope, .. } | WireFrame::Resume { scope, .. } => scope,
            _ => return Err(WireError::new(ErrorCode::PairingDenied, "pairing required")),
        };
        if !scope.valid() || !scope.is_project() {
            return Err(WireError::new(
                ErrorCode::InvalidRequest,
                "invalid binding scope",
            ));
        }
        match frame {
            WireFrame::Pair { token, .. } if auth.require_token => {
                if auth.token.as_ref() != Some(token) {
                    return Err(WireError::new(ErrorCode::PairingDenied, "pairing denied"));
                }
                if Instant::now() >= auth.pair_deadline {
                    return Err(WireError::new(
                        ErrorCode::Timeout,
                        "pairing capability expired; obtain a fresh local capability",
                    ));
                }
            }
            WireFrame::Resume {
                credential, scope, ..
            } if auth.require_token
                && (auth.credential.as_ref() != Some(credential)
                    || auth.scope.as_ref() != Some(scope)
                    || Instant::now() >= auth.credential_deadline) =>
            {
                return Err(WireError::new(ErrorCode::PairingDenied, "resume rejected"));
            }
            _ => (),
        }
        Ok(())
    }
    pub(crate) async fn check_authorization(&self, frame: &WireFrame) -> Result<(), WireError> {
        Self::authorize(&*self.authority.lock().await, frame)
    }
    // The authority lock spans capability validation, effective identity selection,
    // conditional admission and mutation. Inspection never mutates authority.
    pub(crate) async fn authenticate_mode(
        &self,
        frame: WireFrame,
        inspection: Option<String>,
    ) -> Result<WireFrame, WireError> {
        let mut auth = self.authority.lock().await;
        Self::authorize(&auth, &frame)?;
        let (expected_host, expected_workspace, expected_digest, expected_root) = match &frame {
            WireFrame::Pair {
                expected_host_id,
                expected_workspace_id,
                expected_descriptor_digest,
                expected_workspace_root,
                ..
            }
            | WireFrame::Resume {
                expected_host_id,
                expected_workspace_id,
                expected_descriptor_digest,
                expected_workspace_root,
                ..
            } => (
                expected_host_id.as_deref(),
                expected_workspace_id.as_deref(),
                expected_descriptor_digest.as_deref(),
                expected_workspace_root.as_deref(),
            ),
            _ => return Err(WireError::new(ErrorCode::PairingDenied, "pairing required")),
        };
        let mut descriptor = self.descriptor.clone();
        if let Some(host) = expected_host {
            if host != descriptor.host_id && host != self.legacy_host_id {
                return Err(WireError::new(
                    ErrorCode::BindingMismatch,
                    "Host identity mismatch",
                ));
            }
            descriptor.host_id = host.to_owned();
        }
        if expected_workspace.is_some_and(|id| id != descriptor.workspace_id) {
            return Err(WireError::new(
                ErrorCode::BindingMismatch,
                "workspace identity mismatch",
            ));
        }
        if matches!(&frame, WireFrame::Resume { .. })
            && let Some(bound_host) = &auth.bound_host_id
        {
            if expected_host.is_some_and(|expected| expected != bound_host) {
                return Err(WireError::new(
                    ErrorCode::BindingMismatch,
                    "bound Host identity mismatch",
                ));
            }
            descriptor.host_id = bound_host.clone();
        }
        descriptor.descriptor_digest = descriptor_fingerprint(&descriptor)?;
        if expected_digest.is_some_and(|digest| digest != descriptor.descriptor_digest)
            || expected_root.is_some_and(|root| root != descriptor.workspace_root)
        {
            return Err(WireError::new(
                ErrorCode::BindingMismatch,
                "inspected Host descriptor or workspace changed",
            ));
        }
        if let Some(request_id) = inspection {
            return Ok(WireFrame::Inspected {
                request_id,
                result: Ok(descriptor),
            });
        }
        let generation = auth
            .generation
            .checked_add(1)
            .ok_or_else(|| WireError::new(ErrorCode::Limit, "lease generations exhausted"))?;
        match frame {
            WireFrame::Pair { scope, .. } => {
                if auth.require_token
                    && auth.pair_deadline < Instant::now() + Duration::from_secs(3600)
                {
                    auth.token = None;
                }
                auth.credential = Some(Uuid::new_v4().to_string());
                auth.credential_deadline = Instant::now() + Duration::from_secs(43200);
                auth.scope = Some(scope);
                auth.bound_host_id = Some(descriptor.host_id.clone());
            }
            WireFrame::Resume {
                credential, scope, ..
            } => {
                if !auth.require_token {
                    auth.credential = Some(credential);
                    auth.scope = Some(scope);
                    auth.bound_host_id = Some(descriptor.host_id.clone());
                    auth.credential_deadline = Instant::now() + Duration::from_secs(43200);
                }
            }
            _ => return Err(WireError::new(ErrorCode::PairingDenied, "pairing required")),
        }
        auth.generation = generation;
        auth.lease_deadline = Instant::now() + Duration::from_secs(15);
        Ok(WireFrame::Hello {
            descriptor,
            credential: auth
                .credential
                .clone()
                .ok_or_else(|| WireError::new(ErrorCode::Denied, "missing credential"))?,
            lease_generation: generation.to_string(),
        })
    }
    pub(crate) async fn check_lease(&self, generation: &str) -> Result<BindingScope, WireError> {
        let auth = self.authority.lock().await;
        if generation != auth.generation.to_string()
            || Instant::now() >= auth.lease_deadline
            || Instant::now() >= auth.credential_deadline
        {
            return Err(WireError::new(
                ErrorCode::LeaseExpired,
                "controller lease expired or superseded",
            ));
        }
        auth.scope
            .clone()
            .ok_or_else(|| WireError::new(ErrorCode::Denied, "no binding"))
    }
    #[cfg(feature = "ws")]
    pub async fn serve(
        self: Arc<Self>,
        listener: tokio::net::TcpListener,
        allowed_origin: String,
    ) -> Result<(), std::io::Error> {
        BridgeService::single(self)
            .serve(listener, allowed_origin)
            .await
    }
}
fn linked(metadata: &std::fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}

fn read_identity(
    path: &Path,
    root: &Path,
) -> Result<Identity, Box<dyn std::error::Error + Send + Sync>> {
    for private in [root.join(".microcrab"), root.join(".microcrab/host-bridge")] {
        if linked(&std::fs::symlink_metadata(private)?) {
            return Err("bridge private storage must not be a link".into());
        }
    }
    if linked(&std::fs::symlink_metadata(path)?) {
        return Err("bridge identity must not be a link".into());
    }
    let identity: Identity = serde_json::from_slice(&std::fs::read(path)?)?;
    if identity.canonical_root != root {
        return Err("durable workspace identity belongs to another root".into());
    }
    Ok(identity)
}
