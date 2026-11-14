//! Device service selects canonical workspaces before opening private state.
use crate::{Bridge, BridgePolicy};
use microcrab_host_protocol::*;
use std::time::{Duration, Instant};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

pub struct BridgeService {
    pub default: Arc<Bridge>,
    roots: Vec<PathBuf>,
    workspaces: Mutex<HashMap<PathBuf, Arc<Bridge>>>,
    require_token: bool,
    token: Option<String>,
    pairing: Option<Mutex<ServicePairing>>,
}
struct ServicePairing {
    token: Option<String>,
    deadline: Instant,
    reusable: bool,
}
impl BridgeService {
    pub fn open(
        root: &Path,
        allowed_roots: &[PathBuf],
        require_token: bool,
        custom_token: Option<String>,
    ) -> Result<(Arc<Self>, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
        Self::open_with_policy(
            root,
            allowed_roots,
            require_token,
            custom_token,
            BridgePolicy::default(),
        )
    }
    pub fn open_with_policy(
        root: &Path,
        allowed_roots: &[PathBuf],
        require_token: bool,
        custom_token: Option<String>,
        policy: BridgePolicy,
    ) -> Result<(Arc<Self>, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
        let root = std::fs::canonicalize(root)?;
        let mut roots = vec![root.clone()];
        for allowed in allowed_roots {
            let allowed = std::fs::canonicalize(allowed)?;
            if !allowed.is_dir() {
                return Err("allowed root must be a directory".into());
            }
            roots.push(allowed);
        }
        let reusable = custom_token.is_some();
        let (default, token) =
            Bridge::open_with_policy(&root, require_token, custom_token, policy)?;
        let root = PathBuf::from(&default.descriptor.workspace_root);
        let deadline = Instant::now() + Duration::from_secs(if reusable { 315360000 } else { 120 });
        Ok((
            Arc::new(Self {
                default: default.clone(),
                roots,
                workspaces: Mutex::new(HashMap::from([(root, default)])),
                require_token,
                token: token.clone(),
                pairing: require_token.then(|| {
                    Mutex::new(ServicePairing {
                        token: token.clone(),
                        deadline,
                        reusable,
                    })
                }),
            }),
            token,
        ))
    }
    pub(crate) fn single(default: Arc<Bridge>) -> Arc<Self> {
        let root = PathBuf::from(&default.descriptor.workspace_root);
        Arc::new(Self {
            default: default.clone(),
            roots: vec![root.clone()],
            workspaces: Mutex::new(HashMap::from([(root, default)])),
            require_token: true,
            token: None,
            pairing: None,
        })
    }
    pub(crate) async fn authenticate(
        &self,
        frame: WireFrame,
    ) -> Result<(Arc<Bridge>, WireFrame), WireError> {
        let (directory, expected_workspace, expected_host) = match &frame {
            WireFrame::Pair {
                directory,
                expected_workspace_id,
                expected_host_id,
                ..
            }
            | WireFrame::Resume {
                directory,
                expected_workspace_id,
                expected_host_id,
                ..
            } => (
                directory.as_deref(),
                expected_workspace_id.as_deref(),
                expected_host_id.as_deref(),
            ),
            _ => return Err(WireError::new(ErrorCode::PairingDenied, "pairing required")),
        };
        let requested_scope = match &frame {
            WireFrame::Pair { scope, .. } | WireFrame::Resume { scope, .. } => scope,
            _ => unreachable!(),
        };
        if !requested_scope.valid() || !requested_scope.is_project() {
            return Err(WireError::new(
                ErrorCode::InvalidRequest,
                "invalid binding scope",
            ));
        }
        // One service capability retains its original expiry and one-use policy;
        // passing it into a child workspace never turns it into a static token.
        let mut capability = match &self.pairing {
            Some(pairing) => Some(pairing.lock().await),
            None => None,
        };
        if let (Some(capability), WireFrame::Pair { token, .. }) = (capability.as_ref(), &frame) {
            if capability.token.as_ref() != Some(token) {
                return Err(WireError::new(ErrorCode::PairingDenied, "pairing denied"));
            }
            if Instant::now() >= capability.deadline {
                return Err(WireError::new(ErrorCode::Timeout, "pairing expired"));
            }
        }
        let root = match directory {
            None => PathBuf::from(&self.default.descriptor.workspace_root),
            Some(value) if value.is_empty() || value.len() > 32768 => {
                return Err(WireError::new(
                    ErrorCode::WorkspaceDenied,
                    "workspace denied",
                ));
            }
            Some(value) => {
                let path = Path::new(value);
                // Relative paths are resolved only by the bridge, under its configured default root.
                let path = if path.is_absolute() {
                    path.to_path_buf()
                } else {
                    PathBuf::from(&self.default.descriptor.workspace_root).join(path)
                };
                std::fs::canonicalize(path).map_err(|error| {
                    WireError::new(
                        ErrorCode::WorkspaceDenied,
                        format!(
                            "cannot canonicalize requested workspace: {error}; kind={:?}",
                            error.kind()
                        ),
                    )
                })?
            }
        };
        if root.components().any(|part| {
            [".microcrab", ".micro-crab"].iter().any(|name| {
                part.as_os_str()
                    .to_string_lossy()
                    .eq_ignore_ascii_case(name)
            })
        }) || !root.is_dir()
            || !self.roots.iter().any(|allowed| root.starts_with(allowed))
        {
            return Err(WireError::new(
                ErrorCode::WorkspaceDenied,
                "workspace denied",
            ));
        }
        if let Some(expected) = expected_host {
            let device = device_id().map_err(|error| {
                WireError::new(
                    ErrorCode::BindingMismatch,
                    format!("device identity unavailable: {error}"),
                )
            })?;
            if expected != device {
                let identity =
                    crate::read_identity(&root.join(".microcrab/host-bridge/identity.json"), &root)
                        .map_err(|_| {
                            WireError::new(ErrorCode::BindingMismatch, "Host identity mismatch")
                        })?;
                if identity.host_id != expected {
                    return Err(WireError::new(
                        ErrorCode::BindingMismatch,
                        "Host identity mismatch",
                    ));
                }
            }
        }
        let mut workspaces = self.workspaces.lock().await;
        if !workspaces.contains_key(&root) {
            if self.token.is_none() && self.require_token {
                return Err(WireError::new(
                    ErrorCode::WorkspaceDenied,
                    "directory selection requires configured service",
                ));
            }
            if self.require_token
                && !matches!(&frame, WireFrame::Pair { token, .. } if self.token.as_ref() == Some(token))
            {
                return Err(WireError::new(ErrorCode::PairingDenied, "pairing denied"));
            }
        }
        let workspace = if let Some(workspace) = workspaces.get(&root) {
            workspace.clone()
        } else {
            // Reject unknown workspace pins before creating any state in a requested directory.
            if expected_workspace.is_some() {
                let path = root.join(".microcrab/host-bridge/identity.json");
                let identity = crate::read_identity(&path, &root).map_err(|_| {
                    WireError::new(ErrorCode::BindingMismatch, "workspace identity mismatch")
                })?;
                if expected_workspace != Some(identity.workspace_id.as_str()) {
                    return Err(WireError::new(
                        ErrorCode::BindingMismatch,
                        "workspace identity mismatch",
                    ));
                }
            }
            if workspaces.len() >= 128 {
                return Err(WireError::new(
                    ErrorCode::Limit,
                    "workspace capacity reached",
                ));
            }
            let (workspace, _) = Bridge::open_with_policy(
                &root,
                self.require_token,
                self.token.clone(),
                self.default.policy,
            )
            .map_err(|error| {
                WireError::new(
                    ErrorCode::WorkspaceDenied,
                    format!("workspace initialization failed: {error}"),
                )
            })?;
            if let Some(capability) = &capability {
                workspace.authority.lock().await.pair_deadline = capability.deadline;
            }
            // Nested allowed roots may overlap. Protect each workspace's private
            // state from every other typed filesystem view in this service.
            for existing in workspaces.values() {
                for (viewer, private) in [(existing, &workspace), (&workspace, existing)] {
                    let private = private
                        .state_dir
                        .parent()
                        .ok_or_else(|| {
                            WireError::new(
                                ErrorCode::WorkspaceDenied,
                                "private storage unavailable",
                            )
                        })?
                        .to_path_buf();
                    viewer.host.protect_workspace_path(private).map_err(|_| {
                        WireError::new(ErrorCode::WorkspaceDenied, "private storage unavailable")
                    })?;
                }
            }
            workspaces.insert(root, workspace.clone());
            workspace
        };
        drop(workspaces);
        let pairing_request = matches!(&frame, WireFrame::Pair { .. });
        let hello = workspace.authenticate(frame).await?;
        if pairing_request
            && matches!(&hello, WireFrame::Hello { .. })
            && let Some(capability) = capability.as_mut()
            && !capability.reusable
        {
            capability.token = None;
        }
        Ok((workspace, hello))
    }
    #[cfg(feature = "ws")]
    pub async fn serve(
        self: Arc<Self>,
        listener: tokio::net::TcpListener,
        origin: String,
    ) -> Result<(), std::io::Error> {
        crate::socket::serve(self, listener, origin).await
    }
    #[cfg(feature = "grpc")]
    pub async fn serve_grpc(
        self: Arc<Self>,
        listener: tokio::net::TcpListener,
        origin: String,
    ) -> Result<(), std::io::Error> {
        crate::grpc::serve(self, listener, origin).await
    }
}

pub(crate) fn device_id() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // OS device name is identification, never an authentication capability. No subprocess or cwd input.
    #[cfg(windows)]
    let name = std::env::var("COMPUTERNAME")?;
    #[cfg(target_os = "macos")]
    let name = {
        use std::io::Read;
        use std::process::{Command, Stdio};
        let mut child = Command::new("/usr/sbin/scutil")
            .args(["--get", "LocalHostName"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let deadline = Instant::now() + Duration::from_millis(500);
        loop {
            if let Some(status) = child.try_wait()? {
                if !status.success() {
                    return Err("platform device name unavailable".into());
                }
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err("platform device name timed out".into());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let mut name = String::new();
        child
            .stdout
            .take()
            .ok_or("platform device name unavailable")?
            .take(1025)
            .read_to_string(&mut name)?;
        name
    };
    #[cfg(all(not(windows), not(target_os = "macos")))]
    let name = {
        use std::io::Read;
        let mut name = String::new();
        let file = std::fs::File::open("/etc/hostname");
        match file {
            Ok(file) => {
                file.take(1025).read_to_string(&mut name)?;
            }
            Err(_) => {
                name = std::env::var("HOSTNAME").map_err(|_| "platform device name unavailable")?;
            }
        }
        name
    };
    let name = name.trim();
    if name.is_empty() || name.len() > 1024 {
        return Err("device name unavailable or unbounded".into());
    }
    Ok(format!(
        "device-{}",
        sha256(format!("microcrab-device-v1\0{}\0{}", std::env::consts::OS, name).as_bytes())
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn pair(token: &str, directory: Option<String>) -> WireFrame {
        WireFrame::Pair {
            token: token.into(),
            scope: BindingScope::project("backend".into(), "binding".into(), "project".into()),
            directory,
            expected_host_id: None,
            expected_workspace_id: None,
        }
    }
    #[tokio::test]
    async fn inherited_generated_token_keeps_service_expiry_and_single_use() {
        let dir = tempfile::tempdir().unwrap();
        let child = dir.path().join("child");
        std::fs::create_dir(&child).unwrap();
        let (service, token) = BridgeService::open(dir.path(), &[], true, None).unwrap();
        let token = token.unwrap();
        service.pairing.as_ref().unwrap().lock().await.deadline = Instant::now();
        assert!(matches!(
            service
                .authenticate(pair(&token, Some(child.to_string_lossy().into())))
                .await,
            Err(WireError {
                code: ErrorCode::Timeout,
                ..
            })
        ));
        assert!(!child.join(".microcrab").exists());
        drop(service);
        let (service, token) = BridgeService::open(dir.path(), &[], true, None).unwrap();
        let token = token.unwrap();
        service.authenticate(pair(&token, None)).await.unwrap();
        assert!(matches!(
            service
                .authenticate(pair(&token, Some(child.to_string_lossy().into())))
                .await,
            Err(WireError {
                code: ErrorCode::PairingDenied,
                ..
            })
        ));
        assert!(!child.join(".microcrab").exists());
    }
    #[tokio::test]
    async fn static_token_selects_child_but_wrong_token_or_private_root_cannot_create_state() {
        let dir = tempfile::tempdir().unwrap();
        let child = dir.path().join("child");
        std::fs::create_dir(&child).unwrap();
        let (service, _) =
            BridgeService::open(dir.path(), &[], true, Some("static".into())).unwrap();
        assert!(
            service
                .authenticate(pair("wrong", Some(child.to_string_lossy().into())))
                .await
                .is_err()
        );
        assert!(!child.join(".microcrab").exists());
        service.authenticate(pair("static", None)).await.unwrap();
        service
            .authenticate(pair("static", Some(child.to_string_lossy().into())))
            .await
            .unwrap();
        assert!(matches!(
            service
                .authenticate(pair("static", Some(".microcrab".into())))
                .await,
            Err(WireError {
                code: ErrorCode::WorkspaceDenied,
                ..
            })
        ));
        let viewer = &service.default.host;
        use microcrab_host_api::Host;
        assert!(
            viewer
                .read_file(
                    "child/.microcrab/host-bridge/identity.json".into(),
                    4096,
                    tokio_util::sync::CancellationToken::new()
                )
                .await
                .is_err()
        );
    }
}
