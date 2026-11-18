mod binary_logging;
use microcrab_local_bridge::{BridgePolicy, BridgeService};
use std::{net::SocketAddr, path::PathBuf};

#[tokio::main]
async fn main() {
    let mut logging_guard = None;
    if let Err(error) = run(&mut logging_guard).await {
        let category = error
            .downcast_ref::<std::io::Error>()
            .map(|error| format!("{:?}", error.kind()))
            .unwrap_or_else(|| "configuration_or_workspace_state".into());
        let cause = microcrab_host_api::FailureReason::from(error.to_string());
        let cause = cause.public_reason().unwrap_or("bridge failure");
        eprintln!(
            "Local host bridge startup or listener failed error_kind={category} cause={cause}"
        );
        tracing::error!(error_kind=%category, cause, "Local host bridge startup or listener failed");
        drop(logging_guard);
        std::process::exit(1);
    }
}

async fn run(
    logging_guard: &mut Option<binary_logging::FlushGuard>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let mut root = None;
    let mut allowed_roots = Vec::new();
    let mut origin = None;
    let mut ready_file = None;
    #[cfg(feature = "ws")]
    let mut listen: Option<SocketAddr> = Some("127.0.0.1:8788".parse()?);
    #[cfg(not(feature = "ws"))]
    let mut listen: Option<SocketAddr> = None;
    #[cfg(feature = "grpc")]
    let mut grpc_listen: Option<SocketAddr> = Some("127.0.0.1:8789".parse()?);
    #[cfg(not(feature = "grpc"))]
    let mut grpc_listen: Option<SocketAddr> = None;
    let mut require_token = false;
    let mut custom_token = None;
    let mut policy = BridgePolicy::default();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!(
                    "microcrab-local-bridge --root PATH --origin ORIGIN [--allow-root PATH]... [--unrestricted-fs] [--listen 127.0.0.1:8788] [--grpc-listen 127.0.0.1:8789] [--no-token] [--require-token] [--token TOKEN] [--ready-file .microcrab/ready.json]\n\
                     Device Host service with isolated workspaces inside configured allowed roots.\n\
                     WebSocket /host stays loopback for the client relay. gRPC is the direct carrier the server dials.\n\
                     A non-loopback gRPC bind requires --require-token. Default mode: no-token.\n\
                     Token mode: pass --require-token or --token <KEY>.\n\
                     Restrictions: --read-only disables typed writes/cache points and all new shell/process starts.\n\
                     --no-shell and --no-process independently disable those launch capabilities.\n\
                     Repeat --allow-root for workspace-selection grants; an explicit drive/filesystem root grants broad selection.\n\
                     Typed filesystem paths are workspace-confined by default. --unrestricted-fs permits absolute outside paths and relative parent traversal.\n\
                     Private paths, device/ADS and link/reparse safety checks remain enforced. Shell/process authorization is independent; execution is NOT an OS filesystem sandbox."
                );
                return Ok(());
            }
            "--unrestricted-fs" => policy.unrestricted_fs = true,
            "--read-only" => policy.read_only = true,
            "--no-shell" => policy.no_shell = true,
            "--no-process" => policy.no_process = true,
            "--allow-root" => allowed_roots.push(PathBuf::from(args.next().ok_or("--allow-root needs a value")?)),
            "--root" => root = Some(PathBuf::from(args.next().ok_or("--root needs a value")?)),
            "--allowed-origin" | "--origin" => {
                origin = Some(args.next().ok_or("--origin needs a value")?)
            }
            "--ready-file" => {
                ready_file = Some(PathBuf::from(args.next().ok_or("--ready-file needs a value")?))
            }
            "--listen" => {
                if cfg!(not(feature = "ws")) {
                    return Err("this build does not include the WebSocket host carrier".into());
                }
                listen = Some(args.next().ok_or("--listen needs a value")?.parse()?);
            }
            "--grpc-listen" => {
                if cfg!(not(feature = "grpc")) {
                    return Err("this build does not include the gRPC host carrier".into());
                }
                grpc_listen = Some(args.next().ok_or("--grpc-listen needs a value")?.parse()?);
            }
            "--no-token" => require_token = false,
            "--require-token" => require_token = true,
            "--token" => {
                require_token = true;
                custom_token = Some(args.next().ok_or("--token needs a value")?);
            }
            _ => {
                return Err(
                    "usage: microcrab-local-bridge --root PATH --allowed-origin ORIGIN [--allow-root PATH]... [--unrestricted-fs] [--listen 127.0.0.1:8788] [--grpc-listen 127.0.0.1:8789] [--no-token] [--require-token] [--token TOKEN]"
                        .into(),
                )
            }
        }
    }
    if listen.is_some_and(|addr| !addr.ip().is_loopback()) {
        return Err("only loopback WebSocket listeners are supported".into());
    }
    if grpc_listen.is_some_and(|addr| !addr.ip().is_loopback()) && !require_token {
        return Err("a non-loopback gRPC listener requires --require-token".into());
    }
    let origin = origin.ok_or("explicit --allowed-origin is required")?;
    let parsed: axum::http::Uri = origin.parse()?;
    if !matches!(parsed.scheme_str(), Some("http" | "https"))
        || parsed.authority().is_none()
        || parsed.path_and_query().is_some_and(|p| p.as_str() != "/")
    {
        return Err("allowed origin must be an exact HTTP(S) origin".into());
    }
    let root = std::fs::canonicalize(root.ok_or("explicit --root is required")?)?;
    let (bridge, token) = BridgeService::open_with_policy(
        &root,
        &allowed_roots,
        require_token,
        custom_token,
        policy,
    )?;
    let writer = binary_logging::PersistentWriter::open(&root.join(".microcrab/host-bridge"))?;
    // main emits terminal failures before dropping this bounded flush guard.
    *logging_guard = Some(writer.guard());
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            "microcrab_host_bridge=info,microcrab_local_bridge=info,microcrab_host_local=debug".into()
        }))
        .with_ansi(false)
        .with_target(true)
        .with_writer(writer)
        .try_init()?;
    tracing::info!(
        event = "local_logging_ready",
        max_file_bytes = 8 * 1024 * 1024,
        retained_files = 4,
        "Persistent local bridge diagnostics enabled"
    );
    let ws_listener = match listen {
        Some(addr) => Some(tokio::net::TcpListener::bind(addr).await?),
        None => None,
    };
    let grpc_listener = match grpc_listen {
        Some(addr) => Some(tokio::net::TcpListener::bind(addr).await?),
        None => None,
    };
    let url = match &ws_listener {
        Some(listener) => Some(format!("ws://{}/host", listener.local_addr()?)),
        None => None,
    };
    let grpc_url = match &grpc_listener {
        Some(listener) => Some(format!("http://{}", listener.local_addr()?)),
        None => None,
    };

    if let Some(path) = ready_file {
        let path = if path.is_absolute() {
            path
        } else {
            root.join(path)
        };
        let parent =
            std::fs::canonicalize(path.parent().ok_or("ready-file needs a parent directory")?)?;
        let private = std::fs::canonicalize(root.join(".microcrab"))?;
        if !parent.starts_with(&private) {
            return Err("ready-file must be within workspace .microcrab".into());
        }
        let destination = parent.join(path.file_name().ok_or("ready-file needs a filename")?);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)?;
        use std::io::Write;
        file.write_all(&serde_json::to_vec(&serde_json::json!({
            "url": url,
            "grpc_url": grpc_url,
            "pairing_token": token.as_deref().unwrap_or("")
        }))?)?;
        file.sync_all()?;
    }

    if let Some(ref token) = token {
        eprintln!("Pairing token: {token}");
    } else {
        eprintln!(
            "Local host bridge running in no-token mode. Auto-pairing enabled for origin {origin}"
        );
    }

    tracing::info!(
        url = url.as_deref().unwrap_or(""),
        grpc_url = grpc_url.as_deref().unwrap_or(""),
        workspace = %root.display(),
        host_id = %bridge.default.descriptor.host_id,
        workspace_id = %bridge.default.descriptor.workspace_id,
        allowed_origin = %origin,
        protocol_version = bridge.default.descriptor.protocol_version,
        require_token = require_token,
        unrestricted_fs = policy.unrestricted_fs,
        read_only = policy.read_only,
        no_shell = policy.no_shell,
        no_process = policy.no_process,
        mode = "local-host",
        "Local host bridge listening"
    );

    let origin = origin.trim_end_matches('/').to_owned();
    let ws_bridge = bridge.clone();
    let grpc_bridge = bridge.clone();
    let ws_origin = origin.clone();
    let grpc_origin = origin;
    tokio::select! {
        result = async move {
            #[cfg(feature = "ws")]
            if let Some(listener) = ws_listener {
                return ws_bridge.serve(listener, ws_origin).await.map_err(|error| error.to_string());
            }
            #[cfg(not(feature = "ws"))]
            let _ = (ws_listener, ws_bridge, ws_origin);
            std::future::pending::<Result<(), String>>().await
        } => result.map_err(std::io::Error::other)?,
        result = async move {
            #[cfg(feature = "grpc")]
            if let Some(listener) = grpc_listener {
                return grpc_bridge.serve_grpc(listener, grpc_origin).await.map_err(|error| error.to_string());
            }
            #[cfg(not(feature = "grpc"))]
            let _ = (grpc_listener, grpc_bridge, grpc_origin);
            std::future::pending::<Result<(), String>>().await
        } => result.map_err(std::io::Error::other)?,
    }
    Ok(())
}
