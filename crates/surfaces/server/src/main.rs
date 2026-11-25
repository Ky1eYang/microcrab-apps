//! Binary-only logging and lifecycle composition.
mod logging;
use microcrab_server::{
    application_at,
    config::{Config, DEFAULT_CONFIG_PATH},
};
// Debug candidates correlate sanitized Guest checkpoints with the native
// operation ID. Release keeps its existing narrow filter and verbosity.
#[cfg(debug_assertions)]
const DEFAULT_LOG_FILTER: &str = "microcrab_server=info,microcrab_session::runtime::wasm=debug,microcrab_host_remote::web_transport=debug";
#[cfg(not(debug_assertions))]
const DEFAULT_LOG_FILTER: &str = "microcrab_server=info,microcrab_session::runtime::wasm=error";

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let mut flush = None;
    let result = run(&mut flush).await;
    let failed = result.is_err();
    if let Err(error) = result {
        let diagnostic = startup_diagnostic(error.as_ref());
        if flush.is_some() {
            tracing::error!("{diagnostic}");
        } else {
            eprintln!("{diagnostic}");
        }
    }
    // Explicitly flush after terminal diagnostics; ExitCode does not bypass Drop.
    drop(flush);
    if failed {
        std::process::ExitCode::FAILURE
    } else {
        std::process::ExitCode::SUCCESS
    }
}
/// Only typed or producer-marked causes cross the persistent logging boundary.
/// In particular, an arbitrary I/O inner error may contain paths or credentials.
fn startup_diagnostic(error: &(dyn std::error::Error + 'static)) -> String {
    use microcrab_protocol::diagnostic::{json_reason, sanitize};
    if let Some(config) = error.downcast_ref::<microcrab_server::config::ConfigError>() {
        return format!(
            "server startup configuration failed stage={} code={} cause={}",
            config.stage, config.code, config.cause
        );
    }
    if let Some(session) = error.downcast_ref::<microcrab_session::Error>() {
        if let microcrab_session::Error::Json(json) = session {
            return format!("server Session data invalid: {}", json_reason(json));
        }
        return format!(
            "server Session assembly or shutdown failed: {}",
            session.diagnostic_reason()
        );
    }
    let mut message = microcrab_protocol::diagnostic::describe(error, 2048);
    let mut source = error.source();
    for _ in 0..4 {
        let Some(cause) = source else {
            break;
        };
        if message.len() >= 3072 {
            break;
        }
        message.push_str(": ");
        message.push_str(&microcrab_protocol::diagnostic::describe(cause, 512));
        source = cause.source();
    }
    if let Some(io) = error.downcast_ref::<std::io::Error>() {
        message.push_str(&format!(
            "; kind={:?}; os_code={:?}",
            io.kind(),
            io.raw_os_error()
        ));
    }
    format!(
        "server startup or shutdown failed: {}",
        sanitize(&message, 4096)
    )
}
async fn run(flush: &mut Option<logging::FlushGuard>) -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let config = match (args.next().as_deref(), args.next(), args.next()) {
        (None, None, None) => Config::read_default()?,
        (Some("--config"), Some(path), None) => Config::read(path)?,
        (Some("--help"), None, None) => {
            println!(
                "microcrab-server [--config <private configuration.jsonc>]\nDefault: {DEFAULT_CONFIG_PATH} relative to the startup directory\nFalls back to .microcrab/config.json only when the default JSONC file is absent.\nJSONC supports // and /* */ comments and trailing commas. Projects are managed through the project API."
            );
            return Ok(());
        }
        _ => {
            eprintln!("usage: microcrab-server [--config <private configuration.jsonc>]");
            return Err("invalid startup arguments".into());
        }
    };
    let writer = logging::PersistentWriter::open(&config.data_dir)?;
    *flush = Some(writer.guard());
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| DEFAULT_LOG_FILTER.into()),
        )
        .with_target(true)
        .with_ansi(false)
        .with_writer(writer)
        .init();
    config.log_provider_discovery_warnings();
    let listener = tokio::net::TcpListener::bind(&config.listen).await?;
    let address = listener.local_addr()?;
    let (router, state) = application_at(config, address).await?;
    tracing::info!(%address,"microcrab server listening");
    let relay = state.manager.clone();
    let service = axum::serve(listener, router).with_graceful_shutdown(async move {
        let _ = tokio::signal::ctrl_c().await;
        relay.stop_external_reply_reads();
    });
    let result = service.await;
    let cleanup = state.manager.close().await;
    result?;
    cleanup?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct LogCapture(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for LogCapture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn startup_causes_preserve_public_evidence_without_private_error_text() {
        let private = "https://secret.invalid/?token=fixture-private C:\\private\\configuration";
        let marked = microcrab_session::Error::config_with_private(
            "Pending Session deletion recovery failed; inspect durable cleanup receipts",
            private,
        );
        let message = startup_diagnostic(&marked);
        assert!(message.contains("assembly or shutdown failed"));
        assert!(message.contains("Pending Session deletion recovery failed"));
        assert!(!message.contains(private));
        let opaque = microcrab_session::Error::Config(private.into());
        assert!(!startup_diagnostic(&opaque).contains(private));
        let io = std::io::Error::other(private);
        assert!(!startup_diagnostic(&io).contains(private));
        let namespace =
            std::io::Error::other("persisted Host namespace differs from startup directory");
        assert!(
            startup_diagnostic(&namespace)
                .contains("persisted Host namespace differs from startup directory")
        );
        let auth = microcrab_server::auth::AuthError::Configuration;
        assert!(startup_diagnostic(&auth).contains("invalid authentication configuration"));
    }

    #[test]
    fn default_filter_retains_wasm_errors_and_only_enables_diagnostics_in_debug() {
        let output = LogCapture::default();
        let writer = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(DEFAULT_LOG_FILTER)
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            tracing::error!(target: "microcrab_session::runtime::wasm::evidence", evidence_id = "fixture-id", "wasm failure fixture");
            tracing::debug!(target: "microcrab_session::runtime::wasm::evidence", "wasm debug fixture");
            tracing::debug!(target: "microcrab_host_remote::web_transport", "operation correlation fixture");
            tracing::info!(target: "unrelated_library", "suppressed unrelated fixture");
        });
        let bytes = output.0.lock().unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains("ERROR"));
        assert!(text.contains("fixture-id"));
        assert!(text.contains("wasm failure fixture"));
        assert_eq!(text.contains("wasm debug fixture"), cfg!(debug_assertions));
        assert_eq!(text.contains("operation correlation fixture"), cfg!(debug_assertions));
        assert!(!text.contains("suppressed"));
    }
}
