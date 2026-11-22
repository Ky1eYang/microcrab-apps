//! Build identity of the Server process answering the authenticated command.
use serde::Serialize;

#[derive(Serialize)]
pub(super) struct Build {
    version: &'static str,
    sha256: Option<String>,
}

pub(super) async fn metadata() -> Build {
    Build {
        version: env!("CARGO_PKG_VERSION"),
        sha256: executable_sha256().await,
    }
}

#[cfg(target_os = "linux")]
async fn executable_sha256() -> Option<String> {
    use futures_util::{
        FutureExt,
        future::{BoxFuture, Shared},
    };
    use std::sync::OnceLock;

    static SHA256: OnceLock<Shared<BoxFuture<'static, Option<String>>>> = OnceLock::new();
    // The process-wide future retains the single blocking task even if the
    // request that first asked for it is cancelled. An unavailable result is cached too.
    SHA256
        .get_or_init(|| {
            let task = tokio::task::spawn_blocking(|| {
                running_executable()
                    .and_then(hash)
                    .inspect_err(|error| {
                        tracing::debug!(kind = ?error.kind(), "Running executable hash unavailable");
                    })
                    .ok()
            });
            async move { task.await.ok().flatten() }.boxed().shared()
        })
        .clone()
        .await
}

#[cfg(not(target_os = "linux"))]
async fn executable_sha256() -> Option<String> {
    // A pathname alone cannot prove the on-disk file still belongs to this process.
    None
}

#[cfg(target_os = "linux")]
fn running_executable() -> std::io::Result<std::fs::File> {
    // Open the procfs magic link itself, not its pathname target. It still
    // identifies the executing file if a new build replaces it on disk.
    std::fs::File::open("/proc/self/exe")
}

#[cfg(target_os = "linux")]
fn hash(mut source: impl std::io::Read) -> std::io::Result<String> {
    use sha2::{Digest, Sha256};

    let mut digest = Sha256::new();
    std::io::copy(&mut source, &mut digest)?;
    Ok(format!("{:x}", digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unavailable_hash_is_explicit_null() {
        let build = Build {
            version: env!("CARGO_PKG_VERSION"),
            sha256: None,
        };
        assert_eq!(
            json!(build),
            json!({"version": env!("CARGO_PKG_VERSION"), "sha256": null})
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn executable_hash_streams_bytes_as_lowercase_sha256() -> std::io::Result<()> {
        assert_eq!(
            hash(&b"abc"[..])?,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn executable_source_is_the_process_executable() -> std::io::Result<()> {
        use std::os::unix::fs::MetadataExt;

        let source = running_executable()?.metadata()?;
        let process = std::fs::metadata(format!("/proc/{}/exe", std::process::id()))?;
        assert_eq!((source.dev(), source.ino()), (process.dev(), process.ino()));
        Ok(())
    }
}
