//! Bounded loading of a caller-selected Agent component.

use microcrab_host_api::Host;
use microcrab_session::{
    Error,
    runtime::wasm::{WasmAgent, WasmLimits},
};
use sha2::{Digest, Sha256};
use std::{fs::File, io::Read, path::Path, sync::Arc};

/// Load a component against the supplied Host and return its full lowercase
/// SHA-256. Hashing and Core admission use the same bounded byte snapshot.
/// The caller selects the artifact and schedules this blocking work; the digest
/// identifies the loaded bytes, independently of their configuration.
pub fn load(path: &Path, host: Arc<dyn Host>) -> Result<(WasmAgent, String), Error> {
    load_with_limits(path, WasmLimits::default(), host)
}

/// Load a component against the supplied Host and explicit WASM limits, returning
/// its full lowercase SHA-256. Hashing and Core admission use the same bounded snapshot.
pub fn load_with_limits(
    path: &Path,
    limits: WasmLimits,
    host: Arc<dyn Host>,
) -> Result<(WasmAgent, String), Error> {
    let (bytes, digest) = read_snapshot(path, limits.max_component_bytes)?;
    let agent = WasmAgent::load(&bytes, limits, host)?;
    Ok((agent, digest))
}

fn read_snapshot(path: &Path, max_component_bytes: usize) -> Result<(Vec<u8>, String), Error> {
    let mut bytes = Vec::new();
    // Keep one overflow byte so Core reports its existing typed limit error.
    File::open(path)?
        .take(max_component_bytes as u64 + 1)
        .read_to_end(&mut bytes)?;
    let digest = format!("{:x}", Sha256::digest(&bytes));
    Ok((bytes, digest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use microcrab_host_api::{FsApi, HostAPI, NetApi, ProcessApi, ShellApi};
    use microcrab_session::{
        LimitKind,
        observation::diagnostics::{DiagnosticCode, DiagnosticPhase},
    };
    use std::io::ErrorKind;

    struct FixtureHost;
    impl ProcessApi for FixtureHost {}
    impl FsApi for FixtureHost {}
    impl NetApi for FixtureHost {}
    impl ShellApi for FixtureHost {}
    impl HostAPI for FixtureHost {}
    impl Host for FixtureHost {}

    #[test]
    fn snapshot_hashes_exact_binary_bytes() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let input = b"\x00\xff\r\nabc\x00";
        std::fs::write(file.path(), input).unwrap();
        let (bytes, digest) = read_snapshot(file.path(), input.len()).unwrap();
        assert_eq!(bytes, input);
        assert_eq!(
            digest,
            "2a26772bfcee7be9b43bd9c90e0dc414ac8583a294f0e4e0e8b3487ed1d080fb"
        );
    }

    #[test]
    fn loader_preserves_the_core_limit_and_compile_diagnostic() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let cap = WasmLimits::default().max_component_bytes;
        // Zeros at the cap are malformed rather than oversized.
        file.as_file().set_len(cap as u64).unwrap();
        assert!(matches!(
            load(file.path(), Arc::new(FixtureHost)),
            Err(Error::Diagnostic(diagnostic))
                if diagnostic.code == DiagnosticCode::WasmTrap
                    && diagnostic.phase == DiagnosticPhase::Validation
        ));

        file.as_file().set_len(cap as u64 + 64).unwrap();
        let Err(Error::Limit(limit)) = load(file.path(), Arc::new(FixtureHost)) else {
            panic!("expected Core's byte limit error");
        };
        assert_eq!(limit.kind, LimitKind::WasmBytes);
        assert_eq!(limit.required_bytes, Some(cap + 1));
        assert_eq!(limit.budget_bytes, Some(cap));
    }

    #[test]
    fn loader_propagates_open_errors() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("missing.wasm");
        assert!(matches!(
            load(&path, Arc::new(FixtureHost)),
            Err(Error::Io(source)) if source.kind() == ErrorKind::NotFound
        ));
    }
}
