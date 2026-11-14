//! Build and exercise the actual executable used by the external OIDC/WS harness.
#[test]
fn binary_exposes_explicit_configuration_and_safe_failure() {
    let binary = env!("CARGO_BIN_EXE_microcrab-server");
    let help = std::process::Command::new(binary)
        .arg("--help")
        .output()
        .unwrap();
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("--config"));
    let text = String::from_utf8_lossy(&help.stdout);
    assert!(text.contains("Default: .microcrab/config.jsonc"));
    assert!(text.contains("Falls back to .microcrab/config.json only"));
    assert!(text.contains("Projects are managed through the project API"));
    let startup = tempfile::tempdir().unwrap();
    let missing = std::process::Command::new(binary)
        .current_dir(startup.path())
        .output()
        .unwrap();
    assert!(!missing.status.success());
    assert!(!String::from_utf8_lossy(&missing.stdout).contains("jwt_secret"));
}
