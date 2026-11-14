//! Referenced archives use the same canonical scope fence as initial queries.
use super::*;

#[test]
fn required_fork_source_missing_is_rejected_without_creating_paths() {
    let root = tempfile::tempdir().unwrap();
    let sessions = root.path().join("sessions");
    let missing = sessions.join("source");
    assert!(check_scope(&sessions, "source", None, &missing, true).is_err());
    assert!(!sessions.exists());
    assert!(check_scope(&sessions, "source", None, &missing, false).is_ok());
    assert!(!sessions.exists());
}

#[test]
fn linked_fork_source_cannot_escape_the_authorized_session_root() {
    let root = tempfile::tempdir().unwrap();
    let sessions = root.path().join("sessions");
    let outside = root.path().join("outside");
    std::fs::create_dir(&sessions).unwrap();
    std::fs::create_dir(&outside).unwrap();
    let link = sessions.join("source");
    #[cfg(windows)]
    {
        let status = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&link)
            .arg(&outside)
            .status()
            .unwrap();
        assert!(status.success(), "junction fixture creation failed");
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, &link).unwrap();
    assert!(check_scope(&sessions, "source", None, &link, true).is_err());
}

#[test]
fn linked_fork_source_to_a_sibling_scope_is_rejected_too() {
    let root = tempfile::tempdir().unwrap();
    let sessions = root.path().join("sessions");
    let other = sessions.join("other");
    std::fs::create_dir_all(&other).unwrap();
    let link = sessions.join("source");
    #[cfg(windows)]
    {
        let status = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&link)
            .arg(&other)
            .status()
            .unwrap();
        assert!(status.success(), "sibling junction fixture creation failed");
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink(&other, &link).unwrap();
    assert!(check_scope(&sessions, "source", None, &link, true).is_err());
    assert!(check_scope(&sessions, "other", None, &other, true).is_ok());
}
