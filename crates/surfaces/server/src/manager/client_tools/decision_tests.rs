use super::*;
use serde_json::json;
#[test]
fn outcomes_survive_reopen_and_are_immutable() {
    let root = tempfile::tempdir().unwrap();
    let store = DecisionStore::open(root.path()).unwrap();
    let receipt = json!({"decision":"deny","request_id":"fixture"});
    store.record("permission", "fixture", &receipt).unwrap();
    assert!(
        store
            .record("permission", "fixture", &json!({"decision":"allow"}))
            .is_err()
    );
    let reopened = DecisionStore::open(root.path()).unwrap();
    assert_eq!(reopened.get("permission", "fixture"), Some(receipt));
    assert!(reopened.get("round_limit", "fixture").is_none());
}
#[test]
fn failed_receipt_write_never_advances_memory() {
    let root = tempfile::tempdir().unwrap();
    let store = DecisionStore::open(root.path()).unwrap();
    std::fs::create_dir(root.path().join("interaction_outcomes.json.tmp")).unwrap();
    assert!(
        store
            .record("permission", "fixture", &json!({"decision":"allow"}))
            .is_err()
    );
    assert!(store.get("permission", "fixture").is_none());
    assert!(
        DecisionStore::open(root.path())
            .unwrap()
            .get("permission", "fixture")
            .is_none()
    );
}
