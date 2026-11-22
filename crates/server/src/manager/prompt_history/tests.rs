use super::*;
use microcrab_session::context::Context;
use serde_json::{Value, json};

fn selection(epoch: u64) -> Selection {
    Selection {
        history_lineage: "selected".into(),
        epoch,
    }
}

fn payload(selected: &Selection, context: Option<&ContextSnapshot>) -> Value {
    serde_json::from_slice(&render("session", selected, context).unwrap()).unwrap()
}

#[test]
fn frozen_prompt_never_uses_pending_or_other_epoch_catalogs_as_current_evidence() {
    let mut context = Context::new("  exact SYSTEM\n".into(), vec![], 64 * 1024)
        .unwrap()
        .snapshot()
        .unwrap();
    let selected = selection(context.epoch);
    let declared = ToolDefinition {
        name: "declared".into(),
        description: "no executable policy implied".into(),
        parameters: json!({"type":"object","required":["example"]}),
    };
    context.tools = vec![declared.clone()];
    context.historical_tools.insert(100, vec![declared]);
    let exact = payload(&selected, Some(&context));
    assert_eq!(exact["system"], "  exact SYSTEM\n");
    assert_eq!(exact["tools"][0]["name"], "declared");
    assert_eq!(exact["tools"].as_array().unwrap().len(), 1);
    assert_eq!(exact["tools"][0].as_object().unwrap().len(), 3);
    assert_eq!(exact["status"], "available");
    let assert_missing = |value: Value| {
        assert_eq!(value["status"], "unavailable");
        assert_eq!(value["reason"], "missing_evidence");
        assert!(value.get("tools").is_none());
        assert!(value.get("system").is_none());
    };
    assert_missing(payload(&selected, None));
    assert_missing(payload(&selection(100), Some(&context)));
    context.pending_epoch = true;
    assert_missing(payload(&selected, Some(&context)));
    assert_missing(payload(&selection(selected.epoch + 1), Some(&context)));
}

#[test]
fn frozen_prompt_budget_counts_json_encoding_and_never_returns_partial_evidence() {
    let mut context = Context::new("x".into(), vec![], MAX_RESPONSE_BYTES)
        .unwrap()
        .snapshot()
        .unwrap();
    let selected = selection(context.epoch);
    let envelope = render("session", &selected, Some(&context)).unwrap().len() - 1;
    context.system = "x".repeat(MAX_RESPONSE_BYTES - envelope);
    assert_eq!(
        render("session", &selected, Some(&context)).unwrap().len(),
        MAX_RESPONSE_BYTES
    );
    // The raw length stays constant, but JSON escaping adds a byte.
    context.system.replace_range(..1, "\n");
    let limited = payload(&selected, Some(&context));
    assert_eq!(limited["status"], "unavailable");
    assert_eq!(limited["reason"], "response_limit");
    assert!(limited.get("system").is_none());
    assert!(limited.get("tools").is_none());
}
