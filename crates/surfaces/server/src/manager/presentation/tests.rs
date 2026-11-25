use super::*;
use serde_json::json;

fn row() -> ExecutionSnapshot {
    let mut row = ExecutionSnapshot::new(
        "session".into(),
        "call".into(),
        "view_image".into(),
        "agent".into(),
        None,
    );
    row.execution_id = ExecutionId(7);
    row
}

fn authored_image() -> AuditBody {
    AuditBody {
        epoch: 2,
        turn_id: Some(3),
        round: Some(4),
        tool_call_id: Some("call".into()),
        tool_execution_id: Some(7),
        authored_image_arguments: true,
        payload: HookPayload::ToolCall {
            name: "view_image".into(),
            arguments: json!({"image_url":"unmodified authored source"}),
        },
        ..Default::default()
    }
}

#[test]
fn presentation_requires_an_exact_scoped_producer_observation() {
    let known = Presentations::default();
    let row = row();
    let start = authored_image();
    assert_eq!(
        known.observe(
            &AuditBody {
                authored_image_arguments: false,
                ..start.clone()
            },
            &row
        ),
        None
    );
    assert_eq!(
        known.observe(
            &AuditBody {
                tool_execution_id: Some(8),
                ..start.clone()
            },
            &row
        ),
        None
    );
    assert!(
        serde_json::to_value(NativeExecution::new(&row, Some(&known)))
            .unwrap()
            .get("presentation")
            .is_none()
    );
    assert_eq!(known.observe(&start, &row), Some(Presentation::ViewImage));
    assert_eq!(
        known.permission("session", "call", "view_image"),
        Some(Presentation::ViewImage)
    );
    assert_eq!(
        known.permission("different-session", "call", "view_image"),
        None
    );
    assert_eq!(known.permission("session", "call", "arbitrary"), None);
    let mut wire = serde_json::to_value(NativeExecution::new(&row, Some(&known))).unwrap();
    assert_eq!(
        wire.as_object_mut().unwrap().remove("presentation"),
        Some(json!("view-image"))
    );
    assert_eq!(wire, serde_json::to_value(&row).unwrap());

    let done = AuditBody {
        authored_image_arguments: false,
        payload: HookPayload::ToolResult {
            result: Some("unmodified result".into()),
            error: None,
        },
        ..start
    };
    assert_eq!(
        known.observe(
            &AuditBody {
                epoch: 3,
                ..done.clone()
            },
            &row
        ),
        None
    );
    assert_eq!(known.observe(&done, &row), Some(Presentation::ViewImage));
    assert_eq!(known.permission("session", "call", "view_image"), None);
}

#[test]
fn typed_results_bind_generic_presentation_only_to_exact_native_rows() {
    for (witness, value) in [
        (ToolPresentation::CodingFile, "coding-file"),
        (ToolPresentation::MessageReceipt, "message-receipt"),
    ] {
        let known = Presentations::default();
        let mut row = row();
        row.tool = "not-a-recognized-name".into();
        let body = AuditBody {
            authored_image_arguments: false,
            tool_presentation: Some(witness),
            payload: HookPayload::ToolResult {
                result: Some("raw".into()),
                error: None,
            },
            ..authored_image()
        };
        assert_eq!(
            known.observe(
                &AuditBody {
                    tool_execution_id: None,
                    ..body.clone()
                },
                &row
            ),
            None
        );
        assert!(known.observe(&body, &row).is_some());
        assert_eq!(
            known.permission(&row.session_id, &row.call_id, &row.tool),
            None
        );
        let actual = serde_json::to_value(NativeExecution::new(&row, Some(&known))).unwrap();
        assert_eq!(actual["presentation"], value);
        assert!(
            serde_json::to_value(NativeExecution::new(&row, None))
                .unwrap()
                .get("presentation")
                .is_none()
        );
        let mut messages = [
            json!({"role":"assistant","canonical_source_id":"source","tool_calls":[{"id":"call","execution_id":7}]}),
            json!({"role":"tool","call_id":"call","execution_id":7,"execution_session_id":"session","execution_call_source_id":"source","execution_call_index":0,"position":{"epoch":2,"turn":3,"round":4},"text":"raw"}),
        ];
        known.decorate_messages(&mut messages);
        assert_eq!(messages[0]["tool_calls"][0]["presentation"], value);
        assert_eq!(messages[1]["presentation"], value);
        assert_eq!(messages[1]["text"], "raw");
    }
}

#[test]
fn selected_history_requires_a_certified_result_and_exact_call_slot() {
    let known = Presentations::default();
    known.observe(&authored_image(), &row());
    let call =
        json!({"id":"call","name":"view_image","execution_id":7,"arguments":{"value":"raw"}});
    let result = json!({"role":"tool","call_id":"call","execution_id":7,"execution_session_id":"session","execution_call_source_id":"selected","execution_call_index":1,"position":{"epoch":2,"turn":3,"round":4},"text":"raw result"});
    let mut messages = [
        json!({"role":"assistant","canonical_source_id":"selected","tool_calls":[call.clone(),call.clone()]}),
        json!({"role":"assistant","canonical_source_id":"other","tool_calls":[call.clone(),call.clone()]}),
        json!({"role":"tool","call_id":"call","execution_id":7,"text":"uncertified"}),
        result.clone(),
    ];
    known.decorate_messages(&mut messages);
    assert_eq!(messages[0]["tool_calls"][0], call);
    assert_eq!(messages[0]["tool_calls"][1]["presentation"], "view-image");
    assert_eq!(messages[0]["tool_calls"][1]["arguments"], call["arguments"]);
    assert_eq!(messages[1]["tool_calls"][1], call);
    assert!(messages[2].get("presentation").is_none());
    assert_eq!(messages[3]["presentation"], "view-image");
    assert_eq!(messages[3]["text"], result["text"]);
}
