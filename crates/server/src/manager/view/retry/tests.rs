use super::*;

const FIRST: &str = "00000000-0000-4000-8000-000000000001";
const SECOND: &str = "00000000-0000-4000-8000-000000000002";
const THIRD: &str = "00000000-0000-4000-8000-000000000003";

fn failure(id: &str, method: &str, time: u64) -> OperationRecord {
    serde_json::from_value(json!({
        "version":1,"id":id,"owner":"owner","session_id":"session",
        "method":method,"payload_hash":"unrelated","status":"failed",
        "revision":1,"created_at_ms":time,"updated_at_ms":time,
        "result":{"error":{"code":"transport"},"retry_ready":true,
            "retry_witness":"a".repeat(64),"after_message_id":"anchor"},
    }))
    .unwrap()
}
fn legacy_retry(id: &str, previous: &str, time: u64) -> OperationRecord {
    let mut retry = failure(id, "turn.retry", time);
    let command: Command = serde_json::from_value(json!({
        "v":1,"type":"command","request_id":id,"method":"turn.retry",
        "session_id":"session","params":{"operation_id":id,"failed_operation_id":previous},
    }))
    .unwrap();
    retry.payload_hash = super::super::super::commands::hash(&command).unwrap();
    retry
}
fn messages() -> Vec<Value> {
    vec![json!({"id":"anchor"})]
}

#[test]
fn legacy_chain_requires_exact_command_hash_and_only_mutates_projection() {
    let stored = vec![
        failure(FIRST, "turn.submit", 1),
        legacy_retry(SECOND, FIRST, 2),
        legacy_retry(THIRD, SECOND, 3),
    ];
    let original = serde_json::to_value(&stored).unwrap();
    let mut projected = stored.clone();
    assert_eq!(
        retry_supersession(&mut projected, &messages()).unwrap(),
        vec![FIRST, SECOND]
    );
    assert_eq!(
        projected[1].result.as_ref().unwrap()["retry_of_operation_id"],
        FIRST
    );
    assert_eq!(
        projected[2].result.as_ref().unwrap()["retry_of_operation_id"],
        SECOND
    );
    assert_eq!(
        projected[2].result.as_ref().unwrap()["retry_after_message_id"],
        "anchor"
    );
    assert_eq!(serde_json::to_value(stored).unwrap(), original);
}

#[test]
fn same_round_or_hash_does_not_override_owner_witness_anchor_or_order() {
    let base = vec![
        failure(FIRST, "turn.submit", 1),
        legacy_retry(SECOND, FIRST, 2),
    ];
    for case in 0..8 {
        let mut operations = base.clone();
        match case {
            0 => operations[1].payload_hash = "unrelated".into(),
            1 => operations[1].method = "turn.submit".into(),
            2 => operations[0].owner = "another".into(),
            3 => operations[0].session_id = "another".into(),
            4 => operations[0].result.as_mut().unwrap()["retry_witness"] = json!("b".repeat(64)),
            5 => operations[0].result.as_mut().unwrap()["after_message_id"] = json!("other-anchor"),
            6 => operations[0].created_at_ms = operations[1].created_at_ms,
            7 => operations[1].result.as_mut().unwrap()["retry_ready"] = json!(false),
            _ => unreachable!(),
        }
        assert!(
            retry_supersession(&mut operations, &messages())
                .unwrap()
                .is_empty(),
            "case {case}"
        );
        assert!(
            operations[1]
                .result
                .as_ref()
                .unwrap()
                .get("retry_of_operation_id")
                .is_none()
        );
    }
}

#[test]
fn legacy_retry_requires_a_complete_witness_and_a_selected_message() {
    for bad_witness in ["a".repeat(63), "z".repeat(64)] {
        let mut operations = vec![
            failure(FIRST, "turn.submit", 1),
            legacy_retry(SECOND, FIRST, 2),
        ];
        for operation in &mut operations {
            operation.result.as_mut().unwrap()["retry_witness"] = json!(bad_witness);
        }
        assert!(
            retry_supersession(&mut operations, &messages())
                .unwrap()
                .is_empty()
        );
    }
    let mut operations = vec![
        failure(FIRST, "turn.submit", 1),
        legacy_retry(SECOND, FIRST, 2),
    ];
    assert!(retry_supersession(&mut operations, &[]).unwrap().is_empty());
}

#[test]
fn explicit_admission_links_validate_the_target_without_a_failed_retry_outcome() {
    for status in [
        "accepted",
        "running",
        "completed",
        "failed",
        "cancelled",
        "interrupted",
    ] {
        let mut operations = vec![
            failure(FIRST, "turn.inbox", 1),
            failure(SECOND, "turn.retry", 2),
        ];
        operations[1].status = status.into();
        operations[1].result =
            Some(json!({"retry_of_operation_id":FIRST,"retry_after_message_id":"anchor"}));
        assert_eq!(
            retry_supersession(&mut operations, &messages()).unwrap(),
            vec![FIRST]
        );
    }
    for case in 0..5 {
        let mut operations = vec![
            failure(FIRST, "turn.submit", 1),
            failure(SECOND, "turn.retry", 2),
        ];
        operations[1].result =
            Some(json!({"retry_of_operation_id":FIRST,"retry_after_message_id":"anchor"}));
        match case {
            0 => operations[0].owner = "another".into(),
            1 => operations[0].status = "completed".into(),
            2 => operations[0].result.as_mut().unwrap()["after_message_id"] = json!("different"),
            3 => operations[1].method = "turn.submit".into(),
            4 => operations[1].result.as_mut().unwrap()["retry_of_operation_id"] = json!(THIRD),
            _ => unreachable!(),
        }
        assert!(
            retry_supersession(&mut operations, &messages())
                .unwrap()
                .is_empty()
        );
        assert!(
            operations[1]
                .result
                .as_ref()
                .unwrap()
                .get("retry_of_operation_id")
                .is_none()
        );
    }
}

#[test]
fn unknown_retry_statuses_never_supersede_or_expose_relationship_fields() {
    for status in [
        "unknown",
        "",
        "accepted ",
        "FAILED",
        " completed",
        "failed\n",
    ] {
        let mut operations = vec![
            failure(FIRST, "turn.submit", 1),
            failure(SECOND, "turn.retry", 2),
        ];
        operations[1].status = status.into();
        operations[1].result = Some(json!({
            "retry_of_operation_id":FIRST,
            "retry_after_message_id":"anchor",
        }));
        assert!(
            retry_supersession(&mut operations, &messages())
                .unwrap()
                .is_empty(),
            "status {status:?}"
        );
        let result = operations[1].result.as_ref().unwrap();
        assert!(result.get("retry_of_operation_id").is_none());
        assert!(result.get("retry_after_message_id").is_none());
    }
}

#[test]
fn superseded_set_survives_the_link_receipt_falling_outside_the_operation_page() {
    let mut operations = vec![
        failure(FIRST, "turn.submit", 1),
        legacy_retry(SECOND, FIRST, 2),
    ];
    for time in 3..40 {
        let mut unrelated = failure(&format!("metadata-{time}"), "session.rename", time);
        unrelated.status = "completed".into();
        operations.push(unrelated);
    }
    let superseded = retry_supersession(&mut operations, &messages()).unwrap();
    let (page, truncated) = super::super::operation_page(operations, None);
    assert!(truncated);
    assert_eq!(page.len(), 32);
    assert!(page.iter().all(|operation| operation.id != SECOND));
    let snapshot = super::super::bound_snapshot(json!({
        "operations":page,"superseded_failure_operation_ids":superseded,
    }))
    .unwrap();
    assert_eq!(snapshot["superseded_failure_operation_ids"], json!([FIRST]));
}
