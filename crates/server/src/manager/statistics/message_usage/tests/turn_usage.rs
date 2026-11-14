mod compaction;
use super::*;

fn message(epoch: u64, round: u64) -> Value {
    let mut message = super::message(epoch, round);
    message["id"] = json!(format!("assistant-{epoch}-{round}"));
    message
}

fn turns() -> Vec<Value> {
    vec![json!({"epoch":1,"turn":1})]
}

fn usage(canonical: &[Value], result: &AuditQueryResult) -> Value {
    let mut turns = turns();
    super::super::project_turns(&mut turns, canonical, result);
    turns.remove(0)["usage"].clone()
}

#[test]
fn whole_turn_counts_accepted_invocations_once_and_keeps_literal_first_input() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let first = record(
        &mut recorder,
        "failed-first",
        None,
        1,
        1,
        None,
        "rejected",
        json!({"input_tokens":11,"output_tokens":900}),
    );
    let first = record(
        &mut recorder,
        "accepted-first",
        Some(first),
        1,
        1,
        None,
        "accepted",
        json!({"input_tokens":22,"output_tokens":3,"total_tokens":25}),
    );
    let second = record(
        &mut recorder,
        "accepted-second",
        Some(first.clone()),
        1,
        2,
        None,
        "accepted",
        json!({"input_tokens":500,"output_tokens":7,"total_tokens":507}),
    );
    record(
        &mut recorder,
        "off-branch",
        Some(first),
        1,
        2,
        None,
        "accepted",
        json!({"input_tokens":999,"output_tokens":999}),
    );
    record(
        &mut recorder,
        "helper",
        Some(second.clone()),
        1,
        2,
        Some("action_review"),
        "accepted",
        json!({"input_tokens":999,"output_tokens":999}),
    );
    let pending = recorder
        .effective(EffectiveRequest {
            call_id: "pending".into(),
            epoch: 1,
            turn: Some(1),
            round: Some(3),
            parent_request: Some(second),
            history_head: None,
            subagent: None,
            request_body: "pending".into(),
        })
        .unwrap();
    let result = query_selected_turn_metadata(directory.path(), &pending, &[(1, 1)]).unwrap();
    let mut first = message(1, 1);
    first["id"] = json!("canonical-first");
    let canonical = vec![first.clone(), first.clone(), first, message(1, 2)];
    let projected = usage(&canonical, &result);
    assert_eq!(projected["input_tokens"], 11);
    assert_eq!(projected["output_tokens"], 10);
    assert_eq!(projected["audit_sequence"], result.committed_sequence);
    assert!(projected.get("total_tokens").is_none());
    assert!(projected.get("cache_read_tokens").is_none());
    // A fragment in an unmatched round cannot be called a complete Turn sum.
    let mut uncovered = canonical.clone();
    uncovered.push(message(1, 3));
    assert!(usage(&uncovered, &result)["output_tokens"].is_null());
    assert!(usage(&canonical[..3], &result)["output_tokens"].is_null());
}

#[test]
fn unknown_first_input_cannot_borrow_retry_usage_and_unknown_output_clears_sum() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let first = record(
        &mut recorder,
        "unknown-first",
        None,
        1,
        1,
        None,
        "rejected",
        Value::Null,
    );
    let first = record(
        &mut recorder,
        "retry",
        Some(first),
        1,
        1,
        None,
        "accepted",
        json!({"input_tokens":100,"output_tokens":3}),
    );
    let head = record(
        &mut recorder,
        "next-round",
        Some(first),
        1,
        2,
        None,
        "accepted",
        json!({"input_tokens":200,"output_tokens":4}),
    );
    let mut result = query_selected_turn_metadata(directory.path(), &head, &[(1, 1)]).unwrap();
    let canonical = vec![message(1, 1), message(1, 2)];
    let projected = usage(&canonical, &result);
    assert!(projected["input_tokens"].is_null());
    assert_eq!(projected["output_tokens"], 7);
    result.attempts.last_mut().unwrap().usage = None;
    assert!(usage(&canonical, &result)["output_tokens"].is_null());
    result.attempts.last_mut().unwrap().usage = Some(json!({"output_tokens":-1}));
    assert!(usage(&canonical, &result)["output_tokens"].is_null());
    result.attempts.last_mut().unwrap().usage = Some(json!({"output_tokens":u64::MAX}));
    assert!(usage(&canonical, &result)["output_tokens"].is_null());
    for attempt in &mut result.attempts {
        attempt.usage = Some(json!({"input_tokens":0,"output_tokens":0}));
    }
    let projected = usage(&canonical, &result);
    assert_eq!(projected["input_tokens"], 0);
    assert_eq!(projected["output_tokens"], 0);
}

#[test]
fn missing_ancestry_duplicate_accepted_positions_and_conflicting_canonical_identity_stay_unknown() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let head = record(
        &mut recorder,
        "suffix",
        Some("a".repeat(64)),
        1,
        1,
        None,
        "accepted",
        json!({"input_tokens":10,"output_tokens":3}),
    );
    let mut result = query_selected_turn_metadata(directory.path(), &head, &[(1, 1)]).unwrap();
    let projected = usage(&[message(1, 1)], &result);
    assert!(projected["input_tokens"].is_null());
    assert!(projected["output_tokens"].is_null());
    assert_eq!(projected["audit_sequence"], result.committed_sequence);
    result.selected_ancestry_complete = true;
    let mut duplicate = result.attempts[0].clone();
    duplicate.call_id = "distinct-call".into();
    duplicate.request_hash = "b".repeat(64);
    result.attempts.push(duplicate);
    assert!(usage(&[message(1, 1)], &result)["output_tokens"].is_null());
    result.attempts.pop();
    let mut first = message(1, 1);
    first["id"] = json!("same-id");
    let mut conflict = first.clone();
    conflict["text"] = json!("conflicting canonical reference");
    assert!(usage(&[first, conflict], &result)["output_tokens"].is_null());
    result.selected_identities_complete = false;
    let projected = usage(&[message(1, 1)], &result);
    assert!(projected["input_tokens"].is_null());
    assert!(projected["output_tokens"].is_null());
}

#[test]
fn unsafe_json_integer_input_and_sum_are_unknown_without_u64_overflow() {
    const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let first = record(
        &mut recorder,
        "first",
        None,
        1,
        1,
        None,
        "accepted",
        json!({"input_tokens":MAX_SAFE_INTEGER,"output_tokens":MAX_SAFE_INTEGER-1}),
    );
    let head = record(
        &mut recorder,
        "second",
        Some(first),
        1,
        2,
        None,
        "accepted",
        json!({"input_tokens":20,"output_tokens":1}),
    );
    let mut result = query_selected_turn_metadata(directory.path(), &head, &[(1, 1)]).unwrap();
    let canonical = vec![message(1, 1), message(1, 2)];
    let projected = usage(&canonical, &result);
    assert_eq!(projected["input_tokens"], MAX_SAFE_INTEGER);
    assert_eq!(projected["output_tokens"], MAX_SAFE_INTEGER);
    result.attempts[0].usage =
        Some(json!({"input_tokens":MAX_SAFE_INTEGER+1,"output_tokens":MAX_SAFE_INTEGER-1}));
    result.attempts[1].usage = Some(json!({"output_tokens":2}));
    let projected = usage(&canonical, &result);
    assert!(projected["input_tokens"].is_null());
    assert!(projected["output_tokens"].is_null());
}

#[test]
fn identifiable_canonical_conflicts_only_invalidate_the_involved_turns() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let head = record(
        &mut recorder,
        "first",
        None,
        1,
        1,
        None,
        "accepted",
        json!({"input_tokens":10,"output_tokens":3}),
    );
    let mut result = query_selected_turn_metadata(directory.path(), &head, &[(1, 1)]).unwrap();
    for turn in 2..=3 {
        let mut attempt = result.attempts[0].clone();
        attempt.turn = Some(turn);
        attempt.call_id = format!("call-{turn}");
        attempt.request_hash = format!("request-{turn}");
        result.attempts.push(attempt);
    }
    let mut canonical = vec![];
    for turn in 1..=3 {
        let mut message = message(1, 1);
        message["position"]["turn"] = json!(turn);
        message["id"] = json!(format!("canonical-{turn}"));
        canonical.push(message);
    }
    let project = |canonical: &[Value]| {
        let mut turns: Vec<_> = (1..=3).map(|turn| json!({"epoch":1,"turn":turn})).collect();
        super::super::project_turns(&mut turns, canonical, &result);
        turns
    };
    let mut conflict = canonical[1].clone();
    conflict["id"] = json!("distinct-canonical-at-same-position");
    conflict["text"] = json!("different assistant body");
    let mut different_identity = canonical.clone();
    different_identity.push(conflict);
    let turns = project(&different_identity);
    assert_eq!(turns[0]["usage"]["output_tokens"], 3);
    assert!(turns[1]["usage"]["output_tokens"].is_null());
    assert_eq!(turns[2]["usage"]["output_tokens"], 3);
    let mut missing_identity = canonical.clone();
    missing_identity[1].as_object_mut().unwrap().remove("id");
    let turns = project(&missing_identity);
    assert_eq!(turns[0]["usage"]["output_tokens"], 3);
    assert!(turns[1]["usage"]["output_tokens"].is_null());
    assert_eq!(turns[2]["usage"]["output_tokens"], 3);
    let mut reused_identity = canonical.clone();
    reused_identity[1]["id"] = canonical[0]["id"].clone();
    let turns = project(&reused_identity);
    assert!(turns[0]["usage"]["output_tokens"].is_null());
    assert!(turns[1]["usage"]["output_tokens"].is_null());
    assert_eq!(turns[2]["usage"]["output_tokens"], 3);
    let mut unpositioned = canonical;
    unpositioned[1].as_object_mut().unwrap().remove("position");
    assert!(
        project(&unpositioned)
            .iter()
            .all(|turn| turn["usage"]["output_tokens"].is_null())
    );
}

#[tokio::test]
async fn bounded_history_page_enriches_full_compacted_turn_and_active_scope() {
    let directory = tempfile::tempdir().unwrap();
    let config: crate::config::Config = serde_json::from_value(json!({
        "storage":"file","data_dir":directory.path(),
        "provider":{"endpoint":"http://127.0.0.1:1/chat/completions","model":"test"},
        "auth":{"issuer":"http://127.0.0.1","authorization_endpoint":"http://127.0.0.1/authorize",
            "token_endpoint":"http://127.0.0.1/token","jwks_uri":"http://127.0.0.1/jwks",
            "client_id":"test","redirect_uri":"http://127.0.0.1/callback",
            "allowed_origins":["http://127.0.0.1"],"jwt_secret":"test-key-at-least-thirty-two-characters"}
    })).unwrap();
    let mut statistics = Statistics::new(&config, "test-turn");
    statistics.directory = directory.path().to_path_buf();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let mut head = None;
    let mut canonical = vec![];
    for round in 1..=300 {
        head = Some(record(
            &mut recorder,
            &format!("round-{round}"),
            head,
            1,
            round,
            None,
            "accepted",
            json!({"input_tokens":round+100,"output_tokens":1}),
        ));
        let mut message = message(1, round);
        message["id"] = json!(format!("assistant-{round}"));
        message["historical"] = json!(round < 250);
        canonical.push(message);
    }
    let mut page = crate::manager::view::message_page(
        canonical.clone(),
        "selected",
        1,
        None,
        1,
        None,
        None,
        None,
    )
    .unwrap();
    statistics
        .turn_usage(head.clone(), &[], &canonical, &mut page["history_view"])
        .await;
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        page["history_view"]["turns"][0]["usage"]["input_tokens"],
        101
    );
    assert_eq!(
        page["history_view"]["turns"][0]["usage"]["output_tokens"],
        300
    );
    let empty_page = json!({"items":[],"end_cursor":300});
    let mut active =
        crate::manager::view::history_view(&canonical, &empty_page, "selected", 1, Some((1, 1)));
    statistics
        .turn_usage(head, &[], &canonical, &mut active)
        .await;
    assert_eq!(active["turns"][0]["usage"]["output_tokens"], 300);
    statistics
        .turn_usage(None, &[], &canonical, &mut active)
        .await;
    assert!(active["turns"][0]["usage"]["output_tokens"].is_null());
    assert!(active["turns"][0]["usage"]["audit_sequence"].is_null());
}
