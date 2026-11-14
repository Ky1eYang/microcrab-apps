use super::*;
use microcrab_creator::audit::query_selected_turn_metadata;
use microcrab_creator::audit::{EffectiveRequest, Recorder, RecorderOptions};
use serde_json::json;
mod turn_usage;

fn project(messages: &mut [Value], attempts: &[AuditAttempt]) {
    let canonical = messages.to_vec();
    super::project(messages, attempts, &canonical);
}

#[allow(clippy::too_many_arguments)]
fn record(
    recorder: &mut Recorder,
    call: &str,
    parent: Option<String>,
    epoch: u64,
    round: u64,
    subagent: Option<&str>,
    outcome: &str,
    usage: Value,
) -> String {
    let hash = recorder
        .effective(EffectiveRequest {
            call_id: call.into(),
            epoch,
            turn: Some(1),
            round: Some(round),
            parent_request: parent,
            history_head: None,
            subagent: subagent.map(str::to_owned),
            request_body: "private provider request".into(),
        })
        .unwrap();
    recorder
        .publish(
            json!({"event":"request_prepared","call_id":call,"sent_request":"private wire body"}),
        )
        .unwrap();
    recorder.publish(json!({"event":"round_completed","call_id":call,"outcome":outcome,"usage":usage,"response":{"content":"private answer"}})).unwrap();
    hash
}
fn message(epoch: u64, round: u64) -> Value {
    json!({"role":"assistant","position":{"epoch":epoch,"turn":1,"round":round}})
}

#[test]
fn selected_lineage_usage_preserves_unknown_zero_and_excludes_siblings_helpers_and_failures() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let root = record(
        &mut recorder,
        "root",
        None,
        1,
        1,
        None,
        "accepted",
        json!({"input_tokens":10,"output_tokens":0,"samples":1}),
    );
    let selected = record(
        &mut recorder,
        "selected",
        Some(root.clone()),
        2,
        1,
        None,
        "accepted",
        json!({"input_tokens":20,"output_tokens":3,"total_tokens":null,"samples":1}),
    );
    record(
        &mut recorder,
        "sibling",
        Some(root),
        2,
        1,
        None,
        "accepted",
        json!({"input_tokens":999}),
    );
    record(
        &mut recorder,
        "helper",
        Some(selected.clone()),
        2,
        1,
        Some("action_review"),
        "accepted",
        json!({"input_tokens":888}),
    );
    let failed = record(
        &mut recorder,
        "failed",
        Some(selected),
        2,
        2,
        None,
        "rejected",
        json!({"output_tokens":777}),
    );
    let result = query_selected_attempt_metadata(
        directory.path(),
        &failed,
        &[(1, 1, 1), (2, 1, 1), (2, 1, 2)],
    )
    .unwrap();
    assert_eq!(result.attempts.len(), 2);
    assert!(
        result
            .attempts
            .iter()
            .all(|attempt| attempt.sent_request.is_none()
                && attempt.response.is_none()
                && attempt.request_document.is_none())
    );
    let mut messages = vec![
        message(1, 1),
        message(2, 1),
        message(2, 2),
        json!({"role":"assistant","position":null}),
    ];
    project(&mut messages, &result.attempts);
    assert_eq!(messages[0]["usage"]["input_tokens"], 10);
    assert_eq!(messages[0]["usage"]["output_tokens"], 0);
    assert!(messages[0]["usage"]["total_tokens"].is_null());
    assert_eq!(messages[1]["usage"]["output_tokens"], 3);
    assert!(messages[1]["usage"]["total_tokens"].is_null());
    assert!(messages[2]["usage"].is_null());
    assert!(messages[3]["usage"].is_null());
}

#[test]
fn associated_output_keeps_its_reported_total_without_earlier_attempts_or_cache_addition() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let first = record(
        &mut recorder,
        "tool-round",
        None,
        1,
        1,
        None,
        "accepted",
        json!({"input_tokens":10,"output_tokens":3,"total_tokens":13}),
    );
    let rejected = record(
        &mut recorder,
        "rejected-attempt",
        Some(first),
        1,
        2,
        None,
        "rejected",
        json!({"input_tokens":700,"output_tokens":77,"total_tokens":777}),
    );
    let accepted = record(
        &mut recorder,
        "accepted-output",
        Some(rejected),
        1,
        2,
        None,
        "accepted",
        json!({"input_tokens":20,"output_tokens":4,"total_tokens":24,
            "cache_read_tokens":18,"cache_write_tokens":1,"reasoning_tokens":2}),
    );
    let helper = record(
        &mut recorder,
        "helper",
        Some(accepted),
        1,
        2,
        Some("action_review"),
        "accepted",
        json!({"input_tokens":900,"output_tokens":99,"total_tokens":999}),
    );
    let attempts =
        query_selected_attempt_metadata(directory.path(), &helper, &[(1, 1, 1), (1, 1, 2)])
            .unwrap()
            .attempts;
    let mut messages = vec![message(1, 1), message(1, 2)];
    project(&mut messages, &attempts);
    assert_eq!(messages[0]["usage"]["total_tokens"], 13);
    assert_eq!(messages[1]["usage"]["total_tokens"], 24);
    assert_eq!(messages[1]["usage"]["cache_read_tokens"], 18);
    assert_eq!(messages[1]["usage"]["cache_write_tokens"], 1);
    assert_eq!(messages[1]["usage"]["reasoning_tokens"], 2);
}

#[test]
fn duplicate_positions_or_attempt_identities_and_malformed_usage_stay_unknown() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let root = record(
        &mut recorder,
        "root",
        None,
        1,
        1,
        None,
        "accepted",
        json!({"input_tokens":10}),
    );
    let selected = record(
        &mut recorder,
        "selected",
        Some(root),
        1,
        2,
        None,
        "accepted",
        json!({"input_tokens":20}),
    );
    let mut attempts =
        query_selected_attempt_metadata(directory.path(), &selected, &[(1, 1, 1), (1, 1, 2)])
            .unwrap()
            .attempts;
    let mut messages = vec![message(1, 1), message(1, 1), message(1, 2)];
    project(&mut messages, &attempts);
    assert!(messages[0]["usage"].is_null());
    assert!(messages[1]["usage"].is_null());
    assert_eq!(messages[2]["usage"]["input_tokens"], 20);
    attempts[1].round = Some(1);
    let mut messages = vec![message(1, 1)];
    project(&mut messages, &attempts);
    assert!(messages[0]["usage"].is_null());
    attempts[1].round = Some(2);
    attempts[1].call_id = attempts[0].call_id.clone();
    let mut messages = vec![message(1, 1), message(1, 2)];
    project(&mut messages, &attempts);
    assert!(messages.iter().all(|message| message["usage"].is_null()));
    attempts.truncate(1);
    attempts[0].usage = Some(json!({"input_tokens":-1}));
    project(&mut messages, &attempts);
    assert!(messages[0]["usage"].is_null());
}

#[test]
fn long_lineage_keeps_latest_and_old_page_usage_independently_bounded() {
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let mut head = None;
    for round in 1..=300 {
        head = Some(record(
            &mut recorder,
            &format!("call-{round}"),
            head,
            1,
            round,
            None,
            "accepted",
            json!({"input_tokens":round,"output_tokens":0}),
        ));
    }
    let head = head.unwrap();
    let latest = query_selected_attempt_metadata(directory.path(), &head, &[(1, 1, 300)]).unwrap();
    assert_eq!(latest.attempts.len(), 1);
    let canonical: Vec<_> = (1..=300).map(|round| message(1, round)).collect();
    let mut page = vec![message(1, 300)];
    super::project(&mut page, &latest.attempts, &canonical);
    assert_eq!(page[0]["usage"]["input_tokens"], 300);
    assert_eq!(page[0]["usage"]["output_tokens"], 0);
    let older = query_selected_attempt_metadata(directory.path(), &head, &[(1, 1, 10)]).unwrap();
    assert_eq!(older.attempts.len(), 1);
    let mut page = vec![message(1, 10)];
    super::project(&mut page, &older.attempts, &canonical);
    assert_eq!(page[0]["usage"]["input_tokens"], 10);
    // An off-page Context duplicate still makes the visible position unknown.
    let mut duplicated = canonical.clone();
    duplicated.push(message(1, 10));
    super::project(&mut page, &older.attempts, &duplicated);
    assert!(page[0]["usage"].is_null());
    // Off-page call identity reuse invalidates only its corresponding position.
    let head = record(
        &mut recorder,
        "call-10",
        Some(head),
        1,
        301,
        None,
        "accepted",
        json!({"input_tokens":999}),
    );
    let filtered =
        query_selected_attempt_metadata(directory.path(), &head, &[(1, 1, 10), (1, 1, 300)])
            .unwrap();
    assert_eq!(filtered.attempts.len(), 1);
    assert_eq!(filtered.attempts[0].round, Some(300));
    // Even many accepted duplicates at one requested position cannot hide
    // another unambiguous requested position or exhaust the response cap.
    let mut head = Some(head);
    for index in 0..260 {
        head = Some(record(
            &mut recorder,
            &format!("duplicate-{index}"),
            head,
            1,
            20,
            None,
            "accepted",
            json!({"input_tokens":999}),
        ));
    }
    let filtered = query_selected_attempt_metadata(
        directory.path(),
        &head.unwrap(),
        &[(1, 1, 20), (1, 1, 300)],
    )
    .unwrap();
    assert_eq!(filtered.attempts.len(), 1);
    assert_eq!(filtered.attempts[0].round, Some(300));
}
