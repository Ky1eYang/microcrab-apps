use super::*;

#[test]
fn only_positive_date_compatible_json_integers_are_completion_times() {
    assert_eq!(completion_millis(&json!(1)), Some(1));
    assert_eq!(
        completion_millis(&json!(JS_DATE_LIMIT)),
        Some(JS_DATE_LIMIT)
    );
    for invalid in [
        json!(0),
        json!(-1),
        json!(1.0),
        json!("1"),
        json!(JS_DATE_LIMIT + 1),
        Value::Null,
    ] {
        assert_eq!(completion_millis(&invalid), None);
    }
}

#[cfg(feature = "storage-file")]
#[test]
fn fallback_requires_exact_producer_complete_selected_audit_and_unique_canonical_row() {
    use microcrab_creator::audit::{EffectiveRequest, Recorder, RecorderOptions};
    let directory = tempfile::tempdir().unwrap();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let mut terminal = |call: &str, parent_request, elapsed| {
        let head = recorder
            .effective(EffectiveRequest {
                call_id: call.into(),
                epoch: 1,
                turn: Some(1),
                round: Some(1),
                parent_request,
                history_head: None,
                subagent: None,
                request_body: "same".into(),
            })
            .unwrap();
        recorder
            .publish(json!({"event":"request_prepared","call_id":call,"started_at_unix_ms":1000}))
            .unwrap();
        recorder.publish(json!({"event":"round_completed","call_id":call,"outcome":"accepted","started_at_unix_ms":1000,"elapsed_ms":elapsed})).unwrap();
        head
    };
    let head = terminal("selected", None, 53);
    let sibling = terminal("sibling", None, 77);
    let ambiguous = terminal("ambiguous", Some(head.clone()), 93);
    let evidence = LegacyCompletions(
        HashMap::from([(
            "source".into(),
            LegacyProducer {
                producer: "selected".into(),
                request: head.clone(),
                native: "native".into(),
                position: (1, 1, 1),
            },
        )]),
        vec![(1, 1, 1)],
    );
    let row = json!({"role":"assistant","id":"native","canonical_source_id":"source","position":{"epoch":1,"turn":1,"round":1}});
    let selected = query_selected_attempt_metadata(directory.path(), &head, &[(1, 1, 1)]).unwrap();
    let mut page = vec![row.clone()];
    project(&evidence, &selected, &page.clone(), &mut page);
    assert_eq!(page[0][FIELD], 1053);
    for unproven in [sibling, ambiguous] {
        let result =
            query_selected_attempt_metadata(directory.path(), &unproven, &[(1, 1, 1)]).unwrap();
        let mut page = vec![row.clone()];
        project(&evidence, &result, std::slice::from_ref(&row), &mut page);
        assert!(page[0].get(FIELD).is_none());
    }
    let mut page = vec![row.clone()];
    project(&evidence, &selected, &[row.clone(), row.clone()], &mut page);
    assert!(page[0].get(FIELD).is_none());
    let mut incomplete = selected.clone();
    incomplete.selected_ancestry_complete = false;
    project(
        &evidence,
        &incomplete,
        std::slice::from_ref(&row),
        &mut page,
    );
    assert!(page[0].get(FIELD).is_none());
    let mut truncated = selected;
    truncated.incomplete_tail = true;
    project(&evidence, &truncated, std::slice::from_ref(&row), &mut page);
    assert!(page[0].get(FIELD).is_none());
}
