use super::*;

fn messages() -> Vec<Value> {
    (0..70).flat_map(|index| {
        let id = format!("call-{index}");
        let position = json!({"epoch":0,"turn":1,"round":index+1});
        [json!({"id":format!("a-{index}"),"role":"assistant","text":"","position":position,"tool_calls":[{"id":id,"builtin":if index==0{Some("delegate")}else{None}}]}), json!({"id":format!("r-{index}"),"role":"tool","text":"result","position":position,"call_id":id})]
    }).collect()
}

#[test]
fn page_reports_exact_turn_counts_separately_from_loaded_rows() {
    let all = messages();
    let result = message_page(all, "lineage", 0, None, 32, None, None, None).unwrap();
    assert_eq!(result["items"].as_array().unwrap().len(), 32);
    let witness = &result["history_view"];
    assert_eq!(witness["message_count"], 140);
    assert_eq!(witness["start_cursor"], 108);
    assert_eq!(witness["end_cursor"], 140);
    assert_eq!(witness["turns"][0]["tool_calls"], 70);
    assert_eq!(witness["turns"][0]["ordinary_tool_calls"], 69);
    assert_eq!(witness["turns"][0]["delegate_calls"], 1);
}

#[test]
fn legacy_unpositioned_calls_suppress_authoritative_turn_totals() {
    let mut all = messages();
    all[0].as_object_mut().unwrap().remove("position");
    let page = message_page(all, "lineage", 0, None, 32, None, None, None).unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 32);
    assert!(page["history_view"]["turns"].as_array().unwrap().is_empty());
}

#[test]
fn page_boundary_survives_append_but_rejects_replaced_selection() {
    let mut all = messages();
    all.push(json!({"id":"new-round","role":"assistant","text":"next","tool_calls":[],"position":{"epoch":0,"turn":1,"round":71}}));
    let earlier = message_page(
        all.clone(),
        "lineage",
        0,
        None,
        4,
        Some("lineage"),
        Some("a-60"),
        None,
    )
    .unwrap();
    assert_eq!(earlier["history_view"]["end_cursor"], 120);
    assert_eq!(earlier["items"][0]["id"], "a-58");
    let gap = message_page(
        all.clone(),
        "lineage",
        0,
        None,
        4,
        Some("lineage"),
        None,
        Some("r-4"),
    )
    .unwrap();
    assert_eq!(gap["items"][0]["id"], "a-5");
    assert_eq!(gap["history_view"]["start_cursor"], 10);
    assert_eq!(gap["history_view"]["end_cursor"], 14);
    for (lineage, before) in [("other", "a-60"), ("lineage", "missing")] {
        assert_eq!(
            message_page(
                all.clone(),
                "lineage",
                0,
                None,
                4,
                Some(lineage),
                Some(before),
                None
            )
            .unwrap_err()
            .code,
            "stale_history"
        );
    }
    assert!(message_page(all, "lineage", 0, None, 4, None, Some("a-60"), None).is_err());
}
