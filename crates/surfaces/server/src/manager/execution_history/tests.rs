use super::*;
use microcrab_session::runtime::execution::ExecutionId;

fn rows(tool: &str) -> Vec<ExecutionSnapshot> {
    (1..=3)
        .map(|id| {
            let mut row = ExecutionSnapshot::new(
                "session".into(),
                format!("call-{id}"),
                tool.into(),
                "agent".into(),
                None,
            );
            row.execution_id = ExecutionId(id);
            row
        })
        .collect()
}

fn view(rows: Vec<ExecutionSnapshot>) -> Observation {
    Observation {
        lineage: "lineage".into(),
        epoch: 7,
        dormant: false,
        rows,
        presentations: None,
    }
}

#[test]
fn revision_covers_selection_and_all_ordered_metadata() {
    let first = page("owner", "session", view(rows("tool")), 0, 1, None).unwrap();
    let revision = first["witness"]["rows_revision"].as_str().unwrap();
    let cursor = first["next_cursor"].as_u64().unwrap() as usize;
    let second = page(
        "owner",
        "session",
        view(rows("tool")),
        cursor,
        2,
        Some(revision),
    )
    .unwrap();
    assert_eq!(second["witness"], first["witness"]);
    assert_eq!(second["items"][0]["execution_id"], 2);
    assert!(second["next_cursor"].is_null());
    assert_eq!(
        page("owner", "session", view(rows("tool")), 0, 1, Some(revision)).unwrap(),
        first
    );

    let mut changed = view(rows("tool"));
    changed.rows[2].elapsed_ms = Some(123); // outside the first page
    let mut reordered = view(rows("tool"));
    reordered.rows.swap(1, 2);
    let mut evicted = view(rows("tool"));
    evicted.rows.clear(); // the old, now invalid cursor must still report stale
    let mut other_lineage = view(rows("tool"));
    other_lineage.lineage = "replacement".into();
    let mut other_epoch = view(rows("tool"));
    other_epoch.epoch += 1;
    let mut dormant = view(rows("tool"));
    dormant.dormant = true;
    for observation in [
        changed,
        reordered,
        evicted,
        other_lineage,
        other_epoch,
        dormant,
    ] {
        assert_eq!(
            page("owner", "session", observation, cursor, 2, Some(revision))
                .unwrap_err()
                .code,
            "execution_history_changed"
        );
    }
    for (owner, session) in [("other", "session"), ("owner", "other")] {
        assert_eq!(
            page(
                owner,
                session,
                view(rows("tool")),
                cursor,
                2,
                Some(revision)
            )
            .unwrap_err()
            .code,
            "execution_history_changed"
        );
    }
    assert_eq!(
        page("owner", "session", view(rows("tool")), 4, 2, None)
            .unwrap_err()
            .code,
        "invalid_request"
    );
}

#[test]
fn byte_bound_counts_the_witness_json_escaping_delimiters_and_cursor() {
    let large = "\"".repeat(16 * 1024);
    let first = page("owner", "session", view(rows(&large)), 0, 64, None).unwrap();
    assert_eq!(first["items"].as_array().unwrap().len(), 1);
    assert_eq!(first["next_cursor"], 1);
    let revision = first["witness"]["rows_revision"].as_str().unwrap();
    let final_page = page(
        "owner",
        "session",
        view(rows(&large)),
        2,
        64,
        Some(revision),
    )
    .unwrap();
    assert!(final_page["next_cursor"].is_null());
    for value in [first, final_page] {
        assert!(serde_json::to_vec(&value).unwrap().len() <= MAX_PAGE_BYTES);
    }

    let mut exact = view(rows(""));
    exact.rows.truncate(1);
    let empty = page("owner", "session", view(exact.rows.clone()), 0, 64, None).unwrap();
    let overhead = serde_json::to_vec(&empty).unwrap().len();
    exact.rows[0].tool = "x".repeat(MAX_PAGE_BYTES - overhead);
    let full = page("owner", "session", view(exact.rows.clone()), 0, 64, None).unwrap();
    assert_eq!(serde_json::to_vec(&full).unwrap().len(), MAX_PAGE_BYTES);
    exact.rows[0].tool.push('x');
    let oversized = page("owner", "session", exact, 0, 64, None).unwrap_err();
    assert_eq!(oversized.code, "execution_metadata_too_large");
}
