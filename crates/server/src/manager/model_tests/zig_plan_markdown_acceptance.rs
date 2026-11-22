//! Opt-in PLAN acceptance using the composed Zig Coding candidate, real LocalHost,
//! and a scripted HTTP provider on loopback. Set `MICROCRAB_TEST_ZIG_PLAN_COMPONENT`
//! to the absolute path of the candidate, then run the ignored test by name.

use super::*;
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

const CANDIDATE: &str = "MICROCRAB_TEST_ZIG_PLAN_COMPONENT";
const PLAN: &str = "design.md";
const EXISTING: &str = "existing.md";
const NON_MARKDOWN: &str = "ordinary.txt";
const EXISTING_BODY: &str = "# Pre-existing file\n\nDo not overwrite.\n";
const BODY: &str = "# Review steps\n\nInspect the fixture.\n\n- First draft\n";
const EDITED: &str = "# Review steps\n\nInspect the fixture.\n\n- Revised draft\n";

#[derive(Clone, Copy)]
enum Step {
    Create,
    Existing,
    VersionedWrite,
    NonMarkdown,
    Edit,
    StaleEdit,
    ForeignEdit,
    Patch,
}

const STEPS: [Step; 8] = [
    Step::Create,
    Step::Existing,
    Step::VersionedWrite,
    Step::NonMarkdown,
    Step::Edit,
    Step::StaleEdit,
    Step::ForeignEdit,
    Step::Patch,
];

impl Step {
    fn names(self) -> (&'static str, &'static str) {
        match self {
            Self::Create => ("create-plan", "write"),
            Self::Existing => ("existing-plan-path", "write"),
            Self::VersionedWrite => ("versioned-plan-write", "write"),
            Self::NonMarkdown => ("non-markdown-write", "write"),
            Self::Edit => ("edit-plan-body", "edit"),
            Self::StaleEdit => ("stale-plan-edit", "edit"),
            Self::ForeignEdit => ("foreign-plan-edit", "edit"),
            Self::Patch => ("patch-plan", "patch"),
        }
    }

    fn arguments(self, workspace: &Path, created_version: &str) -> Value {
        let current_version = || digest(&fs::read(workspace.join(PLAN)).expect("saved PLAN bytes"));
        match self {
            Self::Create => json!({"path": PLAN, "content": BODY}),
            Self::Existing => json!({"path": EXISTING, "content": BODY}),
            Self::VersionedWrite => json!({
                "path": EXISTING, "content": BODY,
                "expected_version": digest(EXISTING_BODY.as_bytes())
            }),
            Self::NonMarkdown => json!({"path": NON_MARKDOWN, "content": BODY}),
            Self::Edit => json!({
                "path": PLAN, "expected_version": current_version(),
                "old_string": "- First draft", "new_string": "- Revised draft"
            }),
            Self::StaleEdit | Self::ForeignEdit => json!({
                "path": PLAN,
                "expected_version": if matches!(self, Self::StaleEdit) {
                    created_version.to_owned()
                } else {
                    current_version()
                },
                "old_string": "- Revised draft", "new_string": "- Must not change"
            }),
            Self::Patch => json!({
                "path": PLAN, "expected_version": current_version(),
                "replacements": [{"old_string": "- Revised draft", "new_string": "- Patched"}]
            }),
        }
    }
}

fn utc_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn assert_plan(text: &str, session_id: &str, body: &str, earliest: u64, latest: u64) -> u64 {
    let prefix = format!(
        "---\ntitle: \"Review steps\"\ndescription: \"Inspect the fixture.\"\nsession_id: {}\nedited_at_unix_ms: \"",
        json!(session_id)
    );
    let (timestamp, actual_body) = text
        .strip_prefix(&prefix)
        .expect("exact Coding header, including the full Session ID")
        .split_once("\"\n---\n")
        .expect("quoted timestamp and exact header terminator");
    let milliseconds = timestamp.parse::<u64>().expect("Unix milliseconds");
    assert_eq!(timestamp, milliseconds.to_string(), "canonical decimal");
    assert!(
        (earliest..=latest).contains(&milliseconds),
        "invocation timestamp {milliseconds} is outside the observed UTC bounds {earliest}..={latest}"
    );
    assert_eq!(actual_body, body, "Markdown body bytes");
    milliseconds
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires MICROCRAB_TEST_ZIG_PLAN_COMPONENT and a real LocalHost"]
async fn zig_plan_markdown_candidate_enforces_session_cas_and_ask() {
    let (candidate, candidate_digest) = component(CANDIDATE);
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    fs::create_dir(&workspace).unwrap();
    fs::write(workspace.join(EXISTING), EXISTING_BODY).unwrap();
    let provider = indexed_provider(STEPS.len() * 2, {
        let workspace = workspace.clone();
        let mut_version = Arc::new(Mutex::new(String::new()));
        // Record the real create version immediately before issuing the edit so
        // the stale-CAS case can deliberately reuse it after the edit commits.
        move |index| {
            let step = *STEPS.get(index / 2)?;
            if index % 2 == 1 {
                return Some(stop_reply());
            }
            let mut created_version = mut_version.lock().unwrap();
            if matches!(step, Step::Edit) {
                *created_version = digest(&fs::read(workspace.join(PLAN)).expect("created plan"));
            }
            let (call, tool) = step.names();
            Some(tool_reply(
                call,
                tool,
                &step.arguments(&workspace, &created_version),
            ))
        }
    })
    .await;
    let mut config = config(root.path(), &provider, &provider);
    crate::project::seed_test_projects(
        &config.data_dir,
        json!([{"id":"project", "name":"Project", "root": workspace, "owners":[OWNER]}]),
    );
    config.session_defaults.review = Some(false);
    config.agent = Some(candidate);
    let host = Arc::new(LocalHost::new(&workspace, Default::default()).expect("real LocalHost"));
    let manager = Manager::with_host(config, host)
        .await
        .expect("configure Manager with the candidate and loopback provider");
    let owner = Principal::for_test(OWNER);
    let entries = catalog(&manager, &owner).await;
    let entry = catalog_entry(&entries, "coding", &candidate_digest);
    let (first_id, first, lease) = create_selected(&manager, &owner, entry, "plan").await;
    let (other_id, other, _) = create_selected(&manager, &owner, entry, "plan").await;
    assert_ne!(first_id, other_id);
    assert!(provider.wire.requests.lock().unwrap().is_empty());

    let path = workspace.join(PLAN);
    let mut committed = String::new();
    let mut created_at = 0;
    for (index, step) in STEPS.into_iter().enumerate() {
        let (call, tool) = step.names();
        let (id, managed) = if matches!(step, Step::ForeignEdit) {
            (&other_id, &other)
        } else {
            (&first_id, &first)
        };
        let mut events = managed.hub.subscribe();
        let earliest = utc_ms();
        let operation = submit_scripted_turn(&manager, &owner, id, call).await;
        if matches!(step, Step::Patch) {
            let request = pending_ask(&mut events, call, tool, "plan").await;
            assert_eq!(request["arguments"], step.arguments(&workspace, ""));
            assert_eq!(fs::read_to_string(&path).unwrap(), committed);
            assert_eq!(provider.wire.requests.lock().unwrap().len(), index * 2 + 1);
            decide_ask(&manager, &owner, id, &lease, &request, "deny").await;
        }
        settle_turn(&manager, managed, &operation).await;
        while let Ok(event) = events.receiver.try_recv() {
            assert_ne!(
                event.event.name(),
                "permission.requested",
                "unexpected Ask for {call}"
            );
        }
        let (_, result) = wire_result(&provider, index * 2, call, tool);

        match step {
            Step::Create | Step::Edit => {
                let is_edit = matches!(step, Step::Edit);
                let after = fs::read_to_string(&path).unwrap();
                let timestamp = assert_plan(
                    &after,
                    &first_id,
                    if is_edit { EDITED } else { BODY },
                    earliest,
                    utc_ms(),
                );
                assert_eq!(
                    result["version"],
                    digest(after.as_bytes()),
                    "{call}: {result}"
                );
                assert_eq!(result["bytes"], after.len(), "{call}: {result}");
                if is_edit {
                    assert!(timestamp >= created_at);
                    assert_eq!(result["changed"], true);
                    assert_eq!(result["replacement_counts"], json!([1]));
                } else {
                    created_at = timestamp;
                }
                committed = after;
            }
            Step::Existing | Step::StaleEdit => {
                assert!(result.get("diagnostic").is_none(), "{call}: {result}");
                assert_eq!(result["failure"]["code"], "conflict", "{call}: {result}");
            }
            Step::VersionedWrite | Step::NonMarkdown | Step::Patch => {
                // Runtime policy (including a denied Ask) is not a guest failure.
                assert_denied(&result);
                assert!(result.get("failure").is_none(), "{call}: {result}");
            }
            Step::ForeignEdit => {
                // A valid CAS and the same owner/workspace reach the guest. Only
                // the guest's exact Session binding should prevent the edit.
                assert!(result.get("diagnostic").is_none(), "{call}: {result}");
                assert_eq!(
                    result["failure"],
                    json!({"kind":"failed", "code":"denied", "retryable":false,
                        "message":"plan frontmatter belongs to a different Session"}),
                    "{call}: {result}"
                );
            }
        }
        assert_eq!(fs::read_to_string(&path).unwrap(), committed, "{call}");
        assert_eq!(
            fs::read_to_string(workspace.join(EXISTING)).unwrap(),
            EXISTING_BODY,
            "{call}"
        );
        assert!(
            !workspace.join(NON_MARKDOWN).try_exists().unwrap(),
            "{call}"
        );
    }
    manager.close().await.unwrap();
}
