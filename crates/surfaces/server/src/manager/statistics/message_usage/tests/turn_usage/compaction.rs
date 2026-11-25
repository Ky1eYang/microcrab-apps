use super::*;
use microcrab_session::{Journal, Limits, Recover, SessionState, context::Context};
use microcrab_storage::CreatorJournal;

fn checkpoint(context: &Context) -> Recover {
    let snapshot = context.snapshot().unwrap();
    let mut state = SessionState::new(Limits::default());
    state.epoch_id = snapshot.epoch;
    Recover {
        state,
        context: snapshot,
        continuation: None,
        workspace: None,
        cache_report: None,
        prompt_config: None,
        agent: None,
    }
}

fn append(context: &mut Context, epoch: u64, round: u64, text: &str) {
    context
        .synchronize_state(&json!({"session":{"epoch":epoch,"turn":1,"round":round}}))
        .unwrap();
    context
        .append_response(&microcrab_protocol::Response {
            provider_metadata: None,
            content: text.into(),
            tool_calls: vec![],
            usage: Default::default(),
        })
        .unwrap();
}

#[tokio::test]
async fn actual_compaction_reopen_uses_selected_old_epoch_anchor_and_excludes_sibling_attempts() {
    let directory = tempfile::tempdir().unwrap();
    let config: crate::config::Config = serde_json::from_value(json!({
        "storage":"file","data_dir":directory.path(),
        "provider":{"endpoint":"http://127.0.0.1:1/chat/completions","model":"test"},
        "auth":{"issuer":"http://127.0.0.1","authorization_endpoint":"http://127.0.0.1/authorize",
            "token_endpoint":"http://127.0.0.1/token","jwks_uri":"http://127.0.0.1/jwks",
            "client_id":"test","redirect_uri":"http://127.0.0.1/callback",
            "allowed_origins":["http://127.0.0.1"],"jwt_secret":"test-key-at-least-thirty-two-characters"}
    })).unwrap();
    let mut statistics = Statistics::new(&config, "compacted-turn");
    statistics.directory = directory.path().to_path_buf();
    let mut recorder = Recorder::open(directory.path(), RecorderOptions::default()).unwrap();
    let first = record(
        &mut recorder,
        "old-first",
        None,
        0,
        1,
        None,
        "accepted",
        json!({"input_tokens":10,"output_tokens":3}),
    );
    let old_selected = record(
        &mut recorder,
        "old-second",
        Some(first.clone()),
        0,
        2,
        None,
        "accepted",
        json!({"input_tokens":20,"output_tokens":4}),
    );
    // This newer sibling has the same State position as the selected old round.
    record(
        &mut recorder,
        "old-sibling",
        Some(first.clone()),
        0,
        2,
        None,
        "accepted",
        json!({"input_tokens":999,"output_tokens":999}),
    );
    let (journal, _) = CreatorJournal::open(
        directory.path().to_str().unwrap(),
        "compacted-turn".into(),
        true,
        false,
    )
    .await
    .unwrap();
    let mut context =
        Context::new("system".into(), vec![], Limits::default().max_context_bytes).unwrap();
    context
        .admit_user(
            "original user request",
            &json!({"session":{"epoch":0,"turn":1,"round":1}}),
        )
        .unwrap();
    append(&mut context, 0, 1, "old first answer");
    append(&mut context, 0, 2, "old second answer");
    journal.history_anchor().lock().unwrap().request_head = Some(old_selected.clone());
    journal.commit(checkpoint(&context)).await.unwrap();
    context
        .compact(
            "actual retained summary".into(),
            &json!({"session":{"epoch":0,"turn":1,"round":2}}),
        )
        .unwrap();
    // Session's explicit epoch boundary is represented by its current snapshot;
    // the archived checkpoint above retains its original epoch and State frames.
    let mut compacted = context.snapshot().unwrap();
    compacted.epoch = 1;
    compacted.pending_epoch = false;
    let mut context = Context::from_snapshot(compacted).unwrap();
    append(&mut context, 1, 1, "current answer");
    // Real NativeAuditor epoch changes start a fresh main request parent chain.
    let current = record(
        &mut recorder,
        "current-root",
        None,
        1,
        1,
        None,
        "accepted",
        json!({"input_tokens":30,"output_tokens":5}),
    );
    journal.history_anchor().lock().unwrap().request_head = Some(current.clone());
    journal.commit(checkpoint(&context)).await.unwrap();
    journal.close().await.unwrap();
    let (journal, recover) = CreatorJournal::open(
        directory.path().to_str().unwrap(),
        "compacted-turn".into(),
        true,
        true,
    )
    .await
    .unwrap();
    let recover = recover.unwrap();
    let display = crate::manager::view::display_messages(
        &journal,
        recover.context.compaction.as_ref(),
        recover.context.items.clone(),
        recover.state.epoch_id,
        "compacted-turn",
    )
    .await
    .unwrap();
    assert_eq!(
        display.historical_heads,
        vec![(0, Some(old_selected.clone()))]
    );
    let current_head = journal
        .history_anchor()
        .lock()
        .unwrap()
        .request_head
        .clone();
    assert_eq!(current_head.as_ref(), Some(&current));
    let mut page = crate::manager::view::message_page(
        display.messages.clone(),
        "reopened",
        1,
        None,
        128,
        None,
        None,
        None,
    )
    .unwrap();
    statistics
        .turn_usage(
            current_head.clone(),
            &display.historical_heads,
            &display.messages,
            &mut page["history_view"],
        )
        .await;
    let turns = page["history_view"]["turns"].as_array().unwrap();
    let historical = turns.iter().find(|turn| turn["epoch"] == 0).unwrap();
    assert_eq!(historical["usage"]["input_tokens"], 10);
    assert_eq!(historical["usage"]["output_tokens"], 7);
    let current_turn = turns.iter().find(|turn| turn["epoch"] == 1).unwrap();
    assert_eq!(current_turn["usage"]["input_tokens"], 30);
    assert_eq!(current_turn["usage"]["output_tokens"], 5);
    // Overlapping anchors intentionally share ancestors and are counted once.
    let result = microcrab_creator::audit::query_selected_turn_metadata_with_heads(
        directory.path(),
        &[current.clone(), old_selected.clone(), first, old_selected],
        &[(0, 1), (1, 1)],
    )
    .unwrap();
    assert!(result.selected_ancestry_complete);
    assert!(result.selected_identities_complete);
    assert_eq!(result.attempts.len(), 3);
    // A missing historical anchor cannot borrow another epoch's complete chain.
    statistics
        .turn_usage(
            current_head,
            &[(0, None)],
            &display.messages,
            &mut page["history_view"],
        )
        .await;
    let turns = page["history_view"]["turns"].as_array().unwrap();
    assert!(
        turns.iter().find(|turn| turn["epoch"] == 0).unwrap()["usage"]["output_tokens"].is_null()
    );
    assert_eq!(
        turns.iter().find(|turn| turn["epoch"] == 1).unwrap()["usage"]["output_tokens"],
        5
    );
    journal.close().await.unwrap();
}

struct EnvironmentModel {
    recorder: std::sync::Mutex<Recorder>,
    anchor: std::sync::Arc<std::sync::Mutex<microcrab_storage::AuditAnchor>>,
}
#[async_trait::async_trait]
impl microcrab_session::Client for EnvironmentModel {
    async fn complete(
        &self,
        content: &microcrab_transport::Content,
        _: &dyn microcrab_auditor::Auditor,
        _: &dyn microcrab_auditor::VariableView,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<microcrab_protocol::Response, microcrab_transport::Error> {
        let call = format!("epoch-{}", content.epoch);
        let usage = if content.epoch == 0 {
            json!({"input_tokens":10,"output_tokens":3})
        } else {
            json!({"input_tokens":30,"output_tokens":5})
        };
        let mut recorder = self.recorder.lock().unwrap();
        let head = recorder
            .effective(EffectiveRequest {
                call_id: call.clone(),
                epoch: content.epoch,
                turn: content.turn_id,
                round: content.round,
                // Catalog epochs intentionally have independent main request roots.
                parent_request: None,
                history_head: None,
                subagent: None,
                request_body: "same wire body".into(),
            })
            .unwrap();
        recorder
            .publish(
                json!({"event":"request_prepared","call_id":call,"sent_request":"same wire body"}),
            )
            .unwrap();
        recorder.publish(json!({"event":"round_completed","call_id":call,"outcome":"accepted","usage":usage})).unwrap();
        self.anchor.lock().unwrap().request_head = Some(head);
        Ok(microcrab_protocol::Response {
            provider_metadata: None,
            content: format!("epoch {} answer", content.epoch),
            tool_calls: vec![],
            usage: serde_json::from_value(usage).unwrap(),
        })
    }
}
struct RemovedEnvironmentTool;
#[async_trait::async_trait]
impl microcrab_session::runtime::tool::Tool for RemovedEnvironmentTool {
    fn definition(&self) -> microcrab_protocol::ToolDefinition {
        microcrab_protocol::ToolDefinition {
            name: "environment_fixture".into(),
            description: "environment fixture".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn execute(
        &self,
        _: Value,
        _: microcrab_session::runtime::tool::ToolContext,
    ) -> Result<microcrab_session::runtime::execution::ToolOutput, microcrab_session::Error> {
        Ok(microcrab_session::runtime::execution::ToolOutput::text(
            "unused fixture",
        ))
    }
}

#[tokio::test]
async fn real_catalog_epoch_change_without_compaction_retains_old_turn_usage_after_reopen() {
    use microcrab_session::{
        Session,
        runtime::{Runtime, ToolRegistration, tool::DenyAll},
    };
    use std::sync::Arc;
    let directory = tempfile::tempdir().unwrap();
    let config: crate::config::Config = serde_json::from_value(json!({
        "storage":"file","data_dir":directory.path(),
        "provider":{"endpoint":"http://127.0.0.1:1/chat/completions","model":"test"},
        "auth":{"issuer":"http://127.0.0.1","authorization_endpoint":"http://127.0.0.1/authorize",
            "token_endpoint":"http://127.0.0.1/token","jwks_uri":"http://127.0.0.1/jwks",
            "client_id":"test","redirect_uri":"http://127.0.0.1/callback",
            "allowed_origins":["http://127.0.0.1"],"jwt_secret":"test-key-at-least-thirty-two-characters"}
    })).unwrap();
    let mut statistics = Statistics::new(&config, "environment-turns");
    statistics.directory = directory.path().to_path_buf();
    let (journal, _) = CreatorJournal::open(
        directory.path().to_str().unwrap(),
        "environment-turns".into(),
        true,
        false,
    )
    .await
    .unwrap();
    let model = Arc::new(EnvironmentModel {
        recorder: std::sync::Mutex::new(
            Recorder::open(directory.path(), RecorderOptions::default()).unwrap(),
        ),
        anchor: journal.history_anchor(),
    });
    let runtime = Runtime::new(
        vec![ToolRegistration::External(Arc::new(RemovedEnvironmentTool))],
        Arc::new(DenyAll),
    )
    .unwrap();
    let session = Session::builder_arc(model.clone(), runtime)
        .journal_arc(Arc::new(journal.clone()))
        .session_id("environment-turns")
        .build()
        .unwrap();
    session.submit("old epoch user request").await.unwrap();
    let old_head = journal
        .history_anchor()
        .lock()
        .unwrap()
        .request_head
        .clone()
        .unwrap();
    // A newer audit append with the old epoch/position cannot replace Journal's
    // exact selected old head when the current Context retains both epochs.
    {
        let mut recorder = model.recorder.lock().unwrap();
        record(
            &mut recorder,
            "unselected-old-sibling",
            None,
            0,
            1,
            None,
            "accepted",
            json!({"input_tokens":999,"output_tokens":999}),
        );
    }
    session.replace_external_tools(vec![]).await.unwrap();
    session.submit("new epoch user request").await.unwrap();
    assert!(session.compaction().is_none());
    assert_eq!(session.state().epoch_id, 1);
    let live = crate::manager::view::display_messages(
        &journal,
        None,
        session.messages(),
        1,
        "environment-turns",
    )
    .await
    .unwrap();
    assert!(live.historical_heads.contains(&(0, Some(old_head.clone()))));
    let current = journal
        .history_anchor()
        .lock()
        .unwrap()
        .request_head
        .clone();
    let mut page = crate::manager::view::message_page(
        live.messages.clone(),
        "live",
        1,
        None,
        128,
        None,
        None,
        None,
    )
    .unwrap();
    statistics
        .turn_usage(
            current,
            &live.historical_heads,
            &live.messages,
            &mut page["history_view"],
        )
        .await;
    let turns = page["history_view"]["turns"].as_array().unwrap();
    assert_eq!(
        turns.iter().find(|turn| turn["epoch"] == 0).unwrap()["usage"]["input_tokens"],
        10
    );
    assert_eq!(
        turns.iter().find(|turn| turn["epoch"] == 0).unwrap()["usage"]["output_tokens"],
        3
    );
    assert_eq!(
        turns.iter().find(|turn| turn["epoch"] == 1).unwrap()["usage"]["output_tokens"],
        5
    );
    session.close().await.unwrap();
    let (journal, recover) = CreatorJournal::open(
        directory.path().to_str().unwrap(),
        "environment-turns".into(),
        true,
        true,
    )
    .await
    .unwrap();
    let recover = recover.unwrap();
    assert!(recover.context.compaction.is_none());
    let display = crate::manager::view::display_messages(
        &journal,
        None,
        recover.context.items,
        recover.state.epoch_id,
        "environment-turns",
    )
    .await
    .unwrap();
    assert!(display.historical_heads.contains(&(0, Some(old_head))));
    let current = journal
        .history_anchor()
        .lock()
        .unwrap()
        .request_head
        .clone();
    let mut page = crate::manager::view::message_page(
        display.messages.clone(),
        "reopened",
        1,
        None,
        128,
        None,
        None,
        None,
    )
    .unwrap();
    statistics
        .turn_usage(
            current,
            &display.historical_heads,
            &display.messages,
            &mut page["history_view"],
        )
        .await;
    let turns = page["history_view"]["turns"].as_array().unwrap();
    assert_eq!(
        turns.iter().find(|turn| turn["epoch"] == 0).unwrap()["usage"]["input_tokens"],
        10
    );
    assert_eq!(
        turns.iter().find(|turn| turn["epoch"] == 0).unwrap()["usage"]["output_tokens"],
        3
    );
    assert_eq!(
        turns.iter().find(|turn| turn["epoch"] == 1).unwrap()["usage"]["input_tokens"],
        30
    );
    assert_eq!(
        turns.iter().find(|turn| turn["epoch"] == 1).unwrap()["usage"]["output_tokens"],
        5
    );
    journal.close().await.unwrap();
}
