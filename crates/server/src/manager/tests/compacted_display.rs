//! Display history survives compact/reopen independently of selected model Context.
use super::*;
use serde_json::json;

async fn retained_compaction_display(storage: &str) {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(2),
    });
    let (manager, _) = manager(model.clone(), storage).await;
    let created = commands::create(
        &manager,
        "owner",
        &command(
            "session.create",
            None,
            json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()}),
        ),
    )
    .await
    .unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &id).await.unwrap(), true)
        .await
        .unwrap();
    commands::submit(&manager, managed.clone(), &command("turn.submit", Some(&id), json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text("Keep this original user message")}))).await.unwrap();
    let worker = managed.worker.lock().await.take().unwrap();
    worker.await.unwrap();
    let before = manager
        .inspect_messages(&manager.record("owner", &id).await.unwrap())
        .await
        .unwrap();
    assert!(
        before
            .iter()
            .any(|message| message["text"] == "Keep this original user message")
    );
    let cursor_before = managed.journal.cursor().await;
    {
        let _gate = managed.commands.lock().await;
        let request = command(
            "session.compact",
            Some(&id),
            json!({"operation_id":uuid::Uuid::new_v4().to_string()}),
        );
        super::super::compaction::start(
            &manager,
            managed.clone(),
            &request,
            commands::parse(&request).unwrap(),
        )
        .await
        .unwrap();
    }
    let worker = managed.worker.lock().await.take().unwrap();
    worker.await.unwrap();
    let selected = managed.session.checkpoint().await.unwrap();
    assert!(
        !serde_json::to_string(&selected.context.items)
            .unwrap()
            .contains("Keep this original user message")
    );
    let cursor = managed.journal.cursor().await;
    assert_ne!(cursor, cursor_before);
    let messages = manager
        .inspect_messages(&manager.record("owner", &id).await.unwrap())
        .await
        .unwrap();
    for (retained, original) in messages.iter().zip(&before) {
        let mut retained = retained.clone();
        retained.as_object_mut().unwrap().remove("historical");
        assert_eq!(&retained, original);
    }
    assert!(messages.last().unwrap()["compaction"].is_object());
    assert_eq!(
        managed.journal.cursor().await,
        cursor,
        "display inspection never moves selection"
    );
    let page = view::page(messages.clone(), Some(before.len()), 1);
    assert_eq!(page["items"][0]["id"], before.last().unwrap()["id"]);
    let config = manager.config.clone();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let reopened = Manager::with_client(config, model.clone()).await.unwrap();
    let record = reopened.record("owner", &id).await.unwrap();
    let restored = reopened.inspect(&record).await.unwrap();
    let history = reopened.inspect_messages(&record).await.unwrap();
    assert_eq!(history, messages);
    assert!(
        restored["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["text"] == "Keep this original user message")
    );
    assert_eq!(restored["history"], serde_json::to_value(cursor).unwrap());
    assert_eq!(
        model.calls.load(Ordering::SeqCst),
        2,
        "inspection/reopen does not call the model"
    );
    assert!(reopened.record("other-owner", &id).await.is_err());
    reopened.close().await.unwrap();
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_compaction_retains_display_before_summary_after_reopen() {
    retained_compaction_display("file").await;
}
#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_compaction_retains_display_before_summary_after_reopen() {
    retained_compaction_display("sqlite").await;
}
