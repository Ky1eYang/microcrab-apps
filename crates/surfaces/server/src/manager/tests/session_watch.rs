//! Owner-level list increments: create, rename, archive and delete publish
//! `sessions.changed` items equal to `session.list`, scoped to their owner.
use super::*;

#[test]
fn saturated_list_publication_requests_owner_resync_instead_of_growing_queue() {
    let watch = super::super::session_watch::SessionWatch::new();
    let mut events = watch.subscribe();
    let dirty = watch.dirty();
    for index in 0..256 {
        dirty.mark("owner", &format!("session-{index}"));
    }
    assert!(events.try_recv().is_err());
    dirty.mark("other-owner", "overflow");
    let event = events.try_recv().unwrap();
    assert_eq!(event.owner, "other-owner");
    assert_eq!(event.event, "resync_required");
    assert_eq!(event.data["reason"], "slow_consumer");
    assert!(events.try_recv().is_err());
}

async fn next(
    receiver: &mut tokio::sync::broadcast::Receiver<
        Arc<super::super::session_watch::SessionChange>,
    >,
    owner: &str,
    id: &str,
    accept: impl Fn(&Value) -> bool,
) -> Value {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let change = receiver.recv().await.unwrap();
            assert_eq!(change.owner, owner, "watchers receive only their owner");
            if change.event == "sessions.changed"
                && (change.data["session_id"] == id || change.data["session"]["id"] == id)
                && accept(&change.data)
            {
                break change.data.clone();
            }
        }
    })
    .await
    .expect("list increment is published")
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn lifecycle_publishes_owner_scoped_list_increments() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _) = manager(model, "file").await;
    let owner = Principal::for_test("owner");
    let mut watch = manager.watch_sessions();
    let created = commands::create(&manager, "owner", &command("session.create", None,
        serde_json::json!({"project_id":"project","operation_id":uuid::Uuid::new_v4().to_string()})))
        .await.unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let upsert = next(&mut watch, "owner", &id, |data| data["op"] == "upsert").await;
    let list = manager
        .command(
            &owner,
            &command("session.list", None, serde_json::json!({})),
        )
        .await
        .unwrap();
    let listed = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == id.as_str())
        .unwrap()
        .clone();
    assert_eq!(upsert["session"]["id"], listed["id"]);
    assert_eq!(upsert["session"]["project_id"], listed["project_id"]);

    let record = manager.record("owner", &id).await.unwrap();
    manager
        .command(
            &owner,
            &command("session.close", Some(&id), serde_json::json!({})),
        )
        .await
        .unwrap();
    manager
        .command(
            &owner,
            &command(
                "session.rename",
                Some(&id),
                serde_json::json!({"title":"Watched title","expected_revision":record.revision}),
            ),
        )
        .await
        .unwrap();
    let renamed = next(&mut watch, "owner", &id, |data| {
        data["session"]["title"] == "Watched title"
    })
    .await;
    assert_eq!(renamed["op"], "upsert");

    let record = manager.record("owner", &id).await.unwrap();
    manager
        .command(
            &owner,
            &command(
                "session.archive",
                Some(&id),
                serde_json::json!({"expected_revision":record.revision}),
            ),
        )
        .await
        .unwrap();
    let archived = next(&mut watch, "owner", &id, |data| {
        data["session"]["archived"] == true
    })
    .await;
    assert_eq!(archived["op"], "upsert");

    let record = manager.record("owner", &id).await.unwrap();
    manager
        .command(
            &owner,
            &command(
                "session.delete",
                Some(&id),
                serde_json::json!({"expected_revision":record.revision}),
            ),
        )
        .await
        .unwrap();
    // Cleanup settles in the background; its final remove carries the receipt.
    let removed = next(&mut watch, "owner", &id, |data| {
        data["op"] == "remove" && data["state"] == "deleted"
    })
    .await;
    assert_eq!(removed["session_id"], id.as_str());
    manager.close().await.unwrap();
}
