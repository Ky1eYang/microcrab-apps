//! Images stay in canonical Context/Journal; browser projections contain references only.
use super::*;
use crate::manager::images;
use microcrab_protocol::{ImageSource, InputPart, SessionInput};
use serde_json::json;

fn image_png(bytes: usize) -> Vec<u8> {
    use base64::Engine as _;
    let mut data = base64::engine::general_purpose::STANDARD
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j7S8AAAAASUVORK5CYII=")
        .unwrap();
    data.resize(bytes.max(data.len()), 7);
    data
}

#[test]
fn image_projection_preserves_identity_part_indices_and_large_message_references() {
    let item = microcrab_session::context::ContentItem::User {
        source: None,
        parts: vec![
            InputPart::Text {
                text: "x".repeat(40 * 1024),
            },
            InputPart::Image {
                source: ImageSource::Data {
                    media_type: "image/png".into(),
                    data: image_png(64 * 1024),
                },
            },
            InputPart::Image {
                source: ImageSource::Path {
                    path: "private/secret.png".into(),
                },
            },
            InputPart::Image {
                source: ImageSource::Url {
                    url: "https://user:secret@example.invalid/private".into(),
                },
            },
        ],
    };
    let expected_id = format!(
        "message-{}",
        microcrab_storage::history::content_hash(&json!(["session", 3, 0, &item])).unwrap()
    );
    let messages = view::messages(vec![item], 3, "session");
    assert_eq!(messages[0]["id"], expected_id);
    let image_url = format!("/api/v1/sessions/session/images/{expected_id}/1");
    assert_eq!(
        messages[0]["parts"][1]["source"],
        json!({"type":"url","url":image_url})
    );
    assert_eq!(
        messages[0]["parts"][2],
        json!({"type":"image","source_omitted":true})
    );
    let serialized = serde_json::to_string(&messages).unwrap();
    assert!(!serialized.contains("secret"));
    assert!(!serialized.contains("media_type"));
    let page = view::page(messages, None, 64);
    assert_eq!(page["items"][0]["content_truncated"], true);
    assert_eq!(page["items"][0]["parts"].as_array().unwrap().len(), 3);
    assert_eq!(page["items"][0]["parts"][0]["source"]["url"], image_url);
    assert_eq!(page["items"][0]["parts"][1]["source_omitted"], true);
    assert!(serde_json::to_vec(&page).unwrap().len() < 32 * 1024);
    // Raster MIME admission also excludes browser-active SVG content.
    assert!(!images::available(&ImageSource::Data {
        media_type: "image/svg+xml".into(),
        data: b"<svg/>".to_vec()
    }));
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn owner_image_bytes_survive_shutdown_without_reopening_runtime_or_provider() {
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let (manager, _root) = manager(model.clone(), "file").await;
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
    let session_id = created["id"].as_str().unwrap().to_owned();
    let managed = manager
        .active(manager.record("owner", &session_id).await.unwrap(), true)
        .await
        .unwrap();
    let bytes = image_png(0);
    let input = SessionInput {
        parts: vec![
            InputPart::Text {
                text: "attached".into(),
            },
            InputPart::Image {
                source: ImageSource::Data {
                    media_type: "image/png".into(),
                    data: bytes.clone(),
                },
            },
        ],
        variables: Default::default(),
    };
    commands::submit(
        &manager,
        managed.clone(),
        &command(
            "turn.submit",
            Some(&session_id),
            json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":input}),
        ),
    )
    .await
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), model.entered.notified())
        .await
        .expect("admitted image must reach the model within five seconds");
    model.release.add_permits(1);
    if let Some(driver) = managed.worker.lock().await.take() {
        driver.await.unwrap();
    }
    let record = manager.record("owner", &session_id).await.unwrap();
    let messages = manager.inspect_messages(&record).await.unwrap();
    let message = messages
        .iter()
        .find(|message| message["role"] == "user")
        .unwrap();
    let id = message["id"].as_str().unwrap().to_owned();
    assert_eq!(
        message["parts"][1]["source"]["url"],
        format!("/api/v1/sessions/{session_id}/images/{id}/1")
    );
    model.release.add_permits(1);
    {
        let _gate = managed.commands.lock().await;
        let request = command(
            "session.compact",
            Some(&session_id),
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
    let messages = manager.inspect_messages(&record).await.unwrap();
    assert_eq!(messages[0]["id"], id);
    assert!(messages.last().unwrap()["compaction"].is_object());
    let owner = Principal::for_test("owner");
    let read = manager.image(&owner, &session_id, &id, 1).await.unwrap();
    assert_eq!(read.media_type, "image/png");
    assert_eq!(read.data, bytes);
    let denied = manager
        .image(&Principal::for_test("intruder"), &session_id, &id, 1)
        .await
        .err()
        .unwrap();
    assert_eq!(denied.code, "not_found");
    for (message_id, part) in [(&id[..], 0), (&id[..], 2), ("message-missing", 1)] {
        assert_eq!(
            manager
                .image(&owner, &session_id, message_id, part)
                .await
                .err()
                .unwrap()
                .code,
            "not_found"
        );
    }
    let config = manager.config.clone();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let restored = Manager::with_client(config, model.clone()).await.unwrap();
    assert!(restored.registry.lock().unwrap().is_empty());
    let read = restored.image(&owner, &session_id, &id, 1).await.unwrap();
    assert_eq!(read.data, bytes);
    assert!(restored.registry.lock().unwrap().is_empty());
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    let history = restored
        .inspect_messages(&restored.record("owner", &session_id).await.unwrap())
        .await
        .unwrap();
    assert_eq!(history, messages);
    restored.close().await.unwrap();
}
