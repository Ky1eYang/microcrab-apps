//! Durable citation authorization survives runtime closure without activation.
use super::*;
use crate::manager::citations;
struct CitationModel;
#[async_trait::async_trait]
impl microcrab_session::Client for CitationModel {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            content: "[report](report.txt) ![picture](picture.svg) [big](big.bin)".into(),
            tool_calls: vec![],
            provider_metadata: None,
            usage: Usage::default(),
        })
    }
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn citations_are_owner_selected_history_scoped_current_bytes_and_never_activate() {
    let (manager, root) = manager(Arc::new(CitationModel), "file").await;
    std::fs::write(root.join("report.txt"), b"first").unwrap();
    std::fs::write(root.join("picture.svg"), b"<svg/>").unwrap();
    let file = std::fs::File::create(root.join("big.bin")).unwrap();
    file.set_len(citations::MAX_FILE_BYTES as u64 + 1).unwrap();
    let created = commands::create(&manager, "owner", &command("session.create", None, serde_json::json!({"project_id":"project", "operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    commands::submit(&manager, managed.clone(), &command("turn.submit", Some(id), serde_json::json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":{"parts":[{"type":"text","text":"[user](uncited.txt)"}]}}))).await.unwrap();
    if let Some(worker) = managed.worker.lock().await.take() {
        worker.await.unwrap();
    }
    let record = manager.record("owner", id).await.unwrap();
    let (display, lineage, _, _) = manager.inspect_message_source(&record).await.unwrap();
    let message = display
        .messages
        .iter()
        .find(|message| message["role"] == "assistant")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let request = |href: &str, lineage: &str| citations::Request {
        href: href.into(),
        history_lineage: lineage.into(),
        child_session_id: None,
    };
    let owner = Principal::for_test("owner");
    assert_eq!(
        manager
            .citation(
                &owner,
                id,
                &message,
                request("report.txt", &lineage),
                CancellationToken::new()
            )
            .await
            .unwrap()
            .data,
        b"first"
    );
    std::fs::write(root.join("report.txt"), b"current").unwrap();
    assert_eq!(
        manager
            .citation(
                &owner,
                id,
                &message,
                request("report.txt", &lineage),
                CancellationToken::new()
            )
            .await
            .unwrap()
            .data,
        b"current"
    );
    assert_eq!(
        manager
            .citation(
                &owner,
                id,
                &message,
                request("picture.svg", &lineage),
                CancellationToken::new()
            )
            .await
            .unwrap()
            .media_type,
        "application/octet-stream"
    );
    for (principal, href, history, code) in [
        (
            Principal::for_test("intruder"),
            "report.txt",
            lineage.as_str(),
            "not_found",
        ),
        (
            owner.clone(),
            "report.txt",
            "stale-history",
            "stale_history",
        ),
        (
            owner.clone(),
            "uncited.txt",
            lineage.as_str(),
            "permission_denied",
        ),
        (
            owner.clone(),
            "big.bin",
            lineage.as_str(),
            "request_too_large",
        ),
    ] {
        assert_eq!(
            manager
                .citation(
                    &principal,
                    id,
                    &message,
                    request(href, history),
                    CancellationToken::new()
                )
                .await
                .err()
                .unwrap()
                .code,
            code
        );
    }
    let mut expired = Principal::for_test("owner");
    expired.expires_at = 0;
    assert_eq!(
        manager
            .citation(
                &expired,
                id,
                &message,
                request("report.txt", &lineage),
                CancellationToken::new()
            )
            .await
            .err()
            .unwrap()
            .code,
        "unauthenticated"
    );
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert_eq!(
        manager
            .citation(
                &owner,
                id,
                &message,
                request("report.txt", &lineage),
                cancel
            )
            .await
            .err()
            .unwrap()
            .code,
        "cancelled"
    );
    let config = manager.config.clone();
    manager.close().await.unwrap();
    drop(managed);
    drop(manager);
    let restored = Manager::with_client(config, Arc::new(CitationModel))
        .await
        .unwrap();
    let record = restored.record("owner", id).await.unwrap();
    let (_, lineage, _, _) = restored.inspect_message_source(&record).await.unwrap();
    assert!(restored.registry.lock().unwrap().is_empty());
    assert_eq!(
        restored
            .citation(
                &owner,
                id,
                &message,
                request("report.txt", &lineage),
                CancellationToken::new()
            )
            .await
            .unwrap()
            .data,
        b"current"
    );
    assert!(restored.registry.lock().unwrap().is_empty());
    restored.close().await.unwrap();
}
