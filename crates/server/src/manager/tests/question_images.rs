//! Image eligibility belongs to a live exact question, not assistant/tool prose.
mod revocation;
use super::*;
use crate::manager::{
    client_tools::{BindRequest, ResultRequest},
    question_images,
};
use base64::Engine;
use serde_json::json;

struct QuestionModel(AtomicUsize);
#[async_trait::async_trait]
impl microcrab_session::Client for QuestionModel {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let first = self.0.fetch_add(1, Ordering::SeqCst) == 0;
        Ok(Response {
            content: "![assistant-only](assistant.png)".into(),
            tool_calls: if first {
                vec![microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: "question-image".into(),
                    name: "ask_user_question".into(),
                    arguments: json!({"questions":[{"id":"q","question":"![picture](picture.png) [link](link.png) ![svg](picture.svg) ![big](big.bin)","options":[{"label":"![option](option.png)"}]}]}),
                }]
            } else {
                vec![]
            },
            provider_metadata: None,
            usage: Usage::default(),
        })
    }
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn question_images_require_exact_live_question_owner_history_target_and_executor() {
    let (manager, root) = manager(Arc::new(QuestionModel(AtomicUsize::new(0))), "file").await;
    let bytes = base64::engine::general_purpose::STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j7S8AAAAASUVORK5CYII=").unwrap();
    std::fs::write(root.join("picture.png"), &bytes).unwrap();
    std::fs::write(root.join("picture.svg"), b"<svg/>").unwrap();
    let file = std::fs::File::create(root.join("big.bin")).unwrap();
    file.set_len(super::super::citations::MAX_FILE_BYTES as u64 + 1)
        .unwrap();
    let created = commands::create(&manager, "owner", &command("session.create", None, json!({
        "project_id":"project", "operation_id":uuid::Uuid::new_v4().to_string(),
        "client_tools":[{"name":"ask_user_question","description":"Question image fixture", "parameters":{"type":"object"},
            "permissions":[{"tool":{"kind":"exact","value":"ask_user_question"},"args":[],"action":"allow"}],"reclaimable":true}]
    }))).await.unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let binding = managed
        .client_tools
        .bind(
            "question-socket",
            &BindRequest {
                client_id: "question-browser".into(),
                catalog_digest: managed.client_tools.snapshot().await.catalog_digest,
            },
        )
        .await
        .unwrap();
    let lease = binding.lease_id.unwrap();
    commands::submit(&manager, managed.clone(), &command("turn.submit", Some(id), json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text("ask")}))).await.unwrap();
    let call = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(call) = managed
                .client_tools
                .snapshot()
                .await
                .pending
                .into_iter()
                .next()
            {
                break call;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let record = manager.record("owner", id).await.unwrap();
    let (_, lineage, _, _) = manager.inspect_message_source(&record).await.unwrap();
    let request = |href: &str, history: &str, target: &str| question_images::Request {
        history_lineage: history.into(),
        href: href.into(),
        target_session_id: target.into(),
    };
    let owner = Principal::for_test("owner");
    let read = manager
        .question_image(
            &owner,
            id,
            &call.invocation_id,
            request("picture.png", &lineage, id),
            lease.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(read.media_type, "image/png");
    assert_eq!(read.data, bytes);
    for (principal, href, history, target, lease_id, code) in [
        (
            Principal::for_test("intruder"),
            "picture.png",
            lineage.as_str(),
            id,
            lease.as_str(),
            "not_found",
        ),
        (
            owner.clone(),
            "picture.png",
            "stale",
            id,
            lease.as_str(),
            "stale_history",
        ),
        (
            owner.clone(),
            "picture.png",
            lineage.as_str(),
            "other",
            lease.as_str(),
            "permission_denied",
        ),
        (
            owner.clone(),
            "picture.png",
            lineage.as_str(),
            id,
            "stale-lease",
            "permission_denied",
        ),
        (
            owner.clone(),
            "link.png",
            lineage.as_str(),
            id,
            lease.as_str(),
            "permission_denied",
        ),
        (
            owner.clone(),
            "option.png",
            lineage.as_str(),
            id,
            lease.as_str(),
            "permission_denied",
        ),
        (
            owner.clone(),
            "assistant.png",
            lineage.as_str(),
            id,
            lease.as_str(),
            "permission_denied",
        ),
        (
            owner.clone(),
            "picture.svg",
            lineage.as_str(),
            id,
            lease.as_str(),
            "unsupported_media_type",
        ),
        (
            owner.clone(),
            "big.bin",
            lineage.as_str(),
            id,
            lease.as_str(),
            "request_too_large",
        ),
    ] {
        let error = manager
            .question_image(
                &principal,
                id,
                &call.invocation_id,
                request(href, history, target),
                lease_id.into(),
                CancellationToken::new(),
            )
            .await
            .err()
            .unwrap();
        assert_eq!(error.code, code, "{href}");
    }
    managed
        .client_tools
        .result(
            "question-socket",
            &ResultRequest {
                lease_id: lease.clone(),
                invocation_id: call.invocation_id.clone(),
                result: Some(json!({"answer":"done"})),
                error: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        manager
            .question_image(
                &owner,
                id,
                &call.invocation_id,
                request("picture.png", &lineage, id),
                lease,
                CancellationToken::new()
            )
            .await
            .err()
            .unwrap()
            .code,
        "permission_denied"
    );
    manager.close().await.unwrap();
}
