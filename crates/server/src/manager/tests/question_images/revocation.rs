//! Pending previews retire their Host operation when their question authority ends.
use super::*;
struct DelayedHost {
    inner: Arc<dyn microcrab_host_api::Host>,
    entered: Notify,
    dropped: Arc<AtomicUsize>,
}
impl microcrab_host_api::FsApi for DelayedHost {}
impl microcrab_host_api::NetApi for DelayedHost {}
impl microcrab_host_api::ProcessApi for DelayedHost {}
impl microcrab_host_api::ShellApi for DelayedHost {}
impl microcrab_host_api::HostAPI for DelayedHost {}
struct ReadGuard(Arc<AtomicUsize>);
impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl microcrab_host_api::Host for DelayedHost {
    fn info(&self) -> Result<microcrab_host_api::HostInfo, microcrab_host_api::ToolError> {
        self.inner.info()
    }
    fn tool_capabilities(&self) -> Vec<String> {
        self.inner.tool_capabilities()
    }
    async fn read_file(
        &self,
        _: std::path::PathBuf,
        _: usize,
        cancel: CancellationToken,
    ) -> Result<Vec<u8>, microcrab_host_api::ToolError> {
        let _guard = ReadGuard(self.dropped.clone());
        self.entered.notify_one();
        cancel.cancelled().await;
        Err(microcrab_host_api::ToolError::Cancelled)
    }
}
#[cfg(feature = "storage-file")]
#[tokio::test]
async fn delayed_question_image_reads_are_revoked_on_cancel_detach_and_answer() {
    let model = Arc::new(QuestionModel(AtomicUsize::new(0)));
    let (seed, _) = manager(model.clone(), "file").await;
    let config = seed.config.clone();
    let host = Arc::new(DelayedHost {
        inner: seed.host.clone(),
        entered: Notify::new(),
        dropped: Arc::new(AtomicUsize::new(0)),
    });
    seed.close().await.unwrap();
    let manager = Manager::initialize(config, host.clone(), Some(model), true)
        .await
        .unwrap();
    let created = commands::create(&manager, "owner", &command("session.create", None, json!({
        "project_id":"project", "operation_id":uuid::Uuid::new_v4().to_string(),
        "client_tools":[{"name":"ask_user_question","description":"Question revocation fixture", "parameters":{"type":"object"},
            "permissions":[{"tool":{"kind":"exact","value":"ask_user_question"},"args":[],"action":"allow"}],"reclaimable":true}]
    }))).await.unwrap();
    let id = created["id"].as_str().unwrap();
    let managed = manager
        .active(manager.record("owner", id).await.unwrap(), true)
        .await
        .unwrap();
    let bind = || BindRequest {
        client_id: "preview-browser".into(),
        catalog_digest: String::new(),
    };
    let mut binding = bind();
    binding.catalog_digest = managed.client_tools.snapshot().await.catalog_digest;
    let mut lease = managed
        .client_tools
        .bind("preview-socket", &binding)
        .await
        .unwrap()
        .lease_id
        .unwrap();
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
    let (_, lineage, _, _) = manager
        .inspect_message_source(&manager.record("owner", id).await.unwrap())
        .await
        .unwrap();
    let owner = Principal::for_test("owner");
    for reason in ["caller", "detach", "answer"] {
        let cancel = CancellationToken::new();
        let read = manager.question_image(
            &owner,
            id,
            &call.invocation_id,
            question_images::Request {
                history_lineage: lineage.clone(),
                href: "picture.png".into(),
                target_session_id: id.into(),
            },
            lease.clone(),
            cancel.clone(),
        );
        tokio::pin!(read);
        tokio::select! {
            result = &mut read => panic!("read completed before delayed Host entry: {}", result.err().map(|error| error.code).unwrap_or_default()),
            _ = host.entered.notified() => {},
        }
        match reason {
            "caller" => cancel.cancel(),
            "detach" => {
                assert!(
                    managed
                        .client_tools
                        .unbind("preview-socket", &lease)
                        .await
                        .unwrap()
                );
            }
            _ => {
                managed
                    .client_tools
                    .result(
                        "preview-socket",
                        &ResultRequest {
                            lease_id: lease.clone(),
                            invocation_id: call.invocation_id.clone(),
                            result: Some(json!({"answer":"done"})),
                            error: None,
                        },
                    )
                    .await
                    .unwrap();
            }
        }
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), &mut read)
            .await
            .unwrap()
            .err()
            .unwrap();
        assert_eq!(
            error.code,
            if reason == "caller" {
                "cancelled"
            } else {
                "permission_denied"
            }
        );
        assert_eq!(
            host.dropped.load(Ordering::SeqCst),
            match reason {
                "caller" => 1,
                "detach" => 2,
                _ => 3,
            }
        );
        if reason == "detach" {
            lease = managed
                .client_tools
                .bind("preview-socket", &binding)
                .await
                .unwrap()
                .lease_id
                .unwrap();
        }
    }
    manager.close().await.unwrap();
}
