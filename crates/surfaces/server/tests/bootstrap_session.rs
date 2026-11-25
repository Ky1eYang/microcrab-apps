//! Exercise Bootstrap against the real Core lifecycle without a Server or provider.
#[path = "support/host_shell_fixture.rs"]
mod host_shell_fixture;
#[allow(dead_code)]
#[path = "support/typed_agent_fixture.rs"]
mod typed_agent_fixture;

use microcrab_app_bootstrap::session::{Preparation, prepare};
use microcrab_session::{
    AuditRecord, Auditor, Client, Moment, Session, SessionBuilder, VariableView,
    context::Content,
    policy::{Action, PermissionConfig},
    protocol::Response,
    runtime::{
        Runtime, ToolRegistration,
        journal::{AgentInitStatus, MemoryJournal},
        tool::AllowTools,
        wasm::{WasmAgent, WasmLimits},
    },
};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

struct Model;

#[async_trait::async_trait]
impl Client for Model {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            provider_metadata: None,
            content: "Historical assistant reply".into(),
            tool_calls: vec![],
            usage: Default::default(),
        })
    }
}

fn builder(
    agent: &WasmAgent,
    name: &str,
    system: &str,
) -> (
    SessionBuilder,
    MemoryJournal,
    mpsc::UnboundedReceiver<AuditRecord>,
) {
    let runtime = Runtime::new(
        agent
            .tools()
            .into_iter()
            .map(ToolRegistration::Wasm)
            .collect(),
        Arc::new(AllowTools::new(["echo"])),
    )
    .unwrap();
    let journal = MemoryJournal::new();
    let (audit, events) = mpsc::unbounded_channel();
    let builder = Session::builder(Model, runtime)
        .session_id(name)
        .system(system)
        .permissions(PermissionConfig {
            initialization: Action::Allow,
            ..Default::default()
        })
        .journal(journal.clone())
        .audit_sender(audit);
    (builder, journal, events)
}

#[tokio::test]
async fn create_restore_and_fork_leave_start_to_the_caller() {
    let root = tempfile::tempdir().unwrap();
    let host =
        microcrab_host_local::LocalHost::new_filesystem(root.path(), Default::default()).unwrap();
    let catalog = r#"[{"name":"fixture","modes":[{"name":"execute","role":"both"}],"variables":{},"tools":[{"name":"echo","description":"fixture","parameters":{"type":"object"}}]}]"#;
    let wat = host_shell_fixture::shell_component_source(false, catalog, 0);
    let wasm = typed_agent_fixture::component(&wat);
    let agent = WasmAgent::load(&wasm, WasmLimits::default(), Arc::new(host)).unwrap();

    let (create, source_journal, mut events) = builder(&agent, "source", "Frozen SYSTEM");
    let source = prepare(create, Preparation::Create).await.unwrap();
    assert!(source.state().agent_initialization_pending);
    assert!(source.state().agent_init.is_empty());
    assert!(source_journal.latest().is_none());
    assert!(matches!(
        events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    source.start().await.unwrap();
    assert_eq!(events.try_recv().unwrap().event, Moment::SessionCreated);
    assert!(matches!(
        source.state().agent_init[0].status,
        AgentInitStatus::Ready
    ));
    source.submit("Historical user input").await.unwrap();
    let snapshot = source.checkpoint().await.unwrap();
    assert!(!snapshot.context.items.is_empty());
    let frozen = serde_json::to_vec(&snapshot.context).unwrap();

    let (restore, restored_journal, mut events) = builder(&agent, "restored", "Different SYSTEM");
    let restored = prepare(restore, Preparation::Restore(Box::new(snapshot)))
        .await
        .unwrap();
    assert!(!restored.state().agent_initialization_pending);
    assert!(matches!(
        restored.state().agent_init[0].status,
        AgentInitStatus::Ready
    ));
    assert!(restored_journal.latest().is_none());
    assert!(matches!(
        events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(
        serde_json::to_vec(&restored.context_snapshot().unwrap()).unwrap(),
        frozen
    );
    restored.start().await.unwrap();
    assert_eq!(events.try_recv().unwrap().event, Moment::SessionRestored);
    assert_eq!(restored.state().agent_init.len(), 1);

    let (same_identity, _, _) = builder(&agent, "source", "Different SYSTEM");
    let result = prepare(same_identity, Preparation::Fork(&source)).await;
    assert!(
        matches!(result, Err(microcrab_session::Error::Config(reason))
        if reason.public_reason() == Some("fork requires fresh identity, Journal and independent execution wiring"))
    );

    let (fork, forked_journal, mut events) = builder(&agent, "forked", "Different SYSTEM");
    let forked = prepare(fork, Preparation::Fork(&source)).await.unwrap();
    assert!(forked.state().agent_initialization_pending);
    assert!(forked.state().agent_init.is_empty());
    assert!(forked_journal.latest().is_none());
    assert!(matches!(
        events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(
        serde_json::to_vec(&forked.context_snapshot().unwrap()).unwrap(),
        frozen
    );
    forked.start().await.unwrap();
    assert_eq!(events.try_recv().unwrap().event, Moment::SessionRestored);
    assert!(matches!(
        forked.state().agent_init[0].status,
        AgentInitStatus::Ready
    ));

    restored.close().await.unwrap();
    forked.close().await.unwrap();
    source.close().await.unwrap();
}
