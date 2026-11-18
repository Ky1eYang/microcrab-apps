//! Maintained native Component composition gate for explicit persistent child re-entry.
use super::*;
use microcrab_host_api::{FsApi, HostAPI, HostInfo, NetApi, ProcessApi, ShellApi, ToolError};
struct ChatHost;
impl ProcessApi for ChatHost {}
impl FsApi for ChatHost {}
impl NetApi for ChatHost {}
impl ShellApi for ChatHost {}
impl HostAPI for ChatHost {}
impl Host for ChatHost {
    fn info(&self) -> Result<HostInfo, ToolError> {
        let descriptor: Value = serde_json::from_str(include_str!(
            "../host_tests/fixtures/chat-only-host-descriptor.json"
        ))
        .unwrap();
        Ok(serde_json::from_value(descriptor["descriptor"]["info"].clone()).unwrap())
    }
}
struct DelegateOnce(std::sync::atomic::AtomicBool);
#[async_trait::async_trait]
impl microcrab_session::Client for DelegateOnce {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        _: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        Ok(Response {
            content: "maintained Both child answer".into(),
            tool_calls: if self.0.swap(false, Ordering::AcqRel) {
                vec![microcrab_protocol::ToolCall {
                    id: "native-delegate".into(),
                    name: "delegate".into(),
                    arguments: serde_json::json!({"target":"universal","mode":"execute","input":"focused persistent work"}),
                    provider_metadata: None,
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires root-built Universal Component path in MICROCRAB_TEST_UNIVERSAL_COMPONENT"]
async fn maintained_native_both_child_restores_via_manager_entry_without_replaying_delegate() {
    tokio::time::timeout(std::time::Duration::from_secs(30),async {
        let component=std::env::var("MICROCRAB_TEST_UNIVERSAL_COMPONENT").expect("root-built Universal Component path");
        let (base,_root)=manager(Arc::new(DelegateOnce(std::sync::atomic::AtomicBool::new(false))),"file").await;
        let mut config=base.config.clone();base.close().await.unwrap();drop(base);
        config.agent=Some(serde_json::from_value(serde_json::json!({"path":component})).unwrap());
        config.session_defaults.permissions=Some(serde_json::from_value(serde_json::json!([{"tool":{"kind":"exact","value":"delegate"},"args":[],"action":"allow"}])).unwrap());
        config.session_defaults.initialization=Some(microcrab_permission::Action::Allow);
        let manager=Manager::initialize(config.clone(),Arc::new(ChatHost),Some(Arc::new(DelegateOnce(std::sync::atomic::AtomicBool::new(true)))),true).await.unwrap();
        let principal=Principal::for_test("owner");
        let created=manager.command(&principal,&command("session.create",None,serde_json::json!({"project_id":"project","agent":"universal","mode":"execute","operation_id":uuid::Uuid::new_v4().to_string()}))).await.unwrap();
        let id=created["id"].as_str().unwrap().to_owned();
        let parent=manager.active(manager.record("owner",&id).await.unwrap(),true).await.unwrap();
        manager.command(&principal,&command("turn.submit",Some(&id),serde_json::json!({"operation_id":uuid::Uuid::new_v4().to_string(),"input":microcrab_session::SessionInput::text("delegate persistent work")}))).await.unwrap();
        if let Some(driver)=parent.worker.lock().await.take() {driver.await.unwrap();}
        let mut changes=parent.session.executions().subscribe();
        let (child_id,child)=loop {
            let row=parent.session.execution_snapshots().into_iter().find(|row|row.tool=="delegate").expect("real canonical delegate admitted");
            if let Some(binding)=&row.delegate_child && let Ok(child)=parent.session.inspect_child(&binding.session_id,"universal") {break (binding.session_id.clone(),child);}
            assert!(row.status.is_pending(),"delegate failed before child publication: {:?}",row.failure);
            changes.changed().await.unwrap();
        };
        let mut activity=child.subscribe_activity();while activity.borrow().outcome_sequence<1 {activity.changed().await.unwrap();}
        let identity=child.communication_identity().unwrap();
        assert!(parent.session.child_accepts_user_input(&child_id).unwrap());
        manager.close().await.unwrap();drop(child);drop(parent);drop(manager);
        let manager=Manager::initialize(config,Arc::new(ChatHost),Some(Arc::new(DelegateOnce(std::sync::atomic::AtomicBool::new(false)))),true).await.unwrap();
        let readonly=manager.command(&principal,&command(commands::SESSION_CHILD,Some(&id),serde_json::json!({"child_id":child_id}))).await.unwrap();
        assert_eq!(readonly["agent_id"],serde_json::to_value(&identity.agent_id).unwrap());assert_eq!(readonly["accepts_user_input"],false);
        assert!(!manager.registry.lock().unwrap().contains_key(&id),"inspection must not activate parent or child");
        let entered=manager.command(&principal,&command("child.enter",Some(&id),serde_json::json!({"child_id":child_id}))).await.unwrap();
        assert_eq!(entered["agent_id"],readonly["agent_id"]);assert_eq!(entered["interaction"],"persistent");assert_eq!(entered["current_role"],"both");assert_eq!(entered["accepts_user_input"],true);
        let again=manager.command(&principal,&command("child.enter",Some(&id),serde_json::json!({"child_id":child_id}))).await.unwrap();assert_eq!(entered,again,"duplicate entry uses the same endpoint without reopening Journal");
        let operation=uuid::Uuid::new_v4().to_string();
        let accepted=manager.command(&principal,&command("child.turn.submit",Some(&id),serde_json::json!({"child_id":child_id,"operation_id":operation,"input":microcrab_session::SessionInput::text("explicit fresh child turn")}))).await.unwrap();
        assert_eq!(accepted["status"],"accepted");
        let parent=manager.active(manager.record("owner",&id).await.unwrap(),true).await.unwrap();
        {let mut drivers=parent.child_drivers.lock().await;while let Some(result)=drivers.join_next().await {result.unwrap().unwrap();}}
        let observed=manager.command(&principal,&command("child.turn.get",Some(&id),serde_json::json!({"child_id":child_id,"operation_id":operation}))).await.unwrap();assert_eq!(observed["status"],"completed");
        assert_eq!(parent.session.inspect_child(&child_id,"universal").unwrap().communication_identity().unwrap(),identity);
        assert!(!parent.session.executions().has_pending(),"entry does not revive the old delegate Job");
        manager.close().await.unwrap();
    }).await.expect("native Manager child re-entry gate finishes");
}
