//! Server-level retry authorization, durable deduplication and cold failure inspection.
use super::*;
use microcrab_protocol::application::Command;
use microcrab_session::{
    Journal,
    runtime::{
        execution::ToolFailure,
        journal::{AgentInitRecord, AgentInitStatus},
    },
};
use microcrab_storage::{OperationRecord, RECORD_VERSION};
use serde_json::{Value, json};
#[path = "init_retry_fixture.rs"]
mod fixture;
#[path = "init_retry_phase_tests.rs"]
mod phase_tests;
fn command(method: &str, id: &str, params: Value) -> Command {
    serde_json::from_value(json!({"v":1,"type":"command","request_id":uuid::Uuid::new_v4().to_string(),"method":method,"session_id":id,"params":params})).unwrap()
}
#[tokio::test]
async fn init_retry_requires_owner_and_never_replays_an_admitted_receipt() {
    let root = tempfile::tempdir().unwrap();
    let mut configuration = config(root.path());
    configuration.storage = crate::config::Storage::File;
    let manager = Manager::with_host(configuration, Arc::new(TestHost))
        .await
        .unwrap();
    let managed = create(&manager, "a").await;
    let record = managed.record.lock().await.clone();
    let owner = Principal::for_test("owner");
    let retry = command(
        commands::SESSION_RETRY_AGENT_INIT,
        &record.id,
        json!({"operation_id":uuid::Uuid::new_v4().to_string(),"expected_revision":record.revision}),
    );
    assert!(
        commands::execute(&manager, &Principal::for_test("other"), &retry)
            .await
            .is_err()
    );
    assert_eq!(
        commands::execute(&manager, &owner, &retry)
            .await
            .unwrap_err()
            .code,
        "agent_init_ready"
    );
    let operation_id = retry.params["operation_id"].as_str().unwrap();
    assert!(
        manager
            .directory
            .get_operation("owner", operation_id)
            .await
            .unwrap()
            .is_none()
    );
    let mut operation = manager
        .directory
        .create_operation(OperationRecord {
            version: RECORD_VERSION,
            id: operation_id.into(),
            session_id: record.id.clone(),
            owner: "owner".into(),
            method: commands::SESSION_RETRY_AGENT_INIT.into(),
            payload_hash: commands::hash(&retry).unwrap(),
            status: "accepted".into(),
            result: None,
            revision: 0,
            created_at_ms: 1,
            updated_at_ms: 1,
        })
        .await
        .unwrap();
    assert_eq!(
        commands::execute(&manager, &owner, &retry)
            .await
            .unwrap_err()
            .code,
        "operation_interrupted"
    );
    let error = super::super::agent_init::failure_records(&[AgentInitRecord {
        version: 1,
        component: "a".repeat(64),
        generation: 1,
        attempt: 1,
        cache_points: vec![],
        status: AgentInitStatus::Failed {
            failure: ToolFailure::failed(
                "workspace_denied",
                "Initialization could not create the required file",
            ),
        },
    }])
    .unwrap();
    operation.status = "failed".into();
    operation.result = Some(json!({"error":error}));
    operation.revision = 1;
    manager
        .directory
        .replace_operation("owner", 0, operation)
        .await
        .unwrap();
    let repeated = commands::execute(&manager, &owner, &retry)
        .await
        .unwrap_err();
    assert_eq!(repeated.code, "agent_init_failed");
    assert_eq!(
        repeated.details.unwrap()["agent_init"][0]["status"]["failure"]["code"],
        "workspace_denied"
    );
    assert!(managed.session.state().agent_init.is_empty());
    manager.close().await.unwrap();
}
#[tokio::test]
async fn cold_init_failure_is_inspectable_and_ordinary_open_does_not_reset_evidence() {
    cold_failure(false).await;
}
#[tokio::test]
async fn pending_empty_init_is_inspectable_and_explicit_retry_remains_reachable() {
    cold_failure(true).await;
}
async fn cold_failure(pending_empty: bool) {
    let root = tempfile::tempdir().unwrap();
    let configuration = fixture::configuration(root.path(), "execute", false);
    let manager = Manager::with_host(configuration.clone(), fixture::host(root.path()))
        .await
        .unwrap();
    let managed = fixture::admit(&manager, "execute").await;
    let record = managed.record.lock().await.clone();
    let mut checkpoint = managed.session.checkpoint().await.unwrap();
    manager.close().await.unwrap();
    // Publish test evidence as a new Journal checkpoint, without rewriting an
    // old node. The actual WASM component must preserve a failed/unknown fence.
    checkpoint.state.agent_initialization_pending = true;
    checkpoint.state.agent_init = if pending_empty {
        vec![]
    } else {
        vec![AgentInitRecord {
            version: 1,
            component: managed.component.clone().unwrap(),
            generation: 1,
            attempt: 1,
            cache_points: vec![],
            status: AgentInitStatus::Failed {
                failure: ToolFailure::failed(
                    "init_denied",
                    "Required initialization operation was denied",
                ),
            },
        }]
    };
    let directory = configuration.data_dir.join("sessions").join(&record.id);
    let (journal, _) = microcrab_storage::CreatorJournal::open(
        directory.to_str().unwrap(),
        record.id.clone(),
        true,
        true,
    )
    .await
    .unwrap();
    Journal::commit(&journal, checkpoint).await.unwrap();
    Journal::close(&journal).await.unwrap();
    let reopened = Manager::with_host(configuration, fixture::host(root.path()))
        .await
        .unwrap();
    let owner = Principal::for_test("owner");
    let snapshot = reopened
        .subscribe(&owner, &record.id)
        .await
        .unwrap()
        .snapshot;
    assert_eq!(snapshot["session"]["agent_init_retry_required"], true);
    assert_eq!(snapshot["state"]["agent_initialization_pending"], true);
    assert_eq!(
        snapshot["state"]["agent_init"].as_array().unwrap().len(),
        if pending_empty { 0 } else { 1 }
    );
    let error = commands::execute(
        &reopened,
        &owner,
        &command(commands::SESSION_OPEN, &record.id, json!({})),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "agent_init_failed");
    if pending_empty {
        let details = error.details.unwrap();
        assert_eq!(details["agent_initialization_pending"], true);
        assert_eq!(details["reason"], "init_outcome_unknown");
    } else {
        assert_eq!(error.details.unwrap()["agent_init"][0]["attempt"], 1);
    }
    let state = commands::execute(
        &reopened,
        &owner,
        &command(commands::STATE_GET, &record.id, json!({})),
    )
    .await
    .unwrap();
    assert_eq!(state["agent_initialization_pending"], true);
    let inspection = commands::execute(
        &reopened,
        &owner,
        &command(commands::STATE_INSPECTION, &record.id, json!({})),
    )
    .await
    .unwrap();
    assert_eq!(inspection["values"]["agent_initialization_pending"], true);
    for params in [
        json!({"set":{"must_not_commit":true}}),
        json!({"operation_id":uuid::Uuid::new_v4().to_string(),"expected":inspection["witness"],"change":{"set":{"must_not_commit":true}}}),
    ] {
        let error = commands::execute(
            &reopened,
            &owner,
            &command("session.variables.patch", &record.id, params),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "agent_init_failed");
    }
    if pending_empty {
        assert!(state["agent_init"].as_array().unwrap().is_empty());
        let active = reopened
            .activate(
                reopened.record("owner", &record.id).await.unwrap(),
                true,
                None,
                true,
            )
            .await
            .unwrap();
        fixture::bind(&reopened, &active).await;
        let operation_id = uuid::Uuid::new_v4().to_string();
        let revision = reopened.record("owner", &record.id).await.unwrap().revision;
        let result = commands::execute(
            &reopened,
            &owner,
            &command(
                commands::SESSION_RETRY_AGENT_INIT,
                &record.id,
                json!({"operation_id":operation_id,"expected_revision":revision}),
            ),
        )
        .await;
        if let Err(error) = result {
            assert_ne!(error.code, "agent_init_ready");
        }
        assert!(
            reopened
                .directory
                .get_operation("owner", &operation_id)
                .await
                .unwrap()
                .is_some()
        );
    } else {
        assert_eq!(state["agent_init"][0]["attempt"], 1);
        assert_eq!(
            state["agent_init"][0]["status"]["failure"]["code"],
            "init_denied"
        );
    }
    reopened.close().await.unwrap();
}
