//! Real admission receipts and staggered sibling settlement must preserve inspection identity.
use super::*;
use crate::manager::{
    client_tools::{BindRequest, ResultRequest},
    hub::{SessionRuntimeEvent, Subscription, publication},
};
use std::{collections::BTreeSet, time::Duration};

struct InspectionModel {
    calls: AtomicUsize,
    final_entered: Semaphore,
    final_release: Semaphore,
}
#[async_trait::async_trait]
impl microcrab_session::Client for InspectionModel {
    async fn complete(
        &self,
        _: &Content,
        _: &dyn Auditor,
        _: &dyn VariableView,
        token: CancellationToken,
    ) -> Result<Response, microcrab_transport::Error> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let ids: &[&str] = match call {
            0 => &[],
            1 => &["inspection-first", "inspection-sibling"],
            2 => &["inspection-next-round"],
            3 => {
                self.final_entered.add_permits(1);
                tokio::select! {
                    _ = token.cancelled() => return Err(microcrab_transport::Error::Cancelled),
                    permit = self.final_release.acquire() => permit.unwrap().forget(),
                }
                &[]
            }
            _ => panic!("inspection history must not replay model work"),
        };
        Ok(Response {
            provider_metadata: None,
            content: format!("inspection model response {call}"),
            tool_calls: ids
                .iter()
                .map(|id| microcrab_protocol::ToolCall {
                    provider_metadata: None,
                    id: (*id).into(),
                    name: "inspection_effect".into(),
                    arguments: json!({"identity":id}),
                })
                .collect(),
            usage: Usage::default(),
        })
    }
}

fn ids(snapshot: &Value) -> BTreeSet<String> {
    snapshot["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap().to_owned())
        .collect()
}

fn drain(subscription: &mut Subscription, events: &mut Vec<SessionRuntimeEvent>) {
    loop {
        match subscription.receiver.try_recv() {
            Ok(notification) => events.push(notification.event),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => return,
            Err(error) => panic!("inspection event stream lost continuity: {error}"),
        }
    }
}

async fn requested(
    subscription: &mut Subscription,
    events: &mut Vec<SessionRuntimeEvent>,
) -> Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let notification = subscription.receiver.recv().await.unwrap();
            let call = matches!(
                notification.event,
                SessionRuntimeEvent::ClientToolRequested(_)
            )
            .then(|| notification.event.payload()["call"].clone());
            events.push(notification.event);
            if let Some(call) = call {
                return call;
            }
        }
    })
    .await
    .expect("real Session tool admission must request the client effect")
}

async fn snapshot(manager: &Arc<Manager>, managed: &Managed) -> Value {
    let record = managed.record.lock().await.clone();
    manager
        .inspect_subscription(&record)
        .await
        .unwrap()
        .snapshot
}

fn assert_append(previous: &Value, next: &Value) {
    assert_eq!(
        publication::lineage(previous),
        publication::lineage(next),
        "synthetic recovery receipts must not revoke live inspection lineage"
    );
    assert!(
        ids(previous).is_subset(&ids(next)),
        "already canonical rows must retain exact IDs"
    );
    assert!(
        publication::diff(previous, next)
            .iter()
            .all(|event| !matches!(event, SessionRuntimeEvent::HistoryReset(_))),
        "actual live append must publish upserts rather than reset"
    );
}

async fn result(managed: &Managed, lease: &str, call: &Value, text: &str) {
    managed
        .client_tools
        .result(
            "inspection-socket",
            &ResultRequest {
                lease_id: lease.into(),
                invocation_id: call["invocation_id"].as_str().unwrap().into(),
                result: Some(json!({"text":text})),
                error: None,
            },
        )
        .await
        .unwrap();
}

async fn continuity(storage: &str) {
    tokio::time::timeout(Duration::from_secs(30), async {
        let model = Arc::new(InspectionModel {
            calls: AtomicUsize::new(0),
            final_entered: Semaphore::new(0),
            final_release: Semaphore::new(0),
        });
        let (manager, _root) = manager(model.clone(), storage).await;
        let created = commands::create(&manager, "owner", &command("session.create", None, json!({
            "project_id":"project", "operation_id":uuid::Uuid::new_v4().to_string(),
            "client_tools":[{
                "name":"inspection_effect", "description":"Controlled inspection regression effect",
                "parameters":{"type":"object","properties":{"identity":{"type":"string"}},"required":["identity"],"additionalProperties":false},
                "permissions":[{"tool":{"kind":"exact","value":"inspection_effect"},"args":[],"action":"allow"}],
                "reclaimable":true
            }]
        }))).await.unwrap();
        let id = created["id"].as_str().unwrap();
        let managed = manager.active(manager.record("owner", id).await.unwrap(), true).await.unwrap();
        let binding = managed.client_tools.bind("inspection-socket", &BindRequest {
            client_id:"inspection-browser".into(),
            catalog_digest:managed.client_tools.snapshot().await.catalog_digest,
        }).await.unwrap();
        let lease = binding.lease_id.unwrap();
        managed.session.submit("prior canonical input remains inspectable").await.unwrap();
        let before = snapshot(&manager, &managed).await;
        assert_eq!(before["messages"].as_array().unwrap().len(), 2);
        let mut subscription = managed.hub.subscribe();
        let mut events = Vec::new();
        commands::submit(&manager, managed.clone(), &command("turn.submit", Some(id), json!({
            "operation_id":uuid::Uuid::new_v4().to_string(),
            "input":microcrab_session::SessionInput::text("three staggered tool effects")
        }))).await.unwrap();
        let first = requested(&mut subscription, &mut events).await;
        let admitted = snapshot(&manager, &managed).await;
        assert_append(&before, &admitted);
        assert!(!admitted["messages"].as_array().unwrap().iter().any(|row| row["role"] == "tool"),
            "pending synthetic crash receipts must stay out of the actual live projection");

        // Settle one actual sibling, leaving the other pending. The real result
        // deliberately resembles a synthetic status receipt; bytes grant no proof.
        result(&managed, &lease, &first, r#"{"status":"waiting","interrupted":true}"#).await;
        let sibling = requested(&mut subscription, &mut events).await;
        assert_ne!(first["invocation_id"], sibling["invocation_id"]);
        let partial = snapshot(&manager, &managed).await;
        assert_append(&admitted, &partial);
        assert_eq!(partial["messages"].as_array().unwrap().iter().filter(|row| row["role"] == "tool").count(), 1);
        let first_canonical = partial["messages"].as_array().unwrap().iter()
            .find(|row| row["role"] == "tool").unwrap()["id"].clone();

        result(&managed, &lease, &sibling, "second actual sibling result").await;
        let third = requested(&mut subscription, &mut events).await;
        let next_round = snapshot(&manager, &managed).await;
        assert_append(&partial, &next_round);
        assert!(next_round["messages"].as_array().unwrap().iter().any(|row| row["id"] == first_canonical));
        assert_eq!(next_round["messages"].as_array().unwrap().iter().filter(|row| row["role"] == "tool").count(), 2);
        result(&managed, &lease, &third, "third actual result in another round").await;
        model.final_entered.acquire().await.unwrap().forget();
        let awaiting_final = snapshot(&manager, &managed).await;
        assert_append(&next_round, &awaiting_final);
        assert_eq!(awaiting_final["messages"].as_array().unwrap().iter().filter(|row| row["role"] == "tool").count(), 3);
        model.final_release.add_permits(1);
        if let Some(worker) = managed.worker.lock().await.take() { worker.await.unwrap(); }
        let settled = snapshot(&manager, &managed).await;
        assert_append(&awaiting_final, &settled);
        drain(&mut subscription, &mut events);
        assert!(events.iter().any(|event| matches!(event, SessionRuntimeEvent::BlocksUpsert(_))),
            "the real Hub must publish canonical deltas during the Turn");
        assert!(events.iter().all(|event| !matches!(event,
            SessionRuntimeEvent::HistoryReset(_) | SessionRuntimeEvent::ResyncRequired(_))),
            "linear admission/settlement must not reset or reconnect inspection");
        assert_eq!(model.calls.load(Ordering::SeqCst), 4);

        let boundary = events.len();
        commands::execute(&manager, &crate::auth::Principal::for_test("owner"),
            &command("history.travel", Some(id), json!({"direction":"undo"}))).await.unwrap();
        let undone = snapshot(&manager, &managed).await;
        drain(&mut subscription, &mut events);
        assert_ne!(publication::lineage(&settled), publication::lineage(&undone));
        assert_eq!(ids(&undone), ids(&before));
        assert!(publication::diff(&settled, &undone).iter().any(|event| matches!(event, SessionRuntimeEvent::HistoryReset(_))));
        assert!(events[boundary..].iter().any(|event| matches!(event, SessionRuntimeEvent::HistoryReset(_))),
            "actual history selection must invalidate the previously inspected tool rows");
        assert!(!ids(&undone).contains(first_canonical.as_str().unwrap()));
        assert_eq!(model.calls.load(Ordering::SeqCst), 4);
        manager.close().await.unwrap();
    }).await.expect("staggered inspection continuity contract must settle");
}

#[cfg(feature = "storage-file")]
#[tokio::test]
async fn file_real_tool_rounds_preserve_inspection_until_actual_undo() {
    continuity("file").await;
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn sqlite_real_tool_rounds_preserve_inspection_until_actual_undo() {
    continuity("sqlite").await;
}
