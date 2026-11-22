use super::*;
use microcrab_protocol::{ToolCall, ToolDefinition};
use microcrab_session::{
    Limits, SessionState,
    runtime::{
        Runtime, ToolRegistration,
        tool::{AllowTools, Tool, ToolContext},
    },
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};

fn summary(id: &str) -> Value {
    json!({"id": id, "name":id, "version":"1", "artifact":"0123456789abcdef", "description":"public summary"})
}

fn scope(value: Value) -> ScopedVariableSet {
    ScopedVariableSet {
        name: PLUGINS.into(),
        value,
        visibility: VariableVisibility::Visible,
        persistence: VariablePersistence::Persisted,
        editors: Some(vec![VariableRole::Client]),
    }
}

fn input(value: Value) -> SessionInput {
    let mut input = SessionInput::text("Three turns without assuming plugin locality");
    input.variables.scoped_set.push(scope(value));
    input
}

fn plugin_definition() -> super::super::ClientToolDefinition {
    super::super::ClientToolDefinition {
        definition: ToolDefinition {
            name: PLUGIN_TOOL.into(),
            description: "Fixture browser plugin".into(),
            parameters: json!({"type":"object"}),
        },
        permissions: vec![],
        reclaimable: false,
        interaction: None,
    }
}

async fn setup() -> (
    tempfile::TempDir,
    Arc<ClientTools>,
    Arc<dyn Tool>,
    super::super::BindSnapshot,
) {
    let directory = tempfile::tempdir().unwrap();
    let bridge = ClientTools::open(directory.path(), "parent_session")
        .await
        .unwrap();
    bridge.register(vec![plugin_definition()], 0).await.unwrap();
    let binding = bind(&bridge, "socket_a", "client_a").await;
    let ToolRegistration::External(tool) = bridge.registrations().await.remove(0) else {
        panic!("expected browser plugin adapter");
    };
    (directory, bridge, tool, binding)
}

async fn pending(bridge: &ClientTools) -> super::super::CallRecord {
    for _ in 0..1000 {
        if let Some(call) = bridge.snapshot().await.pending.pop() {
            return call;
        }
        tokio::task::yield_now().await;
    }
    panic!("browser fixture did not admit a call");
}

async fn run_plugin_effect(
    tool: Arc<dyn Tool>,
) -> Result<microcrab_session::runtime::tool::ToolOutput, Error> {
    let definition = tool.definition();
    let runtime = Runtime::new(
        vec![ToolRegistration::External(tool.clone())],
        Arc::new(AllowTools::new([definition.name.clone()])),
    )?;
    let mut context = ToolContext::detached(
        SessionState::new(Limits::default()),
        CancellationToken::new(),
    );
    let call = ToolCall {
        id: "plugin-effect".into(),
        name: definition.name.clone(),
        arguments: json!({}),
        provider_metadata: None,
    };
    context.call_id = call.id.clone();
    context.tool = call.name.clone();
    context.session_id = "parent_session".into();
    context.ensure_detached_execution()?;
    let issuer = microcrab_session::context::Content {
        epoch: 0,
        subagent: None,
        system: "Browser fixture admission".into(),
        items: vec![],
        tools: vec![definition],
        max_output_tokens: 1024,
        max_bytes: 65536,
        turn_id: None,
        round: None,
    };
    runtime
        .admit_invocation(&call, &mut context, &issuer)
        .await
        .unwrap();
    tool.execute(call.arguments, context).await
}

async fn bind(bridge: &ClientTools, socket: &str, client: &str) -> super::super::BindSnapshot {
    bridge
        .bind(
            socket,
            &super::super::BindRequest {
                client_id: client.into(),
                catalog_digest: bridge.snapshot().await.catalog_digest,
            },
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn sender_inventory_is_captured_but_all_plugin_writes_are_stripped() {
    let (_dir, bridge, _, _) = setup().await;
    let mut input = input(json!([summary("plug.one")]));
    input.variables.set.insert(PLUGINS.into(), json!("spoof"));
    input
        .variables
        .remove
        .extend([PLUGINS.into(), "other".into()]);
    input.variables.set.insert("keep".into(), json!(1));
    let resolver = bridge
        .browser_input_resolver(
            Some("socket_a".into()),
            &mut input,
            microcrab_session::State::new(Limits::default()),
            || {},
        )
        .unwrap();
    assert_eq!(
        input.variables.set,
        json!({"keep":1}).as_object().unwrap().clone()
    );
    assert_eq!(input.variables.remove, ["other"]);
    assert!(input.variables.scoped_set.is_empty());

    // Steer must not even acquire the bridge lock; it is not a new executor boundary.
    let lock = bridge.state.lock().await;
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            resolver.prepare(
                InputAdmissionBoundary::Steer,
                &input,
                CancellationToken::new(),
            )
        )
        .await
        .unwrap()
        .is_ok()
    );
    assert!(lock.committed_inventory.is_none());

    let mut invalid = scope(json!([summary("duplicate"), summary("duplicate")]));
    assert!(validate_inventory(invalid.clone()).is_err());
    invalid.value = json!([summary("valid-id")]);
    invalid.persistence = VariablePersistence::Ephemeral;
    assert!(validate_inventory(invalid).is_err());
    input
        .variables
        .scoped_set
        .push(scope(json!([summary("non-admitting-edit")])));
    ClientTools::strip_browser_input_plugins(&mut input);
    assert!(input.variables.scoped_set.is_empty());
}

#[tokio::test]
async fn late_handoff_publishes_only_on_commit_and_revokes_stale_lease() {
    let (_dir, bridge, _, old) = setup().await;
    bind(&bridge, "socket_b", "client_b").await;
    let notices = Arc::new(AtomicUsize::new(0));
    let mut input = input(json!([summary("plug-b")]));
    let resolver = bridge
        .browser_input_resolver(
            Some("socket_b".into()),
            &mut input,
            microcrab_session::State::new(Limits::default()),
            {
                let notices = notices.clone();
                move || {
                    notices.fetch_add(1, Ordering::Relaxed);
                }
            },
        )
        .unwrap();
    let watch = bridge.executor_changes();
    let prepared = resolver
        .prepare(
            InputAdmissionBoundary::Turn,
            &input,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(
        bridge.state.try_lock().is_err(),
        "manual takeover cannot race prepared admission"
    );
    assert_eq!(watch.borrow().as_ref().unwrap().0, "socket_a");
    assert_eq!(notices.load(Ordering::Relaxed), 0);
    drop(prepared); // Core rejection discards the closure and releases the lock.
    let mut state = bridge.state.lock().await;
    assert!(state.committed_inventory.is_none());
    assert_eq!(state.lease.as_ref().unwrap().socket, "socket_a");
    let entries = [summary("plug-b")];
    let accepted = select(&state, Some("socket_b"), Some(&entries));
    let (overlay, notice) = accepted.overlay(None, None).unwrap();
    assert_eq!(overlay.scoped_set[0].value, json!(entries));
    assert!(
        notice
            .unwrap()
            .as_str()
            .contains("Do not reuse old call pins or replay calls")
    );
    accepted.commit(&mut state, &bridge, || {
        assert_eq!(watch.borrow().as_ref().unwrap().0, "socket_a");
        notices.fetch_add(1, Ordering::Relaxed);
    });
    let new = watch.borrow().clone().unwrap();
    assert_eq!(new.0, "socket_b");
    assert!(bridge.input_handoff_notified(&new));
    assert_eq!(notices.load(Ordering::Relaxed), 1);
    drop(state);
    assert!(
        !bridge
            .unbind("socket_a", old.lease_id.as_deref().unwrap())
            .await
            .unwrap()
    );
    assert_eq!(*watch.borrow(), Some(new));
}

#[tokio::test]
async fn pending_effect_uses_certified_incumbent_and_never_replays() {
    let (_dir, bridge, tool, old) = setup().await;
    bind(&bridge, "socket_b", "client_b").await;
    let a = [summary("plug-a")];
    let b = [summary("plug-b")];
    let mut state = bridge.state.lock().await;
    let original = select(&state, Some("socket_a"), Some(&a));
    original.commit(&mut state, &bridge, || panic!("already incumbent"));
    drop(state);
    let worker = tokio::spawn(run_plugin_effect(tool));
    let pending_call = pending(&bridge).await;
    let state = bridge.state.lock().await;
    for socket in ["socket_a", "socket_b"] {
        let candidate = select(&state, Some(socket), Some(&b));
        assert!(candidate.replacement.is_none());
        let (overlay, notice) = candidate
            .overlay(state.committed_inventory.as_ref(), Some(&json!(a)))
            .unwrap();
        assert!(
            notice.is_none(),
            "rejected sender inventory does not cause a false notice"
        );
        assert_eq!(overlay.scoped_set[0].value, json!(a));
    }
    assert_eq!(state.lease.as_ref().unwrap().id, old.lease_id.unwrap());
    drop(state);
    assert_eq!(bridge.snapshot().await.pending, [pending_call]);
    bridge.settle_all("test cleanup").await.unwrap();
    assert!(worker.await.unwrap().is_err());
}

#[tokio::test]
async fn detach_restored_data_and_catalog_replacement_cannot_certify_fallback() {
    let (_dir, bridge, _, _) = setup().await;
    bind(&bridge, "socket_b", "client_b").await;
    let original = [summary("plug-a")];
    let different = [summary("historical-only")];
    let var = json!(original);
    let mut state = bridge.state.lock().await;
    let accepted = select(&state, Some("socket_a"), Some(&original));
    accepted.commit(&mut state, &bridge, || {});
    let fallback = select(&state, Some("unbound_c"), Some(&different));
    let (overlay, notice) = fallback
        .overlay(state.committed_inventory.as_ref(), Some(&json!(different)))
        .unwrap();
    assert!(
        notice.is_some(),
        "restored State disagrees with the live certified inventory"
    );
    assert_eq!(overlay.scoped_set[0].value, var);
    let (_, same) = fallback
        .overlay(state.committed_inventory.as_ref(), Some(&var))
        .unwrap();
    assert!(same.is_none());
    drop(state);
    bridge.detach("socket_a").await.unwrap();
    // A newly acquired activity lease carries no certification from the old lease.
    bind(&bridge, "socket_c", "client_c").await;
    let state = bridge.state.lock().await;
    let empty = select(&state, None, None);
    let (overlay, notice) = empty
        .overlay(state.committed_inventory.as_ref(), Some(&var))
        .unwrap();
    assert_eq!(overlay.scoped_set[0].value, json!([]));
    assert!(
        notice
            .unwrap()
            .as_str()
            .contains("No browser plugins are certified")
    );
    drop(state);

    let mut updated = plugin_definition();
    updated.definition.description.push_str(" new catalog");
    bridge.register(vec![updated], 1).await.unwrap();
    let state = bridge.state.lock().await;
    let stale = select(&state, Some("socket_b"), Some(&different));
    assert!(stale.inventory.is_none());
    assert!(stale.replacement.is_none());
    drop(state);
    bind(&bridge, "socket_b", "client_b").await;
    let state = bridge.state.lock().await;
    assert!(
        select(&state, Some("socket_b"), Some(&different))
            .inventory
            .is_some()
    );
}
