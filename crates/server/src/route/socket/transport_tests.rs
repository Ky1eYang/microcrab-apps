//! Real upgrade/sink tests: decoder-only checks cannot prove reply-before-close.
use crate::Application;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpStream, task::JoinHandle};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest},
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Fixture {
    state: Arc<Application>,
    base: String,
    token: String,
    server: JoinHandle<()>,
    _root: tempfile::TempDir,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_endpoint("http://127.0.0.1:1/v1/chat/completions").await
    }
    async fn with_endpoint(endpoint: &str) -> Self {
        Self::with_mode(endpoint, true).await
    }
    async fn standalone() -> Self {
        Self::with_mode("http://127.0.0.1:1/v1/chat/completions", false).await
    }
    async fn with_mode(endpoint: &str, embedded: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let mut value: Value =
            serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
        value["agent"] = Value::Null;
        value["auto_rename"] = json!(false);
        value["provider"]["endpoint"] = json!(endpoint);
        value["storage"] = json!("file");
        value["auth"]["dev_oidc"] = json!(false);
        value["auth"]["discovery"] = json!(false);
        value["auth"]["issuer"] = json!("http://127.0.0.1:19091");
        value["auth"]["authorization_endpoint"] = json!("http://127.0.0.1:19091/authorize");
        value["auth"]["token_endpoint"] = json!("http://127.0.0.1:19091/token");
        value["auth"]["jwks_uri"] = json!("http://127.0.0.1:19091/jwks");
        value["auth"]["jwt_secret"] = json!("test-only-secret-at-least-thirty-two-characters");
        let mut config: crate::config::Config = serde_json::from_value(value).unwrap();
        config.resolve(root.path()).unwrap();
        let (router, state) = if embedded {
            let host = crate::host::startup_local_host(&config).unwrap();
            crate::application_with_host(config, host).await.unwrap()
        } else {
            crate::application(config).await.unwrap()
        };
        let token = state.auth.login_for_test("owner");
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "cookie",
            format!("microcrab_session={token}").parse().unwrap(),
        );
        let owner = state.auth.authenticate(&headers).unwrap().owner;
        state
            .manager
            .projects
            .create(
                &owner,
                serde_json::from_value(json!({
                    "id":"workspace", "name":"Workspace", "root":".", "owners":[owner.clone()]
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            state,
            base,
            token,
            server,
            _root: root,
        }
    }
    async fn create_session(&self) -> String {
        self.create_session_with_tools(json!([])).await
    }
    async fn create_session_with_tools(&self, tools: Value) -> String {
        let response = reqwest::Client::new()
            .post(format!("{}/api/v1/sessions", self.base))
            .header("cookie", format!("microcrab_session={}", self.token))
            .header("origin", "http://127.0.0.1:4096")
            .json(
                &json!({"project_id":"workspace", "operation_id":uuid::Uuid::new_v4().to_string(), "client_tools": tools}),
            )
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success(), "{}", response.status());
        response.json::<Value>().await.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned()
    }
    async fn socket(&self, path: &str, token: &str) -> Socket {
        let url = format!("{}{path}", self.base.replacen("http:", "ws:", 1));
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().insert(
            "cookie",
            format!("microcrab_session={token}").parse().unwrap(),
        );
        request
            .headers_mut()
            .insert("origin", "http://127.0.0.1:4096".parse().unwrap());
        tokio_tungstenite::connect_async(request).await.unwrap().0
    }
    async fn close(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.state.manager.close())
            .await
            .unwrap()
            .unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
async fn frame(socket: &mut Socket) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(4), socket.next())
        .await
        .expect("server frame deadline")
        .expect("socket closed before reply")
        .unwrap();
    let Message::Text(text) = message else {
        panic!("expected JSON before close, got {message:?}")
    };
    serde_json::from_str(&text).unwrap()
}
async fn result(socket: &mut Socket) -> Value {
    // Initial catch-up may be queued immediately after the subscription barrier.
    loop {
        let frame = frame(socket).await;
        if frame["type"] == "result" {
            return frame;
        }
        assert_eq!(frame["type"], "event");
    }
}
async fn closed(socket: &mut Socket) {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(Message::Text(text))) => {
                    let pending: Value = serde_json::from_str(&text).unwrap();
                    assert_eq!(
                        pending["type"], "event",
                        "unexpected extra reply before close: {pending}"
                    );
                }
                other => panic!("unexpected frame before close: {other:?}"),
            }
        }
    })
    .await
    .expect("close deadline");
}
async fn close_client(socket: &mut Socket) {
    socket.close(None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) => {
                    let pending: Value = serde_json::from_str(&text).unwrap();
                    assert_eq!(pending["type"], "event");
                }
                Some(Ok(Message::Close(_))) | None => break,
                other => panic!("unexpected close response: {other:?}"),
            }
        }
    })
    .await
    .expect("client close handshake deadline");
}
fn command(request: &str, method: &str) -> Value {
    json!({"v":1,"type":"command","request_id":request,"method":method,"params":{}})
}
async fn rpc(socket: &mut Socket, method: &str, params: Value) -> Value {
    let request = uuid::Uuid::new_v4().to_string();
    let mut body = command(&request, method);
    body["params"] = params;
    socket
        .send(Message::Text(body.to_string().into()))
        .await
        .unwrap();
    let reply = result(socket).await;
    assert_eq!(reply["request_id"], request);
    assert_eq!(reply["ok"], true, "{reply}");
    reply["data"].clone()
}
async fn bind(socket: &mut Socket, digest: &Value) -> Value {
    rpc(
        socket,
        "client_tools.bind",
        json!({
            "client_id":"same-browser-client", "catalog_digest":digest
        }),
    )
    .await
}
async fn acquire_executor(socket: &mut Socket, digest: &Value) -> Value {
    // Closing is asynchronous. Each correlated bind cooperatively waits for the
    // actual teardown to release the lease, without an arbitrary sleep.
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let binding = bind(socket, digest).await;
            if binding["role"] == "executor" {
                return binding;
            }
            assert_eq!(binding["role"], "observer");
        }
    })
    .await
    .expect("closed executor must release its socket lease")
}
fn track_event(frame: &Value, barrier: &Value, sequence: &mut u64) {
    assert_eq!(frame["type"], "event");
    assert_eq!(frame["session_id"], barrier["session_id"]);
    assert_eq!(frame["subscription_id"], barrier["subscription_id"]);
    assert_eq!(frame["generation"], barrier["generation"]);
    *sequence += 1;
    assert_eq!(frame["sequence"], *sequence, "{frame}");
}

#[tokio::test]
async fn malformed_commands_deliver_one_error_before_close_on_both_sockets() {
    let fixture = Fixture::new().await;
    let session = fixture.create_session().await;
    for path in [
        "/api/v1/ws".to_owned(),
        format!("/api/v1/sessions/{session}/ws"),
    ] {
        for malformed in ["{", "null", r#"{"type":"event"}"#] {
            let mut socket = fixture.socket(&path, &fixture.token).await;
            if path != "/api/v1/ws" {
                assert_eq!(frame(&mut socket).await["type"], "subscribed");
            }
            socket.send(Message::Text(malformed.into())).await.unwrap();
            let reply = result(&mut socket).await;
            assert_eq!(reply["request_id"], "invalid");
            assert_eq!(reply["ok"], false);
            assert_eq!(reply["error"]["code"], "invalid_command");
            closed(&mut socket).await;
        }
    }
    fixture.close().await;
}

#[tokio::test]
async fn socket_scope_rejections_are_correlated_and_keep_the_connection_open() {
    let fixture = Fixture::new().await;
    let session = fixture.create_session().await;
    let other = fixture.create_session().await;
    for (path, wrong_method) in [
        ("/api/v1/ws".to_owned(), "turn.submit"),
        (
            format!("/api/v1/sessions/{session}/ws"),
            "conversation.attach",
        ),
    ] {
        let mut socket = fixture.socket(&path, &fixture.token).await;
        if path != "/api/v1/ws" {
            assert_eq!(frame(&mut socket).await["type"], "subscribed");
        }
        for (request, mut body, code) in [
            (
                "wrong-socket",
                command("wrong-socket", wrong_method),
                "unsupported_method",
            ),
            (
                "wrong-session",
                command("wrong-session", "session.interrupt"),
                "invalid_command",
            ),
            (
                "wrong-version",
                command("wrong-version", wrong_method),
                "unsupported_version",
            ),
            (
                "still-open",
                command("still-open", "probe.alive"),
                "unsupported_method",
            ),
        ] {
            if request == "wrong-session" {
                body["session_id"] = json!(other);
            } else if request == "wrong-version" {
                body["v"] = json!(9);
            }
            socket
                .send(Message::Text(body.to_string().into()))
                .await
                .unwrap();
            let reply = result(&mut socket).await;
            assert_eq!(reply["request_id"], request);
            assert_eq!(reply["ok"], false);
            assert_eq!(reply["error"]["code"], code);
        }
        socket.close(None).await.unwrap();
    }
    fixture.close().await;
}

#[tokio::test]
async fn watermark_reconnect_catches_up_and_live_events_keep_contiguous_wire_sequence() {
    let provider_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        provider_listener.local_addr().unwrap()
    );
    let provider = axum::Router::new().route("/v1/chat/completions", axum::routing::post(|| async {
        ([("content-type", "text/event-stream")], concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"recorded thought\"}}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"recorded answer\"}}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3}}\n\n",
            "data: [DONE]\n\n"
        ))
    }));
    let provider_task = tokio::spawn(async move {
        axum::serve(provider_listener, provider).await.unwrap();
    });
    let fixture = Fixture::with_endpoint(&endpoint).await;
    let session = fixture.create_session().await;
    let path = format!("/api/v1/sessions/{session}/ws");
    let mut original = fixture.socket(&path, &fixture.token).await;
    let barrier = frame(&mut original).await;
    assert_eq!(barrier["type"], "subscribed");
    let mut view = materialize(&mut original, &barrier).await;
    let mut sequence =
        barrier["sequence"].as_u64().unwrap() + barrier["initial_events"].as_u64().unwrap();
    let operation = uuid::Uuid::new_v4().to_string();
    let mut submit = command("submit-recorded", "turn.submit");
    submit["params"] = json!({"operation_id":operation,
        "input":microcrab_session::SessionInput::text("Record one answer")});
    original
        .send(Message::Text(submit.to_string().into()))
        .await
        .unwrap();
    let (completed, high) = tokio::time::timeout(Duration::from_secs(10), async {
        let mut accepted = false;
        let mut completed = None;
        let mut archived = barrier["meta"]["display"]["high_water"]
            .as_u64()
            .unwrap_or(0);
        loop {
            let next = frame(&mut original).await;
            if next["type"] == "result" {
                assert_eq!(next["request_id"], "submit-recorded");
                assert_eq!(next["ok"], true, "{next}");
                accepted = true;
            } else {
                track_event(&next, &barrier, &mut sequence);
                assert_ne!(next["event"], "snapshot");
                if next["event"] == "model_observation"
                    && let Some(recorded) = next["data"]["archive_sequence"].as_u64()
                {
                    assert!(
                        recorded > archived,
                        "real archive records must advance the subscription barrier"
                    );
                    archived = recorded;
                }
                crate::manager::publication::apply(
                    &mut view,
                    next["event"].as_str().unwrap(),
                    &next["data"],
                );
                if next["event"] == "model_observation"
                    && let Some(recorded) = next["data"]["archive_sequence"].as_u64()
                {
                    assert!(recorded <= view["display"]["high_water"].as_u64().unwrap());
                }
                if view["session"]["status"] == "idle"
                    && view["messages"]
                        .as_array()
                        .is_some_and(|rows| rows.iter().any(|row| row["text"] == "recorded answer"))
                {
                    completed = Some(view.clone());
                }
            }
            if accepted && let Some(snapshot) = completed.take() {
                return (snapshot, archived);
            }
        }
    })
    .await
    .expect("recorded turn completion deadline");
    assert!(
        high > 1,
        "the fixture must create a real nonempty archive: {completed}"
    );
    let lineage = completed["history_view"]["lineage"].as_str().unwrap();
    close_client(&mut original).await;

    let watermark = 1u64;
    let mut reconnect = url::Url::parse(&format!("{}{path}", fixture.base)).unwrap();
    reconnect
        .query_pairs_mut()
        .append_pair("display_watermark", &watermark.to_string())
        .append_pair("history_lineage", lineage);
    let mut resumed = fixture
        .socket(
            &format!("{}?{}", reconnect.path(), reconnect.query().unwrap()),
            &fixture.token,
        )
        .await;
    let replacement = frame(&mut resumed).await;
    assert_eq!(replacement["type"], "subscribed");
    assert_ne!(replacement["subscription_id"], barrier["subscription_id"]);
    assert_eq!(replacement["meta"]["display"]["high_water"], high);
    assert_eq!(replacement["resume"], json!({"mode":"fresh"}));
    let canonical = materialize(&mut resumed, &replacement).await;
    assert_eq!(canonical["messages"], completed["messages"]);
    let mut sequence =
        replacement["sequence"].as_u64().unwrap() + replacement["initial_events"].as_u64().unwrap();

    // Queue a genuine Hub event while archive catch-up is being delivered.
    // Both sources must share one wire sequence after the subscription barrier.
    let renamed = reqwest::Client::new().patch(format!("{}/api/v1/sessions/{session}", fixture.base))
        .header("cookie", format!("microcrab_session={}", fixture.token))
        .header("origin", "http://127.0.0.1:4096")
        .json(&json!({"title":"Live rename during catch-up", "expected_revision":replacement["meta"]["session"]["revision"]}))
        .send().await.unwrap();
    assert!(renamed.status().is_success(), "{}", renamed.status());
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut settled = false;
        let mut live = false;
        let mut records = Vec::new();
        let mut after = watermark;
        loop {
            let next = frame(&mut resumed).await;
            track_event(&next, &replacement, &mut sequence);
            if next["event"] == "display.batch" || next["event"] == "display.settled" {
                let page = &next["data"];
                assert_eq!(page["history_lineage"], lineage);
                assert_eq!(page["high_water"], high);
                assert_eq!(page["after"], after);
                assert_eq!(page["coverage"], "available", "{page}");
                let scanned = page["scanned_through"].as_u64().unwrap();
                assert!(scanned >= after && scanned <= high);
                for record in page["records"].as_array().unwrap() {
                    let archived = record["sequence"].as_u64().unwrap();
                    assert!(archived > after && archived <= scanned);
                    records.push(archived);
                }
                after = scanned;
                if next["event"] == "display.settled" {
                    assert_eq!(after, high);
                    settled = true;
                }
            }
            if next["event"] == "session.meta"
                && next["data"]["session"]["title"] == "Live rename during catch-up"
            {
                live = true;
            }
            if settled && live {
                assert!(
                    !records.is_empty(),
                    "reconnect must replay the uncovered archived records"
                );
                assert!(records.windows(2).all(|pair| pair[0] < pair[1]));
                break;
            }
        }
    })
    .await
    .expect("catch-up and live event deadlines");
    close_client(&mut resumed).await;
    fixture.close().await;
    provider_task.abort();
    let _ = provider_task.await;
}

#[tokio::test]
async fn closing_executor_releases_lease_and_observer_or_stale_lease_cannot_revoke_it() {
    let fixture = Fixture::new().await;
    let session = fixture
        .create_session_with_tools(json!([{
            "name":"client_echo", "description":"Socket lease test", "parameters":{"type":"object"}
        }]))
        .await;
    let path = format!("/api/v1/sessions/{session}/ws");
    let mut executor = fixture.socket(&path, &fixture.token).await;
    let barrier = frame(&mut executor).await;
    assert_eq!(barrier["type"], "subscribed");
    let digest = &barrier["meta"]["client_tools"]["catalog_digest"];
    assert!(digest.is_string());
    let first = bind(&mut executor, digest).await;
    assert_eq!(first["role"], "executor");
    let first_lease = &first["lease_id"];
    assert!(first_lease.is_string());

    let mut observer = fixture.socket(&path, &fixture.token).await;
    assert_eq!(frame(&mut observer).await["type"], "subscribed");
    assert_eq!(bind(&mut observer, digest).await["role"], "participant");
    close_client(&mut observer).await;
    // Observer close and unrelated global-socket close leave the executor intact.
    let mut global = fixture.socket("/api/v1/ws", &fixture.token).await;
    close_client(&mut global).await;
    assert_eq!(bind(&mut executor, digest).await["lease_id"], *first_lease);

    let mut replacement = fixture.socket(&path, &fixture.token).await;
    assert_eq!(frame(&mut replacement).await["type"], "subscribed");
    assert_eq!(bind(&mut replacement, digest).await["role"], "participant");
    close_client(&mut executor).await;
    let second = acquire_executor(&mut replacement, digest).await;
    assert_ne!(second["lease_id"], *first_lease);
    assert!(second["lease_id"].is_string());

    // A command carrying the old socket's lease must not revoke the new owner.
    let stale = rpc(
        &mut replacement,
        "client_tools.unbind",
        json!({"lease_id":first_lease}),
    )
    .await;
    assert_eq!(stale["detached"], false);
    assert_eq!(
        bind(&mut replacement, digest).await["lease_id"],
        second["lease_id"]
    );

    let mut successor = fixture.socket(&path, &fixture.token).await;
    assert_eq!(frame(&mut successor).await["type"], "subscribed");
    assert_eq!(bind(&mut successor, digest).await["role"], "participant");
    close_client(&mut replacement).await;
    let third = acquire_executor(&mut successor, digest).await;
    assert_ne!(third["lease_id"], second["lease_id"]);
    close_client(&mut successor).await;
    fixture.close().await;
}

#[tokio::test]
async fn browser_sender_selects_plugin_executor_and_important_at_admission() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let gate = release.clone();
    let provider = axum::Router::new().route("/v1/chat/completions", axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
        let tx = tx.clone();
        let gate = gate.clone();
        async move {
            tx.send(body).await.unwrap();
            gate.acquire().await.unwrap().forget();
            ([("content-type", "text/event-stream")], concat!(
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"fixture answer\"}}]}\n\n",
                "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: [DONE]\n\n"
            ))
        }
    }));
    let provider_task = tokio::spawn(async move {
        axum::serve(listener, provider).await.unwrap();
    });
    let fixture = Fixture::with_endpoint(&endpoint).await;
    let session = fixture
        .create_session_with_tools(json!([{
            "name":"plugin", "description":"Executor fixture", "parameters":{"type":"object"}
        }]))
        .await;
    let path = format!("/api/v1/sessions/{session}/ws");
    let mut a = fixture.socket(&path, &fixture.token).await;
    let barrier = frame(&mut a).await;
    let digest = &barrier["meta"]["client_tools"]["catalog_digest"];
    let bind_client = |client: &str| json!({"client_id":client,"catalog_digest":digest});
    let a_seed = rpc(&mut a, "client_tools.bind", bind_client("browser-a")).await;
    assert_eq!(a_seed["role"], "executor");
    let mut b = fixture.socket(&path, &fixture.token).await;
    assert_eq!(frame(&mut b).await["type"], "subscribed");
    assert_eq!(
        rpc(&mut b, "client_tools.bind", bind_client("browser-b")).await["role"],
        "participant"
    );
    let input = |text: &str, plugin: &str, activate: bool| {
        json!({
            "operation_id":uuid::Uuid::new_v4().to_string(),"activate_if_idle":activate,
            "input":{"parts":[{"type":"text","text":text}],"variables":{"scoped_set":[{
                "name":"plugins","value":[{"id":plugin,"name":plugin,"version":"1.0.0","artifact":"0123456789abcdef","description":"fixture"}],
                "visibility":"visible","persistence":"persisted","editors":["client"]
            }]}}
        })
    };
    let read_request = |body: Value| {
        let messages = body["messages"].as_array().unwrap();
        let value = messages.iter().rev().find(|m| m["role"] == "user").unwrap()["content"].clone();
        if let Some(text) = value.as_str() {
            text.to_owned()
        } else {
            value
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|part| part["text"].as_str())
                .collect::<String>()
        }
    };
    let receive = |mut rx: tokio::sync::mpsc::Receiver<Value>| async move {
        let body = tokio::time::timeout(Duration::from_secs(20), rx.recv())
            .await
            .unwrap()
            .unwrap();
        (rx, body)
    };
    rpc(&mut a, "turn.queue", input("A-start", "plugin-a", true)).await;
    let (next, body) = receive(rx).await;
    rx = next;
    let rendered = read_request(body);
    assert!(
        rendered.contains("plugin-a") && rendered.contains("A-start"),
        "{rendered}"
    );
    assert_eq!(
        rpc(&mut a, "client_tools.bind", bind_client("browser-a")).await["lease_id"],
        a_seed["lease_id"]
    );
    rpc(&mut b, "turn.queue", input("B-queued", "plugin-b", false)).await;
    rpc(
        &mut b,
        "turn.steer",
        input("B-steer", "plugin-steer", false),
    )
    .await;
    assert_eq!(
        rpc(&mut b, "client_tools.bind", bind_client("browser-b")).await["role"],
        "participant"
    );
    release.add_permits(1);
    let (next, body) = receive(rx).await;
    rx = next;
    let rendered = read_request(body);
    assert!(
        rendered.contains("B-steer") && !rendered.contains("plugin-steer"),
        "{rendered}"
    );
    assert!(
        !rendered.contains("<important>"),
        "unchanged active environment: {rendered}"
    );
    assert_eq!(
        rpc(&mut a, "client_tools.bind", bind_client("browser-a")).await["role"],
        "executor"
    );
    release.add_permits(1);
    let (next, body) = receive(rx).await;
    rx = next;
    let rendered = read_request(body);
    assert!(
        rendered.contains("plugin-b") && rendered.contains("B-queued"),
        "{rendered}"
    );
    let state = rendered.find("</state>").unwrap();
    let important = rendered.find("<important>").unwrap();
    let user = rendered.find("<user>").unwrap();
    assert!(state < important && important < user, "{rendered}");
    assert!(rendered[important..user].contains("describe"), "{rendered}");
    assert_eq!(
        rpc(&mut b, "client_tools.bind", bind_client("browser-b")).await["role"],
        "executor"
    );
    assert_eq!(
        rpc(&mut a, "client_tools.bind", bind_client("browser-a")).await["role"],
        "participant"
    );
    // The queued sender can leave before consumption. The next request must
    // use the still-live B inventory, never the now-disconnected A's payload.
    rpc(
        &mut a,
        "turn.queue",
        input("A-offline", "plugin-offline", false),
    )
    .await;
    close_client(&mut a).await;
    release.add_permits(1);
    let (_, body) = receive(rx).await;
    let rendered = read_request(body);
    assert!(
        rendered.contains("A-offline") && !rendered.contains("plugin-offline"),
        "{rendered}"
    );
    assert!(
        !rendered.contains("<important>"),
        "unchanged active environment: {rendered}"
    );
    assert_eq!(
        rpc(&mut b, "client_tools.bind", bind_client("browser-b")).await["role"],
        "executor"
    );
    release.add_permits(1);
    close_client(&mut b).await;
    fixture.close().await;
    provider_task.abort();
}

#[tokio::test]
async fn foreign_and_missing_session_subscriptions_send_error_then_close() {
    let fixture = Fixture::new().await;
    let session = fixture.create_session().await;
    let intruder = fixture.state.auth.login_for_test("intruder");
    for (id, token) in [
        (session.as_str(), intruder.as_str()),
        ("missing-session", fixture.token.as_str()),
    ] {
        let mut socket = fixture
            .socket(&format!("/api/v1/sessions/{id}/ws"), token)
            .await;
        let refusal = frame(&mut socket).await;
        assert_eq!(refusal["type"], "error");
        assert!(refusal["error"]["code"].is_string());
        assert!(refusal["meta"].is_null());
        closed(&mut socket).await;
    }
    fixture.close().await;
}

async fn materialize(socket: &mut Socket, barrier: &Value) -> Value {
    assert!(
        barrier.get("snapshot").is_none(),
        "whole snapshot must never be pushed"
    );
    assert!(barrier["meta"].get("messages").is_none());
    assert!(barrier["pending"].get("permissions").is_some());
    assert!(barrier["pending"].get("round_limits").is_some());
    assert!(
        matches!(barrier["resume"]["mode"].as_str(), Some("fresh" | "reset")),
        "subscription resume must be an object with a supported mode: {}",
        barrier["resume"]
    );
    let mut view = barrier["meta"].clone();
    let mut sequence = barrier["sequence"].as_u64().unwrap();
    let count = barrier["initial_events"].as_u64().unwrap();
    assert!(count >= 1);
    let mut canonical_page = false;
    for _ in 0..count {
        let event = frame(socket).await;
        track_event(&event, barrier, &mut sequence);
        canonical_page |= event["event"] == "blocks.page";
        assert_ne!(event["event"], "snapshot");
        crate::manager::publication::apply(
            &mut view,
            event["event"].as_str().unwrap(),
            &event["data"],
        );
    }
    assert!(
        canonical_page,
        "every opening supplies the complete canonical page"
    );
    view
}

#[tokio::test]
async fn owner_watch_is_global_and_foreign_changes_never_enter_its_sequence() {
    let fixture = Fixture::new().await;
    let session = fixture.create_session().await;
    let mut global = fixture.socket("/api/v1/ws", &fixture.token).await;
    let watch = rpc(&mut global, "sessions.watch", json!({})).await;
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "cookie",
        format!("microcrab_session={}", fixture.token)
            .parse()
            .unwrap(),
    );
    let owner = fixture.state.auth.authenticate(&headers).unwrap().owner;
    fixture.state.manager.session_cleanup_failed(
        "foreign-owner",
        "foreign-session",
        "bytes",
        json!({"message":"private foreign failure"}),
    );
    fixture.state.manager.session_cleanup_failed(
        &owner,
        &session,
        "bytes",
        json!({"message":"own failure"}),
    );
    let mut sequence = 0u64;
    let event = loop {
        let event = frame(&mut global).await;
        assert_eq!(event["type"], "event");
        assert_eq!(event["session_id"], "");
        assert_eq!(event["subscription_id"], watch["subscription_id"]);
        assert_eq!(event["generation"], watch["generation"]);
        sequence += 1;
        assert_eq!(event["sequence"], sequence);
        assert_ne!(event["data"]["session_id"], "foreign-session");
        if event["event"] == "session.cleanup_failed" {
            break event;
        }
        assert_eq!(event["event"], "sessions.changed");
    };
    assert_eq!(event["data"]["session_id"], session);
    assert_eq!(event["data"]["cause"]["message"], "own failure");
    let mut selected = fixture
        .socket(&format!("/api/v1/sessions/{session}/ws"), &fixture.token)
        .await;
    let barrier = frame(&mut selected).await;
    materialize(&mut selected, &barrier).await;
    selected
        .send(Message::Text(
            command("watch-on-session", "sessions.watch")
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        result(&mut selected).await["error"]["code"],
        "unsupported_method"
    );
    let page = rpc(&mut selected, "session.blocks", json!({})).await;
    assert!(page["items"].is_array());
    close_client(&mut selected).await;
    close_client(&mut global).await;
    fixture.close().await;
}

#[tokio::test]
async fn native_relay_uses_binary_global_frames_and_keeps_socket_intakes_isolated() {
    use microcrab_host_protocol::{HostFrame, decode_frame};
    // Native Project attachment belongs to standalone Server; explicit Host
    // embeddings intentionally reject pairing scope construction.
    let fixture = Fixture::standalone().await;
    let selected_fixture = Fixture::new().await;
    let session = selected_fixture.create_session().await;
    let mut global = fixture.socket("/api/v1/ws", &fixture.token).await;
    let mut attach = command("relay-attach", "host.relay_attach");
    attach["params"] = json!({"project_id":"workspace", "url":"ws://127.0.0.1:9/host", "token":"test-pair", "allow_legacy_binding":true});
    global
        .send(Message::Text(attach.to_string().into()))
        .await
        .unwrap();
    let handshake = tokio::time::timeout(Duration::from_secs(4), global.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Binary(bytes) = handshake else {
        panic!("native relay handshake must remain binary: {handshake:?}")
    };
    assert!(
        matches!(decode_frame(&bytes).unwrap(), HostFrame::Inspect { request } if matches!(&request.authentication, microcrab_host_protocol::InspectAuthentication::Pair { token } if token == "test-pair"))
    );

    // An unrelated browser's binary data must never satisfy or fail this relay's
    // handshake. Correlated probes witness both readers still making progress.
    let mut unrelated = fixture.socket("/api/v1/ws", &fixture.token).await;
    unrelated
        .send(Message::Binary(vec![255].into()))
        .await
        .unwrap();
    unrelated
        .send(Message::Text(
            command("unrelated-probe", "probe.alive").to_string().into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        result(&mut unrelated).await["request_id"],
        "unrelated-probe"
    );
    global
        .send(Message::Text(
            command("relay-probe", "probe.alive").to_string().into(),
        ))
        .await
        .unwrap();
    assert_eq!(result(&mut global).await["request_id"], "relay-probe");

    let mut selected = selected_fixture
        .socket(
            &format!("/api/v1/sessions/{session}/ws"),
            &selected_fixture.token,
        )
        .await;
    assert_eq!(frame(&mut selected).await["type"], "subscribed");
    selected
        .send(Message::Text(attach.to_string().into()))
        .await
        .unwrap();
    assert_eq!(
        result(&mut selected).await["error"]["code"],
        "unsupported_method"
    );
    selected
        .send(Message::Binary(vec![255].into()))
        .await
        .unwrap();
    closed(&mut selected).await;

    global
        .send(Message::Binary(vec![255].into()))
        .await
        .unwrap();
    let failure = result(&mut global).await;
    assert_eq!(failure["request_id"], "relay-attach");
    assert_eq!(failure["ok"], false);
    assert!(
        failure["error"]["message"]
            .as_str()
            .unwrap()
            .contains("invalid handshake frame"),
        "{failure}"
    );
    global
        .send(Message::Text(
            command("after-failure", "probe.alive").to_string().into(),
        ))
        .await
        .unwrap();
    assert_eq!(result(&mut global).await["request_id"], "after-failure");
    close_client(&mut unrelated).await;
    close_client(&mut global).await;
    selected_fixture.close().await;
    fixture.close().await;
}

#[tokio::test]
async fn native_session_fastpaths_log_the_actual_completion_result() {
    let capture = crate::route::provenance::testing::capture();
    let fixture = Fixture::new().await;
    let session = fixture.create_session().await;
    let mut socket = fixture
        .socket(&format!("/api/v1/sessions/{session}/ws"), &fixture.token)
        .await;
    assert_eq!(frame(&mut socket).await["type"], "subscribed");
    for method in [
        "display.subscribe",
        "round_limit.reply",
        "permission.reply",
        "client_tools.bind",
        "client_tools.result",
        "client_tools.unbind",
        "client_tools.prepare",
        "client_tools.output",
        "client_tools.takeover",
        "client_tools.arguments",
        "client_tools.secret",
        "client_tools.secret_operation",
        "client_tools.decision",
    ] {
        let id = uuid::Uuid::new_v4().to_string();
        let mut body = command(&id, method);
        // Unknown fields drive each real fast-path validation handler. Neither
        // their names nor values may cross the diagnostic boundary.
        body["params"] = json!({"private-field-never-logged":"private-content-never-logged"});
        socket
            .send(Message::Text(body.to_string().into()))
            .await
            .unwrap();
        let reply = result(&mut socket).await;
        assert_eq!(reply["request_id"], id);
        assert_eq!(reply["ok"], false, "{method}: {reply}");
        let text = capture.text();
        let line = text
            .lines()
            .find(|line| line.contains(&id) && line.contains("WS command completed"))
            .unwrap();
        assert!(line.contains(method), "{line}");
        assert!(line.contains("success=false"), "{line}");
        assert!(
            line.contains(reply["error"]["code"].as_str().unwrap()),
            "{line}"
        );
        assert!(line.contains(&session), "{line}");
        assert!(line.contains("socket_id="), "{line}");
        assert!(line.contains("elapsed_ms="), "{line}");
    }
    // A successful subscription acknowledgement must also report success.
    let id = uuid::Uuid::new_v4().to_string();
    socket
        .send(Message::Text(
            command(&id, "display.subscribe").to_string().into(),
        ))
        .await
        .unwrap();
    assert_eq!(result(&mut socket).await["ok"], true);
    let text = capture.text();
    let line = text
        .lines()
        .find(|line| line.contains(&id) && line.contains("WS command completed"))
        .unwrap();
    assert!(line.contains("success=true"), "{line}");
    assert!(line.contains("error_code=\"none\""), "{line}");
    assert!(!text.contains("private-field-never-logged"));
    assert!(!text.contains("private-content-never-logged"));
    close_client(&mut socket).await;
    fixture.close().await;
}
