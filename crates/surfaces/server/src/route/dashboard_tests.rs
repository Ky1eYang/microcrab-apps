use super::{Fixture, body};
use crate::manager::dashboard::{Model, Outcome, Terminal};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use microcrab_protocol::Usage;
use serde_json::json;
use tower::ServiceExt;

#[tokio::test]
async fn owner_reads_are_authenticated_and_never_infer_unreported_usage() {
    let fixture = Fixture::new(true).await;
    let owner = fixture.owner_of(&fixture.token);
    let url = format!("/api/v1/users/{owner}/dashboard");
    let summary = format!("{url}/summary?range=24h&source=relay");
    for path in [
        &summary,
        &format!("{url}/logs"),
        &format!("{url}/cache-diagnostics"),
        &format!("{url}/logs.csv"),
    ] {
        let bearer = fixture
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("authorization", "Bearer mc_sk_not_a_session")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bearer.status(), StatusCode::UNAUTHORIZED);
        let other_token = fixture.state.auth.login_for_test("other");
        let other = fixture
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("cookie", format!("microcrab_session={other_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(other.status(), StatusCode::FORBIDDEN);
    }
    let send = |owner: &str, request: &str, usage| {
        fixture.state.manager.dashboard.record_relay(
            owner,
            "key-id",
            "=FORMULA()",
            request,
            Model {
                provider_id: "example".into(),
                model: "no-reference-price".into(),
                protocol: Some(microcrab_transport::Protocol::Google),
            },
            Terminal {
                outcome: Outcome::Accepted,
                status_code: Some(200),
                latency_ms: Some(100),
                ttft_ms: None,
                usage,
            },
        );
    };
    send(&owner, "ours-1", None);
    send(
        &owner,
        "ours-2",
        Some(Usage {
            input_tokens: Some(20),
            cache_read_tokens: Some(10),
            samples: 1,
            ..Usage::default()
        }),
    );
    send("other-owner", "theirs", None);
    let response = fixture.request("GET", &summary, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let data = body(response).await;
    assert_eq!(data["kpi"]["requests"], 2);
    assert_eq!(data["kpi"]["usage"]["input_tokens"]["reported_total"], 20);
    assert_eq!(
        data["kpi"]["usage"]["input_tokens"]["unreported_samples"],
        1
    );
    assert_eq!(data["kpi"]["reported_cache"]["cache_hit_rate"], 0.5);
    assert_eq!(data["kpi"]["reported_cache"]["paired_samples"], 1);
    assert_eq!(data["kpi"]["estimated_cost_usd"], json!(null));
    assert_eq!(data["kpi"]["unpriced_requests"], 2);
    assert_eq!(data["coverage"]["backfilled"], false);
    assert_eq!(data["series"].as_array().unwrap().len(), 24);
    let first = body(
        fixture
            .request("GET", &format!("{url}/logs?limit=1&source=relay"), None)
            .await,
    )
    .await;
    let cursor = first["next_cursor"].as_str().unwrap();
    let second = body(
        fixture
            .request(
                "GET",
                &format!("{url}/logs?limit=1&source=relay&cursor={cursor}"),
                None,
            )
            .await,
    )
    .await;
    assert_ne!(
        first["items"][0]["request_id"],
        second["items"][0]["request_id"]
    );
    assert_eq!(second["next_cursor"], json!(null));
    let csv = fixture
        .request(
            "GET",
            &format!("{url}/logs.csv?q=ours-2&source=relay"),
            None,
        )
        .await;
    assert_eq!(csv.status(), StatusCode::OK);
    let raw = axum::body::to_bytes(csv.into_body(), 4096).await.unwrap();
    let text = std::str::from_utf8(&raw).unwrap();
    assert!(text.contains("ours-2"));
    assert!(!text.contains("ours-1"));
    assert!(!text.contains("theirs"));
    assert!(text.contains("\"'=FORMULA()\""));
    fixture.state.manager.close().await.unwrap();
}
