use super::*;

#[tokio::test]
async fn public_model_metadata_is_authenticated_and_manual_refresh_requires_origin() {
    let fixture = Fixture::new(false).await;
    let catalog = body(fixture.request("GET", "/api/v1/models", None).await).await;
    assert_eq!(catalog["metadata_status"]["models"], 0);
    assert!(catalog["metadata_status"]["fetched_at_ms"].is_null());
    let metadata = fixture
        .request("GET", "/api/v1/models/metadata", None)
        .await;
    assert_eq!(metadata.status(), StatusCode::OK);
    let metadata = body(metadata).await;
    assert!(
        metadata["snapshot"]["models"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    for (path, method) in [
        ("/api/v1/models/metadata", "GET"),
        ("/api/v1/models/metadata/refresh", "POST"),
    ] {
        let unauth = fixture
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);
    }
    let no_origin = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/models/metadata/refresh")
                .header("cookie", format!("microcrab_session={}", fixture.token))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(no_origin.status(), StatusCode::FORBIDDEN);
    assert_eq!(body(no_origin).await["error"]["code"], "origin_not_allowed");
    // Invalid arguments are rejected before the optional public network fetch.
    let invalid = fixture
        .request(
            "POST",
            "/api/v1/models/metadata/refresh",
            Some(json!({"unexpected":true})),
        )
        .await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    fixture.state.manager.close().await.unwrap();
}
