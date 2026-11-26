//! Authenticated Personal Content Library HTTP contract tests:
//! user_id == principal.owner isolation, cross-session content reading,
//! quota enforcement without LRU, and session/project mounting.
use super::{attachments::png, variables::create, *};

fn owner_id(fixture: &Fixture) -> String {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "cookie",
        format!("microcrab_session={}", fixture.token)
            .parse()
            .unwrap(),
    );
    fixture.state.auth.authenticate(&headers).unwrap().owner
}

#[tokio::test]
async fn library_enforces_user_id_isolation_and_cross_session_content_delivery() {
    let fixture = Fixture::new(true).await;
    let user_id = owner_id(&fixture);
    let session_a = create(&fixture).await;
    let session_b = create(&fixture).await;
    assert_ne!(session_a, session_b);

    // Identity route mirrors user_id cleanly.
    let ident = body(fixture.request("GET", "/api/v1/identity", None).await).await;
    assert_eq!(ident["user_id"], user_id);
    assert_eq!(ident["id"], user_id);

    // Wrong user_id path is forbidden even for another valid signed-in user.
    let other_token = fixture.state.auth.login_for_test("other");
    let forbidden = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/users/{user_id}/library/objects"))
                .header("cookie", format!("microcrab_session={other_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

    // Raw PNG upload mounted into session_a also enters owner's Personal Library.
    let upload_uri = format!(
        "/api/v1/users/{user_id}/library/objects?title=diagram&mount_session_id={session_a}"
    );
    let created_res = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&upload_uri)
                .header("cookie", format!("microcrab_session={}", fixture.token))
                .header("origin", "http://127.0.0.1:4096")
                .header("content-type", "image/png")
                .header("content-length", png().len())
                .body(Body::from(png()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created_res.status(), StatusCode::OK);
    let created = body(created_res).await;
    let ref_id = created["id"].as_str().unwrap().to_owned();
    assert_eq!(created["source_kind"], "upload");
    assert_eq!(created["media_type"], "image/png");
    assert_eq!(created["mounted_sessions"], json!([session_a]));

    // Cross-session content endpoint serves the uploaded PNG even for session_b (unmounted).
    let content_res = fixture
        .request(
            "GET",
            &format!("/api/v1/users/{user_id}/library/objects/{ref_id}/content"),
            None,
        )
        .await;
    assert_eq!(content_res.status(), StatusCode::OK);
    assert_eq!(
        content_res.headers().get("content-type").unwrap(),
        "image/png"
    );
    assert_eq!(
        content_res.headers().get("content-disposition").unwrap(),
        "inline"
    );
    assert_eq!(
        content_res.headers().get("cache-control").unwrap(),
        "no-store"
    );
    assert_eq!(
        content_res
            .headers()
            .get("x-content-type-options")
            .unwrap(),
        "nosniff"
    );
    assert_eq!(
        content_res
            .headers()
            .get("content-security-policy")
            .unwrap(),
        "default-src 'none'; sandbox"
    );
    let bytes = to_bytes(content_res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), png().as_slice());

    // Text/non-raster upload serves Content-Disposition: attachment.
    let txt_res = fixture
        .request(
            "POST",
            &format!("/api/v1/users/{user_id}/library/objects"),
            Some(json!({
                "title": "notes",
                "media_type": "text/plain",
                "text": "<script>alert(1)</script>"
            })),
        )
        .await;
    assert_eq!(txt_res.status(), StatusCode::OK);
    let txt_id = body(txt_res).await["id"].as_str().unwrap().to_owned();
    let txt_content = fixture
        .request(
            "GET",
            &format!("/api/v1/users/{user_id}/library/objects/{txt_id}/content"),
            None,
        )
        .await;
    assert_eq!(txt_content.status(), StatusCode::OK);
    assert_eq!(
        txt_content.headers().get("content-disposition").unwrap(),
        "attachment"
    );

    // Mount session_b on ref_id, verify session_b state projection sees it, then delete frees quota.
    let mount_res = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!(
                    "/api/v1/users/{user_id}/library/objects/{ref_id}/mounts/sessions/{session_b}"
                ))
                .header("cookie", format!("microcrab_session={}", fixture.token))
                .header("origin", "http://127.0.0.1:4096")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(mount_res.status(), StatusCode::OK);
    let mounted = body(mount_res).await;
    assert_eq!(mounted["mounted_sessions"], json!([session_a, session_b]));

    let inspected_b = fixture.inspect(&session_b).await.unwrap();
    let refs_b = inspected_b["state"]["references"].as_array().unwrap();
    assert!(refs_b.iter().any(|entry| entry["id"] == ref_id));

    // Composer attachment upload still returns attachment_id AND mounts reference_id into session_a.
    let attach_res = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/sessions/{session_a}/attachments"))
                .header("cookie", format!("microcrab_session={}", fixture.token))
                .header("origin", "http://127.0.0.1:4096")
                .header("content-type", "image/png")
                .header("content-length", png().len())
                .body(Body::from(png()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(attach_res.status(), StatusCode::OK);
    let attach_json = body(attach_res).await;
    assert!(attach_json["attachment_id"].is_string());
    assert!(attach_json["reference_id"]
        .as_str()
        .is_some_and(|s| s.starts_with("ref_")));

    // Delete removes the blob and reduces quota.
    let quota_before = body(
        fixture
            .request("GET", &format!("/api/v1/users/{user_id}/library/quota"), None)
            .await,
    )
    .await;
    let del = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/v1/users/{user_id}/library/objects/{ref_id}"))
                .header("cookie", format!("microcrab_session={}", fixture.token))
                .header("origin", "http://127.0.0.1:4096")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(del.status(), StatusCode::OK);
    let quota_after = body(del).await["quota"].clone();
    assert!(
        quota_after["used_bytes"].as_u64().unwrap()
            < quota_before["used_bytes"].as_u64().unwrap()
    );

    // State catalog exposes unified `reference` builtin (and never `view_image` / `expand`).
    let view = fixture.inspect(&session_a).await.unwrap();
    let prompt = format!(
        "/api/v1/sessions/{session_a}/history/prompt?history_lineage={}&epoch={}",
        view["history_view"]["lineage"].as_str().unwrap(),
        view["history_view"]["epoch"]
    );
    let catalog = body(fixture.request("GET", &prompt, None).await).await;
    let tool_names: Vec<&str> = catalog["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["name"].as_str())
        .collect();
    assert!(tool_names.contains(&"reference"));
    assert!(!tool_names.contains(&"view_image"));
    assert!(!tool_names.contains(&"expand"));

    fixture.state.manager.close().await.unwrap();
}
