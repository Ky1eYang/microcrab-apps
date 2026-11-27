use super::*;
use axum::{Router, routing::get};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::TcpListener;

#[test]
fn parses_github_and_gitee_skill_urls() {
    let vercel = parse_git_skill_repo_input(&AddGitSkillRepoInput {
        url: Some("https://github.com/vercel-labs/skills/tree/main/skills".into()),
        provider: None,
        owner: None,
        repo: None,
        branch: None,
        root_path: None,
        api_base: None,
    })
    .unwrap();
    assert_eq!(vercel.provider, GitProvider::GitHub);
    assert_eq!(vercel.owner, "vercel-labs");
    assert_eq!(vercel.repo, "skills");
    assert_eq!(vercel.branch.as_deref(), Some("main"));
    assert_eq!(vercel.root_path, "skills");

    let mattpocock = parse_git_skill_repo_input(&AddGitSkillRepoInput {
        url: Some("https://github.com/mattpocock/skills".into()),
        provider: None,
        owner: None,
        repo: None,
        branch: None,
        root_path: None,
        api_base: None,
    })
    .unwrap();
    assert_eq!(mattpocock.provider, GitProvider::GitHub);
    assert_eq!(mattpocock.owner, "mattpocock");
    assert_eq!(mattpocock.repo, "skills");
    assert_eq!(mattpocock.branch, None);
    assert_eq!(mattpocock.root_path, "");

    let gitee = parse_git_skill_repo_input(&AddGitSkillRepoInput {
        url: Some("https://gitee.com/myorg/agent-skills/tree/master/skills".into()),
        provider: None,
        owner: None,
        repo: None,
        branch: None,
        root_path: None,
        api_base: None,
    })
    .unwrap();
    assert_eq!(gitee.provider, GitProvider::Gitee);
    assert_eq!(gitee.branch.as_deref(), Some("master"));
    assert_eq!(gitee.root_path, "skills");
}

#[tokio::test]
async fn probe_pins_commit_filters_clutter_caches_whole_file_and_isolates_errors() {
    let file_hits = Arc::new(AtomicUsize::new(0));
    let fail_expand = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hits_ref = file_hits.clone();
    let fail_ref = fail_expand.clone();

    let skill_md = "---\nname: web-design\ndescription: Modern UI guidance\ntitle_depth: 2\n---\n# Overview\nCore principles.\n## Forms\nUse native validation.\n";
    let guide_md = "# Security Guide\nSanitize HTML.\n## CSP\nUse strict CSP.\n";

    let app = Router::new()
        .route(
            "/repos/vercel-labs/skills",
            get(|| async { axum::Json(json!({ "default_branch": "main" })) }),
        )
        .route(
            "/repos/vercel-labs/skills/commits/main",
            get(|| async { axum::Json(json!({ "sha": "abc123def4567890abc123def4567890deadbeef" })) }),
        )
        .route(
            "/repos/vercel-labs/skills/git/trees/abc123def4567890abc123def4567890deadbeef",
            get(|| async {
                axum::Json(json!({
                    "truncated": false,
                    "tree": [
                        { "path": "package.json", "type": "blob", "sha": "111", "size": 20 },
                        { "path": "LICENSE", "type": "blob", "sha": "222", "size": 50 },
                        { "path": "skills/web-design/SKILL.md", "type": "blob", "sha": "333", "size": 100 },
                        { "path": "skills/web-design/guides/security.md", "type": "blob", "sha": "444", "size": 80 },
                        { "path": "skills/web-design/node_modules/pkg/index.js", "type": "blob", "sha": "555", "size": 10 }
                    ]
                }))
            }),
        )
        .route(
            "/repos/vercel-labs/skills/contents/skills/web-design/SKILL.md",
            get(move || {
                let encoded = base64::engine::general_purpose::STANDARD.encode(skill_md.as_bytes());
                async move {
                    axum::Json(json!({
                        "encoding": "base64",
                        "content": encoded,
                    }))
                }
            }),
        )
        .route(
            "/repos/vercel-labs/skills/contents/skills/web-design/guides/security.md",
            get(move || {
                let hits = hits_ref.clone();
                let fail = fail_ref.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    if fail.load(Ordering::SeqCst) {
                        return (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            "upstream exploded",
                        )
                            .into_response();
                    }
                    let encoded =
                        base64::engine::general_purpose::STANDARD.encode(guide_md.as_bytes());
                    axum::Json(json!({
                        "encoding": "base64",
                        "content": encoded,
                    }))
                    .into_response()
                }
            }),
        );

    use axum::response::IntoResponse as _;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let cache = Arc::new(RwLock::new(BoundedFileCache::default()));
    let spec = parse_git_skill_repo_input(&AddGitSkillRepoInput {
        url: Some("https://github.com/vercel-labs/skills/tree/main/skills".into()),
        provider: None,
        owner: None,
        repo: None,
        branch: None,
        root_path: None,
        api_base: Some(format!("http://{addr}")),
    })
    .unwrap();
    let probed = probe_repo(&spec, &cache, None).await.unwrap();
    assert_eq!(
        probed.commit_hash,
        "abc123def4567890abc123def4567890deadbeef"
    );
    assert_eq!(probed.skills.len(), 1);
    assert_eq!(probed.skills[0].name, "web-design");
    // Noise files (package.json, LICENSE, node_modules) filtered out; only guides/security.md retained.
    assert_eq!(probed.skills[0].resources.len(), 1);
    assert_eq!(
        probed.skills[0].resources[0].relative_path,
        "guides/security.md"
    );

    // Load into GitSkillsReference using a fresh cache so file_hits tests lazy expand + whole-file cache.
    let runtime_cache = Arc::new(RwLock::new(BoundedFileCache::default()));
    let reference = GitSkillsReference::new("skills", vec![probed.clone()], runtime_cache);
    let cancel = CancellationToken::new();
    reference.load(cancel.clone()).await.unwrap();

    let entries = reference.list().unwrap();
    // Expand SKILL.md section -> works cleanly.
    let overview = entries
        .iter()
        .find(|e| e.node_kind == ReferenceNodeKind::Section && e.title == "Overview")
        .unwrap();
    let expanded_overview = reference
        .expand(&overview.id, cancel.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(expanded_overview.as_text().unwrap().contains("Core principles."));

    // Expand guides/security.md file twice -> only 1 HTTP call because whole file is cached in RAM.
    let guide_file = entries
        .iter()
        .find(|e| e.location.as_deref() == Some("skills/web-design/guides/security.md"))
        .unwrap();
    let first = reference
        .expand(&guide_file.id, cancel.clone())
        .await
        .unwrap()
        .unwrap();
    let second = reference
        .expand(&guide_file.id, cancel.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.as_text(), Some(guide_md));
    assert_eq!(second.as_text(), Some(guide_md));
    assert_eq!(file_hits.load(Ordering::SeqCst), 1);

    // Clear cache and force expand failure -> concrete error is reported explicitly.
    let broken_cache = Arc::new(RwLock::new(BoundedFileCache::default()));
    let ref_err = GitSkillsReference::new("skills", vec![probed.clone()], broken_cache);
    ref_err.load(cancel.clone()).await.unwrap();
    fail_expand.store(true, Ordering::SeqCst);
    let guide_id = ref_err
        .list()
        .unwrap()
        .into_iter()
        .find(|e| e.location.as_deref() == Some("skills/web-design/guides/security.md"))
        .unwrap()
        .id;
    let err = ref_err.expand(&guide_id, cancel.clone()).await.unwrap_err();
    assert!(err.to_string().contains("500") || err.to_string().contains("upstream exploded"));

    // Unreachable repo on initial SessionState load() is silently hidden instead of failing load().
    let mut bad_repo = probed;
    bad_repo.api_base = Some("http://127.0.0.1:1".into());
    let hidden_ref = GitSkillsReference::new(
        "skills",
        vec![bad_repo],
        Arc::new(RwLock::new(BoundedFileCache::default())),
    );
    hidden_ref.load(cancel).await.unwrap();
    assert!(hidden_ref.projections().unwrap().exact.is_none());

    server.abort();
}
