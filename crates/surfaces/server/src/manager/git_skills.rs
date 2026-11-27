//! Remote public Git repository Skills discovery (GitHub & Gitee),
//! probe-filter-pin to immutable commit hash, and whole-file memory cache.
use super::{Manager, safe};
use async_trait::async_trait;
use base64::Engine as _;
use microcrab_host_api::markdown::{
    DocumentRevision, MarkdownDocument, compact_markdown_node, is_markdown_path,
    is_skill_manifest_path, is_valid_skill_file, markdown_metadata_json, parse_markdown,
    stable_reference_id,
};
use microcrab_protocol::application::WireError;
use microcrab_session::{
    Error, Reference, ReferenceCapabilities, ReferenceContent, ReferenceEntry, ReferenceError,
    ReferenceNodeKind, ReferencePayload, ReferenceProjection,
    state::reference::detect_mime_from_bytes,
};
use microcrab_storage::configuration::Configuration;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, RwLock};
use tokio_util::sync::CancellationToken;

const CONFIG_KIND: &str = "user_git_skills";
const CONFIG_TARGET: &str = "default";
const MAX_REPOS_PER_USER: usize = 32;
const MAX_SKILLS_PER_REPO: usize = 128;
const MAX_FILE_BYTES: usize = 2 * 1024 * 1024;
const MAX_CACHE_BYTES: usize = 64 * 1024 * 1024;
const MAX_CACHE_FILES: usize = 512;
const HTTP_TIMEOUT_SECS: u64 = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GitProvider {
    #[serde(rename = "github")]
    GitHub,
    #[serde(rename = "gitee")]
    Gitee,
}

impl GitProvider {
    fn as_str(self) -> &'static str {
        match self {
            Self::GitHub => "github",
            Self::Gitee => "gitee",
        }
    }

    fn default_api_base(self) -> &'static str {
        match self {
            Self::GitHub => "https://api.github.com",
            Self::Gitee => "https://gitee.com/api/v5",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProbedSkillFile {
    pub path: String,
    pub relative_path: String,
    pub blob_sha: String,
    pub size: u64,
    pub media_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProbedGitSkill {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    pub skill_dir: String,
    pub manifest_path: String,
    pub manifest_blob_sha: String,
    pub manifest_digest: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<ProbedSkillFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GitSkillRepoConfig {
    pub id: String,
    pub provider: GitProvider,
    pub owner: String,
    pub repo: String,
    pub branch: String,
    #[serde(default)]
    pub root_path: String,
    pub commit_hash: String,
    pub probed_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_base: Option<String>,
    #[serde(default)]
    pub skills: Vec<ProbedGitSkill>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AddGitSkillRepoInput {
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub provider: Option<GitProvider>,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub root_path: Option<String>,
    #[serde(default)]
    pub api_base: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedGitRepoSpec {
    pub provider: GitProvider,
    pub owner: String,
    pub repo: String,
    pub branch: Option<String>,
    pub root_path: String,
    pub api_base: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct StoredGitSkillRepos {
    #[serde(default)]
    repos: Vec<GitSkillRepoConfig>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CacheKey {
    provider: GitProvider,
    owner: String,
    repo: String,
    commit_hash: String,
    path: String,
}

#[derive(Clone)]
struct CachedGitFile {
    media_type: String,
    payload: ReferencePayload,
    sections: BTreeMap<String, String>,
    byte_len: usize,
}

#[derive(Default)]
pub(crate) struct BoundedFileCache {
    entries: BTreeMap<CacheKey, CachedGitFile>,
    order: VecDeque<CacheKey>,
    used_bytes: usize,
}

impl BoundedFileCache {
    fn get(&self, key: &CacheKey) -> Option<CachedGitFile> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: CacheKey, file: CachedGitFile) {
        if let Some(existing) = self.entries.remove(&key) {
            self.used_bytes = self.used_bytes.saturating_sub(existing.byte_len);
            self.order.retain(|k| k != &key);
        }
        if file.byte_len > MAX_CACHE_BYTES {
            return;
        }
        while self.entries.len() >= MAX_CACHE_FILES
            || self.used_bytes.saturating_add(file.byte_len) > MAX_CACHE_BYTES
        {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(removed) = self.entries.remove(&oldest) {
                self.used_bytes = self.used_bytes.saturating_sub(removed.byte_len);
            }
        }
        self.used_bytes = self.used_bytes.saturating_add(file.byte_len);
        self.order.push_back(key.clone());
        self.entries.insert(key, file);
    }
}

/// Parse GitHub/Gitee repository URLs such as:
/// - `https://github.com/vercel-labs/skills/tree/main/skills`
/// - `https://github.com/mattpocock/skills`
/// - `https://gitee.com/owner/repo/tree/master/skills`
pub(crate) fn parse_git_skill_repo_input(
    input: &AddGitSkillRepoInput,
) -> Result<ParsedGitRepoSpec, WireError> {
    if let Some(raw_url) = input.url.as_deref() {
        let parsed = url::Url::parse(raw_url.trim())
            .map_err(|_| WireError::new("invalid_params", "Invalid Git repository URL"))?;
        if parsed.scheme() != "https"
            || !parsed.username().is_empty()
            || parsed.password().is_some()
        {
            return Err(WireError::new(
                "invalid_params",
                "Git repository URL must use HTTPS without credentials",
            ));
        }
        let host = parsed
            .host_str()
            .ok_or_else(|| WireError::new("invalid_params", "Repository URL missing host"))?
            .to_ascii_lowercase();
        let provider = match host.as_str() {
            "github.com" | "www.github.com" => GitProvider::GitHub,
            "gitee.com" | "www.gitee.com" => GitProvider::Gitee,
            _ => {
                return Err(WireError::new(
                    "invalid_params",
                    "Only public GitHub and Gitee repositories are supported",
                ));
            }
        };
        let segs: Vec<&str> = parsed
            .path_segments()
            .map(|s| s.filter(|p| !p.is_empty()).collect())
            .unwrap_or_default();
        if segs.len() < 2 {
            return Err(WireError::new(
                "invalid_params",
                "Repository URL must include owner and repository name",
            ));
        }
        let owner = validate_slug(segs[0], "owner")?;
        let repo_raw = segs[1].strip_suffix(".git").unwrap_or(segs[1]);
        let repo = validate_slug(repo_raw, "repo")?;
        let (branch, root_path) = if segs.len() >= 4 && segs[2] == "tree" {
            let br = validate_branch(segs[3])?;
            let sub = segs[4..].join("/");
            (Some(br), normalize_subpath(&sub)?)
        } else if segs.len() == 2 {
            (
                input
                    .branch
                    .as_deref()
                    .map(validate_branch)
                    .transpose()?,
                input
                    .root_path
                    .as_deref()
                    .map(normalize_subpath)
                    .transpose()?
                    .unwrap_or_default(),
            )
        } else {
            return Err(WireError::new(
                "invalid_params",
                "Unsupported repository URL path; use repo root or /tree/<branch>[/<path>]",
            ));
        };
        return Ok(ParsedGitRepoSpec {
            provider,
            owner,
            repo,
            branch,
            root_path,
            api_base: normalize_api_base(input.api_base.as_deref())?,
        });
    }

    let provider = input.provider.ok_or_else(|| {
        WireError::new(
            "invalid_params",
            "Specify either repository `url` or explicit `provider`, `owner`, and `repo`",
        )
    })?;
    let owner = validate_slug(
        input
            .owner
            .as_deref()
            .ok_or_else(|| WireError::new("invalid_params", "Repository owner is required"))?,
        "owner",
    )?;
    let repo = validate_slug(
        input
            .repo
            .as_deref()
            .ok_or_else(|| WireError::new("invalid_params", "Repository name is required"))?,
        "repo",
    )?;
    let branch = input
        .branch
        .as_deref()
        .map(validate_branch)
        .transpose()?;
    let root_path = input
        .root_path
        .as_deref()
        .map(normalize_subpath)
        .transpose()?
        .unwrap_or_default();
    Ok(ParsedGitRepoSpec {
        provider,
        owner,
        repo,
        branch,
        root_path,
        api_base: normalize_api_base(input.api_base.as_deref())?,
    })
}

fn validate_slug(raw: &str, label: &str) -> Result<String, WireError> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.len() > 128
        || trimmed.contains(['/', '\\', '?', '#', '\0'])
        || trimmed == "."
        || trimmed == ".."
    {
        return Err(WireError::new(
            "invalid_params",
            format!("Invalid repository {label}"),
        ));
    }
    Ok(trimmed.to_owned())
}

fn validate_branch(raw: &str) -> Result<String, WireError> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.len() > 256
        || trimmed.contains(['?', '#', '\0'])
        || trimmed.contains("..")
    {
        return Err(WireError::new("invalid_params", "Invalid branch name"));
    }
    Ok(trimmed.to_owned())
}

fn normalize_subpath(raw: &str) -> Result<String, WireError> {
    let trimmed = raw.trim().trim_matches('/');
    if trimmed.contains('\0') || trimmed.contains("..") {
        return Err(WireError::new("invalid_params", "Invalid repository subpath"));
    }
    Ok(trimmed.to_owned())
}

fn normalize_api_base(raw: Option<&str>) -> Result<Option<String>, WireError> {
    let Some(base) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let parsed = url::Url::parse(base)
        .map_err(|_| WireError::new("invalid_params", "Invalid api_base URL"))?;
    if !matches!(parsed.scheme(), "https" | "http") {
        return Err(WireError::new(
            "invalid_params",
            "api_base must use http or https",
        ));
    }
    Ok(Some(base.trim_end_matches('/').to_owned()))
}

fn http_client() -> Result<reqwest::Client, ReferenceError> {
    microcrab_transport::http::client_builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(HTTP_TIMEOUT_SECS))
        .user_agent("microcrab-server")
        .build()
        .map_err(|e| ReferenceError::failed(format!("failed to initialize HTTP client: {e}")))
}

async fn get_json(client: &reqwest::Client, url: &str) -> Result<Value, ReferenceError> {
    let parsed = reqwest::Url::parse(url)
        .map_err(|e| ReferenceError::failed(format!("invalid remote Git URL `{url}`: {e}")))?;
    let response = client
        .get(parsed)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .await
        .map_err(|e| ReferenceError::unavailable(format!("remote Git request failed: {e}")))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        let snippet = microcrab_protocol::diagnostic::sanitize(&body, 512);
        return Err(ReferenceError::unavailable(format!(
            "remote Git API returned HTTP {status}: {snippet}"
        )));
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|e| ReferenceError::unavailable(format!("failed reading remote Git body: {e}")))?;
    if bytes.len() > MAX_FILE_BYTES * 4 {
        return Err(ReferenceError::limit("remote Git API response exceeded budget"));
    }
    serde_json::from_slice(&bytes)
        .map_err(|e| ReferenceError::failed(format!("invalid remote Git JSON response: {e}")))
}

#[derive(Clone)]
struct TreeEntry {
    path: String,
    sha: String,
    size: u64,
}

async fn probe_repo(
    spec: &ParsedGitRepoSpec,
    cache: &RwLock<BoundedFileCache>,
    repo_id: Option<&str>,
) -> Result<GitSkillRepoConfig, ReferenceError> {
    let client = http_client()?;
    let base = spec
        .api_base
        .as_deref()
        .unwrap_or_else(|| spec.provider.default_api_base());

    // 1. Resolve branch if not supplied.
    let branch = match &spec.branch {
        Some(branch) => branch.clone(),
        None => {
            let repo_url = format!("{base}/repos/{}/{}", spec.owner, spec.repo);
            let meta = get_json(&client, &repo_url).await?;
            meta.get("default_branch")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| {
                    ReferenceError::failed(
                        "remote repository metadata missing `default_branch`",
                    )
                })?
        }
    };

    // 2. Probe branch HEAD -> immutable commit hash.
    let commit_hash = match spec.provider {
        GitProvider::GitHub => {
            let url = format!("{base}/repos/{}/{}/commits/{branch}", spec.owner, spec.repo);
            let json = get_json(&client, &url).await?;
            json.get("sha")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| ReferenceError::failed("GitHub commit probe missing `sha`"))?
        }
        GitProvider::Gitee => {
            let url = format!("{base}/repos/{}/{}/branches/{branch}", spec.owner, spec.repo);
            let json = get_json(&client, &url).await?;
            json.pointer("/commit/sha")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| ReferenceError::failed("Gitee branch probe missing `commit.sha`"))?
        }
    };

    // 3. Probe repository tree under commit_hash (recursive tree first, contents API fallback).
    let tree_url = format!(
        "{base}/repos/{}/{}/git/trees/{commit_hash}?recursive=1",
        spec.owner, spec.repo
    );
    let tree_entries = match get_json(&client, &tree_url).await {
        Ok(json) if !json.get("truncated").and_then(Value::as_bool).unwrap_or(false) => {
            let mut items = Vec::new();
            if let Some(arr) = json.get("tree").and_then(Value::as_array) {
                for row in arr {
                    if row.get("type").and_then(Value::as_str) == Some("blob")
                        && let (Some(path), Some(sha)) = (
                            row.get("path").and_then(Value::as_str),
                            row.get("sha").and_then(Value::as_str),
                        )
                    {
                        items.push(TreeEntry {
                            path: path.to_owned(),
                            sha: sha.to_owned(),
                            size: row.get("size").and_then(Value::as_u64).unwrap_or(0),
                        });
                    }
                }
            }
            items
        }
        _ => {
            collect_via_contents_api(
                &client,
                base,
                &spec.owner,
                &spec.repo,
                &commit_hash,
                &spec.root_path,
            )
            .await?
        }
    };

    // 4. Discover `{skill_dir}/SKILL.md` under `root_path` and filter out invalid noise files.
    let prefix = if spec.root_path.is_empty() {
        String::new()
    } else {
        format!("{}/", spec.root_path)
    };
    let scoped_entries: Vec<&TreeEntry> = tree_entries
        .iter()
        .filter(|entry| {
            prefix.is_empty() || entry.path == spec.root_path || entry.path.starts_with(&prefix)
        })
        .collect();

    let mut skill_dirs: BTreeMap<String, &TreeEntry> = BTreeMap::new();
    for entry in &scoped_entries {
        if is_skill_manifest_path(&entry.path) {
            let skill_dir = entry
                .path
                .rsplit_once('/')
                .map(|(d, _)| d.to_owned())
                .unwrap_or_default();
            skill_dirs.insert(skill_dir, *entry);
        }
    }

    if skill_dirs.is_empty() {
        return Err(ReferenceError::not_found(
            "no valid SKILL.md manifests found at the configured Git repository path",
        ));
    }

    let mut skills = Vec::new();
    for (skill_dir, manifest_entry) in skill_dirs.into_iter().take(MAX_SKILLS_PER_REPO) {
        let skill_prefix = if skill_dir.is_empty() {
            String::new()
        } else {
            format!("{skill_dir}/")
        };
        let mut resources = Vec::new();
        for entry in &scoped_entries {
            let Some(rel) = (if skill_prefix.is_empty() {
                Some(entry.path.as_str())
            } else {
                entry.path.strip_prefix(&skill_prefix)
            }) else {
                continue;
            };
            if is_valid_skill_file(rel) && rel != "SKILL.md" {
                resources.push(ProbedSkillFile {
                    path: entry.path.clone(),
                    relative_path: rel.to_owned(),
                    blob_sha: entry.sha.clone(),
                    size: entry.size,
                    media_type: detect_mime_from_bytes(Some(&entry.path), b""),
                });
            }
        }

        // Fetch SKILL.md at pinned commit_hash, parse sections, and cache whole file in RAM.
        let cached_manifest = fetch_and_cache_git_file(
            &client,
            base,
            spec.provider,
            &spec.owner,
            &spec.repo,
            &commit_hash,
            &manifest_entry.path,
            "skills",
            cache,
        )
        .await?;
        let ReferencePayload::Text(manifest_text) = &cached_manifest.payload else {
            return Err(ReferenceError::failed("SKILL.md must be UTF-8 text"));
        };
        let manifest_digest = format!("{:x}", Sha256::digest(manifest_text.as_bytes()));
        let doc = parse_markdown(&manifest_identity("skills", spec.provider, &spec.owner, &spec.repo, &commit_hash, &manifest_entry.path), manifest_text)
            .map_err(|e| ReferenceError::failed(e.to_string()))?;
        let name = doc
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| doc.root.title.clone());
        skills.push(ProbedGitSkill {
            name,
            description: doc.metadata.description,
            tags: doc.metadata.tags,
            skill_dir,
            manifest_path: manifest_entry.path.clone(),
            manifest_blob_sha: manifest_entry.sha.clone(),
            manifest_digest,
            resources,
        });
    }

    Ok(GitSkillRepoConfig {
        id: repo_id
            .map(str::to_owned)
            .unwrap_or_else(|| format!("gskill_{}", &uuid::Uuid::new_v4().simple().to_string()[..16])),
        provider: spec.provider,
        owner: spec.owner.clone(),
        repo: spec.repo.clone(),
        branch,
        root_path: spec.root_path.clone(),
        commit_hash,
        probed_at_unix_ms: unix_ms(),
        api_base: spec.api_base.clone(),
        skills,
    })
}

async fn collect_via_contents_api(
    client: &reqwest::Client,
    base: &str,
    owner: &str,
    repo: &str,
    commit_hash: &str,
    root_path: &str,
) -> Result<Vec<TreeEntry>, ReferenceError> {
    let mut queue = VecDeque::new();
    queue.push_back(root_path.to_owned());
    let mut collected = Vec::new();
    let mut visited_dirs = BTreeSet::new();

    while let Some(dir) = queue.pop_front() {
        if !visited_dirs.insert(dir.clone()) || visited_dirs.len() > 256 {
            continue;
        }
        let encoded_path = dir.trim_matches('/');
        let url = if encoded_path.is_empty() {
            format!("{base}/repos/{owner}/{repo}/contents/?ref={commit_hash}")
        } else {
            format!("{base}/repos/{owner}/{repo}/contents/{encoded_path}?ref={commit_hash}")
        };
        let json = get_json(client, &url).await?;
        let Some(entries) = json.as_array() else {
            continue;
        };
        for item in entries {
            let kind = item.get("type").and_then(Value::as_str).unwrap_or("");
            let Some(path) = item.get("path").and_then(Value::as_str) else {
                continue;
            };
            let sha = item
                .get("sha")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();
            if kind == "dir" {
                queue.push_back(path.to_owned());
            } else if kind == "file" {
                collected.push(TreeEntry {
                    path: path.to_owned(),
                    sha,
                    size: item.get("size").and_then(Value::as_u64).unwrap_or(0),
                });
            }
        }
    }
    Ok(collected)
}

#[allow(clippy::too_many_arguments)]
async fn fetch_and_cache_git_file(
    client: &reqwest::Client,
    base: &str,
    provider: GitProvider,
    owner: &str,
    repo: &str,
    commit_hash: &str,
    file_path: &str,
    kind: &str,
    cache: &RwLock<BoundedFileCache>,
) -> Result<CachedGitFile, ReferenceError> {
    let key = CacheKey {
        provider,
        owner: owner.to_owned(),
        repo: repo.to_owned(),
        commit_hash: commit_hash.to_owned(),
        path: file_path.to_owned(),
    };
    if let Some(hit) = cache.read().unwrap_or_else(|e| e.into_inner()).get(&key) {
        return Ok(hit);
    }

    let url = format!("{base}/repos/{owner}/{repo}/contents/{file_path}?ref={commit_hash}");
    let json = get_json(client, &url).await?;
    let bytes = if let Some(b64) = json.get("content").and_then(Value::as_str)
        && json.get("encoding").and_then(Value::as_str) == Some("base64")
    {
        let compact: String = b64.chars().filter(|c| !c.is_ascii_whitespace()).collect();
        base64::engine::general_purpose::STANDARD
            .decode(compact.as_bytes())
            .map_err(|e| ReferenceError::failed(format!("invalid base64 content from Git API: {e}")))?
    } else if let Some(download_url) = json.get("download_url").and_then(Value::as_str) {
        let parsed = reqwest::Url::parse(download_url)
            .map_err(|e| ReferenceError::failed(format!("invalid download_url: {e}")))?;
        let resp = client
            .get(parsed)
            .send()
            .await
            .map_err(|e| ReferenceError::unavailable(format!("raw Git file fetch failed: {e}")))?;
        if !resp.status().is_success() {
            return Err(ReferenceError::unavailable(format!(
                "raw Git file fetch returned HTTP {}",
                resp.status()
            )));
        }
        resp.bytes()
            .await
            .map_err(|e| ReferenceError::unavailable(format!("failed reading Git raw file: {e}")))?
            .to_vec()
    } else {
        return Err(ReferenceError::failed(
            "remote Git contents response had neither base64 content nor download_url",
        ));
    };

    if bytes.len() > MAX_FILE_BYTES {
        return Err(ReferenceError::limit(format!(
            "remote Git file `{file_path}` exceeds {MAX_FILE_BYTES} bytes"
        )));
    }

    let media_type = detect_mime_from_bytes(Some(file_path), &bytes);
    let byte_len = bytes.len();
    let (payload, sections) = if media_type.starts_with("text/")
        || media_type == "application/json"
        || is_markdown_path(file_path)
    {
        let text = String::from_utf8(bytes)
            .map_err(|_| ReferenceError::failed(format!("file `{file_path}` is not valid UTF-8")))?;
        let sections = if is_markdown_path(file_path) {
            let ident = manifest_identity(kind, provider, owner, repo, commit_hash, file_path);
            parse_markdown(&ident, &text)
                .map_err(|e| ReferenceError::failed(e.to_string()))?
                .bodies
        } else {
            BTreeMap::new()
        };
        (ReferencePayload::Text(text), sections)
    } else {
        (
            ReferencePayload::Binary {
                data: bytes,
                width: None,
                height: None,
            },
            BTreeMap::new(),
        )
    };
    let cached = CachedGitFile {
        media_type,
        payload,
        sections,
        byte_len,
    };
    cache
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .insert(key, cached.clone());
    Ok(cached)
}

fn manifest_identity(
    kind: &str,
    provider: GitProvider,
    owner: &str,
    repo: &str,
    commit_hash: &str,
    path: &str,
) -> String {
    format!(
        "{}:{}:git:{}:{}:{}:{}:{}",
        kind.len(),
        kind,
        provider.as_str(),
        owner,
        repo,
        commit_hash,
        path
    )
}

#[derive(Clone)]
struct GitDirNode {
    id: String,
    path: String,
    children: Vec<Value>,
}

#[derive(Clone)]
struct GitFileTarget {
    id: String,
    provider: GitProvider,
    owner: String,
    repo: String,
    commit_hash: String,
    api_base: Option<String>,
    path: String,
}

#[derive(Default)]
struct GitSkillsCatalog {
    entries: Vec<ReferenceEntry>,
    directories: BTreeMap<String, GitDirNode>,
    files: BTreeMap<String, GitFileTarget>,
    section_to_file: BTreeMap<String, String>,
    projection_rows: Vec<(Value, Value)>,
}

/// Runtime `Reference` implementation for user-configured public Git Skills repositories.
///
/// - `load()`: silently hides any repository/skill whose initial exposure fails.
/// - `expand()`: fetches target file on demand (or hits whole-file memory cache) and explicitly
///   surfaces concrete errors on failure.
pub(crate) struct GitSkillsReference {
    kind: String,
    repos: RwLock<Vec<GitSkillRepoConfig>>,
    cache: Arc<RwLock<BoundedFileCache>>,
    catalog: RwLock<GitSkillsCatalog>,
    backing: Option<(Arc<dyn Configuration>, String)>,
}

impl GitSkillsReference {
    pub(crate) fn new(
        kind: impl Into<String>,
        repos: Vec<GitSkillRepoConfig>,
        cache: Arc<RwLock<BoundedFileCache>>,
    ) -> Self {
        Self {
            kind: kind.into(),
            repos: RwLock::new(repos),
            cache,
            catalog: RwLock::new(GitSkillsCatalog::default()),
            backing: None,
        }
    }

    pub(crate) fn with_configuration(
        mut self,
        configuration: Arc<dyn Configuration>,
        owner: impl Into<String>,
    ) -> Self {
        self.backing = Some((configuration, owner.into()));
        self
    }

    pub(crate) fn update_repos(&self, repos: Vec<GitSkillRepoConfig>) {
        *self.repos.write().unwrap_or_else(|e| e.into_inner()) = repos;
    }

    async fn build_catalog_for_repo(
        &self,
        repo: &GitSkillRepoConfig,
        next: &mut GitSkillsCatalog,
        client: &reqwest::Client,
    ) {
        let base = repo
            .api_base
            .as_deref()
            .unwrap_or_else(|| repo.provider.default_api_base());
        for skill in &repo.skills {
            // Initial exposure: if fetching SKILL.md fails, suppress this skill cleanly in SessionState.
            let cached = match fetch_and_cache_git_file(
                client,
                base,
                repo.provider,
                &repo.owner,
                &repo.repo,
                &repo.commit_hash,
                &skill.manifest_path,
                &self.kind,
                &self.cache,
            )
            .await
            {
                Ok(c) => c,
                Err(error) => {
                    tracing::warn!(
                        repo_id = %repo.id,
                        skill = %skill.name,
                        reason = %error,
                        "Hiding remote Git skill from initial SessionState exposure after fetch failure"
                    );
                    continue;
                }
            };
            let ReferencePayload::Text(manifest_text) = &cached.payload else {
                continue;
            };
            let ident = manifest_identity(
                &self.kind,
                repo.provider,
                &repo.owner,
                &repo.repo,
                &repo.commit_hash,
                &skill.manifest_path,
            );
            let Ok(doc) = parse_markdown(&ident, manifest_text) else {
                continue;
            };

            let short_rev = repo.commit_hash[..16.min(repo.commit_hash.len())].to_owned();
            let skill_scope = format!(
                "{}:{}/{}/{}/{}",
                repo.provider.as_str(),
                repo.owner,
                repo.repo,
                repo.commit_hash,
                skill.skill_dir
            );
            let skill_dir_id = stable_reference_id("gsd", &skill_scope);
            let manifest_file_id = stable_reference_id(
                "gsf",
                &format!("{skill_scope}:{}", skill.manifest_path),
            );

            let mut dir_children: BTreeMap<String, Vec<Value>> = BTreeMap::new();
            let mut resource_summary = Vec::new();
            dir_children.entry(skill_dir_id.clone()).or_default().push(json!({
                "id": manifest_file_id,
                "node_kind": "file",
                "name": "SKILL.md",
                "path": skill.manifest_path,
            }));
            next.entries.push(ReferenceEntry {
                id: manifest_file_id.clone(),
                node_kind: ReferenceNodeKind::File,
                parent_id: Some(skill_dir_id.clone()),
                title: "SKILL.md".into(),
                location: Some(skill.manifest_path.clone()),
                description: skill.description.clone(),
                media_type: Some("text/markdown".into()),
                revision: short_rev.clone(),
                writable: false,
                metadata: None,
            });
            next.files.insert(
                manifest_file_id.clone(),
                GitFileTarget {
                    id: manifest_file_id.clone(),
                    provider: repo.provider,
                    owner: repo.owner.clone(),
                    repo: repo.repo.clone(),
                    commit_hash: repo.commit_hash.clone(),
                    api_base: repo.api_base.clone(),
                    path: skill.manifest_path.clone(),
                },
            );

            for res in &skill.resources {
                let fid = stable_reference_id("gsf", &format!("{skill_scope}:{}", res.path));
                let segs: Vec<&str> = res.relative_path.split('/').collect();
                let mut accum = String::new();
                for sub in &segs[..segs.len().saturating_sub(1)] {
                    let parent_id = if accum.is_empty() {
                        skill_dir_id.clone()
                    } else {
                        stable_reference_id("gsd", &format!("{skill_scope}:{accum}"))
                    };
                    if !accum.is_empty() {
                        accum.push('/');
                    }
                    accum.push_str(sub);
                    let cur_id = stable_reference_id("gsd", &format!("{skill_scope}:{accum}"));
                    let siblings = dir_children.entry(parent_id.clone()).or_default();
                    if !siblings.iter().any(|c| c["id"] == cur_id) {
                        let cur_path = if skill.skill_dir.is_empty() {
                            accum.clone()
                        } else {
                            format!("{}/{accum}", skill.skill_dir)
                        };
                        siblings.push(json!({
                            "id": cur_id,
                            "node_kind": "directory",
                            "name": sub,
                            "path": cur_path,
                        }));
                        next.entries.push(ReferenceEntry {
                            id: cur_id.clone(),
                            node_kind: ReferenceNodeKind::Directory,
                            parent_id: Some(parent_id),
                            title: (*sub).to_owned(),
                            location: Some(cur_path.clone()),
                            description: None,
                            media_type: None,
                            revision: short_rev.clone(),
                            writable: false,
                            metadata: None,
                        });
                        next.directories.insert(
                            cur_id.clone(),
                            GitDirNode {
                                id: cur_id,
                                path: cur_path,
                                children: Vec::new(),
                            },
                        );
                    }
                }
                let parent_id = if accum.is_empty() {
                    skill_dir_id.clone()
                } else {
                    stable_reference_id("gsd", &format!("{skill_scope}:{accum}"))
                };
                let fname = segs.last().copied().unwrap_or(&res.relative_path);
                dir_children.entry(parent_id.clone()).or_default().push(json!({
                    "id": fid,
                    "node_kind": "file",
                    "name": fname,
                    "path": res.path,
                }));
                next.entries.push(ReferenceEntry {
                    id: fid.clone(),
                    node_kind: ReferenceNodeKind::File,
                    parent_id: Some(parent_id),
                    title: fname.to_owned(),
                    location: Some(res.path.clone()),
                    description: None,
                    media_type: Some(res.media_type.clone()),
                    revision: short_rev.clone(),
                    writable: false,
                    metadata: None,
                });
                next.files.insert(
                    fid.clone(),
                    GitFileTarget {
                        id: fid.clone(),
                        provider: repo.provider,
                        owner: repo.owner.clone(),
                        repo: repo.repo.clone(),
                        commit_hash: repo.commit_hash.clone(),
                        api_base: repo.api_base.clone(),
                        path: res.path.clone(),
                    },
                );
                resource_summary.push(json!({
                    "id": fid,
                    "path": res.relative_path,
                }));
            }

            next.entries.push(ReferenceEntry {
                id: skill_dir_id.clone(),
                node_kind: ReferenceNodeKind::Directory,
                parent_id: None,
                title: skill.name.clone(),
                location: Some(skill.manifest_path.clone()),
                description: skill.description.clone(),
                media_type: None,
                revision: short_rev.clone(),
                writable: false,
                metadata: None,
            });
            next.directories.insert(
                skill_dir_id.clone(),
                GitDirNode {
                    id: skill_dir_id.clone(),
                    path: if skill.skill_dir.is_empty() {
                        ".".into()
                    } else {
                        skill.skill_dir.clone()
                    },
                    children: Vec::new(),
                },
            );
            for (did, children) in dir_children {
                if let Some(dir) = next.directories.get_mut(&did) {
                    dir.children = children;
                }
            }

            register_git_sections(
                &doc.root,
                &manifest_file_id,
                &skill.manifest_path,
                &short_rev,
                &doc,
                &mut next.entries,
                &mut next.section_to_file,
            );

            let rev = DocumentRevision::new(skill.manifest_digest.clone());
            let exact_row = format_git_skill_row(repo, skill, &skill_dir_id, &manifest_file_id, &doc, &rev, &resource_summary, false);
            let short_row = format_git_skill_row(repo, skill, &skill_dir_id, &manifest_file_id, &doc, &rev, &resource_summary, true);
            next.projection_rows.push((exact_row, short_row));
        }
    }
}

fn register_git_sections(
    node: &microcrab_host_api::markdown::MarkdownNode,
    manifest_file_id: &str,
    manifest_path: &str,
    short_rev: &str,
    doc: &MarkdownDocument,
    entries: &mut Vec<ReferenceEntry>,
    section_to_file: &mut BTreeMap<String, String>,
) {
    if node.char_length > 0 {
        section_to_file.insert(node.id.clone(), manifest_file_id.to_owned());
        entries.push(ReferenceEntry {
            id: node.id.clone(),
            node_kind: ReferenceNodeKind::Section,
            parent_id: Some(manifest_file_id.to_owned()),
            title: node.title.clone(),
            location: Some(manifest_path.to_owned()),
            description: doc.metadata.description.clone(),
            media_type: Some("text/markdown".into()),
            revision: short_rev.to_owned(),
            writable: false,
            metadata: None,
        });
    }
    for child in &node.subjects {
        register_git_sections(
            child,
            manifest_file_id,
            manifest_path,
            short_rev,
            doc,
            entries,
            section_to_file,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn format_git_skill_row(
    repo: &GitSkillRepoConfig,
    skill: &ProbedGitSkill,
    skill_dir_id: &str,
    manifest_file_id: &str,
    doc: &MarkdownDocument,
    rev: &DocumentRevision,
    resource_summary: &[Value],
    short: bool,
) -> Value {
    let projection = compact_markdown_node(&doc.root);
    let mut fields = match projection {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    fields.insert("skill_id".into(), json!(skill_dir_id));
    fields.insert("file_id".into(), json!(manifest_file_id));
    fields.insert(
        "source".into(),
        json!(format!(
            "{}:{}/{}@{}",
            repo.provider.as_str(),
            repo.owner,
            repo.repo,
            if short {
                &repo.commit_hash[..12.min(repo.commit_hash.len())]
            } else {
                &repo.commit_hash
            }
        )),
    );
    fields.insert("path".into(), json!(skill.manifest_path));
    if short {
        fields.insert("revision".into(), json!(rev.label));
    } else {
        fields.insert("content_digest".into(), json!(rev.digest));
        fields.insert("commit_hash".into(), json!(repo.commit_hash));
    }
    if let Ok(Some(meta)) = markdown_metadata_json(&doc.metadata) {
        fields.insert("metadata".into(), meta);
    }
    if !resource_summary.is_empty() {
        fields.insert("resources".into(), Value::Array(resource_summary.to_vec()));
    }
    Value::Object(fields)
}

#[async_trait]
impl Reference for GitSkillsReference {
    fn kind(&self) -> &str {
        &self.kind
    }

    fn capabilities(&self) -> ReferenceCapabilities {
        ReferenceCapabilities::read_only()
    }

    async fn load(&self, cancel: CancellationToken) -> Result<(), ReferenceError> {
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        let repos = self.repos.read().unwrap_or_else(|e| e.into_inner()).clone();
        if repos.is_empty() {
            *self.catalog.write().unwrap_or_else(|e| e.into_inner()) = GitSkillsCatalog::default();
            return Ok(());
        }
        let Ok(client) = http_client() else {
            return Ok(());
        };
        let mut next = GitSkillsCatalog::default();
        for repo in &repos {
            if cancel.is_cancelled() {
                return Err(ReferenceError::cancelled());
            }
            self.build_catalog_for_repo(repo, &mut next, &client).await;
        }
        *self.catalog.write().unwrap_or_else(|e| e.into_inner()) = next;
        Ok(())
    }

    async fn refresh(&self, cancel: CancellationToken) -> Result<(), ReferenceError> {
        if let Some((configuration, owner)) = &self.backing
            && let Ok((_, doc)) = load_stored(configuration.as_ref(), owner).await
        {
            self.update_repos(doc.repos);
        }
        self.load(cancel).await
    }

    fn list(&self) -> Result<Vec<ReferenceEntry>, ReferenceError> {
        Ok(self
            .catalog
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .clone())
    }

    fn projections(&self) -> Result<ReferenceProjection, ReferenceError> {
        let guard = self.catalog.read().unwrap_or_else(|e| e.into_inner());
        if guard.projection_rows.is_empty() {
            return Ok(ReferenceProjection::empty());
        }
        Ok(ReferenceProjection {
            exact: Some(Value::Array(
                guard.projection_rows.iter().map(|(e, _)| e.clone()).collect(),
            )),
            short_model: Some(Value::Array(
                guard.projection_rows.iter().map(|(_, s)| s.clone()).collect(),
            )),
        })
    }

    async fn expand(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<Option<ReferenceContent>, ReferenceError> {
        if cancel.is_cancelled() {
            return Err(ReferenceError::cancelled());
        }
        let (dir_node, file_node, section_file) = {
            let guard = self.catalog.read().unwrap_or_else(|e| e.into_inner());
            (
                guard.directories.get(id).cloned(),
                guard.files.get(id).cloned(),
                guard
                    .section_to_file
                    .get(id)
                    .and_then(|fid| guard.files.get(fid).cloned()),
            )
        };
        if let Some(dir) = dir_node {
            let body = serde_json::to_string(&json!({
                "directory": dir.path,
                "children": dir.children,
            }))
            .map_err(|e| ReferenceError::failed(e.to_string()))?;
            return Ok(Some(ReferenceContent::text_with_mime(
                dir.id,
                "application/json",
                body,
            )));
        }
        if let Some(file) = file_node {
            let client = http_client()?;
            let base = file
                .api_base
                .as_deref()
                .unwrap_or_else(|| file.provider.default_api_base());
            let cached = fetch_and_cache_git_file(
                &client,
                base,
                file.provider,
                &file.owner,
                &file.repo,
                &file.commit_hash,
                &file.path,
                &self.kind,
                &self.cache,
            )
            .await?;
            return Ok(Some(ReferenceContent {
                id: file.id,
                media_type: cached.media_type,
                payload: cached.payload,
            }));
        }
        if let Some(owner_file) = section_file {
            let client = http_client()?;
            let base = owner_file
                .api_base
                .as_deref()
                .unwrap_or_else(|| owner_file.provider.default_api_base());
            let cached = fetch_and_cache_git_file(
                &client,
                base,
                owner_file.provider,
                &owner_file.owner,
                &owner_file.repo,
                &owner_file.commit_hash,
                &owner_file.path,
                &self.kind,
                &self.cache,
            )
            .await?;
            if let Some(section_body) = cached.sections.get(id) {
                return Ok(Some(ReferenceContent::text_with_mime(
                    id,
                    "text/markdown",
                    section_body.clone(),
                )));
            }
            return Err(ReferenceError::not_found(format!(
                "section `{id}` not found in `{}`",
                owner_file.path
            )));
        }
        Ok(None)
    }
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[derive(Default)]
pub(crate) struct GitSkillsService {
    cache: Arc<RwLock<BoundedFileCache>>,
}

impl GitSkillsService {
    pub(crate) fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(BoundedFileCache::default())),
        }
    }

    pub(crate) fn cache(&self) -> Arc<RwLock<BoundedFileCache>> {
        self.cache.clone()
    }
}

async fn load_stored(
    storage: &dyn Configuration,
    owner: &str,
) -> Result<(u64, StoredGitSkillRepos), WireError> {
    let row = storage.get(owner, CONFIG_KIND, CONFIG_TARGET).await.map_err(safe)?;
    let revision = row.as_ref().map_or(0, |r| r.revision);
    let doc: StoredGitSkillRepos = row
        .and_then(|r| r.value)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| {
            WireError::new(
                "configuration_unavailable",
                "Stored Git skills configuration cannot be decoded",
            )
        })?
        .unwrap_or_default();
    Ok((revision, doc))
}

async fn save_stored(
    storage: &dyn Configuration,
    owner: &str,
    expected_revision: u64,
    doc: &StoredGitSkillRepos,
) -> Result<(), WireError> {
    let value = serde_json::to_value(doc).map_err(|e| WireError::new("invalid_params", e.to_string()))?;
    storage
        .compare_exchange(owner, CONFIG_KIND, CONFIG_TARGET, expected_revision, Some(value))
        .await
        .map_err(|error| match &error {
            Error::Protocol(reason)
                if reason.public_reason() == Some("configuration revision conflict; reload before saving") =>
            {
                WireError::new("revision_conflict", error.diagnostic_reason())
            }
            _ => safe(error),
        })?;
    Ok(())
}

impl Manager {
    pub(crate) async fn git_skill_repos_for_owner(
        &self,
        owner: &str,
    ) -> Result<Vec<GitSkillRepoConfig>, WireError> {
        let (_, doc) = load_stored(self.configuration.as_ref(), owner).await?;
        Ok(doc.repos)
    }

    pub(crate) async fn add_git_skill_repo(
        &self,
        owner: &str,
        input: AddGitSkillRepoInput,
    ) -> Result<GitSkillRepoConfig, WireError> {
        let spec = parse_git_skill_repo_input(&input)?;
        let probed = probe_repo(&spec, &self.git_skills.cache(), None)
            .await
            .map_err(|e| WireError::new("git_probe_failed", e.to_string()))?;
        let (revision, mut doc) = load_stored(self.configuration.as_ref(), owner).await?;
        if let Some(existing) = doc.repos.iter().find(|r| {
            r.provider == probed.provider
                && r.owner == probed.owner
                && r.repo == probed.repo
                && r.root_path == probed.root_path
        }) {
            return Err(WireError::new(
                "conflict",
                format!(
                    "Git skill repository `{}/{}` (`{}`) is already configured as `{}`",
                    probed.owner, probed.repo, probed.root_path, existing.id
                ),
            ));
        }
        if doc.repos.len() >= MAX_REPOS_PER_USER {
            return Err(WireError::new(
                "limit_exceeded",
                "Maximum number of configured Git skill repositories reached",
            ));
        }
        doc.repos.push(probed.clone());
        save_stored(self.configuration.as_ref(), owner, revision, &doc).await?;
        Ok(probed)
    }

    pub(crate) async fn refresh_git_skill_repo(
        &self,
        owner: &str,
        repo_id: &str,
    ) -> Result<GitSkillRepoConfig, WireError> {
        let (revision, mut doc) = load_stored(self.configuration.as_ref(), owner).await?;
        let Some(index) = doc.repos.iter().position(|r| r.id == repo_id) else {
            return Err(WireError::new("not_found", "Git skill repository not found"));
        };
        let current = &doc.repos[index];
        let spec = ParsedGitRepoSpec {
            provider: current.provider,
            owner: current.owner.clone(),
            repo: current.repo.clone(),
            branch: Some(current.branch.clone()),
            root_path: current.root_path.clone(),
            api_base: current.api_base.clone(),
        };
        let updated = probe_repo(&spec, &self.git_skills.cache(), Some(repo_id))
            .await
            .map_err(|e| WireError::new("git_probe_failed", e.to_string()))?;
        doc.repos[index] = updated.clone();
        save_stored(self.configuration.as_ref(), owner, revision, &doc).await?;
        Ok(updated)
    }

    pub(crate) async fn remove_git_skill_repo(
        &self,
        owner: &str,
        repo_id: &str,
    ) -> Result<bool, WireError> {
        let (revision, mut doc) = load_stored(self.configuration.as_ref(), owner).await?;
        let before = doc.repos.len();
        doc.repos.retain(|r| r.id != repo_id);
        if doc.repos.len() == before {
            return Ok(false);
        }
        save_stored(self.configuration.as_ref(), owner, revision, &doc).await?;
        Ok(true)
    }

    pub(crate) fn build_git_skills_reference(
        &self,
        owner: &str,
        repos: Vec<GitSkillRepoConfig>,
    ) -> GitSkillsReference {
        GitSkillsReference::new("skills", repos, self.git_skills.cache())
            .with_configuration(self.configuration.clone(), owner)
    }
}

#[cfg(test)]
mod tests;
