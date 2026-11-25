use super::*;
use microcrab_host_api::{
    FsApi, HostAPI, NetApi, ProcessApi, ShellApi, web_transport::ProjectScope,
};
use serde_json::json;

const FIRST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const OTHER: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

fn config() -> Config {
    serde_json::from_str(include_str!("../../../config.example.json")).unwrap()
}

fn selection(
    catalog: &[ComponentCatalog],
    requested: Option<&str>,
    embedded: bool,
) -> Result<Option<String>, String> {
    select_component(catalog, requested, embedded)
        .map(|value| value.map(|component| component.digest.clone()))
        .map_err(|error| error.code)
}

#[test]
fn selection_preserves_exact_digest_and_unambiguous_default() {
    let source = serde_json::from_value(json!({
        "path": "private/browser.wasm", "name": "custom-name", "mode": "custom-mode",
    }))
    .unwrap();
    let mut catalog = vec![ComponentCatalog {
        digest: FIRST.into(),
        source,
        agents: vec![],
    }];
    assert_eq!(selection(&catalog, None, false), Ok(Some(FIRST.into())));
    assert_eq!(
        selection(&catalog, Some(FIRST), false),
        Ok(Some(FIRST.into()))
    );
    assert_eq!(
        selection(&catalog, Some(&FIRST.to_uppercase()), false),
        Err("component_unavailable".into())
    );
    assert_eq!(catalog[0].name(), Some("custom-name"));
    assert_eq!(catalog[0].mode(), Some("custom-mode"));

    catalog.push(ComponentCatalog {
        digest: OTHER.into(),
        source: config().agent.unwrap(),
        agents: vec![],
    });
    assert_eq!(
        selection(&catalog, None, false),
        Err("component_required".into())
    );
    assert_eq!(
        selection(&catalog, Some(OTHER), false),
        Ok(Some(OTHER.into()))
    );
    assert_eq!(
        selection(&[], None, false),
        Err("component_unavailable".into())
    );
    assert_eq!(selection(&[], None, true), Ok(None));
}

struct RouteHost {
    supports: AtomicBool,
    binding: Option<ProjectScope>,
}

impl ProcessApi for RouteHost {}
impl FsApi for RouteHost {}
impl NetApi for RouteHost {}
impl ShellApi for RouteHost {}
impl HostAPI for RouteHost {}
impl Host for RouteHost {
    fn info(&self) -> Result<microcrab_host_api::HostInfo, microcrab_host_api::ToolError> {
        Ok(microcrab_host_api::HostInfo {
            os_family: microcrab_host_api::OsFamily::Unix,
            os: "fixture".into(),
            arch: "fixture".into(),
            shell: "none".into(),
            path_separator: "/".into(),
            newline: "\n".into(),
            capabilities: vec![],
            resources: None,
            virtual_shell: None,
            description: None,
        })
    }

    fn supports_web_transport(&self) -> bool {
        self.supports.load(Ordering::Acquire)
    }

    fn web_transport_binding(&self) -> Option<ProjectScope> {
        self.binding.clone()
    }
}

#[tokio::test]
#[ignore = "requires a built Browser artifact in MICROCRAB_TEST_BROWSER_COMPONENT"]
async fn browser_real_component_is_cataloged_from_its_imports_only_on_eligible_project_hosts() {
    use microcrab_session::ComponentCapability;
    use sha2::{Digest, Sha256};

    let artifact = std::env::var_os("MICROCRAB_TEST_BROWSER_COMPONENT").unwrap();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("fixture.wasm");
    let bytes = std::fs::read(artifact).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    let fixture_digest = format!("{:x}", Sha256::digest(bytes));
    let mut config = config();
    config.agent = None;
    config.agents = vec![serde_json::from_value(json!({"path": path})).unwrap()];
    let source = &config.agents[0];
    let host = Arc::new(RouteHost {
        supports: AtomicBool::new(true),
        binding: Some(ProjectScope {
            binding_id: "binding".into(),
            backend_id: "backend".into(),
            project_id: "project".into(),
        }),
    });
    let unbound = Arc::new(RouteHost {
        supports: AtomicBool::new(true),
        binding: None,
    });
    assert!(components(&config, host.clone()).await.unwrap().is_empty());
    for (project, route) in [("other", host.clone()), ("project", unbound)] {
        assert!(
            project_components(&config, route, project)
                .await
                .unwrap()
                .is_empty()
        );
    }
    let catalog = project_components(&config, host.clone(), "project")
        .await
        .unwrap();
    assert_eq!(catalog.len(), 1);
    assert_eq!(catalog[0].digest, fixture_digest);
    assert!(!catalog[0].agents.is_empty());
    assert_eq!(
        selection(&catalog, None, false),
        Ok(Some(fixture_digest.clone()))
    );
    assert_eq!(
        selection(&catalog, Some(&fixture_digest), false),
        Ok(Some(fixture_digest.clone()))
    );
    assert_eq!(
        selection(&catalog, Some(OTHER), false),
        Err("component_unavailable".into())
    );
    let (loaded, digest) = load(source, host.clone(), Some("project"), config.agent_wasm_fuel).unwrap();
    assert_eq!(
        loaded.required_capabilities(),
        &[ComponentCapability::WebTransport]
    );
    assert_eq!(digest, fixture_digest);

    host.supports.store(false, Ordering::Release);
    assert!(
        project_components(&config, host.clone(), "project")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        load(source, host.clone(), Some("project"), config.agent_wasm_fuel),
        Err(Error::Config(_))
    ));
    host.supports.store(true, Ordering::Release);

    config.agents[0].restore_only = true;
    assert!(
        project_components(&config, host.clone(), "project")
            .await
            .unwrap()
            .is_empty()
    );
    for (project, admitted) in [
        (Some("project"), true),
        (Some("other"), false),
        (None, false),
    ] {
        let catalog = component_catalog(&config, host.clone(), project, true)
            .await
            .unwrap();
        assert_eq!(
            catalog.first().map(|entry| entry.digest.as_str()),
            admitted.then_some(fixture_digest.as_str())
        );
    }

    // A different but valid component at the same path must not match the old pin.
    let mut changed = std::fs::read(&path).unwrap();
    changed.extend([0, 4, 3, b'n', b'e', b'w']); // An inert custom section.
    std::fs::write(&path, changed).unwrap();
    let catalog = component_catalog(&config, host, Some("project"), true)
        .await
        .unwrap();
    assert_eq!(catalog.len(), 1);
    assert_ne!(catalog[0].digest, fixture_digest);
    assert_eq!(
        selection(&catalog, Some(&fixture_digest), false),
        Err("component_unavailable".into())
    );
}
