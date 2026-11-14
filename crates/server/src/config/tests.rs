use super::*;

fn example() -> serde_json::Value {
    serde_json::from_str(include_str!("../../config.example.json")).unwrap()
}
fn write_config(startup: &Path, relative: &str, value: &serde_json::Value) -> PathBuf {
    let path = startup.join(relative);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
    path
}
fn assert_safe_error(error: Box<dyn std::error::Error>, code: &str, cause: &str) {
    let error = error.downcast_ref::<ConfigError>().unwrap();
    assert_eq!(error.stage, "configuration");
    assert_eq!(error.code, code);
    assert!(error.cause.contains(cause), "{error:?}");
    assert!(!error.cause.is_empty());
    assert!(error.cause.len() <= 2048);
    assert_eq!(
        error.to_string(),
        format!("configuration: {code}: {}", error.cause)
    );
    for sentinel in [
        "synthetic-private",
        "synthetic-private-token",
        "synthetic-private-value",
    ] {
        assert!(!format!("{error:?}").contains(sentinel));
    }
}

#[test]
fn direct_provider_protocol_and_auth_are_effective_not_display_metadata() {
    let mut value = example();
    value["provider"]["protocol"] = "anthropic".into();
    value["provider"]["auth"] = "bearer".into();
    let model = value["provider"]["model"].as_str().unwrap().to_owned();
    value["available_models"] = serde_json::json!([
        {"id":model,"name":"Display metadata","provider_id":"configured","protocol":"google"}
    ]);
    let config: Config = serde_json::from_value(value).unwrap();
    let selected = config
        .resolve_selection("configured", &model, None)
        .unwrap();
    assert_eq!(selected.provider.protocol, Protocol::Anthropic);
    assert_eq!(selected.provider.auth, AuthMode::Bearer);
    assert_eq!(
        selected.provider.client_transport_config().auth,
        AuthMode::Bearer
    );
    assert_eq!(config.model_choices()[0].protocol, Protocol::Anthropic);
    assert_eq!(
        serde_json::to_value(&config.model_choices()[0]).unwrap()["protocol"],
        "anthropic"
    );
    let config: Config = serde_json::from_value(example()).unwrap();
    assert_eq!(config.provider.auth, AuthMode::Native);
    let mut invalid = example();
    invalid["provider"]["auth"] = "arbitrary-header-policy".into();
    assert!(serde_json::from_value::<Config>(invalid).is_err());
}

#[test]
fn private_layout_and_host_anchor_follow_startup() {
    let startup = tempfile::tempdir().unwrap();
    let mut value = example();
    value.as_object_mut().unwrap().remove("data_dir");
    value["agent"].as_object_mut().unwrap().remove("path");
    let mut config: Config = serde_json::from_value(value).unwrap();
    config.resolve(startup.path()).unwrap();
    assert_eq!(config.startup_dir, startup.path().canonicalize().unwrap());
    assert_eq!(
        config.data_dir,
        startup.path().join(".microcrab").canonicalize().unwrap()
    );
    assert_eq!(
        config.agent.unwrap().path,
        config.startup_dir.join(DEFAULT_AGENT_PATH)
    );
    assert!(config.private_paths.contains(&config.data_dir));
}

#[test]
fn explicit_storage_and_agent_overrides_remain_startup_relative() {
    let startup = tempfile::tempdir().unwrap();
    let mut value = example();
    value["data_dir"] = "retained-data".into();
    value["agent"]["path"] = "installed/selected.wasm".into();
    let mut config: Config = serde_json::from_value(value).unwrap();
    config.resolve(startup.path()).unwrap();
    assert_eq!(
        config.data_dir,
        startup.path().join("retained-data").canonicalize().unwrap()
    );
    assert_eq!(
        config.agent.unwrap().path,
        config.startup_dir.join("installed/selected.wasm")
    );
    assert!(
        config
            .private_paths
            .contains(&startup.path().canonicalize().unwrap().join(".microcrab"))
    );
}

#[test]
fn jsonc_preserves_urls_unicode_comment_markers_and_escaped_strings() {
    let text = r#"// leading comment
    {
        "url": "https://example.invalid/path//literal?x=/*literal*/",
        /* 中文 block comment */
        "escaped": "quote: \" // string; slash: \\ /* string */",
        "items": ["a", /* between comma and closer */],
        "nested": {"ok": true,},
    } // final comment without newline"#;
    let value = jsonc::parse(text).unwrap();
    assert_eq!(
        value["url"],
        "https://example.invalid/path//literal?x=/*literal*/"
    );
    assert_eq!(
        value["escaped"],
        "quote: \" // string; slash: \\ /* string */"
    );
    assert_eq!(value["items"], serde_json::json!(["a"]));
    assert_eq!(value["nested"], serde_json::json!({"ok":true}));
    assert_eq!(
        jsonc::parse("[1,\r\n// line\r\n]").unwrap(),
        serde_json::json!([1])
    );
}

#[test]
fn jsonc_rejects_invalid_grammar_and_unterminated_comments_without_input_diagnostics() {
    for text in [
        "/* synthetic-private",
        "{\"synthetic-private\":true} /* unfinished",
        "[1,,]",
        "[,]",
        "{,}",
        "{\"x\":,}",
        "[1 2]",
        "{unquoted:1}",
        "{'x':1}",
        "[1/* split token */2]",
        "[tru/* split token */e]",
        "{\"x\":\"unterminated\\\"}",
    ] {
        let error = jsonc::parse(text).unwrap_err();
        assert_eq!(error.code, "invalid_format");
        assert_eq!(error.stage, "configuration");
        if text.contains("/* synthetic-private") || text.contains("/* unfinished") {
            assert_eq!(error.cause, "unterminated JSONC block comment");
        } else {
            let expected = match text {
                "[1,,]" | "{\"x\":,}" => "expected value",
                "[,]" => "expected value",
                "{,}" | "{unquoted:1}" | "{'x':1}" => "key must be a string",
                "[1 2]" | "[1/* split token */2]" => "expected comma or array end",
                "[tru/* split token */e]" => "expected ident",
                _ => "EOF while parsing a string",
            };
            assert!(error.cause.contains(expected), "{text}: {error:?}");
            assert!(error.cause.contains("line 1, column "), "{error:?}");
        }
        assert_eq!(
            error.to_string(),
            format!("configuration: invalid_format: {}", error.cause)
        );
        assert!(error.cause.len() <= 2048);
        assert!(!format!("{error:?}").contains("synthetic-private"));
    }
}

#[test]
fn default_discovery_prefers_jsonc_and_falls_back_only_when_absent() {
    let startup = tempfile::tempdir().unwrap();
    let mut legacy = example();
    legacy["listen"] = "127.0.0.1:4100".into();
    let legacy_path = write_config(startup.path(), LEGACY_CONFIG_PATH, &legacy);
    let loaded = Config::read_default_at(startup.path()).unwrap();
    assert_eq!(loaded.listen, "127.0.0.1:4100");
    assert!(
        loaded
            .private_paths
            .contains(&legacy_path.canonicalize().unwrap())
    );

    let preferred = write_config(startup.path(), DEFAULT_CONFIG_PATH, &example());
    assert_eq!(
        Config::read_default_at(startup.path()).unwrap().listen,
        "127.0.0.1:4096"
    );
    std::fs::write(preferred, "/* synthetic-private unfinished").unwrap();
    assert_safe_error(
        Config::read_default_at(startup.path()).err().unwrap(),
        "invalid_format",
        "unterminated JSONC block comment",
    );
}

#[tokio::test]
async fn renamed_defaults_ignore_retired_configuration_and_keep_it_private() {
    let startup = tempfile::tempdir().unwrap();
    let mut retired = example();
    retired["listen"] = "127.0.0.1:4100".into();
    let retired_path = write_config(startup.path(), ".micro-crab/config.jsonc", &retired);
    let original = std::fs::read(&retired_path).unwrap();
    assert_safe_error(
        Config::read_default_at(startup.path()).err().unwrap(),
        "file_not_found",
        "kind=NotFound; os_error=",
    );
    let mut current = example();
    current.as_object_mut().unwrap().remove("agent");
    write_config(startup.path(), DEFAULT_CONFIG_PATH, &current);
    let loaded = Config::read_default_at(startup.path()).unwrap();
    assert_eq!(loaded.listen, "127.0.0.1:4096");
    let host = crate::host::startup_local_host(&loaded).unwrap();
    let error = host
        .read_file(
            retired_path.canonicalize().unwrap(),
            1024,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.public_reason(),
        Some("target path is protected by Host private-directory policy")
    );
    assert_eq!(std::fs::read(retired_path).unwrap(), original);
}

#[tokio::test]
async fn default_private_directory_protects_preferred_legacy_and_backup_copies() {
    let startup = tempfile::tempdir().unwrap();
    let mut value = example();
    value.as_object_mut().unwrap().remove("agent");
    let preferred = write_config(startup.path(), DEFAULT_CONFIG_PATH, &value);
    let legacy = startup.path().join(LEGACY_CONFIG_PATH);
    let backup = startup.path().join(".microcrab/config.json.pre-jsonc.bak");
    let archived = startup.path().join(".microcrab/backups/config.json");
    std::fs::create_dir_all(archived.parent().unwrap()).unwrap();
    for path in [&legacy, &backup, &archived] {
        // Unselected copies need not parse. Protecting the directory covers all
        // copies without reading their contents during configuration discovery.
        std::fs::write(path, "synthetic-private-backup").unwrap();
    }
    let config = Config::read_default_at(startup.path()).unwrap();
    assert!(
        config
            .private_paths
            .contains(&startup.path().join(".microcrab").canonicalize().unwrap())
    );
    let host = crate::host::startup_local_host(&config).unwrap();
    for path in [preferred, legacy, backup, archived] {
        let error = host
            .read_file(
                path.canonicalize().unwrap(),
                1024,
                tokio_util::sync::CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            &error,
            microcrab_host_api::ToolError::Host(cause)
                if cause.category == microcrab_host_api::ErrorCategory::Permission
        ));
        assert_eq!(
            error.public_reason(),
            Some("target path is protected by Host private-directory policy")
        );
        assert!(!error.to_string().contains("synthetic-private-backup"));
    }
}

#[test]
fn explicit_paths_do_not_fall_back_and_legacy_json_remains_readable() {
    let startup = tempfile::tempdir().unwrap();
    let legacy = write_config(startup.path(), LEGACY_CONFIG_PATH, &example());
    assert_eq!(
        Config::read_at(&legacy, startup.path()).unwrap().listen,
        "127.0.0.1:4096"
    );
    assert_safe_error(
        Config::read_at(&startup.path().join(DEFAULT_CONFIG_PATH), startup.path())
            .err()
            .unwrap(),
        "file_not_found",
        "kind=NotFound; os_error=",
    );
    // Public copy/paste artifacts must remain valid independently of generic
    // parser fixtures. AuthPortal supplies only an auth fragment; assemble it
    // with the full fixture and synthetic secrets without mutating environment.
    for (name, text, auth_fragment) in [
        (
            "startup-template.jsonc",
            include_str!("../../../../config.example.jsonc"),
            false,
        ),
        (
            "public-example.jsonc",
            include_str!("../../config.example.jsonc"),
            false,
        ),
        (
            "authportal-example.jsonc",
            include_str!("../../config.authportal.example.jsonc"),
            true,
        ),
    ] {
        let text = if auth_fragment {
            let mut value = example();
            value["auth"] = jsonc::parse(text).unwrap()["auth"].clone();
            value["auth"]["jwt_secret"] =
                "synthetic-public-example-session-secret-at-least-32-bytes".into();
            value["auth"]["client_secret"] = "synthetic-public-example-oidc-secret".into();
            serde_json::to_string(&value).unwrap()
        } else {
            text.to_owned()
        };
        let path = startup.path().join(name);
        std::fs::write(&path, text).unwrap();
        let config = Config::read_at(&path, startup.path()).unwrap();
        assert!(config.private_paths.contains(&path.canonicalize().unwrap()));
    }
}

#[test]
fn legacy_model_context_metadata_is_exact_scoped_and_does_not_expand_model_authority() {
    let startup = tempfile::tempdir().unwrap();
    let mut value = example();
    let model = value["provider"]["model"].as_str().unwrap().to_owned();
    value["available_models"] = serde_json::json!([
        {"id": model, "name":"Other provider", "provider_id":"other", "context_window_tokens":999999},
        {"id":"unconfigured", "name":"Display only", "provider_id":"configured", "context_window_tokens":888888},
        {"id": model, "name":"Named", "provider_id":"configured", "variants":["high"], "context_window_tokens":131072}
    ]);
    let path = write_config(startup.path(), "server.jsonc", &value);
    let config = Config::read_at(&path, startup.path()).unwrap();
    let choices = config.model_choices();
    assert_eq!(choices.len(), 1);
    assert_eq!(choices[0].id, model);
    assert_eq!(choices[0].provider_id, "configured");
    assert_eq!(choices[0].context_window_tokens, Some(131072));
    assert_eq!(choices[0].variants, ["default"]);
    assert!(
        config
            .resolve_selection("configured", "unconfigured", None)
            .is_err()
    );
    assert!(
        config
            .resolve_selection("configured", &model, Some("high"))
            .is_err()
    );
}

#[test]
fn missing_or_null_legacy_model_context_remains_unknown_without_model_name_inference() {
    let startup = tempfile::tempdir().unwrap();
    for metadata in [
        serde_json::json!({}),
        serde_json::json!({"context_window_tokens":null}),
    ] {
        let mut value = example();
        let model = value["provider"]["model"].as_str().unwrap().to_owned();
        let mut choice = serde_json::json!({"id":model,"name":"Named","provider_id":"configured"});
        choice
            .as_object_mut()
            .unwrap()
            .extend(metadata.as_object().unwrap().clone());
        value["available_models"] = serde_json::json!([choice]);
        let path = write_config(startup.path(), "server.jsonc", &value);
        let config = Config::read_at(&path, startup.path()).unwrap();
        assert_eq!(config.model_choices()[0].context_window_tokens, None);
        assert!(
            serde_json::to_value(&config.model_choices()[0])
                .unwrap()
                .get("context_window_tokens")
                .is_none()
        );
    }
    let mut value = example();
    value["available_models"] = serde_json::json!([
        {"id":"different", "name":"Display only", "provider_id":"configured", "context_window_tokens":1}
    ]);
    let config: Config = serde_json::from_value(value).unwrap();
    assert_eq!(config.model_choices()[0].context_window_tokens, None);
}

#[test]
fn invalid_legacy_model_context_values_fail_with_safe_configuration_errors() {
    let startup = tempfile::tempdir().unwrap();
    for invalid in [
        serde_json::json!(0),
        serde_json::json!(-1),
        serde_json::json!(1.5),
        serde_json::json!("synthetic-private-token"),
        serde_json::json!(true),
    ] {
        let mut value = example();
        value["available_models"] = serde_json::json!([
            {"id":value["provider"]["model"], "name":"Named", "provider_id":"configured", "context_window_tokens":invalid}
        ]);
        let path = write_config(startup.path(), "server.jsonc", &value);
        assert_safe_error(
            Config::read_at(&path, startup.path()).err().unwrap(),
            "invalid_fields",
            "expected a nonzero u64",
        );
    }
}

#[test]
fn in_memory_zero_model_context_is_rejected_before_creating_private_directories() {
    let startup = tempfile::tempdir().unwrap();
    let mut config: Config = serde_json::from_value(example()).unwrap();
    config.available_models = vec![ModelChoice {
        id: config.provider.model.clone(),
        name: config.provider.model.clone(),
        provider_id: "configured".into(),
        protocol: config.provider.protocol,
        variants: vec!["default".into()],
        context_window_tokens: Some(0),
    }];
    assert_safe_error(
        config.resolve(startup.path()).err().unwrap(),
        "invalid_context_window",
        "invalid context window",
    );
    assert!(!startup.path().join(".microcrab").exists());
}

#[test]
fn startup_projects_and_other_unknown_fields_are_rejected_without_disclosing_values() {
    let startup = tempfile::tempdir().unwrap();
    for (key, value) in [
        ("projects", serde_json::json!([])),
        (
            "projects",
            serde_json::json!([{"id":"synthetic-private","owners":["*"]}]),
        ),
        (
            "synthetic-private-unknown",
            serde_json::json!("synthetic-private-value"),
        ),
    ] {
        let mut config = example();
        config[key] = value;
        let path = write_config(startup.path(), "synthetic-private.jsonc", &config);
        assert_safe_error(
            Config::read_at(&path, startup.path()).err().unwrap(),
            "invalid_fields",
            "unknown field",
        );
    }
    let mut config = example();
    config["provider"]["synthetic-private-unknown"] = "synthetic-private-value".into();
    let path = write_config(startup.path(), "synthetic-private.jsonc", &config);
    assert_safe_error(
        Config::read_at(&path, startup.path()).err().unwrap(),
        "invalid_fields",
        "unknown field",
    );
}

#[test]
fn selected_backend_default_is_feature_aware_and_explicit_file_remains_supported() {
    let mut value = example();
    value.as_object_mut().unwrap().remove("storage");
    let config: Config = serde_json::from_value(value.clone()).unwrap();
    assert!(
        config.storage
            == if cfg!(feature = "storage-sqlite") {
                Storage::Sqlite
            } else {
                Storage::File
            }
    );
    value["storage"] = "file".into();
    let config: Config = serde_json::from_value(value).unwrap();
    assert!(config.storage == Storage::File);
}
#[test]
fn sqlite_selection_diagnoses_old_namespaces_without_changing_source_bytes() {
    let startup = tempfile::tempdir().unwrap();
    let mut config: Config = serde_json::from_value(example()).unwrap();
    config.resolve(startup.path()).unwrap();
    config.storage = Storage::Sqlite;
    let old = config
        .data_dir
        .join("sessions/old/subsessions/child/audit.jsonl");
    std::fs::create_dir_all(old.parent().unwrap()).unwrap();
    let frozen = b"historical evidence\n";
    std::fs::write(&old, frozen).unwrap();
    assert!(
        config
            .validate_storage_namespace()
            .unwrap_err()
            .to_string()
            .contains("explicit byte-preserving migration")
    );
    assert_eq!(std::fs::read(&old).unwrap(), frozen);
    assert!(!config.data_dir.join("storage.sqlite").exists());
    config.storage = Storage::File;
    config.validate_storage_namespace().unwrap();
}
