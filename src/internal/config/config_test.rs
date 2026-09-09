//! `internal/config` 的行为回归测试。

use std::path::PathBuf;

use serde_json::json;

use super::*;

/// 显式目录的序列化合同及缺省时的兼容行为。
#[test]
fn git_mount_path_round_trips_and_overrides_repo_name() {
    let mut source: GitRepositorySource = serde_json::from_value(json!({
        "type": "git_repository",
        "mount_path": "/workspace/service",
        "git_info": {"repo": "group/nested/repository"}
    }))
    .unwrap();
    assert_eq!(
        source.get_directory("/other"),
        PathBuf::from("/workspace/service")
    );
    assert_eq!(
        serde_json::to_value(&source).unwrap()["mount_path"],
        "/workspace/service"
    );
    source.mount_path.clear();
    let legacy = serde_json::to_value(&source).unwrap();
    assert!(legacy.get("mount_path").is_none());
    let decoded: GitRepositorySource = serde_json::from_value(legacy).unwrap();
    assert_eq!(
        decoded.get_directory("/workspace"),
        PathBuf::from("/workspace/repository")
    );
}

/// 验证 Claude 默认值与 source 路径、校验辅助函数。
#[test]
fn test_default_claude_config_and_sources() {
    let config = default_claude_config();
    assert_eq!(config.command, "claude");
    assert!(config.args.is_empty());
    assert!(config.environment.is_empty());

    let base = BaseSource {
        source_type: "custom".into(),
    };
    assert_eq!(base.get_type(), "custom");
    assert!(!base.is_repository());
    assert_eq!(
        base.get_directory("/workspace"),
        PathBuf::from("/workspace")
    );

    let source = GitRepositorySource {
        base: BaseSource {
            source_type: "git_repository".into(),
        },
        git_info: GitInfo {
            repo: "anthropic/demo".into(),
            git_ref: Some("main".into()),
            ..GitInfo::default()
        },
        ..GitRepositorySource::default()
    };
    source.validate().expect("valid source should pass");
    assert!(source.is_repository());
    assert_eq!(
        source.get_directory("/workspace"),
        PathBuf::from("/workspace/demo")
    );
}

/// 验证 MCP 本地、远端、混合和缺失配置。
#[test]
fn test_mcp_server_config_validation() {
    let remote = McpServerConfig {
        server_type: "http".into(),
        url: "https://example.test/mcp".into(),
        ..McpServerConfig::default()
    };
    assert!(remote.is_remote());
    remote.validate().expect("remote MCP config should pass");

    let local = McpServerConfig {
        command: "node".into(),
        ..McpServerConfig::default()
    };
    assert!(!local.is_remote());
    local.validate().expect("local MCP config should pass");

    assert!(
        McpServerConfig::default()
            .validate()
            .unwrap_err()
            .to_string()
            .contains("must specify either remote")
    );
    let mixed = McpServerConfig {
        server_type: "http".into(),
        url: "https://example.test/mcp".into(),
        command: "node".into(),
        ..McpServerConfig::default()
    };
    assert!(
        mixed
            .validate()
            .unwrap_err()
            .to_string()
            .contains("cannot have both remote")
    );
}

/// 验证 startup context 的完整 JSON、source 分派和 MCP 未知字段保留。
#[test]
fn test_startup_context_deserialize_validate_and_mcp_extras() {
    let value = json!({
        "api_base_url": "api.example.test",
        "sources": [{
            "type": "git_repository",
            "git_info": {"type": "github", "repo": "anthropic/demo", "ref": "main", "host": "github"}
        }],
        "outcomes": [{
            "type": "git_repository",
            "git_info": {"type": "github", "repo": "anthropic/demo", "branches": ["task"]}
        }],
        "mcp_config": {
            "mcpServers": {
                "remote": {
                    "type": "http",
                    "url": "https://example.test/mcp",
                    "tools": [{"name": "alpha", "enabled": true, "permission_policy": "allow"}],
                    "extra_server": "kept"
                },
                "disabled": null
            },
            "extra_root": "kept"
        },
        "allowed_tools": ["Read"],
        "claude_code_args": {"model": "claude-test"},
        "mcp_config_file": {"path": "/tmp/mcp.json", "content": "e30=", "mode": 384},
        "environment_variables": {"FOO": "bar"},
        "use_code_sessions": true
    });

    let startup: StartupContext =
        serde_json::from_value(value).expect("complete startup context should deserialize");
    startup
        .validate()
        .expect("complete startup context should validate");
    assert_eq!(startup.sources[0].get_type(), "git_repository");
    assert_eq!(startup.allowed_tools, ["Read"]);
    assert!(startup.use_code_sessions);

    let mcp = startup.mcp_config.expect("MCP config should be present");
    assert_eq!(mcp.extra["extra_root"], "kept");
    let remote = mcp.mcp_servers["remote"]
        .as_ref()
        .expect("non-null MCP server should decode");
    assert_eq!(remote.extra["extra_server"], "kept");
    assert_eq!(
        remote.tools[0].permission_policy,
        PermissionPolicy("allow".into())
    );
    assert!(mcp.mcp_servers["disabled"].is_none());
}

/// 验证 startup/session 校验错误和未知 source 错误。
#[test]
fn test_startup_context_reports_validation_errors() {
    assert!(
        StartupContext::default()
            .validate()
            .unwrap_err()
            .to_string()
            .contains("api_base_url")
    );

    let invalid_source = StartupContext {
        api_base_url: "api.example.test".into(),
        sources: vec![Source::GitRepository(GitRepositorySource {
            base: BaseSource {
                source_type: "git_repository".into(),
            },
            ..GitRepositorySource::default()
        })],
        ..StartupContext::default()
    };
    assert!(
        invalid_source
            .validate()
            .unwrap_err()
            .to_string()
            .contains("repository is required")
    );

    let unknown = serde_json::from_value::<StartupContext>(json!({
        "api_base_url": "api.example.test",
        "sources": [{"type": "unknown"}]
    }))
    .expect_err("unknown source should fail");
    assert!(unknown.to_string().contains("unknown source type"));

    assert!(
        Session::default()
            .validate()
            .unwrap_err()
            .to_string()
            .contains("session id is required")
    );
    let no_startup = Session {
        session_id: "session-1".into(),
        ..Session::default()
    };
    assert!(
        no_startup
            .validate()
            .unwrap_err()
            .to_string()
            .contains("startup context is required")
    );
}

/// 验证四种模式及空字符串默认值。
#[test]
fn test_session_validate_and_parse_session_mode() {
    for (wire, expected) in [
        ("new", SessionMode::New),
        ("setup-only", SessionMode::SetupOnly),
        ("resume", SessionMode::Resume),
        ("resume-cached", SessionMode::ResumeCached),
    ] {
        assert_eq!(parse_session_mode(wire), Ok(expected));
        assert_eq!(expected.to_string(), wire);
        assert!(expected.is_valid());
    }
    assert_eq!(parse_session_mode(""), Ok(SessionMode::New));
    assert!(
        parse_session_mode("invalid")
            .unwrap_err()
            .to_string()
            .contains("invalid session mode")
    );
}
