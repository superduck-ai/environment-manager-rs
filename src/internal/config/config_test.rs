//! `internal/config` 的行为回归测试，机械对应 Go 的 `config_test.go`。

use std::path::PathBuf;

use serde_json::{Value, json};

use super::*;

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
fn test_startup_context_unmarshal_validate_and_mcp_extras() {
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

/// 对应 Go：`RawStartupContext` 通过 `encoding/json` 接收显式 null 的零值语义。
///
/// V0 stdin 来自旧客户端，历史 payload 可能把所有值类型字段写成 null。Go 会继续
/// 构造一个全零值 startup context，领域校验随后才报告缺失 API URL；Rust 不能在
/// Serde 类型检查阶段提前失败，否则错误阶段和对外文本都会发生变化。
#[test]
fn test_startup_context_explicit_null_fields_use_go_zero_values() {
    let startup: StartupContext = serde_json::from_value(json!({
        "sources": null,
        "api_base_url": null,
        "outcomes": null,
        "custom_system_prompt": null,
        "append_system_prompt": null,
        "model": null,
        "mcp_config": null,
        "allowed_tools": null,
        "disallowed_tools": null,
        "enabled_tools": null,
        "claude_code_args": null,
        "mcp_config_file": null,
        "use_sandbox_gateway_config": null,
        "entrypoint": null,
        "environment_variables": null,
        "environment_sub_type": null,
        "use_code_sessions": null
    }))
    .expect("V0 startup 的显式 null 值类型字段应采用 Go 零值");
    assert_eq!(startup, StartupContext::default());

    let top_level_null: StartupContext =
        serde_json::from_value(Value::Null).expect("顶层 null 应解成零值 startup context");
    assert_eq!(top_level_null, StartupContext::default());
}

/// 验证嵌套配置 DTO 的 null、null 元素和 null map value 都遵循 Go 零值规则。
///
/// 这些字段会同时出现在 V0 stdin、V1 session context 与 secret 中。测试刻意保留
/// Option 字段的 None 语义，并只把 Go 值类型转换为空字符串、false、0 或默认结构。
#[test]
fn test_nested_config_explicit_null_values_match_go_encoding_json() {
    let git_info: GitInfo = serde_json::from_value(json!({
        "type": null,
        "repo": null,
        "ref": null,
        "url": null,
        "host": null,
        "auth": null,
        "allow_unrestricted_git_push": null
    }))
    .expect("GitInfo 的值类型 null 应解析");
    assert_eq!(git_info, GitInfo::default());

    let repository: GitRepositorySource = serde_json::from_value(json!({
        "type": "git_repository",
        "git_info": null
    }))
    .expect("null git_info 应变为默认结构");
    assert_eq!(repository.git_info, GitInfo::default());

    let server: McpServerConfig = serde_json::from_value(json!({
        "type": null,
        "url": null,
        "command": null,
        "args": [null],
        "headers": {"X-Empty": null},
        "tools": [null, {
            "name": null,
            "enabled": null,
            "permission_policy": null
        }]
    }))
    .expect("MCP server 的显式 null 应采用 Go 零值");
    assert_eq!(server.server_type, "");
    assert_eq!(server.url, "");
    assert_eq!(server.command, "");
    assert_eq!(server.args, [""]);
    assert_eq!(server.headers["X-Empty"], "");
    assert_eq!(
        server.tools,
        vec![
            McpServerConfigTool::default(),
            McpServerConfigTool::default()
        ]
    );

    let mcp: McpConfig =
        serde_json::from_value(json!({"mcpServers": null})).expect("null mcpServers 应变为空 map");
    assert!(mcp.mcp_servers.is_empty());

    let file: McpConfigFile = serde_json::from_value(json!({
        "path": null,
        "content": null,
        "mode": null
    }))
    .expect("MCP 文件值类型 null 应解析");
    assert_eq!(file, McpConfigFile::default());

    let outcome: OutcomeField = serde_json::from_value(json!({
        "type": null,
        "git_info": {
            "type": null,
            "repo": null,
            "branches": [null]
        }
    }))
    .expect("outcome 的显式 null 应采用 Go 零值");
    assert_eq!(outcome.outcome_type, "");
    assert_eq!(outcome.git_info.git_type, "");
    assert_eq!(outcome.git_info.repo, "");
    assert_eq!(outcome.git_info.branches, [""]);

    let null_git_info: OutcomeField = serde_json::from_value(json!({
        "type": "git_repository",
        "git_info": null
    }))
    .expect("null outcome git_info 应变为默认结构");
    assert_eq!(null_git_info.git_info, GitOutcomeInfo::default());
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
