//! `internal/input` 的行为回归测试。

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use serde_json::{Value, json};

use super::*;
use crate::internal::auth::AuthConfig;

///
/// 验证 work secret 的 URL-safe Base64、source、auth 和全部 gateway 字段。
#[test]
fn test_work_secret_unmarshal_and_decode() {
    let encoded = encoded_work_secret(json!({
        "version": 1,
        "session_ingress_token": "ingress-token",
        "api_base_url": "api.example.test",
        "sources": [git_source_json("anthropic/demo", "main")],
        "auth": [{"type": "anthropic_api", "token": "api-token"}],
        "claude_code_args": {"model": "claude-test"},
        "mcp_config": {"path": "/tmp/mcp.json", "content": "e30=", "mode": 384},
        "use_sandbox_gateway_config": true,
        "environment_variables": {"FOO": "bar"},
        "use_code_sessions": true
    }));

    let secret = decode_work_secret(&encoded).expect("valid secret should decode");
    assert_eq!(secret.version, 1);
    assert_eq!(secret.session_ingress_token, "ingress-token");
    assert_eq!(secret.api_base_url, "api.example.test");
    assert_eq!(secret.sources[0].get_type(), "git_repository");
    let crate::internal::config::Source::GitRepository(source) = &secret.sources[0];
    assert_eq!(source.git_info.repo, "anthropic/demo");
    assert_eq!(source.git_info.git_ref.as_deref(), Some("main"));
    assert_eq!(secret.auth[0].auth_type, "anthropic_api");
    assert_eq!(secret.auth[0].token, "api-token");
    assert_eq!(
        secret.claude_code_args.as_ref().unwrap()["model"],
        "claude-test"
    );
    assert_eq!(secret.mcp_config.as_ref().unwrap().path, "/tmp/mcp.json");
    assert!(secret.use_sandbox_gateway_config);
    assert_eq!(secret.environment_variables.as_ref().unwrap()["FOO"], "bar");
    assert!(secret.use_code_sessions);
}

///
/// 验证空值、Base64、JSON、版本和 source 类型错误都被折叠成稳定错误。
#[test]
fn test_decode_work_secret_reports_errors() {
    assert_eq!(
        decode_work_secret("").unwrap_err().to_string(),
        "secret is empty"
    );
    assert_eq!(
        decode_work_secret("not base64").unwrap_err().to_string(),
        "invalid secret"
    );
    assert_eq!(
        decode_work_secret(&URL_SAFE.encode(b"{bad json"))
            .unwrap_err()
            .to_string(),
        "invalid secret"
    );
    assert!(
        decode_work_secret(&encoded_work_secret(json!({
            "version": 2,
            "api_base_url": "api.example.test"
        })))
        .unwrap_err()
        .to_string()
        .contains("unsupported secret version")
    );
    assert!(
        decode_work_secret(&encoded_work_secret(json!({
            "version": 1,
            "sources": [{"type": "unknown"}]
        })))
        .unwrap_err()
        .to_string()
        .contains("invalid secret")
    );
}

/// 验证顶层 null 与值类型 null 的处理。
///
/// 顶层 null 或 null version 都先形成 version=0 的零值 secret，再由版本门禁返回
/// UnsupportedVersion；不能提前折叠成 Invalid，否则 CLI 的错误分类和文本会变化。
#[test]
fn test_work_secret_top_level_and_version_null_reach_version_gate() {
    for encoded in [
        URL_SAFE.encode(b"null"),
        encoded_work_secret(json!({"version": null})),
    ] {
        assert_eq!(
            decode_work_secret(&encoded).unwrap_err().to_string(),
            "unsupported secret version. Download the latest version of the Claude Environment Runner"
        );
    }
}

/// 验证 V1 secret 的 null slice 元素、map value 与 MCP 文件字段采用默认值。
///
/// auth 中的 null 必须保留为一个默认 AuthConfig，供后续认证层按 provider 顺序
/// 报错；可选 map 仍保持 Some，以免把“已提供但值为空”误当成字段缺失。
#[test]
fn test_work_secret_explicit_null_values_use_defaults() {
    let secret = decode_work_secret(&encoded_work_secret(json!({
        "version": 1,
        "session_ingress_token": null,
        "api_base_url": null,
        "sources": null,
        "auth": [null],
        "claude_code_args": {"model": null},
        "mcp_config": {"path": null, "content": null, "mode": null},
        "use_sandbox_gateway_config": null,
        "environment_variables": {"EMPTY": null},
        "use_code_sessions": null
    })))
    .expect("值类型 null 的 V1 secret 应成功解码");

    assert_eq!(secret.session_ingress_token, "");
    assert_eq!(secret.api_base_url, "");
    assert!(secret.sources.is_empty());
    assert_eq!(secret.auth, [AuthConfig::default()]);
    assert_eq!(secret.claude_code_args.as_ref().unwrap()["model"], "");
    assert_eq!(
        secret.mcp_config.as_ref(),
        Some(&crate::internal::config::McpConfigFile::default())
    );
    assert!(!secret.use_sandbox_gateway_config);
    assert_eq!(secret.environment_variables.as_ref().unwrap()["EMPTY"], "");
    assert!(!secret.use_code_sessions);

    let optional_nulls = decode_work_secret(&encoded_work_secret(json!({
        "version": 1,
        "auth": null,
        "claude_code_args": null,
        "mcp_config": null,
        "environment_variables": null
    })))
    .expect("可选 null 字段应保留 Go nil 语义");
    assert!(optional_nulls.auth.is_empty());
    assert!(optional_nulls.claude_code_args.is_none());
    assert!(optional_nulls.mcp_config.is_none());
    assert!(optional_nulls.environment_variables.is_none());
}

///
/// 验证 session/healthcheck 两个合法联合体分支，并逐项检查 envelope 校验顺序。
#[test]
fn test_parse_v1_work_response_validation() {
    let session = parse_v1_work_response(
        br#"{"id":"work-1","type":"work","environment_id":"env-1","data":{"type":"session","id":"session-1"},"secret":"abc"}"#,
    )
    .expect("session work envelope 应合法");
    assert_eq!(session.get_session_id(), "session-1");
    assert!(!session.is_healthcheck());

    let healthcheck = parse_v1_work_response(
        br#"{"id":"work-1","type":"work","environment_id":"env-1","data":{"type":"healthcheck"},"secret":"abc"}"#,
    )
    .expect("healthcheck work envelope 应合法");
    assert!(healthcheck.is_healthcheck());

    assert!(
        parse_v1_work_response(b"{bad")
            .unwrap_err()
            .to_string()
            .contains("failed to parse V1 work response")
    );
    assert!(
        parse_v1_work_response(br#"{"type":"other","data":{"type":"session","id":"s"}}"#)
            .unwrap_err()
            .to_string()
            .contains("unexpected type")
    );
    assert!(
        parse_v1_work_response(br#"{"type":"work","data":{"type":"session"}}"#)
            .unwrap_err()
            .to_string()
            .contains("missing session ID")
    );
    assert!(
        parse_v1_work_response(br#"{"type":"work","data":{"type":"other"}}"#)
            .unwrap_err()
            .to_string()
            .contains("unexpected data.type")
    );
}

/// 验证 code-session secret 不调用 session-context provider，并使用 cwd 回退值。
#[tokio::test]
async fn test_v1_parser_parses_code_sessions_secret_without_fetching_session_context() {
    let secret = encoded_work_secret(json!({
        "version": 1,
        "session_ingress_token": "ingress-token",
        "api_base_url": "api.example.test",
        "sources": [git_source_json("anthropic/demo", "main")],
        "auth": [{"type": "anthropic_oauth", "token": "oauth-token"}],
        "claude_code_args": {"model": "claude-opus"},
        "environment_variables": {"A": "B"},
        "use_code_sessions": true
    }));
    let parser = V1Parser::without_session_context_provider("/workspace");
    let parsed = parser
        .parse(&v1_work_response_json(
            "work-1",
            "env-1",
            "session",
            "session-1",
            &secret,
        ))
        .await
        .expect("code-session work should parse without provider");

    assert_eq!(parsed.session_id, "session-1");
    assert_eq!(parsed.work_id, "work-1");
    assert_eq!(
        parsed.auth_context.get_anthropic_oauth_token(),
        "oauth-token"
    );
    assert_eq!(
        parsed.auth_context.get_session_ingress_token(),
        "ingress-token"
    );
    assert_eq!(
        parsed.startup_context.api_base_url,
        "https://api.example.test"
    );
    assert!(parsed.startup_context.use_code_sessions);
    assert_eq!(
        parsed.environment.as_ref().unwrap().config["cwd"],
        "/workspace"
    );
    assert!(parsed.outcomes.is_empty());
}

#[derive(Clone)]
struct FixtureProvider {
    context: SessionContext,
    observed: Arc<Mutex<Vec<(String, String, String)>>>,
}

impl SessionContextProvider for FixtureProvider {
    /// 记录 provider 请求并返回固定 session fixture。
    fn get_session_context<'a>(
        &'a self,
        request: SessionContextRequest<'a>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<SessionContext, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.observed
                .lock()
                .expect("observation lock poisoned")
                .push((
                    request.api_base_url.to_owned(),
                    request.session_ingress_token.to_owned(),
                    request.session_id.to_owned(),
                ));
            Ok(self.context.clone())
        })
    }
}

/// 验证旧 V1 工作流通过 provider 获取 context，并合并 prompt、MCP 和 outcomes。
#[tokio::test]
async fn test_v1_parser_fetches_session_context_and_builds_result() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let provider = FixtureProvider {
        context: SessionContext {
            cwd: "/session/cwd".into(),
            model: "claude-sonnet".into(),
            custom_system_prompt: "custom".into(),
            append_system_prompt: "append".into(),
            outcomes: Some(json!([{
                "type": "git_repository",
                "git_info": {"type": "github", "repo": "anthropic/demo", "branches": ["task-branch"]}
            }])),
            mcp_config: Some(json!({
                "mcpServers": {"demo": {"type": "http", "url": "http://localhost:1234/mcp"}}
            })),
        },
        observed: Arc::clone(&observed),
    };
    let secret = encoded_work_secret(json!({
        "version": 1,
        "session_ingress_token": "ingress-token",
        "api_base_url": "https://api.example.test",
        "sources": [git_source_json("anthropic/demo", "main")],
        "auth": [{"type": "session_ingress", "token": "old-token"}]
    }));
    let parser = V1Parser::new("/fallback", provider);
    let parsed = parser
        .parse(&v1_work_response_json(
            "work-1",
            "env-1",
            "session",
            "session-1",
            &secret,
        ))
        .await
        .expect("provider-backed V1 work should parse");

    assert_eq!(
        observed
            .lock()
            .expect("observation lock poisoned")
            .as_slice(),
        &[(
            "https://api.example.test".into(),
            "ingress-token".into(),
            "session-1".into()
        )]
    );
    assert_eq!(parsed.startup_context.model, "claude-sonnet");
    assert_eq!(parsed.startup_context.custom_system_prompt, "custom");
    assert_eq!(
        parsed
            .startup_context
            .mcp_config
            .as_ref()
            .unwrap()
            .mcp_servers["demo"]
            .as_ref()
            .expect("non-null MCP server should decode")
            .url,
        "http://localhost:1234/mcp"
    );
    assert_eq!(parsed.outcomes.get_first("anthropic/demo"), "task-branch");
    assert_eq!(
        parsed.environment.as_ref().unwrap().config["cwd"],
        "/session/cwd"
    );
}

/// 验证 V1 parser 不折叠 原始 JSON 的显式 null 与缺失字段。
#[tokio::test]
async fn test_v1_parser_preserves_explicit_null_session_fields() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let provider = FixtureProvider {
        context: SessionContext {
            outcomes: Some(Value::Null),
            mcp_config: Some(Value::Null),
            ..SessionContext::default()
        },
        observed,
    };
    let secret = encoded_work_secret(json!({
        "version": 1,
        "session_ingress_token": "ingress-token",
        "api_base_url": "api.example.test"
    }));
    let parsed = V1Parser::new("/workspace", provider)
        .parse(&v1_work_response_json(
            "work-1",
            "env-1",
            "session",
            "session-1",
            &secret,
        ))
        .await
        .expect("显式 null 应按 Go 零值语义解析");

    assert!(parsed.outcomes.is_empty());
    assert!(
        parsed.startup_context.mcp_config.is_some(),
        "显式 null mcp_config 在 Go 中会创建 non-nil 零值配置"
    );
}

/// 验证 healthcheck 只构建最小 startup/auth 元数据。
#[tokio::test]
async fn test_v1_parser_builds_healthcheck_result() {
    let secret = encoded_work_secret(json!({
        "version": 1,
        "session_ingress_token": "ingress-token",
        "api_base_url": "api.example.test",
        "use_code_sessions": true
    }));
    let parser = V1Parser::without_session_context_provider("/workspace");
    let parsed = parser
        .parse(&v1_work_response_json(
            "work-health",
            "env-1",
            "healthcheck",
            "",
            &secret,
        ))
        .await
        .expect("healthcheck should parse");
    assert!(parsed.is_healthcheck);
    assert_eq!(
        parsed.startup_context.api_base_url,
        "https://api.example.test"
    );
    assert!(parsed.environment.is_none());
    assert!(parsed.session_id.is_empty());
}

/// 验证 V0 startup、environment、auth 和单分支 outcomes。
#[test]
fn test_v0_parser_parses_input() {
    let input = json!({
        "startup_context": {
            "api_base_url": "api.example.test",
            "sources": [git_source_json("anthropic/demo", "main")],
            "outcomes": [
                {"type": "git_repository", "git_info": {"type": "github", "repo": "anthropic/demo", "branches": ["task-branch"]}},
                {"type": "git_repository", "git_info": {"type": "github", "repo": "ignored/demo", "branches": ["one", "two"]}}
            ],
            "claude_code_args": {"model": "claude-test"},
            "environment_sub_type": "baku"
        },
        "environment": {
            "environment_type": "anthropic",
            "cwd": "/workspace",
            "allowed_tools": "Read",
            "disallowed_tools": "Bash",
            "unknown": "preserved"
        },
        "auth": [{"type": "anthropic_api", "token": "api-token"}]
    });
    let parser = V0Parser {
        session_id: "session-1".into(),
    };
    let parsed = parser
        .parse(&must_json(&input))
        .expect("valid V0 input should parse");

    assert_eq!(parsed.session_id, "session-1");
    assert_eq!(parsed.auth_context.get_anthropic_api_token(), "api-token");
    assert_eq!(
        parsed.startup_context.api_base_url,
        "https://api.example.test"
    );
    assert_eq!(parsed.outcomes.get_first("anthropic/demo"), "task-branch");
    assert_eq!(parsed.outcomes.get_first("ignored/demo"), "");
    assert_eq!(
        parsed.environment.as_ref().unwrap().config["unknown"],
        "preserved"
    );
}

/// 验证缺失 startup context 只在 CLI 专用入口转换为错误。
///
/// 普通 `parse` 保留 panic payload，CLI 入口把它转换为 sentinel。
#[test]
fn v0_missing_startup_context_preserves_parser_panic_and_cli_sentinel() {
    let parser = V0Parser {
        session_id: "session-missing-startup".to_owned(),
    };
    let payload = br#"{"environment":{"environment_type":"byoc"}}"#;

    assert_nil_dereference_panic(catch_unwind(AssertUnwindSafe(|| parser.parse(payload))));

    let error = parser
        .parse_for_cli(payload)
        .expect_err("CLI 入口必须把 nil dereference 转换为专用 sentinel");
    assert!(matches!(error, V0ParseError::StartupContextRequired));
}

/// 断言 parser panic payload 保持 runtime 空值解引用 文本。
fn assert_nil_dereference_panic<T>(outcome: std::thread::Result<T>) {
    let payload = match outcome {
        Ok(_) => panic!("普通 V0 parser 入口必须继续 panic"),
        Err(payload) => payload,
    };
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str));
    assert_eq!(
        message,
        Some("runtime error: invalid memory address or nil pointer dereference")
    );
}

/// 验证 V0 environment 必填字段以及缺失 startup context 的安全错误。
#[test]
fn test_v0_parser_reports_environment_errors() {
    let parser = V0Parser {
        session_id: "session-1".into(),
    };
    assert!(
        parser
            .parse_environment(None)
            .unwrap_err()
            .to_string()
            .contains("environment configuration is required")
    );

    for (environment, expected) in [
        (json!({"cwd": "/workspace"}), "environment_type"),
        (
            json!({"environment_type": "anthropic"}),
            "cwd field is required",
        ),
    ] {
        let error = parser
            .parse_environment(Some(&environment))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(expected),
            "V0 environment error {error:?} should contain {expected:?}"
        );
    }
}

/// 构造测试用 git_repository source JSON。
fn git_source_json(repository: &str, git_ref: &str) -> Value {
    json!({
        "type": "git_repository",
        "git_info": {"type": "github", "host": "github", "repo": repository, "ref": git_ref}
    })
}

/// 将 JSON value 编码为 work secret 使用的 padded URL-safe Base64。
fn encoded_work_secret(value: Value) -> String {
    URL_SAFE.encode(work_secret_json(&value))
}

/// 通过统一 JSON helper 生成 secret 明文字节。
fn work_secret_json(fields: &Value) -> Vec<u8> {
    must_json(fields)
}

/// 构造完整 V1 work envelope JSON 字节。
fn v1_work_response_json(
    work_id: &str,
    environment_id: &str,
    data_type: &str,
    session_id: &str,
    secret: &str,
) -> Vec<u8> {
    must_json(&json!({
        "id": work_id,
        "type": "work",
        "environment_id": environment_id,
        "state": "available",
        "data": {"type": data_type, "id": session_id},
        "secret": secret,
        "created_at": "2026-05-31T00:00:00Z"
    }))
}

/// 让所有 fixture 通过相同的 Serde wire 编码路径。
fn must_json(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).expect("测试 JSON 必须能够序列化")
}

/// 静态断言 AuthConfig 仍能以 wire `type` 字段往返序列化。
#[test]
fn auth_config_wire_field_remains_type() {
    let value = serde_json::to_value(AuthConfig {
        auth_type: "anthropic_api".into(),
        token: "token".into(),
    })
    .expect("auth config should serialize");
    assert_eq!(value["type"], "anthropic_api");
}
