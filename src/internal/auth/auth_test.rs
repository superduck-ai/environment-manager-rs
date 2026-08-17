//! `internal/auth` 的行为回归测试，覆盖 provider 解析、凭据访问和错误边界。

use super::*;

///
/// 将测试认证配置编码成生产解析器实际接收的 JSON 字节，避免 fixture 手写 JSON 时
/// 绕过 `type` 字段 rename 或意外改变序列化行为。
fn auth_config_json(configurations: &[AuthConfig]) -> Vec<u8> {
    serde_json::to_vec(configurations).expect("测试认证配置必须能够序列化")
}

///
/// 验证全部 provider、getter，以及 session 与 ingress token 的覆盖 setter。
#[test]
fn test_new_auth_context_with_session_id_parses_all_providers() {
    let auth_json = auth_config_json(&[
        AuthConfig {
            auth_type: "session_ingress".into(),
            token: "session-ingress-token".into(),
        },
        AuthConfig {
            auth_type: "anthropic_api".into(),
            token: "anthropic-api-token".into(),
        },
        AuthConfig {
            auth_type: "anthropic_oauth".into(),
            token: "anthropic-oauth-token".into(),
        },
        AuthConfig {
            auth_type: "vercel_deploy".into(),
            token: "vercel-token".into(),
        },
        AuthConfig {
            auth_type: "antspace_deploy".into(),
            token: r#"{"url":"https://control.example.test","auth_token":"antspace-token"}"#
                .into(),
        },
        AuthConfig {
            auth_type: "supabase".into(),
            token: r#"{"project_ref":"project-ref","anon_key":"anon-key","db_pass":"db-pass","pat":"supabase-pat"}"#
                .into(),
        },
    ]);

    let mut context = new_auth_context_with_session_id(&auth_json, "session-123")
        .expect("全部已恢复 provider 应当解析成功");
    assert_eq!(context.get_session_id(), "session-123");
    assert_eq!(context.get_session_ingress_token(), "session-ingress-token");
    assert_eq!(context.get_anthropic_api_token(), "anthropic-api-token");
    assert_eq!(context.get_anthropic_oauth_token(), "anthropic-oauth-token");
    assert_eq!(context.get_vercel_deploy_token(), "vercel-token");
    assert_eq!(
        context.get_antspace_control_plane_url(),
        "https://control.example.test"
    );
    assert_eq!(context.get_antspace_auth_token(), "antspace-token");
    assert_eq!(context.get_supabase_project_ref(), "project-ref");
    assert_eq!(context.get_supabase_anon_key(), "anon-key");
    assert_eq!(context.get_supabase_db_pass(), "db-pass");
    assert_eq!(context.get_supabase_pat(), "supabase-pat");
    assert!(context.has_supabase());

    context.set_session_id("session-456");
    context.set_session_ingress_token("new-ingress-token");
    assert_eq!(context.get_session_id(), "session-456");
    assert_eq!(context.get_session_ingress_token(), "new-ingress-token");
}

///
/// 验证 `None`/空 auth wire 值只保留 session ID，不凭空产生任何 provider 凭据。
#[test]
fn test_new_auth_context_with_session_id_handles_empty_auth() {
    let context = new_auth_context_with_session_id(&[], "session-123").expect("空 auth 必须被接受");
    assert_eq!(context.get_session_id(), "session-123");
    assert!(context.get_session_ingress_token().is_empty());
    assert!(context.get_anthropic_api_token().is_empty());
    assert!(!context.has_supabase());
}

/// 验证嵌套凭据的缺失字段采用默认值。
#[test]
fn auth_missing_nested_fields_use_defaults() {
    let missing_fields = br#"[
        {"type":"antspace_deploy","token":"{}"},
        {"type":"supabase","token":"{}"}
    ]"#;
    let context = new_auth_context_with_session_id(missing_fields, "session")
        .expect("缺失的凭据字符串使用空默认值");
    assert!(context.get_antspace_control_plane_url().is_empty());
    assert!(context.get_antspace_auth_token().is_empty());
    assert!(context.get_supabase_project_ref().is_empty());
    assert!(context.get_supabase_anon_key().is_empty());
    assert!(!context.has_supabase());
}

///
/// 对非法顶层 JSON、每种 provider 的空 token、嵌套凭据 JSON 和未知 provider
/// 逐项校验稳定错误文本，防止错误归类顺序在重构中变化。
#[test]
fn test_new_auth_context_with_session_id_reports_errors() {
    let invalid =
        new_auth_context_with_session_id(b"{bad json", "session").expect_err("非法 JSON 必须失败");
    assert!(
        invalid
            .to_string()
            .contains("failed to parse auth configuration")
    );

    let cases = [
        (
            AuthConfig {
                auth_type: "session_ingress".into(),
                token: String::new(),
            },
            "session_ingress auth configuration missing required 'token' field",
        ),
        (
            AuthConfig {
                auth_type: "anthropic_api".into(),
                token: String::new(),
            },
            "anthropic_api auth configuration missing required 'token' field",
        ),
        (
            AuthConfig {
                auth_type: "anthropic_oauth".into(),
                token: String::new(),
            },
            "anthropic_oauth auth configuration missing required 'token' field",
        ),
        (
            AuthConfig {
                auth_type: "vercel_deploy".into(),
                token: String::new(),
            },
            "vercel_deploy auth configuration missing required 'token' field",
        ),
        (
            AuthConfig {
                auth_type: "antspace_deploy".into(),
                token: String::new(),
            },
            "antspace_deploy auth configuration missing required 'token' field",
        ),
        (
            AuthConfig {
                auth_type: "antspace_deploy".into(),
                token: "not-json".into(),
            },
            "failed to parse antspace deploy credentials",
        ),
        (
            AuthConfig {
                auth_type: "supabase".into(),
                token: String::new(),
            },
            "supabase auth configuration missing required 'token' field",
        ),
        (
            AuthConfig {
                auth_type: "supabase".into(),
                token: "not-json".into(),
            },
            "failed to parse supabase credentials",
        ),
        (
            AuthConfig {
                auth_type: "unknown".into(),
                token: "token".into(),
            },
            "unknown auth provider type: unknown",
        ),
    ];

    for (configuration, expected) in cases {
        let error =
            new_auth_context_with_session_id(&auth_config_json(&[configuration]), "session")
                .expect_err("错误 fixture 必须被拒绝");
        assert!(
            error.to_string().contains(expected),
            "错误 {error:?} 应包含 {expected:?}"
        );
    }
}

///
/// 验证普通 token 与 username/password token 都只注入 HTTPS URL，且路径、host
/// 与 scheme 不发生变化。
#[test]
fn test_git_hub_source_auth_provider_authenticates_https_urls() {
    let context = AuthContext::default();
    let permission = Permission::from("read");

    let provider = new_github_source_auth_provider("ghp-secret");
    assert_eq!(provider.auth_type(), GitHubAuthType::Token);
    let (authenticated, ok) = provider.authenticate_url(
        &context,
        "https://github.com/anthropic/demo.git",
        &permission,
    );
    assert!(ok);
    let parsed = url::Url::parse(&authenticated).expect("认证 URL 应可解析");
    assert_eq!(parsed.username(), "ghp-secret");
    assert_eq!(parsed.scheme(), "https");
    assert_eq!(parsed.host_str(), Some("github.com"));
    assert_eq!(parsed.path(), "/anthropic/demo.git");

    let colon_provider = new_github_source_auth_provider("x-access-token:secret");
    let (authenticated, ok) = colon_provider.authenticate_url(
        &context,
        "https://github.com/anthropic/demo.git",
        &permission,
    );
    assert!(ok);
    let parsed = url::Url::parse(&authenticated).expect("用户名/密码 URL 应可解析");
    assert_eq!(parsed.username(), "x-access-token");
    assert_eq!(parsed.password(), Some("secret"));

    let (without_path, ok) = provider.authenticate_url(
        &context,
        "https://github.com?tab=repositories#top",
        &permission,
    );
    assert!(ok);
    assert_eq!(
        without_path,
        "https://ghp-secret@github.com?tab=repositories#top"
    );

    let (explicit_root, ok) =
        provider.authenticate_url(&context, "https://github.com/", &permission);
    assert!(ok);
    assert_eq!(explicit_root, "https://ghp-secret@github.com/");
}

///
/// 验证 git proxy JWT 的固定用户名，以及 SSH、HTTP、非法 URL 和空 token 均
/// 原样返回并报告未认证。
#[test]
fn test_git_hub_source_auth_provider_handles_proxy_jwt_and_unsupported_urls() {
    let context = AuthContext::default();
    let permission = Permission::from("read");
    let provider = new_github_source_auth_provider("sk-ant-ccsr-jwt");
    assert_eq!(provider.auth_type(), GitHubAuthType::GitProxyJwt);

    let (authenticated, ok) = provider.authenticate_url(
        &context,
        "https://github.com/anthropic/demo.git",
        &permission,
    );
    assert!(ok);
    let parsed = url::Url::parse(&authenticated).expect("proxy URL 应可解析");
    assert_eq!(parsed.username(), "unused");
    assert_eq!(parsed.password(), Some("sk-ant-ccsr-jwt"));

    for source in [
        "git@github.com:anthropic/demo.git",
        "ssh://git@github.com/anthropic/demo.git",
        "http://github.com/anthropic/demo.git",
        "https://%",
    ] {
        assert_eq!(
            provider.authenticate_url(&context, source, &permission),
            (source.to_owned(), false)
        );
    }

    let empty_provider = new_github_source_auth_provider("");
    let public = "https://github.com/anthropic/demo.git";
    assert_eq!(
        empty_provider.authenticate_url(&context, public, &permission),
        (public.to_owned(), false)
    );
}

///
/// 验证认证 URL、proxy userinfo 与裸 token 都不会从 git 子进程输出中泄漏，同时
/// 保留公开仓库 URL 以便诊断。
#[test]
fn test_git_hub_source_auth_provider_sanitizes_output() {
    let provider = new_github_source_auth_provider("sk-ant-ccsr-secret");
    let public = "https://github.com/anthropic/demo.git";
    let authenticated = "https://unused:sk-ant-ccsr-secret@github.com/anthropic/demo.git";
    let output = format!(
        "fatal: could not read from {authenticated} using unused:sk-ant-ccsr-secret and sk-ant-ccsr-secret"
    );
    let sanitized = provider.sanitize_output(&output, public, authenticated);

    for secret in [
        authenticated,
        "unused:sk-ant-ccsr-secret",
        "sk-ant-ccsr-secret",
    ] {
        assert!(!sanitized.contains(secret), "输出泄漏了 {secret:?}");
    }
    assert!(sanitized.contains(public));
    assert!(sanitized.contains("unused:***"));
}
