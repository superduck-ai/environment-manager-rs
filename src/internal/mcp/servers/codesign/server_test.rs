//! 对应 Go `internal/mcp/servers/codesign/server_test.go` 的完整行为测试。

use std::{
    os::unix::fs::PermissionsExt as _,
    path::Path,
    sync::{Arc, OnceLock},
};

use serde_json::{Value, json};
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;
use wiremock::{Mock, MockServer, Request, ResponseTemplate, matchers::method};

use crate::internal::{
    auth::{AuthConfig, AuthContext, new_auth_context_with_session_id},
    config::{BaseSource, GitInfo, GitRepositorySource, Source, StartupContext},
    mcp::{BaseServer, CallToolParams, CallToolRequest, CallToolResult},
    session::NoopActivityRecorder,
};

use super::{
    CodeSignMCPServer, ERR_EMPTY_CONTENT, ERR_SIGNING_FAILED, RemoteSignRequest, SignRequest,
    SourceInfo, configure_server, read_file_content, sanitize_source, should_register,
};

static SIGNING_URL_ENVIRONMENT_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

/// 保存并恢复 `SIGNING_SERVER_URL`，避免配置测试污染同进程其他用例。
struct SigningUrlEnvironment {
    previous: Option<std::ffi::OsString>,
}

impl SigningUrlEnvironment {
    /// 捕获原始值；调用方随后可在同一锁内切换测试输入。
    fn capture() -> Self {
        Self {
            previous: std::env::var_os("SIGNING_SERVER_URL"),
        }
    }

    /// 设置当前测试所需的 signing URL。
    fn set(value: &str) {
        // SAFETY: 该测试持有 SIGNING_URL_ENVIRONMENT_LOCK，且守卫会在释放锁前恢复。
        unsafe { std::env::set_var("SIGNING_SERVER_URL", value) };
    }
}

impl Drop for SigningUrlEnvironment {
    /// 恢复测试前的 signing URL。
    fn drop(&mut self) {
        // SAFETY: 修改和恢复都发生在同一个进程级互斥锁保护区内。
        unsafe {
            match &self.previous {
                Some(value) => std::env::set_var("SIGNING_SERVER_URL", value),
                None => std::env::remove_var("SIGNING_SERVER_URL"),
            }
        }
    }
}

/// 对应 Go `observedSignRequest`，保存远程签名请求的可观察字段。
struct ObservedSignRequest {
    path: String,
    authorization: String,
    anthropic_version: String,
    runner_version: String,
    claude_code_version: String,
    body: RemoteSignRequest,
}

/// 从 wiremock 捕获请求中提取一个 header，缺失时返回空字符串。
fn request_header(request: &Request, name: &str) -> String {
    request
        .headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

/// 把 wiremock 请求解析为 Go 测试使用的观测结构。
fn observed_sign_request(request: &Request) -> ObservedSignRequest {
    ObservedSignRequest {
        path: request.url.path().to_owned(),
        authorization: request_header(request, "authorization"),
        anthropic_version: request_header(request, "anthropic-version"),
        runner_version: request_header(request, "x-environment-runner-version"),
        claude_code_version: request_header(request, "x-claude-code-version"),
        body: serde_json::from_slice(&request.body).expect("解析签名请求 JSON"),
    }
}

/// 对应 Go `startSigningTestServer`，返回固定状态和正文的 loopback server。
async fn start_signing_test_server(status: u16, body: &str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(status).set_body_raw(body, "application/json"))
        .expect(1)
        .mount(&server)
        .await;
    server
}

/// 对应 Go `newCodesignAuthContext`，构造带 session/ingress token 的真实上下文。
fn new_codesign_auth_context(session_id: &str, session_ingress_token: &str) -> Arc<AuthContext> {
    let auth = if session_ingress_token.is_empty() {
        Vec::new()
    } else {
        serde_json::to_vec(&[AuthConfig {
            auth_type: "session_ingress".to_owned(),
            token: session_ingress_token.to_owned(),
        }])
        .expect("序列化 ingress auth")
    };
    Arc::new(
        new_auth_context_with_session_id(&auth, session_id).expect("构造 code-sign auth context"),
    )
}

/// 对应 Go `mcpResultContains`，只检查实际返回的 MCP 文本内容。
fn mcp_result_contains(result: &CallToolResult, want: &str) -> bool {
    result
        .content
        .iter()
        .any(|content| content.text.contains(want))
}

/// 创建用于 source 匹配/脱敏测试的 Git repository source。
fn git_source_with_secrets() -> Source {
    Source::GitRepository(GitRepositorySource {
        base: BaseSource {
            source_type: "git_repository".to_owned(),
        },
        git_info: GitInfo {
            git_type: "github".to_owned(),
            repo: "anthropic/demo".to_owned(),
            git_ref: Some("main".to_owned()),
            url: Some("https://token@example.test/anthropic/demo.git".to_owned()),
            auth: Some(AuthConfig {
                auth_type: "bearer".to_owned(),
                token: "secret-token".to_owned(),
            }),
            ..GitInfo::default()
        },
    })
}

#[tokio::test(flavor = "current_thread")]
async fn test_configure_server_resolves_signing_server_url() {
    // 父测试进程只启动隔离子进程，不在自身读写全局环境。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_configure_server_resolves_signing_server_url"
    )) {
        return;
    }
    // SIGNING_SERVER_URL 是进程全局状态；先获取 crate 级锁，再获取模块锁，
    // 统一所有环境写入的锁顺序并避免其他模块同时启动子进程。
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let _lock = SIGNING_URL_ENVIRONMENT_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let _environment = SigningUrlEnvironment::capture();
    let auth = new_codesign_auth_context("session-id", "ingress-token");

    SigningUrlEnvironment::set("");
    let server = configure_server(
        Arc::new(BaseServer::new("codesign", "0.0.1")),
        Some(Arc::clone(&auth)),
        Some(Arc::new(StartupContext {
            api_base_url: "api.example.test/".to_owned(),
            ..StartupContext::default()
        })),
        "/workspace".into(),
        Arc::new(NoopActivityRecorder),
    )
    .await
    .expect("使用 API base URL 配置");
    assert_eq!(server.signing_server_url(), "http://api.example.test");
    assert_eq!(server.working_dir, Path::new("/workspace"));
    assert_eq!(
        server
            .auth_context
            .as_deref()
            .map(AuthContext::get_session_id),
        Some("session-id")
    );

    SigningUrlEnvironment::set("https://signing.example.test/base");
    let server = configure_server(
        Arc::new(BaseServer::new("codesign", "0.0.1")),
        Some(Arc::clone(&auth)),
        Some(Arc::new(StartupContext {
            api_base_url: "api.example.test".to_owned(),
            ..StartupContext::default()
        })),
        "/workspace".into(),
        Arc::new(NoopActivityRecorder),
    )
    .await
    .expect("环境 URL 优先");
    assert_eq!(
        server.signing_server_url(),
        "https://signing.example.test/base"
    );

    SigningUrlEnvironment::set("");
    let error = configure_server(
        Arc::new(BaseServer::new("codesign", "0.0.1")),
        Some(auth),
        Some(Arc::new(StartupContext::default())),
        "/workspace".into(),
        Arc::new(NoopActivityRecorder),
    )
    .await
    .expect_err("缺失 API base URL 必须失败");
    assert!(error.to_string().contains("API base URL is required"));
    assert!(should_register(None, None));
}

#[test]
fn test_get_tools_and_identify_source() {
    let workspace = tempdir().expect("workspace");
    let repository = workspace.path().join("demo");
    let nested = repository.join("nested");
    std::fs::create_dir_all(&nested).expect("创建 repository");
    let server = Arc::new(CodeSignMCPServer::new(
        Arc::new(BaseServer::new("codesign", "0.0.1")),
        None,
        workspace.path().to_path_buf(),
        "http://127.0.0.1",
        Some(Arc::new(StartupContext {
            sources: vec![git_source_with_secrets()],
            ..StartupContext::default()
        })),
    ));

    let tools = server.get_tools();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].tool.name, "sign_file");
    assert_eq!(tools[0].tool.input_schema["required"], json!(["file_path"]));
    let exact = server.identify_source(repository.to_str().expect("UTF-8 path"));
    assert_eq!(exact.name, "anthropic/demo (github)");
    assert!(exact.source.is_some());
    let nested = server.identify_source(nested.to_str().expect("UTF-8 path"));
    assert_eq!(nested.name, "anthropic/demo (github)");
    assert!(nested.source.is_some());
    let unknown =
        server.identify_source(workspace.path().join("other").to_str().expect("UTF-8 path"));
    assert_eq!(unknown.name, "unknown");
    assert!(unknown.source.is_none());
}

#[tokio::test]
async fn test_execute_sign_uses_expected_endpoints_and_headers() {
    let signing = start_signing_test_server(200, r#"{"signature":"sig-session-ingress"}"#).await;
    let server = CodeSignMCPServer::new(
        Arc::new(BaseServer::new("codesign", "0.0.1")),
        Some(new_codesign_auth_context("session-123", "ingress-token")),
        Path::new("/workspace").to_path_buf(),
        signing.uri(),
        Some(Arc::new(StartupContext::default())),
    );
    let response = server
        .execute_sign(
            &CancellationToken::new(),
            &SignRequest {
                content: "content".to_owned(),
                source: Some(json!({"type": "git_repository"})),
                git_object_format: "sha256".to_owned(),
            },
        )
        .await
        .expect("session-ingress 签名成功");
    assert_eq!(response.signature, "sig-session-ingress");
    let requests = signing.received_requests().await.expect("捕获请求");
    let observed = observed_sign_request(&requests[0]);
    assert_eq!(
        observed.path,
        "/v1/session_ingress/sources/sign-commit/session-123"
    );
    assert_eq!(observed.authorization, "Bearer ingress-token");
    assert_eq!(observed.anthropic_version, "2023-06-01");
    assert!(!observed.runner_version.is_empty());
    let _optional_claude_version = &observed.claude_code_version;
    assert_eq!(observed.body.contents, "content");
    assert_eq!(observed.body.git_object_format, "sha256");

    let code_sessions =
        start_signing_test_server(200, r#"{"signature":"sig-code-sessions"}"#).await;
    let code_sessions_server = CodeSignMCPServer::new(
        Arc::new(BaseServer::new("codesign", "0.0.1")),
        Some(new_codesign_auth_context("session-123", "ingress-token")),
        Path::new("/workspace").to_path_buf(),
        code_sessions.uri(),
        Some(Arc::new(StartupContext {
            use_code_sessions: true,
            ..StartupContext::default()
        })),
    );
    let response = code_sessions_server
        .execute_sign(
            &CancellationToken::new(),
            &SignRequest {
                content: "content".to_owned(),
                source: None,
                git_object_format: String::new(),
            },
        )
        .await
        .expect("code sessions 签名成功");
    assert_eq!(response.signature, "sig-code-sessions");
    let requests = code_sessions
        .received_requests()
        .await
        .expect("捕获 code sessions 请求");
    assert_eq!(
        requests[0].url.path(),
        "/v1/code/sessions/session-123/sign-commit"
    );
}

#[tokio::test]
async fn test_execute_sign_reports_auth_and_remote_errors() {
    let missing_session = CodeSignMCPServer::new(
        Arc::new(BaseServer::new("codesign", "0.0.1")),
        Some(new_codesign_auth_context("", "ingress-token")),
        Path::new("/workspace").to_path_buf(),
        "http://127.0.0.1",
        None,
    );
    let request = SignRequest {
        content: "content".to_owned(),
        source: None,
        git_object_format: String::new(),
    };
    let error = missing_session
        .execute_sign(&CancellationToken::new(), &request)
        .await
        .expect_err("缺失 session ID");
    assert!(error.to_string().contains("session ID is required"));

    let missing_token = CodeSignMCPServer::new(
        Arc::new(BaseServer::new("codesign", "0.0.1")),
        Some(new_codesign_auth_context("session-123", "")),
        Path::new("/workspace").to_path_buf(),
        "http://127.0.0.1",
        None,
    );
    let error = missing_token
        .execute_sign(&CancellationToken::new(), &request)
        .await
        .expect_err("缺失 ingress token");
    assert!(
        error
            .to_string()
            .contains("session ingress token is required")
    );

    let unavailable = start_signing_test_server(502, "unavailable").await;
    let remote_error = CodeSignMCPServer::new(
        Arc::new(BaseServer::new("codesign", "0.0.1")),
        Some(new_codesign_auth_context("session-123", "ingress-token")),
        Path::new("/workspace").to_path_buf(),
        unavailable.uri(),
        None,
    );
    let error = remote_error
        .execute_sign(&CancellationToken::new(), &request)
        .await
        .expect_err("远端 502");
    assert!(
        error
            .to_string()
            .contains("signing server returned status 502")
    );

    let empty = start_signing_test_server(200, r#"{"signature":""}"#).await;
    let empty_signature = CodeSignMCPServer::new(
        Arc::new(BaseServer::new("codesign", "0.0.1")),
        Some(new_codesign_auth_context("session-123", "ingress-token")),
        Path::new("/workspace").to_path_buf(),
        empty.uri(),
        None,
    );
    let error = empty_signature
        .execute_sign(&CancellationToken::new(), &request)
        .await
        .expect_err("空 signature");
    assert!(error.to_string().contains("no signature returned"));
}

#[tokio::test]
async fn test_sign_content_and_handle_sign_file() {
    let workspace = tempdir().expect("workspace");
    let repository = workspace.path().join("demo");
    std::fs::create_dir_all(&repository).expect("创建 repository");
    let file_path = repository.join("commit.txt");
    let output_path = repository.join("commit.txt.sig");
    std::fs::write(&file_path, b"commit content").expect("写入待签名内容");
    let signing = start_signing_test_server(200, r#"{"signature":"signed-content"}"#).await;
    let server = CodeSignMCPServer::new(
        Arc::new(BaseServer::new("codesign", "0.0.1")),
        Some(new_codesign_auth_context("session-123", "ingress-token")),
        workspace.path().to_path_buf(),
        signing.uri(),
        Some(Arc::new(StartupContext {
            sources: vec![git_source_with_secrets()],
            ..StartupContext::default()
        })),
    );
    let request = CallToolRequest {
        params: CallToolParams {
            arguments: json!({
                "file_path": file_path,
                "output_path": output_path,
                "repo_directory": repository,
                "git_object_format": "sha256"
            }),
            ..CallToolParams::default()
        },
        ..CallToolRequest::default()
    };
    let result = server
        .handle_sign_file(&CancellationToken::new(), &request)
        .await
        .expect("handle sign_file");
    assert!(!result.is_error);
    assert!(mcp_result_contains(&result, "Successfully signed file"));
    assert_eq!(
        std::fs::read_to_string(&output_path).expect("读取 signature"),
        "signed-content"
    );
    assert_eq!(
        std::fs::metadata(&output_path)
            .expect("signature metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let requests = signing.received_requests().await.expect("捕获签名请求");
    let observed = observed_sign_request(&requests[0]);
    assert_eq!(observed.body.contents, "commit content");
    assert_eq!(observed.body.git_object_format, "sha256");
    let source = observed.body.source.expect("携带 source").to_string();
    for secret in ["secret-token", "token@example.test", "url", "auth"] {
        assert!(!source.contains(secret), "source 泄漏 {secret}: {source}");
    }
    assert!(source.contains("anthropic/demo"));
    assert!(source.contains("main"));
}

#[tokio::test]
async fn test_handle_sign_file_validation_errors() {
    let server = CodeSignMCPServer::new(
        Arc::new(BaseServer::new("codesign", "0.0.1")),
        None,
        Path::new("/workspace").to_path_buf(),
        "http://127.0.0.1",
        None,
    );
    let error = server
        .handle_sign_file(&CancellationToken::new(), &CallToolRequest::default())
        .await
        .expect_err("缺失 file_path 是协议错误");
    assert!(error.to_string().contains("file_path is required"));

    let request = CallToolRequest {
        params: CallToolParams {
            arguments: json!({"file_path": "relative.txt"}),
            ..CallToolParams::default()
        },
        ..CallToolRequest::default()
    };
    let result = server
        .handle_sign_file(&CancellationToken::new(), &request)
        .await
        .expect("相对路径返回工具错误");
    assert!(result.is_error);
    assert!(mcp_result_contains(&result, "File path must be absolute"));
}

#[tokio::test]
async fn test_sanitize_source_and_read_file_content() {
    let source = git_source_with_secrets();
    let sanitized: Value = sanitize_source(&source);
    let text = sanitized.to_string();
    for secret in ["secret-token", "token@example.test", "url", "auth"] {
        assert!(!text.contains(secret), "sanitize_source 泄漏 {secret}");
    }
    assert!(text.contains("git_repository"));
    assert!(text.contains("anthropic/demo"));
    assert!(text.contains("main"));

    let directory = tempdir().expect("tempdir");
    let path = directory.path().join("content.txt");
    std::fs::write(&path, b"hello").expect("写入文件");
    assert_eq!(read_file_content(&path).await.expect("读取文件"), "hello");
    let error = read_file_content(&directory.path().join("missing.txt"))
        .await
        .expect_err("缺失文件必须失败");
    assert!(error.to_string().contains("cannot open file"));
}

#[tokio::test]
async fn test_sign_content_wraps_failures() {
    let server = CodeSignMCPServer::new(
        Arc::new(BaseServer::new("codesign", "0.0.1")),
        None,
        Path::new("/workspace").to_path_buf(),
        "http://127.0.0.1",
        None,
    );
    let source = SourceInfo {
        name: "unknown".to_owned(),
        source: None,
    };
    let error = server
        .sign_content(
            &CancellationToken::new(),
            String::new(),
            &source,
            "sha1".to_owned(),
        )
        .await
        .expect_err("空内容必须失败");
    assert_eq!(error.to_string(), ERR_EMPTY_CONTENT);

    let error = server
        .sign_content(
            &CancellationToken::new(),
            "content".to_owned(),
            &source,
            "sha1".to_owned(),
        )
        .await
        .expect_err("远端签名失败必须包装 sentinel");
    assert!(error.to_string().starts_with(ERR_SIGNING_FAILED));
}
