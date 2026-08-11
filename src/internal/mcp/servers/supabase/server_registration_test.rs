//! 对应 Go `internal/mcp/servers/supabase/server_registration_test.go` 的完整行为测试。

use std::{
    collections::HashMap,
    os::unix::fs::PermissionsExt as _,
    process::Command,
    sync::{Arc, OnceLock},
};

use nix::sys::stat::{Mode, umask};
use serde_json::json;
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

use crate::internal::{
    auth::{AuthConfig, AuthContext, new_auth_context_with_session_id},
    config::StartupContext,
    logger::Logger,
    mcp::{BaseServer, CallToolParams, CallToolRequest, CallToolResult},
    o11y::{DiscardO11yService, O11yService},
    session::NoopActivityRecorder,
};

use super::server::create_dir_all_0755;
use super::{
    Client, ERR_PROVISIONING_FAILED, SupabaseMCPServer, configure_server,
    permanent_provisioning_err_body, should_register,
};

static SUPABASE_DISABLED_ENVIRONMENT_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
const ZERO_UMASK_CHILD_DIRECTORY: &str = "ENV_MANAGER_SUPABASE_ZERO_UMASK_DIR";

/// 保存并恢复 `SUPABASE_MCP_DISABLED` 的测试环境。
struct SupabaseDisabledEnvironment {
    previous: Option<std::ffi::OsString>,
}

impl SupabaseDisabledEnvironment {
    /// 捕获原值，供同一测试内切换 enabled/disabled 分支。
    fn capture() -> Self {
        Self {
            previous: std::env::var_os("SUPABASE_MCP_DISABLED"),
        }
    }

    /// 设置当前测试分支的禁用值。
    fn set(value: &str) {
        // SAFETY: 调用方持有 SUPABASE_DISABLED_ENVIRONMENT_LOCK，且 Drop 在锁内恢复。
        unsafe { std::env::set_var("SUPABASE_MCP_DISABLED", value) };
    }
}

impl Drop for SupabaseDisabledEnvironment {
    /// 恢复测试开始前的环境变量。
    fn drop(&mut self) {
        // SAFETY: 修改与恢复由同一个进程级互斥锁串行化。
        unsafe {
            match &self.previous {
                Some(value) => std::env::set_var("SUPABASE_MCP_DISABLED", value),
                None => std::env::remove_var("SUPABASE_MCP_DISABLED"),
            }
        }
    }
}

/// 对应 Go `newSupabaseAuthContext`，生成真实嵌套 Supabase auth token。
fn new_supabase_auth_context(
    session_id: &str,
    pat: &str,
    project_ref: &str,
    session_ingress_token: &str,
) -> Arc<AuthContext> {
    let mut configurations = Vec::new();
    if !pat.is_empty() || !project_ref.is_empty() {
        configurations.push(AuthConfig {
            auth_type: "supabase".to_owned(),
            token: serde_json::to_string(&json!({
                "project_ref": project_ref,
                "anon_key": "anon-key",
                "db_pass": "db-pass",
                "pat": pat,
            }))
            .expect("序列化 Supabase token"),
        });
    }
    if !session_ingress_token.is_empty() {
        configurations.push(AuthConfig {
            auth_type: "session_ingress".to_owned(),
            token: session_ingress_token.to_owned(),
        });
    }
    let auth_json = serde_json::to_vec(&configurations).expect("序列化 auth 配置");
    Arc::new(
        new_auth_context_with_session_id(&auth_json, session_id)
            .expect("创建 Supabase auth context"),
    )
}

/// 对应 Go `supabaseTestLogger`；测试不观察日志，但显式传递 discard logger。
fn supabase_test_logger() -> Logger {
    Logger::new(DiscardO11yService.log_handler())
}

/// 对应 Go `mcpResultContains`，搜索实际 MCP 文本内容。
fn mcp_result_contains(result: &CallToolResult, want: &str) -> bool {
    result
        .content
        .iter()
        .any(|content| content.text.contains(want))
}

/// 对应 Go `toolNames`，提取当前工具顺序供兼容断言。
fn tool_names(tools: &[crate::internal::mcp::ToolConfig]) -> Vec<&str> {
    tools.iter().map(|tool| tool.tool.name.as_str()).collect()
}

/// 创建带 JSON arguments 的 MCP 调用请求。
fn call_request(arguments: serde_json::Value) -> CallToolRequest {
    CallToolRequest {
        params: CallToolParams {
            arguments,
            ..CallToolParams::default()
        },
        ..CallToolRequest::default()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn test_configure_server_and_should_register() {
    // 父测试进程只启动隔离子进程，不在自身读写全局环境。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_configure_server_and_should_register"
    )) {
        return;
    }
    // SUPABASE_MCP_DISABLED 是进程全局状态；crate 级锁覆盖捕获、修改、读取和恢复，
    // 模块私锁继续保护本 fixture，但不能替代跨模块串行化。
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let _lock = SUPABASE_DISABLED_ENVIRONMENT_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let _environment = SupabaseDisabledEnvironment::capture();
    let working_directory = tempdir().expect("working dir");
    let auth =
        new_supabase_auth_context("session-123", "pat-token", "project-ref", "ingress-token");
    let server = configure_server(
        Arc::new(BaseServer::new(supabase_test_logger(), "supabase", "0.0.1")),
        Some(auth),
        Some(Arc::new(StartupContext {
            api_base_url: "api.example.test".to_owned(),
            environment_variables: HashMap::from([(
                "_BAKU_SUPABASE_LAZY".to_owned(),
                "1".to_owned(),
            )]),
            ..StartupContext::default()
        })),
        working_directory.path().to_path_buf(),
        Arc::new(NoopActivityRecorder),
    )
    .await
    .expect("配置 eager client");
    assert_eq!(server.pat, "pat-token");
    assert_eq!(server.session_id, "session-123");
    assert_eq!(server.session_ingress_token, "ingress-token");
    assert!(server.lazy_mode());
    assert_eq!(server.base_working_dir, working_directory.path());
    assert_eq!(server.api_base_url, "api.example.test");
    let state = server.state.read().await;
    let client = state.client.as_ref().expect("已有 project ref 创建 client");
    assert_eq!(client.project_ref(), "project-ref");
    assert_eq!(client.base_url, "https://api.supabase.com/v1");
    drop(state);

    let without_project = configure_server(
        Arc::new(BaseServer::new(supabase_test_logger(), "supabase", "0.0.1")),
        Some(new_supabase_auth_context(
            "session-123",
            "pat-token",
            "",
            "ingress-token",
        )),
        Some(Arc::new(StartupContext::default())),
        working_directory.path().to_path_buf(),
        Arc::new(NoopActivityRecorder),
    )
    .await
    .expect("无 project ref 进入 pending 模式");
    assert!(without_project.state.read().await.client.is_none());

    let error = configure_server(
        Arc::new(BaseServer::new(supabase_test_logger(), "supabase", "0.0.1")),
        Some(new_supabase_auth_context(
            "session-123",
            "",
            "",
            "ingress-token",
        )),
        Some(Arc::new(StartupContext::default())),
        working_directory.path().to_path_buf(),
        Arc::new(NoopActivityRecorder),
    )
    .await
    .expect_err("缺失 PAT");
    assert!(error.to_string().contains("supabase PAT not configured"));

    let baku = StartupContext {
        environment_sub_type: "baku".to_owned(),
        ..StartupContext::default()
    };
    SupabaseDisabledEnvironment::set("");
    assert!(should_register(Some(&baku), None));
    assert!(!should_register(Some(&StartupContext::default()), None));
    SupabaseDisabledEnvironment::set("true");
    assert!(!should_register(Some(&baku), None));
}

#[tokio::test]
async fn test_ensure_configured_fetches_credentials_and_writes_env_files() {
    let working_directory = tempdir().expect("working dir");
    let project = working_directory.path().join("project");
    std::fs::create_dir(&project).expect("project dir");
    let control = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/ccr-sessions/session-123/supabase-credentials"))
        .and(header("authorization", "Bearer ingress-token"))
        .and(header("anthropic-version", "2023-06-01"))
        .and(header("anthropic-beta", "ccr-byoc-2025-07-29"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"project_ref":"project-ref","anon_key":"anon-key","db_pass":"pa ss!"}"#,
        ))
        .expect(1)
        .mount(&control)
        .await;
    let server = SupabaseMCPServer::new(
        Arc::new(BaseServer::new(supabase_test_logger(), "supabase", "0.0.1")),
        None,
        "pat-token",
        working_directory.path().to_path_buf(),
        "session-123",
        control.uri(),
        "ingress-token",
        false,
    )
    .with_credentials_http_client(reqwest::Client::new());
    let client = server
        .ensure_configured(&CancellationToken::new())
        .await
        .expect("获取 credentials");
    assert_eq!(client.project_ref(), "project-ref");
    assert_eq!(client.pat, "pat-token");
    assert_eq!(server.get_project_dir(), project);
    assert_eq!(
        std::fs::read_to_string(project.join(".env")).expect("读取 .env"),
        "VITE_SUPABASE_URL=https://project-ref.supabase.co\nVITE_SUPABASE_ANON_KEY=anon-key\n"
    );
    assert!(
        std::fs::read_to_string(project.join(".env.local"))
            .expect("读取 .env.local")
            .contains("postgresql://postgres:pa+ss%21@db.project-ref.supabase.co:5432/postgres")
    );
}

#[tokio::test]
async fn test_ensure_configured_pending_and_permanent_failures() {
    let pending = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&pending)
        .await;
    let non_lazy = SupabaseMCPServer::new(
        Arc::new(BaseServer::new(supabase_test_logger(), "supabase", "0.0.1")),
        None,
        "pat-token",
        tempdir().unwrap().path().to_path_buf(),
        "session-123",
        pending.uri(),
        "ingress-token",
        false,
    )
    .with_credentials_http_client(reqwest::Client::new());
    let error = non_lazy
        .ensure_configured(&CancellationToken::new())
        .await
        .expect_err("非 lazy pending");
    assert!(error.to_string().contains("still being provisioned"));

    let lazy_pending = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&lazy_pending)
        .await;
    let lazy = SupabaseMCPServer::new(
        Arc::new(BaseServer::new(supabase_test_logger(), "supabase", "0.0.1")),
        None,
        "pat-token",
        tempdir().unwrap().path().to_path_buf(),
        "session-123",
        lazy_pending.uri(),
        "ingress-token",
        true,
    )
    .with_credentials_http_client(reqwest::Client::new());
    let error = lazy
        .ensure_configured(&CancellationToken::new())
        .await
        .expect_err("lazy pending");
    assert!(error.to_string().contains("provision_database"));

    let permanent = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500).set_body_string("quota exhausted"))
        .expect(1)
        .mount(&permanent)
        .await;
    let failed = SupabaseMCPServer::new(
        Arc::new(BaseServer::new(supabase_test_logger(), "supabase", "0.0.1")),
        None,
        "pat-token",
        tempdir().unwrap().path().to_path_buf(),
        "session-123",
        permanent.uri(),
        "ingress-token",
        false,
    )
    .with_credentials_http_client(reqwest::Client::new());
    let error = failed
        .ensure_configured(&CancellationToken::new())
        .await
        .expect_err("永久失败");
    assert!(error.to_string().starts_with(ERR_PROVISIONING_FAILED));
    assert!(error.to_string().contains("quota exhausted"));
    assert_eq!(
        failed.state.read().await.provisioning_error,
        "quota exhausted"
    );
    let error = failed
        .ensure_configured(&CancellationToken::new())
        .await
        .expect_err("永久失败必须从缓存返回");
    assert!(error.to_string().starts_with(ERR_PROVISIONING_FAILED));
    assert_eq!(
        permanent
            .received_requests()
            .await
            .expect("捕获永久失败请求")
            .len(),
        1
    );
}

#[tokio::test]
async fn test_supabase_tool_handlers_use_configured_client() {
    let api = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/projects/project-ref/database/query"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"[{"id":1}]"#))
        .expect(2)
        .mount(&api)
        .await;
    Mock::given(method("GET"))
        .and(path("/projects/project-ref/database/migrations"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"[{"version":"20240531010101","name":"create_todos"}]"#),
        )
        .expect(1)
        .mount(&api)
        .await;
    Mock::given(method("GET"))
        .and(path("/projects/project-ref/types/typescript"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"types":"export type Database = { public: {} }"}"#),
        )
        .expect(2)
        .mount(&api)
        .await;
    Mock::given(method("POST"))
        .and(path("/projects/project-ref/database/migrations"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(1)
        .mount(&api)
        .await;

    let working_directory = tempdir().expect("working dir");
    let http = reqwest::Client::new();
    let client = Arc::new(
        Client::new("pat-token", "project-ref", api.uri())
            .expect("client")
            .with_http_clients(http.clone(), http),
    );
    let eager = Arc::new(SupabaseMCPServer::new(
        Arc::new(BaseServer::new(supabase_test_logger(), "supabase", "0.0.1")),
        Some(Arc::clone(&client)),
        "pat-token",
        working_directory.path().to_path_buf(),
        "session-123",
        "api.example.test",
        "ingress-token",
        false,
    ));
    let tools = eager.get_tools();
    assert_eq!(tools.len(), 5, "tools={:?}", tool_names(&tools));
    assert!(eager.should_register_with_claude());
    let lazy = Arc::new(SupabaseMCPServer::new(
        Arc::new(BaseServer::new(supabase_test_logger(), "supabase", "0.0.1")),
        Some(Arc::clone(&client)),
        "pat-token",
        working_directory.path().to_path_buf(),
        "session-123",
        "api.example.test",
        "ingress-token",
        true,
    ));
    let lazy_tools = lazy.get_tools();
    assert_eq!(lazy_tools.len(), 6);
    assert_eq!(lazy_tools[0].tool.name, "provision_database");

    let cancellation = CancellationToken::new();
    let query = eager
        .handle_query(&cancellation, &call_request(json!({"sql": "select 1"})))
        .await
        .expect("query handler");
    assert!(!query.is_error);
    assert!(mcp_result_contains(&query, r#""id": 1"#));
    let migrations = eager
        .handle_list_migrations(&cancellation, &CallToolRequest::default())
        .await
        .expect("list migrations");
    assert!(!migrations.is_error);
    assert!(mcp_result_contains(&migrations, "create_todos"));
    let types = eager
        .handle_generate_types(&cancellation, &CallToolRequest::default())
        .await
        .expect("generate types");
    assert!(!types.is_error);
    assert!(mcp_result_contains(&types, "TypeScript types regenerated"));
    assert!(
        std::fs::read_to_string(working_directory.path().join("src/lib/database.types.ts"))
            .expect("读取 types 文件")
            .contains("export type Database")
    );
    let migration = eager
        .handle_migrate(
            &cancellation,
            &call_request(json!({
                "sql": "create table todos(id int);",
                "name": "create_todos"
            })),
        )
        .await
        .expect("migrate handler");
    assert!(!migration.is_error);
    assert!(mcp_result_contains(
        &migration,
        "Migration 'create_todos' applied successfully"
    ));
    let migration_files = std::fs::read_dir(working_directory.path().join("supabase/migrations"))
        .expect("读取 migrations 目录")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .ends_with("_create_todos.sql")
        })
        .count();
    assert_eq!(migration_files, 1);
    assert!(!api.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn test_handle_deploy_function_and_provision_database() {
    let working_directory = tempdir().expect("working dir");
    let function = working_directory.path().join("supabase/functions/hello");
    std::fs::create_dir_all(&function).expect("function dir");
    std::fs::write(
        function.join("index.ts"),
        b"Deno.serve(() => new Response('ok'))",
    )
    .expect("index.ts");
    let deploy = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(201).set_body_string(
            r#"{"id":"fn-id","slug":"hello","name":"hello","status":"ACTIVE","version":3}"#,
        ))
        .expect(1)
        .mount(&deploy)
        .await;
    let client = Arc::new(
        Client::new("pat-token", "project-ref", deploy.uri())
            .expect("client")
            .with_http_clients(reqwest::Client::new(), reqwest::Client::new()),
    );
    let server = SupabaseMCPServer::new(
        Arc::new(BaseServer::new(supabase_test_logger(), "supabase", "0.0.1")),
        Some(client),
        "pat-token",
        working_directory.path().to_path_buf(),
        "session-123",
        "api.example.test",
        "ingress-token",
        false,
    );
    let result = server
        .handle_deploy_function(
            &CancellationToken::new(),
            &call_request(json!({"slug": "hello"})),
        )
        .await
        .expect("deploy handler");
    assert!(!result.is_error);
    assert!(mcp_result_contains(
        &result,
        "https://project-ref.supabase.co/functions/v1/hello"
    ));
    let already = server
        .handle_provision_database(&CancellationToken::new(), &CallToolRequest::default())
        .await
        .expect("already provisioned");
    assert!(!already.is_error);
    assert!(mcp_result_contains(
        &already,
        "Database already provisioned"
    ));

    {
        let mut state = server.state.write().await;
        state.client = None;
        state.provisioning_error = "quota exhausted".to_owned();
    }
    let failed = server
        .handle_provision_database(&CancellationToken::new(), &CallToolRequest::default())
        .await
        .expect("cached permanent failure");
    assert!(failed.is_error);
    assert!(mcp_result_contains(&failed, "quota exhausted"));
}

#[tokio::test]
async fn test_handle_provision_database_fetches_credentials_when_already_provisioned() {
    let working_directory = tempdir().expect("working dir");
    let project = working_directory.path().join("project");
    std::fs::create_dir(&project).expect("project dir");
    let control = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(
            "/v2/ccr-sessions/session%2F%252e%3B%2C:@&=+$%E4%B8%AD%E6%96%87/supabase-provision",
        ))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&control)
        .await;
    Mock::given(method("GET"))
        .and(path(
            "/v2/ccr-sessions/session%2F%252e%3B%2C:@&=+$%E4%B8%AD%E6%96%87/supabase-credentials",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"project_ref":"project-ref","anon_key":"anon-key","db_pass":"db-pass"}"#,
        ))
        .expect(1)
        .mount(&control)
        .await;
    let server = SupabaseMCPServer::new(
        Arc::new(BaseServer::new(supabase_test_logger(), "supabase", "0.0.1")),
        None,
        "pat-token",
        working_directory.path().to_path_buf(),
        "session/%2e;,:@&=+$中文",
        control.uri(),
        "ingress-token",
        false,
    )
    .with_credentials_http_client(reqwest::Client::new());
    let result = server
        .handle_provision_database(&CancellationToken::new(), &CallToolRequest::default())
        .await
        .expect("already provisioned 后获取 credentials");
    assert!(!result.is_error);
    assert!(mcp_result_contains(&result, "Database already provisioned"));
    assert_eq!(
        server
            .state
            .read()
            .await
            .client
            .as_ref()
            .map(|client| client.project_ref()),
        Some("project-ref")
    );
    assert!(project.join(".env").is_file());
}

#[test]
fn test_permanent_provisioning_err_body() {
    assert_eq!(
        permanent_provisioning_err_body("supabase provisioning failed: quota exhausted"),
        "quota exhausted"
    );
    assert_eq!(
        permanent_provisioning_err_body("supabase provisioning failed: "),
        "(no error details from server)"
    );
}

/// 在专用子进程把 umask 设为 `000`，验证 Supabase 输出目录仍请求 Go 的 `0755`。
///
/// 子进程方案避免在多线程 test harness 中短暂改变全局 umask，从而不会干扰同时
/// 创建 fixture 的其他测试。
#[test]
fn test_supabase_directories_use_0755_under_zero_umask() {
    if let Some(path) = std::env::var_os(ZERO_UMASK_CHILD_DIRECTORY) {
        umask(Mode::empty());
        create_dir_all_0755(&std::path::PathBuf::from(path).join("first/second"))
            .expect("以 Go mode 创建 Supabase 目录树");
        return;
    }

    let root = tempdir().expect("创建父进程临时目录");
    let target = root.path().join("supabase");
    let status = Command::new(std::env::current_exe().expect("定位当前测试二进制"))
        .args([
            "--exact",
            "internal::mcp::servers::supabase::server_registration_test::test_supabase_directories_use_0755_under_zero_umask",
            "--nocapture",
        ])
        .env(ZERO_UMASK_CHILD_DIRECTORY, &target)
        .status()
        .expect("启动零 umask 子测试");
    assert!(status.success(), "零 umask 子测试失败: {status}");
    for path in [target.join("first"), target.join("first/second")] {
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }
}
