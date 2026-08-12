//! 对应 Go `internal/mcp/registry_test.go` 的注册表、工厂与 Claude CLI 测试。

use std::{
    os::unix::fs::PermissionsExt as _,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, MutexGuard, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use crate::internal::{
    config::StartupContext,
    mcp::{
        MCPServer, MCPServerConfigurator, McpError, McpResult, ServerConfig, Tool, ToolConfig,
        ToolHandler,
    },
    session::NoopActivityRecorder,
};

use super::{
    get_mcp_registrations, new_registration, register_server, register_with_claude,
    registry::clear_registry_for_test,
};

static REGISTRY_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// 对应 Go `fakeRegistryServer`；记录工厂传入的工具和注册谓词调用。
struct FakeRegistryServer {
    name: String,
    tools: Vec<ToolConfig>,
    started_with: Mutex<Vec<String>>,
    start_error: Option<String>,
    register_with_claude: bool,
    should_register_called: AtomicBool,
}

#[async_trait::async_trait]
impl MCPServer for FakeRegistryServer {
    /// 对应 Go `fakeRegistryServer.GetName`。
    fn get_name(&self) -> String {
        self.name.clone()
    }

    /// 对应 Go `fakeRegistryServer.GetTools`。
    fn get_tools(self: Arc<Self>) -> Vec<ToolConfig> {
        self.tools.clone()
    }

    /// 对应 Go `fakeRegistryServer.Start`，并保存实际启动工具名供断言。
    async fn start(&self, tools: Vec<ToolConfig>) -> McpResult<ServerConfig> {
        *self
            .started_with
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            tools.iter().map(|tool| tool.tool.name.clone()).collect();
        if let Some(error) = &self.start_error {
            return Err(McpError::message(error));
        }
        Ok(ServerConfig {
            port: 4321,
            bearer_token: "token".to_owned(),
        })
    }

    /// 对应 Go `fakeRegistryServer.Stop`；测试 server 没有后台资源。
    async fn stop(&self) {}

    /// 对应 Go `fakeRegistryServer.ShouldRegisterWithClaude`。
    fn should_register_with_claude(&self) -> bool {
        self.should_register_called.store(true, Ordering::SeqCst);
        self.register_with_claude
    }
}

/// 在一个测试生命周期内串行化并清空进程级 MCP 注册表。
struct RegistryReset {
    _guard: MutexGuard<'static, ()>,
}

impl Drop for RegistryReset {
    /// 测试结束后再次清空，避免后续用例观察到本用例 registration。
    fn drop(&mut self) {
        clear_registry_for_test();
    }
}

/// 对应 Go `resetRegistryForTest`。
fn reset_registry_for_test() -> RegistryReset {
    let guard = REGISTRY_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    clear_registry_for_test();
    RegistryReset { _guard: guard }
}

/// 临时 Claude 可执行文件及其环境变量恢复守卫。
struct FakeClaudeInstallation {
    _directory: TempDir,
    log_path: PathBuf,
    previous_path: Option<std::ffi::OsString>,
    previous_log: Option<std::ffi::OsString>,
}

impl Drop for FakeClaudeInstallation {
    /// 恢复进程环境，确保 fake Claude 不泄漏到其余测试。
    fn drop(&mut self) {
        // SAFETY: 两个会修改 PATH/CLAUDE_TEST_LOG 的测试持有 crate 级
        // PROCESS_STATE_TEST_LOCK；Drop 在锁守卫释放前执行，因此不会与其他
        // 模块的 PATH 写入或外部命令启动并发。
        unsafe {
            match &self.previous_path {
                Some(value) => std::env::set_var("PATH", value),
                None => std::env::remove_var("PATH"),
            }
            match &self.previous_log {
                Some(value) => std::env::set_var("CLAUDE_TEST_LOG", value),
                None => std::env::remove_var("CLAUDE_TEST_LOG"),
            }
        }
    }
}

/// 对应 Go `installFakeClaude`，创建会记录 argv 的 Linux shell 脚本。
fn install_fake_claude(script: &str) -> FakeClaudeInstallation {
    let directory = tempfile::tempdir().expect("创建 fake Claude 临时目录");
    let log_path = directory.path().join("commands.log");
    let claude_path = directory.path().join("claude");
    std::fs::write(&claude_path, script).expect("写入 fake Claude");
    std::fs::set_permissions(&claude_path, std::fs::Permissions::from_mode(0o700))
        .expect("设置 fake Claude 可执行权限");

    let previous_path = std::env::var_os("PATH");
    let previous_log = std::env::var_os("CLAUDE_TEST_LOG");
    let path = previous_path.as_ref().map_or_else(
        || directory.path().as_os_str().to_owned(),
        |current| {
            let mut paths = vec![directory.path().to_path_buf()];
            paths.extend(std::env::split_paths(current));
            std::env::join_paths(paths).expect("拼接 PATH")
        },
    );
    // SAFETY: 调用方在整个安装守卫生命周期内持有 PROCESS_STATE_TEST_LOCK，且值
    // 都是当前测试拥有的路径；这里不会与本文件其他环境修改并发。
    unsafe {
        std::env::set_var("PATH", path);
        std::env::set_var("CLAUDE_TEST_LOG", &log_path);
    }

    FakeClaudeInstallation {
        _directory: directory,
        log_path,
        previous_path,
        previous_log,
    }
}

/// 创建只有名称的测试工具。
fn test_tool(name: &str) -> ToolConfig {
    ToolConfig {
        tool: Tool::new(name, "", serde_json::json!({"type": "object"})),
        handler: ToolHandler::new(|_, _| async { Ok(Default::default()) }),
    }
}

#[test]
fn test_get_mcp_registrations_filters_by_should_register() {
    // REGISTRY 是进程级 OnceLock；独立进程保证 reset 不会与其他模块的读取交叉。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_get_mcp_registrations_filters_by_should_register"
    )) {
        return;
    }
    let _reset = reset_registry_for_test();
    let included = new_registration(
        "included",
        "included",
        None,
        Arc::new(|_, _| true),
        Arc::new(String::new),
    );
    let _excluded = new_registration(
        "excluded",
        "excluded",
        None,
        Arc::new(|_, _| false),
        Arc::new(String::new),
    );
    register_server(None);

    let registrations = get_mcp_registrations(Some(&StartupContext::default()), None);
    assert_eq!(registrations.len(), 1);
    assert!(Arc::ptr_eq(&registrations[0], &included));
}

#[tokio::test]
async fn test_new_registration_factory_starts_server_and_caches_allowed_tools() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_new_registration_factory_starts_server_and_caches_allowed_tools"
    )) {
        return;
    }
    let _reset = reset_registry_for_test();
    let server = Arc::new(FakeRegistryServer {
        name: "demo".to_owned(),
        tools: vec![test_tool("alpha"), test_tool("beta")],
        started_with: Mutex::new(Vec::new()),
        start_error: None,
        register_with_claude: false,
        should_register_called: AtomicBool::new(false),
    });
    let seen_base = Arc::new(Mutex::new(None));
    let seen_base_from_factory = Arc::clone(&seen_base);
    let server_from_factory = Arc::clone(&server);
    let configurator: MCPServerConfigurator = Arc::new(move |base_server, _, _, working_dir, _| {
        let seen_base = Arc::clone(&seen_base_from_factory);
        let server = Arc::clone(&server_from_factory);
        Box::pin(async move {
            assert_eq!(working_dir, Path::new("/workspace"));
            *seen_base
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                Some((base_server.name.clone(), base_server.version.clone()));
            Ok(server as Arc<dyn MCPServer>)
        })
    });
    let registration = new_registration(
        "demo",
        "Demo server",
        Some(configurator),
        Arc::new(|_, _| true),
        Arc::new(|| "extra prompt".to_owned()),
    );

    assert_eq!(registration.name, "demo");
    assert_eq!(registration.description, "Demo server");
    assert!((registration.get_tools)().is_empty());
    let configured = (registration.factory)(
        CancellationToken::new(),
        None,
        PathBuf::from("/workspace"),
        Some(Arc::new(StartupContext::default())),
        Arc::new(NoopActivityRecorder),
    )
    .await
    .expect("factory 成功");

    assert_eq!(configured.get_name(), "demo");
    assert_eq!(
        *seen_base
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        Some(("demo".to_owned(), "0.0.1".to_owned()))
    );
    assert!(server.should_register_called.load(Ordering::SeqCst));
    assert_eq!(
        *server
            .started_with
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        ["alpha", "beta"]
    );
    assert_eq!(
        (registration.get_tools)(),
        ["mcp__demo__alpha", "mcp__demo__beta"]
    );
}

#[tokio::test]
async fn test_new_registration_factory_reports_configuration_errors() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_new_registration_factory_reports_configuration_errors"
    )) {
        return;
    }
    let _reset = reset_registry_for_test();
    let nil_configurator = new_registration(
        "nil-config",
        "Nil config",
        None,
        Arc::new(|_, _| true),
        Arc::new(String::new),
    );
    let error = match (nil_configurator.factory)(
        CancellationToken::new(),
        None,
        PathBuf::new(),
        Some(Arc::new(StartupContext::default())),
        Arc::new(NoopActivityRecorder),
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("缺失 configurator 必须失败"),
    };
    assert!(error.to_string().contains("configurator is nil"));

    let failed_server = Arc::new(FakeRegistryServer {
        name: "start-failure".to_owned(),
        tools: Vec::new(),
        started_with: Mutex::new(Vec::new()),
        start_error: Some("start failed".to_owned()),
        register_with_claude: false,
        should_register_called: AtomicBool::new(false),
    });
    let configurator: MCPServerConfigurator = Arc::new(move |_, _, _, _, _| {
        let server = Arc::clone(&failed_server);
        Box::pin(async move { Ok(server as Arc<dyn MCPServer>) })
    });
    let registration = new_registration(
        "start-failure",
        "Start failure",
        Some(configurator),
        Arc::new(|_, _| true),
        Arc::new(String::new),
    );
    let error = match (registration.factory)(
        CancellationToken::new(),
        None,
        PathBuf::new(),
        Some(Arc::new(StartupContext::default())),
        Arc::new(NoopActivityRecorder),
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("start failure 必须保留上下文"),
    };
    assert_eq!(
        error.to_string(),
        "failed to start start-failure server: start failed"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_register_with_claude_lists_removes_and_adds() {
    // 父测试进程只启动隔离子进程，不在自身读写全局环境。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_register_with_claude_lists_removes_and_adds"
    )) {
        return;
    }
    let _environment = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let installation = install_fake_claude(
        r#"#!/bin/sh
echo "$@" >> "$CLAUDE_TEST_LOG"
if [ "$1" = "mcp" ] && [ "$2" = "list" ]; then
  echo "demo: http://127.0.0.1:1111/mcp"
fi
exit 0
"#,
    );

    register_with_claude(
        &CancellationToken::new(),
        "demo",
        Some(&ServerConfig {
            port: 1234,
            bearer_token: "secret".to_owned(),
        }),
    )
    .await
    .expect("Claude 注册成功");

    let log = std::fs::read_to_string(&installation.log_path).expect("读取命令日志");
    let lines = log.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "mcp list");
    assert_eq!(lines[1], "mcp remove demo");
    assert!(lines[2].contains("mcp add --transport http demo http://127.0.0.1:1234/mcp"));
    assert!(lines[2].contains("Authorization: Bearer secret"));
    assert!(lines[2].contains("--scope user"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_register_with_claude_reports_nil_config_and_add_failure() {
    // 父测试进程只启动隔离子进程，不在自身读写全局环境。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_register_with_claude_reports_nil_config_and_add_failure"
    )) {
        return;
    }
    let error = register_with_claude(&CancellationToken::new(), "demo", None)
        .await
        .expect_err("nil config 必须失败");
    assert_eq!(
        error.to_string(),
        "claude mcp add failed: server config is nil"
    );

    let _environment = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let _installation = install_fake_claude(
        r#"#!/bin/sh
echo "$@" >> "$CLAUDE_TEST_LOG"
if [ "$1" = "mcp" ] && [ "$2" = "add" ]; then
  exit 7
fi
exit 0
"#,
    );
    let error = register_with_claude(
        &CancellationToken::new(),
        "demo",
        Some(&ServerConfig {
            port: 1234,
            bearer_token: "secret".to_owned(),
        }),
    )
    .await
    .expect_err("add 非零退出必须失败");
    assert!(error.to_string().contains("claude mcp add failed"));
    assert!(error.to_string().contains("exit status 7"));
}
