//! Manager 主状态机、MCP、tunnel 和 lease 生命周期行为测试。
//!
//! 测试主体随 manager 实现继续补入；该文件先与 `mod.rs` 同步落盘，确保共享工作树
//! 在并行 ARM64 检查期间始终保持模块图完整。

use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::SystemTime,
};

use async_trait::async_trait;
use serde_json::json;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use crate::internal::{
    api::{ApiError, GetEnvironmentConfigResponse},
    auth::AuthContext,
    claude::{ClaudeError, Executor, Outcomes},
    config::{ClaudeConfig, Session, SessionMode, StartupContext, default_claude_config},
    envtype::{
        EnvTypes, EnvironmentType, Factory, LeaseManagerHandle, LeaseManagerProvider, Registration,
        new_env_types,
    },
    mcp::{MCPServer, MCPServerRegistration, McpError, McpResult, ServerConfig, ToolConfig},
    session::{ActivityRecorder, NoopActivityRecorder},
    util::BoxError,
};

use super::{
    CreateTunnelClientFunc, Manager, ManagerApiClient, ManagerError, ManagerLeaseRequest,
    ManagerLeaseRuntime, TunnelClient, is_tunnel_enabled,
    new_manager_with_executor_and_claude_config,
    types::{ClaudeExecutorFactory, EnvironmentLeaseAdapter},
};

/// Fake environment 的跨 factory 可观察状态。
#[derive(Default)]
struct FakeEnvironmentState {
    initialized: bool,
    source_environment: Vec<(OsString, OsString)>,
    proxy_ready_during_initialize: bool,
    session_mode: Option<SessionMode>,
    startup_context: Option<StartupContext>,
    auth_context: Option<AuthContext>,
}

/// 可由 EnvTypes factory 重建、共享状态的 environment。
#[derive(Clone)]
struct FakeEnvironment {
    state: Arc<Mutex<FakeEnvironmentState>>,
    cwd: String,
    environment: BTreeMap<String, String>,
    initialization_error: Option<String>,
    pause_initialization: bool,
    lease_manager: Option<Arc<FakeLeaseManager>>,
}

#[async_trait]
impl EnvironmentType for FakeEnvironment {
    /// 保存 manager 传播的 session mode。
    fn set_session_mode(&mut self, mode: SessionMode) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .session_mode = Some(mode);
    }

    /// 保存 manager 传播的 startup context。
    fn set_startup_context(&mut self, context: Option<StartupContext>) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .startup_context = context;
    }

    /// 保存 manager 传播的 auth context。
    fn set_auth_context(&mut self, context: Option<AuthContext>) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .auth_context = context;
    }

    /// 标记初始化并按测试配置返回成功或错误。
    async fn initialize(
        &mut self,
        _cancellation: &CancellationToken,
        _activity_recorder: Option<&(dyn ActivityRecorder + Send + Sync)>,
    ) -> Result<(), BoxError> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .initialized = true;
        let network = crate::internal::sources::GIT_ENVIRONMENT
            .try_with(Clone::clone)
            .unwrap_or_default();
        self.state.lock().unwrap().source_environment = network.clone();
        if !network.is_empty() {
            let ca = network
                .iter()
                .find(|(key, _)| key == "GIT_SSL_CAINFO")
                .unwrap();
            assert!(
                std::path::Path::new(&ca.1).is_file(),
                "CA must exist before sources"
            );
            let proxy = network
                .iter()
                .find(|(key, _)| key == "HTTPS_PROXY")
                .unwrap();
            let url = url::Url::parse(proxy.1.to_str().unwrap()).unwrap();
            let stream =
                tokio::net::TcpStream::connect((url.host_str().unwrap(), url.port().unwrap()))
                    .await?;
            drop(stream);
            self.state.lock().unwrap().proxy_ready_during_initialize = true;
        }
        if self.pause_initialization {
            _cancellation.cancelled().await;
        }
        match &self.initialization_error {
            Some(error) => Err(Box::new(std::io::Error::other(error.clone()))),
            None => Ok(()),
        }
    }

    /// 返回测试指定的工作目录。
    fn get_cwd(&self) -> &str {
        &self.cwd
    }

    /// 返回测试指定的 Claude 环境变量。
    fn get_claude_environment_variables(&self) -> BTreeMap<String, String> {
        self.environment.clone()
    }

    /// 仅为显式配置了 fake lease 的环境暴露动态 provider。
    fn lease_manager_provider(&self) -> Option<&dyn LeaseManagerProvider> {
        self.lease_manager
            .as_ref()
            .map(|_| self as &dyn LeaseManagerProvider)
    }
}

impl LeaseManagerProvider for FakeEnvironment {
    /// 返回环境自身配置的句柄，模拟 BYOC 被动态 trait object 发现后的创建行为。
    fn create_lease_manager(
        &self,
        _cancellation: &CancellationToken,
        _session_id: &str,
        _environment_id: &str,
        _api_base_url: &str,
        _session_ingress_token: &str,
        _organization_uuid: &str,
    ) -> Result<Arc<dyn LeaseManagerHandle>, BoxError> {
        let lease = self
            .lease_manager
            .as_ref()
            .ok_or_else(|| std::io::Error::other("fake lease provider is unavailable"))?;
        let handle: Arc<dyn LeaseManagerHandle> = lease.clone();
        Ok(handle)
    }
}

/// Manager API 的可配置 fake。
struct FakeManagerApiClient {
    response: GetEnvironmentConfigResponse,
    response_error: Option<String>,
    auth_context: Option<AuthContext>,
    outcomes: Option<Outcomes>,
}

#[async_trait]
impl ManagerApiClient for FakeManagerApiClient {
    /// 返回预设 environment response 或 API 错误。
    async fn get_environment_for_session(
        &self,
        _cancellation: &CancellationToken,
        _session_id: &str,
    ) -> Result<GetEnvironmentConfigResponse, ApiError> {
        match &self.response_error {
            Some(error) => Err(ApiError::message(error.clone())),
            None => Ok(self.response.clone()),
        }
    }

    /// 返回预设 auth context 副本。
    fn get_auth_context(&self) -> Option<AuthContext> {
        self.auth_context.clone()
    }

    /// 返回预设 outcomes 副本。
    fn get_outcomes(&self) -> Option<Outcomes> {
        self.outcomes.clone()
    }
}

/// Fake executor 的调用状态。
struct FakeExecutor {
    execute_called: AtomicBool,
    execute_received_cancelled: AtomicBool,
    destroy_called: AtomicBool,
    proxy_environment: Mutex<Vec<(OsString, OsString)>>,
    execution_error: Option<Vec<u8>>,
}

impl FakeExecutor {
    /// 创建可注入任意错误字节的 fake executor，用于验证 稳定错误 非 UTF-8 语义。
    fn new_bytes(execution_error: Option<&[u8]>) -> Self {
        Self {
            execute_called: AtomicBool::new(false),
            execute_received_cancelled: AtomicBool::new(false),
            destroy_called: AtomicBool::new(false),
            proxy_environment: Mutex::new(Vec::new()),
            execution_error: execution_error.map(<[u8]>::to_vec),
        }
    }
}

#[async_trait]
impl Executor for FakeExecutor {
    async fn execute_with_agent_proxy(
        &self,
        cancellation: &CancellationToken,
        proxy: Option<crate::internal::claude::AgentProxyHandle>,
    ) -> Result<(), ClaudeError> {
        if let Some(handle) = proxy.as_ref() {
            *self.proxy_environment.lock().unwrap() = handle.environment().to_vec();
        }
        let result = self.execute(cancellation).await;
        if let Some(handle) = proxy {
            handle.shutdown().await;
        }
        result
    }

    /// 标记执行并返回预设错误。
    async fn execute(&self, cancellation: &CancellationToken) -> Result<(), ClaudeError> {
        self.execute_called.store(true, Ordering::SeqCst);
        self.execute_received_cancelled
            .store(cancellation.is_cancelled(), Ordering::SeqCst);
        match &self.execution_error {
            Some(error) => Err(ClaudeError::message_bytes(error.clone())),
            None => Ok(()),
        }
    }

    /// 标记 detached cleanup 已销毁 executor。
    async fn destroy(&self, _cancellation: &CancellationToken) -> Result<(), ClaudeError> {
        self.destroy_called.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// Executor factory 收到的关键上下文。
#[derive(Default)]
struct FactoryCall {
    session_id: String,
    startup_context: Option<StartupContext>,
    auth_session_id: String,
    configuration: Option<ClaudeConfig>,
    activity_recorder: Option<Arc<dyn ActivityRecorder>>,
    has_lease: bool,
}

/// Fake tunnel 的连接与关闭状态。
struct FakeTunnelClient {
    connect_called: AtomicBool,
    close_called: AtomicBool,
}

#[async_trait]
impl TunnelClient for FakeTunnelClient {
    /// 标记连接并等待 executor 结束触发共享取消。
    async fn connect_and_run(
        self: Arc<Self>,
        cancellation: &CancellationToken,
    ) -> Result<(), ManagerError> {
        self.connect_called.store(true, Ordering::SeqCst);
        cancellation.cancelled().await;
        Err(ManagerError::message("operation cancelled"))
    }

    /// 标记 manager 调用了 Close。
    async fn close(&self) -> Result<(), ManagerError> {
        self.close_called.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// Fake lease 的 start/stop 状态。
struct FakeLeaseManager {
    started: AtomicBool,
    stopped: AtomicBool,
    cancellation: CancellationToken,
    cancel_on_start: bool,
}

#[async_trait]
impl LeaseManagerHandle for FakeLeaseManager {
    /// 标记初始 heartbeat 生命周期已启动。
    async fn start(&self) -> Result<(), BoxError> {
        self.started.store(true, Ordering::SeqCst);
        if self.cancel_on_start {
            self.cancellation.cancel();
        }
        Ok(())
    }

    /// 标记执行与 cleanup 完成后停止 lease。
    async fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }

    /// 返回 lease 自己的 token，供 manager 派生 executor direct child。
    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

/// 把 fake lease 组合成 manager 可消费的 runtime。
struct FakeLeaseAdapter {
    lease: Arc<FakeLeaseManager>,
}

impl EnvironmentLeaseAdapter for FakeLeaseAdapter {
    /// 忽略 environment 具体类型并返回由句柄自身 token 构造的 fake runtime。
    fn create_lease_manager(
        &self,
        _environment: &dyn EnvironmentType,
        _request: ManagerLeaseRequest,
    ) -> Result<Option<ManagerLeaseRuntime>, BoxError> {
        let handle: Arc<dyn LeaseManagerHandle> = self.lease.clone();
        Ok(Some(ManagerLeaseRuntime::new(handle)))
    }
}

/// Fake MCP server，仅用于验证 registration factory 结果透传。
struct FakeMcpServer {
    name: String,
}

#[async_trait]
impl MCPServer for FakeMcpServer {
    /// 返回 fake server 稳定名称。
    fn get_name(&self) -> String {
        self.name.clone()
    }

    /// Fake server 不公开工具。
    fn get_tools(self: Arc<Self>) -> Vec<ToolConfig> {
        Vec::new()
    }

    /// 返回不会被本测试使用的 loopback 配置。
    async fn start(&self, _tools: Vec<ToolConfig>) -> McpResult<ServerConfig> {
        Ok(ServerConfig {
            port: 1,
            bearer_token: String::new(),
        })
    }

    /// Fake server 无后台任务需要停止。
    async fn stop(&self) {}

    /// Fake server 不要求额外 Claude CLI 注册。
    fn should_register_with_claude(&self) -> bool {
        false
    }
}

/// 作用域结束时恢复测试修改过的进程环境变量。
struct EnvironmentRestore(Vec<(OsString, Option<OsString>)>);

impl EnvironmentRestore {
    /// 设置单个 UTF-8 环境变量并保存原值。
    fn set(&mut self, key: &str, value: impl AsRef<OsStr>) {
        self.0.push((OsString::from(key), std::env::var_os(key)));
        // SAFETY: 每个调用测试都持有 crate 级 PROCESS_STATE_TEST_LOCK，且所有受影响
        // Tokio task/child 都在 restore guard 析构前完成。
        unsafe { std::env::set_var(key, value) };
    }
}

impl Drop for EnvironmentRestore {
    /// 以修改逆序恢复原始环境，避免嵌套键覆盖。
    fn drop(&mut self) {
        for (key, value) in self.0.drain(..).rev() {
            match value {
                Some(value) => {
                    // SAFETY: guard 仍持有 crate 级 PROCESS_STATE_TEST_LOCK。
                    unsafe { std::env::set_var(&key, value) };
                }
                None => {
                    // SAFETY: guard 仍持有 crate 级 PROCESS_STATE_TEST_LOCK。
                    unsafe { std::env::remove_var(&key) };
                }
            }
        }
    }
}

/// 配置 manager 测试共用的无外部 plugin/git/tunnel 环境。
fn configure_process_environment(home: &TempDir) -> EnvironmentRestore {
    let mut restore = EnvironmentRestore(Vec::new());
    restore.set("HOME", home.path());
    restore.set("SKIP_PLUGIN_MARKETPLACE", "true");
    restore.set("SKIP_GIT_CONFIG", "true");
    restore.set("TUNNEL_ENABLED", "");
    restore.set("ENVRUNNER_RUNTIME_MCP_ADDR", "127.0.0.1:0");
    restore
}

/// 创建共享状态 fake environment。
fn fake_environment(initialization_error: Option<&str>) -> FakeEnvironment {
    FakeEnvironment {
        state: Arc::new(Mutex::new(FakeEnvironmentState::default())),
        cwd: "/workspace/project".to_owned(),
        environment: BTreeMap::from([(
            "ANTHROPIC_BASE_URL".to_owned(),
            "https://api.example.com".to_owned(),
        )]),
        initialization_error: initialization_error.map(str::to_owned),
        pause_initialization: false,
        lease_manager: None,
    }
}

/// 创建只注册一个 fake type 的 EnvTypes。
fn new_env_types_for_test(type_name: &str, environment: FakeEnvironment) -> Arc<EnvTypes> {
    let mut env_types = new_env_types();
    let factory_environment = environment;
    let factory: Factory = Arc::new(move |_raw_config| {
        Ok(Box::new(factory_environment.clone()) as Box<dyn EnvironmentType>)
    });
    env_types.register(Registration {
        type_name: type_name.to_owned(),
        factory: Some(factory),
        description: "fake".to_owned(),
    });
    Arc::new(env_types)
}

/// 创建带 session token 的测试认证上下文。
fn new_test_auth_context() -> AuthContext {
    let mut context = AuthContext::default();
    context.set_session_ingress_token("session-token");
    context
}

/// 新 Sandbox 必须先准备 CA/relay；成功交接同一 listener，任意失败都回收它。
#[tokio::test]
async fn session_proxy_precedes_sources_and_is_reused_and_cleaned_up() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::session_proxy_precedes_sources_and_is_reused_and_cleaned_up"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    for (ca_status, initialization_error, execution_error, cancel_initialization) in [
        (200, None, None, false),
        (200, Some("source failed"), None, false),
        (200, None, Some("executor failed"), false),
        (503, None, None, false),
        (200, None, None, true),
    ] {
        let home = TempDir::new().unwrap();
        let _environment_restore = configure_process_environment(&home);
        let original_proxy = std::env::var_os("HTTPS_PROXY");
        let original_ca = std::env::var_os("GIT_SSL_CAINFO");
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/v1/code/upstreamproxy/ca-cert"))
            .respond_with(wiremock::ResponseTemplate::new(ca_status).set_body_string("test CA"))
            .expect(1)
            .mount(&server)
            .await;
        let mut environment = fake_environment(initialization_error);
        environment.pause_initialization = cancel_initialization;
        let state = Arc::clone(&environment.state);
        let (mut manager, executor, _) =
            new_manager_for_test("fake", environment, SessionMode::New, true, execution_error);
        let startup = manager
            .session_config
            .as_mut()
            .unwrap()
            .startup_context
            .as_mut()
            .unwrap();
        startup.use_code_sessions = true;
        startup.environment_variables.extend([
            ("CLAUDE_CODE_REMOTE".to_owned(), "1".to_owned()),
            ("CCR_UPSTREAM_PROXY_ENABLED".to_owned(), "1".to_owned()),
            ("ANTHROPIC_BASE_URL".to_owned(), server.uri()),
        ]);
        let cancellation = CancellationToken::new();
        let cancel_during_sources = async {
            if cancel_initialization {
                loop {
                    if state.lock().unwrap().proxy_ready_during_initialize {
                        cancellation.cancel();
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(manager.run(&cancellation), cancel_during_sources)
        })
        .await
        .unwrap();
        assert_eq!(
            result.is_ok(),
            ca_status == 200
                && initialization_error.is_none()
                && execution_error.is_none()
                && !cancel_initialization
        );
        assert_eq!(std::env::var_os("HTTPS_PROXY"), original_proxy);
        assert_eq!(std::env::var_os("GIT_SSL_CAINFO"), original_ca);
        let (initialized, ready, network) = {
            let state = state.lock().unwrap();
            (
                state.initialized,
                state.proxy_ready_during_initialize,
                state.source_environment.clone(),
            )
        };
        if ca_status != 200 {
            assert!(!initialized, "failed CA download must stop before sources");
            assert!(!executor.execute_called.load(Ordering::SeqCst));
            continue;
        }
        assert!(initialized && ready);
        if initialization_error.is_none() && !cancel_initialization {
            assert_eq!(*executor.proxy_environment.lock().unwrap(), network);
        } else {
            assert!(!executor.execute_called.load(Ordering::SeqCst));
        }
        let proxy = network
            .iter()
            .find(|(key, _)| key == "HTTPS_PROXY")
            .unwrap();
        let url = url::Url::parse(proxy.1.to_str().unwrap()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if tokio::net::TcpStream::connect((url.host_str().unwrap(), url.port().unwrap()))
                    .await
                    .is_err()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("relay must stop on success and initialization/executor errors");
    }
}

/// 真实 executor 子进程必须接管 Source 使用的 listener，并覆盖旧的网络环境。
#[tokio::test]
async fn session_proxy_environment_reaches_real_executor_child() {
    use std::os::unix::fs::PermissionsExt as _;

    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::session_proxy_environment_reaches_real_executor_child"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let home = TempDir::new().unwrap();
    let _environment_restore = configure_process_environment(&home);
    let original_proxy = std::env::var_os("HTTPS_PROXY");
    let original_ca = std::env::var_os("GIT_SSL_CAINFO");
    let capture = home.path().join("child-network.json");
    let child = home.path().join("fake-claude");
    std::fs::write(
        &child,
        r#"#!/bin/sh
exec python3 - <<'PY'
import json, os, pathlib, socket, urllib.parse
keys = ["HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy", "SSL_CERT_FILE",
        "GIT_SSL_CAINFO", "NODE_EXTRA_CA_CERTS", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE"]
environment = {key: os.environ[key] for key in keys}
assert pathlib.Path(environment["GIT_SSL_CAINFO"]).is_file()
proxy = urllib.parse.urlsplit(environment["HTTPS_PROXY"])
with socket.create_connection((proxy.hostname, proxy.port), timeout=2) as connection:
    connection.sendall(b"GET / HTTP/1.1\r\nHost: relay.test\r\n\r\n")
    response = connection.makefile("rb").readline()
    assert response == b"HTTP/1.1 405 Method Not Allowed\r\n", response
pathlib.Path(os.environ["NETWORK_CAPTURE_PATH"]).write_text(json.dumps(environment))
print('{"type":"result"}')
PY
"#,
    )
    .unwrap();
    std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/v1/code/upstreamproxy/ca-cert"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string("test CA"))
        .expect(1)
        .mount(&server)
        .await;
    let mut environment = fake_environment(None);
    environment.cwd = home.path().display().to_string();
    let state = Arc::clone(&environment.state);
    let (mut manager, _, _) =
        new_manager_for_test("fake", environment, SessionMode::New, true, None);
    let configuration = manager.claude_config.as_mut().unwrap();
    configuration.claude_path = child.display().to_string();
    configuration.environment.insert(
        "NETWORK_CAPTURE_PATH".to_owned(),
        capture.display().to_string(),
    );
    let startup = manager
        .session_config
        .as_mut()
        .unwrap()
        .startup_context
        .as_mut()
        .unwrap();
    startup.use_code_sessions = true;
    startup.environment_variables.extend([
        ("CLAUDE_CODE_REMOTE".to_owned(), "1".to_owned()),
        ("CCR_UPSTREAM_PROXY_ENABLED".to_owned(), "1".to_owned()),
        ("ANTHROPIC_BASE_URL".to_owned(), server.uri()),
        ("HTTPS_PROXY".to_owned(), "http://127.0.0.1:1".to_owned()),
        ("GIT_SSL_CAINFO".to_owned(), "/missing-ca.pem".to_owned()),
    ]);
    manager.executor_factory = Arc::new(
        |configuration,
         session_id,
         startup,
         auth,
         registrations,
         outcomes,
         activity_recorder,
         lease| {
            let registrations = registrations
                .into_iter()
                .map(|registration| {
                    registration as Arc<dyn crate::internal::claude::McpServerRegistration>
                })
                .collect();
            Ok(Arc::new(crate::internal::claude::new_claude_code_executor(
                Some(configuration),
                session_id,
                startup.unwrap_or_default(),
                Some(auth),
                registrations,
                Some(outcomes),
                activity_recorder,
                lease,
                "current",
            )?) as Arc<dyn Executor>)
        },
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        manager.run(&CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    let network = {
        let state = state.lock().unwrap();
        assert!(state.proxy_ready_during_initialize);
        state
            .source_environment
            .iter()
            .map(|(key, value)| {
                (
                    key.to_str().unwrap().to_owned(),
                    value.to_str().unwrap().to_owned(),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    let captured: BTreeMap<String, String> =
        serde_json::from_slice(&std::fs::read(capture).unwrap()).unwrap();
    assert_eq!(captured, network);
    let proxy = url::Url::parse(&network["HTTPS_PROXY"]).unwrap();
    assert!(
        tokio::net::TcpStream::connect((proxy.host_str().unwrap(), proxy.port().unwrap()))
            .await
            .is_err(),
        "executor must wait for the shared listener to stop"
    );
    assert_eq!(std::env::var_os("HTTPS_PROXY"), original_proxy);
    assert_eq!(std::env::var_os("GIT_SSL_CAINFO"), original_ca);
    server.verify().await;
}

/// 创建标准 manager、executor 和 factory 调用记录。
fn new_manager_for_test(
    environment_type: &str,
    environment: FakeEnvironment,
    session_mode: SessionMode,
    session_mode_explicitly_set: bool,
    execution_error: Option<&str>,
) -> (Manager, Arc<FakeExecutor>, Arc<Mutex<FactoryCall>>) {
    new_manager_for_test_with_error_bytes(
        environment_type,
        environment,
        session_mode,
        session_mode_explicitly_set,
        execution_error.map(str::as_bytes),
    )
}

/// 创建允许注入任意 Claude 错误字节的标准 manager fixture。
fn new_manager_for_test_with_error_bytes(
    environment_type: &str,
    environment: FakeEnvironment,
    session_mode: SessionMode,
    session_mode_explicitly_set: bool,
    execution_error: Option<&[u8]>,
) -> (Manager, Arc<FakeExecutor>, Arc<Mutex<FactoryCall>>) {
    let executor = Arc::new(FakeExecutor::new_bytes(execution_error));
    let factory_executor: Arc<dyn Executor> = executor.clone();
    let factory_call = Arc::new(Mutex::new(FactoryCall::default()));
    let captured_call = Arc::clone(&factory_call);
    let executor_factory: ClaudeExecutorFactory = Arc::new(
        move |configuration,
              session_id,
              startup_context,
              auth_context,
              _registrations,
              _outcomes,
              activity_recorder,
              lease| {
            *captured_call
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = FactoryCall {
                session_id,
                startup_context,
                auth_session_id: auth_context.get_session_id().to_owned(),
                configuration: Some(configuration),
                activity_recorder: Some(activity_recorder),
                has_lease: lease.is_some(),
            };
            Ok(Arc::clone(&factory_executor))
        },
    );
    let startup_context = StartupContext {
        api_base_url: "https://api.example.com".to_owned(),
        ..StartupContext::default()
    };
    let session = Session {
        session_id: "session-123".to_owned(),
        startup_context: Some(startup_context),
        ..Session::default()
    };
    let api_client: Arc<dyn ManagerApiClient> = Arc::new(FakeManagerApiClient {
        response: GetEnvironmentConfigResponse {
            environment_type: environment_type.to_owned(),
            config: json!({}),
            allowed_tools: "Read,Write".to_owned(),
            disallowed_tools: "Bash".to_owned(),
        },
        response_error: None,
        auth_context: Some(new_test_auth_context()),
        outcomes: None,
    });
    let manager = new_manager_with_executor_and_claude_config(
        api_client,
        Some(session),
        new_env_types_for_test(environment_type, environment),
        executor_factory,
        session_mode,
        session_mode_explicitly_set,
        Some(default_claude_config()),
        "env-123",
        "org-123",
        SystemTime::now(),
        None,
    );
    (manager, executor, factory_call)
}

/// 验证 setup-only 完成 environment 配置、创建/销毁 executor，但不执行它。
#[tokio::test]
async fn test_run_setup_only_configures_environment_and_skips_execution() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_run_setup_only_configures_environment_and_skips_execution"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let home = TempDir::new().expect("create HOME");
    let _environment_restore = configure_process_environment(&home);
    let environment = fake_environment(None);
    let environment_state = Arc::clone(&environment.state);
    let (mut manager, executor, factory_call) =
        new_manager_for_test("fake", environment, SessionMode::SetupOnly, true, None);

    manager
        .run(&CancellationToken::new())
        .await
        .expect("setup-only run succeeds");

    let state = environment_state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert!(state.initialized);
    assert_eq!(state.session_mode, Some(SessionMode::SetupOnly));
    assert_eq!(
        state.auth_context.as_ref().map(AuthContext::get_session_id),
        Some("session-123")
    );
    drop(state);
    let configuration = manager.claude_config().expect("final Claude config");
    assert_eq!(configuration.working_dir, "/workspace/project");
    assert_eq!(
        configuration
            .environment
            .get("ANTHROPIC_BASE_URL")
            .map(String::as_str),
        Some("https://api.example.com")
    );
    assert_eq!(configuration.allowed_tools, "Read,Write");
    assert_eq!(configuration.disallowed_tools, "Bash");
    let call = factory_call
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(call.session_id, "session-123");
    assert_eq!(call.auth_session_id, "session-123");
    assert!(call.startup_context.is_some());
    assert!(call.activity_recorder.is_some());
    assert_eq!(
        call.configuration
            .as_ref()
            .map(|config| config.working_dir.as_str()),
        Some("/workspace/project")
    );
    drop(call);
    assert!(!executor.execute_called.load(Ordering::SeqCst));
    assert!(executor.destroy_called.load(Ordering::SeqCst));
}

/// 验证隐式 BYOC mode 在 environment 配置前覆盖为 resume-cached。
#[tokio::test]
async fn test_run_defaults_byoc_to_resume_cached_when_mode_implicit() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_run_defaults_byoc_to_resume_cached_when_mode_implicit"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let home = TempDir::new().expect("create HOME");
    let _environment_restore = configure_process_environment(&home);
    let environment = fake_environment(None);
    let state = Arc::clone(&environment.state);
    let (mut manager, executor, _) =
        new_manager_for_test("byoc", environment, SessionMode::New, false, None);

    manager
        .run(&CancellationToken::new())
        .await
        .expect("BYOC run succeeds");

    assert_eq!(manager.session_mode(), SessionMode::ResumeCached);
    assert_eq!(
        state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .session_mode,
        Some(SessionMode::ResumeCached)
    );
    assert!(executor.execute_called.load(Ordering::SeqCst));
    assert!(executor.destroy_called.load(Ordering::SeqCst));
}

/// 验证 environment 初始化失败发生在 executor factory 之前并保留错误包装。
#[tokio::test]
async fn test_run_returns_environment_initialization_error_before_creating_executor() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_run_returns_environment_initialization_error_before_creating_executor"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let home = TempDir::new().expect("create HOME");
    let _environment_restore = configure_process_environment(&home);
    let (mut manager, executor, factory_call) = new_manager_for_test(
        "fake",
        fake_environment(Some("init failed")),
        SessionMode::New,
        true,
        None,
    );

    let error = manager
        .run(&CancellationToken::new())
        .await
        .expect_err("environment init fails");

    assert_eq!(
        error.to_string(),
        "failed to initialize environment: init failed"
    );
    assert!(
        factory_call
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .session_id
            .is_empty()
    );
    assert!(!executor.execute_called.load(Ordering::SeqCst));
}

/// 验证 executor 的普通错误文本不会被误判为结构化取消。
#[tokio::test]
async fn test_run_does_not_parse_cancellation_text() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_run_does_not_parse_cancellation_text"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let home = TempDir::new().expect("create HOME");
    let _environment_restore = configure_process_environment(&home);
    let (mut manager, executor, _) = new_manager_for_test(
        "fake",
        fake_environment(None),
        SessionMode::New,
        true,
        Some("operation cancelled"),
    );

    let error = manager
        .run(&CancellationToken::new())
        .await
        .expect_err("uncancelled executor error must propagate");
    assert_eq!(error.to_string(), "execution failed: operation cancelled");

    assert!(executor.execute_called.load(Ordering::SeqCst));
    assert!(executor.destroy_called.load(Ordering::SeqCst));
}

/// 验证 Claude 任意错误字节经过 manager 的 `execution failed: source chain` 包装仍保持原值。
///
/// 日志需要合法 UTF-8，因此 `Display` 对 `ff fe` 分别产生 U+FFFD；最终 CLI 使用的
/// `as_bytes` 则必须连同 NUL 一字不改地保留。
#[tokio::test]
async fn manager_preserves_raw_claude_error_bytes() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::manager_preserves_raw_claude_error_bytes"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let home = TempDir::new().expect("create HOME");
    let _environment_restore = configure_process_environment(&home);
    let (mut manager, executor, _) = new_manager_for_test_with_error_bytes(
        "fake",
        fake_environment(None),
        SessionMode::New,
        true,
        Some(b"\xff\xfe\0"),
    );

    let error = manager
        .run(&CancellationToken::new())
        .await
        .expect_err("raw Claude error must propagate");

    assert_eq!(error.as_bytes(), b"execution failed: \xff\xfe\0");
    assert_eq!(error.to_string(), "execution failed: \u{fffd}\u{fffd}\0");
    assert_eq!(
        serde_json::to_vec(&json!({"error": error.to_string()})).expect("serialize log view"),
        b"{\"error\":\"execution failed: \xef\xbf\xbd\xef\xbf\xbd\\u0000\"}"
    );
    assert!(executor.execute_called.load(Ordering::SeqCst));
    assert!(executor.destroy_called.load(Ordering::SeqCst));
}

/// 验证启用 tunnel 时 factory、ConnectAndRun 和 Close 都被调用。
#[tokio::test]
async fn test_run_starts_tunnel_when_enabled() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_run_starts_tunnel_when_enabled"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let home = TempDir::new().expect("create HOME");
    let mut environment_restore = configure_process_environment(&home);
    environment_restore.set("TUNNEL_ENABLED", "true");
    let (mut manager, executor, _) =
        new_manager_for_test("fake", fake_environment(None), SessionMode::New, true, None);
    let tunnel = Arc::new(FakeTunnelClient {
        connect_called: AtomicBool::new(false),
        close_called: AtomicBool::new(false),
    });
    let factory_tunnel: Arc<dyn TunnelClient> = tunnel.clone();
    let factory_called = Arc::new(AtomicBool::new(false));
    let captured_called = Arc::clone(&factory_called);
    manager.set_tunnel_client_factory(Some(Arc::new(
        move |_manager, api_base_url, auth_token, auth_context| {
            assert_eq!(api_base_url, "https://api.example.com");
            assert_eq!(auth_token, "session-token");
            assert_eq!(
                auth_context.map(AuthContext::get_session_id),
                Some("session-123")
            );
            captured_called.store(true, Ordering::SeqCst);
            Ok(Arc::clone(&factory_tunnel))
        },
    )));

    manager
        .run(&CancellationToken::new())
        .await
        .expect("tunnel run succeeds");

    assert!(factory_called.load(Ordering::SeqCst));
    assert!(tunnel.connect_called.load(Ordering::SeqCst));
    assert!(tunnel.close_called.load(Ordering::SeqCst));
    assert!(executor.execute_called.load(Ordering::SeqCst));
    assert!(executor.destroy_called.load(Ordering::SeqCst));
}

/// 验证 tunnel 未启用时不调用已注入的 factory，但 executor 仍完整执行和销毁。
#[tokio::test]
async fn test_run_skips_tunnel_when_disabled() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_run_skips_tunnel_when_disabled"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let home = TempDir::new().expect("create HOME");
    // 公共 fixture 把 `TUNNEL_ENABLED` 设为空串，精确对应 disabled 场景。
    let _environment_restore = configure_process_environment(&home);
    let (mut manager, executor, _) =
        new_manager_for_test("fake", fake_environment(None), SessionMode::New, true, None);
    let factory_called = Arc::new(AtomicBool::new(false));
    let captured_called = Arc::clone(&factory_called);
    let unused_tunnel: Arc<dyn TunnelClient> = Arc::new(FakeTunnelClient {
        connect_called: AtomicBool::new(false),
        close_called: AtomicBool::new(false),
    });
    manager.set_tunnel_client_factory(Some(Arc::new(move |_, _, _, _| {
        captured_called.store(true, Ordering::SeqCst);
        Ok(Arc::clone(&unused_tunnel))
    })));

    manager
        .run(&CancellationToken::new())
        .await
        .expect("disabled tunnel must not affect manager run");

    assert!(!factory_called.load(Ordering::SeqCst));
    assert!(executor.execute_called.load(Ordering::SeqCst));
    assert!(executor.destroy_called.load(Ordering::SeqCst));
}

/// 验证 lease start、factory 透传和最终 stop 生命周期。
#[tokio::test]
async fn run_starts_and_stops_injected_lease_runtime() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::run_starts_and_stops_injected_lease_runtime"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let home = TempDir::new().expect("create HOME");
    let _environment_restore = configure_process_environment(&home);
    let (mut manager, _, factory_call) =
        new_manager_for_test("fake", fake_environment(None), SessionMode::New, true, None);
    let lease = Arc::new(FakeLeaseManager {
        started: AtomicBool::new(false),
        stopped: AtomicBool::new(false),
        cancellation: CancellationToken::new(),
        cancel_on_start: false,
    });
    manager.set_environment_lease_adapter(Some(Arc::new(FakeLeaseAdapter {
        lease: Arc::clone(&lease),
    })));

    manager
        .run(&CancellationToken::new())
        .await
        .expect("lease-backed run succeeds");

    assert!(lease.started.load(Ordering::SeqCst));
    assert!(lease.stopped.load(Ordering::SeqCst));
    assert!(
        factory_call
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .has_lease
    );
}

/// 验证生产默认路径从动态 environment 发现 provider，并把真实 lease token 传给 executor。
#[tokio::test]
async fn run_discovers_environment_lease_and_uses_its_cancellation_token() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::run_discovers_environment_lease_and_uses_its_cancellation_token"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let home = TempDir::new().expect("create HOME");
    let _environment_restore = configure_process_environment(&home);

    let parent_cancellation = CancellationToken::new();
    let lease = Arc::new(FakeLeaseManager {
        started: AtomicBool::new(false),
        stopped: AtomicBool::new(false),
        // 与 podmonitor 一样从 manager token 派生；lease 可独立取消子树而不反向取消父级。
        cancellation: parent_cancellation.child_token(),
        cancel_on_start: true,
    });
    let mut environment = fake_environment(None);
    environment.lease_manager = Some(Arc::clone(&lease));
    let (mut manager, executor, factory_call) =
        new_manager_for_test("provider", environment, SessionMode::New, true, None);

    // 不注入 EnvironmentLeaseAdapter：这正是生产默认发现路径。
    manager
        .run(&parent_cancellation)
        .await
        .expect("dynamically discovered lease-backed run succeeds");

    assert!(lease.started.load(Ordering::SeqCst));
    assert!(lease.stopped.load(Ordering::SeqCst));
    assert!(!parent_cancellation.is_cancelled());
    assert!(executor.execute_received_cancelled.load(Ordering::SeqCst));
    assert!(
        factory_call
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .has_lease
    );
}

/// 验证 tunnel enable 只接受精确 true/1 且要求 factory 已注册。
#[tokio::test]
async fn tunnel_enablement_accepts_supported_values() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::tunnel_enablement_accepts_supported_values"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let home = TempDir::new().expect("create HOME");
    let mut environment_restore = configure_process_environment(&home);
    let placeholder_factory: CreateTunnelClientFunc = Arc::new(
        |_manager: &Manager,
         _api_base_url: &str,
         _auth_token: &str,
         _auth_context: Option<&AuthContext>| { Err(ManagerError::message("unused")) },
    );

    environment_restore.set("TUNNEL_ENABLED", "true");
    assert!(is_tunnel_enabled(Some(&placeholder_factory)));
    assert!(!is_tunnel_enabled(None));
}

/// 验证单个 MCP registration factory 的 server 结果原样返回。
#[tokio::test]
async fn test_setup_mcp_server_with_registration() {
    let environment = fake_environment(None);
    let (manager, _, _) = new_manager_for_test("fake", environment, SessionMode::New, true, None);
    let server = Arc::new(FakeMcpServer {
        name: "codesign".to_owned(),
    });
    let expected_server: Arc<dyn MCPServer> = server.clone();
    let factory_server = Arc::clone(&expected_server);
    let registration = Arc::new(MCPServerRegistration {
        name: "codesign".to_owned(),
        description: "Code signing".to_owned(),
        factory: Arc::new(move |_, _, _, _, _| {
            let server = Arc::clone(&factory_server);
            Box::pin(async move { Ok(server) })
        }),
        should_register: Arc::new(|_, _| true),
        get_additional_system_prompt: Arc::new(String::new),
        get_tools: Arc::new(Vec::new),
    });
    let activity_recorder: Arc<dyn ActivityRecorder> = Arc::new(NoopActivityRecorder);

    let result = manager
        .setup_mcp_server_with_registration(
            &CancellationToken::new(),
            &registration,
            &new_test_auth_context(),
            "/workspace",
            activity_recorder,
        )
        .await
        .expect("MCP factory succeeds");

    assert_eq!(result.get_name(), "codesign");
    assert!(Arc::ptr_eq(&result, &expected_server));
}

/// 验证 MCP factory 错误带有 server 名称和阶段包装。
#[tokio::test]
async fn test_setup_mcp_server_with_registration_wraps_factory_error() {
    let (manager, _, _) =
        new_manager_for_test("fake", fake_environment(None), SessionMode::New, true, None);
    let registration = Arc::new(MCPServerRegistration {
        name: "broken".to_owned(),
        description: "Broken".to_owned(),
        factory: Arc::new(move |_, _, _, _, _| {
            Box::pin(async move { Err(McpError::message("boom")) })
        }),
        should_register: Arc::new(|_, _| true),
        get_additional_system_prompt: Arc::new(String::new),
        get_tools: Arc::new(Vec::new),
    });
    let activity_recorder: Arc<dyn ActivityRecorder> = Arc::new(NoopActivityRecorder);

    let result = manager
        .setup_mcp_server_with_registration(
            &CancellationToken::new(),
            &registration,
            &new_test_auth_context(),
            "/workspace",
            activity_recorder,
        )
        .await;
    let error = match result {
        Ok(_) => panic!("MCP factory unexpectedly succeeded"),
        Err(error) => error,
    };

    assert_eq!(
        error.to_string(),
        "failed to create and start broken MCP server: boom"
    );
}

/// The embedded server is available with the initialized environment even without Claude.
#[tokio::test]
async fn runtime_mcp_starts_in_skip_mode_and_stops_with_manager() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::runtime_mcp_starts_in_skip_mode_and_stops_with_manager"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let home = TempDir::new().unwrap();
    let mut restore = configure_process_environment(&home);
    let reservation = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    restore.set("ENVRUNNER_RUNTIME_MCP_ADDR", format!("0.0.0.0:{port}"));
    let mut environment = fake_environment(None);
    environment.cwd = home.path().to_str().unwrap().into();
    environment
        .environment
        .insert("RUNTIME_MCP_TEST_VALUE".into(), "from-environment".into());
    let (mut manager, executor, _) =
        new_manager_for_test("fake", environment, SessionMode::SetupOnly, true, None);
    manager.claude_config.as_mut().unwrap().skip_claude_code = true;
    let cancellation = CancellationToken::new();
    let run_token = cancellation.clone();
    let task = tokio::spawn(async move { manager.run(&run_token).await });
    let url = format!("http://127.0.0.1:{port}/mcp");
    let request = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"bash","arguments":{"command":"printf '%s|%s' \"$PWD\" \"$RUNTIME_MCP_TEST_VALUE\""},"_meta":qoder_mcp_server::protocol::request_meta()}});
    let client = reqwest::Client::new();
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let result = client
                .post(&url)
                .header("host", "sandbox.external.example")
                .header("accept", "application/json, text/event-stream")
                .header("mcp-protocol-version", "2026-07-28")
                .header("mcp-method", "tools/call")
                .header("mcp-name", "bash")
                .json(&request)
                .send()
                .await;
            if let Ok(response) = result {
                break response;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(response.status().is_success());
    let text = response.text().await.unwrap();
    let result: serde_json::Value = text
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .find_map(|data| {
            serde_json::from_str::<serde_json::Value>(data)
                .ok()
                .filter(|value| value.get("result").is_some())
        })
        .expect("SSE tool result");
    assert_eq!(
        result["result"]["structuredContent"]["stdout"],
        format!("{}|from-environment", home.path().display())
    );
    let mut long_request = request.clone();
    long_request["id"] = json!(2);
    long_request["params"]["arguments"]["command"] =
        json!("trap 'touch stopped; exit 0' TERM; touch ready; sleep 30");
    long_request["params"]["_meta"]["progressToken"] = json!(2);
    let active = client
        .post(&url)
        .header("host", "sandbox.external.example")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "tools/call")
        .header("mcp-name", "bash")
        .json(&long_request)
        .send()
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !home.path().join("ready").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    cancellation.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(7), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let _listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("manager released MCP listener");
    let _ = active.text().await;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !home.path().join("stopped").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("shutdown cancelled the active Bash process group");
    assert!(!executor.execute_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn runtime_mcp_bind_failure_prevents_executor_creation() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::runtime_mcp_bind_failure_prevents_executor_creation"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let home = TempDir::new().unwrap();
    let mut restore = configure_process_environment(&home);
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    restore.set(
        "ENVRUNNER_RUNTIME_MCP_ADDR",
        occupied.local_addr().unwrap().to_string(),
    );
    let (mut manager, _, factory_call) = new_manager_for_test(
        "fake",
        fake_environment(None),
        SessionMode::SetupOnly,
        true,
        None,
    );
    let error = manager.run(&CancellationToken::new()).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("failed to start runtime MCP server")
    );
    assert!(factory_call.lock().unwrap().session_id.is_empty());
}

#[tokio::test]
async fn runtime_mcp_stops_when_executor_factory_fails() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::runtime_mcp_stops_when_executor_factory_fails"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let home = TempDir::new().unwrap();
    let mut restore = configure_process_environment(&home);
    let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    restore.set("ENVRUNNER_RUNTIME_MCP_ADDR", address.to_string());
    let (mut manager, _, _) = new_manager_for_test(
        "fake",
        fake_environment(None),
        SessionMode::SetupOnly,
        true,
        None,
    );
    manager.executor_factory = Arc::new(move |_, _, _, _, _, _, _, _| {
        std::net::TcpStream::connect(address).expect("MCP starts before executor creation");
        Err(Box::new(std::io::Error::other("factory failed")) as BoxError)
    });
    let error = manager.run(&CancellationToken::new()).await.unwrap_err();
    assert!(error.to_string().contains("factory failed"));
    let _listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("error path released MCP listener");
}
