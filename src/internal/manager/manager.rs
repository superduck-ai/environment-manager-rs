//! Environment manager 的主启动、并行初始化、执行与清理状态机。
//!
//! 本文件机械对应 Go `internal/manager/manager.go`。Goroutine/WaitGroup/errgroup 分别
//! 迁移为 Tokio task、join handle 和共享 `CancellationToken`；所有外部命令取消仅终止
//! direct child，绝不创建或清理进程组。

use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs,
    os::unix::{fs::symlink, process::ExitStatusExt as _},
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use nix::{
    fcntl::{FcntlArg, FdFlag, fcntl},
    unistd::pipe,
};
use serde_json::{Value, json};
use tokio::{io::AsyncReadExt as _, process::Command};
use tokio_util::sync::CancellationToken;

use crate::internal::{
    api::{LogCategory, LogLevel},
    auth::AuthContext,
    claude::{self, Executor, Outcomes},
    config::{self, ClaudeConfig, Session, SessionMode},
    envtype::EnvironmentType,
    logger::{LogAttributes, Logger},
    mcp::MCPServer,
    session::{ActivityRecorder, EventPoster, NoopActivityRecorder, new_activity_recorder},
};

use super::{
    tunnel_register::default_create_tunnel_client_func,
    zz_recovered_types::{
        ClaudeExecutorFactory, CommandFailure, CreateTunnelClientFunc, EnvironmentLeaseAdapter,
        Manager, ManagerApiClient, ManagerError, ManagerLeaseRequest, ManagerLeaseRuntime,
        ManagerObservability, McpRegistrationBatch,
    },
};

/// Executor/MCP 清理脱离父取消状态后的固定总预算。
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
/// Code-sign helper 的固定绝对 symlink 路径。
const CODE_SIGN_PATH: &str = "/tmp/code-sign";
/// 官方 Claude plugin marketplace 的稳定 Git URL。
const OFFICIAL_PLUGIN_MARKETPLACE: &str =
    "https://github.com/anthropics/claude-plugins-official.git";

/// 构造完整 manager，并安装默认 tunnel factory。
///
/// 参数顺序机械对应 Go `NewManagerWithExecutorAndClaudeConfig`；可选配置使用 owned clone，
/// event poster 和跨任务依赖使用 `Arc`。BYOC lease 从动态 environment 自动发现；
/// 测试仍可在构造后显式覆盖 lease/o11y adapter。
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn new_manager_with_executor_and_claude_config(
    api_client: Arc<dyn ManagerApiClient>,
    logger: Logger,
    session_config: Option<Session>,
    env_types: Arc<crate::internal::envtype::EnvTypes>,
    executor_factory: ClaudeExecutorFactory,
    session_mode: SessionMode,
    session_mode_explicitly_set: bool,
    claude_config: Option<ClaudeConfig>,
    environment_id: impl Into<String>,
    organization_uuid: impl Into<String>,
    process_start_time: SystemTime,
    event_poster: Option<Arc<dyn EventPoster>>,
) -> Manager {
    Manager {
        api_client,
        logger,
        session_config,
        env_types,
        executor_factory,
        session_mode,
        session_mode_explicitly_set,
        claude_config,
        environment_id: environment_id.into(),
        organization_uuid: organization_uuid.into(),
        process_start_time,
        event_poster,
        create_tunnel_client_func: Some(default_create_tunnel_client_func()),
        environment_lease_adapter: None,
        observability: None,
    }
}

impl Manager {
    /// 替换或注销默认 tunnel factory；`None` 对应 Go 未注册 factory 的状态。
    pub fn set_tunnel_client_factory(&mut self, factory: Option<CreateTunnelClientFunc>) {
        self.create_tunnel_client_func = factory;
    }

    /// 注入 environment lease 发现的测试/特殊组合层覆盖。
    pub fn set_environment_lease_adapter(
        &mut self,
        adapter: Option<Arc<dyn EnvironmentLeaseAdapter>>,
    ) {
        self.environment_lease_adapter = adapter;
    }

    /// 注入无 PII manager 诊断出口；`None` 时静默省略 diag 事件。
    pub fn set_observability(&mut self, observability: Option<Arc<dyn ManagerObservability>>) {
        self.observability = observability;
    }

    /// 返回当前有效 session mode，供启动组合层和行为测试检查 BYOC 默认覆盖。
    #[must_use]
    pub fn session_mode(&self) -> SessionMode {
        self.session_mode
    }

    /// 返回运行过程中应用 environment 覆盖后的 Claude 配置。
    #[must_use]
    pub fn claude_config(&self) -> Option<&ClaudeConfig> {
        self.claude_config.as_ref()
    }

    /// 执行 environment manager 的完整生命周期。
    ///
    /// 顺序严格保持：环境配置与类型创建、三路并行初始化、非致命 MCP/plugin 处理、
    /// lease、executor、setup-only 或 executor+tunnel 并发，以及逆序清理。
    /// Go 的 `registerMCPServersAsync` 已合并为本函数创建并等待的 MCP Tokio task；
    /// `initializeEnvironmentAsync` 则由同一并行阶段直接执行配置应用和 environment
    /// future。两条路径都返回拥有所有权的结果，不再通过 WaitGroup 后写裸指针。
    pub async fn run(&mut self, cancellation: &CancellationToken) -> Result<(), ManagerError> {
        let started = Instant::now();
        // Go 恢复结构保留该字段但 Run 不消费；显式读取避免把协议字段误判为死代码。
        let _ = self.process_start_time;
        let session_id = self
            .session_config
            .as_ref()
            .map_or_else(String::new, |session| session.session_id.clone());
        self.logger.info(
            "Starting environment manager",
            attributes([("session_id", json!(&session_id))]),
        );

        let environment_response = match self
            .api_client
            .get_environment_for_session(cancellation, &session_id)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.log_diag("environment_config_failed", BTreeMap::new());
                return Err(ManagerError::message(format!(
                    "failed to get environment configuration: {error}"
                )));
            }
        };

        if environment_response.environment_type == "byoc" && !self.session_mode_explicitly_set {
            self.logger.info(
                "Defaulting BYOC environment to resume-cached session mode",
                attributes([
                    ("original_mode", json!(self.session_mode.as_str())),
                    ("new_mode", json!(SessionMode::ResumeCached.as_str())),
                ]),
            );
            self.session_mode = SessionMode::ResumeCached;
        }

        let raw_environment_config =
            serde_json::to_vec(&environment_response.config).map_err(|error| {
                ManagerError::message(format!(
                    "failed to create environment type {}: {error}",
                    environment_response.environment_type
                ))
            })?;
        let mut environment = match self.env_types.create(
            &environment_response.environment_type,
            &raw_environment_config,
            self.logger.clone(),
        ) {
            Ok(environment) => environment,
            Err(error) => {
                self.log_diag(
                    "environment_create_failed",
                    attributes([(
                        "environment_type",
                        json!(&environment_response.environment_type),
                    )])
                    .into_iter()
                    .collect(),
                );
                return Err(ManagerError::message(format!(
                    "failed to create environment type {}: {error}",
                    environment_response.environment_type
                )));
            }
        };

        let manager_had_claude_config = self.claude_config.is_some();
        let mut claude_config = self
            .claude_config
            .clone()
            .unwrap_or_else(config::default_claude_config);
        if !environment_response.allowed_tools.is_empty() && claude_config.allowed_tools.is_empty()
        {
            claude_config
                .allowed_tools
                .clone_from(&environment_response.allowed_tools);
            self.logger.info(
                "Using allowed tools from environment config",
                attributes([("allowed_tools", json!(&environment_response.allowed_tools))]),
            );
        }
        if !environment_response.disallowed_tools.is_empty()
            && claude_config.disallowed_tools.is_empty()
        {
            claude_config
                .disallowed_tools
                .clone_from(&environment_response.disallowed_tools);
        }
        // 非 nil Go pointer 在 MCP goroutine 启动前已被原地更新；Rust clone 回字段复刻可见性。
        if manager_had_claude_config {
            self.claude_config = Some(claude_config.clone());
        }

        let mut auth_context = self
            .api_client
            .get_auth_context()
            .expect("No auth context found");
        auth_context.set_session_id(&session_id);

        let working_directory = if claude_config.working_dir.is_empty() {
            environment.get_cwd().to_owned()
        } else {
            claude_config.working_dir.clone()
        };
        let skip_claude_code = claude_config.skip_claude_code;
        let activity_recorder: Arc<dyn ActivityRecorder> = match &self.event_poster {
            Some(event_poster) => Arc::new(new_activity_recorder(
                cancellation.clone(),
                Arc::clone(event_poster),
                self.logger.clone(),
                None,
            )),
            None => Arc::new(NoopActivityRecorder),
        };

        self.logger.info(
            "Starting parallel initialization: MCP servers, plugin marketplace, and environment setup",
            LogAttributes::new(),
        );
        self.log_diag("parallel_init_started", BTreeMap::new());
        let parallel_started = Instant::now();

        let mcp_manager = self.clone();
        let mcp_cancellation = cancellation.clone();
        let mcp_auth_context = auth_context.clone();
        let mcp_working_directory = working_directory.clone();
        let mcp_activity_recorder = Arc::clone(&activity_recorder);
        let mcp_task = tokio::spawn(async move {
            mcp_manager
                .register_mcp_servers(
                    &mcp_cancellation,
                    &mcp_auth_context,
                    &mcp_working_directory,
                    mcp_activity_recorder,
                )
                .await
        });

        let plugin_task = if skip_claude_code {
            self.logger.info(
                "Skipping plugin marketplace installation because Claude Code is disabled",
                LogAttributes::new(),
            );
            None
        } else if matches!(
            std::env::var("SKIP_PLUGIN_MARKETPLACE").as_deref(),
            Ok("true")
        ) {
            self.logger.info(
                "Skipping plugin marketplace installation (SKIP_PLUGIN_MARKETPLACE=true)",
                LogAttributes::new(),
            );
            None
        } else {
            let plugin_manager = self.clone();
            let plugin_cancellation = cancellation.clone();
            Some(tokio::spawn(async move {
                plugin_manager
                    .add_official_plugin_marketplace_async(&plugin_cancellation)
                    .await
            }))
        };

        self.configure_environment(cancellation, environment.as_mut(), &auth_context)
            .await;
        self.apply_environment_config(environment.as_ref(), &mut claude_config);
        let environment_initialization = environment
            .initialize(cancellation, Some(activity_recorder.as_ref()))
            .await;

        let (mcp_batch, mcp_error) = match mcp_task.await {
            Ok(batch) => (batch, None),
            Err(error) => (
                McpRegistrationBatch {
                    servers: Vec::new(),
                    registrations: Vec::new(),
                },
                Some(ManagerError::message(format!(
                    "MCP registration task failed: {error}"
                ))),
            ),
        };
        let plugin_marketplace_error = match plugin_task {
            Some(task) => match task.await {
                Ok(result) => result.err(),
                Err(error) => Some(ManagerError::message(format!(
                    "plugin marketplace task failed: {error}"
                ))),
            },
            None => None,
        };

        let parallel_duration_ms = duration_milliseconds(parallel_started);
        self.logger.info(
            "parallel_init_completed",
            attributes([
                ("duration_ms", json!(parallel_duration_ms)),
                ("mcp_success", json!(mcp_error.is_none())),
                ("plugin_success", json!(plugin_marketplace_error.is_none())),
                ("env_success", json!(environment_initialization.is_ok())),
            ]),
        );
        self.log_diag(
            "parallel_init_completed",
            attributes([
                ("duration_ms", json!(parallel_duration_ms)),
                ("mcp_success", json!(mcp_error.is_none())),
                ("plugin_success", json!(plugin_marketplace_error.is_none())),
                ("env_success", json!(environment_initialization.is_ok())),
            ])
            .into_iter()
            .collect(),
        );
        activity_recorder.record_activity(
            LogLevel::Info,
            LogCategory::Init,
            "Plugin marketplace installation completed (parallel)".to_owned(),
            attributes([("duration_ms", json!(parallel_duration_ms))])
                .into_iter()
                .collect(),
        );

        // 后续 factory、setup-only 和测试需要看到 environment 覆盖后的最终配置。
        self.claude_config = Some(claude_config.clone());
        if let Err(error) = environment_initialization {
            self.log_diag(
                "environment_init_failed",
                attributes([
                    (
                        "environment_type",
                        json!(&environment_response.environment_type),
                    ),
                    ("duration_ms", json!(parallel_duration_ms)),
                ])
                .into_iter()
                .collect(),
            );
            return Err(ManagerError::message(format!(
                "failed to initialize environment: {error}"
            )));
        }
        if let Some(error) = &mcp_error {
            self.log_diag(
                "mcp_registration_failed",
                attributes([("server_count", json!(mcp_batch.registrations.len()))])
                    .into_iter()
                    .collect(),
            );
            self.logger.warn(
                "Failed to register MCP servers (parallel)",
                attributes([("error", json!(error.to_string()))]),
            );
        }
        if let Some(error) = &plugin_marketplace_error {
            self.log_diag("plugin_marketplace_failed", BTreeMap::new());
            self.logger.warn(
                "Plugin marketplace installation failed (parallel)",
                attributes([("error", json!(error.to_string()))]),
            );
        }

        if skip_claude_code {
            self.logger.info(
                "ENVRUNNER_SKIP_CLAUDE_CODE mode: environment initialized, waiting for shutdown signal",
                LogAttributes::new(),
            );
            wait_for_shutdown_signal(cancellation, &self.logger).await;
            return Ok(());
        }

        let outcomes = match self.api_client.get_outcomes() {
            Some(outcomes) if !outcomes.is_empty() => {
                self.logger.info(
                    "ClaudeCodeExecutor initialized with repository outcomes",
                    attributes([("outcome_count", json!(outcomes.len()))]),
                );
                outcomes
            }
            _ => Outcomes::new(),
        };
        let lease_request = ManagerLeaseRequest {
            cancellation: cancellation.clone(),
            session_id: session_id.clone(),
            environment_id: self.environment_id.clone(),
            api_base_url: self
                .session_config
                .as_ref()
                .and_then(|session| session.startup_context.as_ref())
                .map_or_else(String::new, |context| context.api_base_url.clone()),
            session_ingress_token: auth_context.get_session_ingress_token().to_owned(),
            organization_uuid: self.organization_uuid.clone(),
        };
        let lease_runtime = match &self.environment_lease_adapter {
            Some(adapter) => adapter
                .create_lease_manager(environment.as_ref(), lease_request)
                .map_err(|error| ManagerError::message(error.to_string()))?,
            None => environment
                .lease_manager_provider()
                .map(|provider| {
                    provider
                        .create_lease_manager(
                            &lease_request.cancellation,
                            &lease_request.session_id,
                            &lease_request.environment_id,
                            &lease_request.api_base_url,
                            &lease_request.session_ingress_token,
                            &lease_request.organization_uuid,
                        )
                        .map(ManagerLeaseRuntime::new)
                })
                .transpose()
                .map_err(|error| ManagerError::message(error.to_string()))?,
        };
        if let Some(runtime) = &lease_runtime {
            runtime.handle.start().await.map_err(|error| {
                ManagerError::message(format!("failed to start lease manager: {error}"))
            })?;
            self.logger.info(
                "Using lease manager context for Claude Code execution",
                attributes([("session_id", json!(&session_id))]),
            );
        }
        let executor_cancellation = lease_runtime.as_ref().map_or_else(
            || cancellation.clone(),
            |runtime| runtime.cancellation.clone(),
        );

        let executor_create_started = Instant::now();
        let executor = match (self.executor_factory)(
            self.logger.clone(),
            claude_config.clone(),
            session_id.clone(),
            self.session_config
                .as_ref()
                .and_then(|session| session.startup_context.clone()),
            auth_context.clone(),
            mcp_batch.registrations.clone(),
            outcomes,
            Arc::clone(&activity_recorder),
            lease_runtime
                .as_ref()
                .map(|runtime| Arc::clone(&runtime.handle)),
        ) {
            Ok(executor) => executor,
            Err(error) => {
                stop_lease(lease_runtime.as_ref()).await;
                // Factory trait 为了可替换性返回 BoxError；生产 factory 实际会装入
                // ClaudeError。先 downcast 可避免在 Box→String 边界损坏未来可能携带
                // CombinedOutput 的原始字节，其他 UTF-8 错误仍走普通 Display。
                let error = match error.downcast::<claude::ClaudeError>() {
                    Ok(error) => ManagerError::prefixed(
                        b"failed to create Claude executor: ",
                        error.as_bytes(),
                    ),
                    Err(error) => {
                        ManagerError::message(format!("failed to create Claude executor: {error}"))
                    }
                };
                return Err(error);
            }
        };
        let executor_create_duration_ms = duration_milliseconds(executor_create_started);
        let total_startup_duration_ms = duration_milliseconds(started);
        self.logger.info(
            "Starting Claude Code",
            attributes([
                (
                    "executor_create_duration_ms",
                    json!(executor_create_duration_ms),
                ),
                (
                    "total_startup_duration_ms",
                    json!(total_startup_duration_ms),
                ),
            ]),
        );
        activity_recorder.record_activity(
            LogLevel::Info,
            LogCategory::Init,
            "Starting Claude Code".to_owned(),
            attributes([
                ("duration_ms", json!(executor_create_duration_ms)),
                (
                    "total_startup_duration_ms",
                    json!(total_startup_duration_ms),
                ),
            ])
            .into_iter()
            .collect(),
        );

        let operation_result = if self.session_mode == SessionMode::SetupOnly {
            self.logger.info(
                "Session mode is setup-only, running init hooks before exit",
                LogAttributes::new(),
            );
            let result = run_init_only(
                cancellation,
                &self.logger,
                &claude_config,
                self.session_config.as_ref(),
            )
            .await;
            if result.is_ok() {
                self.logger.info(
                    "Session mode is setup-only, skipping Claude Code execution",
                    LogAttributes::new(),
                );
            }
            result
        } else {
            self.execute_claude_and_tunnel(
                &executor_cancellation,
                &session_id,
                &auth_context,
                Arc::clone(&executor),
                started,
                lease_runtime.is_some(),
            )
            .await
        };

        self.cleanup_mcp_and_executor(&mcp_batch.servers, &executor)
            .await;
        stop_lease(lease_runtime.as_ref()).await;
        operation_result
    }

    /// 把 startup/auth/session mode 写入 environment，并在非本地测试模式配置 Git 签名。
    async fn configure_environment(
        &self,
        cancellation: &CancellationToken,
        environment: &mut dyn EnvironmentType,
        auth_context: &AuthContext,
    ) {
        if let Some(startup_context) = self
            .session_config
            .as_ref()
            .and_then(|session| session.startup_context.clone())
        {
            let has_mcp_config = startup_context.mcp_config.is_some();
            environment.set_startup_context(Some(startup_context));
            self.logger.info(
                "Set startup context for environment",
                attributes([("has_mcp_config", json!(has_mcp_config))]),
            );
        }
        environment.set_auth_context(Some(auth_context.clone()));
        self.logger.info(
            "Set auth context for environment",
            attributes([("session_id", json!(auth_context.get_session_id()))]),
        );
        environment.set_session_mode(self.session_mode);
        self.logger.info(
            "Set session mode for environment",
            attributes([("session_mode", json!(self.session_mode.as_str()))]),
        );

        if self
            .claude_config
            .as_ref()
            .is_some_and(|configuration| configuration.local_testing_mode)
        {
            self.logger.info(
                "Skipping git signing configuration in local testing mode",
                LogAttributes::new(),
            );
            return;
        }
        self.configure_git_signing(cancellation).await;
    }

    /// 创建 `/tmp/code-sign` 并尽力写入四项全局 Git SSH 签名配置。
    ///
    /// 路径或 symlink 失败会提前降级；单项 `git config` 失败只 warning 并继续。取消
    /// 通过 [`run_combined_command`] 只 kill 当前 direct child。
    async fn configure_git_signing(&self, cancellation: &CancellationToken) {
        if matches!(std::env::var("SKIP_GIT_CONFIG").as_deref(), Ok("true")) {
            self.logger.info(
                "Skipping git signing configuration (SKIP_GIT_CONFIG=true)",
                LogAttributes::new(),
            );
            return;
        }

        let executable_path = match std::env::current_exe() {
            Ok(path) => path,
            Err(error) => {
                self.logger.warn(
                    "Failed to get executable path for git signing config",
                    attributes([("error", json!(error.to_string()))]),
                );
                return;
            }
        };
        let resolved_path = match executable_path.canonicalize() {
            Ok(path) => path,
            Err(error) => {
                self.logger.warn(
                    "failed to read symlink",
                    attributes([
                        ("path", json!(executable_path.display().to_string())),
                        ("error", json!(error.to_string())),
                    ]),
                );
                return;
            }
        };

        let _ = fs::remove_file(CODE_SIGN_PATH);
        if let Err(error) = symlink(&resolved_path, CODE_SIGN_PATH)
            && error.kind() != std::io::ErrorKind::AlreadyExists
        {
            self.logger.warn(
                "failed to create symlink",
                attributes([
                    ("target", json!(resolved_path.display().to_string())),
                    ("link", json!(CODE_SIGN_PATH)),
                    ("error", json!(error.to_string())),
                ]),
            );
            return;
        }

        let signing_key = std::env::var_os("HOME").map_or_else(
            || PathBuf::from(".ssh/commit_signing_key.pub"),
            |home| PathBuf::from(home).join(".ssh/commit_signing_key.pub"),
        );
        let git_configurations = [
            ("gpg.format", OsString::from("ssh")),
            ("gpg.ssh.program", OsString::from(CODE_SIGN_PATH)),
            ("commit.gpgsign", OsString::from("true")),
            ("user.signingkey", signing_key.into_os_string()),
        ];
        for (key, value) in git_configurations {
            let arguments = [
                OsString::from("config"),
                OsString::from("--global"),
                OsString::from(key),
                value,
            ];
            if let Err(error) =
                run_combined_command(cancellation, OsStr::new("git"), &arguments).await
            {
                self.logger.warn(
                    "Failed to set git signing key config",
                    attributes([
                        ("key", json!(key)),
                        ("error", json!(error.error)),
                        ("output", json!(String::from_utf8_lossy(&error.output))),
                    ]),
                );
            }
        }

        self.logger.info(
            "Configured git signing",
            attributes([
                ("executable", json!(resolved_path.display().to_string())),
                ("path", json!(CODE_SIGN_PATH)),
            ]),
        );
    }

    /// 将 environment 的 Claude 环境变量和非空 CWD 覆盖到最终执行配置。
    fn apply_environment_config(
        &self,
        environment: &dyn EnvironmentType,
        claude_config: &mut ClaudeConfig,
    ) {
        claude_config
            .environment
            .extend(environment.get_claude_environment_variables());
        let working_directory = environment.get_cwd();
        if working_directory.is_empty() {
            self.logger.info(
                "No working directory specified in environment config",
                LogAttributes::new(),
            );
            return;
        }
        claude_config.working_dir = working_directory.to_owned();
        self.logger.info(
            "Set working directory from environment config",
            attributes([("working_dir", json!(working_directory))]),
        );
    }

    /// 运行 `claude plugin marketplace add` 并返回完整 combined output 错误。
    async fn add_official_plugin_marketplace_async(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), ManagerError> {
        let mut claude_path = std::env::var("CLAUDE_DEFAULT_PATH").unwrap_or_default();
        if claude_path.is_empty() {
            claude_path = "claude".to_owned();
        }
        if let Some(configured_path) = self
            .claude_config
            .as_ref()
            .map(|configuration| configuration.claude_path.as_str())
            .filter(|path| !path.is_empty())
        {
            claude_path = configured_path.to_owned();
        }

        let arguments = [
            OsString::from("plugin"),
            OsString::from("marketplace"),
            OsString::from("add"),
            OsString::from(OFFICIAL_PLUGIN_MARKETPLACE),
        ];
        run_combined_command(cancellation, OsStr::new(&claude_path), &arguments)
            .await
            .map(|_| ())
            .map_err(|error| {
                ManagerError::message(format!(
                    "failed to add official plugin marketplace: {}, output: {}",
                    error.error,
                    String::from_utf8_lossy(&error.output)
                ))
            })
    }

    /// 创建可选 tunnel，令它与 Claude executor 并发运行，并解释取消为正常退出。
    async fn execute_claude_and_tunnel(
        &self,
        executor_cancellation: &CancellationToken,
        session_id: &str,
        auth_context: &AuthContext,
        executor: Arc<dyn Executor>,
        manager_started: Instant,
        using_lease_context: bool,
    ) -> Result<(), ManagerError> {
        let mut tunnel_client = None;
        if is_tunnel_enabled(self.create_tunnel_client_func.as_ref()) {
            if let Some(startup_context) = self
                .session_config
                .as_ref()
                .and_then(|session| session.startup_context.as_ref())
            {
                let factory = self
                    .create_tunnel_client_func
                    .as_ref()
                    .expect("is_tunnel_enabled requires a registered factory");
                match factory(
                    self,
                    &startup_context.api_base_url,
                    auth_context.get_session_ingress_token(),
                    Some(auth_context),
                ) {
                    Ok(client) => tunnel_client = Some(client),
                    Err(error) => self.logger.error(
                        "Failed to create tunnel client",
                        attributes([
                            ("session_id", json!(session_id)),
                            ("error", json!(error.to_string())),
                        ]),
                    ),
                }
            } else {
                self.logger.warn(
                    "Tunnel requested but startup context is unavailable",
                    attributes([("session_id", json!(session_id))]),
                );
            }
        } else if self.create_tunnel_client_func.is_some() {
            self.logger.info(
                "Tunnel client disabled",
                attributes([("session_id", json!(session_id))]),
            );
        } else {
            self.logger.info(
                "Tunnel client not registered",
                attributes([("session_id", json!(session_id))]),
            );
        }

        self.logger.info(
            "Starting Claude Code execution",
            attributes([
                ("session_id", json!(session_id)),
                ("session_mode", json!(self.session_mode.as_str())),
            ]),
        );
        let run_cancellation = executor_cancellation.child_token();
        let execute_token = run_cancellation.clone();
        let cancel_after_execute = run_cancellation.clone();
        let execute_future = async move {
            let result = executor.execute(&execute_token).await;
            cancel_after_execute.cancel();
            result
        };

        let execution_result = if let Some(client) = tunnel_client.as_ref() {
            let tunnel_run_client = Arc::clone(client);
            let tunnel_token = run_cancellation.clone();
            let tunnel_logger = self.logger.clone();
            let tunnel_session_id = session_id.to_owned();
            let tunnel_future = async move {
                match tunnel_run_client.connect_and_run(&tunnel_token).await {
                    Err(error) if tunnel_token.is_cancelled() => tunnel_logger.info(
                        "Tunnel client stopped after context cancellation",
                        attributes([
                            ("session_id", json!(&tunnel_session_id)),
                            ("error", json!(error.to_string())),
                        ]),
                    ),
                    Err(error) => tunnel_logger.error(
                        "Tunnel client failed",
                        attributes([
                            ("session_id", json!(&tunnel_session_id)),
                            ("error", json!(error.to_string())),
                        ]),
                    ),
                    Ok(()) => tunnel_logger.info(
                        "Tunnel client stopped",
                        attributes([("session_id", json!(&tunnel_session_id))]),
                    ),
                }
            };
            let (execution_result, ()) = tokio::join!(execute_future, tunnel_future);
            execution_result
        } else {
            execute_future.await
        };

        if let Some(client) = tunnel_client {
            let _ = client.close().await;
        }
        match execution_result {
            Ok(()) => {
                self.logger.info(
                    "Claude Code execution completed",
                    attributes([
                        ("duration_ms", json!(duration_milliseconds(manager_started))),
                        ("session_id", json!(session_id)),
                    ]),
                );
                Ok(())
            }
            Err(error) if using_lease_context && executor_cancellation.is_cancelled() => {
                self.logger.info(
                    "Claude Code execution stopped after lease manager context cancellation",
                    attributes([("error", json!(error.to_string()))]),
                );
                Ok(())
            }
            Err(_) if executor_cancellation.is_cancelled() => Ok(()),
            Err(error) if is_cancellation_error(&error.to_string()) => Ok(()),
            Err(error) => {
                self.logger.error(
                    "Environment manager execution failed",
                    attributes([("error", json!(error.to_string()))]),
                );
                // Go 的 `fmt.Errorf("execution failed: %w", err)` 只拼接字节；如果
                // Claude/npm CombinedOutput 含非法 UTF-8，提前调用 Display 会改变最终
                // stderr。日志上方仍使用逐坏字节替换视图，返回链则必须保留原字节。
                Err(ManagerError::prefixed(
                    b"execution failed: ",
                    error.as_bytes(),
                ))
            }
        }
    }

    /// 在 detached 五秒 context 内顺序停止 MCP servers 并销毁 executor。
    ///
    /// Go 的 `context.WithTimeout` 只会让 `executor.Destroy` 观察到 deadline，
    /// 不会在五秒后强制截断无 context 参数的 `server.Stop`。因此这里用独立
    /// timer 只取消 operation token，仍等待顺序清理返回，也不添加 Go 中不存在的
    /// timeout warning。
    async fn cleanup_mcp_and_executor(
        &self,
        mcp_servers: &[Arc<dyn MCPServer>],
        executor: &Arc<dyn Executor>,
    ) {
        let cleanup_cancellation = CancellationToken::new();
        let deadline_cancellation = cleanup_cancellation.clone();
        // deadline 在创建 cleanup context 的这一刻开始，不能推迟到 timer task
        // 第一次获得调度，否则前面某个 server.Stop 占用 runtime 时会延长 Go
        // `context.WithTimeout` 规定的五秒总预算。
        let cleanup_deadline = tokio::time::Instant::now() + CLEANUP_TIMEOUT;
        let deadline_task = tokio::spawn(async move {
            tokio::time::sleep_until(cleanup_deadline).await;
            deadline_cancellation.cancel();
        });
        for server in mcp_servers {
            self.logger.info(
                "Stopping MCP server",
                attributes([("server_name", json!(server.get_name()))]),
            );
            server.stop().await;
        }
        if let Err(error) = executor.destroy(&cleanup_cancellation).await {
            self.logger.warn(
                "Failed to destroy Claude executor",
                attributes([("error", json!(error.to_string()))]),
            );
        }
        // 清理在 deadline 前完成时，Go 的 cancel defer 会立即通知所有派生
        // context 并释放 timer；先 cancel 再 abort 复刻这个顺序，也避免一个
        // 无用任务继续存活五秒。
        cleanup_cancellation.cancel();
        deadline_task.abort();
    }

    /// 把无 PII 诊断转发给可选 adapter。
    pub(super) fn log_diag(&self, event: &str, fields: BTreeMap<String, Value>) {
        if let Some(observability) = &self.observability {
            observability.log_env_manager_no_pii(event, fields);
        }
    }
}

/// 判断 tunnel factory 已注册且 `TUNNEL_ENABLED` 精确为 `true` 或 `1`。
#[must_use]
pub fn is_tunnel_enabled(factory: Option<&CreateTunnelClientFunc>) -> bool {
    factory.is_some() && matches!(std::env::var("TUNNEL_ENABLED").as_deref(), Ok("true" | "1"))
}

/// 等待 SIGINT、SIGTERM 或父取消，并记录统一的 graceful-shutdown 消息。
///
/// Linux 上 Go `signal.Notify` 没有可恢复的错误返回；Tokio 的注册接口虽然返回
/// `Result`，但若该平台不变量被破坏，应直接暴露为内部 panic，而不是产生 Go
/// 中不存在的 warning 后改成只等待 cancellation。
pub async fn wait_for_shutdown_signal(cancellation: &CancellationToken, logger: &Logger) {
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("Linux SIGINT handler registration must succeed");
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("Linux SIGTERM handler registration must succeed");

    let signal = tokio::select! {
        _ = interrupt.recv() => "interrupt",
        _ = terminate.recv() => "terminated",
        () = cancellation.cancelled() => "context canceled",
    };
    logger.info(
        "Received signal, shutting down gracefully",
        attributes([("signal", json!(signal))]),
    );
}

/// 对 setup-only 会话的每个 repository source 依次执行 `claude --init-only`。
pub async fn run_init_only(
    cancellation: &CancellationToken,
    logger: &Logger,
    claude_config: &ClaudeConfig,
    session_config: Option<&Session>,
) -> Result<(), ManagerError> {
    let working_directory = if claude_config.working_dir.is_empty() {
        Path::new("/home/claude")
    } else {
        Path::new(&claude_config.working_dir)
    };
    let sources = session_config
        .and_then(|session| session.startup_context.as_ref())
        .map_or(&[][..], |context| context.sources.as_slice());
    let mut repository_count = 0usize;
    for source in sources {
        if !source.is_repository() {
            continue;
        }
        let repository_directory = source.get_directory(working_directory);
        if repository_directory.as_os_str().is_empty() {
            continue;
        }
        if let Err(error) = claude::run_init(
            cancellation,
            logger,
            Some(claude_config),
            &repository_directory,
        )
        .await
        {
            logger.error(
                "claude --init-only failed",
                attributes([
                    (
                        "repo_dir",
                        json!(repository_directory.display().to_string()),
                    ),
                    ("error", json!(error.to_string())),
                ]),
            );
            // run_init 的 CombinedOutput 属于 Go error string 的一部分；日志使用上方
            // 的逐坏字节替换视图，返回路径则复刻 `%w` 的原始字节拼接。
            return Err(ManagerError::prefixed(
                b"failed to run init script: ",
                error.as_bytes(),
            ));
        }
        logger.info(
            "claude --init-only completed successfully",
            attributes([(
                "repo_dir",
                json!(repository_directory.display().to_string()),
            )]),
        );
        repository_count += 1;
    }
    if repository_count == 0 {
        logger.info(
            "Session mode is setup-only, no repositories found for init hooks",
            LogAttributes::new(),
        );
    }
    Ok(())
}

/// 停止可选 lease；无 runtime 时保持 Go nil-check 的空操作。
async fn stop_lease(lease_runtime: Option<&ManagerLeaseRuntime>) {
    if let Some(runtime) = lease_runtime {
        runtime.handle.stop().await;
    }
}

/// 使用共享 stdout/stderr pipe 执行可取消命令，并只终止 direct child。
async fn run_combined_command(
    cancellation: &CancellationToken,
    program: &OsStr,
    arguments: &[OsString],
) -> Result<Vec<u8>, CommandFailure> {
    if cancellation.is_cancelled() {
        return Err(CommandFailure {
            error: "context canceled".to_owned(),
            output: Vec::new(),
        });
    }

    let (read_end, write_end) = pipe().map_err(|error| CommandFailure {
        error: error.to_string(),
        output: Vec::new(),
    })?;
    for descriptor in [&read_end, &write_end] {
        fcntl(descriptor, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC)).map_err(|error| {
            CommandFailure {
                error: error.to_string(),
                output: Vec::new(),
            }
        })?;
    }
    let stderr_end = write_end.try_clone().map_err(|error| CommandFailure {
        error: error.to_string(),
        output: Vec::new(),
    })?;

    let mut command = Command::new(program);
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::from(write_end))
        .stderr(Stdio::from(stderr_end))
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|error| CommandFailure {
        error: error.to_string(),
        output: Vec::new(),
    })?;
    drop(command);

    let mut reader = tokio::fs::File::from_std(fs::File::from(read_end));
    let reader_task = tokio::spawn(async move {
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await.map(|_| output)
    });
    let status = tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            Err("context canceled".to_owned())
        }
        result = child.wait() => result.map_err(|error| error.to_string()),
    };
    let output = reader_task
        .await
        .map_err(|error| CommandFailure {
            error: error.to_string(),
            output: Vec::new(),
        })?
        .map_err(|error| CommandFailure {
            error: error.to_string(),
            output: Vec::new(),
        })?;
    let status = status.map_err(|error| CommandFailure {
        error,
        output: output.clone(),
    })?;
    if status.success() {
        Ok(output)
    } else {
        Err(CommandFailure {
            error: exit_status_error(status),
            output,
        })
    }
}

/// 将 Unix exit status 格式化为 Go `exec.ExitError` 风格文本。
fn exit_status_error(status: ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit status {code}");
    }
    match status.signal() {
        Some(libc::SIGKILL) => "signal: killed".to_owned(),
        Some(libc::SIGTERM) => "signal: terminated".to_owned(),
        Some(signal) => format!("signal: {signal}"),
        None => "process exited unsuccessfully".to_owned(),
    }
}

/// 判断 Claude 错误文本是否保持了 Go `context.Canceled` 的可识别语义。
fn is_cancellation_error(error: &str) -> bool {
    error.contains("context canceled") || error.contains("operation canceled")
}

/// 把成对字段收集为 logger 和 diag 共用的稳定排序属性。
fn attributes<const N: usize>(entries: [(&str, Value); N]) -> LogAttributes {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

/// 将单调时钟耗时饱和转换为日志协议使用的有符号毫秒数。
fn duration_milliseconds(started: Instant) -> i64 {
    i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)
}
