//! Environment manager 的主启动、并行初始化、执行与清理状态机。
//!
//! 并发初始化使用 Tokio task、join handle 和共享 `CancellationToken`；所有外部命令取消仅终止
//! direct child，绝不创建或清理进程组。

use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs,
    os::unix::fs::symlink,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use serde_json::{Value, json};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::internal::{
    api::{LogCategory, LogLevel},
    auth::AuthContext,
    claude::{self, Executor, Outcomes},
    config::{self, ClaudeConfig, Session, SessionMode},
    envtype::EnvironmentType,
    mcp::MCPServer,
    process::{CaptureMode, CommandFailure, CommandOutput, run_command},
    session::{ActivityRecorder, EventPoster, NoopActivityRecorder, new_activity_recorder},
};

use super::{
    tunnel_register::default_create_tunnel_client_func,
    types::{
        ClaudeExecutorFactory, CreateTunnelClientFunc, EnvironmentLeaseAdapter, Manager,
        ManagerApiClient, ManagerError, ManagerLeaseRequest, ManagerLeaseRuntime,
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
/// 参数顺序可选配置使用 owned clone，
/// event poster 和跨任务依赖使用 `Arc`。BYOC lease 从动态 environment 自动发现；
/// 测试仍可在构造后显式覆盖 lease/o11y adapter。
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn new_manager_with_executor_and_claude_config(
    api_client: Arc<dyn ManagerApiClient>,
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
    /// 替换或注销默认 tunnel factory；`None` 表示未注册 factory。
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
    /// MCP 注册和环境初始化在同一并行阶段执行；两条路径都返回拥有所有权的结果，
    /// 不通过 join handle 后写裸指针。
    pub async fn run(&mut self, cancellation: &CancellationToken) -> Result<(), ManagerError> {
        let mut runtime_tool_calls = None;
        let result = self
            .run_session(cancellation, &mut runtime_tool_calls)
            .await;
        if let Some(runtime) = runtime_tool_calls {
            runtime.shutdown().await;
        }
        result
    }

    /// 执行一次会话启动流程，Brain 模式下启动工具调用领取循环。
    async fn run_session(
        &mut self,
        cancellation: &CancellationToken,
        runtime_tool_calls: &mut Option<super::runtime_tool_calls::RuntimeToolCalls>,
    ) -> Result<(), ManagerError> {
        let started = Instant::now();
        // 恢复结构保留该字段但 Run 不消费；显式读取避免把协议字段误判为死代码。
        let _ = self.process_start_time;
        let session_id = self
            .session_config
            .as_ref()
            .map_or_else(String::new, |session| session.session_id.clone());
        tracing::info!("session_id" = ?(&session_id), "{}", "Starting environment manager");

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
            tracing::info!("original_mode" = ?(self.session_mode.as_str()), "new_mode" = ?(SessionMode::ResumeCached.as_str()), "{}", "Defaulting BYOC environment to resume-cached session mode");
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
            tracing::info!("allowed_tools" = ?(&environment_response.allowed_tools), "{}", "Using allowed tools from environment config");
        }
        if !environment_response.disallowed_tools.is_empty()
            && claude_config.disallowed_tools.is_empty()
        {
            claude_config
                .disallowed_tools
                .clone_from(&environment_response.disallowed_tools);
        }
        // 非 `None` 在 MCP 后台任务 启动前已被原地更新；Rust clone 回字段保留可见性。
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
                None,
            )),
            None => Arc::new(NoopActivityRecorder),
        };

        self.configure_environment(cancellation, environment.as_mut(), &auth_context)
            .await;
        self.apply_environment_config(environment.as_ref(), &mut claude_config);
        // Source Git 会先于 Claude 运行；CA 与 relay 必须在任何 source I/O 之前就绪。
        let mut agent_proxy = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(()),
            result = claude::prepare_session_agent_proxy(
                &claude_config,
                &session_id,
                self.session_config.as_ref().and_then(|session| session.startup_context.as_ref()),
                &auth_context,
            ) => result.map_err(|error| ManagerError::message(format!(
                "failed to prepare environment network: {error}"
            )))?,
        };

        tracing::info!(
            "{}",
            "Starting parallel initialization: MCP servers, plugin marketplace, and environment setup"
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
            tracing::info!(
                "{}",
                "Skipping plugin marketplace installation because Claude Code is disabled"
            );
            None
        } else if matches!(
            std::env::var("SKIP_PLUGIN_MARKETPLACE").as_deref(),
            Ok("true")
        ) {
            tracing::info!(
                "{}",
                "Skipping plugin marketplace installation (SKIP_PLUGIN_MARKETPLACE=true)"
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

        let source_environment = agent_proxy
            .as_ref()
            .map_or_else(Vec::new, |proxy| proxy.environment().to_vec());
        let initialize = crate::internal::sources::GIT_ENVIRONMENT.scope(
            source_environment,
            environment.initialize(cancellation, Some(activity_recorder.as_ref())),
        );
        let environment_initialization = tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(Box::new(ManagerError::message(
                "environment initialization cancelled"
            )) as crate::internal::util::BoxError),
            failure = claude::wait_for_agent_proxy_failure(agent_proxy.as_mut()) => {
                Err(Box::new(ManagerError::message(failure)) as crate::internal::util::BoxError)
            }
            result = initialize => result,
        };

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
        tracing::info!("duration_ms" = ?(parallel_duration_ms), "mcp_success" = ?(mcp_error.is_none()), "plugin_success" = ?(plugin_marketplace_error.is_none()), "env_success" = ?(environment_initialization.is_ok()), "{}", "parallel_init_completed");
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
            tracing::warn!("error" = ?(error.to_string()), "{}", "Failed to register MCP servers (parallel)");
        }
        if let Some(error) = &plugin_marketplace_error {
            self.log_diag("plugin_marketplace_failed", BTreeMap::new());
            tracing::warn!("error" = ?(error.to_string()), "{}", "Plugin marketplace installation failed (parallel)");
        }

        if cancellation.is_cancelled() {
            return Ok(());
        }
        if skip_claude_code {
            *runtime_tool_calls = Some(
                super::runtime_tool_calls::RuntimeToolCalls::start(
                    &claude_config,
                    cancellation,
                    agent_proxy.as_ref(),
                    self.session_config
                        .as_ref()
                        .and_then(|session| session.startup_context.as_ref())
                        .map(|context| context.api_base_url.as_str())
                        .unwrap_or_default(),
                    auth_context.get_session_ingress_token(),
                )
                .await?,
            );
        }

        if skip_claude_code {
            tracing::info!(
                "{}",
                "ENVRUNNER_SKIP_CLAUDE_CODE mode: environment initialized, waiting for shutdown signal"
            );
            let result = tokio::select! {
                biased;
                () = wait_for_shutdown_signal(cancellation) => Ok(()),
                failure = claude::wait_for_agent_proxy_failure(agent_proxy.as_mut()) => {
                    Err(ManagerError::message(failure))
                }
            };
            if let Some(proxy) = agent_proxy.take() {
                proxy.shutdown().await;
            }
            return result;
        }

        let outcomes = match self.api_client.get_outcomes() {
            Some(outcomes) if !outcomes.is_empty() => {
                tracing::info!("outcome_count" = ?(outcomes.len()), "{}", "ClaudeCodeExecutor initialized with repository outcomes");
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
            tracing::info!("session_id" = ?(&session_id), "{}", "Using lease manager context for Claude Code execution");
        }
        let executor_cancellation = lease_runtime.as_ref().map_or_else(
            || cancellation.clone(),
            |runtime| runtime.cancellation.clone(),
        );

        let executor_create_started = Instant::now();
        let executor = match (self.executor_factory)(
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
                // 子进程捕获输出的原始字节，其他 UTF-8 错误仍走普通 Display。
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
        tracing::info!("executor_create_duration_ms" = ?(executor_create_duration_ms), "total_startup_duration_ms" = ?(total_startup_duration_ms), "{}", "Starting Claude Code");
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
            tracing::info!(
                "{}",
                "Session mode is setup-only, running init hooks before exit"
            );
            let result =
                run_init_only(cancellation, &claude_config, self.session_config.as_ref()).await;
            if result.is_ok() {
                tracing::info!(
                    "{}",
                    "Session mode is setup-only, skipping Claude Code execution"
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
                agent_proxy.take(),
            )
            .await
        };

        self.cleanup_mcp_and_executor(&mcp_batch.servers, &executor)
            .await;
        stop_lease(lease_runtime.as_ref()).await;
        if let Some(proxy) = agent_proxy.take() {
            proxy.shutdown().await;
        }
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
            tracing::info!("has_mcp_config" = ?(has_mcp_config), "{}", "Set startup context for environment");
        }
        environment.set_auth_context(Some(auth_context.clone()));
        tracing::info!("session_id" = ?(auth_context.get_session_id()), "{}", "Set auth context for environment");
        environment.set_session_mode(self.session_mode);
        tracing::info!("session_mode" = ?(self.session_mode.as_str()), "{}", "Set session mode for environment");

        if self
            .claude_config
            .as_ref()
            .is_some_and(|configuration| configuration.local_testing_mode)
        {
            tracing::info!(
                "{}",
                "Skipping git signing configuration in local testing mode"
            );
            return;
        }
        self.configure_git_signing(cancellation).await;
    }

    /// 创建 `/tmp/code-sign` 并尽力写入全局 Git 身份与 SSH 签名配置。
    ///
    /// 路径或 symlink 失败会提前降级；单项 `git config` 失败只 warning 并继续。取消
    /// 通过 [`run_combined_command`] 只 kill 当前 direct child。
    async fn configure_git_signing(&self, cancellation: &CancellationToken) {
        if matches!(std::env::var("SKIP_GIT_CONFIG").as_deref(), Ok("true")) {
            tracing::info!(
                "{}",
                "Skipping git signing configuration (SKIP_GIT_CONFIG=true)"
            );
            return;
        }

        let executable_path = match std::env::current_exe() {
            Ok(path) => path,
            Err(error) => {
                tracing::warn!("error" = ?(error.to_string()), "{}", "Failed to get executable path for git signing config");
                return;
            }
        };
        let resolved_path = match executable_path.canonicalize() {
            Ok(path) => path,
            Err(error) => {
                tracing::warn!("path" = ?(executable_path.display().to_string()), "error" = ?(error.to_string()), "{}", "failed to read symlink");
                return;
            }
        };

        let _ = fs::remove_file(CODE_SIGN_PATH);
        if let Err(error) = symlink(&resolved_path, CODE_SIGN_PATH)
            && error.kind() != std::io::ErrorKind::AlreadyExists
        {
            tracing::warn!("target" = ?(resolved_path.display().to_string()), "link" = ?(CODE_SIGN_PATH), "error" = ?(error.to_string()), "{}", "failed to create symlink");
            return;
        }

        let signing_key = std::env::var_os("HOME").map_or_else(
            || PathBuf::from(".ssh/commit_signing_key.pub"),
            |home| PathBuf::from(home).join(".ssh/commit_signing_key.pub"),
        );
        let git_configurations = [
            ("user.name", OsString::from("oma")),
            ("user.email", OsString::from("oma@superduck.ai")),
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
            let failure = match run_combined_command(cancellation, OsStr::new("git"), &arguments)
                .await
            {
                Ok(output) if output.status.success() => None,
                Ok(output) => Some((output.status.to_string(), output.output.expect_combined())),
                Err(error) => {
                    let message = error.to_string();
                    Some((message, error.output.expect_combined()))
                }
            };
            if let Some((error, output)) = failure {
                tracing::warn!(key, error, output = ?String::from_utf8_lossy(&output), "Failed to set git signing key config");
            }
        }

        tracing::info!("executable" = ?(resolved_path.display().to_string()), "path" = ?(CODE_SIGN_PATH), "{}", "Configured git signing");
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
        if let Some(startup) = self
            .session_config
            .as_ref()
            .and_then(|session| session.startup_context.as_ref())
        {
            claude_config
                .environment
                .extend(startup.environment_variables.clone());
        }
        let working_directory = environment.get_cwd();
        if working_directory.is_empty() {
            tracing::info!("{}", "No working directory specified in environment config");
            return;
        }
        claude_config.working_dir = working_directory.to_owned();
        tracing::info!("working_dir" = ?(working_directory), "{}", "Set working directory from environment config");
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
        let failure =
            match run_combined_command(cancellation, OsStr::new(&claude_path), &arguments).await {
                Ok(output) if output.status.success() => return Ok(()),
                Ok(output) => (output.status.to_string(), output.output.expect_combined()),
                Err(error) => {
                    let message = error.to_string();
                    (message, error.output.expect_combined())
                }
            };
        Err(ManagerError::message(format!(
            "failed to add official plugin marketplace: {}, output: {}",
            failure.0,
            String::from_utf8_lossy(&failure.1)
        )))
    }

    /// 创建可选 tunnel，令它与 Claude executor 并发运行，并解释取消为正常退出。
    #[allow(clippy::too_many_arguments)]
    async fn execute_claude_and_tunnel(
        &self,
        executor_cancellation: &CancellationToken,
        session_id: &str,
        auth_context: &AuthContext,
        executor: Arc<dyn Executor>,
        manager_started: Instant,
        using_lease_context: bool,
        agent_proxy: Option<claude::AgentProxyHandle>,
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
                    Err(error) => {
                        tracing::error!("session_id" = ?(session_id), "error" = ?(error.to_string()), "{}", "Failed to create tunnel client")
                    }
                }
            } else {
                tracing::warn!("session_id" = ?(session_id), "{}", "Tunnel requested but startup context is unavailable");
            }
        } else if self.create_tunnel_client_func.is_some() {
            tracing::info!("session_id" = ?(session_id), "{}", "Tunnel client disabled");
        } else {
            tracing::info!("session_id" = ?(session_id), "{}", "Tunnel client not registered");
        }

        tracing::info!("session_id" = ?(session_id), "session_mode" = ?(self.session_mode.as_str()), "{}", "Starting Claude Code execution");
        let run_cancellation = executor_cancellation.child_token();
        let execute_token = run_cancellation.clone();
        let cancel_after_execute = run_cancellation.clone();
        let execute_future = async move {
            let result = executor
                .execute_with_agent_proxy(&execute_token, agent_proxy)
                .await;
            let was_cancelled = execute_token.is_cancelled();
            cancel_after_execute.cancel();
            (result, was_cancelled)
        };

        let (execution_result, execution_was_cancelled) = if let Some(client) =
            tunnel_client.as_ref()
        {
            let tunnel_run_client = Arc::clone(client);
            let tunnel_token = run_cancellation.clone();
            let tunnel_session_id = session_id.to_owned();
            let tunnel_future = async move {
                match tunnel_run_client.connect_and_run(&tunnel_token).await {
                    Err(error) if tunnel_token.is_cancelled() => {
                        tracing::info!("session_id" = ?(&tunnel_session_id), "error" = ?(error.to_string()), "{}", "Tunnel client stopped after cancellation")
                    }
                    Err(error) => {
                        tracing::error!("session_id" = ?(&tunnel_session_id), "error" = ?(error.to_string()), "{}", "Tunnel client failed")
                    }
                    Ok(()) => {
                        tracing::info!("session_id" = ?(&tunnel_session_id), "{}", "Tunnel client stopped")
                    }
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
                tracing::info!("duration_ms" = ?(duration_milliseconds(manager_started)), "session_id" = ?(session_id), "{}", "Claude Code execution completed");
                Ok(())
            }
            Err(error) if using_lease_context && executor_cancellation.is_cancelled() => {
                tracing::info!("error" = ?(error.to_string()), "{}", "Claude Code execution stopped after lease manager cancellation");
                Ok(())
            }
            Err(_) if executor_cancellation.is_cancelled() => Ok(()),
            Err(_) if execution_was_cancelled => Ok(()),
            Err(error) => {
                tracing::error!("error" = ?(error.to_string()), "{}", "Environment manager execution failed");
                // 错误 source chain 只拼接字节；如果
                // Claude/npm 捕获输出含非法 UTF-8，提前调用 Display 会改变最终
                // stderr。日志上方仍使用逐坏字节替换视图，返回链则必须保留原字节。
                Err(ManagerError::prefixed(
                    b"execution failed: ",
                    error.as_bytes(),
                ))
            }
        }
    }

    /// 在独立的五秒取消窗口内顺序停止 MCP servers 并销毁 executor。
    ///
    /// 超时只会让 executor 的销毁过程观察到取消信号，不会在五秒后强制截断
    /// 不接受取消令牌的 server 停止过程。因此这里用独立 timer 取消 operation
    /// token，仍等待顺序清理返回，也不添加当前清理流程中不存在的
    /// timeout warning。
    async fn cleanup_mcp_and_executor(
        &self,
        mcp_servers: &[Arc<dyn MCPServer>],
        executor: &Arc<dyn Executor>,
    ) {
        let cleanup_cancellation = CancellationToken::new();
        let deadline_cancellation = cleanup_cancellation.clone();
        // deadline 在创建 cleanup context 的这一刻开始，不能推迟到 timer task
        // 第一次获得调度，否则前面某个 server stop 占用 runtime 时会延长五秒总预算。
        let cleanup_deadline = tokio::time::Instant::now() + CLEANUP_TIMEOUT;
        let deadline_task = tokio::spawn(async move {
            tokio::time::sleep_until(cleanup_deadline).await;
            deadline_cancellation.cancel();
        });
        for server in mcp_servers {
            tracing::info!("server_name" = ?(server.get_name()), "{}", "Stopping MCP server");
            server.stop().await;
        }
        if let Err(error) = executor.destroy(&cleanup_cancellation).await {
            tracing::warn!("error" = ?(error.to_string()), "{}", "Failed to destroy Claude executor");
        }
        // 清理在 deadline 前完成时，cancel 会立即通知所有派生 token；先 cancel 再
        // abort 保留这个顺序，也避免一个
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
/// Linux signal handler 注册被视为不可恢复的启动条件。Tokio 的接口虽然返回
/// `Result`，但若该平台不变量被破坏，应直接暴露为内部 panic，而不是记录 warning
/// 后改成只等待 cancellation。
pub async fn wait_for_shutdown_signal(cancellation: &CancellationToken) {
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("Linux SIGINT handler registration must succeed");
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("Linux SIGTERM handler registration must succeed");

    let signal = tokio::select! {
        _ = interrupt.recv() => "interrupt",
        _ = terminate.recv() => "terminated",
        () = cancellation.cancelled() => "operation cancelled",
    };
    tracing::info!("signal" = ?(signal), "{}", "Received signal, shutting down gracefully");
}

/// 对 setup-only 会话的每个 repository source 依次执行 `claude --init-only`。
pub async fn run_init_only(
    cancellation: &CancellationToken,
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
        if let Err(error) =
            claude::run_init(cancellation, Some(claude_config), &repository_directory).await
        {
            tracing::error!("repo_dir" = ?(repository_directory.display().to_string()), "error" = ?(error.to_string()), "{}", "claude --init-only failed");
            // run_init 的捕获输出属于错误字节的一部分；日志使用上方
            // 的逐坏字节替换视图，返回路径则实现`source chain` 的原始字节拼接。
            return Err(ManagerError::prefixed(
                b"failed to run init script: ",
                error.as_bytes(),
            ));
        }
        tracing::info!("repo_dir" = ?(repository_directory.display().to_string()), "{}", "claude --init-only completed successfully");
        repository_count += 1;
    }
    if repository_count == 0 {
        tracing::info!(
            "{}",
            "Session mode is setup-only, no repositories found for init hooks"
        );
    }
    Ok(())
}

/// 停止可选 lease；无 runtime 时保持 `None` 检查 的空操作。
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
) -> Result<CommandOutput, CommandFailure> {
    let mut command = Command::new(program);
    command.args(arguments);
    run_command(command, cancellation, None, None, CaptureMode::Combined).await
}

/// 把成对字段收集为诊断事件使用的稳定排序属性。
fn attributes<const N: usize>(entries: [(&str, Value); N]) -> BTreeMap<String, Value> {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

/// 将单调时钟耗时饱和转换为日志协议使用的有符号毫秒数。
fn duration_milliseconds(started: Instant) -> i64 {
    i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)
}
