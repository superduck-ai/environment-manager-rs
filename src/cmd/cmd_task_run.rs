//! `task-run` 子命令的参数模型、stdin 协议和完整会话运行时。
//!
//! 本文件迁移 Go `cmd/cmd_task_run.go`：Clap 只解析参数，运行层仍按 Go 顺序完成
//! session flock、无条件 stdin 读取、work ACK、后端选择、Claude 安装、诊断与 OTel、
//! environment manager 以及信号关闭。所有外部命令取消只终止 direct child。

use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Display, Formatter},
    future::Future,
    io::Read as _,
    path::PathBuf,
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use async_trait::async_trait;
use clap::builder::ValueParser;
use clap::parser::ValueSource;
use clap::{Arg, ArgAction, ArgMatches, Command, value_parser};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::internal::{
    api::{
        ApiError, CCRBackend, ClientOptions, GetEnvironmentConfigResponse, HttpClient, NoopBackend,
        SessionBackend, SessionContext as ApiSessionContext, SessionEvent, SessionIngressBackend,
        SessionsClient, WorkClient,
    },
    auth::AuthContext,
    claude::{self, ClaudeObservability, Executor, Outcomes},
    config::{self, SessionMode as ConfigSessionMode},
    envtype,
    logger::{LogAttributes, Logger, create_logger_with_file_output},
    manager::{
        ClaudeExecutorFactory, Manager, ManagerApiClient, ManagerError, ManagerObservability,
        new_manager_with_executor_and_claude_config,
    },
    o11y::{self, O11yContext, O11yService as _, O11yServiceImpl},
    process::ProcessExitError,
    session::{ActivityRecorder, EventPoster, new_activity_recorder},
    util::{self, BoxError},
};

use super::error::{CliError, RuntimeError};
use super::utils::{
    LogLevel, bool_value, ignored_positionals, logger_level, mark_required_flags, parse_log_level,
    switch_arg,
};
use super::zz_recovered_types::{StdinConfigClient, TaskContext};

const LONG_ABOUT: &str = r#"Connects directly to the API to execute an existing session id.

Provide a JSON object via stdin with the following structure:
{
  "startup_context": {
    "sources": [...],
    "cwd": "..."
  },
  "environment": {
    "environment_type": "...",
    "version": "...",
    ...
  },
  "auth": [
    {
      "type": "github_app",
      "url": "github.com",
      "token": "ghs_..."
    }
  ]
}"#;

// 该资产由固定 Go 1.25.7 ARM64 oracle 的真实 CLI transcript 生成；文件只包含 panic
// 起始处到 EOF，崩溃前的 logger 行仍由 Rust 正常执行路径产生。只有独立运行中
// 确实变化的 receiver、heap/argv 与 working-directory 字符串 data 指针槽会固定为
// 0x0；signal addr/PC、静态指针、长度与 frame offset 仍保留 oracle 原值并逐字节比较。
const GO_V0_MISSING_STARTUP_CONTEXT_PANIC: &[u8] =
    include_bytes!("cobra_assets/v0_missing_startup_panic.txt");

/// `build_task_run_backend` 的后端选择结果。
///
/// 普通调用只使用 `backend` trait object；code-session 还保留具体 CCR 实例，供注册后
/// 读取 worker epoch 并注入 Claude 子进程环境。
pub(super) struct TaskRunBackend {
    pub(super) backend: Arc<dyn SessionBackend>,
    pub(super) ccr_backend: Option<Arc<CCRBackend>>,
}

/// 将完整 session backend 收窄为活动记录器所需的事件发布接口。
struct BackendEventPoster {
    backend: Arc<dyn SessionBackend>,
}

#[async_trait]
impl EventPoster for BackendEventPoster {
    /// 原样转发 session event，保持 backend 自身的取消、重试和错误文本。
    async fn post_event(
        &self,
        cancellation: &CancellationToken,
        event: &SessionEvent,
    ) -> Result<(), ApiError> {
        self.backend.post_event(cancellation, event).await
    }
}

/// V1 parser 使用的真实 `/v2/sessions/{id}` provider。
#[derive(Clone)]
struct ApiSessionContextProvider {
    cancellation: CancellationToken,
    logger: Logger,
}

impl crate::internal::input::SessionContextProvider for ApiSessionContextProvider {
    /// 为每份 work secret 使用其中的 base URL 与 ingress token 获取 session context。
    ///
    /// token 只进入 Authorization header；返回 DTO 会保留 RawMessage 字段的
    /// missing/null 差别，供 V1 parser 复刻 Go 的 MCP 指针和 outcomes 日志语义。
    fn get_session_context<'a>(
        &'a self,
        request: crate::internal::input::SessionContextRequest<'a>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<crate::internal::input::SessionContext, String>> + Send + 'a,
        >,
    > {
        Box::pin(async move {
            let http_client =
                HttpClient::new(request.api_base_url, Some(self.logger.clone()), None)
                    .map_err(|error| error.to_string())?;
            let client = SessionsClient::new(
                http_client,
                request.session_ingress_token,
                Some(self.logger.clone()),
            );
            let context = client
                .get_session_context(&self.cancellation, request.session_id)
                .await
                .map_err(|error| error.to_string())?;
            Ok(input_session_context(context))
        })
    }
}

/// 把 API 包的 session context 转成 input parser 使用的等形 DTO。
fn input_session_context(context: ApiSessionContext) -> crate::internal::input::SessionContext {
    crate::internal::input::SessionContext {
        cwd: context.cwd,
        outcomes: context.outcomes,
        custom_system_prompt: context.custom_system_prompt,
        append_system_prompt: context.append_system_prompt,
        model: context.model,
        mcp_config: context.mcp_config,
    }
}

/// 同时承载 Claude、manager 所需诊断事件和 OTel 指标的显式上下文。
#[derive(Clone, Default)]
pub(super) struct TaskRunObservability {
    diag: Option<o11y::diag::DiagContext>,
    metrics: Option<O11yContext>,
}

impl TaskRunObservability {
    /// 向已启用的诊断服务写入一条不含 PII 的事件；未启用时保持 Go no-op。
    fn log_diag(&self, event: &str, fields: Option<BTreeMap<String, Value>>) {
        o11y::diag::log_env_manager_no_pii(self.diag.as_ref(), event, fields);
    }
}

impl ClaudeObservability for TaskRunObservability {
    /// 返回 Claude Code 写入 JSONL 的临时文件路径。
    fn claude_code_diagnostics_file_path(&self) -> Option<String> {
        let path = o11y::diag::get_claude_code_diag_file_path(self.diag.as_ref());
        (!path.is_empty()).then_some(path)
    }

    /// 增加 Claude Code 启动 counter。
    fn increment_claude_code_start(&self) {
        if let Some(context) = &self.metrics {
            o11y::increment(context, Some(&o11y::CLAUDE_CODE_START_COUNTER), &[]);
        }
    }

    /// 写入 `claude_code_start` 诊断事件。
    fn log_claude_code_start(&self) {
        self.log_diag("claude_code_start", None);
    }

    /// 写入 Claude 退出状态和毫秒耗时，字段名保持 Go lower_snake_case。
    fn log_claude_code_end(&self, is_error: bool, exit_code: i32, duration_ms: i64) {
        self.log_diag(
            "claude_code_end",
            Some(
                [
                    ("duration_ms".to_owned(), json!(duration_ms)),
                    ("exit_code".to_owned(), json!(exit_code)),
                    ("is_error".to_owned(), json!(is_error)),
                ]
                .into_iter()
                .collect(),
            ),
        );
    }

    /// 增加 Claude 结束 counter，并在可用时保留真实子进程退出码。
    fn increment_claude_code_end(&self, is_error: bool, exit_code: Option<i32>) {
        let Some(context) = &self.metrics else {
            return;
        };
        if !is_error {
            o11y::increment_claude_code_end(context, None);
            return;
        }
        let error: Box<dyn Error + Send + Sync> = exit_code.map_or_else(
            || Box::new(std::io::Error::other("Claude Code execution failed")) as _,
            |code| Box::new(ProcessExitError::ExitCode(code)) as _,
        );
        o11y::increment_claude_code_end(context, Some(error.as_ref()));
    }

    /// 写入 `claude_init_success` 或 `claude_init_failed` 诊断事件。
    fn log_claude_init(&self, succeeded: bool, exit_code: Option<i32>, duration_ms: i64) {
        let mut fields = BTreeMap::from([("duration_ms".to_owned(), json!(duration_ms))]);
        if let Some(exit_code) = exit_code {
            fields.insert("exit_code".to_owned(), json!(exit_code));
        }
        self.log_diag(
            if succeeded {
                "claude_init_success"
            } else {
                "claude_init_failed"
            },
            Some(fields),
        );
    }

    /// 写入存在旧版本兜底时的非致命 Claude 更新失败事件。
    fn log_claude_update_failed_nonfatal(&self, operation: &str) {
        self.log_diag(
            "claude_code_update_failed_nonfatal",
            Some(BTreeMap::from([("operation".to_owned(), json!(operation))])),
        );
    }
}

impl ManagerObservability for TaskRunObservability {
    /// 将 manager 的稳定事件名和字段转发到当前诊断上下文。
    fn log_env_manager_no_pii(&self, event: &str, fields: BTreeMap<String, Value>) {
        self.log_diag(event, Some(fields));
    }
}

/// stdin 负载所采用的线协议版本。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputFormat {
    /// 直接包含 startup_context/environment/auth 的旧格式。
    V0,
    /// 包含 work response 与 packed secret 的 orchestrator 格式。
    V1,
}

impl Display for InputFormat {
    /// 输出 CLI 使用的稳定版本名。
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::V0 => "v0",
            Self::V1 => "v1",
        })
    }
}

impl FromStr for InputFormat {
    type Err = String;

    /// 严格解析 Go stdin loader 支持的两个格式。
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "v0" => Ok(Self::V0),
            "v1" => Ok(Self::V1),
            invalid => Err(format!(
                "unsupported input format: {invalid:?} (expected 'v0' or 'v1')"
            )),
        }
    }
}

/// 会话初始化/恢复策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionMode {
    /// 新会话，执行完整初始化。
    New,
    /// 跳过 clone 与 setup，直接恢复。
    Resume,
    /// 使用自托管缓存进行恢复。
    ResumeCached,
    /// 只执行 setup，随后退出。
    SetupOnly,
}

impl Display for SessionMode {
    /// 输出配置层和 CLI 共用的 kebab-case 名称。
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::New => "new",
            Self::Resume => "resume",
            Self::ResumeCached => "resume-cached",
            Self::SetupOnly => "setup-only",
        })
    }
}

impl FromStr for SessionMode {
    type Err = String;

    /// 按 Go `config.ParseSessionMode` 的值域和错误文本解析。
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "new" | "" => Ok(Self::New),
            "resume" => Ok(Self::Resume),
            "resume-cached" => Ok(Self::ResumeCached),
            "setup-only" => Ok(Self::SetupOnly),
            invalid => Err(format!(
                "invalid session mode: {invalid:?} (must be new, setup-only, resume or resume-cached)"
            )),
        }
    }
}

/// 已验证并完成兼容默认值解析的 task-run 参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRunArgs {
    /// 必填会话 ID。
    pub session_id: String,
    /// 日志级别。
    pub log_level: LogLevel,
    /// 已废弃 `--stdin` 的值；stdin 无论如何都会被读取。
    pub stdin: bool,
    /// 用户是否显式传入了 `--stdin`，用于运行时输出弃用警告。
    pub stdin_was_set: bool,
    /// stdin 线协议格式。
    pub input_format: InputFormat,
    /// 会话初始化/恢复模式。
    pub session_mode: SessionMode,
    /// 用户是否显式选择 session mode，manager 仍依赖该区别。
    pub session_mode_was_set: bool,
    /// 可选 Claude 工具白名单原始逗号分隔值。
    pub allowed_tools: Option<String>,
    /// 是否启用本地测试模式。
    pub local_testing: bool,
    /// 已废弃的升级布尔值。
    pub upgrade_claude_code: bool,
    /// 用户是否显式设置了升级布尔值，用于兼容版本推导和冲突检测。
    pub upgrade_claude_code_was_set: bool,
    /// 解析兼容 flag 后得到的最终 Claude Agent 目标版本。
    pub claude_agent_version: String,
    /// 是否设置 `CLAUDE_CODE_DEBUG=1`。
    pub debug: bool,
    /// 是否把 Claude 日志输出到控制台；local-testing 会强制打开。
    pub verbose_claude_logs: bool,
    /// 是否在执行结束或失败时打印 Claude Code 日志。
    pub print_code_logs: bool,
    /// 自托管 API 调用使用的可选环境 ID。
    pub environment_id: Option<String>,
    /// 自托管 API 调用使用的可选组织 ID。
    pub organization_id: Option<String>,
    /// 追加到系统提示词的本地内容。
    pub local_append_system_prompt: Option<String>,
    /// stdin context 未指定 cwd 时使用的工作目录。
    pub working_directory: PathBuf,
    /// 是否跳过 git 配置；local-testing 会强制打开。
    pub skip_git_config: bool,
    /// 可选 Claude CLI 或 wrapper 路径。
    pub claude_path: Option<PathBuf>,
}

/// 对应 Go `AddTaskRunCommand`，构造 `task-run` 子命令及全部兼容 flag。
pub(crate) fn add_task_run_command() -> Command {
    let command = Command::new("task-run")
        .about("Handles execution of a provided session")
        .long_about(LONG_ABOUT)
        .arg(Arg::new("session").long("session").help("ID of the session to manage (required)"))
        .arg(Arg::new("log-level").long("log-level").default_value("info").help("Log level (debug, info, warn, error)").value_parser(ValueParser::new(parse_log_level)))
        .arg(switch_arg("stdin", "Deprecated: stdin is now always used. This flag is ignored."))
        .arg(Arg::new("input-format").long("input-format").default_value("v0").help("Input format for stdin data: 'v0' (startup_context/environment/auth) or 'v1' (work response with packed secret)").value_parser(value_parser!(InputFormat)))
        .arg(Arg::new("session-mode").long("session-mode").default_value("new").help("Session mode: 'new' (default for Cloud), 'resume' (skip git clone and setup scripts), 'resume-cached' (default for self-hosted), 'setup-only' (exit after setup)").value_parser(value_parser!(SessionMode)))
        .arg(Arg::new("allowed-tools").long("allowed-tools").help("Comma-separated list of allowed tools for Claude (e.g., 'Bash,Edit,Write,MultiEdit,Agent,Glob,Grep,LS,View,Search,NotebookEdit,NotebookRead,TodoRead,TodoWrite')"))
        .arg(switch_arg("local-testing", "Disable Claude WebSocket connections and git configuration (run in standalone mode for local testing)"))
        .arg(upgrade_bool_arg())
        .arg(Arg::new("claude-agent-version").long("claude-agent-version").help("Target Claude Agent version (latest, current, stable, or specific version like 2.0.20), located at `claude --version`. Use 'current' to skip installation and updates. Defaults to 'latest' if not specified."))
        .arg(switch_arg("debug", "Enable debug mode when executing the Claude Agent."))
        .arg(switch_arg("verbose-claude-logs", "Enable verbose logging of Claude Agent output to console (automatically enabled with --local-testing)"))
        .arg(switch_arg("print-code-logs", "Print Claude Code logs to console when execution completes or fails"))
        .arg(Arg::new("environment-id").long("environment-id").help("Environment ID for API calls (required for self-hosted)"))
        .arg(Arg::new("organization-id").long("organization-id").help("Organization ID for API calls (required for self-hosted)"))
        .arg(Arg::new("local-append-system-prompt").long("local-append-system-prompt").help("Additional system prompt content to append locally (useful for self-hosted environments to inject custom instructions)"))
        .arg(Arg::new("working-directory").long("working-directory").default_value_os(get_default_working_directory().into_os_string()).help("Default working directory for the session (used when session context doesn't specify cwd)").value_parser(value_parser!(PathBuf)))
        .arg(switch_arg("skip-git-config", "Skip git configuration setup (use container's existing .gitconfig)"))
        .arg(Arg::new("claude-path").long("claude-path").help("Path to the Claude CLI executable or a wrapper binary. When specified, this executable runs instead of the default 'claude' command. Wrapper binaries must forward all arguments and connect stdin/stdout directly to Claude Code.").value_parser(value_parser!(PathBuf)))
        .arg(ignored_positionals());

    // 保留 Go 的构造顺序：所有 flag 注册完成后，再由共享 helper 集中添加必填约束。
    mark_required_flags(command, &["session"])
}

/// 将 Clap matches 转换为 task-run 参数，并执行旧 RunE 中的纯参数派生逻辑。
pub(crate) fn from_matches(matches: &ArgMatches) -> Result<TaskRunArgs, CliError> {
    let upgrade_was_set = explicitly_set(matches, "upgrade-claude-code");
    let upgrade = *matches
        .get_one::<bool>("upgrade-claude-code")
        .expect("defaulted upgrade flag");
    let requested_version = optional_string(matches, "claude-agent-version");

    if upgrade_was_set && requested_version.is_some() {
        let message = "--upgrade-claude-code and --claude-agent-version are mutually exclusive; use --claude-agent-version=current to skip installation";
        return Err(CliError::validation_rendered(
            super::cobra_compat::render_runtime_error(super::CobraCommand::TaskRun, message),
        ));
    }
    let claude_agent_version = requested_version.unwrap_or_else(|| {
        if upgrade_was_set && !upgrade {
            "current".to_owned()
        } else {
            "latest".to_owned()
        }
    });

    let local_testing = bool_value(matches, "local-testing");
    let verbose_claude_logs = local_testing || bool_value(matches, "verbose-claude-logs");
    let skip_git_config = local_testing || bool_value(matches, "skip-git-config");

    Ok(TaskRunArgs {
        session_id: matches
            .get_one::<String>("session")
            .expect("required session")
            .clone(),
        log_level: *matches
            .get_one::<LogLevel>("log-level")
            .expect("defaulted log level"),
        stdin: bool_value(matches, "stdin"),
        stdin_was_set: explicitly_set(matches, "stdin"),
        input_format: *matches
            .get_one::<InputFormat>("input-format")
            .expect("defaulted input format"),
        session_mode: *matches
            .get_one::<SessionMode>("session-mode")
            .expect("defaulted session mode"),
        session_mode_was_set: explicitly_set(matches, "session-mode"),
        allowed_tools: optional_string(matches, "allowed-tools"),
        local_testing,
        upgrade_claude_code: upgrade,
        upgrade_claude_code_was_set: upgrade_was_set,
        claude_agent_version,
        debug: bool_value(matches, "debug"),
        verbose_claude_logs,
        print_code_logs: bool_value(matches, "print-code-logs"),
        environment_id: optional_string(matches, "environment-id"),
        organization_id: optional_string(matches, "organization-id"),
        local_append_system_prompt: optional_string(matches, "local-append-system-prompt"),
        working_directory: matches
            .get_one::<PathBuf>("working-directory")
            .expect("defaulted working directory")
            .clone(),
        skip_git_config,
        claude_path: matches.get_one::<PathBuf>("claude-path").cloned(),
    })
}

/// 对应 Go `getDefaultWorkingDirectory`，HOME 不可用时回退 `/root`。
pub(crate) fn get_default_working_directory() -> PathBuf {
    std::env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/root"))
}

/// 构造默认值为 true、同时接受 `--flag=false` 的弃用升级 flag。
fn upgrade_bool_arg() -> Arg {
    Arg::new("upgrade-claude-code")
        .long("upgrade-claude-code")
        .help("Deprecated: use --claude-agent-version instead. When true (default), upgrades Claude Agent to latest version.")
        .action(ArgAction::Set)
        .num_args(0..=1)
        .require_equals(true)
        .default_missing_value("true")
        .default_value("true")
        .overrides_with("upgrade-claude-code")
        .value_parser(value_parser!(bool))
}

/// 判断某个参数值是否来自命令行，而不是 Clap 默认值。
fn explicitly_set(matches: &ArgMatches, id: &str) -> bool {
    matches.value_source(id) == Some(ValueSource::CommandLine)
}

/// 将 Cobra 的空字符串约定转换为 Rust 的 `Option`。
fn optional_string(matches: &ArgMatches, id: &str) -> Option<String> {
    matches
        .get_one::<String>(id)
        .filter(|value| !value.is_empty())
        .cloned()
}

#[async_trait]
impl ManagerApiClient for StdinConfigClient {
    /// 返回 stdin 已提供的环境配置，不发起第二次 API 请求。
    async fn get_environment_for_session(
        &self,
        _cancellation: &CancellationToken,
        session_id: &str,
    ) -> Result<GetEnvironmentConfigResponse, ApiError> {
        let configuration = self
            .config
            .as_ref()
            .expect("stdin environment configuration is required");
        self.logger.info(
            "Using stdin-provided environment configuration",
            [
                (
                    "allowed_tools".to_owned(),
                    json!(&configuration.allowed_tools),
                ),
                (
                    "environment_type".to_owned(),
                    json!(&configuration.environment_type),
                ),
                (
                    "has_allowed_tools".to_owned(),
                    json!(!configuration.allowed_tools.is_empty()),
                ),
                ("session_id".to_owned(), json!(session_id)),
            ]
            .into_iter()
            .collect(),
        );
        Ok(configuration.clone())
    }

    /// 返回解析后的认证上下文副本。
    fn get_auth_context(&self) -> Option<AuthContext> {
        self.auth_context.clone()
    }

    /// 返回已经完成单分支校验的 outcomes 副本。
    fn get_outcomes(&self) -> Option<Outcomes> {
        self.outcomes.clone()
    }
}

/// 对应 Go `loadContextFromStdin`，无条件读取 stdin 到 EOF 后按 v0/v1 解析。
///
/// # 副作用
/// 阻塞读取进程 stdin。读取放入 blocking pool，避免管道长时间未关闭时占用 Tokio
/// worker；解析中的 V1 session-context 请求仍在异步运行时执行。
async fn load_context_from_stdin(
    cancellation: &CancellationToken,
    logger: &Logger,
    session_id: &str,
    input_format: InputFormat,
    working_directory: &std::path::Path,
) -> Result<TaskContext, RuntimeError> {
    let started = Instant::now();
    logger.info(
        "Starting to read and parse stdin",
        log_attributes([("input_format", json!(input_format.to_string()))]),
    );
    let data = tokio::task::spawn_blocking(|| {
        let mut data = Vec::new();
        std::io::stdin().lock().read_to_end(&mut data).map(|_| data)
    })
    .await
    .map_err(|error| RuntimeError::message(format!("failed to read from stdin: {error}")))?
    .map_err(|error| RuntimeError::message(format!("failed to read from stdin: {error}")))?;
    logger.info(
        "Read stdin data",
        [
            ("size_bytes".to_owned(), json!(data.len())),
            (
                "read_duration_ms".to_owned(),
                json!(duration_milliseconds(started)),
            ),
        ]
        .into_iter()
        .collect(),
    );

    let context = load_context_from_bytes(
        cancellation,
        logger,
        session_id,
        input_format,
        working_directory,
        &data,
    )
    .await?;
    logger.info(
        "Completed parsing stdin context",
        [
            ("input_format".to_owned(), json!(input_format.to_string())),
            (
                "total_parse_duration_ms".to_owned(),
                json!(duration_milliseconds(started)),
            ),
        ]
        .into_iter()
        .collect(),
    );
    Ok(context)
}

/// 解析调用方提供的完整 stdin 字节，供运行时和无全局 stdin 的兼容测试共用。
pub(super) async fn load_context_from_bytes(
    cancellation: &CancellationToken,
    logger: &Logger,
    session_id: &str,
    input_format: InputFormat,
    working_directory: &std::path::Path,
    data: &[u8],
) -> Result<TaskContext, RuntimeError> {
    let parsed = match input_format {
        InputFormat::V0 => crate::internal::input::V0Parser {
            session_id: session_id.to_owned(),
        }
        .parse_with_logger_for_cli(data, logger)
        .map_err(|error| match error {
            crate::internal::input::V0ParseError::StartupContextRequired => {
                v0_missing_startup_context_panic_error()
            }
            error => RuntimeError::message(format!("failed to parse stdin: {error}")),
        })?,
        InputFormat::V1 => {
            let parser = crate::internal::input::V1Parser::new(
                working_directory.to_string_lossy().into_owned(),
                ApiSessionContextProvider {
                    cancellation: cancellation.clone(),
                    logger: logger.clone(),
                },
            );
            parser
                .parse_with_logger(data, logger)
                .await
                .map_err(|error| RuntimeError::message(format!("failed to parse stdin: {error}")))?
        }
    };
    Ok(task_context_from_parsed(parsed))
}

/// 构造 V0 缺失 startup context 时的 Go runtime 崩溃结果。
///
/// 对应 Go：`internal/input.(*V0Parser).Parse` 对 nil `startupContext` 的字段解引用。
/// transcript 不经过 Cobra，也不添加 `failed to parse/load` 上下文；这些包装代码在
/// Go 进程中尚未获得控制权。退出码由 [`RuntimeError`] 固定为 2。
pub(super) fn v0_missing_startup_context_panic_error() -> RuntimeError {
    RuntimeError::go_runtime_panic(GO_V0_MISSING_STARTUP_CONTEXT_PANIC.to_vec())
}

/// 把 input 层的解析结果转换成 Go `TaskContext` 对应类型。
fn task_context_from_parsed(parsed: crate::internal::input::ParsedContext) -> TaskContext {
    let environment = parsed
        .environment
        .map(|environment| GetEnvironmentConfigResponse {
            environment_type: environment.environment_type,
            config: environment.config,
            allowed_tools: environment.allowed_tools,
            disallowed_tools: environment.disallowed_tools,
        });
    let mut outcomes = Outcomes::new();
    for (repository, branches) in parsed.outcomes.as_map() {
        outcomes.add(repository.clone(), branches.clone());
    }
    TaskContext {
        startup_context: Some(parsed.startup_context),
        environment,
        auth_context: Some(parsed.auth_context),
        outcomes: Some(outcomes),
        session_id: parsed.session_id,
        work_id: parsed.work_id,
        environment_id: parsed.environment_id,
        is_healthcheck: parsed.is_healthcheck,
    }
}

/// 对应 Go `validateAPIBaseURL`，拒绝空值、非法 URL 和缺少 host 的相对 URL。
pub(super) fn validate_api_base_url(api_base_url: &str) -> Result<(), String> {
    if api_base_url.is_empty() {
        return Err("empty API base URL".to_owned());
    }
    match url::Url::parse(api_base_url) {
        Ok(parsed) if parsed.host_str().is_some() => Ok(()),
        Ok(_) | Err(url::ParseError::RelativeUrlWithoutBase) => {
            Err(format!("API base URL has no host: {api_base_url}"))
        }
        Err(error) => Err(format!("invalid API base URL: {error}")),
    }
}

/// 对应 Go `acknowledgeWorkIfNeeded`，仅对完整 v1 work 发送一次 30 秒 ACK。
pub(super) async fn acknowledge_work_if_needed(
    cancellation: &CancellationToken,
    logger: &Logger,
    task_context: &TaskContext,
    work_id: &str,
    environment_id: &str,
    skip_ack: bool,
) -> Result<(), String> {
    if work_id.is_empty() || environment_id.is_empty() || skip_ack {
        return Ok(());
    }
    let auth_context = task_context
        .auth_context
        .as_ref()
        .ok_or_else(|| format!("cannot ACK work {work_id}: missing auth context"))?;
    let startup_context = task_context
        .startup_context
        .as_ref()
        .ok_or_else(|| format!("cannot ACK work {work_id}: missing startup context"))?;
    validate_api_base_url(&startup_context.api_base_url)
        .map_err(|error| format!("cannot ACK work {work_id}: {error}"))?;
    let token = auth_context.get_session_ingress_token();
    if token.is_empty() {
        return Err(format!(
            "cannot ACK work {work_id}: empty session ingress token"
        ));
    }

    logger.info(
        "Acknowledging work item",
        [
            ("environment_id".to_owned(), json!(environment_id)),
            (
                "is_healthcheck".to_owned(),
                json!(task_context.is_healthcheck),
            ),
            ("work_id".to_owned(), json!(work_id)),
        ]
        .into_iter()
        .collect(),
    );
    let http_client = HttpClient::new(&startup_context.api_base_url, Some(logger.clone()), None)
        .map_err(|error| format!("failed to acknowledge work: {error}"))?;
    let client = WorkClient::new(http_client, token, Some(logger.clone()));
    let ack_cancellation = cancellation.child_token();
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        client.acknowledge_work(&ack_cancellation, environment_id, work_id),
    )
    .await;
    ack_cancellation.cancel();
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Err(format!("failed to acknowledge work: {error}")),
        Err(_) => {
            return Err("failed to acknowledge work: context deadline exceeded".to_owned());
        }
    }
    logger.info(
        "Work item acknowledged successfully",
        log_attributes([("work_id", json!(work_id))]),
    );
    Ok(())
}

/// 对应 Go `buildTaskRunBackend`，选择 Noop、SessionIngress 或 CCR v2。
pub(super) async fn build_task_run_backend(
    logger: &Logger,
    task_context: &TaskContext,
    session_id: &str,
    session_mode: ConfigSessionMode,
    skip_claude_code: bool,
) -> Result<TaskRunBackend, String> {
    if skip_claude_code {
        logger.info(
            "Skipping Claude Code installation and backend communication (ENVRUNNER_SKIP_CLAUDE_CODE=true)",
            LogAttributes::new(),
        );
        return selected_backend(
            logger,
            Arc::new(NoopBackend::new()),
            None,
            "*api.NoopBackend",
        );
    }
    if session_mode == ConfigSessionMode::SetupOnly {
        logger.info(
            "Using NoopBackend for setup-only session mode",
            LogAttributes::new(),
        );
        return selected_backend(
            logger,
            Arc::new(NoopBackend::new()),
            None,
            "*api.NoopBackend",
        );
    }

    let startup_context = task_context
        .startup_context
        .as_ref()
        .ok_or_else(|| "missing startup context for session backend".to_owned())?;
    let auth_context = task_context
        .auth_context
        .as_ref()
        .ok_or_else(|| "missing auth context for session backend".to_owned())?;
    let ingress_token = auth_context.get_session_ingress_token();
    if startup_context.use_code_sessions {
        let http_client = HttpClient::with_options(
            &startup_context.api_base_url,
            Some(logger.clone()),
            ClientOptions {
                default_headers: BTreeMap::from([(
                    "anthropic-version".to_owned(),
                    "2023-06-01".to_owned(),
                )]),
                ..ClientOptions::default()
            },
        )
        .map_err(|error| format!("failed to create CCR backend: {error}"))?;
        let ccr = Arc::new(CCRBackend::new(
            http_client,
            ingress_token,
            session_id,
            Some(logger.clone()),
        ));
        let register_cancellation = CancellationToken::new();
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            ccr.register_worker(&register_cancellation),
        )
        .await;
        register_cancellation.cancel();
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                return Err(format!("failed to register worker with CCR: {error}"));
            }
            Err(_) => {
                return Err(
                    "failed to register worker with CCR: context deadline exceeded".to_owned(),
                );
            }
        }
        let backend: Arc<dyn SessionBackend> = ccr.clone();
        return selected_backend(logger, backend, Some(ccr), "*api.CCRBackend");
    }

    let backend = SessionIngressBackend::new(
        &startup_context.api_base_url,
        ingress_token,
        session_id,
        Some(logger.clone()),
    )
    .map_err(|error| format!("failed to create session ingress backend: {error}"))?;
    selected_backend(
        logger,
        Arc::new(backend),
        None,
        "*api.SessionIngressBackend",
    )
}

/// 记录最终 backend 类型并构造统一返回值。
fn selected_backend(
    logger: &Logger,
    backend: Arc<dyn SessionBackend>,
    ccr_backend: Option<Arc<CCRBackend>>,
    backend_type: &str,
) -> Result<TaskRunBackend, String> {
    logger.info(
        "Selected session backend",
        log_attributes([("backend_type", json!(backend_type))]),
    );
    Ok(TaskRunBackend {
        backend,
        ccr_backend,
    })
}

/// 对应 Go `applyCCRBackendClaudeEnvironment`，为 CCR child 注入协议开关和 epoch。
pub(super) fn apply_ccr_backend_claude_environment(
    claude_config: &mut config::ClaudeConfig,
    ccr_backend: Option<&CCRBackend>,
) {
    let Some(ccr_backend) = ccr_backend else {
        return;
    };
    claude_config
        .environment
        .insert("CLAUDE_CODE_USE_CCR_V2".to_owned(), "true".to_owned());
    claude_config.environment.insert(
        "CLAUDE_CODE_WORKER_EPOCH".to_owned(),
        ccr_backend.worker_epoch().to_string(),
    );
}

/// 对应 Go `taskRunClaudePath`，CLI flag 优先于 ambient 默认路径。
pub(super) fn task_run_claude_path(
    logger: &Logger,
    claude_path_flag: Option<&std::path::Path>,
) -> String {
    if let Some(path) = claude_path_flag {
        let path = path.to_string_lossy().into_owned();
        logger.info(
            "Using custom executable path from CLI flag",
            log_attributes([("path", json!(&path))]),
        );
        return path;
    }
    std::env::var("CLAUDE_DEFAULT_PATH")
        .ok()
        .filter(|path| !path.is_empty())
        .unwrap_or_else(|| "claude".to_owned())
}

/// 对应 Go `ensureClaudeCodeAvailableForTaskRun`，在共享 60 秒预算内安装或验证 Claude。
async fn ensure_claude_code_available_for_task_run(
    cancellation: &CancellationToken,
    logger: &Logger,
    claude_path_flag: Option<&std::path::Path>,
    claude_code_version: &str,
    skip_claude_code: bool,
    activity_recorder: &dyn ActivityRecorder,
) -> Result<Duration, RuntimeError> {
    let claude_path = task_run_claude_path(logger, claude_path_flag);
    if skip_claude_code {
        return Ok(Duration::ZERO);
    }

    let started = Instant::now();
    let install_cancellation = cancellation.child_token();
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        claude::install_or_update_claude_code(
            &install_cancellation,
            logger,
            &claude_path,
            claude_code_version,
            activity_recorder,
        ),
    )
    .await;
    install_cancellation.cancel();
    let duration = started.elapsed();
    let installed_version = match result {
        Ok(Ok(version)) => version,
        Ok(Err(error)) => {
            return Err(wrap_claude_code_availability_error(&error));
        }
        Err(_) => {
            return Err(RuntimeError::message(
                "failed to ensure Claude Code is available: context deadline exceeded",
            ));
        }
    };
    util::set_claude_code_version(installed_version);
    logger.info("Claude Code installation completed", LogAttributes::new());
    Ok(duration)
}

/// 为 Claude 安装失败添加 task-run 阶段前缀并保留任意 CombinedOutput 字节。
///
/// 对应 Go `fmt.Errorf("failed to ensure Claude Code is available: %w", err)`；调用方
/// 必须把返回值继续交给原始字节 renderer，日志若需要文本可使用其 `Display` 视图。
pub(super) fn wrap_claude_code_availability_error(error: &claude::ClaudeError) -> RuntimeError {
    // Go 的 `%w` 只拼接 error string 字节；format!/Display 会提前替换非法 UTF-8。
    RuntimeError::prefixed(
        b"failed to ensure Claude Code is available: ",
        error.as_bytes(),
    )
}

/// 把单调耗时安全转换为 Go `Duration.Milliseconds()` 对应的整数。
fn duration_milliseconds(started: Instant) -> i64 {
    i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)
}

/// 按 Go `slog` 调用中的参数顺序构造日志属性。
///
/// `encoding/json` 文件 handler 会在自己的边界重新按 key 排序；stderr JSON handler
/// 必须保留这里的插入顺序，因此不能复用本文件用于诊断字段和 HTTP header 的
/// `BTreeMap`。
fn log_attributes<const N: usize>(entries: [(&str, Value); N]) -> LogAttributes {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

/// `task-run` 启动后需要按逆序关闭的诊断与 OTel 资源。
struct TaskRunTelemetry {
    observability: Arc<TaskRunObservability>,
    o11y_service: Option<Arc<O11yServiceImpl>>,
    diag_service: Option<Arc<o11y::diag::DiagService>>,
}

/// 执行完整 `task-run` 命令。
///
/// 对应 Go `AddTaskRunCommand` 的 RunE。参数已经由 Clap 校验，但 stdin 仍无条件读取；
/// session/work/environment ID 以 stdin 为准，随后严格按 ACK、backend、安装、telemetry、
/// manager 的顺序推进。
///
/// # 副作用
/// 获取 `/tmp` session flock，可能设置两个进程环境变量、读取 stdin、访问后端、安装
/// Claude、创建临时诊断文件、启动 MCP/tunnel/lease，并响应 SIGINT/SIGTERM。
pub async fn run_task_run(arguments: &TaskRunArgs) -> Result<(), RuntimeError> {
    let process_started = Instant::now();
    let process_start_time = SystemTime::now();

    if arguments.debug {
        // SAFETY: task-run 在创建任何应用后台任务或外部命令前执行一次环境初始化；
        // 此后不再修改该键。Tokio runtime 虽已建立，但当前程序没有其他环境读写任务。
        unsafe { std::env::set_var("CLAUDE_CODE_DEBUG", "1") };
    }
    if arguments.skip_git_config {
        // SAFETY: 与上面的 debug 键相同，本写入发生在业务并发启动前，且后续只读。
        unsafe { std::env::set_var("SKIP_GIT_CONFIG", "true") };
    }

    let logger = create_logger_with_file_output(logger_level(arguments.log_level));
    if arguments.stdin_was_set {
        logger.warn(
            "The --stdin flag is deprecated and will be removed in a future release. Stdin is now always used.",
            LogAttributes::new(),
        );
    }
    let _session_lock = util::acquire_lock(&logger, &arguments.session_id)
        .map_err(|error| format!("failed to acquire lock: {error}"))?;
    let session_mode = config_session_mode(arguments.session_mode);

    let context_load_started = Instant::now();
    let mut task_context = load_context_from_stdin(
        &CancellationToken::new(),
        &logger,
        &arguments.session_id,
        arguments.input_format,
        &arguments.working_directory,
    )
    .await
    .map_err(|error| error.context(b"failed to load context from stdin: "))?;
    let session_id = if task_context.session_id.is_empty() {
        arguments.session_id.clone()
    } else {
        task_context.session_id.clone()
    };
    let work_id = task_context.work_id.clone();
    let environment_id = if task_context.environment_id.is_empty() {
        arguments.environment_id.clone().unwrap_or_default()
    } else {
        task_context.environment_id.clone()
    };
    if let Some(auth_context) = task_context.auth_context.as_mut() {
        auth_context.set_session_id(&session_id);
    }
    log_loaded_context_from_stdin(
        &logger,
        &task_context,
        &session_id,
        &work_id,
        &environment_id,
        context_load_started.elapsed(),
    );

    let cancellation = CancellationToken::new();
    acknowledge_work_if_needed(
        &cancellation,
        &logger,
        &task_context,
        &work_id,
        &environment_id,
        std::env::var_os("ENVRUNNER_SKIP_ACK").is_some(),
    )
    .await?;
    if task_context.is_healthcheck {
        return run_task_run_healthcheck(
            &logger,
            &session_id,
            context_load_started.elapsed(),
            process_started,
        )
        .map_err(RuntimeError::from);
    }

    let skip_claude_code = std::env::var("ENVRUNNER_SKIP_CLAUDE_CODE").as_deref() == Ok("true");
    let selected = build_task_run_backend(
        &logger,
        &task_context,
        &session_id,
        session_mode,
        skip_claude_code,
    )
    .await?;
    let event_poster: Arc<dyn EventPoster> = Arc::new(BackendEventPoster {
        backend: Arc::clone(&selected.backend),
    });
    let activity_recorder: Arc<dyn ActivityRecorder> = Arc::new(new_activity_recorder(
        cancellation.clone(),
        Arc::clone(&event_poster),
        logger.clone(),
        None,
    ));
    let install_duration = ensure_claude_code_available_for_task_run(
        &cancellation,
        &logger,
        arguments.claude_path.as_deref(),
        &arguments.claude_agent_version,
        skip_claude_code,
        activity_recorder.as_ref(),
    )
    .await?;

    let telemetry = initialize_task_run_telemetry(
        &cancellation,
        Arc::clone(&selected.backend),
        &logger,
        session_mode,
        install_duration,
        skip_claude_code,
    )
    .await?;
    let result = execute_task_run_manager(
        arguments,
        &cancellation,
        &logger,
        task_context,
        session_id,
        work_id,
        environment_id,
        session_mode,
        selected,
        event_poster,
        Arc::clone(&telemetry.observability),
        process_start_time,
        process_started,
        context_load_started,
    )
    .await;
    shutdown_task_run_telemetry(&logger, telemetry).await;
    result
}

/// 对应 Go `AddTaskRunCommand.func1` 中 session 校验之后的运行时组装。
///
/// 函数依次注册 Anthropic/BYOC environment、构造 stdin API adapter、生成
/// Claude executor factory，再把这些依赖交给 manager。将组装与前置 ACK、
/// 安装和 telemetry 分开，是为了保留 Go RunE 的错误顺序。
///
/// # 副作用
/// 可创建 environment 资源、MCP server、lease、tunnel 和 Claude direct child；
/// 所有取消都从 `cancellation` 派生，不扩展为进程组终止。
#[allow(clippy::too_many_arguments)]
async fn execute_task_run_manager(
    arguments: &TaskRunArgs,
    cancellation: &CancellationToken,
    logger: &Logger,
    task_context: TaskContext,
    session_id: String,
    work_id: String,
    environment_id: String,
    session_mode: ConfigSessionMode,
    selected_backend: TaskRunBackend,
    event_poster: Arc<dyn EventPoster>,
    observability: Arc<TaskRunObservability>,
    process_start_time: SystemTime,
    process_started: Instant,
    context_load_started: Instant,
) -> Result<(), RuntimeError> {
    let session_config = config::Session {
        session_id: session_id.clone(),
        work_id,
        startup_context: task_context.startup_context.clone(),
    };
    session_config
        .validate()
        .map_err(|error| format!("invalid configuration: {error}"))?;
    log_task_run_start(logger, &session_config);

    let mut environment_types = envtype::new_env_types();
    environment_types.register((*envtype::anthropic::REGISTRATION).clone());
    environment_types.register((*envtype::byoc::REGISTRATION).clone());

    log_task_run_environment_from_stdin(logger, &task_context);
    let config_client: Arc<dyn ManagerApiClient> = Arc::new(StdinConfigClient {
        config: task_context.environment.clone(),
        auth_context: task_context.auth_context.clone(),
        outcomes: task_context.outcomes.clone(),
        logger: logger.clone(),
    });

    let mut claude_config = config::default_claude_config();
    claude_config.allowed_tools = arguments.allowed_tools.clone().unwrap_or_default();
    claude_config.local_testing_mode = arguments.local_testing;
    claude_config.verbose_logging = arguments.verbose_claude_logs;
    claude_config.print_code_logs = arguments.print_code_logs;
    claude_config.local_append_system_prompt = arguments
        .local_append_system_prompt
        .clone()
        .unwrap_or_default();
    // Go 的 ClaudeConfig.ClaudePath 只保存 flag；ambient CLAUDE_DEFAULT_PATH 仅在
    // 安装和 executor 内部解析，不能在这里反写进配置快照。
    claude_config.claude_path = arguments
        .claude_path
        .as_deref()
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_default();
    claude_config.skip_claude_code =
        std::env::var("ENVRUNNER_SKIP_CLAUDE_CODE").as_deref() == Ok("true");
    apply_ccr_backend_claude_environment(
        &mut claude_config,
        selected_backend.ccr_backend.as_deref(),
    );
    log_task_run_claude_config(logger, &claude_config);

    let otel_log_handler = task_run_claude_log_handler(&observability);
    let executor_observability = Arc::clone(&observability);
    let target_version = arguments.claude_agent_version.clone();
    let executor_factory: ClaudeExecutorFactory = Arc::new(
        move |logger,
              configuration,
              session_id,
              startup_context,
              auth_context,
              registrations,
              outcomes,
              activity_recorder,
              lease_manager| {
            // Manager 保留具体 MCP registration 以便清理；Claude 目前只需要
            // 不透明 marker，因此在 factory 边界做 Arc unsize，不复制密钥或进程句柄。
            let registrations: Vec<Arc<dyn claude::McpServerRegistration>> = registrations
                .into_iter()
                .map(|registration| {
                    let registration: Arc<dyn claude::McpServerRegistration> = registration;
                    registration
                })
                .collect();
            let mut executor = claude::new_claude_code_executor(
                logger,
                Some(configuration),
                session_id,
                startup_context.unwrap_or_default(),
                Some(auth_context),
                registrations,
                Some(outcomes),
                activity_recorder,
                Arc::clone(&otel_log_handler),
                lease_manager,
                &target_version,
            )
            .map_err(|error| Box::new(error) as BoxError)?;
            executor.set_observability(executor_observability.clone());
            Ok(Arc::new(executor) as Arc<dyn Executor>)
        },
    );

    let mut manager = new_manager_with_executor_and_claude_config(
        config_client,
        logger.clone(),
        Some(session_config),
        Arc::new(environment_types),
        executor_factory,
        session_mode,
        arguments.session_mode_was_set,
        Some(claude_config),
        environment_id,
        arguments.organization_id.clone().unwrap_or_default(),
        process_start_time,
        Some(event_poster),
    );
    manager.set_observability(Some(observability.clone()));

    logger.info(
        "Setup completed, starting manager execution",
        [
            (
                "setup_duration_ms".to_owned(),
                json!(duration_milliseconds(context_load_started)),
            ),
            (
                "total_elapsed_ms".to_owned(),
                json!(duration_milliseconds(process_started)),
            ),
        ]
        .into_iter()
        .collect(),
    );
    run_task_run_manager(
        cancellation,
        logger,
        &mut manager,
        session_mode,
        process_started,
        observability.as_ref(),
    )
    .await
}

/// Go `taskRunManager` 的 Rust 适配边界，使信号包装可独立注入失败与取消。
#[async_trait]
pub(super) trait TaskRunManager: Send {
    /// 执行 manager；返回值同时保留日志文本视图和最终 stderr 原始字节。
    async fn run(&mut self, cancellation: &CancellationToken) -> Result<(), ManagerError>;
}

#[async_trait]
impl TaskRunManager for Manager {
    /// 原样转发 manager 错误，直到 Cobra renderer 前都不调用有损 `Display`。
    async fn run(&mut self, cancellation: &CancellationToken) -> Result<(), ManagerError> {
        Manager::run(self, cancellation).await
    }
}

/// 对应 Go `runTaskRunManager`，监听 SIGINT/SIGTERM 并给 manager 最多十秒清理。
///
/// manager 自然完成时直接返回结果；收到信号时只取消它的 token。
/// `context canceled` 等价于 Go `errors.Is(err, context.Canceled)`，按正常结束
/// 记录指标，其他错误添加 `environment manager failed` 前缀。
pub(super) async fn run_task_run_manager(
    cancellation: &CancellationToken,
    logger: &Logger,
    manager: &mut dyn TaskRunManager,
    session_mode: ConfigSessionMode,
    process_started: Instant,
    observability: &TaskRunObservability,
) -> Result<(), RuntimeError> {
    let manager_cancellation = cancellation.child_token();
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|error| {
        RuntimeError::message(format!(
            "failed to register interrupt signal handler: {error}"
        ))
    })?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|error| {
        RuntimeError::message(format!(
            "failed to register terminate signal handler: {error}"
        ))
    })?;
    let mut manager_future = manager.run(&manager_cancellation);

    let run_result = tokio::select! {
        result = &mut manager_future => result,
        _ = interrupt.recv() => {
            log_task_run_shutdown_signal(logger, "interrupt");
            manager_cancellation.cancel();
            wait_for_task_run_manager_shutdown(logger, &mut manager_future).await
        }
        _ = terminate.recv() => {
            log_task_run_shutdown_signal(logger, "terminated");
            manager_cancellation.cancel();
            wait_for_task_run_manager_shutdown(logger, &mut manager_future).await
        }
    };

    if let Err(error) = run_result
        && !is_task_run_cancellation_error(&error)
    {
        logger.error(
            "Environment manager execution failed",
            [
                ("error".to_owned(), json!(error.to_string())),
                (
                    "total_duration_ms".to_owned(),
                    json!(duration_milliseconds(process_started)),
                ),
            ]
            .into_iter()
            .collect(),
        );
        if let Some(metrics) = &observability.metrics {
            // 指标接口只接受标准 UTF-8 Error；这里使用 Go 风格替换视图，但下面返回
            // 给 CLI 的 RuntimeError 仍从原始字节构造，两条用途不能共用 String。
            let metric_error = std::io::Error::other(error.to_string());
            o11y::increment_env_manager_end(metrics, session_mode, Some(&metric_error));
        }
        observability.log_diag("end_task_run_failure", None);
        return Err(RuntimeError::prefixed(
            b"environment manager failed: ",
            error.as_bytes(),
        ));
    }

    if let Some(metrics) = &observability.metrics {
        o11y::increment_env_manager_end(metrics, session_mode, None);
    }
    observability.log_diag("end_task_run", None);
    logger.info(
        "Environment manager completed successfully",
        log_attributes([(
            "total_duration_ms",
            json!(duration_milliseconds(process_started)),
        )]),
    );
    Ok(())
}

/// 对应 Go `runTaskRunManager` 收到 Unix signal 后的唯一日志调用点。
///
/// Go 的 `select` 在一个 case 中记录动态 `sig.String()`；Tokio 需要两个 signal
/// stream，但不能因此复制 logger callsite，否则 runtime source 和 Go oracle 不一致。
///
/// # 参数
/// - `logger`：task-run 的结构化 logger。
/// - `signal`：Go `os.Signal.String()` 对应的 `interrupt` 或 `terminated`。
fn log_task_run_shutdown_signal(logger: &Logger, signal: &'static str) {
    logger.info(
        "Received signal, shutting down gracefully",
        log_attributes([("signal", json!(signal))]),
    );
}

/// 信号取消后继续 poll 同一个 manager future，避免重新执行状态机。
async fn wait_for_task_run_manager_shutdown(
    logger: &Logger,
    manager_future: &mut Pin<Box<dyn Future<Output = Result<(), ManagerError>> + Send + '_>>,
) -> Result<(), ManagerError> {
    match tokio::time::timeout(Duration::from_secs(10), manager_future).await {
        Ok(result) => result,
        Err(_) => {
            logger.error(
                "Manager shutdown timed out after 10 seconds",
                LogAttributes::new(),
            );
            Err(ManagerError::message(
                "manager shutdown timed out after 10 seconds",
            ))
        }
    }
}

/// 判断 manager 错误的文本视图是否代表 Go `context.Canceled`。
fn is_task_run_cancellation_error(error: &ManagerError) -> bool {
    // 取消分类只影响控制流，可使用日志用的有损视图；真正向上传播的失败仍保留在
    // ManagerError 原始字节中，不会因为本次检查而被替换。
    let error = error.to_string();
    error.contains("context canceled")
        || error.contains("context cancelled")
        || error.contains("operation canceled")
        || error.contains("operation cancelled")
}

/// 对应 Go `logTaskRunStart`，只记录不含 secret 的 session 摘要。
fn log_task_run_start(logger: &Logger, session_config: &config::Session) {
    let sources_count = session_config
        .startup_context
        .as_ref()
        .map_or(0, |context| context.sources.len());
    logger.info(
        "Starting environment manager",
        [
            (
                "has_startup_context".to_owned(),
                json!(session_config.startup_context.is_some()),
            ),
            ("session_id".to_owned(), json!(&session_config.session_id)),
            ("sources_count".to_owned(), json!(sources_count)),
        ]
        .into_iter()
        .collect(),
    );
}

/// 对应 Go `logTaskRunEnvironmentFromStdin`，避免记录 environment config 正文。
fn log_task_run_environment_from_stdin(logger: &Logger, task_context: &TaskContext) {
    let environment_type = task_context
        .environment
        .as_ref()
        .map_or("", |environment| environment.environment_type.as_str());
    logger.info(
        "Using environment configuration from stdin",
        log_attributes([("environment_type", json!(environment_type))]),
    );
}

/// 对应 Go `logTaskRunClaudeConfig`，逐个复刻非敏感开关的条件日志。
fn log_task_run_claude_config(logger: &Logger, claude_config: &config::ClaudeConfig) {
    if !claude_config.allowed_tools.is_empty() {
        logger.info(
            "Configured allowed tools for Claude",
            log_attributes([("tools", json!(&claude_config.allowed_tools))]),
        );
    }
    if !claude_config.local_append_system_prompt.is_empty() {
        logger.info(
            "Configured local append system prompt",
            log_attributes([(
                "prompt_length",
                json!(claude_config.local_append_system_prompt.len()),
            )]),
        );
    }
    if claude_config.local_testing_mode {
        logger.info(
            "WebSocket disabled, Claude Code will run in standalone mode",
            LogAttributes::new(),
        );
    }
    if claude_config.verbose_logging {
        logger.info(
            "Enabled verbose Claude Code logging to console",
            LogAttributes::new(),
        );
    }
    if claude_config.print_code_logs {
        logger.info(
            "Enabled printing Claude Code logs on exit",
            LogAttributes::new(),
        );
    }
}

/// 将 CLI 枚举机械转换为配置层枚举，防止两个公开协议类型互相泄漏。
const fn config_session_mode(mode: SessionMode) -> ConfigSessionMode {
    match mode {
        SessionMode::New => ConfigSessionMode::New,
        SessionMode::Resume => ConfigSessionMode::Resume,
        SessionMode::ResumeCached => ConfigSessionMode::ResumeCached,
        SessionMode::SetupOnly => ConfigSessionMode::SetupOnly,
    }
}

/// 记录 stdin 中不含 secret 的上下文摘要。
fn log_loaded_context_from_stdin(
    logger: &Logger,
    task_context: &TaskContext,
    session_id: &str,
    work_id: &str,
    environment_id: &str,
    duration: Duration,
) {
    let source_count = task_context
        .startup_context
        .as_ref()
        .map_or(0, |context| context.sources.len());
    let environment_type = task_context
        .environment
        .as_ref()
        .map_or("", |environment| environment.environment_type.as_str());
    logger.info(
        "Loaded context from stdin",
        [
            ("environment_id".to_owned(), json!(environment_id)),
            ("environment_type".to_owned(), json!(environment_type)),
            (
                "has_auth".to_owned(),
                json!(task_context.auth_context.is_some()),
            ),
            ("session_id".to_owned(), json!(session_id)),
            ("startup_sources_count".to_owned(), json!(source_count)),
            (
                "stdin_parse_duration_ms".to_owned(),
                json!(duration.as_millis()),
            ),
            ("work_id".to_owned(), json!(work_id)),
        ]
        .into_iter()
        .collect(),
    );
}

/// 对应 Go `runTaskRunHealthcheck`，仅记录成功且不创建 backend/manager。
fn run_task_run_healthcheck(
    logger: &Logger,
    session_id: &str,
    parse_duration: Duration,
    process_started: Instant,
) -> Result<(), String> {
    logger.info(
        "Running healthcheck",
        [
            (
                "healthcheck_parse_duration_ms".to_owned(),
                json!(parse_duration.as_millis()),
            ),
            ("session_id".to_owned(), json!(session_id)),
        ]
        .into_iter()
        .collect(),
    );
    logger.info("Executing healthcheck", LogAttributes::new());
    logger.info(
        "Healthcheck completed successfully",
        log_attributes([(
            "total_duration_ms",
            json!(duration_milliseconds(process_started)),
        )]),
    );
    Ok(())
}

/// 初始化诊断 JSONL 服务；环境开关关闭时返回空值且不创建文件。
pub(super) async fn init_diag_logging(
    cancellation: &CancellationToken,
    backend: Arc<dyn SessionBackend>,
    logger: &Logger,
) -> Result<Option<(Arc<o11y::diag::DiagService>, o11y::diag::DiagContext)>, String> {
    if !o11y::diag::logs_enabled() {
        logger.info("Diagnostic logs are disabled", LogAttributes::new());
        return Ok(None);
    }
    o11y::diag::new_diag_service_with_backend(cancellation.clone(), backend)
        .await
        .map(Some)
        .map_err(|error| format!("failed to initialize diagnostic logging service: {error}"))
}

/// 使用 backend 提供的端点创建 task-run OTel metrics/logging 服务。
fn init_task_run_o11y(
    cancellation: &CancellationToken,
    backend: &dyn SessionBackend,
    logger: &Logger,
) -> Result<(Arc<O11yServiceImpl>, O11yContext), String> {
    let endpoints = backend.otlp_endpoints();
    o11y::new_o11y_service(
        Some(cancellation.clone()),
        Some(logger.clone()),
        Some(&o11y::O11yConfig {
            metrics_endpoint: endpoints.metrics_endpoint,
            logs_endpoint: endpoints.logs_endpoint,
            headers: endpoints.headers,
            enable_logging: true,
            use_stdout_metrics: false,
        }),
    )
    .map_err(|error| format!("failed to initialize observability service: {error}"))
}

/// 按 Go 顺序初始化 diag 后再安装 OTel 上下文和启动指标。
async fn initialize_task_run_telemetry(
    cancellation: &CancellationToken,
    backend: Arc<dyn SessionBackend>,
    logger: &Logger,
    session_mode: ConfigSessionMode,
    install_duration: Duration,
    skip_claude_code: bool,
) -> Result<TaskRunTelemetry, String> {
    if skip_claude_code {
        return Ok(TaskRunTelemetry {
            observability: Arc::new(TaskRunObservability::default()),
            o11y_service: None,
            diag_service: None,
        });
    }
    let diag = init_diag_logging(cancellation, Arc::clone(&backend), logger)
        .await
        .map_err(|error| format!("failed to initialize diagnostic logging: {error}"))?;
    let diag_context = diag.as_ref().map(|(_, context)| context.clone());
    o11y::diag::log_env_manager_no_pii(diag_context.as_ref(), "start_task_run", None);
    let (service, metrics) = match init_task_run_o11y(cancellation, backend.as_ref(), logger) {
        Ok(runtime) => runtime,
        Err(error) => {
            if let Some((service, _)) = &diag {
                shutdown_diag_service(logger, service).await;
            }
            return Err(error);
        }
    };
    o11y::increment_env_manager_start(&metrics, session_mode);
    o11y::record_duration(
        &metrics,
        Some(&o11y::CLAUDE_INSTALL_METRIC),
        install_duration,
        None,
        &[],
    );
    Ok(TaskRunTelemetry {
        observability: Arc::new(TaskRunObservability {
            diag: diag_context,
            metrics: Some(metrics),
        }),
        o11y_service: Some(service),
        diag_service: diag.map(|(service, _)| service),
    })
}

/// 根据 `OTEL_LOGS_EXPORTER` 选择 Claude 结构化日志 handler。
fn task_run_claude_log_handler(
    observability: &TaskRunObservability,
) -> Arc<dyn crate::internal::logger::LogHandler> {
    if std::env::var("OTEL_LOGS_EXPORTER").as_deref() == Ok("otlp") {
        return o11y::get_o11y_service(observability.metrics.as_ref()).log_handler();
    }
    o11y::get_o11y_service(None).log_handler()
}

/// 关闭 OTel 服务；关闭失败只记录日志，不覆盖 manager 的主结果。
async fn shutdown_task_run_o11y(logger: &Logger, service: &Arc<O11yServiceImpl>) {
    let shutdown_cancellation = CancellationToken::new();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        service.shutdown(&shutdown_cancellation),
    )
    .await;
    shutdown_cancellation.cancel();
    let error = match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error.to_string()),
        Err(_) => Some("context deadline exceeded".to_owned()),
    };
    if let Some(error) = error {
        // Go 无论 service 自身返错还是 shutdown context 超时，都只经过
        // `service.Shutdown` 后的这一个 logger callsite。
        logger.error(
            "Failed to shutdown observability",
            log_attributes([("error", json!(error))]),
        );
    }
}

/// 在独立十秒窗口内关闭诊断 collector、最终 flush 和两个文件描述符。
async fn shutdown_diag_service(logger: &Logger, service: &Arc<o11y::diag::DiagService>) {
    let shutdown_cancellation = CancellationToken::new();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        service.shutdown(&shutdown_cancellation),
    )
    .await;
    shutdown_cancellation.cancel();
    let error = match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error.to_string()),
        Err(_) => Some("context deadline exceeded".to_owned()),
    };
    if let Some(error) = error {
        // 与 Go cleanup closure 一样，超时只是 `Shutdown` 的一种 error，不创建
        // 第二个日志 source。
        logger.error(
            "Failed to shutdown diagnostic logging service",
            log_attributes([("error", json!(error))]),
        );
    }
}

/// 按 Go defer 的 LIFO 顺序先关 OTel，再关诊断服务。
async fn shutdown_task_run_telemetry(logger: &Logger, telemetry: TaskRunTelemetry) {
    if let Some(service) = &telemetry.o11y_service {
        shutdown_task_run_o11y(logger, service).await;
    }
    if let Some(service) = &telemetry.diag_service {
        shutdown_diag_service(logger, service).await;
    }
}
