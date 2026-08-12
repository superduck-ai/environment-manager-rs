//! 对应 Go `cmd/cmd_orchestrator.go` 的常驻轮询命令。
//!
//! 本文件逐项映射 Go `cmd/cmd_orchestrator.go` 的 flag、默认值、长帮助和示例。
//! 解析后按原顺序完成容器锁、身份发现、poller/hook、sandbox、自调用和信号关闭。

use std::fmt::{self, Display, Formatter};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use clap::builder::ValueParser;
use clap::{Arg, ArgMatches, Command, value_parser};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::error::CliError;
use super::utils::{
    LogLevel, bool_value, flag_arg, logger_level, parse_go_duration, parse_log_level,
};
use crate::internal::logger::{LogAttributes, Logger, create_logger_with_file_output};
use crate::internal::orchestrator::{
    CommandOutcome, Hook, OrchestratorConfig, OrchestratorError, OrchestratorResult,
    PollerInterface, SandboxAdapter, SandboxRequest, log_command_wrapped_with_sandbox,
    log_sandbox_cleanup_failed, new_orchestrator, new_poll_hook, new_poller_with_worker_id,
    new_whoami_client,
};
use crate::internal::sandbox::{install_sandbox_runtime, new_sandbox_runtime_with_config};
use crate::internal::util::{acquire_container_lock, go_duration_nanoseconds};

const LONG_ABOUT: &str = r#"The orchestrator command polls the API for session tasks and handing off work to be executed.

It handles:
- Discovering environment identity via the /v1/environments/whoami endpoint
- Polling the environments API work/poll endpoint
- Executing work:
  - With --execute-hook: pipes JSON to hook via stdin and exits with hook's exit code
  - Without --execute-hook: auto-invokes 'task-run --input-format=v1'
    - With --sandbox-backend=sandbox-runtime (default): wraps execution in sandbox
    - With --sandbox-backend=none: runs without sandbox (logs warning)
- Running timeout hooks for periodic maintenance (e.g., monorepo updates)
- Sleeping with jitter when queue is empty
- Graceful shutdown on SIGTERM/SIGINT

Required environment variable:
  ENVIRONMENT_SERVICE_KEY: Service key for the environment

The environment ID and organization ID are discovered automatically via the whoami
endpoint. You can optionally provide --environment-id and --organization-id flags
to validate them against the token's identity."#;

const EXAMPLES: &str = r#"Examples:
  # Basic usage with sandbox (recommended - identity discovered via whoami)
  export ENVIRONMENT_SERVICE_KEY="your-environment-service-key"
  environment-runner orchestrator

  # With explicit IDs for validation
  environment-runner orchestrator \
    --environment-id "env_01ABC123" \
    --organization-id "org_01XYZ789"

  # With custom execute hook to handle sessions
  environment-runner orchestrator --execute-hook "./handle-session.sh"

  # Without sandbox
  environment-runner orchestrator --sandbox-backend none"#;

/// 执行 hook 时使用的沙箱后端。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxBackend {
    /// 明确禁用沙箱；旧实现会输出安全警告。
    None,
    /// 使用 `@anthropic-ai/sandbox-runtime`，也是默认值。
    SandboxRuntime,
}

impl Display for SandboxBackend {
    /// 输出 CLI 与配置层共享的稳定字符串。
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::None => "none",
            Self::SandboxRuntime => "sandbox-runtime",
        })
    }
}

impl FromStr for SandboxBackend {
    type Err = String;

    /// 严格解析旧命令支持的两个后端值。
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "none" => Ok(Self::None),
            "sandbox-runtime" => Ok(Self::SandboxRuntime),
            invalid => Err(format!(
                "invalid --sandbox-backend value {invalid:?}, must be one of: none, sandbox-runtime"
            )),
        }
    }
}

/// 已验证的 orchestrator 参数，供 Tokio 运行时直接消费。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorArgs {
    /// Anthropic API 基地址。
    pub api_url: String,
    /// 可选组织 ID；whoami 成功时用于一致性校验。
    pub organization_id: Option<String>,
    /// 可选环境 ID；whoami 成功时用于一致性校验。
    pub environment_id: Option<String>,
    /// 工作器标识；为空时运行时使用 hostname。
    pub client_id: Option<String>,
    /// 单次 poll 请求超时。
    pub poll_timeout: Duration,
    /// 空队列循环多久后触发 timeout hook。
    pub loop_timeout: Duration,
    /// 连续 poll 失败上限；零表示无限。
    pub max_poll_failures: i64,
    /// 可回收的未 ACK 工作年龄，单位毫秒；零使用 API 默认值。
    pub reclaim_older_than_ms: i64,
    /// 空队列超时时执行的命令。
    pub timeout_hook: Option<String>,
    /// 收到任务时执行的自定义命令。
    pub execute_hook: Option<String>,
    /// 自定义轮询命令。
    pub poll_hook: Option<String>,
    /// timeout hook 的执行超时。
    pub timeout_hook_timeout: Duration,
    /// execute hook 的执行超时；零表示不设超时。
    pub execute_hook_timeout: Duration,
    /// poll hook 的执行超时。
    pub poll_hook_timeout: Duration,
    /// 日志级别。
    pub log_level: LogLevel,
    /// hook 使用的沙箱后端。
    pub sandbox_backend: SandboxBackend,
    /// 自定义 sandbox-runtime JSON 配置路径。
    pub sandbox_settings: Option<PathBuf>,
    /// 环境 service key 文件；为空时运行时回退环境变量。
    pub service_key_file: Option<PathBuf>,
    /// 是否跳过 git 配置初始化。
    pub skip_git_config: bool,
    /// 是否跳过容器级互斥锁。
    pub skip_container_lock: bool,
}

/// 构造 `orchestrator` 子命令及全部 flag。
pub(crate) fn command() -> Command {
    Command::new("orchestrator")
        .about("Poll for session tasks and hand off for execution")
        .long_about(LONG_ABOUT)
        .after_long_help(EXAMPLES)
        .arg(string_arg("api-url", "https://api.anthropic.com", "API base URL"))
        .arg(optional_string_arg("organization-id", "Organization ID (find at claude.ai/settings > Account)"))
        .arg(optional_string_arg("environment-id", "Environment ID (find at claude.ai/settings, e.g., env_01ABC123)"))
        .arg(optional_string_arg("client-id", "Client ID (worker identifier, default: hostname)"))
        .arg(duration_arg("poll-timeout", "5m", "Poll request timeout duration"))
        .arg(duration_arg("loop-timeout", "5m", "Loop timeout before triggering timeout hook"))
        .arg(integer_arg("max-poll-failures", "Maximum consecutive poll failures before exiting (0 = infinite)"))
        .arg(integer_arg("reclaim-older-than-ms", "Reclaim unacknowledged work items older than this many milliseconds (0 = use API default of 5000ms)"))
        .arg(optional_string_arg("timeout-hook", "Command to run on loop timeout (e.g., monorepo updates)"))
        .arg(optional_string_arg("execute-hook", "Command to run with session JSON via stdin when task received. If not provided, defaults to self-invoking 'task-run --stdin --input-format=v1' (with or without sandbox based on --sandbox-backend)"))
        .arg(duration_arg("timeout-hook-timeout", "5m", "Timeout for timeout hook execution (e.g., monorepo updates)"))
        .arg(duration_arg("execute-hook-timeout", "0", "Timeout for execute hook (0 = no timeout, session lifetime controlled by API)"))
        .arg(optional_string_arg("poll-hook", "Command to execute for polling instead of built-in Poller. Receives environment context via stdin, returns work JSON via stdout."))
        .arg(duration_arg("poll-hook-timeout", "30s", "Timeout for poll hook execution"))
        .arg(Arg::new("log-level").long("log-level").default_value("info").help("Log level (debug, info, warn, error)").value_parser(ValueParser::new(parse_log_level)))
        .arg(Arg::new("sandbox-backend").long("sandbox-backend").default_value("sandbox-runtime").help("Sandbox backend for execute hook: none, sandbox-runtime.\nUse 'none' to disable sandboxing (allows running as non-root user).\nUse 'sandbox-runtime' (default) for sandboxed execution (requires root or unprivileged user namespaces).").value_parser(value_parser!(SandboxBackend)))
        .arg(optional_path_arg("sandbox-settings", "Path to custom sandbox-runtime settings JSON file (must include Anthropic domains)"))
        .arg(optional_path_arg("service-key-file", "Path to file containing the environment service key. If not set, falls back to ENVIRONMENT_SERVICE_KEY environment variable"))
        .arg(flag_arg("skip-git-config", "Skip git configuration setup (use container's existing .gitconfig)"))
        .arg(flag_arg("skip-container-lock", "Skip container-level lock (WARNING: allows multiple sessions per container, use only for development/testing)"))
}

/// 返回已注册全部 flag 的子命令。
///
/// 该别名使根命令的组装点保持简洁。
pub(crate) fn add_orchestrator_command() -> Command {
    command()
}

/// 将 Clap matches 转换成不再依赖 Clap 生命周期的参数所有权对象。
pub(crate) fn from_matches(matches: &ArgMatches) -> Result<OrchestratorArgs, CliError> {
    Ok(OrchestratorArgs {
        api_url: required_clone(matches, "api-url"),
        organization_id: optional_clone(matches, "organization-id"),
        environment_id: optional_clone(matches, "environment-id"),
        client_id: optional_clone(matches, "client-id"),
        poll_timeout: required_copy(matches, "poll-timeout"),
        loop_timeout: required_copy(matches, "loop-timeout"),
        max_poll_failures: required_copy(matches, "max-poll-failures"),
        reclaim_older_than_ms: required_copy(matches, "reclaim-older-than-ms"),
        timeout_hook: optional_clone(matches, "timeout-hook"),
        execute_hook: optional_clone(matches, "execute-hook"),
        poll_hook: optional_clone(matches, "poll-hook"),
        timeout_hook_timeout: required_copy(matches, "timeout-hook-timeout"),
        execute_hook_timeout: required_copy(matches, "execute-hook-timeout"),
        poll_hook_timeout: required_copy(matches, "poll-hook-timeout"),
        log_level: required_copy(matches, "log-level"),
        sandbox_backend: required_copy(matches, "sandbox-backend"),
        sandbox_settings: matches.get_one::<PathBuf>("sandbox-settings").cloned(),
        service_key_file: matches.get_one::<PathBuf>("service-key-file").cloned(),
        skip_git_config: bool_value(matches, "skip-git-config"),
        skip_container_lock: bool_value(matches, "skip-container-lock"),
    })
}

/// 构造带字符串默认值的 flag。
fn string_arg(id: &'static str, default: &'static str, help: &'static str) -> Arg {
    Arg::new(id).long(id).default_value(default).help(help)
}

/// 构造空值在 Rust 中表示为 `None` 的字符串 flag。
fn optional_string_arg(id: &'static str, help: &'static str) -> Arg {
    Arg::new(id).long(id).help(help)
}

/// 构造空值在 Rust 中表示为 `None` 的路径 flag。
fn optional_path_arg(id: &'static str, help: &'static str) -> Arg {
    Arg::new(id)
        .long(id)
        .help(help)
        .value_parser(value_parser!(PathBuf))
}

/// 构造 Go duration 兼容的超时 flag。
fn duration_arg(id: &'static str, default: &'static str, help: &'static str) -> Arg {
    Arg::new(id)
        .long(id)
        .default_value(default)
        .help(help)
        .allow_hyphen_values(true)
        .value_parser(ValueParser::new(parse_go_duration))
}

/// 构造默认值为零且允许 Go `int` 负数语法的整型 flag。
fn integer_arg(id: &'static str, help: &'static str) -> Arg {
    Arg::new(id)
        .long(id)
        .default_value("0")
        .help(help)
        .allow_hyphen_values(true)
        .value_parser(value_parser!(i64))
}

/// 从存在默认值的 Clap 参数中克隆所有权值。
fn required_clone<T: Clone + Send + Sync + 'static>(matches: &ArgMatches, id: &str) -> T {
    matches
        .get_one::<T>(id)
        .unwrap_or_else(|| panic!("CLI invariant: missing value for {id}"))
        .clone()
}

/// 从存在默认值的 Clap 参数中复制轻量值。
fn required_copy<T: Copy + Send + Sync + 'static>(matches: &ArgMatches, id: &str) -> T {
    *matches
        .get_one::<T>(id)
        .unwrap_or_else(|| panic!("CLI invariant: missing value for {id}"))
}

/// 把缺失或空字符串统一转换成旧运行时使用的“未提供”语义。
fn optional_clone(matches: &ArgMatches, id: &str) -> Option<String> {
    matches
        .get_one::<String>(id)
        .filter(|value| !value.is_empty())
        .cloned()
}

/// 默认 sandbox-runtime 与 orchestrator Hook 之间的窄适配器。
///
/// Go Hook 直接依赖 sandbox 包；Rust 用 trait 隔离测试，本实现把生产路径重新接回
/// 真实 SRT，并确保自动配置在任何执行结果之后、返回 Hook 之前完成清理。
#[derive(Debug, Default)]
struct SandboxRuntimeAdapter;

#[async_trait]
impl SandboxAdapter for SandboxRuntimeAdapter {
    /// 初始化 SRT、包装原命令、执行并在返回前清理自动生成的配置。
    ///
    /// cleanup 失败与 Go defer 一样只记录 warning，不覆盖命令本身的成功或失败结果。
    async fn execute(&self, request: SandboxRequest) -> OrchestratorResult<CommandOutcome> {
        let custom_config = (!request.sandbox_config_path.is_empty())
            .then(|| Path::new(&request.sandbox_config_path));
        let working_directory =
            (!request.working_dir.is_empty()).then(|| Path::new(&request.working_dir));
        let mut runtime = new_sandbox_runtime_with_config(
            &request.stdin_data,
            Some(&request.logger),
            custom_config,
            working_directory,
        )
        .map_err(|error| OrchestratorError::message(error.to_string()))?;
        let (command, arguments) = runtime.wrap_command(&request.command, &request.args);
        log_command_wrapped_with_sandbox(&request.logger, &command, &arguments);
        let outcome = crate::internal::orchestrator::run_command_combined_capture(
            &request.cancellation,
            &command,
            &arguments,
            Some(&request.stdin_data),
            request.timeout,
        )
        .await;
        if let Err(error) = runtime.cleanup() {
            log_sandbox_cleanup_failed(&request.logger, &error.to_string());
        }
        Ok(outcome)
    }
}

/// 构造 Go `AddOrchestratorCommand.RunE` 的首条结构化配置日志属性。
///
/// # 参数
/// - `arguments`：已经由 CLI 层完成默认值和 Go duration 解析的 orchestrator 参数。
///
/// # 返回
/// 返回交给 JSON logger 的有序属性。两个 duration 必须编码为 `slog.Duration` 使用的
/// 纳秒整数，不能使用 Rust `Debug` 文本，否则日志采集端看到的类型和单位都会变化。
#[must_use]
pub(super) fn orchestrator_configured_attributes(arguments: &OrchestratorArgs) -> LogAttributes {
    attributes([
        ("api_url", json!(arguments.api_url)),
        (
            "organization_id",
            json!(arguments.organization_id.as_deref().unwrap_or_default()),
        ),
        (
            "environment_id",
            json!(arguments.environment_id.as_deref().unwrap_or_default()),
        ),
        (
            "client_id",
            json!(arguments.client_id.as_deref().unwrap_or_default()),
        ),
        (
            "poll_timeout",
            json!(go_duration_nanoseconds(arguments.poll_timeout)),
        ),
        (
            "loop_timeout",
            json!(go_duration_nanoseconds(arguments.loop_timeout)),
        ),
        ("skip_container_lock", json!(arguments.skip_container_lock)),
    ])
}

/// 对应 Go：`cmd/cmd_orchestrator.go` 中 `AddOrchestratorCommand` 的 `RunE`。
///
/// 严格保持初始化顺序：先持有容器锁，再读取 service key 和 whoami，随后创建 poller、
/// 安装 sandbox、构造 execute Hook，最后启动 orchestrator 并监听 SIGINT/SIGTERM。
/// 默认 execute Hook 自调用当前 argv0 的 `task-run --input-format=v1`。
///
/// # 参数
/// - `arguments`：Clap 已校验且拥有所有权的运行参数。
/// - `executable`：原始 `argv[0]`；不能替换成 canonical path，否则会改变 wrapper/链接行为。
///
/// # 副作用
/// 获取 `/tmp` flock、读 secret、访问 whoami/work API、安装 SRT、创建子进程、注册
/// Unix 信号并可能创建临时 sandbox 配置。收到信号后等待最多 30 秒并始终成功退出。
pub async fn run_orchestrator(
    arguments: &OrchestratorArgs,
    executable: &std::ffi::OsStr,
) -> Result<(), String> {
    let logger = create_logger_with_file_output(logger_level(arguments.log_level));
    logger.info(
        "orchestrator configured",
        orchestrator_configured_attributes(arguments),
    );

    let _container_lock = if arguments.skip_container_lock {
        logger.warn(
            "SECURITY WARNING: Container lock is disabled. Multiple sessions may run concurrently on this container, which could allow cross-session access to git proxies. Only use --skip-container-lock for development/testing.",
            LogAttributes::new(),
        );
        None
    } else {
        Some(
            acquire_container_lock(&logger)
                .map_err(|error| format!("failed to acquire container lock: {error}"))?,
        )
    };

    let service_key = load_orchestrator_service_key(arguments.service_key_file.as_deref())?;
    if service_key.is_empty() {
        return Err(
            "service key is required: use --service-key-file or set ENVIRONMENT_SERVICE_KEY environment variable\n\nSet this environment variable before running the orchestrator:\n  export ENVIRONMENT_SERVICE_KEY=\"your-environment-service-key\"\n\nOr use --service-key-file to read from a file:\n  --service-key-file /path/to/service-key\n\nYou can create and manage environment service keys at claude.ai/settings\nunder your environment's settings"
                .to_owned(),
        );
    }

    let setup_cancellation = CancellationToken::new();
    let mut organization_id = arguments.organization_id.clone().unwrap_or_default();
    let mut environment_id = arguments.environment_id.clone().unwrap_or_default();
    let whoami = new_whoami_client(&arguments.api_url, &service_key, logger.clone());
    match whoami.get_identity(&setup_cancellation).await {
        Ok(identity) => {
            if !organization_id.is_empty() && organization_id != identity.organization_uuid {
                return Err(format!(
                    "provided --organization-id {:?} does not match token's organization {:?}",
                    organization_id, identity.organization_uuid
                ));
            }
            if !environment_id.is_empty() && environment_id != identity.environment_id {
                return Err(format!(
                    "provided --environment-id {:?} does not match token's environment {:?}",
                    environment_id, identity.environment_id
                ));
            }
            organization_id = identity.organization_uuid;
            environment_id = identity.environment_id;
            logger.info(
                "environment identity confirmed via whoami",
                attributes([
                    ("organization_id", json!(organization_id)),
                    ("environment_id", json!(environment_id)),
                ]),
            );
        }
        Err(error) => {
            if organization_id.is_empty() || environment_id.is_empty() {
                return Err(format!(
                    "failed to get environment identity from whoami (provide --organization-id and --environment-id to use fallback): {error}"
                ));
            }
            logger.warn(
                "whoami failed, using CLI-provided identity",
                attributes([
                    ("error", json!(error.to_string())),
                    ("organization_id", json!(organization_id)),
                    ("environment_id", json!(environment_id)),
                ]),
            );
        }
    }

    let poller: Arc<dyn PollerInterface> = if let Some(poll_hook) = &arguments.poll_hook {
        logger.info(
            "using poll-hook for polling",
            attributes([
                ("poll_hook", json!(poll_hook)),
                ("environment_id", json!(environment_id)),
            ]),
        );
        Arc::new(new_poll_hook(
            poll_hook,
            arguments.poll_hook_timeout,
            &organization_id,
            &environment_id,
            arguments.client_id.as_deref().unwrap_or_default(),
            logger.clone(),
        ))
    } else {
        Arc::new(new_poller_with_worker_id(
            &arguments.api_url,
            &environment_id,
            &service_key,
            arguments.client_id.as_deref().unwrap_or_default(),
            arguments.reclaim_older_than_ms,
            logger.clone(),
        ))
    };

    let timeout_hook = arguments.timeout_hook.as_ref().map(|command| {
        Arc::new(Hook {
            name: "timeout".to_owned(),
            command: command.clone(),
            timeout: arguments.timeout_hook_timeout,
            ..Hook::default()
        })
    });

    let sandbox_enabled = arguments.sandbox_backend == SandboxBackend::SandboxRuntime;
    if arguments.sandbox_settings.is_some() && !sandbox_enabled {
        logger.warn(
            "--sandbox-settings provided but sandbox not enabled; settings will be ignored",
            attributes([
                (
                    "sandbox_backend",
                    json!(arguments.sandbox_backend.to_string()),
                ),
                (
                    "sandbox_settings",
                    json!(
                        arguments
                            .sandbox_settings
                            .as_deref()
                            .map(Path::display)
                            .map(|path| path.to_string())
                            .unwrap_or_default()
                    ),
                ),
            ]),
        );
    }
    if sandbox_enabled {
        let target_version = std::env::var("SANDBOX_RUNTIME_VERSION").unwrap_or_default();
        install_sandbox_runtime(&setup_cancellation, &logger, &target_version)
            .await
            .map_err(|error| format!("failed to install sandbox-runtime: {error}"))?;
    }

    let working_directory = std::env::current_dir()
        .map_err(|error| format!("failed to get current working directory: {error}"))?;
    logger.info(
        "using current working directory for sandbox",
        attributes([(
            "working_dir",
            json!(working_directory.display().to_string()),
        )]),
    );

    let (execute_command, execute_arguments) = match &arguments.execute_hook {
        Some(command) => (command.clone(), Vec::new()),
        None => {
            let mut execute_arguments = vec![
                "task-run".to_owned(),
                "--stdin".to_owned(),
                "--input-format=v1".to_owned(),
                "--working-directory".to_owned(),
                working_directory.display().to_string(),
            ];
            if !organization_id.is_empty() {
                execute_arguments.extend(["--organization-id".to_owned(), organization_id]);
            }
            if !environment_id.is_empty() {
                execute_arguments.extend(["--environment-id".to_owned(), environment_id]);
            }
            if arguments.skip_git_config {
                execute_arguments.push("--skip-git-config".to_owned());
            }
            let command = executable.to_string_lossy().into_owned();
            logger.info(
                "no execute-hook provided, using self-invocation",
                attributes([
                    ("command", json!(command)),
                    ("sandbox_enabled", json!(sandbox_enabled)),
                ]),
            );
            (command, execute_arguments)
        }
    };
    if !sandbox_enabled {
        logger.warn(
            "sandbox is disabled - commands will run without sandboxing",
            attributes([(
                "sandbox_backend",
                json!(arguments.sandbox_backend.to_string()),
            )]),
        );
    }

    let execute_hook = Arc::new(Hook {
        name: "execute".to_owned(),
        command: execute_command,
        args: execute_arguments,
        timeout: arguments.execute_hook_timeout,
        sandbox_enabled,
        sandbox_config_path: arguments
            .sandbox_settings
            .as_deref()
            .map(Path::display)
            .map(|path| path.to_string())
            .unwrap_or_default(),
        working_dir: working_directory.display().to_string(),
        sandbox_adapter: sandbox_enabled
            .then(|| Arc::new(SandboxRuntimeAdapter) as Arc<dyn SandboxAdapter>),
    });
    let orchestrator = new_orchestrator(OrchestratorConfig {
        poller: Some(poller),
        timeout_hook,
        execute_hook: Some(execute_hook),
        poll_timeout: arguments.poll_timeout,
        loop_timeout: arguments.loop_timeout,
        max_poll_failures: arguments.max_poll_failures,
        logger: Some(logger.clone()),
        ..OrchestratorConfig::default()
    })
    .map_err(|error| format!("failed to create orchestrator: {error}"))?;

    run_orchestrator_until_signal(&orchestrator, &logger).await
}

/// 读取 orchestrator service key；空文件与 setup 一样继续回退环境变量。
fn load_orchestrator_service_key(service_key_file: Option<&Path>) -> Result<String, String> {
    let mut key = String::new();
    if let Some(path) = service_key_file {
        let bytes = std::fs::read(path).map_err(|error| {
            format!(
                "failed to read service key file {:?}: {error}",
                path.display().to_string()
            )
        })?;
        key = String::from_utf8_lossy(&bytes).trim().to_owned();
    }
    if key.is_empty() {
        key = std::env::var("ENVIRONMENT_SERVICE_KEY").unwrap_or_default();
    }
    Ok(key)
}

/// 运行 orchestrator，并复刻 SIGINT/SIGTERM 后最多等待 30 秒的优雅关闭窗口。
///
/// 状态机自行结束时传播非取消错误；信号路径无论最终错误或超时都返回成功，这与 Go
/// 命令为了容器编排器正常停止而采用的退出码策略一致。
async fn run_orchestrator_until_signal(
    orchestrator: &crate::internal::orchestrator::Orchestrator,
    logger: &Logger,
) -> Result<(), String> {
    use tokio::signal::unix::{SignalKind, signal};

    let cancellation = CancellationToken::new();
    let mut interrupt = signal(SignalKind::interrupt())
        .map_err(|error| format!("failed to register SIGINT handler: {error}"))?;
    let mut terminate = signal(SignalKind::terminate())
        .map_err(|error| format!("failed to register SIGTERM handler: {error}"))?;
    let mut run = Box::pin(orchestrator.run(&cancellation));
    let signal_name = tokio::select! {
        result = &mut run => {
            return match result {
                Ok(()) => {
                    logger.info("orchestrator exited normally", LogAttributes::new());
                    Ok(())
                }
                Err(error) => {
                    logger.error(
                        "orchestrator failed",
                        attributes([("error", json!(error.to_string()))]),
                    );
                    Err(error.to_string())
                }
            };
        }
        _ = interrupt.recv() => "interrupt",
        _ = terminate.recv() => "terminated",
    };
    logger.info(
        "received signal, shutting down gracefully",
        attributes([("signal", json!(signal_name))]),
    );
    cancellation.cancel();
    match tokio::time::timeout(Duration::from_secs(30), &mut run).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => logger.warn(
            "orchestrator exited with error during shutdown",
            attributes([("error", json!(error.to_string()))]),
        ),
        Err(_) => logger.warn(
            "orchestrator shutdown timed out after 30 seconds",
            LogAttributes::new(),
        ),
    }
    Ok(())
}

/// 从静态键名和值构造稳定排序的 logger 属性。
fn attributes<const N: usize>(entries: [(&str, Value); N]) -> LogAttributes {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}
