//! `setup` 子命令的参数模型与 Clap 元数据。
//!
//! 对应 Go `cmd/cmd_setup.go`。本文件既保留 Clap 参数模型，也按原顺序执行 npm
//! 预检、可选 whoami 健康检查、Claude Code 安装和 sandbox-runtime 安装。

use std::path::PathBuf;

use clap::builder::ValueParser;
use clap::{Arg, ArgMatches, Command, value_parser};
use tokio::process::Command as TokioCommand;
use tokio_util::sync::CancellationToken;

use super::error::CliError;
use super::utils::{LogLevel, bool_value, flag_arg, parse_log_level, tracing_level};
use crate::internal::claude::install_or_update_claude_code;
use crate::internal::orchestrator::new_whoami_client;
use crate::internal::process::{CaptureMode, run_command};
use crate::internal::sandbox::install_sandbox_runtime as install_sandbox_runtime_package;
use crate::internal::session::NoopActivityRecorder;

const LONG_ABOUT: &str = r#"The setup command pre-installs all required dependencies for orchestrator mode.

This command is intended to be run during container image build to ensure that
all dependencies are available when the worker starts. It installs:

  - Claude Code (@anthropic-ai/claude-code) via npm
  - Sandbox Runtime (@anthropic-ai/sandbox-runtime) via npm

Running setup does NOT disable auto-updates. The orchestrator and task-run
commands still check for updates on each session. The benefit of running setup
is faster first session startup since dependencies are pre-installed.

Prerequisites:
  - npm must be available in PATH (comes with Node.js)
  - Node.js must be installed (for running the installed packages)

Optionally, provide an environment service key to verify API connectivity:
  --service-key-file /path/to/key    Verify environment can reach the API

The healthcheck calls /v1/environments/whoami to validate the service key
and confirm network connectivity. Recommended for manual testing.

Note: Language runtimes (Node.js, Python, Go) must be pre-installed in the
container image. The environment manager only creates symlinks to these during
session initialization."#;

const EXAMPLES: &str = r#"Examples:
  environment-runner setup
  environment-runner setup --claude-code-version 2.0.20 --sandbox-runtime-version 1.2.3
  environment-runner setup --skip-sandbox-runtime
  environment-runner setup --service-key-file /path/to/key
  environment-runner setup --log-level debug"#;

/// 已验证的 setup 参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupArgs {
    /// 日志级别。
    pub log_level: LogLevel,
    /// 要安装的 Claude Code 版本。
    pub claude_code_version: String,
    /// 要安装的 sandbox-runtime 版本。
    pub sandbox_runtime_version: String,
    /// 是否跳过 Claude Code 安装。
    pub skip_claude_code: bool,
    /// 是否跳过 sandbox-runtime 安装。
    pub skip_sandbox_runtime: bool,
    /// 可选健康检查使用的 API 基地址。
    pub api_url: String,
    /// 可选 service key 文件；未提供时运行时检查环境变量。
    pub service_key_file: Option<PathBuf>,
}

/// 构造 `setup` 子命令及全部 flag。
pub(crate) fn add_setup_command() -> Command {
    Command::new("setup")
        .about("Install dependencies for orchestrator mode")
        .long_about(LONG_ABOUT)
        .after_long_help(EXAMPLES)
        .arg(Arg::new("log-level").long("log-level").default_value("info").help("Log level (debug, info, warn, error)").value_parser(ValueParser::new(parse_log_level)))
        .arg(Arg::new("claude-code-version").long("claude-code-version").default_value("latest").help("Version of Claude Code to install (latest, stable, or specific version like 2.0.20)"))
        .arg(Arg::new("sandbox-runtime-version").long("sandbox-runtime-version").default_value("latest").help("Version of sandbox-runtime to install (latest or specific version)"))
        .arg(flag_arg("skip-claude-code", "Skip Claude Code installation"))
        .arg(flag_arg("skip-sandbox-runtime", "Skip sandbox-runtime installation"))
        .arg(Arg::new("api-url").long("api-url").default_value("https://api.anthropic.com").help("API base URL for connectivity healthcheck"))
        .arg(Arg::new("service-key-file").long("service-key-file").help("Path to environment service key file for API healthcheck. Falls back to ENVIRONMENT_SERVICE_KEY env var if not set. When provided, verifies connectivity by calling /v1/environments/whoami").value_parser(value_parser!(PathBuf)))
}

/// 将 Clap matches 转换为运行时独立的 setup 参数。
pub(crate) fn from_matches(matches: &ArgMatches) -> Result<SetupArgs, CliError> {
    Ok(SetupArgs {
        log_level: *matches
            .get_one::<LogLevel>("log-level")
            .expect("defaulted log level"),
        claude_code_version: matches
            .get_one::<String>("claude-code-version")
            .expect("defaulted Claude version")
            .clone(),
        sandbox_runtime_version: matches
            .get_one::<String>("sandbox-runtime-version")
            .expect("defaulted sandbox version")
            .clone(),
        skip_claude_code: bool_value(matches, "skip-claude-code"),
        skip_sandbox_runtime: bool_value(matches, "skip-sandbox-runtime"),
        api_url: matches
            .get_one::<String>("api-url")
            .expect("defaulted api-url")
            .clone(),
        service_key_file: matches.get_one::<PathBuf>("service-key-file").cloned(),
    })
}

/// 对应 Go：`cmd/cmd_setup.go` 中的 `loadServiceKey`。
///
/// 优先读取显式文件并清除首尾空白；文件内容为空时继续回退
/// `ENVIRONMENT_SERVICE_KEY`。文件无法读取时返回带路径的兼容错误，且不会把 secret
/// 写入日志。
pub(super) fn load_service_key(
    service_key_file: Option<&std::path::Path>,
) -> Result<String, String> {
    let mut service_key = String::new();
    if let Some(path) = service_key_file {
        let bytes = std::fs::read(path).map_err(|error| {
            format!(
                "failed to read service key file {:?}: {error}",
                path.display().to_string()
            )
        })?;
        service_key = String::from_utf8_lossy(&bytes).trim().to_owned();
    }
    if service_key.is_empty() {
        service_key = std::env::var("ENVIRONMENT_SERVICE_KEY").unwrap_or_default();
    }
    Ok(service_key)
}

/// 对应 Go：`cmd/cmd_setup.go` 中的 `runAPIHealthcheck`。
///
/// 使用 service key 调用 `/v1/environments/whoami`，验证认证与网络连通性。取消令牌
/// 会中断请求；失败时保留 whoami 的状态码或解析原因，并增加 setup 层上下文。
pub(super) async fn run_api_healthcheck(
    cancellation: &CancellationToken,
    api_url: &str,
    service_key: &str,
) -> Result<(), String> {
    tracing::info!("{}", "Running API connectivity healthcheck");
    let client = new_whoami_client(api_url, service_key);
    let identity = client
        .get_identity(cancellation)
        .await
        .map_err(|error| format!("API connectivity healthcheck failed: {error}"))?;
    tracing::info!("environment_id" = ?(identity.environment_id), "organization_uuid" = ?(identity.organization_uuid), "{}", "API connectivity verified");
    Ok(())
}

/// 对应 Go：`cmd/cmd_setup.go` 中的 `runSetup`。
///
/// 严格按“初始化 tracing → npm 预检 → 可选 API 健康检查 → Claude → SRT”的顺序执行。
/// 两个安装都跳过时不会要求 npm；没有 service key 时健康检查也只记录跳过信息。
///
/// # 副作用
/// 可能读取 secret 文件和环境变量、追加 `/tmp/env-manager.log`、运行 npm/claude
/// 子进程并访问 whoami HTTP 接口。取消只终止当前直接子进程或请求。
pub async fn run_setup(
    arguments: &SetupArgs,
    cancellation: &CancellationToken,
) -> Result<(), String> {
    crate::internal::logger::initialize(tracing_level(arguments.log_level));
    tracing::info!("claude_code_version" = ?(arguments.claude_code_version), "sandbox_runtime_version" = ?(arguments.sandbox_runtime_version), "skip_claude_code" = ?(arguments.skip_claude_code), "skip_sandbox_runtime" = ?(arguments.skip_sandbox_runtime), "{}", "Starting setup");

    if !arguments.skip_claude_code || !arguments.skip_sandbox_runtime {
        run_preflight_checks(cancellation).await?;
    }

    let service_key = load_service_key(arguments.service_key_file.as_deref())?;
    if service_key.is_empty() {
        tracing::info!("{}", "Skipping API healthcheck (no service key provided)");
    } else {
        run_api_healthcheck(cancellation, &arguments.api_url, &service_key).await?;
    }

    if arguments.skip_claude_code {
        tracing::info!(
            "{}",
            "Skipping Claude Code installation (--skip-claude-code)"
        );
    } else {
        install_claude_code(cancellation, &arguments.claude_code_version).await?;
    }

    if arguments.skip_sandbox_runtime {
        tracing::info!(
            "{}",
            "Skipping sandbox-runtime installation (--skip-sandbox-runtime)"
        );
    } else {
        install_sandbox_runtime(cancellation, &arguments.sandbox_runtime_version).await?;
    }

    tracing::info!(
        "{}",
        "Setup complete. Worker image is ready for orchestrator mode."
    );
    Ok(())
}

/// 对应 Go：`cmd/cmd_setup.go` 中的 `runPreflightChecks`。
///
/// 当前预检只有 npm，但保留单独编排函数和日志顺序，方便后续与 Go 差分时准确定位
/// 是预检框架还是 npm 探测发生变化。
async fn run_preflight_checks(cancellation: &CancellationToken) -> Result<(), String> {
    tracing::info!("{}", "Running pre-flight checks");
    check_npm_available(cancellation).await?;
    tracing::info!("{}", "Pre-flight checks passed");
    Ok(())
}

/// 通过 `PATH` 启动 `npm --version`，只在退出成功时视为可用。为兼容 Go，错误包含
/// 多行安装提示；取消只终止 npm 这个直接子进程，不操作进程组。
pub(super) async fn check_npm_available(cancellation: &CancellationToken) -> Result<(), String> {
    tracing::debug!("{}", "Checking npm availability");
    let mut command = TokioCommand::new("npm");
    command.arg("--version");
    let output = run_command(command, cancellation, None, None, CaptureMode::Separate)
        .await
        .map_err(|error| npm_preflight_error(&error.to_string()))?;
    if !output.status.success() {
        return Err(npm_preflight_error(&output.status.to_string()));
    }
    let (stdout, _) = output.output.expect_separate();
    tracing::debug!("version" = ?(String::from_utf8_lossy(&stdout)), "{}", "npm is available");
    Ok(())
}

/// 生成 npm 预检的稳定多行错误文本，不暴露 stderr 中可能包含的环境信息。
fn npm_preflight_error(error: &str) -> String {
    format!(
        "pre-flight check failed: npm not found in PATH\n\nnpm is required to install Claude Code and sandbox-runtime.\nPlease install Node.js (which includes npm) before running setup.\n\nError: {error}"
    )
}

/// 对应 Go：`cmd/cmd_setup.go` 中的 `installClaudeCode`。
///
/// `CLAUDE_DEFAULT_PATH` 为空时使用 `claude`，并用无操作活动记录器调用共享安装器。
/// 安装器会根据版本决定安装或升级；这里仅增加 setup 命令的稳定错误上下文。
async fn install_claude_code(
    cancellation: &CancellationToken,
    version: &str,
) -> Result<(), String> {
    tracing::info!("version" = ?(version), "{}", "Installing Claude Code");
    let claude_path = std::env::var("CLAUDE_DEFAULT_PATH")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "claude".to_owned());
    install_or_update_claude_code(cancellation, &claude_path, version, &NoopActivityRecorder)
        .await
        .map_err(|error| format!("failed to install Claude Code: {error}"))?;
    tracing::info!("{}", "Claude Code installation completed");
    Ok(())
}

/// 对应 Go：`cmd/cmd_setup.go` 中的 `installSandboxRuntime`。
///
/// 调用共享 SRT 安装器并保留 setup 层包装文本；安装器内部在升级失败且已有可执行
/// 文件时继续使用旧版本，`current` 则完全跳过 npm。
pub(super) async fn install_sandbox_runtime(
    cancellation: &CancellationToken,
    version: &str,
) -> Result<(), String> {
    tracing::info!("version" = ?(version), "{}", "Installing sandbox-runtime");
    install_sandbox_runtime_package(cancellation, version)
        .await
        .map_err(|error| format!("failed to install sandbox-runtime: {error}"))?;
    tracing::info!("{}", "sandbox-runtime installation completed");
    Ok(())
}
