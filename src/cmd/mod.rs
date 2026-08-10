//! Environment Runner 的命令行兼容层。
//!
//! 这个模块只负责三件事：构造稳定的 Clap 命令树、把原始参数解析为强类型
//! [`Invocation`]，以及把 Clap 的提前退出/参数错误转换成接近 Cobra 的输出约定。
//! 真正的网络、进程、沙箱和会话执行不放在解析层；上层必须显式匹配
//! [`Invocation`] 并调用相应运行时，避免解析成功被误当成业务执行成功。

mod cobra_compat;
#[path = "cmd_code_sign.rs"]
mod code_sign;
mod completion;
mod error;
#[path = "cmd_orchestrator.rs"]
mod orchestrator;
#[path = "cmd_poll.rs"]
mod poll;
#[path = "cmd_print_sandbox_settings.rs"]
mod print_sandbox_settings;
#[path = "cmd_provision_packages.rs"]
mod provision_packages;
#[path = "cmd_setup.rs"]
mod setup;
#[path = "cmd_task_run.rs"]
mod task_run;
mod utils;
mod zz_recovered_types;

use std::ffi::OsString;

use clap::{Arg, ArgAction, Command};

pub use cobra_compat::{
    CobraCommand, render_runtime_error, render_runtime_error_bytes, render_task_run_runtime_error,
};
pub use code_sign::{CodeSignArgs, CodeSignOperation, SshSignArgs, run_code_sign};
pub use completion::{CompletionArgs, CompletionShell, render_completion_script};
pub use error::{CliError, CliErrorKind, OutputStream, RuntimeError};
pub use orchestrator::{OrchestratorArgs, SandboxBackend, run_orchestrator};
pub use poll::{PollArgs, run_poll};
pub use print_sandbox_settings::{PrintSandboxSettingsArgs, run_print_sandbox_settings};
pub use provision_packages::{
    CommandExitError, ProvisionPackagesArgs, exit_code as provision_packages_exit_code,
    run_provision_packages,
};
pub use setup::{SetupArgs, run_setup};
pub use task_run::{InputFormat, SessionMode, TaskRunArgs, run_task_run};
pub use utils::{LogLevel, parse_go_duration, parse_log_level};

const ROOT_LONG_ABOUT: &str = "Environment Runner handles the lifecycle of agentic sessions.";

/// 一次已经完成 CLI 校验、但尚未执行的命令调用。
///
/// 该枚举是 CLI 与异步运行时之间唯一需要依赖的接线面。每个分支都保留了 Go
/// 命令的默认值和“flag 是否由用户显式提供”等语义；执行层不需要再次读取 Clap
/// 的 `ArgMatches`，也不会因命令尚未迁移而得到一个空的成功结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Invocation {
    /// 为指定 shell 生成自动补全脚本的 Cobra 兼容命令。
    Completion(CompletionArgs),
    /// 持续轮询并调度会话工作的 orchestrator 命令。
    Orchestrator(OrchestratorArgs),
    /// 执行单个 stdin 会话负载的 task-run 命令。
    TaskRun(TaskRunArgs),
    /// 发起一次非阻塞取任务请求的 poll 命令。
    Poll(PollArgs),
    /// 从严格 v1 stdin manifest 一次性安装系统与语言 package。
    ProvisionPackages(ProvisionPackagesArgs),
    /// Git/SSH 通过 argv0 或隐藏子命令调用的签名包装器。
    CodeSign(CodeSignArgs),
    /// 输出默认 sandbox-runtime 配置的命令。
    PrintSandboxSettings(PrintSandboxSettingsArgs),
    /// 预安装 Claude Code 与 sandbox-runtime 的 setup 命令。
    Setup(SetupArgs),
}

/// 构造完整的 Environment Runner Clap 命令树。
///
/// `version` 使用静态字符串，是为了同时兼容 Cargo 构建版本与发布流水线在编译期
/// 注入的版本。公开子命令按 Cobra 帮助的稳定顺序注册；隐藏的 `code-sign` 仍可
/// 直接解析，但不会出现在普通帮助中。Cobra 自动注入的 completion 也显式建模。
#[must_use]
pub fn command(version: &'static str) -> Command {
    Command::new("environment-runner")
        .version(version)
        .disable_version_flag(true)
        .about("Manages environments for agentic sessions")
        .long_about(ROOT_LONG_ABOUT)
        .arg(
            Arg::new("version")
                .short('v')
                .long("version")
                .help("version for environment-runner")
                .action(ArgAction::Version),
        )
        .subcommand(completion::command())
        .subcommand(orchestrator::add_orchestrator_command())
        .subcommand(task_run::add_task_run_command())
        .subcommand(provision_packages::add_provision_packages_command())
        .subcommand(poll::add_poll_command())
        .subcommand(print_sandbox_settings::command())
        .subcommand(setup::add_setup_command())
        .subcommand(code_sign::add_code_sign_command())
}

/// 按 Go 入口的 argv0 规则规范化原始参数。
///
/// Git 把 `gpg.ssh.program` 配成名为 `code-sign` 的可执行文件时，不会再传入
/// `code-sign` 子命令。旧 Go 入口会检查 `argv[0]` 的 basename，并在它后面插入
/// 隐藏子命令名；这里逐字保留该行为。参数使用 [`OsString`]，因此签名文件路径即使
/// 不是 UTF-8 也不会在规范化阶段丢失。
#[must_use]
pub fn normalize_argv_for_codesign<I, T>(argv: I) -> Vec<OsString>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let mut normalized: Vec<OsString> = argv.into_iter().map(Into::into).collect();
    let invoked_as_code_sign = normalized
        .first()
        .and_then(|value| std::path::Path::new(value).file_name())
        .is_some_and(|name| name == "code-sign");

    if invoked_as_code_sign {
        normalized.insert(1, OsString::from("code-sign"));
    }
    normalized
}

/// 解析原始 argv，并返回等待运行时执行的强类型调用。
///
/// 帮助、版本以及根命令无子命令这三种情况通过 [`CliError`] 表达为退出码 `0`
/// 的“成功型提前退出”；其余 Clap 错误和跨 flag 业务校验使用退出码 `1`。这样的
/// 返回值让 `main` 能精确选择 stdout/stderr，同时避免调用 Clap 的 `exit()`。
pub fn parse_from<I, T>(version: &'static str, argv: I) -> Result<Invocation, CliError>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let argv = normalize_argv_for_codesign(argv);
    // Cobra 的 `DisableFlagParsing` 会把 `--` 本身也原样交给 RunE；Clap 即使给位置
    // 参数开启 `allow_hyphen_values`，仍会消费 `--` 这个分隔符。code-sign 是 Git
    // 调用的 argv 兼容入口，因此在识别到隐藏子命令后直接截取原始 OsString，避免
    // `--`、非 UTF-8 路径或未来 OpenSSH flag 被解析器改写。普通命令仍走完整 Clap
    // 校验，父级的 orchestrator/setup 等接线不受影响。
    if argv.get(1).is_some_and(|argument| argument == "code-sign") {
        return Ok(Invocation::CodeSign(CodeSignArgs {
            raw_args: argv.into_iter().skip(2).collect(),
        }));
    }
    if let Some(help) = cobra_compat::requested_help(&argv) {
        return Err(CliError::help(help));
    }
    if let Some(version_text) = cobra_compat::requested_version(version, &argv) {
        return Err(CliError::version(version_text));
    }
    let matches = command(version)
        .try_get_matches_from(argv.clone())
        .map_err(|error| cobra_compat::render_clap_error(&argv, &error))?;

    let Some((name, subcommand_matches)) = matches.subcommand() else {
        return Err(CliError::help(cobra_compat::render_help(
            CobraCommand::Root,
        )));
    };

    match name {
        "completion" => completion::from_matches(subcommand_matches).map(Invocation::Completion),
        "orchestrator" => {
            orchestrator::from_matches(subcommand_matches).map(Invocation::Orchestrator)
        }
        "task-run" => task_run::from_matches(subcommand_matches).map(Invocation::TaskRun),
        "provision-packages" => {
            provision_packages::from_matches(subcommand_matches).map(Invocation::ProvisionPackages)
        }
        "poll" => poll::from_matches(subcommand_matches).map(Invocation::Poll),
        "code-sign" => code_sign::from_matches(subcommand_matches).map(Invocation::CodeSign),
        "print-sandbox-settings" => print_sandbox_settings::from_matches(subcommand_matches)
            .map(Invocation::PrintSandboxSettings),
        "setup" => setup::from_matches(subcommand_matches).map(Invocation::Setup),
        // 命令树和分派表在同一个模块中构造；走到这里说明代码维护时漏接了分支。
        unknown => Err(CliError::internal(format!(
            "parsed command {unknown:?} has no dispatch mapping"
        ))),
    }
}

#[cfg(test)]
#[path = "cmd_cli_test.rs"]
mod cmd_cli_test;

#[cfg(test)]
#[path = "cmd_task_run_test.rs"]
mod cmd_task_run_test;
