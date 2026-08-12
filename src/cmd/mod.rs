//! Environment Runner 的命令行解析层。
//!
//! 这个模块只负责三件事：构造稳定的 Clap 命令树、把原始参数解析为强类型
//! [`Invocation`]，以及把 Clap 的提前退出/参数错误交给二进制入口。
//! 真正的网络、进程、沙箱和会话执行不放在解析层；上层必须显式匹配
//! [`Invocation`] 并调用相应运行时，避免解析成功被误当成业务执行成功。

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

pub use code_sign::{CodeSignArgs, CodeSignOperation, SshSignArgs, run_code_sign};
pub use completion::{CompletionArgs, CompletionShell, render_completion_for_root};
pub use error::{CliError, CliErrorKind, OutputStream, RuntimeError, render_runtime_error};
pub use orchestrator::{OrchestratorArgs, SandboxBackend, run_orchestrator};
pub use poll::{PollArgs, run_poll};
pub use print_sandbox_settings::{PrintSandboxSettingsArgs, run_print_sandbox_settings};
pub use provision_packages::{ProvisionPackagesArgs, run_provision_packages};
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
    /// 为指定 shell 生成自动补全脚本。
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
/// `version` 使用静态字符串，是为了同时支持 Cargo 构建版本与发布流水线在编译期
/// 注入的版本。隐藏的 `code-sign` 仍可直接解析，但不会出现在普通帮助中。
#[must_use]
pub fn command(version: &'static str) -> Command {
    Command::new("environment-runner")
        .version(version)
        .disable_version_flag(true)
        .arg(
            Arg::new("version")
                .short('v')
                .visible_short_alias('V')
                .long("version")
                .action(ArgAction::Version)
                .help("Print version"),
        )
        .about("Manages environments for agentic sessions")
        .long_about(ROOT_LONG_ABOUT)
        .subcommand_required(true)
        .arg_required_else_help(true)
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
/// `code-sign` 子命令。入口检查 `argv[0]` 的 basename，并在它后面插入
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
/// 帮助和版本通过 [`CliError`] 表达为退出码 `0` 的“成功型提前退出”；
/// 其余参数错误保留 Clap 的原生退出码。这样的
/// 返回值让 `main` 能精确选择 stdout/stderr，同时避免调用 Clap 的 `exit()`。
pub fn parse_from<I, T>(version: &'static str, argv: I) -> Result<Invocation, CliError>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let argv = normalize_argv_for_codesign(argv);
    // Git 会把 OpenSSH 参数原样交给这个包装器；Clap 即使给位置
    // 参数开启 `allow_hyphen_values`，仍会消费 `--` 这个分隔符。code-sign 是 Git
    // 调用的 argv 兼容入口，因此在识别到隐藏子命令后直接截取原始 OsString，避免
    // `--`、非 UTF-8 路径或未来 OpenSSH flag 被解析器改写。普通命令仍走完整 Clap
    // 校验，父级的 orchestrator/setup 等接线不受影响。
    if argv.get(1).is_some_and(|argument| argument == "code-sign") {
        return Ok(Invocation::CodeSign(CodeSignArgs {
            raw_args: argv.into_iter().skip(2).collect(),
        }));
    }
    let matches = command(version)
        .try_get_matches_from(argv)
        .map_err(CliError::from_clap)?;

    let Some((name, subcommand_matches)) = matches.subcommand() else {
        return Err(CliError::internal("required subcommand was not parsed"));
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
