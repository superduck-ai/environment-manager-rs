//! Cobra 自动生成的 `completion` 命令在 Clap 中的显式兼容实现。
//!
//! Go 代码没有单独的源文件，因为 Cobra 会自动注入该命令；但它属于已经发布的
//! CLI 表面。这里将 shell 和 `--no-descriptions` 解析为 [`CompletionArgs`]，真正
//! 写出的脚本使用固定 Go oracle 生成资产，避免 clap_complete 的格式漂移。

use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use clap::{ArgMatches, Command};

use super::error::CliError;
use super::utils::{bool_value, switch_arg};

const LONG_ABOUT: &str = "Generate the autocompletion script for environment-runner for the specified shell.\nSee each sub-command's help for details on how to use the generated script.";

/// Cobra 原命令支持的四种补全 shell。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionShell {
    /// Bash completion v2 脚本。
    Bash,
    /// Fish completion 脚本。
    Fish,
    /// PowerShell completion 脚本。
    PowerShell,
    /// Zsh completion 脚本。
    Zsh,
}

impl Display for CompletionShell {
    /// 输出子命令使用的小写 shell 名称。
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Bash => "bash",
            Self::Fish => "fish",
            Self::PowerShell => "powershell",
            Self::Zsh => "zsh",
        })
    }
}

impl FromStr for CompletionShell {
    type Err = String;

    /// 严格解析 Cobra 暴露的 shell 子命令名称。
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "bash" => Ok(Self::Bash),
            "fish" => Ok(Self::Fish),
            "powershell" => Ok(Self::PowerShell),
            "zsh" => Ok(Self::Zsh),
            invalid => Err(format!("unsupported completion shell: {invalid}")),
        }
    }
}

impl From<CompletionShell> for clap_complete::Shell {
    /// 转成 `clap_complete` 生成器，供根入口直接输出脚本。
    fn from(value: CompletionShell) -> Self {
        match value {
            CompletionShell::Bash => Self::Bash,
            CompletionShell::Fish => Self::Fish,
            CompletionShell::PowerShell => Self::PowerShell,
            CompletionShell::Zsh => Self::Zsh,
        }
    }
}

/// 返回固定 Go/Cobra 版本生成的 shell completion 脚本。
///
/// Clap 仅解析 shell 和 `--no-descriptions`；实际脚本必须与发布过的 Cobra 生成器
/// 逐字节一致，因此不能在运行时调用 clap_complete。资产由固定 ARM64 Go oracle
/// 生成，四种 shell 均同时保存带说明与无说明版本。
#[must_use]
pub fn render_completion_script(shell: CompletionShell, no_descriptions: bool) -> &'static str {
    match (shell, no_descriptions) {
        (CompletionShell::Bash, false) => include_str!("cobra_assets/completion_bash.txt"),
        (CompletionShell::Bash, true) => {
            include_str!("cobra_assets/completion_bash_no_descriptions.txt")
        }
        (CompletionShell::Fish, false) => include_str!("cobra_assets/completion_fish.txt"),
        (CompletionShell::Fish, true) => {
            include_str!("cobra_assets/completion_fish_no_descriptions.txt")
        }
        (CompletionShell::PowerShell, false) => {
            include_str!("cobra_assets/completion_powershell.txt")
        }
        (CompletionShell::PowerShell, true) => {
            include_str!("cobra_assets/completion_powershell_no_descriptions.txt")
        }
        (CompletionShell::Zsh, false) => include_str!("cobra_assets/completion_zsh.txt"),
        (CompletionShell::Zsh, true) => {
            include_str!("cobra_assets/completion_zsh_no_descriptions.txt")
        }
    }
}

/// 一次自动补全脚本生成请求。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionArgs {
    /// 目标 shell。
    pub shell: CompletionShell,
    /// 是否从候选项中删除说明文本。
    pub no_descriptions: bool,
}

/// 构造兼容 Cobra 自动命令的 completion 子命令树。
pub(crate) fn command() -> Command {
    Command::new("completion")
        .about("Generate the autocompletion script for the specified shell")
        .long_about(LONG_ABOUT)
        .subcommand(shell_command(
            "bash",
            "Generate the autocompletion script for bash",
            "Generate the autocompletion script for the bash shell.",
        ))
        .subcommand(shell_command(
            "fish",
            "Generate the autocompletion script for fish",
            "Generate the autocompletion script for the fish shell.",
        ))
        .subcommand(shell_command(
            "powershell",
            "Generate the autocompletion script for powershell",
            "Generate the autocompletion script for powershell.",
        ))
        .subcommand(shell_command(
            "zsh",
            "Generate the autocompletion script for zsh",
            "Generate the autocompletion script for the zsh shell.",
        ))
}

/// 解析 shell 子命令；缺少 shell 时像 Cobra 一样显示 completion 帮助并成功退出。
pub(crate) fn from_matches(matches: &ArgMatches) -> Result<CompletionArgs, CliError> {
    let Some((shell_name, shell_matches)) = matches.subcommand() else {
        return Err(CliError::help(super::cobra_compat::render_completion_help(
            None,
        )));
    };
    let shell = shell_name
        .parse::<CompletionShell>()
        .map_err(CliError::internal)?;
    Ok(CompletionArgs {
        shell,
        no_descriptions: bool_value(shell_matches, "no-descriptions"),
    })
}

/// 构造一个 shell 叶子命令和 Cobra 的 `--no-descriptions` flag。
fn shell_command(name: &'static str, about: &'static str, long_about: &'static str) -> Command {
    Command::new(name)
        .about(about)
        .long_about(long_about)
        .arg(switch_arg(
            "no-descriptions",
            "disable completion descriptions",
        ))
}
