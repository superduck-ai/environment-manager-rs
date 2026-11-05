//! 为支持的 shell 生成 Clap 自动补全脚本。
//!
//! 命令树由 `clap_complete` 在运行时生成，因此补全内容与当前 Rust CLI
//! 保持一致。

use clap::{Arg, ArgMatches, Command, ValueEnum, value_parser};

use super::error::CliError;

const LONG_ABOUT: &str =
    "Generate the autocompletion script for environment-runner for the specified shell.";

/// 支持的四种补全 shell。
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CompletionShell {
    /// Bash completion v2 脚本。
    Bash,
    /// Fish completion 脚本。
    Fish,
    /// PowerShell completion 脚本。
    #[value(name = "powershell")]
    PowerShell,
    /// Zsh completion 脚本。
    Zsh,
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

/// 根据当前根命令树生成 shell completion 脚本。
#[must_use]
pub fn render_completion_for_root(version: &'static str, shell: CompletionShell) -> Vec<u8> {
    let mut command = super::command(version);
    let mut output = Vec::new();
    clap_complete::generate(
        clap_complete::Shell::from(shell),
        &mut command,
        "environment-runner",
        &mut output,
    );
    output
}

/// 一次自动补全脚本生成请求。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionArgs {
    /// 目标 shell。
    pub shell: CompletionShell,
}

/// 构造 completion 子命令树。
pub(crate) fn command() -> Command {
    Command::new("completion")
        .about("Generate the autocompletion script for the specified shell")
        .long_about(LONG_ABOUT)
        .arg(
            Arg::new("shell")
                .required(true)
                .value_name("SHELL")
                .value_parser(value_parser!(CompletionShell)),
        )
}

/// 解析 shell 位置参数。
pub(crate) fn from_matches(matches: &ArgMatches) -> Result<CompletionArgs, CliError> {
    let shell = *matches
        .get_one::<CompletionShell>("shell")
        .expect("required completion shell");
    Ok(CompletionArgs { shell })
}
