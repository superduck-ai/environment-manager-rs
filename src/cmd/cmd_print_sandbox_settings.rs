//! `print-sandbox-settings` 子命令的 CLI 外壳。
//!
//! 默认 JSON 的数据结构属于 sandbox 模块；命令解析返回显式调用标记，运行函数
//! 则向调用方提供的 writer 输出两空格缩进 JSON，便于 main 和测试共用同一路径。

use std::io::Write;

use clap::{ArgMatches, Command};

use super::error::CliError;

const LONG_ABOUT: &str = r#"Outputs the default sandbox-runtime settings as JSON to stdout.

This is useful for:
- Viewing the default network and filesystem isolation settings
- Creating a custom settings file based on the defaults
- Debugging sandbox configuration issues

The output can be redirected to a file and customized, then passed to
the orchestrator via --sandbox-settings flag."#;

const EXAMPLES: &str = r#"Examples:
  # Print default settings to stdout
  environment-runner print-sandbox-settings

  # Save to file for customization
  environment-runner print-sandbox-settings > my-sandbox-settings.json"#;

/// 无参数的默认 sandbox 配置打印请求。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PrintSandboxSettingsArgs;

/// 构造 `print-sandbox-settings` 子命令。
pub(crate) fn add_print_sandbox_settings_command() -> Command {
    Command::new("print-sandbox-settings")
        .about("Print the default sandbox runtime settings")
        .long_about(LONG_ABOUT)
        .after_long_help(EXAMPLES)
}

/// 构造 `print-sandbox-settings` 子命令的内部短别名。
///
/// 该别名只让命令树的调用点保持简洁；实现细节见
/// [`add_print_sandbox_settings_command`]。
pub(crate) fn command() -> Command {
    add_print_sandbox_settings_command()
}

/// 将匹配结果转换成无参数调用标记。
pub(crate) fn from_matches(_matches: &ArgMatches) -> Result<PrintSandboxSettingsArgs, CliError> {
    Ok(PrintSandboxSettingsArgs)
}

///
/// 将默认 sandbox-runtime 配置序列化成两空格缩进 JSON，并写入一个末尾换行。
///
/// # 参数
/// - `writer`：接收完整 JSON 的同步输出目标；生产路径传入锁定后的 stdout。
///
/// # 返回
/// 序列化或写入失败时返回带完整上下文的错误文本。
///
/// # 副作用
/// 仅写入调用方提供的 writer，不读取环境变量，也不创建文件或子进程。
pub fn run_print_sandbox_settings(writer: &mut dyn Write) -> Result<(), String> {
    let configuration = crate::internal::sandbox::default_config();
    serde_json::to_writer_pretty(&mut *writer, &configuration)
        .map_err(|error| format!("failed to serialize sandbox settings: {error}"))?;
    writer
        .write_all(b"\n")
        .map_err(|error| format!("failed to serialize sandbox settings: {error}"))
}
