//! CLI 解析错误和提前退出的统一表示。
//!
//! Clap 产生最终的帮助、版本和参数错误文本；这里只保留输出流和退出码，
//! 便于二进制入口在不调用 `clap::Error::exit` 的情况下测试。

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use clap::error::ErrorKind;

/// CLI 文本应该写入的标准流。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputStream {
    /// 正常帮助和版本信息写入 stdout。
    Stdout,
    /// 参数、校验和内部接线错误写入 stderr。
    Stderr,
}

/// CLI 结束解析的原因分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliErrorKind {
    /// 用户请求帮助。
    Help,
    /// 用户请求版本信息。
    Version,
    /// Clap 检测到未知参数、缺失必填值或类型错误。
    Usage,
    /// 命令树与强类型分派表不一致，属于程序接线错误。
    Internal,
}

/// 可由上层无副作用地渲染和退出的 CLI 错误。
///
/// 名称保留 `Error` 是为了符合 Rust API 惯例，但 `Help` 和 `Version` 的
/// `exit_code()` 为零；调用方应始终使用这里记录的流和退出码，不应仅凭
/// `Result::Err` 推断执行失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliError {
    kind: CliErrorKind,
    rendered: String,
    exit_code: i32,
}

impl CliError {
    /// 保留 Clap 的原生文本、错误分类和退出码。
    #[must_use]
    pub fn from_clap(error: clap::Error) -> Self {
        let clap_kind = error.kind();
        let kind = match clap_kind {
            ErrorKind::DisplayHelp => CliErrorKind::Help,
            ErrorKind::DisplayVersion => CliErrorKind::Version,
            _ => CliErrorKind::Usage,
        };
        Self {
            kind,
            rendered: error.to_string(),
            exit_code: error.exit_code(),
        }
    }

    /// 创建命令树和解析分派表不一致时的内部错误。
    #[must_use]
    pub(crate) fn internal(message: impl Into<String>) -> Self {
        let message = message.into();
        Self {
            kind: CliErrorKind::Internal,
            rendered: format!("error: internal CLI wiring error: {message}\n"),
            exit_code: 1,
        }
    }

    /// 返回导致解析结束的稳定分类。
    #[must_use]
    pub const fn kind(&self) -> CliErrorKind {
        self.kind
    }

    /// 返回进程退出码。
    #[must_use]
    pub const fn exit_code(&self) -> i32 {
        self.exit_code
    }

    /// 返回文本应写入的输出流。
    #[must_use]
    pub const fn stream(&self) -> OutputStream {
        match self.kind {
            CliErrorKind::Help | CliErrorKind::Version => OutputStream::Stdout,
            CliErrorKind::Usage | CliErrorKind::Internal => OutputStream::Stderr,
        }
    }

    /// 返回已经包含换行和前缀的最终输出文本。
    #[must_use]
    pub fn rendered(&self) -> &str {
        &self.rendered
    }
}

impl Display for CliError {
    /// 直接展示最终可写入终端的文本，不额外添加第二个 `Error:` 前缀。
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.rendered)
    }
}

impl Error for CliError {}

/// 业务子命令返回给二进制入口的原始错误字节。
///
/// 大多数命令错误是 UTF-8 `String`；`task-run` 的 Claude/npm 子进程错误却可能包含
/// 任意 `CombinedOutput` 字节。统一类型让 task-run 可以无损传递这些字节，同时仍为
/// 日志和测试提供不会 panic 的 [`Display`] 视图。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeError {
    message: Vec<u8>,
    exit_code: i32,
}

impl RuntimeError {
    /// 从普通 UTF-8 业务错误构造运行时错误。
    ///
    /// 文本直接转换为 UTF-8 字节，不添加外层错误前缀或换行。
    #[must_use]
    pub fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into().into_bytes(),
            exit_code: 1,
        }
    }

    /// 从 UTF-8 业务错误和命令定义的退出码构造运行时错误。
    #[must_use]
    pub(crate) fn with_exit_code(message: impl Into<String>, exit_code: i32) -> Self {
        Self {
            message: message.into().into_bytes(),
            exit_code,
        }
    }

    /// 接收 manager 交出的任意原始错误字节。
    ///
    /// 该入口限制在 crate 内，外部调用者应使用 [`Self::message`]；task-run 适配层
    /// 使用它无损转交 Claude/npm 输出。
    #[must_use]
    pub(crate) fn message_bytes(message: Vec<u8>) -> Self {
        Self {
            message,
            exit_code: 1,
        }
    }

    /// 给原始下层错误添加兼容 Go `%w` 的静态字节前缀。
    ///
    /// 返回值不经过 UTF-8 解码；用于 `environment manager failed: %w` 这类可能包裹
    /// 非 UTF-8 Claude 错误的路径。
    #[must_use]
    pub(crate) fn prefixed(prefix: &[u8], error: &[u8]) -> Self {
        let mut message = Vec::with_capacity(prefix.len() + error.len());
        message.extend_from_slice(prefix);
        message.extend_from_slice(error);
        Self::message_bytes(message)
    }

    /// 为错误添加调用层上下文，并保留潜在的非 UTF-8 正文。
    #[must_use]
    pub(crate) fn context(self, prefix: &[u8]) -> Self {
        let mut message = Vec::with_capacity(prefix.len() + self.message.len());
        message.extend_from_slice(prefix);
        message.extend_from_slice(&self.message);
        Self {
            message,
            exit_code: self.exit_code,
        }
    }

    /// 返回必须逐字节写入 stderr 的错误正文。
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.message
    }

    /// 返回命令失败时使用的进程退出码。
    #[must_use]
    pub const fn exit_code(&self) -> i32 {
        self.exit_code
    }
}

impl From<String> for RuntimeError {
    /// 把现有命令函数的 UTF-8 `String` 错误无损提升为运行时错误。
    fn from(message: String) -> Self {
        Self::message(message)
    }
}

impl Display for RuntimeError {
    /// 为日志、断言和诊断输出提供 Go 风格的逐坏字节替换视图。
    ///
    /// 原始 stderr 输出不得使用本方法；它会用替换字符显示非 UTF-8 字节。
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&go_utf8_error_view(&self.message))
    }
}

impl Error for RuntimeError {}

/// 把运行时错误转成可直接写入 stderr 的规范字节和退出码。
#[must_use]
pub fn render_runtime_error(error: &RuntimeError) -> (Vec<u8>, i32) {
    let mut rendered = Vec::with_capacity("Error: ".len() + error.as_bytes().len() + 1);
    rendered.extend_from_slice(b"Error: ");
    rendered.extend_from_slice(error.as_bytes());
    rendered.push(b'\n');
    (rendered, error.exit_code())
}

/// 将任意错误字节转换成 Go `encoding/json` 采用的逐坏字节替换文本。
fn go_utf8_error_view(value: &[u8]) -> String {
    let mut output = String::new();
    let mut remaining = value;
    while !remaining.is_empty() {
        match std::str::from_utf8(remaining) {
            Ok(valid) => {
                output.push_str(valid);
                break;
            }
            Err(error) => {
                let valid_end = error.valid_up_to();
                // `valid_up_to` 保证该切片为合法 UTF-8；格式化错误本身不能因损坏的
                // 子进程输出而 panic，所以失败时保守使用空合法前缀。
                output.push_str(std::str::from_utf8(&remaining[..valid_end]).unwrap_or_default());
                output.push('\u{fffd}');
                remaining = &remaining[valid_end + 1..];
            }
        }
    }
    output
}
