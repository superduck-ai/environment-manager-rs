//! CLI 解析错误和提前退出的统一表示。
//!
//! Clap 默认把参数错误视为退出码 `2`，而旧 Go/Cobra 入口最终使用退出码 `1`
//! 并以 `Error:` 开头。这里集中处理差异，避免 `main`、测试和未来的嵌入式调用者
//! 分别实现一套输出规则。

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
    /// 用户请求帮助，或只运行了没有默认动作的根命令。
    Help,
    /// 用户请求版本信息。
    Version,
    /// Clap 检测到未知参数、缺失必填值或类型错误。
    Usage,
    /// 多个单独合法的 flag 组合后违反了旧命令的业务约束。
    Validation,
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
    /// 把 Clap 错误转换成 Cobra 兼容输出。
    ///
    /// Clap 的帮助和版本文本保持原样；其他错误把首个 `error:` 前缀改为旧入口
    /// 使用的 `Error:`，并保留 Clap 生成的 usage 与提示，兼顾兼容性和可用性。
    #[must_use]
    pub fn from_clap(error: clap::Error) -> Self {
        let clap_kind = error.kind();
        let mut rendered = error.to_string();
        let kind = match clap_kind {
            ErrorKind::DisplayHelp | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand => {
                CliErrorKind::Help
            }
            ErrorKind::DisplayVersion => CliErrorKind::Version,
            _ => CliErrorKind::Usage,
        };

        if kind == CliErrorKind::Usage {
            rendered = cobra_error_prefix(&rendered);
        }
        let exit_code = match kind {
            CliErrorKind::Help | CliErrorKind::Version => 0,
            CliErrorKind::Usage | CliErrorKind::Validation | CliErrorKind::Internal => 1,
        };
        Self {
            kind,
            rendered,
            exit_code,
        }
    }

    /// 创建根命令无子命令时的帮助提前退出。
    #[must_use]
    pub(crate) fn help(rendered: String) -> Self {
        Self {
            kind: CliErrorKind::Help,
            rendered,
            exit_code: 0,
        }
    }

    /// 创建已经带有 Cobra 固定措辞的版本提前退出。
    #[must_use]
    pub(crate) fn version(rendered: String) -> Self {
        Self {
            kind: CliErrorKind::Version,
            rendered,
            exit_code: 0,
        }
    }

    /// 创建已经包含 Cobra Error/usage/重复 Error 顺序的参数失败。
    ///
    /// 调用方必须传入最终 stderr，不能再次添加 `Error:`。该构造函数专供
    /// Cobra 兼容渲染层和发生在 Clap 解析后的跨 flag 校验使用。
    #[must_use]
    pub(crate) fn usage_rendered(rendered: String) -> Self {
        Self {
            kind: CliErrorKind::Usage,
            rendered,
            exit_code: 1,
        }
    }

    /// 创建已经按 Cobra 顺序渲染的跨 flag 业务校验失败。
    ///
    /// 该分类与普通 Clap usage 错误分开，便于嵌入式调用者和单元测试识别是多个
    /// 单独合法参数组合后失败；外部 stderr 仍使用完全相同的 Cobra 双 Error。
    #[must_use]
    pub(crate) fn validation_rendered(rendered: String) -> Self {
        Self {
            kind: CliErrorKind::Validation,
            rendered,
            exit_code: 1,
        }
    }

    /// 创建命令树和解析分派表不一致时的内部错误。
    #[must_use]
    pub(crate) fn internal(message: impl Into<String>) -> Self {
        let message = message.into();
        Self {
            kind: CliErrorKind::Internal,
            rendered: format!("Error: internal CLI wiring error: {message}\n"),
            exit_code: 1,
        }
    }

    /// 创建由子命令自行定义文本与退出码、且不追加 usage 的参数错误。
    ///
    /// `provision-packages` 在 Go 中设置 `SilenceErrors`/`SilenceUsage`，并通过
    /// `commandExitError` 把 flag/位置参数失败稳定映射为退出码 2；该构造函数只服务
    /// 这种显式命令契约，普通 Cobra 命令仍使用统一退出码 1。
    #[must_use]
    pub(crate) fn command_exit(rendered: impl Into<String>, exit_code: i32) -> Self {
        Self {
            kind: CliErrorKind::Usage,
            rendered: rendered.into(),
            exit_code,
        }
    }

    /// 返回导致解析结束的稳定分类。
    #[must_use]
    pub const fn kind(&self) -> CliErrorKind {
        self.kind
    }

    /// 返回兼容 Go 入口的进程退出码。
    #[must_use]
    pub const fn exit_code(&self) -> i32 {
        self.exit_code
    }

    /// 返回文本应写入的输出流。
    #[must_use]
    pub const fn stream(&self) -> OutputStream {
        match self.kind {
            CliErrorKind::Help | CliErrorKind::Version => OutputStream::Stdout,
            CliErrorKind::Usage | CliErrorKind::Validation | CliErrorKind::Internal => {
                OutputStream::Stderr
            }
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

/// 运行时错误在二进制入口采用的外部呈现方式。
///
/// 绝大多数错误由 Cobra 兼容层添加双 `Error:` 与 usage。唯一例外是 V0 缺失
/// `startup_context`：Go 在 Cobra 返回前就发生 runtime panic，因此必须直接写入
/// panic transcript 并以 2 退出。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeErrorPresentation {
    /// 普通 RunE 错误，由 Cobra renderer 生成三段 stderr 并以 1 退出。
    Cobra,
    /// Go runtime nil dereference 的受控 transcript，直接写 stderr 并以 2 退出。
    GoRuntimePanic,
}

/// 业务子命令返回给二进制入口的原始错误字节及呈现方式。
///
/// 大多数命令错误是 UTF-8 `String`；`task-run` 的 Claude/npm 子进程错误却可能包含
/// 任意 `CombinedOutput` 字节。统一类型让 task-run 可以把这些字节交给 Cobra 兼容
/// renderer，同时仍为日志和测试提供不会 panic 的 [`Display`] 视图。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeError {
    message: Vec<u8>,
    presentation: RuntimeErrorPresentation,
}

impl RuntimeError {
    /// 从普通 UTF-8 业务错误构造运行时错误。
    ///
    /// 文本直接转换为 UTF-8 字节，不添加 `Error:`、usage 或换行；这些固定外壳只由
    /// 最外层 Cobra renderer 添加，避免嵌套命令重复渲染。
    #[must_use]
    pub fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into().into_bytes(),
            presentation: RuntimeErrorPresentation::Cobra,
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
            presentation: RuntimeErrorPresentation::Cobra,
        }
    }

    /// 构造只用于 V0 缺失 startup context 的 Go runtime panic 输出。
    ///
    /// `transcript` 必须是由固定 Go 1.25.7 ARM64 oracle 捕获并审核过的完整 panic
    /// 后缀；调用方已经输出的 logger 行不应重复包含在这里。该构造器保持 crate 私有，
    /// 防止普通业务错误绕过 Cobra renderer 或擅自使用退出码 2。
    #[must_use]
    pub(crate) fn go_runtime_panic(transcript: Vec<u8>) -> Self {
        Self {
            message: transcript,
            presentation: RuntimeErrorPresentation::GoRuntimePanic,
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

    /// 为普通错误添加调用层上下文，同时让 Go runtime panic 原样穿过包装层。
    ///
    /// 对应 Go task-run 的 `failed to load context from stdin: %w`。真实 nil dereference
    /// 在 `%w` 执行前已经终止进程，所以 `GoRuntimePanic` 绝不能出现该前缀或 Cobra
    /// usage；其他错误仍逐字节添加前缀，避免损坏潜在的非 UTF-8 正文。
    #[must_use]
    pub(crate) fn context(self, prefix: &[u8]) -> Self {
        if self.presentation == RuntimeErrorPresentation::GoRuntimePanic {
            return self;
        }
        Self::prefixed(prefix, &self.message)
    }

    /// 返回 Cobra renderer 必须逐字节写入 stderr 的错误正文。
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.message
    }

    /// 返回错误是否必须跳过 Cobra renderer 并直接写入 stderr。
    ///
    /// 二进制入口只用该布尔值选择输出路径；普通 Rust panic 不经过 `RuntimeError`，
    /// 因而仍由标准 panic hook 处理。
    #[must_use]
    pub fn writes_directly_to_stderr(&self) -> bool {
        self.presentation == RuntimeErrorPresentation::GoRuntimePanic
    }

    /// 返回该运行时错误对应的进程退出码。
    ///
    /// Cobra RunE 错误保持 1；Go nil dereference transcript 保持 runtime 约定的 2。
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self.presentation {
            RuntimeErrorPresentation::Cobra => 1,
            RuntimeErrorPresentation::GoRuntimePanic => 2,
        }
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
    /// 兼容 stderr 不得使用本方法：普通错误应交给 `render_runtime_error_bytes`，
    /// Go runtime panic 应直接写 [`Self::as_bytes`]，两者最终都调用 `Write::write_all`。
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&go_utf8_error_view(&self.message))
    }
}

impl Error for RuntimeError {}

/// 将 Clap 小写的首个错误前缀替换为 Cobra 使用的大小写。
fn cobra_error_prefix(rendered: &str) -> String {
    rendered.strip_prefix("error:").map_or_else(
        || format!("Error: {rendered}"),
        |rest| format!("Error:{rest}"),
    )
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
