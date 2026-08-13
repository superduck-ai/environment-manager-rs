//! Claude 执行器、跨子系统适配 trait 与共享错误声明。
//!
//! 结构体中仅保存而不读取的 MCP registration 和 lease manager 仍原样保存在
//! 执行器上。MCP 以无行为 marker trait 隔离，o11y 则通过逐事件
//! adapter 暴露；没有 adapter 时不会伪造计数器或诊断落盘。

use std::{
    ffi::{OsStr, OsString},
    path::Path,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::internal::{
    auth::AuthContext,
    config::{ClaudeConfig, StartupContext},
    envtype::LeaseManagerHandle,
    process::{CaptureMode, CommandFailure, CommandOutput, run_command},
    session::ActivityRecorder,
};

use super::Outcomes;

/// Claude 子系统对外返回的稳定文本错误。
///
/// 调用链使用 `source chain` 动态包装不同错误类型；边界保留最终稳定文案，
/// 具体 I/O 原因在构造时已经进入该文本和结构化日志。
#[derive(Debug)]
pub struct ClaudeError {
    message: Vec<u8>,
}

impl ClaudeError {
    /// 从已经包含完整上下文的消息构造错误。
    #[must_use]
    pub fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into().into_bytes(),
        }
    }

    /// 从可能包含非 UTF-8 子进程输出的完整错误字节构造错误。
    ///
    /// 稳定错误 string 本身是任意字节序列；保存为 `Vec<u8>` 可让最外层 CLI 使用
    /// `Write::write_all` 原样输出。普通 Rust `Display` 只能提供有损的人类可读视图。
    #[must_use]
    pub(crate) fn message_bytes(message: Vec<u8>) -> Self {
        Self { message }
    }

    /// 返回错误的原始字节，不执行 UTF-8 替换。
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.message
    }

    /// 消费错误并取出原始字节，供 CLI 错误适配层无复制转发。
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.message
    }
}

impl std::fmt::Display for ClaudeError {
    /// 为只接受 `Display` 的日志/诊断接口提供 UTF-8 替换视图。
    ///
    /// 原始 stderr 不应经过此方法，应调用 `as_bytes` 或 `into_bytes`。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&go_utf8_error_view(&self.message))
    }
}

impl std::error::Error for ClaudeError {}

/// 为日志/Display 将每个无效字节分别替换为 U+FFFD。
///
/// 每个无效输入字节都产生一个替换字符；`from_utf8_lossy` 可能合并坏字节段，
/// 因此不能直接使用。
pub(super) fn go_utf8_error_view(value: &[u8]) -> String {
    let mut output = String::new();
    let mut remaining = value;
    while !remaining.is_empty() {
        match std::str::from_utf8(remaining) {
            Ok(valid) => {
                output.push_str(valid);
                break;
            }
            Err(error) => {
                let valid = &remaining[..error.valid_up_to()];
                output.push_str(std::str::from_utf8(valid).unwrap_or_default());
                output.push('\u{fffd}');
                remaining = &remaining[error.valid_up_to() + 1..];
            }
        }
    }
    output
}

/// MCP registry 与 Claude 执行器之间的最小不透明 registration 边界。
///
/// Claude 执行器只保存这些对象而不读取任何字段，因此 trait 故意没有方法；
/// 真实 MCP registration 实现该 marker，无需在 Claude 模块重复配置结构。
pub trait McpServerRegistration: Send + Sync {}

/// o11y/diag 与 Claude 执行器之间的同步事件适配接口。
///
/// 方法与 调用点，避免把 metric 和 diagnostic 合并成一条虚构事件。
pub trait ClaudeObservability: Send + Sync {
    /// 返回本次会话应注入 Claude 的诊断文件路径；`None` 表示诊断落盘未启用。
    fn claude_code_diagnostics_file_path(&self) -> Option<String>;

    /// 增加 Claude Code 启动计数器。
    fn increment_claude_code_start(&self);

    /// 记录无 PII 的 `claude_code_start` 诊断事件。
    fn log_claude_code_start(&self);

    /// 记录无 PII 的 `claude_code_end` 诊断事件。
    fn log_claude_code_end(&self, is_error: bool, exit_code: i32, duration_ms: i64);

    /// 增加带成功/失败维度的 Claude Code 结束计数器。
    ///
    /// `exit_code` 仅在子进程确实启动并退出时存在；启动、等待或取消错误传 `None`。
    /// 不能只传失败布尔值，否则 OTel 的 `error_code` 标签会丢失错误链中的真实退出码。
    fn increment_claude_code_end(&self, is_error: bool, exit_code: Option<i32>);

    /// 记录 `claude_init_success` 或 `claude_init_failed` 诊断事件。
    fn log_claude_init(&self, succeeded: bool, exit_code: Option<i32>, duration_ms: i64);

    /// 记录存在旧版本兜底时的非致命更新失败诊断。
    fn log_claude_update_failed_nonfatal(&self, operation: &str);
}

/// Claude Code 的可共享异步执行接口。
#[async_trait]
pub trait Executor: Send + Sync {
    /// 执行单个 Claude 会话；取消只向 direct child 发送 SIGKILL。
    async fn execute(&self, cancellation: &CancellationToken) -> Result<(), ClaudeError>;

    /// 幂等清理 session token 文件，不终止其他进程。
    async fn destroy(&self, cancellation: &CancellationToken) -> Result<(), ClaudeError>;
}

/// Claude Code 单会话执行器。
pub struct ClaudeCodeExecutor {
    pub(super) cfg: Option<ClaudeConfig>,
    pub(super) session_id: String,
    pub(super) claude_path: String,
    pub(super) startup_context: StartupContext,
    pub(super) auth_context: Option<AuthContext>,
    pub(super) api_base_url: String,
    pub(super) _outcomes: Option<Outcomes>,
    pub(super) _mcp_registrations: Vec<Arc<dyn McpServerRegistration>>,
    pub(super) activity_recorder: Arc<dyn ActivityRecorder>,
    pub(super) _lease_manager: Option<Arc<dyn LeaseManagerHandle>>,
    pub(super) observability: Option<Arc<dyn ClaudeObservability>>,
}

impl std::fmt::Debug for ClaudeCodeExecutor {
    /// 只输出非敏感执行元数据，绝不展开 auth context、handler 或 token。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClaudeCodeExecutor")
            .field("session_id", &self.session_id)
            .field("claude_path", &self.claude_path)
            .field("api_base_url", &self.api_base_url)
            .field("has_auth_context", &self.auth_context.is_some())
            .field("has_observability", &self.observability.is_some())
            .finish_non_exhaustive()
    }
}

/// 捕获 stdout/stderr，保留内核写入顺序。
pub(super) async fn run_combined_command(
    cancellation: &CancellationToken,
    program: &OsStr,
    args: &[OsString],
    working_dir: Option<&Path>,
    environment: Option<&[(OsString, OsString)]>,
) -> Result<CommandOutput, CommandFailure> {
    let mut command = Command::new(program);
    command.args(args);
    apply_command_context(&mut command, working_dir, environment);
    run_command(command, cancellation, None, None, CaptureMode::Combined).await
}

/// 只返回 stdout，同时后台排空 stderr。
///
/// 可选 timeout 和上层取消都会只终止 direct child；调用者仍会得到退出状态错误，
/// 因而版本探测可以忽略全部失败并返回空字符串。
pub(super) async fn run_stdout_command(
    cancellation: &CancellationToken,
    program: &OsStr,
    args: &[OsString],
    timeout: Option<Duration>,
) -> Result<CommandOutput, CommandFailure> {
    let mut command = Command::new(program);
    command.args(args);
    run_command(command, cancellation, timeout, None, CaptureMode::Separate).await
}

/// 把 cwd 与完整环境表应用到 Tokio command。
fn apply_command_context(
    command: &mut Command,
    working_dir: Option<&Path>,
    environment: Option<&[(OsString, OsString)]>,
) {
    if let Some(working_dir) = working_dir.filter(|path| !path.as_os_str().is_empty()) {
        command.current_dir(working_dir);
    }
    if let Some(environment) = environment {
        command.env_clear();
        command.envs(environment.iter().cloned());
    }
}
