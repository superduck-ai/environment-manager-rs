//! Claude 执行器、跨子系统适配 trait 与共享错误声明。
//!
//! Go 结构体中仅保存而不读取的 MCP registration 和 lease manager 仍原样保存在
//! 执行器上。MCP 以无行为 marker trait 隔离，o11y 则通过逐事件
//! adapter 暴露；没有 adapter 时不会伪造计数器或诊断落盘。

use std::{
    ffi::{OsStr, OsString},
    path::Path,
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use nix::{fcntl::OFlag, unistd::pipe2};
use tokio::{io::AsyncReadExt, process::Command};
use tokio_util::sync::CancellationToken;

use crate::internal::{
    auth::AuthContext,
    config::{ClaudeConfig, StartupContext},
    envtype::LeaseManagerHandle,
    process::{go_spawn_error, resolve_go_command},
    session::ActivityRecorder,
};

use super::Outcomes;

/// Claude 子系统对外返回的稳定文本错误。
///
/// Go 版本大量使用 `%w` 动态包装不同错误类型；Rust 边界保留最终兼容文案，
/// 具体 I/O 原因在构造时已经进入该文本和结构化日志。
#[derive(Debug)]
pub struct ClaudeError {
    message: Vec<u8>,
}

impl ClaudeError {
    /// 从已经包含兼容上下文的消息构造错误。
    #[must_use]
    pub fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into().into_bytes(),
        }
    }

    /// 从可能包含非 UTF-8 `CombinedOutput` 的完整错误字节构造错误。
    ///
    /// Go error string 本身是任意字节序列；保存为 `Vec<u8>` 可让最外层 CLI 使用
    /// `Write::write_all` 原样输出。普通 Rust `Display` 只能提供有损的人类可读视图。
    #[must_use]
    pub(crate) fn message_bytes(message: Vec<u8>) -> Self {
        Self { message }
    }

    /// 返回 Go 兼容错误的原始字节，不执行 UTF-8 替换。
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
    /// 外部兼容 stderr 不应经过此方法，应调用 `as_bytes` 或 `into_bytes`。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&go_utf8_error_view(&self.message))
    }
}

impl std::error::Error for ClaudeError {}

/// 为日志/Display 将每个无效字节分别替换为 U+FFFD。
///
/// Go `encoding/json` 对 error string 使用 `utf8.DecodeRune`，每个 size=1 的坏字节
/// 都产生一个替换字符；Rust `from_utf8_lossy` 可能合并坏字节段，因此不能直接用。
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
/// Claude Go 执行器只保存这些对象而不读取任何字段，因此 trait 故意没有方法；
/// 真实 MCP registration 实现该 marker，无需在 Claude 模块重复配置结构。
pub trait McpServerRegistration: Send + Sync {}

/// o11y/diag 与 Claude 执行器之间的同步事件适配接口。
///
/// 方法与 Go 调用点一一对应，避免把 metric 和 diagnostic 合并成一条虚构事件。
pub trait ClaudeObservability: Send + Sync {
    /// 返回本次会话应注入 Claude 的诊断文件路径；`None` 表示诊断落盘未启用。
    fn claude_code_diagnostics_file_path(&self) -> Option<String>;

    /// 对应 `o11y.Increment(ClaudeCodeStartCounter)`。
    fn increment_claude_code_start(&self);

    /// 对应无 PII 的 `claude_code_start` 诊断事件。
    fn log_claude_code_start(&self);

    /// 对应无 PII 的 `claude_code_end` 诊断事件。
    fn log_claude_code_end(&self, is_error: bool, exit_code: i32, duration_ms: i64);

    /// 对应带成功/失败维度的 Claude Code 结束计数器。
    ///
    /// `exit_code` 仅在子进程确实启动并退出时存在；启动、等待或取消错误传 `None`。
    /// 不能只传一个失败布尔值，否则 OTel 的 `error_code` 标签会丢失 Go
    /// `errors.As(*exec.ExitError)` 能取得的真实退出码。
    fn increment_claude_code_end(&self, is_error: bool, exit_code: Option<i32>);

    /// 对应 `claude_init_success` 或 `claude_init_failed` 诊断事件。
    fn log_claude_init(&self, succeeded: bool, exit_code: Option<i32>, duration_ms: i64);

    /// 对应存在旧版本兜底时的非致命更新失败诊断。
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

/// `CombinedOutput`/`Output` 的失败信息与已读取字节。
#[derive(Debug)]
pub(super) struct CapturedCommandError {
    /// Go `exec.Cmd` 风格的底层错误原始字节。
    pub(super) error: Vec<u8>,
    /// 失败前已经读取的目标输出。
    pub(super) output: Vec<u8>,
    /// 普通退出码；启动/等待失败或 signal 终止为 -1。
    pub(super) exit_code: i32,
}

/// 使用同一匿名 pipe 捕获 stdout/stderr，保持 Go `CombinedOutput` 的内核写入顺序。
///
/// 环境为 `Some` 时先清空继承环境再设置给定键值；工作目录为空时保持父进程目录。
/// 取消只调用 `Child::start_kill`，不会创建或杀死 process group。
pub(super) async fn run_combined_command(
    cancellation: &CancellationToken,
    program: &OsStr,
    args: &[OsString],
    working_dir: Option<&Path>,
    environment: Option<&[(OsString, OsString)]>,
) -> Result<Vec<u8>, CapturedCommandError> {
    let resolved_program = resolve_go_command(program).map_err(|error| CapturedCommandError {
        error: error.into_bytes(),
        output: Vec::new(),
        exit_code: -1,
    })?;
    if cancellation.is_cancelled() {
        return Err(CapturedCommandError {
            error: b"context canceled".to_vec(),
            output: Vec::new(),
            exit_code: -1,
        });
    }

    // Go CombinedOutput 使用的 os.Pipe 在 Linux 上原子设置 CLOEXEC；若先创建裸
    // pipe 再逐个 fcntl，并发 spawn 可能短暂继承捕获管道并延迟 EOF。
    let (read_end, write_end) = pipe2(OFlag::O_CLOEXEC).map_err(|error| CapturedCommandError {
        error: error.to_string().into_bytes(),
        output: Vec::new(),
        exit_code: -1,
    })?;
    let stderr_end = write_end
        .try_clone()
        .map_err(|error| CapturedCommandError {
            error: error.to_string().into_bytes(),
            output: Vec::new(),
            exit_code: -1,
        })?;

    let mut command = Command::new(&resolved_program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(write_end))
        .stderr(Stdio::from(stderr_end));
    apply_command_context(&mut command, working_dir, environment);
    let mut child = command.spawn().map_err(|error| CapturedCommandError {
        error: go_spawn_error(&resolved_program, &error).into_bytes(),
        output: Vec::new(),
        exit_code: -1,
    })?;
    drop(command);

    let mut reader = tokio::fs::File::from_std(std::fs::File::from(read_end));
    let reader_task = tokio::spawn(async move {
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await.map(|_| output)
    });
    let status = wait_for_direct_child(cancellation, None, &mut child).await;
    let output = join_captured_reader(reader_task)
        .await
        .map_err(|error| CapturedCommandError {
            error,
            output: Vec::new(),
            exit_code: -1,
        })?;
    classify_status(status, output)
}

/// 只返回 stdout，同时后台排空 stderr，复刻 Go `Cmd.Output` 的输出选择。
///
/// 可选 timeout 和上层取消都会只终止 direct child；调用者仍会得到退出状态错误，
/// 因而版本探测可以像 Go 一样忽略全部失败并返回空字符串。
pub(super) async fn run_stdout_command(
    cancellation: &CancellationToken,
    program: &OsStr,
    args: &[OsString],
    timeout: Option<Duration>,
) -> Result<Vec<u8>, CapturedCommandError> {
    let resolved_program = resolve_go_command(program).map_err(|error| CapturedCommandError {
        error: error.into_bytes(),
        output: Vec::new(),
        exit_code: -1,
    })?;
    if cancellation.is_cancelled() {
        return Err(CapturedCommandError {
            error: b"context canceled".to_vec(),
            output: Vec::new(),
            exit_code: -1,
        });
    }

    let mut command = Command::new(&resolved_program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().map_err(|error| CapturedCommandError {
        error: go_spawn_error(&resolved_program, &error).into_bytes(),
        output: Vec::new(),
        exit_code: -1,
    })?;
    let mut stdout = child.stdout.take().expect("stdout configured as piped");
    let mut stderr = child.stderr.take().expect("stderr configured as piped");
    let stdout_task = tokio::spawn(async move {
        let mut output = Vec::new();
        stdout.read_to_end(&mut output).await.map(|_| output)
    });
    // Go 的 Output 会排空 stderr 以免管道填满，但返回值不包含这些字节。
    let stderr_task = tokio::spawn(async move {
        let mut discarded = Vec::new();
        stderr.read_to_end(&mut discarded).await
    });
    let status = wait_for_direct_child(cancellation, timeout, &mut child).await;
    let output = join_captured_reader(stdout_task)
        .await
        .map_err(|error| CapturedCommandError {
            error,
            output: Vec::new(),
            exit_code: -1,
        })?;
    if let Err(error) = stderr_task.await {
        return Err(CapturedCommandError {
            error: error.to_string().into_bytes(),
            output,
            exit_code: -1,
        });
    }
    classify_status(status, output)
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

/// 等待 child 完成；取消或 timeout 只向该 child PID 发送强制终止。
async fn wait_for_direct_child(
    cancellation: &CancellationToken,
    timeout: Option<Duration>,
    child: &mut tokio::process::Child,
) -> std::io::Result<ExitStatus> {
    if let Some(timeout) = timeout {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                let _direct_child_kill = child.start_kill();
                child.wait().await
            }
            () = tokio::time::sleep(timeout) => {
                let _direct_child_kill = child.start_kill();
                child.wait().await
            }
            result = child.wait() => result,
        }
    } else {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                let _direct_child_kill = child.start_kill();
                child.wait().await
            }
            result = child.wait() => result,
        }
    }
}

/// 汇合捕获任务，并把 Tokio join/read 错误压成 Go 风格单层文本。
async fn join_captured_reader(
    task: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
) -> Result<Vec<u8>, Vec<u8>> {
    task.await
        .map_err(|error| error.to_string().into_bytes())?
        .map_err(|error| error.to_string().into_bytes())
}

/// 把 wait 状态分类为成功字节或包含输出的兼容错误。
fn classify_status(
    status: std::io::Result<ExitStatus>,
    output: Vec<u8>,
) -> Result<Vec<u8>, CapturedCommandError> {
    match status {
        Ok(status) if status.success() => Ok(output),
        Ok(status) => Err(CapturedCommandError {
            error: format_exit_status(status).into_bytes(),
            output,
            exit_code: status.code().unwrap_or(-1),
        }),
        Err(error) => Err(CapturedCommandError {
            error: error.to_string().into_bytes(),
            output,
            exit_code: -1,
        }),
    }
}

/// 生成 Go `exec.ExitError.Error()` 常见的 `exit status N` 或 `signal: name` 文本。
pub(super) fn format_exit_status(status: ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit status {code}");
    }

    use std::os::unix::process::ExitStatusExt as _;
    let signal = status.signal().unwrap_or_default();
    let mut message = format!("signal: {}", go_signal_name(signal));
    if status.core_dumped() {
        message.push_str(" (core dumped)");
    }
    message
}

/// 返回 Go Linux `syscall.Signal.String()` 使用的信号名称。
fn go_signal_name(signal: i32) -> String {
    let name = match signal {
        libc::SIGHUP => "hangup",
        libc::SIGINT => "interrupt",
        libc::SIGQUIT => "quit",
        libc::SIGILL => "illegal instruction",
        libc::SIGTRAP => "trace/breakpoint trap",
        libc::SIGABRT => "aborted",
        libc::SIGBUS => "bus error",
        libc::SIGFPE => "floating point exception",
        libc::SIGKILL => "killed",
        libc::SIGUSR1 => "user defined signal 1",
        libc::SIGSEGV => "segmentation fault",
        libc::SIGUSR2 => "user defined signal 2",
        libc::SIGPIPE => "broken pipe",
        libc::SIGALRM => "alarm clock",
        libc::SIGTERM => "terminated",
        // MIPS 和 SPARC 的 Linux signal ABI 没有 SIGSTKFLT；其他架构按 libc
        // 定义保留 Go 对 "stack fault" 的命名。
        #[cfg(not(any(
            target_arch = "mips",
            target_arch = "mips64",
            target_arch = "sparc",
            target_arch = "sparc64"
        )))]
        libc::SIGSTKFLT => "stack fault",
        libc::SIGCHLD => "child exited",
        libc::SIGCONT => "continued",
        libc::SIGSTOP => "stopped (signal)",
        libc::SIGTSTP => "stopped",
        libc::SIGTTIN => "stopped (tty input)",
        libc::SIGTTOU => "stopped (tty output)",
        libc::SIGURG => "urgent I/O condition",
        libc::SIGXCPU => "CPU time limit exceeded",
        libc::SIGXFSZ => "file size limit exceeded",
        libc::SIGVTALRM => "virtual timer expired",
        libc::SIGPROF => "profiling timer expired",
        libc::SIGWINCH => "window changed",
        libc::SIGIO => "I/O possible",
        libc::SIGPWR => "power failure",
        libc::SIGSYS => "bad system call",
        _ => return format!("signal {signal}"),
    };
    name.to_owned()
}
