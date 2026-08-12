//! Go `internal/process/process.go` 对应的单个可执行文件异步执行与输出转发。
//!
//! stdout 与 stderr 各由一个 Tokio 任务读取，避免任一管道填满后让子进程死锁。
//! 非零退出属于正常完成结果，和 Go 版本一样记录在 `ProcessResult` 而非函数错误中。

use std::{
    io,
    os::fd::{IntoRawFd, OwnedFd},
    path::Path,
    process::Stdio,
    time::{Instant, SystemTime},
};

use nix::unistd::close;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{ChildStderr, ChildStdout, Command},
};
use tokio_util::sync::CancellationToken;

use crate::internal::{
    logger::{LogAttributes, Logger},
    util::{OutputStreamer, StreamType},
};

use super::types::{
    ProcessError, ProcessExitError, ProcessResult, go_errno_text, go_spawn_error,
    resolve_go_command,
};

const STREAM_BUFFER_SIZE: usize = 32 * 1024;

/// 对应 Go：`internal/process/process.go` 中的 `Execute`。
///
/// 执行无参数可执行文件，并并发转发 stdout 与 stderr。
///
/// 该签名与 Go `Execute` 一样只接收 executable path；需要参数的调用应直接使用
/// 更高层 command builder，临时脚本则使用 `execute_script`。取消会发送强制终止，
/// 对应 Go `exec.CommandContext` 的默认 Unix 语义。若调用前已经取消，则在 spawn
/// 之前返回 `failed to start command: context canceled`，绝不创建短命子进程。
///
/// # 参数
/// - `cancellation`：上层 context 的取消令牌，只终止 direct child。
/// - `logger`：manager 注入的具体 session logger；克隆后分别交给 stdout/stderr
///   reader，共享同一个 tracing 接收端。
/// - `path`：不带参数的 executable 名称或路径，允许 Unix 非 UTF-8 字节。
/// - `streamer`：每次管道读取后同步调用的输出处理器；错误会停止对应 reader。
///
/// # 返回
/// 启动前失败返回 `ProcessError`；成功启动后即使非零退出也返回 `ProcessResult`，并把
/// 退出原因写入结果。函数会等待两个 reader 和 direct child，不清理孙进程。
pub async fn execute(
    cancellation: &CancellationToken,
    logger: Logger,
    path: impl AsRef<Path>,
    streamer: OutputStreamer,
) -> Result<ProcessResult, ProcessError> {
    let path = path.as_ref().to_owned();
    if path.as_os_str().is_empty() {
        return Err(ProcessError::EmptyPath);
    }
    let start_time = SystemTime::now();
    let started = Instant::now();
    let resolved_path =
        resolve_go_command(path.as_os_str()).map_err(|source| ProcessError::Start {
            path: path.clone(),
            source,
        })?;
    // Go `CommandContext` 先构造 Cmd（含 LookPath），`Start` 才检查已取消 context。
    // 因此 missing basename 的 LookPath 错误优先；显式 slash 路径仍在 spawn 前取消。
    if cancellation.is_cancelled() {
        return Err(ProcessError::Cancelled);
    }
    let mut command = Command::new(&resolved_path);
    // Go `exec.Cmd` 的 Stdin 为 nil 时从 `/dev/null` 读取；Tokio 默认继承父 stdin，
    // 会让 init/setup 脚本意外消费 task-run 协议输入，因此必须显式关闭。
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().map_err(|source| ProcessError::Start {
        path: path.clone(),
        source: go_spawn_error(&resolved_path, &source),
    })?;
    // 配置为 piped 后 Tokio 保证句柄存在；若未来重构破坏该不变量，expect 会在开发期暴露。
    let stdout = child.stdout.take().expect("stdout was configured as piped");
    let stderr = child.stderr.take().expect("stderr was configured as piped");
    let stdout_streamer = streamer.clone();
    let stdout_logger = logger.clone();
    let stdout_task = tokio::spawn(async move {
        stream_pipe(stdout_logger, stdout, StreamType::Stdout, stdout_streamer).await;
    });
    let stderr_task = tokio::spawn(async move {
        stream_pipe(logger, stderr, StreamType::Stderr, streamer).await;
    });

    // Go `Execute` 先等待两个 `streamPipe` goroutine，再调用 `cmd.Wait`。这个顺序
    // 看似可以反转，但 direct child 已退出、孙进程仍持有 pipe 时，Go 会让 direct
    // 暂时保持 zombie；先 wait 会改变 /proc 和父进程回收时序。与此同时 Go 的
    // CommandContext watcher 能在 reader 阻塞期间 kill direct child，因此这里一边
    // 汇合 reader，一边显式观察 cancellation。
    let stream_tasks = async {
        for task in [stdout_task, stderr_task] {
            // Go goroutine 中未恢复的 panic 会终止进程；Rust 不能把 JoinError 降级为
            // 一条 Rust-only warning 后继续返回成功，因此在汇合点恢复 panic 语义。
            task.await.expect("process stream task panicked");
        }
    };
    tokio::pin!(stream_tasks);
    let streams_finished_before_cancellation = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            kill_cancelled_process(&mut child);
            false
        }
        () = &mut stream_tasks => true,
    };

    let wait_result = if streams_finished_before_cancellation {
        // reader 先结束通常表示 direct child 已关闭 stdio，但它仍可能继续运行；
        // 此阶段继续响应取消，复刻 CommandContext watcher 的完整生命周期。
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                kill_cancelled_process(&mut child);
                child.wait().await
            }
            result = child.wait() => result,
        }
    } else {
        // 取消已经向 direct PID 发出 SIGKILL。Go `Execute` 仍先等孙进程关闭继承
        // pipe，之后才 Wait/reap direct child；此 await 有意保留该阻塞和 zombie。
        stream_tasks.await;
        child.wait().await
    };

    let end_time = SystemTime::now();
    let duration = started.elapsed();
    match wait_result {
        Ok(status) if status.success() => Ok(ProcessResult {
            exit_code: 0,
            start_time,
            end_time,
            duration,
            error: None,
        }),
        Ok(status) => {
            let exit_code = status.code().unwrap_or(-1);
            Ok(ProcessResult {
                exit_code,
                start_time,
                end_time,
                duration,
                error: Some(ProcessExitError::ExitCode(exit_code)),
            })
        }
        Err(error) => Ok(ProcessResult {
            exit_code: -1,
            start_time,
            end_time,
            duration,
            error: Some(ProcessExitError::Wait(error.to_string())),
        }),
    }
}

/// 终止被取消的命令，但只作用于直接子进程。
///
/// Tokio 在 Linux 上的 `start_kill` 向 child PID 发送 SIGKILL，等价于 Go
/// `exec.CommandContext` 的默认 `Cmd.Cancel = Process.Kill`。这里故意不创建或终止
/// process group；孙进程继续持有 pipe 时可能延迟退出，这是必须保留的 Go 行为。
fn kill_cancelled_process(child: &mut tokio::process::Child) {
    // Go CommandContext 的默认 watcher 不额外写业务日志；进程已退出导致的 kill
    // 竞态也不应产生 Rust-only warning。最终 wait 结果仍按 Go 规则进入 Result。
    let _direct_child_kill = child.start_kill();
}

/// 表达 Go `io.ReadCloser` 在 Tokio 中需要的读取与显式关闭能力。
///
/// Tokio 的 `AsyncRead` 用 `Ok(0)` 表示 EOF，无法表达 Go 防御性处理的 `(0, nil)`；
/// `zero_read_is_eof` 让真实 child pipe 明确采用 EOF 语义，同时允许失败注入类型覆盖
/// 为 `false`，从而验证 `Unexpected zero read without error` 日志分支。
pub(super) trait ProcessPipe: AsyncRead + Unpin {
    /// 判断本次 `Ok(0)` 是否是 Tokio 编码的 EOF。
    fn zero_read_is_eof(&self) -> bool {
        true
    }

    /// 消费 reader 并显式执行 Linux close，以便记录 Go defer 可观察的 close 错误。
    fn close(self) -> io::Result<()>;
}

impl ProcessPipe for ChildStdout {
    /// 将 Tokio 注册的 stdout reader 还原为 owned fd 后显式关闭。
    fn close(self) -> io::Result<()> {
        close_owned_pipe(self.into_owned_fd()?)
    }
}

impl ProcessPipe for ChildStderr {
    /// 将 Tokio 注册的 stderr reader 还原为 owned fd 后显式关闭。
    fn close(self) -> io::Result<()> {
        close_owned_pipe(self.into_owned_fd()?)
    }
}

#[cfg(test)]
impl ProcessPipe for tokio::io::DuplexStream {
    /// 测试内存 pipe 没有独立 close syscall；drop 即表示成功关闭。
    fn close(self) -> io::Result<()> {
        drop(self);
        Ok(())
    }
}

/// 对应 Go：`internal/process/process.go` 中的 `streamPipe`。
///
/// 持续读取单条 pipe 并调用输出回调。
///
/// 回调报错后立即停止读取并关闭 pipe。高输出量子进程随后可能因 SIGPIPE 或满管道
/// 阻塞；这是 Go `streamPipe` 的可观察行为，兼容迁移不能自行改成后台排空。
///
/// # 参数
/// - `logger`：与 `execute` 共享 handler 的具体 logger，所有错误使用 Go 原始大小写。
/// - `pipe`：stdout 或 stderr 的异步 reader；函数结束前总会显式 close。
/// - `stream_type`：写入 `stream` 属性的 `stdout`/`stderr` 稳定值。
/// - `streamer`：接收每个最多 32 KiB chunk 的同步回调。
pub(super) async fn stream_pipe<R>(
    logger: Logger,
    mut pipe: R,
    stream_type: StreamType,
    streamer: OutputStreamer,
) where
    R: ProcessPipe,
{
    let mut buffer = vec![0_u8; STREAM_BUFFER_SIZE];
    loop {
        match pipe.read(&mut buffer).await {
            Ok(0) if pipe.zero_read_is_eof() => break,
            Ok(0) => {
                logger.warn(
                    "Unexpected zero read without error",
                    attributes([("stream", json!(stream_type.to_string()))]),
                );
                break;
            }
            Ok(read) => {
                if let Err(error) = streamer(stream_type, &buffer[..read]) {
                    logger.error(
                        "Streamer requested stop",
                        attributes([
                            ("stream", json!(stream_type.to_string())),
                            ("error", json!(error.to_string())),
                        ]),
                    );
                    break;
                }
            }
            Err(error) => {
                logger.warn(
                    "Error reading from pipe",
                    attributes([
                        ("stream", json!(stream_type.to_string())),
                        ("error", json!(error.to_string())),
                    ]),
                );
                break;
            }
        }
    }
    if let Err(error) = pipe.close() {
        logger.warn(
            "Failed to close pipe",
            attributes([
                ("stream", json!(stream_type.to_string())),
                ("error", json!(error.to_string())),
            ]),
        );
    }
}

/// 显式关闭 Tokio child reader 的底层 fd，并转换为 Go `os.File.Close` 错误文本。
fn close_owned_pipe(pipe: OwnedFd) -> io::Result<()> {
    let raw_fd = pipe.into_raw_fd();
    close(raw_fd).map_err(|error| {
        let error = io::Error::from(error);
        io::Error::other(format!("close |0: {}", go_errno_text(&error)))
    })
}

/// 按调用顺序构造结构化日志属性。
pub(super) fn attributes<const N: usize>(pairs: [(&str, Value); N]) -> LogAttributes {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}
