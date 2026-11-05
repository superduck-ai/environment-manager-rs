//! 单个可执行文件的异步执行与输出转发。
//!
//! stdout 与 stderr 各由一个 Tokio 任务读取，避免任一管道填满后让子进程死锁。
//! 非零退出属于正常完成结果，记录在 `ProcessResult` 而非函数错误中。

use std::{
    path::Path,
    process::Stdio,
    time::{Instant, SystemTime},
};

use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
};
use tokio_util::sync::CancellationToken;

use crate::internal::util::{OutputStreamer, StreamType};

use super::{
    command::{CommandFailureKind, wait_for_child},
    types::{ProcessError, ProcessExitError, ProcessResult},
};

const STREAM_BUFFER_SIZE: usize = 32 * 1024;

/// 执行无参数可执行文件，并并发转发 stdout 与 stderr。
///
/// 该接口只接收可执行文件路径；需要参数的调用应直接使用
/// 更高层 command builder，临时脚本则使用 `execute_script`。取消会发送强制终止，
/// 并在 spawn 前检查已经取消的令牌，避免创建短命子进程。
///
/// # 参数
/// - `cancellation`：上层 context 的取消令牌，只终止 direct child。
/// - `path`：不带参数的 executable 名称或路径，允许 Unix 非 UTF-8 字节。
/// - `streamer`：每次管道读取后同步调用的输出处理器；错误会停止对应 reader。
///
/// # 返回
/// 启动前失败返回 `ProcessError`；成功启动后即使非零退出也返回 `ProcessResult`，并把
/// 退出原因写入结果。函数先回收直接子进程，再等待两个 reader 排空管道，不清理孙进程。
pub async fn execute(
    cancellation: &CancellationToken,
    path: impl AsRef<Path>,
    streamer: OutputStreamer,
) -> Result<ProcessResult, ProcessError> {
    let path = path.as_ref().to_owned();
    if path.as_os_str().is_empty() {
        return Err(ProcessError::EmptyPath);
    }
    let start_time = SystemTime::now();
    let started = Instant::now();
    if cancellation.is_cancelled() {
        return Err(ProcessError::Cancelled);
    }
    let mut command = Command::new(&path);
    // 不需要输入的脚本显式连接 `/dev/null`，避免意外消费 task-run 协议输入。
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|source| ProcessError::Start {
        path: path.clone(),
        source,
    })?;
    // 配置为 piped 后 Tokio 保证句柄存在；若未来重构破坏该不变量，expect 会在开发期暴露。
    let stdout = child.stdout.take().expect("stdout was configured as piped");
    let stderr = child.stderr.take().expect("stderr was configured as piped");
    let stdout_streamer = streamer.clone();
    let stdout_task = tokio::spawn(async move {
        stream_pipe(stdout, StreamType::Stdout, stdout_streamer).await;
    });
    let stderr_task = tokio::spawn(async move {
        stream_pipe(stderr, StreamType::Stderr, streamer).await;
    });

    // 取消时先终止并 wait，避免 reader 等待继承写端时让直接子进程成为 zombie。
    let wait_result = wait_for_child(&mut child, cancellation, None).await;
    for task in [stdout_task, stderr_task] {
        task.await.expect("process stream task panicked");
    }

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
        Err(CommandFailureKind::Cancelled) => Ok(ProcessResult {
            exit_code: -1,
            start_time,
            end_time,
            duration,
            error: Some(ProcessExitError::ExitCode(-1)),
        }),
        Err(error) => Ok(ProcessResult {
            exit_code: -1,
            start_time,
            end_time,
            duration,
            error: Some(ProcessExitError::Wait(error.to_string())),
        }),
    }
}

/// 持续读取单条 pipe 并调用输出回调。
///
/// 回调报错后立即停止读取；reader 随 future 一起 drop 并关闭管道。
///
/// # 参数
/// - `pipe`：stdout 或 stderr 的异步 reader。
/// - `stream_type`：写入 `stream` 属性的 `stdout`/`stderr` 稳定值。
/// - `streamer`：接收每个最多 32 KiB chunk 的同步回调。
pub(super) async fn stream_pipe<R>(mut pipe: R, stream_type: StreamType, streamer: OutputStreamer)
where
    R: AsyncRead + Unpin,
{
    let mut buffer = vec![0_u8; STREAM_BUFFER_SIZE];
    loop {
        match pipe.read(&mut buffer).await {
            Ok(0) => break,
            Ok(read) => {
                if let Err(error) = streamer(stream_type, &buffer[..read]) {
                    tracing::error!("stream" = ?(stream_type.to_string()), "error" = ?(error.to_string()), "{}", "Streamer requested stop");
                    break;
                }
            }
            Err(error) => {
                tracing::warn!("stream" = ?(stream_type.to_string()), "error" = ?(error.to_string()), "{}", "Error reading from pipe");
                break;
            }
        }
    }
}
