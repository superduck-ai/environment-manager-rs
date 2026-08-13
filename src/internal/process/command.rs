//! Rust 原生异步命令执行。

use std::{future::pending, io, process::ExitStatus, process::Stdio, time::Duration};

use nix::{fcntl::OFlag, unistd::pipe2};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

/// 子进程输出的捕获方式。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureMode {
    /// 分别捕获 stdout 和 stderr。
    Separate,
    /// 让 stdout 和 stderr 共用一条管道，保留内核观察到的写入顺序。
    Combined,
}

/// 已完成命令的状态、输出和耗时；非零退出仍是正常完成。
#[derive(Debug)]
pub struct CommandOutput {
    pub status: ExitStatus,
    pub output: CapturedOutput,
    pub duration: Duration,
}

/// 与捕获方式一致的子进程输出。
#[derive(Debug)]
pub enum CapturedOutput {
    /// 独立保存 stdout 和 stderr；不承诺两个流之间的交错顺序。
    Separate { stdout: Vec<u8>, stderr: Vec<u8> },
    /// stdout 和 stderr 共用同一条内核管道。
    Combined(Vec<u8>),
}

impl CapturedOutput {
    /// 取得 combined capture；模式不匹配表示调用方与 runner 配置不一致。
    pub fn expect_combined(self) -> Vec<u8> {
        let Self::Combined(output) = self else {
            panic!("combined command output expected");
        };
        output
    }

    /// 取得 separate capture；模式不匹配表示调用方与 runner 配置不一致。
    pub fn expect_separate(self) -> (Vec<u8>, Vec<u8>) {
        let Self::Separate { stdout, stderr } = self else {
            panic!("separate command output expected");
        };
        (stdout, stderr)
    }
}

/// 命令未完成的基础设施原因。
#[derive(Debug, Error)]
pub enum CommandFailureKind {
    #[error("command was cancelled")]
    Cancelled,
    #[error("command timed out")]
    TimedOut,
    #[error("failed to spawn command: {0}")]
    Spawn(#[source] io::Error),
    #[error("failed to wait for command: {0}")]
    Wait(#[source] io::Error),
    #[error("failed to configure command output: {0}")]
    Pipe(#[source] io::Error),
    #[error("failed to read command output: {0}")]
    Read(#[source] io::Error),
    #[error("command output task failed: {0}")]
    OutputTask(#[source] tokio::task::JoinError),
    #[error("failed to write command input: {0}")]
    Write(#[source] io::Error),
    #[error("command input task failed: {0}")]
    InputTask(#[source] tokio::task::JoinError),
}

/// 命令失败原因以及失败前已经捕获的输出。
#[derive(Debug, Error)]
#[error("{kind}")]
pub struct CommandFailure {
    pub kind: CommandFailureKind,
    pub output: CapturedOutput,
    pub duration: Duration,
}

/// 运行已经配置好的 Tokio 命令，并负责取消、超时、回收和管道排空。
pub(crate) async fn run_command(
    mut command: Command,
    cancellation: &CancellationToken,
    timeout: Option<Duration>,
    stdin: Option<&[u8]>,
    capture: CaptureMode,
) -> Result<CommandOutput, CommandFailure> {
    let started = std::time::Instant::now();
    if cancellation.is_cancelled() {
        return Err(failure(
            CommandFailureKind::Cancelled,
            started,
            empty_output(capture),
        ));
    }

    command.kill_on_drop(true);
    if stdin.is_some() {
        command.stdin(Stdio::piped());
    } else {
        command.stdin(Stdio::null());
    }

    let mut combined_reader = None;
    match capture {
        CaptureMode::Separate => {
            command.stdout(Stdio::piped()).stderr(Stdio::piped());
        }
        CaptureMode::Combined => {
            let (read_end, write_end) = pipe2(OFlag::O_CLOEXEC).map_err(|error| {
                failure(
                    CommandFailureKind::Pipe(io::Error::from(error)),
                    started,
                    empty_output(capture),
                )
            })?;
            let stderr_end = write_end.try_clone().map_err(|error| {
                failure(
                    CommandFailureKind::Pipe(error),
                    started,
                    empty_output(capture),
                )
            })?;
            command
                .stdout(Stdio::from(write_end))
                .stderr(Stdio::from(stderr_end));
            combined_reader = Some(tokio::fs::File::from_std(std::fs::File::from(read_end)));
        }
    }

    let mut child = command.spawn().map_err(|error| {
        failure(
            CommandFailureKind::Spawn(error),
            started,
            empty_output(capture),
        )
    })?;
    // 尽早释放父进程侧的 command 状态，避免它意外持有共享管道写端。
    drop(command);

    let (stdout_task, stderr_task) = match capture {
        CaptureMode::Separate => (
            child.stdout.take().map(read_pipe),
            child.stderr.take().map(read_pipe),
        ),
        CaptureMode::Combined => (combined_reader.map(read_pipe), None),
    };
    let stdin_task = match (child.stdin.take(), stdin) {
        (Some(mut input), Some(bytes)) => {
            let bytes = bytes.to_vec();
            Some(tokio::spawn(async move {
                input.write_all(&bytes).await?;
                input.shutdown().await
            }))
        }
        _ => None,
    };

    let status = wait_for_child(&mut child, cancellation, timeout).await;
    let (status, mut primary) = match status {
        Ok(status) => (Some(status), None),
        Err(error) => (None, Some(error)),
    };

    if let Some(task) = stdin_task {
        // direct child 已结束后不再需要继续写 stdin；若孙进程继承读端但不读取，
        // 主动终止写入任务可避免在这里无限等待。
        task.abort();
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) if error.kind() == io::ErrorKind::BrokenPipe => {}
            Ok(Err(error)) if primary.is_none() => primary = Some(CommandFailureKind::Write(error)),
            Err(error) if error.is_cancelled() => {}
            Err(error) if primary.is_none() => primary = Some(CommandFailureKind::InputTask(error)),
            _ => {}
        }
    }
    let stdout = join_pipe(stdout_task, &mut primary).await;
    let stderr = join_pipe(stderr_task, &mut primary).await;
    let output = match capture {
        CaptureMode::Separate => CapturedOutput::Separate { stdout, stderr },
        CaptureMode::Combined => CapturedOutput::Combined(stdout),
    };

    if let Some(kind) = primary {
        Err(failure(kind, started, output))
    } else {
        Ok(CommandOutput {
            status: status.expect("completed command requires an exit status"),
            output,
            duration: started.elapsed(),
        })
    }
}

/// 等待直接子进程，统一处理取消、超时、终止和回收。
pub(crate) async fn wait_for_child(
    child: &mut Child,
    cancellation: &CancellationToken,
    timeout: Option<Duration>,
) -> Result<ExitStatus, CommandFailureKind> {
    let timeout_future = async {
        match timeout {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => pending::<()>().await,
        }
    };
    tokio::pin!(timeout_future);

    tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            terminate_and_reap(child).await.map_err(CommandFailureKind::Wait)?;
            Err(CommandFailureKind::Cancelled)
        }
        () = &mut timeout_future => {
            terminate_and_reap(child).await.map_err(CommandFailureKind::Wait)?;
            Err(CommandFailureKind::TimedOut)
        }
        status = child.wait() => status.map_err(CommandFailureKind::Wait),
    }
}

/// 在独立任务中持续排空一条输出管道。
fn read_pipe<R>(mut pipe: R) -> JoinHandle<io::Result<Vec<u8>>>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut output = Vec::new();
        pipe.read_to_end(&mut output).await?;
        Ok(output)
    })
}

/// 等待管道读取任务，并在尚无主错误时记录读取错误。
async fn join_pipe(
    task: Option<JoinHandle<io::Result<Vec<u8>>>>,
    primary: &mut Option<CommandFailureKind>,
) -> Vec<u8> {
    let Some(task) = task else {
        return Vec::new();
    };
    match task.await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            if primary.is_none() {
                *primary = Some(CommandFailureKind::Read(error));
            }
            Vec::new()
        }
        Err(error) => {
            if primary.is_none() {
                *primary = Some(CommandFailureKind::OutputTask(error));
            }
            Vec::new()
        }
    }
}

/// 终止尚未退出的直接子进程并完成回收。
pub(crate) async fn terminate_and_reap(child: &mut Child) -> io::Result<ExitStatus> {
    if let Some(status) = child.try_wait()? {
        return Ok(status);
    }
    let _ = child.start_kill();
    child.wait().await
}

/// 用已经捕获的输出和实际耗时构造失败结果。
fn failure(
    kind: CommandFailureKind,
    started: std::time::Instant,
    output: CapturedOutput,
) -> CommandFailure {
    CommandFailure {
        kind,
        output,
        duration: started.elapsed(),
    }
}

/// 为尚未产生字节的失败构造与请求模式一致的空输出。
fn empty_output(capture: CaptureMode) -> CapturedOutput {
    match capture {
        CaptureMode::Separate => CapturedOutput::Separate {
            stdout: Vec::new(),
            stderr: Vec::new(),
        },
        CaptureMode::Combined => CapturedOutput::Combined(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 非零退出仍保留退出状态和失败前的交错输出。
    #[tokio::test]
    async fn combined_output_preserves_status_and_bytes() {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("printf out; printf err >&2; exit 7");

        let output = run_command(
            command,
            &CancellationToken::new(),
            None,
            None,
            CaptureMode::Combined,
        )
        .await
        .unwrap();

        assert_eq!(output.status.code(), Some(7));
        assert_eq!(output.output.expect_combined(), b"outerr");
    }

    /// 两条输出管道都超过常见 pipe 容量时不会互相阻塞。
    #[tokio::test]
    async fn separate_output_drains_both_pipes_concurrently() {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg(
            "i=0; while [ $i -lt 10000 ]; do printf 1234567890; printf abcdefghij >&2; i=$((i+1)); done",
        );

        let output = run_command(
            command,
            &CancellationToken::new(),
            None,
            None,
            CaptureMode::Separate,
        )
        .await
        .unwrap();

        let (stdout, stderr) = output.output.expect_separate();
        assert_eq!(stdout.len(), 100_000);
        assert_eq!(stderr.len(), 100_000);
    }

    /// 超时终止并回收直接子进程，并返回类型化原因。
    #[tokio::test]
    async fn timeout_returns_typed_failure() {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("while :; do :; done");

        let failure = run_command(
            command,
            &CancellationToken::new(),
            Some(Duration::from_millis(20)),
            None,
            CaptureMode::Separate,
        )
        .await
        .unwrap_err();

        assert!(matches!(failure.kind, CommandFailureKind::TimedOut));
    }

    /// 预取消优先于 spawn，即使命令路径不存在也返回取消原因。
    #[tokio::test]
    async fn pre_cancelled_command_is_not_spawned() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let failure = run_command(
            Command::new("/definitely/not/executable"),
            &cancellation,
            None,
            None,
            CaptureMode::Separate,
        )
        .await
        .unwrap_err();

        assert!(matches!(failure.kind, CommandFailureKind::Cancelled));
    }
}
