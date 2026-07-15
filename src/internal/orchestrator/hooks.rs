//! timeout Hook 与 execute Hook 的异步子进程执行。
//!
//! 原 Go 代码使用 `exec.CommandContext(...).CombinedOutput()`。这里用 Tokio pipe
//! 并发排空 stdout/stderr，避免任一 pipe 填满造成死锁；取消和超时都会杀死子进程，
//! 等待其退出并收完已经产生的输出后再返回。

use std::{
    future::pending,
    process::Stdio,
    time::{Duration, Instant},
};

use nix::{fcntl::OFlag, unistd::pipe2};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use super::{
    CommandOutcome, Hook, OrchestratorError, OrchestratorResult, SandboxRequest, SessionData,
    log_attributes,
};
use crate::internal::{
    logger::Logger,
    process::{go_spawn_error, resolve_go_command},
};

impl Hook {
    /// 对应 Go `(*Hook).Execute`，执行不带 stdin 的 Hook。
    ///
    /// 空命令是显式 no-op。非零退出、启动失败、取消与超时都包含 Hook 名、耗时和
    /// 已捕获输出，便于保持原命令行错误的诊断密度。
    pub async fn execute(
        &self,
        cancellation: &CancellationToken,
        logger: &Logger,
    ) -> OrchestratorResult<()> {
        if self.command.is_empty() {
            return Ok(());
        }

        let hook_logger = logger.with_attrs(log_attributes([
            ("hook", json!(self.name)),
            ("command", json!(self.command)),
        ]));
        hook_logger.info("executing hook", Default::default());

        let outcome = run_command_combined_capture(
            cancellation,
            &self.command,
            &self.args,
            None,
            duration_timeout(self.timeout),
        )
        .await;
        self.finish_execution(&hook_logger, outcome)
    }

    /// 对应 Go `(*Hook).ExecuteWithStdin`，向 execute Hook 传入原始 work JSON。
    ///
    /// JSON 中存在非空 `id` 时，在固定参数末尾注入 `--session <id>`。解析失败只记录
    /// warning，原始 stdin 仍完整传给命令。sandbox 启用后必须提供 adapter；缺失时
    /// 明确失败，避免意外在宿主机上运行本应隔离的命令。
    pub async fn execute_with_stdin(
        &self,
        cancellation: &CancellationToken,
        logger: &Logger,
        stdin_data: &[u8],
    ) -> OrchestratorResult<()> {
        if self.command.is_empty() {
            return Ok(());
        }

        let hook_logger = logger.with_attrs(log_attributes([
            ("hook", json!(self.name)),
            ("command", json!(self.command)),
            ("sandbox_enabled", json!(self.sandbox_enabled)),
        ]));
        hook_logger.info("executing hook with stdin", Default::default());

        let mut args = self.args.clone();
        match serde_json::from_slice::<SessionData>(stdin_data) {
            Ok(session) if !session.id.is_empty() => {
                hook_logger.info(
                    "injecting session ID into args",
                    log_attributes([("session_id", json!(session.id))]),
                );
                args.push("--session".to_owned());
                args.push(session.id);
            }
            Ok(_) => {}
            Err(error) => hook_logger.warn(
                "failed to parse session ID from stdin",
                log_attributes([("error", json!(error.to_string()))]),
            ),
        }

        let timeout = duration_timeout(self.timeout);
        let outcome = if self.sandbox_enabled {
            let adapter_result = match &self.sandbox_adapter {
                Some(adapter) => {
                    // adapter 把 prepare/wrap/execute/cleanup 包成一次 await；其契约要求
                    // cleanup 在返回前完成，因此任何早退都不会让临时资源越过 Hook。
                    adapter
                        .execute(SandboxRequest {
                            cancellation: cancellation.clone(),
                            stdin_data: stdin_data.to_vec(),
                            sandbox_config_path: self.sandbox_config_path.clone(),
                            working_dir: self.working_dir.clone(),
                            command: self.command.clone(),
                            args,
                            timeout,
                            logger: hook_logger.clone(),
                        })
                        .await
                }
                // 生产构造器不会缺少 adapter；保留这个可测试错误，同时让它与真实
                // runtime 初始化失败汇聚到下面唯一的 Go 日志调用点。
                None => Err(OrchestratorError::message(
                    "sandbox adapter is not configured",
                )),
            };
            match adapter_result {
                Ok(outcome) => outcome,
                Err(error) => {
                    hook_logger.error(
                        "sandbox initialization failed",
                        log_attributes([("error", json!(error.to_string()))]),
                    );
                    return Err(OrchestratorError::message(format!(
                        "sandbox initialization failed: {error}"
                    )));
                }
            }
        } else {
            run_command_combined_capture(
                cancellation,
                &self.command,
                &args,
                Some(stdin_data),
                timeout,
            )
            .await
        };

        self.finish_execution(&hook_logger, outcome)
    }

    /// 统一记录成功/失败并生成与 Go Hook 相同结构的错误。
    fn finish_execution(
        &self,
        hook_logger: &Logger,
        outcome: CommandOutcome,
    ) -> OrchestratorResult<()> {
        let output = String::from_utf8_lossy(&outcome.combined_output()).into_owned();
        let duration = format_duration(outcome.duration);
        if let Some(error) = outcome.error {
            hook_logger.error(
                "hook execution failed",
                log_attributes([
                    ("duration", json!(duration_nanoseconds(outcome.duration))),
                    ("output", json!(output)),
                    ("error", json!(error)),
                ]),
            );
            return Err(OrchestratorError::message(format!(
                "hook {} failed after {duration}: {error}\nOutput: {output}",
                self.name
            )));
        }

        hook_logger.info(
            "hook execution completed",
            log_attributes([
                ("duration", json!(duration_nanoseconds(outcome.duration))),
                ("output", json!(output)),
            ]),
        );
        Ok(())
    }
}

/// 对应 Go `(*Hook).ExecuteWithStdin` 中 sandbox command 包装成功日志。
///
/// 该 helper 刻意不使用 `#[track_caller]`：生产 adapter 位于 `cmd` 镜像文件，但 Go
/// 日志调用点属于 `internal/orchestrator/hooks.go`。固定在本文件调用 Logger 才能让
/// JSON `source` 仍指向正确的 Go 文件和行号。
///
/// # 参数
/// - `logger`：已绑定 hook、command、sandbox_enabled 属性的 Hook logger。
/// - `command`：SRT 包装后的可执行文件路径。
/// - `arguments`：SRT 包装后的完整参数，按原顺序写入 `wrapped_args`。
///
/// # 副作用
/// 同步写出一条 INFO 日志；handler 写入失败不会改变 Hook 结果。
pub(crate) fn log_command_wrapped_with_sandbox(
    logger: &Logger,
    command: &str,
    arguments: &[String],
) {
    logger.info(
        "command wrapped with sandbox",
        log_attributes([
            ("wrapped_command", json!(command)),
            ("wrapped_args", json!(arguments)),
        ]),
    );
}

/// 对应 Go `(*Hook).ExecuteWithStdin.func1` 中 sandbox cleanup 失败日志。
///
/// 与 [`log_command_wrapped_with_sandbox`] 一样，本函数固定 Go source 所属模块；清理
/// 失败只记录 warning，绝不覆盖已经得到的命令执行结果。
///
/// # 参数
/// - `logger`：已绑定 Hook 固定属性的 logger。
/// - `error`：sandbox runtime 返回的清理错误文本。
///
/// # 副作用
/// 同步写出一条 WARN 日志。
pub(crate) fn log_sandbox_cleanup_failed(logger: &Logger, error: &str) {
    logger.warn(
        "sandbox cleanup failed",
        log_attributes([("error", json!(error))]),
    );
}

/// 把 Tokio `Duration` 转成 Go `slog.Duration` 在 JSON handler 中使用的纳秒整数。
fn duration_nanoseconds(duration: Duration) -> i64 {
    duration.as_nanos().min(i64::MAX as u128) as i64
}

/// 把 Hook 的 Go 零值超时映射为“不设置独立 deadline”。
fn duration_timeout(timeout: Duration) -> Option<Duration> {
    (!timeout.is_zero()).then_some(timeout)
}

/// 使用 Tokio 执行命令并捕获两个输出流。
///
/// 取消/超时分支先请求 kill，再 `wait` 回收进程，最后等待三个 pipe 任务。这个顺序
/// 很重要：若先等待 reader，而子进程仍持有写端，reader 会永久等 EOF；若 kill 后不
/// wait，则 Linux 上会留下 zombie。`kill_on_drop` 还是最后一道 panic/任务 abort 保护。
pub(crate) async fn run_command_capture(
    cancellation: &CancellationToken,
    command: &str,
    args: &[String],
    stdin_data: Option<&[u8]>,
    timeout: Option<Duration>,
) -> CommandOutcome {
    let started = Instant::now();
    let resolved_command = match resolve_go_command(std::ffi::OsStr::new(command)) {
        Ok(command) => command,
        Err(error) => {
            return CommandOutcome {
                duration: started.elapsed(),
                error: Some(error.to_string()),
                ..CommandOutcome::default()
            };
        }
    };
    if cancellation.is_cancelled() {
        return CommandOutcome {
            duration: started.elapsed(),
            error: Some("context canceled".to_owned()),
            ..CommandOutcome::default()
        };
    }

    let mut process = Command::new(&resolved_command);
    process
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if stdin_data.is_some() {
        process.stdin(Stdio::piped());
    } else {
        process.stdin(Stdio::null());
    }

    let mut child = match process.spawn() {
        Ok(child) => child,
        Err(error) => {
            return CommandOutcome {
                duration: started.elapsed(),
                error: Some(go_spawn_error(&resolved_command, &error).to_string()),
                ..CommandOutcome::default()
            };
        }
    };

    let stdout_task = child.stdout.take().map(read_pipe);
    let stderr_task = child.stderr.take().map(read_pipe);
    let stdin_task = match (child.stdin.take(), stdin_data) {
        (Some(mut stdin), Some(data)) => {
            let data = data.to_vec();
            Some(tokio::spawn(async move {
                stdin.write_all(&data).await?;
                stdin.shutdown().await
            }))
        }
        _ => None,
    };

    enum StopReason {
        Completed(std::io::Result<std::process::ExitStatus>),
        Cancelled,
        TimedOut,
    }

    let timeout_future = async {
        match timeout {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => pending::<()>().await,
        }
    };
    tokio::pin!(timeout_future);

    let stop_reason = tokio::select! {
        biased;
        _ = cancellation.cancelled() => StopReason::Cancelled,
        _ = &mut timeout_future => StopReason::TimedOut,
        status = child.wait() => StopReason::Completed(status),
    };

    let mut primary_error = match stop_reason {
        StopReason::Completed(Ok(status)) if status.success() => None,
        StopReason::Completed(Ok(status)) => Some(exit_status_error(status)),
        StopReason::Completed(Err(error)) => Some(error.to_string()),
        StopReason::Cancelled => {
            terminate_and_reap(&mut child).await;
            Some("context canceled".to_owned())
        }
        StopReason::TimedOut => {
            terminate_and_reap(&mut child).await;
            Some("context deadline exceeded".to_owned())
        }
    };

    let stdout = join_pipe(stdout_task, &mut primary_error).await;
    let stderr = join_pipe(stderr_task, &mut primary_error).await;
    if let Some(task) = stdin_task {
        match task.await {
            Ok(Ok(())) => {}
            // 子进程提前退出时 EPIPE 是正常伴随现象，主退出状态更有诊断价值。
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::BrokenPipe => {}
            Ok(Err(error)) => set_secondary_error(&mut primary_error, error.to_string()),
            Err(error) => {
                set_secondary_error(&mut primary_error, format!("stdin task failed: {error}"))
            }
        }
    }

    CommandOutcome {
        stdout,
        stderr,
        duration: started.elapsed(),
        error: primary_error,
    }
}

/// 对应 Go `exec.Cmd.CombinedOutput`，使用同一个内核 pipe 捕获 stdout 与 stderr。
///
/// 普通 Hook 和 sandbox Hook 的错误文本会公开两个流的真实写入顺序，因此不能先分流
/// 再在 Rust 端拼接。pipe 在创建时原子设置 `O_CLOEXEC`，避免并发 spawn 在设置
/// close-on-exec 前继承写端并阻止 EOF。取消与超时仍只终止直接子进程。
///
/// # 参数
/// - `cancellation`：上层取消令牌；预取消时不启动进程。
/// - `command`：未经 shell 展开的 executable。
/// - `args`：原样传入的 argv。
/// - `stdin_data`：可选原始 stdin 字节。
/// - `timeout`：可选 Hook deadline；`None` 表示只受上层取消控制。
///
/// # 返回
/// 合并字节保存在 `CommandOutcome.stdout`，`stderr` 保持为空，使现有
/// `combined_output()` 仍返回原始字节；错误、耗时和回收语义与分流版本相同。
pub(crate) async fn run_command_combined_capture(
    cancellation: &CancellationToken,
    command: &str,
    args: &[String],
    stdin_data: Option<&[u8]>,
    timeout: Option<Duration>,
) -> CommandOutcome {
    let started = Instant::now();
    let resolved_command = match resolve_go_command(std::ffi::OsStr::new(command)) {
        Ok(command) => command,
        Err(error) => {
            return CommandOutcome {
                duration: started.elapsed(),
                error: Some(error.to_string()),
                ..CommandOutcome::default()
            };
        }
    };
    if cancellation.is_cancelled() {
        return CommandOutcome {
            duration: started.elapsed(),
            error: Some("context canceled".to_owned()),
            ..CommandOutcome::default()
        };
    }

    // Go CombinedOutput 把两个 fd 指向同一个 bytes.Buffer。这里让两个 fd 指向同一
    // pipe，保留内核接收顺序；O_CLOEXEC 在 pipe2 内原子生效，避免并发继承竞态。
    let (read_end, write_end) = match pipe2(OFlag::O_CLOEXEC) {
        Ok(pipe) => pipe,
        Err(error) => {
            return CommandOutcome {
                duration: started.elapsed(),
                error: Some(error.to_string()),
                ..CommandOutcome::default()
            };
        }
    };
    let stderr_end = match write_end.try_clone() {
        Ok(stderr_end) => stderr_end,
        Err(error) => {
            return CommandOutcome {
                duration: started.elapsed(),
                error: Some(error.to_string()),
                ..CommandOutcome::default()
            };
        }
    };

    let mut process = Command::new(&resolved_command);
    process
        .args(args)
        .stdout(Stdio::from(write_end))
        .stderr(Stdio::from(stderr_end))
        .kill_on_drop(true);
    if stdin_data.is_some() {
        process.stdin(Stdio::piped());
    } else {
        process.stdin(Stdio::null());
    }

    let mut child = match process.spawn() {
        Ok(child) => child,
        Err(error) => {
            return CommandOutcome {
                duration: started.elapsed(),
                error: Some(go_spawn_error(&resolved_command, &error).to_string()),
                ..CommandOutcome::default()
            };
        }
    };
    drop(process);

    let reader = tokio::fs::File::from_std(std::fs::File::from(read_end));
    let output_task = Some(read_pipe(reader));
    let stdin_task = match (child.stdin.take(), stdin_data) {
        (Some(mut stdin), Some(data)) => {
            let data = data.to_vec();
            Some(tokio::spawn(async move {
                stdin.write_all(&data).await?;
                stdin.shutdown().await
            }))
        }
        _ => None,
    };

    enum StopReason {
        Completed(std::io::Result<std::process::ExitStatus>),
        Cancelled,
        TimedOut,
    }

    let timeout_future = async {
        match timeout {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => pending::<()>().await,
        }
    };
    tokio::pin!(timeout_future);

    let stop_reason = tokio::select! {
        biased;
        _ = cancellation.cancelled() => StopReason::Cancelled,
        _ = &mut timeout_future => StopReason::TimedOut,
        status = child.wait() => StopReason::Completed(status),
    };
    let mut primary_error = match stop_reason {
        StopReason::Completed(Ok(status)) if status.success() => None,
        StopReason::Completed(Ok(status)) => Some(exit_status_error(status)),
        StopReason::Completed(Err(error)) => Some(error.to_string()),
        StopReason::Cancelled => {
            terminate_and_reap(&mut child).await;
            Some("context canceled".to_owned())
        }
        StopReason::TimedOut => {
            terminate_and_reap(&mut child).await;
            Some("context deadline exceeded".to_owned())
        }
    };

    let output = join_pipe(output_task, &mut primary_error).await;
    if let Some(task) = stdin_task {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::BrokenPipe => {}
            Ok(Err(error)) => set_secondary_error(&mut primary_error, error.to_string()),
            Err(error) => {
                set_secondary_error(&mut primary_error, format!("stdin task failed: {error}"))
            }
        }
    }

    CommandOutcome {
        stdout: output,
        stderr: Vec::new(),
        duration: started.elapsed(),
        error: primary_error,
    }
}

/// 持续读取 pipe 直至 EOF。
fn read_pipe<R>(mut pipe: R) -> JoinHandle<std::io::Result<Vec<u8>>>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut output = Vec::new();
        pipe.read_to_end(&mut output).await?;
        Ok(output)
    })
}

/// 回收取消或超时的子进程，并保持 Go `exec.CommandContext` 的对外错误优先级。
///
/// `start_kill` 或随后 `wait` 的错误不额外写日志：Go 的 watcher 不会为这两个内部
/// 步骤产生独立日志，而且调用者已经固定返回 `context canceled` 或
/// `context deadline exceeded`。保留回收动作可以避免 Linux zombie，同时不会增加
/// Go 基线中不存在的 stderr 副作用。
async fn terminate_and_reap(child: &mut tokio::process::Child) {
    let _ = child.start_kill();
    let _ = child.wait().await;
}

/// 汇合 reader 任务，并在主错误为空时保存 pipe 错误。
async fn join_pipe(
    task: Option<JoinHandle<std::io::Result<Vec<u8>>>>,
    primary_error: &mut Option<String>,
) -> Vec<u8> {
    let Some(task) = task else {
        return Vec::new();
    };
    match task.await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            set_secondary_error(primary_error, error.to_string());
            Vec::new()
        }
        Err(error) => {
            set_secondary_error(primary_error, format!("output task failed: {error}"));
            Vec::new()
        }
    }
}

/// 仅在没有更直接的进程错误时采用辅助 I/O 错误。
fn set_secondary_error(primary_error: &mut Option<String>, error: String) {
    if primary_error.is_none() {
        *primary_error = Some(error);
    }
}

/// 将 Unix 退出状态格式化为 Go `exec.ExitError` 的常见文本。
fn exit_status_error(status: std::process::ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit status {code}");
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return format!("signal {signal}");
        }
    }
    status.to_string()
}

/// 使用 Rust 的紧凑 Duration 调试格式，得到接近 Go `time.Duration.String()` 的文案。
pub(crate) fn format_duration(duration: Duration) -> String {
    format!("{duration:?}")
}
