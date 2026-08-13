//! timeout Hook 与 execute Hook 的异步子进程执行。
//!
//! 使用 Tokio pipe 并发排空 stdout/stderr，避免任一 pipe 填满造成死锁；取消和
//! 超时都会终止并回收直接子进程，再收完已经产生的输出。

use std::time::Duration;

use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use super::{
    CommandOutcome, Hook, OrchestratorError, OrchestratorResult, SandboxRequest, SessionData,
};
use crate::internal::{
    process::{CaptureMode, run_command},
    util::{duration_nanoseconds, format_go_duration},
};

impl Hook {
    /// 对应 Go `(*Hook).Execute`，执行不带 stdin 的 Hook。
    ///
    /// 空命令是显式 no-op。非零退出、启动失败、取消与超时都包含 Hook 名、耗时和
    /// 已捕获输出，便于保持原命令行错误的诊断密度。
    pub async fn execute(&self, cancellation: &CancellationToken) -> OrchestratorResult<()> {
        if self.command.is_empty() {
            return Ok(());
        }

        tracing::info!(hook = %self.name, command = %self.command, "executing hook");

        let outcome = run_command_capture(
            cancellation,
            &self.command,
            &self.args,
            None,
            duration_timeout(self.timeout),
            CaptureMode::Combined,
        )
        .await;
        self.finish_execution(outcome)
    }

    /// 对应 Go `(*Hook).ExecuteWithStdin`，向 execute Hook 传入原始 work JSON。
    ///
    /// JSON 中存在非空 `id` 时，在固定参数末尾注入 `--session <id>`。解析失败只记录
    /// warning，原始 stdin 仍完整传给命令。sandbox 启用后必须提供 adapter；缺失时
    /// 明确失败，避免意外在宿主机上运行本应隔离的命令。
    pub async fn execute_with_stdin(
        &self,
        cancellation: &CancellationToken,
        stdin_data: &[u8],
    ) -> OrchestratorResult<()> {
        if self.command.is_empty() {
            return Ok(());
        }

        tracing::info!(
            hook = %self.name,
            command = %self.command,
            sandbox_enabled = self.sandbox_enabled,
            "executing hook with stdin"
        );

        let mut args = self.args.clone();
        match serde_json::from_slice::<SessionData>(stdin_data) {
            Ok(session) if !session.id.is_empty() => {
                tracing::info!("session_id" = ?(session.id), "{}", "injecting session ID into args");
                args.push("--session".to_owned());
                args.push(session.id);
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!("error" = ?(error.to_string()), "{}", "failed to parse session ID from stdin")
            }
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
                    tracing::error!("error" = ?(error.to_string()), "{}", "sandbox initialization failed");
                    return Err(OrchestratorError::message(format!(
                        "sandbox initialization failed: {error}"
                    )));
                }
            }
        } else {
            run_command_capture(
                cancellation,
                &self.command,
                &args,
                Some(stdin_data),
                timeout,
                CaptureMode::Combined,
            )
            .await
        };

        self.finish_execution(outcome)
    }

    /// 统一记录成功/失败并生成与 Go Hook 相同结构的错误。
    fn finish_execution(&self, outcome: CommandOutcome) -> OrchestratorResult<()> {
        let (duration, output, error) = match outcome {
            Ok(outcome) => {
                let error = (!outcome.status.success()).then(|| outcome.status.to_string());
                (outcome.duration, outcome.output.expect_combined(), error)
            }
            Err(outcome) => (
                outcome.duration,
                outcome.output.expect_combined(),
                Some(outcome.kind.to_string()),
            ),
        };
        let output = String::from_utf8_lossy(&output).into_owned();
        let formatted_duration = format_go_duration(duration);
        if let Some(error) = error {
            tracing::error!(hook = %self.name, command = %self.command, "duration" = ?(duration_nanoseconds(duration)), "output" = ?(output), "error" = ?(error), "hook execution failed");
            return Err(OrchestratorError::message(format!(
                "hook {} failed after {formatted_duration}: {error}\nOutput: {output}",
                self.name
            )));
        }

        tracing::info!(hook = %self.name, command = %self.command, "duration" = ?(duration_nanoseconds(duration)), "output" = ?(output), "hook execution completed");
        Ok(())
    }
}

/// 对应 Go `(*Hook).ExecuteWithStdin` 中 sandbox command 包装成功日志。
///
/// # 参数
/// - `command`：SRT 包装后的可执行文件路径。
/// - `arguments`：SRT 包装后的完整参数，按原顺序写入 `wrapped_args`。
///
/// # 副作用
/// 同步写出一条 INFO 日志；handler 写入失败不会改变 Hook 结果。
pub(crate) fn log_command_wrapped_with_sandbox(command: &str, arguments: &[String]) {
    tracing::info!("wrapped_command" = ?(command), "wrapped_args" = ?(arguments), "{}", "command wrapped with sandbox");
}

/// 对应 Go `(*Hook).ExecuteWithStdin.func1` 中 sandbox cleanup 失败日志。
///
/// 与 [`log_command_wrapped_with_sandbox`] 一样，本函数集中记录命令包装事件；清理
/// 失败只记录 warning，绝不覆盖已经得到的命令执行结果。
///
/// # 参数
/// - `error`：sandbox runtime 返回的清理错误文本。
///
/// # 副作用
/// 同步写出一条 WARN 日志。
pub(crate) fn log_sandbox_cleanup_failed(error: &str) {
    tracing::warn!("error" = ?(error), "{}", "sandbox cleanup failed");
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
    capture: CaptureMode,
) -> CommandOutcome {
    let mut process = Command::new(command);
    process.args(args);
    run_command(process, cancellation, timeout, stdin_data, capture).await
}
