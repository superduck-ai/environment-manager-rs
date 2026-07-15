//! 以外部命令代替 HTTP 的 PollHook。
//!
//! 命令从 stdin 接收 environment 身份 JSON，从 stdout 返回 work JSON；stderr 仅用于
//! 失败诊断。与 HTTP Poller 一样，它实现 `PollerInterface`，因此状态机无需分支。

use std::time::Duration;

use async_trait::async_trait;
use rand::Rng;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use super::{
    OrchestratorError, OrchestratorResult, PollHook, PollHookContext, PollerInterface, RawMessage,
    log_attributes, run_command_capture,
};
use crate::internal::{
    logger::Logger,
    util::{format_go_duration, go_duration_nanoseconds},
};

/// 对应 Go `NewPollHook`，创建命令型 poller。
///
/// 零超时默认 30 秒；固定参数随后可通过公开 `args` 字段设置。
pub fn new_poll_hook(
    command: impl Into<String>,
    timeout: Duration,
    organization_uuid: impl Into<String>,
    environment_id: impl Into<String>,
    worker_id: impl Into<String>,
    logger: Logger,
) -> PollHook {
    let command = command.into();
    let logger = logger.with_attrs(log_attributes([
        ("component", json!("poll_hook")),
        ("command", json!(command)),
    ]));
    PollHook {
        command,
        args: Vec::new(),
        timeout: if timeout.is_zero() {
            Duration::from_secs(30)
        } else {
            timeout
        },
        stdin_context: PollHookContext {
            organization_uuid: organization_uuid.into(),
            environment_id: environment_id.into(),
            worker_id: worker_id.into(),
        },
        logger,
    }
}

impl PollHook {
    /// 对应 Go `(*PollHook).Poll`，执行一次命令并解释 stdout。
    ///
    /// 空白输出或精确 JSON `null` 表示空队列；其他输出必须是合法 JSON，但不在这一层
    /// 限制具体 schema，以保持外部 poll hook 的协议扩展能力。
    pub async fn poll(
        &self,
        cancellation: &CancellationToken,
    ) -> OrchestratorResult<Option<RawMessage>> {
        if self.command.is_empty() {
            return Err(OrchestratorError::message(
                "poll hook command is not configured",
            ));
        }

        self.logger.debug("executing poll hook", Default::default());
        let stdin_data = serde_json::to_vec(&self.stdin_context).map_err(|error| {
            OrchestratorError::message(format!(
                "failed to serialize poll hook stdin context: {error}"
            ))
        })?;
        let outcome = run_command_capture(
            cancellation,
            &self.command,
            &self.args,
            Some(&stdin_data),
            Some(self.timeout),
        )
        .await;

        if let Some(error) = outcome.error {
            let stderr = String::from_utf8_lossy(&outcome.stderr).into_owned();
            self.logger.error(
                "poll hook execution failed",
                log_attributes([
                    ("duration", json!(go_duration_nanoseconds(outcome.duration))),
                    ("stderr", json!(stderr)),
                    ("error", json!(error)),
                ]),
            );
            return Err(OrchestratorError::message(format!(
                "poll hook failed after {}: {error} (stderr: {stderr})",
                format_go_duration(outcome.duration)
            )));
        }

        let output = String::from_utf8_lossy(&outcome.stdout);
        let output = output.trim();
        if output.is_empty() || output == "null" {
            self.logger.debug(
                "poll hook returned empty queue",
                log_attributes([("duration", json!(go_duration_nanoseconds(outcome.duration)))]),
            );
            return Ok(None);
        }

        if let Err(error) = serde_json::from_str::<serde_json::Value>(output) {
            self.logger.error(
                "poll hook returned invalid JSON",
                log_attributes([
                    ("duration", json!(go_duration_nanoseconds(outcome.duration))),
                    ("output", json!(output)),
                    ("error", json!(error.to_string())),
                ]),
            );
            return Err(OrchestratorError::message(format!(
                "poll hook returned invalid JSON: {error}"
            )));
        }

        self.logger.info(
            "poll hook returned work",
            log_attributes([("duration", json!(go_duration_nanoseconds(outcome.duration)))]),
        );
        Ok(Some(output.as_bytes().to_vec()))
    }

    /// 对应 Go `(*PollHook).SleepWithJitter`，空队列后随机等待 1–3 秒。
    ///
    /// `biased` 让已经取消的 token 优先于恰好同时到期的 timer，避免关闭过程多做一轮
    /// poll。随机数在 await 前生成并丢弃，future 因而保持 `Send`。
    pub async fn sleep_with_jitter(
        &self,
        cancellation: &CancellationToken,
    ) -> OrchestratorResult<()> {
        let seconds = {
            let mut rng = rand::rng();
            rng.random_range(1_u64..=3)
        };
        let sleep_duration = Duration::from_secs(seconds);
        self.logger.debug(
            "sleeping with jitter after empty queue",
            log_attributes([(
                "sleep_duration",
                json!(go_duration_nanoseconds(sleep_duration)),
            )]),
        );

        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(OrchestratorError::message(
                "jitter sleep interrupted: context canceled",
            )),
            _ = tokio::time::sleep(sleep_duration) => Ok(()),
        }
    }
}

#[async_trait]
impl PollerInterface for PollHook {
    /// 通过统一 poller trait 转发到 PollHook 的固有实现。
    async fn poll(
        &self,
        cancellation: &CancellationToken,
    ) -> OrchestratorResult<Option<RawMessage>> {
        PollHook::poll(self, cancellation).await
    }

    /// 通过统一 poller trait 转发可取消 jitter。
    async fn sleep_with_jitter(&self, cancellation: &CancellationToken) -> OrchestratorResult<()> {
        PollHook::sleep_with_jitter(self, cancellation).await
    }
}
