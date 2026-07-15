//! orchestrator 轮询、超时 Hook、指数退避与 session 交付状态机。
//!
//! 状态机只依赖 `PollerInterface`、Hook、observer 和输出 trait；HTTP、命令轮询及
//! stdout 都是可替换边界。这样既保持 Go 控制流，也能用 Tokio 测试精确验证时序。

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use serde_json::json;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use super::{
    NoopOrchestratorObserver, Orchestrator, OrchestratorConfig, OrchestratorError,
    OrchestratorEvent, OrchestratorObserver, OrchestratorResult, RawMessage, SessionOutput,
    StdoutSessionOutput, default_orchestrator_logger, log_attributes,
};

const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const DEFAULT_LOOP_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// 对应 Go `NewOrchestrator`，验证配置并填充五分钟默认超时。
pub fn new_orchestrator(mut config: OrchestratorConfig) -> OrchestratorResult<Orchestrator> {
    if config.poller.is_none() {
        return Err(OrchestratorError::message("poller is required"));
    }
    if config.poll_timeout.is_zero() {
        config.poll_timeout = DEFAULT_POLL_TIMEOUT;
    }
    if config.loop_timeout.is_zero() {
        config.loop_timeout = DEFAULT_LOOP_TIMEOUT;
    }
    if config.observer.is_none() {
        config.observer = Some(Arc::new(NoopOrchestratorObserver));
    }
    if config.session_output.is_none() {
        config.session_output = Some(Arc::new(StdoutSessionOutput));
    }

    let logger = config
        .logger
        .clone()
        .unwrap_or_else(default_orchestrator_logger)
        .with_attrs(log_attributes([("component", json!("orchestrator"))]));
    config.logger = Some(logger.clone());
    Ok(Orchestrator { config, logger })
}

impl Orchestrator {
    /// 对应 Go `(*Orchestrator).handleLoopTimeout`，计算本轮剩余时间并触发 timeout Hook。
    ///
    /// timeout Hook 失败只写日志：它通常用于刷新仓库或依赖，不能让一次维护失败永久
    /// 停止取任务。Hook 完成后才重置 loop 起点，因此 Hook 自身耗时不侵占下一轮预算。
    pub async fn handle_loop_timeout(
        &self,
        cancellation: &CancellationToken,
        loop_start_time: Instant,
    ) -> (Instant, Duration) {
        let elapsed = loop_start_time.elapsed();
        if elapsed < self.config.loop_timeout {
            return (loop_start_time, self.config.loop_timeout - elapsed);
        }

        self.logger.info(
            "loop timeout reached, triggering timeout hook",
            Default::default(),
        );
        self.observer().record_event(OrchestratorEvent::Timeout);
        if let Some(hook) = &self.config.timeout_hook
            && let Err(error) = hook.execute(cancellation, &self.logger).await
        {
            self.logger.error(
                "timeout hook failed",
                log_attributes([("error", json!(error.to_string()))]),
            );
        }

        (Instant::now(), self.config.loop_timeout)
    }

    /// 对应 Go `(*Orchestrator).pollForSession`，在一个 poll 窗口内寻找 session。
    ///
    /// 失败次数跨 poll 窗口保存，成功（包括空队列）才清零。第 N 次连续失败退避
    /// `min(2^N, 60)` 秒，所以第一次是 2 秒。与 Go 一致，deadline 只在循环顶部检查；
    /// 已开始的退避或 jitter 不会被 deadline 截短，但始终可被取消令牌中断。
    pub async fn poll_for_session(
        &self,
        cancellation: &CancellationToken,
        poll_timeout: Duration,
        remaining_loop_time: Duration,
        consecutive_failures: &mut i64,
    ) -> OrchestratorResult<Option<RawMessage>> {
        let now = Instant::now();
        let poll_deadline = now.checked_add(poll_timeout).unwrap_or(now);
        self.logger.info(
            "starting poll loop",
            log_attributes([
                ("poll_timeout_ms", json!(poll_timeout.as_millis())),
                (
                    "remaining_loop_time_ms",
                    json!(remaining_loop_time.as_millis()),
                ),
            ]),
        );

        while Instant::now() < poll_deadline {
            self.observer().record_event(OrchestratorEvent::PollAttempt);
            let started = Instant::now();
            let poll_result = self.poller().poll(cancellation).await;
            let poll_duration = started.elapsed();
            self.observer().record_poll_duration(poll_duration);

            let session_json = match poll_result {
                Ok(session_json) => session_json,
                Err(error) => {
                    self.logger.error(
                        "poll failed",
                        log_attributes([
                            ("duration_ms", json!(poll_duration.as_millis())),
                            ("error", json!(error.to_string())),
                        ]),
                    );
                    self.observer().record_event(OrchestratorEvent::PollError);
                    *consecutive_failures = (*consecutive_failures).saturating_add(1);
                    if self.config.max_poll_failures > 0
                        && *consecutive_failures >= self.config.max_poll_failures
                    {
                        return Err(OrchestratorError::message(format!(
                            "exceeded max poll failures ({}): {error}",
                            self.config.max_poll_failures
                        )));
                    }

                    let exponent = (*consecutive_failures).clamp(0, 6) as u32;
                    let backoff_seconds = (1_u64 << exponent).min(60);
                    let backoff = Duration::from_secs(backoff_seconds);
                    self.logger.warn(
                        "backing off after poll failure",
                        log_attributes([
                            ("consecutive_failures", json!(*consecutive_failures)),
                            ("backoff_ms", json!(backoff.as_millis())),
                        ]),
                    );

                    // 取消优先于刚好同时到期的 timer，保证 shutdown 不会额外发一次请求。
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => {
                            return Err(OrchestratorError::message(
                                "orchestrator stopped during backoff: context canceled",
                            ));
                        }
                        _ = tokio::time::sleep(backoff) => continue,
                    }
                }
            };

            *consecutive_failures = 0;
            if session_json.is_some() {
                return Ok(session_json);
            }

            self.logger
                .debug("queue is empty, will retry", Default::default());
            self.observer().record_event(OrchestratorEvent::QueueEmpty);
            self.poller()
                .sleep_with_jitter(cancellation)
                .await
                .map_err(|error| {
                    OrchestratorError::message(format!(
                        "orchestrator stopped during jitter sleep: {error}"
                    ))
                })?;
        }

        self.logger.info(
            "poll timeout reached, queue empty",
            log_attributes([("poll_timeout_ms", json!(poll_timeout.as_millis()))]),
        );
        Ok(None)
    }

    /// 对应 Go `(*Orchestrator).outputSession`，把原始 JSON 加换行写 stdout。
    pub async fn output_session(&self, session_json: &[u8]) -> OrchestratorResult<()> {
        self.logger.info(
            "received session task, writing to stdout",
            Default::default(),
        );
        self.observer()
            .record_event(OrchestratorEvent::SessionStart);
        self.session_output()
            .write_session(session_json)
            .await
            .map_err(|error| {
                OrchestratorError::message(format!(
                    "failed to write session JSON to stdout: {error}"
                ))
            })?;

        self.logger.info(
            "session task written to stdout, exiting",
            Default::default(),
        );
        self.observer().record_event(OrchestratorEvent::SessionEnd);
        Ok(())
    }

    /// 对应 Go `(*Orchestrator).Run`，运行到收到并交付一条 session 或发生终止错误。
    ///
    /// 每轮有效 poll timeout 是配置窗口与 loop 剩余时间的较小值。execute Hook 存在时
    /// work JSON 只送 stdin，不写 stdout；Hook 失败记录 session end error 并立即返回。
    pub async fn run(&self, cancellation: &CancellationToken) -> OrchestratorResult<()> {
        self.logger.info(
            "starting orchestrator",
            log_attributes([
                (
                    "poll_timeout_ms",
                    json!(self.config.poll_timeout.as_millis()),
                ),
                (
                    "loop_timeout_ms",
                    json!(self.config.loop_timeout.as_millis()),
                ),
                ("max_poll_failures", json!(self.config.max_poll_failures)),
            ]),
        );

        let mut consecutive_failures = 0_i64;
        let mut loop_start_time = Instant::now();
        loop {
            if cancellation.is_cancelled() {
                self.logger.info(
                    "orchestrator shutting down due to context cancellation",
                    Default::default(),
                );
                return Err(OrchestratorError::message(
                    "orchestrator stopped: context canceled",
                ));
            }

            let (new_loop_start, remaining_loop_time) = self
                .handle_loop_timeout(cancellation, loop_start_time)
                .await;
            loop_start_time = new_loop_start;
            let poll_timeout = self.config.poll_timeout.min(remaining_loop_time);
            let Some(session_json) = self
                .poll_for_session(
                    cancellation,
                    poll_timeout,
                    remaining_loop_time,
                    &mut consecutive_failures,
                )
                .await?
            else {
                continue;
            };

            if let Some(execute_hook) = &self.config.execute_hook {
                self.logger
                    .info("executing session with execute hook", Default::default());
                self.observer()
                    .record_event(OrchestratorEvent::SessionStart);
                if let Err(error) = execute_hook
                    .execute_with_stdin(cancellation, &self.logger, &session_json)
                    .await
                {
                    self.logger.error(
                        "execute hook failed",
                        log_attributes([("error", json!(error.to_string()))]),
                    );
                    self.observer()
                        .record_event(OrchestratorEvent::SessionEndError(error.to_string()));
                    return Err(OrchestratorError::message(format!(
                        "execute hook failed: {error}"
                    )));
                }

                self.logger
                    .info("execute hook completed successfully", Default::default());
                self.observer().record_event(OrchestratorEvent::SessionEnd);
                return Ok(());
            }

            self.output_session(&session_json).await.map_err(|error| {
                OrchestratorError::message(format!("failed to output session: {error}"))
            })?;
            return Ok(());
        }
    }

    /// 已由构造函数验证的 poller。
    fn poller(&self) -> &dyn super::PollerInterface {
        self.config
            .poller
            .as_deref()
            .expect("new_orchestrator validates poller")
    }

    /// 已由构造函数填充的 observer。
    fn observer(&self) -> &dyn OrchestratorObserver {
        self.config
            .observer
            .as_deref()
            .expect("new_orchestrator fills observer")
    }

    /// 已由构造函数填充的输出目标。
    fn session_output(&self) -> &dyn SessionOutput {
        self.config
            .session_output
            .as_deref()
            .expect("new_orchestrator fills session output")
    }
}

#[async_trait]
impl SessionOutput for StdoutSessionOutput {
    /// 一次性写 JSON 与换行并 flush，避免进程随即退出时缓冲区丢数据。
    async fn write_session(&self, session_json: &[u8]) -> OrchestratorResult<()> {
        let mut output = Vec::with_capacity(session_json.len() + 1);
        output.extend_from_slice(session_json);
        output.push(b'\n');
        let mut stdout = tokio::io::stdout();
        stdout
            .write_all(&output)
            .await
            .map_err(|error| OrchestratorError::message(error.to_string()))?;
        stdout
            .flush()
            .await
            .map_err(|error| OrchestratorError::message(error.to_string()))
    }
}
