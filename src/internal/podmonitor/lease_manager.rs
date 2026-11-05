//! 异步租约实现。
//!
//! 协议层保持原有 heartbeat CAS、stop body、状态码分类和健康文件格式；并发层则用
//! 服务端 heartbeat 时间与 TTL 计算绝对租约期限，用 `Notify` 代替可重置 timer。
//! 两个循环全部登记到 `TaskTracker`，因此退出顺序可被明确等待和测试。

use std::error::Error;
use std::ffi::OsString;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use futures_util::{FutureExt, StreamExt};
use reqwest::header::CONTENT_TYPE;
use tokio::io::AsyncWriteExt;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::internal::api::HttpClient;
use crate::internal::util::{RetryConfig, RetryError, Stopper, retry_operation};

use super::types::{
    DEFAULT_SHUTDOWN_GRACE_PERIOD, HealthFileError, HeartbeatError, HeartbeatResponse, LeaseInfo,
    LeaseManager, LeaseManagerError, LeaseManagerInner, LeaseState, Lifecycle, NO_SERVER_HEARTBEAT,
    SHUTDOWN_GRACE_PERIOD_BUFFER, StopApiError, StopRequest, StopRequestError,
};

/// 从 `HOME` 生成默认健康文件路径；缺少 `HOME` 时回退到 `/tmp`。
///
/// 当前目标平台为 Linux GNU，因此这里刻意采用 Linux 进程环境语义，不引入跨平台
/// home-directory 依赖。
#[must_use]
pub fn get_default_health_file_path() -> PathBuf {
    let home = std::env::var_os("HOME").unwrap_or_else(|| OsString::from("/tmp"));
    PathBuf::from(home).join(".agent-health/envmgr-healthy")
}

/// 按 marker 规则判断 heartbeat 错误是否永久不可恢复。
///
/// 已知 HTTP 错误优先使用状态码；未知错误仍检查稳定的英文 marker，以支持调用方
/// 包装后的错误文本。
#[must_use]
pub fn is_permanent_heartbeat_error(error: Option<&(dyn Error + 'static)>) -> bool {
    let Some(error) = error else {
        return false;
    };
    if let Some(heartbeat_error) = error.downcast_ref::<HeartbeatError>()
        && let Some(status) = heartbeat_error.status_code()
    {
        return matches!(status, 401 | 403 | 404 | 412);
    }

    let text = error.to_string();
    [
        "401",
        "403",
        "404",
        "412",
        "Unauthorized",
        "Forbidden",
        "Precondition Failed",
    ]
    .iter()
    .any(|marker| text.contains(marker))
}

/// 判断 heartbeat 错误是否为 HTTP 412 前置条件失败。
#[must_use]
pub fn is_precondition_failed_error(error: Option<&(dyn Error + 'static)>) -> bool {
    let Some(error) = error else {
        return false;
    };
    if let Some(heartbeat_error) = error.downcast_ref::<HeartbeatError>()
        && heartbeat_error.status_code() == Some(412)
    {
        return true;
    }
    let text = error.to_string();
    text.contains("412") || text.contains("Precondition Failed")
}

/// 按 sentinel 规则判断 stop 失败是否可重试。
///
/// 取消与 deadline 永不重试，明确的 HTTP 4xx 永不重试，HTTP 5xx 与网络/构造错误重试。
#[must_use]
pub fn is_retriable_stop_error(error: &StopRequestError) -> bool {
    match error {
        StopRequestError::Cancelled | StopRequestError::DeadlineExceeded => false,
        StopRequestError::Api(error) => error.status_code >= 500,
        StopRequestError::Request(_) => true,
    }
}

/// 创建 LeaseManager。
///
/// `api_client` 复用 `internal::api::HttpClient` 的 base URL、超时与 Reqwest 连接池；
/// heartbeat/stop 的特殊状态码语义仍由本模块解释，不经过通用 JSON 200-only helper。
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn new_lease_manager(
    parent_cancellation: CancellationToken,
    session_id: impl Into<String>,
    environment_id: impl Into<String>,
    organization_uuid: impl Into<String>,
    api_client: HttpClient,
    interval: Duration,
    safety_window: Duration,
    health_file_path: impl Into<PathBuf>,
) -> LeaseManager {
    LeaseManager {
        inner: Arc::new(LeaseManagerInner {
            session_id: session_id.into(),
            interval,
            safety_window,
            environment_id: environment_id.into(),
            organization_uuid: organization_uuid.into(),
            api_client,
            health_file_path: health_file_path.into(),
            state: std::sync::RwLock::new(LeaseState::default()),
            cancellation: parent_cancellation.child_token(),
            tasks: tokio_util::task::TaskTracker::new(),
            heartbeat_loop_exited: CancellationToken::new(),
            expiry_changed: tokio::sync::Notify::new(),
        }),
    }
}

/// 判断服务端状态是否已经进入停止流程。
fn is_stopping_state(state: &str) -> bool {
    matches!(state, "stopping" | "stopped")
}

/// 把 panic payload 转成可记录文本，同时避免再次 panic。
fn panic_payload_to_string(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|value| (*value).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_owned())
}

impl LeaseManager {
    /// 以关联函数形式提供与 [`new_lease_manager`] 相同的构造入口。
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        parent_cancellation: CancellationToken,
        session_id: impl Into<String>,
        environment_id: impl Into<String>,
        organization_uuid: impl Into<String>,
        api_client: HttpClient,
        interval: Duration,
        safety_window: Duration,
        health_file_path: impl Into<PathBuf>,
    ) -> Self {
        new_lease_manager(
            parent_cancellation,
            session_id,
            environment_id,
            organization_uuid,
            api_client,
            interval,
            safety_window,
            health_file_path,
        )
    }

    /// 获取短时状态读锁；poison 时恢复内部值以延续停止流程。
    fn read_state(&self) -> RwLockReadGuard<'_, LeaseState> {
        self.inner
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 获取短时状态写锁；poison 时恢复内部值以延续停止流程。
    fn write_state(&self) -> RwLockWriteGuard<'_, LeaseState> {
        self.inner
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 发送初始 heartbeat，然后原子登记 heartbeat、expiry 两个任务。
    ///
    /// 初始请求期间生命周期保持 `Starting`。显式停止会先把它改为 `Stopping`，所以
    /// `TaskTracker::close()` 与这里的 spawn 不会发生“wait 已返回后又新增任务”的竞态。
    pub async fn start(&self) -> Result<(), LeaseManagerError> {
        {
            let mut state = self.write_state();
            if state.lifecycle != Lifecycle::Created {
                return Err(LeaseManagerError::AlreadyStarted);
            }
            state.lifecycle = Lifecycle::Starting;
        }

        if let Err(error) = self.send_heartbeat().await {
            let mut state = self.write_state();
            if state.lifecycle == Lifecycle::Starting {
                state.lifecycle = Lifecycle::Created;
            }
            return Err(LeaseManagerError::InitialHeartbeat(error));
        }

        {
            let mut state = self.write_state();
            if state.lifecycle != Lifecycle::Starting {
                if state.shutdown_started {
                    // 即使 initial heartbeat 已把状态推进到 stopping，也仍会启动
                    // heartbeat loop；该 loop 退出时会通知正在等待的 stop 调用。
                    self.spawn_background_tasks();
                } else {
                    return Err(LeaseManagerError::StoppedWhileStarting);
                }
            } else {
                state.lifecycle = Lifecycle::Running;
                // spawn 本身不 await；持锁登记任务可与 stop 的 lifecycle 修改形成原子边界。
                self.spawn_background_tasks();
            }
        }

        tracing::info!("session_id" = ?(self.inner.session_id), "heartbeat_interval_seconds" = ?(self.inner.interval.as_secs()), "safety_window_seconds" = ?(self.inner.safety_window.as_secs()), "{}", "lease_manager_started");
        Ok(())
    }

    /// 将两个后台循环登记到同一个 TaskTracker。
    fn spawn_background_tasks(&self) {
        let heartbeat = self.clone();
        drop(
            self.inner
                .tasks
                .spawn(async move { heartbeat.run_heartbeat_task().await }),
        );

        let expiry = self.clone();
        drop(
            self.inner
                .tasks
                .spawn(async move { expiry.lease_expiry_loop().await }),
        );
    }

    /// 捕获 heartbeat loop panic，并在退出时完成取消与宽限期设置。
    async fn run_heartbeat_task(self) {
        let result = AssertUnwindSafe(self.heartbeat_loop()).catch_unwind().await;
        if let Err(payload) = result {
            tracing::error!("session_id" = ?(self.inner.session_id), "panic" = ?(panic_payload_to_string(payload.as_ref())), "{}", "heartbeat_loop_panic");
            self.set_grace_period_and_cancel();
        }
        // 独立 token 在 heartbeat loop 退出时通知所有等待者。
        self.inner.heartbeat_loop_exited.cancel();
    }

    /// 周期发送 heartbeat；临时错误保留租约计时，永久错误先 stop 再取消。
    async fn heartbeat_loop(&self) {
        let mut ticker = tokio::time::interval(self.inner.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // Tokio interval 的首 tick 立即就绪；消费它以匹配 NewTicker 首次等待一个周期。
        ticker.tick().await;

        loop {
            tokio::select! {
                           _ = ticker.tick() => {
                               if self.inner.cancellation.is_cancelled() {
            // 周期 heartbeat 分支；
            // 取消时仍保留该分支自身的业务日志。
                                   tracing::info!(session_id = %self.inner.session_id, error = "operation cancelled", "heartbeat_loop_stopped");
                                   return;
                               }
                           }
                           _ = self.inner.cancellation.cancelled() => {
            // 显式取消分支。
            // 显式取消单独记录循环退出原因。
                               tracing::info!("session_id" = ?(self.inner.session_id), "error" = ?("operation cancelled"), "{}", "heartbeat_loop_stopped");
                               return;
                           }
                       }

            if let Err(error) = self.send_heartbeat().await {
                if is_permanent_heartbeat_error(Some(&error)) {
                    let message = if is_precondition_failed_error(Some(&error)) {
                        "heartbeat_precondition_failed"
                    } else {
                        "permanent_heartbeat_error"
                    };
                    tracing::error!("session_id" = ?(self.inner.session_id), "error" = ?(error.to_string()), "{}", message);
                    let cancellation = self.inner.cancellation.clone();
                    if let Err(stop_error) = self.call_stop_api(&cancellation, false).await {
                        tracing::error!("session_id" = ?(self.inner.session_id), "error" = ?(stop_error.to_string()), "{}", "failed_to_stop_after_heartbeat_error");
                    }
                    self.set_grace_period_and_cancel();
                    return;
                }

                tracing::warn!("session_id" = ?(self.inner.session_id), "error" = ?(error.to_string()), "{}", "heartbeat_failed");
                continue;
            }

            // 租约未延长时仍需读取终态并完成停止流程，不能在此提前退出。
            let state = self.read_state().instance_state.clone();
            if is_stopping_state(&state) {
                tracing::info!("session_id" = ?(self.inner.session_id), "state" = ?(state), "{}", "session_entered_terminal_state");
                let cancellation = self.inner.cancellation.clone();
                if let Err(error) = self.call_stop_api(&cancellation, false).await {
                    tracing::error!("session_id" = ?(self.inner.session_id), "state" = ?(state), "error" = ?(error.to_string()), "{}", "failed_to_stop_terminal_session");
                }
                self.set_grace_period_and_cancel();
                return;
            }
        }
    }

    /// 发送一次 heartbeat，并在 200 响应后原子更新租约和活动状态。
    pub(crate) async fn send_heartbeat(&self) -> Result<(), HeartbeatError> {
        let now = Utc::now();
        let expected_last_heartbeat = self
            .read_state()
            .server_last_heartbeat
            .as_deref()
            .filter(|value| !value.is_empty())
            .unwrap_or(NO_SERVER_HEARTBEAT)
            .to_owned();
        let endpoint = self
            .endpoint("heartbeat", Some(&expected_last_heartbeat))
            .map_err(HeartbeatError::InvalidRequest)?;

        let request = self.inner.api_client.http_client.post(endpoint).send();
        let response = tokio::select! {
            _ = self.inner.cancellation.cancelled() => return Err(HeartbeatError::Cancelled),
            response = request => response.map_err(|error| HeartbeatError::Request(error.to_string()))?,
        };
        let status = response.status();
        let body = tokio::select! {
            _ = self.inner.cancellation.cancelled() => return Err(HeartbeatError::Cancelled),
            body = response.bytes() => body.map_err(|error| HeartbeatError::Request(error.to_string()))?,
        };
        if status != reqwest::StatusCode::OK {
            return Err(HeartbeatError::Http {
                status_code: status.as_u16(),
                body: String::from_utf8_lossy(&body).trim().to_owned(),
            });
        }

        let heartbeat: HeartbeatResponse = serde_json::from_slice(&body)
            .map_err(|error| HeartbeatError::Decode(error.to_string()))?;
        if heartbeat.response_type != "work_heartbeat" {
            return Err(HeartbeatError::Decode(
                "heartbeat response type must be work_heartbeat".into(),
            ));
        }
        if !heartbeat.lease_extended {
            tracing::warn!("session_id" = ?(self.inner.session_id), "state" = ?(heartbeat.state), "{}", "heartbeat_lease_not_extended");
            self.write_state().instance_state = heartbeat.state.clone();

            if is_stopping_state(&heartbeat.state) {
                let cancellation = self.inner.cancellation.clone();
                if let Err(error) = self.call_stop_api(&cancellation, false).await {
                    tracing::error!("session_id" = ?(self.inner.session_id), "state" = ?(heartbeat.state), "error" = ?(error.to_string()), "{}", "failed_to_stop_after_lease_not_extended");
                }
                self.set_grace_period_and_cancel();
            }
            return Ok(());
        }

        let heartbeat_at = DateTime::parse_from_rfc3339(&heartbeat.last_heartbeat)
            .map_err(|error| HeartbeatError::Decode(format!("invalid last_heartbeat: {error}")))?
            .with_timezone(&Utc);
        let lease_ttl = u64::try_from(heartbeat.ttl_seconds)
            .ok()
            .filter(|seconds| *seconds > 0)
            .map(Duration::from_secs)
            .ok_or_else(|| HeartbeatError::Decode("ttl_seconds must be positive".into()))?;
        let lease_expires_at = heartbeat_at
            .checked_add_signed(ChronoDuration::seconds(heartbeat.ttl_seconds))
            .ok_or_else(|| {
                HeartbeatError::Decode("lease expiry is outside datetime range".into())
            })?;
        {
            let mut state = self.write_state();
            state.last_heartbeat = Some(now);
            state.lease_ttl = lease_ttl;
            state.lease_expires_at = Some(lease_expires_at);
            state.instance_state = heartbeat.state.clone();
            state.server_last_heartbeat = Some(heartbeat.last_heartbeat.clone());
        }

        self.schedule_lease_expiry_check();
        tracing::info!("session_id" = ?(self.inner.session_id), "state" = ?(heartbeat.state), "ttl_seconds" = ?(heartbeat.ttl_seconds), "lease_extended" = ?(true), "{}", "heartbeat_success");
        if let Err(error) = self.update_health_file().await {
            tracing::warn!("session_id" = ?(self.inner.session_id), "path" = ?(self.inner.health_file_path), "error" = ?(error.to_string()), "{}", "failed_to_update_health_file");
        }
        Ok(())
    }

    /// 返回距离绝对租约过期时间的剩余时长；未知或已过期时为零。
    #[must_use]
    pub fn time_until_expiry(&self) -> Duration {
        self.read_state()
            .lease_expires_at
            .map(|expires_at| (expires_at - Utc::now()).to_std().unwrap_or(Duration::ZERO))
            .unwrap_or(Duration::ZERO)
    }

    /// 通知租约检测任务重新计算安全窗口 deadline；不额外创建 timer task。
    fn schedule_lease_expiry_check(&self) {
        self.inner.expiry_changed.notify_one();
    }

    /// 计算 expiry loop 下一次等待；没有租约时返回 `None`。
    fn lease_expiry_delay(&self) -> Option<Duration> {
        self.read_state().lease_expires_at.map(|expires_at| {
            (expires_at - Utc::now())
                .to_std()
                .unwrap_or(Duration::ZERO)
                .saturating_sub(self.inner.safety_window)
        })
    }

    /// 长驻租约过期循环，在 heartbeat 更新时被 Notify 唤醒并重算 deadline。
    async fn lease_expiry_loop(&self) {
        loop {
            let delay = self.lease_expiry_delay();
            match delay {
                None => {
                    tokio::select! {
                        _ = self.inner.cancellation.cancelled() => return,
                        _ = self.inner.expiry_changed.notified() => continue,
                    }
                }
                Some(delay) => {
                    tokio::select! {
                        _ = self.inner.cancellation.cancelled() => return,
                        _ = self.inner.expiry_changed.notified() => continue,
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }

            if self
                .lease_expiry_delay()
                .is_some_and(|remaining| !remaining.is_zero())
            {
                continue;
            }
            let lease_expires_at = self.read_state().lease_expires_at;
            tracing::error!("session_id" = ?(self.inner.session_id), "lease_expires_at" = ?(lease_expires_at), "{}", "lease_expiry_detected");
            let cancellation = self.inner.cancellation.clone();
            if let Err(error) = self.call_stop_api(&cancellation, false).await {
                tracing::error!("session_id" = ?(self.inner.session_id), "error" = ?(error.to_string()), "{}", "failed_to_stop_expired_session");
            }
            self.set_grace_period_and_cancel();
            return;
        }
    }

    /// 只在第一个 fatal 路径中计算宽限期，然后取消并关闭 tracker。
    ///
    /// 所有权/退出顺序为：持写锁写入 `Stopping` 和 grace → 释放锁 → 取消 token →
    /// `TaskTracker::close()`。当前任务随后返回，另一个任务在 select 中观察取消；
    /// heartbeat loop 退出时再触发独立的 loop-exited token；`stop_and_wait` 先等该 token，
    /// 再等 tracker 计数归零。
    pub(super) fn set_grace_period_and_cancel(&self) {
        {
            let mut state = self.write_state();
            if state.shutdown_started {
                return;
            }
            let grace_period = state
                .lease_expires_at
                .and_then(|expires_at| {
                    (expires_at - Utc::now()
                        + ChronoDuration::from_std(SHUTDOWN_GRACE_PERIOD_BUFFER).ok()?)
                    .to_std()
                    .ok()
                })
                .filter(|duration| !duration.is_zero())
                .unwrap_or(DEFAULT_SHUTDOWN_GRACE_PERIOD);
            state.shutdown_grace_period = grace_period;
            state.shutdown_started = true;
            state.lifecycle = Lifecycle::Stopping;
        }
        self.inner.cancellation.cancel();
        self.inner.tasks.close();
        self.inner.expiry_changed.notify_waiters();
    }

    /// 返回 fatal 退出路径计算出的宽限期；尚未 fatal 时为零。
    #[must_use]
    pub fn get_grace_period(&self) -> Duration {
        self.read_state().shutdown_grace_period
    }

    /// 返回最近 heartbeat、绝对过期时间和服务端状态的一致快照。
    #[must_use]
    pub fn get_lease_info(&self) -> LeaseInfo {
        let state = self.read_state();
        LeaseInfo {
            last_heartbeat: state.last_heartbeat,
            lease_expires_at: state.lease_expires_at,
            instance_state: state.instance_state.clone(),
        }
    }

    /// 返回 manager 子 token 的 clone，供 session/orchestrator 监听退出。
    ///
    /// 克隆共享同一取消状态，不会创建独立生命周期，也不会把取消权限暴露成
    /// 可替换的全局 context。
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancellation.clone()
    }

    /// 使用调用方提供的 token 发送 `force=true` stop 请求。
    pub async fn call_stop_api_forced(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), StopRequestError> {
        self.call_stop_api(cancellation, true).await
    }

    /// 构造 heartbeat 或 stop URL，并按 segment 规则转义 ID。
    fn endpoint(&self, action: &str, expected_last_heartbeat: Option<&str>) -> Result<Url, String> {
        let mut endpoint = Url::parse(self.inner.api_client.base_url())
            .map_err(|error| format!("invalid API base URL: {error}"))?;
        {
            let mut segments = endpoint
                .path_segments_mut()
                .map_err(|_| "API base URL cannot contain path segments".to_owned())?;
            segments.pop_if_empty();
            segments
                .push("v1")
                .push("environments")
                .push(&self.inner.environment_id)
                .push("work")
                .push(&self.inner.session_id)
                .push(action);
        }
        let mut query = endpoint.query_pairs_mut();
        query.clear();
        query.append_pair("organization_uuid", &self.inner.organization_uuid);
        if let Some(expected_last_heartbeat) = expected_last_heartbeat {
            query.append_pair("expected_last_heartbeat", expected_last_heartbeat);
        }
        drop(query);
        Ok(endpoint)
    }

    /// 调用 stop API，并以 1 秒、2 秒退避最多尝试三次。
    pub(crate) async fn call_stop_api(
        &self,
        cancellation: &CancellationToken,
        force: bool,
    ) -> Result<(), StopRequestError> {
        let body = serde_json::to_vec(&StopRequest { force }).map_err(|error| {
            StopRequestError::Request(format!("failed to serialize stop request body: {error}"))
        })?;
        let endpoint = self.endpoint("stop", None).map_err(|error| {
            StopRequestError::Request(format!("failed to create stop request: {error}"))
        })?;
        let config =
            RetryConfig::new(3, Duration::from_secs(1)).with_classifier(is_retriable_stop_error);
        let result = retry_operation(
            |_| {
                let client = self.inner.api_client.http_client.clone();
                let endpoint = endpoint.clone();
                let body = body.clone();
                let cancellation = cancellation.clone();
                async move {
                    let request = client
                        .post(endpoint)
                        .header(CONTENT_TYPE, "application/json")
                        .body(body)
                        .send();
                    let response = tokio::select! {
                    // 每轮请求都观察取消令牌；若在退避期间取消，下一轮会立即停止。
                                           biased;
                                           _ = cancellation.cancelled() => {
                                               return Err(StopRequestError::Cancelled);
                                           }
                                           response = request => response.map_err(|error| {
                                               if error.is_timeout() {
                                                   StopRequestError::DeadlineExceeded
                                               } else {
                                                   StopRequestError::Request(format!(
                                                       "stop API request failed: {error}"
                                                   ))
                                               }
                                           })?,
                                       };
                    let status = response.status();
                    if status.is_success() {
                        return Ok(());
                    }
                    // 收到非 2xx headers 后仍逐块读取正文，同时观察项目取消令牌；
                    // 取消时保留已收到的正文，避免等待客户端的长超时。
                    let response_body =
                        read_stop_response_body(response.bytes_stream(), &cancellation).await;
                    let response_text = String::from_utf8_lossy(&response_body);
                    let response_text = response_text.trim();
                    let message = if response_text.is_empty() {
                        format!("stop API returned status: {}", status.as_u16())
                    } else {
                        format!(
                            "stop API returned status: {}: {response_text}",
                            status.as_u16()
                        )
                    };
                    Err(StopRequestError::Api(StopApiError::new(
                        status.as_u16(),
                        message,
                    )))
                }
            },
            &config,
        )
        .await;

        match result {
            Ok(()) => {
                tracing::info!("session_id" = ?(self.inner.session_id), "force" = ?(force), "{}", "Session terminated");
                Ok(())
            }
            Err(RetryError::InvalidMaxRetries) => Err(StopRequestError::Request(
                "stop API retry configuration requires at least one attempt".to_owned(),
            )),
            Err(RetryError::Operation(error)) => Err(error),
        }
    }

    /// 更新健康文件；空路径无操作，内容为 UTC RFC 3339 纳秒时间戳加换行。
    pub(crate) async fn update_health_file(&self) -> Result<(), HealthFileError> {
        let path = &self.inner.health_file_path;
        if path.as_os_str().is_empty() {
            return Ok(());
        }
        if let Some(parent) = path.parent()
            && parent != Path::new("")
        {
            let mut directory = tokio::fs::DirBuilder::new();
            directory.recursive(true);
            #[cfg(unix)]
            directory.mode(0o755);
            directory
                .create(parent)
                .await
                .map_err(|source| HealthFileError::CreateDirectory {
                    path: parent.to_path_buf(),
                    source,
                })?;
        }
        let content = format!(
            "{}\n",
            Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true)
        );
        let mut options = tokio::fs::OpenOptions::new();
        options.create(true).write(true).truncate(true);
        #[cfg(unix)]
        options.mode(0o644);
        let mut file = options
            .open(path)
            .await
            .map_err(|source| HealthFileError::Write {
                path: path.clone(),
                source,
            })?;
        file.write_all(content.as_bytes())
            .await
            .map_err(|source| HealthFileError::Write {
                path: path.clone(),
                source,
            })?;
        file.flush().await.map_err(|source| HealthFileError::Write {
            path: path.clone(),
            source,
        })
    }

    /// 同步发出显式停止信号，并关闭 tracker 以允许等待完成。
    fn request_stop(&self) {
        {
            let mut state = self.write_state();
            if state.lifecycle != Lifecycle::Stopped {
                state.lifecycle = Lifecycle::Stopping;
            }
        }
        self.inner.cancellation.cancel();
        self.inner.tasks.close();
        self.inner.expiry_changed.notify_waiters();
    }

    /// 取消后等待 heartbeat loop 关闭信号和全部受追踪任务退出。
    ///
    /// `heartbeat_loop_exited` 只在 heartbeat loop 退出时触发，因此尚未 `start` 或初始
    /// heartbeat 失败时该方法会一直等待。独立 token 保留这一
    /// 可观察行为；调用方若要限制等待时间，应在外层使用 `tokio::time::timeout`。
    pub async fn stop(&self) {
        self.stop_and_wait().await;
    }

    /// 以明确名称执行“发出停止、等待 heartbeat 关闭点、再等待任务归零”的完整序列。
    pub async fn stop_and_wait(&self) {
        self.request_stop();
        self.inner.heartbeat_loop_exited.cancelled().await;
        self.inner.tasks.wait().await;
        self.write_state().lifecycle = Lifecycle::Stopped;
    }
}

/// 读取 stop API 的错误响应体，并在取消时返回已经收到的部分字节。
///
/// 调用方忽略读取错误但保留 partial body；这里把任意 stream 错误和取消都视为读取结束，
/// 既不把它们升级成新的请求错误，也不会等待 Reqwest 的全局超时。
///
/// # 参数
/// - `response_stream`：按网络到达顺序产生 response body chunk 的一次性 stream。
/// - `cancellation`：本轮 stop 请求的取消令牌；触发后不再等待下一块数据。
///
/// # 返回
/// 取消或读取错误前已经完整收到的所有 chunk，保持原始字节顺序。
pub(super) async fn read_stop_response_body<S, B, E>(
    response_stream: S,
    cancellation: &CancellationToken,
) -> Vec<u8>
where
    S: futures_util::Stream<Item = Result<B, E>>,
    B: AsRef<[u8]>,
{
    futures_util::pin_mut!(response_stream);
    let mut response_body = Vec::new();
    loop {
        let next = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            next = response_stream.next() => next,
        };
        match next {
            Some(Ok(chunk)) => response_body.extend_from_slice(chunk.as_ref()),
            // 明确忽略响应体读取错误；已经累积的部分正文仍参与错误文案，
            // 不能把 body read error 替换成请求错误。
            Some(Err(_)) | None => break,
        }
    }
    response_body
}

#[async_trait]
impl Stopper for LeaseManager {
    /// 通过统一接口同步取消 lease 后台任务，但不等待它们退出。
    fn request_stop(&self) {
        LeaseManager::request_stop(self);
    }

    /// 通过统一接口执行与固有 `Stop` 相同的取消并等待流程。
    async fn stop(&self) {
        LeaseManager::stop(self).await;
    }
}
