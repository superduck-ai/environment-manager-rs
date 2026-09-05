//! session 活动记录器的数据类型与接口。
//!
//! 会话事件与活动记录器的公共类型。同步 `EventPoster` 在调用点
//! 实际执行网络 I/O，因此 Rust 接口使用 async trait，避免阻塞 Tokio worker。

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::internal::api::{
    ApiError, LogCategory, LogLevel, SessionBackend, SessionError, SessionEvent,
};
use crate::internal::util::Stopper;

/// 负责把单个 session event 发送到后端的最小异步接口。
#[async_trait]
pub trait EventPoster: Send + Sync {
    /// 发送一个事件；取消 token 被触发后实现应尽快终止网络操作。
    async fn post_event(
        &self,
        cancellation: &CancellationToken,
        event: &SessionEvent,
    ) -> Result<(), ApiError>;
}

#[async_trait]
impl<T> EventPoster for T
where
    T: SessionBackend + Send + Sync + ?Sized,
{
    /// 让任意 `SessionBackend` 自动满足 session recorder 的事件发送边界。
    async fn post_event(
        &self,
        cancellation: &CancellationToken,
        event: &SessionEvent,
    ) -> Result<(), ApiError> {
        SessionBackend::post_event(self, cancellation, event).await
    }
}

/// 活动记录操作失败。
#[derive(Debug, Error)]
pub enum ActivityRecorderError {
    /// 合成 assistant 消息发送失败；此时不会再发送 result 事件。
    #[error("failed to post synthetic assistant event: {0}")]
    SyntheticAssistant(#[source] ApiError),
    /// assistant 已成功，但 result 事件发送失败。
    #[error("failed to post result event: {0}")]
    ResultEvent(#[source] ApiError),
    /// 两个失败事件未能在共享的五秒窗口内完成。
    #[error("failed to post failure result within 5 seconds")]
    FailureResultTimeout,
}

/// session 生命周期内的活动记录接口。
#[async_trait]
pub trait ActivityRecorder: Send + Sync {
    /// 异步后台发送一条环境管理器日志事件。
    fn record_activity(
        &self,
        level: LogLevel,
        category: LogCategory,
        message: String,
        extra: BTreeMap<String, Value>,
    );

    /// 等待关键活动发送完成；同步记录器可沿用普通记录方法。
    async fn record_activity_and_wait(
        &self,
        level: LogLevel,
        category: LogCategory,
        message: String,
        extra: BTreeMap<String, Value>,
    ) {
        self.record_activity(level, category, message, extra);
    }

    /// 启动长任务进度日志并返回可幂等停止的句柄。
    fn record_long_running_activity(
        &self,
        category: LogCategory,
        message: String,
    ) -> Arc<dyn Stopper>;

    /// 顺序发送合成 assistant 错误和最终 result 事件。
    async fn record_failure_result(
        &self,
        session_error: &(dyn SessionError + Send + Sync),
    ) -> Result<(), ActivityRecorderError>;
}

/// 活动记录器的周期参数。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecorderOptions {
    /// 长任务普通进度日志间隔；零值回退到 30 秒。
    pub interval: Duration,
    /// 长任务自动停止日志的最大时长；零值回退到 15 分钟。
    pub max_duration: Duration,
}

impl Default for RecorderOptions {
    /// 返回默认的 30 秒请求超时和 15 分钟长任务超时。
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            max_duration: Duration::from_secs(15 * 60),
        }
    }
}

/// 不产生任何事件的 recorder。
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopActivityRecorder;

/// 向 session backend 发送活动事件的 recorder。
pub struct SessionActivityRecorder {
    pub(crate) cancellation: CancellationToken,
    pub(crate) poster: Arc<dyn EventPoster>,
    pub(crate) interval: Duration,
    pub(crate) max_duration: Duration,
}

impl std::fmt::Debug for SessionActivityRecorder {
    /// 只输出时序配置和取消状态，不展开 trait object 或后端凭据。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionActivityRecorder")
            .field("is_cancelled", &self.cancellation.is_cancelled())
            .field("interval", &self.interval)
            .field("max_duration", &self.max_duration)
            .finish_non_exhaustive()
    }
}

/// 长任务首次/后续日志的共享状态。
#[derive(Debug)]
pub struct LongRunningActivityState {
    /// true 表示下一条是初始消息；首次发送后永久变为 false。
    pub(crate) active: Mutex<bool>,
}

impl LongRunningActivityState {
    /// 创建尚未发送首条日志的状态。
    #[must_use]
    pub fn new() -> Self {
        Self {
            active: Mutex::new(true),
        }
    }

    /// 生成下一条内容并原子地推进到“仍在进行”状态。
    ///
    /// 锁只保护布尔状态和字符串选择，调用方必须在执行任何 `.await` 前释放锁。
    #[must_use]
    pub fn next_content(&self, message: &str) -> String {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let content = if *active {
            message.to_owned()
        } else {
            format!("{message} (still in progress...)")
        };
        *active = false;
        content
    }
}

impl Default for LongRunningActivityState {
    /// 默认状态等价于刚创建、尚未发送初始日志。
    fn default() -> Self {
        Self::new()
    }
}
