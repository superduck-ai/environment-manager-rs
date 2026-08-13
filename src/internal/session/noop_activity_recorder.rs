//! 不产生副作用的 session 活动记录器。
//!
//! 用于未启用 session ingress 的运行路径，同时保持调用方无需分支判断。

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::internal::api::{LogCategory, LogLevel, SessionError};
use crate::internal::util::Stopper;

use super::{ActivityRecorder, ActivityRecorderError, NoopActivityRecorder};

/// 不执行任何操作的停止句柄。
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopStopper;

#[async_trait]
impl Stopper for NoopStopper {
    /// 接受同步停止请求但不执行任何操作。
    fn request_stop(&self) {}

    /// 接受停止请求但不执行任何操作。
    async fn stop(&self) {}
}

impl NoopActivityRecorder {
    /// 接受一条活动日志但不发送事件。
    pub fn record_activity(
        &self,
        _level: LogLevel,
        _category: LogCategory,
        _message: String,
        _extra: BTreeMap<String, Value>,
    ) {
    }

    /// 返回始终可安全停止的空句柄。
    #[must_use]
    pub fn record_long_running_activity(
        &self,
        _category: LogCategory,
        _message: String,
    ) -> Arc<dyn Stopper> {
        Arc::new(NoopStopper)
    }

    /// 接受 session 错误但不发送 assistant 或 result 事件。
    pub async fn record_failure_result(
        &self,
        _session_error: &(dyn SessionError + Send + Sync),
    ) -> Result<(), ActivityRecorderError> {
        Ok(())
    }
}

#[async_trait]
impl ActivityRecorder for NoopActivityRecorder {
    /// 通过统一接口丢弃一条活动日志。
    fn record_activity(
        &self,
        level: LogLevel,
        category: LogCategory,
        message: String,
        extra: BTreeMap<String, Value>,
    ) {
        NoopActivityRecorder::record_activity(self, level, category, message, extra);
    }

    /// 通过统一接口创建空停止句柄。
    fn record_long_running_activity(
        &self,
        category: LogCategory,
        message: String,
    ) -> Arc<dyn Stopper> {
        NoopActivityRecorder::record_long_running_activity(self, category, message)
    }

    /// 通过统一接口忽略 session 错误。
    async fn record_failure_result(
        &self,
        session_error: &(dyn SessionError + Send + Sync),
    ) -> Result<(), ActivityRecorderError> {
        NoopActivityRecorder::record_failure_result(self, session_error).await
    }
}
