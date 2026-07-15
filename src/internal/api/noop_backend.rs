//! 对应 Go `internal/api/noop_backend.go` 的无副作用 session backend。
//!
//! local-testing 等无需远端事件的流程仍通过同一 trait 运行，避免在 manager 中散布条件分支。

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::{ApiError, DiagLogEntry, OtlpEndpoints, SessionBackend, SessionEvent};

/// 丢弃所有事件和日志的 backend。
#[derive(Clone, Debug, Default)]
pub struct NoopBackend;

impl NoopBackend {
    /// 对应 Go `NewNoopBackend`。
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SessionBackend for NoopBackend {
    /// 对应 Go `PostEvent`，无条件成功且不产生副作用。
    async fn post_event(
        &self,
        _cancellation: &CancellationToken,
        _event: &SessionEvent,
    ) -> Result<(), ApiError> {
        Ok(())
    }

    /// 对应 Go `FlushLogs`，无条件成功且不产生副作用。
    async fn flush_logs(
        &self,
        _cancellation: &CancellationToken,
        _logs: &[DiagLogEntry],
    ) -> Result<(), ApiError> {
        Ok(())
    }

    /// 对应 Go `OtlpEndpoints`，返回空端点和空 header map。
    fn otlp_endpoints(&self) -> OtlpEndpoints {
        OtlpEndpoints::default()
    }
}
