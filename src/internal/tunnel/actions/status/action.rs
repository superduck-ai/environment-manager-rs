//! 状态探测 action 实现。
//!
//! 对应 Go `internal/tunnel/actions/status/action.go`。只连接 loopback，使用一秒连接
//! 超时，并在成功后立即关闭连接，不向被探测服务发送任何应用数据。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

use crate::internal::tunnel::actions::{
    Action, ActionError, ActionRequest, ActionResult, ProgressReporter,
};

use super::types::{StatusAction, StatusRequest, StatusResult};

/// 对应 Go `(*StatusAction).Timeout`：端口探测 action 最多运行五秒。
pub const STATUS_TIMEOUT: Duration = Duration::from_secs(5);

/// 对应 Go：`status.NewStatusAction`。
#[must_use]
pub fn new_status_action() -> StatusAction {
    StatusAction {}
}

#[async_trait]
impl Action for StatusAction {
    /// 对应 Go：`(*StatusAction).Execute`。
    ///
    /// 解析可选端口并尝试连接 `localhost:<port>`。连接失败不是 action 错误，而是
    /// `listening:false`；只有 JSON 或端口范围无效才返回错误。
    async fn execute(
        &self,
        cancellation: CancellationToken,
        request: ActionRequest,
        _progress: Arc<dyn ProgressReporter>,
    ) -> Result<ActionResult, ActionError> {
        let mut status_request = if request.body.is_empty() {
            StatusRequest::default()
        } else {
            serde_json::from_slice(&request.body)
                .map_err(|error| format!("invalid request body: {error}"))?
        };
        if status_request.port == 0 {
            status_request.port = 3000;
        }
        if !(1..=65_535).contains(&status_request.port) {
            return Err(format!("invalid port: {}", status_request.port).into());
        }

        let address = format!("localhost:{}", status_request.port);
        // Go net.Dialer 同时遵守 context 与一秒 Dialer timeout；select 保持父 action
        // 取消优先，避免 Registry 超时后仍残留连接尝试。
        let listening = tokio::select! {
            () = cancellation.cancelled() => false,
            result = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(&address)) => {
                matches!(result, Ok(Ok(_stream)))
            }
        };

        tracing::info!(
            session_id = %request.session_id,
            address,
            port = status_request.port,
            listening,
            "status_check"
        );

        Ok(ActionResult {
            status_code: 200,
            body: serde_json::to_vec(&StatusResult {
                listening,
                port: status_request.port,
            })?,
        })
    }

    /// 对应 Go：`(*StatusAction).Name`。
    fn name(&self) -> &str {
        "status"
    }

    /// 对应 Go：`(*StatusAction).Timeout`。
    fn timeout(&self) -> Duration {
        STATUS_TIMEOUT
    }
}
