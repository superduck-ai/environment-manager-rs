//! 对应 Go `internal/api/session_ingress_backend.go` 的普通 SessionIngress 后端。
//!
//! 该适配器只绑定 session ID，并把事件、诊断日志和 OTLP 端点统一暴露给 manager。

use std::collections::BTreeMap;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::internal::logger::Logger;

use super::{
    ApiError, DiagLogEntry, HttpClient, HttpSessionIngressClient, OtlpEndpoints, SessionBackend,
    SessionEvent,
};

/// 普通 SessionIngress backend。
pub struct SessionIngressBackend {
    client: HttpSessionIngressClient,
    session_id: String,
    api_base: String,
    auth_token: String,
}

impl SessionIngressBackend {
    /// 对应 Go `NewSessionIngressBackend`，构造默认 v2 SessionIngress client。
    pub fn new(
        api_base_url: &str,
        auth_token: impl Into<String>,
        session_id: impl Into<String>,
        logger: Option<Logger>,
    ) -> Result<Self, ApiError> {
        let auth_token = auth_token.into();
        let session_id = session_id.into();
        let http_client = HttpClient::new(api_base_url, logger.clone(), None)?;
        let client = HttpSessionIngressClient::new(http_client, auth_token.clone(), logger, false);
        Ok(Self {
            client,
            session_id,
            // Go 为事件 client 构造独立的规范化 HttpClient，但 OTLP endpoint 使用
            // 构造参数原文。这里不能复用 `http_client.base_url()`，也不能清理尾 `/`。
            api_base: api_base_url.to_owned(),
            auth_token,
        })
    }
}

#[async_trait]
impl SessionBackend for SessionIngressBackend {
    /// 对应 Go `PostEvent`，将事件发送到绑定 session。
    async fn post_event(
        &self,
        cancellation: &CancellationToken,
        event: &SessionEvent,
    ) -> Result<(), ApiError> {
        self.client
            .post_session_event(cancellation, &self.session_id, event)
            .await
    }

    /// 对应 Go `FlushLogs`，为底层错误增加稳定上下文。
    async fn flush_logs(
        &self,
        cancellation: &CancellationToken,
        logs: &[DiagLogEntry],
    ) -> Result<(), ApiError> {
        self.client
            .post_forward_diag_logs(cancellation, &self.session_id, logs)
            .await
            // Go 使用 `%w` 包装，因此 501 sentinel 必须穿过这一层，供 diag uploader
            // 判定旧部署尚未实现 endpoint 并停止后续上传。
            .map_err(|error| {
                error.with_context("failed to flush diagnostic logs to session ingress")
            })
    }

    /// 对应 Go `OtlpEndpoints`，返回 SessionService ExportMetrics/ExportLogs 地址。
    fn otlp_endpoints(&self) -> OtlpEndpoints {
        OtlpEndpoints {
            metrics_endpoint: format!(
                "{}/anthropic.session_ingress.api.v1alpha.SessionService/ExportMetrics",
                self.api_base
            ),
            logs_endpoint: format!(
                "{}/anthropic.session_ingress.api.v1alpha.SessionService/ExportLogs",
                self.api_base
            ),
            headers: BTreeMap::from([
                (
                    "Authorization".to_owned(),
                    format!("Bearer {}", self.auth_token),
                ),
                ("x-session-id".to_owned(), self.session_id.clone()),
            ]),
        }
    }
}
