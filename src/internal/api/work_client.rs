//! 对应 Go `internal/api/work_client.go` 的 work ACK 客户端。
//!
//! ACK 只接受 HTTP 200 或 204；认证、beta 和版本 header 的名称与 Go 完全一致。

use reqwest::header::HeaderValue;
use reqwest::{Method, StatusCode};
use tokio_util::sync::CancellationToken;

use crate::internal::util::go_path_escape;

use super::client::{add_version_headers, read_response_body_ignoring_errors};
use super::{ApiError, HttpClient};

/// 环境 work API 客户端。
#[derive(Clone)]
pub struct WorkClient {
    http_client: HttpClient,
    auth_token: String,
}

impl WorkClient {
    /// 保存共享 HTTP client 和 service key。
    pub fn new(http_client: HttpClient, auth_token: impl Into<String>) -> Self {
        Self {
            http_client,
            auth_token: auth_token.into(),
        }
    }

    /// 对应 Go `AcknowledgeWork`，确认 worker 已取得指定任务。
    ///
    /// # 副作用
    /// 向 `/v1/environments/{environment}/work/{work}/ack` 发送可重试 POST。
    pub async fn acknowledge_work(
        &self,
        cancellation: &CancellationToken,
        environment_id: &str,
        work_id: &str,
    ) -> Result<(), ApiError> {
        let endpoint = format!(
            "{}/v1/environments/{}/work/{}/ack",
            self.http_client.base_url(),
            go_path_escape(environment_id),
            go_path_escape(work_id)
        );
        let mut request = self
            .http_client
            .http_client
            .request(Method::POST, &endpoint)
            .build()
            .map_err(|error| ApiError::message(format!("failed to create ACK request: {error}")))?;
        self.http_client
            .set_auth_header(&mut request, &self.auth_token)?;
        request
            .headers_mut()
            .insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        request.headers_mut().insert(
            "anthropic-beta",
            HeaderValue::from_static("environments-2025-11-01"),
        );
        add_version_headers(request.headers_mut())?;

        tracing::debug!("endpoint" = ?(&endpoint), "environment_id" = ?(environment_id), "work_id" = ?(work_id), "{}", "Sending work ACK request");
        let response = self
            .http_client
            .retryable_http_do(cancellation, &request, None)
            .await
            .map_err(|error| ApiError::message(format!("ACK request failed: {error}")))?;
        let status = response.status();
        if status != StatusCode::OK && status != StatusCode::NO_CONTENT {
            let body = read_response_body_ignoring_errors(response, cancellation).await;
            Err(ApiError::message(format!(
                "ACK request returned status {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&body)
            )))
        } else {
            tracing::debug!("environment_id" = ?(environment_id), "work_id" = ?(work_id), "{}", "Work ACK successful");
            Ok(())
        }
    }
}
