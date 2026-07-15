//! 对应 Go `internal/api/work_client.go` 的 work ACK 客户端。
//!
//! ACK 只接受 HTTP 200 或 204；认证、beta 和版本 header 的名称与 Go 完全一致。

use reqwest::header::HeaderValue;
use reqwest::{Method, StatusCode};
use serde_json::json;
use tokio_util::sync::CancellationToken;

use crate::internal::logger::Logger;
use crate::internal::util::go_path_escape;

use super::client::{
    add_version_headers, go_response_close_error, read_go_response_body_ignoring_errors,
};
use super::{ApiError, HttpClient};

/// 环境 work API 客户端。
#[derive(Clone)]
pub struct WorkClient {
    http_client: HttpClient,
    auth_token: String,
    logger: Option<Logger>,
}

impl WorkClient {
    /// 对应 Go `NewWorkClient`，保存共享 HTTP client、service key 和结构化 logger。
    pub fn new(
        http_client: HttpClient,
        auth_token: impl Into<String>,
        logger: Option<Logger>,
    ) -> Self {
        Self {
            http_client,
            auth_token: auth_token.into(),
            logger,
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

        if let Some(logger) = &self.logger {
            logger.debug(
                "Sending work ACK request",
                [
                    ("endpoint".to_owned(), json!(&endpoint)),
                    ("environment_id".to_owned(), json!(environment_id)),
                    ("work_id".to_owned(), json!(work_id)),
                ]
                .into_iter()
                .collect(),
            );
        }
        let response = self
            .http_client
            .retryable_http_do(cancellation, &request, None)
            .await
            .map_err(|error| ApiError::message(format!("ACK request failed: {error}")))?;
        let status = response.status();
        let close_error = go_response_close_error(&response);
        let result = if status != StatusCode::OK && status != StatusCode::NO_CONTENT {
            // Go 忽略 ACK body 的 ReadAll 错误，但错误文本仍包含已经到达的 partial
            // bytes；共享 reader 还负责仅对 Transport 自动协商的 gzip 解压。
            let body = read_go_response_body_ignoring_errors(response, cancellation).await;
            Err(ApiError::message(format!(
                "ACK request returned status {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&body)
            )))
        } else {
            if let Some(logger) = &self.logger {
                logger.debug(
                    "Work ACK successful",
                    [
                        ("environment_id".to_owned(), json!(environment_id)),
                        ("work_id".to_owned(), json!(work_id)),
                    ]
                    .into_iter()
                    .collect(),
                );
            }
            Ok(())
        };
        // Go 的 defer 在成功日志或错误值完成之后执行；Close 失败只写日志，不覆盖
        // ACK 的业务返回值。标准 Reqwest 响应没有该扩展，因此不会生成伪警报。
        if let (Some(logger), Some(error)) = (&self.logger, close_error) {
            logger.error(
                "Failed to close ACK response body",
                [("error".to_owned(), json!(error))].into_iter().collect(),
            );
        }
        result
    }
}
