//! 对应 Go `internal/api/get_session_context.go` 的 session context 查询。
//!
//! 状态码分支和错误文本保持独立，调用方可以区分鉴权失败、session 不存在和普通后端
//! 错误，而不需要依赖 Reqwest 内部错误类型。

use reqwest::header::HeaderValue;
use reqwest::{Method, StatusCode};
use serde_json::json;
use tokio_util::sync::CancellationToken;

use crate::internal::logger::Logger;
use crate::internal::util::go_path_escape;

use super::client::{go_response_close_error, read_go_response_body};
use super::{ApiError, HttpClient, SessionContext, SessionContextResponse};

/// `/v2/sessions` 查询客户端。
#[derive(Clone)]
pub struct SessionsClient {
    http_client: HttpClient,
    auth_token: String,
    logger: Option<Logger>,
}

impl SessionsClient {
    /// 对应 Go 内联的 `NewSessionsClient` helper，保存 HTTP client、token 与 logger。
    ///
    /// token 只用于 Authorization header；logger 单独保存是为了让 session 级事件
    /// 对应 Go `SessionsClient.logger`，而重试事件继续使用 `HttpClient.logger`。
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

    /// 对应 Go `GetSessionContext`，查询并解析一个 session 的启动上下文。
    pub async fn get_session_context(
        &self,
        cancellation: &CancellationToken,
        session_id: &str,
    ) -> Result<SessionContext, ApiError> {
        let endpoint = format!(
            "{}/v2/sessions/{}",
            self.http_client.base_url(),
            go_path_escape(session_id)
        );
        let mut request = self
            .http_client
            .http_client
            .request(Method::GET, &endpoint)
            .build()
            .map_err(|error| {
                ApiError::message(format!("failed to create session context request: {error}"))
            })?;
        self.http_client
            .set_auth_header(&mut request, &self.auth_token)?;
        request
            .headers_mut()
            .insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        request.headers_mut().insert(
            "anthropic-beta",
            HeaderValue::from_static("ccr-byoc-2025-07-29"),
        );

        if let Some(logger) = &self.logger {
            logger.debug(
                "Fetching session context",
                [
                    ("endpoint".to_owned(), json!(&endpoint)),
                    ("session_id".to_owned(), json!(session_id)),
                ]
                .into_iter()
                .collect(),
            );
        }
        let response = self
            .http_client
            .retryable_http_do(cancellation, &request, None)
            .await
            .map_err(|error| {
                ApiError::message(format!("failed to fetch session context: {error}"))
            })?;
        let status = response.status();
        // Go 在函数退出时通过 defer 关闭 body。先保存自定义 transport 注入的 Close
        // 结果，再计算完整业务返回值，最后记录 warning，才能保持 defer 的时序。
        let close_error = go_response_close_error(&response);
        let body = read_go_response_body(response, cancellation)
            .await
            .map_err(|error| {
                ApiError::message(format!("failed to read session context response: {error}"))
            });

        let result = match body {
            Ok(body) => self.parse_session_context_response(status, &body, session_id),
            Err(error) => Err(error),
        };
        if let (Some(logger), Some(error)) = (&self.logger, close_error) {
            logger.warn(
                "Failed to close response body",
                [
                    ("error".to_owned(), json!(error)),
                    ("endpoint".to_owned(), json!(endpoint)),
                ]
                .into_iter()
                .collect(),
            );
        }
        result
    }

    /// 解释 `GetSessionContext` 已读取的响应 body，并保持 Go 状态码错误文本。
    ///
    /// # 参数
    /// - `status`：后端返回的 HTTP 状态码。
    /// - `body`：已经按 Go transport 规则解压并完整读取的响应字节。
    /// - `session_id`：未经 URL 转义的原始 session 标识，用于日志和错误文本。
    ///
    /// # 返回
    /// 200 时返回 session context；JSON 无效或状态码非 200 时返回兼容错误。
    fn parse_session_context_response(
        &self,
        status: StatusCode,
        body: &[u8],
        session_id: &str,
    ) -> Result<SessionContext, ApiError> {
        match status {
            StatusCode::OK => {
                let response: Option<SessionContextResponse> = serde_json::from_slice(body)
                    .map_err(|error| {
                        ApiError::message(format!(
                            "failed to parse session context response: {error}"
                        ))
                    })?;
                // Go 把顶层 JSON null 解进预先分配的 response struct 后保留零值；
                // serde 对 struct 的默认行为会报错，因此这里显式恢复该兼容语义。
                let response = response.unwrap_or_default();
                if let Some(logger) = &self.logger {
                    logger.debug(
                        "Successfully fetched session context",
                        [
                            ("session_id".to_owned(), json!(session_id)),
                            ("cwd".to_owned(), json!(&response.session_context.cwd)),
                            (
                                "has_outcomes".to_owned(),
                                json!(response.session_context.outcomes.is_some()),
                            ),
                            (
                                "has_mcp_config".to_owned(),
                                json!(response.session_context.mcp_config.is_some()),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    );
                }
                Ok(response.session_context)
            }
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(ApiError::message(format!(
                "authentication failed for session {} (status {}): {}",
                session_id,
                status.as_u16(),
                String::from_utf8_lossy(body)
            ))),
            StatusCode::NOT_FOUND => Err(ApiError::message(format!(
                "session {} not found: {}",
                session_id,
                String::from_utf8_lossy(body)
            ))),
            _ => Err(ApiError::message(format!(
                "session context request failed with status {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(body)
            ))),
        }
    }
}
