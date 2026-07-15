//! 对应 Go `internal/api/session_ingress_client.go` 的 SessionIngress HTTP 适配器。
//!
//! v1/v2 路径选择、只接受 200 的成功判定和诊断日志扁平 wire 格式均保持 Go 行为。

use std::collections::BTreeMap;

use chrono::SecondsFormat;
use reqwest::header::{CONTENT_TYPE, HeaderValue};
use reqwest::{Method, StatusCode};
use serde::Serialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::internal::logger::Logger;
use crate::internal::util::go_path_escape;

use super::client::{
    add_version_headers, go_response_close_error, marshal_go_json,
    read_go_response_body_ignoring_errors,
};
use super::{ApiError, DiagLogEntry, HttpClient, SessionEvent};

/// SessionIngress events/diag_logs 客户端。
#[derive(Clone)]
pub struct HttpSessionIngressClient {
    http_client: HttpClient,
    auth_token: String,
    logger: Option<Logger>,
    use_v1: bool,
}

impl HttpSessionIngressClient {
    /// 构造 SessionIngress 客户端；`use_v1` 只影响路径版本段。
    ///
    /// logger 与嵌入的 HTTP client 分别保存，与 Go 结构中的两个字段一致：前者记录
    /// 事件/诊断上传阶段，后者记录每个网络 attempt 与 retry backoff。
    pub fn new(
        http_client: HttpClient,
        auth_token: impl Into<String>,
        logger: Option<Logger>,
        use_v1: bool,
    ) -> Self {
        Self {
            http_client,
            auth_token: auth_token.into(),
            logger,
            use_v1,
        }
    }

    /// 对应 Go `sessionEndpoint`，拼接并转义 session ID。
    pub fn session_endpoint(&self, session_id: &str, suffix: &str) -> String {
        let version = if self.use_v1 { "v1" } else { "v2" };
        format!(
            "{}/{version}/session_ingress/session/{}/{}",
            self.http_client.base_url(),
            go_path_escape(session_id),
            suffix
        )
    }

    /// 对应 Go `postJSON`，发送 SessionIngress 专用请求并解释 501 sentinel。
    async fn post_json<T: Serialize + ?Sized>(
        &self,
        cancellation: &CancellationToken,
        endpoint: &str,
        request_body: &T,
        operation_name: &str,
    ) -> Result<(), ApiError> {
        let body = marshal_go_json(request_body).map_err(|error| {
            ApiError::message(format!(
                "failed to marshal {operation_name} request: {error}"
            ))
        })?;
        let mut request = self
            .http_client
            .http_client
            .request(Method::POST, endpoint)
            .body(body)
            .build()
            .map_err(|error| {
                ApiError::message(format!(
                    "failed to create {operation_name} request: {error}"
                ))
            })?;
        self.http_client
            .set_auth_header(&mut request, &self.auth_token)?;
        request
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        add_version_headers(request.headers_mut())?;

        let response = self
            .http_client
            .retryable_http_do(cancellation, &request, None)
            .await
            .map_err(|error| {
                ApiError::message(format!("failed to post {operation_name}: {error}"))
            })?;
        let status = response.status();
        let close_error = go_response_close_error(&response);
        let result = if status == StatusCode::OK {
            Ok(())
        } else {
            // Go 忽略 ReadAll 错误但保留截断前的字节；共享 helper 同时处理 Transport
            // 自动协商的 gzip，避免 4xx/501 文案在压缩或 partial body 时变空。
            let body = read_go_response_body_ignoring_errors(response, cancellation).await;
            let body = String::from_utf8_lossy(&body);
            if status == StatusCode::NOT_IMPLEMENTED {
                Err(ApiError::EndpointNotImplemented(format!(
                    "{operation_name} request responded with {} (not implemented): {body}: endpoint not implemented",
                    status.as_u16()
                )))
            } else {
                Err(ApiError::message(format!(
                    "{operation_name} request responded with non-OK {}: {body}",
                    status.as_u16()
                )))
            }
        };
        // 与 Go defer 一样，Close warning 晚于业务结果计算，且不会替换 501 sentinel。
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

    /// 对应 Go `PostSessionEvent`，用 `events` 数组包裹单个事件。
    pub async fn post_session_event(
        &self,
        cancellation: &CancellationToken,
        session_id: &str,
        event: &SessionEvent,
    ) -> Result<(), ApiError> {
        let endpoint = self.session_endpoint(session_id, "events");
        if let Some(logger) = &self.logger {
            logger.info(
                "Posting session event",
                [
                    ("endpoint".to_owned(), json!(&endpoint)),
                    ("session_id".to_owned(), json!(session_id)),
                    ("event_type".to_owned(), json!(&event.event_type)),
                    ("event_uuid".to_owned(), json!(&event.uuid)),
                ]
                .into_iter()
                .collect(),
            );
        }
        self.post_json(
            cancellation,
            &endpoint,
            &serde_json::json!({ "events": [event] }),
            "session event",
        )
        .await?;
        if let Some(logger) = &self.logger {
            logger.debug(
                "Successfully posted session event",
                [
                    ("session_id".to_owned(), json!(session_id)),
                    ("event_type".to_owned(), json!(&event.event_type)),
                    ("event_uuid".to_owned(), json!(&event.uuid)),
                ]
                .into_iter()
                .collect(),
            );
        }
        Ok(())
    }

    /// 对应 Go `PostForwardDiagLogs`，空日志批次不发送请求。
    pub async fn post_forward_diag_logs(
        &self,
        cancellation: &CancellationToken,
        session_id: &str,
        logs: &[DiagLogEntry],
    ) -> Result<(), ApiError> {
        if logs.is_empty() {
            return Ok(());
        }
        let endpoint = self.session_endpoint(session_id, "diag_logs");
        if let Some(logger) = &self.logger {
            logger.debug(
                "Posting diag logs",
                [
                    ("endpoint".to_owned(), json!(&endpoint)),
                    ("session_id".to_owned(), json!(session_id)),
                    ("count".to_owned(), json!(logs.len())),
                ]
                .into_iter()
                .collect(),
            );
        }
        self.post_json(
            cancellation,
            &endpoint,
            &serde_json::json!({ "lines": diag_logs_to_wire_format(logs) }),
            "diag logs",
        )
        .await?;
        if let Some(logger) = &self.logger {
            logger.debug(
                "Successfully posted diag logs",
                [
                    ("session_id".to_owned(), json!(session_id)),
                    ("count".to_owned(), json!(logs.len())),
                ]
                .into_iter()
                .collect(),
            );
        }
        Ok(())
    }
}

/// 对应 Go `diagLogsToWireFormat`，把 timestamp 与 fields 合并到同一 JSON object。
pub(crate) fn diag_logs_to_wire_format(logs: &[DiagLogEntry]) -> Vec<BTreeMap<String, Value>> {
    logs.iter()
        .map(|entry| {
            let mut line = BTreeMap::new();
            line.insert(
                "timestamp".to_owned(),
                Value::String(entry.timestamp.to_rfc3339_opts(SecondsFormat::Millis, true)),
            );
            // Go 循环在 timestamp 之后复制 fields，因此同名字段必须覆盖生成值。
            line.extend(entry.fields.clone());
            line
        })
        .collect()
}
