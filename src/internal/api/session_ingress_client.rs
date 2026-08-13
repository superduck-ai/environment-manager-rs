//! SessionIngress HTTP 适配器。
//!
//! v1/v2 路径选择、只接受 200 的成功判定和诊断日志扁平 wire 格式均保持。

use std::collections::BTreeMap;

use chrono::SecondsFormat;
use reqwest::header::{CONTENT_TYPE, HeaderValue};
use reqwest::{Method, StatusCode};
use serde::Serialize;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::internal::util::escape_path_segment;

use super::client::{add_version_headers, read_response_body};
use super::{ApiError, DiagLogEntry, HttpClient, SessionEvent};

/// SessionIngress events/diag_logs 客户端。
#[derive(Clone)]
pub struct HttpSessionIngressClient {
    http_client: HttpClient,
    auth_token: String,
    use_v1: bool,
}

impl HttpSessionIngressClient {
    /// 构造 SessionIngress 客户端；`use_v1` 只影响路径版本段。
    ///
    /// 事件上传与网络重试均直接写入进程 tracing subscriber。
    pub fn new(http_client: HttpClient, auth_token: impl Into<String>, use_v1: bool) -> Self {
        Self {
            http_client,
            auth_token: auth_token.into(),
            use_v1,
        }
    }

    /// 拼接并转义 session ID。
    pub fn session_endpoint(&self, session_id: &str, suffix: &str) -> String {
        let version = if self.use_v1 { "v1" } else { "v2" };
        format!(
            "{}/{version}/session_ingress/session/{}/{}",
            self.http_client.base_url(),
            escape_path_segment(session_id),
            suffix
        )
    }

    /// 发送 SessionIngress 专用请求并解释 501 sentinel。
    async fn post_json<T: Serialize + ?Sized>(
        &self,
        cancellation: &CancellationToken,
        endpoint: &str,
        request_body: &T,
        operation_name: &str,
    ) -> Result<(), ApiError> {
        let body = serde_json::to_vec(request_body).map_err(|error| {
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
        if status == StatusCode::OK {
            Ok(())
        } else {
            let body = read_response_body(response, cancellation).await?;
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
        }
    }

    /// 用 `events` 数组包裹单个事件。
    pub async fn post_session_event(
        &self,
        cancellation: &CancellationToken,
        session_id: &str,
        event: &SessionEvent,
    ) -> Result<(), ApiError> {
        let endpoint = self.session_endpoint(session_id, "events");
        tracing::info!("endpoint" = ?(&endpoint), "session_id" = ?(session_id), "event_type" = ?(&event.event_type), "event_uuid" = ?(&event.uuid), "{}", "Posting session event");
        self.post_json(
            cancellation,
            &endpoint,
            &serde_json::json!({ "events": [event] }),
            "session event",
        )
        .await?;
        tracing::debug!("session_id" = ?(session_id), "event_type" = ?(&event.event_type), "event_uuid" = ?(&event.uuid), "{}", "Successfully posted session event");
        Ok(())
    }

    /// 空日志批次不发送请求。
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
        tracing::debug!("endpoint" = ?(&endpoint), "session_id" = ?(session_id), "count" = ?(logs.len()), "{}", "Posting diag logs");
        self.post_json(
            cancellation,
            &endpoint,
            &serde_json::json!({ "lines": diag_logs_to_wire_format(logs) }),
            "diag logs",
        )
        .await?;
        tracing::debug!("session_id" = ?(session_id), "count" = ?(logs.len()), "{}", "Successfully posted diag logs");
        Ok(())
    }
}

/// 把 timestamp 与 fields 合并到同一 JSON object。
pub(crate) fn diag_logs_to_wire_format(logs: &[DiagLogEntry]) -> Vec<BTreeMap<String, Value>> {
    logs.iter()
        .map(|entry| {
            let mut line = BTreeMap::new();
            line.insert(
                "timestamp".to_owned(),
                Value::String(entry.timestamp.to_rfc3339_opts(SecondsFormat::Millis, true)),
            );
            // 循环在 timestamp 之后复制 fields，因此同名字段必须覆盖生成值。
            line.extend(entry.fields.clone());
            line
        })
        .collect()
}
