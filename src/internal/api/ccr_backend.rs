//! 对应 Go `internal/api/ccr_backend.go` 的 Code Session worker 后端。
//!
//! worker epoch 在 register 成功后由后续事件、诊断和 OTLP 请求共享；使用原子整数避免
//! manager 并发读取时引入锁，同时保持 Go 单值字段语义。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;
use chrono::SecondsFormat;
use serde::{Deserialize, Deserializer, de};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::internal::logger::Logger;
use crate::internal::util::go_path_escape;

use super::client::marshal_go_dynamic_json;
use super::{ApiError, DiagLogEntry, HttpClient, OtlpEndpoints, SessionBackend, SessionEvent};

/// CCR v2 worker backend。
pub struct CCRBackend {
    http_client: HttpClient,
    auth_token: String,
    session_id: String,
    logger: Option<Logger>,
    worker_epoch: AtomicI64,
}

/// CCR register 响应的 Go `,string` 兼容 wire 结构。
///
/// Go 对缺失、JSON `null` 以及字符串 `"null"` 都保留 `int64` 零值；单纯使用
/// Rust `String` 会把这些可接受输入误判成协议错误，因此在边界处直接恢复整数。
#[derive(Default, Deserialize)]
#[serde(default)]
struct RegisterResponse {
    #[serde(deserialize_with = "deserialize_string_encoded_i64")]
    worker_epoch: i64,
}

/// 反序列化 Go `json:",string"` 标记的 `int64` 字段。
///
/// 只接受字符串承载的十进制整数；缺失值由结构体默认值处理，显式 `null` 与
/// `"null"` 按 Go primitive 零值规则返回 0。前导 `+` 必须拒绝，因为 Go 的
/// `encoding/json` 不把它视为合法的 quoted number。
fn deserialize_string_encoded_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let encoded = Option::<String>::deserialize(deserializer)?;
    let Some(encoded) = encoded else {
        return Ok(0);
    };
    if encoded == "null" {
        return Ok(0);
    }
    if encoded.starts_with('+') {
        return Err(de::Error::custom(
            "invalid use of ,string struct tag for signed integer",
        ));
    }
    encoded.parse::<i64>().map_err(de::Error::custom)
}

impl CCRBackend {
    /// 对应 Go `NewCCRBackend`，初始 worker epoch 为零，必须在发送事件前 register。
    ///
    /// logger 保存为 backend 自身字段，保证 register 成功事件使用 Go CCR 调用点，
    /// 而 HTTP attempt 日志仍由共享 `HttpClient` 发出。
    pub fn new(
        http_client: HttpClient,
        auth_token: impl Into<String>,
        session_id: impl Into<String>,
        logger: Option<Logger>,
    ) -> Self {
        Self {
            http_client,
            auth_token: auth_token.into(),
            session_id: session_id.into(),
            logger,
            worker_epoch: AtomicI64::new(0),
        }
    }

    /// 对应 Go `RegisterWorker`，注册 code-session worker 并保存字符串编码的 epoch。
    pub async fn register_worker(&self, cancellation: &CancellationToken) -> Result<i64, ApiError> {
        let endpoint = self.endpoint("register");
        let request_body = BTreeMap::from([(
            "session_id".to_owned(),
            Value::String(self.session_id.clone()),
        )]);
        let response: RegisterResponse = self
            .http_client
            .post_json_with_response(
                cancellation,
                &endpoint,
                &request_body,
                &self.auth_token,
                "register worker",
            )
            .await
            .map_err(|error| ApiError::message(format!("failed to register worker: {error}")))?;
        let epoch = response.worker_epoch;
        self.worker_epoch.store(epoch, Ordering::Release);
        if let Some(logger) = &self.logger {
            logger.info(
                "worker_registered",
                [
                    (
                        "session_id".to_owned(),
                        Value::String(self.session_id.clone()),
                    ),
                    ("worker_epoch".to_owned(), Value::from(epoch)),
                ]
                .into_iter()
                .collect(),
            );
        }
        Ok(epoch)
    }

    /// 对应 Go `WorkerEpoch`，返回最近一次成功注册得到的 epoch。
    pub fn worker_epoch(&self) -> i64 {
        self.worker_epoch.load(Ordering::Acquire)
    }

    /// 对应 Go `endpoint`，拼接 CCR worker 子资源 URL。
    fn endpoint(&self, suffix: &str) -> String {
        format!(
            "{}/v1/code/sessions/{}/worker/{suffix}",
            self.http_client.base_url(),
            go_path_escape(&self.session_id)
        )
    }
}

#[async_trait]
impl SessionBackend for CCRBackend {
    /// 对应 Go `PostEvent`，把原事件 JSON 包装在 CCR `payload` 字段中。
    async fn post_event(
        &self,
        cancellation: &CancellationToken,
        event: &SessionEvent,
    ) -> Result<(), ApiError> {
        let payload = serde_json::to_value(event).map_err(|error| {
            ApiError::message(format!("failed to marshal event for CCR: {error}"))
        })?;
        let payload = marshal_go_dynamic_json(&payload).map_err(|error| {
            ApiError::message(format!("failed to marshal event for CCR: {error}"))
        })?;

        // Go 先把 SessionEvent marshal 后 unmarshal 到 map[string]any，因此 payload
        // 的键递归排序、数字按 float64 编码；worker_epoch 则是外层 map 中直接放入
        // 的 int64，不能一并转成 float64。手工拼接固定的外层 map 顺序能同时保留
        // 这两种数值语义，而不会依赖 serde_json preserve_order。
        let mut request_body = b"{\"events\":[{\"payload\":".to_vec();
        request_body.extend_from_slice(&payload);
        request_body.extend_from_slice(b"}],\"worker_epoch\":");
        request_body.extend_from_slice(self.worker_epoch().to_string().as_bytes());
        request_body.push(b'}');
        self.http_client
            .do_post_json_bytes(
                cancellation,
                &self.endpoint("events"),
                request_body,
                &self.auth_token,
                "CCR event",
            )
            .await
            .map(|_| ())
    }

    /// 对应 Go `FlushLogs`，CCR wire 格式把自定义 fields 嵌套在 `fields` 下。
    async fn flush_logs(
        &self,
        cancellation: &CancellationToken,
        logs: &[DiagLogEntry],
    ) -> Result<(), ApiError> {
        if logs.is_empty() {
            return Ok(());
        }
        let lines: Vec<BTreeMap<String, Value>> = logs
            .iter()
            .map(|entry| {
                let mut line = BTreeMap::new();
                line.insert(
                    "timestamp".to_owned(),
                    Value::String(entry.timestamp.to_rfc3339_opts(SecondsFormat::Millis, true)),
                );
                if !entry.fields.is_empty() {
                    line.insert(
                        "fields".to_owned(),
                        serde_json::to_value(&entry.fields)
                            .expect("diagnostic log fields serialization is infallible"),
                    );
                }
                line
            })
            .collect();
        // Go 的 requestBody 是 map[string]any，encoding/json 按键排序为
        // lines→session_id→worker_epoch；BTreeMap 明确表达这一 wire 约束。
        let request_body = BTreeMap::from([
            (
                "lines".to_owned(),
                serde_json::to_value(lines)
                    .expect("CCR diagnostic lines serialization is infallible"),
            ),
            (
                "session_id".to_owned(),
                Value::String(self.session_id.clone()),
            ),
            (
                "worker_epoch".to_owned(),
                Value::Number(self.worker_epoch().into()),
            ),
        ]);
        self.http_client
            .do_post_json(
                cancellation,
                &self.endpoint("diagnostics"),
                &request_body,
                &self.auth_token,
                "CCR diagnostics",
            )
            .await
            .map(|_| ())
            // 与 Go `%w` 一致保留 EndpointNotImplemented 的可判定类型，不能只保留
            // Display 文本，否则诊断 uploader 会把 501 当成普通重试错误。
            .map_err(|error| error.with_context("failed to flush diagnostics to CCR"))
    }

    /// 对应 Go `OtlpEndpoints`，在 query 中附加当前 worker epoch。
    fn otlp_endpoints(&self) -> OtlpEndpoints {
        OtlpEndpoints {
            metrics_endpoint: format!(
                "{}?worker_epoch={}",
                self.endpoint("otlp/metrics"),
                self.worker_epoch()
            ),
            logs_endpoint: format!(
                "{}?worker_epoch={}",
                self.endpoint("otlp/logs"),
                self.worker_epoch()
            ),
            headers: BTreeMap::from([(
                "Authorization".to_owned(),
                format!("Bearer {}", self.auth_token),
            )]),
        }
    }
}
