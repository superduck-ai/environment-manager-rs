//! 控制 action 注册表和 NDJSON 调度实现。
//!
//! 对应 Go `internal/tunnel/actions/registry.go`。Rust 使用 async trait、
//! `CancellationToken` 和 Tokio timeout 替代 goroutine/context，同时保持响应顺序号、
//! 同名互斥、panic 隔离以及 Go `encoding/json` 的 Base64 字节行为。

use std::any::Any;
use std::backtrace::Backtrace;
use std::collections::{BTreeMap, BTreeSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use futures_util::FutureExt as _;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::r#gen::proto::anthropic::sessions::tunnel::v1alpha::{
    Header, HttpTunnelResponseChunk, HttpTunnelResponseError, HttpTunnelResponseHeaders,
};
use crate::internal::logger::{LogAttributes, Logger, default_slog_logger};

use super::{
    Action, ActionError, ActionRequest, ActionResult, ProgressReporter, ProgressUpdate, Registry,
    ResponseSender,
};

/// 对应 Go `DefaultActionTimeout`：action 未声明超时时使用五分钟。
pub const DEFAULT_ACTION_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// 对应 Go：`actions.NewRegistry`。
///
/// 创建空 action 表和运行集合。logger 为 `None` 时回退到进程默认 logger，严格保持
/// Go `(*Registry).log` 返回 `slog.Default()` 的可观察行为。
#[must_use]
pub fn new_registry(logger: Option<Logger>) -> Registry {
    Registry {
        actions: RwLock::new(BTreeMap::new()),
        running: Mutex::new(BTreeSet::new()),
        logger,
    }
}

impl Registry {
    /// 对应 Go：`(*Registry).Register`。
    ///
    /// 以 action 的稳定名称覆盖已有注册项。写锁不会跨异步边界持有；正在执行的旧
    /// action 已经持有自己的 `Arc`，因此覆盖不会中断它。
    pub fn register(&self, action: Arc<dyn Action>) {
        let name = action.name().to_owned();
        self.actions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(name.clone(), action);
        let logger = self.log();
        logger.info(
            "control_action_registered",
            log_attributes([("action", Value::String(name))]),
        );
    }

    /// 对应 Go：`(*Registry).Execute`。
    ///
    /// 从 `/__control/<name>` 提取 action，拒绝同名并发，发送 NDJSON header 后执行
    /// action，最终总会释放运行占位。调用方取消、action 超时和 panic 都会转换成
    /// 最终错误分片；sender 在发送 header 前失败时与 Go 一样直接停止。
    pub async fn execute(
        &self,
        cancellation: CancellationToken,
        request_id: &str,
        path: &str,
        body: Vec<u8>,
        session_id: &str,
        sender: Arc<dyn ResponseSender>,
    ) {
        let action_name = action_name_from_path(path);
        let action = self
            .actions
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&action_name)
            .cloned();
        let sequenced = Arc::new(SequencedSender::new(sender));

        let Some(action) = action else {
            let mut payload = BTreeMap::new();
            payload.insert(
                "error".to_owned(),
                Value::String(format!("unknown action: {action_name}")),
            );
            self.send_json_response(&sequenced, request_id, 404, payload)
                .await;
            return;
        };

        let already_running = {
            let mut running = self
                .running
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            !running.insert(action_name.clone())
        };
        if already_running {
            let mut payload = BTreeMap::new();
            payload.insert(
                "error".to_owned(),
                Value::String(format!("{action_name} is already in progress")),
            );
            self.send_json_response(&sequenced, request_id, 409, payload)
                .await;
            return;
        }

        self.execute_registered_action(
            cancellation,
            request_id,
            &action_name,
            body,
            session_id,
            &sequenced,
            action,
        )
        .await;

        // Go 通过 defer 删除 running 标记；这里把清理放在所有错误都已被内部吸收的
        // 单一出口，确保 sender 错误、超时和 panic 都不会永久锁死 action。
        self.running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&action_name);
    }

    /// 发送初始 header、运行一个已解析 action，并生成最终 NDJSON 分片。
    #[allow(clippy::too_many_arguments)]
    async fn execute_registered_action(
        &self,
        cancellation: CancellationToken,
        request_id: &str,
        action_name: &str,
        body: Vec<u8>,
        session_id: &str,
        sender: &Arc<SequencedSender>,
        action: Arc<dyn Action>,
    ) {
        let logger = self.log();
        logger.info(
            "control_action_start",
            log_attributes([
                ("action", Value::String(action_name.to_owned())),
                ("session_id", Value::String(session_id.to_owned())),
                ("request_id", Value::String(request_id.to_owned())),
            ]),
        );

        let headers = HttpTunnelResponseHeaders {
            request_id: request_id.to_owned(),
            status_code: 200,
            status_text: "200 OK".to_owned(),
            headers: vec![
                Header {
                    name: "Content-Type".to_owned(),
                    value: "application/x-ndjson".to_owned(),
                },
                Header {
                    name: "X-Action".to_owned(),
                    value: action_name.to_owned(),
                },
            ],
            sequence_number: 0,
        };
        if let Err(error) = sender.send_headers(headers).await {
            let logger = self.log();
            logger.error(
                "control_action_send_headers_failed",
                log_attributes([
                    ("action", Value::String(action_name.to_owned())),
                    ("error", Value::String(error.to_string())),
                ]),
            );
            return;
        }

        let timeout = if action.timeout().is_zero() {
            DEFAULT_ACTION_TIMEOUT
        } else {
            action.timeout()
        };
        let action_cancellation = cancellation.child_token();
        let request = ActionRequest {
            body,
            headers: BTreeMap::new(),
            session_id: session_id.to_owned(),
        };
        let reporter: Arc<dyn ProgressReporter> = Arc::new(TunnelProgressReporter {
            sender: Arc::clone(sender),
            request_id: request_id.to_owned(),
        });

        // `catch_unwind` 覆盖 action future 的每次 poll，而不只覆盖 future 的创建；
        // 这才等价于 Go defer/recover 对整个 Execute 调用栈的保护。
        let future =
            AssertUnwindSafe(action.execute(action_cancellation.clone(), request, reporter))
                .catch_unwind();
        let outcome = tokio::select! {
            () = cancellation.cancelled() => {
                action_cancellation.cancel();
                ActionOutcome::Error("context canceled".to_owned())
            }
            timed = tokio::time::timeout(timeout, future) => {
                match timed {
                    Ok(Ok(Ok(result))) => ActionOutcome::Success(result),
                    Ok(Ok(Err(error))) => ActionOutcome::Error(error.to_string()),
                    Ok(Err(panic)) => ActionOutcome::Panic {
                        value: panic_value(panic.as_ref()),
                        stack: Backtrace::force_capture().to_string(),
                    },
                    Err(_elapsed) => {
                        action_cancellation.cancel();
                        ActionOutcome::Error("context deadline exceeded".to_owned())
                    }
                }
            }
        };

        // 三个终态在这里机械展开：日志必须直接从 Go 对应控制分支调用 facade，才能让
        // `#[track_caller]` 保留各自的 source，而不是统一落到一个动态 helper。
        let (payload, is_error, panicked) = match outcome {
            ActionOutcome::Success(result) => {
                let mut payload = BTreeMap::new();
                payload.insert(
                    "data".to_owned(),
                    Value::String(base64::engine::general_purpose::STANDARD.encode(result.body)),
                );
                payload.insert("status".to_owned(), Value::from(result.status_code));
                payload.insert("type".to_owned(), Value::String("result".to_owned()));
                (payload, false, false)
            }
            ActionOutcome::Error(error) => {
                let logger = self.log();
                logger.error(
                    "control_action_failed",
                    log_attributes([
                        ("action", Value::String(action_name.to_owned())),
                        ("error", Value::String(error.clone())),
                        ("session_id", Value::String(session_id.to_owned())),
                    ]),
                );
                let mut payload = BTreeMap::new();
                payload.insert("error".to_owned(), Value::String(error));
                payload.insert("type".to_owned(), Value::String("error".to_owned()));
                (payload, true, false)
            }
            ActionOutcome::Panic { value, stack } => {
                let logger = self.log();
                logger.error(
                    "control_action_panicked",
                    log_attributes([
                        ("action", Value::String(action_name.to_owned())),
                        ("panic", Value::String(value)),
                        ("stack", Value::String(stack)),
                        ("session_id", Value::String(session_id.to_owned())),
                        ("request_id", Value::String(request_id.to_owned())),
                    ]),
                );
                let mut payload = BTreeMap::new();
                payload.insert(
                    "error".to_owned(),
                    Value::String("internal error: action panicked".to_owned()),
                );
                payload.insert("type".to_owned(), Value::String("error".to_owned()));
                (payload, true, true)
            }
        };
        let mut encoded = serde_json::to_vec(&payload).unwrap_or_else(|error| {
            format!(r#"{{"error":"failed to marshal action result: {error}","type":"error"}}"#)
                .into_bytes()
        });
        encoded.push(b'\n');
        let _ = sender
            .send_chunk(HttpTunnelResponseChunk {
                request_id: request_id.to_owned(),
                data: encoded,
                is_final: true,
                sequence_number: 0,
            })
            .await;

        // Go 的 recover defer 在 panic 后直接返回，不会执行函数尾部 complete 事件。
        if panicked {
            return;
        }
        let logger = self.log();
        logger.info(
            "control_action_complete",
            log_attributes([
                ("action", Value::String(action_name.to_owned())),
                ("is_error", Value::Bool(is_error)),
                ("session_id", Value::String(session_id.to_owned())),
            ]),
        );
    }

    /// 对应 Go：`(*Registry).sendJSONResponse`。
    ///
    /// 未知或并发冲突在执行 action 前返回普通 JSON；header/chunk 仍共享顺序号。
    async fn send_json_response(
        &self,
        sender: &Arc<SequencedSender>,
        request_id: &str,
        status_code: i32,
        payload: BTreeMap<String, Value>,
    ) {
        let _ = sender
            .send_headers(HttpTunnelResponseHeaders {
                request_id: request_id.to_owned(),
                status_code,
                status_text: status_code.to_string(),
                headers: vec![Header {
                    name: "Content-Type".to_owned(),
                    value: "application/json".to_owned(),
                }],
                sequence_number: 0,
            })
            .await;
        let mut body = serde_json::to_vec(&payload).unwrap_or_default();
        body.push(b'\n');
        let _ = sender
            .send_chunk(HttpTunnelResponseChunk {
                request_id: request_id.to_owned(),
                data: body,
                is_final: true,
                sequence_number: 0,
            })
            .await;
    }

    /// 返回 registry 配置的 logger；无 logger 时严格回退到 Go 的 `slog.Default()`。
    fn log(&self) -> &Logger {
        match self.logger.as_ref() {
            Some(logger) => logger,
            None => default_slog_logger(),
        }
    }
}

/// action future 的三种终态；panic 与普通错误需要不同兼容文本。
enum ActionOutcome {
    /// 正常返回业务结果。
    Success(ActionResult),
    /// action 错误、取消或超时。
    Error(String),
    /// action 内发生 panic；保存 Go panic 日志所需的值文本与栈文本。
    Panic {
        /// panic payload 的兼容文本。
        value: String,
        /// 捕获到 panic 后立即取得的 Rust backtrace。
        stack: String,
    },
}

/// 把 Rust panic payload 转成 Go `fmt.Sprintf("%v", rec)` 对字符串 panic 的文本。
///
/// Rust 的 `Any` 对任意用户类型不提供格式化能力；生产 action 只使用字符串 panic，
/// 其他类型使用稳定占位文本，仍确保日志和错误响应不会再次 panic。
fn panic_value(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

/// 从控制 URL 中提取第一个 path segment，保持 Go 的 TrimPrefix/Cut 语义。
fn action_name_from_path(path: &str) -> String {
    path.strip_prefix("/__control/")
        .unwrap_or(path)
        .split_once('/')
        .map_or_else(
            || path.strip_prefix("/__control/").unwrap_or(path),
            |(name, _)| name,
        )
        .trim_end_matches('/')
        .to_owned()
}

/// 把定长 key/value 数组转成 logger 使用的确定顺序属性。
fn log_attributes<const N: usize>(values: [(&str, Value); N]) -> LogAttributes {
    values
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

/// 为 header/chunk 分配单调递增序号的 sender 包装器。
pub(super) struct SequencedSender {
    inner: Arc<dyn ResponseSender>,
    sequence_number: AtomicU32,
}

impl SequencedSender {
    /// 创建从零开始分配序号的包装器。
    pub(super) fn new(inner: Arc<dyn ResponseSender>) -> Self {
        Self {
            inner,
            sequence_number: AtomicU32::new(0),
        }
    }
}

#[async_trait]
impl ResponseSender for SequencedSender {
    /// 对应 Go：`(*sequencedSender).SendHeaders`，写入下一个序号并包装错误。
    async fn send_headers(
        &self,
        mut response: HttpTunnelResponseHeaders,
    ) -> Result<(), ActionError> {
        set_sequence_number(&self.sequence_number, &mut response.sequence_number);
        self.inner
            .send_headers(response)
            .await
            .map_err(|error| format!("sequenced send headers: {error}").into())
    }

    /// 对应 Go：`(*sequencedSender).SendChunk`，写入下一个序号并包装错误。
    async fn send_chunk(&self, mut chunk: HttpTunnelResponseChunk) -> Result<(), ActionError> {
        set_sequence_number(&self.sequence_number, &mut chunk.sequence_number);
        self.inner
            .send_chunk(chunk)
            .await
            .map_err(|error| format!("sequenced send chunk: {error}").into())
    }

    /// 对应 Go：`(*sequencedSender).SendError`；协议错误没有 sequence 字段。
    async fn send_error(&self, response: HttpTunnelResponseError) -> Result<(), ActionError> {
        self.inner
            .send_error(response)
            .await
            .map_err(|error| format!("sequenced send error: {error}").into())
    }
}

/// 为带 sequence_number 字段的强类型响应分配下一个序号。
///
/// 对应 Go `setSequenceNumber` 的反射 helper；Rust 在调用点传入确定的 protobuf
/// 字段引用，因此不接受 nil、非结构体或错误字段类型，编译期即可排除这些分支。
fn set_sequence_number(counter: &AtomicU32, destination: &mut u32) {
    *destination = counter.fetch_add(1, Ordering::SeqCst);
}

/// 把 action 进度编码为非最终 NDJSON chunk。
struct TunnelProgressReporter {
    sender: Arc<SequencedSender>,
    request_id: String,
}

#[async_trait]
impl ProgressReporter for TunnelProgressReporter {
    /// 对应 Go：`(*tunnelProgressReporter).SendProgress`。
    async fn send_progress(&self, update: ProgressUpdate) -> Result<(), ActionError> {
        let mut payload = BTreeMap::new();
        payload.insert("message", Value::String(update.message));
        payload.insert("percent", Value::from(update.percent));
        payload.insert("step", Value::String(update.step));
        payload.insert("type", Value::String("progress".to_owned()));
        let mut data = serde_json::to_vec(&payload)
            .map_err(|error| format!("failed to marshal progress: {error}"))?;
        data.push(b'\n');
        self.sender
            .send_chunk(HttpTunnelResponseChunk {
                request_id: self.request_id.clone(),
                data,
                is_final: false,
                sequence_number: 0,
            })
            .await
            .map_err(|error| format!("failed to send progress chunk: {error}").into())
    }
}
