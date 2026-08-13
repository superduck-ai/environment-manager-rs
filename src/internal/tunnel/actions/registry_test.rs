//! Registry 的 NDJSON、互斥和 panic 隔离测试。
//!
//! mock sender 保存强类型 Proto，
//! 测试能同时验证内容和跨 header/chunk 的全局顺序号。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::r#gen::proto::anthropic::sessions::tunnel::v1alpha::{
    HttpTunnelResponseChunk, HttpTunnelResponseError, HttpTunnelResponseHeaders,
};

use super::registry::SequencedSender;
use super::*;

#[derive(Default)]
struct RecordingSender {
    headers: Mutex<Vec<HttpTunnelResponseHeaders>>,
    chunks: Mutex<Vec<HttpTunnelResponseChunk>>,
    errors: Mutex<Vec<HttpTunnelResponseError>>,
    chunk_error: Mutex<Option<String>>,
}

#[async_trait]
impl ResponseSender for RecordingSender {
    async fn send_headers(&self, response: HttpTunnelResponseHeaders) -> Result<(), ActionError> {
        self.headers.lock().unwrap().push(response);
        Ok(())
    }

    async fn send_chunk(&self, chunk: HttpTunnelResponseChunk) -> Result<(), ActionError> {
        if let Some(error) = self.chunk_error.lock().unwrap().clone() {
            return Err(error.into());
        }
        self.chunks.lock().unwrap().push(chunk);
        Ok(())
    }

    async fn send_error(&self, response: HttpTunnelResponseError) -> Result<(), ActionError> {
        self.errors.lock().unwrap().push(response);
        Ok(())
    }
}

enum FakeActionBehavior {
    Progress,
    Panic,
    Blocking {
        started: Arc<Notify>,
        release: Arc<Notify>,
    },
}

/// 以枚举保存各测试所需的确定性执行行为。
struct FakeAction {
    name: &'static str,
    timeout: Duration,
    behavior: FakeActionBehavior,
}

#[async_trait]
impl Action for FakeAction {
    /// 执行当前 fixture 配置的真实测试行为。
    async fn execute(
        &self,
        _cancellation: CancellationToken,
        request: ActionRequest,
        progress: Arc<dyn ProgressReporter>,
    ) -> Result<ActionResult, ActionError> {
        match &self.behavior {
            FakeActionBehavior::Progress => {
                assert_eq!(request.body, br#"{"ok":true}"#);
                assert_eq!(request.session_id, "session-1");
                progress
                    .send_progress(ProgressUpdate {
                        step: "build".to_owned(),
                        message: "working".to_owned(),
                        percent: 42.5,
                    })
                    .await?;
                Ok(ActionResult {
                    status_code: 201,
                    body: b"ok".to_vec(),
                })
            }
            FakeActionBehavior::Panic => panic!("boom"),
            FakeActionBehavior::Blocking { started, release } => {
                started.notify_one();
                release.notified().await;
                Ok(ActionResult {
                    status_code: 200,
                    body: b"done".to_vec(),
                })
            }
        }
    }

    /// 返回注册表路径使用的固定名称。
    fn name(&self) -> &str {
        self.name
    }

    /// 返回该 fixture 的执行预算。
    fn timeout(&self) -> Duration {
        self.timeout
    }
}

#[tokio::test]
async fn test_registry_execute_streams_progress_and_result() {
    let registry = new_registry();
    registry.register(Arc::new(FakeAction {
        name: "snapshot",
        timeout: Duration::ZERO,
        behavior: FakeActionBehavior::Progress,
    }));
    let sender = Arc::new(RecordingSender::default());
    registry
        .execute(
            CancellationToken::new(),
            "req-1",
            "/__control/snapshot/app",
            br#"{"ok":true}"#.to_vec(),
            "session-1",
            sender.clone(),
        )
        .await;

    let headers = sender.headers.lock().unwrap();
    assert_eq!(headers.len(), 1);
    assert_eq!(headers[0].sequence_number, 0);
    assert_eq!(headers[0].status_code, 200);
    drop(headers);

    let chunks = sender.chunks.lock().unwrap();
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].sequence_number, 1);
    assert!(!chunks[0].is_final);
    assert_eq!(chunks[1].sequence_number, 2);
    assert!(chunks[1].is_final);
    let progress = decode_json_chunk(&chunks[0]);
    assert_eq!(progress["type"], "progress");
    assert_eq!(progress["percent"], 42.5);
    let result = decode_json_chunk(&chunks[1]);
    assert_eq!(result["type"], "result");
    assert_eq!(result["status"], 201);
    assert_eq!(result["data"], "b2s=");
}

#[tokio::test]
async fn test_registry_execute_unknown_action_returns_json404() {
    let registry = new_registry();
    let sender = Arc::new(RecordingSender::default());
    registry
        .execute(
            CancellationToken::new(),
            "req-404",
            "/__control/missing",
            Vec::new(),
            "session-1",
            sender.clone(),
        )
        .await;

    let headers = sender.headers.lock().unwrap();
    assert_eq!(headers[0].status_code, 404);
    assert_eq!(headers[0].sequence_number, 0);
    drop(headers);
    let chunks = sender.chunks.lock().unwrap();
    assert_eq!(chunks[0].sequence_number, 1);
    assert!(chunks[0].is_final);
    assert_eq!(
        String::from_utf8_lossy(&chunks[0].data),
        "{\"error\":\"unknown action: missing\"}\n"
    );
}

#[tokio::test]
async fn test_registry_execute_rejects_concurrent_action() {
    let registry = Arc::new(new_registry());
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    registry.register(Arc::new(FakeAction {
        name: "slow",
        timeout: Duration::from_secs(60),
        behavior: FakeActionBehavior::Blocking {
            started: started.clone(),
            release: release.clone(),
        },
    }));

    let first_registry = registry.clone();
    let first_sender = Arc::new(RecordingSender::default());
    let first = tokio::spawn(async move {
        first_registry
            .execute(
                CancellationToken::new(),
                "req-first",
                "/__control/slow",
                Vec::new(),
                "session-1",
                first_sender,
            )
            .await;
    });
    started.notified().await;

    let second_sender = Arc::new(RecordingSender::default());
    registry
        .execute(
            CancellationToken::new(),
            "req-second",
            "/__control/slow",
            Vec::new(),
            "session-1",
            second_sender.clone(),
        )
        .await;
    assert_eq!(second_sender.headers.lock().unwrap()[0].status_code, 409);
    assert_eq!(
        String::from_utf8_lossy(&second_sender.chunks.lock().unwrap()[0].data),
        "{\"error\":\"slow is already in progress\"}\n"
    );

    release.notify_one();
    first.await.unwrap();
}

#[tokio::test]
async fn test_registry_execute_converts_action_panic_to_final_chunk() {
    let registry = new_registry();
    registry.register(Arc::new(FakeAction {
        name: "panic",
        timeout: Duration::ZERO,
        behavior: FakeActionBehavior::Panic,
    }));
    let sender = Arc::new(RecordingSender::default());
    registry
        .execute(
            CancellationToken::new(),
            "req-panic",
            "/__control/panic",
            Vec::new(),
            "session-1",
            sender.clone(),
        )
        .await;

    let chunks = sender.chunks.lock().unwrap();
    assert_eq!(chunks.len(), 1);
    assert!(chunks[0].is_final);
    let payload: serde_json::Value = serde_json::from_slice(&chunks[0].data).unwrap();
    assert_eq!(payload["type"], "error");
    assert_eq!(payload["error"], "internal error: action panicked");
}

#[tokio::test]
async fn test_sequenced_sender_wraps_send_errors() {
    let sender = Arc::new(RecordingSender {
        chunk_error: Mutex::new(Some("write failed".to_owned())),
        ..RecordingSender::default()
    });
    let sequenced = SequencedSender::new(sender);
    let error = sequenced
        .send_chunk(HttpTunnelResponseChunk::default())
        .await
        .expect_err("下游 chunk 错误必须上抛");
    assert_eq!(error.to_string(), "sequenced send chunk: write failed");
}

/// 解析 Registry 产生的 NDJSON chunk。
fn decode_json_chunk(chunk: &HttpTunnelResponseChunk) -> serde_json::Value {
    serde_json::from_slice(&chunk.data).expect("Registry chunk 应为 JSON")
}
