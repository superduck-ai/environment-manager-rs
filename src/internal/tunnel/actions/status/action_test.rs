//! status action 的 Linux loopback 测试，
use std::sync::Arc;

use async_trait::async_trait;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::internal::tunnel::actions::{
    Action, ActionError, ActionRequest, ActionResult, ProgressReporter, ProgressUpdate,
};

use super::{StatusAction, new_status_action};

struct NoopProgress;

#[async_trait]
impl ProgressReporter for NoopProgress {
    async fn send_progress(&self, _update: ProgressUpdate) -> Result<(), ActionError> {
        Ok(())
    }
}

/// 构造无日志副作用的 status action。
fn new_test_status_action() -> StatusAction {
    new_status_action()
}

/// 监听 Linux loopback 随机端口。
async fn listen_on_localhost() -> TcpListener {
    TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener 应创建成功")
}

/// 使用两秒取消预算执行成功请求。
async fn execute_status(action: &StatusAction, body: Vec<u8>) -> ActionResult {
    let cancellation = CancellationToken::new();
    let timeout = cancellation.clone();
    let timer = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        timeout.cancel();
    });
    let result = action
        .execute(
            cancellation,
            ActionRequest {
                body,
                session_id: "session-1".to_owned(),
                ..ActionRequest::default()
            },
            Arc::new(NoopProgress),
        )
        .await
        .expect("status execute 应成功");
    timer.abort();
    result
}

/// 验证响应 JSON 中的端口和监听状态。
fn assert_status_body(body: &[u8], listening: bool, port: u16) {
    let payload: serde_json::Value = serde_json::from_slice(body).expect("status body 应为 JSON");
    assert_eq!(payload["listening"], listening);
    assert_eq!(payload["port"], port);
}

#[tokio::test]
async fn test_status_action_execute_reports_listening_port() {
    let listener = listen_on_localhost().await;
    let port = listener.local_addr().unwrap().port();
    let action = new_test_status_action();
    let result = execute_status(&action, format!(r#"{{"port":{port}}}"#).into_bytes()).await;
    assert_eq!(result.status_code, 200);
    assert_status_body(&result.body, true, port);
}

#[tokio::test]
async fn test_status_action_execute_reports_closed_port() {
    let listener = listen_on_localhost().await;
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let action = new_test_status_action();
    let result = execute_status(&action, format!(r#"{{"port":{port}}}"#).into_bytes()).await;
    assert_eq!(result.status_code, 200);
    assert_status_body(&result.body, false, port);
}

#[tokio::test]
async fn test_status_action_execute_rejects_invalid_json() {
    let action = new_test_status_action();
    let error = action
        .execute(
            CancellationToken::new(),
            ActionRequest {
                body: b"{".to_vec(),
                ..ActionRequest::default()
            },
            Arc::new(NoopProgress),
        )
        .await
        .expect_err("非法 JSON 必须失败");
    assert!(error.to_string().starts_with("invalid request body:"));
}

#[tokio::test]
async fn test_status_action_execute_rejects_invalid_port() {
    let action = new_test_status_action();
    let error = action
        .execute(
            CancellationToken::new(),
            ActionRequest {
                body: br#"{"port":70000}"#.to_vec(),
                ..ActionRequest::default()
            },
            Arc::new(NoopProgress),
        )
        .await
        .expect_err("越界端口必须失败");
    assert_eq!(error.to_string(), "invalid port: 70000");
}
