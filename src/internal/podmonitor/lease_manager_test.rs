//! 租约心跳、超时和停止流程的行为测试。

use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures_util::stream::poll_fn;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use crate::internal::api::{ClientOptions, HttpClient};

use super::get_default_health_file_path;
use super::lease_manager::{
    claude_code_stuck_threshold_from, is_permanent_heartbeat_error, is_precondition_failed_error,
    is_retriable_stop_error, new_lease_manager_with_stuck_threshold, read_stop_response_body,
};
use super::types::{
    DEFAULT_CLAUDE_CODE_STUCK_THRESHOLD, DEFAULT_SHUTDOWN_GRACE_PERIOD, HeartbeatError,
    LeaseManager, LeaseManagerError, NO_SERVER_HEARTBEAT, SHUTDOWN_GRACE_PERIOD_BUFFER,
    StopApiError, StopRequestError,
};

/// 创建测试用共享 API client，避免网络故障测试等待生产级五分钟超时。
fn test_client(base_url: &str) -> HttpClient {
    HttpClient::with_options(
        base_url,
        ClientOptions {
            timeout: Some(Duration::from_secs(2)),
            ..ClientOptions::default()
        },
    )
    .unwrap()
}

/// 创建使用显式 stuck 阈值且不写日志的测试 manager。
fn test_manager(
    base_url: &str,
    interval: Duration,
    safety_window: Duration,
    health_file_path: impl Into<PathBuf>,
    stuck_threshold: Duration,
) -> LeaseManager {
    new_lease_manager_with_stuck_threshold(
        CancellationToken::new(),
        "session-1",
        "env-1",
        "org-1",
        test_client(base_url),
        interval,
        safety_window,
        health_file_path,
        stuck_threshold,
    )
}

/// 读取 mock 请求中的单个 query 值。
fn query_value(request: &Request, key: &str) -> Option<String> {
    request
        .url
        .query_pairs()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
}

/// 生成一次成功延长租约的标准响应。
fn successful_heartbeat(last_heartbeat: &str, lease_updated_at: DateTime<Utc>) -> Value {
    json!({
        "lease_extended": true,
        "state": "running",
        "last_heartbeat": last_heartbeat,
        "ttl_seconds": 3600,
        "lease_updated_at": lease_updated_at,
    })
}

/// 验证默认健康路径和环境变量阈值解析。
#[test]
fn test_default_health_path_and_stuck_threshold() {
    assert!(get_default_health_file_path().ends_with(Path::new(".agent-health/envmgr-healthy")));
    assert_eq!(
        claude_code_stuck_threshold_from(None),
        DEFAULT_CLAUDE_CODE_STUCK_THRESHOLD
    );
    assert_eq!(
        claude_code_stuck_threshold_from(Some("")),
        DEFAULT_CLAUDE_CODE_STUCK_THRESHOLD
    );
    assert_eq!(
        claude_code_stuck_threshold_from(Some("7")),
        Duration::from_secs(7)
    );
    assert_eq!(
        claude_code_stuck_threshold_from(Some("bad")),
        DEFAULT_CLAUDE_CODE_STUCK_THRESHOLD
    );
    assert_eq!(
        claude_code_stuck_threshold_from(Some("0")),
        DEFAULT_CLAUDE_CODE_STUCK_THRESHOLD
    );
}

/// 验证连续 heartbeat 更新租约、健康文件和 CAS 前值。
#[tokio::test]
async fn test_heartbeat_success_updates_lease_health_and_expected_heartbeat() {
    let server = MockServer::start().await;
    let heartbeat_count = Arc::new(AtomicUsize::new(0));
    let response_count = Arc::clone(&heartbeat_count);
    Mock::given(method("POST"))
        .and(path("/v1/environments/env-1/work/session-1/heartbeat"))
        .respond_with(move |_request: &Request| {
            let count = response_count.fetch_add(1, Ordering::SeqCst) + 1;
            ResponseTemplate::new(200).set_body_json(successful_heartbeat(
                &format!("server-hb-{count}"),
                Utc::now(),
            ))
        })
        .expect(2)
        .mount(&server)
        .await;

    let temp_dir = tempfile::tempdir().unwrap();
    let health_file = temp_dir.path().join("health/envmgr-healthy");
    let manager = test_manager(
        &server.uri(),
        Duration::from_secs(3600),
        Duration::ZERO,
        health_file.clone(),
        Duration::from_secs(3600),
    );
    manager.send_heartbeat().await.unwrap();
    manager.send_heartbeat().await.unwrap();

    assert_eq!(heartbeat_count.load(Ordering::SeqCst), 2);
    let requests = server.received_requests().await.unwrap();
    let heartbeats = requests
        .iter()
        .filter(|request| request.url.path().ends_with("/heartbeat"))
        .collect::<Vec<_>>();
    assert_eq!(heartbeats.len(), 2);
    assert_eq!(
        query_value(heartbeats[0], "organization_uuid").as_deref(),
        Some("org-1")
    );
    assert_eq!(
        query_value(heartbeats[0], "expected_last_heartbeat").as_deref(),
        Some(NO_SERVER_HEARTBEAT)
    );
    assert_eq!(
        query_value(heartbeats[1], "expected_last_heartbeat").as_deref(),
        Some("server-hb-1")
    );

    let lease = manager.get_lease_info();
    assert!(lease.last_heartbeat.is_some());
    assert!(lease.lease_expires_at.is_some());
    assert_eq!(lease.instance_state, "running");
    assert!(manager.time_until_expiry() > Duration::ZERO);

    let health_data = tokio::fs::read_to_string(&health_file).await.unwrap();
    DateTime::parse_from_rfc3339(health_data.trim()).unwrap();
}

/// 验证 lease 未延长且进入 stopping 时发送非强制 stop 并取消 manager。
#[tokio::test]
async fn test_heartbeat_lease_not_extended_stopping_stops_and_cancels() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/environments/env-1/work/session-1/heartbeat"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "lease_extended": false,
            "state": "stopping",
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/environments/env-1/work/session-1/stop"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let manager = test_manager(
        &server.uri(),
        Duration::from_secs(3600),
        Duration::ZERO,
        "",
        Duration::from_secs(3600),
    );
    manager.send_heartbeat().await.unwrap();

    assert!(manager.cancellation_token().is_cancelled());
    assert_eq!(manager.get_grace_period(), DEFAULT_SHUTDOWN_GRACE_PERIOD);
    let requests = server.received_requests().await.unwrap();
    let stop = requests
        .iter()
        .find(|request| request.url.path().ends_with("/stop"))
        .unwrap();
    assert_eq!(
        query_value(stop, "organization_uuid").as_deref(),
        Some("org-1")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&stop.body).unwrap(),
        json!({ "force": false })
    );
}

/// 验证初始 heartbeat 错误会被包装，且未成功 start 的 stop 会保持等待。
#[tokio::test]
async fn test_start_initial_heartbeat_failure_and_stop_after_start() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/environments/env-1/work/session-1/heartbeat"))
        .respond_with(ResponseTemplate::new(412).set_body_string("no lease"))
        .expect(1)
        .mount(&server)
        .await;
    let manager = test_manager(
        &server.uri(),
        Duration::from_millis(10),
        Duration::ZERO,
        "",
        Duration::from_secs(3600),
    );

    let error = manager.start().await.unwrap_err();
    assert!(matches!(error, LeaseManagerError::InitialHeartbeat(_)));
    assert!(error.to_string().contains("failed initial heartbeat"));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), manager.stop())
            .await
            .is_err(),
        "failed startup must not report the manager as cleanly stopped"
    );
}

/// 验证成功 start 后显式 stop 会取消并等待三个后台任务退出。
#[tokio::test]
async fn stop_after_start_waits_for_tracked_tasks() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/environments/env-1/work/session-1/heartbeat"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(successful_heartbeat("server-hb", Utc::now())),
        )
        .expect(1)
        .mount(&server)
        .await;
    let manager = test_manager(
        &server.uri(),
        Duration::from_secs(3600),
        Duration::ZERO,
        "",
        Duration::from_secs(3600),
    );
    manager.start().await.unwrap();

    tokio::time::timeout(Duration::from_secs(1), manager.stop())
        .await
        .expect("tracked tasks must observe cancellation");
    assert!(manager.cancellation_token().is_cancelled());
}

/// 验证 heartbeat loop 遇到永久 412 时调用 stop、取消并退出。
#[tokio::test]
async fn test_heartbeat_loop_permanent_error_stops_session() {
    let server = MockServer::start().await;
    let heartbeat_count = Arc::new(AtomicUsize::new(0));
    let response_count = Arc::clone(&heartbeat_count);
    Mock::given(method("POST"))
        .and(path("/v1/environments/env-1/work/session-1/heartbeat"))
        .respond_with(move |_request: &Request| {
            if response_count.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(200)
                    .set_body_json(successful_heartbeat("server-hb", Utc::now()))
            } else {
                ResponseTemplate::new(412).set_body_string("Precondition Failed")
            }
        })
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/environments/env-1/work/session-1/stop"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let manager = test_manager(
        &server.uri(),
        Duration::from_millis(10),
        Duration::ZERO,
        "",
        Duration::from_secs(3600),
    );
    let cancellation = manager.cancellation_token();
    manager.start().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), cancellation.cancelled())
        .await
        .expect("permanent heartbeat error must cancel manager");
    manager.stop_and_wait().await;

    assert_eq!(heartbeat_count.load(Ordering::SeqCst), 2);
}

/// 验证 stop forced wire 格式及 HTTP/cancellation sentinel 分类。
#[tokio::test]
async fn test_call_stop_api_forced_and_stop_error_classification() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/environments/env-1/work/session-1/stop"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    let manager = test_manager(
        &server.uri(),
        Duration::from_secs(3600),
        Duration::ZERO,
        "",
        Duration::from_secs(3600),
    );
    manager
        .call_stop_api_forced(&CancellationToken::new())
        .await
        .unwrap();

    let requests = server.received_requests().await.unwrap();
    let stop = requests
        .iter()
        .find(|request| request.url.path().ends_with("/stop"))
        .unwrap();
    assert_eq!(
        query_value(stop, "organization_uuid").as_deref(),
        Some("org-1")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&stop.body).unwrap(),
        json!({ "force": true })
    );

    let bad_request =
        StopRequestError::Api(StopApiError::new(400, "stop API returned status: 400"));
    assert_eq!(bad_request.to_string(), "stop API returned status: 400");
    assert!(!is_retriable_stop_error(&bad_request));
    assert!(is_retriable_stop_error(&StopRequestError::Api(
        StopApiError::new(500, "server error")
    )));
    assert!(!is_retriable_stop_error(&StopRequestError::Cancelled {
        message: "context canceled".to_owned(),
    }));
    assert!(!is_retriable_stop_error(
        &StopRequestError::DeadlineExceeded
    ));
}

/// 验证预先取消仍会进入 stop operation，再由请求层产生不可重试取消错误。
///
/// 这条用例防止通用 retry 恢复“调用 operation 前直接返回 Cancelled”的旧分支；
/// 取消必须由 `call_stop_api` 闭包观察，才能经过 classifier 和统一日志流程。
#[tokio::test]
async fn test_call_stop_api_observes_pre_cancel_inside_operation() {
    let manager = test_manager(
        "https://api.example.test",
        Duration::from_secs(3600),
        Duration::ZERO,
        "",
        Duration::from_secs(3600),
    );
    let cancellation = CancellationToken::new();
    cancellation.cancel();

    let error = manager
        .call_stop_api(&cancellation, false)
        .await
        .unwrap_err();
    assert!(matches!(&error, StopRequestError::Cancelled { .. }));
    assert_eq!(
        error.to_string(),
        "stop API request failed: Post \"https://api.example.test/v1/environments/env-1/work/session-1/stop?organization_uuid=org-1\": context canceled"
    );
}

/// 验证 HTTP 400 stop 错误在第一次尝试后立即返回。
#[tokio::test]
async fn test_call_stop_api_reports_non_retriable_status_once() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/environments/env-1/work/session-1/stop"))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
        .expect(1)
        .mount(&server)
        .await;
    let manager = test_manager(
        &server.uri(),
        Duration::from_secs(3600),
        Duration::ZERO,
        "",
        Duration::from_secs(3600),
    );
    let error = manager
        .call_stop_api(&CancellationToken::new(), false)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        StopRequestError::Api(StopApiError {
            status_code: 400,
            ..
        })
    ));
}

/// 服务端发出非 2xx headers 后，即使响应体永久停住，取消也必须及时结束请求。
///
/// 完整读取响应体 由 request context 中断且忽略读取错误；本测试使用原始
/// HTTP server 故意声明更长正文但不结束连接，防止 Reqwest 自行把 EOF 当作完成。
/// 取消与客户端交付 headers 存在真实竞态，因此允许请求层取消或 HTTP 400 两种结果，
/// 但两者都必须在一秒内返回；下一个测试再确定性固定已经进入 body 阶段的 partial。
#[tokio::test]
async fn test_call_stop_api_cancels_stalled_error_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (headers_sent, headers_received) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut buffer).await.unwrap();
            if read == 0 {
                return;
            }
            request.extend_from_slice(&buffer[..read]);
        }
        stream
            .write_all(
                b"HTTP/1.1 400 Bad Request\r\nContent-Length: 64\r\nConnection: close\r\n\r\npartial",
            )
            .await
            .unwrap();
        stream.flush().await.unwrap();
        let _ = headers_sent.send(());
        std::future::pending::<()>().await;
    });

    let manager = test_manager(
        &format!("http://{address}"),
        Duration::from_secs(3600),
        Duration::ZERO,
        "",
        Duration::from_secs(3600),
    );
    let cancellation = CancellationToken::new();
    let call_cancellation = cancellation.clone();
    let call = tokio::spawn(async move { manager.call_stop_api(&call_cancellation, false).await });

    tokio::time::timeout(Duration::from_secs(1), headers_received)
        .await
        .expect("server must send error headers")
        .expect("server must signal before exiting");
    cancellation.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), call)
        .await
        .expect("cancellation must interrupt the stalled response body")
        .expect("stop task must not panic")
        .expect_err("HTTP 400 remains an API error after body cancellation");
    assert!(matches!(
        error,
        StopRequestError::Cancelled { .. }
            | StopRequestError::Api(StopApiError {
                status_code: 400,
                ..
            })
    ));

    server.abort();
    let _ = server.await;
}

/// 分片读取已进入 body 阶段后，取消必须立即返回并保留取消前的 partial body。
#[tokio::test]
async fn test_read_stop_response_body_preserves_partial_bytes_on_cancel() {
    let chunk_delivered = Arc::new(tokio::sync::Notify::new());
    let stream_notification = Arc::clone(&chunk_delivered);
    let mut first_poll = true;
    let stream = poll_fn(move |_context| {
        if first_poll {
            first_poll = false;
            stream_notification.notify_one();
            Poll::Ready(Some(Ok::<_, std::io::Error>(b"partial body".to_vec())))
        } else {
            Poll::Pending
        }
    });
    let cancellation = CancellationToken::new();
    let read_cancellation = cancellation.clone();
    let read =
        tokio::spawn(async move { read_stop_response_body(stream, &read_cancellation).await });

    chunk_delivered.notified().await;
    cancellation.cancel();
    let body = tokio::time::timeout(Duration::from_secs(1), read)
        .await
        .expect("取消后 body reader 必须在一秒内完成")
        .expect("body reader 不应 panic");
    assert_eq!(body, b"partial body");
}

/// 验证 heartbeat marker 分类和“剩余租约 + 五秒”的宽限期。
#[test]
fn test_heartbeat_error_classifiers_and_grace_period() {
    let unauthorized = HeartbeatError::Http {
        status_code: 401,
        body: String::new(),
    };
    let forbidden = std::io::Error::other("Forbidden");
    let precondition = std::io::Error::other("Precondition Failed");
    for error in [
        &unauthorized as &(dyn Error + 'static),
        &forbidden as &(dyn Error + 'static),
        &precondition as &(dyn Error + 'static),
    ] {
        assert!(is_permanent_heartbeat_error(Some(error)));
    }

    let rate_limited = HeartbeatError::Http {
        status_code: 429,
        body: String::new(),
    };
    let connection_refused = std::io::Error::other("connection refused");
    let timeout = std::io::Error::other("timeout");
    for error in [
        &rate_limited as &(dyn Error + 'static),
        &connection_refused as &(dyn Error + 'static),
        &timeout as &(dyn Error + 'static),
    ] {
        assert!(!is_permanent_heartbeat_error(Some(error)));
    }
    assert!(!is_permanent_heartbeat_error(None));
    assert!(is_precondition_failed_error(Some(&HeartbeatError::Http {
        status_code: 412,
        body: String::new(),
    })));
    assert!(is_precondition_failed_error(Some(&precondition)));
    assert!(!is_precondition_failed_error(Some(&std::io::Error::other(
        "HTTP 500"
    ))));
    assert!(!is_precondition_failed_error(None));

    let manager = test_manager(
        "https://api.example.test",
        Duration::from_secs(3600),
        Duration::ZERO,
        "",
        Duration::from_secs(3600),
    );
    manager.inner.state.write().unwrap().lease_expires_at =
        Some(Utc::now() + ChronoDuration::milliseconds(50));
    manager.set_grace_period_and_cancel();
    assert!(manager.cancellation_token().is_cancelled());
    assert!(manager.get_grace_period() >= SHUTDOWN_GRACE_PERIOD_BUFFER);
}

/// 验证活动时间戳不再推进时，Notify 重算不会推迟卡死退出。
#[tokio::test]
async fn repeated_heartbeat_with_same_activity_triggers_stuck_shutdown() {
    let server = MockServer::start().await;
    let lease_updated_at = Utc::now();
    Mock::given(method("POST"))
        .and(path("/v1/environments/env-1/work/session-1/heartbeat"))
        .respond_with(move |_request: &Request| {
            ResponseTemplate::new(200)
                .set_body_json(successful_heartbeat("same-heartbeat", lease_updated_at))
        })
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/environments/env-1/work/session-1/stop"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let manager = test_manager(
        &server.uri(),
        Duration::from_millis(10),
        Duration::ZERO,
        "",
        Duration::from_millis(40),
    );
    let cancellation = manager.cancellation_token();
    manager.start().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), cancellation.cancelled())
        .await
        .expect("unchanged activity must trigger stuck shutdown");
    manager.stop_and_wait().await;
}

/// 验证安全窗口到达时租约任务调用 stop 并取消其余任务。
#[tokio::test]
async fn lease_expiry_safety_window_triggers_shutdown() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/environments/env-1/work/session-1/heartbeat"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "lease_extended": true,
            "state": "running",
            "last_heartbeat": "server-hb",
            "ttl_seconds": 1,
            "lease_updated_at": Utc::now(),
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/environments/env-1/work/session-1/stop"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let manager = test_manager(
        &server.uri(),
        Duration::from_secs(3600),
        Duration::from_millis(950),
        "",
        Duration::from_secs(3600),
    );
    let cancellation = manager.cancellation_token();
    manager.start().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), cancellation.cancelled())
        .await
        .expect("safety window must trigger lease shutdown");
    manager.stop_and_wait().await;
}

/// 验证空健康路径无操作，嵌套路径会写出可解析的 RFC 3339 纳秒时间戳。
#[tokio::test]
async fn test_update_health_file() {
    let manager = test_manager(
        "https://api.example.test",
        Duration::from_secs(3600),
        Duration::ZERO,
        "",
        Duration::from_secs(3600),
    );
    manager.update_health_file().await.unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let health_file = temp_dir.path().join("nested/health");
    let manager = test_manager(
        "https://api.example.test",
        Duration::from_secs(3600),
        Duration::ZERO,
        health_file.clone(),
        Duration::from_secs(3600),
    );
    manager.update_health_file().await.unwrap();
    let content = tokio::fs::read_to_string(health_file).await.unwrap();
    DateTime::parse_from_rfc3339(content.trim()).unwrap();
}
