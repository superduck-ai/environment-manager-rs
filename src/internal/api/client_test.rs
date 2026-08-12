//! `internal/api/client_test.go` 对应的 Rust 行为测试。

use std::collections::BTreeMap;
use std::io::Write as _;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use chrono::{TimeZone, Timelike, Utc};
use flate2::{Compression, write::GzEncoder};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{body_json, body_string, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::internal::util::go_path_escape;

use super::session_ingress_client::diag_logs_to_wire_format;
use super::*;

/// 启动一次性 raw HTTP/1.1 loopback，返回 base URL 和实际请求字节。
async fn spawn_raw_api_response(response: Vec<u8>) -> (String, oneshot::Receiver<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (request_sender, request_receiver) = oneshot::channel();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            let count = stream.read(&mut buffer).await.unwrap();
            assert_ne!(count, 0, "客户端在 headers 完成前关闭连接");
            request.extend_from_slice(&buffer[..count]);
            if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.split_once(':').and_then(|(name, value)| {
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
            })
            .unwrap_or(0);
        while request.len() < header_end + content_length {
            let count = stream.read(&mut buffer).await.unwrap();
            if count == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..count]);
        }
        request_sender.send(request).unwrap();
        stream.write_all(&response).await.unwrap();
        let _ = stream.shutdown().await;
    });
    (format!("http://{address}"), request_receiver)
}

/// 验证测试 transport 的 Close 故障标记能穿过统一重试层。
///
/// 三个上层 API 会在各自 Go `defer` 对应位置读取该标记并记录 warning/error；这里
/// 只验证共享 transport 不会在返回响应前丢弃标记，具体消息和级别由日志映射门禁检查。
#[tokio::test]
async fn response_close_error_fault_survives_retry_transport() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let mut client = HttpClient::new(&server.uri(), None).unwrap();
    client.inject_response_close_error_for_test("synthetic close failure");
    let request = client
        .http_client
        .get(server.uri())
        .build()
        .expect("构造故障注入请求");
    let response = client
        .retryable_http_do(&CancellationToken::new(), &request, None)
        .await
        .expect("故障只在 Close 阶段出现，HTTP 请求本身应成功");
    assert_eq!(
        super::client::go_response_close_error(&response).as_deref(),
        Some("synthetic close failure")
    );
}

/// 生成一个完整 gzip member，供 Go Transport 自动解压测试使用。
fn gzip_bytes(body: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(body).unwrap();
    encoder.finish().unwrap()
}

#[test]
fn test_new_http_client() {
    let client = HttpClient::new(
        "api.example.com///",
        Some(RetryConfig {
            max_retries: 1,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            backoff_factor: 1.0,
        }),
    )
    .unwrap();
    assert_eq!(client.base_url(), "https://api.example.com");
}

/// 验证事件时间使用 Go RFC3339Nano 的任意末尾零裁剪规则。
#[test]
fn event_timestamps_serialize_like_go_time_marshal_json() {
    let base = Utc.with_ymd_and_hms(2026, 7, 15, 1, 2, 3).unwrap();
    let log = EnvManagerLogEventData {
        level: LogLevel::Info,
        category: LogCategory::Init,
        content: "ready".to_owned(),
        timestamp: base.with_nanosecond(120_000_000).unwrap(),
        extra: BTreeMap::new(),
    };
    let value = serde_json::to_value(log).unwrap();
    assert_eq!(value["timestamp"], "2026-07-15T01:02:03.12Z");

    let denial = PermissionDenial {
        tool_name: "Bash".to_owned(),
        reason: "blocked".to_owned(),
        requested_at: base.with_nanosecond(123_456_780).unwrap(),
    };
    let value = serde_json::to_value(denial).unwrap();
    assert_eq!(value["requested_at"], "2026-07-15T01:02:03.12345678Z");
}

/// 验证 session context 保留 Go `json.RawMessage` 的 missing/null 差别。
#[test]
fn session_context_preserves_missing_null_and_string_zero_values() {
    let missing: SessionContextResponse =
        serde_json::from_str("{}").expect("缺失 session_context 使用 Go 结构体零值");
    assert!(missing.session_context.outcomes.is_none());
    assert!(missing.session_context.mcp_config.is_none());

    let explicit_null: SessionContextResponse = serde_json::from_value(json!({
        "session_context": {
            "cwd": null,
            "outcomes": null,
            "custom_system_prompt": null,
            "append_system_prompt": null,
            "model": null,
            "mcp_config": null
        }
    }))
    .expect("Go string/RawMessage 字段接受显式 null");
    assert!(explicit_null.session_context.cwd.is_empty());
    assert_eq!(explicit_null.session_context.outcomes, Some(Value::Null));
    assert_eq!(explicit_null.session_context.mcp_config, Some(Value::Null));

    let nested_null: SessionContextResponse = serde_json::from_str(r#"{"session_context":null}"#)
        .expect("显式 null session_context 使用零值");
    assert_eq!(nested_null, SessionContextResponse::default());

    let top_level: Option<SessionContextResponse> =
        serde_json::from_str("null").expect("顶层 null 应可映射到 response 零值");
    assert!(top_level.is_none());
}

#[tokio::test]
async fn test_post_json_with_response() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/endpoint"))
        .and(header("authorization", "Bearer token"))
        .and(header("content-type", "application/json"))
        .and(header("x-test-header", "test-value"))
        .and(body_json(json!({ "hello": "world" })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
        .expect(1)
        .mount(&server)
        .await;

    let client = HttpClient::with_options(
        &server.uri(),
        ClientOptions {
            timeout: None,
            retry_config: Some(RetryConfig {
                max_retries: 0,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
                backoff_factor: 1.0,
            }),
            default_headers: BTreeMap::from([(
                "x-test-header".to_owned(),
                "test-value".to_owned(),
            )]),
        },
    )
    .unwrap();
    let response: Value = client
        .post_json_with_response(
            &CancellationToken::new(),
            &format!("{}/endpoint", server.uri()),
            &json!({ "hello": "world" }),
            "token",
            "test op",
        )
        .await
        .unwrap();
    assert_eq!(response, json!({ "ok": true }));
}

#[tokio::test]
async fn test_post_json_not_implemented() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(501).set_body_string("missing"))
        .mount(&server)
        .await;
    let client = HttpClient::with_options(
        &server.uri(),
        ClientOptions {
            retry_config: Some(RetryConfig {
                max_retries: 0,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
                backoff_factor: 1.0,
            }),
            ..ClientOptions::default()
        },
    )
    .unwrap();
    let error = client
        .post_json(
            &CancellationToken::new(),
            &server.uri(),
            &json!({ "hello": "world" }),
            "token",
            "test op",
        )
        .await
        .unwrap_err();
    assert!(error.is_endpoint_not_implemented());
}

#[test]
fn endpoint_not_implemented_survives_context_wrapping() {
    let error = ApiError::EndpointNotImplemented("endpoint not implemented".to_owned())
        .with_context("failed to flush diagnostics");
    assert!(error.is_endpoint_not_implemented());
    assert_eq!(
        error.to_string(),
        "failed to flush diagnostics: endpoint not implemented"
    );
}

/// 对应 Go `TestRetryableHTTPDoRetriesAndReplaysBody`。
///
/// 第一次响应 503、第二次成功；两次请求都必须携带完整原始 body，防止 Reqwest
/// request clone 在重试时退化成空 POST。
#[tokio::test]
async fn test_retryable_http_do_retries_and_replays_body() {
    let server = MockServer::start().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let response_attempts = Arc::clone(&attempts);
    Mock::given(method("POST"))
        .and(body_string(r#"{"hello":"world"}"#))
        .respond_with(move |_request: &wiremock::Request| {
            if response_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(503).set_body_string("retry")
            } else {
                ResponseTemplate::new(200).set_body_string("ok")
            }
        })
        .expect(2)
        .mount(&server)
        .await;

    let retry = RetryConfig {
        max_retries: 1,
        initial_backoff: Duration::from_nanos(1),
        max_backoff: Duration::from_nanos(1),
        backoff_factor: 1.0,
    };
    let client = HttpClient::new(&server.uri(), Some(retry)).unwrap();
    let request = client
        .http_client
        .post(server.uri())
        .body(r#"{"hello":"world"}"#)
        .build()
        .unwrap();

    let response = client
        .retryable_http_do(&CancellationToken::new(), &request, None)
        .await
        .expect("second replay must succeed");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[test]
fn test_session_ingress_helpers() {
    let client = HttpSessionIngressClient::new(
        HttpClient::new("https://api.example.com", None).unwrap(),
        "token",
        false,
    );
    assert_eq!(
        client.session_endpoint("session/%2e;,:@&=+$中文", "events"),
        "https://api.example.com/v2/session_ingress/session/session%2F%252e%3B%2C:@&=+$%E4%B8%AD%E6%96%87/events"
    );

    let timestamp =
        Utc.with_ymd_and_hms(2026, 5, 28, 1, 2, 3).unwrap() + chrono::Duration::milliseconds(4);
    let lines = diag_logs_to_wire_format(&[DiagLogEntry {
        timestamp,
        fields: BTreeMap::from([
            ("level".to_owned(), json!("info")),
            ("message".to_owned(), json!("hello")),
        ]),
    }]);
    assert_eq!(lines[0]["timestamp"], json!("2026-05-28T01:02:03.004Z"));
    assert_eq!(lines[0]["level"], json!("info"));
}

/// 覆盖 Go `url.PathEscape` 与 URL parser 最容易产生差异的动态 path 值。
#[test]
fn test_api_uses_shared_go_path_escape_edge_cases() {
    let cases = [
        (";", "%3B"),
        (",", "%2C"),
        (".", "."),
        ("..", ".."),
        ("%2e", "%252e"),
        ("%2E%2E", "%252E%252E"),
        ("...", "..."),
        ("/", "%2F"),
        ("a b", "a%20b"),
        ("a:b@c&d=e+f$g", "a:b@c&d=e+f$g"),
        ("中文", "%E4%B8%AD%E6%96%87"),
    ];

    for (input, expected) in cases {
        assert_eq!(go_path_escape(input), expected, "input={input:?}");
    }
}

/// 验证 session context 的实际 GET 调用使用共享 Go path-segment 编码。
#[tokio::test]
async fn test_get_session_context_escapes_session_id_like_go() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/v2/sessions/session%2F%252e%3B%2C:@&=+$%E4%B8%AD%E6%96%87",
        ))
        .respond_with(ResponseTemplate::new(403).set_body_string("denied"))
        .expect(1)
        .mount(&server)
        .await;

    let client = SessionsClient::new(
        HttpClient::new(&server.uri(), None).unwrap(),
        "session-token",
    );
    let error = client
        .get_session_context(&CancellationToken::new(), "session/%2e;,:@&=+$中文")
        .await
        .expect_err("403 必须保留为鉴权错误");
    assert!(error.to_string().contains("authentication failed"));
}

/// 验证 work ACK 的 environment/work 两个动态 segment 都使用共享编码。
#[tokio::test]
async fn test_acknowledge_work_escapes_both_ids_like_go() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(
            "/v1/environments/env%2F%252e%3B%E4%B8%AD%E6%96%87/work/work%2C:@&=+$/ack",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let client = WorkClient::new(HttpClient::new(&server.uri(), None).unwrap(), "service-key");
    client
        .acknowledge_work(&CancellationToken::new(), "env/%2e;中文", "work,:@&=+$")
        .await
        .expect("Go-compatible escaped ACK path should succeed");
}

/// 验证 CCR worker 的所有子资源共享同一个已转义 session segment。
#[tokio::test]
async fn test_ccr_backend_escapes_session_id_like_go() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(
            "/v1/code/sessions/session%2F%252E%252E%3B%2C:@&=+$%E4%B8%AD%E6%96%87/worker/register",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "worker_epoch": "7"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let backend = CCRBackend::new(
        HttpClient::new(&server.uri(), None).unwrap(),
        "session-token",
        "session/%2E%2E;,:@&=+$中文",
    );
    assert_eq!(
        backend
            .register_worker(&CancellationToken::new())
            .await
            .expect("Go-compatible escaped register path should succeed"),
        7
    );
}

/// 验证 CCR register 的 `worker_epoch,string` 保留 Go primitive 零值规则。
#[tokio::test]
async fn ccr_register_worker_epoch_accepts_go_null_and_missing_zero_values() {
    // encoding/json 对缺失、null 和 quoted null 都不会覆盖 int64 的初始零值；这些
    // 响应虽不常见，却会决定 worker 后续事件与 OTLP URL 使用哪个 epoch。
    for body in [
        br#"{}"#.as_slice(),
        br#"{"worker_epoch":null}"#.as_slice(),
        br#"{"worker_epoch":"null"}"#.as_slice(),
    ] {
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        let (base_url, _request) = spawn_raw_api_response(response).await;
        let backend = CCRBackend::new(
            HttpClient::new(
                &base_url,
                Some(RetryConfig {
                    max_retries: 0,
                    initial_backoff: Duration::ZERO,
                    max_backoff: Duration::ZERO,
                    backoff_factor: 1.0,
                }),
            )
            .unwrap(),
            "token",
            "session",
        );
        assert_eq!(
            backend
                .register_worker(&CancellationToken::new())
                .await
                .expect("Go-compatible zero worker epoch should parse"),
            0
        );
    }
}

/// 精确覆盖 Go JSON HTML 转义、默认 transport headers 与自动 gzip 解压。
#[tokio::test]
async fn json_transport_headers_and_gzip_match_go() {
    let compressed = gzip_bytes(br#"{"ok":true}"#);
    let mut raw_response = format!(
        "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        compressed.len()
    )
    .into_bytes();
    raw_response.extend_from_slice(&compressed);
    let (base_url, request) = spawn_raw_api_response(raw_response).await;
    let client = HttpClient::with_options(
        &base_url,
        ClientOptions {
            timeout: Some(Duration::ZERO),
            retry_config: Some(RetryConfig {
                max_retries: 0,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
                backoff_factor: 1.0,
            }),
            ..ClientOptions::default()
        },
    )
    .unwrap();
    let payload = BTreeMap::from([("a", json!("<\u{2028}&>")), ("z", json!("last"))]);
    let response: Value = client
        .post_json_with_response(
            &CancellationToken::new(),
            &format!("{base_url}/json"),
            &payload,
            "token",
            "gzip JSON",
        )
        .await
        .expect("零 timeout 应采用五分钟默认值，gzip JSON 应自动解压");
    assert_eq!(response, json!({"ok": true}));

    let request = request.await.unwrap();
    let (headers, body) = request.split_at(
        request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap()
            + 4,
    );
    let headers = String::from_utf8_lossy(headers).to_ascii_lowercase();
    assert!(headers.contains("user-agent: go-http-client/1.1\r\n"));
    assert!(headers.contains("accept-encoding: gzip\r\n"));
    assert_eq!(body, br#"{"a":"\u003c\u2028\u0026\u003e","z":"last"}"#);
}

/// 调用方显式 Accept-Encoding 时 Go Transport 不覆盖 header，也不自动解压。
#[tokio::test]
async fn explicit_accept_encoding_disables_transport_gzip() {
    let compressed = gzip_bytes(br#"{"ok":true}"#);
    let mut raw_response = format!(
        "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        compressed.len()
    )
    .into_bytes();
    raw_response.extend_from_slice(&compressed);
    let (base_url, request) = spawn_raw_api_response(raw_response).await;
    let client = HttpClient::with_options(
        &base_url,
        ClientOptions {
            retry_config: Some(RetryConfig {
                max_retries: 0,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
                backoff_factor: 1.0,
            }),
            default_headers: BTreeMap::from([
                ("Accept-Encoding".to_owned(), "identity".to_owned()),
                (
                    "User-Agent".to_owned(),
                    "custom-environment-manager".to_owned(),
                ),
            ]),
            ..ClientOptions::default()
        },
    )
    .unwrap();
    let error = client
        .post_json_with_response::<_, Value>(
            &CancellationToken::new(),
            &format!("{base_url}/explicit"),
            &json!({}),
            "token",
            "explicit encoding",
        )
        .await
        .expect_err("显式 identity 时压缩字节必须原样进入 JSON parser");
    assert!(
        error
            .to_string()
            .starts_with("failed to parse explicit encoding response:")
    );
    let request = String::from_utf8_lossy(&request.await.unwrap()).to_ascii_lowercase();
    assert!(request.contains("accept-encoding: identity\r\n"));
    assert!(!request.contains("accept-encoding: gzip\r\n"));
    assert!(request.contains("user-agent: custom-environment-manager\r\n"));
    assert!(!request.contains("user-agent: go-http-client/1.1\r\n"));
}

/// Go 允许显式空 User-Agent 完全禁止默认 header，不能发送一个空值 header。
#[test]
fn explicit_empty_user_agent_suppresses_transport_default() {
    let client = HttpClient::new("https://api.example.test", None).unwrap();
    let mut request = client
        .http_client
        .get("https://api.example.test/path")
        .header("User-Agent", "")
        .build()
        .unwrap();
    assert!(request.headers().contains_key("User-Agent"));

    prepare_go_transport_request(&mut request);

    assert!(!request.headers().contains_key("User-Agent"));
}

/// Go 忽略通用 POST 的 ReadAll error，但错误文案保留截断前的 partial body。
#[tokio::test]
async fn truncated_non_ok_response_keeps_partial_body() {
    let response =
        b"HTTP/1.1 418 I'm a teapot\r\nContent-Length: 10\r\nConnection: close\r\n\r\npart"
            .to_vec();
    let (base_url, _request) = spawn_raw_api_response(response).await;
    let client = HttpClient::with_options(
        &base_url,
        ClientOptions {
            retry_config: Some(RetryConfig {
                max_retries: 0,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
                backoff_factor: 1.0,
            }),
            ..ClientOptions::default()
        },
    )
    .unwrap();
    let error = client
        .post_json(
            &CancellationToken::new(),
            &format!("{base_url}/partial"),
            &json!({}),
            "token",
            "partial",
        )
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "partial responded with non-OK 418: part");
}

/// CCR event 的 map round-trip 与 diagnostics map 顺序必须逐字节等于 Go。
#[tokio::test]
async fn ccr_request_bodies_preserve_go_dynamic_json_and_map_order() {
    let event_response =
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec();
    let (event_base, event_request) = spawn_raw_api_response(event_response).await;
    let event_backend = CCRBackend::new(
        HttpClient::new(
            &event_base,
            Some(RetryConfig {
                max_retries: 0,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
                backoff_factor: 1.0,
            }),
        )
        .unwrap(),
        "token",
        "session",
    );
    event_backend
        .post_event(
            &CancellationToken::new(),
            &SessionEvent {
                event_type: "event".to_owned(),
                uuid: "u".to_owned(),
                data: Some(json!({
                    "z": "<&>",
                    "number": 1.0,
                    "big": 9_007_199_254_740_993_u64,
                })),
                subtype: Some("done".to_owned()),
                ..SessionEvent::default()
            },
        )
        .await
        .unwrap();
    let event_request = event_request.await.unwrap();
    let event_body = &event_request[event_request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap()
        + 4..];
    assert_eq!(
        event_body,
        br#"{"events":[{"payload":{"data":{"big":9007199254740992,"number":1,"z":"\u003c\u0026\u003e"},"subtype":"done","type":"event","uuid":"u"}}],"worker_epoch":0}"#
    );

    let diagnostics_response =
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec();
    let (diagnostics_base, diagnostics_request) =
        spawn_raw_api_response(diagnostics_response).await;
    let diagnostics_backend = CCRBackend::new(
        HttpClient::new(
            &diagnostics_base,
            Some(RetryConfig {
                max_retries: 0,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
                backoff_factor: 1.0,
            }),
        )
        .unwrap(),
        "token",
        "session",
    );
    diagnostics_backend
        .flush_logs(
            &CancellationToken::new(),
            &[DiagLogEntry {
                timestamp: Utc.with_ymd_and_hms(2026, 7, 15, 1, 2, 3).unwrap(),
                fields: BTreeMap::from([("message".to_owned(), json!("<&>"))]),
            }],
        )
        .await
        .unwrap();
    let diagnostics_request = diagnostics_request.await.unwrap();
    let diagnostics_body = &diagnostics_request[diagnostics_request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap()
        + 4..];
    assert_eq!(
        diagnostics_body,
        br#"{"lines":[{"fields":{"message":"\u003c\u0026\u003e"},"timestamp":"2026-07-15T01:02:03.000Z"}],"session_id":"session","worker_epoch":0}"#
    );
}

/// SessionIngress backend 的 OTLP URL 使用原始构造参数，不复用规范化 API base。
#[test]
fn session_ingress_otlp_endpoints_keep_raw_base_url() {
    let backend =
        SessionIngressBackend::new("https://api.example.test///", "token", "session").unwrap();
    let endpoints = backend.otlp_endpoints();
    assert_eq!(
        endpoints.metrics_endpoint,
        "https://api.example.test////anthropic.session_ingress.api.v1alpha.SessionService/ExportMetrics"
    );
    assert_eq!(
        endpoints.logs_endpoint,
        "https://api.example.test////anthropic.session_ingress.api.v1alpha.SessionService/ExportLogs"
    );
}

/// 已取消请求和 connection-refused 都必须走稳定的 Go retry 包装顺序。
#[tokio::test]
async fn retry_errors_use_go_context_and_transport_mapping() {
    let client = HttpClient::new(
        "http://127.0.0.1:9",
        Some(RetryConfig {
            max_retries: 0,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            backoff_factor: 1.0,
        }),
    )
    .unwrap();
    let request = client
        .http_client
        .get("http://127.0.0.1:9/")
        .build()
        .unwrap();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = client
        .retryable_http_do(&cancellation, &request, None)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "request failed after 0 retries: request failed: Get \"http://127.0.0.1:9/\": context canceled"
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let endpoint = format!("http://{address}/");
    let request = client.http_client.get(&endpoint).build().unwrap();
    let error = client
        .retryable_http_do(&CancellationToken::new(), &request, None)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        format!(
            "request failed after 0 retries: request failed: Get \"{endpoint}\": dial tcp {address}: connect: connection refused"
        )
    );
}

#[tokio::test]
async fn test_noop_backend() {
    let backend = NoopBackend::new();
    let cancellation = CancellationToken::new();
    backend
        .post_event(&cancellation, &SessionEvent::default())
        .await
        .unwrap();
    backend.flush_logs(&cancellation, &[]).await.unwrap();
    assert_eq!(backend.otlp_endpoints(), OtlpEndpoints::default());
}
