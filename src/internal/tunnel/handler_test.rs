//! 本地 HTTP tunnel 转发测试。

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, Response, StatusCode};
use axum::routing::any;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

use super::actions::ActionError;
use super::{
    Header, HttpTunnelRequest, HttpTunnelResponseChunk, HttpTunnelResponseError,
    HttpTunnelResponseHeaders, ResponseSender, new_handler, new_handler_with_timing_for_test,
};

#[derive(Default)]
struct RecordingSender {
    headers: Mutex<Vec<HttpTunnelResponseHeaders>>,
    chunks: Mutex<Vec<HttpTunnelResponseChunk>>,
    errors: Mutex<Vec<HttpTunnelResponseError>>,
    chunk_events: Mutex<Option<mpsc::UnboundedSender<HttpTunnelResponseChunk>>>,
}

#[async_trait]
impl ResponseSender for RecordingSender {
    async fn send_headers(&self, response: HttpTunnelResponseHeaders) -> Result<(), ActionError> {
        self.headers.lock().unwrap().push(response);
        Ok(())
    }

    async fn send_chunk(&self, chunk: HttpTunnelResponseChunk) -> Result<(), ActionError> {
        if let Some(events) = self.chunk_events.lock().unwrap().as_ref() {
            let _ = events.send(chunk.clone());
        }
        self.chunks.lock().unwrap().push(chunk);
        Ok(())
    }

    async fn send_error(&self, response: HttpTunnelResponseError) -> Result<(), ActionError> {
        self.errors.lock().unwrap().push(response);
        Ok(())
    }
}

/// 启动一次性原始 HTTP/1 server，以便测试 Axum 会规范化掉的线协议细节。
///
/// server 接收一条连接，读取完整 request head，将原始字节发给测试，再写入调用方给定
/// 的响应。返回的任务必须由测试 await，防止端口在断言前被释放。
async fn start_raw_http_server(
    response: Vec<u8>,
) -> (
    i32,
    oneshot::Receiver<Vec<u8>>,
    JoinHandle<std::io::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let (request_tx, request_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_raw_request_head(&mut stream).await?;
        let _ = request_tx.send(request);
        stream.write_all(&response).await?;
        stream.shutdown().await
    });
    (port, request_rx, task)
}

/// 从原始连接读取到首个 CRLFCRLF，保留 request line 与 header 的确切字节。
async fn read_raw_request_head(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut request = Vec::new();
    let mut buffer = [0u8; 1024];
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "request head ended before CRLFCRLF",
            ));
        }
        request.extend_from_slice(&buffer[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            return Ok(request);
        }
    }
}

/// 发送单个 Tunnel 请求到一次性原始 server，并返回录制结果与原始 request head。
async fn run_raw_http_exchange(
    mut request: HttpTunnelRequest,
    response: Vec<u8>,
) -> (
    Arc<RecordingSender>,
    Result<(), super::TunnelError>,
    Vec<u8>,
) {
    let (port, request_rx, server) = start_raw_http_server(response).await;
    request.port = port;
    let handler = new_handler().unwrap();
    let sender = Arc::new(RecordingSender::default());
    let result = handler
        .handle_request(CancellationToken::new(), request, sender.clone())
        .await;
    let request_head = request_rx.await.unwrap();
    server.await.unwrap().unwrap();
    (sender, result, request_head)
}

/// 合并所有非 final Tunnel chunk，便于断言透明解压和部分读取结果。
fn recorded_body(sender: &RecordingSender) -> Vec<u8> {
    sender
        .chunks
        .lock()
        .unwrap()
        .iter()
        .filter(|chunk| !chunk.is_final)
        .flat_map(|chunk| chunk.data.iter().copied())
        .collect()
}

async fn start_local_http_server(router: Router) -> (i32, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (port, task)
}

#[tokio::test]
async fn test_handle_request_rejects_invalid_port() {
    let handler = new_handler().unwrap();
    let sender = Arc::new(RecordingSender::default());

    handler
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "req-invalid-port".to_owned(),
                port: 0,
                path: "/".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await
        .unwrap();

    let errors = sender.errors.lock().unwrap();
    assert_eq!(errors.len(), 1);
    assert!(errors[0].error_message.contains("invalid port"));
}

#[tokio::test]
async fn test_handle_request_rejects_path_without_slash() {
    let handler = new_handler().unwrap();
    let sender = Arc::new(RecordingSender::default());

    handler
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "req-invalid-path".to_owned(),
                port: 3000,
                path: "api".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await
        .unwrap();

    let errors = sender.errors.lock().unwrap();
    assert_eq!(errors.len(), 1);
    assert!(errors[0].error_message.contains("path must start with /"));
}

#[tokio::test]
async fn test_handle_request_forwards_and_streams_response() {
    let large_body = vec![b'x'; 40 * 1024];
    let response_body = large_body.clone();
    let (request_tx, mut request_rx) = mpsc::unbounded_channel();
    let router = Router::new().route(
        "/api",
        any(move |request: Request<Body>| {
            let request_tx = request_tx.clone();
            let response_body = response_body.clone();
            async move {
                let method = request.method().clone();
                let uri = request.uri().clone();
                let input_header = request
                    .headers()
                    .get("x-input")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                let host = request
                    .headers()
                    .get(http::header::HOST)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                let body = to_bytes(request.into_body(), usize::MAX).await.unwrap();
                request_tx
                    .send((method, uri, input_header, host, body.to_vec()))
                    .unwrap();
                Response::builder()
                    .status(StatusCode::CREATED)
                    .header("x-reply", "ok")
                    .body(Body::from(response_body))
                    .unwrap()
            }
        }),
    );
    let (port, server) = start_local_http_server(router).await;

    let handler = new_handler().unwrap();
    let sender = Arc::new(RecordingSender::default());
    handler
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "req-stream".to_owned(),
                port,
                method: "POST".to_owned(),
                path: "/api?q=1".to_owned(),
                headers: vec![
                    Header {
                        name: "X-Input".to_owned(),
                        value: "yes".to_owned(),
                    },
                    Header {
                        name: "Host".to_owned(),
                        value: "attacker.example".to_owned(),
                    },
                ],
                body: b"request-body".to_vec(),
            },
            sender.clone(),
        )
        .await
        .unwrap();

    let (method, uri, input_header, host, body) = request_rx.recv().await.unwrap();
    assert_eq!(method.as_str(), "POST");
    assert_eq!(uri.to_string(), "/api?q=1");
    assert_eq!(input_header, "yes");
    assert_eq!(host, format!("localhost:{port}"));
    assert_eq!(body, b"request-body");
    assert!(sender.errors.lock().unwrap().is_empty());

    let headers = sender.headers.lock().unwrap();
    assert_eq!(headers.len(), 1);
    assert_eq!(headers[0].status_code, 201);
    assert_eq!(headers[0].sequence_number, 0);
    drop(headers);

    let chunks = sender.chunks.lock().unwrap();
    assert!(chunks.len() >= 2);
    let mut streamed = Vec::new();
    for (index, chunk) in chunks.iter().enumerate() {
        assert_eq!(chunk.sequence_number, (index + 1) as u32);
        if index + 1 == chunks.len() {
            assert!(chunk.is_final);
            assert!(chunk.data.is_empty());
        } else {
            assert!(!chunk.is_final);
            assert!(chunk.data.len() <= super::HTTP_TUNNEL_CHUNK_SIZE);
            streamed.extend_from_slice(&chunk.data);
        }
    }
    assert_eq!(streamed, large_body);
    assert!(
        chunks.iter().filter(|chunk| !chunk.is_final).all(
            |chunk| !chunk.data.is_empty() && chunk.data.len() <= super::HTTP_TUNNEL_CHUNK_SIZE
        ),
        "每个 AsyncRead 返回都应立即成为一个非空、最大 32 KiB 的数据块",
    );
    server.abort();
}

#[tokio::test]
async fn test_cancel_request_cancels_active_http_forward() {
    let received = Arc::new(Notify::new());
    let received_for_server = Arc::clone(&received);
    let router = Router::new().route(
        "/wait",
        any(move || {
            let received = Arc::clone(&received_for_server);
            async move {
                received.notify_one();
                std::future::pending::<Result<Response<Body>, Infallible>>().await
            }
        }),
    );
    let (port, server) = start_local_http_server(router).await;

    let handler = Arc::new(new_handler().unwrap());
    let sender = Arc::new(RecordingSender::default());
    let running_handler = Arc::clone(&handler);
    let running_sender = sender.clone();
    let task = tokio::spawn(async move {
        running_handler
            .handle_request(
                CancellationToken::new(),
                HttpTunnelRequest {
                    request_id: "req-cancel".to_owned(),
                    port,
                    method: "GET".to_owned(),
                    path: "/wait".to_owned(),
                    ..Default::default()
                },
                running_sender,
            )
            .await
    });

    timeout(Duration::from_secs(2), received.notified())
        .await
        .unwrap();
    handler.cancel_request("req-cancel");
    timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let errors = sender.errors.lock().unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].error_message, "request canceled");
    server.abort();
}

#[tokio::test]
async fn test_streams_one_byte_flush_without_waiting_for_eof() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_raw_request_head(&mut stream).await.unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\na")
            .await
            .unwrap();
        stream.flush().await.unwrap();
        let _ = release_rx.await;
        stream.write_all(b"b").await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let handler = new_handler().unwrap();
    let sender = Arc::new(RecordingSender::default());
    let (chunk_tx, mut chunk_rx) = mpsc::unbounded_channel();
    *sender.chunk_events.lock().unwrap() = Some(chunk_tx);
    let running_sender = sender.clone();
    let task = tokio::spawn(async move {
        handler
            .handle_request(
                CancellationToken::new(),
                HttpTunnelRequest {
                    request_id: "req-flush".to_owned(),
                    port,
                    path: "/flush".to_owned(),
                    ..Default::default()
                },
                running_sender,
            )
            .await
    });

    // server 尚未发送第二字节和 EOF；若实现试图凑满 32 KiB，这里会超时。
    let first = timeout(Duration::from_secs(2), chunk_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.data, b"a");
    assert!(!first.is_final);
    let _ = release_tx.send(());
    task.await.unwrap().unwrap();
    server.await.unwrap();
    assert_eq!(recorded_body(&sender), b"ab");
}

#[tokio::test]
async fn test_preserves_partial_body_before_read_error() {
    let (sender, result, _) = run_raw_http_exchange(
        HttpTunnelRequest {
            request_id: "req-partial-error".to_owned(),
            path: "/partial".to_owned(),
            ..Default::default()
        },
        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\na".to_vec(),
    )
    .await;

    assert_eq!(recorded_body(&sender), b"a");
    assert!(
        sender
            .chunks
            .lock()
            .unwrap()
            .iter()
            .all(|chunk| !chunk.is_final),
    );
    let error = result.unwrap_err().to_string();
    assert!(!error.is_empty());
    assert!(
        sender.errors.lock().unwrap()[0]
            .error_message
            .starts_with("failed to read response body: "),
    );
}

#[tokio::test]
async fn test_preserves_custom_and_empty_reason_phrases() {
    let (custom, result, _) = run_raw_http_exchange(
        HttpTunnelRequest {
            request_id: "req-custom-reason".to_owned(),
            path: "/".to_owned(),
            ..Default::default()
        },
        b"HTTP/1.1 299 Custom Thing\r\nContent-Length: 0\r\n\r\n".to_vec(),
    )
    .await;
    result.unwrap();
    assert_eq!(
        custom.headers.lock().unwrap()[0].status_text,
        "299 Custom Thing"
    );

    let (empty, result, _) = run_raw_http_exchange(
        HttpTunnelRequest {
            request_id: "req-empty-reason".to_owned(),
            path: "/".to_owned(),
            ..Default::default()
        },
        b"HTTP/1.1 299 \r\nContent-Length: 0\r\n\r\n".to_vec(),
    )
    .await;
    result.unwrap();
    assert_eq!(empty.headers.lock().unwrap()[0].status_text, "299 ");
}

#[tokio::test]
async fn test_removes_consumed_http_framing_headers() {
    let (sender, result, _) = run_raw_http_exchange(
        HttpTunnelRequest {
            request_id: "req-chunked-headers".to_owned(),
            path: "/".to_owned(),
            ..Default::default()
        },
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 99\r\nTrailer: X-End\r\nConnection: close\r\nX-Keep: yes\r\n\r\n1\r\na\r\n0\r\nX-End: done\r\n\r\n".to_vec(),
    )
    .await;
    result.unwrap();
    assert_eq!(recorded_body(&sender), b"a");
    let headers = &sender.headers.lock().unwrap()[0].headers;
    assert!(headers.iter().any(|header| header.name == "X-Keep"));
    for removed in [
        "Transfer-Encoding",
        "Content-Length",
        "Trailer",
        "Connection",
    ] {
        assert!(headers.iter().all(|header| header.name != removed));
    }
}

#[tokio::test]
async fn test_rebuilds_transport_headers_and_preserves_user_agent() {
    let (sender, result, request_head) = run_raw_http_exchange(
        HttpTunnelRequest {
            request_id: "req-special-headers".to_owned(),
            method: "POST".to_owned(),
            path: "/headers".to_owned(),
            headers: vec![
                Header {
                    name: "Host".to_owned(),
                    value: "attacker.example".to_owned(),
                },
                Header {
                    name: "Content-Length".to_owned(),
                    value: "999".to_owned(),
                },
                Header {
                    name: "Transfer-Encoding".to_owned(),
                    value: "chunked".to_owned(),
                },
                Header {
                    name: "Trailer".to_owned(),
                    value: "X-End".to_owned(),
                },
                Header {
                    name: "User-Agent".to_owned(),
                    value: "first-agent".to_owned(),
                },
                Header {
                    name: "user-agent".to_owned(),
                    value: "second-agent".to_owned(),
                },
            ],
            body: b"abc".to_vec(),
            ..Default::default()
        },
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_vec(),
    )
    .await;
    result.unwrap();
    assert!(sender.errors.lock().unwrap().is_empty());
    let head = String::from_utf8(request_head)
        .unwrap()
        .to_ascii_lowercase();
    assert!(head.contains("content-length: 3\r\n"));
    assert!(head.contains("user-agent: first-agent\r\n"));
    assert!(head.contains("user-agent: second-agent\r\n"));
    assert!(!head.contains("transfer-encoding:"));
    assert!(!head.contains("trailer:"));
    assert!(!head.contains("attacker.example"));
}

#[tokio::test]
async fn test_request_target_preserves_query_and_escapes_backslash() {
    let (_, result, request_head) = run_raw_http_exchange(
        HttpTunnelRequest {
            request_id: "req-url".to_owned(),
            path: "/a\\b?bad=%zz&quote='".to_owned(),
            ..Default::default()
        },
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_vec(),
    )
    .await;
    result.unwrap();
    let request_line = request_head.split(|byte| *byte == b'\n').next().unwrap();
    assert_eq!(request_line, b"GET /a%5Cb?bad=%zz&quote=' HTTP/1.1\r");
}

#[tokio::test]
async fn test_rejects_invalid_method_path_and_header() {
    let handler = new_handler().unwrap();
    let sender = Arc::new(RecordingSender::default());
    let method_error = handler
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "bad-method".to_owned(),
                port: 3000,
                method: "BAD METHOD".to_owned(),
                path: "/".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await
        .unwrap_err();
    assert!(method_error.to_string().contains("invalid HTTP method"));
    assert!(
        sender
            .errors
            .lock()
            .unwrap()
            .pop()
            .unwrap()
            .error_message
            .starts_with("failed to create request: "),
    );

    let url_error = handler
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "bad-url".to_owned(),
                port: 3000,
                path: "/bad%zz".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await
        .unwrap_err();
    assert!(url_error.to_string().contains("invalid percent escape"));

    handler
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "bad-header".to_owned(),
                port: 3000,
                path: "/".to_owned(),
                headers: vec![Header {
                    name: "Bad Header".to_owned(),
                    value: "value".to_owned(),
                }],
                ..Default::default()
            },
            sender.clone(),
        )
        .await
        .unwrap();
    assert!(
        sender
            .errors
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .error_message
            .starts_with("request failed: invalid header name"),
    );

    let control_error = handler
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "bad-control".to_owned(),
                port: 3000,
                path: "/?line=one\ntwo".to_owned(),
                ..Default::default()
            },
            sender,
        )
        .await
        .unwrap_err();
    assert!(control_error.to_string().contains("control character"));
}

#[tokio::test]
async fn test_preserves_content_encoding_without_negotiating_compression() {
    let (sender, result, request_head) = run_raw_http_exchange(
        HttpTunnelRequest {
            request_id: "req-encoded".to_owned(),
            path: "/".to_owned(),
            ..Default::default()
        },
        b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 7\r\n\r\nencoded".to_vec(),
    )
    .await;
    result.unwrap();
    assert!(
        !String::from_utf8(request_head)
            .unwrap()
            .to_ascii_lowercase()
            .contains("accept-encoding:"),
    );
    assert_eq!(recorded_body(&sender), b"encoded");
    let headers = &sender.headers.lock().unwrap()[0].headers;
    assert!(
        headers
            .iter()
            .any(|header| header.name == "Content-Encoding" && header.value == "gzip"),
    );
    assert!(
        headers
            .iter()
            .any(|header| header.name == "Content-Length" && header.value == "7"),
    );
}

#[tokio::test]
async fn test_http_101_streams_upgraded_connection_body() {
    let (sender, result, _) = run_raw_http_exchange(
        HttpTunnelRequest {
            request_id: "req-upgrade".to_owned(),
            path: "/upgrade".to_owned(),
            headers: vec![
                Header {
                    name: "Connection".to_owned(),
                    value: "Upgrade".to_owned(),
                },
                Header {
                    name: "Upgrade".to_owned(),
                    value: "test-protocol".to_owned(),
                },
            ],
            ..Default::default()
        },
        b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test-protocol\r\n\r\nhello".to_vec(),
    )
    .await;
    result.unwrap();
    assert_eq!(
        sender.headers.lock().unwrap()[0].status_text,
        "101 Switching Protocols"
    );
    assert_eq!(recorded_body(&sender), b"hello");
}

#[tokio::test]
async fn test_aborted_future_drops_active_request_registration() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let accepted = Arc::new(Notify::new());
    let server_accepted = Arc::clone(&accepted);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_raw_request_head(&mut stream).await.unwrap();
        server_accepted.notify_one();
        std::future::pending::<()>().await;
    });
    let handler = Arc::new(new_handler().unwrap());
    let running_handler = Arc::clone(&handler);
    let task = tokio::spawn(async move {
        running_handler
            .handle_request(
                CancellationToken::new(),
                HttpTunnelRequest {
                    request_id: "req-abort-guard".to_owned(),
                    port,
                    path: "/".to_owned(),
                    ..Default::default()
                },
                Arc::new(RecordingSender::default()),
            )
            .await
    });
    timeout(Duration::from_secs(2), accepted.notified())
        .await
        .unwrap();
    task.abort();
    let _ = task.await;
    assert!(handler.active_requests.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn test_response_header_timeout_reports_protocol_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_raw_request_head(&mut stream).await.unwrap();
        std::future::pending::<()>().await;
    });
    // 生产门限仍固定为 30 秒；测试通过同一传输状态机的计时构造器缩短等待，避免
    // 仅验证固定错误文本就给每次 Linux ARM64 完整门禁增加 30 秒。
    let handler =
        new_handler_with_timing_for_test(Duration::from_millis(50), Duration::ZERO).unwrap();
    let sender = Arc::new(RecordingSender::default());
    timeout(
        Duration::from_secs(2),
        handler.handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "req-header-timeout".to_owned(),
                port,
                path: "/wait".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        sender.errors.lock().unwrap()[0].error_message,
        "request failed: response header timeout",
    );
    server.abort();
}
