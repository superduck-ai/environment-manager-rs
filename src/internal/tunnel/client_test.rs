//! 后端长连接与 ProtoJSON 分派测试。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::routing::{any, get};
use futures_util::StreamExt as _;
use secrecy::ExposeSecret as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_util::sync::CancellationToken;
use url::Url;

use super::actions::{ResponseSender, new_registry};
use super::client::{
    EnvironmentProxyConfig, PingWriteFailure, connect_websocket_with_proxy_config,
    ping_write_failure, proxy_for_websocket_target, read_proxy_response, read_tunnel_data_message,
    should_offer_socks5_auth, socks5_proxy_port, tunnel_websocket_config,
};
use super::{
    HttpResponseSender, HttpTunnelRequest, HttpTunnelResponseError, TunnelRequest, TunnelResponse,
    add_jitter, new_client, tunnel_request, tunnel_response, with_action_registry,
};

async fn start_server(router: Router) -> (i32, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (port, task)
}

async fn start_tunnel_server() -> (
    i32,
    mpsc::UnboundedReceiver<HeaderMap>,
    mpsc::UnboundedReceiver<WebSocket>,
    JoinHandle<()>,
) {
    let (header_tx, header_rx) = mpsc::unbounded_channel();
    let (socket_tx, socket_rx) = mpsc::unbounded_channel();
    let router = Router::new().route(
        "/anthropic.session_ingress.api.v1alpha.TunnelService/Tunnel",
        any(move |headers: HeaderMap, ws: WebSocketUpgrade| {
            let header_tx = header_tx.clone();
            let socket_tx = socket_tx.clone();
            async move {
                header_tx.send(headers).unwrap();
                ws.on_upgrade(move |socket| async move {
                    socket_tx.send(socket).unwrap();
                })
            }
        }),
    );
    let (port, task) = start_server(router).await;
    (port, header_rx, socket_rx, task)
}

/// 验证 ping 写失败遵循取消优先级并保留底层网络错误文本。
///
/// 网络错误文本不带 tungstenite 的 `IO error:` 前缀；如果写失败与取消并发发生，
/// 则固定返回 `context canceled`。
#[test]
fn ping_write_failure_prefers_cancellation_then_network_error() {
    let cancellation = CancellationToken::new();
    assert_eq!(
        ping_write_failure(&cancellation, PingWriteFailure::Timeout).to_string(),
        "write tunnel ping: i/o timeout"
    );
    assert_eq!(
        ping_write_failure(
            &cancellation,
            PingWriteFailure::WebSocket(tokio_tungstenite::tungstenite::Error::Io(
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "socket write failed",)
            ),),
        )
        .to_string(),
        "write tunnel ping: socket write failed"
    );

    cancellation.cancel();
    assert_eq!(
        ping_write_failure(
            &cancellation,
            PingWriteFailure::WebSocket(tokio_tungstenite::tungstenite::Error::Io(
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "socket write failed",)
            ),),
        )
        .to_string(),
        "context canceled"
    );
}

/// 启动一次性原始 WebSocket 握手服务器，精确控制响应 header。
///
/// Axum/tungstenite 服务端会自动规范化 `Connection`/`Upgrade` 并执行
/// 标准子协议协商，无法构造 WebSocket 与上游 tungstenite 分歧的
/// 响应；因此该 fixture 只解析 challenge key，其余字节原样写回。
async fn start_raw_handshake_server(
    connection: &'static str,
    upgrade: &'static str,
    subprotocol: Option<&'static str>,
) -> (i32, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut byte = [0_u8; 1];
        while !request.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
        }
        let request = String::from_utf8(request).unwrap();
        let key = request
            .lines()
            .find_map(|line| line.strip_prefix("Sec-WebSocket-Key: "))
            .expect("client handshake must include its challenge key");
        let accept = derive_accept_key(key.as_bytes());
        let protocol_header = subprotocol
            .map(|protocol| format!("Sec-WebSocket-Protocol: {protocol}\r\n"))
            .unwrap_or_default();
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nConnection: {connection}\r\nUpgrade: {upgrade}\r\nSec-WebSocket-Accept: {accept}\r\n{protocol_header}\r\n"
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        // 保持 TCP 短暂存活，避免握手成功与 fixture drop 竞态干扰断言。
        tokio::time::sleep(Duration::from_millis(100)).await;
    });
    (port, task)
}

#[tokio::test]
async fn test_new_client_configures_handlers_and_options() {
    let registry = Arc::new(new_registry());
    let client = new_client(
        "session-1",
        "wss://example.com",
        "token-1",
        vec![with_action_registry(Arc::clone(&registry))],
    )
    .unwrap();

    assert!(client.http_handler.is_some());
    assert!(client.ws_handler.is_some());
    assert!(Arc::ptr_eq(
        client.action_registry.as_ref().unwrap(),
        &registry
    ));
    assert_eq!(client.session_id, "session-1");
    assert_eq!(client.ws_url, "wss://example.com");
    assert_eq!(client.auth_token.expose_secret(), "token-1");
}

#[tokio::test]
async fn test_tunnel_endpoint_matches_connect_path() {
    let client = new_client(
        "session-1",
        "wss://example.com/base/",
        "token-1",
        Vec::new(),
    )
    .unwrap();
    assert_eq!(
        client.tunnel_endpoint(),
        "wss://example.com/base/anthropic.session_ingress.api.v1alpha.TunnelService/Tunnel"
    );
}

/// WebSocket 未设置 read limit，两个 tungstenite 默认上限都必须关闭。
#[test]
fn test_websocket_config_has_no_incoming_frame_or_message_limit() {
    let config = tunnel_websocket_config();
    assert_eq!(config.max_frame_size, None);
    assert_eq!(config.max_message_size, None);
}

/// 上游 Tungstenite 接受 RFC 常规握手响应，正常 Tunnel 不依赖私有补丁开关。
#[tokio::test]
async fn test_upstream_handshake_accepts_standard_response_headers() {
    let (port, server) = start_raw_handshake_server("Upgrade", "websocket", None).await;
    let request = format!("ws://127.0.0.1:{port}/standard")
        .into_client_request()
        .unwrap();

    let (_socket, response) =
        connect_websocket_with_proxy_config(request, &EnvironmentProxyConfig::default())
            .await
            .expect("standard response must complete the handshake");
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    server.await.unwrap();
}

/// 不再 vendoring 后，异常 token-list 响应由上游严格握手器拒绝。
#[tokio::test]
async fn test_upstream_handshake_rejects_nonstandard_token_list_response() {
    let (port, server) =
        start_raw_handshake_server("keep-alive, Upgrade", "h2c, WebSocket", None).await;
    let request = format!("ws://127.0.0.1:{port}/token-list")
        .into_client_request()
        .unwrap();

    connect_websocket_with_proxy_config(request, &EnvironmentProxyConfig::default())
        .await
        .expect_err("upstream handshake must reject nonstandard token-list response");
    server.await.unwrap();
}

/// 服务端必须从客户端报价中选择子协议，不能任意返回未声明值。
#[tokio::test]
async fn test_upstream_handshake_rejects_unoffered_server_subprotocol() {
    let (port, server) =
        start_raw_handshake_server("Upgrade", "websocket", Some("server-only")).await;
    let mut request = format!("ws://127.0.0.1:{port}/subprotocol")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "sec-websocket-protocol",
        http::HeaderValue::from_static("client-offered"),
    );

    connect_websocket_with_proxy_config(request, &EnvironmentProxyConfig::default())
        .await
        .expect_err("upstream handshake must reject an unoffered server subprotocol");
    server.await.unwrap();
}

/// ping/pong 只是同一次 WebSocket `ReadMessage` 内部的控制帧，不得重置 deadline。
#[tokio::test]
async fn test_tunnel_read_deadline_is_not_reset_by_control_frames() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let producer = tokio::spawn(async move {
        for index in 0..4 {
            tokio::time::sleep(Duration::from_millis(15)).await;
            let control = if index % 2 == 0 {
                TungsteniteMessage::Ping(Vec::new().into())
            } else {
                TungsteniteMessage::Pong(Vec::new().into())
            };
            tx.send(Ok(control)).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
        let _ = tx.send(Ok(TungsteniteMessage::Text("late".into())));
    });
    let mut stream = futures_util::stream::poll_fn(move |context| rx.poll_recv(context));
    let pong_count = AtomicUsize::new(0);
    let started = std::time::Instant::now();
    let error = read_tunnel_data_message(
        &CancellationToken::new(),
        &mut stream,
        Duration::from_millis(55),
        || {
            pong_count.fetch_add(1, Ordering::Relaxed);
        },
    )
    .await
    .expect_err("control frames must not extend the absolute read deadline");

    assert_eq!(error.to_string(), "read tunnel request: i/o timeout");
    assert!(pong_count.load(Ordering::Relaxed) >= 1);
    assert!(started.elapsed() < Duration::from_millis(150));
    producer.abort();
}

/// 收到应用数据后，下一次 `ReadMessage` 应获得一个全新的 deadline。
#[tokio::test]
async fn test_tunnel_read_deadline_restarts_after_application_message() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let producer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(25)).await;
        tx.send(Ok(TungsteniteMessage::Text("first".into())))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        tx.send(Ok(TungsteniteMessage::Binary(b"second".to_vec().into())))
            .unwrap();
    });
    let mut stream = futures_util::stream::poll_fn(move |context| rx.poll_recv(context));
    let cancellation = CancellationToken::new();

    let first = read_tunnel_data_message(
        &cancellation,
        &mut stream,
        Duration::from_millis(200),
        || {},
    )
    .await
    .unwrap();
    let second = read_tunnel_data_message(
        &cancellation,
        &mut stream,
        Duration::from_millis(200),
        || {},
    )
    .await
    .unwrap();

    assert_eq!(first, b"first");
    assert_eq!(second, b"second");
    producer.await.unwrap();
}

#[tokio::test]
async fn test_client_connect_and_response_sender_writes_tunnel_response() {
    let (port, mut header_rx, mut socket_rx, server) = start_tunnel_server().await;
    let client = new_client(
        "session-1",
        format!("ws://localhost:{port}"),
        "token-1",
        Vec::new(),
    )
    .unwrap();
    client.connect(&CancellationToken::new()).await.unwrap();

    let headers = timeout(Duration::from_secs(2), header_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(headers.get("authorization").unwrap(), "Bearer token-1");
    assert_eq!(headers.get("x-session-id").unwrap(), "session-1");
    let mut socket = timeout(Duration::from_secs(2), socket_rx.recv())
        .await
        .unwrap()
        .unwrap();

    let sender = HttpResponseSender {
        client: Arc::clone(&client),
        request_id: "req-1".to_owned(),
    };
    sender
        .send_error(HttpTunnelResponseError {
            request_id: String::new(),
            error_message: "boom".to_owned(),
        })
        .await
        .unwrap();

    let message = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Text(text) = message else {
        panic!("response was not text");
    };
    assert!(text.contains("\"httpError\""));
    assert!(text.contains("\"requestId\":\"req-1\""));
    assert!(text.contains("\"errorMessage\":\"boom\""));
    client.close().await.unwrap();
    let close = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Close(Some(frame)) = close else {
        panic!("client close must send an explicit normal close frame");
    };
    assert_eq!(frame.code, 1000);
    server.abort();
}

#[tokio::test]
async fn test_client_run_dispatches_proto_jsonhttp_request() {
    let target = Router::new().route(
        "/health",
        get(|| async {
            Response::builder()
                .status(StatusCode::OK)
                .header("x-test", "ok")
                .body(Body::from("ready"))
                .unwrap()
        }),
    );
    let (target_port, target_server) = start_server(target).await;
    let (tunnel_port, _header_rx, mut socket_rx, tunnel_server) = start_tunnel_server().await;
    let client = new_client(
        "session-proto",
        format!("ws://localhost:{tunnel_port}"),
        "",
        Vec::new(),
    )
    .unwrap();
    client.connect(&CancellationToken::new()).await.unwrap();
    let mut socket = timeout(Duration::from_secs(2), socket_rx.recv())
        .await
        .unwrap()
        .unwrap();

    let run_cancellation = CancellationToken::new();
    let running_client = Arc::clone(&client);
    let running_cancellation = run_cancellation.clone();
    let run_task = tokio::spawn(async move { running_client.run(&running_cancellation).await });
    let request = TunnelRequest {
        session_id: "session-proto".to_owned(),
        payload: Some(tunnel_request::Payload::HttpRequest(HttpTunnelRequest {
            request_id: "req-proto".to_owned(),
            port: target_port,
            method: "GET".to_owned(),
            path: "/health".to_owned(),
            ..Default::default()
        })),
    };
    let mut request_json = serde_json::to_value(request).unwrap();
    // 同时在顶层和嵌套层注入未知字段，验证协议的向前兼容行为。
    request_json["futureTopLevel"] = serde_json::json!(true);
    request_json["httpRequest"]["futureNested"] = serde_json::json!("ignored");
    socket
        .send(Message::Text(request_json.to_string().into()))
        .await
        .unwrap();

    let mut seen_headers = false;
    let mut seen_body = false;
    let mut seen_final = false;
    while !seen_headers || !seen_body || !seen_final {
        let message = timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Message::Text(text) = message else {
            continue;
        };
        let response: TunnelResponse = serde_json::from_str(&text).unwrap();
        match response.payload {
            Some(tunnel_response::Payload::HttpHeaders(headers)) => {
                seen_headers = headers.request_id == "req-proto" && headers.status_code == 200;
            }
            Some(tunnel_response::Payload::HttpChunk(chunk)) => {
                seen_body |= chunk.data == b"ready";
                seen_final |= chunk.is_final;
            }
            _ => {}
        }
    }

    run_cancellation.cancel();
    let result = timeout(Duration::from_secs(2), run_task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    target_server.abort();
    tunnel_server.abort();
}

#[test]
fn test_add_jitter_within_expected_window() {
    let base = Duration::from_millis(100);
    for _ in 0..100 {
        let jittered = add_jitter(base);
        assert!(jittered >= base);
        assert!(jittered < base + base / 2);
    }
    assert_eq!(add_jitter(Duration::from_nanos(1)), Duration::from_nanos(1));
}

/// 覆盖 NO_PROXY 常用的 loopback、域名、端口和 CIDR 规则。
#[test]
fn test_proxy_from_environment_selection_and_no_proxy_rules() {
    let config = EnvironmentProxyConfig {
        http_proxy: "proxy.example:8080".to_owned(),
        https_proxy: "http://secure-proxy.example:8443".to_owned(),
        no_proxy: "exact.example,.children.example,port.example:443,10.0.0.0/8,2001:db8::/32"
            .to_owned(),
        cgi: false,
    };

    let selected =
        proxy_for_websocket_target(&Url::parse("ws://public.example/socket").unwrap(), &config)
            .unwrap()
            .expect("public target uses HTTP_PROXY");
    assert_eq!(selected.as_str(), "http://proxy.example:8080/");
    assert!(
        proxy_for_websocket_target(&Url::parse("ws://localhost:3000/").unwrap(), &config)
            .unwrap()
            .is_none()
    );
    assert!(
        proxy_for_websocket_target(&Url::parse("ws://127.0.0.1:3000/").unwrap(), &config)
            .unwrap()
            .is_none()
    );
    assert!(
        proxy_for_websocket_target(&Url::parse("ws://exact.example/").unwrap(), &config)
            .unwrap()
            .is_none()
    );
    assert!(
        proxy_for_websocket_target(&Url::parse("ws://api.children.example/").unwrap(), &config,)
            .unwrap()
            .is_none()
    );
    assert!(
        proxy_for_websocket_target(&Url::parse("ws://children.example/").unwrap(), &config)
            .unwrap()
            .is_some(),
        "NO_PROXY 前导点不匹配域名本身"
    );
    assert!(
        proxy_for_websocket_target(&Url::parse("wss://port.example/").unwrap(), &config)
            .unwrap()
            .is_none()
    );
    assert!(
        proxy_for_websocket_target(&Url::parse("ws://port.example/").unwrap(), &config)
            .unwrap()
            .is_some()
    );
    assert!(
        proxy_for_websocket_target(&Url::parse("ws://10.2.3.4/").unwrap(), &config)
            .unwrap()
            .is_none()
    );
    assert!(
        proxy_for_websocket_target(&Url::parse("wss://[2001:db8::5]/").unwrap(), &config)
            .unwrap()
            .is_none()
    );

    let unicode_config = EnvironmentProxyConfig {
        http_proxy: "http://proxy.example:8080".to_owned(),
        no_proxy: "例子.测试".to_owned(),
        ..EnvironmentProxyConfig::default()
    };
    assert!(
        proxy_for_websocket_target(
            &Url::parse("ws://例子.测试/socket").unwrap(),
            &unicode_config
        )
        .unwrap()
        .is_none(),
        "Unicode NO_PROXY 必须与 URL parser 生成的 punycode host 匹配"
    );
}

/// 非 101 握手返回 Tungstenite 的原生 HTTP 错误。
#[tokio::test]
async fn test_websocket_bad_handshake_includes_status_and_client_context() {
    let router = Router::new().fallback(|| async { StatusCode::FORBIDDEN });
    let (port, server) = start_server(router).await;
    let request = format!("ws://127.0.0.1:{port}/rejected")
        .into_client_request()
        .unwrap();
    let error = connect_websocket_with_proxy_config(request, &EnvironmentProxyConfig::default())
        .await
        .expect_err("403 response must fail the websocket handshake");
    assert_eq!(error.to_string(), "HTTP error: 403 Forbidden");

    let client = new_client(
        "session-rejected",
        format!("ws://127.0.0.1:{port}"),
        "",
        Vec::new(),
    )
    .unwrap();
    let error = client
        .connect(&CancellationToken::new())
        .await
        .expect_err("Client::connect must add the tunnel call-site context");
    assert_eq!(
        error.to_string(),
        "connect tunnel websocket: HTTP error: 403 Forbidden"
    );
    server.abort();
}

/// SOCKS5 只在显式配置端口和有效凭据时启用相应握手字段。
#[test]
fn test_socks5_proxy_defaults_and_auth_offer_boundaries() {
    assert_eq!(
        socks5_proxy_port(&Url::parse("socks5://proxy.example").unwrap()),
        None,
        "未配置的代理端口不能被隐式补成 1080"
    );
    assert_eq!(
        socks5_proxy_port(&Url::parse("socks5://proxy.example:9080").unwrap()),
        Some(9080)
    );
    assert!(should_offer_socks5_auth(b"user", b""));
    assert!(!should_offer_socks5_auth(b"", b"pass"));
    assert!(!should_offer_socks5_auth(&[b'u'; 256], b"pass"));
    assert!(!should_offer_socks5_auth(b"user", &[b'p'; 256]));
}

/// SOCKS5 URL 未写端口时必须返回底层连接器的 missing-port 失败。
#[tokio::test]
async fn test_websocket_socks5_proxy_requires_explicit_port() {
    let config = EnvironmentProxyConfig {
        http_proxy: "socks5://proxy.example".to_owned(),
        ..EnvironmentProxyConfig::default()
    };
    let request = "ws://backend.example:8080/socket"
        .into_client_request()
        .unwrap();
    let error = connect_websocket_with_proxy_config(request, &config)
        .await
        .expect_err("SOCKS5 URL without a port must fail before DNS or TCP dialing");
    let tokio_tungstenite::tungstenite::Error::Io(error) = error else {
        panic!("expected an I/O error")
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("missing port"));
}

/// CGI 安全检查必须早于 loopback/NO_PROXY，防止 HTTP_PROXY header 注入。
#[test]
fn test_proxy_from_environment_rejects_http_proxy_in_cgi() {
    let config = EnvironmentProxyConfig {
        http_proxy: "http://proxy.example:8080".to_owned(),
        no_proxy: "*".to_owned(),
        cgi: true,
        ..EnvironmentProxyConfig::default()
    };
    let error = proxy_for_websocket_target(&Url::parse("ws://localhost:3000/").unwrap(), &config)
        .expect_err("CGI HTTP_PROXY must be rejected before NO_PROXY");
    assert!(error.to_string().contains("refusing to use HTTP_PROXY"));
}

/// 验证 HTTP proxy 收到 CONNECT/Basic auth，并保留 WebSocket 的非 200 reason 文本。
#[tokio::test]
async fn test_websocket_proxy_connect_and_non_200_reason() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let (request_tx, request_rx) = oneshot::channel();
    let proxy = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut byte = [0_u8; 1];
        while !request.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
        }
        request_tx
            .send(String::from_utf8(request).unwrap())
            .unwrap();
        stream
            .write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });
    let config = EnvironmentProxyConfig {
        http_proxy: format!("http://user:pass@127.0.0.1:{proxy_port}"),
        ..EnvironmentProxyConfig::default()
    };
    let request = "ws://backend.example:8080/socket"
        .into_client_request()
        .unwrap();
    let error = connect_websocket_with_proxy_config(request, &config)
        .await
        .expect_err("407 proxy response must fail the websocket dial");
    let tokio_tungstenite::tungstenite::Error::Io(error) = error else {
        panic!("expected an I/O error")
    };
    assert!(error.to_string().contains("Proxy Authentication Required"));

    let connect_request = request_rx.await.unwrap();
    assert!(
        connect_request
            .starts_with("CONNECT backend.example:8080 HTTP/1.1\r\nHost: backend.example:8080\r\n")
    );
    assert!(connect_request.contains("Proxy-Authorization: Basic dXNlcjpwYXNz\r\n"));
    proxy.await.unwrap();
}

#[tokio::test]
async fn test_proxy_response_does_not_consume_tunnel_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        stream
            .write_all(b"HTTP/1.1 200 Connection Established\r\nProxy-Agent: test\r\n\r\ntunnel")
            .await
            .unwrap();
    });

    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let (status_code, reason) = read_proxy_response(&mut stream).await.unwrap();
    assert_eq!(status_code, 200);
    assert_eq!(reason, "Connection Established");

    let mut trailing = [0_u8; 6];
    stream.read_exact(&mut trailing).await.unwrap();
    assert_eq!(&trailing, b"tunnel");
    server.await.unwrap();
}

#[tokio::test]
async fn test_proxy_response_split_across_segments() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // 先写不含终止空行的不完整 header，迫使客户端走 Partial 路径
        stream
            .write_all(b"HTTP/1.1 200 Connection Established\r\nProxy-Agent: test\r\n")
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        // 再补完剩余 header + 终止空行 + 隧道字节
        stream
            .write_all(b"X-Custom: v\r\n\r\ntunnel")
            .await
            .unwrap();
    });

    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let (status_code, reason) = read_proxy_response(&mut stream).await.unwrap();
    assert_eq!(status_code, 200);
    assert_eq!(reason, "Connection Established");

    let mut trailing = [0_u8; 6];
    stream.read_exact(&mut trailing).await.unwrap();
    assert_eq!(&trailing, b"tunnel");
    server.await.unwrap();
}

/// 启动一次性 HTTP proxy，返回 Rust 发送的完整 CONNECT header。
async fn capture_http_proxy_connect(user_info: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let proxy = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut byte = [0_u8; 1];
        while !request.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
        }
        stream
            .write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        String::from_utf8(request).unwrap()
    });
    let config = EnvironmentProxyConfig {
        http_proxy: format!("http://{user_info}127.0.0.1:{proxy_port}"),
        ..EnvironmentProxyConfig::default()
    };
    let request = "ws://backend.example:8080/socket"
        .into_client_request()
        .unwrap();
    connect_websocket_with_proxy_config(request, &config)
        .await
        .expect_err("407 proxy response must fail the websocket dial");
    proxy.await.unwrap()
}

/// WebSocket 只在 URL 显式带 password 时生成 HTTP Proxy Basic header。
#[tokio::test]
async fn test_http_proxy_basic_auth_requires_explicit_password() {
    let without_password = capture_http_proxy_connect("user@").await;
    assert!(!without_password.contains("Proxy-Authorization:"));

    let encoded_colon_in_username = capture_http_proxy_connect("user%3Aname@").await;
    assert!(
        !encoded_colon_in_username.contains("Proxy-Authorization:"),
        "percent-encoded colon 属于用户名，不能被误判为显式密码"
    );

    let explicit_empty_password = capture_http_proxy_connect("user:@").await;
    assert!(
        explicit_empty_password.contains("Proxy-Authorization: Basic dXNlcjo=\r\n"),
        "显式空密码仍应发送 Basic 凭据"
    );
}

/// 验证内建 SOCKS5 分支的 RFC 1928/1929 协商与远程域名请求。
#[tokio::test]
async fn test_websocket_proxy_supports_socks5_with_authentication() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let proxy = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut greeting = [0_u8; 4];
        stream.read_exact(&mut greeting).await.unwrap();
        assert_eq!(greeting, [0x05, 0x02, 0x00, 0x02]);
        stream.write_all(&[0x05, 0x02]).await.unwrap();

        let mut auth_header = [0_u8; 2];
        stream.read_exact(&mut auth_header).await.unwrap();
        assert_eq!(auth_header, [0x01, 4]);
        let mut username = [0_u8; 4];
        stream.read_exact(&mut username).await.unwrap();
        assert_eq!(&username, b"user");
        let mut password_len = [0_u8; 1];
        stream.read_exact(&mut password_len).await.unwrap();
        assert_eq!(password_len, [4]);
        let mut password = [0_u8; 4];
        stream.read_exact(&mut password).await.unwrap();
        assert_eq!(&password, b"pass");
        stream.write_all(&[0x01, 0x00]).await.unwrap();

        let mut connect_prefix = [0_u8; 5];
        stream.read_exact(&mut connect_prefix).await.unwrap();
        assert_eq!(&connect_prefix[..4], &[0x05, 0x01, 0x00, 0x03]);
        let domain_len = usize::from(connect_prefix[4]);
        let mut domain_and_port = vec![0_u8; domain_len + 2];
        stream.read_exact(&mut domain_and_port).await.unwrap();
        assert_eq!(&domain_and_port[..domain_len], b"backend.example");
        assert_eq!(&domain_and_port[domain_len..], &8080_u16.to_be_bytes());
        // 返回 connection refused，让测试在 SOCKS 层结束，不需要伪造
        // 后续 WebSocket 服务端。
        stream
            .write_all(&[0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();
    });
    let config = EnvironmentProxyConfig {
        http_proxy: format!("socks5://user:pass@127.0.0.1:{proxy_port}"),
        ..EnvironmentProxyConfig::default()
    };
    let request = "ws://backend.example:8080/socket"
        .into_client_request()
        .unwrap();
    let error = connect_websocket_with_proxy_config(request, &config)
        .await
        .expect_err("SOCKS5 connection-refused reply must fail the websocket dial");
    let tokio_tungstenite::tungstenite::Error::Io(error) = error else {
        panic!("expected an I/O error")
    };
    assert!(error.to_string().contains("connection refused"));
    proxy.await.unwrap();
}

/// SOCKS5 成功 reply 的地址与端口必须分开读取并保留端口 EOF 错误类别。
#[tokio::test]
async fn test_websocket_socks5_proxy_reports_truncated_reply_port() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let proxy = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut greeting = [0_u8; 3];
        stream.read_exact(&mut greeting).await.unwrap();
        assert_eq!(greeting, [0x05, 0x01, 0x00]);
        stream.write_all(&[0x05, 0x00]).await.unwrap();

        let mut connect_prefix = [0_u8; 5];
        stream.read_exact(&mut connect_prefix).await.unwrap();
        assert_eq!(&connect_prefix[..4], &[0x05, 0x01, 0x00, 0x03]);
        let domain_len = usize::from(connect_prefix[4]);
        let mut domain_and_port = vec![0_u8; domain_len + 2];
        stream.read_exact(&mut domain_and_port).await.unwrap();

        // 只发送成功 header 和完整 IPv4 bind address，故意省略两字节端口。
        // 如果实现把 address/port 合并读取，错误会被误报为 address 截断。
        stream
            .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1])
            .await
            .unwrap();
    });
    let config = EnvironmentProxyConfig {
        http_proxy: format!("socks5://127.0.0.1:{proxy_port}"),
        ..EnvironmentProxyConfig::default()
    };
    let request = "ws://backend.example:8080/socket"
        .into_client_request()
        .unwrap();
    let error = connect_websocket_with_proxy_config(request, &config)
        .await
        .expect_err("truncated SOCKS5 bind port must fail");
    let tokio_tungstenite::tungstenite::Error::Io(error) = error else {
        panic!("expected an I/O error")
    };
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    proxy.await.unwrap();
}

/// WebSocket 未为 `https` proxy URL 注册 dialer，Rust 不能将其降级成明文。
#[tokio::test]
async fn test_websocket_proxy_rejects_unsupported_proxy_scheme() {
    let config = EnvironmentProxyConfig {
        https_proxy: "https://proxy.example:8443".to_owned(),
        ..EnvironmentProxyConfig::default()
    };
    let request = "wss://backend.example/socket"
        .into_client_request()
        .unwrap();
    let error = connect_websocket_with_proxy_config(request, &config)
        .await
        .expect_err("unsupported proxy scheme must not be downgraded");
    let tokio_tungstenite::tungstenite::Error::Io(error) = error else {
        panic!("expected an I/O error")
    };
    assert!(error.to_string().contains("unknown scheme: https"));
}
