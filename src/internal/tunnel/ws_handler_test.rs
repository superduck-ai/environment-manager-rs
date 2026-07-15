//! `internal/tunnel/ws_handler.go` 对应的本地 WebSocket 转发测试。

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::Router;
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::http::HeaderMap;
use axum::routing::any;
use futures_util::{SinkExt as _, StreamExt as _};
use tokio::io::AsyncWriteExt as _;
use tokio::net::TcpListener;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;
use tokio_util::sync::CancellationToken;

use super::ws_handler::websocket_close_message;
use super::{
    Header, WsResponseSender, WsSenderError, WsTunnelClose, WsTunnelError, WsTunnelMessage,
    WsTunnelOpen, WsTunnelOpened, apply_websocket_tunnel_headers, new_ws_handler,
};

/// Gorilla 必须在联网前拒绝调用方重复声明由握手器负责生成的保留头。
#[test]
fn test_websocket_headers_reject_gorilla_reserved_duplicates() {
    let cases = [
        ("Upgrade", "Upgrade"),
        ("connection", "Connection"),
        ("Sec-WebSocket-Key", "Sec-Websocket-Key"),
        ("sec-websocket-version", "Sec-Websocket-Version"),
        ("Sec-WebSocket-Extensions", "Sec-Websocket-Extensions"),
    ];
    for (header_name, canonical_name) in cases {
        let error = apply_websocket_tunnel_headers(
            &mut HeaderMap::new(),
            &[Header {
                name: header_name.to_owned(),
                value: "duplicate".to_owned(),
            }],
            false,
        )
        .expect_err("Gorilla reserved header must fail before dialing");
        assert_eq!(
            error.to_string(),
            format!("websocket: duplicate header not allowed: {canonical_name}")
        );
    }
}

/// `Sec-Websocket-Protocol` 仅在 dialer 已配置 Subprotocols 时属于重复头。
#[test]
fn test_websocket_protocol_header_conflicts_only_with_configured_protocols() {
    let header = Header {
        name: "Sec-WebSocket-Protocol".to_owned(),
        value: "from-header".to_owned(),
    };
    let mut allowed = HeaderMap::new();
    apply_websocket_tunnel_headers(&mut allowed, std::slice::from_ref(&header), false)
        .expect("header-supplied subprotocol is allowed when dialer has none");
    assert_eq!(
        allowed
            .get("sec-websocket-protocol")
            .and_then(|value| value.to_str().ok()),
        Some("from-header")
    );

    let error = apply_websocket_tunnel_headers(&mut HeaderMap::new(), &[header], true)
        .expect_err("configured Subprotocols must conflict with request header");
    assert_eq!(
        error.to_string(),
        "websocket: duplicate header not allowed: Sec-Websocket-Protocol"
    );
}

#[derive(Default)]
struct RecordingWsSender {
    opened: Mutex<Vec<WsTunnelOpened>>,
    messages: Mutex<Vec<WsTunnelMessage>>,
    closes: Mutex<Vec<WsTunnelClose>>,
    errors: Mutex<Vec<WsTunnelError>>,
    message_ready: Notify,
    event_ready: Notify,
}

impl RecordingWsSender {
    /// 对应 Go：`recordingWSSender.waitMessage`。
    ///
    /// 等待 reader task 投递首条消息；先检查缓冲再注册通知，循环可避免消息恰好
    /// 落在检查与 `notified()` 之间时丢失唤醒。
    async fn wait_message(&self) -> WsTunnelMessage {
        timeout(Duration::from_secs(2), async {
            loop {
                let notified = self.message_ready.notified();
                if let Some(message) = self.messages.lock().unwrap().first().cloned() {
                    return message;
                }
                notified.await;
            }
        })
        .await
        .expect("等待 websocket tunnel message 超时")
    }

    /// 等待本地连接上报首个 WebSocket close 事件。
    async fn wait_close(&self) -> WsTunnelClose {
        timeout(Duration::from_secs(2), async {
            loop {
                let notified = self.event_ready.notified();
                if let Some(message) = self.closes.lock().unwrap().first().cloned() {
                    return message;
                }
                notified.await;
            }
        })
        .await
        .expect("等待 websocket tunnel close 超时")
    }
}

#[async_trait]
impl WsResponseSender for RecordingWsSender {
    async fn send_opened(&self, response: WsTunnelOpened) -> Result<(), WsSenderError> {
        self.opened.lock().unwrap().push(response);
        Ok(())
    }

    async fn send_message(&self, message: WsTunnelMessage) -> Result<(), WsSenderError> {
        self.messages.lock().unwrap().push(message);
        self.message_ready.notify_one();
        Ok(())
    }

    async fn send_close(&self, message: WsTunnelClose) -> Result<(), WsSenderError> {
        self.closes.lock().unwrap().push(message);
        self.event_ready.notify_waiters();
        Ok(())
    }

    async fn send_error(&self, response: WsTunnelError) -> Result<(), WsSenderError> {
        self.errors.lock().unwrap().push(response);
        self.event_ready.notify_waiters();
        Ok(())
    }
}

async fn start_server(router: Router) -> (i32, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (port, task)
}

#[tokio::test]
async fn test_open_tunnel_rejects_invalid_port() {
    let handler = new_ws_handler(None);
    let sender = Arc::new(RecordingWsSender::default());
    handler
        .open_tunnel(
            CancellationToken::new(),
            WsTunnelOpen {
                tunnel_id: "ws-invalid-port".to_owned(),
                port: 0,
                path: "/ws".to_owned(),
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

/// 本地服务器拒绝升级时，错误必须复刻 Go `resp.Status + ErrBadHandshake`。
#[tokio::test]
async fn test_open_tunnel_formats_bad_handshake_like_gorilla() {
    let router = Router::new().fallback(|| async { axum::http::StatusCode::FORBIDDEN });
    let (port, server) = start_server(router).await;
    let handler = new_ws_handler(None);
    let sender = Arc::new(RecordingWsSender::default());

    handler
        .open_tunnel(
            CancellationToken::new(),
            WsTunnelOpen {
                tunnel_id: "ws-rejected".to_owned(),
                port,
                path: "/reject".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await
        .unwrap();

    let errors = sender.errors.lock().unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].error_message,
        "403 Forbidden: websocket: bad handshake"
    );
    server.abort();
}

/// 本地 peer 未发 close frame 就 EOF 时，Gorilla 上报 1006 close 而非普通错误。
#[tokio::test]
async fn test_local_websocket_abnormal_eof_is_reported_as_close_1006() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        // 绕过 WebSocket close handshake 直接 drop TCP，复制本地进程崩溃/退出。
        drop(socket.into_inner());
    });
    let handler = new_ws_handler(None);
    let sender = Arc::new(RecordingWsSender::default());

    handler
        .open_tunnel(
            CancellationToken::new(),
            WsTunnelOpen {
                tunnel_id: "ws-abnormal-eof".to_owned(),
                port,
                path: "/ws".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await
        .unwrap();

    let close = sender.wait_close().await;
    assert_eq!(close.tunnel_id, "ws-abnormal-eof");
    assert_eq!(close.code, 1006);
    assert_eq!(close.reason, "unexpected EOF");
    assert!(sender.errors.lock().unwrap().is_empty());
    peer.await.unwrap();
}

/// Gorilla 不校验 text UTF-8，非法字节也必须作为非 binary 消息原样转发。
#[tokio::test]
async fn test_local_websocket_invalid_utf8_text_forwards_raw_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let (release_tx, release_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let mut raw = socket.into_inner();
        // FIN + text，payload 只有一个非法 UTF-8 字节。服务端帧不需 mask。
        raw.write_all(&[0x81, 0x01, 0xff]).await.unwrap();
        // 保持 TCP 存活到断言完成；否则 EOF 的 1006 close 与消息转发并发，
        // 测试会把合法的后续 close 错当成 UTF-8 行为失败。
        let _ = release_rx.await;
        raw.shutdown().await.unwrap();
    });
    let handler = new_ws_handler(None);
    let sender = Arc::new(RecordingWsSender::default());
    let cancellation = CancellationToken::new();

    handler
        .open_tunnel(
            cancellation.clone(),
            WsTunnelOpen {
                tunnel_id: "ws-invalid-utf8".to_owned(),
                port,
                path: "/ws".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await
        .unwrap();

    let message = sender.wait_message().await;
    assert_eq!(message.tunnel_id, "ws-invalid-utf8");
    assert_eq!(message.data, [0xff]);
    assert!(!message.is_binary);
    assert!(sender.closes.lock().unwrap().is_empty());
    assert!(sender.errors.lock().unwrap().is_empty());
    cancellation.cancel();
    release_tx.send(()).unwrap();
    peer.await.unwrap();
}

/// 分片 text 跨帧组合后即使 UTF-8 非法，也必须保留完整原始 payload。
#[tokio::test]
async fn test_local_websocket_fragmented_invalid_utf8_forwards_raw_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let mut raw = socket.into_inner();
        // 首帧是未完结 text 0xc3，终结 continuation 的 0x28 不能
        // 构成 UTF-8；Gorilla 仍返回两字节组合后的 text message。
        raw.write_all(&[0x01, 0x01, 0xc3, 0x80, 0x01, 0x28])
            .await
            .unwrap();
        raw.shutdown().await.unwrap();
    });
    let handler = new_ws_handler(None);
    let sender = Arc::new(RecordingWsSender::default());

    handler
        .open_tunnel(
            CancellationToken::new(),
            WsTunnelOpen {
                tunnel_id: "ws-fragmented-invalid-utf8".to_owned(),
                port,
                path: "/ws".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await
        .unwrap();

    let message = sender.wait_message().await;
    assert_eq!(message.tunnel_id, "ws-fragmented-invalid-utf8");
    assert_eq!(message.data, [0xc3, 0x28]);
    assert!(!message.is_binary);
    assert!(sender.errors.lock().unwrap().is_empty());
    peer.await.unwrap();
}

/// 同 ID 重开必须直接丢弃旧 TCP，不等 close handshake，也不能删掉新连接。
#[tokio::test]
async fn test_replacing_local_websocket_does_not_wait_or_send_close_frame() {
    let old_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let old_port = i32::from(old_listener.local_addr().unwrap().port());
    let (old_result_tx, old_result_rx) = oneshot::channel();
    let old_peer = tokio::spawn(async move {
        let (stream, _) = old_listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let result = timeout(Duration::from_secs(2), socket.next()).await;
        let saw_close = matches!(result, Ok(Some(Ok(TungsteniteMessage::Close(_)))));
        old_result_tx.send(saw_close).unwrap();
    });

    let new_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let new_port = i32::from(new_listener.local_addr().unwrap().port());
    let (new_message_tx, new_message_rx) = oneshot::channel();
    let new_peer = tokio::spawn(async move {
        let (stream, _) = new_listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let message = timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        new_message_tx.send(message).unwrap();
    });

    let handler = new_ws_handler(None);
    handler
        .open_tunnel(
            CancellationToken::new(),
            WsTunnelOpen {
                tunnel_id: "ws-replace".to_owned(),
                port: old_port,
                path: "/ws".to_owned(),
                ..Default::default()
            },
            Arc::new(RecordingWsSender::default()),
        )
        .await
        .unwrap();
    timeout(
        Duration::from_secs(1),
        handler.open_tunnel(
            CancellationToken::new(),
            WsTunnelOpen {
                tunnel_id: "ws-replace".to_owned(),
                port: new_port,
                path: "/ws".to_owned(),
                ..Default::default()
            },
            Arc::new(RecordingWsSender::default()),
        ),
    )
    .await
    .expect("replacement must not wait for the old peer")
    .unwrap();

    handler
        .send_message(
            &CancellationToken::new(),
            WsTunnelMessage {
                tunnel_id: "ws-replace".to_owned(),
                data: b"new".to_vec(),
                is_binary: true,
            },
        )
        .await
        .unwrap();
    let new_message = timeout(Duration::from_secs(2), new_message_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(new_message, TungsteniteMessage::Binary(data) if data.as_ref() == b"new"));
    assert!(
        !timeout(Duration::from_secs(2), old_result_rx)
            .await
            .unwrap()
            .unwrap(),
        "replacement cleanup must not send a WebSocket close frame"
    );
    old_peer.await.unwrap();
    new_peer.await.unwrap();
}

/// 写 deadline 后保留 Gorilla 首错和 registry，但绝不能迟到刷出缓存帧。
#[tokio::test]
async fn test_timed_out_local_websocket_preserves_first_error_without_late_flush() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let (ready_tx, ready_rx) = oneshot::channel();
    let (after_timeout_tx, after_timeout_rx) = oneshot::channel();
    let (message_tx, message_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
            .max_frame_size(None)
            .max_message_size(None);
        let mut socket = tokio_tungstenite::accept_async_with_config(stream, Some(config))
            .await
            .unwrap();
        ready_tx.send(()).unwrap();
        // 在 client deadline 内不读，确保 32 MiB frame 只写出前缀；测试
        // 明确通知超时后，再发 ping 触发 tungstenite 的自动回复路径。
        after_timeout_rx.await.unwrap();
        socket
            .send(TungsteniteMessage::Ping(b"must-not-flush".to_vec().into()))
            .await
            .unwrap();
        socket
            .send(TungsteniteMessage::Binary(
                b"still-readable".to_vec().into(),
            ))
            .await
            .unwrap();
        let received_complete_data = matches!(
            timeout(Duration::from_millis(500), socket.next()).await,
            Ok(Some(Ok(
                TungsteniteMessage::Text(_) | TungsteniteMessage::Binary(_)
            )))
        );
        message_tx.send(received_complete_data).unwrap();
        let _ = finish_rx.await;
    });

    let handler = new_ws_handler(None);
    let sender = Arc::new(RecordingWsSender::default());
    handler
        .open_tunnel(
            CancellationToken::new(),
            WsTunnelOpen {
                tunnel_id: "ws-write-timeout".to_owned(),
                port,
                path: "/ws".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(2), ready_rx)
        .await
        .unwrap()
        .unwrap();

    let error = handler
        .send_message_with_timeout(
            &CancellationToken::new(),
            WsTunnelMessage {
                tunnel_id: "ws-write-timeout".to_owned(),
                data: vec![b'x'; 32 * 1024 * 1024],
                is_binary: true,
            },
            Duration::from_millis(5),
        )
        .await
        .expect_err("a peer that does not read must hit the test write deadline");
    assert_eq!(error.to_string(), "write websocket message: i/o timeout");
    let registered = handler.lookup_tunnel("ws-write-timeout").await.unwrap();
    let marker = Arc::downgrade(&registered.write_poisoned);
    drop(registered);

    let later_write = handler
        .send_message(
            &CancellationToken::new(),
            WsTunnelMessage {
                tunnel_id: "ws-write-timeout".to_owned(),
                data: b"must-not-flush-old-frame".to_vec(),
                is_binary: true,
            },
        )
        .await
        .expect_err("a timed-out connection must reject every later write");
    assert_eq!(
        later_write.to_string(),
        "write websocket message: i/o timeout"
    );
    after_timeout_tx.send(()).unwrap();

    // Go 的读循环不会因 writeErr 停止；即使 pong 被禁止写出，后续本地
    // application message 仍必须能回传后端。
    let inbound = sender.wait_message().await;
    assert_eq!(inbound.tunnel_id, "ws-write-timeout");
    assert_eq!(inbound.data, b"still-readable");
    assert!(inbound.is_binary);
    assert!(
        !timeout(Duration::from_secs(2), message_rx)
            .await
            .unwrap()
            .unwrap(),
        "the timed-out frame must never arrive as a complete future message"
    );

    let close_error = handler
        .close_tunnel(
            &CancellationToken::new(),
            WsTunnelClose {
                tunnel_id: "ws-write-timeout".to_owned(),
                code: 1000,
                reason: String::new(),
            },
        )
        .await
        .expect_err("Gorilla close must return the persistent first write error");
    assert_eq!(
        close_error.to_string(),
        "close websocket tunnel: i/o timeout"
    );
    assert!(handler.lookup_tunnel("ws-write-timeout").await.is_none());
    finish_tx.send(()).unwrap();
    peer.await.unwrap();
    timeout(Duration::from_secs(2), async {
        while marker.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("write-poison marker must not form a connection reference cycle");
}

#[tokio::test]
async fn test_open_tunnel_send_message_and_close() {
    let (header_tx, mut header_rx) = mpsc::unbounded_channel();
    let (message_tx, message_rx) = oneshot::channel();
    let message_tx = Arc::new(Mutex::new(Some(message_tx)));
    let (close_tx, close_rx) = oneshot::channel();
    let close_tx = Arc::new(Mutex::new(Some(close_tx)));
    let router = Router::new().route(
        "/ws",
        any(move |headers: HeaderMap, ws: WebSocketUpgrade| {
            let header_tx = header_tx.clone();
            let message_tx = Arc::clone(&message_tx);
            let close_tx = Arc::clone(&close_tx);
            async move {
                header_tx.send(headers).unwrap();
                ws.protocols(["proto1"])
                    .on_upgrade(move |socket| async move {
                        let (mut sink, mut stream) = socket.split();
                        if let Some(Ok(message)) = stream.next().await {
                            let data = match message {
                                Message::Text(text) => text.as_bytes().to_vec(),
                                Message::Binary(data) => data.to_vec(),
                                other => panic!("unexpected client message: {other:?}"),
                            };
                            if let Some(tx) = message_tx.lock().unwrap().take() {
                                tx.send(data).unwrap();
                            }
                        }
                        sink.send(Message::Binary(b"reply".to_vec().into()))
                            .await
                            .unwrap();
                        while let Some(message) = stream.next().await {
                            if matches!(message, Ok(Message::Close(_))) {
                                break;
                            }
                        }
                        if let Some(tx) = close_tx.lock().unwrap().take() {
                            tx.send(()).unwrap();
                        }
                    })
            }
        }),
    );
    let (port, server) = start_server(router).await;

    let handler = new_ws_handler(None);
    let sender = Arc::new(RecordingWsSender::default());
    handler
        .open_tunnel(
            CancellationToken::new(),
            WsTunnelOpen {
                tunnel_id: "ws-1".to_owned(),
                port,
                path: "/ws".to_owned(),
                headers: vec![
                    Header {
                        name: "X-Tunnel".to_owned(),
                        value: "yes".to_owned(),
                    },
                    Header {
                        name: "Host".to_owned(),
                        value: "first.example".to_owned(),
                    },
                    Header {
                        name: "host".to_owned(),
                        value: "ignored.example".to_owned(),
                    },
                ],
                protocols: vec!["proto1".to_owned()],
            },
            sender.clone(),
        )
        .await
        .unwrap();

    let headers = timeout(Duration::from_secs(2), header_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(headers.get("x-tunnel").unwrap(), "yes");
    assert_eq!(headers.get("host").unwrap(), "first.example");
    {
        let opened = sender.opened.lock().unwrap();
        assert_eq!(opened.len(), 1);
        assert_eq!(opened[0].protocol, "proto1");
    }

    handler
        .send_message(
            &CancellationToken::new(),
            WsTunnelMessage {
                tunnel_id: "ws-1".to_owned(),
                data: b"hello".to_vec(),
                is_binary: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), message_rx)
            .await
            .unwrap()
            .unwrap(),
        b"hello"
    );

    let message = sender.wait_message().await;
    assert!(message.is_binary);
    assert_eq!(message.data, b"reply");

    handler
        .close_tunnel(
            &CancellationToken::new(),
            WsTunnelClose {
                tunnel_id: "ws-1".to_owned(),
                code: 1000,
                reason: String::new(),
            },
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(2), close_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(handler.lookup_tunnel("ws-1").await.is_none());
    server.abort();
}

/// Go 把越界 close code 直接 cast 为 uint16，Rust 不得新增校验错误。
#[test]
fn test_websocket_close_message_wraps_code_like_go() {
    for (input, expected) in [(-1, u16::MAX), (65_536, 0), (1000, 1000)] {
        let TungsteniteMessage::Close(Some(frame)) = websocket_close_message(input, "reason")
        else {
            panic!("code {input} must produce a close payload");
        };
        assert_eq!(u16::from(frame.code), expected);
        assert_eq!(frame.reason, "reason");
    }
    assert!(matches!(
        websocket_close_message(1005, "ignored"),
        TungsteniteMessage::Close(None)
    ));
}
