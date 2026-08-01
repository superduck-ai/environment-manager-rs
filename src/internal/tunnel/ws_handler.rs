//! 本地 WebSocket Tunnel 处理器。
//!
//! 对应 Go `internal/tunnel/ws_handler.go`。本模块负责校验后端请求、连接 loopback
//! WebSocket、双向转发 text/binary/close 帧，并在同一 tunnel ID 被重开时关闭旧连接。
//! Rust 将连接拆成读写半部以替代 Gorilla WebSocket 的显式写 mutex，线上的消息顺序不变。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use http::header::SEC_WEBSOCKET_PROTOCOL;
use serde_json::Value;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::error::Error as WebSocketError;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message};
use tokio_util::sync::CancellationToken;

use super::client::{connect_websocket_with_config, gorilla_websocket_config};
use super::helpers::ws_logger;
use crate::internal::logger::{LogAttributes, Logger};

use super::{
    TunnelError, WsHandler, WsResponseSender, WsTunnel, WsTunnelClose, WsTunnelError,
    WsTunnelMessage, WsTunnelOpen, WsTunnelOpened, apply_websocket_tunnel_headers,
};

/// 对应 Go WebSocket dialer 的 `HandshakeTimeout`。
const WEBSOCKET_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// 对应 Gorilla 每次 `WriteMessage` 前设置的十秒 write deadline。
const WEBSOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// 对应 Go `WSHandler` 的默认构造过程。
///
/// `logger` 为空时不产生结构化日志；连接管理和协议行为不受影响。
#[must_use]
pub fn new_ws_handler(logger: Option<Logger>) -> WsHandler {
    WsHandler {
        logger,
        tunnels: Arc::new(tokio::sync::RwLock::new(BTreeMap::new())),
    }
}

impl WsHandler {
    /// 对应 Go：`(*WSHandler).OpenTunnel`。
    ///
    /// 连接 `ws://localhost:<port><path>`，透传 header 和子协议，成功后注册连接并
    /// 发送 `WSTunnelOpened`。端口、路径或握手错误通过 sender 返回；sender 自身失败
    /// 则作为 `TunnelError` 交给上层，使主 Tunnel 可以按 Go 语义记录并重连。
    pub async fn open_tunnel(
        &self,
        cancellation: CancellationToken,
        request: WsTunnelOpen,
        sender: Arc<dyn WsResponseSender>,
    ) -> Result<(), TunnelError> {
        if request.port <= 0 || request.port > 65_535 {
            return send_ws_error(
                sender.as_ref(),
                &request.tunnel_id,
                &format!("invalid port: {}", request.port),
            )
            .await;
        }
        if !request.path.starts_with('/') {
            return send_ws_error(
                sender.as_ref(),
                &request.tunnel_id,
                "path must start with /",
            )
            .await;
        }

        let target_url = format!("ws://localhost:{}{}", request.port, request.path);
        let mut outbound = match target_url.into_client_request() {
            Ok(outbound) => outbound,
            Err(error) => {
                send_ws_error(sender.as_ref(), &request.tunnel_id, &error.to_string()).await?;
                return Ok(());
            }
        };
        if let Err(error) = apply_websocket_tunnel_headers(
            outbound.headers_mut(),
            &request.headers,
            !request.protocols.is_empty(),
        ) {
            send_ws_error(sender.as_ref(), &request.tunnel_id, &error.to_string()).await?;
            return Ok(());
        }
        if !request.protocols.is_empty() {
            let protocols = request.protocols.join(", ");
            let value = match http::HeaderValue::from_str(&protocols) {
                Ok(value) => value,
                Err(error) => {
                    send_ws_error(sender.as_ref(), &request.tunnel_id, &error.to_string()).await?;
                    return Ok(());
                }
            };
            outbound.headers_mut().insert(SEC_WEBSOCKET_PROTOCOL, value);
        }

        // 本地连接使用上游 Tungstenite 的标准协议状态机。项目不再向依赖内部
        // 注入 write-poison；任何写失败都会在应用层立即取消并移除整条 tunnel。
        let websocket_config = gorilla_websocket_config();
        let handshake = tokio::select! {
            () = cancellation.cancelled() => return Err(TunnelError::message("context canceled")),
            handshake = tokio::time::timeout(
                WEBSOCKET_HANDSHAKE_TIMEOUT,
                // 本地 `localhost` 会按 ProxyFromEnvironment 特例直连。仍经过
                // 共享 dial helper，使得未来非 loopback 的兼容调用不会绕过代理规则。
                connect_websocket_with_config(outbound, websocket_config),
            ) => handshake,
        };
        let (socket, response) = match handshake {
            Ok(Ok(connected)) => connected,
            Ok(Err(error)) => {
                // Go `OpenTunnel` 在握手 response 存在时将状态放在错误前；错误本体
                // 仍是 Gorilla 固定的 `websocket: bad handshake`，不能泄漏 tungstenite
                // 的 `HTTP error` 或 `IO error` 前缀。
                let message = match error.response_status() {
                    Some(status) => format!("{status}: {error}"),
                    None => error.to_string(),
                };
                send_ws_error(sender.as_ref(), &request.tunnel_id, &message).await?;
                return Ok(());
            }
            Err(_) => {
                send_ws_error(
                    sender.as_ref(),
                    &request.tunnel_id,
                    "websocket: handshake timeout",
                )
                .await?;
                return Ok(());
            }
        };

        let selected_protocol = response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let (sink, stream) = socket.split();
        let tunnel = Arc::new(WsTunnel {
            tunnel_id: request.tunnel_id.clone(),
            sink: tokio::sync::Mutex::new(sink),
            sender: Arc::clone(&sender),
            cancellation: cancellation.child_token(),
        });

        let old_tunnel = self
            .tunnels
            .write()
            .await
            .insert(request.tunnel_id.clone(), Arc::clone(&tunnel));
        if let Some(old_tunnel) = old_tunnel {
            Self::close_tunnel_instance(&self.tunnels, old_tunnel).await;
        }

        if let Err(error) = sender
            .send_opened(WsTunnelOpened {
                tunnel_id: request.tunnel_id.clone(),
                protocol: selected_protocol.clone(),
            })
            .await
        {
            Self::close_tunnel_instance(&self.tunnels, Arc::clone(&tunnel)).await;
            return Err(TunnelError::message(format!(
                "send websocket open response: {error}"
            )));
        }

        self.log_opened(&request, &selected_protocol);
        let tunnels = Arc::clone(&self.tunnels);
        let logger = self.logger.clone();
        tokio::spawn(async move {
            WsHandler::read_loop(tunnels, logger, tunnel, stream).await;
        });
        Ok(())
    }

    /// 对应 Go：`(*WSHandler).SendMessage`。
    ///
    /// 根据 `is_binary` 选择 text/binary frame，并在单连接写锁内完整发送。未知 ID
    /// 返回 `tunnel not found`；取消令牌已触发时返回 `context canceled`。
    pub async fn send_message(
        &self,
        cancellation: &CancellationToken,
        message: WsTunnelMessage,
    ) -> Result<(), TunnelError> {
        self.send_message_with_timeout(cancellation, message, WEBSOCKET_WRITE_TIMEOUT)
            .await
    }

    /// 使用显式 deadline 向本地 WebSocket 发送一条消息。
    ///
    /// 对应 Go `(*WSHandler).SendMessage` 的写 deadline。生产调用固定为
    /// 10 秒；参数化仅用于在测试中不真实等待十秒。首次写入超时或底层失败后，
    /// 连接的写状态已经不可判断，因而立即从 registry 移除并取消读循环。后续消息
    /// 会稳定得到 `tunnel not found`，不会尝试复用可能残留半帧的 socket。
    pub(super) async fn send_message_with_timeout(
        &self,
        cancellation: &CancellationToken,
        message: WsTunnelMessage,
        write_timeout: Duration,
    ) -> Result<(), TunnelError> {
        if cancellation.is_cancelled() {
            return Err(TunnelError::message("context canceled"));
        }
        let tunnel = self
            .lookup_tunnel(&message.tunnel_id)
            .await
            .ok_or_else(|| {
                TunnelError::message(format!("tunnel not found: {}", message.tunnel_id))
            })?;
        let outbound = if message.is_binary {
            Message::Binary(message.data.into())
        } else {
            // 后端协议用 bytes 表示 WS payload，但 RFC 6455 要求 text frame 是 UTF-8。
            // 上游 Tungstenite 的 `Message::Text` 只接受字符串，因此在接触 socket 前
            // 拒绝错误标记为 text 的任意二进制数据，调用方应改用 `is_binary=true`。
            let text = String::from_utf8(message.data).map_err(|error| {
                TunnelError::message(format!("write websocket message: invalid UTF-8: {error}"))
            })?;
            Message::Text(text.into())
        };
        let mut sink = tunnel.sink.lock().await;
        let error = match tokio::time::timeout(write_timeout, sink.send(outbound)).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => local_websocket_write_error_text(&error),
            Err(_) => "i/o timeout".to_owned(),
        };

        // timeout 会先丢弃未完成的 send future；随后必须释放写锁再取消读半部，
        // 否则清理任务可能等待同一 split socket，形成不必要的锁顺序依赖。
        drop(sink);
        Self::close_tunnel_instance(&self.tunnels, Arc::clone(&tunnel)).await;
        Err(TunnelError::message(format!(
            "write websocket message: {error}"
        )))
    }

    /// 对应 Go：`(*WSHandler).CloseTunnel`。
    ///
    /// code 为零时按 RFC 6455 使用 1000；1005 时发送无 payload close。连接会先从
    /// registry 删除，使并发下行消息稳定得到 `tunnel not found`，再关闭写半部。
    pub async fn close_tunnel(
        &self,
        cancellation: &CancellationToken,
        message: WsTunnelClose,
    ) -> Result<(), TunnelError> {
        if cancellation.is_cancelled() {
            return Err(TunnelError::message("context canceled"));
        }
        let code = if message.code == 0 {
            1000
        } else {
            message.code
        };
        self.close_tunnel_with_code(&message.tunnel_id, code, &message.reason)
            .await
    }

    /// 对应 Go：`(*WSHandler).CloseAll`，关闭调用时已登记的全部本地连接。
    pub async fn close_all(&self) {
        let tunnel_ids = self
            .tunnels
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for tunnel_id in tunnel_ids {
            let _ = self.close_tunnel_with_code(&tunnel_id, 1000, "").await;
        }
    }

    /// 对应 Go 测试辅助：`(*WSHandler).lookupTunnel`。
    pub(crate) async fn lookup_tunnel(&self, tunnel_id: &str) -> Option<Arc<WsTunnel>> {
        self.tunnels.read().await.get(tunnel_id).cloned()
    }

    /// 从 registry 取出连接并发送指定 close frame。
    async fn close_tunnel_with_code(
        &self,
        tunnel_id: &str,
        code: i32,
        reason: &str,
    ) -> Result<(), TunnelError> {
        let tunnel = self
            .tunnels
            .write()
            .await
            .remove(tunnel_id)
            .ok_or_else(|| TunnelError::message(format!("tunnel not found: {tunnel_id}")))?;
        tunnel.cancellation.cancel();

        let close_message = websocket_close_message(code, reason);
        let mut sink = tunnel.sink.lock().await;
        // Go 的 deadline 覆盖 close frame 写入；把 send 与底层 shutdown 置于
        // 同一十秒窗口，避免恶意或停滞的本地 peer 占住主 Tunnel 分派任务。
        let (write_result, close_result) = tokio::time::timeout(WEBSOCKET_WRITE_TIMEOUT, async {
            let write_result = sink.send(close_message).await;
            let close_result = sink.close().await;
            (write_result, close_result)
        })
        .await
        .map_err(|_| TunnelError::message("close websocket tunnel: i/o timeout"))?;
        match (write_result, close_result) {
            (Err(write_error), Err(close_error)) => Err(TunnelError::message(format!(
                "close websocket tunnel: {write_error}; {close_error}"
            ))),
            (Err(error), _) | (_, Err(error)) => Err(TunnelError::message(format!(
                "close websocket tunnel: {error}"
            ))),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    /// 记录本地 WebSocket 成功建立事件，字段名与 Go slog 调用保持一致。
    fn log_opened(&self, request: &WsTunnelOpen, protocol: &str) {
        let logger = ws_logger(self);
        logger.info(
            "ws_tunnel_opened",
            log_attributes([
                ("path", Value::String(request.path.clone())),
                ("port", Value::from(request.port)),
                ("protocol", Value::String(protocol.to_owned())),
                ("tunnel_id", Value::String(request.tunnel_id.clone())),
            ]),
        );
    }
    /// 持续读取本地 WebSocket，把 text/binary/close/error 消息发送给后端。
    async fn read_loop(
        tunnels: Arc<tokio::sync::RwLock<BTreeMap<String, Arc<WsTunnel>>>>,
        logger: Option<Logger>,
        tunnel: Arc<WsTunnel>,
        mut stream: futures_util::stream::SplitStream<super::LocalWebSocket>,
    ) {
        loop {
            let next = tokio::select! {
                // 写失败会触发 cancellation；若此时本地 socket 也可读，必须先退出，
                // 避免继续驱动一个已经判定不可复用的 WebSocket 状态机。
                biased;
                () = tunnel.cancellation.cancelled() => break,
                next = stream.next() => next,
            };
            let Some(next) = next else {
                break;
            };
            match next {
                Ok(Message::Text(text)) => {
                    if let Err(error) = tunnel
                        .sender
                        .send_message(WsTunnelMessage {
                            tunnel_id: tunnel.tunnel_id.clone(),
                            data: text.as_bytes().to_vec(),
                            is_binary: false,
                        })
                        .await
                    {
                        log_send_failure(logger.as_ref(), &tunnel.tunnel_id, &error.to_string());
                        break;
                    }
                }
                Ok(Message::Binary(data)) => {
                    if let Err(error) = tunnel
                        .sender
                        .send_message(WsTunnelMessage {
                            tunnel_id: tunnel.tunnel_id.clone(),
                            data: data.to_vec(),
                            is_binary: true,
                        })
                        .await
                    {
                        log_send_failure(logger.as_ref(), &tunnel.tunnel_id, &error.to_string());
                        break;
                    }
                }
                Ok(Message::Close(frame)) => {
                    let (code, reason) = frame.map_or((1005, String::new()), |frame| {
                        (u16::from(frame.code) as i32, frame.reason.to_string())
                    });
                    let _ = tunnel
                        .sender
                        .send_close(WsTunnelClose {
                            tunnel_id: tunnel.tunnel_id.clone(),
                            code,
                            reason,
                        })
                        .await;
                    break;
                }
                Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => {}
                Err(error) => {
                    if !tunnel.cancellation.is_cancelled() {
                        if is_gorilla_abnormal_eof(&error) {
                            // Gorilla 把未经 close handshake 的 EOF 包装成
                            // `CloseError{1006, "unexpected EOF"}`，上层因此必须收到
                            // WsClose，不能改成普通 WsError。
                            let _ = tunnel
                                .sender
                                .send_close(WsTunnelClose {
                                    tunnel_id: tunnel.tunnel_id.clone(),
                                    code: 1006,
                                    reason: "unexpected EOF".to_owned(),
                                })
                                .await;
                        } else {
                            let _ = tunnel
                                .sender
                                .send_error(WsTunnelError {
                                    tunnel_id: tunnel.tunnel_id.clone(),
                                    error_message: local_websocket_error_text(&error),
                                })
                                .await;
                        }
                    }
                    break;
                }
            }
        }
        Self::close_tunnel_instance(&tunnels, tunnel).await;
    }

    /// 仅当 registry 仍指向给定实例时移除它，并取消读任务。
    ///
    /// Go 的 `closeTunnelDone` 通过 recover 保护重复关闭 done channel；Rust 把同一
    /// 生命周期信号并入可重复调用的 [`CancellationToken::cancel`]。Go 这条
    /// 清理路径调用的是底层 `Conn.Close`，不发 WebSocket close frame；
    /// Rust 因此也不调用 `SinkExt::close`，而是让取消的读半部和本实例
    /// 相继 drop，直接关闭共享底层 socket。这一路径无网络 await，
    /// 所以 replacement/cancel 不会被不响应的 peer 无限阻塞。
    async fn close_tunnel_instance(
        tunnels: &tokio::sync::RwLock<BTreeMap<String, Arc<WsTunnel>>>,
        tunnel: Arc<WsTunnel>,
    ) {
        let mut registered = tunnels.write().await;
        if registered
            .get(&tunnel.tunnel_id)
            .is_some_and(|current| Arc::ptr_eq(current, &tunnel))
        {
            registered.remove(&tunnel.tunnel_id);
        }
        drop(registered);
        tunnel.cancellation.cancel();
    }
}

/// 按 Gorilla `FormatCloseMessage` 的 integer cast 语义构造 close message。
///
/// 1005 发送空 payload；其他任意 `i32` 先按 Go `uint16(code)`
/// 规则取低 16 位，包括负数和大于 65535 的值。Tungstenite
/// 对出站 `Message::Close` 只编码 payload，不执行入站 close-code
/// 合法性校验，因此可保留 Gorilla 的边界字节。
pub(super) fn websocket_close_message(code: i32, reason: &str) -> Message {
    if code == 1005 {
        Message::Close(None)
    } else {
        Message::Close(Some(CloseFrame {
            code: CloseCode::from(code as u16),
            reason: reason.to_owned().into(),
        }))
    }
}

/// 判断 tungstenite 读错是否对应 Gorilla 的异常 EOF close。
///
/// 底层 TCP 在没有 WebSocket close handshake 时返回 EOF，tungstenite 将其
/// 表示为 `ResetWithoutClosingHandshake`；某些 stream wrapper 会直接保留
/// `UnexpectedEof`。两者在 Gorilla 都是代码 1006、文本 `unexpected EOF`。
fn is_gorilla_abnormal_eof(error: &WebSocketError) -> bool {
    matches!(
        error,
        WebSocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake)
    ) || matches!(
        error,
        WebSocketError::Io(error) if error.kind() == std::io::ErrorKind::UnexpectedEof
    )
}

/// 把本地 WebSocket 底层写错转换为 Gorilla 会保存的首错文本。
///
/// tungstenite 的 `Io` display 会额外添加 `IO error:`，而 Gorilla
/// `writeFatal` 保存的是底层 `net.Conn.Write` 错误。这里只移除库外壳；
/// 协议错误仍保留其原始文本，并由调用点统一添加
/// `write websocket message:` 或 `close websocket tunnel:`。
fn local_websocket_write_error_text(error: &WebSocketError) -> String {
    match error {
        WebSocketError::Io(error) => error.to_string(),
        error => error.to_string(),
    }
}

/// 把本地 WebSocket 读错转成稳定的协议错误文本。
///
/// `Io` 去掉 tungstenite 特有的 `IO error:` 外壳，与 Gorilla 透传
/// `net.Error` 一致。上游 Tungstenite 会拒绝非法 UTF-8 text 和无效 close reason；
/// `Utf8` 分支把依赖内部错误转换为稳定、明确的 Tunnel 错误文本。
fn local_websocket_error_text(error: &WebSocketError) -> String {
    match error {
        WebSocketError::Io(error) => error.to_string(),
        WebSocketError::Utf8(detail) => {
            format!("websocket: invalid UTF-8 text message: {detail}")
        }
        error => error.to_string(),
    }
}

/// 对应 Go：`sendWSError`，发送协议错误并包装 sender 失败。
async fn send_ws_error(
    sender: &dyn WsResponseSender,
    tunnel_id: &str,
    message: &str,
) -> Result<(), TunnelError> {
    sender
        .send_error(WsTunnelError {
            tunnel_id: tunnel_id.to_owned(),
            error_message: message.to_owned(),
        })
        .await
        .map_err(|error| TunnelError::message(format!("send websocket error: {error}")))
}

/// 构造确定顺序的结构化日志属性。
fn log_attributes<const N: usize>(pairs: [(&str, Value); N]) -> LogAttributes {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

/// 记录本地消息向后端发送失败；失败本身会终止该 tunnel 的读循环。
fn log_send_failure(logger: Option<&Logger>, tunnel_id: &str, error: &str) {
    let Some(logger) = logger else {
        return;
    };
    logger.error(
        "ws_tunnel_send_message_failed",
        log_attributes([
            ("error", Value::String(error.to_owned())),
            ("tunnel_id", Value::String(tunnel_id.to_owned())),
        ]),
    );
}
