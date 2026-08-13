//! 本地 HTTP 请求 Tunnel 处理器。
//!
//! 请求只允许 loopback 端口和绝对 path；响应 header 先发送，body 流式回传，
//! 最后发送空的 final chunk。这里使用只支持 loopback 明文 HTTP 的 Hyper 客户端，
//! 避免代理环境变量和 URL 规范化改变 Tunnel 中的 request-target。

use std::collections::BTreeMap;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::TryStreamExt as _;
use http::header::{CONNECTION, CONTENT_LENGTH, HOST, TRAILER, TRANSFER_ENCODING};
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, Version};
use http_body_util::{BodyExt as _, Full};
use hyper::ext::ReasonPhrase;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_util::io::StreamReader;
use tokio_util::sync::CancellationToken;
use xxhash_rust::xxh64::Xxh64;

use crate::internal::util::duration_nanoseconds;

use super::{
    Handler, Header, HttpTunnelRequest, HttpTunnelResponseChunk, HttpTunnelResponseError,
    HttpTunnelResponseHeaders, ResponseSender, TunnelError, flatten_http_headers,
};

/// 对应 Go `httpTunnelChunkSize`：每个响应数据分片最多 32 KiB。
pub const HTTP_TUNNEL_CHUNK_SIZE: usize = 32 * 1024;

/// 对应 Go `http.Transport.ResponseHeaderTimeout`：本地服务必须在 30 秒内返回 header。
const RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(30);

/// 已注册活动请求的作用域守卫。
///
/// Go 使用 `defer` 保证请求无论正常返回、错误返回还是 goroutine 展开都取消 context
/// 并删除 map 项。Rust future 可能被 `JoinHandle::abort` 直接丢弃，因此清理必须放在
/// `Drop` 中，不能依赖 `await` 后面的普通语句。
struct ActiveRequestGuard<'a> {
    handler: &'a Handler,
    request_id: Option<String>,
    cancellation: CancellationToken,
}

impl Drop for ActiveRequestGuard<'_> {
    /// 对应 Go `HandleRequest` 中的延迟清理闭包。
    ///
    /// 先取消本请求拥有的令牌，再删除 request ID。删除操作不跨 `await`，且 poisoned
    /// mutex 仍取回内部值，以免一次 panic 永久泄漏后续请求状态。
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(request_id) = self.request_id.as_deref() {
            self.handler
                .active_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(request_id);
        }
    }
}

/// 对应 Go `NewClient` 内部的默认 HTTP Handler 构造。
///
/// Hyper 客户端不实现自动 redirect，也不会读取 `HTTP_PROXY`/`NO_PROXY`，因此和 Go
/// 中专用于本地端口的显式 `http.Transport` 一样始终直连 loopback。请求本身通过
/// `CancellationToken` 管理，只对响应 header 设置 30 秒门限，不截断长响应 body。
///
/// # 返回
/// 返回初始化完成的处理器。当前 Hyper HTTP connector 的构造不会失败。
pub fn new_handler() -> Result<Handler, TunnelError> {
    Ok(Handler {
        response_header_timeout: RESPONSE_HEADER_TIMEOUT,
        connect_delay: Duration::ZERO,
        active_requests: std::sync::Mutex::new(BTreeMap::new()),
    })
}

/// 构造带可控计时参数的 HTTP Tunnel 处理器，仅供单元测试使用。
///
/// Go 的 `ResponseHeaderTimeout` 从完整请求写入成功后才开始。生产值固定为 30 秒，
/// 无法在测试中快速覆盖“慢连接”和“慢上传不消耗 header timeout”。这个构造器只
/// 缩短测试时间并在拨号前注入确定延迟，传输状态机与生产构造完全相同。
///
/// # 参数
/// - `response_header_timeout`：请求写完后等待首个响应 header 的时限。
/// - `connect_delay`：拨号前延迟；它明确位于 header timeout 计时范围之外。
///
/// # 返回
/// 返回使用指定计时参数的处理器；该构造过程不访问网络且不会失败。
#[cfg(test)]
pub(crate) fn new_handler_with_timing_for_test(
    response_header_timeout: Duration,
    connect_delay: Duration,
) -> Result<Handler, TunnelError> {
    Ok(Handler {
        response_header_timeout,
        connect_delay,
        active_requests: std::sync::Mutex::new(BTreeMap::new()),
    })
}

/// 观察并改写 Hyper HTTP/1 请求行的 loopback TCP 流。
///
/// Hyper 的 `http::Uri` 会拒绝 Go `RawQuery` 允许写出的部分可打印字节。该适配器让
/// Hyper 使用安全占位 URI 构造请求，再仅替换首行的 request-target；响应读取仍完全
/// 交给 Hyper。它还在字节真正被内核 socket 接收后通知计时状态机，不能把“Hyper 已
/// 接受 body buffer”误当成 Go `Request.Write` 已完成。
struct RequestTargetIo {
    stream: TcpStream,
    wire_target: Vec<u8>,
    body_length: usize,
    request_head: Vec<u8>,
    pending_write: Vec<u8>,
    pending_offset: usize,
    expected_wire_bytes: Option<usize>,
    written_wire_bytes: usize,
    request_written: Option<oneshot::Sender<()>>,
}

impl RequestTargetIo {
    /// 创建请求写入观察器。
    ///
    /// # 参数
    /// - `stream`：已经直连 localhost 的 TCP 流。
    /// - `wire_target`：需要写入 HTTP 请求行的 origin-form target。
    /// - `body_length`：固定 `Full<Bytes>` 请求体长度，用于识别完整写入时点。
    /// - `request_written`：完整请求实际写入 socket 后触发的一次性通知。
    fn new(
        stream: TcpStream,
        wire_target: String,
        body_length: usize,
        request_written: oneshot::Sender<()>,
    ) -> Self {
        Self {
            stream,
            wire_target: wire_target.into_bytes(),
            body_length,
            request_head: Vec::new(),
            pending_write: Vec::new(),
            pending_offset: 0,
            expected_wire_bytes: None,
            written_wire_bytes: 0,
            request_written: Some(request_written),
        }
    }

    /// 在请求 header 收齐后替换 request-target 并建立完整线字节计数。
    ///
    /// # 返回
    /// header 尚未收齐时返回 `Ok(false)`；完成改写并把累计字节移入待写缓冲时返回
    /// `Ok(true)`；Hyper 生成不完整首行时返回 IO 错误。
    fn prepare_request_head(&mut self) -> io::Result<bool> {
        let Some(header_end) = find_bytes(&self.request_head, b"\r\n\r\n") else {
            return Ok(false);
        };
        let header_end = header_end + 4;
        let Some(first_space) = self.request_head.iter().position(|byte| *byte == b' ') else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Hyper 生成的 HTTP 请求行缺少 method 分隔符",
            ));
        };
        let Some(second_space_relative) = self.request_head[first_space + 1..]
            .iter()
            .position(|byte| *byte == b' ')
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Hyper 生成的 HTTP 请求行缺少 version 分隔符",
            ));
        };
        let second_space = first_space + 1 + second_space_relative;

        let mut rewritten = Vec::with_capacity(
            self.request_head.len() + self.wire_target.len().saturating_sub(second_space),
        );
        rewritten.extend_from_slice(&self.request_head[..first_space + 1]);
        rewritten.extend_from_slice(&self.wire_target);
        rewritten.extend_from_slice(&self.request_head[second_space..]);

        let rewritten_header_length =
            header_end - (second_space - first_space - 1) + self.wire_target.len();
        self.expected_wire_bytes = Some(rewritten_header_length + self.body_length);
        self.pending_write = rewritten;
        self.pending_offset = 0;
        self.request_head.clear();
        Ok(true)
    }

    /// 尽量把内部待写字节排入 socket，并在完整请求写完时发出通知。
    fn poll_drain_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.pending_offset < self.pending_write.len() {
            let write = Pin::new(&mut self.stream)
                .poll_write(cx, &self.pending_write[self.pending_offset..]);
            let written = match write {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole HTTP request",
                    )));
                }
                Poll::Ready(Ok(written)) => written,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            };
            self.pending_offset += written;
            self.written_wire_bytes += written;
            self.notify_request_written_if_complete();
        }
        self.pending_write.clear();
        self.pending_offset = 0;
        Poll::Ready(Ok(()))
    }

    /// 当 header 与固定长度 body 均被 socket 接受时只通知一次。
    fn notify_request_written_if_complete(&mut self) {
        if self
            .expected_wire_bytes
            .is_some_and(|expected| self.written_wire_bytes >= expected)
            && let Some(request_written) = self.request_written.take()
        {
            let _ = request_written.send(());
        }
    }
}

impl AsyncRead for RequestTargetIo {
    /// 把响应字节直接交给 Hyper。
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}

impl AsyncWrite for RequestTargetIo {
    /// 接收 Hyper 输出；header 完成前先累计，完成后以 Go request-target 改写并写线。
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.expected_wire_bytes.is_none() {
            self.request_head.extend_from_slice(buffer);
            let accepted = buffer.len();
            self.prepare_request_head()?;
            return Poll::Ready(Ok(accepted));
        }

        match self.poll_drain_pending(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
        self.pending_write.extend_from_slice(buffer);
        let accepted = buffer.len();
        // 与普通 TcpStream 一样，poll_write 只需确认当前 buffer 已被传输层接受；实际
        // socket 完成点由下一次 poll/flush 更新，header timer 不会在此过早启动。
        match self.poll_drain_pending(cx) {
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) | Poll::Pending => Poll::Ready(Ok(accepted)),
        }
    }

    /// 确保所有已接受的请求字节真正排入 socket 后再完成 flush。
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_drain_pending(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut self.stream).poll_flush(cx),
            other => other,
        }
    }

    /// 排空请求后关闭写半部；取消与错误路径仍由连接任务 Drop 完成资源释放。
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_drain_pending(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut self.stream).poll_shutdown(cx),
            other => other,
        }
    }
}

/// 在字节切片中查找第一个完整子串。
///
/// # 参数
/// - `haystack`：Hyper 已交付的请求字节。
/// - `needle`：固定 HTTP header 终止符。
///
/// # 返回
/// 命中时返回起始下标；未收齐时返回 `None`。
fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

impl Handler {
    /// 对应 Go：`(*Handler).HandleRequest`。
    ///
    /// 校验目标后把请求发送到 `http://localhost:<port><path>`，注册 request ID 供
    /// cancel 消息查找。网络失败通过协议 error 回传并视为已处理；sender 写失败则
    /// 返回错误，让上层关闭或重连 Tunnel。
    ///
    /// # 参数
    /// - `cancellation`：父 Tunnel 生命周期令牌；取消后中止等待 header 或读取 body。
    /// - `request`：后端下发的 HTTP 请求，body 所有权在本函数内消费。
    /// - `sender`：将 header、数据块和协议错误写回主 Tunnel 的并发安全发送器。
    ///
    /// # 返回
    /// 本地网络错误已通过 `sender` 报告时返回 `Ok(())`；请求构造错误、响应 body
    /// 读取错误或 sender 写失败返回 `TunnelError`。
    ///
    /// # 副作用
    /// 在活动请求表登记非空 request ID、访问 loopback 端口并发送 Tunnel 消息。
    pub async fn handle_request(
        &self,
        cancellation: CancellationToken,
        request: HttpTunnelRequest,
        sender: Arc<dyn ResponseSender>,
    ) -> Result<(), TunnelError> {
        if request.port <= 0 || request.port > 65_535 {
            return send_http_error(
                sender.as_ref(),
                &request.request_id,
                &format!("invalid port: {}", request.port),
            )
            .await;
        }
        if !request.path.starts_with('/') {
            return send_http_error(
                sender.as_ref(),
                &request.request_id,
                "path must start with /",
            )
            .await;
        }

        let request_cancellation = cancellation.child_token();
        if !request.request_id.is_empty() {
            self.active_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(request.request_id.clone(), request_cancellation.clone());
        }

        let _active_request_guard = ActiveRequestGuard {
            handler: self,
            request_id: (!request.request_id.is_empty()).then(|| request.request_id.clone()),
            cancellation: request_cancellation.clone(),
        };

        self.forward_request(&request_cancellation, &request, sender.as_ref())
            .await
    }

    /// 对应 Go `HandleRequest` 中创建 `http.Request` 后的转发主体。
    ///
    /// 请求构造严格保持 Go 的阶段边界：非法 method/URL 属于
    /// `failed to create request` 并向调用方返回原始错误；非法 header 和网络错误属于
    /// `request failed`，写回协议错误后视为已经处理。响应 header 成功发出后，每次
    /// `AsyncRead::read` 的 `n > 0` 数据立即成为一个 Tunnel chunk。
    ///
    /// # 参数
    /// - `cancellation`：本请求专属的取消令牌。
    /// - `request`：已通过 port/path 外层校验的协议请求。
    /// - `sender`：响应协议发送器，借用只覆盖当前请求。
    ///
    /// # 返回
    /// 完整读取并发送 final chunk 时返回成功；构造、body 读取或 sender 写入失败时
    /// 返回错误。连接/响应 header 错误写回后按 Go 行为返回成功。
    async fn forward_request(
        &self,
        cancellation: &CancellationToken,
        request: &HttpTunnelRequest,
        sender: &dyn ResponseSender,
    ) -> Result<(), TunnelError> {
        let method = match parse_http_method(&request.method) {
            Ok(method) => method,
            Err(error) => {
                send_http_error(
                    sender,
                    &request.request_id,
                    &format!("failed to create request: {error}"),
                )
                .await?;
                return Err(TunnelError::message(error));
            }
        };
        let target = match build_http_target(request.port, &request.path) {
            Ok(target) => target,
            Err(error) => {
                send_http_error(
                    sender,
                    &request.request_id,
                    &format!("failed to create request: {error}"),
                )
                .await?;
                return Err(TunnelError::message(error));
            }
        };
        let mut headers = match build_http_headers(&request.headers) {
            Ok(headers) => headers,
            Err(error) => {
                send_http_error(
                    sender,
                    &request.request_id,
                    &format!("request failed: {error}"),
                )
                .await?;
                return Ok(());
            }
        };
        headers.insert(
            HOST,
            HeaderValue::from_bytes(format!("localhost:{}", request.port).as_bytes())
                .expect("validated port must form a Host header"),
        );
        let mut outbound_request = http::Request::builder()
            .method(method.clone())
            // Hyper 只看到可解析的占位 URI；RequestTargetIo 在实际写线前替换
            // request-target。authority 仍保留真实端口，用于生成 Host。
            .uri(target.transport_uri.clone())
            .body(Full::new(Bytes::copy_from_slice(&request.body)))
            .map_err(|error| TunnelError::message(format!("build tunnel request: {error}")))?;
        *outbound_request.headers_mut() = headers;

        let start = Instant::now();
        if !self.connect_delay.is_zero() {
            tokio::select! {
                () = cancellation.cancelled() => {
                    send_http_error(
                        sender,
                        &request.request_id,
                        "request canceled",
                    ).await?;
                    return Ok(());
                }
                () = tokio::time::sleep(self.connect_delay) => {}
            }
        }

        // header timer 只在请求写入 socket 后开始。目标主机固定为 localhost，
        // 不启用 proxy、TLS 或连接池。
        let stream = tokio::select! {
            () = cancellation.cancelled() => {
                send_http_error(
                    sender,
                    &request.request_id,
                    "request canceled",
                ).await?;
                return Ok(());
            }
            result = TcpStream::connect(("localhost", request.port as u16)) => {
                match result {
                    Ok(stream) => stream,
                    Err(error) => {
                        send_http_error(
                            sender,
                            &request.request_id,
                            &format!("request failed: connect to localhost:{}: {error}", request.port),
                        ).await?;
                        return Ok(());
                    }
                }
            }
        };
        let (request_written_sender, mut request_written_receiver) = oneshot::channel();
        let io = RequestTargetIo::new(
            stream,
            target.wire_target.clone(),
            request.body.len(),
            request_written_sender,
        );
        let handshake = tokio::select! {
            () = cancellation.cancelled() => {
                send_http_error(
                    sender,
                    &request.request_id,
                    "request canceled",
                ).await?;
                return Ok(());
            }
            result = hyper::client::conn::http1::handshake(TokioIo::new(io)) => result,
        };
        let (mut request_sender, connection) = match handshake {
            Ok(parts) => parts,
            Err(error) => {
                send_http_error(
                    sender,
                    &request.request_id,
                    &format!("request failed: {error}"),
                )
                .await?;
                return Ok(());
            }
        };
        let connection_task = tokio::spawn(async move { connection.with_upgrades().await });
        let mut response_future = Box::pin(request_sender.send_request(outbound_request));

        // 响应可能在完整请求写入前到达；只有“写成功”通知先到时才启动
        // response-header timeout。
        let response = tokio::select! {
            () = cancellation.cancelled() => {
                connection_task.abort();
                send_http_error(
                    sender,
                    &request.request_id,
                    "request canceled",
                ).await?;
                return Ok(());
            }
            response = &mut response_future => response,
            request_written = &mut request_written_receiver => {
                if request_written.is_err() {
                    // IO 驱动退出时 sender 被丢弃；真正原因由 response future 的 Hyper
                    // 错误携带，不能把 oneshot closed 暴露给后端。
                    tokio::select! {
                        () = cancellation.cancelled() => {
                            connection_task.abort();
                            send_http_error(
                                sender,
                                &request.request_id,
                                "request canceled",
                            ).await?;
                            return Ok(());
                        }
                        response = &mut response_future => response,
                    }
                } else {
                    tokio::select! {
                        () = cancellation.cancelled() => {
                            connection_task.abort();
                            send_http_error(
                                sender,
                                &request.request_id,
                                "request canceled",
                            ).await?;
                            return Ok(());
                        }
                        response = &mut response_future => response,
                        () = tokio::time::sleep(self.response_header_timeout) => {
                            connection_task.abort();
                            send_http_error(
                                sender,
                                &request.request_id,
                                "request failed: response header timeout",
                            ).await?;
                            return Ok(());
                        }
                    }
                }
            }
        };
        let mut response = match response {
            Ok(response) => response,
            Err(error) => {
                connection_task.abort();
                send_http_error(
                    sender,
                    &request.request_id,
                    &format!("request failed: {error}"),
                )
                .await?;
                return Ok(());
            }
        };

        let status_code = i32::from(response.status().as_u16());
        let status_text = response_status_text(&response);
        normalize_response_headers(&mut response, &method);
        let on_upgrade = (response.status() == StatusCode::SWITCHING_PROTOCOLS)
            .then(|| hyper::upgrade::on(&mut response));
        sender
            .send_headers(HttpTunnelResponseHeaders {
                request_id: request.request_id.clone(),
                status_code,
                status_text,
                headers: flatten_http_headers(response.headers()),
                sequence_number: 0,
            })
            .await
            .map_err(|error| TunnelError::message(format!("send response headers: {error}")))?;

        if let Some(on_upgrade) = on_upgrade {
            // Hyper 把 101 升级后的连接放在 OnUpgrade 中；header 发出后继续复用
            // 同一个 32 KiB read loop 回传字节。
            let upgraded = tokio::select! {
                () = cancellation.cancelled() => {
                    return self
                        .send_body_read_error(sender, &request.request_id, "request canceled")
                        .await;
                },
                upgraded = on_upgrade => upgraded.map_err(io::Error::other),
            };
            let mut reader = match upgraded {
                Ok(upgraded) => TokioIo::new(upgraded),
                Err(error) => {
                    return self
                        .send_body_read_error(sender, &request.request_id, &error.to_string())
                        .await;
                }
            };
            self.stream_response_body(
                cancellation,
                request,
                sender,
                status_code,
                start,
                &mut reader,
            )
            .await
        } else {
            let stream = response
                .into_body()
                .into_data_stream()
                .map_err(io::Error::other);
            let mut reader = StreamReader::new(stream);
            self.stream_response_body(
                cancellation,
                request,
                sender,
                status_code,
                start,
                &mut reader,
            )
            .await
        }
    }

    /// 流式读取响应 body 并发送 Tunnel chunk。
    ///
    /// `reader` 既可以是普通 Hyper body 的 `StreamReader`，也可以是 101 upgrade 后的
    /// 原始连接。每次 read 返回的 `n > 0` 都立即发送，即使随后一次 read 报错，已经
    /// 发送的数据也不会被缓冲或丢弃。
    ///
    /// # 参数
    /// - `cancellation`：取消读取与发送的请求令牌。
    /// - `request`：提供 request ID 与日志字段的原请求。
    /// - `sender`：Tunnel 响应发送器。
    /// - `status_code`：已发送的 HTTP 状态码。
    /// - `start`：请求开始单调时间，用于成功日志。
    /// - `reader`：响应字节流；单次最多读取 32 KiB。
    ///
    /// # 返回
    /// EOF 后成功发送 final chunk 返回 `Ok(())`；读、取消或发送失败返回错误。
    async fn stream_response_body<R>(
        &self,
        cancellation: &CancellationToken,
        request: &HttpTunnelRequest,
        sender: &dyn ResponseSender,
        status_code: i32,
        start: Instant,
        reader: &mut R,
    ) -> Result<(), TunnelError>
    where
        R: AsyncRead + Unpin,
    {
        let mut buffer = [0u8; HTTP_TUNNEL_CHUNK_SIZE];
        let mut sequence_number = 1u32;
        let mut total_bytes = 0usize;
        let mut total_chunks = 0usize;
        let mut hasher = Xxh64::new(0);

        loop {
            let read_result = tokio::select! {
                () = cancellation.cancelled() => {
                    return self
                        .send_body_read_error(sender, &request.request_id, "request canceled")
                        .await;
                },
                result = reader.read(&mut buffer) => result,
            };
            match read_result {
                Ok(0) => break,
                Ok(read) => {
                    // 不凑满 buffer：flush 产生的小块必须立即穿过 Tunnel，否则会
                    // 改变交互式本地服务的可见时序。
                    let data = buffer[..read].to_vec();
                    hasher.update(&data);
                    total_bytes += read;
                    total_chunks += 1;
                    sender
                        .send_chunk(HttpTunnelResponseChunk {
                            request_id: request.request_id.clone(),
                            data,
                            is_final: false,
                            sequence_number,
                        })
                        .await
                        .map_err(|error| {
                            TunnelError::message(format!("send response chunk: {error}"))
                        })?;
                    sequence_number = sequence_number.wrapping_add(1);
                }
                Err(error) => {
                    let error = error.to_string();
                    return self
                        .send_body_read_error(sender, &request.request_id, &error)
                        .await;
                }
            }
        }

        sender
            .send_chunk(HttpTunnelResponseChunk {
                request_id: request.request_id.clone(),
                data: Vec::new(),
                is_final: true,
                sequence_number,
            })
            .await
            .map_err(|error| TunnelError::message(format!("send final response chunk: {error}")))?;
        self.log_complete(
            request,
            status_code,
            total_bytes,
            total_chunks,
            hasher.digest(),
            start.elapsed(),
        );
        Ok(())
    }

    /// 对应 Go body read 非 EOF 错误分支。
    ///
    /// 先发送协议级错误；sender 成功时仍返回原始 body 错误，使上层记录
    /// `http_tunnel_request_failed`。sender 失败则优先返回发送错误，与 Go 一致。
    ///
    /// # 参数
    /// - `sender`：Tunnel 响应发送器。
    /// - `request_id`：错误所属请求 ID。
    /// - `error`：不含外层 `failed to read response body` 的底层文本。
    async fn send_body_read_error(
        &self,
        sender: &dyn ResponseSender,
        request_id: &str,
        error: &str,
    ) -> Result<(), TunnelError> {
        let message = format!("failed to read response body: {error}");
        send_http_error(sender, request_id, &message).await?;
        Err(TunnelError::message(error))
    }

    /// 对应 Go：`(*Handler).CancelRequest`。
    ///
    /// 原子移除并取消指定请求；空 ID 或未知 ID 是幂等 no-op。
    ///
    /// # 参数
    /// - `request_id`：后端先前分配的请求 ID；空字符串不会访问活动表。
    ///
    /// # 副作用
    /// 命中时触发请求取消令牌，使等待 header 或读取 body 的任务尽快退出。
    pub fn cancel_request(&self, request_id: &str) {
        if request_id.is_empty() {
            return;
        }
        let cancellation = self
            .active_requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(request_id);
        if let Some(cancellation) = cancellation {
            cancellation.cancel();
        }
    }

    /// 对应 Go 成功读取 EOF 后的 `http_tunnel_request_complete` 日志。
    ///
    /// duration 使用纳秒整数并保留字段名 `duration`；不能改成毫秒字段，否则日志
    /// 消费者会看到不同 schema。
    ///
    /// # 参数
    /// - `request`：完成的请求，用于 request ID。
    /// - `status_code`：本地 HTTP 响应状态码。
    /// - `total_bytes`：所有非 final chunk 的字节总数。
    /// - `total_chunks`：非 final chunk 数量。
    /// - `hash`：响应 body 的 xxHash64。
    /// - `duration`：从发起本地请求到读取 EOF 的单调耗时。
    #[allow(clippy::too_many_arguments)]
    fn log_complete(
        &self,
        request: &HttpTunnelRequest,
        status_code: i32,
        total_bytes: usize,
        total_chunks: usize,
        hash: u64,
        duration: std::time::Duration,
    ) {
        tracing::info!(
            bytes = total_bytes,
            chunks = total_chunks,
            duration = duration_nanoseconds(duration),
            request_id = %request.request_id,
            status_code,
            xxhash = hash,
            "http_tunnel_request_complete"
        );
    }
}

/// 用于连接 Hyper 与实际 HTTP 请求行的本地请求目标。
struct HttpTarget {
    /// Hyper 构造 Host 时使用的安全绝对 URI；线上的 path/query 由适配 IO 替换。
    transport_uri: Uri,
    /// 写入 HTTP 请求行的 origin-form target，不包含 fragment。
    wire_target: String,
}

/// 解析 Tunnel method。
///
/// 空 method 是 Proto 默认值，按 GET 处理；非空值必须是 RFC token。
///
/// # 参数
/// - `method`：Tunnel 请求中的原始 method，必须为 ASCII token 或空字符串。
///
/// # 返回
/// 成功时返回 Hyper 使用的 `Method`；非法 token 返回 parser 错误。
fn parse_http_method(method: &str) -> Result<Method, String> {
    if method.is_empty() {
        return Ok(Method::GET);
    }
    Method::from_bytes(method.as_bytes()).map_err(|error| error.to_string())
}

/// 构造固定 localhost 的 HTTP origin-form request-target。
///
/// path 按 RFC 3986 pchar 编码，query 字节原样保留，fragment 不进入 HTTP 请求行。
/// Hyper 仅看到安全占位 URI，实际 target 由 `RequestTargetIo` 写入。
///
/// # 参数
/// - `port`：已由外层验证为 1..=65535 的 loopback 端口。
/// - `path`：以 `/` 开头的 path/query/fragment 字符串。
///
/// # 返回
/// 成功时返回线 target 和安全占位 URI；控制字符或非法 path percent
/// escape 返回错误。
fn build_http_target(port: i32, path: &str) -> Result<HttpTarget, String> {
    let before_fragment = path.split_once('#').map_or(path, |(head, _)| head);
    if before_fragment
        .bytes()
        .any(|byte| byte < b' ' || byte == 0x7f)
    {
        return Err("request target contains a control character".to_owned());
    }

    let (raw_path, query) = before_fragment
        .split_once('?')
        .map_or((before_fragment, None), |(head, tail)| (head, Some(tail)));
    validate_percent_escapes(raw_path)?;

    let mut request_target = encode_request_path(raw_path);
    if let Some(query) = query {
        request_target.push('?');
        request_target.push_str(query);
    }
    // 占位 URI 只提供 scheme/authority，避免 `http::Uri` 二次验证或规范化 query。
    let transport_uri = format!("http://localhost:{port}/")
        .parse::<Uri>()
        .expect("validated localhost port must form a URI");
    Ok(HttpTarget {
        transport_uri,
        wire_target: request_target,
    })
}

/// 校验 path 中的 `%HH` 序列。
fn validate_percent_escapes(value: &str) -> Result<(), String> {
    let bytes = value.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            index += 1;
            continue;
        }
        if index + 2 >= bytes.len()
            || !bytes[index + 1].is_ascii_hexdigit()
            || !bytes[index + 2].is_ascii_hexdigit()
        {
            return Err(format!("invalid percent escape at byte {index}"));
        }
        index += 3;
    }
    Ok(())
}

/// 将 path 编码为 RFC 3986 origin-form 中可用的 ASCII 字节。
fn encode_request_path(raw_path: &str) -> String {
    const UPPER_HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut escaped = String::with_capacity(raw_path.len());
    for byte in raw_path.bytes() {
        if is_request_path_byte(byte) {
            escaped.push(char::from(byte));
        } else {
            escaped.push('%');
            escaped.push(char::from(UPPER_HEX[usize::from(byte >> 4)]));
            escaped.push(char::from(UPPER_HEX[usize::from(byte & 0x0f)]));
        }
    }
    escaped
}

/// 判断字节是否属于 RFC 3986 pchar、`/` 或已校验 percent escape 的 `%`。
fn is_request_path_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'-' | b'.'
                | b'_'
                | b'~'
                | b'!'
                | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b':'
                | b'@'
                | b'['
                | b']'
                | b'/'
                | b'%'
        )
}

/// 将 Tunnel header 转换为 Hyper header map。
///
/// Host 固定为 loopback 目标；Content-Length、Transfer-Encoding 和 Trailer 由
/// Hyper 根据实际 body 生成。其他重复 header 保持输入顺序。
///
/// # 参数
/// - `headers`：Tunnel Proto 中按出现顺序排列的 header。
///
/// # 返回
/// 成功时返回可供 Hyper 写出的 header map；非法 name/value 返回错误。
fn build_http_headers(headers: &[Header]) -> Result<HeaderMap, String> {
    let mut destination = HeaderMap::new();

    for header in headers {
        let name = HeaderName::from_bytes(header.name.as_bytes())
            .map_err(|error| format!("invalid header name {:?}: {error}", header.name))?;
        let trimmed = header.value.trim_matches([' ', '\t']);
        let value = HeaderValue::from_bytes(trimmed.as_bytes())
            .map_err(|error| format!("invalid value for header {name}: {error}"))?;

        if name != HOST && name != CONTENT_LENGTH && name != TRANSFER_ENCODING && name != TRAILER {
            destination.append(name, value);
        }
    }

    Ok(destination)
}

/// 移除不能原样跨越 Tunnel 的 HTTP/1 framing header。
///
/// Hyper 已解除 chunked framing，Tunnel 又不传递 trailer，因此必须移除相应
/// Transfer-Encoding、Trailer 和冲突的 Content-Length。`Connection: close` 只描述
/// 本地 TCP 连接，也不向 Tunnel 对端传递。
///
/// # 参数
/// - `response`：尚未发送 Tunnel headers 的 Hyper 响应，可原地规范化。
/// - `method`：原始有效 method，用于 HEAD body 规则。
fn normalize_response_headers<B>(response: &mut http::Response<B>, method: &Method) {
    let is_chunked = response
        .headers()
        .get_all(TRANSFER_ENCODING)
        .iter()
        .any(|value| {
            value
                .as_bytes()
                .split(|byte| *byte == b',')
                .any(|part| part.trim_ascii().eq_ignore_ascii_case(b"chunked"))
        });
    response.headers_mut().remove(TRANSFER_ENCODING);

    let status = response.status();
    let body_allowed = method != Method::HEAD
        && !status.is_informational()
        && status != StatusCode::NO_CONTENT
        && status != StatusCode::NOT_MODIFIED;
    if is_chunked {
        response.headers_mut().remove(TRAILER);
        if body_allowed {
            response.headers_mut().remove(CONTENT_LENGTH);
        }
    }

    if response.version() == Version::HTTP_11
        && response.headers().get_all(CONNECTION).iter().any(|value| {
            value
                .as_bytes()
                .split(|byte| *byte == b',')
                .any(|part| part.trim_ascii().eq_ignore_ascii_case(b"close"))
        })
    {
        response.headers_mut().remove(CONNECTION);
    }
}

/// 生成 Tunnel 响应使用的 `<code> <reason>` 状态文本。
///
/// Hyper 仅在 HTTP/1 reason phrase 非 canonical（包括显式空 phrase）时放入
/// `ReasonPhrase`。有扩展时必须逐字节使用；没有扩展时才用 canonical 文本。未知
/// status 的空 phrase 保留 code 后的空格，不擅自补标准描述。
///
/// # 参数
/// - `response`：刚收到且尚未拆 body 的 Hyper 响应。
///
/// # 返回
/// `"<code> <reason>"` 形式的状态字符串。
fn response_status_text<B>(response: &http::Response<B>) -> String {
    let code = response.status().as_u16();
    let reason = response
        .extensions()
        .get::<ReasonPhrase>()
        .map(|reason| String::from_utf8_lossy(reason.as_bytes()).into_owned())
        .or_else(|| response.status().canonical_reason().map(str::to_owned))
        .unwrap_or_default();
    format!("{code} {reason}")
}

/// 发送协议级 HTTP error 并包装 sender 失败。
///
/// # 参数
/// - `sender`：当前请求的协议响应发送器。
/// - `request_id`：错误所属请求 ID，可为空。
/// - `message`：需要写入 Proto `error_message` 的错误文本。
///
/// # 返回
/// 发送成功返回 `Ok(())`；sender 失败返回带 `send response error` 前缀的错误。
async fn send_http_error(
    sender: &dyn ResponseSender,
    request_id: &str,
    message: &str,
) -> Result<(), TunnelError> {
    sender
        .send_error(HttpTunnelResponseError {
            request_id: request_id.to_owned(),
            error_message: message.to_owned(),
        })
        .await
        .map_err(|error| TunnelError::message(format!("send response error: {error}")))
}
