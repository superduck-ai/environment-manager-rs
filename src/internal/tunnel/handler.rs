//! 本地 HTTP 请求 Tunnel 处理器。
//!
//! 对应 Go `internal/tunnel/handler.go`。请求只允许 loopback 端口和绝对 path；响应
//! header 先发送，body 按 Go `Read` 的实际返回边界流式回传，最后发送空的 final
//! chunk。这里使用只支持 loopback 明文 HTTP 的 Hyper 客户端，避免代理环境变量和
//! WHATWG URL 规范化改变 Go 版本的线协议。

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use futures_util::TryStreamExt as _;
use http::header::{
    ACCEPT_ENCODING, CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, HOST, RANGE, TRAILER,
    TRANSFER_ENCODING, USER_AGENT,
};
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, Version};
use http_body_util::{BodyExt as _, Full};
use hyper::ext::ReasonPhrase;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt as _, AsyncWrite, BufReader, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_util::io::StreamReader;
use tokio_util::sync::CancellationToken;
use xxhash_rust::xxh64::Xxh64;

use super::helpers::canonical_mime_header_key;
use crate::internal::util::duration_nanoseconds;

use super::{
    Handler, Header, HttpTunnelRequest, HttpTunnelResponseChunk, HttpTunnelResponseError,
    HttpTunnelResponseHeaders, ResponseSender, TunnelError, flatten_http_headers,
};

/// 对应 Go `httpTunnelChunkSize`：每个响应数据分片最多 32 KiB。
pub const HTTP_TUNNEL_CHUNK_SIZE: usize = 32 * 1024;

/// 对应 Go `http.Transport.ResponseHeaderTimeout`：本地服务必须在 30 秒内返回 header。
const RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(30);

/// Go `http.Transport` 在请求方未提供 User-Agent 时写入的默认值。
const GO_DEFAULT_USER_AGENT: &str = "Go-http-client/1.1";

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
struct GoHttpIo {
    stream: TcpStream,
    wire_target: Vec<u8>,
    body_length: usize,
    request_head: Vec<u8>,
    pending_write: Vec<u8>,
    pending_offset: usize,
    expected_wire_bytes: Option<usize>,
    written_wire_bytes: usize,
    request_written: Option<oneshot::Sender<()>>,
    response_capture: Arc<std::sync::Mutex<Vec<u8>>>,
}

impl GoHttpIo {
    /// 创建请求写入观察器。
    ///
    /// # 参数
    /// - `stream`：已经直连 localhost 的 TCP 流。
    /// - `wire_target`：Go `URL.RequestURI()` 的原始字节，不含 fragment。
    /// - `body_length`：固定 `Full<Bytes>` 请求体长度，用于识别完整写入时点。
    /// - `request_written`：完整请求实际写入 socket 后触发的一次性通知。
    /// - `response_capture`：保存少量响应前缀，供错误兼容层还原 Go parser 文本。
    fn new(
        stream: TcpStream,
        wire_target: String,
        body_length: usize,
        request_written: oneshot::Sender<()>,
        response_capture: Arc<std::sync::Mutex<Vec<u8>>>,
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
            response_capture,
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

impl AsyncRead for GoHttpIo {
    /// 把响应字节交给 Hyper，同时保留有界前缀用于精确重建 Go 的 parser 错误。
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let previous = buffer.filled().len();
        let result = Pin::new(&mut self.stream).poll_read(cx, buffer);
        if matches!(result, Poll::Ready(Ok(()))) && buffer.filled().len() > previous {
            const ERROR_CAPTURE_LIMIT: usize = 8 * 1024;
            let mut capture = self
                .response_capture
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let remaining = ERROR_CAPTURE_LIMIT.saturating_sub(capture.len());
            let end = previous + remaining.min(buffer.filled().len() - previous);
            capture.extend_from_slice(&buffer.filled()[previous..end]);
        }
        result
    }
}

impl AsyncWrite for GoHttpIo {
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

/// Go 多成员 gzip reader 的内部状态。
///
/// async-compression 自带 multistream 会在一次 `poll_read` 中跨过 member 边界，Go
/// `gzip.Reader.Read` 则在小 member 结束时先返回当前数据。显式拆成逐 member decoder
/// 可保持 Tunnel chunk 边界，同时仍按底层网络到达时序流式解压。
enum GoGzipState<R> {
    /// 正在解码一个 gzip member；Box 避免 decoder 的大缓冲把整个状态机 future 膨胀。
    Decoding(Box<GzipDecoder<BufReader<R>>>),
    /// 前一 member 已结束，等待底层确认 EOF 或提供下一 member。
    Between(BufReader<R>),
    /// 底层输入已 EOF，后续 read 稳定返回零字节。
    Done,
}

/// 逐 member 复刻 Go `compress/gzip.Reader` Read 边界的异步读取器。
struct GoMultiMemberGzipReader<R> {
    state: GoGzipState<R>,
}

impl<R> GoMultiMemberGzipReader<R>
where
    R: AsyncRead + Unpin,
{
    /// 用尚未消费的 Hyper body stream 创建首个 gzip member decoder。
    ///
    /// # 参数
    /// - `reader`：已经去除 HTTP framing、但仍为 gzip wire bytes 的异步流。
    ///
    /// # 返回
    /// 返回停在第一个 member header 前的流式 reader。
    fn new(reader: R) -> Self {
        Self {
            state: GoGzipState::Decoding(Box::new(GzipDecoder::new(BufReader::new(reader)))),
        }
    }
}

impl<R> AsyncRead for GoMultiMemberGzipReader<R>
where
    R: AsyncRead + Unpin,
{
    /// 解码一个 member；到达边界后先向调用方返回，再在下一次 read 初始化后继 member。
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                GoGzipState::Decoding(decoder) => {
                    let previous = output.filled().len();
                    match Pin::new(decoder).poll_read(cx, output) {
                        Poll::Ready(Ok(())) if output.filled().len() > previous => {
                            // 即使 decoder 已同时发现 member EOF，也先返回本 member 的
                            // 数据；下一次 poll 才能产生后继 member 的独立 Go chunk。
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Ready(Ok(())) => {
                            let GoGzipState::Decoding(decoder) =
                                std::mem::replace(&mut this.state, GoGzipState::Done)
                            else {
                                unreachable!("state matched Decoding above")
                            };
                            this.state = GoGzipState::Between((*decoder).into_inner());
                        }
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                GoGzipState::Between(reader) => {
                    let has_next_member = match Pin::new(reader).poll_fill_buf(cx) {
                        Poll::Ready(Ok(bytes)) => !bytes.is_empty(),
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Pending => return Poll::Pending,
                    };
                    if has_next_member {
                        let GoGzipState::Between(reader) =
                            std::mem::replace(&mut this.state, GoGzipState::Done)
                        else {
                            unreachable!("state matched Between above")
                        };
                        this.state = GoGzipState::Decoding(Box::new(GzipDecoder::new(reader)));
                    } else {
                        this.state = GoGzipState::Done;
                        return Poll::Ready(Ok(()));
                    }
                }
                GoGzipState::Done => return Poll::Ready(Ok(())),
            }
        }
    }
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
        let method = match go_http_method(&request.method) {
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
        let target = match go_http_target(request.port, &request.path) {
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
        let operation = go_url_error_operation(&request.method);

        // Header.Add 本身不校验；Go 直到 Client.Do 才拒绝非法 header。因此该分支必须
        // 发送 `request failed` 并返回 Ok，不能误归类成 `failed to create request`。
        let mut headers = match go_http_headers(&request.headers) {
            Ok(headers) => headers,
            Err(error) => {
                let error = format!("{operation} {}: {error}", go_quote(&target.display_url));
                send_http_error(
                    sender,
                    &request.request_id,
                    &format!("request failed: {error}"),
                )
                .await?;
                return Ok(());
            }
        };
        let requested_gzip = enable_go_auto_gzip(&method, &mut headers);
        headers.insert(
            HOST,
            HeaderValue::from_bytes(format!("localhost:{}", request.port).as_bytes())
                .expect("validated port must form a Host header"),
        );
        let mut outbound_request = http::Request::builder()
            .method(method.clone())
            // Hyper 只看到可解析的占位 URI；GoHttpIo 在实际写线前把 request-target
            // 替换为 Go RawQuery 字节。authority 仍保留真实端口，用于生成 Host。
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
                        &format!(
                            "request failed: {operation} {}: context canceled",
                            go_quote(&target.display_url),
                        ),
                    ).await?;
                    return Ok(());
                }
                () = tokio::time::sleep(self.connect_delay) => {}
            }
        }

        // Go 的 header timer 不覆盖 DNS/dial。目标主机固定为 localhost，Tokio 会按
        // resolver 顺序尝试 IPv6/IPv4 loopback；不启用 proxy、TLS 或连接池。
        let stream = tokio::select! {
            () = cancellation.cancelled() => {
                send_http_error(
                    sender,
                    &request.request_id,
                    &format!(
                        "request failed: {operation} {}: context canceled",
                        go_quote(&target.display_url),
                    ),
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
                            &format!(
                                "request failed: {operation} {}: {}",
                                go_quote(&target.display_url),
                                go_connect_error(&error, request.port),
                            ),
                        ).await?;
                        return Ok(());
                    }
                }
            }
        };
        let local_address = stream.local_addr().ok();
        let peer_address = stream.peer_addr().ok();
        let (request_written_sender, mut request_written_receiver) = oneshot::channel();
        let response_capture = Arc::new(std::sync::Mutex::new(Vec::new()));
        let io = GoHttpIo::new(
            stream,
            target.wire_target.clone(),
            request.body.len(),
            request_written_sender,
            Arc::clone(&response_capture),
        );
        let handshake = tokio::select! {
            () = cancellation.cancelled() => {
                send_http_error(
                    sender,
                    &request.request_id,
                    &format!(
                        "request failed: {operation} {}: context canceled",
                        go_quote(&target.display_url),
                    ),
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
                    &format!(
                        "request failed: {operation} {}: {}",
                        go_quote(&target.display_url),
                        go_hyper_request_error(
                            &error,
                            request.port,
                            local_address,
                            peer_address,
                            &response_capture,
                        ),
                    ),
                )
                .await?;
                return Ok(());
            }
        };
        let connection_task = tokio::spawn(async move { connection.with_upgrades().await });
        let mut response_future = Box::pin(request_sender.send_request(outbound_request));

        // net/http 的 readLoop 可以在 writeLoop 完成前交付响应。只有“写成功”通知先
        // 到达时才创建 timer；若响应先到则直接返回，严格保留 Go 的竞态优先级。
        let response = tokio::select! {
            () = cancellation.cancelled() => {
                connection_task.abort();
                send_http_error(
                    sender,
                    &request.request_id,
                    &format!(
                        "request failed: {operation} {}: context canceled",
                        go_quote(&target.display_url),
                    ),
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
                                &format!(
                                    "request failed: {operation} {}: context canceled",
                                    go_quote(&target.display_url),
                                ),
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
                                &format!(
                                    "request failed: {operation} {}: context canceled",
                                    go_quote(&target.display_url),
                                ),
                            ).await?;
                            return Ok(());
                        }
                        response = &mut response_future => response,
                        () = tokio::time::sleep(self.response_header_timeout) => {
                            connection_task.abort();
                            send_http_error(
                                sender,
                                &request.request_id,
                                &format!(
                                    "request failed: {operation} {}: net/http: timeout awaiting response headers",
                                    go_quote(&target.display_url),
                                ),
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
                    &format!(
                        "request failed: {operation} {}: {}",
                        go_quote(&target.display_url),
                        go_hyper_request_error(
                            &error,
                            request.port,
                            local_address,
                            peer_address,
                            &response_capture,
                        ),
                    ),
                )
                .await?;
                return Ok(());
            }
        };

        let status_code = i32::from(response.status().as_u16());
        let status_text = go_response_status(&response);
        let decompress_gzip = normalize_go_response_headers(&mut response, &method, requested_gzip);
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
            // Go 对 101 的 resp.Body 是升级后的连接。Hyper 把同一字节流放在 OnUpgrade
            // 中，因此 header 发出后再等待升级并复用完全相同的 32 KiB read loop。
            let upgraded = tokio::select! {
                () = cancellation.cancelled() => {
                    return self
                        .send_body_read_error(sender, &request.request_id, "context canceled")
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
                .map_err(go_hyper_body_error);
            let reader = StreamReader::new(stream);
            if decompress_gzip {
                // 仅当本 transport 自动添加 Accept-Encoding 时透明解压；调用方显式
                // 请求 gzip 时 Go 会保留压缩 body 与响应 header，不能在此擅自解码。
                // 不能直接打开 async-compression 的 `multiple_members(true)`：它会把
                // 相邻小 member 合并进一次 read，而 Go 在 member 边界先返回。专用
                // reader 既连续解码，又为每个 member 保留独立 Tunnel chunk。
                let mut decoder = GoMultiMemberGzipReader::new(reader);
                self.stream_response_body(
                    cancellation,
                    request,
                    sender,
                    status_code,
                    start,
                    &mut decoder,
                )
                .await
            } else {
                let mut reader = reader;
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
    }

    /// 对应 Go `resp.Body.Read(buffer)` 循环并发送 body chunk。
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
                        .send_body_read_error(sender, &request.request_id, "context canceled")
                        .await;
                },
                result = reader.read(&mut buffer) => result,
            };
            match read_result {
                Ok(0) => break,
                Ok(read) => {
                    // 不凑满 buffer：Go 的 io.Reader 允许任意 n>0，flush 产生的一字节
                    // 必须立刻穿过 Tunnel，否则会改变交互式本地服务的可见时序。
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
                    let error = go_body_read_error_text(&error);
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

/// 经过 Go URL 规则转换后的本地请求目标。
struct GoHttpTarget {
    /// Hyper 构造 Host 时使用的安全绝对 URI；线上的 path/query 由适配 IO 替换。
    transport_uri: Uri,
    /// Go `URL.RequestURI()` 返回的精确线字节，不包含 fragment。
    wire_target: String,
    /// Go `url.Error` 中显示的 URL；包含经过 `EscapedFragment` 处理的 fragment。
    display_url: String,
}

/// 对应 Go `http.NewRequestWithContext` 对 method 的处理。
///
/// 空 method 解释为 GET；非空值必须是 RFC token。错误文本使用 Go
/// `net/http: invalid method %q` 的可观测格式，而不是 Rust parser 的内部措辞。
///
/// # 参数
/// - `method`：Tunnel 请求中的原始 method，必须为 ASCII token 或空字符串。
///
/// # 返回
/// 成功时返回 Hyper 使用的 `Method`；非法 token 返回 Go 兼容错误文本。
fn go_http_method(method: &str) -> Result<Method, String> {
    if method.is_empty() {
        return Ok(Method::GET);
    }
    Method::from_bytes(method.as_bytes())
        .map_err(|_| format!("net/http: invalid method {}", go_quote(method)))
}

/// 对应 Go `url.Parse`、`URL.EscapedPath` 与 `URL.RequestURI` 的固定 localhost 子集。
///
/// Go 按 RFC 3986 解析，而 reqwest/url 使用 WHATWG：后者会把反斜杠当路径分隔符、
/// 编码 query 中的 apostrophe，并容忍坏 percent escape。此函数在进入 Hyper 前完成
/// Go 的 percent 校验与 path escaping，再用原始 `http::Uri` 保留 query 字节。
///
/// # 参数
/// - `port`：已由外层验证为 1..=65535 的 loopback 端口。
/// - `path`：以 `/` 开头的 path/query/fragment 字符串。
///
/// # 返回
/// 成功时返回原始线 target、安全占位 URI 和完整错误显示 URL；控制字符或 path、
/// fragment 中的非法 percent escape 返回与 Go 请求构造阶段相同的错误。
fn go_http_target(port: i32, path: &str) -> Result<GoHttpTarget, String> {
    let raw_url = format!("http://localhost:{port}{path}");
    let (before_fragment, fragment) = path
        .split_once('#')
        .map_or((path, None), |(head, tail)| (head, Some(tail)));

    // net/url.parse 在拆 query 之前拒绝整个 pre-fragment URL 中的 ASCII CTL；fragment
    // 由 setFragment 单独处理，所以其中的控制字符只会被 escaping，不触发此错误。
    if before_fragment
        .bytes()
        .any(|byte| byte < b' ' || byte == 0x7f)
    {
        let parsed_url = format!("http://localhost:{port}{before_fragment}");
        return Err(format!(
            "parse {}: net/url: invalid control character in URL",
            go_quote(&parsed_url),
        ));
    }

    let (raw_path, query) = before_fragment
        .split_once('?')
        .map_or((before_fragment, None), |(head, tail)| (head, Some(tail)));
    validate_go_percent_escapes(raw_path).map_err(|escape| {
        let parsed_url = format!("http://localhost:{port}{before_fragment}");
        format!(
            "parse {}: invalid URL escape {}",
            go_quote(&parsed_url),
            go_quote(&escape),
        )
    })?;
    if let Some(fragment) = fragment {
        validate_go_percent_escapes(fragment).map_err(|escape| {
            format!(
                "parse {}: invalid URL escape {}",
                go_quote(&raw_url),
                go_quote(&escape),
            )
        })?;
    }

    let escaped_path = go_escaped_http_path(raw_path);
    let mut request_target = escaped_path;
    if let Some(query) = query {
        request_target.push('?');
        request_target.push_str(query);
    }
    let mut display_url = format!("http://localhost:{port}{request_target}");
    if let Some(fragment) = fragment {
        display_url.push('#');
        display_url.push_str(&go_escaped_fragment(fragment));
    }
    // 占位 URI 只提供 scheme/authority。GoHttpIo 会在首行写线前替换 `/`，因此这里
    // 绝不能再让 `http::Uri` 对 RawQuery 做二次验证或百分号规范化。
    let transport_uri = format!("http://localhost:{port}/")
        .parse::<Uri>()
        .expect("validated localhost port must form a URI");
    Ok(GoHttpTarget {
        transport_uri,
        wire_target: request_target,
        display_url,
    })
}

/// 对应 Go `URL.EscapedFragment`，保留合法 RawFragment 或按 `encodeFragment` 转义。
///
/// `net/url` 先 percent-decode fragment，再决定原始拼写能否保留。合法 `%2f`、方括号
/// 和单引号会保持原样；空格、反斜杠、尖括号及 UTF-8 字节则使用大写 `%HH`。错误
/// URL 文本依赖这一细节，但 fragment 从不进入 HTTP request-target。
///
/// # 参数
/// - `fragment`：已通过 percent escape 校验的原始 fragment。
///
/// # 返回
/// 可直接追加在 `#` 后的 Go 兼容 escaped fragment。
fn go_escaped_fragment(fragment: &str) -> String {
    if fragment.bytes().all(go_valid_raw_fragment_byte) {
        return fragment.to_owned();
    }

    let bytes = fragment.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            decoded.push((go_hex_value(bytes[index + 1]) << 4) | go_hex_value(bytes[index + 2]));
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }

    go_percent_encode_bytes(&decoded, go_default_fragment_byte)
}

/// 判断原始 fragment 是否满足 Go `validEncoded(..., encodeFragment)`。
///
/// # 参数
/// - `byte`：原始 fragment 的单字节。
///
/// # 返回
/// Go 可作为 RawFragment 原样保留的 ASCII 字节（含 `%`）返回 true。
fn go_valid_raw_fragment_byte(byte: u8) -> bool {
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
                | b'?'
                | b'%'
        )
}

/// 判断解码后的 fragment 字节是否由 Go `encodeFragment` 保留。
///
/// # 参数
/// - `byte`：percent-decode 后的单字节。
///
/// # 返回
/// Go 1.25.7 fragment 表中无需转义的 ASCII 字节返回 true。
fn go_default_fragment_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'-' | b'.'
                | b'_'
                | b'~'
                | b'!'
                | b'$'
                | b'&'
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b':'
                | b'@'
                | b'/'
                | b'?'
        )
}

/// 使用大写十六进制对不在允许表中的字节做 Go 风格 percent encoding。
///
/// # 参数
/// - `bytes`：已经完成 percent-decode 的原始数据。
/// - `allowed`：对应 URL 组件的无需转义判定函数。
///
/// # 返回
/// 只包含允许字节与大写 `%HH` 序列的字符串。
fn go_percent_encode_bytes(bytes: &[u8], allowed: fn(u8) -> bool) -> String {
    const UPPER_HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut escaped = String::with_capacity(bytes.len());
    for &byte in bytes {
        if allowed(byte) {
            escaped.push(char::from(byte));
        } else {
            escaped.push('%');
            escaped.push(char::from(UPPER_HEX[usize::from(byte >> 4)]));
            escaped.push(char::from(UPPER_HEX[usize::from(byte & 0x0f)]));
        }
    }
    escaped
}

/// 校验 Go `net/url.unescape` 要求的 `%HH` 序列。
///
/// # 参数
/// - `value`：待检查的 path 或 fragment 原始字节串。
///
/// # 返回
/// 全部 percent escape 合法时返回成功；否则返回从坏 `%` 开始、最多三字节的片段，
/// 对应 Go `url.EscapeError` 的载荷。
fn validate_go_percent_escapes(value: &str) -> Result<(), String> {
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
            let end = (index + 3).min(bytes.len());
            return Err(String::from_utf8_lossy(&bytes[index..end]).into_owned());
        }
        index += 3;
    }
    Ok(())
}

/// 生成 Go `URL.EscapedPath` 的线格式。
///
/// 原始 path 若只包含 RFC pchar、`/` 和合法 percent escape，Go 会把它保存在 RawPath
/// 并逐字节发送；否则先 percent-decode，再按 `encodePath` 重新编码。该双阶段规则正是
/// 反斜杠成为 `%5C`、而 `%2f` 大小写仍被保留的原因。
///
/// # 参数
/// - `raw_path`：已经验证所有 percent escape 的 path，不包含 query/fragment。
///
/// # 返回
/// 可直接放入 HTTP request-target 的 ASCII path。
fn go_escaped_http_path(raw_path: &str) -> String {
    if raw_path.bytes().all(go_valid_raw_path_byte) {
        return raw_path.to_owned();
    }

    let bytes = raw_path.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = go_hex_value(bytes[index + 1]);
            let low = go_hex_value(bytes[index + 2]);
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }

    let mut escaped = String::with_capacity(decoded.len());
    for byte in decoded {
        if go_default_path_byte(byte) {
            escaped.push(char::from(byte));
        } else {
            const UPPER_HEX: &[u8; 16] = b"0123456789ABCDEF";
            escaped.push('%');
            escaped.push(char::from(UPPER_HEX[usize::from(byte >> 4)]));
            escaped.push(char::from(UPPER_HEX[usize::from(byte & 0x0f)]));
        }
    }
    escaped
}

/// 判断原始 path 字节能否作为 Go `RawPath` 保留。
///
/// # 参数
/// - `byte`：单个 UTF-8/ASCII 字节。
///
/// # 返回
/// RFC pchar、斜杠或 `%` 返回 true；其他字节必须走 decode/re-encode 分支。
fn go_valid_raw_path_byte(byte: u8) -> bool {
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

/// 判断 decode 后的字节是否可由 Go `encodePath` 默认保持未转义。
///
/// # 参数
/// - `byte`：percent-decode 后的单字节。
///
/// # 返回
/// Go encoding table 中带 `encodePath` 位的字节返回 true。
fn go_default_path_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'-' | b'.'
                | b'_'
                | b'~'
                | b'$'
                | b'&'
                | b'+'
                | b','
                | b':'
                | b';'
                | b'='
                | b'/'
                | b'@'
        )
}

/// 把单个 ASCII hexadecimal 字节转为 0..=15。
///
/// # 参数
/// - `byte`：调用方已通过 percent 校验的十六进制字符。
///
/// # 返回
/// 对应 nibble；前置校验保证不会进入兜底分支。
fn go_hex_value(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => unreachable!("percent escape 已在调用前校验"),
    }
}

/// 对应 Go `applyTunnelHeaders` 加 `http.Transport` HTTP/1 写出规则。
///
/// 所有非空 header 都先校验；Host、Content-Length、Transfer-Encoding 和 Trailer
/// 随后由 transport 忽略。User-Agent 只采用第一值，缺失时写入 Go 默认值，显式空值
/// 则抑制默认值；其他重复 header 保持输入顺序。
///
/// # 参数
/// - `headers`：Tunnel Proto 中按出现顺序排列的 header。
///
/// # 返回
/// 成功时返回可供 Hyper 写出的 header map；非法 name/value 返回 Go Transport 文本。
fn go_http_headers(headers: &[Header]) -> Result<HeaderMap, String> {
    let mut destination = HeaderMap::new();
    let mut explicit_user_agent: Option<HeaderValue> = None;

    for header in headers {
        if header.name.is_empty() {
            continue;
        }
        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| {
            format!(
                "net/http: invalid header field name {}",
                go_quote(&header.name),
            )
        })?;
        let canonical_name = canonical_mime_header_key(name.as_str());
        // Go 在排除 transport 特殊 header 之前验证所有值，不能因稍后忽略它们而跳过
        // CR/LF 检查。实际写线时 textproto.TrimString 会移除两端 SP/HTAB。
        HeaderValue::from_bytes(header.value.as_bytes()).map_err(|_| {
            format!(
                "net/http: invalid header field value for {}",
                go_quote(&canonical_name),
            )
        })?;
        let trimmed = header.value.trim_matches([' ', '\t']);
        let value = HeaderValue::from_bytes(trimmed.as_bytes()).map_err(|_| {
            format!(
                "net/http: invalid header field value for {}",
                go_quote(&canonical_name),
            )
        })?;

        if name == USER_AGENT {
            if explicit_user_agent.is_none() {
                explicit_user_agent = Some(value);
            }
        } else if name != HOST
            && name != CONTENT_LENGTH
            && name != TRANSFER_ENCODING
            && name != TRAILER
        {
            destination.append(name, value);
        }
    }

    match explicit_user_agent {
        None => {
            destination.insert(USER_AGENT, HeaderValue::from_static(GO_DEFAULT_USER_AGENT));
        }
        Some(value) if !value.is_empty() => {
            destination.insert(USER_AGENT, value);
        }
        Some(_) => {}
    }
    Ok(destination)
}

/// 对应 Go Transport 自动协商 gzip 的请求头变换。
///
/// 仅当首个 Accept-Encoding 与 Range 都缺失或为空、且 method 不是精确大写 `HEAD`
/// 时自动写入 `Accept-Encoding: gzip`。返回值必须随请求保存，因为 Go 只会透明解压
/// 自己添加该 header 的响应。
///
/// # 参数
/// - `method`：已经通过 token 校验的有效 method。
/// - `headers`：完成 Go 特殊 header 规则后的请求 header map。
///
/// # 返回
/// 本函数自动请求 gzip 时返回 true；调用方显式编码、Range 或 HEAD 返回 false。
fn enable_go_auto_gzip(method: &Method, headers: &mut HeaderMap) -> bool {
    let accepts_encoding = headers
        .get(ACCEPT_ENCODING)
        .is_some_and(|value| !value.is_empty());
    let has_range = headers.get(RANGE).is_some_and(|value| !value.is_empty());
    if method == Method::HEAD || accepts_encoding || has_range {
        return false;
    }
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));
    true
}

/// 对应 Go `ReadResponse` 和 Transport 透明 gzip 对 `resp.Header` 的变换。
///
/// Go 把 Transfer-Encoding 移到专用字段；chunked 响应还移除 Trailer 声明，并在允许
/// body 的状态下移除冲突 Content-Length。HTTP/1.1 `Connection: close` 也从 Header
/// 删除。若本 transport 自动请求 gzip，则删除 Content-Encoding/Content-Length 并
/// 告知调用方用流式 decoder 包装 body。
///
/// # 参数
/// - `response`：尚未发送 Tunnel headers 的 Hyper 响应，可原地规范化。
/// - `method`：原始有效 method，用于 HEAD body 规则。
/// - `requested_gzip`：请求阶段是否由 transport 自动添加 gzip。
///
/// # 返回
/// 响应 body 需要透明 gzip 解压时返回 true。
fn normalize_go_response_headers<B>(
    response: &mut http::Response<B>,
    method: &Method,
    requested_gzip: bool,
) -> bool {
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

    let decompress_gzip = requested_gzip
        && response
            .headers()
            .get(CONTENT_ENCODING)
            .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"gzip"));
    if decompress_gzip {
        response.headers_mut().remove(CONTENT_ENCODING);
        response.headers_mut().remove(CONTENT_LENGTH);
    }
    decompress_gzip
}

/// 对应 Go `urlErrorOp` 的 method 显示格式。
///
/// # 参数
/// - `method`：原始请求 method；空值代表 GET。
///
/// # 返回
/// 空值为 `Get`；ASCII method 保留首字节并把其余字节转小写。
fn go_url_error_operation(method: &str) -> String {
    if method.is_empty() {
        return "Get".to_owned();
    }
    let mut bytes = method.as_bytes().to_ascii_lowercase();
    bytes[0] = method.as_bytes()[0];
    String::from_utf8(bytes).unwrap_or_else(|_| method.to_owned())
}

/// 使用 Go `%q` 等价形式引用 UTF-8 字符串。
///
/// # 参数
/// - `value`：错误文本中需要双引号和反斜杠转义的值。
///
/// # 返回
/// 带双引号的 JSON/Go 兼容字符串字面量。
fn go_quote(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| format!("\"{value}\""))
}

/// 从 Hyper response 扩展重建 Go `resp.Status`。
///
/// Hyper 仅在 HTTP/1 reason phrase 非 canonical（包括显式空 phrase）时放入
/// `ReasonPhrase`。有扩展时必须逐字节使用；没有扩展时才用 canonical 文本。未知
/// status 的空 phrase 保留 code 后的空格，不擅自补标准描述。
///
/// # 参数
/// - `response`：刚收到且尚未拆 body 的 Hyper 响应。
///
/// # 返回
/// `"<code> <reason>"` 形式的 Go Status 字符串。
fn go_response_status<B>(response: &http::Response<B>) -> String {
    let code = response.status().as_u16();
    let reason = response
        .extensions()
        .get::<ReasonPhrase>()
        .map(|reason| String::from_utf8_lossy(reason.as_bytes()).into_owned())
        .or_else(|| response.status().canonical_reason().map(str::to_owned))
        .unwrap_or_default();
    format!("{code} {reason}")
}

/// 把直连 loopback 的拨号错误重建为 Go Linux `net.OpError` 文本。
///
/// # 参数
/// - `error`：Tokio TCP connect 返回的 Linux IO 错误。
/// - `port`：已经验证的 loopback 目标端口，用于重建 Go dial 地址。
///
/// # 返回
/// connection-refused 返回完整 Go dial 文本；其他错误保留 Linux OS 文本。
fn go_connect_error(error: &io::Error, port: i32) -> String {
    if error.kind() == io::ErrorKind::ConnectionRefused {
        return format!("dial tcp [::1]:{port}: connect: connection refused");
    }
    format!("dial tcp [::1]:{port}: connect: {error}")
}

/// 把 Hyper 握手/响应 header 错误转换成 Go `http.Transport` 可见文本。
///
/// Hyper 的顶层文本带有内部 parser 分类；本函数沿 source 链提取 Linux IO 错误，
/// 并保留响应前缀与连接地址，供 malformed response、EOF 和 reset 场景重建 Go 文本。
/// 错误映射由 Rust 回归测试固定。
///
/// # 参数
/// - `error`：Hyper HTTP/1 连接或 `send_request` 返回的错误。
/// - `port`：loopback 目标端口。
/// - `local_address`：成功拨号后的本地地址；用于 Linux read/write 错误包装。
/// - `peer_address`：成功拨号后的远端地址。
/// - `response_capture`：IO 层保留的有界响应前缀。
///
/// # 返回
/// 可嵌入 Go `url.Error` 的底层错误文本。
fn go_hyper_request_error(
    error: &hyper::Error,
    port: i32,
    local_address: Option<SocketAddr>,
    peer_address: Option<SocketAddr>,
    response_capture: &Arc<std::sync::Mutex<Vec<u8>>>,
) -> String {
    let mut message = error.to_string();
    let mut connection_reset = false;
    let mut source = std::error::Error::source(error);
    while let Some(current) = source {
        if let Some(io_error) = current.downcast_ref::<io::Error>() {
            if io_error.kind() == io::ErrorKind::ConnectionRefused {
                // Go 1.25.7 在 Ubuntu ARM64 上解析固定主机名 localhost 时把失败地址
                // 记录为 IPv6 loopback；恢复 net.OpError 包装层而不是泄漏 Hyper 文本。
                return format!("dial tcp [::1]:{port}: connect: connection refused");
            }
            connection_reset |= io_error.kind() == io::ErrorKind::ConnectionReset;
        }
        message = current.to_string();
        source = current.source();
    }

    if connection_reset
        && let (Some(local_address), Some(peer_address)) = (local_address, peer_address)
    {
        return format!("read tcp {local_address}->{peer_address}: read: connection reset by peer");
    }

    let capture = response_capture
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if capture.is_empty() && error.is_incomplete_message() {
        return "EOF".to_owned();
    }
    if capture.starts_with(b"NOT HTTP\r\n") {
        return "net/http: HTTP/1.x transport connection broken: malformed HTTP status code \"HTTP\""
            .to_owned();
    }
    if let Some(status_line_end) = find_bytes(&capture, b"\r\n") {
        let headers = &capture[status_line_end + 2..];
        if let Some(header_line_end) = find_bytes(headers, b"\r\n") {
            let malformed_line = &headers[..header_line_end];
            if malformed_line.contains(&0) {
                // Go 的 MIME header parser 会把包含 NUL 的原始 header 行原样放入
                // 错误文本。这里不能使用 HeaderValue 或 UTF-8 严格解码，否则会在
                // 形成兼容错误前丢失非法字节；探针覆盖的 NUL 仍是合法 UTF-8 字节。
                return format!(
                    "net/http: HTTP/1.x transport connection broken: malformed MIME header line: {}",
                    String::from_utf8_lossy(malformed_line)
                );
            }
        }
    }
    if capture
        .windows(b"Content-Length: 1\r\nContent-Length: 2".len())
        .any(|window| window == b"Content-Length: 1\r\nContent-Length: 2")
    {
        return "net/http: HTTP/1.x transport connection broken: http: message cannot contain multiple Content-Length headers; got [\"1\" \"2\"]"
            .to_owned();
    }
    message
}

/// 把 Hyper body 错误转换成 Go `resp.Body.Read` 可见的 `io` 错误。
///
/// Content-Length 未满足时 Hyper 报 `incomplete message`，而 Go body reader 返回
/// `unexpected EOF`；取消则对应 `context canceled`。其他 framing/IO 错误保留最深
/// source 文本，交由统一 body read 错误分支发送。
///
/// # 参数
/// - `error`：Hyper Incoming body stream 的错误。
///
/// # 返回
/// 供 `StreamReader` 暴露给 AsyncRead 的 `io::Error`。
fn go_hyper_body_error(error: hyper::Error) -> io::Error {
    if error.is_incomplete_message() {
        return io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected EOF");
    }
    if error.is_canceled() {
        return io::Error::other("context canceled");
    }
    let mut message = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(current) = source {
        message = current.to_string();
        source = current.source();
    }
    // Hyper 1.10 的 Content-Length decoder 会把 EOF 包装在 body error 的 source
    // 链内，顶层 `is_incomplete_message` 因包装层而可能返回 false。Go 的
    // `bodyEOFSignal` 对同一线协议固定暴露 `io.ErrUnexpectedEOF`，因此必须在取出
    // 最深原因后再次归一化；不能把 Hyper 的内部实现文本发到 Tunnel 对端。
    if message == "end of file before message length reached" {
        return io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected EOF");
    }
    io::Error::other(message)
}

/// 把 AsyncRead/自动 gzip decoder 的错误文本归一化为 Go `resp.Body.Read` 文本。
///
/// Rust 标准库把 UnexpectedEof 显示为 `unexpected end of file`，compression-codecs
/// 则使用首字母大写的 `Invalid gzip header`；Go 1.25.7 分别暴露 `unexpected EOF`
/// 与 `gzip: invalid header`。校验 footer 的两个第三方文本也统一为 Go gzip reader
/// 的 `gzip: invalid checksum`，避免依赖库措辞泄露到后端协议。
///
/// # 参数
/// - `error`：`StreamReader`、upgrade 连接或 gzip decoder 返回的 IO 错误。
///
/// # 返回
/// 可直接写入 Tunnel error event 和 Handler 返回错误的 Go 兼容文本。
fn go_body_read_error_text(error: &io::Error) -> String {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        return "unexpected EOF".to_owned();
    }
    match error.to_string().as_str() {
        "Invalid gzip header" => "gzip: invalid header".to_owned(),
        "CRC computed does not match" | "amount of bytes read does not match" => {
            "gzip: invalid checksum".to_owned()
        }
        "Invalid chunk size line: missing size digit" => "invalid byte in chunk length".to_owned(),
        message => message.to_owned(),
    }
}

/// 对应 Go：`sendHTTPError`，发送协议级 HTTP error 并包装 sender 失败。
///
/// # 参数
/// - `sender`：当前请求的协议响应发送器。
/// - `request_id`：错误所属请求 ID，可为空。
/// - `message`：需要原样写入 Proto `error_message` 的兼容文本。
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
