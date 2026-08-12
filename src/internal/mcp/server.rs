//! loopback Streamable HTTP 与 JSON-RPC 兼容适配层。
//!
//! 这里实现 mcp-go v0.37 在本项目实际使用的协议面：`initialize`、`ping`、
//! `tools/list`、`tools/call` 和通知。业务服务器只看到稳定的本地类型；官方 `rmcp`
//! SDK 被限制在 `rmcp_adapter.rs` 的模型校验边界，不会改变 mcp-go 的 wire 细节。

use std::{
    convert::Infallible,
    future::Future as _,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::{ConnectInfo, State},
    http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header},
    response::Response,
    routing::any,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use rand::TryRngCore;
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Instant, Sleep};
use tokio_util::sync::CancellationToken;

use super::rmcp_adapter;
use super::types::{
    BaseServer, CallToolRequest, MCPServer, McpError, McpResult, ServerConfig, ServerRuntime,
    ToolConfig,
};

/// MCP Streamable HTTP 的固定 endpoint。
pub const MCP_ENDPOINT_PATH: &str = "/mcp";
const JSON_RPC_VERSION: &str = "2.0";
const LATEST_PROTOCOL_VERSION: &str = "2025-06-18";
const VALID_PROTOCOL_VERSIONS: [&str; 3] = [LATEST_PROTOCOL_VERSION, "2025-03-26", "2024-11-05"];
const MAX_REQUEST_BODY: usize = 10 * 1024 * 1024;
/// 对应 Go `http.Server.ReadHeaderTimeout`。
pub(super) const MCP_READ_HEADER_TIMEOUT: Duration = Duration::from_secs(10);
/// 对应 Go `http.Server.ReadTimeout`。
pub(super) const MCP_READ_TIMEOUT: Duration = Duration::from_secs(2 * 60);
/// 对应 Go `http.Server.WriteTimeout`。
pub(super) const MCP_WRITE_TIMEOUT: Duration = Duration::from_secs(2 * 60);
/// 仅为识别请求边界而缓存的最大 header 字节数。
///
/// Go `net/http` 默认允许约 1 MiB header；额外的 4 KiB 为内部读取缓冲余量。
/// 超过该界限后仍把原始字节交给 Hyper，但停止自行推断后续请求边界，避免攻击者
/// 通过一个永不结束的 header 让兼容层无限分配内存。
const MAX_TRACKED_HEADER_BYTES: usize = 1024 * 1024 + 4096;
/// chunk-size 或单行 trailer 的边界跟踪上限。
///
/// 这不是 HTTP 接收上限；超出后由 Hyper 决定协议错误。兼容层只停止继续缓存该行。
const MAX_TRACKED_CHUNK_LINE_BYTES: usize = 8192;

/// 跨 socket read 边界识别 HTTP header 结束标记。
#[derive(Debug, Default)]
pub(super) struct HttpHeaderTerminator {
    matched: usize,
    complete: bool,
}

impl HttpHeaderTerminator {
    /// 观察新读到的字节，只有完整出现 `\r\n\r\n` 时才返回 true。
    ///
    /// 返回值一旦为 true 便保持 true，使 read wrapper 不会因 body 中
    /// 偶然出现同样字节而重置 request deadline。
    pub(super) fn observe(&mut self, bytes: &[u8]) -> bool {
        const TERMINATOR: &[u8; 4] = b"\r\n\r\n";
        if self.complete {
            return true;
        }
        for &byte in bytes {
            if byte == TERMINATOR[self.matched] {
                self.matched += 1;
                if self.matched == TERMINATOR.len() {
                    self.complete = true;
                    return true;
                }
            } else {
                self.matched = usize::from(byte == TERMINATOR[0]);
            }
        }
        false
    }
}

/// 从请求 header 推断原始 HTTP/1 body 的边界类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HttpRequestBodyKind {
    /// 没有 framing header，因此请求在 header 结束时完成。
    None,
    /// `Content-Length` 指定的固定长度 body。
    Fixed(u64),
    /// `Transfer-Encoding: chunked` 指定的分块 body。
    Chunked,
    /// header 非法或使用了本兼容层不负责解释的 framing。
    Untrackable,
}

/// HTTP/1 请求边界解析器的当前阶段。
#[derive(Debug)]
enum HttpRequestReadPhase {
    /// 正在缓存并寻找当前请求的 `\r\n\r\n`。
    Header {
        bytes: Vec<u8>,
        terminator: HttpHeaderTerminator,
    },
    /// 正在消费固定长度 body。
    FixedBody { remaining: u64 },
    /// 正在读取下一条十六进制 chunk-size 行。
    ChunkSize { line: Vec<u8> },
    /// 正在跳过 chunk payload。
    ChunkData { remaining: u64 },
    /// 正在验证 chunk payload 后强制存在的 CRLF。
    ChunkDataTerminator { matched: usize },
    /// 已读到零长度 chunk，正在逐行消费可选 trailer。
    ChunkTrailers { line: Vec<u8> },
    /// 上一个请求已完成；按 Go `net/http` 的 idle `Peek(4)` 语义等待四字节。
    Idle { prefix: Vec<u8> },
    /// 无法可靠追踪当前消息；原始 I/O 继续交给 Hyper 处理和报错。
    Untrackable,
}

/// 请求边界解析过程中产生的 deadline 事件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HttpRequestEvent {
    /// idle 阶段已经收到四字节，新的请求正式开始计时。
    Started,
    /// 当前请求的完整 header 已经到达。
    HeaderComplete,
    /// 当前请求的 body（若有）已经完整到达。
    Complete,
}

/// 只观察原始请求字节、不消费或改写字节的 HTTP/1 请求边界跟踪器。
///
/// 该跟踪器刻意独立于 Hyper：网络层仍收到完全相同的字节；这里仅根据
/// `Content-Length`、`Transfer-Encoding: chunked` 和无 body framing 计算下一次
/// keep-alive 请求何时开始。这样一个 socket read 同时包含 header、body 和下一条
/// pipelined 请求时，deadline 事件也不会丢失。
#[derive(Debug)]
pub(super) struct HttpRequestTracker {
    phase: HttpRequestReadPhase,
}

impl Default for HttpRequestTracker {
    /// 创建与 [`HttpRequestTracker::new`] 相同的首请求 header 状态。
    fn default() -> Self {
        Self::new()
    }
}

impl HttpRequestTracker {
    /// 创建跟踪器；首个请求从 accept 时刻已经开始，因此初始状态直接读取 header。
    pub(super) fn new() -> Self {
        Self {
            phase: Self::new_header_phase(Vec::new()),
        }
    }

    /// 观察一次 socket read 新增的字节，并按发生顺序返回请求生命周期事件。
    ///
    /// 输入可以跨任意边界切分，也可以同时携带多个 pipelined 请求。解析失败不会
    /// 拒绝请求，而会进入 `Untrackable`，让 Hyper 保持最终协议裁决权。
    pub(super) fn observe(&mut self, bytes: &[u8]) -> Vec<HttpRequestEvent> {
        let mut events = Vec::new();
        let mut offset = 0;

        while offset < bytes.len() {
            let phase = std::mem::replace(&mut self.phase, HttpRequestReadPhase::Untrackable);
            match phase {
                HttpRequestReadPhase::Header {
                    bytes: mut header,
                    mut terminator,
                } => {
                    let mut complete = false;
                    while offset < bytes.len() {
                        let byte = bytes[offset];
                        offset += 1;
                        header.push(byte);
                        if terminator.observe(std::slice::from_ref(&byte)) {
                            complete = true;
                            break;
                        }
                        if header.len() > MAX_TRACKED_HEADER_BYTES {
                            self.phase = HttpRequestReadPhase::Untrackable;
                            return events;
                        }
                    }

                    if !complete {
                        self.phase = HttpRequestReadPhase::Header {
                            bytes: header,
                            terminator,
                        };
                        continue;
                    }

                    events.push(HttpRequestEvent::HeaderComplete);
                    match request_body_kind(&header) {
                        HttpRequestBodyKind::None | HttpRequestBodyKind::Fixed(0) => {
                            events.push(HttpRequestEvent::Complete);
                            self.phase = HttpRequestReadPhase::Idle { prefix: Vec::new() };
                        }
                        HttpRequestBodyKind::Fixed(remaining) => {
                            self.phase = HttpRequestReadPhase::FixedBody { remaining };
                        }
                        HttpRequestBodyKind::Chunked => {
                            self.phase = HttpRequestReadPhase::ChunkSize { line: Vec::new() };
                        }
                        HttpRequestBodyKind::Untrackable => {
                            self.phase = HttpRequestReadPhase::Untrackable;
                            return events;
                        }
                    }
                }
                HttpRequestReadPhase::FixedBody { remaining } => {
                    let consumed = remaining.min((bytes.len() - offset) as u64);
                    offset += consumed as usize;
                    let remaining = remaining - consumed;
                    if remaining == 0 {
                        events.push(HttpRequestEvent::Complete);
                        self.phase = HttpRequestReadPhase::Idle { prefix: Vec::new() };
                    } else {
                        self.phase = HttpRequestReadPhase::FixedBody { remaining };
                    }
                }
                HttpRequestReadPhase::ChunkSize { mut line } => {
                    let complete = Self::consume_crlf_line(bytes, &mut offset, &mut line);
                    if line.len() > MAX_TRACKED_CHUNK_LINE_BYTES {
                        self.phase = HttpRequestReadPhase::Untrackable;
                        return events;
                    }
                    if !complete {
                        self.phase = HttpRequestReadPhase::ChunkSize { line };
                        continue;
                    }
                    let Some(size) = parse_chunk_size(&line[..line.len() - 2]) else {
                        self.phase = HttpRequestReadPhase::Untrackable;
                        return events;
                    };
                    if size == 0 {
                        self.phase = HttpRequestReadPhase::ChunkTrailers { line: Vec::new() };
                    } else {
                        self.phase = HttpRequestReadPhase::ChunkData { remaining: size };
                    }
                }
                HttpRequestReadPhase::ChunkData { remaining } => {
                    let consumed = remaining.min((bytes.len() - offset) as u64);
                    offset += consumed as usize;
                    let remaining = remaining - consumed;
                    if remaining == 0 {
                        self.phase = HttpRequestReadPhase::ChunkDataTerminator { matched: 0 };
                    } else {
                        self.phase = HttpRequestReadPhase::ChunkData { remaining };
                    }
                }
                HttpRequestReadPhase::ChunkDataTerminator { mut matched } => {
                    const CRLF: &[u8; 2] = b"\r\n";
                    while offset < bytes.len() && matched < CRLF.len() {
                        if bytes[offset] != CRLF[matched] {
                            self.phase = HttpRequestReadPhase::Untrackable;
                            return events;
                        }
                        offset += 1;
                        matched += 1;
                    }
                    if matched == CRLF.len() {
                        self.phase = HttpRequestReadPhase::ChunkSize { line: Vec::new() };
                    } else {
                        self.phase = HttpRequestReadPhase::ChunkDataTerminator { matched };
                    }
                }
                HttpRequestReadPhase::ChunkTrailers { mut line } => {
                    let complete = Self::consume_crlf_line(bytes, &mut offset, &mut line);
                    if line.len() > MAX_TRACKED_CHUNK_LINE_BYTES {
                        self.phase = HttpRequestReadPhase::Untrackable;
                        return events;
                    }
                    if !complete {
                        self.phase = HttpRequestReadPhase::ChunkTrailers { line };
                        continue;
                    }
                    if line == b"\r\n" {
                        events.push(HttpRequestEvent::Complete);
                        self.phase = HttpRequestReadPhase::Idle { prefix: Vec::new() };
                    } else {
                        self.phase = HttpRequestReadPhase::ChunkTrailers { line: Vec::new() };
                    }
                }
                HttpRequestReadPhase::Idle { mut prefix } => {
                    // Go 在响应完成后先用 idle deadline 执行 Peek(4)，只有收齐四字节
                    // 才进入下一次 readRequest 并启动新的十秒 header deadline。
                    let needed = 4 - prefix.len();
                    let consumed = needed.min(bytes.len() - offset);
                    prefix.extend_from_slice(&bytes[offset..offset + consumed]);
                    offset += consumed;
                    if prefix.len() < 4 {
                        self.phase = HttpRequestReadPhase::Idle { prefix };
                        continue;
                    }

                    events.push(HttpRequestEvent::Started);
                    self.phase = Self::new_header_phase(prefix);
                }
                HttpRequestReadPhase::Untrackable => {
                    // Hyper 仍会读取并验证所有剩余字节；兼容层不在无法证明边界时
                    // 猜测下一请求，否则可能错误地延长攻击流量的 deadline。
                    self.phase = HttpRequestReadPhase::Untrackable;
                    return events;
                }
            }
        }

        events
    }

    /// 返回当前读 deadline 应显示的阶段，用于生成可诊断的 timeout 文本。
    pub(super) fn deadline_stage(&self) -> McpReadDeadlineStage {
        match self.phase {
            HttpRequestReadPhase::Header { .. } => McpReadDeadlineStage::Header,
            HttpRequestReadPhase::Idle { .. } => McpReadDeadlineStage::Idle,
            HttpRequestReadPhase::FixedBody { .. }
            | HttpRequestReadPhase::ChunkSize { .. }
            | HttpRequestReadPhase::ChunkData { .. }
            | HttpRequestReadPhase::ChunkDataTerminator { .. }
            | HttpRequestReadPhase::ChunkTrailers { .. }
            | HttpRequestReadPhase::Untrackable => McpReadDeadlineStage::Body,
        }
    }

    /// 判断上一个请求是否完成，供响应写入时把 idle deadline 移到响应完成附近。
    pub(super) fn is_idle(&self) -> bool {
        matches!(self.phase, HttpRequestReadPhase::Idle { .. })
    }

    /// 以已经在 idle 阶段收集的前缀创建 header 状态，并重放终止符检测状态。
    fn new_header_phase(bytes: Vec<u8>) -> HttpRequestReadPhase {
        let mut terminator = HttpHeaderTerminator::default();
        terminator.observe(&bytes);
        HttpRequestReadPhase::Header { bytes, terminator }
    }

    /// 从输入中追加一条以 CRLF 结束的行，并返回本次是否已经收齐行尾。
    fn consume_crlf_line(bytes: &[u8], offset: &mut usize, line: &mut Vec<u8>) -> bool {
        while *offset < bytes.len() {
            line.push(bytes[*offset]);
            *offset += 1;
            if line.ends_with(b"\r\n") {
                return true;
            }
        }
        false
    }
}

/// 根据完整原始 header 选择请求 body framing。
///
/// 与 Go 一样，合法 `Transfer-Encoding: chunked` 优先于 `Content-Length`；重复且相同
/// 的 Content-Length 可以接受，冲突值或无法解析的 framing 交回 Hyper 判定。
fn request_body_kind(header: &[u8]) -> HttpRequestBodyKind {
    let Some(header) = header.strip_suffix(b"\r\n\r\n") else {
        return HttpRequestBodyKind::Untrackable;
    };
    let mut lines = header.split(|byte| *byte == b'\n');
    if lines.next().is_none() {
        return HttpRequestBodyKind::Untrackable;
    }

    let mut content_length = None;
    let mut transfer_encoding = None;
    for raw_line in lines {
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        let Some(colon) = line.iter().position(|byte| *byte == b':') else {
            return HttpRequestBodyKind::Untrackable;
        };
        let name = trim_ascii_bytes(&line[..colon]);
        let value = trim_ascii_bytes(&line[colon + 1..]);
        if name.eq_ignore_ascii_case(b"content-length") {
            let Ok(value) = std::str::from_utf8(value) else {
                return HttpRequestBodyKind::Untrackable;
            };
            let Ok(value) = value.parse::<u64>() else {
                return HttpRequestBodyKind::Untrackable;
            };
            if content_length.is_some_and(|previous| previous != value) {
                return HttpRequestBodyKind::Untrackable;
            }
            content_length = Some(value);
        } else if name.eq_ignore_ascii_case(b"transfer-encoding") {
            if transfer_encoding.is_some() {
                return HttpRequestBodyKind::Untrackable;
            }
            transfer_encoding = Some(value);
        }
    }

    if let Some(value) = transfer_encoding {
        // Go HTTP/1 server 对请求只接受单一的 chunked 编码；其他组合由协议层拒绝。
        if value.eq_ignore_ascii_case(b"chunked") {
            return HttpRequestBodyKind::Chunked;
        }
        return HttpRequestBodyKind::Untrackable;
    }
    content_length.map_or(HttpRequestBodyKind::None, HttpRequestBodyKind::Fixed)
}

/// 去除 HTTP token/value 两侧的 ASCII 空白，不改变中间字节。
fn trim_ascii_bytes(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

/// 解析 chunk-size 行中分号前的十六进制大小；扩展参数由 Hyper 最终校验。
fn parse_chunk_size(line: &[u8]) -> Option<u64> {
    let size = line.split(|byte| *byte == b';').next()?;
    let size = trim_ascii_bytes(size);
    let size = std::str::from_utf8(size).ok()?;
    if size.is_empty() {
        return None;
    }
    u64::from_str_radix(size, 16).ok()
}

/// 当前 read deadline 所属的 Go `net/http` 阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum McpReadDeadlineStage {
    /// 正在读取请求 header。
    Header,
    /// 正在读取固定长度或 chunked body。
    Body,
    /// 响应结束后等待下一请求的首四字节。
    Idle,
}

impl McpReadDeadlineStage {
    /// 返回稳定的英文阶段名，保持现有 I/O 错误文本便于日志检索。
    fn error_label(self) -> &'static str {
        match self {
            Self::Header => "read header",
            Self::Body => "read",
            Self::Idle => "idle",
        }
    }
}

/// 可独立测试的 MCP HTTP deadline 调度状态。
///
/// 所有时间均为绝对 `Instant`：首请求从 accept 时刻计时；后续请求在 idle 阶段收齐
/// 四字节时计时；header 完成时 write deadline 从当时起算，而整体 read deadline
/// 仍从该请求开始时刻起算。
#[derive(Debug)]
pub(super) struct McpDeadlineState {
    tracker: HttpRequestTracker,
    request_started_at: Instant,
    read_deadline: Instant,
    write_deadline: Instant,
}

impl McpDeadlineState {
    /// 为新 accept 的连接建立首请求 deadline。
    pub(super) fn new(accepted_at: Instant) -> Self {
        Self {
            tracker: HttpRequestTracker::new(),
            request_started_at: accepted_at,
            read_deadline: accepted_at + MCP_READ_HEADER_TIMEOUT,
            // header 完成前不会有正常业务写入；该值只是安全占位，并在
            // `HeaderComplete` 事件发生时按 Go 语义覆盖。
            write_deadline: accepted_at + MCP_WRITE_TIMEOUT,
        }
    }

    /// 观察新读取的字节并按请求事件重算绝对 deadline。
    pub(super) fn observe_read(&mut self, bytes: &[u8], now: Instant) {
        for event in self.tracker.observe(bytes) {
            match event {
                HttpRequestEvent::Started => {
                    self.request_started_at = now;
                    self.read_deadline = now + MCP_READ_HEADER_TIMEOUT;
                }
                HttpRequestEvent::HeaderComplete => {
                    self.read_deadline = self.request_started_at + MCP_READ_TIMEOUT;
                    self.write_deadline = now + MCP_WRITE_TIMEOUT;
                }
                HttpRequestEvent::Complete => {
                    // body 完成并不等于响应完成。这里保留“请求开始 + 两分钟”的整体
                    // read deadline；响应产生实际写入后才切换到新的 idle 窗口。
                }
            }
        }
    }

    /// 成功写入或 flush 响应时，把 idle deadline 推进到响应进度之后两分钟。
    ///
    /// Hyper 在固定响应结束处完成最后一次写入/flush；长时间 handler 因而不会消耗
    /// 下一条 keep-alive 请求的 idle 窗口。流式响应的每次进度也会续期，但在响应
    /// 结束前 Hyper 不会分派下一请求，与 Go 的串行 HTTP/1 生命周期一致。
    pub(super) fn observe_response_progress(&mut self, now: Instant) {
        if self.tracker.is_idle() {
            self.read_deadline = now + MCP_READ_TIMEOUT;
        }
    }

    /// 返回当前 read deadline 的绝对时间。
    pub(super) fn read_deadline(&self) -> Instant {
        self.read_deadline
    }

    /// 返回当前 write deadline 的绝对时间。
    pub(super) fn write_deadline(&self) -> Instant {
        self.write_deadline
    }

    /// 返回当前请求读取阶段。
    pub(super) fn read_stage(&self) -> McpReadDeadlineStage {
        self.tracker.deadline_stage()
    }
}

/// 为单条 MCP TCP 连接实施 Go `http.Server` 的逐请求 header/read/write deadline。
///
/// 该包装不改变网络字节，仅把 [`McpDeadlineState`] 计算出的绝对时间同步到 Tokio
/// `Sleep`。每条 keep-alive 请求都会重新经历 idle、header、body 三阶段；超时作为
/// `TimedOut` I/O 错误交给 Hyper 关闭连接。
struct McpTimeoutStream {
    inner: TcpStream,
    deadlines: McpDeadlineState,
    read_deadline: Pin<Box<Sleep>>,
    write_deadline: Pin<Box<Sleep>>,
}

impl McpTimeoutStream {
    /// 包装新 accept 的 loopback stream 并启动十秒 header deadline。
    fn new(inner: TcpStream) -> Self {
        let accepted_at = Instant::now();
        let deadlines = McpDeadlineState::new(accepted_at);
        Self {
            inner,
            read_deadline: Box::pin(tokio::time::sleep_until(deadlines.read_deadline())),
            write_deadline: Box::pin(tokio::time::sleep_until(deadlines.write_deadline())),
            deadlines,
        }
    }

    /// 生成带明确阶段名的 I/O timeout，便于 Hyper 诊断日志定位。
    fn timeout_error(stage: &str) -> io::Error {
        io::Error::new(io::ErrorKind::TimedOut, format!("MCP HTTP {stage} timeout"))
    }

    /// 把纯状态中最新的绝对时间同步到两个 Tokio deadline future。
    fn reset_deadline_sleeps(&mut self) {
        self.read_deadline
            .as_mut()
            .reset(self.deadlines.read_deadline());
        self.write_deadline
            .as_mut()
            .reset(self.deadlines.write_deadline());
    }

    /// 响应产生实际 I/O 进度后刷新 idle deadline，并同步 read sleep。
    fn observe_response_progress(&mut self) {
        self.deadlines.observe_response_progress(Instant::now());
        self.read_deadline
            .as_mut()
            .reset(self.deadlines.read_deadline());
    }
}

impl AsyncRead for McpTimeoutStream {
    /// 在每次读取前检查当前阶段 deadline，并跟踪同一 read 内的多个请求边界。
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.read_deadline.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(Self::timeout_error(
                this.deadlines.read_stage().error_label(),
            )));
        }
        let previous_len = buffer.filled().len();
        match Pin::new(&mut this.inner).poll_read(context, buffer) {
            Poll::Ready(Ok(())) => {
                let added = &buffer.filled()[previous_len..];
                if !added.is_empty() {
                    this.deadlines.observe_read(added, Instant::now());
                    this.reset_deadline_sleeps();
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl AsyncWrite for McpTimeoutStream {
    /// 在响应写入前检查两分钟 write deadline。
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.write_deadline.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(Self::timeout_error("write")));
        }
        match Pin::new(&mut this.inner).poll_write(context, bytes) {
            Poll::Ready(Ok(written)) => {
                if written != 0 {
                    this.observe_response_progress();
                }
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    /// flush 也属于 Go write deadline 覆盖的 socket 写阶段。
    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.write_deadline.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(Self::timeout_error("write")));
        }
        match Pin::new(&mut this.inner).poll_flush(context) {
            Poll::Ready(Ok(())) => {
                this.observe_response_progress();
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }

    /// shutdown 不得绕过 write deadline，避免停滞 peer 阻塞 server stop。
    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.write_deadline.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(Self::timeout_error("write")));
        }
        Pin::new(&mut this.inner).poll_shutdown(context)
    }
}

/// 把 Axum accept loop 产生的每条 TcpStream 包装为 [`McpTimeoutStream`]。
struct McpTimeoutListener {
    inner: TcpListener,
}

impl McpTimeoutListener {
    /// 保留已绑定 listener 的随机 loopback 端口，仅改变 accept 后的 I/O。
    fn new(inner: TcpListener) -> Self {
        Self { inner }
    }
}

impl axum::serve::Listener for McpTimeoutListener {
    type Io = McpTimeoutStream;
    type Addr = std::net::SocketAddr;

    /// 持续 accept，短暂系统错误时退避一秒后重试，不终止整个 MCP server。
    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.inner.accept().await {
                Ok((stream, address)) => return (McpTimeoutStream::new(stream), address),
                Err(_) => {
                    // 临时 accept 错误只做内部退避，避免额外产生业务日志。
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// 返回底层 loopback listener 的实际绑定地址。
    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// Axum 为自定义 MCP listener 提供的连接信息适配类型。
///
/// Axum 0.8 只为其原生 `TcpListener` 实现了
/// `Connected<IncomingStream<TcpListener>> for SocketAddr`。这里的 listener 会把
/// `TcpStream` 包装成 [`McpTimeoutStream`]，因此必须由本地类型实现 `Connected`；
/// 不能直接为标准库的 `SocketAddr` 实现外部 trait，否则会违反 Rust orphan rule。
/// newtype 中仍保存 `accept` 返回的真实 peer 地址，不会退化成 server 的本地地址，
/// 也不会改变后续日志中的 `remote_addr` 文本。
#[derive(Clone, Copy, Debug)]
struct McpConnectInfo(SocketAddr);

impl axum::extract::connect_info::Connected<axum::serve::IncomingStream<'_, McpTimeoutListener>>
    for McpConnectInfo
{
    /// 从 Axum 当前接收的连接中复制真实 peer 地址。
    ///
    /// `IncomingStream::remote_addr` 引用的值正是 `McpTimeoutListener::accept`
    /// 与超时 I/O wrapper 一并返回的地址；复制到请求 extension 后，其生命周期不再
    /// 依赖 accept loop 中的临时 `IncomingStream`。
    fn connect_info(stream: axum::serve::IncomingStream<'_, McpTimeoutListener>) -> Self {
        Self(*stream.remote_addr())
    }
}

#[derive(Clone)]
struct RpcServerState {
    name: String,
    version: String,
    port: u16,
    bearer_token: String,
    tools: Arc<Vec<ToolConfig>>,
    cancellation: CancellationToken,
}

/// JSON-RPC dispatcher 的 HTTP 级输出。
#[derive(Debug)]
pub struct RpcDispatch {
    /// mcp-go 对通知使用 202，其余 JSON-RPC 响应使用 200。
    pub status: StatusCode,
    /// 通知没有 body；请求结果始终是 JSON object。
    pub body: Option<Value>,
}

/// 在 Axum response 构造前记录首个状态码和已写 body。
///
/// 这保留 Go `responseWriter` 的“第一次 WriteHeader 生效、首次 Write 隐式 200”
/// 语义；内存 body 写入不会产生 Go 底层 socket writer 的 I/O 错误，真正发送错误
/// 仍由 Hyper 连接层管理。
struct ResponseWriter {
    status_code: StatusCode,
    written: bool,
    body: Vec<u8>,
}

impl ResponseWriter {
    /// 对应 Go `responseWriter` 的零值初始化语义。
    ///
    /// 首次写入前预置 200，但仍把 `written` 保持为 false，使显式 `WriteHeader`
    /// 可以覆盖默认状态；body 从空缓冲开始，不产生网络副作用。
    fn new() -> Self {
        Self {
            status_code: StatusCode::OK,
            written: false,
            body: Vec::new(),
        }
    }

    /// 记录首个 HTTP 状态码；后续重复调用保持幂等。
    fn write_header(&mut self, status_code: StatusCode) {
        if self.written {
            return;
        }
        self.status_code = status_code;
        self.written = true;
    }

    /// 追加响应字节；尚未写 header 时隐式选择 200。
    fn write(&mut self, bytes: &[u8]) -> usize {
        if !self.written {
            self.write_header(StatusCode::OK);
        }
        self.body.extend_from_slice(bytes);
        bytes.len()
    }

    /// 把已记录的状态码和 body 一次性转换为 Axum response。
    ///
    /// 该转换消费 writer，避免转换后继续写入导致 Go `ResponseWriter` 不存在的状态；
    /// 状态码来自 `http::StatusCode`，因此 builder 失败只代表内部不变量被破坏。
    fn into_response(self) -> Response<Body> {
        Response::builder()
            .status(self.status_code)
            .body(Body::from(self.body))
            .expect("recorded HTTP status is valid")
    }
}

/// 处理一条已经通过 Bearer 验证的 JSON-RPC 消息。
///
/// ID 保持原始 JSON 数字或字符串类型；协议/params 中无法识别的字段由请求类型
/// 保留，但不会影响当前方法分派。该函数公开是为了让 adapter 和定向协议测试复用。
pub async fn dispatch_json_rpc(
    server_name: &str,
    server_version: &str,
    tools: &[ToolConfig],
    cancellation: CancellationToken,
    body: &[u8],
) -> RpcDispatch {
    let value = match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(value)) => Value::Object(value),
        _ => {
            return RpcDispatch {
                // mcp-go Streamable HTTP 在 JSON decoder 失败时同时使用 HTTP 400
                // 和 JSON-RPC Parse error；只回 200 会让客户端误判为成功 transport。
                status: StatusCode::BAD_REQUEST,
                body: Some(json_rpc_error(
                    Value::Null,
                    -32700,
                    "request body is not valid json",
                )),
            };
        }
    };

    let id = value.get("id").cloned().unwrap_or(Value::Null);
    let jsonrpc = value
        .get("jsonrpc")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if jsonrpc != JSON_RPC_VERSION {
        return RpcDispatch {
            status: StatusCode::OK,
            body: Some(json_rpc_error(id, -32600, "Invalid JSON-RPC version")),
        };
    }

    // mcp-go 把所有没有 ID 的消息当通知；未知通知也只返回 202。
    if id.is_null() {
        return RpcDispatch {
            status: StatusCode::ACCEPTED,
            body: None,
        };
    }

    let method = value
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let result = match method {
        "initialize" => {
            let requested_version = value
                .pointer("/params/protocolVersion")
                .and_then(Value::as_str)
                .unwrap_or("2025-03-26");
            let protocol_version = if VALID_PROTOCOL_VERSIONS.contains(&requested_version) {
                requested_version
            } else {
                LATEST_PROTOCOL_VERSION
            };
            let capabilities = if tools.is_empty() {
                json!({})
            } else {
                // AddTool 会隐式启用 listChanged=true，这是 mcp-go v0.37 的默认值。
                json!({"tools": {"listChanged": true}})
            };
            Ok(json!({
                "protocolVersion": protocol_version,
                "capabilities": capabilities,
                "serverInfo": {"name": server_name, "version": server_version}
            }))
        }
        "ping" => Ok(json!({})),
        "tools/list" => {
            let mut listed = tools
                .iter()
                .map(|tool| tool.tool.clone())
                .collect::<Vec<_>>();
            listed.sort_by(|left, right| left.name.cmp(&right.name));
            match rmcp_adapter::serialize_tool_declarations(&listed) {
                Ok(listed) => Ok(json!({"tools": listed})),
                Err(error) => Err((-32603, error.to_string())),
            }
        }
        "tools/call" => {
            let request = match serde_json::from_value::<CallToolRequest>(value.clone()) {
                Ok(request) => request,
                Err(error) => {
                    return RpcDispatch {
                        status: StatusCode::OK,
                        body: Some(json_rpc_error(
                            id,
                            -32600,
                            &format!("unparsable tools/call request: {error}"),
                        )),
                    };
                }
            };
            let Some(tool) = tools
                .iter()
                .find(|tool| tool.tool.name == request.params.name)
            else {
                return RpcDispatch {
                    status: StatusCode::OK,
                    body: Some(json_rpc_error(
                        id,
                        -32602,
                        &format!("tool '{}' not found: tool not found", request.params.name),
                    )),
                };
            };

            match tool.handler.call(cancellation, request).await {
                Ok(result) => rmcp_adapter::serialize_call_tool_result(result)
                    .map_err(|error| (-32603, error.to_string())),
                Err(error) => Err((-32603, error.to_string())),
            }
        }
        _ => Err((-32601, format!("Method {method} not found"))),
    };

    RpcDispatch {
        status: StatusCode::OK,
        body: Some(match result {
            Ok(result) => json!({"jsonrpc": JSON_RPC_VERSION, "id": id, "result": result}),
            Err((code, message)) => json_rpc_error(id, code, &message),
        }),
    }
}

/// 创建字段集合和序列化顺序均与 mcp-go 一致的 JSON-RPC error。
fn json_rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": JSON_RPC_VERSION,
        "id": id,
        "error": {"code": code, "message": message}
    })
}

/// 处理所有 HTTP 方法，在 endpoint dispatch 前执行精确 Bearer 校验。
async fn handle_http(
    State(state): State<Arc<RpcServerState>>,
    ConnectInfo(McpConnectInfo(remote_address)): ConnectInfo<McpConnectInfo>,
    request: Request<Body>,
) -> Response<Body> {
    let started = std::time::Instant::now();
    let method = request.method().as_str().to_owned();
    let path = request.uri().path().to_owned();
    let remote_address = remote_address.to_string();
    let user_agent = request
        .headers()
        .get(header::USER_AGENT)
        // 非法 UTF-8 以替换字符记录。不能使用 `HeaderValue::to_str` 后回退为空串，
        // 否则诊断值
        // 会在包含 obs-text 的合法 HTTP/1 请求上与 Go 不同。
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
        .unwrap_or_default();
    tracing::info!("server_name" = ?(&state.name), "method" = ?(&method), "path" = ?(&path), "remote_addr" = ?(&remote_address), "user_agent" = ?(&user_agent), "port" = ?(state.port), "{}", "Incoming MCP request");

    let expected = format!("Bearer {}", state.bearer_token);
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == expected);
    if !authorized {
        tracing::warn!("remote_addr" = ?(&remote_address), "user_agent" = ?(&user_agent), "{}", "Unauthorized MCP request");
        return text_response(StatusCode::UNAUTHORIZED, "Unauthorized\n");
    }

    tracing::debug!("server_name" = ?(&state.name), "method" = ?(&method), "path" = ?(&path), "{}", "Authorized MCP request");

    let response = if request.uri().path() != MCP_ENDPOINT_PATH {
        text_response(StatusCode::NOT_FOUND, "404 page not found\n")
    } else {
        match *request.method() {
            Method::POST => handle_post(Arc::clone(&state), request).await,
            Method::DELETE => Response::builder()
                .status(StatusCode::OK)
                .body(Body::empty())
                .expect("static status is valid"),
            // mcp-go 的 GET notification stream 在 client 断开前保持连接；这里用 pending
            // body 表达同一生命周期，并仍由 HTTP server cancellation 统一回收。
            Method::GET => Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .header(header::CACHE_CONTROL, "no-cache")
                .header(header::CONNECTION, "keep-alive")
                .body(Body::from_stream(futures_util::stream::pending::<
                    Result<Bytes, Infallible>,
                >()))
                .expect("static headers are valid"),
            _ => text_response(StatusCode::NOT_FOUND, "404 page not found\n"),
        }
    };
    let status_code = response.status().as_u16();
    let duration_ms = started.elapsed().as_millis();
    match status_code {
        500.. => {
            tracing::error!("server_name" = ?(&state.name), "status_code" = ?(status_code), "duration_ms" = ?(duration_ms), "method" = ?(&method), "path" = ?(&path), "{}", "MCP request completed")
        }
        400..=499 => {
            tracing::warn!("server_name" = ?(&state.name), "status_code" = ?(status_code), "duration_ms" = ?(duration_ms), "method" = ?(&method), "path" = ?(&path), "{}", "MCP request completed")
        }
        _ => {
            tracing::info!("server_name" = ?(&state.name), "status_code" = ?(status_code), "duration_ms" = ?(duration_ms), "method" = ?(&method), "path" = ?(&path), "{}", "MCP request completed")
        }
    }
    response
}

/// 验证 Streamable HTTP content-type、读取受限 body 并执行 JSON-RPC dispatch。
async fn handle_post(state: Arc<RpcServerState>, request: Request<Body>) -> Response<Body> {
    if !has_json_content_type(request.headers()) {
        return text_response(
            StatusCode::BAD_REQUEST,
            "Invalid content type: must be 'application/json'\n",
        );
    }

    // child token 同时继承 server stop；DropGuard 让 Hyper/Axum 因客户端
    // 断开而丢弃 handler future 时也立即取消 tool 子进程。
    let request_cancellation = state.cancellation.child_token();
    let _request_drop_guard = request_cancellation.clone().drop_guard();
    let bytes = match to_bytes(request.into_body(), MAX_REQUEST_BODY).await {
        Ok(bytes) => bytes,
        Err(error) => {
            return json_response(
                StatusCode::OK,
                &json_rpc_error(
                    Value::Null,
                    -32700,
                    &format!("read request body error: {error}"),
                ),
            );
        }
    };
    let dispatch = dispatch_json_rpc(
        &state.name,
        &state.version,
        &state.tools,
        request_cancellation,
        &bytes,
    )
    .await;
    dispatch.body.map_or_else(
        || {
            Response::builder()
                .status(dispatch.status)
                .body(Body::empty())
                .expect("static status is valid")
        },
        |body| json_response(dispatch.status, &body),
    )
}

/// 接受 `application/json` 及其合法参数，拒绝缺失或其他媒体类型。
fn has_json_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim() == "application/json")
}

/// 构造 Go `http.Error` 风格的纯文本响应。
fn text_response(status: StatusCode, body: &'static str) -> Response<Body> {
    let mut writer = ResponseWriter::new();
    writer.write_header(status);
    writer.write(body.as_bytes());
    let mut response = writer.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    response
}

/// 序列化 JSON 并追加 Go `json.Encoder.Encode` 产生的换行。
fn json_response(status: StatusCode, value: &Value) -> Response<Body> {
    let mut bytes = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());
    bytes.push(b'\n');
    let mut writer = ResponseWriter::new();
    writer.write_header(status);
    writer.write(&bytes);
    let mut response = writer.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

#[async_trait::async_trait]
impl MCPServer for BaseServer {
    /// 返回基础 server 名称。
    fn get_name(&self) -> String {
        self.name.clone()
    }

    /// 基础 server 本身没有工具。
    fn get_tools(self: Arc<Self>) -> Vec<ToolConfig> {
        Vec::new()
    }

    /// 生成 token、监听 loopback 随机端口并启动 HTTP/heartbeat 任务。
    async fn start(&self, tools: Vec<ToolConfig>) -> McpResult<ServerConfig> {
        // 调试 token 是显式 opt-in；正常路径每次启动都从 OS CSPRNG 读取 32 字节。
        let bearer_token = if self.debug_hardcoded_token {
            let token = self.bearer_token();
            tracing::warn!("server_name" = ?(&self.name), "bearer_token" = ?(&token), "warning" = ?("this is insecure"), "only_for" = ?("local_debugging"), "{}", "Using hardcoded bearer token for debugging");
            token
        } else {
            let mut bytes = [0_u8; 32];
            rand::rngs::OsRng
                .try_fill_bytes(&mut bytes)
                .map_err(|error| {
                    McpError::message(format!("failed to generate bearer token: {error}"))
                })?;
            let token = URL_SAFE.encode(bytes);
            tracing::info!("Generated secure random bearer token");
            token
        };
        self.set_bearer_token(bearer_token.clone());

        let requested_address = "127.0.0.1:0";
        tracing::info!("address" = ?(requested_address), "{}", "Creating TCP listener");
        let listener = match TcpListener::bind(requested_address).await {
            Ok(listener) => listener,
            Err(error) => {
                tracing::error!("address" = ?(requested_address), "error" = ?(error.to_string()), "{}", "Failed to create TCP listener");
                return Err(McpError::message(format!(
                    "Failed to create listener: {error}"
                )));
            }
        };
        // 成功 bind 后 `local_addr` 对 Tokio TCP listener 是稳定内核元数据；若这一
        // 不变量被平台破坏，仍返回原有启动错误，但不伪造 Go `net.Listen` 失败日志。
        let listener_address = listener
            .local_addr()
            .map_err(|error| McpError::message(format!("Failed to create listener: {error}")))?;
        let port = listener_address.port();
        *self
            .port
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = port;
        tracing::info!("address" = ?(listener_address.to_string()), "port" = ?(port), "{}", "TCP listener created successfully");

        let server_cancellation = CancellationToken::new();
        let state = Arc::new(RpcServerState {
            name: self.name.clone(),
            version: self.version.clone(),
            port,
            bearer_token: bearer_token.clone(),
            tools: Arc::new(tools),
            cancellation: server_cancellation.clone(),
        });
        let router = Router::new().fallback(any(handle_http)).with_state(state);
        let serve_shutdown = server_cancellation.clone();
        let server_name = self.name.clone();
        let server_address = listener_address.to_string();
        // `net.OpError.Addr` 以 `any` 进入 Go JSONHandler 时会按 `net.TCPAddr`
        // 的导出字段编码，而不是调用 String；保留同样的嵌套值形状和字段顺序。
        let server_log_address = json!({
            "IP": listener_address.ip().to_string(),
            "Port": listener_address.port(),
            "Zone": "",
        });
        let server_task = tokio::spawn(async move {
            tracing::info!("address" = ?(&server_address), "server_name" = ?(&server_name), "{}", "MCP HTTP server starting to serve");
            let result = axum::serve(
                McpTimeoutListener::new(listener),
                router.into_make_service_with_connect_info::<McpConnectInfo>(),
            )
            .with_graceful_shutdown(serve_shutdown.cancelled_owned())
            .await;
            match result {
                Ok(()) => {
                    tracing::info!("server_name" = ?(&server_name), "{}", "MCP HTTP server stopped cleanly")
                }
                Err(error) if error.raw_os_error().is_some() => {
                    let timeout = error.kind() == io::ErrorKind::TimedOut;
                    let temporary = matches!(
                        error.kind(),
                        io::ErrorKind::TimedOut
                            | io::ErrorKind::Interrupted
                            | io::ErrorKind::WouldBlock
                    );
                    tracing::error!("error" = ?(error.to_string()), "server_name" = ?(&server_name), "network" = ?("tcp"), "address" = ?(server_log_address.clone()), "temporary" = ?(temporary), "timeout" = ?(timeout), "{}", "MCP HTTP server error");
                }
                Err(error) => {
                    tracing::error!("error" = ?(error.to_string()), "server_name" = ?(&server_name), "{}", "MCP HTTP server error")
                }
            }
        });

        // Go 在 `Stop` 中先完成 HTTP shutdown/显式 listener close，之后才关闭
        // heartbeat channel。使用独立 token，避免一个广播取消把日志阶段顺序颠倒。
        let heartbeat_cancellation = CancellationToken::new();
        let heartbeat_shutdown = heartbeat_cancellation.clone();
        let heartbeat_name = self.name.clone();
        let heartbeat_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            // Tokio 的首个 tick 立即完成；Go ticker 要等一个完整周期，所以先消费它。
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = heartbeat_shutdown.cancelled() => {
                        tracing::info!("server_name" = ?(&heartbeat_name), "{}", "MCP heartbeat monitor stopping");
                        break;
                    },
                    _ = interval.tick() => {
                        tracing::debug!("server_name" = ?(&heartbeat_name), "port" = ?(port), "status" = ?("running"), "healthy" = ?(true), "{}", "MCP server heartbeat");
                    }
                }
            }
        });

        let old_runtime = self.runtime.lock().await.replace(ServerRuntime {
            server_cancellation,
            heartbeat_cancellation,
            server_task,
            heartbeat_task,
            listener_address: listener_address.to_string(),
        });
        if let Some(old_runtime) = old_runtime {
            old_runtime.server_cancellation.cancel();
            old_runtime.heartbeat_cancellation.cancel();
            old_runtime.server_task.abort();
            old_runtime.heartbeat_task.abort();
        }

        tracing::info!("server_name" = ?(&self.name), "port" = ?(port), "version" = ?(&self.version), "{}", "MCP server started");

        Ok(ServerConfig { port, bearer_token })
    }

    /// 在五秒窗口内优雅停止，超时后终止剩余 Tokio task。
    async fn stop(&self) {
        let runtime = self.runtime.lock().await.take();
        let mut listener_address = None;
        let mut heartbeat_runtime = None;
        if let Some(runtime) = runtime {
            // Go `http.Server.Shutdown` 先关闭 listener 并等待 HTTP server；heartbeat
            // channel 要到显式 listener close 之后才关闭，因此这里只触发 server token。
            runtime.server_cancellation.cancel();
            let mut server_task = runtime.server_task;
            if tokio::time::timeout(Duration::from_secs(5), &mut server_task)
                .await
                .is_err()
            {
                tracing::warn!("server_name" = ?(&self.name), "error" = ?("context deadline exceeded"), "{}", "Failed to stop MCP server");
                server_task.abort();
            }
            listener_address = Some(runtime.listener_address);
            heartbeat_runtime = Some((runtime.heartbeat_cancellation, runtime.heartbeat_task));
        } else if self.port() != 0 {
            listener_address = Some(format!("127.0.0.1:{}", self.port()));
        }

        if let Some(listener_address) = listener_address {
            // Go 先调用 `http.Server.Shutdown`（它关闭 listener），随后再次显式 Close；
            // 第二次 close 固定返回 use-of-closed-network-connection，并被记录为 warning。
            tracing::warn!("server_name" = ?(&self.name), "error" = ?(format!(
                            "close tcp {listener_address}: use of closed network connection"
                        )), "{}", "Failed to close MCP server listener");
        }
        if let Some((heartbeat_cancellation, _)) = heartbeat_runtime.as_ref() {
            heartbeat_cancellation.cancel();
            // Go 在关闭 channel 后不等待 goroutine；先记录 stopped，再在函数返回前
            // 回收 Tokio task，既保留日志阶段顺序，也不把后台任务泄漏给测试运行时。
        }
        tracing::info!("server_name" = ?(&self.name), "{}", "MCP server stopped");
        if let Some((_, mut heartbeat_task)) = heartbeat_runtime
            && tokio::time::timeout(Duration::from_secs(5), &mut heartbeat_task)
                .await
                .is_err()
        {
            heartbeat_task.abort();
        }
    }

    /// 普通基础 server 默认需要注册到 Claude。
    fn should_register_with_claude(&self) -> bool {
        true
    }
}

/// 让专用 server 不经 trait object 也能机械调用 Go 风格方法。
impl BaseServer {
    /// 返回 server 名称。
    #[must_use]
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /// 启动基础 server。
    pub async fn start(&self, tools: Vec<ToolConfig>) -> McpResult<ServerConfig> {
        <Self as MCPServer>::start(self, tools).await
    }

    /// 幂等停止基础 server。
    pub async fn stop(&self) {
        <Self as MCPServer>::stop(self).await;
    }

    /// 返回默认 Claude 注册策略。
    #[must_use]
    pub fn should_register_with_claude(&self) -> bool {
        true
    }
}
