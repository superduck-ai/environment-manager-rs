//! Tunnel HTTP/WebSocket 转发器的共享类型。
//!
//! 协议类型由 `gen/proto` 生成并在
//! `tunnel::mod` 重新导出；本文件保存运行时状态和统一错误边界。

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::stream::{SplitSink, SplitStream};
use secrecy::SecretString;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;

use super::actions::Registry;
/// Tunnel 转发、协议发送或本地请求构造错误。
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct TunnelError {
    message: String,
}

impl TunnelError {
    /// 从已经包含完整上下文的文本构造 Tunnel 错误。
    #[must_use]
    pub fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// 本地 HTTP Tunnel 处理器。
pub struct Handler {
    /// 等待响应 header 的超时；只在完整请求实际写入 socket 后启动。
    pub(crate) response_header_timeout: std::time::Duration,
    /// 差分探针使用的连接前延迟；正式构造恒为零，用于证明 header timer 不覆盖 dial。
    pub(crate) connect_delay: std::time::Duration,
    /// request ID 到取消令牌的映射；锁只保护表本身，不跨 await 持有。
    pub(crate) active_requests: Mutex<BTreeMap<String, CancellationToken>>,
}

/// Tunnel WebSocket 响应发送失败的线程安全错误。
pub type WsSenderError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// 本地 WebSocket 响应发送接口。
#[async_trait]
pub trait WsResponseSender: Send + Sync {
    /// 报告握手成功和选中的子协议。
    async fn send_opened(&self, response: super::WsTunnelOpened) -> Result<(), WsSenderError>;

    /// 把本地 WebSocket 消息发回 Tunnel。
    async fn send_message(&self, message: super::WsTunnelMessage) -> Result<(), WsSenderError>;

    /// 把本地关闭帧发回 Tunnel。
    async fn send_close(&self, message: super::WsTunnelClose) -> Result<(), WsSenderError>;

    /// 报告握手或读写错误。
    async fn send_error(&self, response: super::WsTunnelError) -> Result<(), WsSenderError>;
}

/// 本地 WebSocket 连接的具体类型；唯一运行目标是 Linux，因此使用 Tokio TCP 流。
pub(crate) type LocalWebSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// 可由发送路径互斥访问的 WebSocket 写半部。
pub(crate) type LocalWebSocketSink = SplitSink<LocalWebSocket, Message>;

/// 后端主 Tunnel WebSocket 的读半部。
pub(crate) type LocalWebSocketStream = SplitStream<LocalWebSocket>;

/// 一条已建立的本地 WebSocket Tunnel。
pub(crate) struct WsTunnel {
    /// 后端分配的稳定 tunnel ID。
    pub(crate) tunnel_id: String,
    /// 分离后的写半部；避免长时间 read 阻塞后端下行消息。
    pub(crate) sink: tokio::sync::Mutex<LocalWebSocketSink>,
    /// 本地上行消息的协议 sender。
    pub(crate) sender: Arc<dyn WsResponseSender>,
    /// 主动关闭及替换同 ID 连接时使用的取消令牌。
    pub(crate) cancellation: CancellationToken,
}

/// 本地 WebSocket Tunnel 管理器。
pub struct WsHandler {
    /// 共享 map 使用异步读写锁，但任何网络 await 都在释放 map 锁后执行。
    pub(crate) tunnels: Arc<tokio::sync::RwLock<BTreeMap<String, Arc<WsTunnel>>>>,
}

/// 只在 `new_client` 返回共享对象前修改配置。
pub type ClientOption = Box<dyn Fn(&mut Client) + Send + Sync + 'static>;

/// 后端 Tunnel 长连接客户端。
pub struct Client {
    pub(crate) http_handler: Option<Arc<Handler>>,
    pub(crate) ws_handler: Option<Arc<WsHandler>>,
    pub(crate) session_id: String,
    pub(crate) ws_url: String,
    pub(crate) auth_token: SecretString,
    pub(crate) action_registry: Option<Arc<Registry>>,
    /// 所有响应和 ping 共用一个异步写锁，保留 WebSocket 单写者要求。
    pub(crate) writer: tokio::sync::Mutex<Option<LocalWebSocketSink>>,
    /// Connect 安装、Run 独占取走的读半部；同一连接不能并发运行两个 read loop。
    pub(crate) reader: tokio::sync::Mutex<Option<LocalWebSocketStream>>,
    /// 收到 pong 的最近单调时刻，用于 90 秒存活门禁。
    pub(crate) last_pong_time: Mutex<std::time::Instant>,
    /// 当前连接协商出的每流发送额度。
    pub(crate) flow: super::flow::FlowControl,
    /// 流控模式下每个本地 WebSocket 的写队列；读循环只入队，不等待本地写入。
    pub(crate) ws_writers: Mutex<BTreeMap<String, super::local_ws::LocalWsWriter>>,
    /// 收到的入站帧计数（数据帧与 pong），供冻结探测判断连接是否仍然存活。
    pub(crate) inbound_frames: std::sync::atomic::AtomicU64,
    /// 冻结探测失败后置位，使下一次重连跳过退避。
    pub(crate) reconnect_immediately: std::sync::atomic::AtomicBool,
    /// 墙上时钟来源；测试注入以模拟进程冻结后的时间跳变。
    pub(crate) wall_clock: Arc<dyn Fn() -> std::time::SystemTime + Send + Sync>,
    /// 冻结探测的 tick 间隔。
    pub(crate) freeze_tick: std::time::Duration,
    /// 判定时间跳变后等待任意入站帧的时限。
    pub(crate) freeze_probe_timeout: std::time::Duration,
    /// 流控模式下本地 WebSocket 写队列单次写入的超时。
    pub(crate) local_ws_write_timeout: std::time::Duration,
}

/// HTTP Tunnel 响应适配器；空 request ID 会回填请求侧 ID。
pub(crate) struct HttpResponseSender {
    pub(crate) client: Arc<Client>,
    pub(crate) request_id: String,
}

/// WebSocket Tunnel 响应适配器；空 tunnel ID 会回填请求侧 ID。
pub(crate) struct ClientWsResponseSender {
    pub(crate) client: Arc<Client>,
    pub(crate) tunnel_id: String,
}
