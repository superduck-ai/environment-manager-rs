//! 后端 Tunnel WebSocket 长连接客户端。
//!
//! 对应 Go `internal/tunnel/client.go`。客户端建立带 session/auth header 的 WebSocket，
//! 运行读循环和 ping/pong 存活检查，把 HTTP、WebSocket 与控制 action 请求分派给本地
//! handler，并使用 lowerCamel ProtoJSON 回传结果。Rust 通过拆分读写半部保证单写者约束。

use std::io;
use std::net::IpAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt as _, StreamExt as _};
use http::header::{AUTHORIZATION, HeaderValue};
use rand::Rng as _;
use secrecy::{ExposeSecret as _, SecretString};
use serde_json::Value;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::error::Error as WebSocketError;
use tokio_tungstenite::tungstenite::handshake::client::{
    Request as WebSocketRequest, Response as WebSocketResponse,
};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message, WebSocketConfig};
use tokio_tungstenite::{client_async_tls_with_config, connect_async_with_config};
use tokio_util::sync::CancellationToken;
use url::{Host, Url};

use crate::internal::{
    logger::{LogAttributes, Logger},
    util::format_go_duration,
};

use super::actions::{ActionError, Registry, ResponseSender};
use super::helpers::{client_logger, marshal_protojson};
use super::{
    Client, ClientOption, ClientWsResponseSender, HttpResponseSender, HttpTunnelCancel,
    HttpTunnelRequest, HttpTunnelResponseChunk, HttpTunnelResponseError, HttpTunnelResponseHeaders,
    LocalWebSocket, LocalWebSocketStream, TunnelError, TunnelRequest, TunnelResponse,
    WsResponseSender, WsSenderError, WsTunnelClose, WsTunnelError, WsTunnelMessage, WsTunnelOpen,
    WsTunnelOpened, new_handler, new_ws_handler, tunnel_request, tunnel_response,
};

/// 对应 Go `tunnelServicePath`，不能改为普通 `/ws` 路径。
const TUNNEL_SERVICE_PATH: &str = "/anthropic.session_ingress.api.v1alpha.TunnelService/Tunnel";
/// 后端 WebSocket 握手超时。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// 读循环每次等待下一帧的最长时间。
const READ_TIMEOUT: Duration = Duration::from_secs(60);
/// 主连接 ping 间隔。
const PING_INTERVAL: Duration = Duration::from_secs(30);
/// 最近 pong 超过该时长时强制重连。
const PONG_TIMEOUT: Duration = Duration::from_secs(90);
/// 单次响应、ping 或 close frame 写入上限。
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// 重连退避上限。
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
/// 代理 CONNECT 响应 header 的最大尺寸，防止错误代理无界占用内存。
const MAX_PROXY_RESPONSE_HEADER: usize = 64 * 1024;

/// Go `http.ProxyFromEnvironment` 第一次使用时捕获的进程级代理配置。
static ENVIRONMENT_PROXY_CONFIG: OnceLock<EnvironmentProxyConfig> = OnceLock::new();

/// `ProxyFromEnvironment` 所需的原始环境快照。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct EnvironmentProxyConfig {
    pub(super) http_proxy: String,
    pub(super) https_proxy: String,
    pub(super) no_proxy: String,
    pub(super) cgi: bool,
}

/// Gorilla `DialContext` 的兼容错误边界。
///
/// `tokio-tungstenite` 会把底层网络错误显示为 `IO error: ...`，并把非 101
/// 握手显示为 `HTTP error: <status>`；Gorilla 则透明返回底层网络错误，并对握手
/// 失败固定返回 `websocket: bad handshake`，同时通过独立的 response 暴露状态。
/// 这个类型同时保存两部分信息，供主 Tunnel 与本地 WS Tunnel 按各自 Go 格式拼接。
#[derive(Debug)]
pub(super) struct GorillaDialFailure {
    message: String,
    response_status: Option<String>,
}

impl GorillaDialFailure {
    /// 把 tungstenite 的错误归一化为 Gorilla 可观察的文本和 response 状态。
    ///
    /// `Io` 必须只显示内部 `io::Error`；否则代理拒绝、SOCKS5 协商失败等错误会
    /// 多出 Rust 特有前缀。`Http` 表示服务器确实返回了 HTTP response，因此保留
    /// `<code> <reason>` 并把错误本体固定为 Gorilla 的 `ErrBadHandshake` 文本。
    fn from_websocket_error(error: WebSocketError) -> Self {
        match error {
            WebSocketError::Io(error) => Self {
                message: error.to_string(),
                response_status: None,
            },
            WebSocketError::Http(response) => Self {
                message: "websocket: bad handshake".to_owned(),
                response_status: Some(http_status_text(response.status())),
            },
            error => Self {
                message: error.to_string(),
                response_status: None,
            },
        }
    }

    /// 返回 Gorilla 会通过 `*http.Response` 交给调用方的状态文本。
    #[must_use]
    pub(super) fn response_status(&self) -> Option<&str> {
        self.response_status.as_deref()
    }
}

impl std::fmt::Display for GorillaDialFailure {
    /// 只输出 Gorilla error 本体；response 状态由调用点按 Go 源码的格式另行拼接。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for GorillaDialFailure {}

/// 代理选择结果；除解析后的 URL 外保留 Go `url.Userinfo.passwordSet` 位。
///
/// WHATWG `url::Url` 会把 `user:@host` 规范化为 `user@host`，从而丢失“显式空密码”
/// 与“未提供密码”的差别；Gorilla 只对前者发送 Proxy-Authorization，因此不能只
/// 根据解析后 URL 的 `password()` 判断。
#[derive(Debug)]
struct ProxySelection {
    url: Url,
    password_set: bool,
}

/// 主 Tunnel ping 写入的失败类别。
///
/// timeout 不是 tungstenite 错误，因此单独保存；WebSocket 分支保留原始错误，便于
/// 像 Gorilla 一样剥离 Rust 库为底层 `io::Error` 添加的 `IO error:` 前缀。
pub(super) enum PingWriteFailure {
    Timeout,
    WebSocket(WebSocketError),
}

/// 把 ping 写失败转换为 Go `pingLoop` 的错误优先级和文本。
///
/// # 参数
/// - `cancellation`：本轮 Tunnel 的取消信号；Go 在 `WriteMessage` 失败后首先检查
///   `ctx.Err()`，因此并发取消必须覆盖 timeout 或 socket 错误。
/// - `failure`：Tokio timeout 或 tungstenite 写错误。
///
/// # 返回
/// 返回供连接重试循环处理的兼容错误，不记录日志也不修改连接状态。
pub(super) fn ping_write_failure(
    cancellation: &CancellationToken,
    failure: PingWriteFailure,
) -> TunnelError {
    if cancellation.is_cancelled() {
        return TunnelError::message("context canceled");
    }

    match failure {
        PingWriteFailure::Timeout => TunnelError::message("write tunnel ping: i/o timeout"),
        PingWriteFailure::WebSocket(WebSocketError::Io(error)) => {
            TunnelError::message(format!("write tunnel ping: {error}"))
        }
        PingWriteFailure::WebSocket(error) => {
            TunnelError::message(format!("write tunnel ping: {error}"))
        }
    }
}

/// 把 Rust HTTP status 转成 Go `http.Response.Status` 的标准形式。
///
/// Go 的响应状态是 `"<code> <reason>"`；未知状态码没有标准 reason 时只保留数字。
/// tungstenite 的 response 类型已丢弃原始 reason phrase，因此标准状态使用 HTTP
/// 规范中的 canonical reason，这是两端正常握手失败时共同可观察的值。
fn http_status_text(status: http::StatusCode) -> String {
    match status.canonical_reason() {
        Some(reason) => format!("{} {reason}", status.as_u16()),
        None => status.as_u16().to_string(),
    }
}

impl EnvironmentProxyConfig {
    /// 按大写优先、小写回退的顺序读取进程环境。
    ///
    /// 返回值会由 [`ENVIRONMENT_PROXY_CONFIG`] 缓存，因此运行期修改环境
    /// 不会改变后续连接，与 Go `envProxyOnce` 一致。
    fn from_process() -> Self {
        Self {
            http_proxy: first_non_empty_environment("HTTP_PROXY", "http_proxy"),
            https_proxy: first_non_empty_environment("HTTPS_PROXY", "https_proxy"),
            no_proxy: first_non_empty_environment("NO_PROXY", "no_proxy"),
            cgi: std::env::var_os("REQUEST_METHOD").is_some_and(|value| !value.is_empty()),
        }
    }
}

/// 对应 Go：`NewClient`。
///
/// 创建 HTTP/WS handler 并依次应用 option。认证 token 进入 `SecretString`，不会被
/// Debug 或日志意外输出。HTTP client 构造失败时返回错误，不产生半初始化对象。
pub fn new_client(
    logger: Option<Logger>,
    session_id: impl Into<String>,
    ws_url: impl Into<String>,
    auth_token: impl Into<String>,
    options: Vec<ClientOption>,
) -> Result<Arc<Client>, TunnelError> {
    let http_handler = Arc::new(new_handler(logger.clone())?);
    let ws_handler = Arc::new(new_ws_handler(logger.clone()));
    let mut client = Client {
        logger,
        http_handler: Some(http_handler),
        ws_handler: Some(ws_handler),
        session_id: session_id.into(),
        ws_url: ws_url.into(),
        auth_token: SecretString::from(auth_token.into()),
        action_registry: None,
        writer: tokio::sync::Mutex::new(None),
        reader: tokio::sync::Mutex::new(None),
        last_pong_time: std::sync::Mutex::new(Instant::now()),
    };
    for option in options {
        option(&mut client);
    }
    Ok(Arc::new(client))
}

/// 对应 Go：`WithActionRegistry`。
///
/// 把 `/__control/` 请求交给给定 registry；option 只在客户端发布到 `Arc` 前执行。
#[must_use]
pub fn with_action_registry(registry: Arc<Registry>) -> ClientOption {
    Box::new(move |client| client.action_registry = Some(Arc::clone(&registry)))
}

impl Client {
    /// 对应 Go：`(*Client).Connect`。
    ///
    /// 构造固定 service endpoint，注入 Bearer 与 `X-Session-ID`，在十秒内完成握手并
    /// 安装新的读写半部。已有连接会直接丢弃，和 Go 在替换前调用底层 Close 一致。
    pub async fn connect(&self, cancellation: &CancellationToken) -> Result<(), TunnelError> {
        let endpoint = self.tunnel_endpoint();
        let mut request = endpoint
            .into_client_request()
            .map_err(|error| TunnelError::message(format!("connect tunnel websocket: {error}")))?;
        if !self.auth_token.expose_secret().is_empty() {
            let value =
                HeaderValue::from_str(&format!("Bearer {}", self.auth_token.expose_secret()))
                    .map_err(|error| {
                        TunnelError::message(format!("connect tunnel websocket: {error}"))
                    })?;
            request.headers_mut().insert(AUTHORIZATION, value);
        }
        if !self.session_id.is_empty() {
            let value = HeaderValue::from_str(&self.session_id).map_err(|error| {
                TunnelError::message(format!("connect tunnel websocket: {error}"))
            })?;
            request.headers_mut().insert("x-session-id", value);
        }

        let connected = tokio::select! {
            () = cancellation.cancelled() => return Err(TunnelError::message("context canceled")),
            result = tokio::time::timeout(CONNECT_TIMEOUT, connect_websocket(request)) => result,
        };
        let (socket, _response) = match connected {
            Ok(Ok(connected)) => connected,
            Ok(Err(error)) => {
                let message = match error.response_status() {
                    Some(status) => {
                        format!("connect tunnel websocket ({status}): {error}")
                    }
                    None => format!("connect tunnel websocket: {error}"),
                };
                return Err(TunnelError::message(message));
            }
            Err(_) => {
                return Err(TunnelError::message(
                    "connect tunnel websocket: websocket: handshake timeout",
                ));
            }
        };

        let (writer, reader) = socket.split();
        self.drop_connection().await;
        *self.writer.lock().await = Some(writer);
        *self.reader.lock().await = Some(reader);
        *self
            .last_pong_time
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Instant::now();
        let logger = client_logger(self);
        logger.info(
            "tunnel_connected",
            log_attributes([("session_id", Value::String(self.session_id.clone()))]),
        );
        Ok(())
    }

    /// 对应 Go：`(*Client).Run`。
    ///
    /// 独占 Connect 安装的 reader，同时运行请求读取与 ping loop；任一失败都会取消
    /// 另一侧并关闭 writer。调用方取消返回 `context canceled`，供外层决定是否重试。
    pub async fn run(
        self: &Arc<Self>,
        cancellation: &CancellationToken,
    ) -> Result<(), TunnelError> {
        if self.writer.lock().await.is_none() {
            return Err(TunnelError::message("not connected"));
        }
        let reader = self
            .reader
            .lock()
            .await
            .take()
            .ok_or_else(|| TunnelError::message("not connected"))?;
        let run_cancellation = cancellation.child_token();
        let read = self.read_loop(run_cancellation.clone(), reader);
        let ping = self.ping_loop(run_cancellation.clone());
        tokio::pin!(read, ping);

        let result = tokio::select! {
            () = cancellation.cancelled() => Err(TunnelError::message("context canceled")),
            result = &mut read => result,
            result = &mut ping => result,
        };
        run_cancellation.cancel();
        self.close_connection().await;
        result
    }

    /// 对应 Go：`(*Client).ConnectAndRun`。
    ///
    /// Connect 自身使用指数退避重试；连接运行结束后再使用独立的 1–30 秒抖动退避。
    /// 只有调用方取消或不可恢复的取消错误才退出该长期循环。
    pub async fn connect_and_run(
        self: &Arc<Self>,
        cancellation: &CancellationToken,
    ) -> Result<(), TunnelError> {
        let mut delay = Duration::from_secs(1);
        loop {
            self.connect_with_retries(cancellation).await?;
            let result = self.run(cancellation).await;
            self.close_connection().await;
            if cancellation.is_cancelled() {
                return Err(TunnelError::message("context canceled"));
            }
            if let Err(error) = result {
                let logger = client_logger(self);
                logger.warn(
                    "tunnel_run_failed",
                    log_attributes([("error", Value::String(error.to_string()))]),
                );
            }

            let wait = add_jitter(delay);
            tokio::select! {
                () = cancellation.cancelled() => return Err(TunnelError::message("context canceled")),
                () = tokio::time::sleep(wait) => {}
            }
            delay = std::cmp::min(delay.saturating_mul(2), MAX_RETRY_DELAY);
        }
    }

    /// 对应 Go：`(*Client).Close`，先关闭全部本地 WS tunnel，再关闭后端主连接。
    pub async fn close(&self) -> Result<(), TunnelError> {
        if let Some(handler) = &self.ws_handler {
            handler.close_all().await;
        }
        self.close_web_socket().await
    }

    /// 对应 Go：`(*Client).tunnelEndpoint`。
    #[must_use]
    pub fn tunnel_endpoint(&self) -> String {
        format!(
            "{}{}",
            self.ws_url.trim_end_matches('/'),
            TUNNEL_SERVICE_PATH
        )
    }

    /// 使用 1–30 秒退避持续握手，成功后返回。
    async fn connect_with_retries(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), TunnelError> {
        let mut delay = Duration::from_secs(1);
        loop {
            match self.connect(cancellation).await {
                Ok(()) => return Ok(()),
                Err(error) if cancellation.is_cancelled() => return Err(error),
                Err(error) => {
                    let wait = add_jitter(delay);
                    let logger = client_logger(self);
                    logger.warn(
                        "tunnel_connect_failed",
                        log_attributes([
                            ("error", Value::String(error.to_string())),
                            ("retry_in", Value::String(format_go_duration(wait))),
                        ]),
                    );
                    tokio::select! {
                        () = cancellation.cancelled() => return Err(TunnelError::message("context canceled")),
                        () = tokio::time::sleep(wait) => {}
                    }
                    delay = std::cmp::min(delay.saturating_mul(2), MAX_RETRY_DELAY);
                }
            }
        }
    }

    /// 从主 WebSocket 读取、解码并分派 TunnelRequest。
    ///
    /// 每次调用 [`read_tunnel_data_message`] 都对应 Go 在一次
    /// `ReadMessage` 前设置的绝对 deadline。控制帧不能开启新的
    /// 60 秒窗口；只有完整的 text/binary 应用消息返回并分派后，
    /// 下一轮才会重置 deadline。
    async fn read_loop(
        self: &Arc<Self>,
        cancellation: CancellationToken,
        mut reader: LocalWebSocketStream,
    ) -> Result<(), TunnelError> {
        loop {
            let data = read_tunnel_data_message(&cancellation, &mut reader, READ_TIMEOUT, || {
                *self
                    .last_pong_time
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Instant::now();
            })
            .await?;

            let request = match decode_tunnel_request(&data) {
                Ok(request) => request,
                Err(error) => {
                    let logger = client_logger(self);
                    logger.error(
                        "tunnel_request_decode_failed",
                        log_attributes([("error", Value::String(error.to_string()))]),
                    );
                    continue;
                }
            };
            if !request.session_id.is_empty()
                && !self.session_id.is_empty()
                && request.session_id != self.session_id
            {
                let logger = client_logger(self);
                logger.warn(
                    "tunnel_request_session_mismatch",
                    log_attributes([
                        (
                            "request_session_id",
                            Value::String(request.session_id.clone()),
                        ),
                        ("client_session_id", Value::String(self.session_id.clone())),
                    ]),
                );
            }
            self.dispatch_request(cancellation.clone(), request).await;
        }
    }

    /// 每 30 秒发送 ping，并在 90 秒未见 pong 时返回超时。
    async fn ping_loop(&self, cancellation: CancellationToken) -> Result<(), TunnelError> {
        let mut interval = tokio::time::interval(PING_INTERVAL);
        // Tokio interval 的第一次 tick 立即触发，而 Go ticker 等一个周期；先消费它。
        interval.tick().await;
        loop {
            tokio::select! {
                () = cancellation.cancelled() => return Err(TunnelError::message("context canceled")),
                _ = interval.tick() => {}
            }
            let last_pong = *self
                .last_pong_time
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if last_pong.elapsed() > PONG_TIMEOUT {
                return Err(TunnelError::message("tunnel pong timeout"));
            }
            let mut writer = self.writer.lock().await;
            let writer = writer
                .as_mut()
                .ok_or_else(|| TunnelError::message("not connected"))?;
            match tokio::time::timeout(WRITE_TIMEOUT, writer.send(Message::Ping(Vec::new().into())))
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    return Err(ping_write_failure(
                        &cancellation,
                        PingWriteFailure::WebSocket(error),
                    ));
                }
                Err(_) => {
                    return Err(ping_write_failure(&cancellation, PingWriteFailure::Timeout));
                }
            }
        }
    }

    /// 根据 oneof payload 调度 HTTP、取消、本地 WS 或空请求。
    async fn dispatch_request(
        self: &Arc<Self>,
        cancellation: CancellationToken,
        request: TunnelRequest,
    ) {
        match request.payload {
            Some(tunnel_request::Payload::HttpRequest(request)) => {
                let client = Arc::clone(self);
                tokio::spawn(async move {
                    client.handle_http_request(cancellation, Ok(request)).await;
                });
            }
            Some(tunnel_request::Payload::HttpCancel(cancel)) => {
                self.handle_http_cancel(Ok(cancel));
            }
            Some(tunnel_request::Payload::WsOpen(request)) => {
                let client = Arc::clone(self);
                tokio::spawn(async move {
                    client.handle_ws_open(cancellation, Ok(request)).await;
                });
            }
            Some(tunnel_request::Payload::WsMessage(message)) => {
                self.handle_ws_message(&cancellation, Ok(message)).await;
            }
            Some(tunnel_request::Payload::WsClose(message)) => {
                self.handle_ws_close(&cancellation, Ok(message)).await;
            }
            None => {
                let logger = client_logger(self);
                logger.warn("tunnel_request_empty_payload", LogAttributes::new());
            }
        }
    }

    /// 对应 Go：`(*Client).handleHTTPRequest`。
    ///
    /// 执行一个 HTTP 请求；port 0 的控制路径优先交给 action registry。
    /// `request` 保留一个显式的转换结果边界：生产 Proto oneof 总是传入 `Ok`，
    /// 兼容测试和未来的类型擦除入口仍可传入 Go `asHTTPRequest` 等价的转换错误。
    /// 失败时只记录 `http_tunnel_request_invalid`，不会创建 sender 或产生网络副作用。
    async fn handle_http_request(
        self: &Arc<Self>,
        cancellation: CancellationToken,
        request: Result<HttpTunnelRequest, TunnelError>,
    ) {
        let request = match request {
            Ok(request) => request,
            Err(error) => {
                let logger = client_logger(self);
                logger.error(
                    "http_tunnel_request_invalid",
                    log_attributes([("error", Value::String(error.to_string()))]),
                );
                return;
            }
        };
        let sender: Arc<dyn ResponseSender> = Arc::new(HttpResponseSender {
            client: Arc::clone(self),
            request_id: request.request_id.clone(),
        });
        if let Some(registry) = &self.action_registry
            && request.port == 0
            && request.path.starts_with("/__control/")
        {
            registry
                .execute(
                    cancellation,
                    &request.request_id,
                    &request.path,
                    request.body,
                    &self.session_id,
                    sender,
                )
                .await;
            return;
        }
        let Some(handler) = &self.http_handler else {
            let _ = sender
                .send_error(HttpTunnelResponseError {
                    request_id: request.request_id,
                    error_message: "http handler is not configured".to_owned(),
                })
                .await;
            return;
        };
        if let Err(error) = handler
            .handle_request(cancellation, request.clone(), sender)
            .await
        {
            let logger = client_logger(self);
            logger.error(
                "http_tunnel_request_failed",
                log_attributes([
                    ("request_id", Value::String(request.request_id)),
                    ("error", Value::String(error.to_string())),
                ]),
            );
        }
    }

    /// 对应 Go：`(*Client).handleHTTPCancel`。
    ///
    /// 取消一个正在转发的 HTTP 请求；未知 ID 是幂等 no-op。显式 `Result` 参数
    /// 保留 Go 类型擦除转换失败的日志分支，生产 oneof 分派只传入 `Ok`。
    fn handle_http_cancel(&self, cancel: Result<HttpTunnelCancel, TunnelError>) {
        let cancel = match cancel {
            Ok(cancel) => cancel,
            Err(error) => {
                let logger = client_logger(self);
                logger.error(
                    "http_tunnel_cancel_invalid",
                    log_attributes([("error", Value::String(error.to_string()))]),
                );
                return;
            }
        };
        if let Some(handler) = &self.http_handler {
            handler.cancel_request(&cancel.request_id);
        }
        let logger = client_logger(self);
        logger.info(
            "http_tunnel_request_cancelled",
            log_attributes([("request_id", Value::String(cancel.request_id))]),
        );
    }

    /// 对应 Go：`(*Client).handleWSOpen`。
    ///
    /// 建立一条后端请求的本地 WebSocket tunnel。显式转换结果使 Go
    /// `asWSOpen` 的错误事件仍有独立、可测试的运行时日志位置。
    async fn handle_ws_open(
        self: &Arc<Self>,
        cancellation: CancellationToken,
        request: Result<WsTunnelOpen, TunnelError>,
    ) {
        let request = match request {
            Ok(request) => request,
            Err(error) => {
                let logger = client_logger(self);
                logger.error(
                    "ws_tunnel_open_invalid",
                    log_attributes([("error", Value::String(error.to_string()))]),
                );
                return;
            }
        };
        let sender: Arc<dyn WsResponseSender> = Arc::new(ClientWsResponseSender {
            client: Arc::clone(self),
            tunnel_id: request.tunnel_id.clone(),
        });
        let Some(handler) = &self.ws_handler else {
            let _ = sender
                .send_error(WsTunnelError {
                    tunnel_id: request.tunnel_id,
                    error_message: "websocket handler is not configured".to_owned(),
                })
                .await;
            return;
        };
        if let Err(error) = handler
            .open_tunnel(cancellation, request.clone(), sender)
            .await
        {
            let logger = client_logger(self);
            logger.error(
                "ws_tunnel_open_failed",
                log_attributes([
                    ("tunnel_id", Value::String(request.tunnel_id)),
                    ("error", Value::String(error.to_string())),
                ]),
            );
        }
    }

    /// 对应 Go：`(*Client).handleWSMessage`。
    ///
    /// 把后端 WS message 写入指定本地 tunnel。这里保留 Go `asWSMessage`
    /// 转换失败分支，且 nil-handler 与发送失败分支必须使用两个日志调用点。
    async fn handle_ws_message(
        &self,
        cancellation: &CancellationToken,
        message: Result<WsTunnelMessage, TunnelError>,
    ) {
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                let logger = client_logger(self);
                logger.error(
                    "ws_tunnel_message_invalid",
                    log_attributes([("error", Value::String(error.to_string()))]),
                );
                return;
            }
        };
        let Some(handler) = &self.ws_handler else {
            // Go 的 nil-handler 分支只有 error 属性；不能与下方真实发送失败合并，
            // 否则不仅 source 行号错误，还会凭空增加 tunnel_id。
            let logger = client_logger(self);
            logger.error(
                "ws_tunnel_message_failed",
                log_attributes([(
                    "error",
                    Value::String("websocket handler is not configured".to_owned()),
                )]),
            );
            return;
        };
        let tunnel_id = message.tunnel_id.clone();
        if let Err(error) = handler.send_message(cancellation, message).await {
            let logger = client_logger(self);
            logger.error(
                "ws_tunnel_message_failed",
                log_attributes([
                    ("tunnel_id", Value::String(tunnel_id)),
                    ("error", Value::String(error.to_string())),
                ]),
            );
        }
    }

    /// 对应 Go：`(*Client).handleWSClose`。
    ///
    /// 按后端 close 请求关闭指定本地 tunnel。转换失败、nil-handler 与真实关闭失败
    /// 三个事件不得合并，否则相同消息的两个 Go source 无法按分支逐点对应。
    async fn handle_ws_close(
        &self,
        cancellation: &CancellationToken,
        message: Result<WsTunnelClose, TunnelError>,
    ) {
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                let logger = client_logger(self);
                logger.error(
                    "ws_tunnel_close_invalid",
                    log_attributes([("error", Value::String(error.to_string()))]),
                );
                return;
            }
        };
        let Some(handler) = &self.ws_handler else {
            let logger = client_logger(self);
            logger.error(
                "ws_tunnel_close_failed",
                log_attributes([(
                    "error",
                    Value::String("websocket handler is not configured".to_owned()),
                )]),
            );
            return;
        };
        let tunnel_id = message.tunnel_id.clone();
        if let Err(error) = handler.close_tunnel(cancellation, message).await {
            let logger = client_logger(self);
            logger.error(
                "ws_tunnel_close_failed",
                log_attributes([
                    ("tunnel_id", Value::String(tunnel_id)),
                    ("error", Value::String(error.to_string())),
                ]),
            );
        }
    }

    /// 序列化 lowerCamel ProtoJSON 并写入主 Tunnel text frame。
    async fn send_response(&self, response: TunnelResponse) -> Result<(), TunnelError> {
        let data = marshal_protojson(&response)
            .map_err(|error| TunnelError::message(format!("marshal response: {error}")))?;
        let text = String::from_utf8(data)
            .map_err(|error| TunnelError::message(format!("marshal response: {error}")))?;
        let mut writer = self.writer.lock().await;
        let writer = writer
            .as_mut()
            .ok_or_else(|| TunnelError::message("not connected"))?;
        tokio::time::timeout(WRITE_TIMEOUT, writer.send(Message::Text(text.into())))
            .await
            .map_err(|_| TunnelError::message("write response: i/o timeout"))?
            .map_err(|error| TunnelError::message(format!("write response: {error}")))
    }

    /// 关闭主连接并忽略诊断性 close-frame 错误，供运行循环统一清理。
    async fn close_connection(&self) {
        let _ = self.close_web_socket().await;
    }

    /// 发送正常 close frame 后丢弃主连接读写半部。
    async fn close_web_socket(&self) -> Result<(), TunnelError> {
        let writer = self.writer.lock().await.take();
        self.reader.lock().await.take();
        let Some(mut writer) = writer else {
            return Ok(());
        };
        let close = CloseFrame {
            code: CloseCode::Normal,
            reason: String::new().into(),
        };
        tokio::time::timeout(WRITE_TIMEOUT, writer.send(Message::Close(Some(close))))
            .await
            .map_err(|_| TunnelError::message("close tunnel websocket: i/o timeout"))?
            .map_err(|error| TunnelError::message(format!("close tunnel websocket: {error}")))
    }

    /// 不发送 close frame，直接丢弃当前读写半部；用于替换和错误清理。
    async fn drop_connection(&self) {
        self.writer.lock().await.take();
        self.reader.lock().await.take();
    }
}

#[async_trait]
impl ResponseSender for HttpResponseSender {
    /// 把 HTTP header 包装进 TunnelResponse；空 request ID 自动回填。
    async fn send_headers(
        &self,
        mut response: HttpTunnelResponseHeaders,
    ) -> Result<(), ActionError> {
        if response.request_id.is_empty() {
            response.request_id.clone_from(&self.request_id);
        }
        self.client
            .send_response(TunnelResponse {
                payload: Some(tunnel_response::Payload::HttpHeaders(response)),
            })
            .await
            .map_err(|error| Box::new(error) as ActionError)
    }

    /// 把 HTTP body chunk 包装进 TunnelResponse；空 request ID 自动回填。
    async fn send_chunk(&self, mut chunk: HttpTunnelResponseChunk) -> Result<(), ActionError> {
        if chunk.request_id.is_empty() {
            chunk.request_id.clone_from(&self.request_id);
        }
        self.client
            .send_response(TunnelResponse {
                payload: Some(tunnel_response::Payload::HttpChunk(chunk)),
            })
            .await
            .map_err(|error| Box::new(error) as ActionError)
    }

    /// 把 HTTP protocol error 包装进 TunnelResponse；空 request ID 自动回填。
    async fn send_error(&self, mut response: HttpTunnelResponseError) -> Result<(), ActionError> {
        if response.request_id.is_empty() {
            response.request_id.clone_from(&self.request_id);
        }
        self.client
            .send_response(TunnelResponse {
                payload: Some(tunnel_response::Payload::HttpError(response)),
            })
            .await
            .map_err(|error| Box::new(error) as ActionError)
    }
}

#[async_trait]
impl WsResponseSender for ClientWsResponseSender {
    /// 包装本地 WS 握手成功响应并回填空 tunnel ID。
    async fn send_opened(&self, mut response: WsTunnelOpened) -> Result<(), WsSenderError> {
        if response.tunnel_id.is_empty() {
            response.tunnel_id.clone_from(&self.tunnel_id);
        }
        self.client
            .send_response(TunnelResponse {
                payload: Some(tunnel_response::Payload::WsOpened(response)),
            })
            .await
            .map_err(|error| Box::new(error) as WsSenderError)
    }

    /// 包装本地 WS data message 并回填空 tunnel ID。
    async fn send_message(&self, mut message: WsTunnelMessage) -> Result<(), WsSenderError> {
        if message.tunnel_id.is_empty() {
            message.tunnel_id.clone_from(&self.tunnel_id);
        }
        self.client
            .send_response(TunnelResponse {
                payload: Some(tunnel_response::Payload::WsMessage(message)),
            })
            .await
            .map_err(|error| Box::new(error) as WsSenderError)
    }

    /// 包装本地 WS close message 并回填空 tunnel ID。
    async fn send_close(&self, mut message: WsTunnelClose) -> Result<(), WsSenderError> {
        if message.tunnel_id.is_empty() {
            message.tunnel_id.clone_from(&self.tunnel_id);
        }
        self.client
            .send_response(TunnelResponse {
                payload: Some(tunnel_response::Payload::WsClose(message)),
            })
            .await
            .map_err(|error| Box::new(error) as WsSenderError)
    }

    /// 包装本地 WS protocol error 并回填空 tunnel ID。
    async fn send_error(&self, mut response: WsTunnelError) -> Result<(), WsSenderError> {
        if response.tunnel_id.is_empty() {
            response.tunnel_id.clone_from(&self.tunnel_id);
        }
        self.client
            .send_response(TunnelResponse {
                payload: Some(tunnel_response::Payload::WsError(response)),
            })
            .await
            .map_err(|error| Box::new(error) as WsSenderError)
    }
}

/// 使用进程级 `ProxyFromEnvironment` 快照建立 WebSocket。
///
/// 未命中代理时使用 tokio-tungstenite 的直连路径；命中 HTTP 代理
/// 时先建立 CONNECT tunnel，再将同一 TcpStream 交给 rustls/WebSocket
/// handshake。因此 `wss` 始终校验目标主机证书，不会因为代理而降级。
pub(super) async fn connect_websocket(
    request: WebSocketRequest,
) -> Result<(LocalWebSocket, WebSocketResponse), GorillaDialFailure> {
    connect_websocket_with_config(request, gorilla_websocket_config()).await
}

/// 使用进程级代理快照和调用方提供的 WebSocket 配置建立连接。
///
/// 主 Tunnel 与本地 [`WsHandler`](super::WsHandler) 共用这一入口，确保直连、
/// HTTP CONNECT 和 SOCKS5 三条拨号路径采用相同的 frame/message 限制。配置按值
/// 移入连接，不保存任何指向 handler 的反向引用。
pub(super) async fn connect_websocket_with_config(
    request: WebSocketRequest,
    websocket_config: WebSocketConfig,
) -> Result<(LocalWebSocket, WebSocketResponse), GorillaDialFailure> {
    let config = ENVIRONMENT_PROXY_CONFIG.get_or_init(EnvironmentProxyConfig::from_process);
    connect_websocket_with_proxy_and_websocket_config(request, config, websocket_config).await
}

/// 使用显式代理快照建立 WebSocket，供生产路径与无环境竞态的测试共用。
///
/// 返回错误已经转换成 Gorilla 的错误文本；调用方仍可通过
/// [`GorillaDialFailure::response_status`] 区分带 HTTP response 的坏握手。
#[cfg(test)]
pub(super) async fn connect_websocket_with_proxy_config(
    request: WebSocketRequest,
    config: &EnvironmentProxyConfig,
) -> Result<(LocalWebSocket, WebSocketResponse), GorillaDialFailure> {
    connect_websocket_with_proxy_and_websocket_config(request, config, gorilla_websocket_config())
        .await
}

/// 使用显式代理快照和显式 WebSocket 配置建立连接。
///
/// 该内部汇合点保证直连、HTTP CONNECT 与 SOCKS5 三条路径使用完全相同的
/// frame/message 限制和标准 WebSocket 握手规则。配置只会被当前连接持有，
/// 不会写回进程级代理快照。
async fn connect_websocket_with_proxy_and_websocket_config(
    request: WebSocketRequest,
    config: &EnvironmentProxyConfig,
    websocket_config: WebSocketConfig,
) -> Result<(LocalWebSocket, WebSocketResponse), GorillaDialFailure> {
    let target = Url::parse(&request.uri().to_string()).map_err(|error| {
        GorillaDialFailure::from_websocket_error(WebSocketError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            error.to_string(),
        )))
    })?;
    let proxy = proxy_selection_for_websocket_target(&target, config)
        .map_err(GorillaDialFailure::from_websocket_error)?;
    let Some(proxy) = proxy else {
        return connect_async_with_config(request, Some(websocket_config), false)
            .await
            .map_err(GorillaDialFailure::from_websocket_error);
    };
    let stream = connect_via_proxy(&proxy, &target)
        .await
        .map_err(GorillaDialFailure::from_websocket_error)?;
    client_async_tls_with_config(request, stream, Some(websocket_config), None)
        .await
        .map_err(GorillaDialFailure::from_websocket_error)
}

/// 构造 Tunnel 使用的标准 WebSocket 配置。
///
/// Go 路径没有调用 `SetReadLimit`，因此 text/binary 消息和单帧都没有
/// Gorilla 层的 64 MiB/16 MiB 上限。tungstenite 默认值恰好带这两个
/// 上限，必须显式取消。握手校验、子协议协商和 text UTF-8 校验则直接采用
/// 上游 Tungstenite 的 RFC 行为，不再维护项目私有的 Gorilla 补丁。
#[must_use]
pub(super) fn gorilla_websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(None)
        .max_frame_size(None)
}

/// 在一个不可重置的 deadline 内读取下一条 Tunnel 应用消息。
///
/// 对应 Go `(*Client).readLoop` 中一次 `SetReadDeadline` 加
/// `ReadMessage`。`reader` 的 ping/pong 和库内部 frame 在同一 deadline
/// 内继续读，不得通过刷控制帧让连接永不超时。`on_pong` 只更新
/// 独立的 90 秒 pong 存活时钟，不影响本 deadline。
///
/// # 返回
/// 成功时返回 text/binary 的原始字节；调用方取消、连接结束、
/// WebSocket 读错、close frame 或绝对 deadline 到期时返回兼容错误。
pub(super) async fn read_tunnel_data_message<S, F>(
    cancellation: &CancellationToken,
    reader: &mut S,
    read_timeout: Duration,
    mut on_pong: F,
) -> Result<Vec<u8>, TunnelError>
where
    S: futures_util::Stream<Item = Result<Message, WebSocketError>> + Unpin,
    F: FnMut(),
{
    let read_until_data = async {
        loop {
            let Some(next) = reader.next().await else {
                return Err(TunnelError::message(
                    "read tunnel request: connection closed",
                ));
            };
            let message = next
                .map_err(|error| TunnelError::message(format!("read tunnel request: {error}")))?;
            match message {
                Message::Text(data) => return Ok(data.as_bytes().to_vec()),
                Message::Binary(data) => return Ok(data.to_vec()),
                Message::Pong(_) => on_pong(),
                Message::Ping(_) => {}
                Message::Frame(_) => {}
                Message::Close(frame) => {
                    return Err(TunnelError::message(format!(
                        "read tunnel request: websocket close: {frame:?}"
                    )));
                }
            }
        }
    };

    tokio::select! {
        () = cancellation.cancelled() => Err(TunnelError::message("context canceled")),
        result = tokio::time::timeout(read_timeout, read_until_data) => {
            result.unwrap_or_else(|_| Err(TunnelError::message("read tunnel request: i/o timeout")))
        }
    }
}

/// 按 Go `http.ProxyFromEnvironment` 规则为 ws/wss 目标选择代理。
///
/// `ws` 对应 `HTTP_PROXY`，`wss` 对应 `HTTPS_PROXY`。CGI 进程中使用
/// `HTTP_PROXY` 会在 NO_PROXY 判定前拒绝，复刻 Go 防 header 注入的安全顺序。
#[cfg(test)]
pub(super) fn proxy_for_websocket_target(
    target: &Url,
    config: &EnvironmentProxyConfig,
) -> Result<Option<Url>, WebSocketError> {
    Ok(proxy_selection_for_websocket_target(target, config)?.map(|proxy| proxy.url))
}

/// 选择代理并保留解析库无法表达的显式空 password 标志。
///
/// 该内部版本是实际拨号路径；仅在测试构建中存在的 `proxy_for_websocket_target` 只返回
/// URL，而 CONNECT 认证必须使用这里随 URL 一起保存的 `password_set`。
fn proxy_selection_for_websocket_target(
    target: &Url,
    config: &EnvironmentProxyConfig,
) -> Result<Option<ProxySelection>, WebSocketError> {
    let (raw_proxy, is_http) = match target.scheme() {
        "ws" => (&config.http_proxy, true),
        "wss" => (&config.https_proxy, false),
        _ => return Ok(None),
    };
    let Some(proxy) = parse_environment_proxy_selection(raw_proxy) else {
        return Ok(None);
    };
    if is_http && config.cgi {
        return Err(WebSocketError::Io(io::Error::other(
            "refusing to use HTTP_PROXY value in CGI environment; see golang.org/s/cgihttpproxy",
        )));
    }
    if should_bypass_proxy(target, &config.no_proxy) {
        return Ok(None);
    }
    Ok(Some(proxy))
}

/// 读取两个大小写环境名中第一个非空值。
fn first_non_empty_environment(upper: &str, lower: &str) -> String {
    [upper, lower]
        .into_iter()
        .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
        .unwrap_or_default()
}

/// 解析代理 URL，并从未规范化的 authority 恢复 Go `passwordSet`。
///
/// Go 在初始化代理配置时忽略无法解析的值，因此这里返回 `None`，而不把一个无效
/// 环境变量变成每次连接都失败。纯 `host[:port]` 与 Go 一样补 `http://`。只有
/// user-info 中未转义的字面冒号表示密码分隔符；`%3A` 是用户名数据，不能误判。
fn parse_environment_proxy_selection(value: &str) -> Option<ProxySelection> {
    if value.is_empty() {
        return None;
    }
    let (url, raw_url) = match Url::parse(value) {
        Ok(parsed) if parsed.has_host() => (parsed, value.to_owned()),
        _ => {
            let raw_url = format!("http://{value}");
            let parsed = Url::parse(&raw_url).ok().filter(Url::has_host)?;
            (parsed, raw_url)
        }
    };
    Some(ProxySelection {
        url,
        password_set: proxy_url_has_explicit_password(&raw_url),
    })
}

/// 检查原始代理 authority 的 user-info 是否包含未转义密码分隔符。
///
/// Go `net/url` 以最后一个 `@` 前的第一个字面 `:` 设置 `passwordSet`。这里先截掉
/// path/query/fragment，再使用最后一个 `@`，从而允许 percent-encoded 字符且不会把
/// host 端口冒号误当成密码。
fn proxy_url_has_explicit_password(raw_url: &str) -> bool {
    let Some((_, after_scheme)) = raw_url.split_once("://") else {
        return false;
    };
    let authority_end = after_scheme
        .find(['/', '?', '#'])
        .unwrap_or(after_scheme.len());
    let authority = &after_scheme[..authority_end];
    let Some(user_info_end) = authority.rfind('@') else {
        return false;
    };
    authority[..user_info_end].contains(':')
}

/// 判定 loopback 特例与 `NO_PROXY` 列表是否要求直连。
fn should_bypass_proxy(target: &Url, no_proxy: &str) -> bool {
    let Some(host) = url_host_text(target).map(|host| host.to_ascii_lowercase()) else {
        return false;
    };
    let target_ip = url_host_ip(target);
    if host == "localhost" || target_ip.is_some_and(|ip| ip.is_loopback()) {
        return true;
    }
    let port = target
        .port_or_known_default()
        .unwrap_or_else(|| if target.scheme() == "wss" { 443 } else { 80 })
        .to_string();
    no_proxy
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .any(|entry| no_proxy_entry_matches(entry, &host, &port, target_ip))
}

/// 匹配一条 Go `httpproxy` 规则，包括 `*`、CIDR、IP、端口和域名。
fn no_proxy_entry_matches(entry: &str, host: &str, port: &str, target_ip: Option<IpAddr>) -> bool {
    let entry = entry.trim().to_ascii_lowercase();
    if entry == "*" {
        return true;
    }
    if let Some(target_ip) = target_ip
        && cidr_contains(&entry, target_ip)
    {
        return true;
    }

    let (entry_host, entry_port) = split_no_proxy_host_port(&entry);
    if entry_host.is_empty() || entry_port.is_some_and(|entry_port| entry_port != port) {
        return false;
    }
    let entry_host = entry_host.trim_matches(['[', ']']);
    if let (Some(target_ip), Ok(entry_ip)) = (target_ip, entry_host.parse::<IpAddr>()) {
        return target_ip == entry_ip;
    }
    if target_ip.is_some() {
        return false;
    }

    let entry_host = entry_host.strip_prefix("*.").map_or(entry_host, |rest| {
        // Go 把 `*.example.com` 视为前导点规则，只匹配子域名。
        // 为共用下面的后缀比较，恢复一个前导点。
        rest
    });
    let subdomains_only = entry.starts_with('.') || entry.starts_with("*.");
    let unnormalized = entry_host.trim_start_matches('.');
    let normalized = normalize_no_proxy_domain(unnormalized);
    if subdomains_only {
        host != normalized && host.ends_with(&format!(".{normalized}"))
    } else {
        host == normalized || host.ends_with(&format!(".{normalized}"))
    }
}

/// 把 URL host 转为不含 IPv6 方括号的网络文本。
///
/// `Url::host_str` 对 IPv6 保留 `[]`，直接做 `IpAddr` 解析会使
/// loopback 与 CIDR 规则全部失效，因此必须使用强类型 [`Host`]。
fn url_host_text(url: &Url) -> Option<String> {
    match url.host()? {
        Host::Domain(domain) => Some(domain.to_owned()),
        Host::Ipv4(address) => Some(address.to_string()),
        Host::Ipv6(address) => Some(address.to_string()),
    }
}

/// 如果 URL host 是 IP literal，返回无括号的强类型地址。
fn url_host_ip(url: &Url) -> Option<IpAddr> {
    match url.host()? {
        Host::Domain(_) => None,
        Host::Ipv4(address) => Some(IpAddr::V4(address)),
        Host::Ipv6(address) => Some(IpAddr::V6(address)),
    }
}

/// 对 NO_PROXY 域名执行与 Go `idnaASCII` 等价的 IDNA ToASCII。
///
/// 复用 `url` crate 已使用的 WHATWG/IDNA host parser，避免再引入
/// 一套可能与目标 URL 解析不一致的 Unicode 域名规则。
fn normalize_no_proxy_domain(domain: &str) -> String {
    Url::parse(&format!("http://{domain}/"))
        .ok()
        .and_then(|url| match url.host()? {
            Host::Domain(domain) => Some(domain.to_ascii_lowercase()),
            Host::Ipv4(_) | Host::Ipv6(_) => None,
        })
        .unwrap_or_else(|| domain.to_ascii_lowercase())
}

/// 把 NO_PROXY 的 host 和可选端口拆开，不误分裸 IPv6 地址。
fn split_no_proxy_host_port(entry: &str) -> (&str, Option<&str>) {
    if let Some(rest) = entry.strip_prefix('[')
        && let Some(end) = rest.find(']')
    {
        let host_end = end + 2;
        let port = entry
            .get(host_end..)
            .and_then(|suffix| suffix.strip_prefix(':'));
        return (&entry[..host_end], port);
    }
    if entry.parse::<IpAddr>().is_ok() {
        return (entry, None);
    }
    if entry.bytes().filter(|byte| *byte == b':').count() == 1
        && let Some((host, port)) = entry.rsplit_once(':')
    {
        return (host, Some(port));
    }
    (entry, None)
}

/// 使用整数掩码匹配 IPv4/IPv6 CIDR，无效规则按 Go 语义忽略。
fn cidr_contains(entry: &str, target: IpAddr) -> bool {
    let Some((network, prefix)) = entry.split_once('/') else {
        return false;
    };
    let Ok(network) = network.parse::<IpAddr>() else {
        return false;
    };
    let Ok(prefix) = prefix.parse::<u32>() else {
        return false;
    };
    match (network, target) {
        (IpAddr::V4(network), IpAddr::V4(target)) if prefix <= 32 => {
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            u32::from(network) & mask == u32::from(target) & mask
        }
        (IpAddr::V6(network), IpAddr::V6(target)) if prefix <= 128 => {
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            u128::from(network) & mask == u128::from(target) & mask
        }
        _ => false,
    }
}

/// 生成 CONNECT request 使用的带端口 authority，IPv6 保留方括号。
fn websocket_target_authority(target: &Url) -> Result<String, WebSocketError> {
    let host = target.host().ok_or_else(|| {
        WebSocketError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "websocket URL has no host",
        ))
    })?;
    let port = target
        .port_or_known_default()
        .unwrap_or_else(|| if target.scheme() == "wss" { 443 } else { 80 });
    match host {
        Host::Ipv6(address) => Ok(format!("[{address}]:{port}")),
        Host::Domain(domain) => Ok(format!("{domain}:{port}")),
        Host::Ipv4(address) => Ok(format!("{address}:{port}")),
    }
}

/// 按 Gorilla `proxy_FromURL` 支持的 scheme 建立透明代理连接。
///
/// `http` 使用 CONNECT，`socks5` 使用 RFC 1928/1929；其他
/// scheme 返回 Gorilla 风格的 `proxy: unknown scheme` 错误。
async fn connect_via_proxy(
    proxy: &ProxySelection,
    target: &Url,
) -> Result<TcpStream, WebSocketError> {
    match proxy.url.scheme() {
        "http" => {
            let target_authority = websocket_target_authority(target)?;
            connect_via_http_proxy(&proxy.url, &target_authority, proxy.password_set).await
        }
        "socks5" => connect_via_socks5_proxy(&proxy.url, target).await,
        scheme => Err(WebSocketError::Io(io::Error::other(format!(
            "proxy: unknown scheme: {scheme}"
        )))),
    }
}

/// 连接 HTTP proxy，发送 CONNECT 并在 200 响应后返回透明 TCP stream。
///
/// Gorilla WebSocket 为 `http` 注册 CONNECT dialer；本函数只处理
/// 该分支，其他 scheme 由 [`connect_via_proxy`] 分派。
async fn connect_via_http_proxy(
    proxy: &Url,
    target_authority: &str,
    password_set: bool,
) -> Result<TcpStream, WebSocketError> {
    let proxy_host = url_host_text(proxy).ok_or_else(|| {
        WebSocketError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "proxy URL has no host",
        ))
    })?;
    let proxy_port = proxy.port_or_known_default().unwrap_or(80);
    let mut stream = TcpStream::connect((proxy_host, proxy_port))
        .await
        .map_err(WebSocketError::Io)?;

    // `http.Request.Write` 会在 Host 后自动写这一 UA；代理或差分
    // probe 可观测到 CONNECT 原始字节，顺序不能省略。
    let mut connect_request = format!(
        "CONNECT {target_authority} HTTP/1.1\r\nHost: {target_authority}\r\nUser-Agent: Go-http-client/1.1\r\n"
    );
    if password_set {
        let mut credentials = percent_decode_user_info(proxy.username());
        credentials.push(b':');
        credentials.extend(percent_decode_user_info(
            proxy.password().unwrap_or_default(),
        ));
        connect_request.push_str("Proxy-Authorization: Basic ");
        connect_request.push_str(&STANDARD.encode(credentials));
        connect_request.push_str("\r\n");
    }
    connect_request.push_str("\r\n");
    stream
        .write_all(connect_request.as_bytes())
        .await
        .map_err(WebSocketError::Io)?;

    let response_header = read_proxy_response_header(&mut stream).await?;
    let status_line = response_header
        .split(|byte| *byte == b'\n')
        .next()
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
        .ok_or_else(|| WebSocketError::Io(io::Error::other("malformed proxy response")))?;
    let status_line = std::str::from_utf8(status_line)
        .map_err(|_| WebSocketError::Io(io::Error::other("malformed proxy response")))?;
    let mut fields = status_line.splitn(3, ' ');
    let _http_version = fields.next();
    let status_code = fields
        .next()
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| WebSocketError::Io(io::Error::other("malformed proxy response")))?;
    let reason = fields.next().unwrap_or_default();
    if status_code != 200 {
        // Gorilla 对非 200 返回 `resp.Status` 中第一个空格后的 reason。
        let message = if reason.is_empty() {
            status_code.to_string()
        } else {
            reason.to_owned()
        };
        return Err(WebSocketError::Io(io::Error::other(message)));
    }
    Ok(stream)
}

/// 使用 RFC 1928 SOCKS5（可选 RFC 1929 用户名密码）连接目标。
///
/// 域名以 SOCKS5 domain address 交给代理解析，不在本地做 DNS，
/// 与 Go `x/net/proxy.SOCKS5` 一致。成功返回的 TCP stream 会在上层
/// 按 `ws`/`wss` 继续 WebSocket 或 rustls 握手。
async fn connect_via_socks5_proxy(proxy: &Url, target: &Url) -> Result<TcpStream, WebSocketError> {
    let proxy_host = url_host_text(proxy).ok_or_else(|| {
        WebSocketError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "proxy URL has no host",
        ))
    })?;
    // Gorilla 把 proxy URL 的 `u.Host` 原样交给底层 Dialer；与 HTTP
    // proxy 不同，SOCKS5 URL 没有隐式 1080。因此在真正发起 DNS/连接前
    // 复刻 net.Dial 的缺端口错误，避免错误地连到一个未配置的端口。
    let proxy_port = socks5_proxy_port(proxy).ok_or_else(|| {
        let authority = match proxy.host() {
            Some(Host::Ipv6(address)) => format!("[{address}]"),
            Some(Host::Domain(domain)) => domain.to_owned(),
            Some(Host::Ipv4(address)) => address.to_string(),
            None => String::new(),
        };
        WebSocketError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("dial tcp: address {authority}: missing port in address"),
        ))
    })?;
    let proxy_address = match proxy.host() {
        Some(Host::Ipv6(address)) => format!("[{address}]:{proxy_port}"),
        Some(Host::Domain(domain)) => format!("{domain}:{proxy_port}"),
        Some(Host::Ipv4(address)) => format!("{address}:{proxy_port}"),
        None => {
            return Err(WebSocketError::Io(io::Error::other(
                "proxy URL has no host",
            )));
        }
    };
    let mut stream = TcpStream::connect((proxy_host.as_str(), proxy_port))
        .await
        .map_err(WebSocketError::Io)?;

    let username = percent_decode_user_info(proxy.username());
    let password = percent_decode_user_info(proxy.password().unwrap_or_default());
    let has_auth = should_offer_socks5_auth(&username, &password);
    let methods: &[u8] = if has_auth { &[0x00, 0x02] } else { &[0x00] };
    let mut greeting = vec![0x05, u8::try_from(methods.len()).unwrap_or(0)];
    greeting.extend_from_slice(methods);
    stream.write_all(&greeting).await.map_err(|error| {
        WebSocketError::Io(io::Error::new(
            error.kind(),
            format!("proxy: failed to write greeting to SOCKS5 proxy at {proxy_address}: {error}"),
        ))
    })?;
    let mut selection = [0_u8; 2];
    stream.read_exact(&mut selection).await.map_err(|error| {
        WebSocketError::Io(io::Error::new(
            error.kind(),
            format!("proxy: failed to read greeting from SOCKS5 proxy at {proxy_address}: {error}"),
        ))
    })?;
    if selection[0] != 0x05 {
        return Err(WebSocketError::Io(io::Error::other(format!(
            "proxy: SOCKS5 proxy at {proxy_address} has unexpected version {}",
            selection[0]
        ))));
    }
    match selection[1] {
        0x00 => {}
        0x02 => authenticate_socks5(&mut stream, &proxy_address, &username, &password).await?,
        0xff => {
            return Err(WebSocketError::Io(io::Error::other(format!(
                "proxy: SOCKS5 proxy at {proxy_address} requires authentication"
            ))));
        }
        // x/net/proxy 对代理返回的其他 method 不作预先限制，
        // 而是直接继续 CONNECT，这个宽容分支不能自作主张收紧。
        _ => {}
    }

    let port = target
        .port_or_known_default()
        .unwrap_or_else(|| if target.scheme() == "wss" { 443 } else { 80 });
    if port == 0 {
        return Err(WebSocketError::Io(io::Error::other(
            "proxy: port number out of range: 0",
        )));
    }
    let host = target.host().ok_or_else(|| {
        WebSocketError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "websocket URL has no host",
        ))
    })?;
    let mut request = vec![0x05, 0x01, 0x00];
    match host {
        Host::Ipv4(address) => {
            request.push(0x01);
            request.extend_from_slice(&address.octets());
        }
        Host::Ipv6(address) => {
            request.push(0x04);
            request.extend_from_slice(&address.octets());
        }
        Host::Domain(domain) => {
            let length = u8::try_from(domain.len()).map_err(|_| {
                WebSocketError::Io(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("proxy: destination host name too long: {domain}"),
                ))
            })?;
            request.extend_from_slice(&[0x03, length]);
            request.extend_from_slice(domain.as_bytes());
        }
    }
    request.extend_from_slice(&port.to_be_bytes());
    stream.write_all(&request).await.map_err(|error| {
        WebSocketError::Io(io::Error::new(
            error.kind(),
            format!(
                "proxy: failed to write connect request to SOCKS5 proxy at {proxy_address}: {error}"
            ),
        ))
    })?;
    read_socks5_connect_response(&mut stream, &proxy_address).await?;
    Ok(stream)
}

/// 返回 SOCKS5 proxy URL 中显式声明的端口。
///
/// Gorilla v1.5.3 的 `proxy_FromURL` 会把 `u.Host` 原样交给
/// `proxy_SOCKS5`，不会应用 `httpproxy.portMap` 中的 1080；因此
/// 未声明端口必须返回 `None`，由调用方生成底层 Dial 的缺端口错误。
pub(super) fn socks5_proxy_port(proxy: &Url) -> Option<u16> {
    proxy.port()
}

/// 判定 SOCKS5 greeting 是否应提供 RFC 1929 认证方法。
///
/// Go 只在用户名非空且用户名/密码都可由单字节长度
/// 表示时 offer method 2；超长或空用户名会只 offer no-auth。
pub(super) fn should_offer_socks5_auth(username: &[u8], password: &[u8]) -> bool {
    !username.is_empty() && username.len() < 256 && password.len() < 256
}

/// 执行 RFC 1929 SOCKS5 用户名/密码子协议。
async fn authenticate_socks5(
    stream: &mut TcpStream,
    proxy_address: &str,
    username: &[u8],
    password: &[u8],
) -> Result<(), WebSocketError> {
    // Go 在子协议层使用 uint8 cast；正常只有 greeting 已
    // offer method 2 才会进入，此时长度已被限制在 255 以内。
    let username_len = username.len() as u8;
    let password_len = password.len() as u8;
    let mut request = vec![0x01, username_len];
    request.extend_from_slice(username);
    request.push(password_len);
    request.extend_from_slice(password);
    stream
        .write_all(&request)
        .await
        .map_err(|error| {
            WebSocketError::Io(io::Error::new(
                error.kind(),
                format!(
                    "proxy: failed to write authentication request to SOCKS5 proxy at {proxy_address}: {error}"
                ),
            ))
        })?;
    let mut response = [0_u8; 2];
    stream
        .read_exact(&mut response)
        .await
        .map_err(|error| {
            WebSocketError::Io(io::Error::new(
                error.kind(),
                format!(
                    "proxy: failed to read authentication reply from SOCKS5 proxy at {proxy_address}: {error}"
                ),
            ))
        })?;
    if response[1] != 0x00 {
        return Err(WebSocketError::Io(io::Error::other(format!(
            "proxy: SOCKS5 proxy at {proxy_address} rejected username/password"
        ))));
    }
    Ok(())
}

/// 读取 SOCKS5 CONNECT 结果并消费可变长度的 bind address。
async fn read_socks5_connect_response(
    stream: &mut TcpStream,
    proxy_address: &str,
) -> Result<(), WebSocketError> {
    let mut header = [0_u8; 4];
    stream.read_exact(&mut header).await.map_err(|error| {
        WebSocketError::Io(io::Error::new(
            error.kind(),
            format!(
                "proxy: failed to read connect reply from SOCKS5 proxy at {proxy_address}: {error}"
            ),
        ))
    })?;
    let failure = match header[1] {
        0 => None,
        1 => Some("general failure"),
        2 => Some("connection forbidden"),
        3 => Some("network unreachable"),
        4 => Some("host unreachable"),
        5 => Some("connection refused"),
        6 => Some("TTL expired"),
        7 => Some("command not supported"),
        8 => Some("address type not supported"),
        _ => Some("unknown error"),
    };
    if let Some(failure) = failure {
        return Err(WebSocketError::Io(io::Error::other(format!(
            "proxy: SOCKS5 proxy at {proxy_address} failed to connect: {failure}"
        ))));
    }
    let address_len = match header[3] {
        0x01 => 4,
        0x04 => 16,
        0x03 => {
            let mut length = [0_u8; 1];
            stream
                .read_exact(&mut length)
                .await
                .map_err(|error| {
                    WebSocketError::Io(io::Error::new(
                        error.kind(),
                        format!(
                            "proxy: failed to read domain length from SOCKS5 proxy at {proxy_address}: {error}"
                        ),
                    ))
                })?;
            usize::from(length[0])
        }
        _ => {
            return Err(WebSocketError::Io(io::Error::other(format!(
                "proxy: got unknown address type {} from SOCKS5 proxy at {proxy_address}",
                header[3]
            ))));
        }
    };
    let mut address = vec![0_u8; address_len];
    stream.read_exact(&mut address).await.map_err(|error| {
        WebSocketError::Io(io::Error::new(
            error.kind(),
            format!("proxy: failed to read address from SOCKS5 proxy at {proxy_address}: {error}"),
        ))
    })?;
    // Gorilla 内嵌 x_net_proxy.go 在消费 bind address 后单独 ReadFull
    // 两字节端口。分开读取不仅保留顺序，也确保 address 完整而 port
    // 截断时返回 `failed to read port`，不能合并成 address 错误。
    let mut port = [0_u8; 2];
    stream.read_exact(&mut port).await.map_err(|error| {
        WebSocketError::Io(io::Error::new(
            error.kind(),
            format!("proxy: failed to read port from SOCKS5 proxy at {proxy_address}: {error}"),
        ))
    })?;
    Ok(())
}

/// 读取到完整 `\r\n\r\n`，并限制代理响应 header 的内存使用。
async fn read_proxy_response_header(stream: &mut TcpStream) -> Result<Vec<u8>, WebSocketError> {
    let mut header = Vec::with_capacity(1024);
    let mut byte = [0_u8; 1];
    while header.len() < MAX_PROXY_RESPONSE_HEADER {
        let read = stream.read(&mut byte).await.map_err(WebSocketError::Io)?;
        if read == 0 {
            return Err(WebSocketError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF reading proxy response",
            )));
        }
        header.push(byte[0]);
        if header.ends_with(b"\r\n\r\n") {
            return Ok(header);
        }
    }
    Err(WebSocketError::Io(io::Error::other(
        "proxy response header too large",
    )))
}

/// 对 proxy URL user-info 执行 percent decode；`+` 在 URL user-info 中保持原字节。
fn percent_decode_user_info(value: &str) -> Vec<u8> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let (Some(high), Some(low)) =
                (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
        {
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    decoded
}

/// 将单个 ASCII 十六进制字符转为数值。
fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

/// 对应 Go：`addJitter`，返回 `[base, base + base/2)` 的随机时长。
#[must_use]
pub fn add_jitter(base: Duration) -> Duration {
    let window = base / 2;
    if window.is_zero() {
        return base;
    }
    let upper = u64::try_from(window.as_nanos()).unwrap_or(u64::MAX);
    base.saturating_add(Duration::from_nanos(rand::rng().random_range(0..upper)))
}

/// 解码允许未知字段的 Tunnel ProtoJSON。
///
/// Go 先由 `decodeTunnelValue` 支持动态 interface，再分别通过 `asHTTPRequest`、
/// `asHTTPCancel`、`asWSOpen`、`asWSMessage` 与 `asWSClose` 做运行时断言；Rust 在
/// 此处一次解码为强类型 oneof，后续 match 已保证每个分支拿到正确消息类型。
///
/// pbjson 默认拒绝未知字段，而 Go 明确使用 `DiscardUnknown: true`；因此先只删除消息
/// schema 以外的 key，再交给生成的反序列化器处理别名、Base64、数值范围和 oneof 冲突。
fn decode_tunnel_request(data: &[u8]) -> Result<TunnelRequest, TunnelError> {
    let mut value: Value = serde_json::from_slice(data)
        .map_err(|error| TunnelError::message(format!("decode tunnel request: {error}")))?;
    discard_unknown_tunnel_fields(&mut value);
    serde_json::from_value(value)
        .map_err(|error| TunnelError::message(format!("decode tunnel request: {error}")))
}

/// 按 TunnelRequest 及其嵌套消息 schema 原地删除未知 JSON 字段。
fn discard_unknown_tunnel_fields(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    retain_fields(
        object,
        &[
            "sessionId",
            "session_id",
            "httpRequest",
            "http_request",
            "httpCancel",
            "http_cancel",
            "wsOpen",
            "ws_open",
            "wsMessage",
            "ws_message",
            "wsClose",
            "ws_close",
        ],
    );
    for key in ["httpRequest", "http_request"] {
        if let Some(message) = object.get_mut(key) {
            sanitize_http_request(message);
        }
    }
    for key in ["httpCancel", "http_cancel"] {
        if let Some(message) = object.get_mut(key)
            && let Some(message) = message.as_object_mut()
        {
            retain_fields(message, &["requestId", "request_id"]);
        }
    }
    for key in ["wsOpen", "ws_open"] {
        if let Some(message) = object.get_mut(key) {
            sanitize_ws_open(message);
        }
    }
    for key in ["wsMessage", "ws_message"] {
        if let Some(message) = object.get_mut(key)
            && let Some(message) = message.as_object_mut()
        {
            retain_fields(
                message,
                &["tunnelId", "tunnel_id", "data", "isBinary", "is_binary"],
            );
        }
    }
    for key in ["wsClose", "ws_close"] {
        if let Some(message) = object.get_mut(key)
            && let Some(message) = message.as_object_mut()
        {
            retain_fields(message, &["tunnelId", "tunnel_id", "code", "reason"]);
        }
    }
}

/// 删除 HTTP request 及 header 消息中的未知字段。
fn sanitize_http_request(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    retain_fields(
        object,
        &[
            "requestId",
            "request_id",
            "port",
            "method",
            "path",
            "headers",
            "body",
        ],
    );
    sanitize_headers(object.get_mut("headers"));
}

/// 删除 WebSocket open 及 header 消息中的未知字段。
fn sanitize_ws_open(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    retain_fields(
        object,
        &[
            "tunnelId",
            "tunnel_id",
            "port",
            "path",
            "headers",
            "protocols",
        ],
    );
    sanitize_headers(object.get_mut("headers"));
}

/// 删除 repeated Header 数组元素中的未知字段。
fn sanitize_headers(value: Option<&mut Value>) {
    let Some(headers) = value.and_then(Value::as_array_mut) else {
        return;
    };
    for header in headers {
        if let Some(header) = header.as_object_mut() {
            retain_fields(header, &["name", "value"]);
        }
    }
}

/// 仅保留允许的 JSON object key，其他 key 模拟 ProtoJSON DiscardUnknown 丢弃。
fn retain_fields(object: &mut serde_json::Map<String, Value>, allowed: &[&str]) {
    object.retain(|key, _| allowed.contains(&key.as_str()));
}

/// 构造稳定顺序的结构化日志属性。
fn log_attributes<const N: usize>(pairs: [(&str, Value); N]) -> LogAttributes {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}
