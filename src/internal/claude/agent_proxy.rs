//! CCRv2 CONNECT-over-WebSocket agent proxy 中继实现。

use std::{
    ffi::OsString,
    future::pending,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context as _, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures_util::{SinkExt as _, StreamExt as _};
use http::header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use httparse::{EMPTY_HEADER, Request, Status};
use nix::sys::prctl::set_dumpable;
use tokio::{
    io::{AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::oneshot,
    task::{JoinHandle, JoinSet},
    time::{Instant, interval_at},
};
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest as _,
    protocol::{Message, WebSocketConfig},
};
use tokio_util::sync::CancellationToken;
use url::Url;

use prost::Message as ProstMessage;

use crate::internal::tunnel::{
    LocalWebSocketSink, LocalWebSocketStream, connect_websocket_with_config,
};

use super::types::ClaudeError;

pub(super) const STALE_RELAY_TOKEN_PATH: &str = "/run/ccr/session_token";
const SYSTEM_CA_BUNDLE_PATH: &str = "/etc/ssl/certs/ca-certificates.crt";
const CA_CERT_PATH: &str = "/v1/code/upstreamproxy/ca-cert";
const WEBSOCKET_PATH: &str = "/v1/code/upstreamproxy/ws";
const CA_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(5);
const WEBSOCKET_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CONNECT_HEADER_BYTES: usize = 8 * 1024;
const MAX_CONNECT_HEADERS: usize = 64;
const MAX_CHUNK_BYTES: usize = 512 * 1024;
const MAX_ENCODED_CHUNK_BYTES: usize = MAX_CHUNK_BYTES + 4;
const TCP_BUFFER_BYTES: usize = 32 * 1024;
#[cfg(not(test))]
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(test)]
const KEEPALIVE_INTERVAL: Duration = Duration::from_millis(50);

/// CCRv2 CONNECT-over-WebSocket 私有 wire message；不属于公共 Tunnel API。
#[derive(Clone, PartialEq, prost::Message)]
struct UpstreamProxyChunk {
    /// `optional` 让空 payload 仍编码为稳定的 `0a 00` keepalive。
    #[prost(bytes = "vec", optional, tag = "1")]
    data: Option<Vec<u8>>,
}

const NO_PROXY_LIST: &str = concat!(
    "localhost,127.0.0.1,::1,169.254.0.0/16,10.0.0.0/8,172.16.0.0/12,",
    "192.168.0.0/16,anthropic.com,.anthropic.com,*.anthropic.com,github.com,",
    "api.github.com,*.github.com,*.githubusercontent.com,registry.npmjs.org,pypi.org,",
    "files.pythonhosted.org,index.crates.io,proxy.golang.org"
);

/// 启动 relay 所需的会话配置；所有字段只在 Claude 子系统内构造。
pub(super) struct AgentProxyConfig {
    pub(super) api_base_url: String,
    pub(super) session_id: String,
    pub(super) token: String,
}

/// 已启动的本地 relay。显式 shutdown 负责等待 listener 退出，Drop 仅兜底取消。
pub struct AgentProxyHandle {
    environment: Vec<(OsString, OsString)>,
    cancellation: CancellationToken,
    failure: oneshot::Receiver<String>,
    listener_task: Option<JoinHandle<()>>,
}

/// CCRv2 relay 的无状态构造入口。
pub(super) struct AgentProxy;

struct AgentProxyPaths {
    system_ca: PathBuf,
    ca_bundle: PathBuf,
}

struct RelayConfig {
    websocket_url: Url,
    websocket_authorization: HeaderValue,
    proxy_authorization: String,
}

impl RelayConfig {
    /// 构造上游代理 WebSocket 握手请求（含认证与协议头）。
    fn build_websocket_request(&self) -> Result<http::Request<()>> {
        let mut request = self
            .websocket_url
            .as_str()
            .into_client_request()
            .context("failed to build upstream proxy WebSocket request")?;
        let headers = request.headers_mut();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/proto"));
        headers.insert(AUTHORIZATION, self.websocket_authorization.clone());
        Ok(request)
    }
}

struct ConnectRequest {
    target: String,
    trailing: Vec<u8>,
}

enum ConnectParse {
    Incomplete,
    Request(ConnectRequest),
    Reject(&'static [u8]),
}

impl AgentProxyPaths {
    /// 从固定系统路径与当前 HOME 构造生产环境文件位置。
    fn production() -> Result<Self> {
        let home = std::env::var_os("HOME")
            .filter(|home| !home.is_empty())
            .ok_or_else(|| anyhow!("HOME is not set"))?;
        Ok(Self {
            system_ca: PathBuf::from(SYSTEM_CA_BUNDLE_PATH),
            ca_bundle: PathBuf::from(home).join(".ccr/ca-bundle.crt"),
        })
    }
}

impl AgentProxy {
    /// 准备 CA 并启动只监听回环地址的本地 relay。
    pub(super) async fn start(config: AgentProxyConfig) -> Result<AgentProxyHandle, ClaudeError> {
        let paths = AgentProxyPaths::production().map_err(agent_proxy_start_error)?;
        Self::start_with_paths(config, paths)
            .await
            .map_err(agent_proxy_start_error)
    }

    /// 使用显式路径完成启动，避免把文件位置混入网络中继逻辑。
    async fn start_with_paths(
        config: AgentProxyConfig,
        paths: AgentProxyPaths,
    ) -> Result<AgentProxyHandle> {
        if config.token.is_empty() {
            bail!("session ingress token is empty");
        }

        set_non_dumpable();

        let base_url = Url::parse(&config.api_base_url).context("invalid API base URL")?;
        let host = base_url
            .host_str()
            .ok_or_else(|| anyhow!("API base URL has no host"))?;
        download_ca_bundle(&base_url, &paths.system_ca, &paths.ca_bundle).await?;

        let mut websocket_url = base_url
            .join(WEBSOCKET_PATH)
            .context("failed to build upstream proxy WebSocket URL")?;
        let websocket_scheme = match websocket_url.scheme() {
            "http" => "ws",
            "https" => "wss",
            scheme => bail!("unsupported API base URL scheme: {scheme}"),
        };
        websocket_url
            .set_scheme(websocket_scheme)
            .map_err(|()| anyhow!("failed to set upstream proxy WebSocket scheme"))?;

        let websocket_authorization = HeaderValue::from_str(&format!("Bearer {}", config.token))
            .context("invalid session ingress token for WebSocket authorization")?;
        let proxy_authorization = format!(
            "Basic {}",
            BASE64_STANDARD.encode(format!("{}:{}", config.session_id, config.token))
        );
        let relay_config = Arc::new(RelayConfig {
            websocket_url,
            websocket_authorization,
            proxy_authorization,
        });

        let listener = bind_local_listener(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let port = listener.local_addr()?.port();
        let environment = proxy_environment(port, &paths.ca_bundle, host)?;

        let cancellation = CancellationToken::new();
        let listener_cancellation = cancellation.clone();
        let (failure_sender, failure) = oneshot::channel();
        let listener_task = tokio::spawn(async move {
            if let Err(error) = run_listener(listener, relay_config, listener_cancellation).await {
                let _ = failure_sender.send(format!("CCR upstream proxy relay failed: {error:#}"));
            }
        });
        Ok(AgentProxyHandle {
            environment,
            cancellation,
            failure,
            listener_task: Some(listener_task),
        })
    }
}

impl AgentProxyHandle {
    /// 返回必须最后覆盖到 source 和 Claude 子进程的代理环境。
    pub(crate) fn environment(&self) -> &[(OsString, OsString)] {
        &self.environment
    }

    /// 仅在 listener 意外退出时完成；正常关闭发生在 Claude 已经结束之后。
    pub(crate) async fn wait_failure(&mut self) -> String {
        match (&mut self.failure).await {
            Ok(error) => error,
            Err(_) => "CCR upstream proxy relay stopped unexpectedly".to_owned(),
        }
    }

    /// 取消 listener 和连接任务，并等待主任务退出。
    pub(crate) async fn shutdown(mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.listener_task.take()
            && let Err(error) = task.await
        {
            eprintln!("[upstreamproxy] failed to stop relay: {error}");
        }
    }
}

impl Drop for AgentProxyHandle {
    /// 异常返回时取消并中止 relay，避免后台任务泄漏。
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.listener_task.take() {
            task.abort();
        }
    }
}

/// 把内部错误转换为 Claude 执行器的稳定启动错误。
fn agent_proxy_start_error(error: impl std::fmt::Display) -> ClaudeError {
    ClaudeError::message(format!("failed to start CCR upstream proxy: {error}"))
}

/// 尽力禁止同 UID 进程通过 ptrace 读取父进程中的会话 token。
fn set_non_dumpable() {
    if let Err(error) = set_dumpable(false) {
        eprintln!(
            "[upstreamproxy] prctl(PR_SET_DUMPABLE, 0) failed: {}",
            io::Error::from(error)
        );
    }
}

/// 下载 CCR CA，与系统 bundle 合并后写入 Claude HOME。
async fn download_ca_bundle(base_url: &Url, system_ca: &Path, output: &Path) -> Result<()> {
    download_ca_bundle_with_timeout(base_url, system_ca, output, CA_DOWNLOAD_TIMEOUT).await
}

/// 使用显式超时下载并合并 CA，供生产固定上限与故障测试共用。
async fn download_ca_bundle_with_timeout(
    base_url: &Url,
    system_ca: &Path,
    output: &Path,
    timeout: Duration,
) -> Result<()> {
    let endpoint = base_url.join(CA_CERT_PATH)?;
    let ccr_ca = reqwest::Client::builder()
        .timeout(timeout)
        .build()?
        .get(endpoint)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    if ccr_ca.iter().all(u8::is_ascii_whitespace) {
        bail!("upstream proxy CA certificate is empty");
    }

    let system_ca = tokio::fs::read(system_ca).await?;

    let directory = output
        .parent()
        .ok_or_else(|| anyhow!("upstream proxy CA bundle path has no parent"))?;
    tokio::fs::create_dir_all(directory).await?;

    let mut bundle = Vec::with_capacity(system_ca.len() + ccr_ca.len() + 1);
    bundle.extend_from_slice(&system_ca);
    bundle.push(b'\n');
    bundle.extend_from_slice(&ccr_ca);
    tokio::fs::write(output, bundle).await?;
    Ok(())
}

/// 绑定本地回环 listener，并为启动失败补充稳定上下文。
async fn bind_local_listener(address: SocketAddr) -> Result<TcpListener> {
    TcpListener::bind(address)
        .await
        .context("failed to bind local upstream proxy relay")
}

/// 构造 HTTPS proxy、NO_PROXY 与各运行时 CA 环境变量。
fn proxy_environment(
    port: u16,
    ca_bundle: &Path,
    api_host: &str,
) -> Result<Vec<(OsString, OsString)>> {
    let proxy_url = format!("http://127.0.0.1:{port}");
    let ca_bundle = ca_bundle
        .to_str()
        .ok_or_else(|| anyhow!("upstream proxy CA bundle path is not UTF-8"))?;
    let no_proxy = format!("{NO_PROXY_LIST},{api_host}");
    Ok([
        ("HTTPS_PROXY", proxy_url.as_str()),
        ("https_proxy", proxy_url.as_str()),
        ("NO_PROXY", no_proxy.as_str()),
        ("no_proxy", no_proxy.as_str()),
        ("SSL_CERT_FILE", ca_bundle),
        ("GIT_SSL_CAINFO", ca_bundle),
        ("NODE_EXTRA_CA_CERTS", ca_bundle),
        ("REQUESTS_CA_BUNDLE", ca_bundle),
        ("CURL_CA_BUNDLE", ca_bundle),
    ]
    .into_iter()
    .map(|(key, value)| (OsString::from(key), OsString::from(value)))
    .collect())
}

/// 接受本地连接并管理所有连接任务的取消生命周期。
async fn run_listener(
    listener: TcpListener,
    config: Arc<RelayConfig>,
    cancellation: CancellationToken,
) -> Result<()> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                connections.shutdown().await;
                return Ok(());
            }
            _ = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => {
                let (client_stream, _) = accepted.context("local relay accept failed")?;
                let config = config.clone();
                let cancellation = cancellation.clone();
                connections.spawn(async move {
                    let _ = handle_connection(client_stream, &config, &cancellation).await;
                });
            }
        }
    }
}

/// 把一条本地 CONNECT 流量双向转发到 CCR WebSocket。
async fn handle_connection(
    client_stream: TcpStream,
    config: &RelayConfig,
    cancellation: &CancellationToken,
) -> Result<()> {
    handle_connection_with_handshake_timeout(
        client_stream,
        config,
        cancellation,
        WEBSOCKET_HANDSHAKE_TIMEOUT,
    )
    .await
}

/// 使用可注入握手超时处理一条 CONNECT 连接，供生产默认值和边界测试共用。
async fn handle_connection_with_handshake_timeout(
    mut client_stream: TcpStream,
    config: &RelayConfig,
    cancellation: &CancellationToken,
    handshake_timeout: Duration,
) -> Result<()> {
    // CONNECT example.com:443 HTTP/1.1
    // Host: example.com:443
    // Proxy-Authorization: xx
    // 核心流程： 只解析 CONNECT 行，不解析原始 CONNECT 的 Host、User-Agent 等 header。除了请求行，其余原始 header 都被忽略。
    // WebSocket 建立后，它会重新构造一个 CONNECT 请求，只保留原始 CONNECT 行并注入 Proxy-Authorization
    let Some(request) = read_connect_request(&mut client_stream, cancellation).await? else {
        return Ok(());
    };
    // websocket_url: wss://api.example.com/v1/code/upstreamproxy/ws
    let websocket_request = config.build_websocket_request()?;
    let websocket_config = WebSocketConfig::default()
        .max_message_size(Some(MAX_ENCODED_CHUNK_BYTES))
        .max_frame_size(Some(MAX_ENCODED_CHUNK_BYTES));
    let handshake = tokio::select! {
        () = cancellation.cancelled() => return Ok(()),
        result = tokio::time::timeout(
            handshake_timeout,
            connect_websocket_with_config(websocket_request, websocket_config),
        ) => result,
    };
    let (websocket, _) = match handshake {
        Ok(Ok(connection)) => connection,
        Ok(Err(error)) => {
            write_bad_gateway(&mut client_stream).await;
            return Err(anyhow!(
                "upstream proxy WebSocket handshake failed: {error}"
            ));
        }
        Err(_) => {
            write_bad_gateway(&mut client_stream).await;
            return Err(anyhow!("upstream proxy WebSocket handshake timed out"));
        }
    };
    let (mut websocket_writer, websocket_reader) = websocket.split();

    // 送给服务端的首个 WebSocket chunk 是：
    // CONNECT api.example.com:443 HTTP/1.1
    // Proxy-Authorization: Basic ...
    let connect_head = format!(
        "CONNECT {} HTTP/1.1\r\nProxy-Authorization: {}\r\n\r\n",
        request.target, config.proxy_authorization
    );
    if let Err(error) = send_chunked(&mut websocket_writer, connect_head.as_bytes()).await {
        write_bad_gateway(&mut client_stream).await;
        return Err(error);
    }
    if let Err(error) = send_chunked(&mut websocket_writer, &request.trailing).await {
        write_bad_gateway(&mut client_stream).await;
        return Err(error);
    }

    let (client_reader, client_writer) = client_stream.into_split();
    let mut directions = JoinSet::new();
    directions.spawn(relay_tcp_to_websocket(
        client_reader,
        websocket_writer,
        cancellation.clone(),
    ));
    directions.spawn(relay_websocket_to_tcp(websocket_reader, client_writer));

    // 任一方向结束后，通过 JoinSet::shutdown() 中止并回收另一方向
    let result = directions
        .join_next()
        .await
        .ok_or_else(|| anyhow!("agent proxy relay has no direction tasks"))?;
    directions.shutdown().await;
    result.context("agent proxy relay task failed")?
}

/// 独立推进本地 TCP 到上游 WebSocket 的传输、keepalive 和连接关闭。
async fn relay_tcp_to_websocket(
    mut client_reader: OwnedReadHalf,
    mut websocket_writer: LocalWebSocketSink,
    cancellation: CancellationToken,
) -> Result<()> {
    let mut tcp_buffer = vec![0_u8; TCP_BUFFER_BYTES];
    let mut keepalive = interval_at(Instant::now() + KEEPALIVE_INTERVAL, KEEPALIVE_INTERVAL);
    loop {
        tokio::select! {
            () = cancellation.cancelled() => {
                let _ = websocket_writer.close().await;
                return Ok(());
            }
            read = client_reader.read(&mut tcp_buffer) => {
                let read = read.context("failed to read local CONNECT socket")?;
                if read == 0 {
                    let _ = websocket_writer.close().await;
                    return Ok(());
                }
                send_chunked(&mut websocket_writer, &tcp_buffer[..read]).await?;
            }
            _ = keepalive.tick() => {
                websocket_writer
                    .send(Message::Binary(encode_chunk(&[])?.into()))
                    .await
                    .context("failed to send upstream proxy keepalive")?;
            }
        }
    }
}

/// 独立推进上游 WebSocket 到本地 TCP 的传输。
async fn relay_websocket_to_tcp(
    mut websocket_reader: LocalWebSocketStream,
    mut client_writer: OwnedWriteHalf,
) -> Result<()> {
    let mut established = false;
    loop {
        match websocket_reader.next().await {
            Some(Ok(Message::Binary(message))) => {
                let payload = decode_chunk(&message)?;
                if !payload.is_empty() {
                    established = true;
                    client_writer
                        .write_all(&payload)
                        .await
                        .context("failed to write local CONNECT socket")?;
                }
            }
            // Tungstenite 会自动排队 Pong，并在下一次 read 时将其 flush。
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
            Some(Ok(Message::Close(_))) | None => return Ok(()),
            Some(Ok(Message::Text(_) | Message::Frame(_))) => {
                if !established {
                    write_bad_gateway(&mut client_writer).await;
                }
                bail!("upstream proxy WebSocket sent a non-binary data frame");
            }
            Some(Err(error)) => {
                if !established {
                    write_bad_gateway(&mut client_writer).await;
                }
                return Err(error).context("failed to read upstream proxy WebSocket");
            }
        }
    }
}

/// 分片读取并校验最大 8 KiB 的本地 CONNECT 请求头。
async fn read_connect_request(
    client_stream: &mut TcpStream,
    cancellation: &CancellationToken,
) -> Result<Option<ConnectRequest>> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        match parse_connect_request(&buffer) {
            ConnectParse::Incomplete => {}
            ConnectParse::Request(request) => return Ok(Some(request)),
            ConnectParse::Reject(response) => {
                client_stream.write_all(response).await.ok();
                client_stream.shutdown().await.ok();
                return Ok(None);
            }
        }
        let read = tokio::select! {
            () = cancellation.cancelled() => return Ok(None),
            read = client_stream.read(&mut chunk) => read.context("failed to read CONNECT request")?,
        };
        if read == 0 {
            return Ok(None);
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

/// 解析已累计的 CONNECT header，并保留 header 后的尾随字节。
fn parse_connect_request(buffer: &[u8]) -> ConnectParse {
    const BAD_REQUEST: &[u8] = b"HTTP/1.1 400 Bad Request\r\n\r\n";
    const METHOD_NOT_ALLOWED: &[u8] = b"HTTP/1.1 405 Method Not Allowed\r\n\r\n";

    let mut headers = [EMPTY_HEADER; MAX_CONNECT_HEADERS];
    let mut request = Request::new(&mut headers);
    let header_end = match request.parse(buffer) {
        Ok(Status::Partial) if buffer.len() <= MAX_CONNECT_HEADER_BYTES => {
            return ConnectParse::Incomplete;
        }
        Ok(Status::Complete(header_end)) if header_end <= MAX_CONNECT_HEADER_BYTES => header_end,
        Ok(_) | Err(_) => return ConnectParse::Reject(BAD_REQUEST),
    };

    let valid = request
        .method
        .is_some_and(|method| method.eq_ignore_ascii_case("CONNECT"))
        && request.path.is_some_and(|target| !target.is_empty())
        && matches!(request.version, Some(0) | Some(1));
    if !valid {
        return ConnectParse::Reject(METHOD_NOT_ALLOWED);
    }

    ConnectParse::Request(ConnectRequest {
        target: request.path.unwrap().to_owned(),
        trailing: buffer[header_end..].to_vec(),
    })
}

/// 将任意字节按协议上限切片后写入 WebSocket。
async fn send_chunked(writer: &mut LocalWebSocketSink, data: &[u8]) -> Result<()> {
    for chunk in data.chunks(MAX_CHUNK_BYTES) {
        writer
            .send(Message::Binary(encode_chunk(chunk)?.into()))
            .await
            .context("failed to write upstream proxy WebSocket")?;
    }
    Ok(())
}

/// 用 prost 编码只有 bytes 字段的 UpstreamProxyChunk protobuf 消息。
fn encode_chunk(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() > MAX_CHUNK_BYTES {
        bail!("upstream proxy chunk exceeds {MAX_CHUNK_BYTES} bytes");
    }
    let msg = UpstreamProxyChunk {
        data: Some(data.to_vec()),
    };
    Ok(msg.encode_to_vec())
}

/// 用 prost 解码 UpstreamProxyChunk 的 bytes 字段并拒绝畸形或超限长度。
fn decode_chunk(encoded: &[u8]) -> Result<Vec<u8>> {
    if encoded.is_empty() {
        return Ok(Vec::new());
    }
    let chunk =
        UpstreamProxyChunk::decode(encoded).context("upstream proxy chunk has invalid protobuf")?;
    let data = chunk
        .data
        .ok_or_else(|| anyhow!("upstream proxy chunk is missing data"))?;
    if data.len() > MAX_CHUNK_BYTES {
        bail!("upstream proxy chunk exceeds {MAX_CHUNK_BYTES} bytes");
    }
    Ok(data)
}

/// 在隧道建立前向本地 CONNECT 客户端返回固定 502。
async fn write_bad_gateway(client_writer: &mut (impl AsyncWrite + Unpin)) {
    let _ = client_writer
        .write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
        .await;
    let _ = client_writer.shutdown().await;
}

/// 让没有 relay 的执行路径提供一个永不完成的健康信号。
pub(crate) async fn wait_for_agent_proxy_failure(proxy: Option<&mut AgentProxyHandle>) -> String {
    match proxy {
        Some(proxy) => proxy.wait_failure().await,
        None => pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Mutex};

    use futures_util::{SinkExt as _, StreamExt as _};
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpListener,
    };
    use tokio_tungstenite::{accept_hdr_async, tungstenite::protocol::Message};

    use super::*;

    #[test]
    fn protobuf_chunk_round_trips_boundaries_and_rejects_malformed_input() {
        for size in [0, 127, 128, 16_383, 16_384, MAX_CHUNK_BYTES] {
            let data = vec![0x5a; size];
            let encoded = encode_chunk(&data).unwrap();
            assert_eq!(decode_chunk(&encoded).unwrap(), data);
        }
        assert!(encode_chunk(&vec![0; MAX_CHUNK_BYTES + 1]).is_err());
        for malformed in [&[0x08, 0x00][..], &[0x0a, 0x80], &[0x0a, 0x02, 0x01]] {
            assert!(decode_chunk(malformed).is_err(), "malformed={malformed:?}");
        }
    }

    #[test]
    fn connect_parser_handles_fragments_trailing_data_and_rejections() {
        assert!(matches!(
            parse_connect_request(b"CONNECT example.com:443 HTTP/1.1\r\n"),
            ConnectParse::Incomplete
        ));
        match parse_connect_request(
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\nhello",
        ) {
            ConnectParse::Request(request) => {
                assert_eq!(request.target, "example.com:443");
                assert_eq!(request.trailing, b"hello");
            }
            _ => panic!("CONNECT request should parse"),
        }
        assert!(matches!(
            parse_connect_request(b"GET https://example.com HTTP/1.1\r\n\r\n"),
            ConnectParse::Reject(response) if response.starts_with(b"HTTP/1.1 405")
        ));
        assert!(matches!(
            parse_connect_request(b"CONNECT example.com:443 HTTP/1.1\r\ninvalid-header\r\n\r\n"),
            ConnectParse::Reject(response) if response.starts_with(b"HTTP/1.1 400")
        ));
        let oversized = vec![b'a'; MAX_CONNECT_HEADER_BYTES + 1];
        assert!(matches!(
            parse_connect_request(&oversized),
            ConnectParse::Reject(response) if response.starts_with(b"HTTP/1.1 400")
        ));
    }

    #[test]
    fn proxy_environment_preserves_legacy_bypasses_and_dynamic_api_host() {
        let generated =
            proxy_environment(4321, Path::new("/tmp/ccr-ca.pem"), "control.example.test").unwrap();
        let environment = generated
            .iter()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.to_string_lossy().into_owned(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(environment["HTTPS_PROXY"], "http://127.0.0.1:4321");
        assert_eq!(environment["https_proxy"], environment["HTTPS_PROXY"]);
        assert!(environment["NO_PROXY"].contains("*.github.com"));
        assert!(environment["NO_PROXY"].ends_with(",control.example.test"));
        assert!(!environment.contains_key("HTTP_PROXY"));
        for key in [
            "GIT_SSL_CAINFO",
            "SSL_CERT_FILE",
            "NODE_EXTRA_CA_CERTS",
            "REQUESTS_CA_BUNDLE",
            "CURL_CA_BUNDLE",
        ] {
            assert_eq!(environment[key], "/tmp/ccr-ca.pem");
        }

        let mut child_environment = vec![
            (OsString::from("HTTPS_PROXY"), OsString::from("http://old")),
            (OsString::from("NO_PROXY"), OsString::from("old.example")),
        ];
        child_environment.extend(generated);
        let final_value = |key: &str| {
            child_environment
                .iter()
                .rev()
                .find(|(candidate, _)| candidate == key)
                .map(|(_, value)| value.to_string_lossy().into_owned())
        };
        assert_eq!(
            final_value("HTTPS_PROXY").as_deref(),
            Some("http://127.0.0.1:4321")
        );
        assert!(
            final_value("NO_PROXY")
                .as_deref()
                .is_some_and(|value| value.ends_with(",control.example.test"))
        );
    }

    #[tokio::test]
    async fn ca_bundle_combines_system_and_downloaded_certificates() {
        let temp = TempDir::new().unwrap();
        let system_ca = temp.path().join("system.pem");
        let output = temp.path().join("home/.ccr/ca-bundle.crt");
        tokio::fs::write(&system_ca, b"SYSTEM-CA").await.unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let read = socket.read(&mut request).await.unwrap();
            assert!(String::from_utf8_lossy(&request[..read]).contains(CA_CERT_PATH));
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nCCR-CA")
                .await
                .unwrap();
        });
        let base_url = Url::parse(&format!("http://{address}")).unwrap();
        download_ca_bundle(&base_url, &system_ca, &output)
            .await
            .unwrap();
        server.await.unwrap();
        assert_eq!(tokio::fs::read(output).await.unwrap(), b"SYSTEM-CA\nCCR-CA");
    }

    #[tokio::test]
    async fn ca_bundle_rejects_http_errors_empty_ca_and_write_failures() {
        async fn endpoint(response: &'static [u8]) -> (Url, JoinHandle<()>) {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 2048];
                let _ = socket.read(&mut request).await.unwrap();
                socket.write_all(response).await.unwrap();
            });
            (Url::parse(&format!("http://{address}")).unwrap(), server)
        }

        let temp = TempDir::new().unwrap();
        let system_ca = temp.path().join("system.pem");
        tokio::fs::write(&system_ca, b"SYSTEM-CA").await.unwrap();
        let output = temp.path().join("ca-bundle.crt");

        let (url, server) =
            endpoint(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n").await;
        assert!(download_ca_bundle(&url, &system_ca, &output).await.is_err());
        server.await.unwrap();

        let (url, server) = endpoint(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n   ").await;
        assert!(download_ca_bundle(&url, &system_ca, &output).await.is_err());
        server.await.unwrap();

        let blocked_parent = temp.path().join("not-a-directory");
        tokio::fs::write(&blocked_parent, b"file").await.unwrap();
        let (url, server) = endpoint(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nCCR-CA").await;
        assert!(
            download_ca_bundle(&url, &system_ca, &blocked_parent.join("ca.pem"))
                .await
                .is_err()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ca_bundle_download_obeys_timeout() {
        let temp = TempDir::new().unwrap();
        let system_ca = temp.path().join("system.pem");
        let output = temp.path().join("ca-bundle.crt");
        tokio::fs::write(&system_ca, b"SYSTEM-CA").await.unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        let url = Url::parse(&format!("http://{address}")).unwrap();
        assert!(
            download_ca_bundle_with_timeout(&url, &system_ca, &output, Duration::from_millis(20),)
                .await
                .is_err()
        );
        assert!(!output.exists());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn local_listener_bind_failure_is_reported() {
        let occupied = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = occupied.local_addr().unwrap();
        assert!(bind_local_listener(address).await.is_err());
    }

    #[tokio::test]
    #[allow(clippy::result_large_err)]
    async fn relay_forwards_connect_data_and_sends_keepalive() {
        let websocket_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let websocket_address = websocket_listener.local_addr().unwrap();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_headers = Arc::clone(&observed);
        let websocket_server = tokio::spawn(async move {
            let (socket, _) = websocket_listener.accept().await.unwrap();
            let mut websocket =
                accept_hdr_async(socket, move |request: &http::Request<()>, response| {
                    observed_headers.lock().unwrap().extend([
                        request.headers()[CONTENT_TYPE].to_str().unwrap().to_owned(),
                        request.headers()[AUTHORIZATION]
                            .to_str()
                            .unwrap()
                            .to_owned(),
                    ]);
                    Ok(response)
                })
                .await
                .unwrap();
            let first = websocket.next().await.unwrap().unwrap().into_data();
            let first = decode_chunk(&first).unwrap();
            assert_eq!(
                first,
                b"CONNECT example.com:443 HTTP/1.1\r\nProxy-Authorization: Basic c2Vzc2lvbi0xMjM6dG9rZW4tNDU2\r\n\r\n"
            );
            let second = websocket.next().await.unwrap().unwrap().into_data();
            assert_eq!(decode_chunk(&second).unwrap(), b"client-data");
            websocket
                .send(Message::Binary(
                    encode_chunk(b"HTTP/1.1 200 Connection Established\r\n\r\nserver-data")
                        .unwrap()
                        .into(),
                ))
                .await
                .unwrap();
            websocket
                .send(Message::Ping(b"ping".as_slice().into()))
                .await
                .unwrap();
            let mut saw_pong = false;
            let mut saw_keepalive = false;
            while !saw_pong || !saw_keepalive {
                let message = tokio::time::timeout(Duration::from_secs(1), websocket.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                match message {
                    Message::Pong(payload) => {
                        assert_eq!(payload, b"ping".as_slice());
                        saw_pong = true;
                    }
                    Message::Binary(payload) => {
                        assert!(decode_chunk(&payload).unwrap().is_empty());
                        saw_keepalive = true;
                    }
                    message => panic!("unexpected relay message: {message:?}"),
                }
            }
        });

        let relay_config = Arc::new(RelayConfig {
            websocket_url: Url::parse(&format!("ws://{websocket_address}{WEBSOCKET_PATH}"))
                .unwrap(),
            websocket_authorization: HeaderValue::from_static("Bearer token-456"),
            proxy_authorization: "Basic c2Vzc2lvbi0xMjM6dG9rZW4tNDU2".to_owned(),
        });
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let relay_address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let relay_task = tokio::spawn(run_listener(listener, relay_config, cancellation.clone()));
        let mut client = TcpStream::connect(relay_address).await.unwrap();
        client
            .write_all(b"CONNECT example.com:443 HTTP/1.0\r\nHost: example.com\r\n\r\nclient-data")
            .await
            .unwrap();
        let expected = b"HTTP/1.1 200 Connection Established\r\n\r\nserver-data";
        let mut response = vec![0_u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);
        websocket_server.await.unwrap();
        assert_eq!(
            *observed.lock().unwrap(),
            ["application/proto", "Bearer token-456"]
        );
        cancellation.cancel();
        relay_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn websocket_handshake_failure_returns_502_without_direct_fallback() {
        let unused_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let unused_address = unused_listener.local_addr().unwrap();
        drop(unused_listener);
        let relay_config = Arc::new(RelayConfig {
            websocket_url: Url::parse(&format!("ws://{unused_address}{WEBSOCKET_PATH}")).unwrap(),
            websocket_authorization: HeaderValue::from_static("Bearer token-456"),
            proxy_authorization: "Basic unused".to_owned(),
        });
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let relay_address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let relay_task = tokio::spawn(run_listener(listener, relay_config, cancellation.clone()));

        let mut client = TcpStream::connect(relay_address).await.unwrap();
        client
            .write_all(b"CONNECT example.com:443 HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"HTTP/1.1 502 Bad Gateway\r\n\r\n");

        cancellation.cancel();
        relay_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn websocket_handshake_timeout_returns_502() {
        let websocket_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let websocket_address = websocket_listener.local_addr().unwrap();
        let websocket_server = tokio::spawn(async move {
            let (_socket, _) = websocket_listener.accept().await.unwrap();
            pending::<()>().await;
        });
        let relay_config = RelayConfig {
            websocket_url: Url::parse(&format!("ws://{websocket_address}{WEBSOCKET_PATH}"))
                .unwrap(),
            websocket_authorization: HeaderValue::from_static("Bearer token-456"),
            proxy_authorization: "Basic unused".to_owned(),
        };
        let relay_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let relay_address = relay_listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let relay_task = tokio::spawn(async move {
            let (client_stream, _) = relay_listener.accept().await.unwrap();
            handle_connection_with_handshake_timeout(
                client_stream,
                &relay_config,
                &cancellation,
                Duration::from_millis(20),
            )
            .await
        });

        let mut client = TcpStream::connect(relay_address).await.unwrap();
        client
            .write_all(b"CONNECT example.com:443 HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), client.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response, b"HTTP/1.1 502 Bad Gateway\r\n\r\n");
        assert!(
            relay_task
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("handshake timed out")
        );
        websocket_server.abort();
    }
}
