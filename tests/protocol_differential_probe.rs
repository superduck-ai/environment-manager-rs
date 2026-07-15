//! Linux ARM64 Go/Rust 协议差分的 Rust 侧 probe。
//!
//! 本测试只连接进程内 loopback mock，不访问 staging。门禁以 `--ignored --exact`
//! 显式运行它，并通过 `PROTOCOL_TRANSCRIPT_DIR` 指定临时输出目录；普通 `cargo test`
//! 不执行这些较重的真实 HTTP server 场景。

use std::{
    collections::BTreeMap,
    io::Write as _,
    os::fd::AsRawFd as _,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{
        State,
        ws::{Message as AxumWebSocketMessage, WebSocketUpgrade},
    },
    http::{HeaderMap, Request, StatusCode, header},
    response::Response,
    routing::any,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use environment_manager::internal::tunnel::actions::ActionError;
use environment_manager::{
    r#gen::proto::anthropic::sessions::tunnel::v1alpha::{
        Header, HttpTunnelCancel, HttpTunnelRequest, HttpTunnelResponseChunk,
        HttpTunnelResponseHeaders, TunnelRequest, TunnelResponse, WsTunnelClose, WsTunnelError,
        WsTunnelMessage, WsTunnelOpen, tunnel_request, tunnel_response,
    },
    internal::{
        api::{ClientOptions, HttpClient, RetryConfig, WorkClient},
        logger::default_slog_logger,
        mcp::{BaseServer, Tool, ToolConfig, ToolHandler, new_tool_result_text},
        tunnel::{
            HttpTunnelResponseError, ResponseSender, TunnelError, WsResponseSender, WsSenderError,
            WsTunnelOpened, marshal_protojson, new_handler, new_handler_with_timing_for_probe,
            new_ws_handler,
        },
    },
};
use flate2::{Compression, write::GzEncoder};
use serde::{Deserialize, Serialize};
use serde_json::{Map, json};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::{Notify, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_util::sync::CancellationToken;

/// 记录 Environment Manager 及 Go Transport 共同决定的稳定 API 请求头。
///
/// Date 和 Content-Length 仍由具体 HTTP 实现决定，不纳入契约；默认 User-Agent 与
/// 自动 Accept-Encoding 会被后端观察并改变响应解码，必须参加 Go/Rust 差分。
#[derive(Debug, Serialize)]
struct ProtocolHttpHeaders {
    authorization: String,
    accept_encoding: String,
    content_type: String,
    environment_runner_version: String,
    claude_code_version: String,
    test_header: String,
    user_agent: String,
}

/// Mock 后端实际收到的一次 API 请求。`body` 保留原始字节对应的 UTF-8 文本，字段
/// 顺序、omitempty 与结尾换行不会被 probe 自己重新编码。
#[derive(Debug, Serialize)]
struct ProtocolHttpRequest {
    method: String,
    target: String,
    headers: ProtocolHttpHeaders,
    body: String,
}

/// API 调用者可见的成功结果或错误字符串；两个字段始终同时出现。
#[derive(Debug, Serialize)]
struct ProtocolHttpOutcome {
    name: &'static str,
    result: String,
    error: String,
}

/// HTTP API probe 的稳定文档格式，与 Go overlay 中的结构逐字段对应。
#[derive(Debug, Serialize)]
struct ProtocolHttpTranscript {
    schema_version: u32,
    protocol: &'static str,
    requests: Vec<ProtocolHttpRequest>,
    outcomes: Vec<ProtocolHttpOutcome>,
}

/// API probe 的请求 payload。使用声明顺序固定的 struct，而不是无序 map，确保 Rust
/// 发送的 JSON 字节可以与 Go `encoding/json` 的排序结果直接比较。
#[derive(Debug, Serialize)]
struct ApiPayload {
    hello: &'static str,
    html: &'static str,
    sequence: u32,
}

/// API 成功响应的强类型结果，字段顺序与 Go probe 的匿名结构一致。
#[derive(Debug, Deserialize, Serialize)]
struct ApiSuccessResponse {
    ok: bool,
    echo: String,
}

/// Axum mock 后端共享的请求记录器。
#[derive(Clone)]
struct ApiCaptureState {
    requests: Arc<tokio::sync::Mutex<Vec<ProtocolHttpRequest>>>,
}

/// 单条 MCP 请求/响应 transcript。请求和响应 body 都是未经解析的 wire 字符串。
#[derive(Debug, Serialize)]
struct ProtocolMcpCase {
    name: &'static str,
    authorized: bool,
    request_body: &'static str,
    status: u16,
    response_content_type: String,
    response_body: String,
}

/// MCP probe 的稳定文档格式。随机端口只用于连接；endpoint 和固定调试 token 则是
/// 实际外部契约的一部分，因此明确写入 transcript。
#[derive(Debug, Serialize)]
struct ProtocolMcpTranscript {
    schema_version: u32,
    protocol: &'static str,
    endpoint: &'static str,
    bearer_token: String,
    cases: Vec<ProtocolMcpCase>,
}

/// 一条 Tunnel ProtoJSON 原始 wire 字符串。
#[derive(Debug, Serialize)]
struct ProtocolProtoJsonCase {
    name: &'static str,
    wire: String,
}

/// Tunnel ProtoJSON probe 的稳定文档格式。
#[derive(Debug, Serialize)]
struct ProtocolProtoJsonTranscript {
    schema_version: u32,
    protocol: &'static str,
    cases: Vec<ProtocolProtoJsonCase>,
}

/// Mock 实际收到的稳定 HTTP Tunnel 请求字段。
///
/// 随机端口不写入 transcript；`host_matches_loopback_target` 直接验证 Host 是否等于
/// 本次真实 `localhost:<port>` authority，因此既覆盖 Host 又不需要 normalizer。
#[derive(Clone, Debug, Default, Serialize)]
struct ProtocolTunnelHttpRequest {
    observed: bool,
    method: String,
    target: String,
    host_matches_loopback_target: bool,
    user_agent: String,
    input_header: String,
    body: String,
}

/// Handler 经 ResponseSender 发出的单个 HTTP Tunnel 事件。
#[derive(Clone, Debug, Default, Serialize)]
struct ProtocolTunnelHttpEvent {
    kind: String,
    request_id: String,
    status_code: i32,
    status_text: String,
    reply_header: String,
    sequence_number: u32,
    data_base64: String,
    is_final: bool,
    error_message: String,
    error_class: String,
}

/// 一次真实 HTTP Tunnel 转发的输入、事件和函数错误。
#[derive(Debug, Serialize)]
struct ProtocolTunnelHttpCase {
    name: &'static str,
    request: ProtocolTunnelHttpRequest,
    events: Vec<ProtocolTunnelHttpEvent>,
    handler_error: String,
}

/// HTTP Tunnel runtime 的稳定差分文档。
#[derive(Debug, Serialize)]
struct ProtocolTunnelHttpTranscript {
    schema_version: u32,
    protocol: &'static str,
    cases: Vec<ProtocolTunnelHttpCase>,
}

/// 本地 WebSocket 服务实际收到的稳定握手字段。
#[derive(Clone, Debug, Default, Serialize)]
struct ProtocolTunnelHandshake {
    observed: bool,
    host: String,
    tunnel_header: String,
    offered_protocols: String,
}

/// Mock WebSocket 服务收到的一个客户端 frame。
#[derive(Clone, Debug, Serialize)]
struct ProtocolTunnelWsWireMessage {
    kind: String,
    data_base64: String,
}

/// Mock WebSocket 服务收到的客户端 close frame。
#[derive(Clone, Debug, Default, Serialize)]
struct ProtocolTunnelWsClose {
    observed: bool,
    code: u16,
    reason: String,
}

/// WSHandler 经 WSResponseSender 发出的一个事件。
#[derive(Clone, Debug, Default, Serialize)]
struct ProtocolTunnelWsEvent {
    kind: String,
    tunnel_id: String,
    protocol: String,
    data_base64: String,
    is_binary: bool,
    code: i32,
    reason: String,
    error_message: String,
}

/// 一次本地 WebSocket Tunnel 生命周期的稳定记录。
#[derive(Debug, Serialize)]
struct ProtocolTunnelWsCase {
    name: &'static str,
    handshake: ProtocolTunnelHandshake,
    client_messages: Vec<ProtocolTunnelWsWireMessage>,
    client_close: ProtocolTunnelWsClose,
    events: Vec<ProtocolTunnelWsEvent>,
    handler_error: String,
}

/// 本地 WebSocket Tunnel runtime 的稳定差分文档。
#[derive(Debug, Serialize)]
struct ProtocolTunnelWsTranscript {
    schema_version: u32,
    protocol: &'static str,
    cases: Vec<ProtocolTunnelWsCase>,
}

/// 记录 HTTP Handler 的协议事件，可选通知 raw server 当前 flush 已被消费。
#[derive(Default)]
struct ProtocolTunnelHttpSender {
    events: Mutex<Vec<ProtocolTunnelHttpEvent>>,
    chunk_ack: Option<mpsc::UnboundedSender<()>>,
}

/// 记录 WSHandler 上行事件，并通过 Notify 等待确定事件而不依赖任意 sleep。
#[derive(Default)]
struct ProtocolTunnelWsSender {
    events: Mutex<Vec<ProtocolTunnelWsEvent>>,
    wake: Notify,
}

#[async_trait]
impl ResponseSender for ProtocolTunnelHttpSender {
    /// 记录响应状态、sequence zero 与受控 `X-Reply`，过滤 Date 等非客户端契约字段。
    async fn send_headers(&self, response: HttpTunnelResponseHeaders) -> Result<(), ActionError> {
        self.events.lock().unwrap().push(ProtocolTunnelHttpEvent {
            kind: "headers".to_owned(),
            request_id: response.request_id,
            status_code: response.status_code,
            status_text: response.status_text,
            reply_header: tunnel_header_value(&response.headers, "X-Reply"),
            sequence_number: response.sequence_number,
            ..Default::default()
        });
        Ok(())
    }

    /// 无损记录数据和 final 标志；数据块到达后通知分段 mock 可以执行下一次 flush。
    async fn send_chunk(&self, chunk: HttpTunnelResponseChunk) -> Result<(), ActionError> {
        let is_final = chunk.is_final;
        self.events.lock().unwrap().push(ProtocolTunnelHttpEvent {
            kind: "chunk".to_owned(),
            request_id: chunk.request_id,
            sequence_number: chunk.sequence_number,
            data_base64: STANDARD.encode(chunk.data),
            is_final,
            ..Default::default()
        });
        if !is_final && let Some(chunk_ack) = &self.chunk_ack {
            let _ = chunk_ack.send(());
        }
        Ok(())
    }

    /// 记录 Handler 对后端可见的完整协议错误文本。
    async fn send_error(&self, response: HttpTunnelResponseError) -> Result<(), ActionError> {
        self.events.lock().unwrap().push(ProtocolTunnelHttpEvent {
            kind: "error".to_owned(),
            request_id: response.request_id,
            error_message: response.error_message,
            ..Default::default()
        });
        Ok(())
    }
}

impl ProtocolTunnelHttpSender {
    /// 返回与异步网络任务解耦的事件副本。
    fn snapshot(&self) -> Vec<ProtocolTunnelHttpEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait]
impl WsResponseSender for ProtocolTunnelWsSender {
    /// 记录握手成功与所选子协议。
    async fn send_opened(&self, response: WsTunnelOpened) -> Result<(), WsSenderError> {
        self.events.lock().unwrap().push(ProtocolTunnelWsEvent {
            kind: "opened".to_owned(),
            tunnel_id: response.tunnel_id,
            protocol: response.protocol,
            ..Default::default()
        });
        self.wake.notify_waiters();
        Ok(())
    }

    /// 记录本地服务端发来的 text/binary frame。
    async fn send_message(&self, message: WsTunnelMessage) -> Result<(), WsSenderError> {
        self.events.lock().unwrap().push(ProtocolTunnelWsEvent {
            kind: "message".to_owned(),
            tunnel_id: message.tunnel_id,
            data_base64: STANDARD.encode(message.data),
            is_binary: message.is_binary,
            ..Default::default()
        });
        self.wake.notify_waiters();
        Ok(())
    }

    /// 记录本地服务端 close frame。
    async fn send_close(&self, message: WsTunnelClose) -> Result<(), WsSenderError> {
        self.events.lock().unwrap().push(ProtocolTunnelWsEvent {
            kind: "close".to_owned(),
            tunnel_id: message.tunnel_id,
            code: message.code,
            reason: message.reason,
            ..Default::default()
        });
        self.wake.notify_waiters();
        Ok(())
    }

    /// 记录握手或读循环的原始错误文本。
    async fn send_error(&self, response: WsTunnelError) -> Result<(), WsSenderError> {
        self.events.lock().unwrap().push(ProtocolTunnelWsEvent {
            kind: "error".to_owned(),
            tunnel_id: response.tunnel_id,
            error_message: response.error_message,
            ..Default::default()
        });
        self.wake.notify_waiters();
        Ok(())
    }
}

impl ProtocolTunnelWsSender {
    /// 返回当前事件副本。
    fn snapshot(&self) -> Vec<ProtocolTunnelWsEvent> {
        self.events.lock().unwrap().clone()
    }

    /// 在三秒内等待事件集合满足 predicate；注册通知早于检查以避免丢失唤醒。
    async fn wait_for(&self, predicate: impl Fn(&[ProtocolTunnelWsEvent]) -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let notified = self.wake.notified();
                if predicate(&self.snapshot()) {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("等待 WebSocket runtime 事件超时");
    }
}

/// 在 Linux ARM64 上生成 Rust 的 HTTP、MCP 与 Tunnel transcript。
///
/// 该测试故意标记 ignored：只有 `run_differential.sh` 完成 Go 基线校验并注入输出
/// 目录后才可运行，避免普通单测把本 probe 的结果误当成独立兼容证明。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "由 Linux ARM64 协议差分门禁显式运行"]
async fn protocol_differential_transcript() {
    let output_dir = PathBuf::from(
        std::env::var_os("PROTOCOL_TRANSCRIPT_DIR").expect("PROTOCOL_TRANSCRIPT_DIR is required"),
    );
    std::fs::create_dir_all(&output_dir).expect("create Rust transcript directory");

    let http_transcript = capture_http_api_transcript().await;
    write_transcript(&output_dir.join("http_api.json"), &http_transcript);

    // 定向排查 HTTP API 时允许在第一份 transcript 后结束；统一门禁从不设置该变量，
    // 因而仍会强制生成并比较全部五份协议文件，不能借此绕过 Tunnel/MCP 验收。
    if std::env::var_os("PROTOCOL_HTTP_API_ONLY").is_some() {
        return;
    }

    let mcp_transcript = capture_mcp_transcript().await;
    write_transcript(&output_dir.join("mcp.json"), &mcp_transcript);

    let protojson_transcript = capture_tunnel_protojson_transcript();
    write_transcript(
        &output_dir.join("tunnel_protojson.json"),
        &protojson_transcript,
    );

    let tunnel_http_transcript = capture_tunnel_http_runtime_transcript().await;
    write_transcript(
        &output_dir.join("tunnel_http_runtime.json"),
        &tunnel_http_transcript,
    );

    let tunnel_ws_transcript = capture_tunnel_ws_runtime_transcript().await;
    write_transcript(
        &output_dir.join("tunnel_ws_runtime.json"),
        &tunnel_ws_transcript,
    );
}

/// 启动进程内 HTTP mock，通过真实 `HttpClient` 覆盖 HTML JSON、默认传输 header、
/// gzip 与通用响应分支，并通过真实 `WorkClient` 固定 `.`/`..` 动态 ID 的原始 ACK
/// request-target。
async fn capture_http_api_transcript() -> ProtocolHttpTranscript {
    let requests = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(4)));
    let state = ApiCaptureState {
        requests: Arc::clone(&requests),
    };
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind API probe listener");
    let port = listener
        .local_addr()
        .expect("read API probe address")
        .port();
    let cancellation = CancellationToken::new();
    let server_cancellation = cancellation.clone();
    let server_task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .fallback(any(capture_api_request))
                .with_state(state),
        )
        .with_graceful_shutdown(server_cancellation.cancelled_owned())
        .await
        .expect("serve API probe");
    });

    let base_url = format!("http://127.0.0.1:{port}");
    let client = HttpClient::with_options(
        &base_url,
        None,
        ClientOptions {
            timeout: Some(Duration::from_secs(2)),
            retry_config: Some(RetryConfig {
                max_retries: 0,
                initial_backoff: Duration::from_nanos(1),
                max_backoff: Duration::from_nanos(1),
                backoff_factor: 1.0,
            }),
            default_headers: BTreeMap::from([(
                "x-test-header".to_owned(),
                "stable-value".to_owned(),
            )]),
        },
    )
    .expect("construct API probe client");
    let payload = ApiPayload {
        hello: "world",
        html: "<\u{2028}&>",
        sequence: 7,
    };
    let request_cancellation = CancellationToken::new();

    let success = client
        .post_json_with_response::<_, ApiSuccessResponse>(
            &request_cancellation,
            &format!("{base_url}/ok?mode=success"),
            &payload,
            "fixed-api-token",
            "protocol success",
        )
        .await;
    let success_outcome = match success {
        Ok(response) => ProtocolHttpOutcome {
            name: "success",
            result: serde_json::to_string(&response).expect("encode API success response"),
            error: String::new(),
        },
        Err(error) => ProtocolHttpOutcome {
            name: "success",
            result: String::new(),
            error: error.to_string(),
        },
    };

    let not_implemented = client
        .post_json(
            &request_cancellation,
            &format!("{base_url}/not-implemented"),
            &payload,
            "fixed-api-token",
            "protocol missing",
        )
        .await;
    let failure = client
        .post_json(
            &request_cancellation,
            &format!("{base_url}/failure"),
            &payload,
            "fixed-api-token",
            "protocol failure",
        )
        .await;

    // 不手工拼接探针 URL：必须调用生产 WorkClient，才能同时覆盖共享 PathEscape、
    // URL parser 和 Reqwest/Hyper 写入 request-target 的完整链路。Go 会保留两个 dot
    // segment，所以服务端看到的原始目标必须是 `/v1/environments/./work/../ack`。
    let work_ack = WorkClient::new(client.clone(), "fixed-work-token", None)
        .acknowledge_work(&request_cancellation, ".", "..")
        .await;

    // Go 在最后一次（这里也是唯一一次）attempt 被取消时仍套 exhausted 错误；只有
    // 尚有下一次重试并进入 backoff 才返回 retry-cancelled。使用固定 `.invalid` URL；
    // token 已预先取消，绝不会访问 DNS 或外部网络，且 transcript 不含随机 server 端口。
    let cancelled_request = CancellationToken::new();
    cancelled_request.cancel();
    let cancelled = client
        .post_json(
            &cancelled_request,
            "http://api.invalid/cancelled",
            &payload,
            "fixed-api-token",
            "protocol cancelled",
        )
        .await;

    // 先取得再关闭随机 loopback 端口，触发真实 Linux connection-refused。端口只在
    // probe 内替换为占位符，其他 Go url.Error/dial 文案仍逐字比较。
    let refused_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind refused API probe port");
    let refused_address = refused_listener
        .local_addr()
        .expect("read refused API probe address");
    drop(refused_listener);
    let refused_endpoint = format!("http://{refused_address}/refused");
    let refused = client
        .post_json(
            &request_cancellation,
            &refused_endpoint,
            &payload,
            "fixed-api-token",
            "protocol refused",
        )
        .await;
    let refused_error =
        result_error_string(refused).replace(&refused_address.to_string(), "{address}");

    cancellation.cancel();
    await_server_shutdown(server_task).await;
    let captured = Arc::try_unwrap(requests)
        .expect("API request recorder still has owners")
        .into_inner();
    ProtocolHttpTranscript {
        schema_version: 1,
        protocol: "http_api",
        requests: captured,
        outcomes: vec![
            success_outcome,
            ProtocolHttpOutcome {
                name: "not_implemented",
                result: String::new(),
                error: result_error_string(not_implemented),
            },
            ProtocolHttpOutcome {
                name: "non_ok",
                result: String::new(),
                error: result_error_string(failure),
            },
            ProtocolHttpOutcome {
                name: "work_ack_dot_segments",
                result: String::new(),
                error: result_error_string(work_ack),
            },
            ProtocolHttpOutcome {
                name: "cancelled_no_retry",
                result: String::new(),
                error: result_error_string(cancelled),
            },
            ProtocolHttpOutcome {
                name: "connection_refused",
                result: String::new(),
                error: refused_error,
            },
        ],
    }
}

/// 接收一条 API 请求、记录稳定 wire 字段，并按 path 返回确定性响应。
async fn capture_api_request(
    State(state): State<ApiCaptureState>,
    request: Request<Body>,
) -> Response<Body> {
    let method = request.method().to_string();
    let target = request
        .uri()
        .path_and_query()
        .map_or_else(|| request.uri().path().to_owned(), ToString::to_string);
    let headers = ProtocolHttpHeaders {
        authorization: header_string(request.headers(), header::AUTHORIZATION.as_str()),
        accept_encoding: header_string(request.headers(), header::ACCEPT_ENCODING.as_str()),
        content_type: header_string(request.headers(), header::CONTENT_TYPE.as_str()),
        environment_runner_version: header_string(
            request.headers(),
            "x-environment-runner-version",
        ),
        claude_code_version: header_string(request.headers(), "x-claude-code-version"),
        test_header: header_string(request.headers(), "x-test-header"),
        user_agent: header_string(request.headers(), header::USER_AGENT.as_str()),
    };
    let path = request.uri().path().to_owned();
    let body = to_bytes(request.into_body(), 1024 * 1024)
        .await
        .expect("read API probe body");
    state.requests.lock().await.push(ProtocolHttpRequest {
        method,
        target,
        headers,
        body: String::from_utf8(body.to_vec()).expect("API request body is UTF-8 JSON"),
    });

    if path == "/ok" {
        // Go Transport 只有在自己添加 Accept-Encoding 时才解压；用压缩成功响应让
        // transcript 的 result 同时验证请求协商 header 与客户端自动解压路径。
        let body = gzip_probe_bytes(br#"{"ok":true,"echo":"stable"}"#);
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_ENCODING, "gzip")
            .body(Body::from(body))
            .expect("construct compressed API probe response");
    }

    let (status, body) = match path.as_str() {
        "/not-implemented" => (StatusCode::NOT_IMPLEMENTED, "missing"),
        "/v1/environments/./work/../ack" => (StatusCode::NO_CONTENT, ""),
        _ => (StatusCode::IM_A_TEAPOT, "teapot"),
    };
    Response::builder()
        .status(status)
        .body(Body::from(body))
        .expect("construct API probe response")
}

/// 把确定性 API success body 编码为一个 gzip member。
fn gzip_probe_bytes(body: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(body).expect("write API probe gzip body");
    encoder.finish().expect("finish API probe gzip body")
}

/// 生成真实 HTTP Tunnel runtime transcript，case 顺序由 manifest 固定。
async fn capture_tunnel_http_runtime_transcript() -> ProtocolTunnelHttpTranscript {
    let mut cases = Vec::new();
    // 每个 Handler future 内含 32 KiB 响应缓冲；直接写进一个 `vec![...await]` 会让
    // 编译器把多个挂起状态一起放进测试线程栈。逐个 Box 后 await 可保持默认 2 MiB
    // 栈，并不改变 case 执行顺序或网络生命周期。
    macro_rules! capture_case {
        ($future:expr) => {
            cases.push(Box::pin($future).await)
        };
    }

    capture_case!(capture_raw_tunnel_http_case(
        "stream_flush_and_request",
        "201 Created",
        "stream",
        vec![b"alpha".to_vec(), b"beta".to_vec()],
        HttpTunnelRequest {
            request_id: "req-stream".to_owned(),
            method: "POST".to_owned(),
            path: "/stream?q=1".to_owned(),
            headers: vec![
                Header {
                    name: "X-Input".to_owned(),
                    value: "yes".to_owned(),
                },
                Header {
                    name: "Host".to_owned(),
                    value: "ignored.example".to_owned(),
                },
            ],
            body: b"request-body".to_vec(),
            ..Default::default()
        },
    ));
    capture_case!(capture_raw_tunnel_http_case(
        "custom_reason_phrase",
        "299 Custom Tunnel Reason",
        "custom",
        Vec::new(),
        HttpTunnelRequest {
            request_id: "req-custom".to_owned(),
            method: "GET".to_owned(),
            path: "/custom".to_owned(),
            ..Default::default()
        },
    ));
    capture_case!(capture_raw_tunnel_http_case(
        "empty_reason_phrase",
        "298 ",
        "empty",
        Vec::new(),
        HttpTunnelRequest {
            request_id: "req-empty".to_owned(),
            method: "GET".to_owned(),
            path: "/empty".to_owned(),
            ..Default::default()
        },
    ));
    capture_case!(capture_invalid_tunnel_http_method());
    capture_case!(capture_invalid_tunnel_http_header());
    capture_case!(capture_fragment_tunnel_http_header_error());
    capture_case!(capture_tunnel_http_connection_refused());
    capture_case!(capture_fragment_tunnel_http_connection_refused());
    capture_case!(capture_raw_query_printable_bytes());
    capture_case!(capture_slow_connect_outside_header_timeout());
    capture_case!(capture_response_before_slow_body_completes());
    capture_case!(capture_slow_response_header_timeout());

    let mut multi_member = gzip_probe_bytes(b"first-member|");
    multi_member.extend_from_slice(&gzip_probe_bytes(b"second-member"));
    capture_case!(capture_auto_gzip_error(
        "multi_member_auto_gzip",
        "req-multi-gzip",
        multi_member,
    ));
    capture_case!(capture_auto_gzip_error(
        "invalid_auto_gzip",
        "req-invalid-gzip",
        b"not-a-gzip-stream".to_vec(),
    ));
    capture_case!(capture_auto_gzip_error(
        "truncated_auto_gzip",
        "req-truncated-gzip",
        vec![0x1f, 0x8b],
    ));
    let mut truncated = gzip_probe_bytes(b"payload-before-truncated-footer");
    truncated.truncate(truncated.len() - 4);
    capture_case!(capture_auto_gzip_error(
        "partial_truncated_auto_gzip",
        "req-partial-truncated-gzip",
        truncated,
    ));
    let mut bad_checksum = gzip_probe_bytes(b"payload-before-bad-checksum");
    let checksum = bad_checksum.len() - 8;
    bad_checksum[checksum] ^= 0xff;
    capture_case!(capture_auto_gzip_error(
        "checksum_auto_gzip",
        "req-checksum-gzip",
        bad_checksum,
    ));

    capture_case!(capture_malformed_tunnel_http_response(
        "early_eof_response",
        "req-early-eof",
        Vec::new(),
    ));
    capture_case!(capture_malformed_tunnel_http_response(
        "malformed_status_response",
        "req-malformed-status",
        b"NOT HTTP\r\n\r\n".to_vec(),
    ));
    capture_case!(capture_malformed_tunnel_http_response(
        "invalid_header_response",
        "req-invalid-response-header",
        b"HTTP/1.1 200 OK\r\nBad\0Header: x\r\n\r\n".to_vec(),
    ));
    capture_case!(capture_malformed_tunnel_http_response(
        "conflicting_content_length",
        "req-conflicting-length",
        b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nx".to_vec(),
    ));
    capture_case!(capture_malformed_tunnel_http_response(
        "malformed_chunk",
        "req-malformed-chunk",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\nZ\r\n".to_vec(),
    ));
    capture_case!(capture_connection_reset_before_headers());
    capture_case!(capture_tunnel_http_cancellation());

    ProtocolTunnelHttpTranscript {
        schema_version: 1,
        protocol: "tunnel_http_runtime",
        cases,
    }
}

/// 启动一次性 raw HTTP/1.1 mock，调用真实 Handler 并记录 flush 与 reason phrase。
async fn capture_raw_tunnel_http_case(
    name: &'static str,
    status_line: &'static str,
    reply_header: &'static str,
    chunks: Vec<Vec<u8>>,
    mut request: HttpTunnelRequest,
) -> ProtocolTunnelHttpCase {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 HTTP Tunnel runtime listener");
    let port = listener.local_addr().expect("读取 HTTP listener").port();
    let (capture_tx, capture_rx) = oneshot::channel();
    let (chunk_ack_tx, mut chunk_ack_rx) = mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("接收 HTTP Tunnel 请求");
        let capture = read_runtime_http_request(&mut stream, port).await;
        capture_tx.send(capture).expect("发送 HTTP request capture");
        let header = format!(
            "HTTP/1.1 {status_line}\r\nTransfer-Encoding: chunked\r\nX-Reply: {reply_header}\r\nConnection: close\r\n\r\n"
        );
        stream
            .write_all(header.as_bytes())
            .await
            .expect("写 HTTP response header");
        stream.flush().await.expect("flush HTTP response header");
        for chunk in chunks {
            stream
                .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                .await
                .expect("写 chunk length");
            stream.write_all(&chunk).await.expect("写 chunk data");
            stream.write_all(b"\r\n").await.expect("写 chunk suffix");
            stream.flush().await.expect("flush response chunk");
            // 正确实现会在下一次 flush 前把当前小块交给 sender。超时仍继续，
            // 让错误实现产生 transcript 差异，而不是把差分门禁卡死。
            let _ = tokio::time::timeout(Duration::from_millis(500), chunk_ack_rx.recv()).await;
        }
        stream
            .write_all(b"0\r\n\r\n")
            .await
            .expect("写 final HTTP chunk");
        stream.flush().await.expect("flush final HTTP chunk");
    });

    request.port = i32::from(port);
    let sender = Arc::new(ProtocolTunnelHttpSender {
        events: Mutex::new(Vec::new()),
        chunk_ack: Some(chunk_ack_tx),
    });
    let result = new_handler(None)
        .expect("构造 HTTP Tunnel handler")
        .handle_request(CancellationToken::new(), request, sender.clone())
        .await;
    let request = tokio::time::timeout(Duration::from_secs(3), capture_rx)
        .await
        .expect("等待 HTTP request capture 超时")
        .expect("HTTP capture channel closed");
    server.await.expect("HTTP runtime server panicked");
    ProtocolTunnelHttpCase {
        name,
        request,
        events: sender.snapshot(),
        handler_error: tunnel_handler_error(result),
    }
}

/// 固定 Rust/Go 对非法 HTTP method 的 sender 顺序与返回错误。
async fn capture_invalid_tunnel_http_method() -> ProtocolTunnelHttpCase {
    let sender = Arc::new(ProtocolTunnelHttpSender::default());
    let result = new_handler(None)
        .expect("构造 invalid-method handler")
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "req-invalid-method".to_owned(),
                port: 9,
                method: "BAD METHOD".to_owned(),
                path: "/invalid".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await;
    ProtocolTunnelHttpCase {
        name: "invalid_method",
        request: ProtocolTunnelHttpRequest::default(),
        events: sender.snapshot(),
        handler_error: tunnel_handler_error(result),
    }
}

/// 固定 Transport 对非法 HTTP header name 的协议错误；该场景不得实际联网。
async fn capture_invalid_tunnel_http_header() -> ProtocolTunnelHttpCase {
    let sender = Arc::new(ProtocolTunnelHttpSender::default());
    let result = new_handler(None)
        .expect("构造 invalid-header handler")
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "req-invalid-header".to_owned(),
                port: 9,
                method: "GET".to_owned(),
                path: "/invalid".to_owned(),
                headers: vec![Header {
                    name: "Bad Header".to_owned(),
                    value: "value".to_owned(),
                }],
                ..Default::default()
            },
            sender.clone(),
        )
        .await;
    ProtocolTunnelHttpCase {
        name: "invalid_header",
        request: ProtocolTunnelHttpRequest::default(),
        events: sender.snapshot(),
        handler_error: tunnel_handler_error(result),
    }
}

/// 固定 fragment 在非法 header `url.Error` 中的 Go `EscapedFragment` 文本。
async fn capture_fragment_tunnel_http_header_error() -> ProtocolTunnelHttpCase {
    let sender = Arc::new(ProtocolTunnelHttpSender::default());
    let result = new_handler(None)
        .expect("构造 fragment-header handler")
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "req-fragment-header".to_owned(),
                port: 9,
                method: "GET".to_owned(),
                path: "/invalid?x=1#frag <\\é%2f'[]".to_owned(),
                headers: vec![Header {
                    name: "Bad Header".to_owned(),
                    value: "value".to_owned(),
                }],
                ..Default::default()
            },
            sender.clone(),
        )
        .await;
    ProtocolTunnelHttpCase {
        name: "fragment_header_error",
        request: ProtocolTunnelHttpRequest::default(),
        events: sender.snapshot(),
        handler_error: tunnel_handler_error(result),
    }
}

/// 固定 fragment 在 Linux connection-refused 错误 URL 中的保留规则。
async fn capture_fragment_tunnel_http_connection_refused() -> ProtocolTunnelHttpCase {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 fragment-refused listener");
    let port = listener
        .local_addr()
        .expect("读取 fragment-refused port")
        .port();
    drop(listener);

    let sender = Arc::new(ProtocolTunnelHttpSender::default());
    let result = new_handler(None)
        .expect("构造 fragment-refused handler")
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "req-fragment-refused".to_owned(),
                port: i32::from(port),
                method: "GET".to_owned(),
                path: "/refused?x=1#frag <\\é%2f'[]".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await;
    let mut events = sender.snapshot();
    assert_eq!(events.len(), 1, "fragment 拒绝必须只发送一个错误");
    let port_text = port.to_string();
    assert!(
        events[0].error_message.matches(&port_text).count() >= 2,
        "fragment 拒绝错误必须同时包含 URL 与 dial 端口: {:?}",
        events[0],
    );
    events[0].error_message = events[0].error_message.replace(&port_text, "{port}");
    ProtocolTunnelHttpCase {
        name: "fragment_connection_refused",
        request: ProtocolTunnelHttpRequest::default(),
        events,
        handler_error: tunnel_handler_error(result),
    }
}

/// 逐字验证 Go RawQuery 允许的全部可打印 ASCII、UTF-8 与坏 percent escape。
async fn capture_raw_query_printable_bytes() -> ProtocolTunnelHttpCase {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 raw-query listener");
    let port = listener.local_addr().expect("读取 raw-query port").port();
    let (capture_tx, capture_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("接收 raw-query 请求");
        let head = read_raw_http_head_bytes(&mut stream).await;
        let line_end = find_probe_bytes(&head, b"\r\n").expect("raw query request line terminator");
        let line = &head[..line_end];
        assert!(line.starts_with(b"GET "), "raw query method 必须为 GET");
        let suffix = b" HTTP/1.1";
        assert!(line.ends_with(suffix), "raw query 必须使用 HTTP/1.1");
        let target = String::from_utf8(line[4..line.len() - suffix.len()].to_vec())
            .expect("raw query target 必须保留 UTF-8");
        let host = format!("Host: localhost:{port}");
        let head_text = String::from_utf8_lossy(&head);
        capture_tx
            .send(ProtocolTunnelHttpRequest {
                observed: true,
                method: "GET".to_owned(),
                target,
                host_matches_loopback_target: head_text
                    .lines()
                    .any(|line| line.eq_ignore_ascii_case(&host)),
                ..Default::default()
            })
            .expect("发送 raw-query capture");
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .expect("写 raw-query response");
    });

    let printable = (0x20_u8..=0x7e)
        .filter(|byte| *byte != b'#')
        .collect::<Vec<_>>();
    let mut path = format!(
        "/raw?bytes={}",
        String::from_utf8(printable).expect("printable ASCII")
    );
    path.push_str("é%zz");
    let sender = Arc::new(ProtocolTunnelHttpSender::default());
    let result = new_handler(None)
        .expect("构造 raw-query handler")
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "req-raw-query".to_owned(),
                port: i32::from(port),
                method: "GET".to_owned(),
                path,
                ..Default::default()
            },
            sender.clone(),
        )
        .await;
    let request = tokio::time::timeout(Duration::from_secs(3), capture_rx)
        .await
        .expect("等待 raw-query capture 超时")
        .expect("raw-query capture channel closed");
    server.await.expect("raw-query server panicked");
    ProtocolTunnelHttpCase {
        name: "raw_query_printable_bytes",
        request,
        events: sender.snapshot(),
        handler_error: tunnel_handler_error(result),
    }
}

/// 验证拨号延迟位于 ResponseHeaderTimeout 计时范围之外。
async fn capture_slow_connect_outside_header_timeout() -> ProtocolTunnelHttpCase {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 slow-connect listener");
    let port = listener
        .local_addr()
        .expect("读取 slow-connect port")
        .port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("接收 slow-connect 请求");
        let _ = read_raw_http_head_bytes(&mut stream).await;
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
            .await
            .expect("写 slow-connect response");
    });

    let handler = new_handler_with_timing_for_probe(
        None,
        Duration::from_millis(30),
        Duration::from_millis(120),
    )
    .expect("构造 slow-connect handler");
    let sender = Arc::new(ProtocolTunnelHttpSender::default());
    let started = std::time::Instant::now();
    let result = handler
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "req-slow-connect".to_owned(),
                port: i32::from(port),
                method: "GET".to_owned(),
                path: "/slow-connect".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await;
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "slow-connect 延迟必须真实执行"
    );
    server.await.expect("slow-connect server panicked");
    ProtocolTunnelHttpCase {
        name: "slow_connect_outside_header_timeout",
        request: ProtocolTunnelHttpRequest::default(),
        events: sender.snapshot(),
        handler_error: tunnel_handler_error(result),
    }
}

/// 固定服务端不读取 8 MiB body、但先返回响应时的 Go readLoop/writeLoop 竞态。
async fn capture_response_before_slow_body_completes() -> ProtocolTunnelHttpCase {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 slow-body listener");
    let port = listener.local_addr().expect("读取 slow-body port").port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("接收 slow-body 请求");
        let head = read_raw_http_head_bytes(&mut stream).await;
        let head_text = String::from_utf8_lossy(&head);
        assert!(
            head_text
                .lines()
                .any(|line| line.eq_ignore_ascii_case("content-length: 8388608")),
            "slow-body 必须声明 8 MiB Content-Length"
        );
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
            .await
            .expect("写 early response");
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let handler =
        new_handler_with_timing_for_probe(None, Duration::from_millis(30), Duration::ZERO)
            .expect("构造 slow-body handler");
    let sender = Arc::new(ProtocolTunnelHttpSender::default());
    let result = handler
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "req-slow-body".to_owned(),
                port: i32::from(port),
                method: "POST".to_owned(),
                path: "/slow-body".to_owned(),
                body: vec![b'x'; 8 * 1024 * 1024],
                ..Default::default()
            },
            sender.clone(),
        )
        .await;
    server.await.expect("slow-body server panicked");
    ProtocolTunnelHttpCase {
        name: "response_before_slow_body_complete",
        request: ProtocolTunnelHttpRequest::default(),
        events: sender.snapshot(),
        handler_error: tunnel_handler_error(result),
    }
}

/// 固定请求完整写入后，慢响应 header 触发 Go timeout 文本。
async fn capture_slow_response_header_timeout() -> ProtocolTunnelHttpCase {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 slow-header listener");
    let port = listener.local_addr().expect("读取 slow-header port").port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("接收 slow-header 请求");
        let _ = read_raw_http_head_bytes(&mut stream).await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let _ = stream
            .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
            .await;
    });

    let handler =
        new_handler_with_timing_for_probe(None, Duration::from_millis(30), Duration::ZERO)
            .expect("构造 slow-header handler");
    let sender = Arc::new(ProtocolTunnelHttpSender::default());
    let result = handler
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "req-slow-header".to_owned(),
                port: i32::from(port),
                method: "GET".to_owned(),
                path: "/slow-header".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await;
    let mut events = sender.snapshot();
    assert_eq!(events.len(), 1, "slow-header 必须只产生 timeout error");
    events[0].error_message = events[0].error_message.replace(&port.to_string(), "{port}");
    server.await.expect("slow-header server panicked");
    ProtocolTunnelHttpCase {
        name: "slow_response_header_timeout",
        request: ProtocolTunnelHttpRequest::default(),
        events,
        handler_error: tunnel_handler_error(result),
    }
}

/// 在刚关闭的 loopback 端口上记录完整连接拒绝错误。
///
/// 动态端口只替换为单一占位符；地址族、`dial tcp` 前缀、标点和 OS 错误原文都保留
/// 在 transcript 中，因此笼统的第三方库错误文本仍会触发严格差分。
async fn capture_tunnel_http_connection_refused() -> ProtocolTunnelHttpCase {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 connection-refused listener");
    let port = listener
        .local_addr()
        .expect("读取 connection-refused port")
        .port();
    drop(listener);

    let sender = Arc::new(ProtocolTunnelHttpSender::default());
    let result = new_handler(None)
        .expect("构造 connection-refused handler")
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "req-refused".to_owned(),
                port: i32::from(port),
                method: "GET".to_owned(),
                path: "/refused".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await;
    let mut events = sender.snapshot();
    assert_eq!(events.len(), 1, "连接拒绝必须只发送一个 error event");
    assert_eq!(events[0].kind, "error");
    let port_text = port.to_string();
    assert!(
        events[0].error_message.matches(&port_text).count() >= 2,
        "连接拒绝错误必须同时包含 URL 与 dial 端口: {:?}",
        events[0],
    );
    events[0].error_message = events[0].error_message.replace(&port_text, "{port}");
    ProtocolTunnelHttpCase {
        name: "connection_refused",
        request: ProtocolTunnelHttpRequest::default(),
        events,
        handler_error: tunnel_handler_error(result),
    }
}

/// 用 raw HTTP/1.1 server 返回损坏的自动 gzip body，并记录完整解码错误。
///
/// `body` 由调用方选择非法 header 或截断 header；本函数同时保留自动移除
/// Content-Encoding/Content-Length 后的 headers event 与后续 error event 顺序。
async fn capture_auto_gzip_error(
    name: &'static str,
    request_id: &'static str,
    body: Vec<u8>,
) -> ProtocolTunnelHttpCase {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 gzip-error listener");
    let port = listener.local_addr().expect("读取 gzip-error port").port();
    let (capture_tx, capture_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("接收 gzip-error 请求");
        let capture = read_runtime_http_request(&mut stream, port).await;
        capture_tx.send(capture).expect("发送 gzip request capture");
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len(),
        );
        stream
            .write_all(header.as_bytes())
            .await
            .expect("写 gzip-error response header");
        stream
            .write_all(&body)
            .await
            .expect("写 gzip-error response body");
        stream.shutdown().await.expect("关闭 gzip-error response");
    });

    let sender = Arc::new(ProtocolTunnelHttpSender::default());
    let result = new_handler(None)
        .expect("构造 gzip-error handler")
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: request_id.to_owned(),
                port: i32::from(port),
                method: "GET".to_owned(),
                path: "/gzip-error".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await;
    let request = tokio::time::timeout(Duration::from_secs(3), capture_rx)
        .await
        .expect("等待 gzip request capture 超时")
        .expect("gzip capture channel closed");
    server.await.expect("gzip-error server panicked");
    ProtocolTunnelHttpCase {
        name,
        request,
        events: sender.snapshot(),
        handler_error: tunnel_handler_error(result),
    }
}

/// 把受控 raw response 交给真实 Handler，固定 HTTP parser/framing 错误文本。
async fn capture_malformed_tunnel_http_response(
    name: &'static str,
    request_id: &'static str,
    response: Vec<u8>,
) -> ProtocolTunnelHttpCase {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 malformed-response listener");
    let port = listener
        .local_addr()
        .expect("读取 malformed-response port")
        .port();
    let (capture_tx, capture_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener
            .accept()
            .await
            .expect("接收 malformed-response 请求");
        let capture = read_runtime_http_request(&mut stream, port).await;
        capture_tx
            .send(capture)
            .expect("发送 malformed-response capture");
        if !response.is_empty() {
            stream
                .write_all(&response)
                .await
                .expect("写 malformed response");
        }
        stream.shutdown().await.expect("关闭 malformed response");
    });

    let sender = Arc::new(ProtocolTunnelHttpSender::default());
    let result = new_handler(None)
        .expect("构造 malformed-response handler")
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: request_id.to_owned(),
                port: i32::from(port),
                method: "GET".to_owned(),
                path: "/malformed".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await;
    let request = tokio::time::timeout(Duration::from_secs(3), capture_rx)
        .await
        .expect("等待 malformed-response capture 超时")
        .expect("malformed-response capture channel closed");
    server.await.expect("malformed-response server panicked");
    let mut events = sender.snapshot();
    for event in &mut events {
        event.error_message = event.error_message.replace(&port.to_string(), "{port}");
    }
    ProtocolTunnelHttpCase {
        name,
        request,
        events,
        handler_error: tunnel_handler_error(result),
    }
}

/// 使用 Linux `SO_LINGER=0` 产生响应 header 前 TCP reset，并保留 Go net.OpError 结构。
async fn capture_connection_reset_before_headers() -> ProtocolTunnelHttpCase {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 reset listener");
    let port = listener.local_addr().expect("读取 reset port").port();
    let (capture_tx, capture_rx) = oneshot::channel();
    let (addresses_tx, addresses_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("接收 reset 请求");
        let capture = read_runtime_http_request(&mut stream, port).await;
        capture_tx.send(capture).expect("发送 reset capture");
        let remote = stream.peer_addr().expect("读取 reset client address");
        let local = stream.local_addr().expect("读取 reset server address");
        addresses_tx
            .send((remote, local))
            .expect("发送 reset socket addresses");
        let linger = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        // SAFETY: `stream.as_raw_fd()` 在本作用域内保持有效；指针指向已初始化的
        // `libc::linger`，长度与目标类型完全一致。调用仅设置 Linux socket 选项。
        let result = unsafe {
            libc::setsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                std::ptr::from_ref(&linger).cast(),
                std::mem::size_of::<libc::linger>() as libc::socklen_t,
            )
        };
        assert_eq!(
            result,
            0,
            "设置 SO_LINGER 失败: {}",
            std::io::Error::last_os_error()
        );
        drop(stream);
    });

    let sender = Arc::new(ProtocolTunnelHttpSender::default());
    let result = new_handler(None)
        .expect("构造 reset handler")
        .handle_request(
            CancellationToken::new(),
            HttpTunnelRequest {
                request_id: "req-reset".to_owned(),
                port: i32::from(port),
                method: "GET".to_owned(),
                path: "/reset".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await;
    let request = tokio::time::timeout(Duration::from_secs(3), capture_rx)
        .await
        .expect("等待 reset capture 超时")
        .expect("reset capture channel closed");
    let (client_address, server_address) =
        addresses_rx.await.expect("reset address channel closed");
    server.await.expect("reset server panicked");
    let mut events = sender.snapshot();
    assert_eq!(events.len(), 1, "reset 必须只产生一个 error event");
    let expected_network_text =
        format!("read tcp {client_address}->{server_address}: read: connection reset by peer");
    assert!(
        events[0].error_message.contains(&expected_network_text),
        "reset error {:?} 必须包含 {expected_network_text:?}",
        events[0],
    );
    events[0].error_message = events[0]
        .error_message
        .replace(&client_address.to_string(), "127.0.0.1:{local_port}")
        .replace(&server_address.to_string(), "127.0.0.1:{port}")
        .replace(&port.to_string(), "{port}");
    ProtocolTunnelHttpCase {
        name: "connection_reset_before_headers",
        request,
        events,
        handler_error: tunnel_handler_error(result),
    }
}

/// 等待 mock 收到真实请求后调用 CancelRequest，并记录端口无关的取消分类。
async fn capture_tunnel_http_cancellation() -> ProtocolTunnelHttpCase {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 cancellation listener");
    let port = listener
        .local_addr()
        .expect("读取 cancellation port")
        .port();
    let (capture_tx, capture_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("接收 cancellation 请求");
        let capture = read_runtime_http_request(&mut stream, port).await;
        capture_tx.send(capture).expect("发送 cancellation capture");
        let mut byte = [0_u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut byte))
            .await
            .expect("取消后客户端没有关闭连接");
        // 与 Go mock 的 io.Copy 判定一致：FIN 返回 0，RST 返回读错误，两者都证明
        // 取消已终止连接；只有仍阻塞到超时才表示 CancelRequest 没有生效。
        if let Ok(read) = read {
            assert_eq!(read, 0, "取消后客户端不得继续发送 HTTP body");
        }
    });

    let handler = Arc::new(new_handler(None).expect("构造 cancellation handler"));
    let sender = Arc::new(ProtocolTunnelHttpSender::default());
    let running_handler = Arc::clone(&handler);
    let running_sender = Arc::clone(&sender);
    let running = tokio::spawn(async move {
        running_handler
            .handle_request(
                CancellationToken::new(),
                HttpTunnelRequest {
                    request_id: "req-cancel".to_owned(),
                    port: i32::from(port),
                    method: "GET".to_owned(),
                    path: "/cancel".to_owned(),
                    ..Default::default()
                },
                running_sender,
            )
            .await
    });
    let request = tokio::time::timeout(Duration::from_secs(3), capture_rx)
        .await
        .expect("等待 cancellation capture 超时")
        .expect("cancellation capture channel closed");
    handler.cancel_request("req-cancel");
    let result = tokio::time::timeout(Duration::from_secs(3), running)
        .await
        .expect("取消后 Handler 未返回")
        .expect("取消 Handler task panicked");
    server.await.expect("cancellation server panicked");
    let mut events = sender.snapshot();
    assert_eq!(events.len(), 1, "取消必须只发送一个 HTTP error");
    assert_eq!(events[0].kind, "error");
    assert!(
        events[0].error_message.contains("request failed"),
        "取消错误必须保留 request failed 上下文: {:?}",
        events[0]
    );
    events[0].error_class = if events[0].error_message.contains("context canceled") {
        "context_canceled".to_owned()
    } else {
        "request_failed_without_context_marker".to_owned()
    };
    // 原始错误包含随机 loopback 端口，不写 transcript；分类值本身由上面的严格
    // 文本判断生成，因此 Rust 若缺少 Go 的 context marker 会成为明确差异。
    events[0].error_message.clear();
    ProtocolTunnelHttpCase {
        name: "cancel",
        request,
        events,
        handler_error: tunnel_handler_error(result),
    }
}

/// 读取到 HTTP header 终止符，但不消费随后可能很大的请求体。
async fn read_raw_http_head_bytes(stream: &mut TcpStream) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1024);
    let mut byte = [0_u8; 1];
    while !bytes.ends_with(b"\r\n\r\n") {
        stream
            .read_exact(&mut byte)
            .await
            .expect("读取 raw HTTP request header");
        bytes.push(byte[0]);
        assert!(bytes.len() <= 64 * 1024, "raw HTTP request header 过大");
    }
    bytes
}

/// 在 probe 的有界字节切片中查找固定分隔符。
fn find_probe_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// 从单个 HTTP/1.1 请求读取 method、target、受控 header 和完整 body。
async fn read_runtime_http_request(stream: &mut TcpStream, port: u16) -> ProtocolTunnelHttpRequest {
    let bytes = read_raw_http_head_bytes(stream).await;
    let header_text = std::str::from_utf8(&bytes).expect("HTTP request header 必须是 UTF-8");
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next().expect("缺少 HTTP request line");
    let mut request_fields = request_line.split_whitespace();
    let method = request_fields.next().unwrap_or_default().to_owned();
    let target = request_fields.next().unwrap_or_default().to_owned();
    let mut headers = BTreeMap::<String, String>::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').expect("HTTP header 缺少冒号");
        headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
    }
    let content_length = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_default();
    let mut body = vec![0_u8; content_length];
    stream
        .read_exact(&mut body)
        .await
        .expect("读取 HTTP request body");
    ProtocolTunnelHttpRequest {
        observed: true,
        method,
        target,
        host_matches_loopback_target: headers.get("host") == Some(&format!("localhost:{port}")),
        user_agent: headers.get("user-agent").cloned().unwrap_or_default(),
        input_header: headers.get("x-input").cloned().unwrap_or_default(),
        body: String::from_utf8(body).expect("HTTP probe body 必须是 UTF-8"),
    }
}

/// 在已排序 Tunnel header 中读取首个名称匹配值。
fn tunnel_header_value(headers: &[Header], name: &str) -> String {
    headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case(name))
        .map(|header| header.value.clone())
        .unwrap_or_default()
}

/// 把 Handler Result 转为 Go probe 使用的空串或完整错误文本。
fn tunnel_handler_error(result: Result<(), TunnelError>) -> String {
    result
        .err()
        .map_or_else(String::new, |error| error.to_string())
}

/// 成功 WebSocket mock 收到的客户端 frame 与 close 汇总。
struct ProtocolTunnelWsServerResult {
    messages: Vec<ProtocolTunnelWsWireMessage>,
    close: ProtocolTunnelWsClose,
}

/// 生成真实本地 WebSocket Tunnel runtime transcript，case 顺序由 manifest 固定。
async fn capture_tunnel_ws_runtime_transcript() -> ProtocolTunnelWsTranscript {
    ProtocolTunnelWsTranscript {
        schema_version: 1,
        protocol: "tunnel_ws_runtime",
        cases: vec![
            capture_tunnel_ws_success().await,
            capture_tunnel_ws_bad_handshake().await,
            capture_tunnel_ws_abnormal_eof().await,
        ],
    }
}

/// 覆盖握手 header、子协议、双向 text/binary frame 和客户端主动 close。
async fn capture_tunnel_ws_success() -> ProtocolTunnelWsCase {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 WebSocket success listener");
    let port = listener.local_addr().expect("读取 WebSocket port").port();
    let (handshake_tx, mut handshake_rx) = mpsc::unbounded_channel();
    let (result_tx, result_rx) = oneshot::channel();
    let result_tx = Arc::new(Mutex::new(Some(result_tx)));
    let router = Router::new().route(
        "/socket",
        any(move |headers: HeaderMap, websocket: WebSocketUpgrade| {
            let handshake_tx = handshake_tx.clone();
            let result_tx = Arc::clone(&result_tx);
            async move {
                handshake_tx
                    .send(ProtocolTunnelHandshake {
                        observed: true,
                        host: header_string(&headers, header::HOST.as_str()),
                        tunnel_header: header_string(&headers, "x-tunnel"),
                        offered_protocols: header_string(
                            &headers,
                            header::SEC_WEBSOCKET_PROTOCOL.as_str(),
                        ),
                    })
                    .expect("发送 WebSocket handshake capture");
                websocket
                    .protocols(["proto1"])
                    .on_upgrade(move |mut socket| async move {
                        let mut messages = Vec::with_capacity(2);
                        let first = socket
                            .recv()
                            .await
                            .expect("客户端未发送 text frame")
                            .expect("读取客户端 text frame");
                        messages.push(axum_ws_wire_message(first));
                        socket
                            .send(AxumWebSocketMessage::Text("server-text".into()))
                            .await
                            .expect("发送 server text frame");
                        socket
                            .send(AxumWebSocketMessage::Binary(vec![0x00, 0xfb, 0xff].into()))
                            .await
                            .expect("发送 server binary frame");
                        let second = socket
                            .recv()
                            .await
                            .expect("客户端未发送 binary frame")
                            .expect("读取客户端 binary frame");
                        messages.push(axum_ws_wire_message(second));
                        let close = socket
                            .recv()
                            .await
                            .expect("客户端未发送 close frame")
                            .expect("读取客户端 close frame");
                        let AxumWebSocketMessage::Close(close) = close else {
                            panic!("期待客户端 close frame，实际为 {close:?}");
                        };
                        let close = close.map_or_else(ProtocolTunnelWsClose::default, |close| {
                            ProtocolTunnelWsClose {
                                observed: true,
                                code: close.code,
                                reason: close.reason.to_string(),
                            }
                        });
                        result_tx
                            .lock()
                            .unwrap()
                            .take()
                            .expect("WebSocket result 只能发送一次")
                            .send(ProtocolTunnelWsServerResult { messages, close })
                            .ok();
                    })
            }
        }),
    );
    let server_cancellation = CancellationToken::new();
    let server_stop = server_cancellation.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(server_stop.cancelled_owned())
            .await
            .expect("运行 WebSocket success server");
    });

    let handler = new_ws_handler(None);
    let sender = Arc::new(ProtocolTunnelWsSender::default());
    let open_result = handler
        .open_tunnel(
            CancellationToken::new(),
            WsTunnelOpen {
                tunnel_id: "ws-success".to_owned(),
                port: i32::from(port),
                path: "/socket".to_owned(),
                headers: vec![
                    Header {
                        name: "X-Tunnel".to_owned(),
                        value: "yes".to_owned(),
                    },
                    Header {
                        name: "Host".to_owned(),
                        value: "fixed.example".to_owned(),
                    },
                ],
                protocols: vec!["proto2".to_owned(), "proto1".to_owned()],
            },
            sender.clone(),
        )
        .await;
    assert!(
        open_result.is_ok(),
        "打开 WebSocket success tunnel: {open_result:?}"
    );
    handler
        .send_message(
            &CancellationToken::new(),
            WsTunnelMessage {
                tunnel_id: "ws-success".to_owned(),
                data: b"client-text".to_vec(),
                is_binary: false,
            },
        )
        .await
        .expect("发送客户端 text frame");
    sender
        .wait_for(|events| {
            events
                .iter()
                .filter(|event| event.kind == "message")
                .count()
                == 2
        })
        .await;
    handler
        .send_message(
            &CancellationToken::new(),
            WsTunnelMessage {
                tunnel_id: "ws-success".to_owned(),
                data: vec![0x01, 0x02, 0x03],
                is_binary: true,
            },
        )
        .await
        .expect("发送客户端 binary frame");
    handler
        .close_tunnel(
            &CancellationToken::new(),
            WsTunnelClose {
                tunnel_id: "ws-success".to_owned(),
                code: 1000,
                reason: "client-done".to_owned(),
            },
        )
        .await
        .expect("关闭 WebSocket success tunnel");

    let handshake = tokio::time::timeout(Duration::from_secs(3), handshake_rx.recv())
        .await
        .expect("等待 WebSocket handshake 超时")
        .expect("WebSocket handshake channel closed");
    let result = tokio::time::timeout(Duration::from_secs(3), result_rx)
        .await
        .expect("等待 WebSocket server result 超时")
        .expect("WebSocket result channel closed");
    server_cancellation.cancel();
    await_server_shutdown(server).await;
    ProtocolTunnelWsCase {
        name: "success_frames_and_close",
        handshake,
        client_messages: result.messages,
        client_close: result.close,
        events: sender.snapshot(),
        handler_error: tunnel_handler_error(open_result),
    }
}

/// 固定带 HTTP response 的 403 坏握手错误文本。
async fn capture_tunnel_ws_bad_handshake() -> ProtocolTunnelWsCase {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 bad-handshake listener");
    let port = listener
        .local_addr()
        .expect("读取 bad-handshake port")
        .port();
    let cancellation = CancellationToken::new();
    let server_stop = cancellation.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().fallback(|| async { StatusCode::FORBIDDEN }),
        )
        .with_graceful_shutdown(server_stop.cancelled_owned())
        .await
        .expect("运行 bad-handshake server");
    });
    let sender = Arc::new(ProtocolTunnelWsSender::default());
    let result = new_ws_handler(None)
        .open_tunnel(
            CancellationToken::new(),
            WsTunnelOpen {
                tunnel_id: "ws-bad-handshake".to_owned(),
                port: i32::from(port),
                path: "/reject".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await;
    cancellation.cancel();
    await_server_shutdown(server).await;
    ProtocolTunnelWsCase {
        name: "bad_handshake",
        handshake: ProtocolTunnelHandshake::default(),
        client_messages: Vec::new(),
        client_close: ProtocolTunnelWsClose::default(),
        events: sender.snapshot(),
        handler_error: tunnel_handler_error(result),
    }
}

/// 完成合法 101 后直接关闭 TCP，不发送 WebSocket close，记录 1006 异常关闭文本。
async fn capture_tunnel_ws_abnormal_eof() -> ProtocolTunnelWsCase {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 abnormal-EOF listener");
    let port = listener
        .local_addr()
        .expect("读取 abnormal-EOF port")
        .port();
    let (handshake_tx, handshake_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("接收 abnormal-EOF 握手");
        let headers = read_runtime_websocket_headers(&mut stream).await;
        handshake_tx
            .send(ProtocolTunnelHandshake {
                observed: true,
                ..Default::default()
            })
            .expect("发送 abnormal-EOF handshake capture");
        let key = headers
            .get("sec-websocket-key")
            .expect("握手缺少 Sec-WebSocket-Key");
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
            derive_accept_key(key.as_bytes())
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("写 abnormal-EOF 101 response");
        stream.flush().await.expect("flush abnormal-EOF handshake");
        tokio::time::sleep(Duration::from_millis(100)).await;
        // 直接 drop TCP stream，以 FIN 结束且不产生 WebSocket close frame。
    });

    let sender = Arc::new(ProtocolTunnelWsSender::default());
    let result = new_ws_handler(None)
        .open_tunnel(
            CancellationToken::new(),
            WsTunnelOpen {
                tunnel_id: "ws-abnormal-eof".to_owned(),
                port: i32::from(port),
                path: "/eof".to_owned(),
                ..Default::default()
            },
            sender.clone(),
        )
        .await;
    sender
        .wait_for(|events| events.iter().any(|event| event.kind == "close"))
        .await;
    let handshake = tokio::time::timeout(Duration::from_secs(3), handshake_rx)
        .await
        .expect("等待 abnormal-EOF handshake 超时")
        .expect("abnormal-EOF handshake channel closed");
    server.await.expect("abnormal-EOF server panicked");
    ProtocolTunnelWsCase {
        name: "abnormal_eof",
        handshake,
        client_messages: Vec::new(),
        client_close: ProtocolTunnelWsClose::default(),
        events: sender.snapshot(),
        handler_error: tunnel_handler_error(result),
    }
}

/// 把 Axum 收到的客户端 text/binary frame 转成稳定 wire schema。
fn axum_ws_wire_message(message: AxumWebSocketMessage) -> ProtocolTunnelWsWireMessage {
    match message {
        AxumWebSocketMessage::Text(data) => ProtocolTunnelWsWireMessage {
            kind: "text".to_owned(),
            data_base64: STANDARD.encode(data.as_bytes()),
        },
        AxumWebSocketMessage::Binary(data) => ProtocolTunnelWsWireMessage {
            kind: "binary".to_owned(),
            data_base64: STANDARD.encode(data),
        },
        other => panic!("期待客户端 text/binary frame，实际为 {other:?}"),
    }
}

/// 读取 raw WebSocket 握手 header，名称统一小写但值保持原始字节对应文本。
async fn read_runtime_websocket_headers(stream: &mut TcpStream) -> BTreeMap<String, String> {
    let mut bytes = Vec::with_capacity(1024);
    let mut byte = [0_u8; 1];
    while !bytes.ends_with(b"\r\n\r\n") {
        stream
            .read_exact(&mut byte)
            .await
            .expect("读取 WebSocket handshake header");
        bytes.push(byte[0]);
        assert!(bytes.len() <= 64 * 1024, "WebSocket handshake header 过大");
    }
    let text = std::str::from_utf8(&bytes).expect("WebSocket handshake 必须是 UTF-8");
    text.split("\r\n")
        .skip(1)
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (name, value) = line.split_once(':').expect("WebSocket header 缺少冒号");
            (name.to_ascii_lowercase(), value.trim().to_owned())
        })
        .collect()
}

/// 启动真实 Rust MCP Streamable HTTP server，并执行与 Go probe 完全相同的请求序列。
async fn capture_mcp_transcript() -> ProtocolMcpTranscript {
    assert!(
        std::env::var_os("MCP_DEBUG_TOKEN").is_some(),
        "MCP_DEBUG_TOKEN must be set before the Rust test process starts"
    );
    // Go 的 nil logger 在构造器内回退到 slog.Default；Rust BaseServer 已把该
    // 归一化放到调用边界，因此 probe 显式传入同一个兼容 logger。
    let server = BaseServer::new(default_slog_logger().clone(), "protocol-probe", "1.2.3");
    server.set_bearer_token("fixed-mcp-token");

    // mcp-go 的 Tool.MarshalJSON 会始终输出 annotations；显式扩展字段确保 Rust
    // compatibility DTO 表达相同声明，具体字段顺序仍保留在原始响应体中接受门禁。
    let mut extra = Map::new();
    extra.insert("annotations".to_owned(), json!({}));
    let tools = vec![ToolConfig {
        tool: Tool {
            name: "echo".to_owned(),
            description: "Return a deterministic value".to_owned(),
            input_schema: json!({
                "properties": {"message": {"type": "string"}},
                "required": ["message"],
                "type": "object"
            }),
            extra,
        },
        handler: ToolHandler::new(|_cancellation, request| async move {
            Ok(new_tool_result_text(format!(
                "echo:{}",
                request.get_string("message", "")
            )))
        }),
    }];
    let config = server.start(tools).await.expect("start MCP probe server");
    assert_eq!(config.bearer_token, "fixed-mcp-token");
    let endpoint = format!("http://127.0.0.1:{}/mcp", config.port);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("construct MCP probe client");
    let requests = [
        (
            "unauthorized",
            false,
            r#"{"jsonrpc":"2.0","id":0,"method":"tools/list"}"#,
        ),
        (
            "initialize",
            true,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"probe","version":"1"}}}"#,
        ),
        (
            "initialized_notification",
            true,
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        ),
        (
            "tools_list",
            true,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
        ),
        (
            "tools_call",
            true,
            r#"{"jsonrpc":"2.0","id":"call-1","method":"tools/call","params":{"name":"echo","arguments":{"message":"hello"},"futureField":"kept"},"futureTopLevel":true}"#,
        ),
        (
            "tool_not_found",
            true,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"missing","arguments":{}}}"#,
        ),
        (
            "method_not_found",
            true,
            r#"{"jsonrpc":"2.0","id":4,"method":"future/method"}"#,
        ),
        ("parse_error", true, r#"{"jsonrpc":"2.0","#),
    ];
    let mut cases = Vec::with_capacity(requests.len());
    for (name, authorized, request_body) in requests {
        cases.push(
            perform_mcp_request(
                &client,
                &endpoint,
                &config.bearer_token,
                name,
                authorized,
                request_body,
            )
            .await,
        );
    }
    server.stop().await;

    ProtocolMcpTranscript {
        schema_version: 1,
        protocol: "mcp",
        endpoint: "/mcp",
        bearer_token: config.bearer_token,
        cases,
    }
}

/// 发出一条 MCP HTTP 请求并无损记录响应 body；除连接用随机端口外不做归一化。
async fn perform_mcp_request(
    client: &reqwest::Client,
    endpoint: &str,
    token: &str,
    name: &'static str,
    authorized: bool,
    request_body: &'static str,
) -> ProtocolMcpCase {
    let mut request = client
        .post(endpoint)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .body(request_body);
    if authorized {
        request = request.bearer_auth(token);
    }
    let response = request.send().await.expect("execute MCP probe request");
    let status = response.status().as_u16();
    let response_content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let response_body = response
        .bytes()
        .await
        .expect("read MCP probe response")
        .to_vec();
    ProtocolMcpCase {
        name,
        authorized,
        request_body,
        status,
        response_content_type,
        response_body: String::from_utf8(response_body).expect("MCP response is UTF-8"),
    }
}

/// 构造 HTTP 与 WebSocket Tunnel 消息，并调用生产 ProtoJSON 编码边界生成 wire。
///
/// 这里不能先转成紧凑 JSON 再修改空格；测试必须经过 `Client::send_response` 使用的
/// 同一个 `marshal_protojson`，才能同时固定 pbjson 字段顺序与 Go 的逗号后空格。
fn capture_tunnel_protojson_transcript() -> ProtocolProtoJsonTranscript {
    let cases = vec![
        (
            "http_request",
            protocol_protojson_wire(&TunnelRequest {
                session_id: "session-1".to_owned(),
                payload: Some(tunnel_request::Payload::HttpRequest(HttpTunnelRequest {
                    request_id: "request-1".to_owned(),
                    port: 8080,
                    method: "POST".to_owned(),
                    path: "/upload?mode=raw".to_owned(),
                    headers: vec![
                        Header {
                            name: "X-First".to_owned(),
                            value: "one".to_owned(),
                        },
                        Header {
                            name: "X-First".to_owned(),
                            value: "two".to_owned(),
                        },
                    ],
                    body: vec![0xfb, 0xff],
                })),
            }),
        ),
        (
            "http_cancel",
            protocol_protojson_wire(&TunnelRequest {
                session_id: "session-1".to_owned(),
                payload: Some(tunnel_request::Payload::HttpCancel(HttpTunnelCancel {
                    request_id: "request-1".to_owned(),
                })),
            }),
        ),
        (
            "ws_open",
            protocol_protojson_wire(&TunnelRequest {
                session_id: "session-1".to_owned(),
                payload: Some(tunnel_request::Payload::WsOpen(WsTunnelOpen {
                    tunnel_id: "tunnel-1".to_owned(),
                    port: 9090,
                    path: "/socket".to_owned(),
                    headers: vec![Header {
                        name: "Origin".to_owned(),
                        value: "https://example.test".to_owned(),
                    }],
                    protocols: vec!["chat.v2".to_owned(), "chat.v1".to_owned()],
                })),
            }),
        ),
        (
            "ws_binary_message",
            protocol_protojson_wire(&TunnelRequest {
                session_id: "session-1".to_owned(),
                payload: Some(tunnel_request::Payload::WsMessage(WsTunnelMessage {
                    tunnel_id: "tunnel-1".to_owned(),
                    data: vec![0x00, 0xfb, 0xff],
                    is_binary: true,
                })),
            }),
        ),
        (
            "ws_text_message_default_bool",
            protocol_protojson_wire(&TunnelRequest {
                session_id: "session-1".to_owned(),
                payload: Some(tunnel_request::Payload::WsMessage(WsTunnelMessage {
                    tunnel_id: "tunnel-1".to_owned(),
                    data: b"hello".to_vec(),
                    is_binary: false,
                })),
            }),
        ),
        (
            "ws_close",
            protocol_protojson_wire(&TunnelRequest {
                session_id: "session-1".to_owned(),
                payload: Some(tunnel_request::Payload::WsClose(WsTunnelClose {
                    tunnel_id: "tunnel-1".to_owned(),
                    code: 1000,
                    reason: "done".to_owned(),
                })),
            }),
        ),
        (
            "http_response_headers",
            protocol_protojson_wire(&TunnelResponse {
                payload: Some(tunnel_response::Payload::HttpHeaders(
                    HttpTunnelResponseHeaders {
                        request_id: "request-1".to_owned(),
                        status_code: 201,
                        status_text: "201 Created".to_owned(),
                        headers: vec![Header {
                            name: "Content-Type".to_owned(),
                            value: "application/json".to_owned(),
                        }],
                        sequence_number: 7,
                    },
                )),
            }),
        ),
        (
            "http_response_chunk",
            protocol_protojson_wire(&TunnelResponse {
                payload: Some(tunnel_response::Payload::HttpChunk(
                    HttpTunnelResponseChunk {
                        request_id: "request-1".to_owned(),
                        data: vec![0xfb, 0xff],
                        is_final: true,
                        sequence_number: 8,
                    },
                )),
            }),
        ),
        (
            "ws_response_error",
            protocol_protojson_wire(&TunnelResponse {
                payload: Some(tunnel_response::Payload::WsError(WsTunnelError {
                    tunnel_id: "tunnel-1".to_owned(),
                    error_message: "dial failed".to_owned(),
                })),
            }),
        ),
    ];
    ProtocolProtoJsonTranscript {
        schema_version: 1,
        protocol: "tunnel_protojson",
        cases: cases
            .into_iter()
            .map(|(name, wire)| ProtocolProtoJsonCase { name, wire })
            .collect(),
    }
}

/// 通过生产 `marshal_protojson` 生成 UTF-8 wire 文本。
///
/// # 参数
/// - `value`：带 pbjson `Serialize` 实现的 Tunnel 请求或响应。
///
/// # 返回
/// 返回将被真实 WebSocket text frame 发送的完整字节对应字符串；序列化或 UTF-8
/// 约束被破坏时立即使 probe 失败，不能回退到测试自己的编码器。
fn protocol_protojson_wire<T>(value: &T) -> String
where
    T: Serialize + ?Sized,
{
    String::from_utf8(marshal_protojson(value).expect("encode production Tunnel ProtoJSON"))
        .expect("Tunnel ProtoJSON 必须是 UTF-8")
}

/// 从 header map 中读取 UTF-8 值；缺失或非法值与 Go `Header.Get` 一样返回空串。
fn header_string(headers: &axum::http::HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

/// 把 `Result<(), E>` 转成 Go probe 使用的空串/错误文本表示。
fn result_error_string<E: std::fmt::Display>(result: Result<(), E>) -> String {
    result
        .err()
        .map_or_else(String::new, |error| error.to_string())
}

/// 等待 loopback server 在取消后退出；超时说明测试自身泄漏任务，应立即失败。
async fn await_server_shutdown(task: JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("API probe server shutdown timed out")
        .expect("API probe server task panicked");
}

/// 把结构化 transcript 以 UTF-8 JSON 和单个结尾换行写入门禁临时目录。
fn write_transcript<T: Serialize>(path: &Path, transcript: &T) {
    let mut encoded = serde_json::to_vec(transcript).expect("serialize Rust transcript");
    encoded.push(b'\n');
    std::fs::write(path, encoded).expect("write Rust transcript");
}
