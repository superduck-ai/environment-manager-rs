//! loopback Streamable HTTP 与 JSON-RPC 协议适配层。
//!
//! 这里实现 MCP wire 在本项目实际使用的协议面：`initialize`、`ping`、
//! `tools/list`、`tools/call` 和通知。业务服务器只看到稳定的本地类型；官方 `rmcp`
//! 协议值由本地强类型模型直接序列化。

use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::{ConnectInfo, State},
    http::{HeaderMap, Method, Request, StatusCode, header},
    response::Response,
    routing::any,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use rand::TryRngCore;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

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
    /// MCP wire 对通知使用 202，其余 JSON-RPC 响应使用 200。
    pub status: StatusCode,
    /// 通知没有 body；请求结果始终是 JSON object。
    pub body: Option<Value>,
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
                // MCP wire Streamable HTTP 在 JSON decoder 失败时同时使用 HTTP 400
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

    // MCP wire 把所有没有 ID 的消息当通知；未知通知也只返回 202。
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
                // AddTool 会隐式启用 listChanged=true，这是 既有 MCP wire 的默认值。
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
            if let Some(tool) = listed.iter().find(|tool| !tool.input_schema.is_object()) {
                Err((
                    -32603,
                    format!("tool '{}' inputSchema must be a JSON object", tool.name),
                ))
            } else {
                serde_json::to_value(listed)
                    .map(|listed| json!({"tools": listed}))
                    .map_err(|error| (-32603, format!("serialize MCP tools: {error}")))
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
                Ok(result) => serde_json::to_value(result)
                    .map_err(|error| (-32603, format!("serialize MCP tool result: {error}"))),
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

/// 创建字段集合和序列化顺序均与 MCP wire 一致的 JSON-RPC error。
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
    ConnectInfo(remote_address): ConnectInfo<SocketAddr>,
    request: Request<Body>,
) -> Response<Body> {
    let started = std::time::Instant::now();
    let method = request.method().as_str().to_owned();
    let path = request.uri().path().to_owned();
    let remote_address = remote_address.to_string();
    let user_agent = request
        .headers()
        .get(header::USER_AGENT)
        // 非法 UTF-8 以替换字符记录。不能使用 `to_str` 后回退为空串，
        // 否则诊断值
        // 会在包含 obs-text 的合法 HTTP/1 请求上与 不同。
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
        return text_response(StatusCode::UNAUTHORIZED, "Unauthorized");
    }

    tracing::debug!("server_name" = ?(&state.name), "method" = ?(&method), "path" = ?(&path), "{}", "Authorized MCP request");

    let response = if request.uri().path() != MCP_ENDPOINT_PATH {
        text_response(StatusCode::NOT_FOUND, "404 page not found")
    } else {
        match *request.method() {
            Method::POST => handle_post(Arc::clone(&state), request).await,
            Method::DELETE => Response::builder()
                .status(StatusCode::OK)
                .body(Body::empty())
                .expect("static status is valid"),
            // MCP wire 的 GET notification stream 在 client 断开前保持连接；这里用 pending
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
            _ => text_response(StatusCode::NOT_FOUND, "404 page not found"),
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
            "Invalid content type: must be 'application/json'",
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

/// 构造 HTTP 纯文本响应。
fn text_response(status: StatusCode, body: &'static str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(body))
        .expect("static response is valid")
}

/// 序列化 JSON response。
fn json_response(status: StatusCode, value: &Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec()),
        ))
        .expect("static response is valid")
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

    /// 生成 token、监听 loopback 随机端口并启动 HTTP server。
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
        // 不变量被平台破坏，仍返回原有启动错误，不额外伪造失败日志。
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
        let server_task = tokio::spawn(async move {
            tracing::info!("address" = ?(&server_address), "server_name" = ?(&server_name), "{}", "MCP HTTP server starting to serve");
            let result = axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(serve_shutdown.cancelled_owned())
            .await;
            match result {
                Ok(()) => {
                    tracing::info!("server_name" = ?(&server_name), "{}", "MCP HTTP server stopped cleanly")
                }
                Err(error) => {
                    tracing::error!("error" = ?(error.to_string()), "server_name" = ?(&server_name), "{}", "MCP HTTP server error")
                }
            }
        });

        let old_runtime = self.runtime.lock().await.replace(ServerRuntime {
            server_cancellation,
            server_task,
        });
        if let Some(old_runtime) = old_runtime {
            old_runtime.server_cancellation.cancel();
            old_runtime.server_task.abort();
        }

        tracing::info!("server_name" = ?(&self.name), "port" = ?(port), "version" = ?(&self.version), "{}", "MCP server started");

        Ok(ServerConfig { port, bearer_token })
    }

    /// 在五秒窗口内优雅停止，超时后终止剩余 Tokio task。
    async fn stop(&self) {
        let runtime = self.runtime.lock().await.take();
        if let Some(runtime) = runtime {
            runtime.server_cancellation.cancel();
            let mut server_task = runtime.server_task;
            if tokio::time::timeout(Duration::from_secs(5), &mut server_task)
                .await
                .is_err()
            {
                tracing::warn!("server_name" = ?(&self.name), "error" = ?("context deadline exceeded"), "{}", "Failed to stop MCP server");
                server_task.abort();
            }
        }
        tracing::info!("server_name" = ?(&self.name), "{}", "MCP server stopped");
    }

    /// 普通基础 server 默认需要注册到 Claude。
    fn should_register_with_claude(&self) -> bool {
        true
    }
}

/// 让专用 server 不经 trait object 也能调用 风格方法。
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
