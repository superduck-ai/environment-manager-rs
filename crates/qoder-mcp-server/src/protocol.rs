//! 保留参考服务 JSON-RPC 及 stdio/HTTP 传输行为。
use crate::{plugins::Manager, tools::Registry};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::State,
    http::{Request, StatusCode},
    response::Response,
    routing::any,
};
use serde_json::{Value, json};
use std::{io, sync::Arc};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};
use tokio_util::sync::CancellationToken;

/// 服务初始化时公开的名称和版本。
#[derive(Clone, Debug)]
pub struct Config {
    pub name: String,
    pub version: String,
}
impl Default for Config {
    /// 使用参考服务的默认标识。
    fn default() -> Self {
        Self {
            name: "sandbox-runtime-tools".into(),
            version: "0.1.0".into(),
        }
    }
}
/// 兼容服务，注册表优先于插件工具。
pub struct Server {
    pub registry: Arc<Registry>,
    pub plugins: Arc<Manager>,
    pub config: Config,
}
impl Server {
    /// 构造服务，并补齐空配置字段。
    pub fn new(registry: Arc<Registry>, plugins: Arc<Manager>, mut config: Config) -> Self {
        let default = Config::default();
        if config.name.is_empty() {
            config.name = default.name;
        }
        if config.version.is_empty() {
            config.version = default.version;
        }
        Self {
            registry,
            plugins,
            config,
        }
    }
    /// 分派单条消息；缺失或 null ID 的有效消息作为通知处理。
    pub async fn dispatch(&self, body: &[u8], cancellation: CancellationToken) -> Option<Value> {
        let request = match crate::json::request(body) {
            Ok(request) => request,
            Err(()) => return Some(error(Value::Null, -32700, "Parse error")),
        };
        let id = request.id;
        if request.jsonrpc != "2.0" {
            return Some(error(id, -32600, "Invalid Request"));
        }
        if id.is_null() {
            return None;
        }
        let method = request.method.as_str();
        let result = match method {
            "initialize" => {
                json!({"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":self.config.name,"version":self.config.version}})
            }
            "ping" => json!({}),
            "tools/list" => {
                let definitions: Vec<_> = self
                    .registry
                    .list()
                    .iter()
                    .chain(self.plugins.list_tools().iter())
                    .map(|t| t.definition())
                    .collect();
                json!({"tools":if definitions.is_empty() { Value::Null } else { json!(definitions) }})
            }
            "tools/call" => {
                let p = request.params.as_ref();
                if p.is_none() {
                    return Some(error(
                        id,
                        -32602,
                        "Invalid params: unexpected end of JSON input",
                    ));
                }
                let p = p.unwrap();
                if let Some(message) = request.params_error {
                    return Some(error(id, -32602, &format!("Invalid params: {message}")));
                }
                let name = p["name"].as_str().unwrap_or("");
                let Some(tool) = self
                    .registry
                    .get(name)
                    .or_else(|| self.plugins.get_tool(name))
                else {
                    return Some(error(id, -32602, &format!("Tool not found: {name}")));
                };
                let arguments = p.get("arguments").cloned().unwrap_or(Value::Null);
                let result = match tool.validate(&arguments) {
                    Err(e) => crate::ToolResult::error(format!("Validation failed: {e}")),
                    Ok(()) => tool
                        .handle(cancellation, arguments)
                        .await
                        .unwrap_or_else(|e| {
                            crate::ToolResult::error(format!("Execution failed: {e}"))
                        }),
                };
                serde_json::to_value(result).expect("serializable tool result")
            }
            _ => return Some(error(id, -32601, &format!("Method not found: {method}"))),
        };
        Some(json!({"jsonrpc":"2.0","id":id,"result":result}))
    }
    /// 逐行处理 stdio；EOF 前无换行的最后一段与参考服务一样丢弃。
    pub async fn serve_stdio(&self) -> io::Result<()> {
        crate::logging::info("MCP server started", &[("transport", "stdio")]);
        let mut input = BufReader::new(tokio::io::stdin());
        let mut output = tokio::io::stdout();
        let mut line = Vec::new();
        loop {
            line.clear();
            let n = input.read_until(b'\n', &mut line).await.map_err(|e| {
                io::Error::other(format!(
                    "read stdin: read /dev/stdin: {}",
                    crate::util::errno(&e)
                ))
            })?;
            if n == 0 || line.last() != Some(&b'\n') {
                return Ok(());
            }
            if let Some(response) = self.dispatch(&line, CancellationToken::new()).await {
                let mut bytes = encode(&response);
                bytes.push(b'\n');
                output.write_all(&bytes).await?;
                output.flush().await?;
            }
        }
    }
    /// 构造无认证的兼容 HTTP 路由，保留通知和 DELETE 状态码。
    pub fn router(self: Arc<Self>) -> Router {
        Router::new().fallback(any(http_handler)).with_state(self)
    }
    /// 监听指定地址，`:端口` 使用全部网卡。
    pub async fn serve_http(self: Arc<Self>, address: &str) -> io::Result<()> {
        crate::logging::info(
            "MCP server started",
            &[("transport", "http"), ("addr", address)],
        );
        if address.matches(':').count() > 1 && !address.starts_with('[') {
            return Err(io::Error::other(format!(
                "listen tcp: address {address}: too many colons in address"
            )));
        }
        if let Some((_, port)) = address.rsplit_once(':')
            && port.starts_with('-')
        {
            return Err(io::Error::other(format!(
                "listen tcp: address {port}: invalid port"
            )));
        }
        let address = if address.starts_with(':') {
            format!("[::]{address}")
        } else {
            address.into()
        };
        let listener = TcpListener::bind(address).await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let server = self.clone();
            let stream = stream.into_std()?;
            let monitor = stream.try_clone()?;
            let stream = tokio::net::TcpStream::from_std(stream)?;
            tokio::spawn(async move {
                let closed = CancellationToken::new();
                let monitor_token = closed.clone();
                let monitor_task = tokio::spawn(async move {
                    let mut byte = [0; 1];
                    loop {
                        if monitor_token.is_cancelled() {
                            break;
                        }
                        match monitor.peek(&mut byte) {
                            Ok(0) => {
                                monitor_token.cancel();
                                break;
                            }
                            Err(e) if e.kind() != io::ErrorKind::WouldBlock => {
                                monitor_token.cancel();
                                break;
                            }
                            _ => {}
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                });
                let request_token = closed.clone();
                let service =
                    hyper::service::service_fn(move |request: Request<hyper::body::Incoming>| {
                        let server = server.clone();
                        let token = request_token.child_token();
                        async move {
                            let _guard = token.clone().drop_guard();
                            let response = tokio::spawn(http_with_cancellation(
                                server,
                                request.map(Body::new),
                                token,
                            ))
                            .await
                            .expect("HTTP request task");
                            Ok::<_, std::convert::Infallible>(response)
                        }
                    });
                let _ = hyper::server::conn::http1::Builder::new()
                    .title_case_headers(true)
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                    .await;
                closed.cancel();
                monitor_task.abort();
            });
        }
    }
}
/// 为协议响应省略空 ID，包含 false、0 和空字符串 ID。
fn error(id: Value, code: i64, message: &str) -> Value {
    let mut v = json!({"jsonrpc":"2.0","error":{"code":code,"message":message}});
    if !id.is_null() {
        v["id"] = id;
    }
    v
}
/// 保留 JSON 字符串中 HTML 字符及行分隔符的转义。
pub fn encode(value: &Value) -> Vec<u8> {
    crate::wire_json::encode(value)
}
/// 返回兼容 HTTP 文本错误。
fn text_response(status: StatusCode, text: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .header("x-content-type-options", "nosniff")
        .header("content-length", text.len())
        .body(Body::from(text.to_owned()))
        .expect("static response")
}
/// 请求被丢弃时将取消传给子进程和搜索任务。
async fn http_handler(State(server): State<Arc<Server>>, request: Request<Body>) -> Response<Body> {
    http_with_cancellation(server, request, CancellationToken::new()).await
}
/// 将连接断开信号传入工具执行，同时保留 HTTP 响应格式。
async fn http_with_cancellation(
    server: Arc<Server>,
    request: Request<Body>,
    cancellation: CancellationToken,
) -> Response<Body> {
    let path = request.uri().path();
    let mut cleaned = crate::util::clean(path).to_string_lossy().into_owned();
    if path.ends_with('/') && cleaned != "/" {
        cleaned.push('/');
    }
    if cleaned != path {
        if let Some(query) = request.uri().query() {
            cleaned.push('?');
            cleaned.push_str(query);
        }
        let method = request.method().as_str();
        let body = if method == "GET" {
            format!(
                "<a href=\"{}\">Temporary Redirect</a>.\n\n",
                cleaned
                    .replace('&', "&amp;")
                    .replace('<', "&lt;")
                    .replace('>', "&gt;")
                    .replace('\"', "&#34;")
            )
        } else {
            String::new()
        };
        let mut response = Response::builder().status(307).header("location", &cleaned);
        if matches!(method, "GET" | "HEAD") {
            response = response.header("content-type", "text/html; charset=utf-8");
        }
        let body = if method == "HEAD" {
            Body::from_stream(tokio_stream::empty::<
                Result<Vec<u8>, std::convert::Infallible>,
            >())
        } else {
            Body::from(body)
        };
        return drain_response(request, response.body(body).unwrap()).await;
    }
    if path != "/mcp" {
        return drain_response(
            request,
            text_response(StatusCode::NOT_FOUND, "404 page not found\n"),
        )
        .await;
    }
    match request.method().as_str() {
        "DELETE" => {
            return drain_response(
                request,
                Response::builder().status(204).body(Body::empty()).unwrap(),
            )
            .await;
        }
        "POST" => {}
        _ => {
            return drain_response(
                request,
                text_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n"),
            )
            .await;
        }
    }
    let body = match to_bytes(request.into_body(), usize::MAX).await {
        Ok(b) => b,
        Err(_) => return text_response(StatusCode::BAD_REQUEST, "bad request\n"),
    };
    let _guard = cancellation.clone().drop_guard();
    match server.dispatch(&body, cancellation).await {
        None => Response::builder().status(202).body(Body::empty()).unwrap(),
        Some(response) => {
            let mut bytes = encode(&response);
            bytes.push(b'\n');
            Response::builder()
                .header("content-type", "application/json")
                .body(if bytes.len() > 2048 {
                    Body::from_stream(tokio_stream::once(Ok::<_, std::convert::Infallible>(bytes)))
                } else {
                    Body::from(bytes)
                })
                .unwrap()
        }
    }
}

/// 消耗无需业务处理的小请求体，保留参考 HTTP 服务的连接复用行为。
async fn drain_response(request: Request<Body>, response: Response<Body>) -> Response<Body> {
    let _ = to_bytes(request.into_body(), 256 << 10).await;
    response
}
