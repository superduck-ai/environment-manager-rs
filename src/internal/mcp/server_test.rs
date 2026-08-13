//! loopback HTTP、认证和 accessor 测试。

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{
    BaseServer, CallToolResult, MCPServer, McpError, Tool, ToolConfig, ToolHandler,
    new_tool_result_text,
};

/// 保留 HTTP 状态和原始响应文本。
struct McpHttpResponse {
    status: reqwest::StatusCode,
    body: String,
}

/// 按需附加 Bearer token 并返回未解析正文。
async fn post_mcp_json(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    body: &str,
) -> McpHttpResponse {
    let mut request = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body.to_owned());
    if !token.is_empty() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await.expect("发送 MCP 请求");
    let status = response.status();
    let body = response.text().await.expect("读取 MCP 响应");
    McpHttpResponse { status, body }
}

/// 从同一个原始 TCP stream 读取一条带 Content-Length 的 HTTP/1 响应。
///
/// MCP 的 JSON 和错误响应均由 Hyper 写出明确长度；测试辅助函数只实现这一受控
/// 子集，以便断言第二个请求确实复用了同一 socket，而不会引入另一个 HTTP client。
/// 调用方必须等本函数返回后才发送下一请求，因此一次 read 不会夹带第二条响应；若
/// 将来改成真正的 response pipeline，必须把多读字节保存在跨调用缓冲区中。
async fn read_raw_http_response(stream: &mut tokio::net::TcpStream) -> (u16, Vec<u8>) {
    let mut received = Vec::new();
    let mut scratch = [0_u8; 1024];
    let header_end = loop {
        if let Some(position) = received.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        let read =
            tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut scratch))
                .await
                .expect("等待原始 MCP HTTP header 不应超时")
                .expect("读取原始 MCP HTTP header");
        assert_ne!(read, 0, "响应 header 完成前连接不应关闭");
        received.extend_from_slice(&scratch[..read]);
    };

    let header = std::str::from_utf8(&received[..header_end]).expect("HTTP header 必须是 ASCII");
    let status = header
        .lines()
        .next()
        .and_then(|line| line.split_ascii_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .expect("响应必须含有效状态码");
    let content_length = header
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .expect("受控 MCP 响应必须含 Content-Length");

    while received.len() < header_end + content_length {
        let read =
            tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut scratch))
                .await
                .expect("等待原始 MCP HTTP body 不应超时")
                .expect("读取原始 MCP HTTP body");
        assert_ne!(read, 0, "响应 body 完成前连接不应关闭");
        received.extend_from_slice(&scratch[..read]);
    }
    assert_eq!(
        received.len(),
        header_end + content_length,
        "顺序发送约束下不应预读下一条响应"
    );
    (
        status,
        received[header_end..header_end + content_length].to_vec(),
    )
}

#[tokio::test]
async fn test_response_writer_tracks_first_status() {
    let base = Arc::new(BaseServer::new("status", "0.0.1"));
    let config = base.start(Vec::new()).await.expect("server 启动");
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{}/mcp", config.port);

    // Unauthorized 分支先写 401 再写正文；正文写入不能把已记录状态覆盖成 200。
    let response = post_mcp_json(
        &client,
        &url,
        "",
        r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#,
    )
    .await;
    assert_eq!(response.status, reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(response.body, "Unauthorized");
    base.stop().await;
}

#[tokio::test]
async fn test_tool_errors_use_json_rpc_response() {
    let failing = ToolConfig {
        tool: Tool::new("fail", "fail", json!({"type": "object"})),
        handler: ToolHandler::new(|_, _| async { Err(McpError::message("write failed")) }),
    };
    let base = Arc::new(BaseServer::new("writer", "0.0.1"));
    let config = base.start(vec![failing]).await.expect("server 启动");
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{}/mcp", config.port);

    // JSON-RPC 成功发送路径没有显式业务状态码，必须保持隐式 200，并把 handler
    // 错误包装在 JSON-RPC error 中而不是升级成 HTTP 失败。
    let response = post_mcp_json(
        &client,
        &url,
        &config.bearer_token,
        r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"fail","arguments":{}}}"#,
    )
    .await;
    assert_eq!(response.status, reqwest::StatusCode::OK);
    assert!(response.body.contains("write failed"));
    assert!(!response.body.ends_with('\n'));
    base.stop().await;
}

#[tokio::test]
async fn test_base_server_start_serves_authenticated_mcp() {
    let called = Arc::new(AtomicBool::new(false));
    let called_by_handler = Arc::clone(&called);
    let tool = ToolConfig {
        tool: Tool::new(
            "ping",
            "A test tool",
            json!({"type": "object", "properties": {}}),
        ),
        handler: ToolHandler::new(move |_, _| {
            let called = Arc::clone(&called_by_handler);
            async move {
                called.store(true, Ordering::SeqCst);
                Ok(new_tool_result_text("pong"))
            }
        }),
    };
    let base = Arc::new(BaseServer::new("demo", "1.2.3"));
    let config = base.start(vec![tool]).await.expect("server 启动");
    assert_ne!(config.port, 0);
    assert_eq!(base.port(), config.port);
    assert!(!config.bearer_token.is_empty());
    assert_eq!(base.bearer_token(), config.bearer_token);

    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{}/mcp", config.port);
    let unauthorized = post_mcp_json(
        &client,
        &url,
        "",
        r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#,
    )
    .await;
    assert_eq!(unauthorized.status, reqwest::StatusCode::UNAUTHORIZED);

    let listed = post_mcp_json(
        &client,
        &url,
        &config.bearer_token,
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
    )
    .await;
    assert_eq!(listed.status, reqwest::StatusCode::OK);
    assert!(listed.body.contains(r#""name":"ping""#));

    let call = post_mcp_json(
        &client,
        &url,
        &config.bearer_token,
        r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"ping","arguments":{}}}"#,
    )
    .await;
    assert_eq!(call.status, reqwest::StatusCode::OK);
    assert!(call.body.contains("pong"));
    assert!(called.load(Ordering::SeqCst));

    let malformed = post_mcp_json(&client, &url, &config.bearer_token, "{").await;
    assert_eq!(malformed.status, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_str::<Value>(&malformed.body).unwrap()["error"]["code"],
        json!(-32700)
    );

    // Stop 必须幂等，重复调用不能 panic 或等待五秒超时。
    base.stop().await;
    base.stop().await;
}

/// 使用一个原始 loopback socket 顺序发送两个 POST，验证首响应后连接仍可复用，且
/// 第二条请求 header 分片不会沿用首请求已经过期的 header deadline 状态。
#[tokio::test]
async fn test_base_server_serves_two_posts_on_same_tcp_connection() {
    let base = Arc::new(BaseServer::new("keep-alive", "0.0.1"));
    let config = base.start(Vec::new()).await.expect("server 启动");
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", config.port))
        .await
        .expect("连接 MCP loopback server");

    let first_body = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
    let first_request = format!(
        "POST /mcp HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
        config.bearer_token,
        first_body.len(),
        first_body
    );
    stream
        .write_all(first_request.as_bytes())
        .await
        .expect("发送首个 MCP POST");
    let (first_status, first_response) = read_raw_http_response(&mut stream).await;
    assert_eq!(first_status, 200);
    assert_eq!(
        serde_json::from_slice::<Value>(&first_response).expect("解析首响应")["id"],
        json!(1)
    );

    let second_body = r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#;
    let second_request_tail = format!(
        "ST /mcp HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        config.bearer_token,
        second_body.len(),
        second_body
    );
    // 先只发送两字节，覆盖 idle Peek(4) 仍使用 120 秒期限的分支；再补足 `POST`
    // 和其余 header，触发第二条请求独立的十秒 header deadline。
    stream.write_all(b"PO").await.expect("发送第二请求前缀");
    tokio::task::yield_now().await;
    stream
        .write_all(second_request_tail.as_bytes())
        .await
        .expect("发送第二个 MCP POST");
    let (second_status, second_response) = read_raw_http_response(&mut stream).await;
    assert_eq!(second_status, 200);
    assert_eq!(
        serde_json::from_slice::<Value>(&second_response).expect("解析第二响应")["id"],
        json!(2)
    );

    base.stop().await;
}

#[tokio::test]
async fn test_base_server_start_honors_debug_bearer_token() {
    let mut base = BaseServer::new("debug", "test");
    base.set_debug_hardcoded_token(true);
    base.set_bearer_token("fixed-token");
    let base = Arc::new(base);

    let config = base.start(Vec::new()).await.expect("server 启动");
    assert_eq!(config.bearer_token, "fixed-token");
    assert_eq!(base.bearer_token(), "fixed-token");
    base.stop().await;
}

/// server stop 必须通过每请求 child token 取消正在执行的 tool。
#[tokio::test]
async fn test_base_server_stop_cancels_active_tool_request() {
    let started = Arc::new(tokio::sync::Notify::new());
    let cancelled = Arc::new(AtomicBool::new(false));
    let handler_started = Arc::clone(&started);
    let handler_cancelled = Arc::clone(&cancelled);
    let tool = ToolConfig {
        tool: Tool::new("wait", "wait", json!({"type": "object"})),
        handler: ToolHandler::new(move |cancellation, _| {
            let started = Arc::clone(&handler_started);
            let cancelled = Arc::clone(&handler_cancelled);
            async move {
                started.notify_one();
                cancellation.cancelled().await;
                cancelled.store(true, Ordering::SeqCst);
                Ok(new_tool_result_text("cancelled"))
            }
        }),
    };
    let base = Arc::new(BaseServer::new("cancel", "0.0.1"));
    let config = base.start(vec![tool]).await.expect("server 启动");
    let url = format!("http://127.0.0.1:{}/mcp", config.port);
    let token = config.bearer_token.clone();
    let request = tokio::spawn(async move {
        post_mcp_json(
            &reqwest::Client::new(),
            &url,
            &token,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"wait","arguments":{}}}"#,
        )
        .await
    });

    tokio::time::timeout(std::time::Duration::from_secs(2), started.notified())
        .await
        .expect("tool handler 必须开始执行");
    base.stop().await;
    assert!(cancelled.load(Ordering::SeqCst));
    let _ = request.await;
}

#[test]
fn test_base_server_accessors() {
    let server = Arc::new(BaseServer::new("demo", "0.0.1"));
    assert_eq!(server.get_name(), "demo");
    assert!(Arc::clone(&server).get_tools().is_empty());
    assert!(server.should_register_with_claude());
}

/// 覆盖未知字段保留与工具排序的协议行为。
#[tokio::test]
async fn dispatch_preserves_ids_sorts_tools_and_calls_handler() {
    use tokio_util::sync::CancellationToken;

    use super::dispatch_json_rpc;

    let ping = ToolConfig {
        tool: Tool::new("ping", "ping", json!({"type": "object", "properties": {}})),
        handler: ToolHandler::new(|_, request| async move {
            assert_eq!(request.extra.get("futureField"), Some(&json!(true)));
            Ok(new_tool_result_text("pong"))
        }),
    };
    let alpha = ToolConfig {
        tool: Tool::new("alpha", "alpha", json!({"type": "object"})),
        handler: ToolHandler::new(|_, _| async move { Ok(CallToolResult::default()) }),
    };
    let listed = dispatch_json_rpc(
        "test",
        "0.0.1",
        &[ping.clone(), alpha],
        CancellationToken::new(),
        br#"{"jsonrpc":"2.0","id":"abc","method":"tools/list","unknown":1}"#,
    )
    .await;
    let listed: Value = listed.body.expect("响应正文");
    assert_eq!(listed["id"], json!("abc"));
    assert_eq!(listed["result"]["tools"][0]["name"], json!("alpha"));

    let called = dispatch_json_rpc(
        "test",
        "0.0.1",
        &[ping],
        CancellationToken::new(),
        br#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"ping","arguments":{}},"futureField":true}"#,
    )
    .await;
    assert_eq!(
        called.body.expect("响应正文")["result"]["content"][0]["text"],
        json!("pong")
    );
}
