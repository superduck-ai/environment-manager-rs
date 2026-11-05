#![allow(dead_code)]
use mcp_server::Server;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, ReadHalf, WriteHalf};
pub fn request(id: Value, method: &str, mut params: Value) -> Value {
    if !params.is_object() {
        params = json!({});
    }
    params["_meta"] = mcp_server::protocol::request_meta();
    json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
}
pub struct Connection {
    pub reader: BufReader<ReadHalf<DuplexStream>>,
    pub writer: WriteHalf<DuplexStream>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Connection {
    pub async fn new(server: Arc<Server>) -> Self {
        let (client, transport) = tokio::io::duplex(1 << 20);
        let (input, output) = tokio::io::split(transport);
        let task = tokio::spawn(server.serve_stream(input, output));
        let (reader, writer) = tokio::io::split(client);
        Self {
            reader: BufReader::new(reader),
            writer,
            task,
        }
    }
    pub async fn send(&mut self, value: &Value) {
        let mut line = serde_json::to_vec(value).unwrap();
        line.push(b'\n');
        self.writer.write_all(&line).await.unwrap();
        self.writer.flush().await.unwrap();
    }
    pub async fn receive(&mut self) -> Value {
        let mut line = String::new();
        let n = tokio::time::timeout(Duration::from_secs(5), self.reader.read_line(&mut line))
            .await
            .expect("response timeout")
            .unwrap();
        assert_ne!(n, 0, "SDK closed before responding");
        serde_json::from_str(&line).unwrap()
    }
}
pub async fn rpc(server: Arc<Server>, mut message: Value) -> Value {
    if !message["params"].is_object() {
        message["params"] = json!({});
    }
    message["params"]["_meta"] = mcp_server::protocol::request_meta();
    let mut connection = Connection::new(server).await;
    connection.send(&message).await;
    connection.receive().await
}
pub struct HttpServer {
    pub url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for HttpServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub async fn http(server: Arc<Server>) -> HttpServer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, server.router()).await.unwrap();
    });
    HttpServer { url, task }
}
pub fn post(url: &str, message: &Value) -> reqwest::RequestBuilder {
    let mut builder = reqwest::Client::new()
        .post(url)
        .timeout(Duration::from_secs(8))
        .header("accept", "application/json, text/event-stream")
        .header(
            "mcp-protocol-version",
            message
                .pointer("/params/_meta/io.modelcontextprotocol~1protocolVersion")
                .and_then(Value::as_str)
                .unwrap_or("2026-07-28"),
        )
        .header("mcp-method", message["method"].as_str().unwrap());
    if let Some(name) = message.pointer("/params/name").and_then(Value::as_str) {
        builder = builder.header("mcp-name", name);
    }
    builder.json(message)
}
pub async fn http_result(response: reqwest::Response) -> Value {
    assert!(response.status().is_success(), "HTTP {}", response.status());
    if response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("application/json")
    {
        return response.json().await.unwrap();
    }
    let text = response.text().await.unwrap();
    for line in text.lines() {
        if let Some(data) = line.strip_prefix("data: ")
            && let Ok(value) = serde_json::from_str::<Value>(data)
            && value.get("id").is_some()
        {
            return value;
        }
    }
    panic!("missing SSE result: {text}")
}
