//! Wire-level tests of the official SDK configuration and our tool adapter.
mod common;
use common::{Connection, http, http_result, post, request};
use mcp_server::{Config, Registry, Server, Tool, ToolResult, plugins::Manager};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
fn server(path: &std::path::Path) -> Arc<Server> {
    Arc::new(Server::new(
        Arc::new(Registry::builtin(path.into(), None)),
        Arc::new(Manager::default()),
        Config::default(),
    ))
}
async fn wait_file(path: &std::path::Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("command side effect");
}
fn call(id: i32, command: &str) -> Value {
    request(
        json!(id),
        "tools/call",
        json!({"name":"bash","arguments":{"command":command}}),
    )
}
#[tokio::test]
async fn discover_advertises_only_modern_protocol_without_initialize() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    let mut client = Connection::new(s).await;
    client
        .send(&request(json!(1), "server/discover", json!({})))
        .await;
    let response = client.receive().await;
    assert_eq!(response["result"]["resultType"], "complete");
    assert_eq!(
        response["result"]["supportedVersions"],
        json!(["2026-07-28"])
    );
    assert!(response["result"]["capabilities"]["tools"].is_object());
    assert!(response["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"].is_string());
    client.send(&call(2, "printf modern")).await;
    let response = client.receive().await;
    assert_eq!(response["result"]["resultType"], "complete");
    assert_eq!(response["result"]["structuredContent"]["stdout"], "modern");
    client.send(&request(json!(3), "ping", json!({}))).await;
    assert_eq!(client.receive().await["error"]["code"], -32601);
}
#[tokio::test]
async fn http_rejects_missing_metadata_old_versions_and_legacy_handshake() {
    let dir = tempfile::tempdir().unwrap();
    let h = http(server(dir.path())).await;
    let mut message = request(json!(1), "server/discover", json!({}));
    message["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"] = json!("2025-11-25");
    let response = post(&h.url, &message).send().await.unwrap();
    assert_eq!(response.status(), 400);
    let response: Value = response.json().await.unwrap();
    assert_eq!(response["error"]["code"], -32022);
    assert_eq!(
        response["error"]["data"]["supported"],
        json!(["2026-07-28"])
    );
    let message = request(
        json!(2),
        "initialize",
        json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"old","version":"1"}}),
    );
    let response = post(&h.url, &message).send().await.unwrap();
    assert!(!response.status().is_success());
    let message = json!({"jsonrpc":"2.0","id":3,"method":"tools/list"});
    let response = post(&h.url, &message).send().await.unwrap();
    assert_eq!(response.status(), 400);
    let mut message = request(json!(4), "tools/list", json!({}));
    message["params"]["_meta"]
        .as_object_mut()
        .unwrap()
        .remove("io.modelcontextprotocol/clientCapabilities");
    let response = post(&h.url, &message).send().await.unwrap();
    assert_eq!(response.status(), 400);
    assert_eq!(
        response.json::<Value>().await.unwrap()["error"]["code"],
        -32602
    );
}
#[tokio::test]
async fn http_requires_consistent_routing_headers_and_rejects_get_delete() {
    let dir = tempfile::tempdir().unwrap();
    let h = http(server(dir.path())).await;
    let message = call(1, "touch forbidden");
    for (name, value) in [
        ("mcp-method", "tools/list"),
        ("mcp-name", "read"),
        ("mcp-protocol-version", "2025-11-25"),
    ] {
        let mut wire = post(&h.url, &message).build().unwrap();
        wire.headers_mut().insert(name, value.parse().unwrap());
        let response = reqwest::Client::new().execute(wire).await.unwrap();
        assert_eq!(response.status(), 400, "{name}");
        assert_eq!(
            response.json::<Value>().await.unwrap()["error"]["code"],
            -32020,
            "{name}"
        );
    }
    assert!(!dir.path().join("forbidden").exists());
    for method in [reqwest::Method::GET, reqwest::Method::DELETE] {
        let response = reqwest::Client::new()
            .request(method, &h.url)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 405);
    }
    let response = post(&h.url, &request(json!(2), "server/discover", json!({})))
        .header("origin", "https://evil.example")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
}
#[tokio::test]
async fn stdio_processes_cancellation_concurrently_and_can_reuse_the_id() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = Connection::new(server(dir.path())).await;
    client
        .send(&request(json!(1), "server/discover", json!({})))
        .await;
    client.receive().await;
    client
        .send(&call(
            7,
            "trap 'touch stopped; exit 0' TERM; touch ready; sleep 30",
        ))
        .await;
    wait_file(&dir.path().join("ready")).await;
    client
        .send(&request(json!(8), "tools/list", json!({})))
        .await;
    assert_eq!(client.receive().await["id"], 8);
    client
        .send(&json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":7}}))
        .await;
    wait_file(&dir.path().join("stopped")).await;
    client.send(&call(7, "printf resumed")).await;
    let response = client.receive().await;
    assert_eq!(response["id"], 7);
    assert_eq!(response["result"]["structuredContent"]["stdout"], "resumed");
}
#[tokio::test]
async fn http_stream_disconnect_cancels_only_its_request() {
    let dir = tempfile::tempdir().unwrap();
    let h = http(server(dir.path())).await;
    let streaming_call = |command: &str| {
        let mut message = call(7, command);
        message["params"]["_meta"]["progressToken"] = json!(7);
        message
    };
    let a = post(
        &h.url,
        &streaming_call("trap 'touch stopped_a; exit 0' TERM; touch ready_a; sleep 30"),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(a.status(), 200);
    assert!(
        a.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );
    assert!(!a.headers().contains_key("mcp-session-id"));
    wait_file(&dir.path().join("ready_a")).await;
    let b = post(
        &h.url,
        &streaming_call("trap 'touch stopped_b; exit 0' TERM; touch ready_b; sleep 30"),
    )
    .send()
    .await
    .unwrap();
    wait_file(&dir.path().join("ready_b")).await;
    drop(a);
    wait_file(&dir.path().join("stopped_a")).await;
    assert!(!dir.path().join("stopped_b").exists());
    drop(b);
    wait_file(&dir.path().join("stopped_b")).await;
    let response = post(&h.url, &call(7, "printf resumed"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        http_result(response).await["result"]["structuredContent"]["stdout"],
        "resumed"
    );
}
struct HeaderTool(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl Tool for HeaderTool {
    fn definition(&self) -> Value {
        json!({"name":"header_tool","inputSchema":{"type":"object","properties":{"region":{"type":"string","x-mcp-header":"Region"}},"required":["region"]}})
    }
    async fn handle(&self, _: CancellationToken, args: Value) -> Result<ToolResult, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolResult::text(args["region"].as_str().unwrap()))
    }
}
#[tokio::test]
async fn sdk_validates_mirrored_custom_headers_and_decodes_base64() {
    let registry = Arc::new(Registry::default());
    let count = Arc::new(AtomicUsize::new(0));
    registry
        .register(Arc::new(HeaderTool(count.clone())))
        .unwrap();
    let s = Arc::new(Server::new(
        registry,
        Arc::new(Manager::default()),
        Config::default(),
    ));
    let h = http(s).await;
    let message = request(
        json!(1),
        "tools/call",
        json!({"name":"header_tool","arguments":{"region":"上海"}}),
    );
    // The SDK learns custom parameter routing annotations from list_tools.
    let list = post(&h.url, &request(json!(2), "tools/list", json!({})))
        .send()
        .await
        .unwrap();
    http_result(list).await;
    let response = post(&h.url, &message)
        .header("mcp-param-region", "wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let response = post(&h.url, &message)
        .header("mcp-param-region", "=?base64?5LiK5rW3?=")
        .send()
        .await
        .unwrap();
    assert_eq!(
        http_result(response).await["result"]["content"][0]["text"],
        "上海"
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn http_disconnect_before_first_response_also_stops_the_process() {
    let dir = tempfile::tempdir().unwrap();
    let h = http(server(dir.path())).await;
    let url = h.url.clone();
    let pending = tokio::spawn(async move {
        post(
            &url,
            &call(
                9,
                "trap 'touch stopped_early; exit 0' TERM; touch ready_early; sleep 30",
            ),
        )
        .send()
        .await
    });
    wait_file(&dir.path().join("ready_early")).await;
    pending.abort();
    let _ = pending.await;
    wait_file(&dir.path().join("stopped_early")).await;
    let response = post(&h.url, &call(10, "printf reusable"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        http_result(response).await["result"]["structuredContent"]["stdout"],
        "reusable"
    );
}
