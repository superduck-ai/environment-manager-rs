//! 独立 Rust 回归测试；不依赖参考服务、Docker 或外部源码。
mod common;
use async_trait::async_trait;
use mcp_server::{
    Config, Registry, Server, Tool, ToolResult,
    environment::{EnvironmentProvider, UnixSocketProvider},
    plugins::{Manager, Plugin, ServiceClient},
};
use serde_json::{Value, json};
use std::{
    os::unix::fs::{PermissionsExt, symlink},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio_util::sync::CancellationToken;

fn server(dir: &std::path::Path) -> Arc<Server> {
    Arc::new(Server::new(
        Arc::new(Registry::builtin(dir.into(), None)),
        Arc::new(Manager::default()),
        Config::default(),
    ))
}
async fn call(server: &Arc<Server>, name: &str, arguments: Value) -> Value {
    common::rpc(server.clone(), json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":name,"arguments":arguments}})).await["result"].clone()
}
/// 完整的大型快照带尾部空白时，无需等待 socket 关闭即可返回。
#[tokio::test]
async fn large_environment_snapshot_with_whitespace_keeps_connection_open() {
    use tokio::{io::AsyncWriteExt, net::UnixListener};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("env.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let snapshot = format!(
        "{}\n \t\r\n",
        json!({"env":{"GOOD":"ok","PADDING":"x".repeat(12000)}})
    );
    let (release, keep_open) = tokio::sync::oneshot::channel();
    let sender = tokio::spawn(async move {
        let (mut connection, _) = listener.accept().await.unwrap();
        connection.write_all(snapshot.as_bytes()).await.unwrap();
        let _ = keep_open.await;
        drop(connection);
    });
    let provider = UnixSocketProvider::new(path.to_str().unwrap());
    let env = provider.environ(CancellationToken::new()).await.unwrap();
    assert_eq!(env["GOOD"], "ok");
    assert_eq!(env["PADDING"].len(), 12000);
    release.send(()).unwrap();
    sender.await.unwrap();
}

/// socket 接收辅助器：可保持连接，直到客户端完成读取或返回超时。
async fn socket_snapshot(
    payload: Vec<u8>,
    close: bool,
) -> Result<std::collections::BTreeMap<String, String>, String> {
    use tokio::{io::AsyncWriteExt, net::UnixListener};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("env.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let (release, keep_open) = tokio::sync::oneshot::channel();
    let sender = tokio::spawn(async move {
        let (mut connection, _) = listener.accept().await.unwrap();
        // 超限读取可能先关闭客户端，服务端写入失败仍属预期场景。
        let _ = connection.write_all(&payload).await;
        if !close {
            let _ = keep_open.await;
        }
    });
    let provider = UnixSocketProvider::new(path.to_str().unwrap());
    let result = provider.environ(CancellationToken::new()).await;
    let _ = release.send(());
    sender.await.unwrap();
    result
}

/// 顶层标量等待结束字节或 EOF；对象无需尾部空白即可完成。
#[tokio::test]
async fn environment_scalar_stream_boundaries() {
    for scalar in ["null", "true", "1", r#""text""#] {
        let error = socket_snapshot(scalar.as_bytes().to_vec(), false)
            .await
            .unwrap_err();
        assert!(error.ends_with("i/o timeout"), "{scalar}: {error}");
    }
    for (payload, close) in [
        (b"null\n".to_vec(), false),
        (b"null".to_vec(), true),
        (b"{}".to_vec(), false),
    ] {
        assert!(socket_snapshot(payload, close).await.unwrap().is_empty());
    }
    for scalar in ["true", "1", r#""text""#] {
        let error = socket_snapshot(format!("{scalar}\n").into_bytes(), false)
            .await
            .unwrap_err();
        assert!(error.contains("cannot unmarshal"), "{scalar}: {error}");
    }
}

/// 大型未知字段只扫描一次，debug 下也能在默认期限内处理容量边界。
#[tokio::test]
async fn environment_snapshot_size_limit_without_reparsing() {
    let prefix = br#"{"env":{"GOOD":"ok"},"padding":""#;
    let suffix = br#""}"#;
    let mut payload = prefix.to_vec();
    payload.resize((1 << 20) - suffix.len(), b'x');
    payload.extend_from_slice(suffix);
    assert_eq!(payload.len(), 1 << 20);
    assert_eq!(
        socket_snapshot(payload.clone(), false).await.unwrap()["GOOD"],
        "ok"
    );
    payload.insert(payload.len() - suffix.len(), b'x');
    assert_eq!(
        socket_snapshot(payload, false).await.unwrap_err(),
        "environment snapshot exceeds 1048576 bytes"
    );
}

/// 取消正在等待标量结束字节的读取，也应释放后台扫描器。
#[tokio::test]
async fn environment_cancellation_releases_stream_decoder() {
    use tokio::{io::AsyncWriteExt, net::UnixListener};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("env.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let (sent, received) = tokio::sync::oneshot::channel();
    let (release, keep_open) = tokio::sync::oneshot::channel();
    let sender = tokio::spawn(async move {
        let (mut connection, _) = listener.accept().await.unwrap();
        connection.write_all(b"null").await.unwrap();
        sent.send(()).unwrap();
        let _ = keep_open.await;
    });
    let token = CancellationToken::new();
    let cancel = token.clone();
    let mut provider = UnixSocketProvider::new(path.to_str().unwrap());
    provider.timeout = std::time::Duration::from_secs(10);
    let reader = tokio::spawn(async move { provider.environ(token).await });
    received.await.unwrap();
    cancel.cancel();
    let error = tokio::time::timeout(std::time::Duration::from_secs(1), reader)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error, "decode environment snapshot: context canceled");
    release.send(()).unwrap();
    sender.await.unwrap();
}

#[tokio::test]
async fn declarations_and_alias_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    let response = common::rpc(
        s.clone(),
        json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
    )
    .await;
    let tools = response["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 12);
    for name in [
        "bash",
        "read",
        "write",
        "edit",
        "grep",
        "glob",
        "run_in_terminal",
        "file_read",
        "file_write",
        "file_edit",
        "grep_run",
        "glob_run",
    ] {
        assert!(tools.iter().any(|t| t["name"] == name), "{name}");
    }
    let path = dir.path().join("lines");
    std::fs::write(&path, "first\nsecond\nthird\n").unwrap();
    assert_eq!(
        call(
            &s,
            "file_read",
            json!({"file_path":path,"startLine":2,"endLine":2})
        )
        .await,
        call(
            &s,
            "read",
            json!({"file_path":path,"start_line":2,"end_line":2})
        )
        .await
    );
}
#[tokio::test]
async fn atomic_write_preserves_symlink_mode_bom_and_line_endings() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    let path = dir.path().join("target");
    let link = dir.path().join("link");
    std::fs::write(&path, b"\xef\xbb\xbfold\r\nlines\r\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o750)).unwrap();
    symlink(&path, &link).unwrap();
    let result = call(
        &s,
        "write",
        json!({"file_path":"link","content":"new\nlines\n"}),
    )
    .await;
    assert_ne!(result["isError"], true);
    assert!(
        std::fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"\xef\xbb\xbfnew\r\nlines\r\n"
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o750
    );
}
#[tokio::test]
async fn editing_requires_unique_match_and_preserves_invalid_utf8() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    let path = dir.path().join("data");
    std::fs::write(&path, b"\xffneedle twice needle\x80").unwrap();
    assert_eq!(
        call(
            &s,
            "edit",
            json!({"file_path":"data","old_string":"needle","new_string":"changed"})
        )
        .await["isError"],
        true
    );
    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"\xffneedle twice needle\x80"
    );
    assert_ne!(
        call(
            &s,
            "edit",
            json!({"file_path":"data","old_string":" twice ","new_string":" once "})
        )
        .await["isError"],
        true
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"\xffneedle once needle\x80");
}
#[tokio::test]
async fn command_environment_and_nonzero_exit_are_tool_results() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    let result=call(&s,"bash",json!({"command":"printf '%s:%s' \"$CI\" \"$CUSTOM\"; exit 7","env":{"CUSTOM":"value","CI":"override"}})).await;
    let text = result["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("override:value"));
    assert!(text.contains("[exit code: 7]"));
    assert_eq!(result["isError"], true);
}
#[tokio::test]
async fn cancellation_terminates_process_group() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::builtin(dir.path().into(), None);
    let tool = registry.get("bash").unwrap();
    let token = CancellationToken::new();
    let cancel = token.clone();
    let task = tokio::spawn(async move {
        tool.handle(token, json!({"command":"sleep 10","timeout":20}))
            .await
            .unwrap()
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    cancel.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(4), task)
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.content[0]
            .text
            .contains("[cancelled; process group terminated]")
    );
}
struct TestTool;
#[async_trait]
impl Tool for TestTool {
    fn definition(&self) -> Value {
        json!({"name":"plugin_tool","inputSchema":{"type":"object"}})
    }
    async fn handle(&self, _: CancellationToken, _: Value) -> Result<ToolResult, String> {
        Ok(ToolResult::text("plugin result"))
    }
}
struct TestPlugin(Arc<AtomicUsize>);
#[async_trait]
impl Plugin for TestPlugin {
    fn name(&self) -> String {
        "plugin".into()
    }
    fn description(&self) -> String {
        "test".into()
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(TestTool)]
    }
    async fn initialize(&self) -> Result<(), String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn shutdown(&self) -> Result<(), String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn plugin_lifecycle_and_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let manager = Arc::new(Manager::default());
    manager
        .register(Arc::new(TestPlugin(count.clone())))
        .await
        .unwrap();
    let s = Arc::new(Server::new(
        Arc::new(Registry::default()),
        manager.clone(),
        Config::default(),
    ));
    assert_eq!(
        call(&s, "plugin_tool", json!({})).await["content"][0]["text"],
        "plugin result"
    );
    manager.shutdown_all().await;
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert!(manager.list_tools().is_empty());
    drop(dir);
}
#[tokio::test]
async fn service_stop_uses_delete_and_decodes_non_success_status() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for (status, body, expected) in [
        (404, "", None),
        (
            500,
            r#"{"name":"worker","status":"stopped","pid":42}"#,
            Some(42),
        ),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 2048];
            let n = stream.read(&mut buffer).await.unwrap();
            assert!(
                String::from_utf8_lossy(&buffer[..n])
                    .starts_with("DELETE /_runtime/services/worker/ HTTP/1.1")
            );
            stream.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        });
        let result = ServiceClient::new(&address.to_string())
            .stop("worker")
            .await
            .unwrap();
        assert_eq!(result.map(|s| s.pid), expected);
        task.await.unwrap();
    }
}

#[tokio::test]
async fn image_pixels_and_encoded_bytes_match_reference_goldens() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    for (name, source, expected) in [
        (
            "wide.jpg",
            include_bytes!("fixtures/wide.jpg").as_slice(),
            include_bytes!("fixtures/wide.expected.jpg").as_slice(),
        ),
        (
            "wide.png",
            include_bytes!("fixtures/wide.png").as_slice(),
            include_bytes!("fixtures/wide.expected.png").as_slice(),
        ),
    ] {
        let path = dir.path().join(name);
        std::fs::write(&path, source).unwrap();
        let result = call(&s, "read", json!({"file_path":path})).await;
        assert_ne!(result["isError"], true);
        let encoded = result["content"][1]["data"].as_str().unwrap();
        assert_eq!(STANDARD.decode(encoded).unwrap(), expected, "{name}");
        assert!(
            result["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("Coordinate mapping: multiply returned x by 1.2505 and y by 1.2667")
        );
    }
}

#[tokio::test]
async fn gif_subframe_keeps_reference_bounds() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    let path = dir.path().join("offset.gif");
    std::fs::write(&path, include_bytes!("fixtures/offset-first-frame.gif")).unwrap();
    let result = call(&s, "read", json!({"file_path":path})).await;
    let text = result["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("Original: 2501x103"));
    assert!(text.contains("Returned: 1000x100"));
    assert_eq!(
        STANDARD
            .decode(result["content"][1]["data"].as_str().unwrap())
            .unwrap(),
        include_bytes!("fixtures/offset-first-frame.expected.jpg")
    );
}

#[tokio::test]
async fn unsupported_image_data_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    let path = dir.path().join("unsupported.png");
    std::fs::write(&path, b"BM\0\0\0\0\0\0\0\0\0\0\0\0\0\0").unwrap();
    assert_eq!(
        call(&s, "read", json!({"file_path":path})).await["content"][0]["text"],
        "failed to prepare image file: invalid or unsupported image data: image: unknown format"
    );
}

#[tokio::test]
async fn wildcard_http_listener_serves_ipv4_and_ipv6() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    let reservation = std::net::TcpListener::bind("[::]:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    let task = tokio::spawn(async move { s.serve_http(&format!(":{port}")).await });
    for host in ["[::1]", "127.0.0.1"] {
        let address = format!("{host}:{port}");
        let mut stream = None;
        for _ in 0..30 {
            if let Ok(conn) = tokio::net::TcpStream::connect(&address).await {
                stream = Some(conn);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let mut stream = stream.unwrap();
        let body = common::request(json!(1), "server/discover", json!({})).to_string();
        stream.write_all(format!("POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nMCP-Protocol-Version: 2026-07-28\r\nMcp-Method: server/discover\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("2026-07-28"));
        assert!(response.contains("supportedVersions"));
    }
    task.abort();
}
