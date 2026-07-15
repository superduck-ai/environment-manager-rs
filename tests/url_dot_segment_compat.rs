//! `url` 路径点段兼容补丁的定向测试。
//!
//! 测试覆盖无 base 的绝对 URL、`set_path`、带 base 的 `join` 以及 reqwest 最终写出的
//! HTTP request-line，确保补丁只保留 Go 动态路径段所需的字节，不改变重定向式相对解析。

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use url::Url;

/// 从 loopback 测试连接读取一份完整 HTTP/1 header。
///
/// # 参数
/// - `stream`：已经建立的服务端 TCP 流；函数只读取到首个 `\r\n\r\n` 为止。
///
/// # 返回
/// 返回包含 request-line 与全部 header 的原始字节，供逐字节兼容断言使用。
async fn read_http_header(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1024];

    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut buffer).await.unwrap();
        assert_ne!(read, 0, "客户端在发送完整 HTTP header 前关闭连接");
        request.extend_from_slice(&buffer[..read]);
    }

    request
}

/// 验证无 base 的绝对 URL 解析不会再折叠完整的点段。
///
/// 百分号形式也属于 WHATWG 的点段集合；补丁保留它们可以防止未来调用方先编码再解析时
/// 重新引入同一类兼容差异。
#[test]
fn absolute_parse_without_base_preserves_complete_dot_segments() {
    let url = Url::parse("https://example.test/v1/sessions/./../events").unwrap();
    assert_eq!(url.as_str(), "https://example.test/v1/sessions/./../events");

    let encoded = Url::parse("https://example.test/v1/%2e/%2E%2e/events").unwrap();
    assert_eq!(
        encoded.as_str(),
        "https://example.test/v1/%2e/%2E%2e/events"
    );
}

/// 验证 `Url::set_path` 保留点段，同时不破坏既有 query 与 fragment 的恢复位置。
#[test]
fn set_path_preserves_dot_segments_and_suffix() {
    let mut url = Url::parse("https://example.test/old?mode=1#result").unwrap();
    url.set_path("/v1/sessions/../events");

    assert_eq!(
        url.as_str(),
        "https://example.test/v1/sessions/../events?mode=1#result"
    );
}

/// 验证带 base 的解析仍执行上游规范化，覆盖相对跳转和绝对 Location 两种形式。
///
/// reqwest 的相对重定向解析依赖同一类 base URL 语义，因此这里是补丁作用域的回归门禁。
#[test]
fn join_and_base_aware_parse_still_normalize_dot_segments() {
    let base = Url::parse("https://example.test/v1/sessions/current/").unwrap();

    assert_eq!(
        base.join("../next").unwrap().as_str(),
        "https://example.test/v1/sessions/next"
    );
    assert_eq!(
        base.join("https://example.test/v1/./sessions/../next")
            .unwrap()
            .as_str(),
        "https://example.test/v1/next"
    );
}

/// 验证 `PathSegmentsMut` 仍采用上游的路径段过滤语义。
///
/// 该接口使用独立的 `PathSegmentSetter` 上下文，不是 Go 兼容调用入口，因此 feature
/// 不应让它开始接受 `.` 或 `..`。
#[test]
fn path_segments_mut_keeps_upstream_dot_segment_behavior() {
    let mut url = Url::parse("https://example.test/original").unwrap();
    url.path_segments_mut()
        .unwrap()
        .clear()
        .extend(["v1", ".", "sessions", "..", "events"]);

    assert_eq!(url.as_str(), "https://example.test/v1/sessions/events");
}

/// 验证 reqwest 经 Hyper 写到 TCP 连接的 request-target 与 Go 保持逐字节一致。
///
/// 测试只监听 loopback 随机端口，不访问外部网络。服务端读取完整 header 后返回 204，
/// 再断言首行仍包含 `../`；这能发现 URL 对象正确但 HTTP 层再次规范化的回归。
#[tokio::test]
async fn reqwest_writes_preserved_dot_segment_in_request_line() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_http_header(&mut stream).await;

        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        request
    });

    let url = Url::parse(&format!(
        "http://{address}/v1/sessions/../events?source=compat"
    ))
    .unwrap();
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    let request = server.await.unwrap();
    let request_line = request.split(|byte| *byte == b'\n').next().unwrap();
    assert_eq!(
        request_line,
        b"GET /v1/sessions/../events?source=compat HTTP/1.1\r"
    );
}

/// 验证 reqwest 处理相对重定向时仍按 WHATWG 规则折叠 Location 中的点段。
///
/// 首个响应强制关闭连接，使第二次请求必定能作为独立 request-line 被捕获；测试由此
/// 直接覆盖运行时重定向路径，而不只依赖 `Url::join` 的单元级推断。
#[tokio::test]
async fn reqwest_relative_redirect_still_normalizes_dot_segments() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut first_stream, _) = listener.accept().await.unwrap();
        let first_request = read_http_header(&mut first_stream).await;
        first_stream
            .write_all(
                b"HTTP/1.1 302 Found\r\nLocation: /v1/./sessions/../next\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        drop(first_stream);

        let (mut second_stream, _) = listener.accept().await.unwrap();
        let second_request = read_http_header(&mut second_stream).await;
        second_stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();

        (first_request, second_request)
    });

    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(2))
        .no_proxy()
        .build()
        .unwrap()
        .get(format!("http://{address}/start"))
        .send()
        .await
        .unwrap();

    let (first_request, second_request) = server.await.unwrap();
    assert_eq!(
        first_request.split(|byte| *byte == b'\n').next().unwrap(),
        b"GET /start HTTP/1.1\r"
    );
    assert_eq!(
        second_request.split(|byte| *byte == b'\n').next().unwrap(),
        b"GET /v1/next HTTP/1.1\r"
    );
}
