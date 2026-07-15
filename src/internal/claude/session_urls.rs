//! Claude SDK 与 resume endpoint 的兼容 URL 拼接。
//!
//! 这里有意不用 `url::Url`：Go 实现只是去 scheme、截断 path/query 并做字符串
//! 前后缀判断，使用标准 URL 规范化会改变端口、本地 host 和异常输入的结果。

/// 根据 API base、session 与 endpoint 版本生成 SDK URL 和 resume URL。
///
/// 对应 Go `buildSessionURLs`；名称只把 URL 缩写按 Rust 习惯作为完整语义词处理。
///
/// `use_code_sessions` 为 true 时两个返回值相同且固定走 `/v1/code/sessions`；
/// 否则分别返回 websocket ingress 与 HTTP persistence endpoint。
#[must_use]
pub fn build_session_urls(
    base_url: &str,
    session_id: &str,
    api_version: &str,
    use_code_sessions: bool,
) -> (String, String) {
    let mut host = base_url;
    let mut is_https = true;
    if let Some(stripped) = host.strip_prefix("https://") {
        host = stripped;
    } else if let Some(stripped) = host.strip_prefix("http://") {
        host = stripped;
        is_https = false;
    }

    if let Some((before_slash, _)) = host.split_once('/') {
        host = before_slash;
    }
    if let Some((before_query, _)) = host.split_once('?') {
        host = before_query;
    }

    let is_local = host.starts_with("localhost")
        || host.starts_with("127.0.0.1")
        || host.ends_with(".orb.local");
    let http_scheme = if is_https && !is_local {
        "https"
    } else {
        "http"
    };

    if use_code_sessions {
        let session_url = format!("{http_scheme}://{host}/v1/code/sessions/{session_id}");
        return (session_url.clone(), session_url);
    }

    let ws_scheme = if is_https && !is_local { "wss" } else { "ws" };
    (
        format!("{ws_scheme}://{host}/{api_version}/session_ingress/ws/{session_id}"),
        format!("{http_scheme}://{host}/{api_version}/session_ingress/session/{session_id}"),
    )
}
