//! Git smart-HTTP 请求解析、认证改写与 session ingress 转发。
//!
//! 客户端只把 `Authorization` 当作完成 Git Basic challenge 的信号；真正发往上游的
//! 凭据始终由仓库表重写。成功响应保持流式传输，上游错误响应则先完整读取并脱敏。
//! HTTP 客户端禁止跟随重定向，从根源上避免 Basic token 跨 host 泄漏。

use std::{collections::HashMap, fmt, sync::Arc};

use crate::internal::util::{claude_code_version, escape_path_segment, version};
use axum::{body::Body, response::Response};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use http::{
    HeaderMap, HeaderValue, Request, StatusCode,
    header::{AUTHORIZATION, CONTENT_TYPE, HOST, WWW_AUTHENTICATE},
};

use super::{Config, GitProxyError, GitProxyResult};

const REDACTED: &str = "[REDACTED]";
const X_TOKEN_BACKEND: &str = "x-token-backend";
const X_ENVIRONMENT_RUNNER_VERSION: &str = "x-environment-runner-version";
const X_CLAUDE_CODE_VERSION: &str = "x-claude-code-version";

/// 单次 Git proxy 请求所需的不可变共享状态。
#[derive(Clone)]
pub(super) struct Handler {
    config: Arc<Config>,
    http_client: reqwest::Client,
    repo_tokens: Arc<HashMap<String, String>>,
}

impl Handler {
    /// 创建 handler，并保留构造时生成的仓库 token 快照。
    ///
    /// server 每次 `Start` 都从 `RepoAuths` 构造一张新 map；这里使用 `Arc` 让
    /// Axum 为并发连接克隆 handler 时不复制敏感字符串，同时保持启动后的配置隔离。
    pub(super) fn new(
        config: Arc<Config>,
        http_client: reqwest::Client,
        repo_tokens: HashMap<String, String>,
    ) -> Self {
        Self {
            config,
            http_client,
            repo_tokens: Arc::new(repo_tokens),
        }
    }

    /// 解析 GitHub/GHE 两种本地路径并返回完整 HTTP 响应。
    ///
    /// `info/refs` 的首次无认证请求只返回 Basic challenge；其他路径即使没有认证头
    /// 也继续校验仓库表。进入 Reqwest 后，请求 future 被连接层丢弃会立即取消
    /// 上游传输。
    pub(super) async fn handle_request(&self, request: Request<Body>) -> Response {
        let method = request.method().as_str().to_owned();
        let path = percent_decode_path(request.uri().path());
        if !path.starts_with("/git/") {
            tracing::warn!("path" = ?(path.clone()), "{}", "Invalid path format - missing components");
            return http_error(StatusCode::BAD_REQUEST, "Invalid path format");
        }

        let local_path = &path["/git/".len()..];
        let first_segment = local_path
            .split_once('/')
            .map_or(local_path, |(head, _)| head);
        let parsed = if first_segment.contains('.') {
            let parts = local_path.splitn(4, '/').collect::<Vec<_>>();
            if parts.len() < 4 {
                tracing::warn!("path" = ?(path.clone()), "{}", "Invalid GHE path format - missing components");
                return http_error(StatusCode::BAD_REQUEST, "Invalid path format");
            }
            (
                parts[1].to_owned(),
                parts[2].strip_suffix(".git").unwrap_or(parts[2]).to_owned(),
                parts[3].to_owned(),
            )
        } else {
            let parts = local_path.splitn(3, '/').collect::<Vec<_>>();
            if parts.len() < 3 {
                tracing::warn!("path" = ?(path.clone()), "{}", "Invalid path format - missing components");
                return http_error(StatusCode::BAD_REQUEST, "Invalid path format");
            }
            (
                parts[0].to_owned(),
                parts[1].strip_suffix(".git").unwrap_or(parts[1]).to_owned(),
                parts[2].to_owned(),
            )
        };
        let (org, repo, git_path) = parsed;

        let has_auth = request
            .headers()
            .get(AUTHORIZATION)
            .is_some_and(|value| !value.as_bytes().is_empty());
        tracing::info!("method" = ?(method), "path" = ?(path), "org" = ?(org.clone()), "repo" = ?(repo.clone()), "git_path" = ?(git_path.clone()), "has_auth" = ?(serde_json::Value::Bool(has_auth)), "{}", "Received git request");

        if git_path == "info/refs" && !has_auth {
            tracing::info!("org" = ?(org.clone()), "repo" = ?(repo.clone()), "{}", "Challenging unauthenticated info/refs request");
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::UNAUTHORIZED;
            response.headers_mut().insert(
                WWW_AUTHENTICATE,
                HeaderValue::from_static("Basic realm=\"Git Proxy\""),
            );
            return response;
        }

        match self.forward_request(request, &org, &repo, &git_path).await {
            Ok(response) => response,
            Err(error) => {
                let sanitized = sanitize_error(Some(&error));
                tracing::error!("error" = ?(sanitized.clone()), "org" = ?(org), "repo" = ?(repo), "git_path" = ?(git_path), "{}", "Failed to proxy git request");
                http_error(
                    StatusCode::BAD_GATEWAY,
                    &format!("Proxy error: {sanitized}"),
                )
            }
        }
    }

    /// 验证仓库与 Git 子路径后流式转发请求和响应。
    ///
    /// 入站 `Authorization`、`Host` 与 `X-Token-Backend` 永远丢弃，随后写入 server
    /// 配置中的可信值。构造 URL 时逐段实现path-segment 百分号编码，原始 query 则逐字追加；
    /// 这样 Git 的 service 参数和 `%2F` 路径编码均不会被二次解释。
    async fn forward_request(
        &self,
        request: Request<Body>,
        org: &str,
        repo: &str,
        git_path: &str,
    ) -> GitProxyResult<Response> {
        if !is_valid_git_path(git_path) {
            tracing::warn!("git_path" = ?(git_path.to_owned()), "org" = ?(org.to_owned()), "repo" = ?(repo.to_owned()), "{}", "Invalid git path detected");
            return Err(GitProxyError::message("invalid git path"));
        }

        let repo_key = format!("{org}/{repo}");
        let auth_token = match self
            .repo_tokens
            .get(&repo_key)
            .filter(|token| !token.is_empty())
        {
            Some(auth_token) => auth_token,
            None => {
                tracing::warn!("repo" = ?(repo_key.clone()), "{}", "No auth token configured for repository");
                return Err(GitProxyError::message("repository not authorized"));
            }
        };

        let (parts, body) = request.into_parts();
        let method = parts.method.as_str().to_owned();
        let base_url = self.config.session_ingress_url.trim_end_matches('/');
        let mut upstream_url = format!(
            "{base_url}/v1/session_ingress/session/{}/git_proxy/{}/{}.git/{}",
            escape_path_segment(&self.config.session_id),
            escape_path_segment(org),
            escape_path_segment(repo),
            escape_path_segment(git_path),
        );
        if let Some(query) = parts.uri.query().filter(|query| !query.is_empty()) {
            upstream_url.push('?');
            upstream_url.push_str(query);
        }
        tracing::info!("upstream_url" = ?(sanitize_string(&upstream_url)), "repo" = ?(repo_key.clone()), "git_path" = ?(git_path.to_owned()), "{}", "Forwarding request to session ingress proxy");

        let mut headers = filtered_request_headers(parts.headers);
        let basic_auth = format!("Basic {}", STANDARD.encode(format!("git:{auth_token}")));
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&basic_auth).map_err(|error| {
                GitProxyError::message(format!("failed to create upstream request: {error}"))
            })?,
        );
        headers.insert(
            X_ENVIRONMENT_RUNNER_VERSION,
            HeaderValue::from_str(version()).map_err(|error| {
                GitProxyError::message(format!("failed to create upstream request: {error}"))
            })?,
        );
        if let Some(claude_version) = claude_code_version().filter(|value| !value.is_empty()) {
            headers.insert(
                X_CLAUDE_CODE_VERSION,
                HeaderValue::from_str(claude_version).map_err(|error| {
                    GitProxyError::message(format!("failed to create upstream request: {error}"))
                })?,
            );
        }
        if !self.config.token_backend.is_empty() {
            headers.insert(
                X_TOKEN_BACKEND,
                HeaderValue::from_str(&self.config.token_backend).map_err(|error| {
                    GitProxyError::message(format!("failed to create upstream request: {error}"))
                })?,
            );
        }

        // Body 保持流式，不先聚合到内存，直接交给 HTTP transport。
        let upstream_request = self
            .http_client
            .request(parts.method, &upstream_url)
            .headers(headers)
            .body(reqwest::Body::wrap_stream(body.into_data_stream()))
            .build()
            .map_err(|error| {
                GitProxyError::message(format!("failed to create upstream request: {error}"))
            })?;

        tracing::info!("repo" = ?(repo_key.clone()), "method" = ?(method), "{}", "Sending authenticated request to upstream");
        let upstream_response =
            self.http_client
                .execute(upstream_request)
                .await
                .map_err(|error| {
                    GitProxyError::message(format!("failed to send request to upstream: {error}"))
                })?;
        let status = upstream_response.status();
        let response_headers = upstream_response.headers().clone();
        tracing::info!("repo" = ?(repo_key.clone()), "status" = ?(i64::from(status.as_u16())), "{}", "Received response from upstream");

        let response_body = if status >= StatusCode::BAD_REQUEST {
            let bytes = upstream_response.bytes().await.map_err(|error| {
                GitProxyError::message(format!("failed to read upstream error response: {error}"))
            })?;
            Body::from(sanitize_string(&String::from_utf8_lossy(&bytes)))
        } else {
            Body::from_stream(upstream_response.bytes_stream())
        };

        let mut response = Response::new(response_body);
        *response.status_mut() = status;
        *response.headers_mut() = response_headers;
        Ok(response)
    }
}

/// 创建明确禁止自动重定向的 HTTP 客户端。
///
/// Reqwest 的 `Policy::none` 会把原始 3xx 状态、header 与 body 返回给 Git 客户端，
/// 并确保 Basic token 不会被
/// 重放到 Location 指向的其他 host。TLS 使用 Cargo 已启用的 rustls 栈。
pub(super) fn new_http_client() -> GitProxyResult<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| GitProxyError::message(format!("failed to create HTTP client: {error}")))
}

/// 删除客户端不能控制的三个请求头，并保留其余 header 的全部多值。
fn filtered_request_headers(mut headers: HeaderMap) -> HeaderMap {
    headers.remove(AUTHORIZATION);
    headers.remove(HOST);
    headers.remove(X_TOKEN_BACKEND);
    headers
}

/// 构造 HTTP 纯文本错误响应。
fn http_error(status: StatusCode, message: &str) -> Response {
    let mut response = Response::new(Body::from(message.to_owned()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// 把 URI 原始 path 的 `%XX` 恢复为解码后的字节序列。
///
/// Hyper 在 handler 中保留 escaped path，而 HTTP/1 已经完成一次 PathUnescape。
/// 这里的解码让 `%2e%2e` 等编码穿越仍会被后续 `is_valid_git_path` 拒绝。
fn percent_decode_path(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let (Some(high), Some(low)) =
                (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
        {
            decoded.push((high << 4) | low);
            index += 3;
            continue;
        }
        decoded.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

/// 把一个 ASCII 十六进制字符转换为半字节；其他字节返回 `None`。
const fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// 对可空错误执行文本脱敏。
pub(super) fn sanitize_error(error: Option<&dyn fmt::Display>) -> String {
    error.map_or_else(String::new, |error| sanitize_string(&error.to_string()))
}

/// 按固定顺序隐藏 Authorization、Anthropic key 与 token 参数。
///
/// 顺序不可交换：`sk-ant-ccsr-*` 必须先于通用 `sk-ant-*`，完整 Authorization
/// 形式也必须先于通用 Bearer/Basic。实现只识别协议文本中的 ASCII 空白集合，
/// 并严格保留原始前缀、大小写和空白。
pub(super) fn sanitize_string(value: &str) -> String {
    let mut result = value.to_owned();
    result = replace_header_scheme(&result, "Authorization:", "Bearer", TokenKind::Bearer);
    result = replace_header_scheme(&result, "authorization:", "bearer", TokenKind::Bearer);
    result = replace_header_scheme(&result, "Authorization:", "Basic", TokenKind::Basic);
    result = replace_header_scheme(&result, "authorization:", "basic", TokenKind::Basic);
    result = replace_prefixed_token(&result, "sk-ant-ccsr-", TokenKind::Anthropic);
    result = replace_prefixed_token(&result, "sk-ant-", TokenKind::Anthropic);
    result = replace_scheme_token(&result, "Bearer", TokenKind::Bearer);
    result = replace_scheme_token(&result, "bearer", TokenKind::Bearer);
    result = replace_scheme_token(&result, "Basic", TokenKind::Basic);
    result = replace_scheme_token(&result, "basic", TokenKind::Basic);
    replace_token_assignment(&result)
}

/// 判断子路径是否属于 Git smart-HTTP 白名单。
pub(super) fn is_valid_git_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.contains("..")
        && [
            "info/refs",
            "git-upload-pack",
            "git-receive-pack",
            "HEAD",
            "objects/",
            "refs/",
        ]
        .iter()
        .any(|prefix| path.starts_with(prefix))
}

/// 正则 token 字符集类别。
#[derive(Clone, Copy)]
enum TokenKind {
    Bearer,
    Basic,
    Anthropic,
}

/// 替换 `Header: <ws> Scheme <ws> token` 形式，同时保留 capture group 1。
fn replace_header_scheme(input: &str, header: &str, scheme: &str, token_kind: TokenKind) -> String {
    replace_scanned(input, |bytes, cursor| {
        let found = find_bytes(bytes, header.as_bytes(), cursor)?;
        let mut index = found + header.len();
        let whitespace_start = index;
        index = consume_whitespace(bytes, index);
        if index == whitespace_start || !bytes[index..].starts_with(scheme.as_bytes()) {
            return Some(ScanMatch::retry(found + header.len()));
        }
        index += scheme.len();
        let whitespace_start = index;
        index = consume_whitespace(bytes, index);
        if index == whitespace_start {
            return Some(ScanMatch::retry(index));
        }
        token_end(bytes, index, token_kind)
            .map(|end| ScanMatch::replace(index, end))
            .or_else(|| Some(ScanMatch::retry(index)))
    })
}

/// 替换通用 `Scheme <ws> token` 形式，同时保留 scheme 与空白。
fn replace_scheme_token(input: &str, scheme: &str, token_kind: TokenKind) -> String {
    replace_scanned(input, |bytes, cursor| {
        let found = find_bytes(bytes, scheme.as_bytes(), cursor)?;
        let mut index = found + scheme.len();
        let whitespace_start = index;
        index = consume_whitespace(bytes, index);
        if index == whitespace_start {
            return Some(ScanMatch::retry(found + scheme.len()));
        }
        token_end(bytes, index, token_kind)
            .map(|end| ScanMatch::replace(index, end))
            .or_else(|| Some(ScanMatch::retry(index)))
    })
}

/// 替换 `sk-ant-*` 一类固定前缀及其连续 token 字符。
fn replace_prefixed_token(input: &str, prefix: &str, token_kind: TokenKind) -> String {
    replace_scanned(input, |bytes, cursor| {
        let found = find_bytes(bytes, prefix.as_bytes(), cursor)?;
        let token_start = found + prefix.len();
        token_end(bytes, token_start, token_kind)
            .map(|end| ScanMatch::replace(found, end))
            .or_else(|| Some(ScanMatch::retry(token_start)))
    })
}

/// 替换小写 `token=` 或 `token:` 后的 Bearer 字符集值。
fn replace_token_assignment(input: &str) -> String {
    replace_scanned(input, |bytes, cursor| {
        let found = find_bytes(bytes, b"token", cursor)?;
        let separator = found + "token".len();
        if !matches!(bytes.get(separator), Some(b'=' | b':')) {
            return Some(ScanMatch::retry(separator));
        }
        let token_start = consume_whitespace(bytes, separator + 1);
        token_end(bytes, token_start, TokenKind::Bearer)
            .map(|end| ScanMatch::replace(token_start, end))
            .or_else(|| Some(ScanMatch::retry(token_start)))
    })
}

/// 一次 token 扫描的结果：替换一个范围，或从新位置继续查找。
struct ScanMatch {
    replace: Option<(usize, usize)>,
    next_cursor: usize,
}

impl ScanMatch {
    /// 创建一个有效 token 替换范围。
    const fn replace(start: usize, end: usize) -> Self {
        Self {
            replace: Some((start, end)),
            next_cursor: end,
        }
    }

    /// 创建一个不替换、只推进扫描位置的结果。
    const fn retry(next_cursor: usize) -> Self {
        Self {
            replace: None,
            next_cursor,
        }
    }
}

/// 反复运行 pattern scanner，并用 `[REDACTED]` 替换所有不重叠匹配。
fn replace_scanned(
    input: &str,
    mut scanner: impl FnMut(&[u8], usize) -> Option<ScanMatch>,
) -> String {
    let mut result = input.to_owned();
    let mut cursor = 0;
    while let Some(found) = scanner(result.as_bytes(), cursor) {
        if let Some((start, end)) = found.replace {
            result.replace_range(start..end, REDACTED);
            cursor = start + REDACTED.len();
        } else {
            cursor = found.next_cursor.max(cursor.saturating_add(1));
        }
        if cursor >= result.len() {
            break;
        }
    }
    result
}

/// 从指定字节下标寻找 ASCII needle；找不到返回 `None`。
fn find_bytes(haystack: &[u8], needle: &[u8], start: usize) -> Option<usize> {
    if needle.is_empty() || start > haystack.len() {
        return None;
    }
    haystack[start..]
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|offset| start + offset)
}

/// 消费 ASCII 空白并返回第一个非空白下标。
fn consume_whitespace(bytes: &[u8], mut index: usize) -> usize {
    while bytes
        .get(index)
        .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0c))
    {
        index += 1;
    }
    index
}

/// 按指定正则字符集返回 token 末尾；空匹配返回 `None`。
fn token_end(bytes: &[u8], start: usize, token_kind: TokenKind) -> Option<usize> {
    let mut index = start;
    match token_kind {
        TokenKind::Bearer => {
            while bytes.get(index).is_some_and(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
            }) {
                index += 1;
            }
        }
        TokenKind::Anthropic => {
            while bytes
                .get(index)
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            {
                index += 1;
            }
        }
        TokenKind::Basic => {
            while bytes
                .get(index)
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/'))
            {
                index += 1;
            }
            let payload_end = index;
            while bytes.get(index) == Some(&b'=') {
                index += 1;
            }
            if payload_end == start {
                return None;
            }
        }
    }
    (index > start).then_some(index)
}
