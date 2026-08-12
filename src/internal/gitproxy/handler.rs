//! Git smart-HTTP 请求解析、认证改写与 session ingress 转发。
//!
//! 本文件机械对应 Go `handler.go`。客户端只把 `Authorization` 当作完成 Git Basic
//! challenge 的信号；真正发往上游的凭据始终由仓库表重写。成功响应保持流式传输，
//! 上游错误响应则先完整读取并脱敏。HTTP 客户端禁止跟随重定向，对应恢复出的
//! `CheckRedirect = http.ErrUseLastResponse`，从根源上避免 Basic token 跨 host 泄漏。

use std::{collections::HashMap, fmt, io, sync::Arc};

use crate::internal::{
    logger::{LogAttributes, LogLevel, Logger, default_logger},
    util::{claude_code_version, go_path_escape, version},
};
use axum::{body::Body, response::Response};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use futures_util::StreamExt as _;
use http::{
    HeaderMap, HeaderValue, Request, StatusCode,
    header::{AUTHORIZATION, CONTENT_TYPE, HOST, WWW_AUTHENTICATE},
};

use super::{Config, GitProxyError, GitProxyResult};

const REDACTED: &str = "[REDACTED]";
const X_TOKEN_BACKEND: &str = "x-token-backend";
const X_ENVIRONMENT_RUNNER_VERSION: &str = "x-environment-runner-version";
const X_CLAUDE_CODE_VERSION: &str = "x-claude-code-version";

/// `forwardRequest` 失败及 Go 已经写出的可选部分响应。
///
/// Go 在复制上游 header/status 后才读取错误响应体；此时读取失败仍会返回 error，
/// 但 ServeHTTP 已不能把状态改成 502。Rust 必须显式携带这份部分响应，才能同时
/// 记录外层失败日志并保留上游状态。
struct ForwardRequestFailure {
    error: GitProxyError,
    response: Option<Response>,
}

impl ForwardRequestFailure {
    /// 创建尚未向下游提交 header/status 的失败。
    ///
    /// # 参数
    /// - `error`：已按 Go `%w` 文本完成包装的错误。
    fn before_response(error: GitProxyError) -> Self {
        Self {
            error,
            response: None,
        }
    }

    /// 创建已经提交上游 header/status 后发生的失败。
    ///
    /// # 参数
    /// - `error`：响应体读取阶段返回的原始错误。
    /// - `response`：保持上游状态和 header 的部分响应。
    fn after_response(error: GitProxyError, response: Response) -> Self {
        Self {
            error,
            response: Some(response),
        }
    }
}

impl From<GitProxyError> for ForwardRequestFailure {
    /// 让构造请求、发送请求等提交响应前的 `?` 自动保留“尚未写 header”状态。
    fn from(error: GitProxyError) -> Self {
        Self::before_response(error)
    }
}

/// 单次 Git proxy 请求所需的不可变共享状态。
#[derive(Clone)]
pub(super) struct Handler {
    config: Arc<Config>,
    http_client: reqwest::Client,
    logger: Logger,
    repo_tokens: Arc<HashMap<String, String>>,
}

impl Handler {
    /// 创建 handler，并保留构造时生成的仓库 token 快照。
    ///
    /// Go server 每次 `Start` 都从 `RepoAuths` 构造一张新 map；这里使用 `Arc` 让
    /// Axum 为并发连接克隆 handler 时不复制敏感字符串，同时保持启动后的配置隔离。
    pub(super) fn new(
        config: Arc<Config>,
        http_client: reqwest::Client,
        repo_tokens: HashMap<String, String>,
    ) -> Self {
        // 测试会直接构造 Handler，因此这里也在边界解析默认
        // tracing logger，保证两条入口行为一致。
        let logger = config
            .logger
            .clone()
            .unwrap_or_else(|| default_logger().clone());
        Self {
            config,
            http_client,
            logger,
            repo_tokens: Arc::new(repo_tokens),
        }
    }

    /// 对应 Go `ServeHTTP`，解析 GitHub/GHE 两种本地路径并返回完整 HTTP 响应。
    ///
    /// `info/refs` 的首次无认证请求只返回 Basic challenge；其他路径即使没有认证头
    /// 也沿用 Go 行为继续校验仓库表。进入 Reqwest 后，请求 future 被连接层丢弃会
    /// 立即取消上游传输，对应 Go request context 的断连取消。
    pub(super) async fn serve_http(&self, request: Request<Body>) -> Response {
        let method = request.method().as_str().to_owned();
        let path = percent_decode_path(request.uri().path());
        if !path.starts_with("/git/") {
            self.log(
                LogLevel::Warn,
                "Invalid path format - missing components",
                log_attributes([("path", serde_json::Value::String(path.clone()))]),
            );
            return http_error(StatusCode::BAD_REQUEST, "Invalid path format");
        }

        let local_path = &path["/git/".len()..];
        let first_segment = local_path
            .split_once('/')
            .map_or(local_path, |(head, _)| head);
        let parsed = if first_segment.contains('.') {
            let parts = local_path.splitn(4, '/').collect::<Vec<_>>();
            if parts.len() < 4 {
                self.log(
                    LogLevel::Warn,
                    "Invalid GHE path format - missing components",
                    log_attributes([("path", serde_json::Value::String(path.clone()))]),
                );
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
                self.log(
                    LogLevel::Warn,
                    "Invalid path format - missing components",
                    log_attributes([("path", serde_json::Value::String(path.clone()))]),
                );
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
        self.log(
            LogLevel::Info,
            "Received git request",
            log_attributes([
                ("method", serde_json::Value::String(method)),
                ("path", serde_json::Value::String(path)),
                ("org", serde_json::Value::String(org.clone())),
                ("repo", serde_json::Value::String(repo.clone())),
                ("git_path", serde_json::Value::String(git_path.clone())),
                ("has_auth", serde_json::Value::Bool(has_auth)),
            ]),
        );

        if git_path == "info/refs" && !has_auth {
            self.log(
                LogLevel::Info,
                "Challenging unauthenticated info/refs request",
                log_attributes([
                    ("org", serde_json::Value::String(org.clone())),
                    ("repo", serde_json::Value::String(repo.clone())),
                ]),
            );
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
            Err(failure) => {
                let sanitized = sanitize_error(Some(&failure.error));
                self.log(
                    LogLevel::Error,
                    "Failed to proxy git request",
                    log_attributes([
                        ("error", serde_json::Value::String(sanitized.clone())),
                        ("org", serde_json::Value::String(org)),
                        ("repo", serde_json::Value::String(repo)),
                        ("git_path", serde_json::Value::String(git_path)),
                    ]),
                );
                failure.response.unwrap_or_else(|| {
                    http_error(
                        StatusCode::BAD_GATEWAY,
                        &format!("Proxy error: {sanitized}"),
                    )
                })
            }
        }
    }

    /// 对应 Go `forwardRequest`，验证仓库与 Git 子路径后流式转发请求和响应。
    ///
    /// 入站 `Authorization`、`Host` 与 `X-Token-Backend` 永远丢弃，随后写入 server
    /// 配置中的可信值。构造 URL 时逐段复刻 `url.PathEscape`，原始 query 则逐字追加；
    /// 这样 Git 的 service 参数和 `%2F` 路径编码均不会被二次解释。
    async fn forward_request(
        &self,
        request: Request<Body>,
        org: &str,
        repo: &str,
        git_path: &str,
    ) -> Result<Response, ForwardRequestFailure> {
        if !is_valid_git_path(git_path) {
            self.log(
                LogLevel::Warn,
                "Invalid git path detected",
                log_attributes([
                    ("git_path", serde_json::Value::String(git_path.to_owned())),
                    ("org", serde_json::Value::String(org.to_owned())),
                    ("repo", serde_json::Value::String(repo.to_owned())),
                ]),
            );
            return Err(GitProxyError::message("invalid git path").into());
        }

        let repo_key = format!("{org}/{repo}");
        let auth_token = match self
            .repo_tokens
            .get(&repo_key)
            .filter(|token| !token.is_empty())
        {
            Some(auth_token) => auth_token,
            None => {
                self.log(
                    LogLevel::Warn,
                    "No auth token configured for repository",
                    log_attributes([("repo", serde_json::Value::String(repo_key.clone()))]),
                );
                return Err(GitProxyError::message("repository not authorized").into());
            }
        };

        let (parts, body) = request.into_parts();
        let method = parts.method.as_str().to_owned();
        let base_url = self.config.session_ingress_url.trim_end_matches('/');
        let mut upstream_url = format!(
            "{base_url}/v1/session_ingress/session/{}/git_proxy/{}/{}.git/{}",
            go_path_escape(&self.config.session_id),
            go_path_escape(org),
            go_path_escape(repo),
            go_path_escape(git_path),
        );
        if let Some(query) = parts.uri.query().filter(|query| !query.is_empty()) {
            upstream_url.push('?');
            upstream_url.push_str(query);
        }
        self.log(
            LogLevel::Info,
            "Forwarding request to session ingress proxy",
            log_attributes([
                (
                    "upstream_url",
                    serde_json::Value::String(sanitize_string(&upstream_url)),
                ),
                ("repo", serde_json::Value::String(repo_key.clone())),
                ("git_path", serde_json::Value::String(git_path.to_owned())),
            ]),
        );

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

        // Body 保持流式；不先聚合到内存，才能复刻 Go 直接把 `r.Body` 交给 Transport。
        let upstream_request = self
            .http_client
            .request(parts.method, &upstream_url)
            .headers(headers)
            .body(reqwest::Body::wrap_stream(body.into_data_stream()))
            .build()
            .map_err(|error| {
                GitProxyError::message(format!("failed to create upstream request: {error}"))
            })?;

        self.log(
            LogLevel::Info,
            "Sending authenticated request to upstream",
            log_attributes([
                ("repo", serde_json::Value::String(repo_key.clone())),
                ("method", serde_json::Value::String(method)),
            ]),
        );
        let upstream_response =
            self.http_client
                .execute(upstream_request)
                .await
                .map_err(|error| {
                    GitProxyError::message(format!("failed to send request to upstream: {error}"))
                })?;
        let status = upstream_response.status();
        let response_headers = upstream_response.headers().clone();
        self.log(
            LogLevel::Info,
            "Received response from upstream",
            log_attributes([
                ("repo", serde_json::Value::String(repo_key.clone())),
                (
                    "status",
                    serde_json::Value::from(i64::from(status.as_u16())),
                ),
            ]),
        );

        let response_body = if status >= StatusCode::BAD_REQUEST {
            // Go 在写出上游 status/header 后才读取错误体；读取失败不能再改写成 502。
            match upstream_response.bytes().await {
                Ok(bytes) => logged_sanitized_body(
                    sanitize_string(&String::from_utf8_lossy(&bytes)),
                    self.logger.clone(),
                    repo_key.clone(),
                    status,
                ),
                Err(error) => {
                    let sanitized = sanitize_error(Some(&error));
                    self.log(
                        LogLevel::Error,
                        "Error reading error response body",
                        log_attributes([
                            // 后续还要把同一脱敏文本写入已提交状态码的响应体；日志
                            // handler 拥有自己的 JSON 值，因此这里只复制一次字符串。
                            ("error", serde_json::Value::String(sanitized.clone())),
                            ("repo", serde_json::Value::String(repo_key)),
                        ]),
                    );
                    // Go 此时已经执行 `WriteHeader(resp.StatusCode)`。如果上游没有
                    // Content-Type，外层 `http.Error` 的状态修改会被忽略，但错误文本
                    // 仍会追加到 body；已有 Content-Type 时外层不会再写任何内容。
                    let body = if response_headers
                        .get(CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .is_empty()
                    {
                        Body::from(format!("Proxy error: {sanitized}\n"))
                    } else {
                        Body::empty()
                    };
                    let mut response = Response::new(body);
                    *response.status_mut() = status;
                    *response.headers_mut() = response_headers;
                    return Err(ForwardRequestFailure::after_response(
                        GitProxyError::message(error.to_string()),
                        response,
                    ));
                }
            }
        } else {
            logged_streaming_body(upstream_response, self.logger.clone(), repo_key, status)
        };

        let mut response = Response::new(response_body);
        *response.status_mut() = status;
        *response.headers_mut() = response_headers;
        Ok(response)
    }

    /// 向构造时已具体化的 logger 记录结构化事件。
    ///
    /// # 参数
    /// - `level`：对应 Go `InfoContext`、`WarnContext` 或 `ErrorContext` 的级别。
    /// - `message`：与 Go 调用点逐字相同的消息。
    /// - `attributes`：保持 Go 参数顺序和原始标量类型的属性。
    ///
    /// # 副作用
    /// 调用 logger handler；写日志失败不会改变 HTTP 响应。
    #[track_caller]
    fn log(&self, level: LogLevel, message: &str, attributes: LogAttributes) {
        self.logger.log(level, message, attributes);
    }
}

/// 把一组按 Go 调用顺序给出的键值转换为保序日志属性。
///
/// # 参数
/// - `attributes`：固定长度的键值数组；值必须已经完成脱敏并保留原始 JSON 类型。
///
/// # 返回
/// 返回启用了 `preserve_order` 的 `LogAttributes`，供 logger 原样编码。
fn log_attributes<const N: usize>(attributes: [(&str, serde_json::Value); N]) -> LogAttributes {
    attributes
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

/// 响应体复制路径，用于区分 Go 中两个独立的 `io.WriteString`/`io.Copy` 错误日志。
#[derive(Clone, Copy)]
enum ResponseWriteMode {
    Sanitized,
    Streaming,
}

/// 在 Axum 异步发送响应体期间累计字节，并把完成/中断事件转换为 Go 日志。
///
/// Go 在 handler 内直接写 `ResponseWriter`，因此写入错误和完成字节数都在
/// `forwardRequest` 返回前可见；Axum 把真正的 socket 写入延后到返回 Response 后。
/// guard 跟随 body stream 生命周期，从而在正常 EOF 或客户端提前断开时补回同一组
/// 可观察日志。`completed` 防止显式错误之后的 Drop 再写一条重复事件。
struct ResponseWriteGuard {
    logger: Logger,
    repo: String,
    status: StatusCode,
    bytes_written: i64,
    mode: ResponseWriteMode,
    completed: bool,
}

impl ResponseWriteGuard {
    /// 创建尚未写出任何字节的响应日志 guard。
    ///
    /// # 参数
    /// - `logger`：与当前 handler 相同的具体 logger。
    /// - `repo`：`org/repo` 仓库键。
    /// - `status`：上游返回的 HTTP 状态。
    /// - `mode`：错误响应的脱敏写入或成功响应的流式复制路径。
    fn new(logger: Logger, repo: String, status: StatusCode, mode: ResponseWriteMode) -> Self {
        Self {
            logger,
            repo,
            status,
            bytes_written: 0,
            mode,
            completed: false,
        }
    }

    /// 累加一次成功发送给 Axum 的响应数据块长度。
    ///
    /// # 参数
    /// - `length`：数据块字节数；超过 `i64` 的理论值按 `i64::MAX` 饱和。
    fn add_bytes(&mut self, length: usize) {
        let length = i64::try_from(length).unwrap_or(i64::MAX);
        self.bytes_written = self.bytes_written.saturating_add(length);
    }

    /// 对应 Go `forwardRequest` 末尾的 `Successfully proxied request`。
    ///
    /// # 副作用
    /// 在 body 到达 EOF 后记录仓库、HTTP 状态和实际传输字节数，并关闭 Drop 失败
    /// 兜底；日志写入失败不会影响已经发送的 HTTP 响应。
    fn finish(&mut self) {
        self.completed = true;
        self.logger.log(
            LogLevel::Info,
            "Successfully proxied request",
            log_attributes([
                ("repo", serde_json::Value::String(self.repo.clone())),
                (
                    "status",
                    serde_json::Value::from(i64::from(self.status.as_u16())),
                ),
                ("bytes_written", serde_json::Value::from(self.bytes_written)),
            ]),
        );
    }

    /// 对应 Go 的响应体写入失败分支，并按写入模式选择精确消息。
    ///
    /// # 参数
    /// - `error`：已经过 `sanitizeError` 等价处理的错误文本。
    ///
    /// # 副作用
    /// 记录 `error`、`repo` 两个有序属性；调用后 guard 不会在 Drop 时重复记录。
    fn fail(&mut self, error: String) {
        self.completed = true;
        let attributes = log_attributes([
            ("error", serde_json::Value::String(error)),
            ("repo", serde_json::Value::String(self.repo.clone())),
        ]);
        match self.mode {
            ResponseWriteMode::Sanitized => self.logger.log(
                LogLevel::Error,
                "Error writing sanitized response body",
                attributes,
            ),
            ResponseWriteMode::Streaming => {
                self.logger
                    .log(LogLevel::Error, "Error streaming response body", attributes)
            }
        }
    }
}

impl Drop for ResponseWriteGuard {
    /// 把尚未到达 EOF 就被 Hyper 丢弃的 body 视为 Go `ResponseWriter` 的取消错误。
    ///
    /// 客户端断连时 Axum 不把 socket 错误传回 body stream；`context canceled` 是
    /// Go request context 在同一时刻提供给传输链的稳定错误文本，且不包含凭据。
    fn drop(&mut self) {
        if !self.completed {
            self.fail("context canceled".to_owned());
        }
    }
}

/// 把已聚合并脱敏的上游错误体包装为带完成日志的 Axum body。
///
/// # 参数
/// - `body`：脱敏后的 UTF-8 错误体。
/// - `logger`：当前 handler logger。
/// - `repo`：`org/repo` 仓库键。
/// - `status`：上游 HTTP 状态。
///
/// # 返回
/// 返回只产生一个数据块的 body；正常 EOF 记录成功字节数，提前丢弃记录 Go 的
/// `Error writing sanitized response body` 分支。
fn logged_sanitized_body(body: String, logger: Logger, repo: String, status: StatusCode) -> Body {
    let mut guard = ResponseWriteGuard::new(logger, repo, status, ResponseWriteMode::Sanitized);
    if body.is_empty() {
        guard.finish();
        return Body::empty();
    }

    let chunk = Bytes::from(body);
    let stream = futures_util::stream::unfold(Some((Some(chunk), guard)), |state| async move {
        let (chunk, mut guard) = state?;
        match chunk {
            Some(chunk) => {
                guard.add_bytes(chunk.len());
                Some((Ok::<Bytes, io::Error>(chunk), Some((None, guard))))
            }
            None => {
                guard.finish();
                None
            }
        }
    });
    Body::from_stream(stream)
}

/// 把 Reqwest 上游成功响应包装为边读边转发且可记录错误/字节数的 body。
///
/// # 参数
/// - `response`：尚未消费响应体的上游响应。
/// - `logger`：当前 handler logger。
/// - `repo`：`org/repo` 仓库键。
/// - `status`：上游 HTTP 状态。
///
/// # 返回
/// 返回保持 backpressure 的流式 body；上游读取错误原样传给 Hyper，并记录 Go 的
/// `Error streaming response body`，正常 EOF 则记录成功事件。
fn logged_streaming_body(
    response: reqwest::Response,
    logger: Logger,
    repo: String,
    status: StatusCode,
) -> Body {
    let upstream = Box::pin(response.bytes_stream());
    let guard = ResponseWriteGuard::new(logger, repo, status, ResponseWriteMode::Streaming);
    let stream = futures_util::stream::unfold(Some((upstream, guard)), |state| async move {
        let (mut upstream, mut guard) = state?;
        match upstream.as_mut().next().await {
            Some(Ok(chunk)) => {
                guard.add_bytes(chunk.len());
                Some((Ok(chunk), Some((upstream, guard))))
            }
            Some(Err(error)) => {
                guard.fail(sanitize_error(Some(&error)));
                Some((Err(error), None))
            }
            None => {
                guard.finish();
                None
            }
        }
    });
    Body::from_stream(stream)
}

/// 创建与 Go `http.DefaultClient` 传输语义相近、但明确禁止自动重定向的客户端。
///
/// 恢复出的闭包始终返回 `http.ErrUseLastResponse`。Reqwest 的 `Policy::none` 会把
/// 原始 3xx 状态、header 与 body 返回给 Git 客户端，并确保 Basic token 不会被
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

/// 构造 Go `http.Error` 兼容响应，包括末尾换行与 nosniff header。
fn http_error(status: StatusCode, message: &str) -> Response {
    let mut response = Response::new(Body::from(format!("{message}\n")));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    response
}

/// 把 URI 原始 path 的 `%XX` 恢复为 Go `URL.Path` 使用的字节序列。
///
/// Hyper 在 handler 中保留 escaped path，而 Go `net/http` 已经完成一次 PathUnescape。
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

/// 对可空错误执行与 Go `sanitizeError` 相同的文本脱敏。
pub(super) fn sanitize_error(error: Option<&dyn fmt::Display>) -> String {
    error.map_or_else(String::new, |error| sanitize_string(&error.to_string()))
}

/// 按 Go 中固定的正则顺序隐藏 Authorization、Anthropic key 与 token 参数。
///
/// 顺序不可交换：`sk-ant-ccsr-*` 必须先于通用 `sk-ant-*`，完整 Authorization
/// 形式也必须先于通用 Bearer/Basic。实现只识别 Go RE2 `\s` 的 ASCII 空白集合，
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

/// 判断子路径是否属于恢复出的 Git smart-HTTP 白名单。
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

/// 一次手工 RE2 扫描的结果：替换一个范围，或从新位置继续查找。
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

/// 消费 Go regexp `\s` 对应的 ASCII 空白，并返回第一个非空白下标。
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
