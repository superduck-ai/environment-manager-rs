//! 对应 Go `internal/api/client.go` 的共享 JSON HTTP 客户端。
//!
//! Reqwest client 在构造后复用连接池；默认超时、认证头、runner 版本头和错误文本保持
//! Go 行为。敏感 token 只写入请求头，不进入 tracing 字段。

use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Read as _};
use std::time::Duration;

use flate2::read::MultiGzDecoder;
use futures_util::StreamExt as _;
use reqwest::header::{
    ACCEPT_ENCODING, AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderName,
    HeaderValue, RANGE, USER_AGENT,
};
use reqwest::{Method, Request, Response};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::internal::logger::Logger;
use crate::internal::util::{claude_code_version, version};

use super::RetryConfig;

/// Go `net/http.Transport` 在调用方没有显式提供 UA 时写入的默认值。
///
/// Reqwest 默认不承诺同一 UA；显式固定该值既保持后端观测兼容，也避免把 Rust
/// HTTP 库及版本暴露到服务端日志中。调用方请求上的 `User-Agent` 仍会覆盖它。
pub(crate) const GO_DEFAULT_USER_AGENT: &str = "Go-http-client/1.1";

/// 标记本次响应的请求由兼容层自动协商了 gzip。
///
/// Go 只会自动解压由 Transport 自己添加 `Accept-Encoding: gzip` 的请求；调用方
/// 显式设置同名 header 时，即使值也是 gzip，也必须把响应原样交给调用方。
#[derive(Clone, Copy, Debug)]
struct AutoGzipResponse;

/// 自定义 HTTP transport 注入的 Go `Response.Body.Close` 失败。
///
/// Reqwest 的标准响应 body 通过 Drop 释放连接，Drop 不返回错误；Go 的标准
/// `net/http.Transport` 在本项目这些读取/提前关闭路径上也通常返回 `nil`。不过 Go
/// API 允许测试或嵌入方提供自定义 `RoundTripper` 与会失败的 `io.ReadCloser`，三个
/// 调用点会把该失败写入业务 logger。扩展标记为这一可观察边界保留等价注入通道，
/// 同时避免把 body 的读取错误错误地伪装成 Close 错误。
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GoResponseCloseError {
    message: String,
}

impl GoResponseCloseError {
    /// 创建一个供自定义 transport 或故障注入测试附加到响应上的 Close 错误。
    ///
    /// # 参数
    /// - `message`：对应 Go `error.Error()` 的原始文本；不得包含 Rust error wrapper。
    ///
    /// # 返回
    /// 返回可插入 `reqwest::Response::extensions_mut()` 的不可变错误标记。
    #[cfg(test)]
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// 返回与 Go `slog` 的 `error` 属性相同的错误文本。
    pub(crate) fn message(&self) -> &str {
        &self.message
    }
}

/// 读取自定义 transport 附加的 Go response body Close 错误。
///
/// # 参数
/// - `response`：尚未被 body reader 消费的 Reqwest 响应引用。
///
/// # 返回
/// 标记存在时复制其错误文本；标准 Reqwest transport 返回 `None`。
///
/// # 兼容性
/// 调用方必须在消费 `Response` 前调用本函数，并在业务返回值已经计算完成后记录，
/// 从而复刻 Go `defer` 的执行顺序。
pub(crate) fn go_response_close_error(response: &Response) -> Option<String> {
    response
        .extensions()
        .get::<GoResponseCloseError>()
        .map(|error| error.message().to_owned())
}

/// 响应 body 读取失败的稳定兼容错误。
///
/// 该类型刻意不保存 Reqwest 的 `Display` 文本，避免依赖升级改变 CLI 错误，也避免
/// 底层错误意外带出带凭据 URL。`message` 使用 Go 常见的 body 读取错误词汇。
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GoHttpBodyError {
    message: &'static str,
}

impl fmt::Display for GoHttpBodyError {
    /// 输出稳定的 Go 风格 body 错误文本。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for GoHttpBodyError {}

/// API 层统一错误；`EndpointNotImplemented` 保留 Go sentinel 的可判定语义。
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// 普通兼容错误文本。
    #[error("{0}")]
    Message(String),
    /// HTTP 501，调用方可降级到旧端点。
    #[error("{0}")]
    EndpointNotImplemented(String),
}

impl ApiError {
    /// 构造普通文本错误，集中避免调用点暴露内部错误类型。
    pub fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }

    /// 判断错误是否对应 Go `ErrEndpointNotImplemented` sentinel。
    pub fn is_endpoint_not_implemented(&self) -> bool {
        matches!(self, Self::EndpointNotImplemented(_))
    }

    /// 增加与 Go `fmt.Errorf("context: %w", err)` 等价的文本上下文。
    ///
    /// 普通错误保持普通 variant；501 sentinel 则在增加前缀后继续保持
    /// `EndpointNotImplemented`，确保诊断上传层仍能可靠执行兼容降级。
    pub fn with_context(self, context: &str) -> Self {
        match self {
            Self::Message(message) => Self::Message(format!("{context}: {message}")),
            Self::EndpointNotImplemented(message) => {
                Self::EndpointNotImplemented(format!("{context}: {message}"))
            }
        }
    }
}

/// `HttpClient` 构造选项，对应 Go `ClientOptions`。
#[derive(Clone, Debug)]
pub struct ClientOptions {
    /// 整个请求的超时；`None` 或零值使用 Go 默认 5 分钟。
    pub timeout: Option<Duration>,
    /// 重试策略；`None` 使用默认值。
    pub retry_config: Option<RetryConfig>,
    /// 每个 JSON POST 附带的默认 header。
    pub default_headers: BTreeMap<String, String>,
}

impl Default for ClientOptions {
    /// 返回与 Go 零值 options 相同的构造参数。
    fn default() -> Self {
        Self {
            timeout: None,
            retry_config: None,
            default_headers: BTreeMap::new(),
        }
    }
}

/// 可复用的异步 HTTP client。
#[derive(Clone)]
pub struct HttpClient {
    /// 去除尾部 `/` 的 API base URL。
    pub(crate) base_url: String,
    /// Reqwest 连接池。
    pub(crate) http_client: reqwest::Client,
    /// 默认重试配置。
    pub(crate) retry_config: RetryConfig,
    /// 对应 Go `HttpClient.logger`，所有重试事件必须进入同一个 `slog` 兼容 handler。
    pub(crate) logger: Option<Logger>,
    /// 已验证的默认 header。
    default_headers: HeaderMap,
    /// 单元测试专用的 Go `Body.Close` 故障；生产构建不包含该状态。
    #[cfg(test)]
    pub(crate) response_close_error_for_test: Option<String>,
}

impl HttpClient {
    /// 对应 Go `NewHttpClient`，缺少 scheme 时补 `https://` 并保存调用方 logger。
    ///
    /// # 参数
    /// - `base_url`：后端根 URL；未显式提供 HTTP(S) scheme 时按 Go 行为补 HTTPS。
    /// - `logger`：生产路径传入的结构化 logger；测试可以传 `None` 关闭事件输出。
    /// - `retry_config`：可选重试策略；为空时使用 Go 的三次指数退避默认值。
    ///
    /// # 返回
    /// 成功时返回共享连接池；HTTP client 配置失败时返回稳定的 API 错误。
    pub fn new(
        base_url: &str,
        logger: Option<Logger>,
        retry_config: Option<RetryConfig>,
    ) -> Result<Self, ApiError> {
        let normalized = if base_url.starts_with("http://") || base_url.starts_with("https://") {
            base_url.to_owned()
        } else {
            format!("https://{base_url}")
        };
        Self::with_options(
            &normalized,
            logger,
            ClientOptions {
                retry_config,
                ..ClientOptions::default()
            },
        )
    }

    /// 对应 Go `NewHttpClientWithOptions`，创建带 30 秒连接超时的连接池。
    ///
    /// `logger` 与返回 client 绑定，确保 `RetryableHTTPDo` 的 attempt/backoff 日志使用
    /// 调用链原有 handler、绑定属性与 Go source 映射，而不是无订阅者的全局 tracing。
    pub fn with_options(
        base_url: &str,
        logger: Option<Logger>,
        options: ClientOptions,
    ) -> Result<Self, ApiError> {
        // Go 把 `ClientOptions.Timeout == 0` 当作未配置，而不是“立即超时”。Rust 用
        // Option 表达缺省，但调用方仍可能显式传入 Duration::ZERO，必须合并处理。
        let timeout = options
            .timeout
            .filter(|timeout| !timeout.is_zero())
            .unwrap_or(Duration::from_secs(5 * 60));
        let http_client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| ApiError::message(format!("failed to create HTTP client: {error}")))?;

        let mut default_headers = HeaderMap::new();
        for (key, value) in options.default_headers {
            let name = HeaderName::from_bytes(key.as_bytes()).map_err(|error| {
                ApiError::message(format!("invalid default header {key}: {error}"))
            })?;
            let value = HeaderValue::from_str(&value).map_err(|error| {
                ApiError::message(format!("invalid default header {key}: {error}"))
            })?;
            default_headers.insert(name, value);
        }

        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            http_client,
            retry_config: options.retry_config.unwrap_or_default(),
            logger,
            default_headers,
            #[cfg(test)]
            response_close_error_for_test: None,
        })
    }

    /// 使用调用方已经配置好的 Reqwest 连接池构造共享 API client。
    ///
    /// 对应 Go 中把自定义 `*http.Client` 直接传给 `podmonitor.NewLeaseManager` 的路径。
    /// 这里不会重建连接池，也不会覆盖 client 自带的默认 header、代理或超时；仅保存
    /// 去除尾部 `/` 的 base URL，以及 podmonitor 不会使用到的通用重试默认值。
    ///
    /// # 参数
    /// - `base_url`：后端 API 根地址，保持原 scheme/path，仅移除全部尾随斜杠。
    /// - `http_client`：所有权转入返回值的已配置 Reqwest client；clone 仍共享连接池。
    /// - `logger`：沿用上层 Go `slog.Logger` 的兼容 facade；测试可传 `None` 静默。
    /// - `retry_config`：通用 JSON API 的可选重试参数；为空时采用默认值。
    ///
    /// # 兼容性
    /// BYOC transport 把认证信息放在 Reqwest client 的 default headers 中，因此这里
    /// 必须保留原 client，不能只复制 timeout 后重新构造，否则 heartbeat/stop 会丢失认证。
    #[must_use]
    pub(crate) fn from_reqwest_client(
        base_url: &str,
        http_client: reqwest::Client,
        logger: Option<Logger>,
        retry_config: Option<RetryConfig>,
    ) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            http_client,
            retry_config: retry_config.unwrap_or_default(),
            logger,
            default_headers: HeaderMap::new(),
            #[cfg(test)]
            response_close_error_for_test: None,
        }
    }

    /// 为下一步单元测试请求注入 Go `Response.Body.Close` 错误。
    ///
    /// # 参数
    /// - `message`：自定义 `io.ReadCloser.Close` 返回的 Go error 文本。
    ///
    /// # 副作用
    /// 后续所有由此测试 client 返回的非重试响应都会带同一扩展标记；本入口不会编译
    /// 到生产二进制，防止后端响应控制本地日志故障注入。
    #[cfg(test)]
    pub(crate) fn inject_response_close_error_for_test(&mut self, message: impl Into<String>) {
        self.response_close_error_for_test = Some(message.into());
    }

    /// 返回规范化后的 base URL，供 endpoint 适配器拼接路径。
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// 对应 Go `setAuthHeader`，设置 Bearer header 并拒绝非法控制字符。
    pub(crate) fn set_auth_header(
        &self,
        request: &mut Request,
        auth_token: &str,
    ) -> Result<(), ApiError> {
        let value = HeaderValue::from_str(&format!("Bearer {auth_token}"))
            .map_err(|error| ApiError::message(format!("invalid authorization token: {error}")))?;
        request.headers_mut().insert(AUTHORIZATION, value);
        Ok(())
    }

    /// 对应 Go `PostJSON`，发送 JSON 并忽略成功响应 body。
    pub async fn post_json<T: Serialize + ?Sized>(
        &self,
        cancellation: &CancellationToken,
        endpoint: &str,
        request_body: &T,
        auth_token: &str,
        operation_name: &str,
    ) -> Result<(), ApiError> {
        self.do_post_json(
            cancellation,
            endpoint,
            request_body,
            auth_token,
            operation_name,
        )
        .await
        .map(|_| ())
    }

    /// 对应 Go `PostJSONWithResponse`，成功后反序列化响应 body。
    pub async fn post_json_with_response<T, R>(
        &self,
        cancellation: &CancellationToken,
        endpoint: &str,
        request_body: &T,
        auth_token: &str,
        operation_name: &str,
    ) -> Result<R, ApiError>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let response = self
            .do_post_json(
                cancellation,
                endpoint,
                request_body,
                auth_token,
                operation_name,
            )
            .await?;
        serde_json::from_slice(&response).map_err(|error| {
            ApiError::message(format!(
                "failed to parse {operation_name} response: {error}"
            ))
        })
    }

    /// 对应 Go `doPostJSON`，构造兼容 header、执行重试并解释 HTTP 状态码。
    pub(crate) async fn do_post_json<T: Serialize + ?Sized>(
        &self,
        cancellation: &CancellationToken,
        endpoint: &str,
        request_body: &T,
        auth_token: &str,
        operation_name: &str,
    ) -> Result<Vec<u8>, ApiError> {
        let body = marshal_go_json(request_body).map_err(|error| {
            ApiError::message(format!(
                "failed to marshal {operation_name} request: {error}"
            ))
        })?;
        self.do_post_json_bytes(cancellation, endpoint, body, auth_token, operation_name)
            .await
    }

    /// 发送调用方已经按 Go `encoding/json` 规则编码好的 JSON body。
    ///
    /// 该入口只供 CCR 的 marshal→`map[string]any`→marshal 兼容路径使用；普通调用
    /// 应使用 `do_post_json`，由后者负责序列化错误上下文。认证、默认 header、重试、
    /// 状态码和响应读取仍集中在这里，避免预编码分支悄悄偏离共享 HTTP 行为。
    pub(crate) async fn do_post_json_bytes(
        &self,
        cancellation: &CancellationToken,
        endpoint: &str,
        body: Vec<u8>,
        auth_token: &str,
        operation_name: &str,
    ) -> Result<Vec<u8>, ApiError> {
        let mut request = self
            .http_client
            .request(Method::POST, endpoint)
            .body(body)
            .build()
            .map_err(|error| {
                ApiError::message(format!(
                    "failed to create {operation_name} request: {error}"
                ))
            })?;
        self.set_auth_header(&mut request, auth_token)?;
        request
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        request.headers_mut().extend(self.default_headers.clone());
        add_version_headers(request.headers_mut())?;

        let response = self
            .retryable_http_do(cancellation, &request, Some(&self.retry_config))
            .await
            .map_err(|error| {
                ApiError::message(format!("failed to post {operation_name}: {error}"))
            })?;
        let status = response.status();
        // Go 的通用 POST 路径故意忽略 io.ReadAll 错误，但会保留错误前已经读到的
        // 字节。兼容 helper 因此不能使用 `Response::bytes().unwrap_or_default()`；后者
        // 会在截断响应上把可见的 partial body 一并丢掉。
        let response_body = read_go_response_body_ignoring_errors(response, cancellation).await;
        if status == reqwest::StatusCode::OK {
            return Ok(response_body);
        }
        let response_text = String::from_utf8_lossy(&response_body);
        if status == reqwest::StatusCode::NOT_IMPLEMENTED {
            return Err(ApiError::EndpointNotImplemented(format!(
                "{operation_name} responded with {} (not implemented): {response_text}: endpoint not implemented",
                status.as_u16()
            )));
        }
        Err(ApiError::message(format!(
            "{operation_name} responded with non-OK {}: {response_text}",
            status.as_u16()
        )))
    }
}

/// 写入每个环境管理器请求都必须携带的版本 header。
pub(crate) fn add_version_headers(headers: &mut HeaderMap) -> Result<(), ApiError> {
    let runner = HeaderValue::from_str(version()).map_err(|error| {
        ApiError::message(format!("invalid environment runner version: {error}"))
    })?;
    headers.insert("x-environment-runner-version", runner);
    if let Some(claude_version) = claude_code_version()
        && !claude_version.is_empty()
    {
        let value = HeaderValue::from_str(claude_version)
            .map_err(|error| ApiError::message(format!("invalid Claude Code version: {error}")))?;
        headers.insert("x-claude-code-version", value);
    }
    Ok(())
}

/// 在发送前复刻 Go `net/http.Transport` 的自动 gzip 协商规则。
///
/// 只有非 HEAD、没有非空 `Range` 且没有非空 `Accept-Encoding` 的请求才由 Transport
/// 自动添加 gzip。返回值必须随最终响应保存；只有返回 `true` 时 Go 才会自动解压。
pub(crate) fn prepare_go_transport_request(request: &mut Request) -> bool {
    // Go 在序列化 Request 时才补默认 UA，因此即使底层 client 来自 BYOC 等外部
    // 构造路径也具有同一行为。显式空值表示“完全不发送 UA”；Reqwest 若直接保留
    // 空 HeaderValue 会写出空 header，必须先删除才能等价于 Go。
    match request.headers().get(USER_AGENT) {
        None => {
            request
                .headers_mut()
                .insert(USER_AGENT, HeaderValue::from_static(GO_DEFAULT_USER_AGENT));
        }
        Some(value) if value.as_bytes().is_empty() => {
            request.headers_mut().remove(USER_AGENT);
        }
        Some(_) => {}
    }

    let header_is_empty = |name: &reqwest::header::HeaderName| {
        request
            .headers()
            .get(name)
            .is_none_or(|value| value.as_bytes().is_empty())
    };
    let auto_gzip = request.method() != Method::HEAD
        && header_is_empty(&ACCEPT_ENCODING)
        && header_is_empty(&RANGE);
    if auto_gzip {
        // Go 的 extraHeaders 会让传输层实际只写出 gzip；这里直接替换显式空值，
        // 避免 HeaderMap 留下一个空 header 和一个 gzip header 的多值差异。
        request
            .headers_mut()
            .insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));
    }
    auto_gzip
}

/// 把请求阶段的自动 gzip 决策附加到响应，供 body reader 决定是否解压。
pub(crate) fn mark_go_transport_response(response: &mut Response, auto_gzip: bool) {
    if auto_gzip {
        response.extensions_mut().insert(AutoGzipResponse);
    }
}

/// 严格读取响应 body，并复刻 Go Transport 的自动 gzip 解压。
///
/// SessionContext 与 Whoami 的 Go 路径会检查 `io.ReadAll` 错误，因此该函数在保留
/// partial bytes 的同时返回稳定错误。取消、client timeout、截断和 gzip 损坏均不会
/// 暴露 Reqwest/flate2 的版本相关文本。
pub(crate) async fn read_go_response_body(
    response: Response,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, GoHttpBodyError> {
    let outcome = read_go_response_body_outcome(response, cancellation).await;
    match outcome.error {
        Some(error) => Err(error),
        None => Ok(outcome.body),
    }
}

/// 读取响应 body，但像 Go 的通用 POST/ACK 分支一样忽略读取错误并保留 partial bytes。
pub(crate) async fn read_go_response_body_ignoring_errors(
    response: Response,
    cancellation: &CancellationToken,
) -> Vec<u8> {
    read_go_response_body_outcome(response, cancellation)
        .await
        .body
}

/// body reader 的内部结果；错误路径仍携带已经读取或解压出的字节。
struct GoHttpBodyOutcome {
    body: Vec<u8>,
    error: Option<GoHttpBodyError>,
}

/// 收集传输流、处理取消并按 Go 的“仅自动协商才解 gzip”规则生成最终 body。
async fn read_go_response_body_outcome(
    response: Response,
    cancellation: &CancellationToken,
) -> GoHttpBodyOutcome {
    let should_decompress = response.extensions().get::<AutoGzipResponse>().is_some()
        && response
            .headers()
            .get(CONTENT_ENCODING)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("gzip"));
    let mut stream = response.bytes_stream();
    let mut encoded = Vec::new();
    let mut transport_error = None;

    loop {
        let next = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                transport_error = Some(GoHttpBodyError { message: "context canceled" });
                break;
            }
            next = stream.next() => next,
        };
        match next {
            Some(Ok(chunk)) => encoded.extend_from_slice(&chunk),
            Some(Err(error)) => {
                transport_error = Some(classify_body_transport_error(&error));
                break;
            }
            None => break,
        }
    }

    if !should_decompress {
        return GoHttpBodyOutcome {
            body: encoded,
            error: transport_error,
        };
    }

    // Go 的 lazy gzipReader 把完全空的底层 body 首次返回的 io.EOF 交给
    // io.ReadAll；ReadAll 将该 EOF 视为正常结束。因此只有“零字节”在带 gzip
    // header 时仍成功，任意非空截断 header 才是 unexpected EOF。
    if encoded.is_empty() && transport_error.is_none() {
        return GoHttpBodyOutcome {
            body: Vec::new(),
            error: None,
        };
    }

    let mut decoded = Vec::new();
    let gzip_result = MultiGzDecoder::new(encoded.as_slice()).read_to_end(&mut decoded);
    match gzip_result {
        Ok(_) => GoHttpBodyOutcome {
            body: decoded,
            // 完整 gzip member 自带结束边界；Go gzip reader 到达该边界后也不会再
            // 为多报的 HTTP Content-Length 强制读取，因此成功解压优先于传输尾错。
            error: None,
        },
        Err(error) => GoHttpBodyOutcome {
            body: decoded,
            error: Some(classify_gzip_error(&error)),
        },
    }
}

/// 把 Reqwest body 错误归并为 Go 调用方稳定可见的分类。
fn classify_body_transport_error(error: &reqwest::Error) -> GoHttpBodyError {
    if error.is_timeout() {
        return GoHttpBodyError {
            message: "context deadline exceeded (Client.Timeout or context cancellation while reading body)",
        };
    }
    if find_io_error_kind(error) == Some(io::ErrorKind::UnexpectedEof) {
        return GoHttpBodyError {
            message: "unexpected EOF",
        };
    }
    // Hyper 对 Content-Length 截断通常不保留 std::io::UnexpectedEof，但仍会把
    // body 标为 decode error；Go 对同一 wire 统一返回 unexpected EOF。
    if error.is_body() || error.is_decode() {
        return GoHttpBodyError {
            message: "unexpected EOF",
        };
    }
    GoHttpBodyError {
        message: "transport body read error",
    }
}

/// 把 flate2 错误映射为 Go `compress/gzip` 常用错误文本。
fn classify_gzip_error(error: &io::Error) -> GoHttpBodyError {
    let text = error.to_string().to_ascii_lowercase();
    if error.kind() == io::ErrorKind::UnexpectedEof || text.contains("unexpected end") {
        return GoHttpBodyError {
            message: "unexpected EOF",
        };
    }
    if text.contains("checksum") || text.contains("corrupt") {
        return GoHttpBodyError {
            message: "gzip: invalid checksum",
        };
    }
    GoHttpBodyError {
        message: "gzip: invalid header",
    }
}

/// 在错误 source chain 中查找底层 `std::io::ErrorKind`。
///
/// 某些 connector 会先包一层 `Other`，再把真正的 ConnectionRefused/TimedOut 放在
/// 更深处；优先返回具体 kind，避免外层无信息 wrapper 把稳定的 Linux 原因吞掉。
fn find_io_error_kind(error: &(dyn std::error::Error + 'static)) -> Option<io::ErrorKind> {
    let mut current = Some(error);
    let mut fallback = None;
    while let Some(source) = current {
        if let Some(io_error) = source.downcast_ref::<io::Error>() {
            let kind = io_error.kind();
            if kind != io::ErrorKind::Other {
                return Some(kind);
            }
            fallback = Some(kind);
        }
        current = source.source();
    }
    fallback
}

/// 保存一次 HTTP attempt 的 Go `url.Error` 格式化上下文。
///
/// Request 在交给 Reqwest 后会被 move；预先保存 method、脱敏 URL 和 dial authority
/// 才能在错误分支稳定复刻 `Post "...": ...`，且不会在日志中泄漏 URL password。
#[derive(Clone, Debug)]
pub(crate) struct GoRequestErrorContext {
    operation: String,
    redacted_url: String,
    dial_authority: String,
}

impl GoRequestErrorContext {
    /// 从待发送请求提取 Go `url.Error` 所需的非敏感字段。
    pub(crate) fn from_request(request: &Request) -> Self {
        let method = request.method().as_str().to_ascii_lowercase();
        let mut characters = method.chars();
        let operation = characters.next().map_or_else(String::new, |first| {
            first.to_uppercase().collect::<String>() + characters.as_str()
        });

        let mut redacted = request.url().clone();
        if redacted.password().is_some() {
            let _ = redacted.set_password(Some("xxxxx"));
        }
        let host = redacted.host_str().unwrap_or_default();
        let host = if host.contains(':') {
            format!("[{host}]")
        } else {
            host.to_owned()
        };
        let dial_authority = redacted
            .port_or_known_default()
            .map_or(host.clone(), |port| format!("{host}:{port}"));
        Self {
            operation,
            redacted_url: redacted.into(),
            dial_authority,
        }
    }

    /// 生成 context 已取消时 Go `url.Error.Error()` 的文本。
    pub(crate) fn canceled(&self) -> String {
        self.with_cause("context canceled")
    }

    /// 把 Reqwest 错误分类为稳定的 Go transport 文案。
    pub(crate) fn request_error(&self, error: &reqwest::Error) -> String {
        let cause = match find_io_error_kind(error) {
            Some(io::ErrorKind::ConnectionRefused) => format!(
                "dial tcp {}: connect: connection refused",
                self.dial_authority
            ),
            // Reqwest 把 Dialer connect timeout 和整个 Client deadline 都标为 timeout；
            // 只有前者的 source chain 含 io::TimedOut，必须先判定才能还原 Go 文本。
            Some(io::ErrorKind::TimedOut) => {
                format!("dial tcp {}: i/o timeout", self.dial_authority)
            }
            Some(io::ErrorKind::UnexpectedEof) => "EOF".to_owned(),
            Some(io::ErrorKind::ConnectionReset) => "connection reset by peer".to_owned(),
            Some(io::ErrorKind::BrokenPipe) => "broken pipe".to_owned(),
            _ if error.is_timeout() => {
                "context deadline exceeded (Client.Timeout exceeded while awaiting headers)"
                    .to_owned()
            }
            _ => "transport error".to_owned(),
        };
        self.with_cause(&cause)
    }

    /// 按 Go `%q` URL 形态拼接 operation 和原因。
    fn with_cause(&self, cause: &str) -> String {
        format!("{} {:?}: {cause}", self.operation, self.redacted_url)
    }
}

/// 使用 Serde 编码强类型值，并补齐 Go `json.Marshal` 默认 HTML 转义。
///
/// 该函数不擅自重排 struct 字段；Go 对 struct 也保持声明顺序。调用方若对应 Go map，
/// 必须传入 `BTreeMap`。输出没有末尾换行，与 `json.Marshal` 一致。
pub(crate) fn marshal_go_json<T: Serialize + ?Sized>(
    value: &T,
) -> Result<Vec<u8>, serde_json::Error> {
    let encoded = serde_json::to_string(value)?;
    Ok(escape_go_json_html(encoded).into_bytes())
}

/// 把一棵来自 Go `json.Unmarshal(..., *any)` 的动态 JSON 值编码为紧凑 JSON。
///
/// 动态 object 的键必须递归排序，number 必须先收窄到 IEEE-754 float64；CCR event
/// 正好经历这次 round-trip，不能沿用强类型 Serde 的字段顺序和 64 位整数精度。
pub(crate) fn marshal_go_dynamic_json(value: &Value) -> Result<Vec<u8>, serde_json::Error> {
    let mut output = String::new();
    write_go_dynamic_json_value(value, &mut output)?;
    Ok(output.into_bytes())
}

/// 递归写入一项紧凑的 Go `any` JSON 值。
fn write_go_dynamic_json_value(
    value: &Value,
    output: &mut String,
) -> Result<(), serde_json::Error> {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Value::Number(value) => output.push_str(&format_go_dynamic_number(value)),
        Value::String(value) => output.push_str(&go_json_string(value)?),
        Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                write_go_dynamic_json_value(value, output)?;
            }
            output.push(']');
        }
        Value::Object(values) => {
            output.push('{');
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_unstable_by_key(|(key, _)| *key);
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                output.push_str(&go_json_string(key)?);
                output.push(':');
                write_go_dynamic_json_value(value, output)?;
            }
            output.push('}');
        }
    }
    Ok(())
}

/// 使用 Serde 完成控制字符转义，再补 Go 默认启用的 HTML/行分隔符转义。
fn go_json_string(value: &str) -> Result<String, serde_json::Error> {
    serde_json::to_string(value).map(escape_go_json_html)
}

/// 对一段已经合法的紧凑 JSON 补齐 Go `encodeState.string` 的额外转义。
fn escape_go_json_html(encoded: String) -> String {
    encoded
        .replace('&', "\\u0026")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}

/// 把动态 JSON number 模拟为 Go `json.Unmarshal` 的 float64 后按 JSON 阈值编码。
fn format_go_dynamic_number(number: &serde_json::Number) -> String {
    let value = number
        .as_f64()
        .expect("serde_json::Number without arbitrary_precision always fits f64");
    let shortest = value.to_string();
    let (negative, unsigned) = shortest
        .strip_prefix('-')
        .map_or((false, shortest.as_str()), |value| (true, value));
    let (mantissa, parsed_exponent) = unsigned
        .split_once(['e', 'E'])
        .map_or((unsigned, 0_i32), |(mantissa, exponent)| {
            (mantissa, exponent.parse::<i32>().unwrap_or_default())
        });
    let decimal_index = mantissa.find('.').unwrap_or(mantissa.len());
    let mut digits = mantissa.replace('.', "");
    let Some(first_non_zero) = digits.find(|character| character != '0') else {
        return if negative { "-0" } else { "0" }.to_owned();
    };
    let removed_leading = i32::try_from(first_non_zero).unwrap_or(i32::MAX);
    digits.drain(..first_non_zero);
    let decimal_index = i32::try_from(decimal_index).unwrap_or(i32::MAX) - removed_leading;
    let scientific_exponent = parsed_exponent + decimal_index - 1;
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
    }

    let absolute = value.abs();
    let use_exponent = absolute != 0.0 && !(1e-6..1e21).contains(&absolute);
    let mut formatted = String::new();
    if negative {
        formatted.push('-');
    }
    if use_exponent {
        formatted.push(digits.as_bytes()[0] as char);
        if digits.len() > 1 {
            formatted.push('.');
            formatted.push_str(&digits[1..]);
        }
        formatted.push('e');
        if scientific_exponent >= 0 {
            formatted.push('+');
        }
        formatted.push_str(&scientific_exponent.to_string());
        return formatted;
    }

    let fixed_decimal_index = scientific_exponent + 1;
    if fixed_decimal_index <= 0 {
        formatted.push_str("0.");
        for _ in 0..fixed_decimal_index.unsigned_abs() {
            formatted.push('0');
        }
        formatted.push_str(&digits);
    } else {
        let fixed_decimal_index = usize::try_from(fixed_decimal_index).unwrap_or(usize::MAX);
        if fixed_decimal_index >= digits.len() {
            formatted.push_str(&digits);
            for _ in digits.len()..fixed_decimal_index {
                formatted.push('0');
            }
        } else {
            formatted.push_str(&digits[..fixed_decimal_index]);
            formatted.push('.');
            formatted.push_str(&digits[fixed_decimal_index..]);
        }
    }
    formatted
}
