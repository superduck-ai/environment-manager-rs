//! 共享 JSON HTTP 客户端。
//!
//! Reqwest client 在构造后复用连接池；敏感 token 只写入请求头，不进入 tracing 字段。

use std::collections::BTreeMap;
use std::time::Duration;

use futures_util::StreamExt as _;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, Request, Response};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::internal::util::{claude_code_version, version};

use super::RetryConfig;

/// API 层统一错误；`EndpointNotImplemented` 保留 sentinel 的可判定语义。
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// 普通稳定错误文本。
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

    /// 判断错误是否为可降级的 501 sentinel。
    pub fn is_endpoint_not_implemented(&self) -> bool {
        matches!(self, Self::EndpointNotImplemented(_))
    }

    /// 增加稳定的文本上下文，同时保留错误分类。
    ///
    /// 普通错误保持普通 variant；501 sentinel 则在增加前缀后继续保持
    /// `EndpointNotImplemented`，确保诊断上传层仍能可靠降级到旧端点。
    pub fn with_context(self, context: &str) -> Self {
        match self {
            Self::Message(message) => Self::Message(format!("{context}: {message}")),
            Self::EndpointNotImplemented(message) => {
                Self::EndpointNotImplemented(format!("{context}: {message}"))
            }
        }
    }
}

/// `HttpClient` 构造选项。
#[derive(Clone, Debug)]
pub struct ClientOptions {
    /// 整个请求的超时；`None` 或零值使用 默认 5 分钟。
    pub timeout: Option<Duration>,
    /// 重试策略；`None` 使用默认值。
    pub retry_config: Option<RetryConfig>,
    /// 每个 JSON POST 附带的默认 header。
    pub default_headers: BTreeMap<String, String>,
}

impl Default for ClientOptions {
    /// 返回空超时、默认重试策略和空 header 的构造参数。
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
    /// 已验证的默认 header。
    default_headers: HeaderMap,
}

impl HttpClient {
    /// 缺少 scheme 时补 `https://` 并创建 HTTP client。
    ///
    /// # 参数
    /// - `base_url`：后端根 URL；未显式提供 HTTP(S) scheme 时按 补 HTTPS。
    /// - `retry_config`：可选重试策略；为空时使用 三次指数退避默认值。
    ///
    /// # 返回
    /// 成功时返回共享连接池；HTTP client 配置失败时返回稳定的 API 错误。
    pub fn new(base_url: &str, retry_config: Option<RetryConfig>) -> Result<Self, ApiError> {
        let normalized = if base_url.starts_with("http://") || base_url.starts_with("https://") {
            base_url.to_owned()
        } else {
            format!("https://{base_url}")
        };
        Self::with_options(
            &normalized,
            ClientOptions {
                retry_config,
                ..ClientOptions::default()
            },
        )
    }

    /// 创建带 30 秒连接超时的连接池。
    ///
    /// 重试 attempt/backoff 事件直接写入进程 tracing subscriber。
    pub fn with_options(base_url: &str, options: ClientOptions) -> Result<Self, ApiError> {
        // 把 `ClientOptions.Timeout == 0` 当作未配置，而不是“立即超时”。Rust 用
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
            default_headers,
        })
    }

    /// 使用调用方已经配置好的 Reqwest 连接池构造共享 API client。
    ///
    /// 把共享 HTTP client 直接传给 podmonitor lease manager。
    /// 这里不会重建连接池，也不会覆盖 client 自带的默认 header、代理或超时；仅保存
    /// 去除尾部 `/` 的 base URL，以及 podmonitor 不会使用到的通用重试默认值。
    ///
    /// # 参数
    /// - `base_url`：后端 API 根地址，保持原 scheme/path，仅移除全部尾随斜杠。
    /// - `http_client`：所有权转入返回值的已配置 Reqwest client；clone 仍共享连接池。
    /// - `retry_config`：通用 JSON API 的可选重试参数；为空时采用默认值。
    ///
    /// # Transport 约束
    /// BYOC transport 把认证信息放在 Reqwest client 的 default headers 中，因此这里
    /// 必须保留原 client，不能只复制 timeout 后重新构造，否则 heartbeat/stop 会丢失认证。
    #[must_use]
    pub(crate) fn from_reqwest_client(
        base_url: &str,
        http_client: reqwest::Client,
        retry_config: Option<RetryConfig>,
    ) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            http_client,
            retry_config: retry_config.unwrap_or_default(),
            default_headers: HeaderMap::new(),
        }
    }

    /// 返回规范化后的 base URL，供 endpoint 适配器拼接路径。
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// 设置 Bearer header 并拒绝非法控制字符。
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

    /// 发送 JSON 并忽略成功响应 body。
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

    /// 成功后反序列化响应 body。
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

    /// 构造协议 header、执行重试并解释 HTTP 状态码。
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

    /// 发送调用方已经按 wire 规则编码好的 JSON body。
    ///
    /// 该入口只供 CCR 的动态 JSON 二次编码路径使用；普通调用
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
        // 错误响应仍应保留传输中断前已经收到的 body，便于显示服务端诊断。
        let response_body = read_response_body_ignoring_errors(response, cancellation).await;
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

/// 严格读取响应 body，并在取消或传输失败时返回错误。
///
/// Reqwest 负责其原生支持的 content decoding；该 helper 只保留取消处理和 partial
/// body 收集，避免在多个 API 调用点重复流读取循环。
pub(crate) async fn read_response_body(
    response: Response,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, ApiError> {
    let outcome = read_response_body_outcome(response, cancellation).await;
    match outcome.error {
        Some(error) => Err(error),
        None => Ok(outcome.body),
    }
}

/// 读取响应 body，忽略读取错误但保留传输中断前已经到达的字节。
pub(crate) async fn read_response_body_ignoring_errors(
    response: Response,
    cancellation: &CancellationToken,
) -> Vec<u8> {
    read_response_body_outcome(response, cancellation)
        .await
        .body
}

/// body reader 的内部结果；错误路径仍携带已经读取或解压出的字节。
struct ResponseBodyOutcome {
    body: Vec<u8>,
    error: Option<ApiError>,
}

/// 收集 Reqwest 响应流并处理取消。
async fn read_response_body_outcome(
    response: Response,
    cancellation: &CancellationToken,
) -> ResponseBodyOutcome {
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    let mut error = None;

    loop {
        let next = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                error = Some(ApiError::message("response body read canceled"));
                break;
            }
            next = stream.next() => next,
        };
        match next {
            Some(Ok(chunk)) => body.extend_from_slice(&chunk),
            Some(Err(source)) => {
                error = Some(ApiError::message(source.without_url().to_string()));
                break;
            }
            None => break,
        }
    }

    ResponseBodyOutcome { body, error }
}

/// 使用 Serde 编码强类型值，并补齐协议要求的 HTML 安全转义。
///
/// 该函数不擅自重排 struct 字段，而是保持声明顺序。调用方若要求 map 键有序，
/// 必须传入 `BTreeMap`。输出没有末尾换行。
pub(crate) fn marshal_go_json<T: Serialize + ?Sized>(
    value: &T,
) -> Result<Vec<u8>, serde_json::Error> {
    let encoded = serde_json::to_string(value)?;
    Ok(escape_go_json_html(encoded).into_bytes())
}

/// 把一棵动态 JSON 值编码为紧凑 JSON。
///
/// 动态 object 的键必须递归排序，number 必须先收窄到 IEEE-754 float64；CCR event
/// 正好经历这次 round-trip，不能沿用强类型 Serde 的字段顺序和 64 位整数精度。
pub(crate) fn marshal_go_dynamic_json(value: &Value) -> Result<Vec<u8>, serde_json::Error> {
    let mut output = String::new();
    write_go_dynamic_json_value(value, &mut output)?;
    Ok(output.into_bytes())
}

/// 递归写入一项紧凑的动态 JSON 值。
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

/// 使用 Serde 完成控制字符转义，再补 HTML 和行分隔符转义。
fn go_json_string(value: &str) -> Result<String, serde_json::Error> {
    serde_json::to_string(value).map(escape_go_json_html)
}

/// 对一段已经合法的紧凑 JSON 补齐 HTML 和行分隔符转义。
fn escape_go_json_html(encoded: String) -> String {
    encoded
        .replace('&', "\\u0026")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}

/// 把动态 JSON number 收窄为 float64 后按 JSON 指数阈值编码。
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
