//! Tunnel header 转换与辅助函数。
//!
//! 这里处理 header 与 ProtoJSON 字节布局；未知 ProtoJSON 字段的宽容解码放在 client。

use std::io;

use http::header::{HOST, HeaderMap, HeaderName, HeaderValue};
use serde::Serialize;

use super::{Header, TunnelError};

/// Tunnel 协议紧凑格式的 Serde JSON formatter。
///
/// 协议格式不在冒号后加空格，但会在 object
/// 和 array 的每个逗号后写一个 ASCII 空格。`serde_json`默认
/// 紧凑 formatter 不写这个空格，因此需要在 token 边界定制，不能
/// 对最终字符串做替换（字符串值自身可以包含逗号）。
struct GoProtoJsonFormatter;

impl serde_json::ser::Formatter for GoProtoJsonFormatter {
    /// 在非首个 array 元素前写入 ProtoJSON 的逗号和单空格。
    fn begin_array_value<W>(&mut self, writer: &mut W, first: bool) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    /// 在非首个 object 键前写入 ProtoJSON 的逗号和单空格。
    fn begin_object_key<W>(&mut self, writer: &mut W, first: bool) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }
}

/// 按稳定字节布局编码。
///
/// # 参数
/// - `value`：已由 `pbjson` 生成 `Serialize` 实现的 Tunnel Proto 消息。
///
/// # 返回
/// 成功时返回 lowerCamel ProtoJSON 字节；生成的 Serde 实现拒绝
/// 某个字段时原样返回序列化错误。
///
/// # 协议字节
/// 保留 ProtoJSON 在逗号后的单空格、pbjson 字段顺序、Base64 bytes
/// 和默认值省略规则；该空格是 WebSocket text frame 中可观察的协议字节。
pub fn marshal_protojson<T>(value: &T) -> Result<Vec<u8>, serde_json::Error>
where
    T: Serialize + ?Sized,
{
    let mut output = Vec::new();
    let mut serializer = serde_json::Serializer::with_formatter(&mut output, GoProtoJsonFormatter);
    value.serialize(&mut serializer)?;
    Ok(output)
}

///
/// 按输入顺序追加非空 header，保留同名多值。无效 header name/value 会返回请求
/// 构造错误，而不会把未经验证的字节交给本地 HTTP/WS 客户端。`Host`
/// 特例忽略：把它写入 `Request.Header` 后，Transport 仍只使用
/// `Request.Host`/目标 URL 生成 Host；Rust 若直接透传则可被后端请求覆盖
/// loopback 目标 authority，因此必须在这一共享边界丢弃。
pub fn apply_tunnel_headers(
    destination: &mut HeaderMap,
    headers: &[Header],
) -> Result<(), TunnelError> {
    for header in headers {
        if header.name.is_empty() {
            continue;
        }
        let name = HeaderName::from_bytes(header.name.as_bytes())
            .map_err(|error| TunnelError::message(format!("invalid header name: {error}")))?;
        if name == HOST {
            continue;
        }
        let value = HeaderValue::from_str(&header.value)
            .map_err(|error| TunnelError::message(format!("invalid header value: {error}")))?;
        destination.append(name, value);
    }
    Ok(())
}

/// 应用 WebSocket 握手 header，并特殊处理 `Host`。
///
/// 普通 header 仍按输入顺序 append；第一个 `Host` 覆盖
/// `into_client_request` 从 loopback URL 生成的握手 Host，后续 `Host`
/// 忽略。第一值写入握手 request-target 的 authority，不能复用 HTTP transport 忽略
/// `Host` header 的分支。
/// WebSocket 自己生成的握手保留头若被调用方重复传入，会在联网前返回
/// `websocket: duplicate header not allowed`；仅 `Sec-Websocket-Protocol`
/// 在 dialer 未配置子协议时允许由 header 提供。
pub fn apply_websocket_tunnel_headers(
    destination: &mut HeaderMap,
    headers: &[Header],
    subprotocols_configured: bool,
) -> Result<(), TunnelError> {
    let mut host_replaced = false;
    for header in headers {
        if header.name.is_empty() {
            continue;
        }
        let name = HeaderName::from_bytes(header.name.as_bytes())
            .map_err(|error| TunnelError::message(format!("invalid header name: {error}")))?;
        if let Some(canonical_name) =
            websocket_duplicate_header_name(&name, subprotocols_configured)
        {
            return Err(TunnelError::message(format!(
                "websocket: duplicate header not allowed: {canonical_name}"
            )));
        }
        let value = HeaderValue::from_str(&header.value)
            .map_err(|error| TunnelError::message(format!("invalid header value: {error}")))?;
        if name == HOST {
            if !host_replaced {
                destination.insert(HOST, value);
                host_replaced = true;
            }
        } else {
            destination.append(name, value);
        }
    }
    Ok(())
}

/// 返回 WebSocket 禁止由 `requestHeader` 重复提供的握手头规范名称。
///
/// `HeaderName` 内部统一为小写，本函数显式返回 canonical HTTP header casing 产生的可观测错误文本。协议头只有在
/// dialer 已通过 `Subprotocols` 配置时才冲突；否则 WebSocket 允许它作为
/// request header，并保留全部值。
fn websocket_duplicate_header_name(
    name: &HeaderName,
    subprotocols_configured: bool,
) -> Option<&'static str> {
    match name.as_str() {
        "upgrade" => Some("Upgrade"),
        "connection" => Some("Connection"),
        "sec-websocket-key" => Some("Sec-Websocket-Key"),
        "sec-websocket-version" => Some("Sec-Websocket-Version"),
        "sec-websocket-extensions" => Some("Sec-Websocket-Extensions"),
        "sec-websocket-protocol" if subprotocols_configured => Some("Sec-Websocket-Protocol"),
        _ => None,
    }
}

///
/// 名称按字典序排列，每个名称的多个值保持原顺序，并转为 canonical HTTP header casing 风格，以稳定差分输出。
#[must_use]
pub fn flatten_http_headers(source: &HeaderMap) -> Vec<Header> {
    let mut names = source
        .keys()
        .map(|name| name.as_str().to_owned())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();

    let mut headers = Vec::new();
    for name in names {
        let canonical_name = canonical_mime_header_key(&name);
        for value in source.get_all(&name) {
            headers.push(Header {
                name: canonical_name.clone(),
                value: String::from_utf8_lossy(value.as_bytes()).into_owned(),
            });
        }
    }
    headers
}

/// 将 ASCII header 名称转为每个连字符分段首字母大写的 MIME 形式。
pub(crate) fn canonical_mime_header_key(name: &str) -> String {
    let mut upper_next = true;
    name.bytes()
        .map(|byte| {
            if byte == b'-' {
                upper_next = true;
                '-'
            } else if upper_next {
                upper_next = false;
                byte.to_ascii_uppercase() as char
            } else {
                byte.to_ascii_lowercase() as char
            }
        })
        .collect()
}
