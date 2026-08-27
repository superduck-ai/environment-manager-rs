//! Tunnel header 转换与辅助函数。
//!
//! 这里处理 header 与 ProtoJSON 字节布局；未知 ProtoJSON 字段的宽容解码放在 client。

use http::header::{HOST, HeaderMap, HeaderName, HeaderValue};
use serde::Serialize;

use super::{Header, TunnelError};

/// 编码 Tunnel ProtoJSON。
///
/// # 参数
/// - `value`：已由 `pbjson` 生成 `Serialize` 实现的 Tunnel Proto 消息。
///
/// # 返回
/// 成功时返回 lowerCamel ProtoJSON 字节；生成的 Serde 实现拒绝某个字段时原样返回
/// 序列化错误。JSON 空白不属于协议语义。
pub fn encode_protojson<T>(value: &T) -> Result<Vec<u8>, serde_json::Error>
where
    T: Serialize + ?Sized,
{
    serde_json::to_vec(value)
}

/// 按输入顺序追加非空 header，保留同名多值。无效 header name/value 会返回请求
/// 构造错误，而不会把未经验证的字节交给本地 HTTP/WS 客户端。普通 HTTP 转发忽略
/// `Host`，避免后端请求覆盖 loopback 目标 authority。
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
/// `websocket: duplicate header not allowed`；仅子协议 header 在握手器未配置子协议时
/// 允许由调用方提供。
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
        if is_websocket_handshake_header(&name, subprotocols_configured) {
            return Err(TunnelError::message(format!(
                "websocket: duplicate header not allowed: {name}"
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

/// 判断 WebSocket 握手器自行生成、调用方不得重复提供的 header。
///
/// 子协议头只在调用方已经配置子协议时冲突；否则允许直接由 header 提供。
fn is_websocket_handshake_header(name: &HeaderName, subprotocols_configured: bool) -> bool {
    match name.as_str() {
        "upgrade"
        | "connection"
        | "sec-websocket-key"
        | "sec-websocket-version"
        | "sec-websocket-extensions" => true,
        "sec-websocket-protocol" => subprotocols_configured,
        _ => false,
    }
}

/// 名称按字典序排列，每个名称的多个值保持原顺序。
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
        for value in source.get_all(&name) {
            headers.push(Header {
                name: name.clone(),
                value: String::from_utf8_lossy(value.as_bytes()).into_owned(),
            });
        }
    }
    headers
}
