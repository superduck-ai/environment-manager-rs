//! Go `net/url.PathEscape` 的共享兼容实现。
//!
//! Go 工作树在 API、Git proxy、Supabase MCP 和 session lockfile 中都用
//! `url.PathEscape` 编码单个动态 path segment。本模块集中保存同一套逐字节规则，
//! 避免各调用方分别使用 query/form encoder 后，在 `: @ & = + $ ; ,` 等保留字符上
//! 产生不同的外部 URL 或文件名。

/// 逐 UTF-8 字节复刻 Go 标准库 `net/url.PathEscape`。
///
/// 对应 Go：所有生产态 `url.PathEscape` 调用，包括 `internal/api`、
/// `internal/gitproxy/handler.go`、`internal/mcp/servers/supabase/server.go` 和
/// `internal/util/lockfile.go`。Go 的 path-segment 模式保留字母、数字、`-._~`
/// 以及 `: @ & = + $`，并转义 `/ ; , ?`、百分号、空格和全部非 ASCII 字节。
///
/// # 参数
/// - `value`：要放入单个 URL path segment 的 UTF-8 字符串；函数只借用输入，
///   不要求其满足 ID 格式，也不会先做 Unicode 归一化。
///
/// # 返回
/// 返回只包含 ASCII 的兼容编码。需要转义的每个 UTF-8 字节都写成大写 `%XX`；
/// 该过程不会失败，也不会修改 `.` 或 `..` 的文本内容。
///
/// # 副作用
/// 无文件、网络或环境变量副作用；唯一分配是返回字符串。
///
/// # 兼容性
/// 这里只复刻 `PathEscape` 本身，不负责完整 URL parser 对 `.`/`..` path segment
/// 的归一化。调用方必须在随后构造请求时继续保留这里生成的原始 escaped path。
#[must_use]
pub(crate) fn go_path_escape(value: &str) -> String {
    const UPPER_HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut escaped = String::with_capacity(value.len());
    for &byte in value.as_bytes() {
        let is_path_segment_safe = byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_' | b'.' | b'~' | b':' | b'@' | b'&' | b'=' | b'+' | b'$'
            );
        if is_path_segment_safe {
            escaped.push(char::from(byte));
        } else {
            escaped.push('%');
            escaped.push(char::from(UPPER_HEX[usize::from(byte >> 4)]));
            escaped.push(char::from(UPPER_HEX[usize::from(byte & 0x0f)]));
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::go_path_escape;

    /// 表驱动覆盖 Go path-segment 模式的保留字符、百分号二次编码和 UTF-8 字节。
    #[test]
    fn matches_go_net_url_path_escape_table() {
        let cases = [
            (".", "."),
            ("..", ".."),
            ("%2e", "%252e"),
            ("%2E%2E", "%252E%252E"),
            ("...", "..."),
            ("/", "%2F"),
            (";", "%3B"),
            (",", "%2C"),
            (":", ":"),
            ("@", "@"),
            ("&", "&"),
            ("=", "="),
            ("+", "+"),
            ("$", "$"),
            (": @ & = + $", ":%20@%20&%20=%20+%20$"),
            ("中文", "%E4%B8%AD%E6%96%87"),
        ];

        for (input, expected) in cases {
            assert_eq!(go_path_escape(input), expected, "input={input:?}");
        }
    }
}
