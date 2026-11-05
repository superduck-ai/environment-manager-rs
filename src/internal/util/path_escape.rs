//! path-segment 百分号编码的共享实现。
//!
//! 在 API、Git proxy、Supabase MCP 和 session lockfile 中都用
//! 单个动态 path segment 使用统一的逐字节编码规则，
//! 避免各调用方分别使用 query/form encoder 后，在 `: @ & = + $ ; ,` 等保留字符上
//! 产生不同的外部 URL 或文件名。

/// 逐 UTF-8 字节编码一个 URL path segment。
///
/// API、Git proxy、Supabase MCP 和 session lockfile 共用该函数。path-segment 模式
/// 保留字母、数字、`-._~`
/// 以及 `: @ & = + $`，并转义 `/ ; , ?`、百分号、空格和全部非 ASCII 字节。
///
/// # 参数
/// - `value`：要放入单个 URL path segment 的 UTF-8 字符串；函数只借用输入，
///   不要求其满足 ID 格式，也不会先做 Unicode 归一化。
///
/// # 返回
/// 返回只包含 ASCII 的 path-segment 编码。需要转义的每个 UTF-8 字节都写成大写 `%XX`；
/// 该过程不会失败，也不会修改 `.` 或 `..` 的文本内容。
///
/// # 副作用
/// 无文件、网络或环境变量副作用；唯一分配是返回字符串。
///
/// # 协议约束
/// 这里只实现后端 path-segment 编码，不负责完整 URL parser 对 `.`/`..` path segment
/// 的归一化；上游 `url` 会按 WHATWG 清理后端明确禁止的完整 `.`/`..` ID。
#[must_use]
pub(crate) fn escape_path_segment(value: &str) -> String {
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
    use super::escape_path_segment;

    /// 表驱动覆盖 path-segment 模式的保留字符、百分号二次编码和 UTF-8 字节。
    #[test]
    fn path_escape_encodes_reserved_and_utf8_bytes() {
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
            assert_eq!(escape_path_segment(input), expected, "input={input:?}");
        }
    }
}
