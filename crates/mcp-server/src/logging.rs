//! 兼容参考服务的简单 stderr 日志格式。
/// 记录信息级日志。
pub fn info(message: &str, fields: &[(&str, &str)]) {
    write("INFO", message, fields);
}
/// 记录错误级日志。
pub fn error(message: &str, fields: &[(&str, &str)]) {
    write("ERROR", message, fields);
}
/// 按 slog 的文本属性规则添加引号。
fn quote(value: &str) -> String {
    if value.is_empty()
        || value
            .chars()
            .any(|c| c.is_whitespace() || c == '=' || c == '"' || c < ' ')
    {
        serde_json::to_string(value).unwrap()
    } else {
        value.into()
    }
}
/// 用本地时区写入单行 stderr，不污染 MCP stdout。
fn write(level: &str, message: &str, fields: &[(&str, &str)]) {
    let mut out = format!(
        "{} {level} {message}",
        chrono::Local::now().format("%Y/%m/%d %H:%M:%S")
    );
    for (key, value) in fields {
        out.push_str(&format!(" {key}={}", quote(value)));
    }
    eprintln!("{out}");
}

/// 记录警告级日志。
pub fn warn(message: &str, fields: &[(&str, &str)]) {
    write("WARN", message, fields);
}
