//! 保留协议结构体字段顺序和参考 JSON 浮点输出规则。
use serde_json::Value;
#[derive(Clone, Copy)]
enum Shape {
    Generic,
    Response,
    Result,
    Definition,
    Content,
    Initialize,
    ServerInfo,
    Error,
}
/// 输出浮点数，保留参考编码器的十进制与科学计数法分界。
fn number(value: &serde_json::Number) -> String {
    let n = value.as_f64().expect("finite number");
    if n == 0.0 {
        return "0".into();
    }
    let a = n.abs();
    if (1e-6..1e21).contains(&a) {
        return n.to_string();
    }
    let raw = format!("{n:e}");
    let (mantissa, exponent) = raw.split_once('e').unwrap();
    let exponent: i32 = exponent.parse().unwrap();
    format!(
        "{mantissa}e{}{exponent}",
        if exponent >= 0 { "+" } else { "" }
    )
}
/// 按协议上下文选择子对象的结构体布局，普通用户对象仍按键排序。
fn child(shape: Shape, key: &str) -> Shape {
    match (shape, key) {
        (Shape::Response, "result") => Shape::Result,
        (Shape::Response, "error") => Shape::Error,
        (Shape::Result, "tools") => Shape::Definition,
        (Shape::Result, "content") => Shape::Content,
        (Shape::Initialize, "serverInfo") => Shape::ServerInfo,
        _ => Shape::Generic,
    }
}
/// 递归编码值，仅为已知协议结构体指定字段顺序。
fn write(out: &mut String, value: &Value, mut shape: Shape) {
    match value {
        Value::Number(n) => out.push_str(&number(n)),
        Value::Array(values) => {
            out.push('[');
            for (i, v) in values.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write(out, v, shape);
            }
            out.push(']');
        }
        Value::Object(map) => {
            if matches!(shape, Shape::Result) && map.contains_key("protocolVersion") {
                shape = Shape::Initialize;
            }
            let ordered: &[&str] = match shape {
                Shape::Response => &["jsonrpc", "id", "result", "error"],
                Shape::Result if map.contains_key("content") => &["content", "isError"],
                Shape::Initialize => &["protocolVersion", "capabilities", "serverInfo"],
                Shape::ServerInfo => &["name", "version"],
                Shape::Definition => &["name", "description", "inputSchema"],
                Shape::Content => &["type", "text", "data", "mimeType"],
                Shape::Error => &["code", "message", "data"],
                _ => &[],
            };
            out.push('{');
            let mut first = true;
            for key in ordered
                .iter()
                .copied()
                .filter(|k| map.contains_key(*k))
                .chain(
                    map.keys()
                        .map(String::as_str)
                        .filter(|k| !ordered.contains(k)),
                )
            {
                if !first {
                    out.push(',');
                }
                first = false;
                out.push_str(&serde_json::to_string(key).unwrap());
                out.push(':');
                write(out, &map[key], child(shape, key));
            }
            out.push('}');
        }
        _ => out.push_str(&serde_json::to_string(value).expect("JSON value")),
    }
}
/// 序列化 MCP 响应并应用 HTML 及行分隔符转义。
pub fn encode(value: &Value) -> Vec<u8> {
    let mut out = String::new();
    write(&mut out, value, Shape::Response);
    out.replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
        .into_bytes()
}
