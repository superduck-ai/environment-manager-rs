//! JSON 解码兼容适配：结构体字段忽略大小写、重复字段与浮点数语义。
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Number, Value};
use std::fmt;

#[derive(Debug)]
enum Json {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}
impl<'de> Deserialize<'de> for Json {
    /// 保留对象字段输入顺序，便于模拟结构体重复字段的处理。
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(JsonVisitor)
    }
}
struct JsonVisitor;
impl<'de> Visitor<'de> for JsonVisitor {
    type Value = Json;
    /// 描述可接受的 JSON 值。
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("JSON value")
    }
    /// 解码 null。
    fn visit_unit<E: de::Error>(self) -> Result<Json, E> {
        Ok(Json::Null)
    }
    /// 解码布尔值。
    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Json, E> {
        Ok(Json::Bool(v))
    }
    /// 整数转换为参考解码器的 float64。
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Json, E> {
        Ok(Json::Number(v as f64))
    }
    /// 无符号整数转换为参考解码器的 float64。
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Json, E> {
        Ok(Json::Number(v as f64))
    }
    /// 保留浮点 JSON 数值。
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Json, E> {
        Ok(Json::Number(v))
    }
    /// 解码字符串。
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Json, E> {
        Ok(Json::String(v.into()))
    }
    /// 解码已分配字符串。
    fn visit_string<E: de::Error>(self, v: String) -> Result<Json, E> {
        Ok(Json::String(v))
    }
    /// 解码数组元素。
    fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Json, A::Error> {
        let mut out = Vec::new();
        while let Some(v) = a.next_element()? {
            out.push(v);
        }
        Ok(Json::Array(out))
    }
    /// 解码对象并保留重复字段。
    fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Json, A::Error> {
        let mut out = Vec::new();
        while let Some(pair) = a.next_entry()? {
            out.push(pair);
        }
        Ok(Json::Object(out))
    }
}
impl Json {
    /// 转换为工具可用的 JSON 值；map 重复字段取最后一个。
    fn value(self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(v) => Value::Bool(v),
            Self::String(v) => Value::String(v),
            Self::Number(v) => {
                if v == 0.0 {
                    Value::from(0)
                } else if v.fract() == 0.0 && v >= i64::MIN as f64 && v < (i64::MAX as f64) {
                    Value::from(v as i64)
                } else if v.fract() == 0.0 && v >= 0.0 && v < (u64::MAX as f64) {
                    Value::from(v as u64)
                } else {
                    Value::Number(Number::from_f64(v).expect("finite JSON number"))
                }
            }
            Self::Array(v) => Value::Array(v.into_iter().map(Self::value).collect()),
            Self::Object(v) => Value::Object(v.into_iter().map(|(k, v)| (k, v.value())).collect()),
        }
    }
}
/// 解析过的请求；params 保留独立结构体的大小写和 null 语义。
pub struct Request {
    pub jsonrpc: String,
    pub method: String,
    pub id: Value,
    pub params: Option<Value>,
    pub params_error: Option<String>,
}
/// 修复 JSON 字符串里的孤立 UTF-16 代理项；合法配对原样保留。
fn repair(body: &[u8]) -> String {
    let s = crate::util::lossy(body);
    let bytes = s.as_bytes();
    let mut out = String::new();
    let mut i = 0;
    let mut in_string = false;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'"' {
            in_string = !in_string;
            out.push('"');
            i += 1;
            continue;
        }
        if in_string && c == b'\\' {
            if bytes.get(i + 1) == Some(&b'u')
                && bytes
                    .get(i + 2..i + 6)
                    .is_some_and(|b| b.iter().all(u8::is_ascii_hexdigit))
                && let Ok(n) = u16::from_str_radix(&s[i + 2..i + 6], 16)
            {
                if (0xd800..=0xdbff).contains(&n) {
                    let low = if bytes.get(i + 6..i + 8) == Some(b"\\u")
                        && bytes
                            .get(i + 8..i + 12)
                            .is_some_and(|b| b.iter().all(u8::is_ascii_hexdigit))
                    {
                        u16::from_str_radix(&s[i + 8..i + 12], 16).ok()
                    } else {
                        None
                    };
                    if low.is_some_and(|n| (0xdc00..=0xdfff).contains(&n)) {
                        out.push_str(&s[i..i + 12]);
                        i += 12;
                        continue;
                    }
                    out.push_str("\\ufffd");
                    i += 6;
                    continue;
                }
                if (0xdc00..=0xdfff).contains(&n) {
                    out.push_str("\\ufffd");
                    i += 6;
                    continue;
                }
            }
            out.push('\\');
            i += 1;
            if i < bytes.len() {
                let ch = s[i..].chars().next().unwrap();
                out.push(ch);
                i += ch.len_utf8();
            }
            continue;
        }
        let ch = s[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}
/// 对请求结构体按输入字段顺序进行兼容解码。
pub fn request(body: &[u8]) -> Result<Request, ()> {
    let root: Json = serde_json::from_str(&repair(body)).map_err(|_| ())?;
    let mut out = Request {
        jsonrpc: String::new(),
        method: String::new(),
        id: Value::Null,
        params: None,
        params_error: None,
    };
    let fields = match root {
        Json::Null => return Ok(out),
        Json::Object(v) => v,
        _ => return Err(()),
    };
    for (k, v) in fields {
        if k.eq_ignore_ascii_case("jsonrpc") || k.eq_ignore_ascii_case("method") {
            let target = if k.eq_ignore_ascii_case("jsonrpc") {
                &mut out.jsonrpc
            } else {
                &mut out.method
            };
            match v {
                Json::Null => {}
                Json::String(v) => *target = v,
                _ => return Err(()),
            }
        } else if k.eq_ignore_ascii_case("id") {
            out.id = v.value();
        } else if k.eq_ignore_ascii_case("params") {
            let (value, error) = params(v);
            out.params = Some(value);
            out.params_error = error;
        }
    }
    Ok(out)
}
/// 参数结构体兼容解码，未知字段不会产生类型错误。
fn params(v: Json) -> (Value, Option<String>) {
    let fields = match v {
        Json::Null => return (Value::Null, None),
        Json::Object(v) => v,
        v => {
            let v = v.value();
            return (
                v.clone(),
                Some(format!(
                    "json: cannot unmarshal {} into Go value of type mcp.toolCallParams",
                    type_name(&v)
                )),
            );
        }
    };
    let mut name = Value::Null;
    let mut arguments = Value::Null;
    let mut error = None;
    for (k, v) in fields {
        if k.eq_ignore_ascii_case("name") {
            match v {
                Json::Null => {}
                Json::String(v) => name = Value::String(v),
                v => {
                    if error.is_none() {
                        error = Some(format!(
                            "json: cannot unmarshal {} into Go struct field toolCallParams.name of type string",
                            type_name(&v.value())
                        ));
                    }
                }
            }
        } else if k.eq_ignore_ascii_case("arguments") {
            let v = v.value();
            if !v.is_null() && !v.is_object() {
                if error.is_none() {
                    error = Some(format!(
                        "json: cannot unmarshal {} into Go struct field toolCallParams.arguments of type map[string]interface {{}}",
                        type_name(&v)
                    ));
                }
            } else if let (Value::Object(existing), Value::Object(incoming)) = (&mut arguments, &v)
            {
                existing.extend(incoming.clone());
            } else {
                arguments = v;
            }
        }
    }
    (
        serde_json::json!({"name":name,"arguments":arguments}),
        error,
    )
}
/// 生成兼容解码器类型名称。
fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// 解码第一个环境快照值；未读完的 JSON 返回 None 继续读取。
pub fn snapshot(
    data: &[u8],
    eof: bool,
) -> Result<Option<std::collections::BTreeMap<String, String>>, String> {
    let repaired = repair(data);
    let mut decoder = serde_json::Deserializer::from_str(&repaired);
    let value = match Json::deserialize(&mut decoder) {
        Ok(v) => v,
        Err(e) if e.is_eof() && !eof => return Ok(None),
        Err(e) if e.is_eof() => {
            return Err(if data.iter().all(u8::is_ascii_whitespace) {
                "EOF".into()
            } else {
                "unexpected EOF".into()
            });
        }
        Err(e) => {
            let c = data
                .get(e.column().saturating_sub(1))
                .copied()
                .unwrap_or(b'?') as char;
            return Err(format!(
                "invalid character '{c}' looking for beginning of value"
            ));
        }
    };
    let fields = match value {
        Json::Null => return Ok(Some(Default::default())),
        Json::Object(v) => v,
        v => {
            return Err(format!(
                r#"json: cannot unmarshal {} into Go value of type struct {{ Env map[string]string "json:\"env\"" }}"#,
                type_name(&v.value())
            ));
        }
    };
    let mut out = std::collections::BTreeMap::new();
    for (k, v) in fields {
        if !k.eq_ignore_ascii_case("env") {
            continue;
        }
        match v {
            Json::Null => out.clear(),
            Json::Object(v) => {
                for (k, v) in v {
                    let s = match v {
                        Json::Null => String::new(),
                        Json::String(s) => s,
                        v => {
                            return Err(format!(
                                "json: cannot unmarshal {} into Go struct field .env of type string",
                                type_name(&v.value())
                            ));
                        }
                    };
                    out.insert(k, s);
                }
            }
            v => {
                return Err(format!(
                    "json: cannot unmarshal {} into Go struct field .env of type map[string]string",
                    type_name(&v.value())
                ));
            }
        }
    }
    Ok(Some(out))
}
