//! Go 进程级 `slog.Default()` 的兼容 logger。
//!
//! Go 项目没有调用 `slog.SetDefault`，因此少数直接使用 `slog.Default()` 的路径会
//! 经过标准库 legacy `log` handler，输出 `日期 时间 LEVEL 消息 key=value`，而不是
//! 应用其余部分采用的 JSON。这个适配器只复刻该全局 logger；显式传入的业务 logger
//! 仍由 `file_logger.rs` 处理，二者不能互换。

use std::{
    io::{self, Write},
    sync::{Arc, Mutex, OnceLock},
};

use chrono::Local;

use super::{LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, Logger};

/// 进程生命周期内唯一的 Go 默认 logger。
static DEFAULT_SLOG_LOGGER: OnceLock<Logger> = OnceLock::new();

/// 对应 Go 标准库：`slog.Default` 在应用未调用 `slog.SetDefault` 时的返回值。
///
/// # 返回
/// 返回一个进程级共享 logger；handler 的最低级别固定为 INFO，并把完整记录原子地
/// 写到 stderr。返回引用具有静态生命周期，调用方不需要管理关闭顺序。
///
/// # 副作用
/// 第一次调用会惰性创建 stderr handler；之后的 INFO 及以上日志会使用 Go legacy
/// `log` 的日期、时间和文本属性格式写入进程 stderr。
///
/// # 兼容性
/// 当前 Go 基线没有调用 `slog.SetDefault`。如果未来基线改变这一点，必须同步扩展为
/// 可替换的全局状态，不能在调用处悄悄改用应用 JSON logger。
pub fn default_slog_logger() -> &'static Logger {
    DEFAULT_SLOG_LOGGER.get_or_init(|| Logger::new(Arc::new(GoDefaultHandler::stderr())))
}

/// Go 默认 text handler 的最小状态；所有派生 handler 共享同一把 writer 锁。
struct GoDefaultHandler {
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    bound_attributes: LogAttributes,
    groups: Vec<String>,
}

impl GoDefaultHandler {
    /// 创建直接写向进程 stderr 的根 handler。
    fn stderr() -> Self {
        Self {
            writer: Arc::new(Mutex::new(Box::new(io::stderr()))),
            bound_attributes: LogAttributes::new(),
            groups: Vec::new(),
        }
    }

    /// 将一组属性按 Go TextHandler 的 `group.key=value` 形状追加到输出行。
    fn append_attributes(output: &mut String, groups: &[String], attributes: &LogAttributes) {
        for (key, value) in attributes {
            output.push(' ');
            for group in groups.iter().filter(|group| !group.is_empty()) {
                output.push_str(group);
                output.push('.');
            }
            output.push_str(key);
            output.push('=');
            output.push_str(&format_text_value(value));
        }
    }
}

impl LogHandler for GoDefaultHandler {
    /// Go 默认 logger 的有效最低级别为 INFO。
    fn enabled(&self, level: LogLevel) -> bool {
        level >= LogLevel::Info
    }

    /// 把一条记录编码为 legacy `log` 文本并在一把锁内写出。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        if !self.enabled(record.level) {
            return Ok(());
        }

        // Go defaultHandler 先构造 `LEVEL message attrs`，随后交给设置了
        // `Ldate|Ltime` 的 legacy log.Logger。时间使用写出时的本地时区，而不是
        // slog.Record 中事件产生时的 UTC 时间。
        let mut output = format!(
            "{} {} {}",
            Local::now().format("%Y/%m/%d %H:%M:%S"),
            record.level.as_str(),
            record.message
        );
        Self::append_attributes(&mut output, &[], &self.bound_attributes);
        Self::append_attributes(&mut output, &self.groups, &record.attributes);
        output.push('\n');

        self.writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .write_all(output.as_bytes())
            .map_err(|error| Box::new(error) as LogHandlerError)
    }

    /// 返回共享 writer、并按调用顺序附加固定属性的派生 handler。
    fn with_attrs(&self, attributes: LogAttributes) -> Arc<dyn LogHandler> {
        let mut bound_attributes = self.bound_attributes.clone();
        bound_attributes.extend(attributes);
        Arc::new(Self {
            writer: Arc::clone(&self.writer),
            bound_attributes,
            groups: self.groups.clone(),
        })
    }

    /// 返回共享 writer、并为后续属性增加点号分隔组前缀的派生 handler。
    fn with_group(&self, name: &str) -> Arc<dyn LogHandler> {
        let mut groups = self.groups.clone();
        if !name.is_empty() {
            groups.push(name.to_owned());
        }
        Arc::new(Self {
            writer: Arc::clone(&self.writer),
            bound_attributes: self.bound_attributes.clone(),
            groups,
        })
    }
}

/// 把 JSON 属性编码为 Go slog 文本值。
///
/// JSON 数组来自原 Go 调用中的 slice；`slog` 会按 `fmt` 的 `%+v` 规则输出为
/// `[first second]`，而不是把紧凑 JSON 再包一层引号。对象同理近似为 Go map 的
/// `map[key:value]` 形状，并按键排序以复刻 `fmt` 对字符串键 map 的稳定输出。
fn format_text_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "<nil>".to_owned(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) if !needs_quoting(value) => value.clone(),
        serde_json::Value::String(value) => quote_go_string(value),
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(format_go_any_value)
                .collect::<Vec<_>>()
                .join(" ")
        ),
        serde_json::Value::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_unstable_by_key(|(key, _)| *key);
            format!(
                "map[{}]",
                entries
                    .into_iter()
                    .map(|(key, value)| format!("{key}:{}", format_go_any_value(value)))
                    .collect::<Vec<_>>()
                    .join(" ")
            )
        }
    }
}

/// 编码 Go `fmt` 在 slice 或 map 内部看到的 JSON 值。
///
/// 容器内部的字符串不会走 slog 的 token quoting，因为原 Go 属性在这一层已经是
/// `KindAny`，格式完全由 `fmt` 决定；这一区别正是 `[]string{"two"}` 必须输出
/// `[two]` 而不是 `["two"]` 的原因。
fn format_go_any_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "<nil>".to_owned(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => format_text_value(value),
    }
}

/// 判断字符串作为 slog text value 时是否必须加引号。
fn needs_quoting(value: &str) -> bool {
    value.is_empty()
        || value.chars().any(|character| {
            character.is_whitespace() || character.is_control() || matches!(character, '=' | '"')
        })
}

/// 使用与 `strconv.Quote` 在 UTF-8 业务文本上的兼容转义生成双引号字符串。
fn quote_go_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_owned())
}
