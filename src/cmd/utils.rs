//! 多个命令共享的纯解析工具。
//!
//! 这里刻意不依赖日志实现或异步运行时，使命令层可以在内部模块尚未完成时独立
//! 测试。持续时间解析覆盖 CLI 常用的 duration 语法，从而不会把
//! 原有 `5m`、`1h30m` 等部署参数强制改写成另一套格式。

use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use std::time::Duration;

use clap::{Arg, ArgAction, ArgMatches};

/// 命令行支持的 tracing 日志级别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    /// 诊断级日志。
    Debug,
    /// 默认信息级日志。
    Info,
    /// 警告级日志。
    Warn,
    /// 错误级日志。
    Error,
}

impl Display for LogLevel {
    /// 输出旧 CLI 接受的小写稳定拼写。
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        })
    }
}

impl FromStr for LogLevel {
    type Err = String;

    /// 严格解析 命令接受的四个小写值；不隐式接受大小写变体。
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_log_level(value)
    }
}

/// 解析日志级别，并保留错误文本。
pub fn parse_log_level(value: &str) -> Result<LogLevel, String> {
    match value {
        "debug" => Ok(LogLevel::Debug),
        "info" => Ok(LogLevel::Info),
        "warn" => Ok(LogLevel::Warn),
        "error" => Ok(LogLevel::Error),
        invalid => Err(format!(
            "invalid log level: {invalid} (valid: debug, info, warn, error)"
        )),
    }
}

/// 将 CLI 日志级别映射为 `tracing` 级别。
pub(crate) const fn tracing_level(level: LogLevel) -> tracing::Level {
    match level {
        LogLevel::Debug => tracing::Level::DEBUG,
        LogLevel::Info => tracing::Level::INFO,
        LogLevel::Warn => tracing::Level::WARN,
        LogLevel::Error => tracing::Level::ERROR,
    }
}

/// 解析 CLI 使用的非负 duration 语法。
///
/// 支持 `ns`、`us`、`µs`、`μs`、`ms`、`s`、`m`、`h` 以及连续组合和小数，
/// 例如 `500ms`、`1.5s`、`1h30m`。CLI 的超时没有负值语义，因此负数会被明确
/// 拒绝；`0` 表示“不设超时”。
pub fn parse_duration(input: &str) -> Result<Duration, String> {
    let unsigned_input = input.strip_prefix('+').unwrap_or(input);
    if unsigned_input == "0" {
        return Ok(Duration::ZERO);
    }
    if unsigned_input.is_empty() {
        return Err("invalid duration: empty value".to_owned());
    }
    if input.starts_with('-') {
        return Err(format!(
            "invalid duration {input:?}: negative values are not supported"
        ));
    }

    let mut remaining = unsigned_input;
    let mut total_nanos = 0_f64;
    while !remaining.is_empty() {
        let number_end = duration_number_end(remaining);
        if number_end == 0 {
            return Err(format!("invalid duration {input:?}"));
        }
        let number_text = &remaining[..number_end];
        let number = number_text
            .parse::<f64>()
            .map_err(|_| format!("invalid duration {input:?}"))?;
        if !number.is_finite() || number < 0.0 {
            return Err(format!("invalid duration {input:?}"));
        }

        remaining = &remaining[number_end..];
        let (unit_nanos, unit_len) = duration_unit(remaining)
            .ok_or_else(|| format!("invalid duration {input:?}: missing or unknown unit"))?;
        // 会对每个分段的小数纳秒部分截断，再把各段相加。
        total_nanos += (number * unit_nanos).trunc();
        if !total_nanos.is_finite() || total_nanos > i64::MAX as f64 {
            return Err(format!("invalid duration {input:?}: value is too large"));
        }
        remaining = &remaining[unit_len..];
    }

    Ok(Duration::from_nanos(total_nanos as u64))
}

/// 构造一个默认关闭、出现时置真的 Clap 布尔开关。
pub(crate) fn flag_arg(id: &'static str, help: &'static str) -> Arg {
    Arg::new(id).long(id).help(help).action(ArgAction::SetTrue)
}

/// 读取由 [`flag_arg`] 创建的布尔参数。
pub(crate) fn bool_value(matches: &ArgMatches, id: &str) -> bool {
    matches.get_flag(id)
}

/// 找出一个持续时间片段中十进制数字（含至多一个小数点）的末尾。
fn duration_number_end(value: &str) -> usize {
    let mut saw_digit = false;
    let mut saw_dot = false;
    let mut end = 0;
    for (index, character) in value.char_indices() {
        if character.is_ascii_digit() {
            saw_digit = true;
            end = index + character.len_utf8();
        } else if character == '.' && !saw_dot {
            saw_dot = true;
            end = index + 1;
        } else {
            break;
        }
    }
    if saw_digit { end } else { 0 }
}

/// 识别 持续时间单位，并返回其纳秒倍率与 UTF-8 字节长度。
fn duration_unit(value: &str) -> Option<(f64, usize)> {
    const UNITS: &[(&str, f64)] = &[
        ("ns", 1.0),
        ("us", 1_000.0),
        ("µs", 1_000.0),
        ("μs", 1_000.0),
        ("ms", 1_000_000.0),
        ("s", 1_000_000_000.0),
        ("m", 60_000_000_000.0),
        ("h", 3_600_000_000_000.0),
    ];
    UNITS
        .iter()
        .find(|(unit, _)| value.starts_with(unit))
        .map(|(unit, nanos)| (*nanos, unit.len()))
}
