//! 多个命令共享的纯解析工具。
//!
//! 这里刻意不依赖日志实现或异步运行时，使命令层可以在内部模块尚未完成时独立
//! 测试。持续时间解析覆盖 Go `time.ParseDuration` 的 CLI 常用语法，从而不会把
//! 原有 `5m`、`1h30m` 等部署参数强制改写成另一套格式。

use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use std::time::Duration;

use clap::{Arg, ArgAction, ArgMatches, Command, value_parser};

/// 与 Go `slog` 命令行取值一一对应的日志级别。
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

    /// 严格解析 Go 命令接受的四个小写值；不隐式接受大小写变体。
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_log_level(value)
    }
}

/// 解析日志级别，并保留 Go `parseLogLevel` 的错误文本。
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

/// 对应 Go：`cmd/utils.go` 中的 `markRequiredFlags`。
///
/// 在命令的参数全部注册后，把指定参数统一标记为必填。这个辅助函数保留 Go
/// 版本“先声明 flag、再集中标记 required”的构造顺序，使迁移符号可以直接对照，
/// 也避免必填规则分散在较长的 Clap builder 链中。
///
/// # 参数
/// - `command`：尚未用于解析参数的 Clap 命令；函数取得其所有权并更新参数定义。
/// - `flags`：需要标记为必填的 Clap 参数 ID，必须已经注册在 `command` 中。
///
/// # 返回
/// 返回已更新必填约束的命令，供调用方继续注册到根命令。
///
/// # Panics
/// 与 Go 实现一致，参数 ID 不存在表示程序构造错误，函数会立即 panic，而不会把
/// 该错误延迟到用户运行命令时才暴露。
pub(crate) fn mark_required_flags(mut command: Command, flags: &[&str]) -> Command {
    for flag in flags {
        if !command
            .get_arguments()
            .any(|argument| argument.get_id().as_str() == *flag)
        {
            panic!("failed to mark {flag} as required: flag {flag:?} does not exist");
        }

        command = command.mut_arg(flag, |argument| argument.required(true));
    }
    command
}

/// 将 CLI 日志级别映射为内部 logger 的等价级别。
///
/// 两个枚举刻意分离：CLI 层需要保持 Cobra 接受的字符串集合，logger 层还包含
/// `Trace`。集中转换可避免各命令在默认级别或大小写规则上出现漂移。
pub(crate) const fn logger_level(level: LogLevel) -> crate::internal::logger::LogLevel {
    match level {
        LogLevel::Debug => crate::internal::logger::LogLevel::Debug,
        LogLevel::Info => crate::internal::logger::LogLevel::Info,
        LogLevel::Warn => crate::internal::logger::LogLevel::Warn,
        LogLevel::Error => crate::internal::logger::LogLevel::Error,
    }
}

/// 解析 Go `time.ParseDuration` 风格的非负持续时间。
///
/// 支持 `ns`、`us`、`µs`、`μs`、`ms`、`s`、`m`、`h` 以及连续组合和小数，
/// 例如 `500ms`、`1.5s`、`1h30m`。CLI 的超时没有负值语义，因此负数会被明确
/// 拒绝；`0` 则保留为“不设超时”的兼容值。
pub fn parse_go_duration(input: &str) -> Result<Duration, String> {
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
        // Go 会对每个分段的小数纳秒部分截断，再把各段相加。
        total_nanos += (number * unit_nanos).trunc();
        if !total_nanos.is_finite() || total_nanos > i64::MAX as f64 {
            return Err(format!("invalid duration {input:?}: value is too large"));
        }
        remaining = &remaining[unit_len..];
    }

    Ok(Duration::from_nanos(total_nanos as u64))
}

/// 构造一个默认关闭、出现时置真的 Cobra 风格布尔开关。
///
/// 所有普通 bool flag 通过同一帮助/动作定义生成，避免不同子命令意外显示 `false`
/// 默认值或采用不一致的重复参数策略。
pub(crate) fn switch_arg(id: &'static str, help: &'static str) -> Arg {
    Arg::new(id)
        .long(id)
        .help(help)
        .action(ArgAction::Set)
        .num_args(0..=1)
        .require_equals(true)
        .default_missing_value("true")
        .default_value("false")
        .hide_default_value(true)
        .overrides_with(id)
        .value_name("bool")
        .value_parser(value_parser!(bool))
}

/// 读取由 [`switch_arg`] 创建、始终具有默认值的布尔参数。
pub(crate) fn bool_value(matches: &ArgMatches, id: &str) -> bool {
    *matches
        .get_one::<bool>(id)
        .unwrap_or_else(|| panic!("CLI invariant: missing boolean value for {id}"))
}

/// 构造用于兼容 Cobra 默认 `Args` 校验的隐藏位置参数。
///
/// Cobra 在未配置 `Args` 回调时允许多余的位置参数，旧命令随后忽略它们；Clap
/// 默认会拒绝。每个普通子命令加入该参数即可保留这个较少见但真实存在的接口，
/// 同时未知的 `--flag` 仍由 Clap 报错。
pub(crate) fn ignored_positionals() -> Arg {
    Arg::new("_ignored-positionals").num_args(0..).hide(true)
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

/// 识别 Go 持续时间单位，并返回其纳秒倍率与 UTF-8 字节长度。
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
