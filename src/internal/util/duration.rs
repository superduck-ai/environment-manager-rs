//! Duration 的日志数值与文本格式化工具。
//!
//! 项目中的 tracing duration 字段统一使用纳秒整数编码，文本使用紧凑的
//! `h`/`m`/`s`/`ms`/`µs`/`ns` 组合。超过 `int64` 纳秒上限的 Rust 值会饱和，避免
//! JSON 数值回绕。

use std::time::Duration;

/// 把非负 Rust 时长格式化为紧凑 duration 文本。
///
/// 小于一秒时依次选择 `ns`、
/// `µs`、`ms`，大于等于一秒时展开小时、分钟和秒；所有小数都保留纳秒精度并删除
/// 末尾零。项目中的错误文本和字符串日志属性必须共用本函数，不能使用 Rust
/// `Debug` 格式，因为后者不会把 `90s` 展开为 `1m30s`。
///
/// # 参数
/// - `duration`：需要格式化的非负时长；所有权不转移。
///
/// # 返回
/// 返回紧凑字符串，例如 `0s`、`1.5ms`、`1m30s`。
#[must_use]
pub(crate) fn format_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos == 0 {
        return "0s".to_owned();
    }
    if nanos < 1_000 {
        return format!("{nanos}ns");
    }
    if nanos < 1_000_000 {
        return format_subsecond(nanos, 1_000, "µs");
    }
    if nanos < 1_000_000_000 {
        return format_subsecond(nanos, 1_000_000, "ms");
    }

    let total_seconds = duration.as_secs();
    let hours = total_seconds / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;
    let second_text = if duration.subsec_nanos() == 0 {
        format!("{seconds}s")
    } else {
        // 最多保留九位秒小数，并从右侧删除零；固定宽度是保留中间零的关键。
        let fraction = format!("{:09}", duration.subsec_nanos())
            .trim_end_matches('0')
            .to_owned();
        format!("{seconds}.{fraction}s")
    };
    if hours > 0 {
        format!("{hours}h{minutes}m{second_text}")
    } else if minutes > 0 {
        format!("{minutes}m{second_text}")
    } else {
        second_text
    }
}

/// 把 Rust 时长编码为 tracing 使用的有符号纳秒整数。
///
/// 饱和分支保护意外构造的超大值，防止强制转换回绕为负数。
///
/// # 参数
/// - `duration`：需要写入结构化日志的非负时长。
///
/// # 返回
/// 纳秒整数；超过 `i64::MAX` 纳秒时返回 `i64::MAX`。
#[must_use]
pub(crate) fn duration_nanoseconds(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

/// 格式化小于一秒的 duration，并删除小数末尾零。
///
/// `unit_nanos` 只能是 `1_000` 或 `1_000_000`，分别生成微秒和毫秒文本；调用方已
/// 按数量级分流，因此整数部分始终处于 `1..=999`。
fn format_subsecond(nanos: u128, unit_nanos: u128, suffix: &str) -> String {
    let whole = nanos / unit_nanos;
    let remainder = nanos % unit_nanos;
    if remainder == 0 {
        return format!("{whole}{suffix}");
    }
    let width = unit_nanos.ilog10() as usize;
    let fraction = format!("{remainder:0width$}")
        .trim_end_matches('0')
        .to_owned();
    format!("{whole}.{fraction}{suffix}")
}
