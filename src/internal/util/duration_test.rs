//! Go duration 兼容格式与纳秒日志编码的表驱动测试。

use std::time::Duration;

use super::{format_go_duration, go_duration_nanoseconds};

/// 覆盖 Go `time.Duration.String()` 的全部单位切换和复合时长边界。
#[test]
fn format_go_duration_matches_go_positive_duration_strings() {
    let cases = [
        (Duration::ZERO, "0s"),
        (Duration::from_nanos(1), "1ns"),
        (Duration::from_nanos(999), "999ns"),
        (Duration::from_nanos(1_000), "1µs"),
        (Duration::from_nanos(1_001), "1.001µs"),
        (Duration::from_nanos(1_100), "1.1µs"),
        (Duration::from_nanos(999_999), "999.999µs"),
        (Duration::from_millis(1), "1ms"),
        (Duration::from_nanos(1_001_000), "1.001ms"),
        (Duration::from_nanos(999_999_999), "999.999999ms"),
        (Duration::from_secs(1), "1s"),
        (Duration::from_millis(1_500), "1.5s"),
        (Duration::from_secs(60), "1m0s"),
        (Duration::from_secs(90), "1m30s"),
        (Duration::from_secs(7_200), "2h0m0s"),
        (
            Duration::from_secs(2 * 3_600 + 3 * 60 + 4) + Duration::from_nanos(5_006_007),
            "2h3m4.005006007s",
        ),
        (
            Duration::from_nanos(i64::MAX as u64),
            "2562047h47m16.854775807s",
        ),
    ];

    for (duration, expected) in cases {
        assert_eq!(format_go_duration(duration), expected, "{duration:?}");
    }
}

/// 验证 `slog.Duration` 数值使用纳秒，超出 Go 范围时不会回绕。
#[test]
fn go_duration_nanoseconds_uses_int64_nanoseconds() {
    assert_eq!(go_duration_nanoseconds(Duration::ZERO), 0);
    assert_eq!(
        go_duration_nanoseconds(Duration::from_millis(1_500)),
        1_500_000_000
    );
    assert_eq!(
        go_duration_nanoseconds(Duration::from_nanos(i64::MAX as u64)),
        i64::MAX
    );
    assert_eq!(
        go_duration_nanoseconds(Duration::from_nanos(i64::MAX as u64 + 1)),
        i64::MAX
    );
}
