//! 业务语义指标 helper 与函数耗时派生逻辑。

use std::{error::Error, time::Duration};

use crate::internal::config::SessionMode;

use super::{
    CLAUDE_CODE_END_COUNTER, ENV_MANAGER_END_COUNTER, ENV_MANAGER_START_COUNTER, KvTagProvider,
    O11yContext, O11yFunctionMetric, O11yMetric, ORCHESTRATOR_SESSION_END_COUNTER, TagProvider,
    error_tags, increment, merge_tags, record_gauge,
};

/// 记录环境管理器启动，并使用稳定字段名 `session_mode`。
pub fn increment_env_manager_start(context: &O11yContext, mode: SessionMode) {
    let mode_tags = KvTagProvider::one("session_mode", mode.as_str());
    increment(context, Some(&ENV_MANAGER_START_COUNTER), &[&mode_tags]);
}

/// 记录环境管理器结束。
///
/// 在启动和结束指标上分别使用 `session_mode` 与 `mode`；这里有意保留这一
/// 不对称，避免现有 dashboard 的标签维度发生变化。
pub fn increment_env_manager_end(
    context: &O11yContext,
    mode: SessionMode,
    error: Option<&(dyn Error + Send + Sync + 'static)>,
) {
    let mode_tags = KvTagProvider::one("mode", mode.as_str());
    let error_tags = error_tags(error);
    increment(
        context,
        Some(&ENV_MANAGER_END_COUNTER),
        &[&mode_tags, &error_tags],
    );
}

/// 记录 Claude Code 结束，并在失败时附加退出码或 `unknown`。
pub fn increment_claude_code_end(
    context: &O11yContext,
    error: Option<&(dyn Error + Send + Sync + 'static)>,
) {
    let error_tags = error_tags(error);
    increment(context, Some(&CLAUDE_CODE_END_COUNTER), &[&error_tags]);
}

/// 记录 orchestrator session 结束，并保留错误分类标签。
pub fn increment_orchestrator_session_end(
    context: &O11yContext,
    error: Option<&(dyn Error + Send + Sync + 'static)>,
) {
    let error_tags = error_tags(error);
    increment(
        context,
        Some(&ORCHESTRATOR_SESSION_END_COUNTER),
        &[&error_tags],
    );
}

/// 将一次函数执行同时记录为调用次数与毫秒耗时。
///
/// 错误标签最后合并，因此会覆盖调用方误传的同名 `error`/`error_code`。耗时使用
/// `Duration::as_millis` 舍弃不足 1ms 的部分。
pub fn record_duration(
    context: &O11yContext,
    metric: Option<&O11yFunctionMetric>,
    duration: Duration,
    error: Option<&(dyn Error + Send + Sync + 'static)>,
    tag_providers: &[&dyn TagProvider],
) {
    let Some(metric) = metric else {
        return;
    };

    let error_tags = error_tags(error);
    let mut providers = Vec::with_capacity(tag_providers.len() + 1);
    providers.extend_from_slice(tag_providers);
    providers.push(&error_tags);
    let merged = KvTagProvider::new(merge_tags(&providers));

    let count_metric = O11yMetric::new(
        format!("{}.count", metric.name),
        format!("{} (count)", metric.description),
        "1",
    );
    let latency_metric = O11yMetric::new(
        format!("{}.latency_ms", metric.name),
        format!("{} (latency in ms)", metric.description),
        "ms",
    );

    increment(context, Some(&count_metric), &[&merged]);
    record_gauge(
        context,
        Some(&latency_metric),
        duration.as_millis() as f64,
        &[&merged],
    );
}
