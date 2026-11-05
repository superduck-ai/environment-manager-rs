//! 指标业务 helper 与派生值回归测试。

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::internal::config::SessionMode;

use super::{
    CLAUDE_CODE_END_COUNTER, ENV_MANAGER_END_COUNTER, ENV_MANAGER_START_COUNTER,
    GIT_CHECKOUT_METRIC, KvTagProvider, MetricTags, O11yContext, O11yError, O11yFunctionMetric,
    O11yMetric, O11yService, ORCHESTRATOR_POLL_DURATION_METRIC, increment_claude_code_end,
    increment_env_manager_end, increment_env_manager_start, record_duration,
};

/// 一次被测试替身捕获的 counter 写入。
#[derive(Clone, Debug)]
pub(super) struct RecordedIncrement {
    pub(super) metric: O11yMetric,
    pub(super) tags: MetricTags,
}

/// 一次被测试替身捕获的 gauge 写入。
#[derive(Clone, Debug)]
pub(super) struct RecordedGauge {
    pub(super) metric: O11yMetric,
    pub(super) value: f64,
    pub(super) tags: MetricTags,
}

/// 只在内存中记录调用的 O11y 服务替身。
#[derive(Default)]
pub(super) struct RecordingO11yService {
    pub(super) increments: Mutex<Vec<RecordedIncrement>>,
    pub(super) gauges: Mutex<Vec<RecordedGauge>>,
}

#[async_trait]
impl O11yService for RecordingO11yService {
    /// 测试替身没有后台资源。
    async fn shutdown(&self, _cancellation: &CancellationToken) -> Result<(), O11yError> {
        Ok(())
    }

    /// 保存 counter 定义和标签快照。
    fn increment(
        &self,
        _cancellation: &CancellationToken,
        metric: Option<&O11yMetric>,
        tags: Option<&MetricTags>,
    ) {
        if let Some(metric) = metric {
            self.increments
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(RecordedIncrement {
                    metric: metric.clone(),
                    tags: copy_tags(tags),
                });
        }
    }

    /// 保存 gauge 定义、值和标签快照。
    fn record_gauge(
        &self,
        _cancellation: &CancellationToken,
        metric: Option<&O11yMetric>,
        value: f64,
        tags: Option<&MetricTags>,
    ) {
        if let Some(metric) = metric {
            self.gauges
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(RecordedGauge {
                    metric: metric.clone(),
                    value,
                    tags: copy_tags(tags),
                });
        }
    }
}

/// 创建安装了 recording service 的显式上下文。
pub(super) fn context_with_recording_service() -> (O11yContext, Arc<RecordingO11yService>) {
    let service = Arc::new(RecordingO11yService::default());
    let service_for_context: Arc<dyn O11yService> = service.clone();
    (
        O11yContext::with_service(CancellationToken::new(), service_for_context),
        service,
    )
}

/// 为异步/延迟断言复制调用时刻的标签快照。
fn copy_tags(tags: Option<&MetricTags>) -> MetricTags {
    tags.cloned().unwrap_or_default()
}

/// 验证 end helper 的指标选择、错误标签和 mode 字段。
#[test]
fn test_increment_end_metrics() {
    let (context, service) = context_with_recording_service();
    let error = std::io::Error::other("boom");
    increment_claude_code_end(&context, Some(&error));
    increment_env_manager_end(&context, SessionMode::ResumeCached, None);

    let increments = service
        .increments
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(increments.len(), 2);
    assert_eq!(increments[0].metric, CLAUDE_CODE_END_COUNTER);
    assert_eq!(
        increments[0].tags.get("error").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        increments[0].tags.get("error_code").map(String::as_str),
        Some("unknown")
    );
    assert_eq!(increments[1].metric, ENV_MANAGER_END_COUNTER);
    assert_eq!(
        increments[1].tags.get("mode").map(String::as_str),
        Some("resume-cached")
    );
    assert!(!increments[1].tags.contains_key("error"));
}

/// 验证启动 helper 使用稳定字段名 `session_mode`。
#[test]
fn test_increment_env_manager_start() {
    let (context, service) = context_with_recording_service();
    increment_env_manager_start(&context, SessionMode::Resume);

    let increments = service
        .increments
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(increments.len(), 1);
    assert_eq!(increments[0].metric, ENV_MANAGER_START_COUNTER);
    assert_eq!(
        increments[0].tags.get("session_mode").map(String::as_str),
        Some("resume")
    );
}

/// 验证代表性指标名称与单位。
#[test]
fn test_metric_definitions() {
    assert_eq!(GIT_CHECKOUT_METRIC.name, "git.checkout");
    assert_eq!(
        ORCHESTRATOR_POLL_DURATION_METRIC.name,
        "orchestrator.poll.duration_ms"
    );
    assert_eq!(ORCHESTRATOR_POLL_DURATION_METRIC.unit, "ms");
}

/// 验证函数指标同时派生 count 与 latency，并携带基础和错误标签。
#[test]
fn test_record_duration() {
    let (context, service) = context_with_recording_service();
    let metric = O11yFunctionMetric::new("env_manager.test", "Test operation");
    let phase = KvTagProvider::one("phase", "setup");
    let error = std::io::Error::other("boom");

    record_duration(
        &context,
        Some(&metric),
        std::time::Duration::from_millis(1_500),
        Some(&error),
        &[&phase],
    );

    let increments = service
        .increments
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(increments.len(), 1);
    assert_eq!(increments[0].metric.name, "env_manager.test.count");
    assert_eq!(increments[0].metric.description, "Test operation (count)");
    assert_eq!(increments[0].metric.unit, "1");
    drop(increments);

    let gauges = service
        .gauges
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(gauges.len(), 1);
    assert_eq!(gauges[0].metric.name, "env_manager.test.latency_ms");
    assert_eq!(
        gauges[0].metric.description,
        "Test operation (latency in ms)"
    );
    assert_eq!(gauges[0].metric.unit, "ms");
    assert_eq!(gauges[0].value, 1_500.0);
    assert_eq!(
        gauges[0].tags.get("phase").map(String::as_str),
        Some("setup")
    );
    assert_eq!(
        gauges[0].tags.get("error").map(String::as_str),
        Some("true")
    );
}
