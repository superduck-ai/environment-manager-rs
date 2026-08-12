//! 未配置可观测性时的无副作用服务。

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::{MetricTags, O11yError, O11yMetric, O11yService};

/// 未安装真实 exporter 时的 O11y 服务。
#[derive(Clone, Copy, Debug, Default)]
pub struct DiscardO11yService;

#[async_trait]
impl O11yService for DiscardO11yService {
    /// discard 服务没有需要 flush 或释放的资源。
    async fn shutdown(&self, _cancellation: &CancellationToken) -> Result<(), O11yError> {
        Ok(())
    }

    /// counter 记录被有意忽略。
    fn increment(
        &self,
        _cancellation: &CancellationToken,
        _metric: Option<&O11yMetric>,
        _tags: Option<&MetricTags>,
    ) {
    }

    /// gauge 记录被有意忽略。
    fn record_gauge(
        &self,
        _cancellation: &CancellationToken,
        _metric: Option<&O11yMetric>,
        _value: f64,
        _tags: Option<&MetricTags>,
    ) {
    }
}
