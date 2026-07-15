//! 未配置可观测性时的无副作用服务和日志 handler。

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::internal::logger::{LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord};

use super::{MetricTags, O11yError, O11yMetric, O11yService};

/// 静默丢弃所有结构化日志的 handler。
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct DiscardLogHandler;

impl LogHandler for DiscardLogHandler {
    /// 复刻 `slog.NewTextHandler(io.Discard, nil)` 的默认 Info 级别门槛。
    ///
    /// handler 虽然不产生 I/O，但 Info/Warn/Error 仍报告 enabled；这会保留调用方是否
    /// 构造结构化属性的可观察行为，而 Trace/Debug 在进入 handler 前被过滤。
    fn enabled(&self, level: LogLevel) -> bool {
        level >= LogLevel::Info
    }

    /// 即使被直接调用也始终成功且不产生 I/O。
    fn handle(&self, _record: &LogRecord) -> Result<(), LogHandlerError> {
        Ok(())
    }

    /// 固定属性对 discard handler 没有影响。
    fn with_attrs(&self, _attributes: LogAttributes) -> Arc<dyn LogHandler> {
        Arc::new(*self)
    }

    /// group 对 discard handler 没有影响。
    fn with_group(&self, _name: &str) -> Arc<dyn LogHandler> {
        Arc::new(*self)
    }
}

/// 未安装真实 exporter 时的 O11y 服务。
#[derive(Clone, Copy, Debug, Default)]
pub struct DiscardO11yService;

#[async_trait]
impl O11yService for DiscardO11yService {
    /// 每次返回一个轻量 discard handler。
    fn log_handler(&self) -> Arc<dyn LogHandler> {
        Arc::new(DiscardLogHandler)
    }

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
