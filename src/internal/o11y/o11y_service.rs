//! O11y 服务装配、显式上下文安装与有序关闭。

use std::sync::Arc;

use async_trait::async_trait;
use opentelemetry_sdk::Resource;
use tokio_util::sync::CancellationToken;

use crate::internal::logger::{LogHandler, Logger};

use super::{
    DiscardO11yService, MetricTags, O11yConfig, O11yContext, O11yError, O11yMetric, O11yService,
    O11yServiceImpl,
    discard_o11y_service::DiscardLogHandler,
    otel_logging::init_otel_logger,
    otel_metrics::{init_otel_metrics, make_otlp_exporter, make_stdout_exporter},
};

/// 创建 O11y 服务并返回安装了该服务的显式上下文。
///
/// `None` cancellation 对应 Go 的 nil context，并创建独立后台 token；`None` config
/// 使用零值配置。stdout exporter 优先于 OTLP endpoint，未配置 exporter 时仍创建 SDK
/// meter provider，但没有 reader，因而记录不会离开进程。
pub fn new_o11y_service(
    cancellation: Option<CancellationToken>,
    logger: Option<Logger>,
    config: Option<&O11yConfig>,
) -> Result<(Arc<O11yServiceImpl>, O11yContext), O11yError> {
    let cancellation = cancellation.unwrap_or_default();
    let config = config.cloned().unwrap_or_default();
    let resource = new_resource(&cancellation)?;

    let exporter = if config.use_stdout_metrics {
        Some(make_stdout_exporter()?)
    } else if config.metrics_endpoint.is_empty() {
        None
    } else {
        Some(make_otlp_exporter(&cancellation, &config)?)
    };

    let metrics_provider = Arc::new(init_otel_metrics(
        logger.clone(),
        Some(resource.clone()),
        exporter,
    )?);
    let mut log_handler: Arc<dyn LogHandler> = Arc::new(DiscardLogHandler);
    let logging_provider = if config.enable_logging {
        let provider = init_otel_logger(&cancellation, &resource, &config)?;
        log_handler = Arc::clone(&provider.handler);
        Some(provider)
    } else {
        None
    };

    let service = Arc::new(O11yServiceImpl {
        metrics_provider: Some(metrics_provider),
        logging_provider,
        log_handler,
    });
    let service_for_context: Arc<dyn O11yService> = service.clone();
    let context = O11yContext::with_service(cancellation, service_for_context);
    Ok((service, context))
}

/// 从显式上下文获取服务；缺少上下文时返回 discard 服务。
pub fn get_o11y_service(context: Option<&O11yContext>) -> Arc<dyn O11yService> {
    context
        .map(|context| Arc::clone(&context.service))
        .unwrap_or_else(|| Arc::new(DiscardO11yService))
}

/// 创建包含 SDK 默认服务名、telemetry SDK 信息与环境属性的 OTel resource。
///
/// Go `resource.Default()` 本身不失败，但调用前会观察 context 取消；Rust 保留相同检查
/// 与错误前缀，使已经取消的 session 不会继续创建 exporter 后台线程。
pub(crate) fn new_resource(cancellation: &CancellationToken) -> Result<Resource, O11yError> {
    if cancellation.is_cancelled() {
        return Err(O11yError::message(
            "failed to create otel resource: context canceled",
        ));
    }
    Ok(Resource::builder().build())
}

#[async_trait]
impl O11yService for O11yServiceImpl {
    /// 返回初始化时选定的 handler；构造不变量保证其永不为空。
    fn log_handler(&self) -> Arc<dyn LogHandler> {
        Arc::clone(&self.log_handler)
    }

    /// 先关闭 metrics，再关闭 logging，并像 Go `errors.Join` 一样汇总全部错误。
    async fn shutdown(&self, cancellation: &CancellationToken) -> Result<(), O11yError> {
        let mut errors = Vec::new();
        if let Some(provider) = &self.metrics_provider
            && let Err(error) = provider.shutdown(cancellation).await
        {
            errors.push(error.to_string());
        }
        if let Some(provider) = &self.logging_provider
            && let Err(error) = provider.shutdown(cancellation).await
        {
            errors.push(error.to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(O11yError::message(errors.join("\n")))
        }
    }

    /// 把 counter 记录委托给 metrics provider；未初始化 provider 时 no-op。
    fn increment(
        &self,
        _cancellation: &CancellationToken,
        metric: Option<&O11yMetric>,
        tags: Option<&MetricTags>,
    ) {
        if let Some(provider) = &self.metrics_provider {
            provider.increment(metric, tags);
        }
    }

    /// 把 gauge 记录委托给 metrics provider；未初始化 provider 时 no-op。
    fn record_gauge(
        &self,
        _cancellation: &CancellationToken,
        metric: Option<&O11yMetric>,
        value: f64,
        tags: Option<&MetricTags>,
    ) {
        if let Some(provider) = &self.metrics_provider {
            provider.record_gauge(metric, value, tags);
        }
    }
}
