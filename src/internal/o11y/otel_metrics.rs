//! OpenTelemetry 0.32 metrics exporter、provider、instrument cache 与记录入口。

use std::{collections::HashMap, time::Instant};

use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Gauge, MeterProvider as _},
};
use opentelemetry_otlp::{Protocol, WithExportConfig as _, WithHttpConfig as _};
use opentelemetry_sdk::{
    Resource,
    metrics::{InstrumentKind, SdkMeterProvider, Temporality},
};
use tokio_util::sync::CancellationToken;

use super::{
    CachedInstrument, KvTagProvider, MetricTags, MetricsExporter, O11yConfig, O11yContext,
    O11yError, O11yFunctionMetric, O11yMetric, O11yMetricsProvider, SharedMetricError, TagProvider,
    get_o11y_service, merge_tags, metrics::record_duration,
};

const METER_SCOPE: &str =
    "github.com/anthropics/anthropic/api-go/environment-manager/internal/o11y";

/// 对任意 instrument kind 选择 cumulative temporality。
///
/// OTel Rust 0.32 将 Go 的 selector callback 简化为 exporter 级 `Temporality`；保留本
/// 函数名以便逐符号对照，并由两个 exporter 构造函数共同调用。
pub(crate) const fn cumulative_temporality_selector(
    _instrument_kind: InstrumentKind,
) -> Temporality {
    Temporality::Cumulative
}

/// 创建 SDK meter provider，并在配置 exporter 时挂载默认 60 秒 PeriodicReader。
///
/// 对应 Go `initOTelMetrics`；OTel 缩写按 Rust 单词命名，provider、resource 与
/// exporter 的组装行为仍集中在此唯一入口。
pub(crate) fn init_otel_metrics(
    resource: Option<Resource>,
    exporter: Option<MetricsExporter>,
) -> Result<O11yMetricsProvider, O11yError> {
    let mut builder = SdkMeterProvider::builder();
    if let Some(resource) = resource {
        builder = builder.with_resource(resource);
    }
    let meter_provider = match exporter {
        Some(MetricsExporter::Otlp(exporter)) => builder.with_periodic_exporter(exporter).build(),
        Some(MetricsExporter::Stdout(exporter)) => builder.with_periodic_exporter(exporter).build(),
        None => builder.build(),
    };
    let meter = meter_provider.meter(METER_SCOPE);

    Ok(O11yMetricsProvider {
        meter_provider: Some(meter_provider),
        instrument_cache: std::sync::Mutex::new(HashMap::new()),
        meter: std::sync::Mutex::new(Some(meter)),
    })
}

/// 创建 cumulative OTLP/HTTP protobuf metrics exporter。
///
/// Cargo 必须为 `opentelemetry-otlp` 启用 `reqwest-blocking-client`：0.32 的稳定
/// `PeriodicReader` 在专用线程上驱动 exporter，官方不支持在该 reader 中使用异步
/// `reqwest-client`。网络 I/O 因此不会占用 Tokio worker，TLS 仍由 rustls 提供。
pub(crate) fn make_otlp_exporter(
    cancellation: &CancellationToken,
    config: &O11yConfig,
) -> Result<MetricsExporter, O11yError> {
    if cancellation.is_cancelled() {
        return Err(O11yError::message(
            "failed to create OTLP metrics exporter: context canceled",
        ));
    }

    let mut builder = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_temporality(cumulative_temporality_selector(InstrumentKind::Counter));
    if !config.metrics_endpoint.is_empty() {
        builder = builder.with_endpoint(config.metrics_endpoint.clone());
    }
    if !config.headers.is_empty() {
        builder = builder.with_headers(config.headers.clone().into_iter().collect());
    }
    builder.build().map(MetricsExporter::Otlp).map_err(|error| {
        O11yError::message(format!("failed to create OTLP metrics exporter: {error}"))
    })
}

/// 创建人类可读、cumulative 的 stdout metrics exporter。
pub(crate) fn make_stdout_exporter() -> Result<MetricsExporter, O11yError> {
    Ok(MetricsExporter::Stdout(
        opentelemetry_stdout::MetricExporter::builder()
            .with_temporality(cumulative_temporality_selector(InstrumentKind::Counter))
            .build(),
    ))
}

impl O11yMetricsProvider {
    /// 刷新并关闭 SDK provider。
    ///
    /// OTel 0.32 的标准 reader 在专用线程上工作，`shutdown` 是同步操作；这里将其放入
    /// `spawn_blocking`，并与调用方 token 竞争，避免阻塞 Tokio worker。取消只停止等待，
    /// 已开始的 SDK 关闭仍在后台完成资源释放。
    pub(crate) async fn shutdown(&self, cancellation: &CancellationToken) -> Result<(), O11yError> {
        let Some(provider) = self.meter_provider.clone() else {
            return Ok(());
        };
        let shutdown = tokio::task::spawn_blocking(move || provider.shutdown());
        if cancellation.is_cancelled() {
            return Err(O11yError::message(
                "failed to shutdown otel metrics: context canceled",
            ));
        }

        tokio::select! {
            _ = cancellation.cancelled() => Err(O11yError::message(
                "failed to shutdown otel metrics: context canceled",
            )),
            result = shutdown => match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(O11yError::message(format!(
                    "failed to shutdown otel metrics: {error}"
                ))),
                Err(error) => Err(O11yError::message(format!(
                    "failed to shutdown otel metrics: {error}"
                ))),
            }
        }
    }

    /// 获取或创建单调 `u64` counter，并按 `counter:{name}` 缓存。
    pub(crate) fn get_or_create_counter(
        &self,
        metric: Option<&O11yMetric>,
    ) -> Result<Option<Counter<u64>>, O11yError> {
        let Some(metric) = metric else {
            return Ok(None);
        };
        let cache_key = format!("counter:{}", metric.name);
        let mut cache = self
            .instrument_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(cached) = cache.get(&cache_key) {
            return match cached {
                CachedInstrument::Counter(counter) => Ok(Some(counter.clone())),
                CachedInstrument::Gauge(_) => Err(O11yError::message(format!(
                    "cached instrument {:?} has wrong type",
                    metric.name
                ))),
            };
        }

        let meter = self.meter();
        let counter = meter
            .u64_counter(metric.name.clone())
            .with_description(metric.description.clone())
            .with_unit(metric.unit.clone())
            .build();
        cache.insert(cache_key, CachedInstrument::Counter(counter.clone()));
        Ok(Some(counter))
    }

    /// 获取或创建同步 `f64` gauge，并按 `gauge:{name}` 缓存。
    pub(crate) fn get_or_create_gauge(
        &self,
        metric: Option<&O11yMetric>,
    ) -> Result<Option<Gauge<f64>>, O11yError> {
        let Some(metric) = metric else {
            return Ok(None);
        };
        let cache_key = format!("gauge:{}", metric.name);
        let mut cache = self
            .instrument_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(cached) = cache.get(&cache_key) {
            return match cached {
                CachedInstrument::Gauge(gauge) => Ok(Some(gauge.clone())),
                CachedInstrument::Counter(_) => Err(O11yError::message(format!(
                    "cached instrument {:?} has wrong type",
                    metric.name
                ))),
            };
        }

        let meter = self.meter();
        let gauge = meter
            .f64_gauge(metric.name.clone())
            .with_description(metric.description.clone())
            .with_unit(metric.unit.clone())
            .build();
        cache.insert(cache_key, CachedInstrument::Gauge(gauge.clone()));
        Ok(Some(gauge))
    }

    /// 对应 Go：`o11yMetricsProvider.increment`，记录一次 counter 增量。
    ///
    /// 缺少 metric 时静默返回；instrument 创建或 cache 类型错误按 `metric`、`error`
    /// 顺序写 ERROR 并吞掉错误；成功时增加 1。日志 handler 故障和 OTel 记录均不会
    /// 反向改变业务控制流，保持 Go 无返回值方法的副作用边界。
    pub(crate) fn increment(&self, metric: Option<&O11yMetric>, tags: Option<&MetricTags>) {
        let counter = match self.get_or_create_counter(metric) {
            Ok(counter) => counter,
            Err(error) => {
                let metric_name = metric.map_or("", |metric| metric.name.as_ref());
                tracing::error!(metric = metric_name, error = %error, "Failed to get or create counter");
                return;
            }
        };
        if let Some(counter) = counter {
            counter.add(1, &tags_to_attributes(tags));
        }
    }

    /// 对应 Go：`o11yMetricsProvider.recordGauge`，记录一次同步 gauge 读数。
    ///
    /// 缺少 metric 时静默返回；instrument 创建或 cache 类型错误按 `metric`、`error`
    /// 顺序写 ERROR 并吞掉错误；成功时记录调用方给出的 `f64`。该路径不重试，也不会
    /// 因 tracing subscriber 或 exporter 状态改变调用方的返回结果。
    pub(crate) fn record_gauge(
        &self,
        metric: Option<&O11yMetric>,
        value: f64,
        tags: Option<&MetricTags>,
    ) {
        let gauge = match self.get_or_create_gauge(metric) {
            Ok(gauge) => gauge,
            Err(error) => {
                let metric_name = metric.map_or("", |metric| metric.name.as_ref());
                tracing::error!(metric = metric_name, error = %error, "Failed to get or create gauge");
                return;
            }
        };
        if let Some(gauge) = gauge {
            gauge.record(value, &tags_to_attributes(tags));
        }
    }

    /// 返回本 provider 的 meter；测试构造的空 meter 会回退到全局 provider。
    fn meter(&self) -> opentelemetry::metrics::Meter {
        let mut meter = self
            .meter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(meter) = meter.as_ref() {
            return meter.clone();
        }
        let fallback = global::meter(METER_SCOPE);
        *meter = Some(fallback.clone());
        fallback
    }
}

/// 合并标签并通过上下文中的服务增加 counter。
pub fn increment(
    context: &O11yContext,
    metric: Option<&O11yMetric>,
    tag_providers: &[&dyn TagProvider],
) {
    let tags = merge_tags(tag_providers);
    get_o11y_service(Some(context)).increment(context.cancellation(), metric, Some(&tags));
}

/// 合并标签并通过上下文中的服务写入 gauge。
pub fn record_gauge(
    context: &O11yContext,
    metric: Option<&O11yMetric>,
    value: f64,
    tag_providers: &[&dyn TagProvider],
) {
    let tags = merge_tags(tag_providers);
    get_o11y_service(Some(context)).record_gauge(
        context.cancellation(),
        metric,
        value,
        Some(&tags),
    );
}

/// 创建一个结束闭包，在调用时读取共享错误的最终值并记录函数耗时。
///
/// Go 可以捕获 `*error` 并在函数返回前修改；Rust 用 `SharedMetricError` 的
/// `Arc<Mutex<Option<Arc<dyn Error>>>>` 明确同步边界。基础标签在创建闭包时快照，错误
/// 则在执行闭包时读取，从而精确保留“晚到失败”语义。
pub fn record_function_deferred(
    context: &O11yContext,
    metric: Option<&O11yFunctionMetric>,
    error: Option<SharedMetricError>,
    tag_providers: &[&dyn TagProvider],
) -> impl FnOnce() + Send + 'static {
    let started = Instant::now();
    let context = context.clone();
    let metric = metric.cloned();
    let base_tags = KvTagProvider::new(merge_tags(tag_providers));
    move || {
        let final_error = error.as_ref().and_then(|error| {
            error
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        });
        record_duration(
            &context,
            metric.as_ref(),
            started.elapsed(),
            final_error.as_deref(),
            &[&base_tags],
        );
    }
}

/// 将有序标签 map 转换为 OTel attributes。
fn tags_to_attributes(tags: Option<&MetricTags>) -> Vec<KeyValue> {
    tags.map_or_else(Vec::new, |tags| {
        tags.iter()
            .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
            .collect()
    })
}
