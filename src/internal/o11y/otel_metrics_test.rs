//! OTel 0.32 exporter、provider、cache、精确日志与取消路径的静态行为测试。

use opentelemetry_sdk::metrics::{InstrumentKind, Temporality};
use tokio_util::sync::CancellationToken;

use super::{
    CachedInstrument, MetricsExporter, O11yConfig, O11yMetric, O11yService, get_o11y_service,
    o11y_service::new_resource,
    otel_metrics::{
        cumulative_temporality_selector, init_otel_metrics, make_otlp_exporter,
        make_stdout_exporter,
    },
};

/// 验证 stdout exporter 使用强类型 metrics variant。
#[test]
fn test_make_stdout_exporter() {
    assert!(matches!(
        make_stdout_exporter().expect("stdout exporter"),
        MetricsExporter::Stdout(_)
    ));
    assert_eq!(
        cumulative_temporality_selector(InstrumentKind::Counter),
        Temporality::Cumulative
    );
}

/// 验证 OTLP/HTTP endpoint 与 headers 可用于构造 exporter，构造阶段不发网络请求。
#[test]
fn test_make_otlp_exporter() {
    let config = O11yConfig {
        metrics_endpoint: "http://127.0.0.1:4318/v1/metrics".to_owned(),
        headers: [("Authorization".to_owned(), "Bearer token".to_owned())]
            .into_iter()
            .collect(),
        ..O11yConfig::default()
    };
    assert!(matches!(
        make_otlp_exporter(&CancellationToken::new(), &config).expect("OTLP exporter"),
        MetricsExporter::Otlp(_)
    ));
}

/// 验证 resource 与 exporter 被装配到 SDK provider，并创建 meter。
#[tokio::test]
async fn test_init_o_tel_metrics_uses_exporter_and_resource() {
    let provider = init_otel_metrics(
        Some(new_resource(&CancellationToken::new()).expect("resource")),
        Some(make_stdout_exporter().expect("stdout exporter")),
    )
    .expect("metrics provider");
    assert!(provider.meter_provider.is_some());
    assert!(
        provider
            .meter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
    );
    provider
        .shutdown(&CancellationToken::new())
        .await
        .expect("provider shutdown");
}

/// 验证 instrument cache 返回同类实例并报告被测试注入的错误类型。
#[test]
fn test_init_o_tel_metrics_reports_unexpected_types() {
    let provider = init_otel_metrics(None, None).expect("metrics provider");
    let metric = O11yMetric::new("cache.test", "Cache test", "1");
    assert!(
        provider
            .get_or_create_counter(Some(&metric))
            .expect("counter")
            .is_some()
    );
    assert_eq!(
        provider
            .instrument_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len(),
        1
    );

    let wrong_metric = O11yMetric::new("wrong.test", "Wrong test", "1");
    let gauge = provider
        .get_or_create_gauge(Some(&wrong_metric))
        .expect("gauge")
        .expect("gauge instrument");
    provider
        .instrument_cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(
            "counter:wrong.test".to_owned(),
            CachedInstrument::Gauge(gauge),
        );
    let error = provider
        .get_or_create_counter(Some(&wrong_metric))
        .expect_err("wrong cache type must fail");
    assert!(error.to_string().contains("has wrong type"));
}

/// 验证 stdout 配置仍把服务安装进上下文。
#[tokio::test]
async fn test_new_o11y_service_creates_configured_metrics_provider() {
    let config = O11yConfig {
        use_stdout_metrics: true,
        ..O11yConfig::default()
    };
    let (service, context) = super::new_o11y_service(Some(CancellationToken::new()), Some(&config))
        .expect("create stdout o11y service");
    assert!(service.metrics_provider.is_some());
    assert!(
        service
            .metrics_provider
            .as_ref()
            .and_then(|provider| provider.meter_provider.as_ref())
            .is_some()
    );
    assert!(std::sync::Arc::ptr_eq(
        &(service.clone() as std::sync::Arc<dyn O11yService>),
        &get_o11y_service(Some(&context))
    ));
    service
        .shutdown(&CancellationToken::new())
        .await
        .expect("service shutdown");
}

/// 验证 resource 初始化在已取消 session 中保留稳定错误前缀。
#[test]
fn test_new_resource_honors_canceled_context() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = new_resource(&cancellation).expect_err("canceled resource must fail");
    assert!(error.to_string().contains("failed to create otel resource"));
}
