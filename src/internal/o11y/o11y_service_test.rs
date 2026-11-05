//! O11y 上下文安装、fallback 与 deferred error 回归测试。

use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

use super::metrics_test::context_with_recording_service;
use super::{
    O11yFunctionMetric, O11yService, SharedMetricError, get_o11y_service, new_o11y_service,
    record_function_deferred,
};

/// 验证新服务被安装到返回上下文并可干净关闭。
#[tokio::test]
async fn test_new_o11y_service_installs_service_in_context() {
    let (service, context) = new_o11y_service(None, None).expect("create o11y service");
    assert!(Arc::ptr_eq(
        &(service.clone() as Arc<dyn O11yService>),
        &get_o11y_service(Some(&context))
    ));
    service
        .shutdown(&CancellationToken::new())
        .await
        .expect("shutdown o11y service");
}

/// 验证缺少上下文时返回无副作用 discard 服务。
#[tokio::test]
async fn test_get_o11y_service_falls_back_to_discard() {
    let service = get_o11y_service(None);
    service
        .shutdown(&CancellationToken::new())
        .await
        .expect("discard shutdown");
}

/// 验证 deferred closure 在执行时而非创建时读取最终错误。
#[test]
fn test_record_function_deferred_uses_final_error() {
    let (context, service) = context_with_recording_service();
    let error: SharedMetricError = Arc::new(Mutex::new(None));
    let metric = O11yFunctionMetric::new("env_manager.deferred", "Deferred operation");
    let done = record_function_deferred(&context, Some(&metric), Some(Arc::clone(&error)), &[]);

    *error
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) =
        Some(Arc::new(std::io::Error::other("late failure")));
    done();

    let increments = service
        .increments
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(increments.len(), 1);
    assert_eq!(increments[0].metric.name, "env_manager.deferred.count");
    assert_eq!(
        increments[0].tags.get("error").map(String::as_str),
        Some("true")
    );
}
