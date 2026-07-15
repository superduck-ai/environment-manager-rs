//! 恢复版 OTel logging provider。
//!
//! Go 源文件的实现尚未连接 OTLP logs exporter：启用 logging 时仍返回 discard
//! handler。本文件精确保留该副作用边界，同时把 API 留作将来接入 OTel logs signal。

use std::sync::Arc;

use opentelemetry_sdk::Resource;
use tokio_util::sync::CancellationToken;

use super::{O11yConfig, O11yError, O11yLoggingProvider, discard_o11y_service::DiscardLogHandler};

/// 初始化恢复版 logger provider，并始终返回 discard handler。
///
/// 对应 Go `initOTelLogger`；OTel 缩写在 Rust 标识符中作为一个完整单词处理，参数
/// 仍保留 resource/config 边界以便未来接入真实 logs exporter。
pub(crate) fn init_otel_logger(
    _cancellation: &CancellationToken,
    _resource: &Resource,
    _config: &O11yConfig,
) -> Result<Arc<O11yLoggingProvider>, O11yError> {
    Ok(Arc::new(O11yLoggingProvider {
        handler: Arc::new(DiscardLogHandler),
    }))
}

impl O11yLoggingProvider {
    /// 当前 provider 没有 exporter 或后台任务，因此关闭始终成功。
    pub(crate) async fn shutdown(
        &self,
        _cancellation: &CancellationToken,
    ) -> Result<(), O11yError> {
        Ok(())
    }
}
