//! OpenTelemetry 指标与诊断日志设施。
//!
//! 上下文值由 `O11yContext` 显式表达，取消统一使用 `CancellationToken`；这避免全局可变状态，
//! 未安装服务时静默丢弃指标。

mod discard_o11y_service;
mod metric_types;
mod metrics;
mod o11y_service;
mod otel_metrics;
mod types;

pub mod diag;

pub use discard_o11y_service::DiscardO11yService;
pub use metric_types::{KvTagProvider, error_tags, merge_tags};
pub use metrics::{
    increment_claude_code_end, increment_env_manager_end, increment_env_manager_start,
    increment_orchestrator_session_end, record_duration,
};
pub use o11y_service::{get_o11y_service, new_o11y_service};
pub use otel_metrics::{increment, record_function_deferred, record_gauge};
pub use types::*;

#[cfg(test)]
mod metric_types_test;
#[cfg(test)]
mod metrics_test;
#[cfg(test)]
mod o11y_service_test;
#[cfg(test)]
mod otel_metrics_test;
