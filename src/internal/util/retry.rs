//! 支持指数退避、错误分类和取消的异步重试。
//!
//! 与 Go 版本相同，最大次数包含第一次尝试，延迟序列为
//! `base_delay * 2^(attempt-1)`。退避 sleep 不响应取消；操作阶段通过取消令牌复刻 Go
//! 闭包中 `http.NewRequestWithContext` 的取消行为。

use std::{sync::Arc, time::Duration};

use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::internal::logger::{LogAttributes, default_slog_logger};

/// 判断某个具体错误是否允许重试的线程安全分类器。
pub type ErrorClassifier<E> = Arc<dyn Fn(&E) -> bool + Send + Sync + 'static>;

/// 某种错误类型对应的重试配置。
pub struct RetryConfig<E> {
    /// 包含首次调用在内的最大尝试次数。
    pub max_retries: usize,
    /// 第一次重试前的基础等待时间。
    pub base_delay: Duration,
    /// 返回 `true` 表示错误可重试；`None` 表示所有错误均可重试。
    pub error_classifier: Option<ErrorClassifier<E>>,
}

impl<E> RetryConfig<E> {
    /// 创建默认重试所有错误的配置。
    pub fn new(max_retries: usize, base_delay: Duration) -> Self {
        Self {
            max_retries,
            base_delay,
            error_classifier: None,
        }
    }

    /// 设置错误分类器，并返回更新后的配置以便链式构造。
    pub fn with_classifier<F>(mut self, classifier: F) -> Self
    where
        F: Fn(&E) -> bool + Send + Sync + 'static,
    {
        self.error_classifier = Some(Arc::new(classifier));
        self
    }
}

/// 重试最终失败的原因。
#[derive(Debug, Error)]
pub enum RetryError<E> {
    /// 最大次数必须至少为一次。
    #[error("max_retries must be at least 1")]
    InvalidMaxRetries,
    /// 操作被取消。
    #[error("operation was cancelled")]
    Cancelled,
    /// 操作遇到不可重试错误或耗尽次数。
    #[error("operation failed: {0}")]
    Operation(E),
}

/// 执行异步操作，直到成功、不可重试、取消或耗尽次数。
///
/// `log_attributes` 对应 Go 的 `[]interface{}` 固定日志属性；本函数会在其后按原始
/// 顺序追加 attempt、delay、error 等字段。操作 future 在取消时会被丢弃，因此
/// 调用方应确保 future 的 drop 是取消安全的，或在操作内部屏蔽取消。
///
/// # 兼容性
/// 四类重试事件必须经过 Go 进程级 `slog.Default()` 的 legacy stderr handler，
/// 不能使用调用方的应用 JSON logger。退避 sleep 也故意不可被取消打断，与 Go
/// `time.Sleep` 一致；取消会在下一次操作开始时被观察。
pub async fn retry_operation<T, E, F, Fut>(
    cancellation: &CancellationToken,
    mut operation: F,
    config: &RetryConfig<E>,
    log_attributes: LogAttributes,
) -> Result<T, RetryError<E>>
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::error::Error,
{
    if config.max_retries == 0 {
        return Err(RetryError::InvalidMaxRetries);
    }

    for attempt in 0..config.max_retries {
        if attempt > 0 {
            let shift = u32::try_from(attempt - 1).unwrap_or(u32::MAX);
            let multiplier = 1_u32.checked_shl(shift).unwrap_or(u32::MAX);
            let delay = config.base_delay.saturating_mul(multiplier);
            let mut attributes = log_attributes.clone();
            attributes.insert("attempt".to_owned(), serde_json::json!(attempt + 1));
            attributes.insert(
                "delay_ms".to_owned(),
                serde_json::json!(duration_milliseconds(delay)),
            );
            default_slog_logger().info("retrying_operation", attributes);
            // Go 使用不可取消的 time.Sleep。即使 context 已取消，也要先完成本轮
            // backoff，之后才由操作等待分支观察取消，不能提前改变返回时序。
            tokio::time::sleep(delay).await;
        }

        let result = tokio::select! {
            // Go 的 HTTP operation 在进入时若 context 已取消，会确定性返回取消错误。
            // Tokio 默认会随机选择多个已就绪分支；这里固定让取消优先，避免一个立即
            // 返回错误的 future 抢先执行并把已取消状态误报为“重试耗尽”。
            biased;
            _ = cancellation.cancelled() => return Err(RetryError::Cancelled),
            result = operation(attempt + 1) => result,
        };
        match result {
            Ok(value) => return Ok(value),
            Err(error) => {
                let retryable = config
                    .error_classifier
                    .as_ref()
                    .is_none_or(|classifier| classifier(&error));
                if !retryable {
                    let mut attributes = log_attributes.clone();
                    attributes.insert("attempt".to_owned(), serde_json::json!(attempt + 1));
                    attributes.insert("error".to_owned(), serde_json::json!(error.to_string()));
                    attributes.insert("attempts".to_owned(), serde_json::json!(config.max_retries));
                    default_slog_logger().info("error_not_retryable", attributes);
                    return Err(RetryError::Operation(error));
                }
                let mut attributes = log_attributes.clone();
                attributes.insert("attempt".to_owned(), serde_json::json!(attempt + 1));
                attributes.insert("error".to_owned(), serde_json::json!(error.to_string()));
                default_slog_logger().warn("operation_attempt_failed", attributes);
                if attempt + 1 == config.max_retries {
                    let mut attributes = log_attributes.clone();
                    attributes.insert("attempts".to_owned(), serde_json::json!(config.max_retries));
                    attributes.insert("error".to_owned(), serde_json::json!(error.to_string()));
                    default_slog_logger().error("operation_failed_after_retries", attributes);
                    return Err(RetryError::Operation(error));
                }
            }
        }
    }

    unreachable!("max_retries was validated and every loop branch returns on the last attempt")
}

/// 将 Rust Duration 转成 Go `time.Duration.Milliseconds()` 的截断整数。
fn duration_milliseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
