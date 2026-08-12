//! 支持指数退避、错误分类和取消的异步重试。
//!
//! 与 Go 版本相同，最大次数包含第一次尝试，延迟序列为
//! `base_delay * 2^(attempt-1)`。通用循环不直接观察取消；具体 operation 负责像 Go
//! 闭包中的 `http.NewRequestWithContext` 一样把取消转换成可分类错误。

use std::{sync::Arc, time::Duration};

use thiserror::Error;

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
    /// 操作遇到不可重试错误或耗尽次数。
    #[error("operation failed: {0}")]
    Operation(E),
}

/// 执行异步操作，直到成功、不可重试或耗尽次数。
///
/// 取消不是通用重试器的控制信号：需要取消
/// 网络请求的调用方必须在 `operation` 内观察自己的 token，并把取消转换为普通错误，
/// 让同一错误分类器和日志路径处理它。
///
/// # 兼容性
/// 四类重试事件使用进程级 tracing subscriber。退避 sleep 故意不可被取消打断，
/// 与 Go
/// `time.Sleep` 一致。即使调用方在退避期间取消，sleep 完成后仍必须调用一次
/// `operation`；这复刻 Go 闭包在 `time.Sleep` 后创建带 context 的 HTTP 请求，并
/// 由该请求返回 `context.Canceled` 的时序。
pub async fn retry_operation<T, E, F, Fut>(
    mut operation: F,
    config: &RetryConfig<E>,
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
            tracing::info!(
                attempt = attempt + 1,
                delay_ms = duration_milliseconds(delay),
                "retrying_operation"
            );
            // Go 使用不可取消的 time.Sleep。即使 context 已取消，也要先完成本轮
            // backoff，之后才由操作等待分支观察取消，不能提前改变返回时序。
            tokio::time::sleep(delay).await;
        }

        // 不在通用层 select cancellation。Go RetryOperation 无取消分支；无论 context
        // 是否已经取消，每一轮都同步调用 operation，并等待它返回后再执行分类和日志。
        let result = operation(attempt + 1).await;
        match result {
            Ok(value) => return Ok(value),
            Err(error) => {
                let retryable = config
                    .error_classifier
                    .as_ref()
                    .is_none_or(|classifier| classifier(&error));
                if !retryable {
                    tracing::info!(attempt = attempt + 1, error = %error, attempts = config.max_retries, "error_not_retryable");
                    return Err(RetryError::Operation(error));
                }
                tracing::warn!(attempt = attempt + 1, error = %error, "operation_attempt_failed");
                if attempt + 1 == config.max_retries {
                    tracing::error!(attempts = config.max_retries, error = %error, "operation_failed_after_retries");
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
