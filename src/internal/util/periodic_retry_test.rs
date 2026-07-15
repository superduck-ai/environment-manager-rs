//! 对应 Go `internal/util/periodic_retry_test.go` 的周期执行与重试测试。
//!
//! Go 构造器以 panic 报告编程错误；Rust 内部 API 使用类型化错误表达相同约束。
//! 其余调用次数、错误分类、耗尽返回与可重复停止时序保持一致。

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use crate::internal::logger::{
    LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, Logger,
};

use super::{PeriodicInvoker, PeriodicInvokerError, RetryConfig, RetryError, retry_operation};

/// 对应 Go：`periodic_retry_test.go` 中的 `utilTestLogger`。
///
/// 不写任何输出的测试 handler。
struct DiscardHandler;

impl LogHandler for DiscardHandler {
    fn enabled(&self, _level: LogLevel) -> bool {
        false
    }

    fn handle(&self, _record: &LogRecord) -> Result<(), LogHandlerError> {
        Ok(())
    }

    fn with_attrs(&self, _attributes: LogAttributes) -> Arc<dyn LogHandler> {
        Arc::new(Self)
    }

    fn with_group(&self, _name: &str) -> Arc<dyn LogHandler> {
        Arc::new(Self)
    }
}

/// 对应 Go `utilTestLogger`，保留显式 logger 参数但丢弃测试日志。
fn util_test_logger() -> Logger {
    Logger::new(Arc::new(DiscardHandler))
}

/// 构造与 Go 重试测试相同的固定 `operation=test` 日志属性。
fn retry_test_attributes() -> LogAttributes {
    [("operation".to_owned(), serde_json::json!("test"))]
        .into_iter()
        .collect()
}

/// 等待异步计数达到目标；一秒未完成即视为后台任务时序错误。
async fn wait_until(calls: &AtomicUsize, target: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while calls.load(Ordering::SeqCst) < target {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("周期任务未在一秒内达到预期调用次数");
}

/// 对应 Go：`periodic_retry_test.go` 中的 `TestPeriodicInvokerValidationPanics`。
///
/// Rust 版本不接受可空 logger/函数，并将唯一可由运行时输入触发的零间隔错误改为
/// `Result`；这里验证错误文本仍与 Go panic 文本逐字一致。
#[test]
fn test_periodic_invoker_validation_panics() {
    let error = match PeriodicInvoker::new(util_test_logger(), Duration::ZERO, || async {
        Ok::<_, io::Error>(())
    }) {
        Err(error) => error,
        Ok(_) => panic!("零间隔必须被拒绝"),
    };
    assert_eq!(error.to_string(), "interval must be positive, got 0s");
}

/// 对应 Go：`periodic_retry_test.go` 中的 `TestPeriodicInvokerStartStopAndDoubleStart`。
///
/// 验证首次调用无需等待 interval、重复启动被拒绝，以及多次停止保持幂等。
#[tokio::test]
async fn test_periodic_invoker_start_stop_and_double_start() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let invoker = PeriodicInvoker::new(util_test_logger(), Duration::from_millis(1), move || {
        let calls = Arc::clone(&observed);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, io::Error>(())
        }
    })
    .unwrap();

    assert!(!invoker.is_running());
    invoker.start().unwrap();
    assert!(invoker.is_running());
    wait_until(&calls, 1).await;
    assert!(matches!(
        invoker.start(),
        Err(PeriodicInvokerError::AlreadyRunning)
    ));
    invoker.stop();
    invoker.stop();
    invoker.stop_and_wait().await;
    assert!(!invoker.is_running());
}

/// 对应 Go：`periodic_retry_test.go` 中的 `TestPeriodicInvokerContinuesAfterFunctionError`。
///
/// 首次 callback 故意失败，确认后台循环只记录错误而不会终止后续调用。
#[tokio::test]
async fn test_periodic_invoker_continues_after_function_error() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let invoker = PeriodicInvoker::new(util_test_logger(), Duration::from_millis(1), move || {
        let calls = Arc::clone(&observed);
        async move {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                Err(io::Error::other("first call"))
            } else {
                Ok(())
            }
        }
    })
    .unwrap();
    invoker.start().unwrap();
    wait_until(&calls, 2).await;
    invoker.stop_and_wait().await;
}

/// 对应 Go：`periodic_retry_test.go` 中的 `TestRetryOperationSucceedsAfterRetry`。
///
/// 操作前两次返回可重试错误，第三次返回成功值，验证最大次数包含首次调用。
#[tokio::test]
async fn test_retry_operation_succeeds_after_retry() {
    let attempts = AtomicUsize::new(0);
    let value = retry_operation(
        &CancellationToken::new(),
        |_| {
            let current = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if current < 2 {
                    Err(io::Error::other("retry"))
                } else {
                    Ok(42)
                }
            }
        },
        &RetryConfig::new(3, Duration::ZERO),
        retry_test_attributes(),
    )
    .await
    .unwrap();
    assert_eq!(value, 42);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

/// 对应 Go：`periodic_retry_test.go` 中的 `TestRetryOperationStopsForNonRetriableError`。
///
/// 分类器返回 false 后必须立即返回原始错误，不得继续消耗剩余尝试次数。
#[tokio::test]
async fn test_retry_operation_stops_for_non_retriable_error() {
    let attempts = AtomicUsize::new(0);
    let config = RetryConfig::new(3, Duration::ZERO).with_classifier(|_: &io::Error| false);
    let error = retry_operation(
        &CancellationToken::new(),
        |_| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(io::Error::other("fatal")) }
        },
        &config,
        retry_test_attributes(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, RetryError::Operation(_)));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

/// 对应 Go：`periodic_retry_test.go` 中的 `TestRetryOperationReturnsLastErrorAfterExhaustion`。
///
/// 所有尝试都失败时，确认返回最后一次操作错误并精确执行配置的两次调用。
#[tokio::test]
async fn test_retry_operation_returns_last_error_after_exhaustion() {
    let attempts = AtomicUsize::new(0);
    let error = retry_operation(
        &CancellationToken::new(),
        |_| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(io::Error::other("last")) }
        },
        &RetryConfig::new(2, Duration::ZERO),
        retry_test_attributes(),
    )
    .await
    .unwrap_err();
    match error {
        RetryError::Operation(error) => assert_eq!(error.to_string(), "last"),
        other => panic!("unexpected retry result: {other}"),
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

/// 验证取消不会打断与 Go `time.Sleep` 对齐的退避，并会在下一次操作前被观察。
#[tokio::test]
async fn cancellation_is_observed_after_backoff() {
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        cancel.cancel();
    });
    let error = retry_operation(
        &cancellation,
        |_| async { Err::<(), _>(io::Error::other("retry")) },
        &RetryConfig::new(2, Duration::from_millis(10)),
        retry_test_attributes(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, RetryError::Cancelled));
}
