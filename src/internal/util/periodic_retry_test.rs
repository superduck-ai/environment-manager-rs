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

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::{PeriodicInvoker, PeriodicInvokerError, RetryConfig, RetryError, retry_operation};

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
/// Rust 版本将唯一可由运行时输入触发的零间隔错误改为
/// `Result`；这里验证错误文本仍与 Go panic 文本逐字一致。
#[test]
fn test_periodic_invoker_validation_panics() {
    let error = match PeriodicInvoker::new(Duration::ZERO, || async { Ok::<_, io::Error>(()) }) {
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
    let invoker = PeriodicInvoker::new(Duration::from_millis(1), move || {
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
    invoker.stop().await;
    invoker.stop().await;
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
    let invoker = PeriodicInvoker::new(Duration::from_millis(1), move || {
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

/// 验证统一 Stopper 先同步取消后，完整 Stop 仍会等待已进入的 callback。
///
/// 两个 Notify 把时序固定为“callback 已进入 → request_stop 已取消 → async Stop 等待
/// → callback 获准返回”；在放行前 stop task 必须保持 Pending，证明同步阶段未丢句柄。
#[tokio::test]
async fn periodic_invoker_stop_waits_for_in_flight_callback() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let completed = Arc::new(AtomicUsize::new(0));
    let callback_entered = Arc::clone(&entered);
    let callback_release = Arc::clone(&release);
    let callback_completed = Arc::clone(&completed);
    let invoker = Arc::new(
        PeriodicInvoker::new(Duration::from_secs(60), move || {
            let entered = Arc::clone(&callback_entered);
            let release = Arc::clone(&callback_release);
            let completed = Arc::clone(&callback_completed);
            async move {
                entered.notify_one();
                release.notified().await;
                completed.fetch_add(1, Ordering::SeqCst);
                Ok::<_, io::Error>(())
            }
        })
        .unwrap(),
    );
    invoker.start().unwrap();
    entered.notified().await;

    let stopper: Arc<dyn super::Stopper> = invoker.clone();
    // 模拟 RAII Drop：先同步取消，再由正常清理路径执行完整异步 Stop。同步阶段
    // 不能丢掉 JoinHandle，否则下面的 Stop 会在 callback 尚未完成时提前返回。
    stopper.request_stop();
    let stop_task = tokio::spawn(async move {
        stopper.stop().await;
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while invoker.is_running() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Stop 应先把 running 置为 false");

    assert_eq!(completed.load(Ordering::SeqCst), 0);
    assert!(
        !stop_task.is_finished(),
        "callback 未返回时 Stop 不得提前完成"
    );
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), stop_task)
        .await
        .expect("放行 callback 后 Stop 应完成")
        .expect("Stop task 不应 panic");
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}

/// 对应 Go：`periodic_retry_test.go` 中的 `TestRetryOperationSucceedsAfterRetry`。
///
/// 操作前两次返回可重试错误，第三次返回成功值，验证最大次数包含首次调用。
#[tokio::test]
async fn test_retry_operation_succeeds_after_retry() {
    let attempts = AtomicUsize::new(0);
    let value = retry_operation(
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
        |_| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(io::Error::other("fatal")) }
        },
        &config,
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
        |_| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(io::Error::other("last")) }
        },
        &RetryConfig::new(2, Duration::ZERO),
    )
    .await
    .unwrap_err();
    match error {
        RetryError::Operation(error) => assert_eq!(error.to_string(), "last"),
        other => panic!("unexpected retry result: {other}"),
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

/// 验证取消不会打断 Go `time.Sleep` 对齐的退避，也不会让通用重试器跳过下一次操作。
///
/// 测试 operation 模拟 Go 中带 context 的 HTTP 请求：token 取消后仍会进入闭包，再
/// 返回普通取消错误。分类器必须看到该错误并结束重试，而不是由 retry 提前短路。
#[tokio::test]
async fn cancellation_during_backoff_still_invokes_operation_and_classifier() {
    let cancellation = CancellationToken::new();
    let attempts = Arc::new(AtomicUsize::new(0));
    let classifications = Arc::new(AtomicUsize::new(0));
    let observed_attempts = Arc::clone(&attempts);
    let operation_cancellation = cancellation.clone();
    let observed_classifications = Arc::clone(&classifications);
    let config =
        RetryConfig::new(3, Duration::from_millis(10)).with_classifier(move |error: &io::Error| {
            observed_classifications.fetch_add(1, Ordering::SeqCst);
            error.to_string() != "cancelled"
        });

    let error = retry_operation(
        move |attempt| {
            let cancellation = operation_cancellation.clone();
            let attempts = Arc::clone(&observed_attempts);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                if attempt == 1 {
                    cancellation.cancel();
                    Err::<(), _>(io::Error::other("retry"))
                } else if cancellation.is_cancelled() {
                    Err(io::Error::other("cancelled"))
                } else {
                    panic!("第一次调用已经取消 token")
                }
            }
        },
        &config,
    )
    .await
    .unwrap_err();
    match error {
        RetryError::Operation(error) => assert_eq!(error.to_string(), "cancelled"),
        other => panic!("unexpected retry result: {other}"),
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(classifications.load(Ordering::SeqCst), 2);
}
