//! 可启动、停止并可重复启动的周期异步任务。
//!
//! 会立即调用一次函数，随后按固定间隔调用。这里保持该时序，
//! 并用 `CancellationToken` 和受控 `JoinHandle` 保证停止后没有遗留任务。

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{BoxError, Stopper, format_duration};

type PeriodicFuture = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'static>>;
type PeriodicFunction = Arc<dyn Fn() -> PeriodicFuture + Send + Sync + 'static>;

/// 周期调用器的状态错误。
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum PeriodicInvokerError {
    /// 间隔必须大于零。
    #[error("interval must be positive, got {}", format_duration(*interval))]
    NonPositiveInterval {
        /// 调用方传入的无效间隔。
        interval: Duration,
    },
    /// 同一个实例不能同时运行两份任务。
    #[error("periodic invoker already running")]
    AlreadyRunning,
    /// `start` 必须在 Tokio runtime 上下文中调用。
    #[error("periodic invoker requires an active Tokio runtime")]
    NoRuntime,
}

#[derive(Default)]
struct InvokerState {
    running: bool,
    cancellation: Option<CancellationToken>,
    tasks: Vec<JoinHandle<()>>,
}

/// 管理单个周期函数生命周期的异步组件。
pub struct PeriodicInvoker {
    interval: Duration,
    function: PeriodicFunction,
    state: Mutex<InvokerState>,
}

impl PeriodicInvoker {
    /// 创建周期调用器。
    ///
    /// 配置错误通过 `Result` 返回，避免配置输入
    /// 导致整个服务崩溃。函数使用 async future，网络调用无需阻塞 Tokio worker。
    pub fn new<F, Fut, E>(interval: Duration, function: F) -> Result<Self, PeriodicInvokerError>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        if interval.is_zero() {
            return Err(PeriodicInvokerError::NonPositiveInterval { interval });
        }
        let function = Arc::new(move || {
            let future = function();
            Box::pin(async move { future.await.map_err(|error| Box::new(error) as BoxError) })
                as PeriodicFuture
        });
        Ok(Self {
            interval,
            function,
            state: Mutex::new(InvokerState::default()),
        })
    }

    /// 启动后台任务，并保证函数在等待第一个间隔前先执行一次。
    ///
    /// 该方法同步返回，便于 session 初始化路径直接调用；
    /// 它只负责 spawn，实际 callback 始终在 Tokio worker 上异步执行。
    pub fn start(&self) -> Result<(), PeriodicInvokerError> {
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| PeriodicInvokerError::NoRuntime)?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.running {
            return Err(PeriodicInvokerError::AlreadyRunning);
        }

        // 已完成任务的 JoinHandle 可以安全丢弃；正常 Stop 会在返回前取走并汇合
        // 未完成任务，因此这里通常只清理异常 drop 后留下的已完成句柄。
        state.tasks.retain(|task| !task.is_finished());

        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let function = Arc::clone(&self.function);
        let interval = self.interval;
        let task = runtime.spawn(Self::run(function, interval, task_cancellation));

        state.running = true;
        state.cancellation = Some(cancellation);
        state.tasks.push(task);
        Ok(())
    }

    /// 同步请求后台周期任务停止，但不等待在途 callback 返回。
    ///
    /// 该入口供无法 `.await` 的 `Drop` guard 使用。取消后仍保留全部 `JoinHandle`，
    /// 因此正常控制流稍后调用 [`Self::stop`] 时仍能等待全部任务完成。方法可重复调用，
    /// 不会重新启动或提前丢弃任务。
    pub fn request_stop(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(cancellation) = &state.cancellation {
            cancellation.cancel();
        }
        state.running = false;
    }

    ///
    /// 先把 `running` 置为 false，再发送取消，并等待所有已启动 callback 完整返回。
    /// 使用 async JoinHandle 等待而不阻塞 Tokio worker；返回后所有 callback 均已结束。
    pub async fn stop(&self) {
        let tasks = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // 取消、切换状态和取走句柄必须保持在同一个临界区；否则另一个线程可能
            // 在 request_stop 与第二次加锁之间重新 start，造成新任务未被取消却被等待。
            if let Some(cancellation) = &state.cancellation {
                cancellation.cancel();
            }
            state.running = false;
            let _ = state.cancellation.take();
            std::mem::take(&mut state.tasks)
        };
        Self::wait_for_tasks(tasks).await;
    }

    /// 显式表达“停止并等待”的别名；语义与 [`Self::stop`] 完全相同。
    pub async fn stop_and_wait(&self) {
        self.stop().await;
    }

    /// 等待一组后台任务，并把 callback panic 恢复成调用方可见的 panic。
    async fn wait_for_tasks(tasks: Vec<JoinHandle<()>>) {
        for task in tasks {
            if let Err(error) = task.await {
                // 后台任务中未恢复的 panic 会终止进程。Tokio 默认把 panic 包进
                // JoinError；这里重新抛出，不能静默降级为一条 Rust-only warning。
                if error.is_panic() {
                    std::panic::resume_unwind(error.into_panic());
                }
            }
        }
    }

    /// 查询当前是否已经启动且尚未停止。
    pub fn is_running(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .running
    }

    ///
    /// 立即调用一次 callback，随后按固定间隔持续执行；单次错误只记录而不终止。
    /// `cancellation` 被触发后退出循环，JoinHandle 由 `stop` 汇合。
    async fn run(function: PeriodicFunction, interval: Duration, cancellation: CancellationToken) {
        if let Err(error) = function().await {
            tracing::error!(error = %error, interval_ms = interval.as_millis(), "periodic function failed");
        }
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // `interval` 的第一次 tick 立即就绪；先消费它，避免和上面的立即调用重复。
        ticker.tick().await;

        loop {
            tokio::select! {
                           _ = cancellation.cancelled() => break,
                           _ = ticker.tick() => {
                               if let Err(error) = function().await {
            // 与首次调用保留独立事件，便于区分两个停止分支。
                                   tracing::error!(error = %error, interval_ms = interval.as_millis(), "periodic function failed");
                               }
                           },
                       }
        }
    }
}

///
/// 自由函数构造入口；间隔配置错误由 [`PeriodicInvoker::new`] 的类型化结果返回。
pub fn new_periodic_invoker<F, Fut, E>(
    interval: Duration,
    function: F,
) -> Result<PeriodicInvoker, PeriodicInvokerError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    PeriodicInvoker::new(interval, function)
}

#[async_trait]
impl Stopper for PeriodicInvoker {
    /// 通过统一 Stopper 接口同步取消，保留任务句柄供后续异步等待。
    fn request_stop(&self) {
        PeriodicInvoker::request_stop(self);
    }

    /// 通过统一 Stopper 接口取消并等待在途 callback 完成。
    async fn stop(&self) {
        PeriodicInvoker::stop(self).await;
    }
}

impl Drop for PeriodicInvoker {
    /// 最后一个 owner 销毁时保证至少发送取消，避免后台任务永久继续运行。
    fn drop(&mut self) {
        self.request_stop();
    }
}
