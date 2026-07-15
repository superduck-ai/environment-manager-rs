//! 可分阶段停止并等待后台工作退出的最小生命周期接口。
//!
//! 该 trait 对应 Go 的 `Stopper`。Go 的 `Stop()` 会在取消后同步等待 goroutine 退出；
//! Rust 不能在 Tokio worker 上安全地同步阻塞，因此把“发出取消”和“等待退出”拆成
//! 同步、异步两个阶段。RAII guard 被丢弃时至少执行前者，正常控制流仍执行完整等待。

use async_trait::async_trait;

/// 能够请求停止并确定性等待后台工作退出的组件。
#[async_trait]
pub trait Stopper: Send + Sync {
    /// 同步、幂等地发出停止请求，但不等待后台工作退出。
    ///
    /// 该方法专供 `Drop` 等不能 `.await` 的清理边界使用。实现必须确保返回后取消
    /// 信号已经可见，且后续调用 [`Self::stop`] 仍能汇合原有后台任务；不得在这里
    /// 丢弃等待所需的 `JoinHandle` 或其他完成状态。
    fn request_stop(&self);

    /// 幂等地发送取消，并在返回前等待已启动的后台工作完全退出。
    ///
    /// 实现应先完成与 [`Self::request_stop`] 相同的同步取消，再异步等待全部在途工作。
    /// 即使某个 RAII guard 已提前调用 `request_stop`，再次调用也必须安全且仍要等待。
    async fn stop(&self);
}
