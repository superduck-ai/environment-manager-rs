//! 可同步触发停止的最小生命周期接口。
//!
//! 该 trait 对应 Go 的 `Stopper`，只负责发出停止信号，不承诺已经等待后台任务退出。
//! 需要确定性回收时，具体类型应另外提供 async `stop_and_wait` 方法。

/// 能够从同步清理路径请求停止的组件。
pub trait Stopper: Send + Sync {
    /// 幂等地发出停止信号；实现不得阻塞等待异步任务。
    fn stop(&self);
}
