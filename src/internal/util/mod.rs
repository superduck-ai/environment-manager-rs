//! 通用基础设施。
//!
//! 资源清理由 RAII 管理，周期工作使用 Tokio 任务，取消统一通过
//! [`tokio_util::sync::CancellationToken`] 传播。

mod duration;
mod git;
mod lockfile;
mod path_escape;
mod periodic_invoker;
mod retry;
mod stopper;
mod stream;
mod tailer;
mod types;
mod version;

pub(crate) use duration::{duration_nanoseconds, format_go_duration};
pub use git::{GitRefError, update_remote_tracking_branch};
pub use lockfile::{FileLockGuard, LockError, acquire_container_lock, acquire_lock};
pub(crate) use path_escape::go_path_escape;
pub use periodic_invoker::{PeriodicInvokerError, new_periodic_invoker};
pub use retry::{RetryError, retry_operation};
pub use stream::{BoxError, output_streamer};
pub use tailer::{TailerError, new_tailer};
pub use types::{
    ErrorClassifier, Line, OutputStreamer, PeriodicInvoker, RetryConfig, Stopper, StreamType,
    TailLine, Tailer,
};
pub use version::{
    VersionInitError, claude_code_version, initialize_versions, set_claude_code_version, version,
};

#[cfg(test)]
mod duration_test;
#[cfg(test)]
mod periodic_retry_test;
