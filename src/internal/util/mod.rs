//! 通用基础设施。
//!
//! 本模块对应 Go 项目的 `internal/util`。迁移时保留了外部可观察行为，
//! 同时把手工清理闭包、goroutine 和 `context.Context` 分别替换为 RAII、
//! Tokio 任务与 [`tokio_util::sync::CancellationToken`]。

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

pub(crate) use duration::{format_go_duration, go_duration_nanoseconds};
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
