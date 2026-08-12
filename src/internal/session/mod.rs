//! 会话活动事件记录器。
//!
//! 本模块机械对应 Go 的 `internal/session` 包。网络发送通过异步 `EventPoster`
//! 抽象，周期任务复用 `internal::util::PeriodicInvoker`，并用 cancellation token
//! 替代 `context.Context` 表达取消边界。

pub mod noop_activity_recorder;
pub mod session_activity_recorder;
pub mod types;

pub use noop_activity_recorder::NoopStopper;
pub use session_activity_recorder::new_activity_recorder;
pub use types::{
    ActivityRecorder, ActivityRecorderError, EventPoster, LongRunningActivityState,
    NoopActivityRecorder, RecorderOptions, SessionActivityRecorder,
};

#[cfg(test)]
mod session_activity_recorder_test;
