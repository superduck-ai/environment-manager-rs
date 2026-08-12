//! 环境工作租约、心跳和停止请求的生命周期管理。
//!
//! 本模块机械对应 Go `internal/podmonitor`。Go 版分别用 ticker、两个可重置
//! `time.Timer` 与 `context.Context` 管理心跳、租约过期和 Claude Code 卡死检测；
//! Rust 版保留相同协议与判定规则，但把它们收敛到一个
//! [`tokio_util::task::TaskTracker`] 和一个子
//! [`tokio_util::sync::CancellationToken`] 下，以便确定性等待所有后台任务退出。

mod lease_manager;
mod types;

pub use lease_manager::{
    get_default_health_file_path, is_permanent_heartbeat_error, is_precondition_failed_error,
    is_retriable_stop_error, new_lease_manager,
};
pub use types::{
    HealthFileError, HeartbeatError, LeaseInfo, LeaseManager, LeaseManagerError, StopApiError,
    StopRequestError,
};

#[cfg(test)]
#[path = "lease_manager_test.rs"]
mod lease_manager_test;
