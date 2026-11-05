//! 环境工作租约、心跳和停止请求的生命周期管理。
//!
//! 心跳和租约过期检测由可重置定时器与取消令牌管理，并收敛到一个
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
