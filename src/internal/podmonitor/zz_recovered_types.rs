//! podmonitor 的共享状态、wire DTO 与可判定错误类型。
//!
//! 文件名对应 Go `zz_recovered_types.go`。与 Go 把所有字段直接放进一个结构体不同，
//! Rust 用 `Arc<LeaseManagerInner>` 共享只读配置，用 `RwLock<LeaseState>` 保护短时状态，
//! 并让网络请求和定时等待永远不持有锁。

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::internal::api::HttpClient;
use crate::internal::logger::Logger;

/// 服务端尚未返回过 heartbeat 标识时发送的 CAS sentinel。
pub(super) const NO_SERVER_HEARTBEAT: &str = "NO_HEARTBEAT";

/// Claude Code 活动时间不再变化时的默认判定阈值。
pub(super) const DEFAULT_CLAUDE_CODE_STUCK_THRESHOLD: Duration = Duration::from_secs(10 * 60);

/// 没有可用租约过期时间时采用的最短退出宽限期。
pub(super) const DEFAULT_SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(2);

/// 在剩余租约时间后附加的进程退出缓冲。
pub(super) const SHUTDOWN_GRACE_PERIOD_BUFFER: Duration = Duration::from_secs(5);

/// `/heartbeat` 成功响应的 wire 结构。
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub(super) struct HeartbeatResponse {
    /// 服务端是否实际延长租约。
    #[serde(default)]
    pub(super) lease_extended: bool,
    /// 环境实例状态。
    #[serde(default)]
    pub(super) state: String,
    /// 下一次心跳要作为 CAS 前置条件回传的服务端标识。
    #[serde(default)]
    pub(super) last_heartbeat: String,
    /// 新租约 TTL，单位秒。
    #[serde(default)]
    pub(super) ttl_seconds: i64,
    /// 服务端最后观察到 Claude Code 活动并更新租约的时间。
    #[serde(default = "go_zero_time")]
    pub(super) lease_updated_at: DateTime<Utc>,
}

/// 返回 Go `time.Time{}` 对应的公元一年 UTC 零值。
fn go_zero_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(1, 1, 1, 0, 0, 0)
        .single()
        .expect("公元一年 UTC 是固定有效时间")
}

/// `/stop` 请求主体。
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(super) struct StopRequest {
    /// 是否要求后端强制终止。
    pub(super) force: bool,
}

/// 对应 Go `stopAPIError`，保留 HTTP 状态码供重试分类。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StopApiError {
    /// 非 2xx HTTP 状态码。
    pub status_code: u16,
    /// 与 Go `Error()` 完全相同的稳定文本。
    pub message: String,
}

impl StopApiError {
    /// 创建带状态码的 stop API 错误 sentinel。
    #[must_use]
    pub fn new(status_code: u16, message: impl Into<String>) -> Self {
        Self {
            status_code,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for StopApiError {
    /// 输出服务端 stop 错误正文，对应 Go `stopAPIError.Error`。
    ///
    /// 状态码只用于重试分类，不拼入展示文本，保持被包装后的错误链与 Go 一致。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StopApiError {}

/// 单次 heartbeat 失败的具体原因。
#[derive(Debug, Error)]
pub enum HeartbeatError {
    /// base URL 或 path segment 无法构造请求 URL。
    #[error("failed to create heartbeat request: {0}")]
    InvalidRequest(String),
    /// 请求传输失败；文本仍参与 Go 兼容的 marker 分类。
    #[error("heartbeat request failed: {0}")]
    Request(String),
    /// manager token 已取消。
    #[error("heartbeat request failed: operation cancelled")]
    Cancelled,
    /// 服务端返回非 200；状态码允许无字符串猜测地判定永久错误。
    #[error("heartbeat failed: HTTP {status_code}: {body}")]
    Http {
        /// HTTP 状态码。
        status_code: u16,
        /// 去除首尾空白后的响应 body。
        body: String,
    },
    /// HTTP 200 body 不是合法 heartbeat JSON。
    #[error("failed to decode heartbeat response: {0}")]
    Decode(String),
}

impl HeartbeatError {
    /// 返回 HTTP 状态码；非 HTTP 错误返回 `None`。
    #[must_use]
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::Http { status_code, .. } => Some(*status_code),
            _ => None,
        }
    }
}

/// stop 请求及其重试过程的稳定错误集合。
#[derive(Debug, Error)]
pub enum StopRequestError {
    /// `CancellationToken` 已被触发，对应 Go `context.Canceled` sentinel。
    #[error("operation was cancelled")]
    Cancelled,
    /// 供 deadline 驱动的调用方保留 Go `context.DeadlineExceeded` sentinel 语义。
    #[error("operation deadline exceeded")]
    DeadlineExceeded,
    /// URL、序列化或网络传输错误；网络错误默认可重试。
    #[error("{0}")]
    Request(String),
    /// 非 2xx stop 响应，状态码决定是否重试。
    #[error(transparent)]
    Api(#[from] StopApiError),
}

/// LeaseManager 启动阶段错误。
#[derive(Debug, Error)]
pub enum LeaseManagerError {
    /// 初始 heartbeat 失败，因此尚未创建后台任务。
    #[error("failed initial heartbeat: {0}")]
    InitialHeartbeat(#[source] HeartbeatError),
    /// 同一 manager 只能从 `Created` 状态启动一次。
    #[error("lease manager has already been started")]
    AlreadyStarted,
    /// 启动过程中收到并发停止请求，因而不会再创建后台任务。
    #[error("lease manager was stopped while starting")]
    StoppedWhileStarting,
}

/// 写健康文件时的路径化错误。
#[derive(Debug, Error)]
pub enum HealthFileError {
    /// 创建父目录失败。
    #[error("failed to create health file directory {path}: {source}")]
    CreateDirectory {
        /// 失败的父目录。
        path: PathBuf,
        /// 底层文件系统错误。
        #[source]
        source: std::io::Error,
    },
    /// 写入时间戳失败。
    #[error("failed to write health file {path}: {source}")]
    Write {
        /// 目标健康文件。
        path: PathBuf,
        /// 底层文件系统错误。
        #[source]
        source: std::io::Error,
    },
}

/// 对外暴露的租约状态快照。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LeaseInfo {
    /// 最近一次成功延长租约的本地时间。
    pub last_heartbeat: Option<DateTime<Utc>>,
    /// 由服务端更新时间加 TTL 得到的绝对过期时间。
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// 最近一次服务端实例状态。
    pub instance_state: String,
}

/// manager 内部生命周期，用于阻止停止后又创建任务的竞态。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum Lifecycle {
    /// 构造完成但尚未发送初始 heartbeat。
    #[default]
    Created,
    /// 初始 heartbeat 正在进行。
    Starting,
    /// 三个后台任务已经交给 tracker。
    Running,
    /// 已发出取消，不允许再创建后台任务。
    Stopping,
    /// tracker 已关闭且任务已经归零。
    Stopped,
}

/// 所有会随 heartbeat 或退出推进而变化的短时状态。
#[derive(Debug, Default)]
pub(super) struct LeaseState {
    /// 最近成功 heartbeat 的本地墙钟时间。
    pub(super) last_heartbeat: Option<DateTime<Utc>>,
    /// 当前绝对租约过期时间。
    pub(super) lease_expires_at: Option<DateTime<Utc>>,
    /// 最近 TTL；主要用于日志和对照 Go 状态。
    pub(super) lease_ttl: Duration,
    /// 最近服务端状态。
    pub(super) instance_state: String,
    /// 最近一次发现 `lease_updated_at` 变化的本地墙钟时间。
    ///
    /// Go 的同名字段既参与 `time.Since`，又会原样写入结构化日志；Rust 将日志值
    /// 单独保留为 UTC 墙钟，从而确保 `last_updated_at` 仍是绝对时间而不是 elapsed。
    pub(super) last_updated_at: Option<DateTime<Utc>>,
    /// 与 `last_updated_at` 同时更新的单调时钟，用于抵抗系统时间跳变。
    pub(super) last_updated_instant: Option<Instant>,
    /// 最近一个服务端 Claude Code 活动时间戳。
    pub(super) last_claude_code_activity: Option<DateTime<Utc>>,
    /// fatal 退出时计算出的宽限期。
    pub(super) shutdown_grace_period: Duration,
    /// heartbeat CAS 的服务端前值。
    pub(super) server_last_heartbeat: Option<String>,
    /// 保证 fatal 路径只计算一次宽限期并只执行一次取消。
    pub(super) shutdown_started: bool,
    /// manager 启停状态。
    pub(super) lifecycle: Lifecycle,
}

/// 不随 heartbeat 改变的配置与异步任务所有权。
pub(super) struct LeaseManagerInner {
    /// work session ID。
    pub(super) session_id: String,
    /// heartbeat 周期。
    pub(super) interval: Duration,
    /// 租约过期前提前停止的安全窗口。
    pub(super) safety_window: Duration,
    /// 环境 ID。
    pub(super) environment_id: String,
    /// 组织 UUID query 参数。
    pub(super) organization_uuid: String,
    /// 复用现有 API 连接池和规范化 base URL。
    pub(super) api_client: HttpClient,
    /// 构造阶段已经完成 nil 回退的兼容 logger。
    pub(super) logger: Logger,
    /// 成功 heartbeat 后更新的健康文件路径；空路径禁用。
    pub(super) health_file_path: PathBuf,
    /// Claude Code 卡死阈值。
    pub(super) claude_code_stuck_threshold: Duration,
    /// 短临界区共享状态。
    pub(super) state: RwLock<LeaseState>,
    /// 从调用方 token 派生、由 manager 独占取消权的子 token。
    pub(super) cancellation: CancellationToken,
    /// 统一追踪 heartbeat、expiry 和 stuck 三个任务。
    pub(super) tasks: TaskTracker,
    /// 对应 Go `stopChan` 的关闭信号；只有 heartbeat loop 真正退出才触发。
    pub(super) heartbeat_loop_exited: CancellationToken,
    /// heartbeat 更新租约后唤醒 expiry 任务重新计算 deadline。
    pub(super) expiry_changed: Notify,
    /// heartbeat 更新活动后唤醒 stuck 任务重新计算 deadline。
    pub(super) activity_changed: Notify,
}

/// 可克隆的 LeaseManager 句柄。
///
/// 克隆只增加 `Arc` 引用计数；共享 token 和 tracker 的唯一逻辑所有者仍是 inner。
/// tracker 仅计数任务而不持有任务 future，因此任务持有 manager clone 不会形成引用环。
#[derive(Clone)]
pub struct LeaseManager {
    /// 所有配置、状态与任务追踪器的共享所有权。
    pub(super) inner: Arc<LeaseManagerInner>,
}

impl std::fmt::Debug for LeaseManager {
    /// 只打印非敏感 ID、取消和任务状态，不展开 HTTP 连接池或 logger。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LeaseManager")
            .field("session_id", &self.inner.session_id)
            .field("environment_id", &self.inner.environment_id)
            .field("is_cancelled", &self.inner.cancellation.is_cancelled())
            .field("tracked_tasks", &self.inner.tasks.len())
            .finish_non_exhaustive()
    }
}
