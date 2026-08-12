//! 诊断服务、文件状态、flusher trait 与显式上下文类型。

use std::{
    fs::File,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::internal::{
    api::{ApiError, DiagLogEntry, SessionBackend},
    logger::Logger,
};

use super::CcLogCollector;

/// 诊断设施的构造、flush、关闭或任务错误。
#[derive(Debug, thiserror::Error)]
pub enum DiagError {
    /// 已经带完整 Go 兼容上下文的普通错误。
    #[error("{0}")]
    Message(String),
    /// 远端 batch flush 失败；保留 `ApiError` 便于识别 HTTP 501。
    #[error("failed to flush diag logs: {source}")]
    Flush {
        /// API 层原始错误。
        #[source]
        source: ApiError,
    },
}

impl DiagError {
    /// 构造稳定文本错误。
    pub(crate) fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }

    /// 判断错误链是否对应后端未实现诊断日志 endpoint。
    pub(crate) fn is_endpoint_not_implemented(&self) -> bool {
        matches!(
            self,
            Self::Flush { source } if source.is_endpoint_not_implemented()
        )
    }
}

/// 诊断日志批量发送的最小依赖面。
#[async_trait]
pub trait LogFlusher: Send + Sync {
    /// 发送一个按时间排序的非空日志批次。
    async fn flush_logs(
        &self,
        cancellation: &CancellationToken,
        logs: &[DiagLogEntry],
    ) -> Result<(), ApiError>;
}

/// 将完整 `SessionBackend` 适配为只暴露 `flush_logs` 的对象。
pub struct SessionBackendLogFlusher {
    backend: Arc<dyn SessionBackend>,
}

impl SessionBackendLogFlusher {
    /// 包装现有 session backend，避免诊断服务依赖 post-event 与 endpoint 方法。
    pub fn new(backend: Arc<dyn SessionBackend>) -> Self {
        Self { backend }
    }
}

#[async_trait]
impl LogFlusher for SessionBackendLogFlusher {
    /// 原样委托到 backend，并保留其取消和错误语义。
    async fn flush_logs(
        &self,
        cancellation: &CancellationToken,
        logs: &[DiagLogEntry],
    ) -> Result<(), ApiError> {
        self.backend.flush_logs(cancellation, logs).await
    }
}

/// 保留路径并允许显式 close 的持久临时文件。
pub(super) struct DiagFile {
    /// `NamedTempFile::keep` 后不会在 drop 时删除的路径。
    pub(super) path: PathBuf,
    /// 尚未关闭时持有的文件描述符。
    pub(super) file: File,
}

/// 由单把 mutex 保护的环境日志、文件和关闭状态。
pub(super) struct DiagState {
    /// 尚未交给周期 flusher 的环境管理器日志。
    pub(super) env_manager_logs: Vec<DiagLogEntry>,
    /// 环境管理器 JSONL 本地文件。
    pub(super) env_manager_log_file: Option<DiagFile>,
    /// Claude Code 写入、collector 读取的 JSONL 文件。
    pub(super) cc_log_file: Option<DiagFile>,
    /// 第一次 shutdown 开始后置 true，禁止新日志和重复关闭。
    pub(super) is_shutdown: bool,
}

/// 诊断日志服务的共享运行状态。
pub struct DiagService {
    /// 对应 Go 包级 `slog.Default()`；构造时固定下来，供后台任务和同步记录路径共享。
    pub(super) logger: Logger,
    pub(super) state: Mutex<DiagState>,
    pub(super) cc_collector: Option<Arc<CcLogCollector>>,
    pub(super) flusher: Arc<dyn LogFlusher>,
    pub(super) flusher_stop: CancellationToken,
    pub(super) flusher_task: Mutex<Option<JoinHandle<Result<(), DiagError>>>>,
    pub(super) parent_cancellation: CancellationToken,
}

/// 显式替代 Go `context.WithValue(diagLogsContextKey, service)`。
#[derive(Clone)]
pub struct DiagContext {
    pub(super) cancellation: CancellationToken,
    pub(super) service: Arc<DiagService>,
}

impl DiagContext {
    /// 返回创建服务时继承的 session 取消 token。
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// 返回上下文中的诊断服务，便于统一生命周期管理。
    pub fn service(&self) -> Arc<DiagService> {
        Arc::clone(&self.service)
    }
}
