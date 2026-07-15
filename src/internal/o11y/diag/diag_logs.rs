//! 诊断服务构造、本地写入、周期 flush、缓冲上限与关闭流程。

use std::{
    collections::BTreeMap,
    io::Write,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use serde_json::Value;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::internal::api::{DiagLogEntry, SessionBackend};
use crate::internal::logger::{LogAttributes, Logger, default_slog_logger};

use super::{
    DiagContext, DiagError, DiagFile, DiagService, DiagState, LogFlusher, SessionBackendLogFlusher,
    new_cc_log_collector,
};

pub(super) const MAX_BUFFERED_LOGS: usize = 5_000;
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// 创建两个持久临时文件、启动 Claude Code tailer 与周期 flusher。
///
/// 文件模式由 `tempfile` 保持为 `0600`。创建后的文件通过 `keep()` 脱离 RAII 自动删除，
/// 对应 Go `os.CreateTemp`：shutdown 只关闭 fd，不删除路径，调用方仍可读取诊断文件。
pub async fn new_diag_service(
    cancellation: CancellationToken,
    flusher: Arc<dyn LogFlusher>,
) -> Result<(Arc<DiagService>, DiagContext), DiagError> {
    new_diag_service_with_logger(cancellation, flusher, default_slog_logger().clone()).await
}

/// 对应 Go：`diag.NewDiagService`，并允许 Rust 测试注入可观测 logger。
///
/// 生产入口总是传入 `slog.Default()` 兼容 logger；单独保留此内部入口，是为了验证每个
/// Go 日志调用点的消息、级别和属性顺序，而不修改进程级 stderr。两份临时文件创建、
/// collector 启动和 flusher task 启动仍严格按 Go 顺序执行；任一步失败时，已经创建的
/// `File` 会被 drop/close，但 `keep()` 后的路径与 Go `os.CreateTemp` 一样不会删除。
///
/// # 参数
/// - `cancellation`：父会话取消树，供 tailer 与远端 flush 使用。
/// - `flusher`：把按时间排序的非空 batch 发送到 session backend 的实现。
/// - `logger`：已经解析完成、可跨异步任务克隆的 concrete logger。
///
/// # 返回
/// 成功时返回共享服务和显式诊断上下文；文件或 tailer 初始化失败时返回带 Go 前缀错误。
///
/// # 副作用
/// 创建两个 `0600` 临时文件，并启动一个 collector task 和一个周期 flush task。
pub(super) async fn new_diag_service_with_logger(
    cancellation: CancellationToken,
    flusher: Arc<dyn LogFlusher>,
    logger: Logger,
) -> Result<(Arc<DiagService>, DiagContext), DiagError> {
    let env_manager_log_file =
        create_diag_file("environment-manager", &logger).map_err(|error| {
            DiagError::message(format!(
                "failed to create diag log for environment-manager: {error}"
            ))
        })?;
    let cc_log_file = create_diag_file("claude-code", &logger).map_err(|error| {
        DiagError::message(format!(
            "failed to create diag log for claude-code: {error}"
        ))
    })?;
    let cc_collector =
        new_cc_log_collector(cancellation.clone(), &cc_log_file.path, logger.clone()).await?;

    let service = Arc::new(DiagService {
        logger,
        state: Mutex::new(DiagState {
            env_manager_logs: Vec::new(),
            env_manager_log_file: Some(env_manager_log_file),
            cc_log_file: Some(cc_log_file),
            is_shutdown: false,
        }),
        cc_collector: Some(cc_collector),
        flusher,
        flusher_stop: CancellationToken::new(),
        flusher_task: Mutex::new(None),
        parent_cancellation: cancellation.clone(),
    });

    let worker = Arc::clone(&service);
    let task = tokio::spawn(async move { worker.flush_periodically().await });
    *service
        .flusher_task
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(task);

    let context = DiagContext {
        cancellation,
        service: Arc::clone(&service),
    };
    Ok((service, context))
}

/// 直接从完整 session backend 创建诊断服务。
pub async fn new_diag_service_with_backend(
    cancellation: CancellationToken,
    backend: Arc<dyn SessionBackend>,
) -> Result<(Arc<DiagService>, DiagContext), DiagError> {
    let flusher: Arc<dyn LogFlusher> = Arc::new(SessionBackendLogFlusher::new(backend));
    new_diag_service(cancellation, flusher).await
}

/// 对应 Go：`createDiagFile`，在系统临时目录创建 `{prefix}-*.diag.log` 持久文件。
///
/// 成功后先以 `file` 为唯一属性写 INFO，再返回仍打开的文件与路径；创建或 `keep`
/// 失败时保留 `error creating file {prefix}` 前缀且不写成功日志。文件权限由 tempfile
/// 维持为 `0600`，路径脱离 RAII 删除，后续 shutdown 只关闭 fd。
pub(super) fn create_diag_file(prefix: &str, logger: &Logger) -> Result<DiagFile, DiagError> {
    let named = tempfile::Builder::new()
        .prefix(&format!("{prefix}-"))
        .suffix(".diag.log")
        .tempfile()
        .map_err(|error| DiagError::message(format!("error creating file {prefix}: {error}")))?;
    let (file, path) = named
        .keep()
        .map_err(|error| DiagError::message(format!("error creating file {prefix}: {error}")))?;
    let mut attributes = LogAttributes::new();
    attributes.insert(
        "file".to_owned(),
        serde_json::json!(path.to_string_lossy().into_owned()),
    );
    logger.info("Created diagnostic file", attributes);
    Ok(DiagFile { path, file })
}

/// 返回 Claude Code 应写入的诊断文件路径；关闭后返回空字符串。
pub fn get_claude_code_diag_file_path(context: Option<&DiagContext>) -> String {
    let Some(context) = context else {
        return String::new();
    };
    let state = context
        .service
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if state.is_shutdown {
        return String::new();
    }
    state
        .cc_log_file
        .as_ref()
        .map_or_else(String::new, |file| file.path.to_string_lossy().into_owned())
}

impl DiagService {
    /// 停止 collector，触发最终 flush，受调用方 token 约束等待，并显式关闭两份文件。
    ///
    /// 第一次调用会先将 `is_shutdown` 置位；后续调用立即成功。即使最终 flush 超时或
    /// 失败，本地文件仍会被关闭，多个错误按 Go `errors.Join` 的换行形式汇总。
    pub async fn shutdown(&self, cancellation: &CancellationToken) -> Result<(), DiagError> {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.is_shutdown {
                return Ok(());
            }
            state.is_shutdown = true;
        }

        if let Some(collector) = &self.cc_collector {
            collector.stop().await;
        }
        self.flusher_stop.cancel();

        let mut errors = Vec::new();
        let task = self
            .flusher_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(mut task) = task {
            tokio::select! {
                result = &mut task => match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => errors.push(error),
                    Err(error) if error.is_panic() => {
                        // Go flusher goroutine 的 panic 会终止进程；Tokio 只把它保存在
                        // JoinError 中，因此这里恢复 unwind，不能返回 Rust 独有错误。
                        std::panic::resume_unwind(error.into_panic());
                    }
                    Err(error) => errors.push(DiagError::message(format!(
                        "diag log flusher task failed: {error}"
                    ))),
                },
                _ = cancellation.cancelled() => {
                    self.logger.warn(
                        "Shutdown timed out waiting for diag log flusher to stop",
                        LogAttributes::new(),
                    );
                    errors.push(DiagError::message(
                        "shutdown timed out waiting for final flush: context canceled"
                    ));
                }
            }
        }

        let (env_manager_file, cc_file) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (state.env_manager_log_file.take(), state.cc_log_file.take())
        };
        if let Some(file) = env_manager_file
            && let Err(error) = close_diag_file(file)
        {
            errors.push(DiagError::message(format!(
                "failed to close environment manager diag log file: {error}"
            )));
        }
        if let Some(file) = cc_file
            && let Err(error) = close_diag_file(file)
        {
            errors.push(DiagError::message(format!(
                "failed to close claude-code diag log file: {error}"
            )));
        }

        join_errors(errors)
    }

    /// 把无 PII 环境事件同时追加到内存队列与本地 JSONL 文件。
    fn log_env_manager_no_pii(&self, event: &str, data_map: Option<BTreeMap<String, Value>>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.is_shutdown {
            self.logger.warn(
                "DiagService is shutdown; discarding log entry",
                LogAttributes::new(),
            );
            return;
        }

        let timestamp = Utc::now();
        let data = data_map.map_or(Value::Null, |data| {
            Value::Object(data.into_iter().collect())
        });
        let fields = [
            ("data".to_owned(), data.clone()),
            ("event".to_owned(), Value::String(event.to_owned())),
            ("source".to_owned(), Value::String("env_manager".to_owned())),
        ]
        .into_iter()
        .collect();
        let existing = std::mem::take(&mut state.env_manager_logs);
        state.env_manager_logs = append_and_cap_logs(
            &self.logger,
            existing,
            vec![DiagLogEntry { timestamp, fields }],
        );

        let file_entry: BTreeMap<String, Value> = [
            ("data".to_owned(), data),
            ("event".to_owned(), Value::String(event.to_owned())),
            ("source".to_owned(), Value::String("env_manager".to_owned())),
            (
                "timestamp".to_owned(),
                serde_json::to_value(timestamp).expect("chrono timestamp is always serializable"),
            ),
        ]
        .into_iter()
        .collect();
        let mut line = match serde_json::to_vec(&file_entry) {
            Ok(line) => line,
            Err(error) => {
                let mut attributes = LogAttributes::new();
                attributes.insert("error".to_owned(), serde_json::json!(error.to_string()));
                self.logger
                    .error("Failed to marshal diag log entry", attributes);
                return;
            }
        };
        line.push(b'\n');

        if let Some(file) = state.env_manager_log_file.as_mut()
            && let Err(error) = file.file.write_all(&line)
        {
            let mut attributes = LogAttributes::new();
            attributes.insert("error".to_owned(), serde_json::json!(error.to_string()));
            self.logger
                .error("Failed to write diag log entry", attributes);
        }
    }

    /// 每 5 秒 flush 一次，并在 stop token 触发时执行最终 flush。
    async fn flush_periodically(&self) -> Result<(), DiagError> {
        let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // Tokio interval 的首个 tick 立即完成；先消费它以匹配 Go ticker 首次等待 5 秒。
        ticker.tick().await;
        let mut pending = Vec::new();

        loop {
            tokio::select! {
                _ = self.flusher_stop.cancelled() => {
                    pending = self.collect_and_merge_logs(pending);
                    return match self.flush_diag_logs_to_remote(&pending).await {
                        Ok(()) => Ok(()),
                        Err(error) if error.is_endpoint_not_implemented() => {
                            let mut attributes = LogAttributes::new();
                            attributes.insert("count".to_owned(), serde_json::json!(pending.len()));
                            self.logger.warn(
                                "Final diag log flush skipped; endpoint not implemented",
                                attributes,
                            );
                            Ok(())
                        }
                        Err(error) => {
                            let mut attributes = LogAttributes::new();
                            attributes
                                .insert("error".to_owned(), serde_json::json!(error.to_string()));
                            self.logger.error(
                                "Final diag log flush failed during shutdown",
                                attributes,
                            );
                            Err(error)
                        }
                    };
                }
                _ = ticker.tick() => {
                    pending = self.collect_and_merge_logs(pending);
                    match self.flush_diag_logs_to_remote(&pending).await {
                        Ok(()) => pending.clear(),
                        Err(error) if error.is_endpoint_not_implemented() => {
                            let mut attributes = LogAttributes::new();
                            attributes.insert("count".to_owned(), serde_json::json!(pending.len()));
                            self.logger.warn(
                                "Diag logs endpoint not implemented; discarding logs",
                                attributes,
                            );
                            pending.clear();
                        }
                        Err(error) => {
                            let mut attributes = LogAttributes::new();
                            attributes
                                .insert("error".to_owned(), serde_json::json!(error.to_string()));
                            self.logger.error("Diag log flush failed", attributes);
                        }
                    }
                }
            }
        }
    }

    /// 合并上次失败批次、环境日志与 Claude Code 日志，再稳定按时间升序排序。
    pub(super) fn collect_and_merge_logs(
        &self,
        mut pending: Vec<DiagLogEntry>,
    ) -> Vec<DiagLogEntry> {
        pending = append_and_cap_logs(&self.logger, pending, self.drain_env_manager_logs());
        if let Some(collector) = &self.cc_collector {
            pending = append_and_cap_logs(&self.logger, pending, collector.drain());
        }
        // `sort_by_key` 仍是稳定排序；相同时间戳的条目继续保留合并前的来源顺序。
        pending.sort_by_key(|entry| entry.timestamp);
        pending
    }

    /// 原子取走全部环境管理器内存日志。
    pub(super) fn drain_env_manager_logs(&self) -> Vec<DiagLogEntry> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::mem::take(&mut state.env_manager_logs)
    }

    /// 非空时调用远端 flusher，并增加稳定错误前缀。
    async fn flush_diag_logs_to_remote(&self, pending: &[DiagLogEntry]) -> Result<(), DiagError> {
        if pending.is_empty() {
            return Ok(());
        }
        self.flusher
            .flush_logs(&self.parent_cancellation, pending)
            .await
            .map_err(|source| DiagError::Flush { source })
    }
}

/// 通过显式上下文记录环境管理器无 PII 事件；缺少上下文时 no-op。
pub fn log_env_manager_no_pii(
    context: Option<&DiagContext>,
    event: &str,
    data_map: Option<BTreeMap<String, Value>>,
) {
    if let Some(context) = context {
        context.service.log_env_manager_no_pii(event, data_map);
    }
}

/// 对应 Go：`appendAndCapLogs`，追加日志并把总数限制为 5000。
///
/// 未超限时原样保留插入顺序；超限时先按 `discarded_count`、`max_entries` 顺序写一条
/// WARN，再删除最早插入项。该函数不按时间排序，排序只发生在远端 flush 前，避免
/// 改变 Go 在多个来源分别触发 cap 时的丢弃对象。
pub(super) fn append_and_cap_logs(
    logger: &Logger,
    mut existing: Vec<DiagLogEntry>,
    new_logs: Vec<DiagLogEntry>,
) -> Vec<DiagLogEntry> {
    existing.extend(new_logs);
    if existing.len() <= MAX_BUFFERED_LOGS {
        return existing;
    }

    let discarded_count = existing.len() - MAX_BUFFERED_LOGS;
    let mut attributes = LogAttributes::new();
    attributes.insert(
        "discarded_count".to_owned(),
        serde_json::json!(discarded_count),
    );
    attributes.insert(
        "max_entries".to_owned(),
        serde_json::json!(MAX_BUFFERED_LOGS),
    );
    logger.warn(
        "Dropping oldest diag log entries due to buffer cap",
        attributes,
    );
    drop(existing.drain(..discarded_count));
    existing
}

/// 解析 `ENV_MANAGER_ENABLE_DIAG_LOGS`，保持 Go `strconv.ParseBool` 的接受集合。
pub fn logs_enabled() -> bool {
    let Ok(value) = std::env::var("ENV_MANAGER_ENABLE_DIAG_LOGS") else {
        return false;
    };
    matches!(value.trim(), "1" | "t" | "T" | "TRUE" | "true" | "True")
}

/// 显式关闭诊断文件并返回 OS close 错误。
fn close_diag_file(file: DiagFile) -> Result<(), std::io::Error> {
    nix::unistd::close(file.file).map_err(std::io::Error::from)
}

/// 将零个、一个或多个错误转为与 Go `errors.Join` 等价的结果。
fn join_errors(mut errors: Vec<DiagError>) -> Result<(), DiagError> {
    match errors.len() {
        0 => Ok(()),
        1 => Err(errors.remove(0)),
        _ => Err(DiagError::message(
            errors
                .into_iter()
                .map(|error| error.to_string())
                .collect::<Vec<_>>()
                .join("\n"),
        )),
    }
}
