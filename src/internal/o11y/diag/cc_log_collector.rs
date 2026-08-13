//! Claude Code JSONL 文件增量 collector。

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::internal::{
    api::DiagLogEntry,
    util::{TailLine, Tailer},
};

use super::{DiagError, append_and_cap_logs};

/// 运行中的 Claude Code 文件跟随器及其待 flush 队列。
pub(super) struct CcLogCollector {
    tail: Arc<Tailer>,
    buffer: Arc<Mutex<Vec<DiagLogEntry>>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl CcLogCollector {
    /// 原子取走当前全部日志；空队列返回空 `Vec`。
    pub(super) fn drain(&self) -> Vec<DiagLogEntry> {
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::mem::take(&mut *buffer)
    }

    /// 停止 tailer 并等待 collector 完全退出。
    ///
    /// `Tailer::stop` 先关闭生产端，随后等待 consumer 消费完已经送达 channel 的行。
    /// 正常关闭没有返回值；若后台任务 panic，则恢复 unwind 以实现后台任务 panic
    /// 的致命语义，而不是记录 Rust 独有 warning 后继续运行。
    pub(super) async fn stop(&self) {
        self.tail.stop().await;
        let task = self
            .task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(task) = task
            && let Err(error) = task.await
            && error.is_panic()
        {
            // collector 后台任务的 panic 会终止整个进程。Tokio 默认只把 panic
            // 封装进 JoinError，因此必须在唯一 join 点恢复 unwind，不能改写成一条
            // Rust 独有 warning 后继续运行。
            std::panic::resume_unwind(error.into_panic());
        }
    }

    #[cfg(test)]
    /// 用预置 buffer 创建不带后台 tailer 的 collector 测试替身。
    pub(super) fn buffered_for_test(buffer: Vec<DiagLogEntry>) -> Arc<Self> {
        let file = tempfile::NamedTempFile::new().expect("create collector test file");
        let tail = Arc::new(Tailer::new(file.path(), Duration::ZERO).expect("create test tailer"));
        Arc::new(Self {
            tail,
            buffer: Arc::new(Mutex::new(buffer)),
            task: Mutex::new(None),
        })
    }

    /// 消费 tailer 行并写入有界内存队列。
    ///
    /// 空行和 tailer 错误行静默跳过；JSON 解析失败时按 `error`、`line` 顺序写 WARN。
    /// 有效对象会移除可解析的 RFC 3339 纳秒 `timestamp`、覆盖 `source=cc`，最后在 mutex
    /// 内追加。日志直接发送到进程级 tracing subscriber。
    async fn collect(mut lines: mpsc::Receiver<TailLine>, buffer: Arc<Mutex<Vec<DiagLogEntry>>>) {
        while let Some(line) = lines.recv().await {
            if line.error.is_some() || line.text.is_empty() {
                continue;
            }

            let mut fields = match serde_json::from_str::<BTreeMap<String, Value>>(&line.text) {
                Ok(fields) => fields,
                Err(error) => {
                    tracing::warn!(error = %error, line = %line.text, "Failed to parse CC log line as JSON");
                    continue;
                }
            };
            let timestamp = fields
                .remove("timestamp")
                .and_then(|value| value.as_str().map(ToOwned::to_owned))
                .and_then(|timestamp| DateTime::parse_from_rfc3339(&timestamp).ok())
                .map(|timestamp| timestamp.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);
            fields.insert("source".to_owned(), Value::String("cc".to_owned()));
            let entry = DiagLogEntry { timestamp, fields };

            let mut buffered = buffer
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let existing = std::mem::take(&mut *buffered);
            *buffered = append_and_cap_logs(existing, vec![entry]);
        }
    }
}

/// 创建从文件当前末尾开始读取的 Claude Code collector。
///
/// # 参数
/// - `cancellation`：父会话取消 token，负责结束 tailer。
/// - `log_file_path`：Claude Code 持续追加 JSONL 的文件路径。
/// # 返回
/// 成功时返回带后台 consumer task 的共享 collector；tailer 创建、channel 获取或启动
/// 失败时返回统一的 `failed to start tailing claude-code log file` 前缀。
///
/// # 副作用
/// 打开目标文件并启动 tailer 与一个 Tokio task；调用方必须执行 `stop` 完成回收。
pub(super) async fn new_cc_log_collector(
    cancellation: CancellationToken,
    log_file_path: impl AsRef<Path>,
) -> Result<Arc<CcLogCollector>, DiagError> {
    let tail = Arc::new(
        Tailer::new(log_file_path.as_ref(), Duration::ZERO).map_err(|error| {
            DiagError::message(format!(
                "failed to start tailing claude-code log file: {error}"
            ))
        })?,
    );
    let lines = tail.take_lines().map_err(|error| {
        DiagError::message(format!(
            "failed to start tailing claude-code log file: {error}"
        ))
    })?;
    tail.start(cancellation).await.map_err(|error| {
        DiagError::message(format!(
            "failed to start tailing claude-code log file: {error}"
        ))
    })?;

    let buffer = Arc::new(Mutex::new(Vec::new()));
    let collector_buffer = Arc::clone(&buffer);
    let task = tokio::spawn(async move {
        CcLogCollector::collect(lines, collector_buffer).await;
    });
    Ok(Arc::new(CcLogCollector {
        tail,
        buffer,
        task: Mutex::new(Some(task)),
    }))
}
