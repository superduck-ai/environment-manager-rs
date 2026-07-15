//! 诊断日志开关、文件、collector、排序、上限与关闭 flush 回归测试。

use std::{
    collections::BTreeMap,
    fs,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{TimeDelta, TimeZone, Utc};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::internal::{
    api::{ApiError, DiagLogEntry},
    logger::{LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, Logger},
    o11y::discard_o11y_service::DiscardLogHandler,
};

use super::{
    CcLogCollector, DiagService, DiagState, LogFlusher, append_and_cap_logs,
    diag_logs::{MAX_BUFFERED_LOGS, new_diag_service_with_logger},
    get_claude_code_diag_file_path, log_env_manager_no_pii, logs_enabled, new_cc_log_collector,
};

/// 保存 logger 收到的完整结构化记录，供消息、级别和属性顺序断言。
#[derive(Clone, Default)]
struct RecordingLogHandler {
    records: Arc<Mutex<Vec<LogRecord>>>,
}

impl LogHandler for RecordingLogHandler {
    /// 测试需要观察 DEBUG 以上的所有输入，不在 handler 层过滤。
    fn enabled(&self, _level: LogLevel) -> bool {
        true
    }

    /// 按调用顺序保存 owned 记录快照。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(record.clone());
        Ok(())
    }

    /// 本测试没有绑定属性；返回共享同一记录队列的 handler。
    fn with_attrs(&self, _attributes: LogAttributes) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }

    /// 本测试没有 group；返回共享同一记录队列的 handler。
    fn with_group(&self, _name: &str) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }
}

/// 测试 flusher 的固定失败模式。
#[derive(Clone, Copy, Debug, Default)]
enum FlushFailure {
    /// 正常成功。
    #[default]
    None,
    /// 模拟 API endpoint 返回 501。
    EndpointNotImplemented,
    /// 模拟普通远端故障。
    RemoteUnavailable,
}

/// 保存每次 batch 快照的 flusher 替身。
#[derive(Default)]
struct RecordingFlusher {
    calls: Mutex<Vec<Vec<DiagLogEntry>>>,
    failure: FlushFailure,
}

impl RecordingFlusher {
    /// 返回所有 batch 按调用顺序拼接后的日志。
    fn all_logs(&self) -> Vec<DiagLogEntry> {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .flatten()
            .cloned()
            .collect()
    }
}

#[async_trait]
impl LogFlusher for RecordingFlusher {
    /// 保存输入，并按配置返回可判定 API 错误。
    async fn flush_logs(
        &self,
        _cancellation: &CancellationToken,
        logs: &[DiagLogEntry],
    ) -> Result<(), ApiError> {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(logs.to_vec());
        match self.failure {
            FlushFailure::None => Ok(()),
            FlushFailure::EndpointNotImplemented => Err(ApiError::EndpointNotImplemented(
                "endpoint not implemented".to_owned(),
            )),
            FlushFailure::RemoteUnavailable => Err(ApiError::message("remote unavailable")),
        }
    }
}

/// 验证环境变量仅接受 Go `strconv.ParseBool` 的 true 取值。
#[tokio::test]
async fn test_logs_enabled() {
    // 环境变量解析在独立进程内覆盖全部输入，父 libtest 仍可安全并行启动子进程。
    if !crate::internal::enter_isolated_process_test(concat!(module_path!(), "::test_logs_enabled"))
    {
        return;
    }
    let _guard = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let original = std::env::var_os("ENV_MANAGER_ENABLE_DIAG_LOGS");
    for (value, expected) in [
        ("", false),
        ("true", true),
        (" TRUE ", true),
        ("false", false),
        ("yes please", false),
    ] {
        // SAFETY: 全仓库共享测试锁确保此进程全局变量不会被并发测试读写。
        unsafe { std::env::set_var("ENV_MANAGER_ENABLE_DIAG_LOGS", value) };
        assert_eq!(logs_enabled(), expected, "value={value:?}");
    }
    match original {
        Some(value) => {
            // SAFETY: 同上，仍持有跨模块进程状态测试锁。
            unsafe { std::env::set_var("ENV_MANAGER_ENABLE_DIAG_LOGS", value) };
        }
        None => {
            // SAFETY: 同上，仍持有跨模块进程状态测试锁。
            unsafe { std::env::remove_var("ENV_MANAGER_ENABLE_DIAG_LOGS") };
        }
    }
}

/// 验证上下文路径、环境日志最终 flush、本地 JSONL 与关闭后隐藏路径。
#[tokio::test]
async fn test_new_diag_service_context_and_shutdown_flushes() {
    let flusher = Arc::new(RecordingFlusher::default());
    let flusher_trait: Arc<dyn LogFlusher> = flusher.clone();
    let (logger, log_records) = recording_logger();
    let (service, context) =
        new_diag_service_with_logger(CancellationToken::new(), flusher_trait, logger)
            .await
            .expect("create diag service");
    let (env_path, cc_path) = service_paths(&service);

    assert_eq!(
        get_claude_code_diag_file_path(Some(&context)),
        cc_path.to_string_lossy()
    );
    log_env_manager_no_pii(
        Some(&context),
        "start_task_run",
        Some([("answer".to_owned(), json!(42))].into_iter().collect()),
    );
    service
        .shutdown(&CancellationToken::new())
        .await
        .expect("shutdown diag service");
    assert!(get_claude_code_diag_file_path(Some(&context)).is_empty());
    // Go 在 shutdown 后收到事件时写一条无属性 WARN，并且不会重新打开文件或入队。
    log_env_manager_no_pii(Some(&context), "after_shutdown", None);

    let logs = flusher.all_logs();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].fields.get("event"), Some(&json!("start_task_run")));
    assert_eq!(logs[0].fields.get("source"), Some(&json!("env_manager")));

    let contents = fs::read_to_string(&env_path).expect("read env diag file");
    let lines: Vec<_> = contents.lines().collect();
    assert_eq!(lines.len(), 1);
    let file_entry: Value = serde_json::from_str(lines[0]).expect("JSONL entry");
    assert_eq!(file_entry["event"], "start_task_run");
    assert_eq!(file_entry["source"], "env_manager");

    let records = recorded_logs(&log_records);
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].level, LogLevel::Info);
    assert_eq!(records[0].message, "Created diagnostic file");
    assert_eq!(
        records[0]
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["file"]
    );
    assert_eq!(records[1].level, LogLevel::Info);
    assert_eq!(records[1].message, "Created diagnostic file");
    assert_eq!(
        records[1]
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["file"]
    );
    assert_eq!(records[2].level, LogLevel::Warn);
    assert_eq!(
        records[2].message,
        "DiagService is shutdown; discarding log entry"
    );
    assert!(records[2].attributes.is_empty());

    let _ = fs::remove_file(env_path);
    let _ = fs::remove_file(cc_path);
}

/// 验证 endpoint 未实现会在最终 flush 时降级为干净关闭。
#[tokio::test]
async fn test_new_diag_service_treats_endpoint_not_implemented_as_clean_shutdown() {
    let flusher: Arc<dyn LogFlusher> = Arc::new(RecordingFlusher {
        calls: Mutex::new(Vec::new()),
        failure: FlushFailure::EndpointNotImplemented,
    });
    let (logger, log_records) = recording_logger();
    let (service, context) =
        new_diag_service_with_logger(CancellationToken::new(), flusher, logger)
            .await
            .expect("create diag service");
    let (env_path, cc_path) = service_paths(&service);
    log_env_manager_no_pii(Some(&context), "endpoint_missing", None);
    service
        .shutdown(&CancellationToken::new())
        .await
        .expect("501 must be clean shutdown");
    let records = recorded_logs(&log_records);
    let final_flush = records
        .iter()
        .find(|record| record.message == "Final diag log flush skipped; endpoint not implemented")
        .expect("final endpoint warning");
    assert_eq!(final_flush.level, LogLevel::Warn);
    assert_eq!(
        final_flush
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["count"]
    );
    assert_eq!(final_flush.attributes["count"], json!(1));
    let _ = fs::remove_file(env_path);
    let _ = fs::remove_file(cc_path);
}

/// 验证普通最终 flush 错误被 shutdown 返回且保留稳定前缀。
#[tokio::test]
async fn test_shutdown_returns_final_flush_error() {
    let flusher: Arc<dyn LogFlusher> = Arc::new(RecordingFlusher {
        calls: Mutex::new(Vec::new()),
        failure: FlushFailure::RemoteUnavailable,
    });
    let (logger, log_records) = recording_logger();
    let (service, context) =
        new_diag_service_with_logger(CancellationToken::new(), flusher, logger)
            .await
            .expect("create diag service");
    let (env_path, cc_path) = service_paths(&service);
    log_env_manager_no_pii(Some(&context), "flush_error", None);
    let error = service
        .shutdown(&CancellationToken::new())
        .await
        .expect_err("remote failure must escape shutdown");
    assert!(
        error
            .to_string()
            .contains("failed to flush diag logs: remote unavailable")
    );
    let records = recorded_logs(&log_records);
    let final_flush = records
        .iter()
        .find(|record| record.message == "Final diag log flush failed during shutdown")
        .expect("final flush failure log");
    assert_eq!(final_flush.level, LogLevel::Error);
    assert_eq!(
        final_flush
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["error"]
    );
    assert_eq!(
        final_flush.attributes["error"],
        json!("failed to flush diag logs: remote unavailable")
    );
    let _ = fs::remove_file(env_path);
    let _ = fs::remove_file(cc_path);
}

/// 验证 Claude Code collector 解析 JSONL、移除 timestamp、覆盖 source 并跳过坏行。
#[tokio::test]
async fn test_cc_log_collector_parses_json_lines() {
    let directory = tempfile::tempdir().expect("temp directory");
    let path = directory.path().join("claude-code.diag.log");
    fs::write(&path, b"").expect("create empty log");
    let cancellation = CancellationToken::new();
    let (logger, log_records) = recording_logger();
    let collector = new_cc_log_collector(cancellation.clone(), &path, logger)
        .await
        .expect("create CC collector");

    let timestamp_text = "2025-01-02T03:04:05.123456789Z";
    fs::write(
        &path,
        format!(
            "{{\"timestamp\":\"{timestamp_text}\",\"message\":\"hello\"}}\nnot json\n{{\"message\":\"world\"}}\n"
        ),
    )
    .expect("write CC JSONL");

    let mut logs = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        logs.extend(collector.drain());
        if logs.len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    collector.stop().await;
    cancellation.cancel();

    assert_eq!(logs.len(), 2);
    let expected = chrono::DateTime::parse_from_rfc3339(timestamp_text)
        .expect("timestamp")
        .with_timezone(&Utc);
    assert_eq!(logs[0].timestamp, expected);
    assert!(!logs[0].fields.contains_key("timestamp"));
    assert_eq!(logs[0].fields.get("source"), Some(&json!("cc")));
    assert_eq!(logs[0].fields.get("message"), Some(&json!("hello")));
    assert_eq!(logs[1].fields.get("source"), Some(&json!("cc")));
    assert_eq!(logs[1].fields.get("message"), Some(&json!("world")));
    let records = recorded_logs(&log_records);
    let invalid_line = records
        .iter()
        .find(|record| record.message == "Failed to parse CC log line as JSON")
        .expect("invalid JSON warning");
    assert_eq!(invalid_line.level, LogLevel::Warn);
    assert_eq!(
        invalid_line
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["error", "line"]
    );
    assert_eq!(invalid_line.attributes["line"], json!("not json"));
}

/// 验证 pending、环境与 CC 三个来源稳定排序，并在合并后清空来源队列。
#[test]
fn test_collect_and_merge_logs_sorts_and_drains_buffers() {
    let base = Utc
        .with_ymd_and_hms(2025, 1, 2, 3, 4, 5)
        .single()
        .expect("base timestamp");
    let collector =
        CcLogCollector::buffered_for_test(vec![entry(base + TimeDelta::seconds(1), "cc")]);
    let flusher: Arc<dyn LogFlusher> = Arc::new(RecordingFlusher::default());
    let service = DiagService {
        logger: diag_test_logger(),
        state: Mutex::new(DiagState {
            env_manager_logs: vec![entry(base + TimeDelta::seconds(2), "env")],
            env_manager_log_file: None,
            cc_log_file: None,
            is_shutdown: false,
        }),
        cc_collector: Some(collector.clone()),
        flusher,
        flusher_stop: CancellationToken::new(),
        flusher_task: Mutex::new(None),
        parent_cancellation: CancellationToken::new(),
    };

    let merged =
        service.collect_and_merge_logs(vec![entry(base + TimeDelta::seconds(3), "pending")]);
    let sources: Vec<_> = merged
        .iter()
        .map(|entry| entry.fields["source"].as_str().expect("source"))
        .collect();
    assert_eq!(sources, ["cc", "env", "pending"]);
    assert!(service.drain_env_manager_logs().is_empty());
    assert!(collector.drain().is_empty());
}

/// 验证超过 5000 条时只丢弃最老插入项。
#[test]
fn test_append_and_cap_logs_drops_oldest_entries() {
    let existing = (0..MAX_BUFFERED_LOGS)
        .map(|index| DiagLogEntry {
            timestamp: Utc
                .timestamp_opt(index as i64, 0)
                .single()
                .expect("timestamp"),
            fields: [("index".to_owned(), json!(index))].into_iter().collect(),
        })
        .collect();
    let newest = DiagLogEntry {
        timestamp: Utc
            .timestamp_opt(MAX_BUFFERED_LOGS as i64, 0)
            .single()
            .expect("timestamp"),
        fields: [("index".to_owned(), json!(MAX_BUFFERED_LOGS))]
            .into_iter()
            .collect(),
    };

    let (logger, log_records) = recording_logger();
    let logs = append_and_cap_logs(&logger, existing, vec![newest]);
    assert_eq!(logs.len(), MAX_BUFFERED_LOGS);
    assert_eq!(logs[0].fields.get("index"), Some(&json!(1)));
    assert_eq!(
        logs.last().and_then(|entry| entry.fields.get("index")),
        Some(&json!(MAX_BUFFERED_LOGS))
    );
    let records = recorded_logs(&log_records);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].level, LogLevel::Warn);
    assert_eq!(
        records[0].message,
        "Dropping oldest diag log entries due to buffer cap"
    );
    assert_eq!(
        records[0]
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["discarded_count", "max_entries"]
    );
    assert_eq!(records[0].attributes["discarded_count"], json!(1));
    assert_eq!(
        records[0].attributes["max_entries"],
        json!(MAX_BUFFERED_LOGS)
    );
}

/// 读取服务当前两份本地文件路径的快照。
fn service_paths(service: &DiagService) -> (std::path::PathBuf, std::path::PathBuf) {
    let state = service
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    (
        state
            .env_manager_log_file
            .as_ref()
            .expect("env file")
            .path
            .clone(),
        state.cc_log_file.as_ref().expect("CC file").path.clone(),
    )
}

/// 创建只含 source 字段的测试日志。
fn entry(timestamp: chrono::DateTime<Utc>, source: &str) -> DiagLogEntry {
    DiagLogEntry {
        timestamp,
        fields: BTreeMap::from([("source".to_owned(), json!(source))]),
    }
}

/// 构造不写 stderr 的 concrete logger，避免测试覆盖默认进程日志。
fn diag_test_logger() -> Logger {
    Logger::new(Arc::new(DiscardLogHandler))
}

/// 构造记录 logger，并返回其共享记录队列用于断言。
fn recording_logger() -> (Logger, Arc<Mutex<Vec<LogRecord>>>) {
    let handler = RecordingLogHandler::default();
    let records = Arc::clone(&handler.records);
    (Logger::new(Arc::new(handler)), records)
}

/// 获取当前完整日志快照，避免断言期间长期持有 mutex。
fn recorded_logs(records: &Arc<Mutex<Vec<LogRecord>>>) -> Vec<LogRecord> {
    records
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}
