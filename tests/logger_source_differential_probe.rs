//! stderr 日志 `source` 的 Go/Rust 严格差分 probe。
//!
//! 对应 Go probe：`compat/go-probes/logger_source/main.go`。测试只在 Linux ARM64
//! 门禁中显式运行；时间完全不进入 transcript，而 source 三字段不得归一化。

use std::{
    io::Write,
    sync::{Arc, Mutex},
};

use environment_manager::internal::logger::{
    LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, LogWriter, Logger,
    MAX_LOG_WRITER_BUFFERED_LINE,
};
use serde::Serialize;

/// 一条便于与 Go `runtime.Frame` 逐字段比较的 source。
#[derive(Serialize)]
struct SourceTranscript {
    /// Go runtime 函数全名。
    function: String,
    /// Go `-trimpath` 后的 module 文件名。
    file: String,
    /// Go logger 调用表达式的一基行号。
    line: u32,
}

/// 单个 LogWriter 分支的稳定 transcript；故意不包含时间。
#[derive(Serialize)]
struct TranscriptEntry {
    /// `complete` 或 `truncated` 分支标识。
    case: String,
    /// 运行时 resolver 输出的完整 source，禁止比较器归一化。
    source: SourceTranscript,
}

/// 收集 Logger 记录并立即复制 source，避免测试结束后借用 handler 内部数据。
#[derive(Clone, Default)]
struct SourceHandler {
    entries: Arc<Mutex<Vec<TranscriptEntry>>>,
}

impl LogHandler for SourceHandler {
    /// probe 必须同时接收 info 与 warn 两个 LogWriter 分支。
    fn enabled(&self, _level: LogLevel) -> bool {
        true
    }

    /// 把大消息折叠为分支名；source 三字段保持逐字节原值。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        let source = record
            .source
            .as_ref()
            .expect("Logger 生产记录必须携带 source");
        let case = if record.message.ends_with(" [line truncated - no newline]") {
            "truncated"
        } else {
            record.message.as_str()
        };
        self.entries.lock().unwrap().push(TranscriptEntry {
            case: case.to_owned(),
            source: SourceTranscript {
                function: source.function.clone(),
                file: source.file.clone(),
                line: source.line,
            },
        });
        Ok(())
    }

    /// probe 不绑定额外属性，但仍返回共享收集槽以满足 handler 契约。
    fn with_attrs(&self, _attributes: LogAttributes) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }

    /// source 与 group 无关；派生 handler 继续共享收集槽。
    fn with_group(&self, _name: &str) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }
}

/// 在 Linux ARM64 上生成两个生产 LogWriter caller 的确定性 JSONL transcript。
#[test]
#[ignore = "由 scripts/compat/run_logger_source_differential.sh 显式运行"]
fn write_logger_source_differential_transcript() {
    assert_eq!(std::env::consts::OS, "linux");
    assert_eq!(std::env::consts::ARCH, "aarch64");
    let output = std::env::var_os("LOGGER_SOURCE_TRANSCRIPT_OUTPUT")
        .expect("LOGGER_SOURCE_TRANSCRIPT_OUTPUT is required");
    let handler = SourceHandler::default();
    let entries = Arc::clone(&handler.entries);
    let logger = Logger::new(Arc::new(handler));

    let mut complete = LogWriter::new(logger.clone(), LogLevel::Info, "", LogAttributes::new());
    complete.write_all(b"complete\n").unwrap();

    let mut truncated = LogWriter::new(logger, LogLevel::Warn, "", LogAttributes::new());
    truncated
        .write_all(&vec![b'x'; MAX_LOG_WRITER_BUFFERED_LINE + 1])
        .unwrap();

    let entries = entries.lock().unwrap();
    assert_eq!(entries.len(), 2);
    let mut encoded = Vec::new();
    for entry in entries.iter() {
        serde_json::to_writer(&mut encoded, entry).unwrap();
        encoded.push(b'\n');
    }
    std::fs::write(output, encoded).unwrap();
}
