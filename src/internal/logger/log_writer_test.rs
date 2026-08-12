//! `LogWriter` 的行缓冲行为测试。

use std::{
    io::Write,
    sync::{Arc, Mutex},
};

use super::{
    LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, LogWriter, Logger,
    MAX_LOG_WRITER_BUFFERED_LINE,
};

/// 收集日志记录的测试 handler，对应 Go 的 `recordingHandler`。
#[derive(Clone)]
struct RecordingHandler {
    records: Arc<Mutex<Vec<LogRecord>>>,
}

impl LogHandler for RecordingHandler {
    /// 测试中启用全部级别。
    fn enabled(&self, _level: LogLevel) -> bool {
        true
    }

    /// 保存收到记录的克隆。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        self.records.lock().unwrap().push(record.clone());
        Ok(())
    }

    /// 返回共享收集槽并会合并固定属性的 `AttrRecordingHandler`。
    fn with_attrs(&self, attributes: LogAttributes) -> Arc<dyn LogHandler> {
        Arc::new(AttrRecordingHandler {
            handler: self.clone(),
            attributes,
        })
    }

    /// 返回共享收集槽的等价派生 handler。
    fn with_group(&self, _name: &str) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }
}

/// 带固定属性的收集 handler，对应 Go 的 `attrRecordingHandler`。
#[derive(Clone)]
struct AttrRecordingHandler {
    handler: RecordingHandler,
    attributes: LogAttributes,
}

impl LogHandler for AttrRecordingHandler {
    /// 对应 Go：`attrRecordingHandler.Enabled`，沿用基础 handler 的级别判断。
    fn enabled(&self, level: LogLevel) -> bool {
        self.handler.enabled(level)
    }

    /// 对应 Go：`attrRecordingHandler.Handle`，在保存前合并派生属性。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        let mut enriched = record.clone();
        enriched.attributes.extend(self.attributes.clone());
        self.handler.handle(&enriched)
    }

    /// 对应 Go：`attrRecordingHandler.WithAttrs`，按调用顺序追加固定属性。
    fn with_attrs(&self, attributes: LogAttributes) -> Arc<dyn LogHandler> {
        let mut combined = self.attributes.clone();
        combined.extend(attributes);
        Arc::new(Self {
            handler: self.handler.clone(),
            attributes: combined,
        })
    }

    /// 对应 Go：`attrRecordingHandler.WithGroup`；测试 handler 不改变属性键编码。
    fn with_group(&self, _name: &str) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }
}

/// 创建 writer 与共享记录槽。
fn writer(level: LogLevel, attributes: LogAttributes) -> (LogWriter, Arc<Mutex<Vec<LogRecord>>>) {
    let records = Arc::new(Mutex::new(Vec::new()));
    let logger = Logger::new(Arc::new(RecordingHandler {
        records: Arc::clone(&records),
    }));
    (LogWriter::new(logger, level, "cc: ", attributes), records)
}

/// 验证跨 write 半行拼接、prefix、级别与固定属性。
#[test]
fn test_log_writer_writes_complete_lines() {
    let mut attributes = LogAttributes::new();
    attributes.insert("component".to_owned(), serde_json::json!("claude"));
    let (mut writer, records) = writer(LogLevel::Info, attributes);

    assert_eq!(writer.write(b"first\nsecond").unwrap(), 12);
    assert_eq!(records.lock().unwrap().len(), 1);
    assert_eq!(writer.write(b" line\n").unwrap(), 6);

    let records = records.lock().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].level, LogLevel::Info);
    assert_eq!(records[0].message, "cc: first");
    assert_eq!(records[1].message, "cc: second line");
    assert_eq!(records[0].attributes["component"], "claude");
    let source = records[0]
        .source
        .as_ref()
        .expect("LogWriter 通过 Logger 写出的记录必须携带 Rust source");
    assert!(source.file.ends_with("internal/logger/log_writer.rs"));
    assert!(source.line > 0);
}

/// 验证超过 1 MiB 的半行会完整发送、附加标志并清空缓冲区。
#[test]
fn test_log_writer_truncates_overlong_buffered_line() {
    let (mut writer, records) = writer(LogLevel::Warn, LogAttributes::new());
    let payload = vec![b'x'; MAX_LOG_WRITER_BUFFERED_LINE + 1];

    assert_eq!(writer.write(&payload).unwrap(), payload.len());

    let records = records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].level, LogLevel::Warn);
    let suffix = " [line truncated - no newline]";
    assert!(records[0].message.starts_with("cc: "));
    assert!(records[0].message.ends_with(suffix));
    assert_eq!(
        records[0].message.len(),
        "cc: ".len() + payload.len() + suffix.len()
    );
    assert!(
        records[0].message["cc: ".len().."cc: ".len() + payload.len()]
            .bytes()
            .all(|byte| byte == b'x')
    );
    assert_eq!(writer.buffered_len(), 0);
    let source = records[0]
        .source
        .as_ref()
        .expect("超长记录也必须携带 Rust source");
    assert!(source.file.ends_with("internal/logger/log_writer.rs"));
    assert!(source.line > 0);
}
