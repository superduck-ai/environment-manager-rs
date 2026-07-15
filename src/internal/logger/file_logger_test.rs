//! `FileLoggingHandler` 对恢复 Go 文件日志语义的单元测试。

use std::{
    error::Error,
    fmt, fs, io,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use chrono::{DateTime, Utc};
use tempfile::TempDir;

use super::{
    LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, LogSource, Logger,
    new_file_logging_handler,
};
use crate::internal::logger::file_logger::json_stream_handler_for_test;

/// 保存 inner handler 收到记录的共享槽。
#[derive(Default)]
struct HandlerSink {
    records: Mutex<Vec<LogRecord>>,
}

/// 把 JSON stream handler 的字节保存到测试共享缓冲。
struct SharedWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for SharedWriter {
    /// 原样追加一次 write；mutex 只用于测试观察，不改变 handler 的外层串行语义。
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    /// 内存 writer 没有待刷新的系统缓冲。
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// 模拟 Go 测试中可配置最低级别、属性派生和失败行为的 handler。
struct FanoutTestHandler {
    sink: Arc<HandlerSink>,
    min_level: LogLevel,
    bound: LogAttributes,
    groups: Vec<String>,
    fail: bool,
}

impl FanoutTestHandler {
    /// 构造无绑定属性且不会失败的测试 handler。
    fn new(sink: Arc<HandlerSink>, min_level: LogLevel) -> Self {
        Self {
            sink,
            min_level,
            bound: LogAttributes::new(),
            groups: Vec::new(),
            fail: false,
        }
    }
}

impl LogHandler for FanoutTestHandler {
    /// 按测试配置的最低级别过滤记录。
    fn enabled(&self, level: LogLevel) -> bool {
        level >= self.min_level
    }

    /// 保存记录及派生属性，并可返回可 downcast 的固定错误。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        let mut record = record.clone();
        let group_prefix = if self.groups.is_empty() {
            String::new()
        } else {
            format!("{}.", self.groups.join("."))
        };
        for (key, value) in &self.bound {
            record
                .attributes
                .insert(format!("{group_prefix}{key}"), value.clone());
        }
        self.sink.records.lock().unwrap().push(record);
        if self.fail {
            Err(Box::new(Boom))
        } else {
            Ok(())
        }
    }

    /// 返回合并固定属性且共享 sink 的派生 handler。
    fn with_attrs(&self, attributes: LogAttributes) -> Arc<dyn LogHandler> {
        let mut bound = self.bound.clone();
        bound.extend(attributes);
        Arc::new(Self {
            sink: Arc::clone(&self.sink),
            min_level: self.min_level,
            bound,
            groups: self.groups.clone(),
            fail: self.fail,
        })
    }

    /// 返回进入 group 且共享 sink 的派生 handler。
    fn with_group(&self, name: &str) -> Arc<dyn LogHandler> {
        let mut groups = self.groups.clone();
        if !name.is_empty() {
            groups.push(name.to_owned());
        }
        Arc::new(Self {
            sink: Arc::clone(&self.sink),
            min_level: self.min_level,
            bound: self.bound.clone(),
            groups,
            fail: self.fail,
        })
    }
}

/// 用于验证错误 source chain 未在 wrapper 中丢失。
#[derive(Debug)]
struct Boom;

impl fmt::Display for Boom {
    /// 输出与 Go 测试固定错误相同的文本。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("boom")
    }
}

impl Error for Boom {}

/// 构造测试 inner handler trait object。
fn inner(sink: Arc<HandlerSink>, min_level: LogLevel) -> Arc<dyn LogHandler> {
    Arc::new(FanoutTestHandler::new(sink, min_level))
}

/// 返回固定 UTC 时间，验证 RFC3339Nano 的零小数裁剪行为。
fn fixed_event_time() -> SystemTime {
    DateTime::parse_from_rfc3339("2026-05-31T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
        .into()
}

/// 验证目录创建、inner 委托、确定字段顺序与单行 JSON 内容。
#[test]
fn test_file_logging_handler_writes_json_and_delegates_to_inner() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("nested/env-manager.log");
    let sink = Arc::new(HandlerSink::default());
    let handler = new_file_logging_handler(
        inner(Arc::clone(&sink), LogLevel::Info),
        &path,
        LogLevel::Info,
    )
    .unwrap();
    let mut attributes = LogAttributes::new();
    attributes.insert("component".to_owned(), serde_json::json!("test"));
    attributes.insert("count".to_owned(), serde_json::json!(2));
    let record = LogRecord {
        time: fixed_event_time(),
        level: LogLevel::Info,
        message: "hello".to_owned(),
        source: Some(LogSource {
            function: "environment_manager::tests::fixture".to_owned(),
            file: "/workspace/src/tests/fixture.rs".to_owned(),
            line: 42,
        }),
        attributes,
    };

    handler.handle(&record).unwrap();
    handler.close().unwrap();

    assert_eq!(sink.records.lock().unwrap().len(), 1);
    let line = fs::read_to_string(path).unwrap();
    assert_eq!(line.matches('\n').count(), 1);
    let line = line.strip_suffix('\n').unwrap();
    assert!(line.starts_with(
        r#"{"attributes":{"component":"test","count":2},"level":"INFO","message":"hello","time":"2026-05-31T12:00:00Z","timestamp":""#
    ));
    assert!(line.ends_with("\"}"));
    assert!(line.contains(r#""attributes":{"component":"test","count":2}"#));

    let entry: serde_json::Value = serde_json::from_str(line).unwrap();
    assert_eq!(entry["message"], "hello");
    assert_eq!(entry["level"], "INFO");
    assert_eq!(entry["attributes"]["component"], "test");
    assert_eq!(entry["attributes"]["count"], 2);
    assert!(
        entry.get("source").is_none(),
        "FileLoggingHandler 必须像 Go wrapper 一样忽略 record source"
    );
    assert!(
        entry["timestamp"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
}

/// 验证文件 JSON 使用 encoding/json 的排序及 HTML/行分隔符转义。
#[test]
fn file_logging_handler_uses_go_encoding_json_wire_rules() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("encoding.log");
    let handler = new_file_logging_handler(
        inner(Arc::new(HandlerSink::default()), LogLevel::Debug),
        &path,
        LogLevel::Debug,
    )
    .unwrap();
    let mut attributes = LogAttributes::new();
    attributes.insert("z".to_owned(), serde_json::json!("last <"));
    attributes.insert("a".to_owned(), serde_json::json!("first &"));
    handler
        .handle(&LogRecord {
            time: fixed_event_time(),
            level: LogLevel::Info,
            message: "html <>& \u{2028} \u{2029}".to_owned(),
            source: None,
            attributes,
        })
        .unwrap();
    handler.close().unwrap();

    let line = fs::read_to_string(path).unwrap();
    assert!(line.contains(r#""attributes":{"a":"first \u0026","z":"last \u003c"}"#));
    assert!(line.contains(r#""message":"html \u003c\u003e\u0026 \u2028 \u2029""#));
}

/// 验证 stderr slog JSON 保留属性调用顺序、group 边界且不转义 HTML 字符。
#[test]
fn json_stream_handler_matches_go_slog_order_group_and_escaping() {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let handler = json_stream_handler_for_test(
        Box::new(SharedWriter {
            bytes: Arc::clone(&bytes),
        }),
        LogLevel::Debug,
    );
    let mut bound = LogAttributes::new();
    bound.insert("bound_z".to_owned(), serde_json::json!(2));
    bound.insert("bound_a".to_owned(), serde_json::json!(1));
    let derived = handler.with_attrs(bound).with_group("group");
    let mut record_attributes = LogAttributes::new();
    record_attributes.insert("record_z".to_owned(), serde_json::json!(true));
    record_attributes.insert("record_a".to_owned(), serde_json::json!(false));
    derived
        .handle(&LogRecord {
            time: fixed_event_time(),
            level: LogLevel::Warn,
            message: "grouped <>& \u{2028} \u{2029}".to_owned(),
            source: None,
            attributes: record_attributes,
        })
        .unwrap();

    assert_eq!(
        String::from_utf8(bytes.lock().unwrap().clone()).unwrap(),
        concat!(
            r#"{"time":"2026-05-31T12:00:00Z","level":"WARN","msg":"grouped <>& \u2028 \u2029","bound_z":2,"bound_a":1,"group":{"record_z":true,"record_a":false}}"#,
            "\n"
        )
    );
}

/// 从具名 helper 调用 logger，并返回调用表达式的预期一基行号。
fn write_logger_record_from_named_helper(logger: &Logger) -> u32 {
    let expected_line = line!() + 1;
    logger.info("caller probe", LogAttributes::new());
    expected_line
}

/// 验证 facade 逐层传递 caller，且 stderr source 字段顺序和形状匹配 Go slog。
#[test]
fn json_stream_handler_emits_business_caller_source() {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let handler = json_stream_handler_for_test(
        Box::new(SharedWriter {
            bytes: Arc::clone(&bytes),
        }),
        LogLevel::Debug,
    );
    let logger = Logger::new(handler);
    let expected_line = write_logger_record_from_named_helper(&logger);

    let output = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(output.trim_end()).unwrap();
    let source = parsed["source"]
        .as_object()
        .expect("stderr must include source");
    assert_eq!(
        source.keys().map(String::as_str).collect::<Vec<_>>(),
        ["function", "file", "line"]
    );
    assert!(
        source["function"].as_str().is_some_and(|function| function
            .starts_with("environment_manager::internal::logger::file_logger_test::")
            && !function.contains("Logger::")),
        "source.function 必须描述调用文件中的函数，不能指向 logger facade: {source:?}"
    );
    assert!(
        source["file"]
            .as_str()
            .is_some_and(|file| file.ends_with("/src/internal/logger/file_logger_test.rs"))
    );
    assert_eq!(source["line"], expected_line);

    // 直接检查原始 wire，避免 serde 解析掩盖内建字段或 source 子字段顺序变化。
    let time_end = output
        .find(r#"","level":"INFO","source":{"function":"#)
        .unwrap();
    assert!(time_end > 0);
    assert!(output.contains(r#"","file":"#));
    assert!(output.contains(&format!(
        r#"","line":{expected_line}}},"msg":"caller probe"#
    )));
}

/// 验证 enabled 只取 inner 结果，并验证 inner 错误上下文和 source chain。
#[test]
fn test_file_logging_handler_enabled_and_inner_errors() {
    let temp = TempDir::new().unwrap();
    let sink = Arc::new(HandlerSink::default());
    let handler = new_file_logging_handler(
        inner(Arc::clone(&sink), LogLevel::Warn),
        temp.path().join("enabled.log"),
        LogLevel::Trace,
    )
    .unwrap();

    assert!(!handler.enabled(LogLevel::Info));
    assert!(handler.enabled(LogLevel::Warn));
    handler.close().unwrap();

    let failing_sink = Arc::new(HandlerSink::default());
    let mut failing_inner = FanoutTestHandler::new(Arc::clone(&failing_sink), LogLevel::Info);
    failing_inner.fail = true;
    let path = temp.path().join("error.log");
    let error_handler =
        new_file_logging_handler(Arc::new(failing_inner), &path, LogLevel::Info).unwrap();
    let error = error_handler
        .handle(&LogRecord::new(
            LogLevel::Info,
            "hello",
            LogAttributes::new(),
        ))
        .unwrap_err();

    assert_eq!(error.to_string(), "inner handler error: boom");
    assert!(error.source().is_some_and(|source| source.is::<Boom>()));
    error_handler.close().unwrap();
    assert!(fs::read(path).unwrap().is_empty());
}

/// 验证派生属性和 group 只进入 inner，文件仍只保存 record 自身属性。
#[test]
fn test_file_logging_handler_with_attrs_and_group_returns_file_handler() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("attrs.log");
    let sink = Arc::new(HandlerSink::default());
    let handler = new_file_logging_handler(
        inner(Arc::clone(&sink), LogLevel::Info),
        &path,
        LogLevel::Info,
    )
    .unwrap();
    let mut bound = LogAttributes::new();
    bound.insert("component".to_owned(), serde_json::json!("test"));
    let derived = handler.with_attrs(bound).with_group("group");
    let mut record_attributes = LogAttributes::new();
    record_attributes.insert("record".to_owned(), serde_json::json!(true));

    derived
        .handle(&LogRecord::new(
            LogLevel::Info,
            "derived",
            record_attributes,
        ))
        .unwrap();
    handler.close().unwrap();

    let records = sink.records.lock().unwrap();
    assert_eq!(records[0].attributes["group.component"], "test");
    drop(records);
    let entry: serde_json::Value =
        serde_json::from_str(fs::read_to_string(path).unwrap().trim()).unwrap();
    assert_eq!(entry["attributes"]["record"], true);
    assert!(entry["attributes"].get("component").is_none());
    assert!(entry["attributes"].get("group").is_none());
}

/// 验证生产默认 logger 可以构造并接受一条 error 记录。
#[test]
fn test_create_logger_with_file_output() {
    let logger = super::create_logger_with_file_output(LogLevel::Error);
    logger.error("file logger smoke test", LogAttributes::new());
}

/// 验证派生 handler 共享关闭状态、关闭后 inner 仍处理且再次关闭返回上下文错误。
#[test]
fn derived_handlers_share_file_and_close_state() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("shared.log");
    let sink = Arc::new(HandlerSink::default());
    let handler = new_file_logging_handler(
        inner(Arc::clone(&sink), LogLevel::Info),
        &path,
        LogLevel::Info,
    )
    .unwrap();
    let derived = handler.with_attrs(LogAttributes::new());

    handler.close().unwrap();
    derived
        .handle(&LogRecord::new(
            LogLevel::Info,
            "stderr still works",
            LogAttributes::new(),
        ))
        .unwrap();

    assert_eq!(sink.records.lock().unwrap().len(), 1);
    assert!(fs::read(path).unwrap().is_empty());
    let error = handler.close().unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("failed to close log file: close ")
    );
    assert!(error.to_string().ends_with(": file already closed"));
}

/// 验证构造错误文本具有稳定上下文，并确认新文件没有可执行位。
#[test]
fn file_logging_handler_preserves_open_context_and_file_mode() {
    let temp = TempDir::new().unwrap();
    let blocker = temp.path().join("blocker");
    fs::write(&blocker, b"not a directory").unwrap();
    let sink = Arc::new(HandlerSink::default());
    let error = new_file_logging_handler(
        inner(Arc::clone(&sink), LogLevel::Info),
        blocker.join("env-manager.log"),
        LogLevel::Info,
    )
    .err()
    .expect("regular-file parent must reject directory creation");
    assert!(
        error
            .to_string()
            .starts_with("failed to create log directory: ")
    );

    let path = temp.path().join("mode.log");
    let handler =
        new_file_logging_handler(inner(sink, LogLevel::Info), &path, LogLevel::Info).unwrap();
    handler.close().unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        assert_eq!(fs::metadata(path).unwrap().permissions().mode() & 0o111, 0);
    }

    let open_error = new_file_logging_handler(
        inner(Arc::new(HandlerSink::default()), LogLevel::Info),
        temp.path(),
        LogLevel::Info,
    )
    .err()
    .expect("opening a directory as an append-only file must fail");
    assert!(
        open_error
            .to_string()
            .starts_with("failed to open log file: ")
    );
    assert_eq!(open_error.kind(), io::ErrorKind::IsADirectory);
}
