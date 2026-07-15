//! `MultiHandler` 与 `MinLevelHandler` 的恢复 Go 行为测试。

use std::{
    io,
    sync::{Arc, Mutex},
};

use super::{
    LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, Logger, MinLevelHandler,
    MultiHandler,
};

/// 可配置最低级别、固定属性与失败行为的内存 handler。
struct RecordingHandler {
    min_level: LogLevel,
    records: Arc<Mutex<Vec<LogRecord>>>,
    fail: bool,
    bound: LogAttributes,
}

impl LogHandler for RecordingHandler {
    /// 按测试配置判断级别。
    fn enabled(&self, level: LogLevel) -> bool {
        level >= self.min_level
    }

    /// 保存克隆记录，并可在保存后模拟下游 I/O 错误。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        let mut record = record.clone();
        record.attributes.extend(self.bound.clone());
        self.records.lock().unwrap().push(record);
        if self.fail {
            Err(Box::new(io::Error::other("boom")))
        } else {
            Ok(())
        }
    }

    /// 克隆测试 handler 并合并固定属性。
    fn with_attrs(&self, attributes: LogAttributes) -> Arc<dyn LogHandler> {
        let mut bound = self.bound.clone();
        bound.extend(attributes);
        Arc::new(Self {
            min_level: self.min_level,
            records: Arc::clone(&self.records),
            fail: self.fail,
            bound,
        })
    }

    /// 此测试 handler 不解释 group，仅返回共享 sink 的派生实例。
    fn with_group(&self, _name: &str) -> Arc<dyn LogHandler> {
        self.with_attrs(LogAttributes::new())
    }
}

/// 构造 handler trait object 与共享记录槽。
fn recording(min_level: LogLevel, fail: bool) -> (Arc<dyn LogHandler>, Arc<Mutex<Vec<LogRecord>>>) {
    let records = Arc::new(Mutex::new(Vec::new()));
    (
        Arc::new(RecordingHandler {
            min_level,
            records: Arc::clone(&records),
            fail,
            bound: LogAttributes::new(),
        }),
        records,
    )
}

/// 验证 info 只发送给启用该级别的下游。
#[test]
fn test_multi_handler_fans_out_to_enabled_handlers() {
    let (info, info_records) = recording(LogLevel::Info, false);
    let (warn, warn_records) = recording(LogLevel::Warn, false);
    let handler = MultiHandler::new(vec![info, warn]);

    assert!(handler.enabled(LogLevel::Info));
    handler
        .handle(&LogRecord::new(
            LogLevel::Info,
            "hello",
            LogAttributes::new(),
        ))
        .unwrap();

    assert_eq!(info_records.lock().unwrap().len(), 1);
    assert!(warn_records.lock().unwrap().is_empty());
}

/// 验证一个下游报错后仍继续调用后续下游且整体返回成功。
#[test]
fn test_multi_handler_continues_after_handler_error() {
    let (failing, _) = recording(LogLevel::Info, true);
    let (healthy, records) = recording(LogLevel::Info, false);
    let handler = MultiHandler::new(vec![failing, healthy]);

    handler
        .handle(&LogRecord::new(
            LogLevel::Error,
            "error",
            LogAttributes::new(),
        ))
        .unwrap();

    assert_eq!(records.lock().unwrap().len(), 1);
}

/// 验证固定属性会派生到 fan-out 中的每个下游。
#[test]
fn test_multi_handler_with_attrs() {
    let (inner, records) = recording(LogLevel::Info, false);
    let handler = MultiHandler::new(vec![inner]);
    let mut attributes = LogAttributes::new();
    attributes.insert("component".to_owned(), serde_json::json!("manager"));
    let derived = handler.with_attrs(attributes);

    derived
        .handle(&LogRecord::new(
            LogLevel::Info,
            "hello",
            LogAttributes::new(),
        ))
        .unwrap();

    assert_eq!(
        attr_value(&records.lock().unwrap()[0], "component"),
        Some("manager")
    );
}

/// 验证最低级别 wrapper 过滤 info 并保留恰好位于门槛的 warn。
#[test]
fn test_min_level_handler_filters_below_minimum() {
    let (inner, records) = recording(LogLevel::Trace, false);
    let logger = Logger::new(Arc::new(MinLevelHandler::new(LogLevel::Warn, inner)));

    logger.info("below", LogAttributes::new());
    logger.warn("at minimum", LogAttributes::new());

    let records = records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].message, "at minimum");
}

/// 对应 Go：`attrValue`，返回记录中指定字符串属性的借用视图。
fn attr_value<'a>(record: &'a LogRecord, key: &str) -> Option<&'a str> {
    record
        .attributes
        .get(key)
        .and_then(serde_json::Value::as_str)
}
