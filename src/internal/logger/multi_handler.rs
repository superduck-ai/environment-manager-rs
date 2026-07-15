//! 多路分发和最低级别过滤 handler。
//!
//! MultiHandler 保持 Go 行为：每个启用的下游都会收到记录；某个下游失败只写
//! stderr，不阻止其他下游，也不把错误返回给业务调用方。

use std::{
    fmt,
    io::{self, Write},
    sync::Arc,
};

use super::{LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord};

/// 把同一条记录 fan-out 到多个独立 handler。
pub struct MultiHandler {
    handlers: Vec<Arc<dyn LogHandler>>,
}

impl MultiHandler {
    /// 从有序 handler 列表构造；处理顺序与列表顺序一致。
    pub fn new(handlers: Vec<Arc<dyn LogHandler>>) -> Self {
        Self { handlers }
    }

    /// 返回下游 handler 数量，主要用于启动诊断和测试。
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// 判断 fan-out 列表是否为空。
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}

impl LogHandler for MultiHandler {
    /// 任意下游启用该级别时，整体即启用。
    fn enabled(&self, level: LogLevel) -> bool {
        self.handlers.iter().any(|handler| handler.enabled(level))
    }

    /// 依次调用所有启用的下游，并隔离单个下游错误。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        for handler in &self.handlers {
            if !handler.enabled(record.level) {
                continue;
            }
            if let Err(error) = handler.handle(record) {
                write_stderr_line(format_args!("handler error: {error}"));
            }
        }
        Ok(())
    }

    /// 在每个下游上派生相同固定属性，保持 fan-out 结构。
    fn with_attrs(&self, attributes: LogAttributes) -> Arc<dyn LogHandler> {
        Arc::new(Self::new(
            self.handlers
                .iter()
                .map(|handler| handler.with_attrs(attributes.clone()))
                .collect(),
        ))
    }

    /// 在每个下游进入相同 group，保持 fan-out 结构。
    fn with_group(&self, name: &str) -> Arc<dyn LogHandler> {
        Arc::new(Self::new(
            self.handlers
                .iter()
                .map(|handler| handler.with_group(name))
                .collect(),
        ))
    }
}

/// 在任意 handler 外增加最低级别门槛。
pub struct MinLevelHandler {
    min_level: LogLevel,
    handler: Arc<dyn LogHandler>,
}

impl MinLevelHandler {
    /// 构造最低级别过滤器。
    pub fn new(min_level: LogLevel, handler: Arc<dyn LogHandler>) -> Self {
        Self { min_level, handler }
    }
}

impl LogHandler for MinLevelHandler {
    /// 先检查本层门槛，再尊重下游自身过滤规则。
    fn enabled(&self, level: LogLevel) -> bool {
        level >= self.min_level && self.handler.enabled(level)
    }

    /// 转发已通过 `enabled` 的记录；直接调用时仍由下游决定处理方式。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        self.handler.handle(record)
    }

    /// 在下游派生属性，同时保留最低级别门槛。
    fn with_attrs(&self, attributes: LogAttributes) -> Arc<dyn LogHandler> {
        Arc::new(Self::new(
            self.min_level,
            self.handler.with_attrs(attributes),
        ))
    }

    /// 在下游进入 group，同时保留最低级别门槛。
    fn with_group(&self, name: &str) -> Arc<dyn LogHandler> {
        Arc::new(Self::new(self.min_level, self.handler.with_group(name)))
    }
}

/// 尽力向 stderr 写一行 handler 诊断；stderr 失败不能中断 fan-out。
fn write_stderr_line(message: fmt::Arguments<'_>) {
    let _ = writeln!(io::stderr().lock(), "{message}");
}
