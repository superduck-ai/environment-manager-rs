//! 应用日志的轻量注入边界。
//!
//! 生产 handler 把记录交给 Rust `tracing`；trait 只保留测试捕获和 Claude 子进程
//! 日志转发所需的依赖注入，不再模拟 Go `slog` 的 source、格式或 handler 细节。

use std::{error::Error, panic::Location, sync::Arc, time::SystemTime};

/// handler 失败的线程安全动态错误。
pub type LogHandlerError = Box<dyn Error + Send + Sync + 'static>;

/// 应用日志级别。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogLevel {
    /// 最细粒度的运行轨迹。
    Trace,
    /// 开发和故障排查信息。
    Debug,
    /// 正常业务状态。
    Info,
    /// 可恢复的异常状态。
    Warn,
    /// 已失败的操作。
    Error,
}

impl LogLevel {
    /// 返回适合 transcript 和外部结构化记录使用的稳定大写名称。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "TRACE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        }
    }
}

/// 保留调用顺序的结构化日志属性。
pub type LogAttributes = serde_json::Map<String, serde_json::Value>;

/// Rust 调用位置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSource {
    /// Rust 源文件路径。
    pub file: String,
    /// 一基源码行号。
    pub line: u32,
}

/// 一条拥有全部数据的结构化日志记录。
#[derive(Debug, Clone)]
pub struct LogRecord {
    /// 事件产生时间。
    pub time: SystemTime,
    /// 事件级别。
    pub level: LogLevel,
    /// 人类可读消息。
    pub message: String,
    /// Rust 调用位置；手工构造的记录可以没有来源。
    pub source: Option<LogSource>,
    /// 结构化业务属性。
    pub attributes: LogAttributes,
}

impl LogRecord {
    /// 使用当前时间构造一条尚未绑定调用位置的记录。
    pub fn new(level: LogLevel, message: impl Into<String>, attributes: LogAttributes) -> Self {
        Self {
            time: SystemTime::now(),
            level,
            message: message.into(),
            source: None,
            attributes,
        }
    }

    /// 记录由 `track_caller` 捕获的 Rust 文件和行号。
    fn set_caller(&mut self, location: &'static Location<'static>) {
        self.source = Some(LogSource {
            file: location.file().to_owned(),
            line: location.line(),
        });
    }
}

/// 可注入的日志接收端。生产实现位于 `tracing_logger`，其余实现主要用于测试捕获。
pub trait LogHandler: Send + Sync {
    /// 判断给定级别是否启用。
    fn enabled(&self, level: LogLevel) -> bool;
    /// 处理一条结构化记录。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError>;
    /// 派生一个绑定固定属性的 handler。
    fn with_attrs(&self, attributes: LogAttributes) -> Arc<dyn LogHandler>;
    /// 派生一个进入属性组的 handler。
    fn with_group(&self, name: &str) -> Arc<dyn LogHandler>;
}

/// 可克隆的日志句柄；生产记录由底层 handler 发送给 `tracing`。
#[derive(Clone)]
pub struct Logger {
    handler: Arc<dyn LogHandler>,
}

impl Logger {
    /// 从可注入 handler 创建日志句柄。
    pub fn new(handler: Arc<dyn LogHandler>) -> Self {
        Self { handler }
    }

    /// 判断给定级别是否启用。
    pub fn enabled(&self, level: LogLevel) -> bool {
        self.handler.enabled(level)
    }

    /// 发送指定级别、消息和属性的记录。
    #[track_caller]
    pub fn log(&self, level: LogLevel, message: impl Into<String>, attributes: LogAttributes) {
        if !self.handler.enabled(level) {
            return;
        }
        let mut record = LogRecord::new(level, message, attributes);
        record.set_caller(Location::caller());
        let _ = self.handler.handle(&record);
    }

    /// 记录调试事件。
    #[track_caller]
    pub fn debug(&self, message: impl Into<String>, attributes: LogAttributes) {
        self.log(LogLevel::Debug, message, attributes);
    }

    /// 记录普通信息事件。
    #[track_caller]
    pub fn info(&self, message: impl Into<String>, attributes: LogAttributes) {
        self.log(LogLevel::Info, message, attributes);
    }

    /// 记录警告事件。
    #[track_caller]
    pub fn warn(&self, message: impl Into<String>, attributes: LogAttributes) {
        self.log(LogLevel::Warn, message, attributes);
    }

    /// 记录错误事件。
    #[track_caller]
    pub fn error(&self, message: impl Into<String>, attributes: LogAttributes) {
        self.log(LogLevel::Error, message, attributes);
    }

    /// 返回绑定固定属性的新句柄。
    pub fn with_attrs(&self, attributes: LogAttributes) -> Self {
        Self {
            handler: self.handler.with_attrs(attributes),
        }
    }

    /// 返回进入属性组的新句柄。
    pub fn with_group(&self, name: &str) -> Self {
        Self {
            handler: self.handler.with_group(name),
        }
    }
}
