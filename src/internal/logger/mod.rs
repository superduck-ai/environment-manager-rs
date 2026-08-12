//! 基于 Rust `tracing` 的结构化日志组件。

mod log_writer;
mod tracing_logger;
mod types;

#[cfg(test)]
mod log_writer_test;
pub use log_writer::{LogWriter, MAX_LOG_WRITER_BUFFERED_LINE};
pub use tracing_logger::{create_logger, default_logger};
pub use types::{
    LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, LogSource, Logger,
};
