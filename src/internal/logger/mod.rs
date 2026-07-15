//! 结构化日志基础组件。
//!
//! 本模块逐项对应 Go `slog` handler、文件 JSONL 输出和行缓冲 writer。
//! 应用其余部分可以继续使用 `tracing`；这些类型主要用于需要 Go 行为兼容的
//! 子进程日志、双路 handler 和可显式关闭文件的场景。

mod file_logger;
mod go_default_logger;
mod log_writer;
mod multi_handler;
mod zz_recovered_types;

#[cfg(test)]
mod file_logger_test;
#[cfg(test)]
mod log_writer_test;
#[cfg(test)]
mod multi_handler_test;

pub use file_logger::{
    FileLoggingHandler, create_logger_with_file_output, new_file_logging_handler,
};
pub use go_default_logger::default_slog_logger;
pub use log_writer::{LogWriter, MAX_LOG_WRITER_BUFFERED_LINE};
pub use multi_handler::{MinLevelHandler, MultiHandler};
pub use zz_recovered_types::{
    LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, LogSource, Logger,
};
