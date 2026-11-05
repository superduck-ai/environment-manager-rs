//! 应用级 tracing subscriber 与子进程字节流适配器。

mod log_writer;
#[cfg(test)]
mod log_writer_test;
mod subscriber;
pub use log_writer::{LogWriter, MAX_LOG_WRITER_BUFFERED_LINE};
pub use subscriber::initialize;
