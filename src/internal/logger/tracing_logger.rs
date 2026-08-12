//! Rust `tracing` 日志后端。

use std::sync::{Arc, OnceLock};

use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

use super::{LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, Logger};

const LOG_DIRECTORY: &str = "/tmp";
const LOG_FILE: &str = "env-manager.log";

static TRACING_RUNTIME: OnceLock<TracingRuntime> = OnceLock::new();
static DEFAULT_LOGGER: OnceLock<Logger> = OnceLock::new();

struct TracingRuntime {
    _file_guard: tracing_appender::non_blocking::WorkerGuard,
}

/// 安装同时写 stderr 和 `/tmp/env-manager.log` 的进程级 tracing subscriber。
fn initialize_tracing() {
    TRACING_RUNTIME.get_or_init(|| {
        let appender = tracing_appender::rolling::never(LOG_DIRECTORY, LOG_FILE);
        let (file_writer, file_guard) = tracing_appender::non_blocking(appender);
        let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            tracing_subscriber::EnvFilter::new(
                "warn,environment_manager::internal::logger::tracing_logger=trace",
            )
        });
        let stderr = tracing_subscriber::fmt::layer()
            .json()
            .with_ansi(false)
            .with_writer(std::io::stderr);
        let file = tracing_subscriber::fmt::layer()
            .json()
            .with_ansi(false)
            .with_writer(file_writer);
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(stderr)
            .with(file)
            .try_init();
        TracingRuntime {
            _file_guard: file_guard,
        }
    });
}

/// 创建生产 logger。级别过滤留在句柄中，subscriber 只负责输出。
pub fn create_logger(level: LogLevel) -> Logger {
    initialize_tracing();
    Logger::new(Arc::new(TracingHandler::new(level)))
}

/// 未显式注入 logger 的路径使用 INFO 级 tracing 句柄。
pub fn default_logger() -> &'static Logger {
    DEFAULT_LOGGER.get_or_init(|| {
        initialize_tracing();
        Logger::new(Arc::new(TracingHandler::new(LogLevel::Info)))
    })
}

#[derive(Clone)]
struct TracingHandler {
    min_level: LogLevel,
    attributes: LogAttributes,
    groups: Vec<String>,
}

impl TracingHandler {
    /// 创建指定最低级别的 tracing handler。
    fn new(min_level: LogLevel) -> Self {
        Self {
            min_level,
            attributes: LogAttributes::new(),
            groups: Vec::new(),
        }
    }

    /// 合并固定属性、记录属性和属性组。
    fn attributes(&self, record: &LogRecord) -> serde_json::Value {
        let mut attributes = self.attributes.clone();
        attributes.extend(record.attributes.clone());
        let mut value = serde_json::Value::Object(attributes);
        for group in self.groups.iter().rev().filter(|group| !group.is_empty()) {
            value = serde_json::json!({ group: value });
        }
        value
    }
}

impl LogHandler for TracingHandler {
    /// 按句柄配置的最低级别过滤记录。
    fn enabled(&self, level: LogLevel) -> bool {
        level >= self.min_level
    }

    /// 把记录转换为原生 tracing 事件。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        if !self.enabled(record.level) {
            return Ok(());
        }
        let attributes = self.attributes(record);
        let source_file = record
            .source
            .as_ref()
            .map_or("", |source| source.file.as_str());
        let source_line = record.source.as_ref().map_or(0, |source| source.line);
        match record.level {
            LogLevel::Trace => tracing::trace!(
                message = %record.message,
                attributes = %attributes,
                source.file = source_file,
                source.line = source_line,
            ),
            LogLevel::Debug => tracing::debug!(
                message = %record.message,
                attributes = %attributes,
                source.file = source_file,
                source.line = source_line,
            ),
            LogLevel::Info => tracing::info!(
                message = %record.message,
                attributes = %attributes,
                source.file = source_file,
                source.line = source_line,
            ),
            LogLevel::Warn => tracing::warn!(
                message = %record.message,
                attributes = %attributes,
                source.file = source_file,
                source.line = source_line,
            ),
            LogLevel::Error => tracing::error!(
                message = %record.message,
                attributes = %attributes,
                source.file = source_file,
                source.line = source_line,
            ),
        }
        Ok(())
    }

    /// 返回绑定固定属性的新 tracing handler。
    fn with_attrs(&self, attributes: LogAttributes) -> Arc<dyn LogHandler> {
        let mut combined = self.attributes.clone();
        combined.extend(attributes);
        Arc::new(Self {
            min_level: self.min_level,
            attributes: combined,
            groups: self.groups.clone(),
        })
    }

    /// 返回进入属性组的新 tracing handler。
    fn with_group(&self, name: &str) -> Arc<dyn LogHandler> {
        let mut groups = self.groups.clone();
        groups.push(name.to_owned());
        Arc::new(Self {
            min_level: self.min_level,
            attributes: self.attributes.clone(),
            groups,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证最低级别过滤，并确认记录可以交给原生 tracing subscriber。
    #[test]
    fn tracing_handler_emits_native_event() {
        let subscriber = tracing_subscriber::fmt().with_test_writer().json().finish();
        tracing::subscriber::with_default(subscriber, || {
            let logger = Logger::new(Arc::new(TracingHandler::new(LogLevel::Info)));
            assert!(!logger.enabled(LogLevel::Debug));
            assert!(logger.enabled(LogLevel::Info));
            logger.info(
                "native tracing event",
                serde_json::json!({"component": "logger"})
                    .as_object()
                    .cloned()
                    .expect("对象字面量必须是 JSON object"),
            );
        });
    }
}
