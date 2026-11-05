//! 进程级 `tracing` subscriber 初始化。

use std::sync::OnceLock;

use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

const LOG_DIRECTORY: &str = "/tmp";
const LOG_FILE: &str = "env-manager.log";

static TRACING_RUNTIME: OnceLock<TracingRuntime> = OnceLock::new();

struct TracingRuntime {
    _file_guard: tracing_appender::non_blocking::WorkerGuard,
}

/// 安装同时写 stderr 和 `/tmp/env-manager.log` 的进程级 subscriber。
pub fn initialize(level: tracing::Level) {
    TRACING_RUNTIME.get_or_init(|| {
        let appender = tracing_appender::rolling::never(LOG_DIRECTORY, LOG_FILE);
        let (file_writer, file_guard) = tracing_appender::non_blocking(appender);
        let default_filter = format!(
            "warn,environment_manager={}",
            level.as_str().to_ascii_lowercase()
        );
        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter));
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
