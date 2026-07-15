//! 子进程输出流的公共类型。
//!
//! Go 版本把 stdout/stderr 作为整数枚举传给同步回调。这里保留同步回调语义，
//! 使读取管道的异步任务不会持有借用跨越 `.await`，也便于调用方做有界转发。

use std::{error::Error, fmt, sync::Arc};

/// 可在线程与 Tokio 任务之间传递的动态错误。
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// 子进程输出回调。
///
/// 回调必须快速返回；需要执行网络或磁盘异步操作时，应把数据复制进有界队列，
/// 由独立任务消费，从而避免同时阻塞 stdout 与 stderr 管道。
pub type OutputStreamer = Arc<dyn Fn(StreamType, &[u8]) -> Result<(), BoxError> + Send + Sync>;

/// stdout 与 stderr 的类型安全标识。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamType {
    /// 标准输出。
    Stdout,
    /// 标准错误。
    Stderr,
}

impl StreamType {
    /// 对应 Go：`stream.go` 中的 `StreamType.String`。
    ///
    /// 返回用于日志和错误文本的小写稳定名称；Rust 枚举不能构造 Go 整数枚举的
    /// 未知值，因此所有可构造实例都映射为 `stdout` 或 `stderr`。
    #[must_use]
    pub const fn string(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

impl fmt::Display for StreamType {
    /// 输出与 Go `StreamType.String` 一致的小写名称。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.string())
    }
}

/// 把普通闭包包装成可共享的 [`OutputStreamer`]。
///
/// 该辅助函数隐藏 `Arc<dyn Fn>` 的样板代码，并确保调用方在编译期满足
/// `Send + Sync`，因为 stdout 与 stderr 会由两个并发任务调用。
pub fn output_streamer<F, E>(streamer: F) -> OutputStreamer
where
    F: Fn(StreamType, &[u8]) -> Result<(), E> + Send + Sync + 'static,
    E: Error + Send + Sync + 'static,
{
    Arc::new(move |stream, bytes| {
        streamer(stream, bytes).map_err(|error| Box::new(error) as BoxError)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证日志和错误消息依赖的流名称保持兼容。
    #[test]
    fn stream_type_uses_go_compatible_names() {
        assert_eq!(StreamType::Stdout.to_string(), "stdout");
        assert_eq!(StreamType::Stderr.to_string(), "stderr");
    }
}
