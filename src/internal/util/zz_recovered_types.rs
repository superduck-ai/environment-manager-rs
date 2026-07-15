//! 对应 Go `internal/util/zz_recovered_types.go` 的类型对照入口。
//!
//! 实际实现按职责保留在 `periodic_invoker.rs`、`retry.rs`、`stream.rs`、
//! `stopper.rs` 与 `tailer.rs`；本文件统一重导出恢复出的 Go 类型，便于符号清单
//! 和后续逐文件差分，同时不复制状态或引入第二套实现。

pub use super::periodic_invoker::PeriodicInvoker;
pub use super::retry::{ErrorClassifier, RetryConfig};
pub use super::stopper::Stopper;
pub use super::stream::{OutputStreamer, StreamType};
pub use super::tailer::{TailLine, Tailer};

/// 对应 Go：`zz_recovered_types.go` 中的 `Line`。
///
/// Rust 使用更明确的 `TailLine` 名称避免与文本行等通用概念混淆；别名保持恢复
/// 类型的对照入口以及完全相同的所有权和字段布局。
pub type Line = TailLine;
