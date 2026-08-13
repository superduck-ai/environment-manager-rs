//! Util 模块的公共类型重导出入口。
//!
//! 实际实现按职责保留在 `periodic_invoker.rs`、`retry.rs`、`stream.rs`、
//! `stopper.rs` 与 `tailer.rs`；本文件统一重导出这些公共类型，不复制状态或引入
//! 第二套实现。

pub use super::periodic_invoker::PeriodicInvoker;
pub use super::retry::{ErrorClassifier, RetryConfig};
pub use super::stopper::Stopper;
pub use super::stream::{OutputStreamer, StreamType};
pub use super::tailer::{TailLine, Tailer};

/// 按行解析的文本片段。
///
/// Rust 使用更明确的 `TailLine` 名称避免与文本行等通用概念混淆；别名保持恢复
/// 类型的统一入口以及完全相同的所有权和字段布局。
pub type Line = TailLine;
