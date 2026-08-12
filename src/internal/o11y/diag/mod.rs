//! 本地 JSONL 诊断日志、Claude Code tail collector 与远端批量 flush。
//!
//! 环境管理器事件立即写入本地临时文件，同时进入最多 5000 条的内存队列；Claude
//! Code 日志通过现有 Tokio `Tailer` 从文件末尾增量读取。两个来源每 5 秒合并、稳定
//! 按时间排序并发往 `SessionBackend`，关闭时还会执行一次最终 flush。

mod cc_log_collector;
mod diag_logs;
mod types;

pub use diag_logs::{
    get_claude_code_diag_file_path, log_env_manager_no_pii, logs_enabled, new_diag_service,
    new_diag_service_with_backend,
};
pub use types::{DiagContext, DiagError, DiagService, LogFlusher, SessionBackendLogFlusher};

// 这些名称只用于 `diag` 自身的兄弟模块及其测试。保持为父模块私有导入，既允许
// 子模块通过 `super` 访问，也不会把子模块中 `pub(super)` 的实现细节错误地扩大
// 到 `o11y`，从而触发 Rust 的“重导出可见性高于原条目”错误。
use cc_log_collector::{CcLogCollector, new_cc_log_collector};
use diag_logs::append_and_cap_logs;
use types::{DiagFile, DiagState};

#[cfg(test)]
mod diag_logs_test;
