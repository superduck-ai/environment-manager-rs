//! 本地 Baku 草稿快照 action。
//!
//! 对应 Go `internal/tunnel/actions/snapshot`。实现位于相邻 `action.rs`，并严格限制
//! 文件类型、symlink、单文件和总读取量。

mod action;
mod zz_recovered_types;

pub use action::{SNAPSHOT_TIMEOUT, new_snapshot_action};
pub use zz_recovered_types::SnapshotAction;

#[cfg(test)]
mod action_test;
