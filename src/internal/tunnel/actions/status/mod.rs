//! 本地服务端口状态 action。
//!
//! 对应 Go `internal/tunnel/actions/status`，用于后端通过控制 Tunnel 判断本地开发
//! 服务是否已经监听。

mod action;
mod zz_recovered_types;

pub use action::{STATUS_TIMEOUT, new_status_action};
pub use zz_recovered_types::StatusAction;

#[cfg(test)]
mod action_test;
