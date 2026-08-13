//! 本地服务端口状态 action。
//!
//! 用于后端通过控制 Tunnel 判断本地开发
//! 服务是否已经监听。

mod action;
mod types;

pub use action::{STATUS_TIMEOUT, new_status_action};
pub use types::StatusAction;

#[cfg(test)]
mod action_test;
