//! 版本化 stdin package manifest 的一次性 Linux 安装子系统。
//!
//! 目录逐文件对应 Go `internal/packages`：`manifest` 负责严格输入边界，`application`
//! 生成固定安装计划，`executor` 管理 Linux 进程组，`result` 定义唯一外部 JSON。
//! package spec 与 manager 输出都不会写入日志、stderr 或终态结果。

mod application;
mod executor;
mod manifest;
mod result;

pub(crate) use application::ProvisionContext;
pub use application::Provisioner;
pub use result::{Category, Manager, Result, Stage, Status};

#[cfg(test)]
#[path = "application_test.rs"]
mod application_test;

#[cfg(test)]
#[path = "executor_test.rs"]
mod executor_test;
