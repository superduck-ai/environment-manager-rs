//! Python、Node.js 与 Go 版本切换脚本。

// 保留 Go `install_scripts/install_scripts.go` 的机械文件映射。
#[allow(clippy::module_inception)]
mod install_scripts;

pub use install_scripts::{go, node, python};

#[cfg(test)]
mod install_scripts_test;
