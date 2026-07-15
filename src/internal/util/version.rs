//! 进程版本信息。
//!
//! Go 实现通过可变包级变量在启动阶段写入版本。Rust 版本改用 `OnceLock`，
//! 既允许入口在启动时注入构建版本，也避免并发读取可变静态变量的数据竞争。

use std::sync::{OnceLock, RwLock};

use thiserror::Error;

static VERSION: OnceLock<String> = OnceLock::new();
static CLAUDE_CODE_VERSION: OnceLock<RwLock<Option<&'static str>>> = OnceLock::new();

/// 版本初始化失败。
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum VersionInitError {
    /// 任一版本值已经被读取或显式初始化；此时再次写入可能造成请求头不一致。
    #[error("version information has already been initialized")]
    AlreadyInitialized,
}

/// 在进程启动阶段一次性写入 runner 与 Claude Code 版本。
///
/// 调用方应在启动任何后台任务之前调用本函数。两个值采用“全有或全无”的
/// 预检查，尽量避免只写入其中一个；`OnceLock` 本身仍负责最终的线程安全。
pub fn initialize_versions(
    runner_version: impl Into<String>,
    claude_version: Option<String>,
) -> Result<(), VersionInitError> {
    if VERSION.get().is_some() || CLAUDE_CODE_VERSION.get().is_some() {
        return Err(VersionInitError::AlreadyInitialized);
    }

    VERSION
        .set(runner_version.into())
        .map_err(|_| VersionInitError::AlreadyInitialized)?;
    let claude_version = claude_version
        .filter(|value| !value.is_empty())
        .map(|value| Box::leak(value.into_boxed_str()) as &'static str);
    CLAUDE_CODE_VERSION
        .set(RwLock::new(claude_version))
        .map_err(|_| VersionInitError::AlreadyInitialized)
}

/// 返回 environment runner 版本。
///
/// 若入口没有显式初始化，则使用 Cargo 包版本。返回值具有进程生命周期，
/// 可以直接用于 HTTP header 构造而无需每次分配 `String`。
pub fn version() -> &'static str {
    VERSION
        .get_or_init(|| env!("CARGO_PKG_VERSION").to_owned())
        .as_str()
}

/// 返回已安装的 Claude Code 版本。
///
/// 未安装或未知时返回 `None`，调用方应省略 `x-claude-code-version` 请求头。
/// 构建系统也可通过 `CLAUDE_CODE_VERSION` 在编译期提供默认值。
pub fn claude_code_version() -> Option<&'static str> {
    CLAUDE_CODE_VERSION
        .get_or_init(|| RwLock::new(option_env!("CLAUDE_CODE_VERSION")))
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .copied()
}

/// 更新本进程后续请求使用的 Claude Code 版本。
///
/// 对应 Go 在安装成功后写入包级 `ClaudeCodeVersion`。ACK 可能在安装前读取过版本，
/// 因此这里不能使用“只初始化一次”的值；更新只影响后续构造的 HTTP 请求。字符串
/// 最多按安装结果泄漏一次并与进程同寿命，从而允许读侧在释放锁后安全返回静态引用。
pub fn set_claude_code_version(version: impl Into<String>) {
    let version = version.into();
    let value = if version.is_empty() {
        None
    } else {
        Some(Box::leak(version.into_boxed_str()) as &'static str)
    };
    let cell = CLAUDE_CODE_VERSION.get_or_init(|| RwLock::new(None));
    *cell
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = value;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证未显式初始化时 runner 版本与 Cargo 包版本一致。
    #[test]
    fn version_has_a_stable_default() {
        assert_eq!(version(), env!("CARGO_PKG_VERSION"));
    }
}
