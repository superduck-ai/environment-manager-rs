//! source 获取、Git clone/fetch、outcome branch 与远端 URL 后处理。
//!
//! 文件和符号保持 Go `internal/sources` 的机械映射；所有外部 Git 操作继续调用
//! 系统 `git`，不嵌入 libgit2。同步 `context.Context` 映射为
//! [`tokio_util::sync::CancellationToken`]，并保持只终止直接子进程的语义。

mod git;
mod gitproxy_adapter;
#[allow(clippy::module_inception)]
mod sources;
mod types;

pub use git::new_git_handler;
pub use sources::new_source_handler_manager;
pub use types::{
    GitHandler, GitProxyConfig, GitProxyFactory, GitProxyManager, GitRemoteUpdater, ProcessMode,
    RepoAuth, SourceHandler, SourceHandlerManager, SourceSlot, SourceValue, SourcesError,
    SourcesResult,
};

#[cfg(test)]
#[path = "git_test.rs"]
mod git_test;
