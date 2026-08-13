//! source 获取、Git clone/fetch、outcome branch 与远端 URL 后处理。
//!
//! 所有外部 Git 操作调用系统 `git`，不嵌入 libgit2。取消通过
//! [`tokio_util::sync::CancellationToken`] 传播，并且只终止直接子进程。

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
