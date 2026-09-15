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

tokio::task_local! {
    /// 仅在 source 初始化 future 被 poll 时生效，不修改全局进程环境。
    /// source Git 在当前任务中执行；若将来改为 spawn，必须显式传递此 scope。
    pub(crate) static GIT_ENVIRONMENT: Vec<(std::ffi::OsString, std::ffi::OsString)>;
}

#[cfg(test)]
#[path = "git_test.rs"]
mod git_test;
