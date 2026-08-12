//! Git proxy 的数据类型、异步接口与稳定错误边界。
//!
//! Git proxy 配置与仓库认证类型：配置和认证字段保持同名的
//! snake_case 映射，`context.Context` 映射为 `CancellationToken`，同步接口则改为
//! 可在线程间共享的异步 trait。认证 token 的 `Debug` 实现始终脱敏，避免诊断日志
//! 偶然暴露仓库凭据。

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// Git proxy 子系统的稳定文本错误。
///
/// Go 调用方依赖 `%w` 逐层拼接后的英文文案；Rust 因而在错误产生处完成兼容包装，
/// 不把 Axum、Reqwest 或 Tokio 的具体错误类型泄漏到公共接口。
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct GitProxyError {
    message: String,
}

impl GitProxyError {
    /// 使用已经完成 Go 兼容包装的文本构造错误。
    #[must_use]
    pub fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Git proxy 公共结果类型。
pub type GitProxyResult<T> = Result<T, GitProxyError>;

/// 单个仓库允许代理的认证信息。
#[derive(Clone, Default, Eq, PartialEq)]
pub struct RepoAuth {
    /// `org/repo` 形式的逻辑仓库名。
    pub repo: String,
    /// Git 主机标签；GitHub 可用空字符串、`github` 或 `github.com`。
    pub host: String,
    /// 转发给 session ingress 的仓库 token。
    pub auth_token: String,
}

impl fmt::Debug for RepoAuth {
    /// 输出仓库元数据，同时无条件隐藏认证 token。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RepoAuth")
            .field("repo", &self.repo)
            .field("host", &self.host)
            .field("auth_token", &"[REDACTED]")
            .finish()
    }
}

/// 本地 Git proxy 的完整配置。
#[derive(Clone, Default)]
pub struct Config {
    /// Session ingress 的基础 URL；仅去掉末尾 `/` 后追加恢复出的路径。
    pub session_ingress_url: String,
    /// 当前 session ID。
    pub session_id: String,
    /// 允许代理的仓库及其 token；重复仓库由最后一项覆盖。
    pub repo_auths: Vec<RepoAuth>,
    /// 监听端口；`0` 让内核分配随机 loopback 端口。
    pub port: i64,
    /// 空字符串使用默认 token 后端，`tvs` 显式选择 TVS。
    pub token_backend: String,
}

impl fmt::Debug for Config {
    /// 输出非敏感配置；嵌套 `RepoAuth` 会继续对 token 脱敏。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Config")
            .field("session_ingress_url", &self.session_ingress_url)
            .field("session_id", &self.session_id)
            .field("repo_auths", &self.repo_auths)
            .field("port", &self.port)
            .field("token_backend", &self.token_backend)
            .finish()
    }
}

/// Go `Server` 的 Tokio 异步对应接口。
#[async_trait]
pub trait Server: Send + Sync {
    /// 在 `127.0.0.1` 启动 HTTP proxy；重复启动返回 `server already started`。
    async fn start(&self, cancellation: &CancellationToken) -> GitProxyResult<()>;

    /// 发起优雅关闭并等待活跃连接；取消时保留 started 状态以便再次清理。
    async fn stop(&self, cancellation: &CancellationToken) -> GitProxyResult<()>;

    /// 返回当前监听端口；未启动或成功停止后返回配置端口。
    fn port(&self) -> i64;

    /// 返回固定使用 IPv4 loopback 的 HTTP base URL。
    fn base_url(&self) -> String;
}

/// Go `Manager` 的 Tokio 异步对应接口。
#[async_trait]
pub trait Manager: Send + Sync {
    /// 幂等启动 proxy，并返回不含用户名和仓库路径的 base URL。
    async fn start(&self, cancellation: &CancellationToken) -> GitProxyResult<String>;

    /// 幂等停止 proxy；底层关闭失败时保留 running 状态。
    async fn stop(&self, cancellation: &CancellationToken) -> GitProxyResult<()>;

    /// 返回 Git 可直接使用的 `local_proxy@` 仓库 URL；未运行时返回空字符串。
    fn get_proxy_url(&self, repo: &str) -> String;

    /// 判断 manager 是否完成过一次成功启动且尚未成功停止。
    fn is_running(&self) -> bool;
}

/// 可跨调用方安全共享的 server trait object。
pub type SharedServer = Arc<dyn Server>;

/// 可跨调用方安全共享的 manager trait object。
pub type SharedManager = Arc<dyn Manager>;
