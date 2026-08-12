//! `internal/sources` 的共享类型与跨子系统适配边界。
//!
//! Go 版本依赖 interface、可空 interface 和具体 `gitproxy.Manager`。Rust
//! 版本分别使用 trait object、[`SourceSlot`] 与显式 proxy factory 表达这些语义；
//! 生产 Git handler 默认接入真实 proxy，测试仍可覆盖或禁用 factory。

use std::{
    collections::HashMap,
    fmt,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::internal::{
    auth::{AuthContext, SourceAuthProvider},
    config::{GitRepositorySource, Source as ConfigSource},
    session::ActivityRecorder,
};

/// sources 子系统使用的稳定文本错误。
///
/// 恢复出的 Go 调用方主要依赖 `%w` 拼接后的英文文本。这里把外部库错误在产生处
/// 转成完整消息，避免公共 API 暴露 Tokio 或平台专属错误枚举。
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct SourcesError {
    message: String,
}

impl SourcesError {
    /// 从已完成兼容包装的文本构造错误。
    #[must_use]
    pub fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// sources 子系统的公共结果类型。
pub type SourcesResult<T> = Result<T, SourcesError>;

/// source 处理模式的透明字符串类型。
///
/// 不使用封闭 enum 是刻意的：Go 的 `ProcessMode` 是字符串别名，除 `fresh` 与
/// `allow-prefetched` 外的任意值都会进入 resume 分支，Rust 必须保留这一零值语义。
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct ProcessMode(pub String);

impl ProcessMode {
    /// 返回调用方传入的原始模式文本。
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ProcessMode {
    /// 从借用字符串机械构造处理模式。
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for ProcessMode {
    /// 从拥有所有权的字符串机械构造处理模式。
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Go `config.Source` 在 sources 分派层所需的最小动态接口。
///
/// 配置模块当前用封闭 enum 表示 wire source；这个窄 trait 仍允许测试和未来 source
/// 类型参与 handler 分派，同时让 Git handler 能明确拒绝错误的具体类型。
pub trait SourceValue: Send + Sync {
    /// 返回 source 的稳定类型标签。
    fn get_type(&self) -> &str;

    /// 若该值确实是 Git repository source，则返回其具体配置。
    fn as_git_repository(&self) -> Option<&GitRepositorySource> {
        None
    }
}

impl SourceValue for ConfigSource {
    /// 转发配置联合类型的标签。
    fn get_type(&self) -> &str {
        ConfigSource::get_type(self)
    }

    /// 从当前唯一的 Git variant 借出具体 source。
    fn as_git_repository(&self) -> Option<&GitRepositorySource> {
        match self {
            Self::GitRepository(source) => Some(source),
        }
    }
}

impl SourceValue for GitRepositorySource {
    /// 返回 Git source 顶层标签。
    fn get_type(&self) -> &str {
        self.base.get_type()
    }

    /// Git source 直接返回自身。
    fn as_git_repository(&self) -> Option<&GitRepositorySource> {
        Some(self)
    }
}

/// 把 Rust 切片元素映射为 Go 可空 `config.Source`。
///
/// 普通配置切片直接实现该接口；测试可传 `Option<&T>` 精确覆盖 nil source 分支，
/// 而无需在生产配置 enum 中制造非法 variant。
pub trait SourceSlot: Sync {
    /// 返回当前元素中的 source；`None` 对应 Go nil interface。
    fn source_value(&self) -> Option<&dyn SourceValue>;
}

impl SourceSlot for ConfigSource {
    /// 普通配置 source 永远非空。
    fn source_value(&self) -> Option<&dyn SourceValue> {
        Some(self)
    }
}

impl SourceSlot for Option<ConfigSource> {
    /// 保留 owned `Option<Source>` 的空值。
    fn source_value(&self) -> Option<&dyn SourceValue> {
        self.as_ref().map(|source| source as &dyn SourceValue)
    }
}

impl<T> SourceSlot for &T
where
    T: SourceValue,
{
    /// 借用的具体 source 永远非空。
    fn source_value(&self) -> Option<&dyn SourceValue> {
        Some(*self)
    }
}

impl<T> SourceSlot for Option<&T>
where
    T: SourceValue,
{
    /// 保留借用 source 的空值，主要用于兼容性测试和动态组装。
    fn source_value(&self) -> Option<&dyn SourceValue> {
        self.map(|source| source as &dyn SourceValue)
    }
}

/// 可跨异步边界共享的 source 认证提供方。
pub type SharedAuthProvider = Arc<dyn SourceAuthProvider + Send + Sync>;

/// Go `SourceHandler` 的异步 Rust 对应接口。
#[async_trait]
pub trait SourceHandler: Send + Sync {
    /// 判断 handler 是否处理给定 source 标签。
    fn can_handle(&self, source_type: &str) -> bool;

    /// 处理单个 source；取消只作用于当前路径启动的直接子进程。
    async fn process(
        &self,
        cancellation: &CancellationToken,
        source: &dyn SourceValue,
    ) -> SourcesResult<()>;

    /// 暴露可选的 Git remote 更新能力，等价于 Go 的运行时 interface assertion。
    fn as_git_remote_updater(&self) -> Option<&dyn GitRemoteUpdater> {
        None
    }

    /// 暴露具体 Git handler，供 proxy 的包级后处理阶段调用。
    ///
    /// 返回值借用 `self`，其中 recorder 生命周期保持为调用方构造 manager 时的原始
    /// 生命周期；这里不能要求 `'static`，否则局部 initialize recorder 会被迫擦除。
    fn as_git_handler(&self) -> Option<&GitHandler<'_>> {
        None
    }
}

/// 只包含 Go `gitRemoteUpdater` 所需方法的窄接口。
#[async_trait]
pub trait GitRemoteUpdater: Send + Sync {
    /// 若 source 配置了自定义 URL，则更新已存在 clone 的 origin。
    async fn update_remote_url(
        &self,
        cancellation: &CancellationToken,
        source: &dyn SourceValue,
    ) -> SourcesResult<()>;
}

/// 单个仓库交给 Git proxy 的认证配置。
#[derive(Clone, Eq, PartialEq)]
pub struct RepoAuth {
    /// 逻辑仓库名。
    pub repo: String,
    /// Git 主机标签。
    pub host: String,
    /// 仓库认证 token。
    pub auth_token: String,
}

impl fmt::Debug for RepoAuth {
    /// 输出仓库元数据并固定脱敏 token。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RepoAuth")
            .field("repo", &self.repo)
            .field("host", &self.host)
            .field("auth_token", &"[REDACTED]")
            .finish()
    }
}

/// 创建本地 Git proxy 所需的完整配置。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitProxyConfig {
    /// Session ingress 基础 URL。
    pub session_ingress_url: String,
    /// 当前 session ID。
    pub session_id: String,
    /// 所有需要代理的仓库认证。
    pub repo_auths: Vec<RepoAuth>,
    /// 空字符串使用默认后端，`tvs` 选择 TVS。
    pub token_backend: String,
}

/// 尚未迁移的 `gitproxy.Manager` 的显式异步适配接口。
#[async_trait]
pub trait GitProxyManager: Send + Sync {
    /// 启动本地 proxy 并返回 base URL；重复启动语义由实现保持。
    async fn start(&self, cancellation: &CancellationToken) -> SourcesResult<String>;

    /// 停止本地 proxy。
    async fn stop(&self, cancellation: &CancellationToken) -> SourcesResult<()>;

    /// 返回指定仓库使用的带本地 basic-auth username 的 URL。
    fn get_proxy_url(&self, repo: &str) -> String;

    /// 判断 proxy 是否已经成功启动。
    fn is_running(&self) -> bool;
}

/// 从 sources 收集出的配置创建 Git proxy manager。
pub trait GitProxyFactory: Send + Sync {
    /// 验证配置并构造尚未启动的 manager。
    fn create(&self, config: GitProxyConfig) -> SourcesResult<Arc<dyn GitProxyManager>>;
}

/// Git repository source 的处理器。
///
/// `'recorder` 是可选 activity recorder 的有效期。生产 session 通常传入拥有所有权
/// 的 `'static` recorder；Anthropic/BYOC 初始化也可安全传入仅活到当前 future 的
/// wrapper。handler 及包含它的 manager 都不得比该借用活得更久，这一约束由类型系统
/// 在构造、注册 handler 和销毁 manager 时统一检查。
pub struct GitHandler<'recorder> {
    pub(super) base_dir: PathBuf,
    pub(super) auth_context: Option<Arc<AuthContext>>,
    pub(super) sdk_base_url: String,
    pub(super) repo_branches: Option<HashMap<String, String>>,
    pub(super) last_auth_provider: Mutex<Option<SharedAuthProvider>>,
    pub(super) activity_recorder: Option<Arc<dyn ActivityRecorder + 'recorder>>,
    pub(super) process_mode: ProcessMode,
    pub(super) git_proxy_manager: RwLock<Option<Arc<dyn GitProxyManager>>>,
    pub(super) git_proxy_factory: RwLock<Option<Arc<dyn GitProxyFactory>>>,
    pub(super) should_validate: bool,
    pub(super) post_clone_hook_path: String,
}

/// 按 source 类型分派具体处理器的 manager。
///
/// `'recorder` 同时约束 manager 自己保存的 recorder 和所有动态 handler。这样 Git
/// handler 不可能经 `register_handler` 或 trait object 容器逃出 recorder 的借用范围。
pub struct SourceHandlerManager<'recorder> {
    pub(super) handlers: Vec<Arc<dyn SourceHandler + 'recorder>>,
    pub(super) base_dir: PathBuf,
    pub(super) activity_recorder: Option<Arc<dyn ActivityRecorder + 'recorder>>,
    pub(super) should_validate: bool,
}
