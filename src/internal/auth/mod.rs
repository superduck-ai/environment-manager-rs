//! 认证配置、运行时凭据上下文与源码仓库 URL 认证适配器。
//!
//! 本模块机械对应 Go 的 `internal/auth` 包。模块只负责解析和保存凭据，
//! 不负责从磁盘或网络获取密钥；调用方应在更外层完成秘密注入与生命周期管理。

pub mod context;
pub mod github_source_provider;
pub mod types;

pub use context::{AuthError, new_auth_context_with_session_id};
pub use github_source_provider::new_github_source_auth_provider;
pub use types::{
    AuthConfig, AuthContext, GitHubAuthType, GitHubSourceAuthProvider, Permission,
    SourceAuthProvider,
};

#[cfg(test)]
mod auth_test;
