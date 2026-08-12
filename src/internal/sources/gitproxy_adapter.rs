//! `sources` 的 Git proxy trait 到生产 `internal::gitproxy` 的薄适配。
//!
//! sources 保留可替换 factory 以隔离 Git 测试，但生产 `GitHandler` 必须默认使用
//! 已迁移的 Tokio/Axum proxy，不能依赖上层调用方再次手工注入。

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::{GitProxyConfig, GitProxyFactory, GitProxyManager, SourcesError, SourcesResult};
use crate::internal::gitproxy;

/// 将 sources 配置转换给真实 gitproxy manager 的默认 factory。
pub(super) struct ProductionGitProxyFactory {}

impl ProductionGitProxyFactory {
    /// 创建生产 Git proxy factory。
    pub(super) fn new() -> Self {
        Self {}
    }
}

impl GitProxyFactory for ProductionGitProxyFactory {
    /// 保留 Go `gitproxy.NewManager` 的字段映射；端口 0 交给内核分配。
    fn create(&self, config: GitProxyConfig) -> SourcesResult<Arc<dyn GitProxyManager>> {
        let manager = gitproxy::new_manager(Some(gitproxy::Config {
            session_ingress_url: config.session_ingress_url,
            session_id: config.session_id,
            repo_auths: config
                .repo_auths
                .into_iter()
                .map(|repo_auth| gitproxy::RepoAuth {
                    repo: repo_auth.repo,
                    host: repo_auth.host,
                    auth_token: repo_auth.auth_token,
                })
                .collect(),
            port: 0,
            token_backend: config.token_backend,
        }))
        .map_err(|error| SourcesError::message(error.to_string()))?;
        Ok(Arc::new(ProductionGitProxyManager { inner: manager }))
    }
}

/// 把 gitproxy 的稳定错误文本转换到 sources 错误边界。
struct ProductionGitProxyManager {
    inner: gitproxy::SharedManager,
}

#[async_trait]
impl GitProxyManager for ProductionGitProxyManager {
    /// 启动真实 loopback proxy。
    async fn start(&self, cancellation: &CancellationToken) -> SourcesResult<String> {
        self.inner
            .start(cancellation)
            .await
            .map_err(|error| SourcesError::message(error.to_string()))
    }

    /// 停止真实 loopback proxy。
    async fn stop(&self, cancellation: &CancellationToken) -> SourcesResult<()> {
        self.inner
            .stop(cancellation)
            .await
            .map_err(|error| SourcesError::message(error.to_string()))
    }

    /// 转发带 `local_proxy@` 用户名的仓库 URL。
    fn get_proxy_url(&self, repo: &str) -> String {
        self.inner.get_proxy_url(repo)
    }

    /// 转发真实 manager 的已启动状态。
    fn is_running(&self) -> bool {
        self.inner.is_running()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal::sources::RepoAuth;

    /// 默认 factory 应启动并停止真实 loopback proxy，而不是测试占位实现。
    #[tokio::test]
    async fn production_factory_uses_internal_gitproxy_manager() {
        let factory = ProductionGitProxyFactory::new();
        let manager = factory
            .create(GitProxyConfig {
                session_ingress_url: "https://api.example.test".to_owned(),
                session_id: "session-1".to_owned(),
                repo_auths: vec![RepoAuth {
                    repo: "anthropic/demo".to_owned(),
                    host: "github".to_owned(),
                    auth_token: "secret".to_owned(),
                }],
                token_backend: String::new(),
            })
            .expect("valid production proxy config should construct");
        assert!(!manager.is_running());

        let cancellation = CancellationToken::new();
        let base_url = manager
            .start(&cancellation)
            .await
            .expect("real proxy should bind loopback");
        assert!(base_url.starts_with("http://127.0.0.1:"), "{base_url}");
        assert!(manager.is_running());
        assert!(
            manager
                .get_proxy_url("anthropic/demo")
                .contains("local_proxy@127.0.0.1")
        );

        manager
            .stop(&cancellation)
            .await
            .expect("real proxy should stop cleanly");
        assert!(!manager.is_running());
    }
}
