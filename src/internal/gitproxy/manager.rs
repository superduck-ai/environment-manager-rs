//! Git proxy manager 的幂等生命周期与仓库 URL 生成。
//!
//! 本文件机械对应 Go `manager.go`。启动和停止通过 Tokio mutex 串行化，快速状态
//! 查询使用原子值；只有底层操作成功后才切换 running。仓库 URL 固定注入
//! `local_proxy@` 用户名，让 Git 在 `info/refs` challenge 后自动发送 Basic 认证头。

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::internal::logger::{LogAttributes, LogLevel, Logger, default_slog_logger};

use super::{
    Config, GitProxyError, GitProxyResult, Manager, SharedManager, SharedServer, new_server,
};

/// Go 私有 `manager` 的 Tokio 对应实现。
pub(super) struct ManagerImpl {
    server: SharedServer,
    config: Arc<Config>,
    logger: Logger,
    operation: Mutex<()>,
    running: AtomicBool,
}

impl ManagerImpl {
    /// 使用已创建的 server 与同一份逻辑配置构造未启动 manager。
    fn new(server: SharedServer, config: Config) -> Self {
        // Go `NewManager` 会在 Config.Logger 为 nil 时立即捕获 `slog.Default()`；这里
        // 同样在构造边界完成替换，避免后续每条日志走不同的 tracing 后备路径。
        let logger = config
            .logger
            .clone()
            .unwrap_or_else(|| default_slog_logger().clone());
        Self {
            server,
            config: Arc::new(config),
            logger,
            operation: Mutex::new(()),
            running: AtomicBool::new(false),
        }
    }

    /// 写入 manager 生命周期日志，并把真实业务调用点透传给 source 兼容层。
    ///
    /// # 参数
    /// - `level`：与 Go `slog` 调用相同的日志级别。
    /// - `message`：与 Go 源码逐字一致的静态消息。
    /// - `attributes`：按 Go 参数出现顺序构造的结构化属性。
    ///
    /// # 副作用
    /// 调用配置 logger；handler 自身的写入错误与 Go `slog.Logger` 一样不会改变
    /// manager 生命周期操作的返回结果。
    #[track_caller]
    fn log(&self, level: LogLevel, message: &str, attributes: LogAttributes) {
        self.logger.log(level, message, attributes);
    }
}

/// 对应 Go `NewManager`，验证 config 并包装 server 构造错误。
///
/// `None` 在进入 server 前直接返回 `config is required`；其他验证错误统一添加
/// `failed to create server:` 前缀，与 Go `%w` 形成的可观察文本一致。
pub fn new_manager(config: Option<Config>) -> GitProxyResult<SharedManager> {
    let config = config.ok_or_else(|| GitProxyError::message("config is required"))?;
    let server = new_server(Some(config.clone()))
        .map_err(|error| GitProxyError::message(format!("failed to create server: {error}")))?;
    Ok(Arc::new(ManagerImpl::new(server, config)))
}

#[async_trait]
impl Manager for ManagerImpl {
    /// 幂等启动底层 server，并只在成功后发布 running 状态。
    async fn start(&self, cancellation: &CancellationToken) -> GitProxyResult<String> {
        let _operation = self.operation.lock().await;
        if self.running.load(Ordering::Acquire) {
            return Ok(self.server.base_url());
        }

        self.log(
            LogLevel::Info,
            "Starting git proxy manager",
            LogAttributes::new(),
        );
        self.server
            .start(cancellation)
            .await
            .map_err(|error| GitProxyError::message(format!("failed to start server: {error}")))?;
        self.running.store(true, Ordering::Release);

        let base_url = self.server.base_url();
        let mut attributes = LogAttributes::new();
        attributes.insert(
            "base_url".to_owned(),
            serde_json::Value::String(base_url.clone()),
        );
        attributes.insert(
            "port".to_owned(),
            serde_json::Value::from(self.server.port()),
        );
        self.log(LogLevel::Info, "Git proxy manager started", attributes);
        Ok(base_url)
    }

    /// 幂等停止底层 server；关闭失败时继续报告 running，允许调用方再次清理。
    async fn stop(&self, cancellation: &CancellationToken) -> GitProxyResult<()> {
        let _operation = self.operation.lock().await;
        if !self.running.load(Ordering::Acquire) {
            return Ok(());
        }

        self.log(
            LogLevel::Info,
            "Stopping git proxy manager",
            LogAttributes::new(),
        );
        self.server
            .stop(cancellation)
            .await
            .map_err(|error| GitProxyError::message(format!("failed to stop server: {error}")))?;
        self.running.store(false, Ordering::Release);
        self.log(
            LogLevel::Info,
            "Git proxy manager stopped",
            LogAttributes::new(),
        );
        Ok(())
    }

    /// 返回 GitHub 或 host-qualified GHE 的本地 proxy URL。
    fn get_proxy_url(&self, repo: &str) -> String {
        if !self.running.load(Ordering::Acquire) {
            return String::new();
        }

        let base_url = self
            .server
            .base_url()
            .replacen("http://", "http://local_proxy@", 1);
        let host = self
            .config
            .repo_auths
            .iter()
            .find(|repo_auth| repo_auth.repo == repo)
            .map(|repo_auth| repo_auth.host.as_str())
            .unwrap_or_default();
        if !host.is_empty()
            && !host.eq_ignore_ascii_case("github")
            && !host.eq_ignore_ascii_case("github.com")
        {
            return format!("{base_url}/git/{host}/{repo}");
        }
        format!("{base_url}/git/{repo}")
    }

    /// 返回最近一次成功生命周期操作发布的 running 状态。
    fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }
}
