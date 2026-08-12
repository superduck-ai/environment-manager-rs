//! Loopback HTTP server 的启动、随机端口发现与优雅关闭。
//!
//! 本文件机械对应 Go `server.go`。监听地址固定为 `127.0.0.1`，端口 `0` 由内核
//! 分配；每次启动都会冻结一份仓库 token map 并创建禁止重定向的 rustls 客户端。
//! 关闭信号与等待完成分离，使调用方取消 `Stop` 后仍可再次进入并完成资源回收，
//! 对应 `http.Server.Shutdown(ctx)` 已关闭 listener 但可能尚有活跃连接的语义。

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

use async_trait::async_trait;
use axum::Router;
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::internal::logger::{LogAttributes, LogLevel, Logger, default_logger};

use super::{
    Config, GitProxyError, GitProxyResult, Server, SharedServer,
    handler::{Handler, new_http_client},
};

/// server 受串行化保护的生命周期资源。
#[derive(Default)]
struct ServerState {
    started: bool,
    shutdown: Option<CancellationToken>,
    serve_task: Option<JoinHandle<()>>,
}

/// Go 私有 `server` 的 Tokio 对应实现。
pub(super) struct ServerImpl {
    config: Arc<Config>,
    logger: Logger,
    state: Mutex<ServerState>,
    bound_port: AtomicI64,
}

impl ServerImpl {
    /// 使用已验证配置创建尚未监听的 server。
    fn new(config: Config) -> Self {
        let bound_port = config.port;
        // 在构造边界解析默认 logger，让 server、serve task 和 handler
        // 共享同一个 tracing 接收端。
        let logger = config
            .logger
            .clone()
            .unwrap_or_else(|| default_logger().clone());
        Self {
            config: Arc::new(config),
            logger,
            state: Mutex::new(ServerState::default()),
            bound_port: AtomicI64::new(bound_port),
        }
    }

    /// 记录 server 生命周期事件。
    ///
    /// # 参数
    /// - `level`：Go 调用点对应的日志级别。
    /// - `message`：与 Go 源码逐字一致的消息。
    /// - `attributes`：保持 Go 参数顺序和 JSON 标量类型的属性。
    ///
    /// # 副作用
    /// 调用结构化 logger；日志接收端错误不会改变 server 结果。
    #[track_caller]
    fn log(&self, level: LogLevel, message: &str, attributes: LogAttributes) {
        self.logger.log(level, message, attributes);
    }
}

/// 对应 Go `NewServer`，逐项验证配置并返回共享 server 接口。
///
/// 传入 `None` 明确表示 Go nil config。验证顺序和英文错误文本保持一致，调用方可
/// 继续依赖第一项失败；端口不在构造时校验，而是在 `Start` 的实际 bind 中报错。
pub fn new_server(config: Option<Config>) -> GitProxyResult<SharedServer> {
    let config = config.ok_or_else(|| GitProxyError::message("config is required"))?;
    if config.session_id.is_empty() {
        return Err(GitProxyError::message("session ID is required"));
    }
    if config.session_ingress_url.is_empty() {
        return Err(GitProxyError::message("session ingress URL is required"));
    }
    if config.repo_auths.is_empty() {
        return Err(GitProxyError::message(
            "at least one repository auth is required",
        ));
    }
    for (index, repo_auth) in config.repo_auths.iter().enumerate() {
        if repo_auth.repo.is_empty() {
            return Err(GitProxyError::message(format!(
                "repo is required for RepoAuth at index {index}"
            )));
        }
        if repo_auth.auth_token.is_empty() {
            return Err(GitProxyError::message(format!(
                "auth token is required for repo {}",
                repo_auth.repo
            )));
        }
    }
    if !config.token_backend.is_empty() && config.token_backend != "tvs" {
        return Err(GitProxyError::message(format!(
            "unsupported token backend: {}",
            config.token_backend
        )));
    }

    Ok(Arc::new(ServerImpl::new(config)))
}

#[async_trait]
impl Server for ServerImpl {
    /// 在 IPv4 loopback 上监听，并把 Axum serve loop 放入受控 Tokio task。
    async fn start(&self, _cancellation: &CancellationToken) -> GitProxyResult<()> {
        let mut state = self.state.lock().await;
        if state.started {
            return Err(GitProxyError::message("server already started"));
        }

        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.config.port))
            .await
            .map_err(|error| GitProxyError::message(error.to_string()))?;
        let port = i64::from(
            listener
                .local_addr()
                .map_err(|error| GitProxyError::message(error.to_string()))?
                .port(),
        );

        let mut repo_tokens = HashMap::with_capacity(self.config.repo_auths.len());
        for repo_auth in &self.config.repo_auths {
            // 与 Go map 赋值一致，重复 repo 的后一项覆盖前一项。
            repo_tokens.insert(repo_auth.repo.clone(), repo_auth.auth_token.clone());
        }
        let handler = Handler::new(Arc::clone(&self.config), new_http_client()?, repo_tokens);
        let app = Router::new().fallback(move |request: http::Request<axum::body::Body>| {
            let handler = handler.clone();
            async move { handler.serve_http(request).await }
        });

        let shutdown = CancellationToken::new();
        let serve_shutdown = shutdown.clone();
        let logger = self.logger.clone();
        let serve_task = tokio::spawn(async move {
            let result = axum::serve(listener, app)
                .with_graceful_shutdown(serve_shutdown.cancelled_owned())
                .await;
            if let Err(error) = result {
                let mut attributes = LogAttributes::new();
                attributes.insert(
                    "error".to_owned(),
                    serde_json::Value::String(error.to_string()),
                );
                logger.log(LogLevel::Error, "Git proxy server failed", attributes);
            }
        });

        self.bound_port.store(port, Ordering::Release);
        state.shutdown = Some(shutdown);
        state.serve_task = Some(serve_task);
        state.started = true;

        let mut attributes = LogAttributes::new();
        attributes.insert(
            "base_url".to_owned(),
            serde_json::Value::String(format!("http://127.0.0.1:{port}")),
        );
        attributes.insert("port".to_owned(), serde_json::Value::from(port));
        self.log(LogLevel::Info, "Git proxy server started", attributes);
        Ok(())
    }

    /// 关闭 listener 并等待所有活跃连接退出；调用取消时保留生命周期资源。
    async fn stop(&self, cancellation: &CancellationToken) -> GitProxyResult<()> {
        let mut state = self.state.lock().await;
        if !state.started {
            return Ok(());
        }

        if let Some(shutdown) = &state.shutdown {
            // Shutdown 与 Go 一样先关闭 listener；等待阶段被取消不会撤销关闭动作。
            shutdown.cancel();
        }

        if let Some(serve_task) = state.serve_task.as_mut() {
            // 无活跃连接时让 serve loop 先观察关闭信号，可复刻 Go 在检查 ctx 前先
            // 判断连接已经 quiescent 的行为。
            tokio::task::yield_now().await;
            if !serve_task.is_finished() {
                tokio::select! {
                    biased;
                    _ = &mut *serve_task => {}
                    _ = cancellation.cancelled() => {
                        return Err(GitProxyError::message("context canceled"));
                    }
                }
            } else {
                let _ = (&mut *serve_task).await;
            }
        }

        state.started = false;
        state.shutdown = None;
        state.serve_task = None;
        self.bound_port.store(self.config.port, Ordering::Release);
        self.log(
            LogLevel::Info,
            "Git proxy server stopped",
            LogAttributes::new(),
        );
        Ok(())
    }

    /// 返回活跃 listener 的随机端口，或未启动状态下的配置端口。
    fn port(&self) -> i64 {
        self.bound_port.load(Ordering::Acquire)
    }

    /// 以固定 IPv4 loopback 和当前端口拼出 base URL。
    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port())
    }
}
