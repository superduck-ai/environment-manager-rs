//! Loopback HTTP server 的启动、随机端口发现与优雅关闭。
//!
//! 监听地址固定为 `127.0.0.1`，端口 `0` 由内核分配；每次启动都会冻结一份仓库 token
//! map 并创建禁止重定向的 rustls 客户端。
//! 关闭信号与等待完成分离，使调用方取消 `Stop` 后仍可再次进入并完成资源回收，
//! 关闭 listener 后仍允许活跃连接完成回收。

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

/// 基于 Tokio/Axum 的私有 proxy server 实现。
pub(super) struct ServerImpl {
    config: Arc<Config>,
    state: Mutex<ServerState>,
    bound_port: AtomicI64,
}

impl ServerImpl {
    /// 使用已验证配置创建尚未监听的 server。
    fn new(config: Config) -> Self {
        let bound_port = config.port;
        Self {
            config: Arc::new(config),
            state: Mutex::new(ServerState::default()),
            bound_port: AtomicI64::new(bound_port),
        }
    }
}

/// 逐项验证配置并返回共享 server 接口。
///
/// 传入 `None` 明确表示 `None` 配置。验证顺序和英文错误文本保持一致，调用方可
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
            // 重复 repo 的后一项覆盖前一项。
            repo_tokens.insert(repo_auth.repo.clone(), repo_auth.auth_token.clone());
        }
        let handler = Handler::new(Arc::clone(&self.config), new_http_client()?, repo_tokens);
        let app = Router::new().fallback(move |request: http::Request<axum::body::Body>| {
            let handler = handler.clone();
            async move { handler.handle_request(request).await }
        });

        let shutdown = CancellationToken::new();
        let serve_shutdown = shutdown.clone();
        let serve_task = tokio::spawn(async move {
            let result = axum::serve(listener, app)
                .with_graceful_shutdown(serve_shutdown.cancelled_owned())
                .await;
            if let Err(error) = result {
                tracing::error!(error = %error, "Git proxy server failed");
            }
        });

        self.bound_port.store(port, Ordering::Release);
        state.shutdown = Some(shutdown);
        state.serve_task = Some(serve_task);
        state.started = true;

        tracing::info!(base_url = %format!("http://127.0.0.1:{port}"), port, "Git proxy server started");
        Ok(())
    }

    /// 关闭 listener 并等待所有活跃连接退出；调用取消时保留生命周期资源。
    async fn stop(&self, cancellation: &CancellationToken) -> GitProxyResult<()> {
        let mut state = self.state.lock().await;
        if !state.started {
            return Ok(());
        }

        if let Some(shutdown) = &state.shutdown {
            // Shutdown 先关闭 listener；等待阶段被取消不会撤销关闭动作。
            shutdown.cancel();
        }

        if let Some(serve_task) = state.serve_task.as_mut() {
            // 无活跃连接时先让 serve loop 观察关闭信号，以便在检查调用方取消前
            // 确认服务已经退出。
            tokio::task::yield_now().await;
            if !serve_task.is_finished() {
                tokio::select! {
                    biased;
                    _ = &mut *serve_task => {}
                    _ = cancellation.cancelled() => {
                        return Err(GitProxyError::message("operation cancelled"));
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
        tracing::info!("{}", "Git proxy server stopped");
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
