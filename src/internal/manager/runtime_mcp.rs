//! Embedded runtime-tools server owned by a manager run.
use super::ManagerError;
use crate::internal::{
    config::ClaudeConfig,
    mcp::servers::runtime_tools::{
        self, Config, Registry, Server, environment::EnvironmentProvider, plugins::Manager,
    },
};
use async_trait::async_trait;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

struct RuntimeEnvironment(BTreeMap<String, String>);
#[async_trait]
impl EnvironmentProvider for RuntimeEnvironment {
    async fn environ(&self, _: CancellationToken) -> Result<BTreeMap<String, String>, String> {
        Ok(self.0.clone())
    }
}

pub(super) struct RuntimeMcpServer {
    cancellation: CancellationToken,
    task: Option<JoinHandle<std::io::Result<()>>>,
    server: Arc<Server>,
}
impl RuntimeMcpServer {
    pub async fn start(
        configuration: &ClaudeConfig,
        parent: &CancellationToken,
        proxy: Option<&crate::internal::claude::AgentProxyHandle>,
    ) -> Result<Self, ManagerError> {
        let address =
            std::env::var("ENVRUNNER_RUNTIME_MCP_ADDR").unwrap_or_else(|_| "0.0.0.0:3001".into());
        let listener = TcpListener::bind(&address).await.map_err(|error| {
            ManagerError::message(format!(
                "failed to start runtime MCP server on {address}: {error}"
            ))
        })?;
        let local_address = listener.local_addr().map_err(|error| {
            ManagerError::message(format!("failed to read runtime MCP address: {error}"))
        })?;
        let working_directory = if configuration.working_dir.is_empty() {
            std::env::current_dir().map_err(|error| {
                ManagerError::message(format!(
                    "failed to read runtime MCP working directory: {error}"
                ))
            })?
        } else {
            PathBuf::from(&configuration.working_dir)
        };
        let mut config = Config {
            allowed_hosts: Vec::new(),
            ..Config::default()
        };
        if let Ok(hosts) = std::env::var("MCP_ALLOWED_HOSTS") {
            config.allowed_hosts = comma_values(&hosts);
        }
        if let Ok(origins) = std::env::var("MCP_ALLOWED_ORIGINS") {
            config.allowed_origins = comma_values(&origins);
        }
        let mut environment: BTreeMap<String, String> = configuration
            .environment
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if let Some(proxy) = proxy {
            environment.extend(proxy.environment().iter().map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.to_string_lossy().into_owned(),
                )
            }));
        }
        let server = Arc::new(Server::new(
            Arc::new(Registry::builtin(
                working_directory,
                Some(Arc::new(RuntimeEnvironment(environment))),
            )),
            Arc::new(Manager::default()),
            config,
        ));
        let cancellation = parent.child_token();
        let router = server
            .clone()
            .router_with_cancellation(cancellation.clone());
        let shutdown = cancellation.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
        });
        tracing::info!(address = %local_address, protocol_version = runtime_tools::protocol::PROTOCOL_VERSION, "Runtime MCP server started");
        Ok(Self {
            cancellation,
            task: Some(task),
            server,
        })
    }
    pub async fn shutdown(mut self) {
        self.cancellation.cancel();
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(result) => tracing::warn!(?result, "Runtime MCP server stopped with an error"),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    tracing::warn!("Runtime MCP server shutdown timed out");
                }
            }
        }
        self.server.plugins.shutdown_all().await;
    }
}
impl Drop for RuntimeMcpServer {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}
fn comma_values(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}
