use super::ManagerError;
use crate::internal::{
    config::ClaudeConfig,
    mcp::servers::runtime_tools::{Registry, environment::EnvironmentProvider},
};
use async_trait::async_trait;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

struct RuntimeEnvironment(BTreeMap<String, String>);
#[async_trait]
impl EnvironmentProvider for RuntimeEnvironment {
    async fn environ(&self, _: CancellationToken) -> Result<BTreeMap<String, String>, String> {
        Ok(self.0.clone())
    }
}

pub(super) struct RuntimeToolCalls {
    cancellation: CancellationToken,
    task: Option<JoinHandle<std::io::Result<()>>>,
}
impl RuntimeToolCalls {
    pub async fn start(
        configuration: &ClaudeConfig,
        parent: &CancellationToken,
        proxy: Option<&crate::internal::claude::AgentProxyHandle>,
        base: &str,
        token: &str,
    ) -> Result<Self, ManagerError> {
        let working_directory = if configuration.working_dir.is_empty() {
            std::env::current_dir().map_err(|error| {
                ManagerError::message(format!("failed to read tool working directory: {error}"))
            })?
        } else {
            PathBuf::from(&configuration.working_dir)
        };
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
        let registry = Arc::new(Registry::builtin(
            working_directory,
            Some(Arc::new(RuntimeEnvironment(environment))),
        ));
        let cancellation = parent.child_token();
        let task = tokio::spawn(super::tool_calls::run(
            base.to_owned(),
            token.to_owned(),
            registry,
            cancellation.clone(),
        ));
        Ok(Self {
            cancellation,
            task: Some(task),
        })
    }
    pub async fn shutdown(mut self) {
        self.cancellation.cancel();
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(result) => tracing::warn!(?result, "Tool claim loop stopped with an error"),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    tracing::warn!("Tool claim loop shutdown timed out");
                }
            }
        }
    }
}
impl Drop for RuntimeToolCalls {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}
