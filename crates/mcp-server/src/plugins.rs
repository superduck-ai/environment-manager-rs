//! 插件生命周期与运行服务停止客户端。
use crate::Tool;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::Duration,
};

/// 可提供工具和独立服务的插件接口。
#[async_trait]
pub trait Plugin: Send + Sync {
    /// 插件的稳定名称。
    fn name(&self) -> String;
    /// 插件说明。
    fn description(&self) -> String;
    /// 返回插件工具集合。
    fn tools(&self) -> Vec<Arc<dyn Tool>>;
    /// 执行插件初始化。
    async fn initialize(&self) -> Result<(), String>;
    /// 执行插件停止。
    async fn shutdown(&self) -> Result<(), String>;
    /// 可选独立服务名称。
    fn service_name(&self) -> Option<String> {
        None
    }
    /// 可选独立服务启动命令。
    fn service_command(&self) -> Option<String> {
        None
    }
}
/// 运行服务接口返回的状态。
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ServiceState {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub status: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub pid: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub command: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}
/// 判断省略规则中的零 PID。
fn is_zero(n: &i64) -> bool {
    *n == 0
}
/// 带十秒超时的服务停止客户端。
pub struct ServiceClient {
    base_url: String,
    client: reqwest::Client,
}
impl ServiceClient {
    /// 从 core 地址构造运行接口根 URL。
    pub fn new(core_address: &str) -> Self {
        Self {
            base_url: format!("http://{core_address}/_runtime"),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("HTTP client"),
        }
    }
    /// 停止服务；404 表示服务不存在，其他状态仍解码响应。
    pub async fn stop(&self, name: &str) -> Result<Option<ServiceState>, String> {
        let response = self
            .client
            .delete(format!("{}/services/{name}/", self.base_url))
            .send()
            .await
            .map_err(|e| format!("service stop {name}: {e}"))?;
        if response.status() == 404 {
            return Ok(None);
        }
        response
            .json()
            .await
            .map(Some)
            .map_err(|e| format!("service stop {name} decode: {e}"))
    }
}
#[derive(Default)]
struct State {
    plugins: BTreeMap<String, Arc<dyn Plugin>>,
    tools: BTreeMap<String, Arc<dyn Tool>>,
}
/// 并发安全的插件管理器。
#[derive(Default)]
pub struct Manager {
    state: RwLock<State>,
    service: Option<ServiceClient>,
}
impl Manager {
    /// 配置服务停止接口。
    pub fn with_service(core_address: &str) -> Self {
        Self {
            service: Some(ServiceClient::new(core_address)),
            ..Self::default()
        }
    }
    /// 初始化并安装插件，使其工具可被协议层发现。
    pub async fn register(&self, plugin: Arc<dyn Plugin>) -> Result<(), String> {
        plugin.initialize().await?;
        let mut s = self.state.write().unwrap_or_else(|e| e.into_inner());
        for tool in plugin.tools() {
            let name = tool.definition()["name"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            s.tools.insert(name, tool);
        }
        s.plugins.insert(plugin.name(), plugin);
        Ok(())
    }
    /// 获取插件工具的共享引用。
    pub fn get_tool(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.state
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .tools
            .get(name)
            .cloned()
    }
    /// 返回插件工具快照。
    pub fn list_tools(&self) -> Vec<Arc<dyn Tool>> {
        self.state
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .tools
            .values()
            .cloned()
            .collect()
    }
    /// 清空管理器并停止插件及其运行服务。
    pub async fn shutdown_all(&self) {
        let plugins =
            std::mem::take(&mut *self.state.write().unwrap_or_else(|e| e.into_inner())).plugins;
        for (name, p) in plugins {
            if let Err(e) = p.shutdown().await {
                crate::logging::error(
                    "failed to shutdown plugin",
                    &[("plugin", &name), ("error", &e)],
                );
            }
            if let (Some(service), Some(service_name)) = (&self.service, p.service_name())
                && let Err(e) = service.stop(&service_name).await
            {
                crate::logging::error(
                    "failed to stop plugin service",
                    &[("plugin", &name), ("service", &service_name), ("error", &e)],
                );
            }
        }
    }
}
