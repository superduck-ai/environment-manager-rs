//! Tunnel action 的共享类型声明。
//!
//! Action 注册、执行与响应类型。当前命令的空接口 sender 已收紧为
//! Prost 消息参数，消除运行时反射和类型断言；网络侧 JSON/Proto 行为不变。

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// action、进度上报或 Tunnel sender 返回的线程安全错误。
pub type ActionError = Box<dyn Error + Send + Sync + 'static>;

/// 可由 Tunnel 控制路径执行的操作。
#[async_trait]
pub trait Action: Send + Sync {
    ///
    /// 在 `cancellation` 被取消或 Registry 超时前执行操作。`request` 拥有请求数据，
    /// `progress` 可发送零到多条 NDJSON 进度。实现不得在返回后继续使用 reporter。
    async fn execute(
        &self,
        cancellation: CancellationToken,
        request: ActionRequest,
        progress: Arc<dyn ProgressReporter>,
    ) -> Result<ActionResult, ActionError>;

    /// 返回控制路径中的稳定 action 名称。
    fn name(&self) -> &str;

    /// 返回零时由 Registry 使用五分钟默认值。
    fn timeout(&self) -> Duration;
}

/// 传给 action 的已解码请求。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionRequest {
    /// 原 HTTP Tunnel 请求体。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub body: Vec<u8>,
    /// 控制请求 header；当前 Registry 不会主动填充。
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    /// 当前 Tunnel session ID。
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub session_id: String,
}

/// action 的最终 HTTP 语义结果。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActionResult {
    /// 业务状态码；控制流本身仍先发送 `200 OK` 的 NDJSON header。
    pub status_code: i32,
    /// 结果原始字节；外层 JSON 按 JSON Base64 规则编码成标准 Base64。
    pub body: Vec<u8>,
}

/// action 用于发送增量进度的接口。
#[async_trait]
pub trait ProgressReporter: Send + Sync {
    ///
    /// 把一次进度更新写为非最终 NDJSON 分片；sender 写入失败会原样向 action 返回。
    async fn send_progress(&self, update: ProgressUpdate) -> Result<(), ActionError>;
}

/// 一次可选字段的进度更新。
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProgressUpdate {
    /// 当前阶段名称。
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub step: String,
    /// 面向用户的阶段说明。
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// 百分比进度；保持 IEEE-754 双精度数 的取值范围，不在类型层截断。
    #[serde(default, skip_serializing_if = "is_zero")]
    pub percent: f64,
}

/// 判断进度百分比是否为零，供 wire 序列化省略零值。
fn is_zero(value: &f64) -> bool {
    *value == 0.0
}

/// Tunnel 网络层发送响应消息的强类型接口。
#[async_trait]
pub trait ResponseSender: Send + Sync {
    /// 发送一次 HTTP 响应元数据。
    async fn send_headers(
        &self,
        response: crate::r#gen::proto::anthropic::sessions::tunnel::v1alpha::HttpTunnelResponseHeaders,
    ) -> Result<(), ActionError>;

    /// 发送响应体分片。
    async fn send_chunk(
        &self,
        chunk: crate::r#gen::proto::anthropic::sessions::tunnel::v1alpha::HttpTunnelResponseChunk,
    ) -> Result<(), ActionError>;

    /// 发送 Tunnel 协议级错误。
    async fn send_error(
        &self,
        response: crate::r#gen::proto::anthropic::sessions::tunnel::v1alpha::HttpTunnelResponseError,
    ) -> Result<(), ActionError>;
}

/// action 注册表及同名运行互斥状态。
pub struct Registry {
    /// 已注册 action；读锁只用于取得 `Arc`，不会跨 await 持有。
    pub(crate) actions: RwLock<BTreeMap<String, Arc<dyn Action>>>,
    /// 当前正在执行的 action 名称集合。
    pub(crate) running: Mutex<std::collections::BTreeSet<String>>,
}
