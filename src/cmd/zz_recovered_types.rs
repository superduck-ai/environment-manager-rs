//! 对应 Go `cmd/zz_recovered_types.go` 的恢复类型集合。
//!
//! code-sign 的 JSON-RPC DTO 与重试参数保留在原文件基名中；TaskContext 和 stdin
//! client 则承载 task-run 已接线的 v0/v1 统一上下文。所有 JSON 字段显式保持 Go 拼写。

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::internal::{
    api::GetEnvironmentConfigResponse, auth::AuthContext, claude::Outcomes, config::StartupContext,
    logger::Logger,
};

/// stdin v0/v1 解析后交给 `task-run` 主流程的统一上下文。
///
/// 对应 Go `TaskContext`。指针字段使用 `Option` 保留 healthcheck 和错误注入测试中的
/// nil 语义；普通 session 在进入 manager 前会再次完成必需字段校验。
#[derive(Clone, Debug)]
pub(super) struct TaskContext {
    /// 会话启动配置；healthcheck 也至少携带仅含 API base URL 的上下文。
    pub(super) startup_context: Option<StartupContext>,
    /// stdin 提供的环境配置；healthcheck 为 `None`。
    pub(super) environment: Option<GetEnvironmentConfigResponse>,
    /// 已解析且脱敏 Debug 的认证上下文。
    pub(super) auth_context: Option<AuthContext>,
    /// repository 到允许分支的映射。
    pub(super) outcomes: Option<Outcomes>,
    /// stdin 覆盖后的实际 session ID。
    pub(super) session_id: String,
    /// v1 work ID；v0 为空。
    pub(super) work_id: String,
    /// v1 environment ID；v0 为空。
    pub(super) environment_id: String,
    /// 是否只执行 ACK 和健康检查。
    pub(super) is_healthcheck: bool,
}

/// 把 stdin 中已经取得的配置适配成 manager 的 API client。
///
/// 对应 Go `stdinConfigClient`；它不再次访问网络，所有 getter 返回深拷贝，以免
/// manager 对 session ID 或环境配置的修改反向污染原始 stdin 上下文。
pub(super) struct StdinConfigClient {
    /// stdin 环境配置；普通 session 必须存在。
    pub(super) config: Option<GetEnvironmentConfigResponse>,
    /// stdin/work secret 的认证上下文。
    pub(super) auth_context: Option<AuthContext>,
    /// 已校验 outcomes。
    pub(super) outcomes: Option<Outcomes>,
    /// 记录“使用 stdin 配置”事件的应用 logger。
    pub(super) logger: Logger,
}

/// 本地 code-sign MCP HTTP server 的连接配置。
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub(super) struct CodeSignConfig {
    /// loopback HTTP 监听端口。
    #[serde(deserialize_with = "deserialize_null_as_default")]
    pub(super) port: i64,
    /// `Authorization: Bearer` 使用的临时 token。
    #[serde(
        rename = "bearer_token",
        deserialize_with = "deserialize_null_as_default"
    )]
    pub(super) bearer_token: String,
}

/// MCP 工具结果中的一段文本内容。
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub(super) struct McpContent {
    /// 内容类型，当前通常为 `text`。
    #[serde(rename = "type", deserialize_with = "deserialize_null_as_default")]
    pub(super) r#type: String,
    /// 服务端返回的人类可读正文。
    #[serde(deserialize_with = "deserialize_null_as_default")]
    pub(super) text: String,
}

/// JSON-RPC error 对象。
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub(super) struct McpError {
    /// JSON-RPC 错误码。
    #[serde(deserialize_with = "deserialize_null_as_default")]
    pub(super) code: i64,
    /// 错误消息；code-sign 对外只展示此字段。
    #[serde(deserialize_with = "deserialize_null_as_default")]
    pub(super) message: String,
}

/// 发送到本地 MCP server 的 JSON-RPC 请求。
#[derive(Clone, Debug, Serialize, PartialEq)]
pub(super) struct McpRequest {
    /// 固定为 `2.0`。
    pub(super) jsonrpc: String,
    /// 每次签名生成的请求 ID。
    pub(super) id: String,
    /// 固定为 `tools/call`。
    pub(super) method: String,
    /// 工具调用参数。
    pub(super) params: McpToolCallParams,
}

/// MCP response 的可恢复字段模型。
///
/// 实际解码需要区分缺失 `result` 与显式 `null`，因此执行代码会先读取 JSON object；
/// 本类型仍保留原 Go 声明，供符号映射和协议文档直接对照。
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub(super) struct McpResponse {
    /// JSON-RPC 版本。
    #[serde(deserialize_with = "deserialize_null_as_default")]
    pub(super) jsonrpc: String,
    /// 对应请求 ID。
    #[serde(deserialize_with = "deserialize_null_as_default")]
    pub(super) id: String,
    /// 工具结果原始值。
    pub(super) result: Option<Value>,
    /// 可选 JSON-RPC 错误。
    pub(super) error: Option<McpError>,
}

/// `tools/call` 的工具名称和参数。
#[derive(Clone, Debug, Serialize, PartialEq)]
pub(super) struct McpToolCallParams {
    /// 工具名称，code-sign 固定为 `sign_file`。
    pub(super) name: String,
    /// 有序参数对象，确保请求快照稳定。
    pub(super) arguments: BTreeMap<String, Value>,
}

/// `sign_file` 工具返回值。
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub(super) struct McpToolResult {
    /// 文本或其他内容块。
    #[serde(deserialize_with = "deserialize_content_list")]
    pub(super) content: Vec<McpContent>,
    /// MCP 工具级失败标志。
    #[serde(rename = "isError", deserialize_with = "deserialize_null_as_default")]
    pub(super) is_error: bool,
}

/// Go `encoding/json` 把显式 `null` 解到标量字段时保留其零值；serde 默认会报类型
/// 错误。这个适配器只放宽 `null`，数字到字符串等真正的类型错误仍然会失败。
fn deserialize_null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

/// Go 允许 `content: null`，也允许数组中的 `null` 作为一个零值结构体；两者都在
/// 签名失败文案中回退到 `unknown error`，不能被 serde 的严格 Vec 解码提前截断。
fn deserialize_content_list<'de, D>(deserializer: D) -> Result<Vec<McpContent>, D::Error>
where
    D: Deserializer<'de>,
{
    let values = Option::<Vec<Option<McpContent>>>::deserialize(deserializer)?.unwrap_or_default();
    Ok(values.into_iter().map(Option::unwrap_or_default).collect())
}

/// code-sign HTTP 请求的有界指数退避参数。
#[derive(Clone, Copy, Debug)]
pub(super) struct RetryConfig {
    /// 首次请求之后最多重试次数。
    pub(super) max_retries: usize,
    /// 第一次重试前的基础等待。
    pub(super) initial_backoff: Duration,
    /// 退避上限；抖动在截断后计算，因此可达到上限的 130%。
    pub(super) max_backoff: Duration,
    /// 每轮基础等待的指数因子。
    pub(super) backoff_factor: f64,
}
